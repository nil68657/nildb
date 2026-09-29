package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// options are the console's flags.
type options struct {
	nildb    string
	readonly bool
}

// server is the console: the HTTP API over the pool, the Console
// sessions and the embedded frontend.
type server struct {
	opts   options
	pool   *pool
	sess   *sessions
	cmds   *commands
	static http.Handler
	port   string // the listening port, which the Host header must name
	hello  atomic.Pointer[helloInfo]
}

// maxDB is the highest database NilDB serves (config.Databases is 16).
const maxDB = 15

// maxBody bounds a request body; DOC.INSERT of a large batch fits.
const maxBody = 64 << 20

// csp keeps the page to its own origin: no inline scripts or styles, no
// requests anywhere but this server.
const csp = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; " +
	"connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

func newServer(opts options, cfg *clientConfig, poolSize int, port string) (*server, error) {
	st, err := newStatic()
	if err != nil {
		return nil, err
	}
	s := &server{opts: opts, pool: newPool(cfg, poolSize), sess: newSessions(cfg), static: st, port: port}
	s.cmds = &commands{pool: s.pool}
	s.pool.onDial = func(c *conn) {
		h := c.hello
		s.hello.Store(&h)
		s.cmds.stale()
	}
	return s, nil
}

func (s *server) close() {
	s.sess.closeAll()
	s.pool.close()
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", s.handleStatus)
	mux.HandleFunc("GET /api/commands", s.handleCommands)
	mux.HandleFunc("POST /api/exec", s.handleExec)
	mux.HandleFunc("POST /api/scan", s.handleScan)
	mux.HandleFunc("POST /api/session/close", s.handleSessionClose)
	mux.HandleFunc("GET /api/", apiGetFallback)
	mux.Handle("GET /", s.static)
	return s.guard(mux)
}

// apiGetFallback answers GET requests for API paths that take POST, or do
// not exist, so they do not fall through to the static files.
func apiGetFallback(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/exec", "/api/scan", "/api/session/close":
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, r.URL.Path+" takes POST")
	default:
		writeError(w, http.StatusNotFound, "no API endpoint "+r.URL.Path)
	}
}

// guard sets the security headers and refuses requests another site could
// have made: a Host header that names neither localhost nor an IP address
// (DNS rebinding), and API calls whose Origin or Sec-Fetch-Site says they
// came from another page. Requests without those headers, from curl for
// example, pass.
func (s *server) guard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")
		h.Set("Cross-Origin-Resource-Policy", "same-origin")
		if !s.allowedHost(r.Host) {
			writeError(w, http.StatusForbidden, "Host "+strconv.Quote(r.Host)+" is not this console; open it by localhost or IP address")
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/") {
			h.Set("Cache-Control", "no-store")
			if msg := crossOrigin(r); msg != "" {
				writeError(w, http.StatusForbidden, msg)
				return
			}
			if r.Method == http.MethodPost {
				if mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); mt != "application/json" {
					writeError(w, http.StatusUnsupportedMediaType, "send Content-Type: application/json")
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// allowedHost accepts localhost and IP literals on the listening port.
func (s *server) allowedHost(host string) bool {
	name, port, err := net.SplitHostPort(host)
	if err != nil {
		name, port = host, "80"
	}
	if port != s.port {
		return false
	}
	return strings.EqualFold(name, "localhost") || net.ParseIP(name) != nil
}

// crossOrigin explains why a request came from another origin, or returns "".
func crossOrigin(r *http.Request) string {
	switch site := r.Header.Get("Sec-Fetch-Site"); site {
	case "", "same-origin", "none":
	default:
		return "refused a " + site + " request"
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return ""
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host != r.Host || (u.Scheme != "http" && u.Scheme != "https") {
		return "refused a request from Origin " + origin
	}
	return ""
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		code, b = http.StatusInternalServerError, []byte(`{"error":"encoding the reply failed"}`)
	}
	writeRaw(w, code, b)
}

func writeRaw(w http.ResponseWriter, code int, b []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(b)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// readJSON decodes the request body into v and answers 400 when it cannot.
func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "bad request body: "+err.Error())
		return false
	}
	return true
}

// connError answers a failure to reach NilDB or to hear back from it.
func connError(w http.ResponseWriter, r *http.Request, err error) {
	var se *selectError
	var be *blockedError
	var sc *scanError
	switch {
	case r.Context().Err() != nil:
		// The browser gave up; nobody reads the answer.
	case errors.As(err, &be):
		writeJSON(w, http.StatusForbidden, map[string]any{"error": be.msg, "blocked": true})
	case errors.As(err, &se), errors.As(err, &sc):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, errTimeout):
		writeError(w, http.StatusGatewayTimeout, err.Error())
	case errors.Is(err, errTooManySessions):
		writeError(w, http.StatusTooManyRequests, err.Error())
	default:
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "conn": true})
	}
}

// decodeArgs turns a JSON command into arguments. Each argument is a
// string, a number (sent as written), or {"b64": "..."} for bytes that
// are not UTF-8.
func decodeArgs(raw []json.RawMessage) ([][]byte, error) {
	if len(raw) == 0 {
		return nil, errors.New("empty command")
	}
	args := make([][]byte, len(raw))
	for i, m := range raw {
		m = json.RawMessage(strings.TrimSpace(string(m)))
		switch {
		case len(m) > 0 && m[0] == '"':
			var s string
			if err := json.Unmarshal(m, &s); err != nil {
				return nil, fmt.Errorf("argument %d: %v", i, err)
			}
			args[i] = []byte(s)
		case len(m) > 0 && m[0] == '{':
			var b struct {
				B64 string `json:"b64"`
			}
			if err := json.Unmarshal(m, &b); err != nil {
				return nil, fmt.Errorf("argument %d: %v", i, err)
			}
			v, err := base64.StdEncoding.DecodeString(b.B64)
			if err != nil {
				return nil, fmt.Errorf("argument %d: bad base64: %v", i, err)
			}
			args[i] = v
		default:
			var n json.Number
			if err := json.Unmarshal(m, &n); err != nil {
				return nil, fmt.Errorf("argument %d: want a string, a number or {\"b64\": ...}", i)
			}
			args[i] = []byte(n.String())
		}
	}
	return args, nil
}

// execRequest runs one command (args) or a batch (cmds) on database db.
// A batch runs on one connection in one round trip; atomic wraps it in
// MULTI and EXEC. session names a Console tab, whose connection keeps its
// state between requests.
type execRequest struct {
	Args    []json.RawMessage   `json:"args"`
	Cmds    [][]json.RawMessage `json:"cmds"`
	Atomic  bool                `json:"atomic"`
	DB      int                 `json:"db"`
	Session string              `json:"session"`
}

// maxBatch bounds the commands in one request.
const maxBatch = 20000

func (s *server) handleExec(w http.ResponseWriter, r *http.Request) {
	var req execRequest
	if !readJSON(w, r, &req) {
		return
	}
	if req.DB < 0 || req.DB > maxDB {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("db must be 0 to %d", maxDB))
		return
	}
	switch {
	case req.Args != nil && req.Cmds == nil && !req.Atomic:
		s.execOne(w, r, &req)
	case req.Cmds != nil && req.Args == nil && req.Session == "":
		s.execBatch(w, r, &req)
	default:
		writeError(w, http.StatusBadRequest, "send args (with an optional session) or cmds (with an optional atomic)")
	}
}

func elapsedMS(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000
}

func (s *server) execOne(w http.ResponseWriter, r *http.Request, req *execRequest) {
	ctx := r.Context()
	args, err := decodeArgs(req.Args)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	inSession := req.Session != ""
	if inSession && !validSessionID(req.Session) {
		writeError(w, http.StatusBadRequest, "session must be 1 to 64 letters, digits or dashes")
		return
	}
	if err := s.cmds.check(ctx, args, inSession, s.opts.readonly); err != nil {
		connError(w, r, err)
		return
	}
	start := time.Now()
	var v Value
	var st sessionState
	if inSession {
		v, st, err = s.sess.do(ctx, req.Session, req.DB, args)
	} else {
		var vals []Value
		cmds := [][][]byte{args}
		vals, err = s.pool.do(ctx, req.DB, cmds, s.cmds.readOnly(ctx, cmds))
		if err == nil {
			v = vals[0]
		}
	}
	if err != nil {
		connError(w, r, err)
		return
	}
	b := append([]byte(`{"reply":`), AppendJSON(nil, &v)...)
	b = append(b, `,"ms":`...)
	b = strconv.AppendFloat(b, elapsedMS(start), 'f', -1, 64)
	if inSession {
		sj, _ := json.Marshal(st)
		b = append(append(b, `,"session":`...), sj...)
	}
	writeRaw(w, http.StatusOK, append(b, '}'))
}

func (s *server) execBatch(w http.ResponseWriter, r *http.Request, req *execRequest) {
	ctx := r.Context()
	if len(req.Cmds) == 0 || len(req.Cmds) > maxBatch {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("cmds must hold 1 to %d commands", maxBatch))
		return
	}
	cmds := make([][][]byte, len(req.Cmds))
	for i, raw := range req.Cmds {
		args, err := decodeArgs(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("command %d: %v", i, err))
			return
		}
		if err := s.cmds.check(ctx, args, false, s.opts.readonly); err != nil {
			connError(w, r, err)
			return
		}
		cmds[i] = args
	}
	start := time.Now()
	if !req.Atomic {
		vals, err := s.pool.do(ctx, req.DB, cmds, s.cmds.readOnly(ctx, cmds))
		if err != nil {
			connError(w, r, err)
			return
		}
		b := append([]byte(`{"replies":`), appendJSONList(nil, vals)...)
		b = append(b, `,"ms":`...)
		b = strconv.AppendFloat(b, elapsedMS(start), 'f', -1, 64)
		writeRaw(w, http.StatusOK, append(b, '}'))
		return
	}
	wrapped := make([][][]byte, 0, len(cmds)+2)
	wrapped = append(wrapped, [][]byte{[]byte("MULTI")})
	wrapped = append(wrapped, cmds...)
	wrapped = append(wrapped, [][]byte{[]byte("EXEC")})
	vals, err := s.pool.do(ctx, req.DB, wrapped, false)
	if err != nil {
		connError(w, r, err)
		return
	}
	exec := vals[len(vals)-1]
	replies := vals[1 : len(vals)-1] // QUEUED, or the error that aborted EXEC
	if exec.Kind == KindArray {
		replies = exec.Elems
	}
	b := append([]byte(`{"replies":`), appendJSONList(nil, replies)...)
	b = append(b, `,"exec":`...)
	b = AppendJSON(b, &exec)
	b = append(b, `,"ms":`...)
	b = strconv.AppendFloat(b, elapsedMS(start), 'f', -1, 64)
	writeRaw(w, http.StatusOK, append(b, '}'))
}

type statusReply struct {
	NilDB     string     `json:"nildb"`
	Readonly  bool       `json:"readonly"`
	Connected bool       `json:"connected"`
	Error     string     `json:"error,omitempty"`
	PingMS    float64    `json:"pingMs,omitempty"`
	Hello     *helloInfo `json:"hello,omitempty"`
}

// handleStatus pings NilDB and reports how the console is set up.
func (s *server) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	out := statusReply{NilDB: s.opts.nildb, Readonly: s.opts.readonly}
	start := time.Now()
	vals, err := s.pool.do(ctx, 0, [][][]byte{{[]byte("PING")}}, true)
	switch {
	case err != nil:
		out.Error = err.Error()
	case vals[0].IsErr():
		out.Error = vals[0].Text()
	default:
		out.Connected, out.PingMS, out.Hello = true, elapsedMS(start), s.hello.Load()
	}
	writeJSON(w, http.StatusOK, out)
}

// handleCommands lists NilDB's commands with their flags and summaries,
// and which ones --readonly refuses.
func (s *server) handleCommands(w http.ResponseWriter, r *http.Request) {
	list, err := s.cmds.list(r.Context(), s.opts.readonly)
	if err != nil {
		connError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"commands": list, "readonly": s.opts.readonly})
}

func (s *server) handleSessionClose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Session string `json:"session"`
	}
	if !readJSON(w, r, &req) {
		return
	}
	s.sess.end(req.Session)
	w.WriteHeader(http.StatusNoContent)
}
