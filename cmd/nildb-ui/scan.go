package main

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// scanRequest asks for the next page of keys: SCAN from cursor ("0" to
// start) on database db, with MATCH and TYPE, until about count keys are
// found.
type scanRequest struct {
	DB     int    `json:"db"`
	Cursor string `json:"cursor"`
	Match  string `json:"match"`
	Type   string `json:"type"`
	Count  int    `json:"count"`
}

// keyInfo is one key of a page: its name, TYPE, PTTL (-1 without an
// expiry) and length (STRLEN, HLEN, LLEN, SCARD or ZCARD).
type keyInfo struct {
	K   string `json:"k"`
	B64 string `json:"b64,omitempty"` // the name's bytes when they are not UTF-8
	T   string `json:"t"`
	TTL int64  `json:"ttl"`
	N   int64  `json:"n"`
}

type scanReply struct {
	Cursor string    `json:"cursor"` // "0" once the database is exhausted
	Keys   []keyInfo `json:"keys"`
	Calls  int       `json:"calls"` // SCAN round trips this page took
	MS     float64   `json:"ms"`
}

const (
	scanDefault = 200
	scanMax     = 2000
	// scanBudget bounds one page, so a MATCH that finds nothing in 100,000
	// keys returns with its cursor instead of holding the request.
	scanBudget   = 300 * time.Millisecond
	scanMaxCalls = 64
)

var lenCommand = map[string]string{"string": "STRLEN", "hash": "HLEN", "list": "LLEN", "set": "SCARD", "zset": "ZCARD"}

func (s *server) handleScan(w http.ResponseWriter, r *http.Request) {
	var req scanRequest
	if !readJSON(w, r, &req) {
		return
	}
	if req.Cursor == "" {
		req.Cursor = "0"
	}
	if _, err := strconv.ParseUint(req.Cursor, 10, 64); err != nil {
		writeError(w, http.StatusBadRequest, "cursor must be a SCAN cursor such as 0")
		return
	}
	if req.DB < 0 || req.DB > maxDB {
		writeError(w, http.StatusBadRequest, "db must be 0 to 15")
		return
	}
	req.Type = strings.ToLower(req.Type)
	if _, ok := lenCommand[req.Type]; req.Type != "" && !ok {
		writeError(w, http.StatusBadRequest, "type must be string, hash, list, set or zset")
		return
	}
	if req.Count <= 0 {
		req.Count = scanDefault
	}
	req.Count = min(req.Count, scanMax)
	start := time.Now()
	keys, cursor, calls, err := s.scanKeys(r.Context(), &req)
	if err != nil {
		connError(w, r, err)
		return
	}
	infos, err := s.keyMeta(r.Context(), req.DB, keys, req.Type)
	if err != nil {
		connError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, scanReply{Cursor: cursor, Keys: infos, Calls: calls, MS: elapsedMS(start)})
}

// scanError is a SCAN the server refused, such as ERR invalid cursor after
// NilDB restarted.
type scanError struct{ reply string }

func (e *scanError) Error() string { return "SCAN: " + e.reply }

// scanKeys calls SCAN until it has req.Count keys, the cursor is 0, or the
// page budget is spent. Each call asks for the keys still wanted, and for
// at least 500 when MATCH or TYPE filter, since NilDB examines ten
// entries per COUNT and a narrow pattern would otherwise take many trips.
func (s *server) scanKeys(ctx context.Context, req *scanRequest) (keys [][]byte, cursor string, calls int, err error) {
	cursor = req.Cursor
	filtered := req.Type != "" || (req.Match != "" && req.Match != "*")
	deadline := time.Now().Add(scanBudget)
	for {
		per := req.Count - len(keys)
		if filtered {
			per = max(per, 500)
		}
		per = min(max(per, 10), 5000)
		args := [][]byte{[]byte("SCAN"), []byte(cursor), []byte("COUNT"), []byte(strconv.Itoa(per))}
		if req.Match != "" {
			args = append(args, []byte("MATCH"), []byte(req.Match))
		}
		if req.Type != "" {
			args = append(args, []byte("TYPE"), []byte(req.Type))
		}
		vals, err := s.pool.do(ctx, req.DB, [][][]byte{args}, true)
		if err != nil {
			return nil, "", calls, err
		}
		calls++
		v := vals[0]
		if v.IsErr() {
			return nil, "", calls, &scanError{v.Text()}
		}
		if v.Kind != KindArray || len(v.Elems) != 2 {
			return nil, "", calls, errors.New("SCAN replied with an unexpected shape")
		}
		cursor = v.Elems[0].Text()
		for _, k := range v.Elems[1].Elems {
			keys = append(keys, k.Str)
		}
		if cursor == "0" || len(keys) >= req.Count || calls >= scanMaxCalls || time.Now().After(deadline) {
			return keys, cursor, calls, nil
		}
	}
}

// keyMeta reads TYPE (unless the scan filtered on one), PTTL and the
// length of every key in two pipelines. Keys that are gone by then
// (TYPE none) are dropped.
func (s *server) keyMeta(ctx context.Context, db int, keys [][]byte, typ string) ([]keyInfo, error) {
	infos := make([]keyInfo, 0, len(keys))
	if len(keys) == 0 {
		return infos, nil
	}
	per := 1
	if typ == "" {
		per = 2
	}
	cmds := make([][][]byte, 0, per*len(keys))
	for _, k := range keys {
		if typ == "" {
			cmds = append(cmds, [][]byte{[]byte("TYPE"), k})
		}
		cmds = append(cmds, [][]byte{[]byte("PTTL"), k})
	}
	vals, err := s.pool.do(ctx, db, cmds, true)
	if err != nil {
		return nil, err
	}
	kept := make([][]byte, 0, len(keys))
	for i, k := range keys {
		in := keyInfo{T: typ}
		j := i * per
		if typ == "" {
			in.T = vals[j].Text()
			j++
		}
		if in.T == "none" {
			continue
		}
		in.TTL = vals[j].Int
		in.K, in.B64 = keyText(k)
		infos = append(infos, in)
		kept = append(kept, k)
	}
	cmds = cmds[:0]
	idx := make([]int, 0, len(kept))
	for i, in := range infos {
		if c, ok := lenCommand[in.T]; ok {
			cmds = append(cmds, [][]byte{[]byte(c), kept[i]})
			idx = append(idx, i)
		}
	}
	if len(cmds) == 0 {
		return infos, nil
	}
	if vals, err = s.pool.do(ctx, db, cmds, true); err != nil {
		return nil, err
	}
	for j, i := range idx {
		infos[i].N = vals[j].Int
	}
	return infos, nil
}

// keyText returns a key as text, with its bytes in base64 when they are
// not valid UTF-8.
func keyText(k []byte) (text, b64 string) {
	if utf8.Valid(k) {
		return string(k), ""
	}
	return strings.ToValidUTF8(string(k), "\uFFFD"), base64.StdEncoding.EncodeToString(k)
}
