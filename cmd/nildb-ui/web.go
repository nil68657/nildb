package main

import (
	"bytes"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"time"
)

// webFiles is the frontend: plain HTML, CSS and ES modules, no build step.
//
//go:embed web
var webFiles embed.FS

type asset struct {
	body  []byte
	ctype string
	etag  string
}

// static serves the embedded files with a content hash as ETag, so a
// reload revalidates in one round trip and a new build is never stale.
type static struct{ files map[string]*asset }

func newStatic() (*static, error) {
	sub, err := fs.Sub(webFiles, "web")
	if err != nil {
		return nil, err
	}
	st := &static{files: make(map[string]*asset)}
	err = fs.WalkDir(sub, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		body, err := fs.ReadFile(sub, p)
		if err != nil {
			return err
		}
		ctype := mime.TypeByExtension(path.Ext(p))
		if ctype == "" {
			ctype = http.DetectContentType(body)
		}
		sum := sha256.Sum256(body)
		st.files["/"+p] = &asset{body: body, ctype: ctype, etag: `"` + hex.EncodeToString(sum[:8]) + `"`}
		return nil
	})
	if err != nil {
		return nil, err
	}
	st.files["/"] = st.files["/index.html"]
	return st, nil
}

func (st *static) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a := st.files[r.URL.Path]
	if a == nil {
		http.NotFound(w, r)
		return
	}
	h := w.Header()
	h.Set("Content-Type", a.ctype)
	h.Set("Etag", a.etag)
	h.Set("Cache-Control", "no-cache")
	http.ServeContent(w, r, r.URL.Path, time.Time{}, bytes.NewReader(a.body))
}
