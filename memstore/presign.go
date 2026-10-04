package memstore

/*
MIT License

Copyright (c) 2026 Shane

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/internal/check"
)

// A page token names the last entry of the previous page and whether it was
// a common prefix, whose keys the next page must skip as a group.
func encodeToken(last string, isPrefix bool) string {
	kind := "k"
	if isPrefix {
		kind = "p"
	}
	return base64.RawURLEncoding.EncodeToString([]byte(kind + last))
}

func decodeToken(tok string) (string, bool, error) {
	if tok == "" {
		return "", false, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil || len(raw) < 2 || (raw[0] != 'k' && raw[0] != 'p') {
		return "", false, errors.New("malformed page token")
	}
	return string(raw[1:]), raw[0] == 'p', nil
}

// PresignGet signs a GET of key for the Handler.
func (s *Store) PresignGet(ctx context.Context, key string, expiry time.Duration) (objectstore.Presigned, error) {
	return s.presign(ctx, objectstore.OpPresignGet, http.MethodGet, key, expiry, "")
}

// PresignPut signs a PUT of key for the Handler.
func (s *Store) PresignPut(ctx context.Context, key string, expiry time.Duration, opts objectstore.PresignPutOptions) (objectstore.Presigned, error) {
	return s.presign(ctx, objectstore.OpPresignPut, http.MethodPut, key, expiry, opts.ContentType)
}

func (s *Store) presign(ctx context.Context, op objectstore.Op, method, key string, expiry time.Duration, contentType string) (objectstore.Presigned, error) {
	if err := check.Expiry(op, key, expiry); err != nil {
		return objectstore.Presigned{}, err
	}
	if err := check.Canceled(ctx, op, key); err != nil {
		return objectstore.Presigned{}, err
	}
	if s.base == nil {
		cause := s.baseErr
		if cause == nil {
			cause = errors.New("no base URL; set one with WithBaseURL and serve Handler there")
		}
		return objectstore.Presigned{}, check.Fail(op, key, objectstore.ErrUnavailable, cause)
	}
	expires := time.Now().Add(expiry).Truncate(time.Second)
	exp := strconv.FormatInt(expires.Unix(), 10)
	q := url.Values{"X-Expires": {exp}, "X-Signature": {s.sign(method, key, exp, contentType)}}
	header := http.Header{}
	if contentType != "" {
		q.Set("X-Content-Type", contentType)
		header.Set("Content-Type", contentType)
	}
	u := *s.base
	u.Path = s.base.Path + "/" + key
	u.RawPath = ""
	u.RawQuery = q.Encode()
	return objectstore.Presigned{Method: method, URL: &u, Header: header, Expires: expires}, nil
}

func (s *Store) sign(method, key, expires, contentType string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(strings.Join([]string{method, key, expires, contentType}, "\n")))
	return hex.EncodeToString(mac.Sum(nil))
}

// Handler serves the store's presigned URLs: a signed GET downloads, a signed
// PUT uploads. Anything unsigned, expired or altered gets 403, the way S3
// answers it.
func (s *Store) Handler() http.Handler {
	return http.HandlerFunc(s.serve)
}

func (s *Store) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	basePath := ""
	if s.base != nil {
		basePath = s.base.Path
	}
	key, ok := strings.CutPrefix(r.URL.Path, basePath+"/")
	q := r.URL.Query()
	exp := q.Get("X-Expires")
	ct := q.Get("X-Content-Type")
	want := s.sign(r.Method, key, exp, ct)
	if !ok || !hmac.Equal([]byte(want), []byte(q.Get("X-Signature"))) {
		http.Error(w, "signature does not match", http.StatusForbidden)
		return
	}
	if unix, err := strconv.ParseInt(exp, 10, 64); err != nil || time.Now().Unix() > unix {
		http.Error(w, "request has expired", http.StatusForbidden)
		return
	}
	if r.Method == http.MethodPut {
		s.servePut(w, r, key, ct)
		return
	}
	obj, err := s.lookup(objectstore.OpGet, key)
	if err != nil {
		http.Error(w, "no such key", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", obj.info.ContentType)
	w.Header().Set("ETag", `"`+obj.info.ETag+`"`)
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
	_, _ = io.Copy(w, bytes.NewReader(obj.data))
}

func (s *Store) servePut(w http.ResponseWriter, r *http.Request, key, signedType string) {
	got := r.Header.Get("Content-Type")
	if signedType != "" && got != signedType {
		http.Error(w, "signature does not match", http.StatusForbidden)
		return
	}
	if got == "" {
		got = check.DefaultContentType
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, check.NewBody(r.Context(), r.Body, key, s.max, 0)); err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, objectstore.ErrTooLarge) {
			code = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), code)
		return
	}
	info, err := s.store(key, buf.Bytes(), got, nil, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("ETag", `"`+info.ETag+`"`)
	w.WriteHeader(http.StatusOK)
}
