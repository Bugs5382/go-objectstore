package memstore_test

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
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/memstore"
	"github.com/Bugs5382/go-objectstore/objectstoretest"
)

func newServed(t *testing.T, opts ...memstore.Option) *memstore.Store {
	t.Helper()
	var st *memstore.Store
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	st = memstore.New(append(opts, memstore.WithBaseURL(srv.URL+"/bucket"))...)
	return st
}

func TestContract(t *testing.T) {
	objectstoretest.Run(t, func(t *testing.T) objectstore.Store { return newServed(t) })
}

func TestStoreSizeLimit(t *testing.T) {
	st := newServed(t, memstore.WithMaxObjectSize(4))
	_, err := st.Put(context.Background(), "k", strings.NewReader("12345"), objectstore.PutOptions{})
	if !errors.Is(err, objectstore.ErrTooLarge) {
		t.Fatalf("Put over the store limit: %v", err)
	}
	_, err = st.Put(context.Background(), "k", strings.NewReader("1234"), objectstore.PutOptions{MaxSize: 10})
	if err != nil {
		t.Fatalf("Put at the store limit with a larger call limit: %v", err)
	}
}

func TestPresignWithoutBaseURL(t *testing.T) {
	st := memstore.New()
	_, err := st.PresignGet(context.Background(), "k", objectstore.MinPresignExpiry)
	if !errors.Is(err, objectstore.ErrUnavailable) {
		t.Fatalf("PresignGet without a base URL: %v", err)
	}
}

func TestHandlerRejectsUnsignedRequests(t *testing.T) {
	st := newServed(t)
	if _, err := st.Put(context.Background(), "k", strings.NewReader("x"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	st.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/bucket/k", nil))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("unsigned GET = %d, want 403", rec.Code)
	}
	rec = httptest.NewRecorder()
	st.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/bucket/k", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE = %d, want 405", rec.Code)
	}
}
