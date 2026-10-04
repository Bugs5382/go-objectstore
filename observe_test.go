package objectstore_test

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
	"sync"
	"testing"
	"time"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/memstore"
)

func served(t *testing.T) *memstore.Store {
	t.Helper()
	var st *memstore.Store
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		st.Handler().ServeHTTP(w, r)
	}))
	t.Cleanup(srv.Close)
	st = memstore.New(memstore.WithBaseURL(srv.URL + "/b"))
	return st
}

type recorder struct {
	mu     sync.Mutex
	events []objectstore.Event
}

func (r *recorder) record(e objectstore.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func TestObserveReportsEveryCall(t *testing.T) {
	rec := &recorder{}
	s := objectstore.Observe(served(t), rec.record)
	ctx := context.Background()

	if _, err := s.Put(ctx, "a", strings.NewReader("hello"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	obj, err := s.Get(ctx, "a", objectstore.GetOptions{Offset: 1, Length: 2})
	if err != nil {
		t.Fatal(err)
	}
	_ = obj.Body.Close()
	if _, err := s.Stat(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.List(ctx, objectstore.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Copy(ctx, "a", "b", objectstore.CopyOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PresignGet(ctx, "a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PresignPut(ctx, "c", time.Minute, objectstore.PresignPutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	_, missErr := s.Stat(ctx, "a")

	want := []struct {
		op    objectstore.Op
		key   string
		bytes int64
	}{
		{objectstore.OpPut, "a", 5},
		{objectstore.OpGet, "a", 2},
		{objectstore.OpStat, "a", 5},
		{objectstore.OpList, "", 1},
		{objectstore.OpCopy, "b", 5},
		{objectstore.OpPresignGet, "a", 0},
		{objectstore.OpPresignPut, "c", 0},
		{objectstore.OpDelete, "a", 0},
		{objectstore.OpStat, "a", 0},
	}
	if len(rec.events) != len(want) {
		t.Fatalf("got %d events, want %d: %+v", len(rec.events), len(want), rec.events)
	}
	for i, w := range want {
		e := rec.events[i]
		if e.Op != w.op || e.Key != w.key || e.Bytes != w.bytes || e.Duration <= 0 {
			t.Fatalf("event %d = %+v, want %+v", i, e, w)
		}
	}
	last := rec.events[len(rec.events)-1]
	if !errors.Is(last.Err, objectstore.ErrNotFound) || last.Err != missErr {
		t.Fatalf("last event error = %v, call returned %v", last.Err, missErr)
	}
	for _, e := range rec.events[:len(rec.events)-1] {
		if e.Err != nil {
			t.Fatalf("event %+v has an error", e)
		}
	}
}
