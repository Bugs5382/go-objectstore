// Package objectstoretest is the contract every objectstore.Store passes. A
// backend's tests call Run with a factory, and the suite checks the behaviour
// callers rely on: streaming round trips, ranges, listing, copy, presigned
// URLs over plain HTTP, size limits, errors and cancellation.
package objectstoretest

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
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bugs5382/go-objectstore"
)

// Factory returns a new, empty Store for one subtest. It registers its own
// cleanup with t.
type Factory func(t *testing.T) objectstore.Store

// Option tunes Run.
type Option func(*config)

type config struct {
	largeSize int64
	client    *http.Client
}

// WithLargeSize sets the size of the large streaming uploads. Pick one that
// spans several of the backend's parts. The default is a little over 20 MiB.
func WithLargeSize(n int64) Option { return func(c *config) { c.largeSize = n } }

// WithHTTPClient sets the client that follows presigned URLs; the default is
// a plain http.Client.
func WithHTTPClient(hc *http.Client) Option { return func(c *config) { c.client = hc } }

// Run runs the contract against stores from newStore. Subtests run in
// parallel, each on its own store.
func Run(t *testing.T, newStore Factory, opts ...Option) {
	t.Helper()
	cfg := config{largeSize: 20<<20 + 7, client: &http.Client{Timeout: time.Minute}}
	for _, o := range opts {
		o(&cfg)
	}
	tests := []struct {
		name string
		fn   func(*testing.T, objectstore.Store, config)
	}{
		{"RoundTrip", testRoundTrip},
		{"DefaultContentType", testDefaultContentType},
		{"EmptyObject", testEmptyObject},
		{"LargeStreamingUnknownSize", testLargeUnknownSize},
		{"LargeStreamingKnownSize", testLargeKnownSize},
		{"Overwrite", testOverwrite},
		{"IfNotExists", testIfNotExists},
		{"RangedGet", testRangedGet},
		{"NotFound", testNotFound},
		{"Delete", testDelete},
		{"InvalidKeys", testInvalidKeys},
		{"InvalidMetadata", testInvalidMetadata},
		{"SizeLimit", testSizeLimit},
		{"SizeMismatch", testSizeMismatch},
		{"ListPagination", testListPagination},
		{"ListDelimiter", testListDelimiter},
		{"ListInvalidLimit", testListInvalidLimit},
		{"AllWalksPages", testAll},
		{"Copy", testCopy},
		{"CopyReplaceMetadata", testCopyReplace},
		{"CopyOntoItself", testCopySelf},
		{"PresignGet", testPresignGet},
		{"PresignGetExpired", testPresignExpired},
		{"PresignGetTampered", testPresignTampered},
		{"PresignPut", testPresignPut},
		{"PresignPutWrongContentType", testPresignPutWrongType},
		{"PresignInvalidExpiry", testPresignInvalidExpiry},
		{"CancelDuringPut", testCancelPut},
		{"CancelDuringGet", testCancelGet},
		{"CanceledContext", testCanceledContext},
		{"Concurrent", testConcurrent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newStore(t), cfg)
		})
	}
}

func put(t *testing.T, s objectstore.Store, key string, data []byte, opts objectstore.PutOptions) objectstore.Info {
	t.Helper()
	info, err := s.Put(context.Background(), key, bytes.NewReader(data), opts)
	if err != nil {
		t.Fatalf("Put %q: %v", key, err)
	}
	return info
}

func read(t *testing.T, s objectstore.Store, key string, opts objectstore.GetOptions) (*objectstore.Object, []byte) {
	t.Helper()
	obj, err := s.Get(context.Background(), key, opts)
	if err != nil {
		t.Fatalf("Get %q: %v", key, err)
	}
	defer func() { _ = obj.Body.Close() }()
	data, err := io.ReadAll(obj.Body)
	if err != nil {
		t.Fatalf("read %q: %v", key, err)
	}
	return obj, data
}

func wantErr(t *testing.T, what string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s: got error %v, want %v", what, err, want)
	}
}

func wantMissing(t *testing.T, s objectstore.Store, key string) {
	t.Helper()
	_, err := s.Stat(context.Background(), key)
	wantErr(t, "Stat "+key, err, objectstore.ErrNotFound)
}

// stream is a deterministic pseudo-random body of n bytes.
func stream(seed byte, n int64) io.Reader {
	var key [32]byte
	key[0] = seed
	return io.LimitReader(rand.NewChaCha8(key), n)
}

func digest(r io.Reader) ([]byte, int64, error) {
	h := sha256.New()
	n, err := io.Copy(h, r)
	return h.Sum(nil), n, err
}

func testRoundTrip(t *testing.T, s objectstore.Store, _ config) {
	data := []byte("an editor image, more or less")
	start := time.Now().Add(-time.Minute)
	info := put(t, s, "images/a.png", data, objectstore.PutOptions{
		ContentType: "image/png",
		Metadata:    map[string]string{"Owner-Ref": "doc-1", "kind": "inline"},
	})
	if info.Key != "images/a.png" || info.Size != int64(len(data)) || info.ETag == "" {
		t.Fatalf("Put info = %+v", info)
	}
	st, err := s.Stat(context.Background(), "images/a.png")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.Size != int64(len(data)) || st.ETag != info.ETag || st.ContentType != "image/png" {
		t.Fatalf("Stat = %+v, Put = %+v", st, info)
	}
	if st.LastModified.Before(start) || st.LastModified.After(time.Now().Add(time.Minute)) {
		t.Fatalf("LastModified %v is not recent", st.LastModified)
	}
	wantMD := map[string]string{"owner-ref": "doc-1", "kind": "inline"}
	if !mapsEqual(st.Metadata, wantMD) {
		t.Fatalf("Stat metadata = %v, want %v", st.Metadata, wantMD)
	}
	obj, got := read(t, s, "images/a.png", objectstore.GetOptions{})
	if !bytes.Equal(got, data) {
		t.Fatalf("Get = %q, want %q", got, data)
	}
	if obj.Size != int64(len(data)) || obj.Offset != 0 || obj.Length != int64(len(data)) ||
		obj.ContentType != "image/png" || obj.ETag != info.ETag || !mapsEqual(obj.Metadata, wantMD) {
		t.Fatalf("Get object = %+v", obj.Info)
	}
}

func mapsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func testDefaultContentType(t *testing.T, s objectstore.Store, _ config) {
	put(t, s, "blob", []byte("x"), objectstore.PutOptions{})
	st, err := s.Stat(context.Background(), "blob")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.ContentType != "application/octet-stream" {
		t.Fatalf("ContentType = %q", st.ContentType)
	}
}

func testEmptyObject(t *testing.T, s objectstore.Store, _ config) {
	info := put(t, s, "empty", nil, objectstore.PutOptions{})
	if info.Size != 0 {
		t.Fatalf("Size = %d", info.Size)
	}
	obj, got := read(t, s, "empty", objectstore.GetOptions{})
	if len(got) != 0 || obj.Size != 0 || obj.Length != 0 {
		t.Fatalf("Get empty = %d bytes, %+v", len(got), obj)
	}
}

func testLargeUnknownSize(t *testing.T, s objectstore.Store, cfg config) {
	testLarge(t, s, cfg, 0)
}

func testLargeKnownSize(t *testing.T, s objectstore.Store, cfg config) {
	testLarge(t, s, cfg, cfg.largeSize)
}

func testLarge(t *testing.T, s objectstore.Store, cfg config, declared int64) {
	want, _, _ := digest(stream(7, cfg.largeSize))
	// Hide any WriterTo or Len so the store has to stream.
	body := struct{ io.Reader }{stream(7, cfg.largeSize)}
	info, err := s.Put(context.Background(), "large/report.pdf", body, objectstore.PutOptions{
		ContentType: "application/pdf", Size: declared,
	})
	if err != nil {
		t.Fatalf("Put large: %v", err)
	}
	if info.Size != cfg.largeSize {
		t.Fatalf("Put size = %d, want %d", info.Size, cfg.largeSize)
	}
	obj, err := s.Get(context.Background(), "large/report.pdf", objectstore.GetOptions{})
	if err != nil {
		t.Fatalf("Get large: %v", err)
	}
	defer func() { _ = obj.Body.Close() }()
	got, n, err := digest(obj.Body)
	if err != nil {
		t.Fatalf("read large: %v", err)
	}
	if n != cfg.largeSize || obj.Size != cfg.largeSize || !bytes.Equal(got, want) {
		t.Fatalf("large round trip: read %d of %d bytes, digest match %v", n, cfg.largeSize, bytes.Equal(got, want))
	}
}

func testOverwrite(t *testing.T, s objectstore.Store, _ config) {
	first := put(t, s, "doc", []byte("version one"), objectstore.PutOptions{Metadata: map[string]string{"v": "1"}})
	second := put(t, s, "doc", []byte("version two!"), objectstore.PutOptions{ContentType: "text/plain"})
	if first.ETag == second.ETag {
		t.Fatalf("ETag did not change on overwrite: %q", first.ETag)
	}
	obj, got := read(t, s, "doc", objectstore.GetOptions{})
	if string(got) != "version two!" || obj.ContentType != "text/plain" || len(obj.Metadata) != 0 {
		t.Fatalf("after overwrite: %q %+v", got, obj.Info)
	}
}

func testIfNotExists(t *testing.T, s objectstore.Store, _ config) {
	put(t, s, "once", []byte("first"), objectstore.PutOptions{IfNotExists: true})
	_, err := s.Put(context.Background(), "once", strings.NewReader("second"), objectstore.PutOptions{IfNotExists: true})
	wantErr(t, "second Put", err, objectstore.ErrAlreadyExists)
	_, got := read(t, s, "once", objectstore.GetOptions{})
	if string(got) != "first" {
		t.Fatalf("content = %q, want the first upload", got)
	}
}

func testRangedGet(t *testing.T, s objectstore.Store, _ config) {
	data := []byte("0123456789abcdefghij")
	put(t, s, "r", data, objectstore.PutOptions{ContentType: "text/plain"})
	cases := []struct {
		opts           objectstore.GetOptions
		want           string
		offset, length int64
	}{
		{objectstore.GetOptions{Offset: 5, Length: 4}, "5678", 5, 4},
		{objectstore.GetOptions{Offset: 15}, "fghij", 15, 5},
		{objectstore.GetOptions{Length: 3}, "012", 0, 3},
		{objectstore.GetOptions{Offset: 18, Length: 10}, "ij", 18, 2},
		{objectstore.GetOptions{Offset: 19, Length: 1}, "j", 19, 1},
	}
	for _, c := range cases {
		obj, got := read(t, s, "r", c.opts)
		if string(got) != c.want || obj.Offset != c.offset || obj.Length != c.length || obj.Size != 20 {
			t.Fatalf("Get %+v = %q offset %d length %d size %d; want %q offset %d length %d size 20",
				c.opts, got, obj.Offset, obj.Length, obj.Size, c.want, c.offset, c.length)
		}
		if obj.ContentType != "text/plain" {
			t.Fatalf("ranged ContentType = %q", obj.ContentType)
		}
	}
	for _, bad := range []objectstore.GetOptions{{Offset: 20}, {Offset: 25, Length: 1}, {Offset: -1}, {Length: -2}} {
		_, err := s.Get(context.Background(), "r", bad)
		wantErr(t, fmt.Sprintf("Get %+v", bad), err, objectstore.ErrInvalid)
	}
}

func testNotFound(t *testing.T, s objectstore.Store, _ config) {
	ctx := context.Background()
	_, err := s.Get(ctx, "missing", objectstore.GetOptions{})
	wantErr(t, "Get", err, objectstore.ErrNotFound)
	_, err = s.Get(ctx, "missing", objectstore.GetOptions{Offset: 1, Length: 1})
	wantErr(t, "ranged Get", err, objectstore.ErrNotFound)
	_, err = s.Stat(ctx, "missing")
	wantErr(t, "Stat", err, objectstore.ErrNotFound)
	_, err = s.Copy(ctx, "missing", "dst", objectstore.CopyOptions{})
	wantErr(t, "Copy", err, objectstore.ErrNotFound)
	if err := s.Delete(ctx, "missing"); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
}

func testDelete(t *testing.T, s objectstore.Store, _ config) {
	put(t, s, "gone", []byte("x"), objectstore.PutOptions{})
	if err := s.Delete(context.Background(), "gone"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	wantMissing(t, s, "gone")
	page, err := s.List(context.Background(), objectstore.ListOptions{})
	if err != nil || len(page.Objects) != 0 {
		t.Fatalf("List after delete = %+v, %v", page, err)
	}
}

func testInvalidKeys(t *testing.T, s objectstore.Store, _ config) {
	ctx := context.Background()
	for _, key := range []string{"", strings.Repeat("k", 1025), "bad\xffutf8"} {
		_, err := s.Put(ctx, key, strings.NewReader("x"), objectstore.PutOptions{})
		wantErr(t, "Put", err, objectstore.ErrInvalid)
		_, err = s.Get(ctx, key, objectstore.GetOptions{})
		wantErr(t, "Get", err, objectstore.ErrInvalid)
		_, err = s.Stat(ctx, key)
		wantErr(t, "Stat", err, objectstore.ErrInvalid)
		wantErr(t, "Delete", s.Delete(ctx, key), objectstore.ErrInvalid)
		_, err = s.Copy(ctx, key, "dst", objectstore.CopyOptions{})
		wantErr(t, "Copy src", err, objectstore.ErrInvalid)
		_, err = s.PresignGet(ctx, key, time.Minute)
		wantErr(t, "PresignGet", err, objectstore.ErrInvalid)
	}
	put(t, s, "fine", []byte("x"), objectstore.PutOptions{})
	_, err := s.Copy(ctx, "fine", "", objectstore.CopyOptions{})
	wantErr(t, "Copy dst", err, objectstore.ErrInvalid)
	// Path segments stay short: MinIO caps each one at 255 bytes.
	put(t, s, strings.Repeat("abc/", 255)+"abcd", []byte("x"), objectstore.PutOptions{})
}

func testInvalidMetadata(t *testing.T, s objectstore.Store, _ config) {
	for _, md := range []map[string]string{
		{"has space": "x"},
		{"under_score": "x"},
		{"ok": "line\nbreak"},
		{"ok": "café"},
		{"big": strings.Repeat("v", 2100)},
	} {
		_, err := s.Put(context.Background(), "m", strings.NewReader("x"), objectstore.PutOptions{Metadata: md})
		wantErr(t, fmt.Sprintf("Put metadata %q", md), err, objectstore.ErrInvalid)
	}
	wantMissing(t, s, "m")
}

// noRead fails the test if the store reads it.
type noRead struct{ t *testing.T }

func (n noRead) Read([]byte) (int, error) {
	n.t.Error("the store read a body it should have refused")
	return 0, io.EOF
}

func testSizeLimit(t *testing.T, s objectstore.Store, _ config) {
	ctx := context.Background()
	_, err := s.Put(ctx, "big", struct{ io.Reader }{stream(1, 3<<20)}, objectstore.PutOptions{MaxSize: 1 << 20})
	wantErr(t, "Put over MaxSize", err, objectstore.ErrTooLarge)
	wantMissing(t, s, "big")

	_, err = s.Put(ctx, "big", noRead{t}, objectstore.PutOptions{Size: 2 << 20, MaxSize: 1 << 20})
	wantErr(t, "Put with a declared size over MaxSize", err, objectstore.ErrTooLarge)
	wantMissing(t, s, "big")

	info, err := s.Put(ctx, "exact", stream(2, 1<<20), objectstore.PutOptions{MaxSize: 1 << 20})
	if err != nil || info.Size != 1<<20 {
		t.Fatalf("Put at exactly MaxSize = %+v, %v", info, err)
	}
}

func testSizeMismatch(t *testing.T, s objectstore.Store, _ config) {
	ctx := context.Background()
	_, err := s.Put(ctx, "short", strings.NewReader("12345"), objectstore.PutOptions{Size: 10})
	wantErr(t, "Put shorter than Size", err, objectstore.ErrInvalid)
	wantMissing(t, s, "short")
	_, err = s.Put(ctx, "long", strings.NewReader("1234567890"), objectstore.PutOptions{Size: 5})
	wantErr(t, "Put longer than Size", err, objectstore.ErrInvalid)
	wantMissing(t, s, "long")
}

func testListPagination(t *testing.T, s objectstore.Store, _ config) {
	var want []string
	for i := range 25 {
		key := fmt.Sprintf("p/%02d", i)
		want = append(want, key)
		put(t, s, key, bytes.Repeat([]byte{'x'}, i), objectstore.PutOptions{})
	}
	put(t, s, "other/1", []byte("x"), objectstore.PutOptions{})
	// Not "p": MinIO cannot hold an object and a prefix of the same name.
	put(t, s, "p-sibling", []byte("x"), objectstore.PutOptions{})

	var got []string
	var sizes []int
	opts := objectstore.ListOptions{Prefix: "p/", Limit: 10}
	for pages := 0; ; pages++ {
		if pages > 5 {
			t.Fatalf("pagination did not end; keys so far %v", got)
		}
		page, err := s.List(context.Background(), opts)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		sizes = append(sizes, len(page.Objects))
		for _, o := range page.Objects {
			got = append(got, o.Key)
			if o.Size != int64(len(got)-1) || o.ETag == "" || o.LastModified.IsZero() {
				t.Fatalf("listed %+v", o)
			}
		}
		if page.NextPageToken == "" {
			break
		}
		opts.PageToken = page.NextPageToken
	}
	if !slices.Equal(got, want) {
		t.Fatalf("listed %v, want %v", got, want)
	}
	if !slices.Equal(sizes, []int{10, 10, 5}) {
		t.Fatalf("page sizes %v, want [10 10 5]", sizes)
	}
}

func testListDelimiter(t *testing.T, s objectstore.Store, _ config) {
	for _, k := range []string{"a/1", "a/2", "a/deep/3", "b/1", "c", "d/1"} {
		put(t, s, k, []byte("x"), objectstore.PutOptions{})
	}
	page, err := s.List(context.Background(), objectstore.ListOptions{Delimiter: "/"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if keys := keysOf(page.Objects); !slices.Equal(keys, []string{"c"}) || !slices.Equal(page.Prefixes, []string{"a/", "b/", "d/"}) {
		t.Fatalf("List / = objects %v prefixes %v", keys, page.Prefixes)
	}
	page, err = s.List(context.Background(), objectstore.ListOptions{Prefix: "a/", Delimiter: "/"})
	if err != nil {
		t.Fatalf("List a/: %v", err)
	}
	if keys := keysOf(page.Objects); !slices.Equal(keys, []string{"a/1", "a/2"}) || !slices.Equal(page.Prefixes, []string{"a/deep/"}) {
		t.Fatalf("List a/ = objects %v prefixes %v", keys, page.Prefixes)
	}

	// One entry per page: prefixes and objects share the limit and come out
	// in key order without repeats.
	var seen []string
	opts := objectstore.ListOptions{Delimiter: "/", Limit: 1}
	for range 10 {
		page, err := s.List(context.Background(), opts)
		if err != nil {
			t.Fatalf("List page: %v", err)
		}
		if n := len(page.Objects) + len(page.Prefixes); n > 1 {
			t.Fatalf("page has %d entries, want at most 1", n)
		}
		seen = append(seen, page.Prefixes...)
		seen = append(seen, keysOf(page.Objects)...)
		if page.NextPageToken == "" {
			break
		}
		opts.PageToken = page.NextPageToken
	}
	if !slices.Equal(seen, []string{"a/", "b/", "c", "d/"}) {
		t.Fatalf("paged delimiter listing = %v", seen)
	}
}

func keysOf(infos []objectstore.Info) []string {
	keys := []string{}
	for _, i := range infos {
		keys = append(keys, i.Key)
	}
	return keys
}

func testListInvalidLimit(t *testing.T, s objectstore.Store, _ config) {
	for _, l := range []int{-1, 1001} {
		_, err := s.List(context.Background(), objectstore.ListOptions{Limit: l})
		wantErr(t, fmt.Sprintf("List limit %d", l), err, objectstore.ErrInvalid)
	}
}

func testAll(t *testing.T, s objectstore.Store, _ config) {
	for i := range 7 {
		put(t, s, fmt.Sprintf("all/%d", i), []byte("x"), objectstore.PutOptions{})
	}
	var got []string
	for info, err := range objectstore.All(context.Background(), s, objectstore.ListOptions{Prefix: "all/", Limit: 3}) {
		if err != nil {
			t.Fatalf("All: %v", err)
		}
		got = append(got, info.Key)
	}
	if len(got) != 7 || got[0] != "all/0" || got[6] != "all/6" {
		t.Fatalf("All = %v", got)
	}
	n := 0
	for range objectstore.All(context.Background(), s, objectstore.ListOptions{Prefix: "all/", Limit: 3}) {
		n++
		if n == 4 {
			break
		}
	}
	if n != 4 {
		t.Fatalf("early break yielded %d", n)
	}
}

func testCopy(t *testing.T, s objectstore.Store, _ config) {
	src := put(t, s, "src", []byte("copy me"), objectstore.PutOptions{
		ContentType: "text/plain", Metadata: map[string]string{"k": "v"},
	})
	info, err := s.Copy(context.Background(), "src", "dst/copy", objectstore.CopyOptions{})
	if err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if info.Key != "dst/copy" || info.Size != src.Size || info.ETag == "" {
		t.Fatalf("Copy info = %+v", info)
	}
	obj, got := read(t, s, "dst/copy", objectstore.GetOptions{})
	if string(got) != "copy me" || obj.ContentType != "text/plain" || !mapsEqual(obj.Metadata, map[string]string{"k": "v"}) {
		t.Fatalf("copied object %q %+v", got, obj.Info)
	}
	if _, err := s.Stat(context.Background(), "src"); err != nil {
		t.Fatalf("source gone after copy: %v", err)
	}
}

func testCopyReplace(t *testing.T, s objectstore.Store, _ config) {
	put(t, s, "src", []byte("copy me"), objectstore.PutOptions{
		ContentType: "text/plain", Metadata: map[string]string{"k": "v"},
	})
	_, err := s.Copy(context.Background(), "src", "dst", objectstore.CopyOptions{
		ReplaceMetadata: true, Metadata: map[string]string{"New": "yes"},
	})
	if err != nil {
		t.Fatalf("Copy replace: %v", err)
	}
	st, err := s.Stat(context.Background(), "dst")
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if st.ContentType != "text/plain" || !mapsEqual(st.Metadata, map[string]string{"new": "yes"}) {
		t.Fatalf("replaced = %+v", st)
	}
	_, err = s.Copy(context.Background(), "src", "dst2", objectstore.CopyOptions{
		ReplaceMetadata: true, ContentType: "text/markdown",
	})
	if err != nil {
		t.Fatalf("Copy replace type: %v", err)
	}
	st, err = s.Stat(context.Background(), "dst2")
	if err != nil || st.ContentType != "text/markdown" || len(st.Metadata) != 0 {
		t.Fatalf("replaced type = %+v, %v", st, err)
	}
	_, err = s.Copy(context.Background(), "src", "dst3", objectstore.CopyOptions{
		ReplaceMetadata: true, Metadata: map[string]string{"bad key": "x"},
	})
	wantErr(t, "Copy with bad metadata", err, objectstore.ErrInvalid)
}

func testCopySelf(t *testing.T, s objectstore.Store, _ config) {
	put(t, s, "self", []byte("x"), objectstore.PutOptions{ContentType: "text/plain"})
	_, err := s.Copy(context.Background(), "self", "self", objectstore.CopyOptions{})
	wantErr(t, "Copy onto itself", err, objectstore.ErrInvalid)
	_, err = s.Copy(context.Background(), "self", "self", objectstore.CopyOptions{
		ReplaceMetadata: true, Metadata: map[string]string{"touched": "1"},
	})
	if err != nil {
		t.Fatalf("Copy onto itself with new metadata: %v", err)
	}
	st, err := s.Stat(context.Background(), "self")
	if err != nil || st.Metadata["touched"] != "1" || st.ContentType != "text/plain" {
		t.Fatalf("after self copy %+v, %v", st, err)
	}
}

func do(t *testing.T, cfg config, p objectstore.Presigned, body io.Reader) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(p.Method, p.URL.String(), body)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	for k, vs := range p.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := cfg.client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", p.Method, p.URL.Redacted(), err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

func testPresignGet(t *testing.T, s objectstore.Store, cfg config) {
	put(t, s, "files/report.pdf", []byte("%PDF-1.7 not really"), objectstore.PutOptions{ContentType: "application/pdf"})
	before := time.Now()
	p, err := s.PresignGet(context.Background(), "files/report.pdf", 10*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	if p.Method != http.MethodGet || p.URL == nil || p.Expires.Before(before.Add(9*time.Minute)) || p.Expires.After(before.Add(11*time.Minute)) {
		t.Fatalf("Presigned = %+v", p)
	}
	code, body := do(t, cfg, p, nil)
	if code != http.StatusOK || string(body) != "%PDF-1.7 not really" {
		t.Fatalf("GET presigned = %d %q", code, body)
	}
	missing, err := s.PresignGet(context.Background(), "files/missing.pdf", time.Minute)
	if err != nil {
		t.Fatalf("PresignGet missing: %v", err)
	}
	if code, _ := do(t, cfg, missing, nil); code != http.StatusNotFound {
		t.Fatalf("GET presigned missing = %d, want 404", code)
	}
}

func testPresignExpired(t *testing.T, s objectstore.Store, cfg config) {
	put(t, s, "soon", []byte("x"), objectstore.PutOptions{})
	p, err := s.PresignGet(context.Background(), "soon", time.Second)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	time.Sleep(2500 * time.Millisecond)
	if code, _ := do(t, cfg, p, nil); code != http.StatusForbidden {
		t.Fatalf("GET expired = %d, want 403", code)
	}
}

func testPresignTampered(t *testing.T, s objectstore.Store, cfg config) {
	put(t, s, "mine", []byte("mine"), objectstore.PutOptions{})
	put(t, s, "yours", []byte("yours"), objectstore.PutOptions{})
	p, err := s.PresignGet(context.Background(), "mine", time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	u := *p.URL
	i := strings.LastIndex(u.Path, "mine")
	u.Path = u.Path[:i] + "yours" + u.Path[i+len("mine"):]
	u.RawPath = ""
	p.URL = &u
	if code, body := do(t, cfg, p, nil); code != http.StatusForbidden {
		t.Fatalf("GET tampered = %d %q, want 403", code, body)
	}
}

func testPresignPut(t *testing.T, s objectstore.Store, cfg config) {
	p, err := s.PresignPut(context.Background(), "uploads/pic.jpg", 5*time.Minute, objectstore.PresignPutOptions{ContentType: "image/jpeg"})
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}
	if p.Method != http.MethodPut || p.Header.Get("Content-Type") != "image/jpeg" {
		t.Fatalf("Presigned = %+v", p)
	}
	if code, body := do(t, cfg, p, strings.NewReader("jpeg bytes")); code != http.StatusOK {
		t.Fatalf("PUT presigned = %d %q", code, body)
	}
	st, err := s.Stat(context.Background(), "uploads/pic.jpg")
	if err != nil || st.Size != 10 || st.ContentType != "image/jpeg" {
		t.Fatalf("after presigned PUT: %+v, %v", st, err)
	}

	plain, err := s.PresignPut(context.Background(), "uploads/any", time.Minute, objectstore.PresignPutOptions{})
	if err != nil {
		t.Fatalf("PresignPut without type: %v", err)
	}
	if code, body := do(t, cfg, plain, strings.NewReader("abc")); code != http.StatusOK {
		t.Fatalf("PUT presigned without type = %d %q", code, body)
	}
	_, got := read(t, s, "uploads/any", objectstore.GetOptions{})
	if string(got) != "abc" {
		t.Fatalf("uploaded %q", got)
	}
}

func testPresignPutWrongType(t *testing.T, s objectstore.Store, cfg config) {
	p, err := s.PresignPut(context.Background(), "typed", time.Minute, objectstore.PresignPutOptions{ContentType: "image/png"})
	if err != nil {
		t.Fatalf("PresignPut: %v", err)
	}
	p.Header = http.Header{"Content-Type": {"text/html"}}
	if code, _ := do(t, cfg, p, strings.NewReader("<script>")); code != http.StatusForbidden {
		t.Fatalf("PUT with another type = %d, want 403", code)
	}
	wantMissing(t, s, "typed")
}

func testPresignInvalidExpiry(t *testing.T, s objectstore.Store, _ config) {
	for _, d := range []time.Duration{0, -time.Minute, 500 * time.Millisecond, 8 * 24 * time.Hour} {
		_, err := s.PresignGet(context.Background(), "k", d)
		wantErr(t, fmt.Sprintf("PresignGet %s", d), err, objectstore.ErrInvalid)
		_, err = s.PresignPut(context.Background(), "k", d, objectstore.PresignPutOptions{})
		wantErr(t, fmt.Sprintf("PresignPut %s", d), err, objectstore.ErrInvalid)
	}
}

// cancelAfter cancels its context once n bytes have been read, then keeps
// producing data so only the store's own cancellation can stop the upload.
type cancelAfter struct {
	r      io.Reader
	n      int64
	cancel context.CancelFunc
}

func (c *cancelAfter) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n -= int64(n)
	if c.n <= 0 {
		c.cancel()
	}
	return n, err
}

func testCancelPut(t *testing.T, s objectstore.Store, cfg config) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body := &cancelAfter{r: stream(3, 1<<40), n: cfg.largeSize / 2, cancel: cancel}
	done := make(chan error, 1)
	go func() {
		_, err := s.Put(ctx, "cancelled", body, objectstore.PutOptions{})
		done <- err
	}()
	select {
	case err := <-done:
		wantErr(t, "cancelled Put", err, context.Canceled)
	case <-time.After(2 * time.Minute):
		t.Fatal("Put did not stop after its context was cancelled")
	}
	wantMissing(t, s, "cancelled")
}

func testCancelGet(t *testing.T, s objectstore.Store, cfg config) {
	if _, err := s.Put(context.Background(), "big", stream(4, cfg.largeSize), objectstore.PutOptions{}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	obj, err := s.Get(ctx, "big", objectstore.GetOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer func() { _ = obj.Body.Close() }()
	if _, err := io.ReadFull(obj.Body, make([]byte, 1024)); err != nil {
		t.Fatalf("first read: %v", err)
	}
	cancel()
	_, err = io.Copy(io.Discard, obj.Body)
	if err == nil {
		t.Fatal("reading the body after cancel succeeded")
	}
	wantErr(t, "read after cancel", err, context.Canceled)
}

func testCanceledContext(t *testing.T, s objectstore.Store, _ config) {
	put(t, s, "here", []byte("x"), objectstore.PutOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Put(ctx, "k", strings.NewReader("x"), objectstore.PutOptions{})
	wantErr(t, "Put", err, context.Canceled)
	_, err = s.Get(ctx, "here", objectstore.GetOptions{})
	wantErr(t, "Get", err, context.Canceled)
	_, err = s.Stat(ctx, "here")
	wantErr(t, "Stat", err, context.Canceled)
	wantErr(t, "Delete", s.Delete(ctx, "here"), context.Canceled)
	_, err = s.List(ctx, objectstore.ListOptions{})
	wantErr(t, "List", err, context.Canceled)
	_, err = s.Copy(ctx, "here", "there", objectstore.CopyOptions{})
	wantErr(t, "Copy", err, context.Canceled)
	if _, err := s.Stat(context.Background(), "here"); err != nil {
		t.Fatalf("object touched by cancelled calls: %v", err)
	}
	wantMissing(t, s, "k")
	wantMissing(t, s, "there")
}

func testConcurrent(t *testing.T, s objectstore.Store, _ config) {
	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for i := range 16 {
		wg.Go(func() {
			key := fmt.Sprintf("c/%02d", i)
			data := bytes.Repeat([]byte{"abcdefghijklmnop"[i]}, 1000+i)
			if _, err := s.Put(context.Background(), key, bytes.NewReader(data), objectstore.PutOptions{}); err != nil {
				errs <- err
				return
			}
			obj, err := s.Get(context.Background(), key, objectstore.GetOptions{})
			if err != nil {
				errs <- err
				return
			}
			got, err := io.ReadAll(obj.Body)
			_ = obj.Body.Close()
			if err != nil || !bytes.Equal(got, data) {
				errs <- fmt.Errorf("%s: read %d bytes, %v", key, len(got), err)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	page, err := s.List(context.Background(), objectstore.ListOptions{Prefix: "c/"})
	if err != nil || len(page.Objects) != 16 {
		t.Fatalf("List after concurrent puts: %d objects, %v", len(page.Objects), err)
	}
}
