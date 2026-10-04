// Package memstore is an in-memory objectstore.Store for tests. It passes the
// same contract suite as the S3 backend, so code tested against it behaves the
// same against real storage. Its presigned URLs work over plain HTTP once
// Handler is served at the base URL given to WithBaseURL.
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
	"crypto/md5" // #nosec G501 -- an S3-style ETag, not a security check
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"maps"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/internal/check"
)

// Store holds objects in memory. It is safe for concurrent use. The zero
// value is not usable; call New.
type Store struct {
	mu      sync.RWMutex
	objects map[string]*object
	max     int64
	base    *url.URL
	baseErr error
	secret  []byte
}

// object is immutable once stored: a Put or Copy replaces the pointer.
type object struct {
	data []byte
	info objectstore.Info
}

// Option configures a Store.
type Option func(*Store)

// WithMaxObjectSize caps every upload at n bytes.
func WithMaxObjectSize(n int64) Option { return func(s *Store) { s.max = n } }

// WithBaseURL sets where Handler is served, such as an httptest.Server URL
// with an optional path. Presigned URLs point there.
func WithBaseURL(u string) Option {
	return func(s *Store) {
		parsed, err := url.Parse(strings.TrimSuffix(u, "/"))
		if err != nil || parsed.Scheme == "" || parsed.Host == "" {
			s.baseErr = errors.New("memstore: base URL must be absolute")
			return
		}
		s.base = parsed
	}
}

// New returns an empty Store.
func New(opts ...Option) *Store {
	s := &Store{objects: map[string]*object{}, secret: make([]byte, 32)}
	_, _ = rand.Read(s.secret)
	for _, o := range opts {
		o(s)
	}
	return s
}

var _ objectstore.Store = (*Store)(nil)

// Put stores body in memory under key.
func (s *Store) Put(ctx context.Context, key string, body io.Reader, opts objectstore.PutOptions) (objectstore.Info, error) {
	md, limit, err := check.Put(key, opts, s.max)
	if err != nil {
		return objectstore.Info{}, err
	}
	if err := check.Canceled(ctx, objectstore.OpPut, key); err != nil {
		return objectstore.Info{}, err
	}
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, check.NewBody(ctx, struct{ io.Reader }{body}, key, limit, opts.Size)); err != nil {
		return objectstore.Info{}, err
	}
	ct := opts.ContentType
	if ct == "" {
		ct = check.DefaultContentType
	}
	return s.store(key, buf.Bytes(), ct, md, opts.IfNotExists)
}

func (s *Store) store(key string, data []byte, contentType string, md map[string]string, ifNotExists bool) (objectstore.Info, error) {
	sum := md5.Sum(data) // #nosec G401 -- an S3-style ETag
	obj := &object{data: data, info: objectstore.Info{
		Key:          key,
		Size:         int64(len(data)),
		ETag:         hex.EncodeToString(sum[:]),
		LastModified: time.Now().UTC().Truncate(time.Second),
		ContentType:  contentType,
		Metadata:     md,
	}}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.objects[key]; exists && ifNotExists {
		return objectstore.Info{}, check.Fail(objectstore.OpPut, key, objectstore.ErrAlreadyExists, nil)
	}
	s.objects[key] = obj
	info := obj.public()
	info.LastModified = time.Time{}
	return info, nil
}

func (o *object) public() objectstore.Info {
	info := o.info
	info.Metadata = maps.Clone(o.info.Metadata)
	return info
}

func (s *Store) lookup(op objectstore.Op, key string) (*object, error) {
	s.mu.RLock()
	obj, ok := s.objects[key]
	s.mu.RUnlock()
	if !ok {
		return nil, check.Fail(op, key, objectstore.ErrNotFound, nil)
	}
	return obj, nil
}

// Get opens the object, or the range opts selects.
func (s *Store) Get(ctx context.Context, key string, opts objectstore.GetOptions) (*objectstore.Object, error) {
	if err := check.Range(key, opts); err != nil {
		return nil, err
	}
	if err := check.Canceled(ctx, objectstore.OpGet, key); err != nil {
		return nil, err
	}
	obj, err := s.lookup(objectstore.OpGet, key)
	if err != nil {
		return nil, err
	}
	size := int64(len(obj.data))
	start, end := opts.Offset, size
	if start > 0 || opts.Length > 0 {
		if start >= size {
			return nil, check.Fail(objectstore.OpGet, key, objectstore.ErrInvalid, errors.New("range starts past the end"))
		}
		if opts.Length > 0 && start+opts.Length < size {
			end = start + opts.Length
		}
	}
	return &objectstore.Object{
		Info:   obj.public(),
		Offset: start,
		Length: end - start,
		Body:   io.NopCloser(&ctxReader{ctx: ctx, key: key, r: bytes.NewReader(obj.data[start:end])}),
	}, nil
}

// ctxReader stops a body once its context is done.
type ctxReader struct {
	ctx context.Context
	key string
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := check.Canceled(c.ctx, objectstore.OpGet, c.key); err != nil {
		return 0, err
	}
	return c.r.Read(p)
}

// Stat returns the object's info.
func (s *Store) Stat(ctx context.Context, key string) (objectstore.Info, error) {
	if err := check.Key(objectstore.OpStat, key); err != nil {
		return objectstore.Info{}, err
	}
	if err := check.Canceled(ctx, objectstore.OpStat, key); err != nil {
		return objectstore.Info{}, err
	}
	obj, err := s.lookup(objectstore.OpStat, key)
	if err != nil {
		return objectstore.Info{}, err
	}
	return obj.public(), nil
}

// Delete removes the object; a missing key is not an error.
func (s *Store) Delete(ctx context.Context, key string) error {
	if err := check.Key(objectstore.OpDelete, key); err != nil {
		return err
	}
	if err := check.Canceled(ctx, objectstore.OpDelete, key); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	return nil
}

// List returns one page of keys in byte order, grouping by Delimiter the way
// S3 does: objects and prefixes share the page limit.
func (s *Store) List(ctx context.Context, opts objectstore.ListOptions) (objectstore.Page, error) {
	limit, err := check.List(opts)
	if err != nil {
		return objectstore.Page{}, err
	}
	if err := check.Canceled(ctx, objectstore.OpList, opts.Prefix); err != nil {
		return objectstore.Page{}, err
	}
	after, afterPrefix, err := decodeToken(opts.PageToken)
	if err != nil {
		return objectstore.Page{}, check.Fail(objectstore.OpList, opts.Prefix, objectstore.ErrInvalid, err)
	}

	s.mu.RLock()
	keys := make([]string, 0, len(s.objects))
	for k := range s.objects {
		if strings.HasPrefix(k, opts.Prefix) && k > after && (!afterPrefix || !strings.HasPrefix(k, after)) {
			keys = append(keys, k)
		}
	}
	objs := make(map[string]*object, len(keys))
	for _, k := range keys {
		objs[k] = s.objects[k]
	}
	s.mu.RUnlock()
	slices.Sort(keys)

	page := objectstore.Page{Objects: []objectstore.Info{}}
	entries, last, lastIsPrefix := 0, "", false
	for _, k := range keys {
		cp := ""
		if opts.Delimiter != "" {
			if i := strings.Index(k[len(opts.Prefix):], opts.Delimiter); i >= 0 {
				cp = k[:len(opts.Prefix)+i+len(opts.Delimiter)]
			}
		}
		if cp != "" && lastIsPrefix && cp == last {
			continue
		}
		if entries == limit {
			page.NextPageToken = encodeToken(last, lastIsPrefix)
			break
		}
		entries++
		if cp != "" {
			page.Prefixes = append(page.Prefixes, cp)
			last, lastIsPrefix = cp, true
			continue
		}
		info := objs[k].info
		page.Objects = append(page.Objects, objectstore.Info{
			Key: info.Key, Size: info.Size, ETag: info.ETag, LastModified: info.LastModified,
		})
		last, lastIsPrefix = k, false
	}
	return page, nil
}

// Copy copies src to dst, sharing the immutable content.
func (s *Store) Copy(ctx context.Context, src, dst string, opts objectstore.CopyOptions) (objectstore.Info, error) {
	if err := check.Key(objectstore.OpCopy, src); err != nil {
		return objectstore.Info{}, err
	}
	if err := check.Key(objectstore.OpCopy, dst); err != nil {
		return objectstore.Info{}, err
	}
	if err := check.Canceled(ctx, objectstore.OpCopy, dst); err != nil {
		return objectstore.Info{}, err
	}
	md, err := check.Metadata(objectstore.OpCopy, dst, opts.Metadata)
	if err != nil {
		return objectstore.Info{}, err
	}
	obj, err := s.lookup(objectstore.OpCopy, src)
	if err != nil {
		return objectstore.Info{}, err
	}
	if src == dst && !opts.ReplaceMetadata {
		return objectstore.Info{}, check.Fail(objectstore.OpCopy, dst, objectstore.ErrInvalid,
			errors.New("copying an object onto itself needs ReplaceMetadata"))
	}
	ct := obj.info.ContentType
	if !opts.ReplaceMetadata {
		md = maps.Clone(obj.info.Metadata)
	} else if opts.ContentType != "" {
		ct = opts.ContentType
	}
	info, err := s.store(dst, obj.data, ct, md, false)
	if err != nil {
		return objectstore.Info{}, err
	}
	info.LastModified = time.Now().UTC().Truncate(time.Second)
	return info, nil
}
