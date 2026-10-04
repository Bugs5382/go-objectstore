// Package check holds the request validation and error shaping that every
// backend in this module shares, so they accept and refuse the same inputs.
package check

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
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Bugs5382/go-objectstore"
)

// MaxKeyBytes is the longest key S3 accepts.
const MaxKeyBytes = 1024

// MaxMetadataBytes is S3's limit on the user metadata of one object.
const MaxMetadataBytes = 2048

// DefaultContentType is stored when a Put names none.
const DefaultContentType = "application/octet-stream"

// Fail builds the error a backend returns: kind is one of the objectstore
// sentinels and cause is the backend's own error, or nil.
func Fail(op objectstore.Op, key string, kind, cause error) error {
	if cause == nil {
		return fmt.Errorf("%w: %s %q", kind, op, key)
	}
	return fmt.Errorf("%w: %s %q: %w", kind, op, key, cause)
}

// Canceled returns the context's error wrapped with the operation when ctx is
// done, and nil otherwise.
func Canceled(ctx context.Context, op objectstore.Op, key string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("objectstore: %s %q: %w", op, key, err)
	}
	return nil
}

// Key reports whether key is usable on every backend.
func Key(op objectstore.Op, key string) error {
	switch {
	case key == "":
		return Fail(op, key, objectstore.ErrInvalid, errors.New("empty key"))
	case len(key) > MaxKeyBytes:
		return Fail(op, key, objectstore.ErrInvalid, fmt.Errorf("key longer than %d bytes", MaxKeyBytes))
	case !utf8.ValidString(key):
		return Fail(op, key, objectstore.ErrInvalid, errors.New("key is not valid UTF-8"))
	}
	return nil
}

// Metadata validates user metadata and returns it with lower-case keys.
func Metadata(op objectstore.Op, key string, md map[string]string) (map[string]string, error) {
	if len(md) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(md))
	total := 0
	for k, v := range md {
		lk := strings.ToLower(k)
		if lk == "" || strings.Trim(lk, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" {
			return nil, Fail(op, key, objectstore.ErrInvalid, fmt.Errorf("metadata key %q: use letters, digits and '-'", k))
		}
		for i := 0; i < len(v); i++ {
			if v[i] < 0x20 || v[i] > 0x7e {
				return nil, Fail(op, key, objectstore.ErrInvalid, fmt.Errorf("metadata %q: value is not printable ASCII", k))
			}
		}
		if _, dup := out[lk]; dup {
			return nil, Fail(op, key, objectstore.ErrInvalid, fmt.Errorf("metadata key %q given twice", lk))
		}
		out[lk] = v
		total += len(lk) + len(v)
	}
	if total > MaxMetadataBytes {
		return nil, Fail(op, key, objectstore.ErrInvalid, fmt.Errorf("metadata larger than %d bytes", MaxMetadataBytes))
	}
	return out, nil
}

// Put validates a Put and returns its metadata normalised and the effective
// size limit (zero for none), the smaller of the call's and the store's.
func Put(key string, opts objectstore.PutOptions, storeMax int64) (map[string]string, int64, error) {
	const op = objectstore.OpPut
	if err := Key(op, key); err != nil {
		return nil, 0, err
	}
	if opts.Size < 0 || opts.MaxSize < 0 {
		return nil, 0, Fail(op, key, objectstore.ErrInvalid, errors.New("negative size"))
	}
	md, err := Metadata(op, key, opts.Metadata)
	if err != nil {
		return nil, 0, err
	}
	limit := storeMax
	if opts.MaxSize > 0 && (limit == 0 || opts.MaxSize < limit) {
		limit = opts.MaxSize
	}
	if limit > 0 && opts.Size > limit {
		return nil, 0, Fail(op, key, objectstore.ErrTooLarge, fmt.Errorf("size %d over the limit %d", opts.Size, limit))
	}
	return md, limit, nil
}

// Range validates GetOptions.
func Range(key string, opts objectstore.GetOptions) error {
	if err := Key(objectstore.OpGet, key); err != nil {
		return err
	}
	if opts.Offset < 0 || opts.Length < 0 {
		return Fail(objectstore.OpGet, key, objectstore.ErrInvalid, errors.New("negative range"))
	}
	return nil
}

// List validates ListOptions and returns the effective page size.
func List(opts objectstore.ListOptions) (int, error) {
	if opts.Limit < 0 || opts.Limit > objectstore.MaxListLimit {
		return 0, Fail(objectstore.OpList, opts.Prefix, objectstore.ErrInvalid,
			fmt.Errorf("limit %d outside 1..%d", opts.Limit, objectstore.MaxListLimit))
	}
	if opts.Limit == 0 {
		return objectstore.MaxListLimit, nil
	}
	return opts.Limit, nil
}

// Expiry validates a presigned URL lifetime.
func Expiry(op objectstore.Op, key string, d time.Duration) error {
	if err := Key(op, key); err != nil {
		return err
	}
	if d < objectstore.MinPresignExpiry || d > objectstore.MaxPresignExpiry {
		return Fail(op, key, objectstore.ErrInvalid,
			fmt.Errorf("expiry %s outside %s..%s", d, objectstore.MinPresignExpiry, objectstore.MaxPresignExpiry))
	}
	return nil
}

// Body wraps an upload body: it counts what it reads, fails with
// ErrTooLarge as soon as the count passes limit, fails with ErrInvalid when
// the body ends before or runs past a declared size, and stops on a done
// context. Nothing past the first failure is read.
type Body struct {
	ctx   context.Context
	r     io.Reader
	key   string
	limit int64
	size  int64
	n     int64
	err   error
}

// NewBody wraps r. limit and size are zero when there is none.
func NewBody(ctx context.Context, r io.Reader, key string, limit, size int64) *Body {
	return &Body{ctx: ctx, r: r, key: key, limit: limit, size: size}
}

// N is the number of bytes read so far.
func (b *Body) N() int64 { return b.n }

// Err is the failure that stopped the body, if any; io.EOF is not a failure.
func (b *Body) Err() error { return b.err }

func (b *Body) Read(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if err := Canceled(b.ctx, objectstore.OpPut, b.key); err != nil {
		b.err = err
		return 0, err
	}
	if b.size > 0 && int64(len(p)) > b.size-b.n+1 {
		p = p[:b.size-b.n+1]
	}
	n, err := b.r.Read(p)
	b.n += int64(n)
	switch {
	case b.limit > 0 && b.n > b.limit:
		b.err = Fail(objectstore.OpPut, b.key, objectstore.ErrTooLarge, fmt.Errorf("body over the limit %d", b.limit))
		return 0, b.err
	case b.size > 0 && b.n > b.size:
		b.err = Fail(objectstore.OpPut, b.key, objectstore.ErrInvalid, fmt.Errorf("body longer than the declared size %d", b.size))
		return 0, b.err
	case errors.Is(err, io.EOF) && b.size > 0 && b.n < b.size:
		b.err = Fail(objectstore.OpPut, b.key, objectstore.ErrInvalid, fmt.Errorf("body ended at %d of the declared size %d", b.n, b.size))
		return n, b.err
	case err != nil && !errors.Is(err, io.EOF):
		b.err = fmt.Errorf("objectstore: put %q: read body: %w", b.key, err)
		return n, b.err
	}
	return n, err
}
