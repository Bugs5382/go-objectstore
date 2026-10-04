package objectstore

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
	"io"
	"time"
)

// Op names a Store operation in an Event.
type Op string

// The operations an Event reports.
const (
	OpPut        Op = "put"
	OpGet        Op = "get"
	OpStat       Op = "stat"
	OpDelete     Op = "delete"
	OpList       Op = "list"
	OpCopy       Op = "copy"
	OpPresignGet Op = "presign_get"
	OpPresignPut Op = "presign_put"
)

// Event reports one finished Store call.
type Event struct {
	Op Op
	// Key is the object key; for a List it is the prefix, for a Copy the
	// destination.
	Key      string
	Duration time.Duration
	// Bytes is the object size for a Put, Stat or Copy, the length of Body
	// for a Get, and the number of entries for a List.
	Bytes int64
	Err   error
}

// Observe wraps s and calls fn after every call with what happened, so the
// consumer logs, counts or traces store traffic with its own tools. fn runs on
// the calling goroutine and must not block.
func Observe(s Store, fn func(Event)) Store {
	return &observed{next: s, fn: fn}
}

type observed struct {
	next Store
	fn   func(Event)
}

func (o *observed) report(op Op, key string, start time.Time, n int64, err error) {
	o.fn(Event{Op: op, Key: key, Duration: time.Since(start), Bytes: n, Err: err})
}

func (o *observed) Put(ctx context.Context, key string, body io.Reader, opts PutOptions) (Info, error) {
	start := time.Now()
	info, err := o.next.Put(ctx, key, body, opts)
	o.report(OpPut, key, start, info.Size, err)
	return info, err
}

func (o *observed) Get(ctx context.Context, key string, opts GetOptions) (*Object, error) {
	start := time.Now()
	obj, err := o.next.Get(ctx, key, opts)
	var n int64
	if obj != nil {
		n = obj.Length
	}
	o.report(OpGet, key, start, n, err)
	return obj, err
}

func (o *observed) Stat(ctx context.Context, key string) (Info, error) {
	start := time.Now()
	info, err := o.next.Stat(ctx, key)
	o.report(OpStat, key, start, info.Size, err)
	return info, err
}

func (o *observed) Delete(ctx context.Context, key string) error {
	start := time.Now()
	err := o.next.Delete(ctx, key)
	o.report(OpDelete, key, start, 0, err)
	return err
}

func (o *observed) List(ctx context.Context, opts ListOptions) (Page, error) {
	start := time.Now()
	page, err := o.next.List(ctx, opts)
	o.report(OpList, opts.Prefix, start, int64(len(page.Objects)+len(page.Prefixes)), err)
	return page, err
}

func (o *observed) Copy(ctx context.Context, src, dst string, opts CopyOptions) (Info, error) {
	start := time.Now()
	info, err := o.next.Copy(ctx, src, dst, opts)
	o.report(OpCopy, dst, start, info.Size, err)
	return info, err
}

func (o *observed) PresignGet(ctx context.Context, key string, expiry time.Duration) (Presigned, error) {
	start := time.Now()
	p, err := o.next.PresignGet(ctx, key, expiry)
	o.report(OpPresignGet, key, start, 0, err)
	return p, err
}

func (o *observed) PresignPut(ctx context.Context, key string, expiry time.Duration, opts PresignPutOptions) (Presigned, error) {
	start := time.Now()
	p, err := o.next.PresignPut(ctx, key, expiry, opts)
	o.report(OpPresignPut, key, start, 0, err)
	return p, err
}
