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
	"errors"
	"io"
	"time"

	"github.com/Bugs5382/go-apperr"
)

// Codes are the consumer's go-apperr codes, one per sentinel. A zero field
// leaves that error uncoded.
type Codes struct {
	NotFound         int
	AlreadyExists    int
	PermissionDenied int
	Unavailable      int
	TooLarge         int
	Invalid          int
}

// WithCodes wraps s so every error it returns carries the matching code from
// c through apperr.Coded. errors.Is still finds the sentinel and the cause.
func WithCodes(s Store, c Codes) Store {
	return &coded{next: s, codes: c}
}

type coded struct {
	next  Store
	codes Codes
}

func (c *coded) wrap(err error) error {
	if err == nil {
		return nil
	}
	for _, m := range []struct {
		sentinel error
		code     int
	}{
		{ErrNotFound, c.codes.NotFound},
		{ErrAlreadyExists, c.codes.AlreadyExists},
		{ErrPermissionDenied, c.codes.PermissionDenied},
		{ErrUnavailable, c.codes.Unavailable},
		{ErrTooLarge, c.codes.TooLarge},
		{ErrInvalid, c.codes.Invalid},
	} {
		if errors.Is(err, m.sentinel) {
			if m.code == 0 {
				return err
			}
			return apperr.Coded(m.code, err)
		}
	}
	return err
}

func (c *coded) Put(ctx context.Context, key string, body io.Reader, opts PutOptions) (Info, error) {
	info, err := c.next.Put(ctx, key, body, opts)
	return info, c.wrap(err)
}

func (c *coded) Get(ctx context.Context, key string, opts GetOptions) (*Object, error) {
	obj, err := c.next.Get(ctx, key, opts)
	return obj, c.wrap(err)
}

func (c *coded) Stat(ctx context.Context, key string) (Info, error) {
	info, err := c.next.Stat(ctx, key)
	return info, c.wrap(err)
}

func (c *coded) Delete(ctx context.Context, key string) error {
	return c.wrap(c.next.Delete(ctx, key))
}

func (c *coded) List(ctx context.Context, opts ListOptions) (Page, error) {
	page, err := c.next.List(ctx, opts)
	return page, c.wrap(err)
}

func (c *coded) Copy(ctx context.Context, src, dst string, opts CopyOptions) (Info, error) {
	info, err := c.next.Copy(ctx, src, dst, opts)
	return info, c.wrap(err)
}

func (c *coded) PresignGet(ctx context.Context, key string, expiry time.Duration) (Presigned, error) {
	p, err := c.next.PresignGet(ctx, key, expiry)
	return p, c.wrap(err)
}

func (c *coded) PresignPut(ctx context.Context, key string, expiry time.Duration, opts PresignPutOptions) (Presigned, error) {
	p, err := c.next.PresignPut(ctx, key, expiry, opts)
	return p, c.wrap(err)
}
