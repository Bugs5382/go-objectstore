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
	"strings"
	"testing"
	"time"

	"github.com/Bugs5382/go-apperr"
	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/memstore"
	"github.com/Bugs5382/go-objectstore/objectstoretest"
)

var testCodes = objectstore.Codes{
	NotFound: 4041, AlreadyExists: 4091, PermissionDenied: 4031,
	Unavailable: 5031, TooLarge: 4131, Invalid: 4001,
}

func TestWithCodesAttachesCodes(t *testing.T) {
	s := objectstore.WithCodes(memstore.New(), testCodes)
	ctx := context.Background()

	_, err := s.Stat(ctx, "missing")
	assertCoded(t, err, objectstore.ErrNotFound, 4041)

	if _, err := s.Put(ctx, "k", strings.NewReader("x"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	_, err = s.Put(ctx, "k", strings.NewReader("x"), objectstore.PutOptions{IfNotExists: true})
	assertCoded(t, err, objectstore.ErrAlreadyExists, 4091)

	_, err = s.Put(ctx, "big", strings.NewReader("12345"), objectstore.PutOptions{MaxSize: 2})
	assertCoded(t, err, objectstore.ErrTooLarge, 4131)

	_, err = s.Get(ctx, "", objectstore.GetOptions{})
	assertCoded(t, err, objectstore.ErrInvalid, 4001)

	_, err = s.PresignGet(ctx, "k", time.Minute)
	assertCoded(t, err, objectstore.ErrUnavailable, 5031)
}

func assertCoded(t *testing.T, err, sentinel error, code int) {
	t.Helper()
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v does not match %v", err, sentinel)
	}
	got, ok := apperr.Code(err)
	if !ok || got != code {
		t.Fatalf("code = %d, %v; want %d", got, ok, code)
	}
}

func TestWithCodesLeavesOtherErrorsUncoded(t *testing.T) {
	s := objectstore.WithCodes(memstore.New(), objectstore.Codes{Invalid: 4001})
	_, err := s.Stat(context.Background(), "missing")
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if code, ok := apperr.Code(err); ok {
		t.Fatalf("a zero code field still coded the error as %d", code)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = objectstore.WithCodes(memstore.New(), testCodes).Stat(ctx, "k")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if code, ok := apperr.Code(err); ok {
		t.Fatalf("a cancelled call was coded as %d", code)
	}
}

func TestWithCodesPassesSuccess(t *testing.T) {
	s := objectstore.WithCodes(memstore.New(), testCodes)
	if _, err := s.Put(context.Background(), "k", strings.NewReader("x"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(context.Background(), "k"); err != nil {
		t.Fatal(err)
	}
}

func TestDecoratorsPassTheContract(t *testing.T) {
	objectstoretest.Run(t, func(t *testing.T) objectstore.Store {
		return objectstore.Observe(objectstore.WithCodes(served(t), testCodes), func(objectstore.Event) {})
	})
}
