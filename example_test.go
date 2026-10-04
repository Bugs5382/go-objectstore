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
	"fmt"
	"io"
	"strings"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/memstore"
)

func Example() {
	ctx := context.Background()
	var store objectstore.Store = memstore.New()

	_, err := store.Put(ctx, "attachments/42/notes.txt", strings.NewReader("hello"), objectstore.PutOptions{
		ContentType: "text/plain",
		Metadata:    map[string]string{"document": "42"},
		MaxSize:     10 << 20,
	})
	if err != nil {
		fmt.Println(err)
		return
	}

	obj, err := store.Get(ctx, "attachments/42/notes.txt", objectstore.GetOptions{})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = obj.Body.Close() }()
	body, _ := io.ReadAll(obj.Body)
	fmt.Println(string(body), obj.ContentType, obj.Metadata["document"])

	_, err = store.Stat(ctx, "attachments/42/missing.txt")
	fmt.Println(errors.Is(err, objectstore.ErrNotFound))
	// Output:
	// hello text/plain 42
	// true
}

func ExampleAll() {
	ctx := context.Background()
	store := memstore.New()
	for _, k := range []string{"pdf/1", "pdf/2", "pdf/3"} {
		if _, err := store.Put(ctx, k, strings.NewReader("x"), objectstore.PutOptions{}); err != nil {
			fmt.Println(err)
			return
		}
	}
	for info, err := range objectstore.All(ctx, store, objectstore.ListOptions{Prefix: "pdf/", Limit: 2}) {
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(info.Key, info.Size)
	}
	// Output:
	// pdf/1 1
	// pdf/2 1
	// pdf/3 1
}
