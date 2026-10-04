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
	"iter"
)

// All walks every page of a listing and yields each object, then stops at the
// first error. Prefixes are not yielded; use List with a Delimiter for those.
func All(ctx context.Context, s Store, opts ListOptions) iter.Seq2[Info, error] {
	return func(yield func(Info, error) bool) {
		for {
			page, err := s.List(ctx, opts)
			if err != nil {
				yield(Info{}, err)
				return
			}
			for _, info := range page.Objects {
				if !yield(info, nil) {
					return
				}
			}
			if page.NextPageToken == "" {
				return
			}
			opts.PageToken = page.NextPageToken
		}
	}
}
