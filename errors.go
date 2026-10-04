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

import "errors"

// Every Store reports failures with these sentinels, wrapped with the
// operation, the key and the backend's cause. A cancelled or expired context
// is reported as the context's own error instead.
var (
	// ErrNotFound means the object (or the bucket) does not exist.
	ErrNotFound = errors.New("objectstore: not found")
	// ErrAlreadyExists means a Put with IfNotExists found an object there.
	ErrAlreadyExists = errors.New("objectstore: already exists")
	// ErrPermissionDenied means the backend refused the credentials or the
	// request, or a presigned URL was expired or altered.
	ErrPermissionDenied = errors.New("objectstore: permission denied")
	// ErrUnavailable means the backend could not be reached or failed; a
	// retry later may succeed.
	ErrUnavailable = errors.New("objectstore: unavailable")
	// ErrTooLarge means an upload exceeded the per-call or the store limit.
	ErrTooLarge = errors.New("objectstore: too large")
	// ErrInvalid means the key, the options or the body length were wrong.
	ErrInvalid = errors.New("objectstore: invalid request")
)
