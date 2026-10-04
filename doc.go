// Package objectstore is S3-compatible object storage behind one small
// interface, for files such as editor images, attachments and rendered PDFs.
//
// # The Store interface
//
// [Store] has eight methods: Put and Get stream (a Put never buffers more than
// one part, a Get can read a byte range), Stat reads an object's info, Delete
// is idempotent, List returns one page at a time with an opaque page token and
// optional delimiter grouping, Copy runs on the server, and PresignGet and
// PresignPut return [Presigned] requests that a browser or a plain
// http.Client can make until they expire.
//
// # Backends
//
//   - [github.com/Bugs5382/go-objectstore/s3store]: AWS S3, MinIO, SeaweedFS
//     and other S3-compatible services, on aws-sdk-go-v2. Custom endpoints,
//     path-style or virtual-host addressing, SSE-S3, SSE-KMS and SSE-C, and a
//     SHA-256 or Content-MD5 checksum on every upload request.
//   - [github.com/Bugs5382/go-objectstore/memstore]: an in-memory fake for
//     unit tests, with an http.Handler that serves its presigned URLs.
//   - [github.com/Bugs5382/go-objectstore/objectstoretest]: the contract
//     suite. Both backends pass it, and a new backend proves itself with it.
//
// This package imports no SDK: code that takes a Store, and its tests on
// memstore, never pull in aws-sdk-go-v2.
//
// # Errors
//
// Every backend reports failures with the same sentinels, wrapped with the
// operation, the key and the backend's cause: [ErrNotFound],
// [ErrAlreadyExists], [ErrPermissionDenied], [ErrUnavailable], [ErrTooLarge]
// and [ErrInvalid]. Branch with errors.Is. A cancelled or expired context is
// returned as the context's own error. [WithCodes] attaches the
// application's go-apperr codes to the sentinels, since a library should not
// pick codes for its callers.
//
// # Limits and cancellation
//
// PutOptions.MaxSize caps one upload and each backend has a store-wide cap;
// an upload that passes either fails with ErrTooLarge and leaves nothing
// behind, and a declared PutOptions.Size over the cap fails before any byte is
// read. Every call honours its context, including mid-upload and while the
// caller reads a Get body.
//
// # Observability
//
// The package does not log. [Observe] wraps any Store and reports each call
// (operation, key, duration, bytes, error) to a function, so the application
// logs, counts or traces store traffic with its own tools.
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
