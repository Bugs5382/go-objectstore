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
	"net/http"
	"net/url"
	"time"
)

// Store is a bucket of objects addressed by key. Every implementation is safe
// for concurrent use, honours context cancellation, and reports failures with
// the sentinel errors in this package, so callers branch with errors.Is and
// never on a backend's own error types.
type Store interface {
	// Put streams body to key. It reads body until io.EOF and never buffers
	// more than the backend's part size. An existing object is replaced unless
	// PutOptions.IfNotExists is set.
	Put(ctx context.Context, key string, body io.Reader, opts PutOptions) (Info, error)
	// Get opens the object at key, or the byte range opts selects. The caller
	// closes Object.Body.
	Get(ctx context.Context, key string, opts GetOptions) (*Object, error)
	// Stat returns the object's info without its content.
	Stat(ctx context.Context, key string) (Info, error)
	// Delete removes the object at key. Deleting a missing key succeeds.
	Delete(ctx context.Context, key string) error
	// List returns one page of the objects under opts.Prefix, in key order.
	List(ctx context.Context, opts ListOptions) (Page, error)
	// Copy copies src to dst inside the store, without moving the content
	// through the caller.
	Copy(ctx context.Context, src, dst string, opts CopyOptions) (Info, error)
	// PresignGet returns a URL that downloads key with a plain HTTP GET until
	// it expires. The object does not have to exist yet.
	PresignGet(ctx context.Context, key string, expiry time.Duration) (Presigned, error)
	// PresignPut returns a URL that uploads key with a plain HTTP PUT until it
	// expires. The request must carry the returned headers.
	PresignPut(ctx context.Context, key string, expiry time.Duration, opts PresignPutOptions) (Presigned, error)
}

// Info describes a stored object.
type Info struct {
	Key  string
	Size int64
	// ETag is the backend's entity tag without quotes. It changes whenever the
	// content changes; it is an MD5 of the content only for single-part
	// uploads, so treat it as opaque.
	ETag         string
	LastModified time.Time
	ContentType  string
	// Metadata holds the user metadata, keys in lower case.
	Metadata map[string]string
}

// Object is an open object, or the part of it that GetOptions selected.
type Object struct {
	// Info describes the whole object: Size is the full size, not the
	// length of Body.
	Info
	// Offset is the position of the first byte of Body within the object.
	Offset int64
	// Length is the number of bytes Body yields.
	Length int64
	Body   io.ReadCloser
}

// PutOptions tune a Put.
type PutOptions struct {
	// ContentType defaults to application/octet-stream.
	ContentType string
	// Metadata is stored with the object. Keys are case-insensitive and are
	// returned in lower case; keys use letters, digits and '-', and values are
	// printable ASCII, 2 KiB in total.
	Metadata map[string]string
	// Size is the exact length of body when it is known, which lets the
	// backend reject an oversized upload before reading it. Zero means
	// unknown. A body that ends early or runs long fails with ErrInvalid.
	Size int64
	// MaxSize caps this upload. Zero leaves only the store's own limit.
	MaxSize int64
	// IfNotExists makes the Put fail with ErrAlreadyExists rather than
	// replace an existing object.
	IfNotExists bool
	// CacheControl and ContentDisposition are stored as the object's
	// Cache-Control and Content-Disposition headers.
	CacheControl       string
	ContentDisposition string
}

// GetOptions select a byte range. The zero value reads the whole object.
type GetOptions struct {
	// Offset is the first byte to read. An offset at or past the end of a
	// non-empty object fails with ErrInvalid.
	Offset int64
	// Length is the number of bytes to read; zero reads to the end. A range
	// that runs past the end is cut short at the end.
	Length int64
}

// ListOptions select one page of a listing.
type ListOptions struct {
	// Prefix limits the listing to keys that start with it.
	Prefix string
	// Delimiter groups keys that share a prefix up to the next delimiter into
	// Page.Prefixes, the way folders work. Usually "/" or empty.
	Delimiter string
	// PageToken is the NextPageToken of the previous page; empty starts at
	// the beginning.
	PageToken string
	// Limit is the page size, objects and prefixes together, from 1 to 1000.
	// Zero means 1000.
	Limit int
}

// Page is one page of a listing. Listed objects carry Key, Size, ETag and
// LastModified only; call Stat for the content type and metadata.
type Page struct {
	Objects  []Info
	Prefixes []string
	// NextPageToken is empty on the last page.
	NextPageToken string
}

// CopyOptions tune a Copy.
type CopyOptions struct {
	// ReplaceMetadata gives dst the ContentType and Metadata below instead of
	// the source's. An empty ContentType keeps the source's.
	ReplaceMetadata bool
	ContentType     string
	Metadata        map[string]string
}

// PresignPutOptions tune a presigned upload.
type PresignPutOptions struct {
	// ContentType is signed into the URL, so the upload must send exactly
	// this Content-Type. Empty leaves it unsigned.
	ContentType string
}

// Presigned is a signed request a plain HTTP client can make.
type Presigned struct {
	Method string
	URL    *url.URL
	// Header lists the headers the request must send, exactly as given.
	Header  http.Header
	Expires time.Time
}

// Presigned URL lifetimes are bounded the way S3 bounds them.
const (
	MinPresignExpiry = time.Second
	MaxPresignExpiry = 7 * 24 * time.Hour
)

// MaxListLimit is the largest page a List returns.
const MaxListLimit = 1000
