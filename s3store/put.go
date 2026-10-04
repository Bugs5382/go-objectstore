package s3store

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
	"crypto/md5" // #nosec G501 -- Content-MD5 is the protocol's integrity header
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/internal/check"
)

// Put streams body to key. A body that fits in one part goes up in a single
// PUT; a longer one becomes a multipart upload, one part buffered at a time,
// and is aborted if anything fails, so no parts are left behind.
func (s *Store) Put(ctx context.Context, key string, body io.Reader, opts objectstore.PutOptions) (objectstore.Info, error) {
	const op = objectstore.OpPut
	md, limit, err := check.Put(key, opts, s.max)
	if err != nil {
		return objectstore.Info{}, err
	}
	if err := check.Canceled(ctx, op, key); err != nil {
		return objectstore.Info{}, err
	}
	if opts.Size > s.partSize*maxParts {
		return objectstore.Info{}, check.Fail(op, key, objectstore.ErrTooLarge,
			fmt.Errorf("size %d over %d parts of %d bytes", opts.Size, maxParts, s.partSize))
	}
	ct := opts.ContentType
	if ct == "" {
		ct = check.DefaultContentType
	}
	b := check.NewBody(ctx, body, key, limit, opts.Size)
	bufSize := s.partSize
	if opts.Size > 0 && opts.Size < s.partSize {
		bufSize = opts.Size + 1
	}
	buf := make([]byte, bufSize)
	n, err := io.ReadFull(b, buf)
	switch {
	case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
		return s.putSingle(ctx, key, buf[:n], ct, md, opts)
	case err != nil:
		return objectstore.Info{}, s.fail(ctx, op, key, err)
	}
	return s.putMultipart(ctx, key, b, buf, ct, md, opts)
}

func (s *Store) sum(data []byte) (sha, md *string) {
	if s.checksum == ChecksumMD5 {
		h := md5.Sum(data) // #nosec G401 -- Content-MD5
		return nil, aws.String(base64.StdEncoding.EncodeToString(h[:]))
	}
	h := sha256.Sum256(data)
	return aws.String(base64.StdEncoding.EncodeToString(h[:])), nil
}

func (s *Store) putSingle(ctx context.Context, key string, data []byte, ct string, md map[string]string, opts objectstore.PutOptions) (objectstore.Info, error) {
	in := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String(ct),
		Metadata:      md,
	}
	setHeaders(&in.CacheControl, &in.ContentDisposition, opts)
	in.ChecksumSHA256, in.ContentMD5 = s.sum(data)
	in.ServerSideEncryption, in.SSEKMSKeyId = s.sseManaged()
	in.SSECustomerAlgorithm, in.SSECustomerKey, in.SSECustomerKeyMD5 = s.sseC()
	if opts.IfNotExists {
		in.IfNoneMatch = aws.String("*")
	}
	out, err := s.client.PutObject(ctx, in)
	if err != nil {
		return objectstore.Info{}, s.fail(ctx, objectstore.OpPut, key, err)
	}
	return objectstore.Info{Key: key, Size: int64(len(data)), ETag: etag(out.ETag), ContentType: ct, Metadata: md}, nil
}

func setHeaders(cache, disposition **string, opts objectstore.PutOptions) {
	if opts.CacheControl != "" {
		*cache = aws.String(opts.CacheControl)
	}
	if opts.ContentDisposition != "" {
		*disposition = aws.String(opts.ContentDisposition)
	}
}

func (s *Store) putMultipart(ctx context.Context, key string, b *check.Body, buf []byte, ct string, md map[string]string, opts objectstore.PutOptions) (info objectstore.Info, err error) {
	const op = objectstore.OpPut
	create := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), ContentType: aws.String(ct), Metadata: md,
	}
	setHeaders(&create.CacheControl, &create.ContentDisposition, opts)
	if s.checksum == ChecksumSHA256 {
		create.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
	}
	create.ServerSideEncryption, create.SSEKMSKeyId = s.sseManaged()
	create.SSECustomerAlgorithm, create.SSECustomerKey, create.SSECustomerKeyMD5 = s.sseC()
	started, err := s.client.CreateMultipartUpload(ctx, create)
	if err != nil {
		return objectstore.Info{}, s.fail(ctx, op, key, err)
	}
	uploadID := started.UploadId
	defer func() {
		if err != nil {
			s.abort(ctx, key, uploadID)
		}
	}()

	var parts []types.CompletedPart
	n := len(buf)
	for num := int32(1); ; num++ {
		if num > maxParts {
			return objectstore.Info{}, check.Fail(op, key, objectstore.ErrTooLarge,
				fmt.Errorf("more than %d parts of %d bytes", maxParts, s.partSize))
		}
		part := &s3.UploadPartInput{
			Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: uploadID,
			PartNumber: aws.Int32(num), Body: bytes.NewReader(buf[:n]), ContentLength: aws.Int64(int64(n)),
		}
		part.ChecksumSHA256, part.ContentMD5 = s.sum(buf[:n])
		part.SSECustomerAlgorithm, part.SSECustomerKey, part.SSECustomerKeyMD5 = s.sseC()
		out, err := s.client.UploadPart(ctx, part)
		if err != nil {
			return objectstore.Info{}, s.fail(ctx, op, key, err)
		}
		parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(num), ETag: out.ETag, ChecksumSHA256: out.ChecksumSHA256})
		if n < len(buf) {
			break
		}
		n, err = io.ReadFull(b, buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return objectstore.Info{}, s.fail(ctx, op, key, err)
		}
	}

	done := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}
	done.SSECustomerAlgorithm, done.SSECustomerKey, done.SSECustomerKeyMD5 = s.sseC()
	if opts.IfNotExists {
		done.IfNoneMatch = aws.String("*")
	}
	out, err := s.client.CompleteMultipartUpload(ctx, done)
	if err != nil {
		return objectstore.Info{}, s.fail(ctx, op, key, err)
	}
	return objectstore.Info{Key: key, Size: b.N(), ETag: etag(out.ETag), ContentType: ct, Metadata: md}, nil
}

// abort runs even when ctx is already cancelled, so a stopped upload does
// not leave billable parts behind.
func (s *Store) abort(ctx context.Context, key string, uploadID *string) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	_, _ = s.client.AbortMultipartUpload(actx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: uploadID,
	})
}
