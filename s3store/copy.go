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
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/internal/check"
)

// Copy copies src to dst on the server. Objects up to 5 GiB take one
// CopyObject call; larger ones are copied part by part. Both pin the source's
// ETag, so a source replaced mid-copy fails the copy instead of mixing
// versions.
func (s *Store) Copy(ctx context.Context, src, dst string, opts objectstore.CopyOptions) (objectstore.Info, error) {
	const op = objectstore.OpCopy
	if err := check.Key(op, src); err != nil {
		return objectstore.Info{}, err
	}
	if err := check.Key(op, dst); err != nil {
		return objectstore.Info{}, err
	}
	if err := check.Canceled(ctx, op, dst); err != nil {
		return objectstore.Info{}, err
	}
	md, err := check.Metadata(op, dst, opts.Metadata)
	if err != nil {
		return objectstore.Info{}, err
	}
	from, err := s.head(ctx, op, src)
	if err != nil {
		return objectstore.Info{}, err
	}
	if src == dst && !opts.ReplaceMetadata {
		return objectstore.Info{}, check.Fail(op, dst, objectstore.ErrInvalid,
			errors.New("copying an object onto itself needs ReplaceMetadata"))
	}
	ct := from.ContentType
	if !opts.ReplaceMetadata {
		md = from.Metadata
	} else if opts.ContentType != "" {
		ct = opts.ContentType
	}
	if from.Size > s.copySingle {
		return s.copyMultipart(ctx, src, dst, from, ct, md)
	}
	in := &s3.CopyObjectInput{
		Bucket:            aws.String(s.bucket),
		Key:               aws.String(dst),
		CopySource:        aws.String(s.source(src)),
		CopySourceIfMatch: aws.String(`"` + from.ETag + `"`),
		MetadataDirective: types.MetadataDirectiveCopy,
	}
	if opts.ReplaceMetadata {
		in.MetadataDirective = types.MetadataDirectiveReplace
		in.ContentType = aws.String(ct)
		in.Metadata = md
	}
	in.ServerSideEncryption, in.SSEKMSKeyId = s.sseManaged()
	in.SSECustomerAlgorithm, in.SSECustomerKey, in.SSECustomerKeyMD5 = s.sseC()
	in.CopySourceSSECustomerAlgorithm, in.CopySourceSSECustomerKey, in.CopySourceSSECustomerKeyMD5 = s.sseC()
	out, err := s.client.CopyObject(ctx, in)
	if err != nil {
		return objectstore.Info{}, s.fail(ctx, op, dst, err)
	}
	info := objectstore.Info{Key: dst, Size: from.Size, ContentType: ct, Metadata: md}
	if r := out.CopyObjectResult; r != nil {
		info.ETag, info.LastModified = etag(r.ETag), aws.ToTime(r.LastModified)
	}
	return info, nil
}

// source is the x-amz-copy-source value: bucket and key, each path segment
// escaped.
func (s *Store) source(key string) string {
	segs := strings.Split(key, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	return s.bucket + "/" + strings.Join(segs, "/")
}

func (s *Store) copyMultipart(ctx context.Context, src, dst string, from objectstore.Info, ct string, md map[string]string) (info objectstore.Info, err error) {
	const op = objectstore.OpCopy
	create := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(dst), ContentType: aws.String(ct), Metadata: md,
	}
	create.ServerSideEncryption, create.SSEKMSKeyId = s.sseManaged()
	create.SSECustomerAlgorithm, create.SSECustomerKey, create.SSECustomerKeyMD5 = s.sseC()
	started, err := s.client.CreateMultipartUpload(ctx, create)
	if err != nil {
		return objectstore.Info{}, s.fail(ctx, op, dst, err)
	}
	defer func() {
		if err != nil {
			s.abort(ctx, dst, started.UploadId)
		}
	}()
	var parts []types.CompletedPart
	for num, start := int32(1), int64(0); start < from.Size; num, start = num+1, start+s.copyPart {
		end := min(start+s.copyPart, from.Size) - 1
		in := &s3.UploadPartCopyInput{
			Bucket: aws.String(s.bucket), Key: aws.String(dst), UploadId: started.UploadId, PartNumber: aws.Int32(num),
			CopySource:        aws.String(s.source(src)),
			CopySourceIfMatch: aws.String(`"` + from.ETag + `"`),
			CopySourceRange:   aws.String("bytes=" + strconv.FormatInt(start, 10) + "-" + strconv.FormatInt(end, 10)),
		}
		in.SSECustomerAlgorithm, in.SSECustomerKey, in.SSECustomerKeyMD5 = s.sseC()
		in.CopySourceSSECustomerAlgorithm, in.CopySourceSSECustomerKey, in.CopySourceSSECustomerKeyMD5 = s.sseC()
		out, err := s.client.UploadPartCopy(ctx, in)
		if err != nil {
			return objectstore.Info{}, s.fail(ctx, op, dst, err)
		}
		var tag *string
		if out.CopyPartResult != nil {
			tag = out.CopyPartResult.ETag
		}
		parts = append(parts, types.CompletedPart{PartNumber: aws.Int32(num), ETag: tag})
	}
	done := &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(dst), UploadId: started.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}
	done.SSECustomerAlgorithm, done.SSECustomerKey, done.SSECustomerKeyMD5 = s.sseC()
	out, err := s.client.CompleteMultipartUpload(ctx, done)
	if err != nil {
		return objectstore.Info{}, s.fail(ctx, op, dst, err)
	}
	return objectstore.Info{Key: dst, Size: from.Size, ETag: etag(out.ETag), ContentType: ct, Metadata: md}, nil
}
