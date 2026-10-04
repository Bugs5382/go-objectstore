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
	"fmt"
	"net/http"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/internal/check"
)

var sentinels = []error{
	objectstore.ErrNotFound, objectstore.ErrAlreadyExists, objectstore.ErrPermissionDenied,
	objectstore.ErrUnavailable, objectstore.ErrTooLarge, objectstore.ErrInvalid,
}

// byCode maps S3 error codes to sentinels. Codes not listed fall back to the
// HTTP status.
var byCode = map[string]error{
	"NoSuchKey":                   objectstore.ErrNotFound,
	"NoSuchBucket":                objectstore.ErrNotFound,
	"NoSuchUpload":                objectstore.ErrNotFound,
	"NotFound":                    objectstore.ErrNotFound,
	"AccessDenied":                objectstore.ErrPermissionDenied,
	"AllAccessDisabled":           objectstore.ErrPermissionDenied,
	"InvalidAccessKeyId":          objectstore.ErrPermissionDenied,
	"SignatureDoesNotMatch":       objectstore.ErrPermissionDenied,
	"ExpiredToken":                objectstore.ErrPermissionDenied,
	"InvalidToken":                objectstore.ErrPermissionDenied,
	"Forbidden":                   objectstore.ErrPermissionDenied,
	"PreconditionFailed":          objectstore.ErrAlreadyExists,
	"BucketAlreadyExists":         objectstore.ErrAlreadyExists,
	"EntityTooLarge":              objectstore.ErrTooLarge,
	"InvalidRange":                objectstore.ErrInvalid,
	"BadDigest":                   objectstore.ErrInvalid,
	"InvalidDigest":               objectstore.ErrInvalid,
	"XAmzContentSHA256Mismatch":   objectstore.ErrInvalid,
	"XAmzContentChecksumMismatch": objectstore.ErrInvalid,
	"InvalidArgument":             objectstore.ErrInvalid,
	"InvalidRequest":              objectstore.ErrInvalid,
	"SlowDown":                    objectstore.ErrUnavailable,
	"ServiceUnavailable":          objectstore.ErrUnavailable,
	"InternalError":               objectstore.ErrUnavailable,
	"RequestTimeout":              objectstore.ErrUnavailable,
}

// fail turns an SDK error into the store's error: the context's own error
// when the caller gave up, otherwise a sentinel picked by the S3 error code,
// then by HTTP status. A request that got no response is unavailable.
func (s *Store) fail(ctx context.Context, op objectstore.Op, key string, err error) error {
	if cerr := check.Canceled(ctx, op, key); cerr != nil {
		return cerr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("objectstore: %s %q: %w", op, key, err)
	}
	for _, sentinel := range sentinels {
		if errors.Is(err, sentinel) {
			return err
		}
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		if kind, ok := byCode[apiErr.ErrorCode()]; ok {
			return check.Fail(op, key, kind, err)
		}
	}
	var respErr *awshttp.ResponseError
	if errors.As(err, &respErr) {
		return check.Fail(op, key, byStatus(respErr.HTTPStatusCode()), err)
	}
	return check.Fail(op, key, objectstore.ErrUnavailable, err)
}

func byStatus(code int) error {
	switch {
	case code == http.StatusNotFound:
		return objectstore.ErrNotFound
	case code == http.StatusForbidden || code == http.StatusUnauthorized:
		return objectstore.ErrPermissionDenied
	case code == http.StatusPreconditionFailed || code == http.StatusConflict:
		return objectstore.ErrAlreadyExists
	case code == http.StatusRequestEntityTooLarge:
		return objectstore.ErrTooLarge
	case code >= 500:
		return objectstore.ErrUnavailable
	case code >= 400:
		return objectstore.ErrInvalid
	}
	return objectstore.ErrUnavailable
}
