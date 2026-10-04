// Package s3store is the objectstore.Store for AWS S3 and S3-compatible
// services such as MinIO and SeaweedFS, built on aws-sdk-go-v2. It lives in
// its own package so that importing objectstore does not pull in the SDK.
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
	"crypto/md5" // #nosec G501 -- S3 requires the MD5 of an SSE-C key
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/internal/check"
)

// maxCopySize is the largest object one CopyObject call may copy.
const maxCopySize = 5 << 30

// maxParts is S3's limit on parts per multipart upload.
const maxParts = 10000

// Store is an objectstore.Store over one bucket. It is safe for concurrent
// use; build one per bucket and share it.
type Store struct {
	client     *s3.Client
	presign    *s3.PresignClient
	bucket     string
	region     string
	partSize   int64
	max        int64
	checksum   Checksum
	enc        Encryption
	ssecKey    string
	ssecKeyMD5 string
	copySingle int64
	copyPart   int64
}

var _ objectstore.Store = (*Store)(nil)

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: s3store: %s", objectstore.ErrInvalid, fmt.Sprintf(format, args...))
}

// New validates cfg and builds a Store. It makes no request.
func New(cfg Config) (*Store, error) {
	if err := validBucket(cfg.Bucket); err != nil {
		return nil, err
	}
	if cfg.Region == "" {
		return nil, invalid("region is required")
	}
	creds := cfg.Credentials
	if creds == nil {
		if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
			return nil, invalid("credentials are required: AccessKeyID and SecretAccessKey, or Credentials")
		}
		creds = credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
	}
	var endpoint *string
	if cfg.Endpoint != "" {
		u, err := url.Parse(cfg.Endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, invalid("endpoint %q must be an http or https URL", cfg.Endpoint)
		}
		if strings.Trim(u.Path, "/") != "" || u.RawQuery != "" {
			return nil, invalid("endpoint %q must not have a path or query; the bucket goes in Bucket", cfg.Endpoint)
		}
		endpoint = aws.String(cfg.Endpoint)
	}
	part := cfg.PartSize
	if part == 0 {
		part = DefaultPartSize
	}
	if part < MinPartSize || part > maxCopySize {
		return nil, invalid("part size %d outside %d..%d", part, MinPartSize, maxCopySize)
	}
	if cfg.MaxObjectSize < 0 || cfg.RetryMaxAttempts < 0 {
		return nil, invalid("MaxObjectSize and RetryMaxAttempts must not be negative")
	}
	if cfg.Checksum != ChecksumSHA256 && cfg.Checksum != ChecksumMD5 {
		return nil, invalid("unknown checksum %d", cfg.Checksum)
	}
	st := &Store{
		bucket: cfg.Bucket, region: cfg.Region, partSize: part, max: cfg.MaxObjectSize,
		checksum: cfg.Checksum, enc: cfg.Encryption, copySingle: maxCopySize, copyPart: 512 << 20,
	}
	if err := st.setEncryption(); err != nil {
		return nil, err
	}
	st.client = s3.New(s3.Options{
		Region:           cfg.Region,
		Credentials:      aws.NewCredentialsCache(creds),
		BaseEndpoint:     endpoint,
		UsePathStyle:     cfg.PathStyle,
		HTTPClient:       cfg.HTTPClient,
		RetryMaxAttempts: cfg.RetryMaxAttempts,
		// The store computes its own checksums per request; the SDK's
		// default CRC32 trailers break some S3-compatible services.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	st.presign = s3.NewPresignClient(st.client)
	return st, nil
}

func validBucket(b string) error {
	if len(b) < 3 || len(b) > 63 {
		return invalid("bucket %q must be 3 to 63 characters", b)
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		alnum := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
		if !alnum && c != '-' && c != '.' {
			return invalid("bucket %q: use lower-case letters, digits, '-' and '.'", b)
		}
		if (i == 0 || i == len(b)-1) && !alnum {
			return invalid("bucket %q must start and end with a letter or digit", b)
		}
	}
	return nil
}

func (s *Store) setEncryption() error {
	e := s.enc
	switch e.Mode {
	case SSENone, SSES3:
		if e.KMSKeyID != "" || len(e.CustomerKey) != 0 {
			return invalid("KMSKeyID and CustomerKey need SSEKMS and SSEC")
		}
	case SSEKMS:
		if len(e.CustomerKey) != 0 {
			return invalid("CustomerKey needs SSEC")
		}
	case SSEC:
		if len(e.CustomerKey) != 32 || e.KMSKeyID != "" {
			return invalid("SSEC needs a 32-byte CustomerKey and no KMSKeyID")
		}
		sum := md5.Sum(e.CustomerKey) // #nosec G401 -- required by the SSE-C protocol
		s.ssecKey = base64.StdEncoding.EncodeToString(e.CustomerKey)
		s.ssecKeyMD5 = base64.StdEncoding.EncodeToString(sum[:])
	default:
		return invalid("unknown encryption mode %d", e.Mode)
	}
	return nil
}

// Bucket is the bucket the store works in.
func (s *Store) Bucket() string { return s.bucket }

// EnsureBucket creates the bucket when it does not exist. It is meant for
// bootstrap and tests; production buckets are usually made by the platform.
func (s *Store) EnsureBucket(ctx context.Context) error {
	in := &s3.CreateBucketInput{Bucket: aws.String(s.bucket)}
	if s.region != "us-east-1" {
		in.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(s.region),
		}
	}
	_, err := s.client.CreateBucket(ctx, in)
	var owned *types.BucketAlreadyOwnedByYou
	if err == nil || errors.As(err, &owned) {
		return nil
	}
	return s.fail(ctx, "create_bucket", s.bucket, err)
}

// sseC returns the SSE-C request fields, all nil unless the mode is SSEC.
func (s *Store) sseC() (alg, key, keyMD5 *string) {
	if s.enc.Mode != SSEC {
		return nil, nil, nil
	}
	return aws.String("AES256"), aws.String(s.ssecKey), aws.String(s.ssecKeyMD5)
}

// sseManaged returns the SSE-S3 or SSE-KMS request fields.
func (s *Store) sseManaged() (types.ServerSideEncryption, *string) {
	switch s.enc.Mode {
	case SSES3:
		return types.ServerSideEncryptionAes256, nil
	case SSEKMS:
		if s.enc.KMSKeyID == "" {
			return types.ServerSideEncryptionAwsKms, nil
		}
		return types.ServerSideEncryptionAwsKms, aws.String(s.enc.KMSKeyID)
	}
	return "", nil
}

func lowerKeys(md map[string]string) map[string]string {
	if len(md) == 0 {
		return nil
	}
	out := make(map[string]string, len(md))
	for k, v := range md {
		out[strings.ToLower(k)] = v
	}
	return out
}

func etag(p *string) string { return strings.Trim(aws.ToString(p), `"`) }

// Get opens the object, or the range opts selects.
func (s *Store) Get(ctx context.Context, key string, opts objectstore.GetOptions) (*objectstore.Object, error) {
	const op = objectstore.OpGet
	if err := check.Range(key, opts); err != nil {
		return nil, err
	}
	if err := check.Canceled(ctx, op, key); err != nil {
		return nil, err
	}
	in := &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}
	in.SSECustomerAlgorithm, in.SSECustomerKey, in.SSECustomerKeyMD5 = s.sseC()
	ranged := opts.Offset > 0 || opts.Length > 0
	if ranged {
		end := ""
		if opts.Length > 0 {
			end = strconv.FormatInt(opts.Offset+opts.Length-1, 10)
		}
		in.Range = aws.String("bytes=" + strconv.FormatInt(opts.Offset, 10) + "-" + end)
	} else {
		in.ChecksumMode = types.ChecksumModeEnabled
	}
	out, err := s.client.GetObject(ctx, in)
	if err != nil {
		return nil, s.fail(ctx, op, key, err)
	}
	length := aws.ToInt64(out.ContentLength)
	obj := &objectstore.Object{
		Info: objectstore.Info{
			Key: key, Size: length, ETag: etag(out.ETag), LastModified: aws.ToTime(out.LastModified),
			ContentType: aws.ToString(out.ContentType), Metadata: lowerKeys(out.Metadata),
		},
		Length: length,
		Body:   &body{ctx: ctx, key: key, rc: out.Body},
	}
	if ranged {
		start, total, err := parseContentRange(aws.ToString(out.ContentRange))
		if err != nil {
			_ = out.Body.Close()
			return nil, check.Fail(op, key, objectstore.ErrUnavailable, err)
		}
		obj.Offset, obj.Size = start, total
	}
	return obj, nil
}

// parseContentRange reads "bytes 5-8/20".
func parseContentRange(v string) (start, total int64, err error) {
	rest, ok := strings.CutPrefix(v, "bytes ")
	span, size, ok2 := strings.Cut(rest, "/")
	first, _, ok3 := strings.Cut(span, "-")
	if !ok || !ok2 || !ok3 {
		return 0, 0, fmt.Errorf("unexpected Content-Range %q", v)
	}
	if start, err = strconv.ParseInt(first, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("unexpected Content-Range %q", v)
	}
	if total, err = strconv.ParseInt(size, 10, 64); err != nil {
		return 0, 0, fmt.Errorf("unexpected Content-Range %q", v)
	}
	return start, total, nil
}

// body reports a read cut short by the caller's context as that context's
// error, whatever the transport called it.
type body struct {
	ctx context.Context
	key string
	rc  io.ReadCloser
}

func (b *body) Read(p []byte) (int, error) {
	n, err := b.rc.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		if cerr := check.Canceled(b.ctx, objectstore.OpGet, b.key); cerr != nil {
			return n, cerr
		}
		return n, fmt.Errorf("objectstore: get %q: read: %w", b.key, err)
	}
	return n, err
}

func (b *body) Close() error { return b.rc.Close() }

// Stat returns the object's info from a HEAD request.
func (s *Store) Stat(ctx context.Context, key string) (objectstore.Info, error) {
	const op = objectstore.OpStat
	if err := check.Key(op, key); err != nil {
		return objectstore.Info{}, err
	}
	if err := check.Canceled(ctx, op, key); err != nil {
		return objectstore.Info{}, err
	}
	return s.head(ctx, op, key)
}

func (s *Store) head(ctx context.Context, op objectstore.Op, key string) (objectstore.Info, error) {
	in := &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}
	in.SSECustomerAlgorithm, in.SSECustomerKey, in.SSECustomerKeyMD5 = s.sseC()
	out, err := s.client.HeadObject(ctx, in)
	if err != nil {
		return objectstore.Info{}, s.fail(ctx, op, key, err)
	}
	return objectstore.Info{
		Key: key, Size: aws.ToInt64(out.ContentLength), ETag: etag(out.ETag),
		LastModified: aws.ToTime(out.LastModified), ContentType: aws.ToString(out.ContentType),
		Metadata: lowerKeys(out.Metadata),
	}, nil
}

// Delete removes the object; a missing key is not an error.
func (s *Store) Delete(ctx context.Context, key string) error {
	const op = objectstore.OpDelete
	if err := check.Key(op, key); err != nil {
		return err
	}
	if err := check.Canceled(ctx, op, key); err != nil {
		return err
	}
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return s.fail(ctx, op, key, err)
	}
	return nil
}

// List returns one page from ListObjectsV2; the page token is S3's
// continuation token.
func (s *Store) List(ctx context.Context, opts objectstore.ListOptions) (objectstore.Page, error) {
	const op = objectstore.OpList
	limit, err := check.List(opts)
	if err != nil {
		return objectstore.Page{}, err
	}
	if err := check.Canceled(ctx, op, opts.Prefix); err != nil {
		return objectstore.Page{}, err
	}
	in := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		MaxKeys: aws.Int32(int32(limit)), // #nosec G115 -- check.List caps limit at 1000
	}
	if opts.Prefix != "" {
		in.Prefix = aws.String(opts.Prefix)
	}
	if opts.Delimiter != "" {
		in.Delimiter = aws.String(opts.Delimiter)
	}
	if opts.PageToken != "" {
		in.ContinuationToken = aws.String(opts.PageToken)
	}
	out, err := s.client.ListObjectsV2(ctx, in)
	if err != nil {
		return objectstore.Page{}, s.fail(ctx, op, opts.Prefix, err)
	}
	page := objectstore.Page{Objects: make([]objectstore.Info, 0, len(out.Contents))}
	for _, o := range out.Contents {
		page.Objects = append(page.Objects, objectstore.Info{
			Key: aws.ToString(o.Key), Size: aws.ToInt64(o.Size), ETag: etag(o.ETag), LastModified: aws.ToTime(o.LastModified),
		})
	}
	for _, p := range out.CommonPrefixes {
		page.Prefixes = append(page.Prefixes, aws.ToString(p.Prefix))
	}
	if aws.ToBool(out.IsTruncated) {
		page.NextPageToken = aws.ToString(out.NextContinuationToken)
	}
	return page, nil
}

// PresignGet signs a GET. SSE-C objects cannot be presigned: the request
// would have to carry the customer key.
func (s *Store) PresignGet(ctx context.Context, key string, expiry time.Duration) (objectstore.Presigned, error) {
	const op = objectstore.OpPresignGet
	if err := s.presignable(ctx, op, key, expiry); err != nil {
		return objectstore.Presigned{}, err
	}
	expires := time.Now().Add(expiry)
	req, err := s.presign.PresignGetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)},
		s3.WithPresignExpires(expiry))
	if err != nil {
		return objectstore.Presigned{}, s.fail(ctx, op, key, err)
	}
	return presigned(req.Method, req.URL, req.SignedHeader, expires, op, key)
}

// PresignPut signs a PUT. The returned headers include the content type and
// the SSE-S3 or SSE-KMS headers, which the upload must send.
func (s *Store) PresignPut(ctx context.Context, key string, expiry time.Duration, opts objectstore.PresignPutOptions) (objectstore.Presigned, error) {
	const op = objectstore.OpPresignPut
	if err := s.presignable(ctx, op, key, expiry); err != nil {
		return objectstore.Presigned{}, err
	}
	in := &s3.PutObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}
	if opts.ContentType != "" {
		in.ContentType = aws.String(opts.ContentType)
	}
	in.ServerSideEncryption, in.SSEKMSKeyId = s.sseManaged()
	expires := time.Now().Add(expiry)
	req, err := s.presign.PresignPutObject(ctx, in, s3.WithPresignExpires(expiry))
	if err != nil {
		return objectstore.Presigned{}, s.fail(ctx, op, key, err)
	}
	return presigned(req.Method, req.URL, req.SignedHeader, expires, op, key)
}

func (s *Store) presignable(ctx context.Context, op objectstore.Op, key string, expiry time.Duration) error {
	if err := check.Expiry(op, key, expiry); err != nil {
		return err
	}
	if s.enc.Mode == SSEC {
		return check.Fail(op, key, objectstore.ErrInvalid, errors.New("SSE-C objects cannot be presigned"))
	}
	return check.Canceled(ctx, op, key)
}

func presigned(method, raw string, signed map[string][]string, expires time.Time, op objectstore.Op, key string) (objectstore.Presigned, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return objectstore.Presigned{}, check.Fail(op, key, objectstore.ErrUnavailable, err)
	}
	header := make(map[string][]string, len(signed))
	for k, v := range signed {
		if !strings.EqualFold(k, "Host") {
			header[k] = v
		}
	}
	return objectstore.Presigned{Method: method, URL: u, Header: header, Expires: expires}, nil
}
