//go:build integration

package s3store_test

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
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/objectstoretest"
	"github.com/Bugs5382/go-objectstore/s3store"
)

const largeSize = 3*s3store.MinPartSize + 12345

func TestContractPathStyle(t *testing.T) {
	srv := minio(t, plainServer)
	objectstoretest.Run(t, func(t *testing.T) objectstore.Store {
		return open(t, srv.config(t))
	}, objectstoretest.WithLargeSize(largeSize), objectstoretest.WithHTTPClient(srv.client))
}

func TestContractVirtualHost(t *testing.T) {
	srv := minio(t, vhostServer)
	objectstoretest.Run(t, func(t *testing.T) objectstore.Store {
		cfg := srv.config(t)
		cfg.PathStyle = false
		return open(t, cfg)
	}, objectstoretest.WithLargeSize(largeSize), objectstoretest.WithHTTPClient(srv.client))
}

func TestContractMD5Checksum(t *testing.T) {
	srv := minio(t, plainServer)
	objectstoretest.Run(t, func(t *testing.T) objectstore.Store {
		cfg := srv.config(t)
		cfg.Checksum = s3store.ChecksumMD5
		return open(t, cfg)
	}, objectstoretest.WithLargeSize(largeSize), objectstoretest.WithHTTPClient(srv.client))
}

// headerLog records the checksum headers of every upload request, and can
// flip one body byte after the client has computed its checksum.
type headerLog struct {
	next    http.RoundTripper
	corrupt bool
	mu      sync.Mutex
	puts    []http.Header
}

func (h *headerLog) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/k") {
		h.mu.Lock()
		h.puts = append(h.puts, r.Header.Clone())
		h.mu.Unlock()
		if h.corrupt && r.Body != nil {
			data, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			_ = r.Body.Close()
			if len(data) > 0 {
				data[len(data)/2] ^= 0xff
			}
			r = r.Clone(r.Context())
			r.Body = io.NopCloser(bytes.NewReader(data))
			r.GetBody = nil
		}
	}
	return h.next.RoundTrip(r)
}

func TestChecksumsSentOnEveryUpload(t *testing.T) {
	srv := minio(t, plainServer)
	for _, tc := range []struct {
		name     string
		checksum s3store.Checksum
		header   string
	}{
		{"SHA256", s3store.ChecksumSHA256, "X-Amz-Checksum-Sha256"},
		{"MD5", s3store.ChecksumMD5, "Content-Md5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			log := &headerLog{next: srv.client.Transport}
			cfg := srv.config(t)
			cfg.Checksum = tc.checksum
			cfg.HTTPClient = &http.Client{Transport: log}
			st := open(t, cfg)
			for _, n := range []int64{10, 2*s3store.MinPartSize + 1} {
				if _, err := st.Put(ctxT(t), "k", io.LimitReader(rand.Reader, n), objectstore.PutOptions{}); err != nil {
					t.Fatalf("Put %d bytes: %v", n, err)
				}
			}
			// One single PUT, then three parts.
			if len(log.puts) != 4 {
				t.Fatalf("saw %d upload requests, want 4", len(log.puts))
			}
			for i, h := range log.puts {
				if h.Get(tc.header) == "" {
					t.Fatalf("upload request %d has no %s header: %v", i, tc.header, h)
				}
			}
		})
	}
}

func TestCorruptedUploadIsRefused(t *testing.T) {
	srv := minio(t, plainServer)
	for _, checksum := range []s3store.Checksum{s3store.ChecksumSHA256, s3store.ChecksumMD5} {
		cfg := srv.config(t)
		cfg.Checksum = checksum
		cfg.RetryMaxAttempts = 1
		cfg.HTTPClient = &http.Client{Transport: &headerLog{next: srv.client.Transport, corrupt: true}}
		st := open(t, cfg)
		for _, n := range []int64{100, s3store.MinPartSize + 100} {
			_, err := st.Put(ctxT(t), "k", io.LimitReader(rand.Reader, n), objectstore.PutOptions{})
			if err == nil {
				t.Fatalf("checksum %d: a corrupted %d-byte upload was stored", checksum, n)
			}
			if !errors.Is(err, objectstore.ErrInvalid) {
				t.Fatalf("checksum %d: corrupted upload error = %v, want ErrInvalid", checksum, err)
			}
			clean := must(s3store.New(srv.withBucket(t, cfg.Bucket)))
			_, err = clean.Stat(ctxT(t), "k")
			if !errors.Is(err, objectstore.ErrNotFound) {
				t.Fatalf("after a corrupted upload Stat = %v", err)
			}
		}
	}
}

func (srv *server) withBucket(t *testing.T, bucket string) s3store.Config {
	cfg := srv.config(t)
	cfg.Bucket = bucket
	return cfg
}

func TestFailedUploadsLeaveNoParts(t *testing.T) {
	srv := minio(t, plainServer)
	cfg := srv.config(t)
	st := open(t, cfg)

	_, err := st.Put(ctxT(t), "too-big", io.LimitReader(rand.Reader, 3*s3store.MinPartSize), objectstore.PutOptions{MaxSize: 2*s3store.MinPartSize + 1})
	if !errors.Is(err, objectstore.ErrTooLarge) {
		t.Fatalf("Put over MaxSize: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	body := &cancelReader{r: io.LimitReader(rand.Reader, 1<<40), after: 2 * s3store.MinPartSize, cancel: cancel}
	_, err = st.Put(ctx, "cancelled", body, objectstore.PutOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Put: %v", err)
	}
	out := must(srv.raw().ListMultipartUploads(ctxT(t), &s3.ListMultipartUploadsInput{Bucket: aws.String(cfg.Bucket)}))
	if len(out.Uploads) != 0 {
		t.Fatalf("%d multipart uploads left behind", len(out.Uploads))
	}
}

type cancelReader struct {
	r      io.Reader
	after  int64
	cancel context.CancelFunc
}

func (c *cancelReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.after -= int64(n)
	if c.after <= 0 {
		c.cancel()
	}
	return n, err
}

func TestStoreSizeLimit(t *testing.T) {
	srv := minio(t, plainServer)
	cfg := srv.config(t)
	cfg.MaxObjectSize = 1000
	st := open(t, cfg)
	_, err := st.Put(ctxT(t), "k", io.LimitReader(rand.Reader, 1001), objectstore.PutOptions{})
	if !errors.Is(err, objectstore.ErrTooLarge) {
		t.Fatalf("Put over MaxObjectSize: %v", err)
	}
	if _, err := st.Put(ctxT(t), "k", io.LimitReader(rand.Reader, 1000), objectstore.PutOptions{MaxSize: 5000}); err != nil {
		t.Fatalf("Put at MaxObjectSize: %v", err)
	}
}

func TestMultipartCopy(t *testing.T) {
	srv := minio(t, plainServer)
	st := open(t, srv.config(t))
	s3store.SetCopyLimits(st, s3store.MinPartSize, s3store.MinPartSize)
	data := make([]byte, 2*s3store.MinPartSize+777)
	if _, err := rand.Read(data); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Put(ctxT(t), "src", bytes.NewReader(data), objectstore.PutOptions{
		ContentType: "application/pdf", Metadata: map[string]string{"k": "v"},
	}); err != nil {
		t.Fatal(err)
	}
	for _, opts := range []objectstore.CopyOptions{{}, {ReplaceMetadata: true, Metadata: map[string]string{"n": "1"}}} {
		info, err := st.Copy(ctxT(t), "src", "dst", opts)
		if err != nil {
			t.Fatalf("multipart Copy %+v: %v", opts, err)
		}
		if info.Size != int64(len(data)) {
			t.Fatalf("Copy size = %d", info.Size)
		}
		obj := must(st.Get(ctxT(t), "dst", objectstore.GetOptions{}))
		got := must(io.ReadAll(obj.Body))
		_ = obj.Body.Close()
		if !bytes.Equal(got, data) || obj.ContentType != "application/pdf" {
			t.Fatalf("multipart copy content differs (%d bytes, type %q)", len(got), obj.ContentType)
		}
		want := map[string]string{"k": "v"}
		if opts.ReplaceMetadata {
			want = opts.Metadata
		}
		if len(obj.Metadata) != len(want) || obj.Metadata[firstKey(want)] != want[firstKey(want)] {
			t.Fatalf("multipart copy metadata = %v, want %v", obj.Metadata, want)
		}
	}
}

func firstKey(m map[string]string) string {
	for k := range m {
		return k
	}
	return ""
}

func TestWrongCredentials(t *testing.T) {
	srv := minio(t, plainServer)
	good := open(t, srv.config(t))
	if _, err := good.Put(ctxT(t), "k", strings.NewReader("x"), objectstore.PutOptions{}); err != nil {
		t.Fatal(err)
	}
	bad := srv.withBucket(t, good.Bucket())
	bad.SecretAccessKey = "not-the-secret"
	st := must(s3store.New(bad))
	_, err := st.Stat(ctxT(t), "k")
	if !errors.Is(err, objectstore.ErrPermissionDenied) {
		t.Fatalf("Stat with a wrong secret: %v", err)
	}
	_, err = st.Put(ctxT(t), "k", strings.NewReader("x"), objectstore.PutOptions{})
	if !errors.Is(err, objectstore.ErrPermissionDenied) {
		t.Fatalf("Put with a wrong secret: %v", err)
	}
	_, err = st.List(ctxT(t), objectstore.ListOptions{})
	if !errors.Is(err, objectstore.ErrPermissionDenied) {
		t.Fatalf("List with a wrong secret: %v", err)
	}
}

func TestMissingBucketIsNotFound(t *testing.T) {
	srv := minio(t, plainServer)
	st := must(s3store.New(srv.config(t)))
	_, err := st.Stat(ctxT(t), "k")
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Stat in a missing bucket: %v", err)
	}
	_, err = st.List(ctxT(t), objectstore.ListOptions{})
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("List in a missing bucket: %v", err)
	}
	_, err = st.Put(ctxT(t), "k", strings.NewReader("x"), objectstore.PutOptions{})
	if !errors.Is(err, objectstore.ErrNotFound) {
		t.Fatalf("Put in a missing bucket: %v", err)
	}
}

func TestUnreachableIsUnavailable(t *testing.T) {
	st := must(s3store.New(s3store.Config{
		Endpoint: "http://127.0.0.1:1", Region: region, Bucket: "files",
		AccessKeyID: "a", SecretAccessKey: "b", PathStyle: true, RetryMaxAttempts: 1,
	}))
	_, err := st.Stat(ctxT(t), "k")
	if !errors.Is(err, objectstore.ErrUnavailable) {
		t.Fatalf("Stat on a closed port: %v", err)
	}
	_, err = st.Put(ctxT(t), "k", strings.NewReader("x"), objectstore.PutOptions{})
	if !errors.Is(err, objectstore.ErrUnavailable) {
		t.Fatalf("Put on a closed port: %v", err)
	}
}

func TestEnsureBucketIsIdempotent(t *testing.T) {
	srv := minio(t, plainServer)
	st := open(t, srv.config(t))
	if err := st.EnsureBucket(ctxT(t)); err != nil {
		t.Fatalf("second EnsureBucket: %v", err)
	}
}

func TestPresignedURLsShowNoCredentials(t *testing.T) {
	srv := minio(t, plainServer)
	st := open(t, srv.config(t))
	p := must(st.PresignGet(ctxT(t), "k", time.Minute))
	if strings.Contains(p.URL.String(), srv.secret) {
		t.Fatal("presigned URL contains the secret key")
	}
}
