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
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/s3store"
)

func TestSSEManagedKeys(t *testing.T) {
	srv := minio(t, plainServer)
	for _, tc := range []struct {
		name string
		enc  s3store.Encryption
		want types.ServerSideEncryption
	}{
		{"SSE-S3", s3store.Encryption{Mode: s3store.SSES3}, types.ServerSideEncryptionAes256},
		{"SSE-KMS", s3store.Encryption{Mode: s3store.SSEKMS, KMSKeyID: kmsKeyName}, types.ServerSideEncryptionAwsKms},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := srv.config(t)
			cfg.Encryption = tc.enc
			st := open(t, cfg)
			for _, n := range []int64{64, 2*s3store.MinPartSize + 3} {
				if _, err := st.Put(ctxT(t), "secret.pdf", io.LimitReader(rand.Reader, n), objectstore.PutOptions{}); err != nil {
					t.Fatalf("Put %d bytes: %v", n, err)
				}
				head := must(srv.raw().HeadObject(ctxT(t), &s3.HeadObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String("secret.pdf")}))
				if head.ServerSideEncryption != tc.want {
					t.Fatalf("%d bytes stored with encryption %q, want %q", n, head.ServerSideEncryption, tc.want)
				}
			}
			if _, err := st.Copy(ctxT(t), "secret.pdf", "copy.pdf", objectstore.CopyOptions{}); err != nil {
				t.Fatalf("Copy: %v", err)
			}
			head := must(srv.raw().HeadObject(ctxT(t), &s3.HeadObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String("copy.pdf")}))
			if head.ServerSideEncryption != tc.want {
				t.Fatalf("copy stored with encryption %q, want %q", head.ServerSideEncryption, tc.want)
			}

			p := must(st.PresignPut(ctxT(t), "browser.pdf", time.Minute, objectstore.PresignPutOptions{ContentType: "application/pdf"}))
			req := must(newRequest(p, "%PDF"))
			resp := must(srv.client.Do(req))
			_ = resp.Body.Close()
			if resp.StatusCode != 200 {
				t.Fatalf("presigned PUT = %d", resp.StatusCode)
			}
			head = must(srv.raw().HeadObject(ctxT(t), &s3.HeadObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String("browser.pdf")}))
			if head.ServerSideEncryption != tc.want {
				t.Fatalf("presigned upload stored with encryption %q, want %q", head.ServerSideEncryption, tc.want)
			}
		})
	}
}

func TestSSECustomerKey(t *testing.T) {
	srv := minio(t, tlsServer)
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	cfg := srv.config(t)
	cfg.Encryption = s3store.Encryption{Mode: s3store.SSEC, CustomerKey: key}
	st := open(t, cfg)
	for _, n := range []int64{64, 2*s3store.MinPartSize + 3} {
		data := make([]byte, n)
		if _, err := rand.Read(data); err != nil {
			t.Fatal(err)
		}
		if _, err := st.Put(ctxT(t), "c", strings.NewReader(string(data)), objectstore.PutOptions{ContentType: "text/plain"}); err != nil {
			t.Fatalf("Put %d bytes: %v", n, err)
		}
		if _, err := st.Copy(ctxT(t), "c", "c2", objectstore.CopyOptions{}); err != nil {
			t.Fatalf("Copy: %v", err)
		}
		for _, k := range []string{"c", "c2"} {
			obj := must(st.Get(ctxT(t), k, objectstore.GetOptions{Offset: 1, Length: 10}))
			got := must(io.ReadAll(obj.Body))
			_ = obj.Body.Close()
			if string(got) != string(data[1:11]) || obj.Size != n || obj.ContentType != "text/plain" {
				t.Fatalf("%s: SSE-C ranged read differs", k)
			}
		}
	}
	_, err := srv.raw().GetObject(ctxT(t), &s3.GetObjectInput{Bucket: aws.String(cfg.Bucket), Key: aws.String("c")})
	if err == nil {
		t.Fatal("an SSE-C object was readable without the key")
	}
	other := cfg
	other.Encryption.CustomerKey = make([]byte, 32)
	wrong := must(s3store.New(other))
	if _, err := wrong.Stat(ctxT(t), "c"); err == nil {
		t.Fatal("an SSE-C object was readable with another key")
	}
	_, err = st.PresignGet(ctxT(t), "c", time.Minute)
	if !errors.Is(err, objectstore.ErrInvalid) {
		t.Fatalf("PresignGet with SSE-C: %v", err)
	}
	_, err = st.PresignPut(ctxT(t), "c", time.Minute, objectstore.PresignPutOptions{})
	if !errors.Is(err, objectstore.ErrInvalid) {
		t.Fatalf("PresignPut with SSE-C: %v", err)
	}
}
