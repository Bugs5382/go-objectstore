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
	"errors"
	"testing"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/s3store"
)

func TestNewRejectsBadConfig(t *testing.T) {
	good := s3store.Config{
		Endpoint: "http://127.0.0.1:9000", Region: "us-east-1", Bucket: "files",
		AccessKeyID: "access", SecretAccessKey: "secret", PathStyle: true,
	}
	if _, err := s3store.New(good); err != nil {
		t.Fatalf("New(good): %v", err)
	}
	cases := map[string]func(*s3store.Config){
		"no bucket":          func(c *s3store.Config) { c.Bucket = "" },
		"bad bucket":         func(c *s3store.Config) { c.Bucket = "Not_A_Bucket" },
		"no region":          func(c *s3store.Config) { c.Region = "" },
		"no credentials":     func(c *s3store.Config) { c.AccessKeyID, c.SecretAccessKey = "", "" },
		"half credentials":   func(c *s3store.Config) { c.SecretAccessKey = "" },
		"endpoint not a URL": func(c *s3store.Config) { c.Endpoint = "127.0.0.1:9000" },
		"endpoint with path": func(c *s3store.Config) { c.Endpoint = "http://127.0.0.1:9000/files" },
		"small parts":        func(c *s3store.Config) { c.PartSize = s3store.MinPartSize - 1 },
		"negative max":       func(c *s3store.Config) { c.MaxObjectSize = -1 },
		"short SSE-C key": func(c *s3store.Config) {
			c.Encryption = s3store.Encryption{Mode: s3store.SSEC, CustomerKey: make([]byte, 16)}
		},
		"KMS key without KMS": func(c *s3store.Config) { c.Encryption = s3store.Encryption{Mode: s3store.SSES3, KMSKeyID: "k"} },
		"customer key without SSE-C": func(c *s3store.Config) {
			c.Encryption = s3store.Encryption{Mode: s3store.SSEKMS, CustomerKey: make([]byte, 32)}
		},
		"unknown SSE mode": func(c *s3store.Config) { c.Encryption = s3store.Encryption{Mode: 9} },
		"unknown checksum": func(c *s3store.Config) { c.Checksum = 9 },
		"negative retries": func(c *s3store.Config) { c.RetryMaxAttempts = -1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := good
			mutate(&cfg)
			if _, err := s3store.New(cfg); !errors.Is(err, objectstore.ErrInvalid) {
				t.Fatalf("New = %v, want ErrInvalid", err)
			}
		})
	}
}

func TestBucket(t *testing.T) {
	st, err := s3store.New(s3store.Config{Region: "us-east-1", Bucket: "files", AccessKeyID: "a", SecretAccessKey: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if st.Bucket() != "files" {
		t.Fatalf("Bucket = %q", st.Bucket())
	}
}
