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
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/Bugs5382/go-objectstore"
	"github.com/Bugs5382/go-objectstore/s3store"
)

// defaultImage is a community build of MinIO; the upstream images are no
// longer published. OBJECTSTORE_MINIO_IMAGE overrides it.
const defaultImage = "pgsty/minio:RELEASE.2026-08-04T00-00-00Z"

const (
	region = "us-east-1"
	// vhostDomain is MinIO's virtual-host domain in the tests; the test
	// dialer sends every name under it to the container.
	vhostDomain = "s3.example.test"
	kmsKeyName  = "objectstore-test"
)

// server is one running MinIO container with throwaway credentials.
type server struct {
	endpoint  string
	access    string
	secret    string
	client    *http.Client
	container testcontainers.Container
}

type serverKind int

const (
	plainServer serverKind = iota
	vhostServer
	tlsServer
)

var (
	serversMu sync.Mutex
	servers   = map[serverKind]*server{}
)

func TestMain(m *testing.M) {
	code := m.Run()
	for _, s := range servers {
		_ = s.container.Terminate(context.Background())
	}
	os.Exit(code)
}

func randomHex(t testing.TB, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// minio returns the shared container of the given kind, starting it on first
// use. A missing Docker daemon fails the test: these tests are the proof that
// the backend works, so they never skip.
func minio(t *testing.T, kind serverKind) *server {
	t.Helper()
	serversMu.Lock()
	defer serversMu.Unlock()
	if s, ok := servers[kind]; ok {
		return s
	}
	image := os.Getenv("OBJECTSTORE_MINIO_IMAGE")
	if image == "" {
		image = defaultImage
	}
	kms := make([]byte, 32)
	if _, err := rand.Read(kms); err != nil {
		t.Fatal(err)
	}
	s := &server{access: "test" + randomHex(t, 8), secret: randomHex(t, 20)}
	req := testcontainers.ContainerRequest{
		Image:        image,
		ExposedPorts: []string{"9000/tcp"},
		Env: map[string]string{
			"MINIO_ROOT_USER":      s.access,
			"MINIO_ROOT_PASSWORD":  s.secret,
			"MINIO_KMS_SECRET_KEY": kmsKeyName + ":" + base64.StdEncoding.EncodeToString(kms),
		},
		Cmd:        []string{"server", "/data"},
		WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp").WithStartupTimeout(2 * time.Minute),
	}
	scheme := "http"
	var tlsCfg *tls.Config
	switch kind {
	case vhostServer:
		req.Env["MINIO_DOMAIN"] = vhostDomain
	case tlsServer:
		certPEM, keyPEM, pool := selfSigned(t)
		req.Cmd = []string{"server", "/data", "--certs-dir", "/certs"}
		req.Files = []testcontainers.ContainerFile{
			{Reader: strings.NewReader(string(certPEM)), ContainerFilePath: "/certs/public.crt", FileMode: 0o644},
			{Reader: strings.NewReader(string(keyPEM)), ContainerFilePath: "/certs/private.key", FileMode: 0o600},
		}
		tlsCfg = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		req.WaitingFor = wait.ForHTTP("/minio/health/live").WithPort("9000/tcp").
			WithTLS(true, tlsCfg).WithStartupTimeout(2 * time.Minute)
		scheme = "https"
	}
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("start MinIO (%s): %v", image, err)
	}
	s.container = c
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	if host == "localhost" {
		host = "127.0.0.1"
	}
	addr := net.JoinHostPort(host, port.Port())
	transport := &http.Transport{TLSClientConfig: tlsCfg, MaxIdleConnsPerHost: 64}
	s.endpoint = scheme + "://" + addr
	if kind == vhostServer {
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		transport.DialContext = func(ctx context.Context, network, a string) (net.Conn, error) {
			h, _, err := net.SplitHostPort(a)
			if err == nil && (h == vhostDomain || strings.HasSuffix(h, "."+vhostDomain)) {
				a = addr
			}
			return dialer.DialContext(ctx, network, a)
		}
		s.endpoint = "http://" + vhostDomain + ":" + port.Port()
	}
	s.client = &http.Client{Transport: transport, Timeout: 2 * time.Minute}
	servers[kind] = s
	return s
}

func selfSigned(t *testing.T) (certPEM, keyPEM []byte, pool *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "minio.example.test"},
		DNSNames:              []string{"localhost", "minio.example.test"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool = x509.NewCertPool()
	pool.AddCert(cert)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), pool
}

// config is a Config for a fresh bucket on srv.
func (srv *server) config(t *testing.T) s3store.Config {
	t.Helper()
	return s3store.Config{
		Endpoint:        srv.endpoint,
		Region:          region,
		Bucket:          "t-" + randomHex(t, 8),
		AccessKeyID:     srv.access,
		SecretAccessKey: srv.secret,
		PathStyle:       true,
		PartSize:        s3store.MinPartSize,
		HTTPClient:      srv.client,
	}
}

// open builds a Store from cfg and creates its bucket.
func open(t *testing.T, cfg s3store.Config) *s3store.Store {
	t.Helper()
	st, err := s3store.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := st.EnsureBucket(ctx); err != nil {
		t.Fatalf("EnsureBucket %s: %v", cfg.Bucket, err)
	}
	return st
}

// raw is a plain SDK client on srv, for checking what the store did.
func (srv *server) raw() *s3.Client {
	return s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: aws.String(srv.endpoint),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(srv.access, srv.secret, ""),
		HTTPClient:   srv.client,
	})
}

// must panics on err, which fails the running test with the error; it keeps
// the checks of setup calls on one line.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func ctxT(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func newRequest(p objectstore.Presigned, body string) (*http.Request, error) {
	req, err := http.NewRequest(p.Method, p.URL.String(), strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, vs := range p.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	return req, nil
}
