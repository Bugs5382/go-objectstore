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
	"github.com/aws/aws-sdk-go-v2/aws"
)

// Config describes one bucket on an S3-compatible service.
type Config struct {
	// Endpoint is the service's base URL, such as http://minio:9000 or
	// https://seaweedfs-s3.example.org:8333. Empty means AWS S3 for Region.
	Endpoint string
	// Region is required. MinIO and SeaweedFS accept any value; us-east-1 is
	// the usual one.
	Region string
	// Bucket is the bucket every key lives in.
	Bucket string

	// AccessKeyID, SecretAccessKey and SessionToken are static credentials.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	// Credentials, when set, is used instead of the static keys: pass
	// aws.Config.Credentials from config.LoadDefaultConfig for IAM roles,
	// web identity (IRSA) or profiles.
	Credentials aws.CredentialsProvider

	// PathStyle puts the bucket in the path (http://host/bucket/key), which
	// MinIO and SeaweedFS need unless they are set up for virtual hosts. The
	// default is virtual-host style (http://bucket.host/key), which AWS wants.
	PathStyle bool

	// Encryption is the server-side encryption applied to every object.
	Encryption Encryption
	// Checksum is the integrity check sent with every upload and part.
	Checksum Checksum

	// PartSize is the multipart part size, at least 5 MiB; default 8 MiB.
	// An upload buffers one part at a time, so it bounds the memory a Put
	// uses whatever the object size. Bodies up to one part go in one request.
	PartSize int64
	// MaxObjectSize caps every upload; zero means no cap of our own.
	MaxObjectSize int64

	// HTTPClient sends the requests; set it for a private CA, a proxy or
	// timeouts. Nil uses the SDK's default client.
	HTTPClient aws.HTTPClient
	// RetryMaxAttempts is the number of attempts per request, retries
	// included. Zero uses the SDK default (3).
	RetryMaxAttempts int
}

// SSE selects a server-side encryption mode.
type SSE int

// Server-side encryption modes.
const (
	// SSENone leaves encryption to the bucket's default.
	SSENone SSE = iota
	// SSES3 is SSE-S3: the service manages the keys (AES256).
	SSES3
	// SSEKMS is SSE-KMS with Encryption.KMSKeyID, or the service's default
	// KMS key when that is empty.
	SSEKMS
	// SSEC is SSE-C: the service encrypts with Encryption.CustomerKey, which
	// every read, copy and stat must send again. It needs HTTPS.
	SSEC
)

// Encryption configures server-side encryption.
type Encryption struct {
	Mode SSE
	// KMSKeyID names the key for SSEKMS.
	KMSKeyID string
	// CustomerKey is the 32-byte AES-256 key for SSEC. Keep it out of logs
	// and config files; load it from a secret store.
	CustomerKey []byte
}

// Checksum selects the integrity check on uploads.
type Checksum int

// Upload checksums. The service recomputes the checksum and refuses a body
// that does not match, so a corrupted upload never becomes an object.
const (
	// ChecksumSHA256 sends x-amz-checksum-sha256 on every request.
	ChecksumSHA256 Checksum = iota
	// ChecksumMD5 sends Content-MD5, for services without flexible
	// checksums.
	ChecksumMD5
)

const (
	// MinPartSize is S3's smallest multipart part.
	MinPartSize = 5 << 20
	// DefaultPartSize is used when Config.PartSize is zero.
	DefaultPartSize = 8 << 20
)
