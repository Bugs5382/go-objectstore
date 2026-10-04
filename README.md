# go-objectstore 🪣

> 🧭 S3-compatible object storage for Go: a small Store interface with streaming put and get, pagination, presigned URLs and copy, an S3 backend tested on MinIO, and an in-memory fake.

Editor images, attachments and rendered PDFs all end up in object storage: AWS S3 in one place,
MinIO or SeaweedFS in another. `go-objectstore` puts them behind one eight-method interface, so
application code never touches an SDK, its unit tests run on an in-memory fake, and the fake is
held to the same contract as the real backend.

## ✨ Highlights

- 🌊 **Streaming both ways:** a Put buffers one part at a time whatever the object size, and a Get
  can read a byte range.
- 🧩 **No SDK in the root:** `objectstore` depends only on `go-apperr`; aws-sdk-go-v2 comes in only
  when you import `s3store`.
- 🧪 **One contract, two backends:** `objectstoretest.Run` is the suite. `s3store` passes it on real
  MinIO (path-style, virtual-host and Content-MD5 runs) and `memstore` passes it in memory.
- 🔗 **Presigned URLs that work:** `PresignGet` and `PresignPut` return the method, URL, required
  headers and expiry, ready for a browser or a plain `http.Client`, the fake included.
- 🔐 **Integrity and encryption:** a SHA-256 (or Content-MD5) checksum on every upload request,
  and SSE-S3, SSE-KMS or SSE-C on every object.
- 🚦 **Errors you can branch on:** `ErrNotFound`, `ErrAlreadyExists`, `ErrPermissionDenied`,
  `ErrUnavailable`, `ErrTooLarge` and `ErrInvalid` from every backend, with `WithCodes` for your
  go-apperr codes.

## 📦 Install

```bash
go get github.com/Bugs5382/go-objectstore
```

## 🚀 Usage

Build an S3 store once per bucket and pass it around as an `objectstore.Store`:

```go
store, err := s3store.New(s3store.Config{
    Endpoint:        "http://seaweedfs-s3.example.org:8333", // empty for AWS S3
    Region:          "us-east-1",
    Bucket:          "documents",
    AccessKeyID:     os.Getenv("S3_ACCESS_KEY"),
    SecretAccessKey: os.Getenv("S3_SECRET_KEY"),
    PathStyle:       true, // MinIO and SeaweedFS; AWS wants virtual-host style
    Encryption:      s3store.Encryption{Mode: s3store.SSES3},
    MaxObjectSize:   100 << 20,
})

info, err := store.Put(ctx, "pdf/2026/report.pdf", file, objectstore.PutOptions{
    ContentType: "application/pdf",
    Metadata:    map[string]string{"document": "42"},
    IfNotExists: true,
})

obj, err := store.Get(ctx, "pdf/2026/report.pdf", objectstore.GetOptions{Offset: 0, Length: 1024})
defer obj.Body.Close()

link, err := store.PresignGet(ctx, "pdf/2026/report.pdf", 15*time.Minute)
// hand link.URL to the browser
```

On AWS with IAM roles or IRSA, load the SDK config yourself and pass `Credentials: awsCfg.Credentials`
instead of static keys.

| Method | What it does |
|---|---|
| `Put(ctx, key, body, PutOptions)` | Streams a body. Content type, metadata, `Size`, `MaxSize`, `IfNotExists`, cache and disposition headers. |
| `Get(ctx, key, GetOptions)` | Opens the object or a range; `Object` carries the full size, the offset and the body length. |
| `Stat(ctx, key)` | Size, ETag, last modified, content type and metadata. |
| `Delete(ctx, key)` | Removes the object; a missing key is not an error. |
| `List(ctx, ListOptions)` | One page: prefix, delimiter, page token, limit (up to 1000). |
| `Copy(ctx, src, dst, CopyOptions)` | Server-side copy, keeping or replacing the metadata. |
| `PresignGet(ctx, key, expiry)` | A signed download, 1 second to 7 days. |
| `PresignPut(ctx, key, expiry, PresignPutOptions)` | A signed upload, optionally pinned to one content type. |

`objectstore.All(ctx, store, opts)` walks every page as an iterator.

## 🧱 Design

- 📐 **Small interface, rich options:** eight methods, each with an options struct, so new knobs do
  not break implementations.
- 📏 **Limits before bytes:** a declared `Size` over the limit fails before the body is read; an
  undeclared one fails as soon as it passes the limit. A body shorter or longer than its declared
  `Size` fails with `ErrInvalid`. Nothing is stored in either case.
- 🧹 **No orphaned parts:** a failed or cancelled multipart upload is aborted, even after the
  caller's context is gone.
- 🛑 **Cancellation everywhere:** every call checks its context, an upload stops mid-stream, and a
  Get body returns `context.Canceled` once its context is done.
- 🧾 **Checksums by the store:** `s3store` computes the checksum of each request itself and turns
  off the SDK's default CRC32 trailers, which some S3-compatible services reject. Full-object reads
  turn on the SDK's response checksum validation.
- 📋 **Copy pins the source:** the copy sends the source's ETag, so a source replaced mid-copy fails
  the copy rather than mixing versions. Objects over 5 GiB are copied part by part.
- 🔏 **Presigning and SSE-C don't mix:** an SSE-C request must carry the customer key, so presigning
  fails with `ErrInvalid` on an SSE-C store. SSE-S3 and SSE-KMS presigned uploads return the
  encryption headers the upload must send.
- 🏷️ **Codes are the caller's:** the library never picks go-apperr codes. Wrap a store with
  `objectstore.WithCodes(store, objectstore.Codes{NotFound: 1404, ...})` to attach yours.
- 🔭 **No logging inside:** `objectstore.Observe(store, fn)` reports every call (operation, key,
  duration, bytes, error) for your logger, metrics or tracer.

### Keys and metadata

Keys are 1 to 1024 bytes of valid UTF-8. Keep path segments under 255 bytes and don't store an
object and a prefix with the same name (`a` and `a/b`): MinIO's file-system layout refuses both,
though AWS allows them. Metadata keys use letters, digits and `-`, come back in lower case, and
values are printable ASCII, 2 KiB in total. Listed objects carry key, size, ETag and last modified
only; call `Stat` for the rest.

## 🧪 Testing your code

Use `memstore` in unit tests. Serve its handler when the code under test follows presigned URLs:

```go
var store *memstore.Store
srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    store.Handler().ServeHTTP(w, r)
}))
defer srv.Close()
store = memstore.New(memstore.WithBaseURL(srv.URL+"/bucket"), memstore.WithMaxObjectSize(10<<20))
```

Writing another backend? Run the contract against it:

```go
func TestContract(t *testing.T) {
    objectstoretest.Run(t, func(t *testing.T) objectstore.Store { return newStore(t) })
}
```

## 🛠 Develop

```bash
task build             # go build ./...
task test              # go test ./... (no Docker needed)
task test:integration  # also runs s3store against MinIO in Docker (testcontainers)
task lint              # gofmt check + golangci-lint + yamllint
task license           # check MIT headers (golic)
```

The integration tests use a community MinIO build (`pgsty/minio`), since upstream MinIO images are
no longer published; set `OBJECTSTORE_MINIO_IMAGE` to use another. Each run makes its own
throwaway credentials, and the SSE-C tests run MinIO over TLS with a certificate made for the run.

## ⚖️ License

MIT (c) 2026 Shane
