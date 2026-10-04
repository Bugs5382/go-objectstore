# AGENTS.md - go-objectstore

Guide for AI agents working in this repository. Pair with `CLAUDE.md` (the working agreement and
hook-enforced rules). Keep this file current when the build, layout, or public API changes.

## What this is

S3-compatible object storage for Go: a small Store interface with streaming put and get, pagination, presigned URLs and copy, an S3 backend tested on MinIO, and an in-memory fake.

A Go library: the `objectstore.Store` interface (put, get, stat, delete, list, copy, presigned
GET and PUT), an S3 backend (`s3store`, aws-sdk-go-v2), an in-memory fake (`memstore`) and the
contract suite both pass (`objectstoretest`). Two things to know before changing it: the root
package must not import an SDK, and every backend behaviour lives in the contract suite, so a
change to one backend's behaviour starts with a contract test that both must pass.

## Using go-objectstore

- Consumers take an `objectstore.Store`; they build `s3store.New(cfg)` in production and
  `memstore.New()` in unit tests.
- Errors are the sentinels in `errors.go`, always matched with `errors.Is`. A cancelled context
  comes back as the context's error, never as a sentinel.
- `WithCodes` attaches the consumer's go-apperr codes; `Observe` is the logging and metrics hook.
  The library itself does not log.
- SSE-C stores cannot presign (`ErrInvalid`): the request would have to carry the key.

## Layout

- `store.go`, `errors.go` - the interface, options and sentinels
- `codes.go`, `observe.go`, `walk.go` - the `WithCodes` and `Observe` decorators and the `All`
  iterator
- `internal/check` - validation and error shaping every backend shares (keys, metadata, limits,
  the counting upload body)
- `objectstoretest/` - the contract suite (`Run`)
- `memstore/` - the fake and its presigned-URL handler
- `s3store/` - the S3 backend: `config.go`, `s3store.go` (get, stat, list, delete, presign),
  `put.go` (single and multipart uploads), `copy.go`, `errors.go` (S3 error mapping)
- `s3store/integration_*_test.go` - MinIO tests behind the `integration` build tag

## Build, test, lint

- Build: `task build`
- Test: `task test` (hermetic, no Docker). `task test:integration` adds the s3store tests, which
  start MinIO containers through testcontainers and need Docker; CI runs them in
  `job-go-integration.yaml`.
- Lint: `task lint` (gofmt, golangci-lint, yamllint); also `golangci-lint run --build-tags
  integration ./...` and `gosec ./...`
- License headers: `task license` (verify), `task license:fix` (inject)

## Logging

Follow the logging rules in `CLAUDE.md`. In short:

- Log generously: entry and exit of significant operations, decisions and branches, retries, state
  changes, external calls (target, duration, outcome), and every error with its context.
- Levels: `trace` for step-by-step detail, `debug` for flow, `info` for lifecycle, `warn` and
  `error` for problems. The environment filters the volume, so err on the side of too much.
- Environments: local dev `trace` with `LOG_FORMAT=console` (never JSON), dev cluster `debug`,
  qa/staging `info`, production `error`. Every cluster environment logs JSON. Set levels through
  `LOG_LEVEL` and `LOG_FORMAT`, never in code; local settings live in the run target or
  `.env.example`.
- Never log secrets, tokens, or personal data, not even at `trace`. Log an opaque or keyed ID.

## Conventions and gotchas

- See `CLAUDE.md` for the branch/commit/PR rules; they are enforced by the git hooks in
  `.claude/hooks` (run `bash .claude/hooks/install.sh` once per clone).
- Open every PR as a draft. CI skips drafts, so run the full checks locally, push once they pass,
  and mark the PR ready when the work is finished; see CLAUDE.md "CI and Actions minutes".
- MinIO cannot store an object and a prefix of the same name (`a` and `a/b`) and caps path
  segments at 255 bytes; keep contract keys inside those limits.
- The default MinIO image is the community `pgsty/minio` build (`OBJECTSTORE_MINIO_IMAGE`
  overrides it); upstream images are no longer published.
- Test identities are fakes under `example.org`/`example.test`, and container credentials are
  generated per run. Never put real hosts or keys in tests.
