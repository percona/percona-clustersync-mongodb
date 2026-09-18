# MONGODB INTEGRATION KNOWLEDGE

## OVERVIEW

Score: 8; distinct driver-policy boundary. Centralizes URI sanitization, topology/version discovery, schema helpers, and transient retries.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change client policy | `connect.go` | URI validation, option allowlist, concerns, compressors, ping |
| Change retry behavior | `retry.go` | Transient classification, bounded and unbounded backoff |
| Change version support | `version.go` | Version parsing, downgrade rejection, feature gates |
| Change topology discovery | `topo.go` | Replica-set versus sharded detection |
| Change schema inspection | `schema.go` | Collection and index specifications |
| Change sharding helpers | `sharding.go` | Shard metadata and operations |
| Change driver error classes | `errors.go` | Transient and terminal MongoDB errors |

## CONVENTIONS

- `Connect` validates then sanitizes URIs before applying MongoDB driver options.
- Clients use Stable API v1, primary read preference, majority read/write concern, and configured operation timeout.
- URI options pass only through the explicit `allowedConnStringOptions` list.
- Source and target compressor lists are selected independently from config.
- `maxPoolSize` survives URI sanitization: omitted means the driver's 100-connection default; explicit zero means unlimited.
- Ping failure triggers a bounded disconnect before returning the connection error.
- `RunWithRetry` retries only transient errors and stops on context cancellation.
- `RetryWithBackoff` supports bounded attempts or context-bounded unlimited retries.
- Compatibility compares major versions only; source major greater than target is a downgrade error.

## ANTI-PATTERNS

- Do not pass arbitrary MongoDB URI options through the sanitizer.
- Do not use `directConnection` in production connection URIs.
- Do not retry duplicate-key or other classified terminal errors as transient failures.
- Do not continue retry waits after context cancellation.
- Do not add minor/patch compatibility rejection to the major-only contract.
- Do not wrap driver errors when callers intentionally classify them; use a justified `wrapcheck` exception.
