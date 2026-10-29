# tiny-idempotency
[![CI](https://github.com/hussain-alsaibai/tiny-idempotency/actions/workflows/ci.yml/badge.svg)](https://github.com/hussain-alsaibai/tiny-idempotency/actions/workflows/ci.yml)

**Zero-dependency idempotency keys for Python.**

Single-file library that gives you Stripe-style "exactly-once" semantics on
top of at-least-once delivery:

* `@idempotent(store=..., key_arg=...)` decorator — retries return the
  cached result instead of re-running the function.
* Atomic `claim`/`complete`/`fail` lifecycle — two concurrent callers with
  the same key cannot both run the function.
* Argument fingerprinting — same key with different payload raises
  `KeyMismatch` instead of silently returning the wrong cached result.
* `InMemoryStore` for single-process services.
* `FileStore` for cross-process / crash-resilient persistence (atomic
  NDJSON append with optional `fsync`).

> Part of the [`tiny-*` ecosystem](https://github.com/hussain-alsaibai) — 21+
> single-file Python libraries, **0 dependencies across the entire stack**.

---

## Why?

The "exactly-once" promise is impossible at the network layer; every
reliable distributed system is "at-least-once" and must defend against
duplicates on the receiving side. The standard tool is an **idempotency
key**: a caller-supplied token that, for a given operation, uniquely
identifies the attempt.

Implementing it correctly is non-trivial:

* **Atomic claim** — two concurrent requests with the same key must not
  both run the function.
* **Crash recovery** — if the server dies mid-request, the next request
  with the same key must run the function (not return a partial).
* **Payload fingerprinting** — same key with different body is a bug
  (caller reused the key).

The two popular implementations are both heavyweight: Stripe's
open-source `stripe-ruby` is 1 KLOC, and the Python `idempotency-key`
package on PyPI ships 4 transitive deps. `tiny-idempotency` does the
same in ~400 LOC with 0 deps.

## Install

```bash
curl -O https://raw.githubusercontent.com/hussain-alsaibai/tiny-idempotency/main/tiny_idempotency.py
```

## Quickstart

```python
from tiny_idempotency import idempotent, InMemoryStore, KeyMismatch

store = InMemoryStore(ttl_seconds=24 * 3600)

@idempotent(store=store, key_arg="idem_key")
def charge_card(idem_key, amount, currency):
    # ... call Stripe ...
    return {"id": "ch_123", "amount": amount}

# First call: runs the function, caches the result.
result = charge_card(idem_key="k1", amount=100, currency="usd")

# Retry within TTL: returns the cached result without re-running.
result = charge_card(idem_key="k1", amount=100, currency="usd")

# Same key, different payload: raises KeyMismatch.
try:
    charge_card(idem_key="k1", amount=200, currency="usd")
except KeyMismatch as e:
    print(f"key {e.key} reused with different args")
```

## Crash-resilient FileStore

```python
from tiny_idempotency import idempotent, FileStore

store = FileStore(directory="/var/lib/myapp/idem", ttl_seconds=24*3600)

@idempotent(store=store, key_arg="idem_key")
def send_email(idem_key, to, subject, body):
    ...
```

`FileStore` writes an append-only NDJSON log with atomic `os.replace` and
optional `fsync`. If your service crashes mid-request, the next request
with the same key either re-runs (if the claim was never completed) or
returns the cached result (if `complete` was written).

## Concurrent calls

When two workers receive the same key simultaneously:

1. The first one wins the atomic `claim`, sees `status="in_progress"` was
   not set, and proceeds to run the function.
2. The second one sees `status="in_progress"` and raises
   `InProgressError` (caller should retry with backoff).
3. After the first finishes, subsequent callers get the cached result
   until the TTL expires.

## API

### `@idempotent(store, *, key_arg=None, key_fn=None, ttl_seconds=None, fingerprint_arg=None, fingerprint_fn=None)`

Wrap a function so retries with the same key return the cached result.

| Param             | Meaning |
|-------------------|---------|
| `store`           | `InMemoryStore` or `FileStore`. |
| `key_arg`         | Name of the keyword argument carrying the idempotency key (mutually exclusive with `key_fn`). |
| `key_fn`          | Callable `(*args, **kwargs) -> str` deriving the key dynamically. |
| `ttl_seconds`     | Override the store's default TTL for this function. |
| `fingerprint_arg` | Extra keyword arg included in the fingerprint (e.g. a client nonce). |
| `fingerprint_fn`  | Callable `(*args, **kwargs) -> str` for a custom fingerprint. |

### `InMemoryStore(ttl_seconds=86400, max_entries=100_000)`

Thread-safe in-process store. Records are lost on process exit.

### `FileStore(directory, ttl_seconds=86400, fsync=True)`

Crash-resilient NDJSON-backed store. Cross-process safe on POSIX
(advisory `fcntl.flock`); best-effort on Windows.

### Exceptions

| Exception         | When |
|-------------------|------|
| `KeyMismatch`     | Same key reused with different arguments (or different fingerprint). |
| `InProgressError` | Concurrent worker still processing the key (caller should retry). |

## Use cases

* **HTTP APIs** — accept an `Idempotency-Key` header and replay the
  cached response on retries (Stripe, Square, PayPal all do this).
* **Webhook handlers** — protect against duplicate webhook deliveries.
* **At-least-once queues** — pair with [`tiny-queue`](https://github.com/hussain-alsaibai/tiny-queue)
  so retries don't double-charge.
* **Background jobs** — guard `send_email`, `charge_card`, `create_user`
  with `idempotency_key` so retries are safe.

## Migration from common patterns

| Old pattern                                              | New |
|----------------------------------------------------------|-----|
| Hand-rolled "SELECT then INSERT" race condition          | `store.claim(...)` |
| Redis `SETNX` + result cache                            | `InMemoryStore` or `FileStore` |
| Ignore duplicates and hope for the best                  | `@idempotent(...)` |
| `stripe-ruby` 1 KLOC                                    | `tiny_idempotency` ~400 LOC |

## Tests

```bash
python3 test_tiny_idempotency.py
```

23 tests, covers fingerprinting, in-memory + file store lifecycle,
concurrent claims, decorator behaviour, failure recovery.

## License

MIT.

## Sibling libraries

- [`tiny-router`](https://github.com/hussain-alsaibai/tiny-router) — HTTP routing
- [`tiny-log`](https://github.com/hussain-alsaibai/tiny-log) — structured logging
- [`tiny-config`](https://github.com/hussain-alsaibai/tiny-config) — config loader
- [`tiny-validator`](https://github.com/hussain-alsaibai/tiny-validator) — input validation
- [`tiny-cli`](https://github.com/hussain-alsaibai/tiny-cli) — CLI builder
- [`tiny-cron`](https://github.com/hussain-alsaibai/tiny-cron) — scheduler
- [`tiny-flags`](https://github.com/hussain-alsaibai/tiny-flags) — feature flags
- [`tiny-queue`](https://github.com/hussain-alsaibai/tiny-queue) — persistent queue
- [`tiny-rate`](https://github.com/hussain-alsaibai/tiny-rate) — rate limiter
- [`tiny-retry`](https://github.com/hussain-alsaibai/tiny-retry) — retry + backoff
- [`tiny-pool`](https://github.com/hussain-alsaibai/tiny-pool) — worker pool
- [`tiny-cache`](https://github.com/hussain-alsaibai/fast-cache) — LRU + TTL cache
- [`tiny-trace`](https://github.com/hussain-alsaibai/tiny-trace) — distributed tracing
- [`tiny-secret`](https://github.com/hussain-alsaibai/tiny-secret) — secret loader
- [`tiny-compose`](https://github.com/hussain-alsaibai/tiny-compose) — DI container
- [`tiny-agent`](https://github.com/hussain-alsaibai/tiny-agent) — agent framework
- [`tiny-mcp`](https://github.com/hussain-alsaibai/tiny-mcp) — MCP server
- [`tiny-embed`](https://github.com/hussain-alsaibai/tiny-embed) — embeddings
- [`tiny-metrics`](https://github.com/hussain-alsaibai/tiny-metrics) — Prometheus metrics
- [`tiny-timeout`](https://github.com/hussain-alsaibai/tiny-timeout) — timeouts that work

Built by [OpenClaw](https://github.com/hussain-alsaibai) — autonomous developer agent.