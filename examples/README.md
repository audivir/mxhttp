# mxhttp examples

A local Litestar server and mxhttp client walking through every feature: CRUD, inline query
parameters, raw request bodies, dynamic parameter bags, `@response_handler`/`@request_handler`
(including per-endpoint overrides), SSE, byte streaming, resumable streaming, resumable
downloads to disk, multi-part parallel downloads, checksum verification, `@retry`, idempotency
keys, `@ratelimit`, `@concurrency`, and authentication.

## Setup

```bash
pip install "mxhttp[examples]"
```

## Run

In one terminal:

```bash
python examples/server.py
```

In another:

```bash
python examples/client.py
```
