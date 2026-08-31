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

Both accept `--host`/`--port` (default `127.0.0.1:8000`); pass the same values to both, e.g.
`python examples/server.py --port 8010` and `python examples/client.py --port 8010`.
