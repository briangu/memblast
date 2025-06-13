# Component Quick-Start

This document outlines optional runtime instrumentation that works well with **memblast**.
It describes which crates to enable and the benefits of each choice.

## Why it fits Memblast

`memblast` relies heavily on asynchronous tasks and network IO. The tooling below
provides visibility without altering the core API or adding measurable overhead.

## Metrics

- **tokio-metrics** – collects runtime and task statistics.
- Wrapped with `tokio-metrics-collector` to expose a Prometheus-ready `/metrics` endpoint on `0.0.0.0:9898`.
- Zero overhead sampling that integrates cleanly with the async stack.

## Tracing / Logs

- Use `tracing` with the `tracing-opentelemetry` layer.
- Structured JSON logs can be shipped to Loki/Grafana or Jaeger without code changes.

## Custom counters

Expose the following counters from your application:

- `updates_total`
- `bytes_sent`
- `p99_latency`
- `snapshot_compactions_total`
- `rdma_cq_errors`

These metrics give you end-to-end visibility across logical operations and the
RDMA transport.

## Health probes

- `/readyz` returns `200` when the current head has advanced within *N* ms.
- `/livez` always returns `200` unless the process panicked.

These endpoints are Kubernetes‑friendly and trivial to unit test.

## Runtime debugging

Ship with the **tokio-console** feature flag. `tokio-metrics` already feeds it so
no extra wiring is required.

Launch with:

```bash
TOKIO_CONSOLE_BIND=0.0.0.0:6669 cargo run --features console-subscriber
```

to generate flame graphs of all tasks.
