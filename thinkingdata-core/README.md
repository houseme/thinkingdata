# thinkingdata-core

[![Crates.io](https://img.shields.io/crates/v/thinkingdata-core.svg)](https://crates.io/crates/thinkingdata-core)
[![Documentation](https://docs.rs/thinkingdata-core/badge.svg)](https://docs.rs/thinkingdata-core)
[![CI](https://github.com/houseme/thinkingdata/actions/workflows/ci.yml/badge.svg)](https://github.com/houseme/thinkingdata/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Core implementation of the ThinkingData Analytics Rust SDK: the wire-format data model, input
validation, diagnostics logging, and the log/batch/debug consumers ported from the official
[Go SDK](https://github.com/ThinkingDataAnalytics/go-sdk).

You normally do not depend on this crate directly — use the
[`thinkingdata`](https://crates.io/crates/thinkingdata) facade, which re-exports everything and adds
the CLI. Depend on `thinkingdata-core` when you want the library pieces without the CLI dependency
tree.

## Modules

| Module | Contents |
|---|---|
| `data` | `Data` wire model (`#account_id`, `#type`, `properties`, …), `DataType`, preset-key hoisting, `PRESET_PROPERTIES`, `IntoPropertyMap` |
| `consumer` | `Consumer` (blocking) / `AsyncConsumer` (async) traits and the three consumers |
| `analytics` | `TDAnalytics` (blocking) and `TDAnalyticsAsync` facades with super & dynamic super properties |
| `log` | `TDLogLevel`, pluggable `TDLogger` |
| `utils` | key pattern validation, RFC 3339 rewriting (`parse_time`), TE v5.0 value limits (`check_value_limits`), `jiff` timestamps |
| `error` | `TDError` / `Result` |

## Consumers

- **`LogConsumer`** — NDJSON files for LogBus; daily/hourly rotation, size-based paging with
  index restore on restart; a streaming MPSC writer task owns all file I/O.
- **`BatchConsumer`** — buffered upload to `/sync_server`; gzip compression, three retries on
  non-200 responses, bounded failure cache, optional auto-flush timer; a single sender task
  serializes deliveries.
- **`DebugConsumer`** — per-event validation against `/data_debug` (dry-run supported);
  *stringent* mode enforces property keys and the documented value limits.

## Feature flags

| Feature | API |
|---|---|
| `blocking` (default) | `Consumer` trait, `TDAnalytics`, blocking consumer methods |
| `async` | `AsyncConsumer` trait, `TDAnalyticsAsync`, `*_async` methods on Tokio streaming pipelines |
| both | every consumer implements both traits |

## TE v5.0 validation

```rust
use thinkingdata_core::utils::{check_value_limits, is_documented_time_format};
use thinkingdata_core::PRESET_PROPERTIES;

check_value_limits("price", &serde_json::json!(9.9))?;
data.check_limits()?; // #time format, ID length (<=128), and every value
```

See the bilingual guides for the full rule set: [data rules](https://github.com/houseme/thinkingdata/blob/main/docs/data-format.md)
and [preset properties](https://github.com/houseme/thinkingdata/blob/main/docs/preset-properties.md)
(中文 versions alongside).

## Notes

- Timestamps are produced with `jiff`; `chrono` is intentionally not used.
- TLS: rustls with the **aws-lc-rs** provider only (reqwest 0.13 defaults).
- 89 tests including mock-receiver integration tests for every consumer.

## License

Apache-2.0
