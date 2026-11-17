# thinkingdata

Rust SDK for [ThinkingData](https://www.thinkingdata.cn) — a server-side SDK for sending events and user properties to the ThinkingData analytics platform, plus a CLI of the same name.

This crate is a Rust port of the official [ThinkingData Go SDK](https://github.com/ThinkingDataAnalytics/go-sdk) (with semantics also cross-checked against the [Lua SDK](https://github.com/ThinkingDataAnalytics/lua-sdk)), preserving its consumer-based architecture (`LogConsumer`, `BatchConsumer`, `DebugConsumer`) while following idiomatic Rust API conventions.

The workspace is split in two:

| Crate | Role |
|---|---|
| `thinkingdata` | API entry (re-exports) **and** the `thinkingdata` CLI binary |
| `thinkingdata-core` | All functional logic: data model, validation, consumers |

---

## Features

- **Event tracking** — track / track_first / track_update / track_overwrite
- **User properties** — user_set, user_set_once, user_add, user_append, user_uniq_append, user_unset, user_del
- **Multiple consumers** — log consumer (LogBus compatible), batch consumer (gzip + retry + failure cache + auto flush), debug consumer (receiver-side validation)
- **Super properties** — static and dynamic global properties applied to every event
- **Blocking and async APIs** — selected by Cargo features; the async side runs on Tokio streaming pipelines (channel-fed writer/sender tasks)
- **TLS** — rustls with the **aws-lc-rs** crypto provider only (via reqwest 0.13 defaults); no native-tls/openssl backend
- **Time** — timestamps are produced with the `jiff` crate (chrono is deliberately not used)

---

## Installation

Add the facade crate to your `Cargo.toml`:

```toml
[dependencies]
thinkingdata = { version = "0.1", features = ["blocking"] }  # default
# thinkingdata = { version = "0.1", features = ["async"] }   # async API
# thinkingdata = { version = "0.1", features = ["full"] }    # both
```

---

## Quick Start

### Initialize the SDK

```rust
use thinkingdata::{TDAnalytics, consumer::LogConsumer};

let consumer = LogConsumer::new("/path/to/log/directory")?;
let ta = TDAnalytics::new(consumer)?;
```

The Go SDK uses `NewTDAnalytics(consumer)` with a consumer argument; this Rust port follows the same shape: the consumer is injected at construction time, and the SDK delegates data delivery to it.

### Send an event

```rust
use std::collections::HashMap;

let mut properties = HashMap::new();
properties.insert("product_name".to_string(), "Rust SDK".into());
properties.insert("price".to_string(), 99.0.into());

ta.track("account_id_123", "purchase", properties)?;
```

The `track` method mirrors the Go SDK's `Track` API; `serde_json::json!({...})` literals and `HashMap`s are both accepted as properties. Operations that need both identifiers take the `*_with_distinct_id` form (`ta.track_with_distinct_id(account_id, distinct_id, event_name, properties)`).

### Set user properties

```rust
let mut user_props = HashMap::new();
user_props.insert("level".to_string(), 10.into());
user_props.insert("vip".to_string(), true.into());

ta.user_set("account_id_123", user_props)?;
```

`user_set` corresponds to the Go SDK's `UserSet` method for general user-property assignment.

### Set super properties

```rust
let mut super_props = HashMap::new();
super_props.insert("channel".to_string(), "rust".into());

ta.set_super_properties(super_props)?;
```

Super properties are applied automatically to every subsequent event, matching the behavior of `SetSuperProperties` in the Go SDK.

### Close the SDK

```rust
ta.close()?;
```

`close` flushes any buffered data and shuts down the consumer gracefully. This is the Rust counterpart of the Go SDK's `Close` method.

---

## Async API

Enable the `async` feature and use `TDAnalyticsAsync` with the `AsyncConsumer` trait. Internals are streaming pipelines: a channel-fed writer task owns all log-file I/O, and the batch consumer's sender task owns all uploads and retries — `add_async` never blocks the calling task.

```rust
use thinkingdata::{TDAnalyticsAsync, consumer::BatchConsumer};

#[tokio::main]
async fn main() -> Result<(), thinkingdata::TDError> {
    let consumer = BatchConsumer::new("https://receiver.thinkingdata.io", "your_app_id")?;
    let ta = TDAnalyticsAsync::new(consumer)?;

    ta.track("account_id_123", "purchase", serde_json::json!({ "price": 99.0 })).await?;
    ta.close().await?;
    Ok(())
}
```

| Feature | API |
|---|---|
| `blocking` (default) | `Consumer` trait, `TDAnalytics`, `consumer.add(&data)` |
| `async` | `AsyncConsumer` trait, `TDAnalyticsAsync`, `consumer.add_async(&data).await` |
| `full` | both of the above |

---

## CLI

The same crate name doubles as the `thinkingdata` binary. Pick exactly one consumer: `--log-dir`, `--server-url` (+ `--app-id`), or `--debug` (+ `--app-id`).

```bash
# Write events as NDJSON files (LogBus compatible)
thinkingdata --log-dir /tmp/ta-logs track \
    --account-id acc_1 --event-name purchase \
    --properties '{"price": 9.9, "product_name": "book"}'

# Upload through the batch consumer
thinkingdata --server-url https://receiver.thinkingdata.io --app-id YOUR_APP_ID \
    track --account-id acc_1 --event-name login

# Validate through the debug consumer (dry run)
thinkingdata --server-url https://receiver.thinkingdata.io --app-id YOUR_APP_ID --debug --dry-run \
    user-set --account-id acc_1 --properties '{"vip": true}'

# Other commands
thinkingdata --log-dir /tmp/ta-logs user-set --account-id acc_1 -p '{"level": 10}'
thinkingdata --log-dir /tmp/ta-logs user-unset --account-id acc_1 --keys level,vip
thinkingdata --log-dir /tmp/ta-logs track-first --account-id acc_1 --event-name register --first-check-id acc_1
thinkingdata --log-dir /tmp/ta-logs flush
```

Commands: `track`, `track-first`, `track-update`, `track-overwrite`, `user-set`, `user-set-once`, `user-add`, `user-append`, `user-uniq-append`, `user-unset`, `user-del`, `flush`, `close`.

---

## Consumers

### LogConsumer

Writes events to a local log file for later ingestion via LogBus or another transfer tool. Corresponds to `TDLogConsumer` in the Go SDK.

```rust
let consumer = LogConsumer::new("/var/log/thinkingdata")?;
```

Files rotate by day (or hour, via `RotateMode::Hourly`) and are paged with `_N` suffixes when `file_size_mb` is set; the writer task resumes at the latest paged index after a restart.

### BatchConsumer

Buffers events and sends them in batches to the ThinkingData receiver endpoint. Corresponds to `TDBatchConsumer` in the Go SDK.

```rust
let consumer = BatchConsumer::new(
    "https://receiver.thinkingdata.io",
    "your_app_id",
)?;
```

Requests are gzip-compressed by default, retried up to three times on non-200 responses, and undelivered batches stay in a bounded cache until a later flush or `close` succeeds.

### DebugConsumer

Reports events one by one and validates them against a receiver endpoint, useful during development. Corresponds to `TDDebugConsumer` in the Go SDK. It is *stringent*: invalid property keys are rejected, and construction raises the global log level to `Debug`.

```rust
let consumer = DebugConsumer::new("https://receiver.thinkingdata.io", "your_app_id", true)?;
```

---

## Relationship to the Go SDK

| Go SDK | Rust SDK |
|---|---|
| `TDAnalytics` | `TDAnalytics` (blocking) / `TDAnalyticsAsync` (async) |
| `Track` | `track` |
| `UserSet` | `user_set` |
| `SetSuperProperties` | `set_super_properties` |
| `Close` | `close` |
| `TDLogConsumer` | `LogConsumer` |
| `TDBatchConsumer` | `BatchConsumer` |
| `TDDebugConsumer` | `DebugConsumer` |

The Rust port preserves the Go SDK's separation between the analytics façade and the delivery mechanism (consumers), so existing Go users can map their mental model directly onto the Rust API. Wire format details (`#lib: Rust`, `#lib_version`, preset-key hoisting, RFC 3339 → `yyyy-MM-dd HH:mm:ss.SSS` rewriting) follow the official SDKs.

---

## Documentation

Bilingual guides live in [docs/](docs/): the TE v5.0 data rules and the full preset-property catalog,
each in [English](docs/data-format.md) and [中文](docs/data-format.zh-CN.md).

Official ThinkingData SDK documentation (including data format and receiver configuration) is available in English, 中文, and 日本語:

- [English](https://docs.thinkingdata.cn/ta-manual/latest/en/installation/installation_menu/server_sdk/golang_sdk_installation/golang_sdk_installation.html)
- [中文](https://docs.thinkingdata.cn/ta-manual/latest/installation/installation_menu/server_sdk/golang_sdk_installation/golang_sdk_installation.html)
- [日本語](https://docs.thinkingdata.cn/ta-manual/latest/installation/installation_menu/server_sdk/golang_sdk_installation/golang_sdk_installation.html)

The Go SDK documentation is directly applicable because this Rust crate implements the same wire protocol and consumer semantics.

---

## Development

```bash
cargo build --workspace              # default features (blocking)
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo run -p thinkingdata --bin thinkingdata -- --help
```

Dependencies are centralized in the root `[workspace.dependencies]` and pinned to their latest published versions.

---

## License

Apache-2.0
