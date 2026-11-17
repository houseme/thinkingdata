
# thinkingdata

Rust SDK for [ThinkingData](https://www.thinkingdata.cn) — a server-side SDK for sending events and user properties to the ThinkingData analytics platform.

This crate is a Rust port of the official [ThinkingData Go SDK](https://github.com/ThinkingDataAnalytics/go-sdk), preserving its consumer-based architecture (`LogConsumer`, `BatchConsumer`, `DebugConsumer`) while following idiomatic Rust API conventions.

---

## Features

- **Event tracking** — send custom events with arbitrary properties
- **User properties** — set, update, and overwrite user-level attributes
- **Multiple consumers** — log consumer, batch consumer, and debug consumer
- **Super properties** — register global properties applied to every event
- **Async-ready** — built on Tokio for non-blocking I/O

---

## Installation

Add the following to your `Cargo.toml`:

```toml
[dependencies]
thinkingdata = "0.1"
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

The `track` method mirrors the Go SDK’s `Track` API: it takes an account ID, an event name, and a property map.

### Set user properties

```rust
let mut user_props = HashMap::new();
user_props.insert("level".to_string(), 10.into());
user_props.insert("vip".to_string(), true.into());

ta.user_set("account_id_123", user_props)?;
```

`user_set` corresponds to the Go SDK’s `UserSet` method for general user-property assignment.

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

`close` flushes any buffered data and shuts down the consumer gracefully. This is the Rust counterpart of the Go SDK’s `Close` method.

---

## Consumers

### LogConsumer

Writes events to a local log file for later ingestion via LogBus or another transfer tool. Corresponds to `TDLogConsumer` in the Go SDK.

```rust
let consumer = LogConsumer::new("/var/log/thinkingdata")?;
```

### BatchConsumer

Buffers events and sends them in batches to the ThinkingData receiver endpoint. Corresponds to `TDBatchConsumer` in the Go SDK.

```rust
let consumer = BatchConsumer::new(
    "https://receiver.thinkingdata.io",
    "your_app_id",
)?;
```

### DebugConsumer

Prints events to stderr and optionally validates them against a receiver endpoint, useful during development. Corresponds to `TDDebugConsumer` in the Go SDK.

```rust
let consumer = DebugConsumer::new("https://receiver.thinkingdata.io", "your_app_id", true)?;
```

---

## Relationship to the Go SDK

| Go SDK | Rust SDK |
|---|---|
| `TDAnalytics` | `TDAnalytics` |
| `Track` | `track` |
| `UserSet` | `user_set` |
| `SetSuperProperties` | `set_super_properties` |
| `Close` | `close` |
| `TDLogConsumer` | `LogConsumer` |
| `TDBatchConsumer` | `BatchConsumer` |
| `TDDebugConsumer` | `DebugConsumer` |

The Rust port preserves the Go SDK’s separation between the analytics façade (`TDAnalytics`) and the delivery mechanism (consumers), so existing Go users can map their mental model directly onto the Rust API.

---

## Documentation

ThinkingData SDK documentation (including data format and receiver configuration) is available in English, 中文, and 日本語:

- [English](https://docs.thinkingdata.cn/ta-manual/latest/en/installation/installation_menu/server_sdk/golang_sdk_installation/golang_sdk_installation.html)
- [中文](https://docs.thinkingdata.cn/ta-manual/latest/installation/installation_menu/server_sdk/golang_sdk_installation/golang_sdk_installation.html)
- [日本語](https://docs.thinkingdata.cn/ta-manual/latest/ja/installation/installation_menu/server_sdk/golang_sdk_installation/golang_sdk_installation.html)

The Go SDK documentation is directly applicable because this Rust crate implements the same wire protocol and consumer semantics.

---

## License

Apache-2.0
```
