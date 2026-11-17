# thinkingdata

[![Crates.io](https://img.shields.io/crates/v/thinkingdata.svg)](https://crates.io/crates/thinkingdata)
[![Documentation](https://docs.rs/thinkingdata/badge.svg)](https://docs.rs/thinkingdata)
[![CI](https://github.com/houseme/thinkingdata/actions/workflows/ci.yml/badge.svg)](https://github.com/houseme/thinkingdata/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-stable-orange.svg)](#installation)

Rust SDK for [ThinkingData Analytics](https://www.thinkingdata.cn) — **API entry** (blocking & async)
plus the `thinkingdata` **CLI**. All functional logic lives in
[`thinkingdata-core`](https://crates.io/crates/thinkingdata-core); this crate re-exports it and adds
the command line client.

The workspace overview, relationship to the official Go SDK, and bilingual data-format guides live
in the [root README](https://github.com/houseme/thinkingdata#readme).

## Installation

```toml
[dependencies]
thinkingdata = { version = "0.1", features = ["blocking"] }  # default
# thinkingdata = { version = "0.1", features = ["async"] }   # async API
# thinkingdata = { version = "0.1", features = ["full"] }    # both
```

## Quick start

### Blocking

```no_run
use thinkingdata::{TDAnalytics, consumer::LogConsumer};

# fn main() -> Result<(), thinkingdata::TDError> {
let consumer = LogConsumer::new("/path/to/log/directory")?;
let ta = TDAnalytics::new(consumer)?;

ta.track("account_id_123", "purchase", serde_json::json!({
    "product_name": "Rust SDK",
    "price": 99.0,
}))?;

ta.close()?;
# Ok(())
# }
```

### Async

```no_run
# #[cfg(feature = "async")]
# async fn demo() -> Result<(), thinkingdata::TDError> {
use thinkingdata::{TDAnalyticsAsync, consumer::BatchConsumer};

let consumer = BatchConsumer::new("https://receiver.thinkingdata.io", "your_app_id")?;
let ta = TDAnalyticsAsync::new(consumer)?;

ta.track("account_id_123", "purchase", serde_json::json!({ "price": 99.0 })).await?;
ta.close().await?;
# Ok(())
# }
```

## CLI

The same crate name doubles as the `thinkingdata` binary. Pick exactly one consumer:
`--log-dir`, `--server-url` (+ `--app-id`), or `--debug` (+ `--app-id`).

```bash
thinkingdata --log-dir /tmp/ta-logs track \
    --account-id acc_1 --event-name purchase \
    --properties '{"price": 9.9, "product_name": "book"}'

thinkingdata --server-url https://receiver.thinkingdata.io --app-id YOUR_APP_ID \
    track --account-id acc_1 --event-name login

thinkingdata --server-url https://receiver.thinkingdata.io --app-id YOUR_APP_ID --debug \
    user-set --account-id acc_1 --properties '{"vip": true}'
```

Commands: `track`, `track-first`, `track-update`, `track-overwrite`, `user-set`, `user-set-once`,
`user-add`, `user-append`, `user-uniq-append`, `user-unset`, `user-del`, `flush`, `close`.

## Documentation

- [Workspace README](https://github.com/houseme/thinkingdata#readme) — full API tour and consumers
- [Data rules (EN / 中文)](https://github.com/houseme/thinkingdata/tree/main/docs)
- Official docs: <https://docs-v2.thinkingdata.cn/?code=data_format&lan=zh-CN&version=5.0>

## License

Apache-2.0
