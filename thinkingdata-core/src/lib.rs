// Copyright 2026 houseme
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Core implementation of the ThinkingData Analytics Rust SDK.
//!
//! This crate carries all functional logic: the wire-format [`data::Data`]
//! model, input validation, diagnostics logging, and the three consumers
//! (`log`, `batch`, `debug`) ported from the official ThinkingData Go/Lua
//! SDKs. The [`thinkingdata`](https://crates.io/crates/thinkingdata) facade
//! crate re-exports everything and adds a CLI.
//!
//! # Feature flags
//!
//! - `blocking` (default): the synchronous [`consumer::Consumer`] trait,
//!   [`TDAnalytics`], and blocking consumer methods.
//! - `async`: the [`consumer::AsyncConsumer`] trait, [`TDAnalyticsAsync`], and
//!   `*_async` consumer methods. Internals are streaming pipelines built on
//!   Tokio (channel-fed writer/sender tasks).
//!
//! Both features may be enabled together; every consumer then implements both
//! traits.
//!
//! # TLS
//!
//! HTTP transport uses `reqwest` 0.13 with its default rustls stack backed by
//! the **aws-lc-rs** crypto provider only; no native-tls/openssl backend is
//! supported or compiled in.
//!
//! # Quick start (blocking)
//!
//! ```no_run
//! # #[cfg(feature = "blocking")]
//! # fn quickstart() -> Result<(), thinkingdata_core::TDError> {
//! # use thinkingdata_core::{TDAnalytics, consumer::LogConsumer};
//!
//! # let consumer = LogConsumer::new("/var/log/thinkingdata")?;
//! # let ta = TDAnalytics::new(consumer)?;
//!
//! # ta.track("account_id_123", "purchase", serde_json::json!({
//! #     "product_name": "Rust SDK",
//! #     "price": 99.0,
//! # }))?;
//!
//! # ta.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Quick start (async)
//!
//! ```no_run
//! # #[cfg(feature = "async")]
//! # async fn demo() -> Result<(), thinkingdata_core::TDError> {
//! # use thinkingdata_core::{TDAnalyticsAsync, consumer::BatchConsumer};
//!
//! # let consumer = BatchConsumer::new("https://receiver.thinkingdata.io", "your_app_id")?;
//! # let ta = TDAnalyticsAsync::new(consumer)?;
//!
//! # ta.track("account_id_123", "purchase", serde_json::json!({ "price": 99.0 })).await?;
//!
//! # ta.close().await?;
//! # Ok(())
//! # }
//! ```

pub mod analytics;
pub mod consumer;
pub mod data;
pub mod error;
pub mod log;
pub mod utils;

pub use data::{Data, DataType, IntoPropertyMap, LIB_NAME, PRESET_PROPERTIES, is_preset_property};
pub use error::{Result, TDError};

/// The version of the SDK, reported as `#lib_version` on every event.
pub const SDK_VERSION: &str = env!("CARGO_PKG_VERSION");

/// The blocking analytics facade. Available with the `blocking` feature.
#[cfg(feature = "blocking")]
pub use analytics::TDAnalytics;

/// The asynchronous analytics facade. Available with the `async` feature.
#[cfg(feature = "async")]
pub use analytics::TDAnalyticsAsync;
