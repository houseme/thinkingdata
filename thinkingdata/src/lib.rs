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

//! Rust SDK for [ThinkingData](https://www.thinkingdata.cn) — API entry.
//!
//! This crate is the API entry of the ThinkingData Rust SDK: it re-exports
//! everything from [`thinkingdata_core`] so users depend on a single crate,
//! and additionally ships the `thinkingdata` CLI binary. All functional
//! logic lives in `thinkingdata-core`.
//!
//! # Feature flags
//!
//! - `blocking` (default): synchronous API — [`consumer::Consumer`],
//!   [`TDAnalytics`], blocking consumer methods.
//! - `async`: asynchronous API — [`consumer::AsyncConsumer`],
//!   [`TDAnalyticsAsync`], `*_async` consumer methods built on Tokio
//!   streaming pipelines.
//! - `full`: both of the above.
//!
//! # Quick start
//!
//! ```no_run
//! use thinkingdata::{TDAnalytics, consumer::LogConsumer};
//!
//! # fn main() -> Result<(), thinkingdata::TDError> {
//! let consumer = LogConsumer::new("/path/to/log/directory")?;
//! let ta = TDAnalytics::new(consumer)?;
//!
//! let mut properties = std::collections::HashMap::new();
//! properties.insert("product_name".to_string(), "Rust SDK".into());
//! properties.insert("price".to_string(), 99.0.into());
//! ta.track("account_id_123", "purchase", properties)?;
//!
//! ta.close()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Command line
//!
//! The same crate name doubles as the CLI binary:
//!
//! ```text
//! thinkingdata --log-dir /tmp/ta-logs track \
//!     --account-id acc_1 --event-name purchase \
//!     --properties '{"price": 9.9, "product_name": "book"}'
//!
//! thinkingdata --server-url https://receiver.thinkingdata.io --app-id YOUR_APP_ID \
//!     track --account-id acc_1 --event-name login
//!
//! thinkingdata --server-url https://receiver.thinkingdata.io --app-id YOUR_APP_ID --debug \
//!     user-set --account-id acc_1 --properties '{"vip": true}'
//! ```

/// Delivery mechanisms: [`consumer::Consumer`], [`consumer::AsyncConsumer`]
/// and the built-in log/batch/debug consumers.
pub use thinkingdata_core::consumer;

/// Diagnostics logging (`TDLogLevel`, custom loggers).
pub use thinkingdata_core::log;

/// Wire-format data model and validation helpers.
pub use thinkingdata_core::{data, error, utils};

pub use thinkingdata_core::{Data, DataType, IntoPropertyMap, LIB_NAME, Result, TDError};

/// The SDK version, reported as `#lib_version` on every event.
pub const SDK_VERSION: &str = thinkingdata_core::SDK_VERSION;

/// The blocking analytics facade. Available with the `blocking` feature.
#[cfg(feature = "blocking")]
pub use thinkingdata_core::TDAnalytics;

/// The asynchronous analytics facade. Available with the `async` feature.
#[cfg(feature = "async")]
pub use thinkingdata_core::TDAnalyticsAsync;

#[cfg(feature = "blocking")]
pub mod cli;
