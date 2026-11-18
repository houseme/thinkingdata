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

//! Delivery mechanisms for [`Data`]: the [`Consumer`] / [`AsyncConsumer`]
//! traits and the three built-in consumers ported from the Go SDK.
//!
//! - [`LogConsumer`] writes NDJSON log files consumed later by LogBus. In
//!   async mode it runs a streaming writer task fed by an MPSC channel.
//! - [`BatchConsumer`] buffers events and uploads them over HTTPS (gzip
//!   compression, three retry attempts, a failure cache, optional auto flush).
//! - [`DebugConsumer`] validates each event against the receiver, one by one.
//!
//! # Choosing an API style
//!
//! ```no_run
//! # // Blocking (feature `blocking`): returns immediately after the request.
//! # #[cfg(feature = "blocking")]
//! # fn demo() -> Result<(), thinkingdata_core::TDError> {
//! # use thinkingdata_core::consumer::{BatchConsumer, Consumer};
//!
//! # let consumer = BatchConsumer::new("https://receiver.thinkingdata.io", "app_id")?;
//! # consumer.flush()?;
//! # consumer.close()?;
//! # Ok(())
//! # }
//!
//! # // Async (feature `async`): awaits the delivery result.
//! # #[cfg(feature = "async")]
//! # async fn demo_async() -> Result<(), thinkingdata_core::TDError> {
//! # use thinkingdata_core::consumer::{AsyncConsumer, BatchConsumer};
//!
//! # let consumer = BatchConsumer::new("https://receiver.thinkingdata.io", "app_id")?;
//! # consumer.flush_async().await?;
//! # consumer.close_async().await?;
//! # Ok(())
//! # }
//! ```

pub(crate) mod batch_consumer;
pub(crate) mod debug_consumer;
pub(crate) mod log_consumer;

pub use batch_consumer::{
    BatchConsumer, BatchConsumerConfig, DEFAULT_BATCH_SIZE, DEFAULT_CACHE_CAPACITY,
    DEFAULT_INTERVAL_SECS, DEFAULT_TIMEOUT_MS, MAX_BATCH_SIZE,
};
pub use debug_consumer::DebugConsumer;
pub use log_consumer::{LogConsumer, LogConsumerConfig, RotateMode};

// The traits are feature-gated, so their imports must be too; the shared
// runtime is not — every consumer spawns its pipeline regardless of features.
#[cfg(any(feature = "blocking", feature = "async"))]
use crate::data::Data;
#[cfg(any(feature = "blocking", feature = "async"))]
use crate::error::Result;
#[cfg(any(feature = "blocking", feature = "async"))]
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// Synchronous delivery interface implemented by all consumers.
///
/// This is the Rust counterpart of the Go SDK's `TDConsumer` interface: the
/// [`TDAnalytics`](crate::analytics::TDAnalytics) facade validates and builds
/// events, then delegates delivery to a consumer. Available with the
/// `blocking` feature.
#[cfg(feature = "blocking")]
pub trait Consumer: Send + Sync {
    /// Deliver one event. Called with an already validated snapshot.
    fn add(&self, data: &Data) -> Result<()>;

    /// Deliver everything currently buffered.
    fn flush(&self) -> Result<()>;

    /// Flush, release resources and stop accepting new events. Repeated calls
    /// return the same result.
    fn close(&self) -> Result<()>;

    /// Whether this consumer validates property keys strictly. Only the debug
    /// consumer is stringent.
    fn is_stringent(&self) -> bool {
        false
    }
}

/// Asynchronous delivery interface implemented by all consumers.
///
/// Mirrors [`Consumer`] with async methods; available with the `async`
/// feature. Background work (file writing, HTTP upload) always runs on
/// streaming Tokio tasks, so `add_async` performs no blocking I/O on the
/// calling task.
#[cfg(feature = "async")]
#[async_trait::async_trait]
pub trait AsyncConsumer: Send + Sync {
    /// Deliver one event. Called with an already validated snapshot.
    async fn add_async(&self, data: &Data) -> Result<()>;

    /// Deliver everything currently buffered.
    async fn flush_async(&self) -> Result<()>;

    /// Flush, release resources and stop accepting new events. Repeated calls
    /// return the same result.
    async fn close_async(&self) -> Result<()>;

    /// Whether this consumer validates property keys strictly. Only the debug
    /// consumer is stringent.
    fn is_stringent(&self) -> bool {
        false
    }
}

#[cfg(feature = "blocking")]
impl<C: Consumer + ?Sized> Consumer for Arc<C> {
    fn add(&self, data: &Data) -> Result<()> {
        (**self).add(data)
    }

    fn flush(&self) -> Result<()> {
        (**self).flush()
    }

    fn close(&self) -> Result<()> {
        (**self).close()
    }

    fn is_stringent(&self) -> bool {
        (**self).is_stringent()
    }
}

#[cfg(feature = "blocking")]
impl<C: Consumer + ?Sized> Consumer for Box<C> {
    fn add(&self, data: &Data) -> Result<()> {
        (**self).add(data)
    }

    fn flush(&self) -> Result<()> {
        (**self).flush()
    }

    fn close(&self) -> Result<()> {
        (**self).close()
    }

    fn is_stringent(&self) -> bool {
        (**self).is_stringent()
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<C: AsyncConsumer + ?Sized + Sync> AsyncConsumer for Arc<C> {
    async fn add_async(&self, data: &Data) -> Result<()> {
        (**self).add_async(data).await
    }

    async fn flush_async(&self) -> Result<()> {
        (**self).flush_async().await
    }

    async fn close_async(&self) -> Result<()> {
        (**self).close_async().await
    }

    fn is_stringent(&self) -> bool {
        (**self).is_stringent()
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl<C: AsyncConsumer + ?Sized + Sync> AsyncConsumer for Box<C> {
    async fn add_async(&self, data: &Data) -> Result<()> {
        (**self).add_async(data).await
    }

    async fn flush_async(&self) -> Result<()> {
        (**self).flush_async().await
    }

    async fn close_async(&self) -> Result<()> {
        (**self).close_async().await
    }

    fn is_stringent(&self) -> bool {
        (**self).is_stringent()
    }
}

/// Process-wide async runtime shared by the consumers.
///
/// The consumers drive their streaming pipelines (log writer task, batch
/// sender task, HTTP requests) on this runtime. The blocking API additionally
/// uses `Runtime::block_on` to await pipeline results from synchronous code —
/// therefore consumer methods must not be called from inside an async context
/// when using the blocking API.
pub(crate) fn shared_runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("thinkingdata-runtime")
            .enable_all()
            .build()
            .expect("failed to start the ThinkingData async runtime")
    })
}
