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

//! The batch consumer: buffers events and uploads them to the receiver.
//!
//! Architecture (ported from the Go SDK, made async-native): `add` appends the
//! serialized event onto an in-memory buffer; a single sender task owns the
//! upload pipeline, which serializes concurrent flushes, retries transient
//! failures three times, keeps undelivered batches in a bounded cache, and
//! optionally auto-flushes on a timer.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::data::Data;
use crate::error::{Result, TDError};
use crate::log::td_log_info;
use crate::utils::parse_time;

/// Event count threshold for triggering a flush.
pub const DEFAULT_BATCH_SIZE: usize = 20;
/// Hard upper bound for the batch size.
pub const MAX_BATCH_SIZE: usize = 200;
/// Request deadline in milliseconds.
pub const DEFAULT_TIMEOUT_MS: u64 = 30_000;
/// Auto flush spacing in seconds.
pub const DEFAULT_INTERVAL_SECS: u64 = 30;
/// Maximum number of cached (undelivered) batches.
pub const DEFAULT_CACHE_CAPACITY: usize = 50;

/// Configuration for [`BatchConsumer`], mirroring the Go SDK's
/// `TDBatchConfig`.
#[derive(Debug, Clone)]
pub struct BatchConsumerConfig {
    /// Receiver URL, e.g. `https://receiver.thinkingdata.io`. The path is
    /// always rewritten to `/sync_server`.
    pub server_url: String,
    /// ThinkingData APP ID.
    pub app_id: String,
    /// Event count threshold for triggering a flush; `0` selects
    /// [`DEFAULT_BATCH_SIZE`], values above [`MAX_BATCH_SIZE`] are clamped.
    pub batch_size: usize,
    /// Request deadline in milliseconds; `0` selects [`DEFAULT_TIMEOUT_MS`].
    pub timeout_ms: u64,
    /// Whether to gzip-compress request bodies (default: enabled).
    pub compress: bool,
    /// Whether a background task flushes on a fixed interval.
    pub auto_flush: bool,
    /// Auto flush spacing in seconds; `0` selects [`DEFAULT_INTERVAL_SECS`].
    pub interval_secs: u64,
    /// Maximum number of cached (undelivered) batches; `0` selects
    /// [`DEFAULT_CACHE_CAPACITY`].
    pub cache_capacity: usize,
}

impl Default for BatchConsumerConfig {
    fn default() -> Self {
        Self {
            server_url: String::new(),
            app_id: String::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            compress: true,
            auto_flush: false,
            interval_secs: DEFAULT_INTERVAL_SECS,
            cache_capacity: DEFAULT_CACHE_CAPACITY,
        }
    }
}

/// Buffered events waiting for delivery.
#[derive(Default)]
struct Queue {
    /// Serialized events not yet assigned to a batch.
    buffer: Vec<Vec<u8>>,
    /// Complete batches that failed delivery and will be retried.
    cache: VecDeque<Vec<Vec<u8>>>,
    /// Set when the consumer is closed; stops admission.
    closed: bool,
}

/// A request for the streaming sender task.
enum Request {
    Flush {
        all: bool,
        respond: oneshot::Sender<Result<()>>,
    },
    Close {
        respond: oneshot::Sender<Result<()>>,
    },
}

/// Shared state of the batch consumer.
struct Inner {
    server_url: String,
    app_id: String,
    compress: bool,
    batch_size: usize,
    cache_capacity: usize,
    client: reqwest::Client,
    queue: Mutex<Queue>,
    req_tx: mpsc::UnboundedSender<Request>,
    close_result: OnceLock<Result<()>>,
}

/// Buffer events and upload them in batches to the ThinkingData receiver over
/// HTTPS.
///
/// This is the Rust counterpart of the Go SDK's `TDBatchConsumer`. When the
/// buffer reaches `batch_size`, or when cached batches exist, the next add
/// (or an explicit `flush`) triggers a delivery. Delivered batches are
/// dropped; undelivered batches stay in a bounded cache and are retried by
/// subsequent flushes.
///
/// ```no_run
/// # fn main() -> Result<(), thinkingdata_core::TDError> {
/// # #[cfg(feature = "blocking")]
/// # {
/// # use thinkingdata_core::consumer::{BatchConsumer, Consumer};
/// # let consumer = BatchConsumer::new("https://receiver.thinkingdata.io", "your_app_id")?;
/// # let data = thinkingdata_core::Data::new(thinkingdata_core::DataType::Track);
/// # consumer.add(&data)?;
/// # consumer.close()?;
/// # }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct BatchConsumer {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for BatchConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchConsumer")
            .field("server_url", &self.inner.server_url)
            .field("app_id", &self.inner.app_id)
            .field("batch_size", &self.inner.batch_size)
            .field("cache_capacity", &self.inner.cache_capacity)
            .field("compress", &self.inner.compress)
            .finish_non_exhaustive()
    }
}

impl BatchConsumer {
    /// Create a batch consumer with default settings (gzip compression
    /// enabled, batch size 20).
    pub fn new(server_url: &str, app_id: &str) -> Result<Self> {
        Self::with_config(BatchConsumerConfig {
            server_url: server_url.to_string(),
            app_id: app_id.to_string(),
            ..BatchConsumerConfig::default()
        })
    }

    /// Create a batch consumer with an explicit batch size.
    pub fn with_batch_size(server_url: &str, app_id: &str, batch_size: usize) -> Result<Self> {
        Self::with_config(BatchConsumerConfig {
            server_url: server_url.to_string(),
            app_id: app_id.to_string(),
            batch_size,
            ..BatchConsumerConfig::default()
        })
    }

    /// Create a batch consumer with explicit compression control.
    pub fn with_compress(server_url: &str, app_id: &str, compress: bool) -> Result<Self> {
        Self::with_config(BatchConsumerConfig {
            server_url: server_url.to_string(),
            app_id: app_id.to_string(),
            compress,
            ..BatchConsumerConfig::default()
        })
    }

    /// Create a batch consumer from a full configuration.
    pub fn with_config(config: BatchConsumerConfig) -> Result<Self> {
        if config.server_url.is_empty() {
            return Err(TDError::msg("ServerUrl not be empty"));
        }
        let mut server_url = url::Url::parse(&config.server_url)?;
        server_url.set_path("/sync_server");

        let batch_size = if config.batch_size > MAX_BATCH_SIZE {
            MAX_BATCH_SIZE
        } else if config.batch_size == 0 {
            DEFAULT_BATCH_SIZE
        } else {
            config.batch_size
        };
        let cache_capacity = if config.cache_capacity == 0 {
            DEFAULT_CACHE_CAPACITY
        } else {
            config.cache_capacity
        };
        let timeout_ms = if config.timeout_ms == 0 {
            DEFAULT_TIMEOUT_MS
        } else {
            config.timeout_ms
        };
        let interval_secs = if config.interval_secs == 0 {
            DEFAULT_INTERVAL_SECS
        } else {
            config.interval_secs
        };

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()?;
        let (req_tx, req_rx) = mpsc::unbounded_channel::<Request>();

        let inner = Arc::new(Inner {
            server_url: server_url.to_string(),
            app_id: config.app_id.clone(),
            compress: config.compress,
            batch_size,
            cache_capacity,
            client,
            queue: Mutex::new(Queue::default()),
            req_tx,
            close_result: OnceLock::new(),
        });

        // Spawn the streaming sender task that owns all uploads.
        let task_inner = inner.clone();
        let auto_flush = config.auto_flush;
        let interval = Duration::from_secs(interval_secs);
        crate::consumer::shared_runtime().spawn(async move {
            sender_loop(task_inner, req_rx, auto_flush, interval).await;
        });

        td_log_info(&format!(
            "Mode: batch consumer, appId: {}, serverUrl: {}",
            config.app_id, inner.server_url
        ));

        Ok(Self { inner })
    }

    /// Enqueue one event; flushes when the buffer reaches the batch size or
    /// when cached batches are waiting for retry.
    async fn do_add(&self, data: &Data) -> Result<()> {
        let json = serde_json::to_vec(data)?;
        let enqueue_log = parse_time(&String::from_utf8_lossy(&json));
        let (ready, has_cache) = {
            let mut queue = self
                .inner
                .queue
                .lock()
                .expect("ThinkingData queue poisoned");
            if queue.closed {
                return Err(TDError::ConsumerClosed);
            }
            queue.buffer.push(json);
            (
                queue.buffer.len() >= self.inner.batch_size,
                !queue.cache.is_empty(),
            )
        };
        td_log_info(&format!("Enqueue event data: {enqueue_log}"));
        if ready || has_cache {
            self.do_flush_inner(false).await
        } else {
            Ok(())
        }
    }

    /// Request a delivery of one (or, with `all`, every) pending batch.
    async fn do_flush_inner(&self, all: bool) -> Result<()> {
        let (respond_tx, respond_rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(Request::Flush {
                all,
                respond: respond_tx,
            })
            .map_err(|_| TDError::ConsumerClosed)?;
        match respond_rx.await {
            Ok(result) => result,
            // A concurrent `close` won the race and shut the sender task down
            // before this request was served. The enqueued data was part of
            // the close-time delivery, so its retained result is the answer.
            Err(_) => match self.inner.close_result.get() {
                Some(result) => result.clone(),
                None => Err(TDError::ConsumerClosed),
            },
        }
    }

    /// Deliver every currently pending batch.
    pub async fn flush_all_async(&self) -> Result<()> {
        self.do_flush_inner(true).await
    }

    /// Stop admission, deliver every pending batch and shut the sender task
    /// down. Repeated calls return the same result and do not resend failed
    /// batches.
    async fn do_close(&self) -> Result<()> {
        if let Some(result) = self.inner.close_result.get() {
            return result.clone();
        }
        let (respond_tx, respond_rx) = oneshot::channel();
        self.inner
            .req_tx
            .send(Request::Close {
                respond: respond_tx,
            })
            .map_err(|_| TDError::ConsumerClosed)?;
        let result = match respond_rx.await {
            Ok(result) => result,
            // A concurrent `close` won the race and shut the sender task down
            // before this request was served; report its retained result.
            Err(_) => match self.inner.close_result.get() {
                Some(result) => result.clone(),
                None => Err(TDError::ConsumerClosed),
            },
        };
        let _ = self.inner.close_result.set(result.clone());
        result
    }
}

/// The streaming sender task: serializes all deliveries, runs the auto-flush
/// timer and processes close.
async fn sender_loop(
    inner: Arc<Inner>,
    mut req_rx: mpsc::UnboundedReceiver<Request>,
    auto_flush: bool,
    interval: Duration,
) {
    loop {
        let request = if auto_flush {
            tokio::select! {
                request = req_rx.recv() => request,
                _ = tokio::time::sleep(interval) => {
                    // Timer-triggered flush never surfaces errors to a caller.
                    let _ = flush_batches(&inner, false, false).await;
                    continue;
                }
            }
        } else {
            req_rx.recv().await
        };

        match request {
            None => return,
            Some(Request::Flush { all, respond }) => {
                let result = flush_batches(&inner, all, false).await;
                let _ = respond.send(result);
            }
            Some(Request::Close { respond }) => {
                {
                    let mut queue = inner.queue.lock().expect("ThinkingData queue poisoned");
                    queue.closed = true;
                }
                let result = flush_batches(&inner, true, true).await;
                let _ = inner.close_result.set(result.clone());
                let _ = respond.send(result);
                return;
            }
        }
    }
}

/// Move the buffer into the cache, deliver pending batches, and put failed
/// batches back (bounded by the cache capacity, keeping the newest).
async fn flush_batches(inner: &Arc<Inner>, all: bool, closing: bool) -> Result<()> {
    let pending: Vec<Vec<Vec<u8>>> = {
        let mut queue = inner.queue.lock().expect("ThinkingData queue poisoned");
        if !closing && queue.closed {
            return Err(TDError::ConsumerClosed);
        }
        if !queue.buffer.is_empty()
            && (all || queue.cache.is_empty() || queue.buffer.len() >= inner.batch_size)
        {
            let buffer = std::mem::take(&mut queue.buffer);
            queue.cache.push_back(buffer);
        }
        let count = if all {
            queue.cache.len()
        } else {
            queue.cache.len().min(1)
        };
        queue.cache.drain(..count).collect()
    };

    let mut first_err: Option<TDError> = None;
    let mut retained: Vec<Vec<Vec<u8>>> = Vec::new();
    for batch in pending {
        let (discard, err) = upload_batch(inner, &batch).await;
        if !discard {
            retained.push(batch);
        }
        if err.is_some() && first_err.is_none() {
            first_err = err;
        }
    }

    let mut queue = inner.queue.lock().expect("ThinkingData queue poisoned");
    // Re-add failed batches in their original order, ahead of newer batches.
    for batch in retained.into_iter().rev() {
        queue.cache.push_front(batch);
    }
    // Keep only the newest `cache_capacity` batches.
    while queue.cache.len() > inner.cache_capacity {
        queue.cache.pop_front();
    }
    first_err.map_or(Ok(()), Err)
}

/// Deliver one batch: gzip-encode (optional), POST with retries and interpret
/// the receiver's response code.
///
/// `discard` is true for every HTTP 200 response, preserving the deletion
/// policy of the Go SDK: batches acknowledged with a data error are dropped.
async fn upload_batch(inner: &Arc<Inner>, batch: &[Vec<u8>]) -> (bool, Option<TDError>) {
    let mut payload = Vec::new();
    payload.push(b'[');
    for (index, event) in batch.iter().enumerate() {
        if index > 0 {
            payload.push(b',');
        }
        payload.extend_from_slice(event);
    }
    payload.push(b']');
    let params = parse_time(&String::from_utf8_lossy(&payload));

    // Encode the request body once; every retry attempt reuses the same bytes.
    let body = if inner.compress {
        match gzip_encode(params.as_bytes()) {
            Ok(encoded) => encoded,
            Err(err) => return (false, Some(err)),
        }
    } else {
        params.clone().into_bytes()
    };

    for attempt in 0..3 {
        let (status, code, err) = send_once(inner, &body, batch.len()).await;
        if status == 200 {
            if let Some(err) = err {
                return (true, Some(err));
            }
            return match code {
                Some(0) => {
                    td_log_info(&format!("send success: {params}"));
                    (true, None)
                }
                Some(1) | Some(-1) => (true, Some(TDError::msg("invalid data format"))),
                Some(-2) => (true, Some(TDError::msg("APP ID doesn't exist"))),
                Some(-3) => (true, Some(TDError::msg("invalid ip transmission"))),
                _ => (true, Some(TDError::msg("unknown error"))),
            };
        }
        // Transport errors surface immediately without a retry, matching Go.
        if let Some(err) = err {
            return (false, Some(err));
        }
        if attempt == 2 {
            return (
                false,
                Some(TDError::msg(format!(
                    "upload failed after 3 attempts. Status code is: {status}"
                ))),
            );
        }
    }
    (false, None)
}

/// The receiver's response envelope: `{"code": 0}`.
#[derive(Debug, serde::Deserialize)]
struct BatchResponse {
    code: Option<i64>,
}

/// POST one request; returns `(status, code, error)`.
async fn send_once(
    inner: &Arc<Inner>,
    body: &[u8],
    size: usize,
) -> (u16, Option<i64>, Option<TDError>) {
    let request = inner
        .client
        .post(&inner.server_url)
        .header("appid", &inner.app_id)
        .header("user-agent", "ta-rust-sdk")
        .header("version", crate::SDK_VERSION)
        .header("compress", if inner.compress { "gzip" } else { "none" })
        .header("TA-Integration-Type", crate::LIB_NAME)
        .header("TA-Integration-Version", crate::SDK_VERSION)
        .header("TA-Integration-Count", size.to_string())
        .header("Content-Type", "application/json")
        .body(body.to_vec());

    let response = match request.send().await {
        Ok(response) => response,
        Err(err) => return (0, None, Some(err.into())),
    };
    let status = response.status().as_u16();
    if status != 200 {
        return (status, None, None);
    }

    let body = match response.text().await {
        Ok(body) => body,
        Err(err) => {
            return (
                status,
                Some(1),
                Some(TDError::msg(format!("read batch response: {err}"))),
            );
        }
    };
    let parsed: BatchResponse = match serde_json::from_str(&body) {
        Ok(parsed) => parsed,
        Err(err) => {
            return (
                status,
                Some(1),
                Some(TDError::msg(format!("decode batch response: {err}"))),
            );
        }
    };
    match parsed.code {
        Some(code) => (status, Some(code), None),
        None => (
            status,
            Some(1),
            Some(TDError::msg(
                "invalid batch response: code is missing or null",
            )),
        ),
    }
}

/// Gzip-compress a request body.
fn gzip_encode(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(data)?;
    encoder.finish().map_err(Into::into)
}

#[cfg(feature = "blocking")]
impl crate::consumer::Consumer for BatchConsumer {
    /// Enqueue one event, blocking until a triggered delivery finishes.
    fn add(&self, data: &Data) -> Result<()> {
        crate::consumer::shared_runtime().block_on(self.do_add(data))
    }

    /// Deliver the next pending batch, blocking until it completes.
    fn flush(&self) -> Result<()> {
        crate::consumer::shared_runtime().block_on(self.do_flush_inner(false))
    }

    /// Stop admission, deliver every pending batch and shut down. Repeated
    /// calls return the same result.
    fn close(&self) -> Result<()> {
        crate::consumer::shared_runtime().block_on(self.do_close())
    }
}

#[cfg(feature = "blocking")]
impl BatchConsumer {
    /// Deliver every currently pending batch.
    pub fn flush_all(&self) -> Result<()> {
        crate::consumer::shared_runtime().block_on(self.flush_all_async())
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl crate::consumer::AsyncConsumer for BatchConsumer {
    /// Enqueue one event; flushes when the buffer reaches the batch size or
    /// when cached batches are waiting for retry.
    async fn add_async(&self, data: &Data) -> Result<()> {
        self.do_add(data).await
    }

    /// Deliver the next pending batch.
    async fn flush_async(&self) -> Result<()> {
        self.do_flush_inner(false).await
    }

    /// Stop admission, deliver every pending batch and shut the sender task
    /// down. Repeated calls return the same result.
    async fn close_async(&self) -> Result<()> {
        self.do_close().await
    }
}
