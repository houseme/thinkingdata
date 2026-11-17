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

//! The debug consumer: reports events one by one and validates them against
//! the receiver, printing the response on the console.
//!
//! Mirrors the Go SDK's `TDDebugConsumer`. Constructing it raises the global
//! log level to [`TDLogLevel::Debug`](crate::log::TDLogLevel::Debug) — debug
//! mode implies verbose diagnostics — and marks the consumer as *stringent*,
//! so property keys are validated against the key pattern.

use std::time::Duration;

use serde::Deserialize;

use crate::data::Data;
use crate::error::{Result, TDError};
use crate::log::{TDLogLevel, set_log_level, td_log_error, td_log_info};
use crate::utils::parse_time;

#[cfg(feature = "async")]
use crate::consumer::AsyncConsumer;
#[cfg(feature = "blocking")]
use crate::consumer::Consumer;

/// Request deadline for debug validation requests, matching the Go SDK's
/// `DefaultTimeOut`.
pub const DEFAULT_DEBUG_TIMEOUT_MS: u64 = crate::consumer::DEFAULT_TIMEOUT_MS;

/// The receiver's response envelope: `{"errorLevel": 0}`.
#[derive(Debug, Deserialize)]
struct DebugResponse {
    #[serde(rename = "errorLevel")]
    error_level: Option<i32>,
}

/// Validate data against the ThinkingData receiver, one event per request.
///
/// `write_data = false` sends a dry run: the receiver validates and replies
/// but archives nothing — handy during development.
///
/// ```no_run
/// # use thinkingdata_core::consumer::{Consumer, DebugConsumer};
///
/// # #[cfg(feature = "blocking")]
/// # fn main() -> Result<(), thinkingdata_core::TDError> {
/// # // dry run: validate only, do not archive
/// # let consumer = DebugConsumer::new("https://receiver.thinkingdata.io", "your_app_id", false)?;
/// # let data = thinkingdata_core::Data::new(thinkingdata_core::DataType::Track);
/// # consumer.add(&data)?;
/// # consumer.close()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct DebugConsumer {
    server_url: String,
    app_id: String,
    write_data: bool,
    device_id: String,
    client: reqwest::Client,
}

impl DebugConsumer {
    /// Create a debug consumer.
    ///
    /// * `server_url` — receiver URL; the path is rewritten to `/data_debug`.
    /// * `app_id` — ThinkingData APP ID.
    /// * `write_data` — when false, the receiver validates without archiving.
    pub fn new(server_url: &str, app_id: &str, write_data: bool) -> Result<Self> {
        Self::with_device_id(server_url, app_id, write_data, "")
    }

    /// Create a debug consumer bound to a device id, which lets you find the
    /// validated data in the TE console.
    pub fn with_device_id(
        server_url: &str,
        app_id: &str,
        write_data: bool,
        device_id: &str,
    ) -> Result<Self> {
        if server_url.is_empty() {
            let message = "ServerUrl not be empty";
            td_log_error(message);
            return Err(TDError::msg(message));
        }
        let mut url = url::Url::parse(server_url)?;
        url.set_path("/data_debug");

        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(DEFAULT_DEBUG_TIMEOUT_MS))
            .build()?;

        let consumer = Self {
            server_url: url.to_string(),
            app_id: app_id.to_string(),
            write_data,
            device_id: device_id.to_string(),
            client,
        };

        // Enable debug logging only after initialization succeeds, matching
        // the Go SDK.
        set_log_level(TDLogLevel::Debug);
        td_log_info(&format!(
            "Mode: debug consumer, appId: {}, serverUrl: {}",
            consumer.app_id, consumer.server_url
        ));

        Ok(consumer)
    }

    /// Serialize and POST one event as a form request.
    async fn do_add(&self, data: &Data) -> Result<()> {
        let json = serde_json::to_string(data)?;
        // Nested (complex) values may embed RFC 3339 timestamps that the
        // receiver cannot read; rewrite them like the Go SDK does.
        let json_str = if data.is_complex {
            parse_time(&json)
        } else {
            json
        };
        td_log_info(&json_str);

        let dry_run = if self.write_data { "0" } else { "1" };
        let mut form = vec![
            ("data", json_str.as_str()),
            ("appid", self.app_id.as_str()),
            ("source", "server"),
            ("dryRun", dry_run),
        ];
        if !self.device_id.is_empty() {
            form.push(("deviceId", self.device_id.as_str()));
        }

        let response = self
            .client
            .post(&self.server_url)
            .form(&form)
            .send()
            .await?;
        let status = response.status().as_u16();
        if status != 200 {
            return Err(TDError::msg(format!("Unexpected Status Code: {status}")));
        }

        let body = response.text().await?;
        let parsed: DebugResponse = serde_json::from_str(&body).map_err(TDError::from)?;
        match parsed.error_level {
            Some(0) => {
                td_log_info("send success");
                Ok(())
            }
            Some(_) => {
                let message = format!("send to receiver failed with return content:  {body}");
                td_log_error(&message);
                Err(TDError::msg(message))
            }
            None => Err(TDError::msg(
                "invalid debug response: errorLevel is missing or null",
            )),
        }
    }
}

#[cfg(feature = "blocking")]
impl Consumer for DebugConsumer {
    /// Serialize and POST one event, blocking until the receiver answers.
    fn add(&self, data: &Data) -> Result<()> {
        crate::consumer::shared_runtime().block_on(self.do_add(data))
    }

    /// The debug consumer sends every event immediately; nothing to flush.
    fn flush(&self) -> Result<()> {
        Ok(())
    }

    /// The debug consumer is stateless; nothing to close.
    fn close(&self) -> Result<()> {
        Ok(())
    }

    /// The debug consumer validates property keys strictly.
    fn is_stringent(&self) -> bool {
        true
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncConsumer for DebugConsumer {
    /// Serialize and POST one event without blocking the calling task.
    async fn add_async(&self, data: &Data) -> Result<()> {
        self.do_add(data).await
    }

    /// The debug consumer sends every event immediately; nothing to flush.
    async fn flush_async(&self) -> Result<()> {
        Ok(())
    }

    /// The debug consumer is stateless; nothing to close.
    async fn close_async(&self) -> Result<()> {
        Ok(())
    }

    /// The debug consumer validates property keys strictly.
    fn is_stringent(&self) -> bool {
        true
    }
}
