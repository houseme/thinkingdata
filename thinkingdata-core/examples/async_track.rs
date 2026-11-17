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

//! Async quick start (requires the `async` feature): the batch consumer's
//! streaming sender task delivers events while your task keeps running.
//!
//! ```text
//! cargo run -p thinkingdata-core --example async_track --features async -- \
//!     https://receiver.thinkingdata.io YOUR_APP_ID
//! ```

use thinkingdata_core::consumer::BatchConsumer;
use thinkingdata_core::{TDAnalyticsAsync, TDError};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), TDError> {
    let mut args = std::env::args().skip(1);
    let server_url = args
        .next()
        .unwrap_or_else(|| "https://receiver.thinkingdata.io".to_string());
    let app_id = args.next().unwrap_or_else(|| "YOUR_APP_ID".to_string());

    let consumer = BatchConsumer::with_config(thinkingdata_core::consumer::BatchConsumerConfig {
        server_url,
        app_id,
        auto_flush: true,
        interval_secs: 5,
        ..Default::default()
    })?;
    let ta = TDAnalyticsAsync::new(consumer)?;

    // Auto flush delivers buffered events every 5 seconds without blocking
    // this task; add_async only awaits the enqueue acknowledgement.
    for i in 0..10 {
        ta.track(
            "account_id_123",
            "async_event",
            serde_json::json!({ "index": i }),
        )
        .await?;
    }

    ta.close().await?;
    println!("async upload finished");
    Ok(())
}
