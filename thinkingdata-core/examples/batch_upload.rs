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

//! Blocking batch upload: buffer events and POST them to the receiver.
//!
//! ```text
//! cargo run -p thinkingdata-core --example batch_upload -- \
//!     https://receiver.thinkingdata.io YOUR_APP_ID
//! ```

use thinkingdata_core::consumer::BatchConsumer;
use thinkingdata_core::{TDAnalytics, TDError};

fn main() -> Result<(), TDError> {
    let mut args = std::env::args().skip(1);
    let server_url = args
        .next()
        .unwrap_or_else(|| "https://receiver.thinkingdata.io".to_string());
    let app_id = args.next().unwrap_or_else(|| "YOUR_APP_ID".to_string());

    // Gzip compression is on by default; batches flush every 20 events.
    let consumer = BatchConsumer::new(&server_url, &app_id)?;
    let ta = TDAnalytics::new(consumer)?;

    ta.set_super_properties(serde_json::json!({ "channel": "rust-example" }))?;

    for i in 0..25 {
        ta.track(
            "account_id_123",
            "batch_event",
            serde_json::json!({
                "index": i,
                "#ip": "10.0.0.1",
            }),
        )?;
    }

    // close() flushes every pending batch before shutting down.
    ta.close()?;
    println!("uploaded 25 events to {server_url}");
    Ok(())
}
