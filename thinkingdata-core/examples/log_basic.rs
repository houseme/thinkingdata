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

//! Blocking quick start: write events to local log files (LogBus compatible).
//!
//! ```text
//! cargo run -p thinkingdata-core --example log_basic
//! ```

use std::collections::HashMap;

use thinkingdata_core::consumer::LogConsumer;
use thinkingdata_core::{TDAnalytics, TDError};

fn main() -> Result<(), TDError> {
    // 1. Pick a consumer: here, NDJSON files under a local directory.
    let directory = std::env::temp_dir().join("thinkingdata-example-logs");
    let consumer = LogConsumer::new(directory.to_str().expect("utf-8 temp dir"))?;

    // 2. Create the analytics facade around the consumer.
    let ta = TDAnalytics::new(consumer)?;

    // 3. Super properties are merged into every subsequent event.
    let mut super_properties = HashMap::new();
    super_properties.insert("channel".to_string(), "rust".into());
    ta.set_super_properties(super_properties)?;

    // 4. Track events and user properties.
    let mut properties = HashMap::new();
    properties.insert("product_name".to_string(), "Rust SDK".into());
    properties.insert("price".to_string(), 99.0.into());
    ta.track("account_id_123", "purchase", properties)?;

    ta.user_set(
        "account_id_123",
        serde_json::json!({ "level": 10, "vip": true }),
    )?;

    // A first event is deduplicated by its first_check_id.
    ta.track_first(
        "account_id_123",
        "register",
        "account_id_123",
        serde_json::json!({ "signup_channel": "cli" }),
    )?;

    // 5. Flush buffered data and release resources.
    ta.flush()?;
    ta.close()?;

    println!("events written to {}", directory.display());
    Ok(())
}
