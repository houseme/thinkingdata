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

//! Facade quick start: the same program as the core `log_basic` example, but
//! depending on the `thinkingdata` facade crate.
//!
//! ```text
//! cargo run -p thinkingdata --example quickstart
//! ```

use thinkingdata::TDAnalytics;
use thinkingdata::TDError;
use thinkingdata::consumer::LogConsumer;

fn main() -> Result<(), TDError> {
    let directory = std::env::temp_dir().join("thinkingdata-facade-quickstart");
    let consumer = LogConsumer::new(directory.to_str().expect("utf-8 temp dir"))?;
    let ta = TDAnalytics::new(consumer)?;

    ta.set_super_properties(serde_json::json!({ "channel": "rust" }))?;
    ta.track(
        "account_id_123",
        "purchase",
        serde_json::json!({ "product_name": "Rust SDK", "price": 99.0 }),
    )?;

    ta.close()?;
    println!("events written to {}", directory.display());
    Ok(())
}
