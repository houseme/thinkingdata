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

//! Debug validation: report one event to the receiver and print its verdict.
//! The debug consumer is *stringent*: invalid property keys fail the report.
//!
//! ```text
//! cargo run -p thinkingdata-core --example debug_validate -- \
//!     https://receiver.thinkingdata.io YOUR_APP_ID
//! ```

use thinkingdata_core::consumer::{Consumer, DebugConsumer};
use thinkingdata_core::{Data, DataType, TDError};

fn main() -> Result<(), TDError> {
    let mut args = std::env::args().skip(1);
    let server_url = args
        .next()
        .unwrap_or_else(|| "https://receiver.thinkingdata.io".to_string());
    let app_id = args.next().unwrap_or_else(|| "YOUR_APP_ID".to_string());

    // write_data = false → dry run: validate only, archive nothing.
    let consumer = DebugConsumer::new(&server_url, &app_id, false)?;

    let mut event = Data::new(DataType::Track);
    event.account_id = "account_id_123".to_string();
    event.event_name = "debug_check".to_string();
    event.properties.insert(
        "product_name".to_string(),
        serde_json::Value::String("Rust SDK".to_string()),
    );
    consumer.add(&event)?;
    consumer.close()?;
    println!("event validated against {server_url}");
    Ok(())
}
