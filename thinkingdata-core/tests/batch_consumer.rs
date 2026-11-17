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

//! Integration tests for the blocking `BatchConsumer` against a mock
//! receiver: batching, gzip, retries, failure cache and close semantics.

#![cfg(feature = "blocking")]

mod common;

use common::MockServer;
use thinkingdata_core::consumer::{BatchConsumer, BatchConsumerConfig, Consumer};
use thinkingdata_core::{Data, DataType, TDError};

fn event(marker: &str) -> Data {
    let mut data = Data::new(DataType::Track);
    data.account_id = "acc".to_string();
    data.event_name = "batch_test".to_string();
    data.properties
        .insert("marker".to_string(), serde_json::json!(marker));
    data
}

fn config(server: &MockServer, batch_size: usize) -> BatchConsumerConfig {
    BatchConsumerConfig {
        server_url: server.url.clone(),
        app_id: "test_app".to_string(),
        batch_size,
        compress: false,
        ..BatchConsumerConfig::default()
    }
}

#[test]
fn flushes_batch_when_threshold_reached() {
    let server = MockServer::start();
    let consumer = BatchConsumer::with_config(config(&server, 5)).unwrap();

    for index in 0..5 {
        consumer.add(&event(&format!("e{index}"))).unwrap();
    }
    server.wait_requests(1);

    let requests = server.requests();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.path, "/sync_server");
    assert_eq!(request.header("appid").as_deref(), Some("test_app"));
    assert_eq!(request.header("compress").as_deref(), Some("none"));
    assert_eq!(request.header("user-agent").as_deref(), Some("ta-rust-sdk"));
    assert_eq!(
        request.header("TA-Integration-Type").as_deref(),
        Some("Rust")
    );
    assert_eq!(request.header("TA-Integration-Count").as_deref(), Some("5"));

    let body: serde_json::Value = serde_json::from_str(&request.body_string()).unwrap();
    let events = body.as_array().unwrap();
    assert_eq!(events.len(), 5);
    assert_eq!(events[0]["properties"]["marker"], "e0");
    assert_eq!(events[4]["properties"]["marker"], "e4");
    // Raw Data snapshots bypass the facade, so no `#lib` stamp is expected here.

    consumer.close().unwrap();
}

#[test]
fn gzip_payload_is_decompressed_by_the_receiver() {
    let server = MockServer::start();
    // Default configuration compresses with gzip.
    let consumer = BatchConsumer::new(&server.url.clone(), "gzip_app").unwrap();
    consumer.add(&event("gz")).unwrap();
    consumer.close().unwrap();
    server.wait_requests(1);

    let request = &server.requests()[0];
    assert_eq!(request.header("compress").as_deref(), Some("gzip"));
    let decoded = {
        let mut decoder = flate2::read::GzDecoder::new(request.body.as_slice());
        let mut out = String::new();
        std::io::Read::read_to_string(&mut decoder, &mut out).unwrap();
        out
    };
    let body: serde_json::Value = serde_json::from_str(&decoded).unwrap();
    assert_eq!(body[0]["properties"]["marker"], "gz");
    // `#app_id` is only serialized when supplied in the properties.
    assert!(body[0].get("#app_id").is_none());
}

#[test]
fn server_data_errors_discard_the_batch() {
    let server = MockServer::start();
    let consumer = BatchConsumer::with_config(config(&server, 1)).unwrap();

    // add triggers a flush at batch size 1; the receiver answers 200 with a
    // data error. HTTP 200 always discards the batch.
    server.script(200, "{\"code\":-2}");
    let err = consumer.add(&event("x")).unwrap_err();
    assert!(err.to_string().contains("APP ID doesn't exist"), "{err}");
    server.wait_requests(1);

    // The batch was discarded, so a later flush sends nothing.
    assert_eq!(server.request_count(), 1);
    consumer.close().unwrap();
    assert_eq!(server.request_count(), 1);
}

#[test]
fn network_errors_are_retried_three_times_then_cached() {
    let server = MockServer::start();
    let consumer = BatchConsumer::with_config(config(&server, 1)).unwrap();

    // The add-triggered flush is attempted three times against HTTP 500.
    server.script_many(3, 500, "{}");
    let err = consumer.add(&event("retry")).unwrap_err();
    assert!(err.to_string().contains("Status code is: 500"), "{err}");
    assert_eq!(server.request_count(), 3);

    // The failed batch stays cached; the next flush retries it successfully.
    server.script(200, "{\"code\":0}");
    consumer.flush().unwrap();
    server.wait_requests(4);
    let body: serde_json::Value =
        serde_json::from_str(&server.requests()[3].body_string()).unwrap();
    assert_eq!(body[0]["properties"]["marker"], "retry");
    assert_eq!(server.request_count(), 4);
    consumer.close().unwrap();
}

#[test]
fn cached_batches_keep_submission_order_and_close_delivers_all() {
    let server = MockServer::start();
    let consumer = BatchConsumer::with_config(config(&server, 2)).unwrap();

    // Trace with batch_size = 2:
    //   add 1 → buffered, no flush
    //   add 2 → flush b1 (3 × 500)
    //   add 3 → cache non-empty → flush b1 again (3 × 500)
    //   add 4 → buffer full again → flush b1 a third time (3 × 500)
    // cache now holds [b1, b2].
    server.script_many(9, 500, "{}");
    let _ = consumer.add(&event("b1-e1"));
    let _ = consumer.add(&event("b1-e2"));
    let _ = consumer.add(&event("b2-e1"));
    let _ = consumer.add(&event("b2-e2"));
    server.wait_requests(9);

    // close() flushes every cached batch in submission order.
    server.script(200, "{\"code\":0}");
    server.script(200, "{\"code\":0}");
    consumer.close().unwrap();
    server.wait_requests(11);

    let requests = server.requests();
    assert_eq!(requests.len(), 11);
    let first: serde_json::Value = serde_json::from_str(&requests[9].body_string()).unwrap();
    let second: serde_json::Value = serde_json::from_str(&requests[10].body_string()).unwrap();
    assert_eq!(first[0]["properties"]["marker"], "b1-e1");
    assert_eq!(second[0]["properties"]["marker"], "b2-e1");
}

#[test]
fn auto_flush_delivers_without_explicit_flush() {
    let server = MockServer::start();
    let consumer = BatchConsumer::with_config(BatchConsumerConfig {
        server_url: server.url.clone(),
        app_id: "auto".to_string(),
        compress: false,
        auto_flush: true,
        interval_secs: 1,
        ..BatchConsumerConfig::default()
    })
    .unwrap();

    consumer.add(&event("timer")).unwrap();
    server.wait_requests(1);
    let body: serde_json::Value =
        serde_json::from_str(&server.requests()[0].body_string()).unwrap();
    assert_eq!(body[0]["properties"]["marker"], "timer");
    consumer.close().unwrap();
}

#[test]
fn close_is_idempotent_and_rejects_later_adds() {
    let server = MockServer::start();
    let consumer = BatchConsumer::with_config(config(&server, 10)).unwrap();

    consumer.close().unwrap();
    let second = consumer.close();
    assert!(second.is_ok());

    let err = consumer.add(&event("late")).unwrap_err();
    assert!(err.to_string().contains("closed"), "{err}");

    let err = consumer.flush().unwrap_err();
    assert!(err.to_string().contains("closed"), "{err}");
    assert_eq!(server.request_count(), 0);
}

#[test]
fn invalid_server_url_is_rejected() {
    let err = BatchConsumer::new("", "app").unwrap_err();
    assert!(err.to_string().contains("ServerUrl"), "{err}");

    let err = BatchConsumer::new("not a url", "app").unwrap_err();
    assert!(matches!(err, TDError::InvalidServerUrl(_)), "{err}");
}
