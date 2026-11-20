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

//! Integration tests for the blocking `DebugConsumer` against a mock
//! receiver: form encoding, dry run, response codes and stringency.

#![cfg(feature = "blocking")]

mod common;

use common::MockServer;
use thinkingdata_core::consumer::{Consumer, DebugConsumer};
use thinkingdata_core::{Data, DataType};

fn debug_consumer(server: &MockServer, write_data: bool) -> DebugConsumer {
    DebugConsumer::new(&server.url.clone(), "debug_app", write_data).unwrap()
}

fn event() -> Data {
    let mut data = Data::new(DataType::Track);
    data.account_id = "acc".to_string();
    data.event_name = "debug_test".to_string();
    data.properties
        .insert("price".to_string(), serde_json::json!(9.9));
    data
}

#[test]
fn posts_form_encoded_payload_with_dry_run() {
    let server = MockServer::start();
    let consumer = debug_consumer(&server, false); // dry run

    consumer.add(&event()).unwrap();
    server.wait_requests(1);

    let request = &server.requests()[0];
    assert_eq!(request.path, "/data_debug");
    assert!(
        request
            .header("content-type")
            .as_deref()
            .unwrap_or_default()
            .starts_with("application/x-www-form-urlencoded")
    );
    assert_eq!(request.form_field("appid").as_deref(), Some("debug_app"));
    assert_eq!(request.form_field("source").as_deref(), Some("server"));
    assert_eq!(request.form_field("dryRun").as_deref(), Some("1"));

    let payload = request.form_field("data").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(parsed["#type"], "track");
    assert_eq!(parsed["properties"]["price"], 9.9);
}

#[test]
fn write_data_mode_sets_dry_run_to_zero() {
    let server = MockServer::start();
    let consumer = debug_consumer(&server, true);

    consumer.add(&event()).unwrap();
    server.wait_requests(1);
    assert_eq!(
        server.requests()[0].form_field("dryRun").as_deref(),
        Some("0")
    );
}

#[test]
fn device_id_is_appended_when_present() {
    let server = MockServer::start();
    let consumer =
        DebugConsumer::with_device_id(&server.url.clone(), "debug_app", false, "device-42")
            .unwrap();

    consumer.add(&event()).unwrap();
    server.wait_requests(1);
    assert_eq!(
        server.requests()[0].form_field("deviceId").as_deref(),
        Some("device-42")
    );
}

#[test]
fn nonzero_error_level_is_an_error() {
    let server = MockServer::start();
    let consumer = debug_consumer(&server, false);
    server.script(
        200,
        "{\"errorLevel\":2,\"errors\":[{\"reason\":\"bad data\"}]}",
    );

    let err = consumer.add(&event()).unwrap_err();
    assert!(err.to_string().contains("send to receiver failed"), "{err}");
}

#[test]
fn unexpected_status_code_is_reported() {
    let server = MockServer::start();
    let consumer = debug_consumer(&server, false);
    server.script(503, "{}");

    let err = consumer.add(&event()).unwrap_err();
    assert!(
        err.to_string().contains("Unexpected Status Code: 503"),
        "{err}"
    );
}

#[test]
fn missing_error_level_is_invalid_response() {
    let server = MockServer::start();
    let consumer = debug_consumer(&server, false);
    server.script(200, "{\"code\":0}");

    let err = consumer.add(&event()).unwrap_err();
    assert!(
        err.to_string().contains("errorLevel is missing or null"),
        "{err}"
    );
}

#[test]
fn stringent_mode_rejects_invalid_keys_before_any_http_request() {
    let server = MockServer::start();
    // Stringency is applied by the analytics facade, which inspects the
    // consumer's `is_stringent` before handing over an event.
    let consumer = debug_consumer(&server, false);
    let ta = thinkingdata_core::TDAnalytics::new(consumer).unwrap();

    let err = ta
        .track("acc", "event", serde_json::json!({ "bad key": 1 }))
        .unwrap_err();
    assert!(err.to_string().contains("invalid property key"), "{err}");
    assert_eq!(server.request_count(), 0);
}

#[test]
fn complex_payloads_rewrite_rfc3339_timestamps() {
    let server = MockServer::start();
    let consumer = debug_consumer(&server, false);
    let ta = thinkingdata_core::TDAnalytics::new(consumer).unwrap();

    // A nested (complex) value with an RFC 3339 timestamp must be rewritten
    // before being submitted.
    ta.track(
        "acc",
        "debug_test",
        serde_json::json!({
            "price": 9.9,
            "detail": { "ts": "2024-01-02T03:04:05.123+08:00" },
        }),
    )
    .unwrap();
    server.wait_requests(1);

    let payload = server.requests()[0].form_field("data").unwrap();
    assert!(payload.contains("2024-01-02 03:04:05.123"), "{payload}");
}

#[test]
fn scalar_payloads_rewrite_the_top_level_time() {
    let server = MockServer::start();
    let consumer = debug_consumer(&server, false);

    // A raw `Data` snapshot (as the debug example builds one) whose `#time`
    // header is RFC 3339 and whose properties are all scalars: the debug
    // consumer still rewrites it, matching the log/batch consumers. Through
    // the analytics facade this input is rejected up front by the stringent
    // `#time` check, so only the direct-consumer path sees it.
    let mut data = Data::new(DataType::Track);
    data.account_id = "acc".to_string();
    data.event_name = "debug_test".to_string();
    data.time = "2024-01-02T03:04:05Z".to_string();
    data.properties
        .insert("price".to_string(), serde_json::json!(9.9));
    consumer.add(&data).unwrap();
    server.wait_requests(1);

    let payload = server.requests()[0].form_field("data").unwrap();
    assert!(
        payload.contains(r##""#time":"2024-01-02 03:04:05.000""##),
        "{payload}"
    );
}

#[test]
fn empty_server_url_is_rejected() {
    let err = DebugConsumer::new("", "app", true).unwrap_err();
    assert!(err.to_string().contains("ServerUrl"), "{err}");
}
