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

//! Integration tests for the async API (`async` feature): streaming pipelines
//! of `BatchConsumer`, `LogConsumer` and the async analytics facade.

#![cfg(feature = "async")]

mod common;

use std::time::Duration;

use common::MockServer;
use thinkingdata_core::consumer::{AsyncConsumer, BatchConsumer, BatchConsumerConfig, LogConsumer};
use thinkingdata_core::{Data, DataType, TDAnalyticsAsync};

fn event(marker: &str) -> Data {
    let mut data = Data::new(DataType::Track);
    data.account_id = "acc".to_string();
    data.event_name = "async_test".to_string();
    data.properties
        .insert("marker".to_string(), serde_json::json!(marker));
    data
}

#[tokio::test]
async fn batch_consumer_async_delivers_events() {
    let server = MockServer::start();
    let consumer = BatchConsumer::with_config(BatchConsumerConfig {
        server_url: server.url.clone(),
        app_id: "async_app".to_string(),
        batch_size: 3,
        compress: false,
        ..BatchConsumerConfig::default()
    })
    .unwrap();

    for index in 0..3 {
        consumer
            .add_async(&event(&format!("a{index}")))
            .await
            .unwrap();
    }
    server.wait_requests(1);

    let body: serde_json::Value =
        serde_json::from_str(&server.requests()[0].body_string()).unwrap();
    assert_eq!(body.as_array().unwrap().len(), 3);
    assert_eq!(body[0]["properties"]["marker"], "a0");
    assert_eq!(body[0]["#type"], "track");

    consumer.flush_async().await.unwrap();
    consumer.close_async().await.unwrap();

    // After close, further adds are rejected.
    let err = consumer.add_async(&event("late")).await.unwrap_err();
    assert!(err.to_string().contains("closed"), "{err}");
}

#[tokio::test]
async fn batch_consumer_async_retries_and_caches() {
    let server = MockServer::start();
    let consumer = BatchConsumer::with_config(BatchConsumerConfig {
        server_url: server.url.clone(),
        app_id: "async_app".to_string(),
        batch_size: 1,
        compress: false,
        ..BatchConsumerConfig::default()
    })
    .unwrap();

    server.script_many(3, 502, "{}");
    let err = consumer.add_async(&event("retry")).await.unwrap_err();
    assert!(err.to_string().contains("Status code is: 502"), "{err}");
    assert_eq!(server.request_count(), 3);

    server.script(200, "{\"code\":0}");
    consumer.flush_async().await.unwrap();
    server.wait_requests(4);
    assert_eq!(server.request_count(), 4);
    consumer.close_async().await.unwrap();
}

#[tokio::test]
async fn log_consumer_async_streams_lines() {
    let dir =
        std::env::temp_dir().join(format!("thinkingdata-it-async-log-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let consumer = LogConsumer::with_config(thinkingdata_core::consumer::LogConsumerConfig {
        directory: dir.to_string_lossy().into_owned(),
        rotate_mode: thinkingdata_core::consumer::RotateMode::Daily,
        file_size_mb: 0,
        file_name_prefix: "ta".to_string(),
    })
    .unwrap();

    consumer.add_async(&event("first")).await.unwrap();
    consumer.add_async(&event("second")).await.unwrap();
    consumer.flush_async().await.unwrap();
    consumer.close_async().await.unwrap();

    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(entries.len(), 1);
    let content = std::fs::read_to_string(entries.pop().unwrap()).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    assert_eq!(lines.len(), 2);
    let parsed: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(parsed["properties"]["marker"], "second");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn analytics_async_end_to_end() {
    let server = MockServer::start();
    let consumer = BatchConsumer::with_config(BatchConsumerConfig {
        server_url: server.url.clone(),
        app_id: "async_ta".to_string(),
        batch_size: 1,
        compress: false,
        ..BatchConsumerConfig::default()
    })
    .unwrap();
    let ta = TDAnalyticsAsync::new(consumer).unwrap();

    ta.set_super_properties(serde_json::json!({ "channel": "async" }))
        .unwrap();
    ta.track(
        "account_id_123",
        "async_purchase",
        serde_json::json!({ "price": 199.0 }),
    )
    .await
    .unwrap();
    ta.user_set("account_id_123", serde_json::json!({ "vip": true }))
        .await
        .unwrap();
    server.wait_requests(2);

    let requests = server.requests();
    let event: serde_json::Value = serde_json::from_str(&requests[0].body_string()).unwrap();
    assert_eq!(event[0]["#type"], "track");
    assert_eq!(event[0]["properties"]["channel"], "async");
    assert_eq!(event[0]["properties"]["#lib"], "Rust");
    let user: serde_json::Value = serde_json::from_str(&requests[1].body_string()).unwrap();
    assert_eq!(user[0]["#type"], "user_set");
    assert_eq!(user[0]["properties"]["vip"], true);

    ta.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_flush_runs_while_the_caller_sleeps() {
    let server = MockServer::start();
    let consumer = BatchConsumer::with_config(BatchConsumerConfig {
        server_url: server.url.clone(),
        app_id: "timer".to_string(),
        compress: false,
        auto_flush: true,
        interval_secs: 1,
        ..BatchConsumerConfig::default()
    })
    .unwrap();

    consumer.add_async(&event("tick")).await.unwrap();
    // No explicit flush: the streaming sender task delivers on its own. The
    // deadline is generous — it only bites on a genuine failure.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while server.request_count() == 0 {
        assert!(std::time::Instant::now() < deadline, "auto flush timed out");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    consumer.close_async().await.unwrap();
}
