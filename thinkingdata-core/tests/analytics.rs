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

//! End-to-end tests for the blocking `TDAnalytics` facade.

#![cfg(feature = "blocking")]

use std::sync::{Arc, Mutex};

use thinkingdata_core::consumer::Consumer;
use thinkingdata_core::{Data, DataType, TDAnalytics, TDError};

/// Records every event the facade hands over; optionally stringent.
#[derive(Clone, Default)]
struct RecordingConsumer {
    events: Arc<Mutex<Vec<Data>>>,
    stringent: bool,
}

impl RecordingConsumer {
    fn new(stringent: bool) -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            stringent,
        }
    }

    fn snapshot(&self) -> Vec<Data> {
        self.events.lock().expect("events lock").clone()
    }
}

impl Consumer for RecordingConsumer {
    fn add(&self, data: &Data) -> Result<(), TDError> {
        self.events.lock().expect("events lock").push(data.clone());
        Ok(())
    }

    fn flush(&self) -> Result<(), TDError> {
        Ok(())
    }

    fn close(&self) -> Result<(), TDError> {
        Ok(())
    }

    fn is_stringent(&self) -> bool {
        self.stringent
    }
}

#[test]
fn track_event_hoists_presets_and_merges_super_properties() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();

    ta.set_super_properties(
        serde_json::json!({ "channel": "rust", "#lib": "should-be-overwritten" }),
    )
    .unwrap();
    ta.track(
        "account_id_123",
        "purchase",
        serde_json::json!({
            "price": 99.0,
            "#ip": "10.0.0.1",
        }),
    )
    .unwrap();

    let events = consumer.snapshot();
    assert_eq!(events.len(), 1);
    let event = &events[0];
    assert_eq!(event.account_id, "account_id_123");
    assert_eq!(event.data_type, DataType::Track);
    assert_eq!(event.event_name, "purchase");
    assert_eq!(event.ip, "10.0.0.1");
    assert!(!event.uuid.is_empty());
    assert_eq!(event.time.len(), 23, "#time: {}", event.time);

    let properties = &event.properties;
    assert_eq!(properties["channel"], "rust");
    // Custom preset keys are hoisted out of the property map.
    assert!(properties.get("#ip").is_none());
    // Wire presets `#lib`/`#lib_version` are stamped by the SDK.
    assert_eq!(properties["#lib"], "Rust");
    assert_eq!(properties["#lib_version"], thinkingdata_core::SDK_VERSION);

    let json = serde_json::to_value(event).unwrap();
    assert_eq!(json["#type"], "track");
    assert_eq!(json["#account_id"], "account_id_123");
    assert_eq!(json["#ip"], "10.0.0.1");
}

#[test]
fn property_precedence_is_dynamic_then_custom() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();

    ta.set_super_properties(serde_json::json!({ "a": "super", "b": "super" }))
        .unwrap();
    ta.set_dynamic_super_properties(|| serde_json::json!({ "b": "dynamic", "c": "dynamic" }));
    ta.track("acc", "event", serde_json::json!({ "c": "custom" }))
        .unwrap();

    let properties = &consumer.snapshot()[0].properties;
    assert_eq!(properties["a"], "super");
    assert_eq!(properties["b"], "dynamic");
    assert_eq!(properties["c"], "custom");
}

#[test]
fn dynamic_super_properties_snapshot_is_taken_outside_the_lock() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();
    ta.set_dynamic_super_properties(|| serde_json::json!({ "tick": 1 }));

    let snapshot = ta.dynamic_super_properties();
    assert_eq!(snapshot["tick"], 1);
    ta.clear_super_properties();
    assert!(ta.super_properties().is_empty());
}

#[test]
fn dynamic_super_properties_invalid_value_falls_back_to_empty() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();

    // A callback returning a non-object Value cannot become properties; the
    // SDK logs a warning and continues with an empty dynamic map.
    ta.set_dynamic_super_properties(|| serde_json::json!("not-an-object"));
    assert!(ta.dynamic_super_properties().is_empty());

    ta.track("acc", "event", serde_json::json!({})).unwrap();
    let properties = &consumer.snapshot()[0].properties;
    assert!(!properties.contains_key("dynamic"));
}

#[test]
fn clones_share_consumer_and_super_properties() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();
    let cloned = ta.clone();

    ta.set_super_properties(serde_json::json!({ "from": "original" }))
        .unwrap();
    cloned.track("acc", "event", serde_json::json!({})).unwrap();

    let events = consumer.snapshot();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].properties["from"], "original");
}

#[test]
fn track_first_requires_first_check_id_and_hoists_it() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();

    let err = ta
        .track_first("acc", "register", "", serde_json::json!({}))
        .unwrap_err();
    assert!(err.to_string().contains("firstCheckId"), "{err}");

    ta.track_first(
        "acc",
        "register",
        "check-id-1",
        serde_json::json!({ "channel": "cli" }),
    )
    .unwrap();

    let event = &consumer.snapshot()[0];
    assert_eq!(event.first_check_id, "check-id-1");
    assert!(event.properties.get("#first_check_id").is_none());
}

#[test]
fn track_update_requires_event_id() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();

    let err = ta
        .track_update("acc", "order_status", "", serde_json::json!({}))
        .unwrap_err();
    assert!(err.to_string().contains("event id"), "{err}");

    ta.track_update(
        "acc",
        "order_status",
        "event-1",
        serde_json::json!({ "status": 1 }),
    )
    .unwrap();
    let event = &consumer.snapshot()[0];
    assert_eq!(event.data_type, DataType::TrackUpdate);
    assert_eq!(event.event_id, "event-1");
}

#[test]
fn track_overwrite_requires_event_id_and_uses_wire_type() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();
    ta.track_overwrite("acc", "order_status", "event-1", serde_json::json!({}))
        .unwrap();
    let event = &consumer.snapshot()[0];
    assert_eq!(event.data_type, DataType::TrackOverwrite);
}

#[test]
fn validation_errors() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();

    // Both IDs empty.
    let err = ta
        .track_with_distinct_id("", "", "login", serde_json::json!({}))
        .unwrap_err();
    assert!(err.to_string().contains("cannot be empty"), "{err}");

    // Empty event name.
    let err = ta.track("acc", "", serde_json::json!({})).unwrap_err();
    assert!(err.to_string().contains("event name"), "{err}");

    // Invalid event name.
    let err = ta
        .track("acc", "bad name", serde_json::json!({}))
        .unwrap_err();
    assert!(err.to_string().contains("invalid event name"), "{err}");

    // user_add with non-numeric values.
    let err = ta
        .user_add("acc", serde_json::json!({ "level": "high" }))
        .unwrap_err();
    assert!(err.to_string().contains("UserAdd"), "{err}");

    // user_unset without keys.
    let err = ta.user_unset("acc", &Vec::<String>::new()).unwrap_err();
    assert!(err.to_string().contains("UserUnset"), "{err}");

    assert!(consumer.snapshot().is_empty());
}

#[test]
fn user_operations_produce_wire_types() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();

    ta.user_set("acc", serde_json::json!({ "vip": true }))
        .unwrap();
    ta.user_set_once("acc", serde_json::json!({ "first_day": "2026-01-01" }))
        .unwrap();
    ta.user_add("acc", serde_json::json!({ "points": 30 }))
        .unwrap();
    ta.user_append("acc", serde_json::json!({ "tags": ["a"] }))
        .unwrap();
    ta.user_uniq_append("acc", serde_json::json!({ "tags": ["b"] }))
        .unwrap();
    ta.user_unset("acc", &["tags", "points"]).unwrap();
    ta.user_del("acc").unwrap();
    ta.user_del_with_properties("acc", serde_json::json!({ "last_seen": "2026-01-01" }))
        .unwrap();
    ta.user_set_with_distinct_id("acc", "distinct-1", serde_json::json!({}))
        .unwrap();

    let types: Vec<DataType> = consumer
        .snapshot()
        .iter()
        .map(|event| event.data_type)
        .collect();
    assert_eq!(
        types,
        vec![
            DataType::UserSet,
            DataType::UserSetOnce,
            DataType::UserAdd,
            DataType::UserAppend,
            DataType::UserUniqAppend,
            DataType::UserUnset,
            DataType::UserDel,
            DataType::UserDel,
            DataType::UserSet,
        ]
    );

    // user_unset keys map to 0 values.
    let unset = &consumer.snapshot()[5];
    assert_eq!(unset.properties["tags"], 0);
    assert_eq!(unset.properties["points"], 0);

    // user_del has empty properties; the with-properties variant keeps them.
    assert!(consumer.snapshot()[6].properties.is_empty());
    assert_eq!(consumer.snapshot()[7].properties["last_seen"], "2026-01-01");
    assert_eq!(consumer.snapshot()[8].distinct_id, "distinct-1");
}

#[test]
fn stringent_consumer_rejects_invalid_property_keys() {
    let lenient = TDAnalytics::new(RecordingConsumer::new(false)).unwrap();
    lenient
        .track("acc", "event", serde_json::json!({ "bad key": 1 }))
        .unwrap();

    let stringent_consumer = RecordingConsumer::new(true);
    let stringent = TDAnalytics::new(stringent_consumer.clone()).unwrap();
    let err = stringent
        .track("acc", "event", serde_json::json!({ "bad key": 1 }))
        .unwrap_err();
    assert!(err.to_string().contains("invalid property key"), "{err}");
    assert!(stringent_consumer.snapshot().is_empty());
}

#[test]
fn track_with_distinct_id_only() {
    let consumer = RecordingConsumer::new(false);
    let ta = TDAnalytics::new(consumer.clone()).unwrap();
    ta.track_with_distinct_id("", "device-1", "anonymous_event", serde_json::json!({}))
        .unwrap();
    let event = &consumer.snapshot()[0];
    assert_eq!(event.distinct_id, "device-1");
    assert!(event.account_id.is_empty());
}

#[test]
fn log_consumer_end_to_end_via_facade() {
    let dir =
        std::env::temp_dir().join(format!("thinkingdata-it-analytics-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let consumer = thinkingdata_core::consumer::LogConsumer::new(dir.to_str().unwrap()).unwrap();
    let ta = TDAnalytics::new(consumer.clone()).unwrap();

    ta.track("acc", "login", serde_json::json!({ "success": true }))
        .unwrap();
    ta.close().unwrap();

    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(entries.len(), 1);
    let content = std::fs::read_to_string(entries.pop().unwrap()).unwrap();
    let parsed: serde_json::Value = content.lines().next().unwrap().parse().unwrap();
    assert_eq!(parsed["#type"], "track");
    assert_eq!(parsed["properties"]["success"], true);
    let _ = std::fs::remove_dir_all(&dir);
}
