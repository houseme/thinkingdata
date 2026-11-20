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

//! The wire-format data model of the ThinkingData receiver.

use std::collections::{BTreeMap, HashMap};

use serde::Serialize;
use serde_json::{Map, Value};

use crate::error::{Result, TDError};
#[cfg(any(feature = "blocking", feature = "async"))]
use crate::log::td_log_error;
use crate::utils::format_now;
#[cfg(any(feature = "blocking", feature = "async"))]
use crate::utils::{generate_uuid, is_valid_key};

/// The name of the SDK that produced the data, reported as `#lib`.
pub const LIB_NAME: &str = "Rust";

/// Property keys that are hoisted out of `properties` into dedicated top-level
/// fields of the wire format.
#[cfg(any(feature = "blocking", feature = "async"))]
pub(crate) const PRESET_KEYS: [&str; 7] = [
    "#ip",
    "#app_id",
    "#time",
    "#first_check_id",
    "#uuid",
    "#transaction_property",
    "#import_tool_id",
];

/// Every TE preset property (v5.0 documentation, "预置属性与系统字段").
///
/// All preset properties start with `#` and are event properties generated or
/// obtained by TE. Any `#`-prefixed property outside this list is considered
/// illegal by the receiver and will not be stored.
pub const PRESET_PROPERTIES: [&str; 48] = [
    "#ip",
    "#country",
    "#country_code",
    "#province",
    "#city",
    "#os_version",
    "#manufacturer",
    "#os",
    "#device_id",
    "#screen_height",
    "#screen_width",
    "#device_model",
    "#device_type",
    "#app_version",
    "#bundle_id",
    "#lib",
    "#lib_version",
    "#network_type",
    "#carrier",
    "#browser",
    "#browser_version",
    "#duration",
    "#url",
    "#url_path",
    "#referrer",
    "#referrer_host",
    "#title",
    "#screen_name",
    "#element_id",
    "#element_type",
    "#resume_from_background",
    "#element_selector",
    "#element_position",
    "#element_content",
    "#scene",
    "#mp_platform",
    "#app_crashed_reason",
    "#zone_offset",
    "#system_language",
    "#install_time",
    "#simulator",
    "#ram",
    "#disk",
    "#fps",
    "#background_duration",
    "#start_reason",
    "#ua",
    "#utm",
];

/// Whether `key` is one of the TE preset properties documented for v5.0.
pub fn is_preset_property(key: &str) -> bool {
    PRESET_PROPERTIES.contains(&key)
}

/// The kind of data reported to the receiver.
///
/// The string forms are the wire values expected by ThinkingData; note the
/// historical `user_setOnce` casing, which is preserved on purpose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    /// Ordinary event.
    Track,
    /// Updatable event.
    TrackUpdate,
    /// Overridable event.
    TrackOverwrite,
    /// Overwrite user properties.
    UserSet,
    /// Clear user properties.
    UserUnset,
    /// Set user properties only once.
    UserSetOnce,
    /// Accumulate numeric user properties.
    UserAdd,
    /// Append to array user properties.
    UserAppend,
    /// Append unique values to array user properties.
    UserUniqAppend,
    /// Delete a user; irreversible.
    UserDel,
}

impl DataType {
    /// The wire representation of this data type.
    pub const fn as_str(self) -> &'static str {
        match self {
            DataType::Track => "track",
            DataType::TrackUpdate => "track_update",
            DataType::TrackOverwrite => "track_overwrite",
            DataType::UserSet => "user_set",
            DataType::UserUnset => "user_unset",
            DataType::UserSetOnce => "user_setOnce",
            DataType::UserAdd => "user_add",
            DataType::UserAppend => "user_append",
            DataType::UserUniqAppend => "user_uniq_append",
            DataType::UserDel => "user_del",
        }
    }

    /// True when the data type is one of `track`, `track_update`, `track_overwrite`.
    pub const fn is_track(self) -> bool {
        matches!(
            self,
            DataType::Track | DataType::TrackUpdate | DataType::TrackOverwrite
        )
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for DataType {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

/// Conversion into the property map carried by [`Data`].
///
/// Implemented for `serde_json::Map`, `HashMap`, `BTreeMap` and
/// `serde_json::Value`, so callers can pass their favorite container —
/// including `serde_json::json!({...})` literals, which must be JSON objects:
///
/// ```
/// # fn main() -> Result<(), thinkingdata_core::TDError> {
/// # #[cfg(feature = "blocking")]
/// # {
/// # use std::collections::HashMap;
/// # use thinkingdata_core::{TDAnalytics, consumer::LogConsumer};
/// # let dir = std::env::temp_dir().join("thinkingdata-core-doctest-props");
/// # let consumer = LogConsumer::new(dir.to_str().unwrap())?;
/// # let ta = TDAnalytics::new(consumer)?;
///
/// # // A HashMap of serde_json values…
/// # let mut properties = HashMap::new();
/// # properties.insert("product_name".to_string(), "Rust SDK".into());
/// # properties.insert("price".to_string(), 99.0.into());
/// # ta.track("account_id_123", "purchase", properties)?;
///
/// # // …or a json! literal.
/// # ta.user_set("account_id_123", serde_json::json!({ "level": 10 }))?;
///
/// # ta.close()?;
/// # }
/// # Ok(())
/// # }
/// ```
pub trait IntoPropertyMap {
    /// Convert the value into a `serde_json` property map.
    ///
    /// Fails when a [`Value`] argument is not a JSON object.
    fn into_property_map(self) -> Result<Map<String, Value>>;
}

impl IntoPropertyMap for Map<String, Value> {
    fn into_property_map(self) -> Result<Map<String, Value>> {
        Ok(self)
    }
}

impl IntoPropertyMap for HashMap<String, Value> {
    fn into_property_map(self) -> Result<Map<String, Value>> {
        Ok(self.into_iter().collect())
    }
}

impl IntoPropertyMap for BTreeMap<String, Value> {
    fn into_property_map(self) -> Result<Map<String, Value>> {
        Ok(self.into_iter().collect())
    }
}

impl IntoPropertyMap for Value {
    fn into_property_map(self) -> Result<Map<String, Value>> {
        match self {
            Value::Object(map) => Ok(map),
            _ => Err(TDError::msg(
                "properties must be a JSON object of string keys",
            )),
        }
    }
}

/// One data point in the ThinkingData wire format.
///
/// Fields whose name starts with `#` are preset properties recognized by the
/// receiver; they are serialized with `omitempty` semantics, except `#type`,
/// `#time` and `properties` which are always present.
#[derive(Debug, Clone, Serialize)]
pub struct Data {
    /// Account ID of the user (`#account_id`).
    #[serde(rename = "#account_id", skip_serializing_if = "String::is_empty")]
    pub account_id: String,
    /// Anonymous/distinct ID of the user (`#distinct_id`).
    #[serde(rename = "#distinct_id", skip_serializing_if = "String::is_empty")]
    pub distinct_id: String,
    /// Data type (`#type`), always serialized.
    #[serde(rename = "#type")]
    pub data_type: DataType,
    /// Event time, `yyyy-MM-dd HH:mm:ss.SSS` (`#time`), always serialized.
    #[serde(rename = "#time")]
    pub time: String,
    /// Event name (`#event_name`).
    #[serde(rename = "#event_name", skip_serializing_if = "String::is_empty")]
    pub event_name: String,
    /// Event ID for updatable/overridable events (`#event_id`).
    #[serde(rename = "#event_id", skip_serializing_if = "String::is_empty")]
    pub event_id: String,
    /// First-check ID for first events (`#first_check_id`).
    #[serde(rename = "#first_check_id", skip_serializing_if = "String::is_empty")]
    pub first_check_id: String,
    /// IP address used for geolocation (`#ip`).
    #[serde(rename = "#ip", skip_serializing_if = "String::is_empty")]
    pub ip: String,
    /// Unique ID that identifies this data point (`#uuid`).
    #[serde(rename = "#uuid", skip_serializing_if = "String::is_empty")]
    pub uuid: String,
    /// App ID override (`#app_id`).
    #[serde(rename = "#app_id", skip_serializing_if = "String::is_empty")]
    pub app_id: String,
    /// Transaction properties for `track_update`/`track_overwrite`
    /// (`#transaction_property`).
    #[serde(
        rename = "#transaction_property",
        skip_serializing_if = "String::is_empty"
    )]
    pub transaction_property: String,
    /// Identifier of the import tool (`#import_tool_id`).
    #[serde(rename = "#import_tool_id", skip_serializing_if = "String::is_empty")]
    pub import_tool_id: String,
    /// Custom event/user properties, always serialized.
    pub properties: Map<String, Value>,
}

impl Data {
    /// Create an empty [`Data`] for `data_type`, stamping `#time` with the
    /// current local time.
    pub fn new(data_type: DataType) -> Self {
        Self {
            account_id: String::new(),
            distinct_id: String::new(),
            data_type,
            time: format_now(),
            event_name: String::new(),
            event_id: String::new(),
            first_check_id: String::new(),
            ip: String::new(),
            uuid: String::new(),
            app_id: String::new(),
            transaction_property: String::new(),
            import_tool_id: String::new(),
            properties: Map::new(),
        }
    }

    /// Validate this snapshot against the limits documented in the TE v5.0
    /// data rules:
    ///
    /// - `#time` must be `yyyy-MM-dd HH:mm:ss` or `yyyy-MM-dd HH:mm:ss.SSS`
    /// - `#account_id` / `#distinct_id` are limited to 128 characters
    /// - property values must respect the documented type limits (string
    ///   ≤ 2 KB, number within ±9E15, list ≤ 500 elements with string elements
    ///   ≤ 255 bytes, object ≤ 100 sub-properties)
    ///
    /// The debug consumer runs this check in stringent mode; other consumers
    /// leave it to the receiver, which silently drops mismatched values. Call
    /// it yourself for pre-flight validation.
    pub fn check_limits(&self) -> Result<()> {
        if !crate::utils::is_documented_time_format(&self.time) {
            return Err(TDError::msg(format!(
                "invalid #time {:?}: expected \"yyyy-MM-dd HH:mm:ss\" or \"yyyy-MM-dd HH:mm:ss.SSS\"",
                self.time
            )));
        }
        const MAX_ID_LENGTH: usize = 128;
        if self.account_id.len() > MAX_ID_LENGTH {
            return Err(TDError::msg(format!(
                "#account_id exceeds {MAX_ID_LENGTH} characters"
            )));
        }
        if self.distinct_id.len() > MAX_ID_LENGTH {
            return Err(TDError::msg(format!(
                "#distinct_id exceeds {MAX_ID_LENGTH} characters"
            )));
        }
        for (key, value) in &self.properties {
            crate::utils::check_value_limits(key, value)?;
        }
        Ok(())
    }
}

/// Remove `key` from `properties` and return it when it is a string.
///
/// Non-string values are dropped with an error log, matching the Go SDK.
#[cfg(any(feature = "blocking", feature = "async"))]
pub(crate) fn extract_string_property(properties: &mut Map<String, Value>, key: &str) -> String {
    match properties.remove(key) {
        Some(Value::String(value)) => value,
        Some(_) => {
            td_log_error(&format!("Invalid data type for {key}"));
            String::new()
        }
        None => String::new(),
    }
}

/// Remove `#time` from `properties` and interpret it.
///
/// Strings are passed through unchanged; an empty string or any other type
/// falls back to the current local time.
#[cfg(any(feature = "blocking", feature = "async"))]
pub(crate) fn extract_time(properties: &mut Map<String, Value>) -> String {
    match properties.remove("#time") {
        Some(Value::String(value)) if !value.is_empty() => value,
        Some(_) => format_now(),
        None => format_now(),
    }
}

/// Validate the event name and property values of `data`, and flag complex
/// (nested) property values.
///
/// When `stringent` is set (debug consumer), property keys must also match the
/// key pattern and the documented v5.0 value limits are enforced; otherwise
/// invalid keys are passed through, mirroring the Go SDK's lenient consumers.
#[cfg(any(feature = "blocking", feature = "async"))]
pub(crate) fn format_properties(data: &mut Data, stringent: bool) -> Result<()> {
    if !data.event_name.is_empty() && !is_valid_key(&data.event_name) {
        let message = format!("invalid event name: {}", data.event_name);
        crate::log::td_log_info(&message);
        return Err(TDError::InvalidEventName(data.event_name.clone()));
    }

    for (key, value) in &data.properties {
        if stringent && !is_valid_key(key) {
            let message = format!("invalid property key: {key}");
            crate::log::td_log_info(&message);
            return Err(TDError::InvalidPropertyKey(key.clone()));
        }

        if data.data_type == DataType::UserAdd && !value.is_number() {
            crate::log::td_log_info("invalid property value: only numbers is supported by UserAdd");
            return Err(TDError::InvalidUserAddValue);
        }
    }

    if stringent {
        data.check_limits()?;
    }

    Ok(())
}

/// Fill the preset fields of `data` from `properties`, removing the preset
/// keys from the property map, and generate a `#uuid` when none was supplied.
#[cfg(any(feature = "blocking", feature = "async"))]
pub(crate) fn apply_preset_properties(data: &mut Data, properties: &mut Map<String, Value>) {
    // `PRESET_KEYS` documents the hoisted header fields; extraction is
    // key-specific below.
    debug_assert!(PRESET_KEYS.contains(&"#ip"));
    data.ip = extract_string_property(properties, "#ip");
    data.app_id = extract_string_property(properties, "#app_id");
    data.time = extract_time(properties);
    data.first_check_id = extract_string_property(properties, "#first_check_id");
    data.transaction_property = extract_string_property(properties, "#transaction_property");
    data.import_tool_id = extract_string_property(properties, "#import_tool_id");
    let mut uuid = extract_string_property(properties, "#uuid");
    if uuid.is_empty() {
        uuid = generate_uuid();
    }
    data.uuid = uuid;
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn sample_properties() -> Map<String, Value> {
        let mut props = Map::new();
        props.insert("string".to_string(), Value::String("v".to_string()));
        props.insert("number".to_string(), Value::from(1));
        props.insert("bool".to_string(), Value::Bool(true));
        props
    }

    #[test]
    fn data_serializes_wire_field_names() {
        let mut data = Data::new(DataType::Track);
        data.account_id = "acc".to_string();
        data.event_name = "purchase".to_string();
        data.properties = sample_properties();

        let json = serde_json::to_value(&data).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["#account_id"], "acc");
        assert_eq!(obj["#type"], "track");
        assert!(obj.get("#distinct_id").is_none());
        assert!(obj.get("#event_id").is_none());
        assert!(obj["#time"].as_str().unwrap().contains('.'));
        assert!(
            obj["properties"]
                .as_object()
                .unwrap()
                .contains_key("string")
        );
    }

    #[test]
    fn user_del_serializes_empty_properties() {
        let data = Data::new(DataType::UserDel);
        let json = serde_json::to_value(&data).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["#type"], "user_del");
        assert!(obj["properties"].as_object().unwrap().is_empty());
    }

    #[test]
    fn data_type_wire_values() {
        assert_eq!(DataType::Track.as_str(), "track");
        assert_eq!(DataType::UserSetOnce.as_str(), "user_setOnce");
        assert_eq!(DataType::UserUniqAppend.as_str(), "user_uniq_append");
        assert!(DataType::TrackUpdate.is_track());
        assert!(!DataType::UserSet.is_track());
    }

    #[test]
    fn preset_property_catalog_matches_the_documented_list() {
        assert_eq!(PRESET_PROPERTIES.len(), 48);
        assert!(is_preset_property("#lib"));
        assert!(is_preset_property("#utm"));
        assert!(is_preset_property("#country_code"));
        assert!(!is_preset_property("#not_a_preset"));
        assert!(!is_preset_property("custom_property"));
    }

    #[test]
    fn check_limits_accepts_a_well_formed_snapshot() {
        let mut data = Data::new(DataType::Track);
        data.account_id = "acc".to_string();
        data.event_name = "purchase".to_string();
        data.properties = sample_properties();
        assert!(data.check_limits().is_ok());
    }

    #[test]
    fn check_limits_rejects_malformed_time() {
        let mut data = Data::new(DataType::Track);
        data.time = "2024-01-02T03:04:05Z".to_string();
        let err = data.check_limits().unwrap_err();
        assert!(err.to_string().contains("#time"), "{err}");
    }

    #[test]
    fn check_limits_rejects_oversized_ids() {
        let mut data = Data::new(DataType::Track);
        data.account_id = "a".repeat(129);
        let err = data.check_limits().unwrap_err();
        assert!(err.to_string().contains("#account_id"), "{err}");

        let mut data = Data::new(DataType::Track);
        data.distinct_id = "d".repeat(129);
        let err = data.check_limits().unwrap_err();
        assert!(err.to_string().contains("#distinct_id"), "{err}");
    }

    #[test]
    fn check_limits_rejects_values_over_documented_limits() {
        let mut data = Data::new(DataType::Track);
        data.properties
            .insert("big".to_string(), Value::String("x".repeat(2 * 1024 + 1)));
        let err = data.check_limits().unwrap_err();
        assert!(err.to_string().contains("big"), "{err}");
    }
}

// Wire-assembly helpers (`apply_preset_properties`, `format_properties`, …)
// exist only with an API feature, so their tests live behind the same gate.
#[cfg(all(test, any(feature = "blocking", feature = "async")))]
mod wire_tests {
    use super::tests::sample_properties;
    use super::*;

    #[test]
    fn preset_keys_are_hoisted_into_fields() {
        let mut data = Data::new(DataType::Track);
        data.event_name = "login".to_string();
        let mut props = sample_properties();
        props.insert("#ip".to_string(), Value::String("1.2.3.4".to_string()));
        props.insert(
            "#time".to_string(),
            Value::String("2024-01-02 03:04:05.123".to_string()),
        );
        props.insert("#uuid".to_string(), Value::String("fixed-uuid".to_string()));

        apply_preset_properties(&mut data, &mut props);

        assert_eq!(data.ip, "1.2.3.4");
        assert_eq!(data.time, "2024-01-02 03:04:05.123");
        assert_eq!(data.uuid, "fixed-uuid");
        assert!(!props.contains_key("#ip"));
        assert!(!props.contains_key("#time"));
        assert!(!props.contains_key("#uuid"));
    }

    #[test]
    fn missing_uuid_is_generated() {
        let mut data = Data::new(DataType::Track);
        let mut props = sample_properties();
        apply_preset_properties(&mut data, &mut props);
        assert!(!data.uuid.is_empty());
    }

    #[test]
    fn non_string_preset_value_is_dropped() {
        let mut props = Map::new();
        props.insert("#ip".to_string(), Value::from(123));
        let ip = extract_string_property(&mut props, "#ip");
        assert!(ip.is_empty());
        assert!(!props.contains_key("#ip"));
    }

    #[test]
    fn user_add_rejects_non_numeric_values() {
        let mut data = Data::new(DataType::UserAdd);
        data.properties
            .insert("level".to_string(), Value::String("10".to_string()));
        assert!(matches!(
            format_properties(&mut data, false),
            Err(TDError::InvalidUserAddValue)
        ));
    }

    #[test]
    fn user_add_accepts_numeric_values() {
        let mut data = Data::new(DataType::UserAdd);
        data.properties.insert("level".to_string(), Value::from(10));
        assert!(format_properties(&mut data, false).is_ok());
    }

    #[test]
    fn stringent_mode_rejects_invalid_keys() {
        let mut data = Data::new(DataType::Track);
        data.event_name = "purchase".to_string();
        data.properties
            .insert("bad key".to_string(), Value::from(1));
        assert!(matches!(
            format_properties(&mut data, true),
            Err(TDError::InvalidPropertyKey(_))
        ));

        let mut lenient = Data::new(DataType::Track);
        lenient.event_name = "purchase".to_string();
        lenient
            .properties
            .insert("bad key".to_string(), Value::from(1));
        assert!(format_properties(&mut lenient, false).is_ok());
    }

    #[test]
    fn invalid_event_name_is_rejected() {
        let mut data = Data::new(DataType::Track);
        data.event_name = "bad event".to_string();
        assert!(matches!(
            format_properties(&mut data, false),
            Err(TDError::InvalidEventName(_))
        ));
    }

    #[test]
    fn extract_time_falls_back_to_now_for_unsupported_types() {
        let mut props = Map::new();
        props.insert("#time".to_string(), Value::from(123));
        let time = extract_time(&mut props);
        assert_eq!(time.len(), 23);
    }

    #[test]
    fn stringent_format_properties_enforces_value_limits() {
        let mut data = Data::new(DataType::Track);
        data.event_name = "purchase".to_string();
        data.properties
            .insert("big".to_string(), Value::String("x".repeat(2 * 1024 + 1)));
        assert!(format_properties(&mut data, false).is_ok());
        assert!(format_properties(&mut data, true).is_err());
    }
}
