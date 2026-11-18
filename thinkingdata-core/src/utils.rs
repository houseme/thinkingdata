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

//! Time and validation helpers shared by the analytics facade and consumers.
//!
//! All timestamps are produced with the [`jiff`] crate (the successor of
//! chrono, which is deliberately not used anywhere in this workspace).

use std::sync::OnceLock;

use regex::Regex;

use crate::error::Result;

/// Time-of-day pattern for the `#time` wire format; milliseconds are appended
/// separately so the output is exactly `yyyy-MM-dd HH:mm:ss.SSS` (local time).
const CLOCK_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// The timestamp rewrite applied to serialized payloads, ported from the Go
/// SDK: RFC 3339 timestamps embedded anywhere in a JSON payload are rewritten
/// to the `yyyy-MM-dd HH:mm:ss.SSS` form the receiver expects.
const TIME_PATTERN: &str =
    r#""((\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2})(?:\.(\d{3}))\d*)(Z|[+\-]\d{2}:\d{2})""#;

fn time_regex() -> &'static Regex {
    static TIME_RE: OnceLock<Regex> = OnceLock::new();
    TIME_RE.get_or_init(|| Regex::new(TIME_PATTERN).expect("valid timestamp regex"))
}

/// Check that an event name or property key matches
/// `^[a-zA-Z#][A-Za-z0-9_]{0,49}$` — a 1..=50 character identifier starting
/// with a letter or `#`.
///
/// ```
/// use thinkingdata_core::utils::is_valid_key;
///
/// assert!(is_valid_key("product_name"));
/// assert!(is_valid_key("#lib"));
/// assert!(!is_valid_key("9invalid"));
/// ```
pub fn is_valid_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        Some(first) if first.is_ascii_alphabetic() || first == '#' => {}
        _ => return false,
    }
    let mut length = 1usize;
    for ch in chars {
        if !(ch.is_ascii_alphanumeric() || ch == '_') {
            return false;
        }
        length += 1;
        if length > 50 {
            return false;
        }
    }
    true
}

/// Rewrite RFC 3339 timestamps inside a serialized JSON payload to
/// `"yyyy-MM-dd HH:mm:ss.SSS"`, mirroring the Go SDK's `parseTime`.
///
/// ```
/// use thinkingdata_core::utils::parse_time;
///
/// let payload = r##"{"#time":"2024-01-02T03:04:05.123456+08:00"}"##;
/// assert!(parse_time(payload).contains(r#""2024-01-02 03:04:05.123""#));
/// ```
pub fn parse_time(payload: &str) -> String {
    let re = time_regex();
    if !re.is_match(payload) {
        return payload.to_string();
    }
    let mut current = payload.to_string();
    loop {
        let next = re.replace_all(&current, "\"$2 $3.$4\"").into_owned();
        if next == current {
            return next;
        }
        current = next;
    }
}

/// Current local time formatted as `yyyy-MM-dd HH:mm:ss.SSS`.
pub(crate) fn format_now() -> String {
    let zoned = jiff::ZonedDateTime::now();
    let millis = zoned.timestamp().subsec_nanosecond() / 1_000_000;
    format!("{}.{millis:03}", zoned.strftime(CLOCK_FORMAT))
}

/// Current local time formatted with an arbitrary jiff `strftime` pattern.
///
/// Only the (feature-gated) log consumer rotates by period, so this helper
/// follows the same feature gate.
#[cfg(any(feature = "blocking", feature = "async"))]
pub(crate) fn format_with(pattern: &str) -> String {
    jiff::ZonedDateTime::now().strftime(pattern).to_string()
}

/// Generate a random (v4) UUID string for the `#uuid` field.
#[cfg(any(feature = "blocking", feature = "async"))]
pub(crate) fn generate_uuid() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Documented property limits from the TE v5.0 data rules.
pub mod limits {
    /// Maximum byte length of a string property value (2 KB).
    pub const MAX_STRING_BYTES: usize = 2 * 1024;
    /// Maximum absolute value of a numeric property (±9E15).
    pub const MAX_NUMBER_ABS: f64 = 9e15;
    /// Maximum number of elements in a list or object-group property (500).
    pub const MAX_ARRAY_ELEMENTS: usize = 500;
    /// Maximum byte length of a list element after string conversion (255).
    pub const MAX_ARRAY_ELEMENT_BYTES: usize = 255;
    /// Maximum number of sub-properties inside an object property (100).
    pub const MAX_OBJECT_KEYS: usize = 100;
    /// Maximum length of `#account_id` / `#distinct_id` (128 characters).
    pub const MAX_ID_LENGTH: usize = 128;
}

fn time_format_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{3})?$").expect("valid time regex")
    })
}

/// Whether `value` matches the `#time` formats documented in the data rules:
/// `yyyy-MM-dd HH:mm:ss` or `yyyy-MM-dd HH:mm:ss.SSS`.
///
/// ```
/// use thinkingdata_core::utils::is_documented_time_format;
///
/// assert!(is_documented_time_format("2026-11-17 09:30:00"));
/// assert!(is_documented_time_format("2026-11-17 09:30:00.123"));
/// assert!(!is_documented_time_format("2026-11-17T09:30:00Z"));
/// ```
pub fn is_documented_time_format(value: &str) -> bool {
    time_format_regex().is_match(value)
}

/// Validate one property value against the documented v5.0 type limits.
///
/// The receiver silently drops values that break these limits (keeping the
/// rest of the event), so this check is most useful before submitting data:
/// the debug consumer runs it in stringent mode via
/// [`Data::check_limits`](crate::data::Data::check_limits).
///
/// Rules enforced:
///
/// - string ≤ [`limits::MAX_STRING_BYTES`] (2 KB)
/// - number within ±[`limits::MAX_NUMBER_ABS`] (9E15)
/// - list / object group ≤ [`limits::MAX_ARRAY_ELEMENTS`] (500 elements);
///   string elements ≤ [`limits::MAX_ARRAY_ELEMENT_BYTES`] (255 bytes after
///   the receiver's string conversion); nested objects are validated too
/// - object ≤ [`limits::MAX_OBJECT_KEYS`] (100 sub-properties), validated
///   recursively
///
/// `key` is used to build the error message.
pub fn check_value_limits(key: &str, value: &serde_json::Value) -> Result<()> {
    use limits::{
        MAX_ARRAY_ELEMENT_BYTES, MAX_ARRAY_ELEMENTS, MAX_NUMBER_ABS, MAX_OBJECT_KEYS,
        MAX_STRING_BYTES,
    };
    use serde_json::Value;

    let over_limit = |detail: String| {
        Err(crate::error::TDError::msg(format!(
            "property {key}: {detail}"
        )))
    };

    match value {
        Value::String(text) => {
            if text.len() > MAX_STRING_BYTES {
                return over_limit(format!("string value exceeds {MAX_STRING_BYTES} bytes"));
            }
        }
        Value::Number(number) => {
            let magnitude = number.as_f64().unwrap_or(f64::INFINITY).abs();
            if magnitude > MAX_NUMBER_ABS {
                return over_limit(format!("number value exceeds ±{MAX_NUMBER_ABS}"));
            }
        }
        Value::Array(items) => {
            if items.len() > MAX_ARRAY_ELEMENTS {
                return over_limit(format!(
                    "list has {} elements, limit is {MAX_ARRAY_ELEMENTS}",
                    items.len()
                ));
            }
            for item in items {
                match item {
                    Value::String(text) => {
                        if text.len() > MAX_ARRAY_ELEMENT_BYTES {
                            return over_limit(format!(
                                "list element exceeds {MAX_ARRAY_ELEMENT_BYTES} bytes"
                            ));
                        }
                    }
                    Value::Object(_) => check_value_limits(key, item)?,
                    _ => {}
                }
            }
        }
        Value::Object(entries) => {
            if entries.len() > MAX_OBJECT_KEYS {
                return over_limit(format!(
                    "object has {} sub-properties, limit is {MAX_OBJECT_KEYS}",
                    entries.len()
                ));
            }
            for (sub_key, sub_value) in entries {
                check_value_limits(&format!("{key}.{sub_key}"), sub_value)?;
            }
        }
        Value::Null | Value::Bool(_) => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_validation_matches_go_pattern() {
        assert!(is_valid_key("a"));
        assert!(is_valid_key("#lib"));
        assert!(is_valid_key("product_name"));
        assert!(is_valid_key("A9_"));
        assert!(!is_valid_key(""));
        assert!(!is_valid_key("9abc"));
        assert!(!is_valid_key("_abc"));
        assert!(!is_valid_key("has space"));
        assert!(!is_valid_key("has-dash"));
        assert!(!is_valid_key("中文"));
        assert!(!is_valid_key(&"a".repeat(51)));
        assert!(is_valid_key(&"a".repeat(50)));
    }

    #[test]
    fn parse_time_rewrites_rfc3339_timestamps() {
        let input = r##"{"#time":"2024-01-02T03:04:05.123456+08:00","x":1}"##;
        let output = parse_time(input);
        assert!(output.contains(r#""2024-01-02 03:04:05.123""#), "{output}");

        // No fractional seconds: the pattern requires `.ddd`, so it is left alone.
        let no_millis = r##"{"#time":"2024-01-02T03:04:05Z"}"##;
        assert_eq!(parse_time(no_millis), no_millis);

        // Plain payloads pass through untouched.
        assert_eq!(parse_time("hello"), "hello");
    }

    #[test]
    fn format_now_matches_wire_format() {
        let ts = format_now();
        // yyyy-MM-dd HH:mm:ss.SSS => 23 characters
        assert_eq!(ts.len(), 23, "{ts}");
        assert_eq!(ts.as_bytes()[10], b' ');
        assert_eq!(ts.as_bytes()[19], b'.');
    }

    #[test]
    fn documented_time_format_detection() {
        assert!(is_documented_time_format("2026-11-17 09:30:00"));
        assert!(is_documented_time_format("2026-11-17 09:30:00.123"));
        assert!(!is_documented_time_format("2026-11-17T09:30:00Z"));
        assert!(!is_documented_time_format("2026-11-17"));
        assert!(!is_documented_time_format("2026-11-17 09:30:00.123456"));
    }

    #[test]
    fn value_limits_follow_the_documented_rules() {
        use serde_json::json;

        // Strings over 2 KB are rejected.
        let err = check_value_limits("text", &json!("x".repeat(2 * 1024 + 1))).unwrap_err();
        assert!(err.to_string().contains("2048 bytes"), "{err}");
        assert!(check_value_limits("text", &json!("x".repeat(2 * 1024))).is_ok());

        // Numbers beyond ±9E15 are rejected.
        let err = check_value_limits("num", &json!(9.1e15)).unwrap_err();
        assert!(
            err.to_string().contains("9E15") || err.to_string().contains("9000000000000000"),
            "{err}"
        );
        assert!(check_value_limits("num", &json!(9e15)).is_ok());

        // Lists are limited to 500 elements, string elements to 255 bytes.
        let big: Vec<_> = (0..501).map(|i| json!(i)).collect();
        let err = check_value_limits("list", &json!(big)).unwrap_err();
        assert!(err.to_string().contains("500"), "{err}");
        let err = check_value_limits("list", &json!(["x".repeat(256)])).unwrap_err();
        assert!(err.to_string().contains("255 bytes"), "{err}");
        assert!(check_value_limits("list", &json!(["a", "b"])).is_ok());

        // Objects are limited to 100 sub-properties, validated recursively.
        let mut object = serde_json::Map::new();
        for i in 0..101 {
            object.insert(format!("k{i}"), json!(i));
        }
        let err = check_value_limits("obj", &serde_json::Value::Object(object)).unwrap_err();
        assert!(err.to_string().contains("100"), "{err}");
        let err = check_value_limits(
            "obj",
            &json!({ "nested": { "deep": "x".repeat(2 * 1024 + 1) } }),
        )
        .unwrap_err();
        assert!(err.to_string().contains("obj.nested.deep"), "{err}");

        // Nulls and booleans always pass.
        assert!(check_value_limits("ok", &serde_json::Value::Null).is_ok());
        assert!(check_value_limits("ok", &json!(true)).is_ok());
    }
}
