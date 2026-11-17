# Data Format & Rules (TE v5.0)

English | [中文](data-format.zh-CN.md)

This document describes the wire format the ThinkingData (TE) receiver expects, and how the
`thinkingdata` Rust SDK produces it. It summarizes the official
[data rules](https://docs-v2.thinkingdata.cn/?code=data_format&lan=zh-CN&version=5.0) for TE 5.0 and
maps every rule onto the Rust API.

## 1. Structure: one JSON object per line

The receiver accepts NDJSON: **one JSON object per line**, where each line is either one user
behavior (an event) or one user property operation.

```json
{
  "#account_id": "ABCDEFG-123-abc",
  "#distinct_id": "F53A58ED-E5DA-4F18-B082-7E1228746E88",
  "#type": "track",
  "#ip": "192.168.171.111",
  "#uuid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "#time": "2017-12-18 14:37:28.527",
  "#event_name": "test",
  "properties": {
    "argString": "abc",
    "argNum": 123,
    "argBool": true
  }
}
```

A user property operation has the same shape with `#type` replaced by `user_set`, `user_setOnce`,
`user_add`, `user_unset`, `user_append`, `user_uniq_append` or `user_del`.

Two parts per object:

1. **Header** — the fields at the same level as `properties` (all starting with `#`).
2. **Body** — the `properties` object: event properties or the user properties to set.

## 2. Header fields

| Field | Meaning | Required | Notes |
|---|---|---|---|
| `#account_id` | Account ID (logged-in identity) | at least one of `#account_id` / `#distinct_id` | ≤ 128 characters |
| `#distinct_id` | Distinct ID (anonymous identity) | | ≤ 128 characters |
| `#type` | Data type | yes | see the table below |
| `#event_name` | Event name | required when `#type` = `track` | letter start, lowercase letters / digits / `_`, ≤ 50 chars |
| `#time` | Trigger time | yes | `yyyy-MM-dd HH:mm:ss` or `yyyy-MM-dd HH:mm:ss.SSS` |
| `#ip` | Trigger location | no | TE derives geo information; values you pass for `#country`/`#province`/`#city` win |
| `#uuid` | Data uniqueness ID | no | standard UUID format; receiver deduplicates recent hours by it |

The Rust SDK additionally hoists four official-SDK header fields out of `properties` when you pass
them as properties (matching the Go SDK wire behavior):

| Field | Meaning |
|---|---|
| `#app_id` | Project ID override for this event |
| `#first_check_id` | Deduplication key for first events (`track_first`) |
| `#transaction_property` | Transaction properties for updatable/overridable events |
| `#import_tool_id` | Identifier of an importing tool |

Anything else starting with `#` must stay inside `properties`, and only the
[preset properties](preset-properties.md) are legal there — unknown `#` keys are rejected by the
receiver.

### How the SDK fills the header

- `#time` — stamped with the current local time (`jiff`), unless you pass `#time` in the
  properties (string values pass through unchanged; any other type falls back to now).
- `#uuid` — a random UUID v4, unless you pass `#uuid` yourself.
- `#ip`, `#app_id`, `#first_check_id`, `#transaction_property`, `#import_tool_id` — hoisted from
  the properties you pass and removed from the body.
- RFC 3339 timestamps embedded anywhere in a serialized payload (e.g. nested objects) are
  rewritten to `yyyy-MM-dd HH:mm:ss.SSS` before submission, like the Go SDK's `parseTime`.

## 3. Data types

| `#type` | Operation | Rust API (blocking / async identical names) |
|---|---|---|
| `track` | Insert one event | `track`, `track_first` (sets `#first_check_id`), `track_update` (`#event_id`), `track_overwrite` (`#event_id`) |
| `user_set` | Overwrite user properties (create if missing) | `user_set` |
| `user_setOnce` | Initialize user properties; existing values are kept | `user_set_once` |
| `user_add` | Accumulate numeric user properties (missing treated as 0; negative values subtract) | `user_add` — values must be numbers, enforced by the SDK |
| `user_unset` | Clear listed user properties (set to NULL; missing keys are not created) | `user_unset` (keys map to `0` on the wire), `user_unset_with_properties` |
| `user_append` | Append elements to list-typed user properties | `user_append` |
| `user_uniq_append` | Append and de-duplicate the whole list (original order preserved) | `user_uniq_append` |
| `user_del` | Delete the user (events are kept) | `user_del`, `user_del_with_properties` |

Every variant has a `*_with_distinct_id` form taking `(account_id, distinct_id, …)` for full
control over both identities.

User property operations are applied in **arrival order**, not by `#time`; re-sending old
`user_set` data still overwrites. User operations do not create behavior rows, so they are not
visible in behavior analysis models (except SQL queries).

## 4. Property value types & limits

The type of a property is fixed by the **first** received value; later values of a different type
are dropped (the rest of the event is kept) — TE does not coerce types.

| TE type | Example | Limits | JSON representation |
|---|---|---|---|
| Number | `123`, `1.23` | −9E15 … 9E15 | number |
| String | `"ABC"`, `"上海"` | ≤ 2 KB | string |
| Time | `"2019-01-01 00:00:00.000"` | `yyyy-MM-dd HH:mm:ss[.SSS]`; dates as `yyyy-MM-dd 00:00:00` | string |
| Boolean | `true`, `false` | — | bool |
| List | `["a","1","true"]` | ≤ 500 elements; elements are converted to strings, ≤ 255 bytes each | array |
| Object | `{"hero_name":"刘备","hero_level":22}` | ≤ 100 sub-properties; each sub-property follows the rules above | object |
| Object group | `[{...},{...}]` | ≤ 500 objects; each object ≤ 100 sub-properties | array of objects |

### Pre-flight validation in the SDK

```rust
use thinkingdata_core::utils::{check_value_limits, is_documented_time_format};
use thinkingdata_core::PRESET_PROPERTIES;

// Opt-in check of one value against the documented limits (2 KB strings,
// ±9E15 numbers, 500-element lists, 255-byte list elements, 100-key objects).
check_value_limits("price", &serde_json::json!(9.9))?;

// Or validate a whole snapshot: #time format, ID length (≤128) and every value.
data.check_limits()?;
```

The debug consumer runs `check_limits` automatically (it is the *stringent* consumer), so invalid
keys and over-limit values fail fast during development instead of being silently dropped by the
receiver. The batch and log consumers leave validation to the receiver, preserving the official
SDKs' behavior.

## 5. Naming rules

- **Event names**: letter start; lowercase letters, digits and `_`; ≤ 50 characters; no spaces.
  (The SDK key pattern follows the Go SDK and also accepts uppercase; TE property names are
  case-insensitive, so `Purchase` and `purchase` collide.)
- **Property keys**: same rule; only preset properties may start with `#`.
- `#account_id` / `#distinct_id`: ≤ 128 characters (projects created in TE ≥ 3.1).
- Encoding: always UTF-8.

## 6. Volume & time limits

| Limit | Soft (recommended) | Hard |
|---|---|---|
| Event types per project | 100 | 500 |
| Event properties per project | 300 | 1000 |
| User properties per project | 100 | 500 |

- Server-side events are accepted within `[server time − 3 years, + 3 days]` (client-side:
  `− 10 days`); older data is discarded.
- The receiver deduplicates `#uuid` values seen within the last few hours (network-jitter
  protection, not full-history deduplication).

## 7. Common pitfalls

1. **No data arrives** — check APP ID, receiver URL and the `/sync_server` suffix; with LogBus,
   ensure one JSON object per line and that header keys start with `#`.
2. **Properties missing** — the property type at first submission wins; a later value of another
   type is dropped. Check the metadata page in the TE console.
3. **`#`-prefixed custom keys disappear** — only [preset properties](preset-properties.md) may
   start with `#`; anything else is rejected.
4. **User operations are invisible in behavior models** — they only modify the user table.
5. **Data older than three years** is not ingested.

## 8. Receiver endpoints used by this SDK

| Consumer | Endpoint | Payload |
|---|---|---|
| `BatchConsumer` | `{server_url}/sync_server` | JSON array of events; gzip-compressed body by default; headers `appid`, `compress`, `TA-Integration-*`; response `{"code": 0}` (`-1` invalid data, `-2` unknown APP ID, `-3` invalid IP); non-200 retried up to 3 times; failed batches cached |
| `DebugConsumer` | `{server_url}/data_debug` | form-encoded `data`, `appid`, `source=server`, `dryRun`, optional `deviceId`; response `{"errorLevel": n}` |
| `LogConsumer` | local NDJSON files | consumed later via LogBus |

## 9. References

- Official data rules (v5.0): <https://docs-v2.thinkingdata.cn/?code=data_format&lan=zh-CN&version=5.0>
- Official preset properties (v5.0): <https://docs-v2.thinkingdata.cn/?code=preset_properties&lan=zh-CN&version=5.0>
- Go SDK (wire-behavior reference): <https://github.com/ThinkingDataAnalytics/go-sdk>
