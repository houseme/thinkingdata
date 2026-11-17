# Changelog

All notable changes to the `thinkingdata` workspace (crates `thinkingdata`
and `thinkingdata-core`) are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the workspace
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-17

First release. A server-side Rust SDK for [ThinkingData Analytics](https://www.thinkingdata.cn)
ported from the official [Go SDK](https://github.com/ThinkingDataAnalytics/go-sdk)
(cross-checked against the Lua SDK), plus a CLI of the same name.

### Added

- **Wire-format data model** (`thinkingdata-core::data`) — `Data` with the `#`-prefixed header
  fields (`#account_id`, `#distinct_id`, `#type`, `#time`, `#event_name`, `#event_id`,
  `#first_check_id`, `#ip`, `#uuid`, `#app_id`, `#transaction_property`, `#import_tool_id`),
  `DataType` wire values (`track`, `track_update`, `track_overwrite`, `user_set`, `user_unset`,
  `user_setOnce`, `user_add`, `user_append`, `user_uniq_append`, `user_del`), preset-key hoisting,
  UUID v4 generation, and RFC 3339 → `yyyy-MM-dd HH:mm:ss.SSS` timestamp rewriting.
- **Analytics facades** — `TDAnalytics` (blocking, feature `blocking`) and `TDAnalyticsAsync`
  (async, feature `async`): `track` / `track_first` / `track_update` / `track_overwrite`, the
  seven user-property operations, static and dynamic super properties, `flush` / `close`; every
  operation also has a `*_with_distinct_id` variant.
- **Consumers** (`thinkingdata-core::consumer`)
  - `LogConsumer` — NDJSON log files for LogBus, daily/hourly rotation, size-based paging with
    index restore on restart; backed by a streaming MPSC writer task.
  - `BatchConsumer` — buffered HTTPS upload to `/sync_server` with gzip compression, three
    retries on non-200 responses, a bounded failure cache, and an optional auto-flush timer,
    driven by a single streaming sender task.
  - `DebugConsumer` — per-event validation against `/data_debug` (dry-run supported);
    *stringent* mode enforces property keys and the TE v5.0 value limits.
- **TE v5.0 rule support** — key pattern validation (`is_valid_key`), the 48-entry
  `PRESET_PROPERTIES` catalog with `is_preset_property`, documented value limits
  (`check_value_limits`, `Data::check_limits`: strings ≤ 2 KB, numbers within ±9E15, lists
  ≤ 500 elements of ≤ 255 bytes, objects ≤ 100 sub-properties, IDs ≤ 128 characters) and
  documented `#time` formats (`is_documented_time_format`).
- **Diagnostics logging** (`thinkingdata-core::log`) — five log levels, pluggable
  `TDLogger`, panic-contained custom logger callbacks.
- **`thinkingdata` CLI** — `track`, `track-first`, `track-update`, `track-overwrite`,
  `user-set`, `user-set-once`, `user-add`, `user-append`, `user-uniq-append`, `user-unset`,
  `user-del`, `flush`, `close`, reported through the log, batch or debug consumer.
- **Bilingual documentation** (`docs/`) — TE v5.0 data rules and the preset-property/system-field
  catalog in English and Chinese.
- **CI & release workflows** — feature-matrix tests (default / all / no-default / async-only)
  across Linux, macOS and Windows, CLI smoke tests, lint and doc gates, musl cross builds, and a
  tag-driven release pipeline publishing `thinkingdata-core` before the `thinkingdata` facade.

### Notes

- Timestamps are produced with the `jiff` crate; `chrono` is intentionally not used anywhere.
- TLS uses rustls with the **aws-lc-rs** crypto provider only (reqwest 0.13 defaults); no
  native-tls/openssl backend is supported.
- Dependency versions are centralized in the root `[workspace.dependencies]`, pinned to the
  latest published versions; the workspace uses edition 2024.
