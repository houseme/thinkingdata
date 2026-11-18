# Changelog

All notable changes to the `thinkingdata` workspace (crates `thinkingdata`
and `thinkingdata-core`) are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the workspace
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-11-19

### Fixed

- `thinkingdata-core` did not compile with crate-level feature combinations:
  `--no-default-features` (missing `Runtime`) and
  `--no-default-features --features async` (missing `Arc`/`Runtime`). The
  analytics and consumer modules are now compiled out entirely when no API
  feature is enabled (keeping the data model, validation and logging), and the
  workspace CI matrix gained dedicated crate-level jobs and clippy gates so
  the gap cannot reopen.
- RFC 3339 timestamps without fractional seconds (`2024-01-02T03:04:05Z`) are
  now rewritten to `yyyy-MM-dd HH:mm:ss.SSS` like every other form: `parse_time`
  pads the fraction to `.000`, pads sub-100ms fractions to three digits, and
  truncates longer fractions. This makes the CLI `--time` option reliable for
  RFC 3339 input.
- `thinkingdata --keys` trims whitespace around comma-separated keys (and skips
  empty items), so `--keys level, vip` no longer sends keys the receiver
  silently drops.
- Concurrent `close` calls on `LogConsumer`/`BatchConsumer` now all report the
  retained close result instead of a transient writer/sender error.
- The per-crate `README.md` packaging of 0.1.1 is restored: both crates resolve
  `readme = "README.md"` relative to their own directory again, so crates.io
  pages show the crate-specific documentation instead of the workspace root
  README. The duplicated `## [Unreleased]` heading was removed.
- The TLS supply-chain note now matches the lockfile: rustls backed by
  aws-lc-rs is the only TLS backend, while `ring` and `openssl-probe` remain
  present as unused transitive dependencies (via quinn / rustls-native-certs).

### Changed

- Removed the `flush` and `close` CLI subcommands: every invocation builds a
  fresh consumer, so they had no observable effect. Every command now flushes
  and closes the consumer before exiting.
- Batch failure-cache eviction is logged (`cache is full, discard N batch(es)`)
  instead of dropping the oldest batches silently.
- `LogConsumer` moves each event line into the blocking writer task instead of
  cloning it; the analytics facades consume the per-event super-property
  snapshots by value; the batch consumer gzip-encodes the request body once
  and reuses it across retry attempts.

## [0.1.1] - 2026-11-17

### Fixed

- Invalid dynamic super property callbacks (a non-object value) are now
  reported through a warning log instead of being silently dropped on every
  event.

### Added

- Per-crate `README.md` for both workspace members with `readme = "README.md"`
  manifest entries, so the crates.io pages render crate-specific documentation
  (the facade with installation and CLI usage; the core with its module map
  and validation helpers).
- `repository` metadata declared for both crates.

### Changed

- README badges point at the published `thinkingdata` crate, docs.rs and the
  CI workflow; SDK examples restored to the canonical `)?;` form.


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
