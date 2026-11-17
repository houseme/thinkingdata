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

//! The `thinkingdata` command line interface.
//!
//! One binary, one entry point: the CLI mirrors the SDK API surface
//! (`track`, `track_first`, `user_set`, …) and reports through one of the
//! three consumers, selected with `--log-dir`, `--server-url` (+`--app-id`)
//! or `--debug`.

use std::collections::HashMap;

use clap::{Parser, Subcommand};
use serde_json::{Map, Value};

use crate::consumer::{BatchConsumer, BatchConsumerConfig, DebugConsumer, LogConsumer};
use crate::{TDAnalytics, TDError};

/// The `thinkingdata` command line client.
#[derive(Debug, Parser)]
#[command(
    name = "thinkingdata",
    version,
    about = "ThinkingData Analytics command line client (Rust SDK)",
    long_about = "Send events and user property operations to ThinkingData (TE) via the log, batch or debug consumer.\n\nPick exactly one consumer: --log-dir, --server-url (+ --app-id) or --debug (+ --app-id)."
)]
pub struct Cli {
    /// Write through a local log consumer (NDJSON files, LogBus compatible).
    #[arg(long, value_name = "DIR", group = "consumer")]
    pub log_dir: Option<String>,

    /// Upload through a batch consumer to this receiver URL.
    #[arg(long, value_name = "URL", group = "consumer")]
    pub server_url: Option<String>,

    /// ThinkingData APP ID (required with --server-url and --debug).
    #[arg(long, value_name = "ID", requires = "server_url")]
    pub app_id: Option<String>,

    /// Validate events against the receiver via the debug consumer
    /// (one event per request, requires --server-url and --app-id).
    #[arg(long, requires = "server_url")]
    pub debug: bool,

    /// With --debug: do not archive data on the receiver (dry run).
    #[arg(long, requires = "debug")]
    pub dry_run: bool,

    /// With --debug: device id used to inspect data in the TE console.
    #[arg(long, value_name = "ID", requires = "debug")]
    pub device_id: Option<String>,

    /// Batch size threshold for the batch consumer.
    #[arg(long, default_value_t = crate::consumer::DEFAULT_BATCH_SIZE)]
    pub batch_size: usize,

    /// HTTP timeout in milliseconds for the batch/debug consumers.
    #[arg(long, default_value_t = crate::consumer::DEFAULT_TIMEOUT_MS)]
    pub timeout_ms: u64,

    /// Disable gzip compression for the batch consumer.
    #[arg(long)]
    pub no_compress: bool,

    /// Auto flush interval in seconds for the batch consumer (0 disables).
    #[arg(long, default_value_t = 0)]
    pub auto_flush_interval_secs: u64,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Report an ordinary event.
    Track(TrackArgs),
    /// Report a first-check event (deduplicated by --first-check-id).
    TrackFirst(TrackFirstArgs),
    /// Report an updatable event (updated by --event-id).
    TrackUpdate(TrackUpdateArgs),
    /// Report an overridable event (overwritten by --event-id).
    TrackOverwrite(TrackUpdateArgs),
    /// Overwrite user properties.
    UserSet(UserArgs),
    /// Set user properties only where they were not set before.
    UserSetOnce(UserArgs),
    /// Accumulate numeric user properties.
    UserAdd(UserArgs),
    /// Append values to array-typed user properties.
    UserAppend(UserArgs),
    /// Append unique values to array-typed user properties.
    UserUniqAppend(UserArgs),
    /// Clear the listed user properties (comma separated --keys).
    UserUnset(UserUnsetArgs),
    /// Delete a user; this operation cannot be undone.
    UserDel(UserDelArgs),
    /// Deliver everything currently buffered.
    Flush,
    /// Flush and shut the consumer down.
    Close,
}

/// Common identifier options shared by all reporting commands.
#[derive(Debug, Clone, Parser)]
struct IdArgs {
    /// Account ID of the user.
    #[arg(long, value_name = "ID")]
    account_id: Option<String>,

    /// Anonymous/distinct ID of the user.
    #[arg(long, value_name = "ID")]
    distinct_id: Option<String>,
}

impl IdArgs {
    /// Resolve the (account_id, distinct_id) pair; at least one is required.
    fn resolve(&self) -> Result<(String, String), TDError> {
        let account = self.account_id.clone().unwrap_or_default();
        let distinct = self.distinct_id.clone().unwrap_or_default();
        if account.is_empty() && distinct.is_empty() {
            return Err(TDError::msg(
                "one of --account-id or --distinct-id is required",
            ));
        }
        Ok((account, distinct))
    }
}

/// Arguments of `track`.
#[derive(Debug, Clone, Parser)]
pub struct TrackArgs {
    #[command(flatten)]
    ids: IdArgs,

    /// Event name, e.g. `purchase`.
    #[arg(long, value_name = "NAME")]
    pub event_name: String,

    /// Event properties as a JSON object, e.g. '{"price": 9.9}'.
    #[arg(long, short = 'p', value_name = "JSON")]
    pub properties: Option<String>,

    /// Event time; RFC 3339 or `yyyy-MM-dd HH:mm:ss.SSS` (defaults to now).
    #[arg(long, value_name = "TIME")]
    pub time: Option<String>,
}

/// Arguments of `track-first`.
#[derive(Debug, Clone, Parser)]
pub struct TrackFirstArgs {
    #[command(flatten)]
    ids: IdArgs,

    /// Event name, e.g. `register`.
    #[arg(long, value_name = "NAME")]
    pub event_name: String,

    /// First-check ID used for deduplication.
    #[arg(long, value_name = "ID")]
    pub first_check_id: String,

    /// Event properties as a JSON object.
    #[arg(long, short = 'p', value_name = "JSON")]
    pub properties: Option<String>,

    /// Event time; RFC 3339 or `yyyy-MM-dd HH:mm:ss.SSS` (defaults to now).
    #[arg(long, value_name = "TIME")]
    pub time: Option<String>,
}

/// Arguments of `track-update` and `track-overwrite`.
#[derive(Debug, Clone, Parser)]
pub struct TrackUpdateArgs {
    #[command(flatten)]
    ids: IdArgs,

    /// Event name, e.g. `order_status`.
    #[arg(long, value_name = "NAME")]
    pub event_name: String,

    /// ID of the event to update or overwrite.
    #[arg(long, value_name = "ID")]
    pub event_id: String,

    /// Event properties as a JSON object.
    #[arg(long, short = 'p', value_name = "JSON")]
    pub properties: Option<String>,

    /// Event time; RFC 3339 or `yyyy-MM-dd HH:mm:ss.SSS` (defaults to now).
    #[arg(long, value_name = "TIME")]
    pub time: Option<String>,
}

/// Arguments of the user property commands.
#[derive(Debug, Clone, Parser)]
pub struct UserArgs {
    #[command(flatten)]
    ids: IdArgs,

    /// User properties as a JSON object, e.g. '{"level": 10}'.
    #[arg(long, short = 'p', value_name = "JSON")]
    pub properties: Option<String>,
}

/// Arguments of `user-unset`.
#[derive(Debug, Clone, Parser)]
pub struct UserUnsetArgs {
    #[command(flatten)]
    ids: IdArgs,

    /// Property keys to clear, comma separated.
    #[arg(long, value_name = "KEYS", value_delimiter = ',')]
    pub keys: Vec<String>,
}

/// Arguments of `user-del`.
#[derive(Debug, Clone, Parser)]
pub struct UserDelArgs {
    #[command(flatten)]
    ids: IdArgs,
}

/// Build the analytics instance described by the CLI flags.
fn build_analytics(cli: &Cli) -> Result<TDAnalytics, TDError> {
    if let Some(directory) = &cli.log_dir {
        return TDAnalytics::new(LogConsumer::new(directory)?);
    }
    if let Some(server_url) = &cli.server_url {
        let app_id = cli
            .app_id
            .clone()
            .ok_or_else(|| TDError::msg("--app-id is required with --server-url and --debug"))?;
        if cli.debug {
            return TDAnalytics::new(DebugConsumer::with_device_id(
                server_url,
                &app_id,
                !cli.dry_run,
                cli.device_id.as_deref().unwrap_or_default(),
            )?);
        }
        return TDAnalytics::new(BatchConsumer::with_config(BatchConsumerConfig {
            server_url: server_url.clone(),
            app_id,
            batch_size: cli.batch_size,
            timeout_ms: cli.timeout_ms,
            compress: !cli.no_compress,
            auto_flush: cli.auto_flush_interval_secs > 0,
            interval_secs: cli.auto_flush_interval_secs,
            ..BatchConsumerConfig::default()
        })?);
    }
    Err(TDError::msg(
        "pick exactly one consumer: --log-dir, --server-url or --debug",
    ))
}

/// Parse the `--properties` JSON object (or an empty map when absent).
fn parse_properties(raw: Option<&str>) -> Result<Map<String, Value>, TDError> {
    let Some(raw) = raw else {
        return Ok(Map::new());
    };
    let value: Value = serde_json::from_str(raw).map_err(TDError::from)?;
    match value {
        Value::Object(map) => Ok(map),
        _ => Err(TDError::msg(
            "--properties must be a JSON object, e.g. '{\"price\": 9.9}'",
        )),
    }
}

/// Merge an optional event time into the property map as `#time`.
fn apply_event_time(properties: &mut Map<String, Value>, time: Option<&str>) {
    if let Some(time) = time {
        properties.insert("#time".to_string(), Value::String(time.to_string()));
    }
}

/// Inject `#time` supplied via the CLI into properties (hoisted by the SDK).
fn track_properties(args: &impl TimeBearingArgs) -> Result<Map<String, Value>, TDError> {
    let mut properties = parse_properties(args.properties_raw())?;
    apply_event_time(&mut properties, args.time_raw());
    Ok(properties)
}

/// Helper for reading property/time fields from the different arg structs.
trait TimeBearingArgs {
    fn properties_raw(&self) -> Option<&str>;
    fn time_raw(&self) -> Option<&str>;
}

impl TimeBearingArgs for TrackArgs {
    fn properties_raw(&self) -> Option<&str> {
        self.properties.as_deref()
    }
    fn time_raw(&self) -> Option<&str> {
        self.time.as_deref()
    }
}

impl TimeBearingArgs for TrackFirstArgs {
    fn properties_raw(&self) -> Option<&str> {
        self.properties.as_deref()
    }
    fn time_raw(&self) -> Option<&str> {
        self.time.as_deref()
    }
}

impl TimeBearingArgs for TrackUpdateArgs {
    fn properties_raw(&self) -> Option<&str> {
        self.properties.as_deref()
    }
    fn time_raw(&self) -> Option<&str> {
        self.time.as_deref()
    }
}

/// Execute the parsed command against a freshly built analytics instance.
///
/// Reporting commands flush and close the consumer before returning so that
/// buffered data is always delivered.
pub fn run(cli: Cli) -> Result<(), TDError> {
    let ta = build_analytics(&cli)?;

    let requires_close = !matches!(cli.command, Command::Close | Command::Flush);
    match cli.command {
        Command::Track(args) => {
            let (account, distinct) = args.ids.resolve()?;
            let properties = track_properties(&args)?;
            if account.is_empty() {
                ta.track_with_distinct_id("", &distinct, &args.event_name, properties)
            } else {
                ta.track_with_distinct_id(&account, &distinct, &args.event_name, properties)
            }
        }
        Command::TrackFirst(args) => {
            let (account, distinct) = args.ids.resolve()?;
            let properties = track_properties(&args)?;
            ta.track_first_with_distinct_id(
                &account,
                &distinct,
                &args.event_name,
                &args.first_check_id,
                properties,
            )
        }
        Command::TrackUpdate(args) => {
            let (account, distinct) = args.ids.resolve()?;
            let properties = track_properties(&args)?;
            ta.track_update_with_distinct_id(
                &account,
                &distinct,
                &args.event_name,
                &args.event_id,
                properties,
            )
        }
        Command::TrackOverwrite(args) => {
            let (account, distinct) = args.ids.resolve()?;
            let properties = track_properties(&args)?;
            ta.track_overwrite_with_distinct_id(
                &account,
                &distinct,
                &args.event_name,
                &args.event_id,
                properties,
            )
        }
        Command::UserSet(args) => user_op(&ta, &args, |ta, a, d, p| {
            ta.user_set_with_distinct_id(&a, &d, p)
        }),
        Command::UserSetOnce(args) => user_op(&ta, &args, |ta, a, d, p| {
            ta.user_set_once_with_distinct_id(&a, &d, p)
        }),
        Command::UserAdd(args) => user_op(&ta, &args, |ta, a, d, p| {
            ta.user_add_with_distinct_id(&a, &d, p)
        }),
        Command::UserAppend(args) => user_op(&ta, &args, |ta, a, d, p| {
            ta.user_append_with_distinct_id(&a, &d, p)
        }),
        Command::UserUniqAppend(args) => user_op(&ta, &args, |ta, a, d, p| {
            ta.user_uniq_append_with_distinct_id(&a, &d, p)
        }),
        Command::UserUnset(args) => {
            let (account, distinct) = args.ids.resolve()?;
            if args.keys.is_empty() {
                return Err(TDError::EmptyUnsetKeys);
            }
            if account.is_empty() {
                ta.user_unset_with_distinct_id("", &distinct, &args.keys)
            } else {
                ta.user_unset_with_distinct_id(&account, &distinct, &args.keys)
            }
        }
        Command::UserDel(args) => {
            let (account, distinct) = args.ids.resolve()?;
            ta.user_del_with_distinct_id(&account, &distinct)
        }
        Command::Flush => ta.flush(),
        Command::Close => ta.close(),
    }?;

    if requires_close {
        ta.close()?;
    }
    Ok(())
}

/// Run one user-property operation, resolving IDs and parsing properties.
fn user_op(
    ta: &TDAnalytics,
    args: &UserArgs,
    op: impl Fn(&TDAnalytics, String, String, Map<String, Value>) -> Result<(), TDError>,
) -> Result<(), TDError> {
    let (account, distinct) = args.ids.resolve()?;
    let properties = parse_properties(args.properties.as_deref())?;
    op(ta, account, distinct, properties)
}

/// Quiet-mode helper for tests/examples: build a property map from pairs.
#[allow(dead_code)]
pub(crate) fn properties_from_pairs(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cli_with(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("thinkingdata").chain(args.iter().copied()))
    }

    #[test]
    fn consumer_flags_are_mutually_exclusive() {
        assert!(
            cli_with(&[
                "--log-dir",
                "/tmp/a",
                "--debug",
                "--server-url",
                "https://x",
                "--app-id",
                "a",
                "flush"
            ])
            .is_err()
        );
        assert!(cli_with(&["--log-dir", "/tmp/a", "flush"]).is_ok());
    }

    #[test]
    fn debug_requires_server_url_and_app_id() {
        assert!(cli_with(&["--debug", "flush"]).is_err());
        // clap accepts these without --app-id; build_analytics rejects them.
        let cli = cli_with(&["--server-url", "https://x", "flush"]).unwrap();
        assert!(build_analytics(&cli).is_err());
        let cli = cli_with(&["--debug", "--server-url", "https://x", "flush"]).unwrap();
        assert!(build_analytics(&cli).is_err());
        assert!(cli_with(&["--server-url", "https://x", "--app-id", "a", "flush"]).is_ok());
        assert!(
            cli_with(&[
                "--debug",
                "--server-url",
                "https://x",
                "--app-id",
                "a",
                "flush"
            ])
            .is_ok()
        );
    }

    #[test]
    fn properties_must_be_a_json_object() {
        let parsed = parse_properties(Some("[1,2]")).unwrap_err();
        assert!(parsed.to_string().contains("JSON object"), "{parsed}");
        let ok = parse_properties(Some("{\"price\": 9.9}")).unwrap();
        assert_eq!(ok["price"], 9.9);
        assert!(parse_properties(None).unwrap().is_empty());
    }

    #[test]
    fn ids_require_at_least_one() {
        let mut args = TrackArgs::try_parse_from(["track", "--event-name", "login"]).unwrap();
        assert!(args.ids.resolve().is_err());
        args.ids.account_id = Some("a".to_string());
        assert!(args.ids.resolve().is_ok());
    }

    #[test]
    fn unset_keys_are_comma_separated() {
        let args = UserUnsetArgs::try_parse_from([
            "user-unset",
            "--account-id",
            "a",
            "--keys",
            "k1,k2 , k3",
        ])
        .unwrap();
        assert_eq!(args.keys, vec!["k1", "k2 ", " k3"]);
    }

    #[test]
    fn properties_from_pairs_helper() {
        let map = properties_from_pairs(&[("a", Value::from(1))]);
        assert_eq!(map.len(), 1);
    }
}
