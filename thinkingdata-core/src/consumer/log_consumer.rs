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

//! The log consumer: writes NDJSON files for later ingestion via LogBus.
//!
//! Internally it is a streaming pipeline: every event is pushed onto an MPSC
//! channel and a single writer task owns all file I/O (opening, rotating,
//! paging, syncing), executing the blocking file operations via
//! `tokio::task::spawn_blocking`.

use std::fs::{File, OpenOptions, create_dir_all, read_dir};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::{mpsc, oneshot};

use crate::error::{Result, TDError};
use crate::log::td_log_info;
use crate::utils::{format_with, parse_time};

#[cfg(feature = "async")]
use crate::consumer::AsyncConsumer;
#[cfg(feature = "blocking")]
use crate::consumer::Consumer;
use crate::data::Data;

/// How often the log file rotates by time.
///
/// The Rust port of the Go SDK's `RotateMode` constants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum RotateMode {
    /// Rotate once a day; file names look like `log.2026-11-17`.
    #[default]
    Daily,
    /// Rotate once an hour; file names look like `log.2026-11-17-09`.
    Hourly,
}

impl RotateMode {
    /// The jiff `strftime` pattern used for the time part of the file name.
    fn date_format(self) -> &'static str {
        match self {
            RotateMode::Daily => "%Y-%m-%d",
            RotateMode::Hourly => "%Y-%m-%d-%H",
        }
    }

    fn from_go(value: i32) -> Result<RotateMode> {
        match value {
            0 => Ok(RotateMode::Daily),
            1 => Ok(RotateMode::Hourly),
            _ => Err(TDError::msg("unknown rotate mode")),
        }
    }
}

/// Configuration for [`LogConsumer`], mirroring the Go SDK's
/// `TDLogConsumerConfig`.
#[derive(Debug, Clone)]
pub struct LogConsumerConfig {
    /// Directory the log files live in. Created when missing.
    pub directory: String,
    /// Time-based rotation of log files.
    pub rotate_mode: RotateMode,
    /// Maximum size of a single log file in megabytes; `0` disables paging.
    pub file_size_mb: u64,
    /// Prefix placed before `log.` in the file name.
    pub file_name_prefix: String,
}

impl Default for LogConsumerConfig {
    fn default() -> Self {
        Self {
            directory: String::new(),
            rotate_mode: RotateMode::Daily,
            file_size_mb: 0,
            file_name_prefix: String::new(),
        }
    }
}

/// One command for the streaming writer task.
enum LogCommand {
    Write {
        line: String,
        ack: oneshot::Sender<Result<()>>,
    },
    Flush {
        ack: oneshot::Sender<Result<()>>,
    },
    Close {
        ack: oneshot::Sender<Result<()>>,
    },
}

/// Immutable configuration shared with the writer task.
struct WriterShared {
    directory: String,
    rotate_mode: RotateMode,
    max_file_size: u64,
    file_name_prefix: String,
    state: Mutex<WriterState>,
    /// Result of the final close, published by the writer task so repeated
    /// `close` calls return the same result after the task exits.
    close_result: OnceLock<Result<()>>,
}

/// File state owned by the writer task.
struct WriterState {
    file: Option<File>,
    file_name: String,
    file_index: usize,
    write_err: Option<TDError>,
}

/// Write data to local log files, one JSON object per line, for later
/// ingestion with LogBus or another transfer tool.
///
/// This is the Rust counterpart of the Go SDK's `TDLogConsumer`. Files are
/// named `{directory}/{prefix.}log.{date}` (or `..._{n}` when size-based
/// paging is enabled) and are opened in append mode, so restarting the
/// process never overwrites existing data.
///
/// ```
/// # fn main() -> Result<(), thinkingdata_core::TDError> {
/// # #[cfg(feature = "blocking")]
/// # {
/// # use thinkingdata_core::consumer::{Consumer, LogConsumer};
/// # let consumer = LogConsumer::new("/tmp/thinkingdata-logs")?;
/// # consumer.close()?;
/// # }
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct LogConsumer {
    tx: mpsc::UnboundedSender<LogCommand>,
    shared: Arc<WriterShared>,
}

impl std::fmt::Debug for LogConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogConsumer")
            .field("directory", &self.shared.directory)
            .field("rotate_mode", &self.shared.rotate_mode)
            .field("max_file_size", &self.shared.max_file_size)
            .field("file_name_prefix", &self.shared.file_name_prefix)
            .finish_non_exhaustive()
    }
}

impl LogConsumer {
    /// Create a consumer writing daily-rotated logs into `directory`.
    pub fn new(directory: &str) -> Result<Self> {
        Self::with_config(LogConsumerConfig {
            directory: directory.to_string(),
            ..LogConsumerConfig::default()
        })
    }

    /// Create a consumer with an explicit [`RotateMode`].
    pub fn with_rotate_mode(directory: &str, rotate_mode: RotateMode) -> Result<Self> {
        Self::with_config(LogConsumerConfig {
            directory: directory.to_string(),
            rotate_mode,
            ..LogConsumerConfig::default()
        })
    }

    /// Numeric rotate-mode constructor matching the Go SDK's
    /// `NewLogConsumer(directory, rotateMode)`: `0` selects daily rotation,
    /// `1` hourly rotation.
    pub fn with_go_rotate_mode(directory: &str, rotate_mode: i32) -> Result<Self> {
        Self::with_config(LogConsumerConfig {
            directory: directory.to_string(),
            rotate_mode: RotateMode::from_go(rotate_mode)?,
            ..LogConsumerConfig::default()
        })
    }

    /// Create a consumer from a full configuration.
    pub fn with_config(config: LogConsumerConfig) -> Result<Self> {
        const MAX_FILE_SIZE_MB: u64 = u64::MAX / (1024 * 1024);
        if config.file_size_mb > MAX_FILE_SIZE_MB {
            return Err(TDError::msg(format!(
                "invalid FileSize {}: must be between 0 and {} MB",
                config.file_size_mb, MAX_FILE_SIZE_MB
            )));
        }

        create_dir_all(&config.directory).map_err(|err| {
            TDError::msg(format!("create log directory {}: {err}", config.directory))
        })?;

        let max_file_size = config.file_size_mb.saturating_mul(1024 * 1024);
        let mut file_index = 0usize;
        if max_file_size > 0 {
            let period = format_with(config.rotate_mode.date_format());
            restore_file_index(
                &config.directory,
                &config.file_name_prefix,
                &period,
                &mut file_index,
            )?;
        }
        let (file, file_name) = open_writable(
            &config.directory,
            &config.file_name_prefix,
            config.rotate_mode.date_format(),
            max_file_size,
            &mut file_index,
        )?;

        td_log_info(&format!(
            "Mode: log consumer, directory: {}, rotate mode: {:?}",
            config.directory, config.rotate_mode
        ));

        let shared = Arc::new(WriterShared {
            directory: config.directory,
            rotate_mode: config.rotate_mode,
            max_file_size,
            file_name_prefix: config.file_name_prefix,
            state: Mutex::new(WriterState {
                file: Some(file),
                file_name,
                file_index,
                write_err: None,
            }),
            close_result: OnceLock::new(),
        });

        let (tx, rx) = mpsc::unbounded_channel::<LogCommand>();
        crate::consumer::shared_runtime().spawn(writer_loop(shared.clone(), rx));
        Ok(Self { tx, shared })
    }

    /// Serialize one event into a JSON line and hand it to the writer task.
    async fn do_add(&self, data: &Data) -> Result<()> {
        let json = serde_json::to_string(data)?;
        let line = parse_time(&json);
        td_log_info(&format!("Enqueue event data: {line}"));
        let (ack_tx, ack_rx) = oneshot::channel();
        self.tx
            .send(LogCommand::Write { line, ack: ack_tx })
            .map_err(|_| TDError::msg("add event failed, SDK has been closed"))?;
        match ack_rx.await {
            Ok(result) => result,
            // The writer task exited while this command was queued. If a
            // concurrent `close` won the race, the event was not delivered
            // because the consumer closed; otherwise the task is gone.
            Err(_) if self.shared.close_result.get().is_some() => {
                Err(TDError::msg("add event failed, SDK has been closed"))
            }
            Err(_) => Err(TDError::msg("log writer task terminated")),
        }
    }

    /// Sync the current file to disk and return the first retained error.
    async fn do_flush(&self) -> Result<()> {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(LogCommand::Flush { ack: ack_tx }).is_err() {
            // The writer task has exited; report the retained close result.
            return match self.shared.close_result.get() {
                Some(result) => result.clone(),
                None => Err(TDError::msg("log consumer has been closed")),
            };
        }
        match ack_rx.await {
            Ok(result) => result,
            // A concurrent `close` won the race and terminated the writer task
            // before this command was served; report its retained result.
            Err(_) => match self.shared.close_result.get() {
                Some(result) => result.clone(),
                None => Err(TDError::msg("log writer task terminated")),
            },
        }
    }

    /// Stop accepting events, sync and close the current file, and return the
    /// first retained error. Repeated calls return the same result.
    async fn do_close(&self) -> Result<()> {
        if let Some(result) = self.shared.close_result.get() {
            return result.clone();
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.tx.send(LogCommand::Close { ack: ack_tx }).is_err() {
            return match self.shared.close_result.get() {
                Some(result) => result.clone(),
                None => Err(TDError::msg("log consumer has been closed")),
            };
        }
        match ack_rx.await {
            Ok(result) => result,
            // A concurrent `close` won the race and terminated the writer task
            // before this command was served; report its retained result.
            Err(_) => match self.shared.close_result.get() {
                Some(result) => result.clone(),
                None => Err(TDError::msg("log writer task terminated")),
            },
        }
    }
}

/// The streaming writer task: consumes commands in order and owns all file I/O.
async fn writer_loop(shared: Arc<WriterShared>, mut rx: mpsc::UnboundedReceiver<LogCommand>) {
    while let Some(command) = rx.recv().await {
        match command {
            LogCommand::Write { line, ack } => {
                let result = write_event(&shared, line).await;
                let _ = ack.send(result);
            }
            LogCommand::Flush { ack } => {
                let result = sync_file(&shared).await;
                let _ = ack.send(result);
            }
            LogCommand::Close { ack } => {
                let result = close_writer(&shared).await;
                let _ = shared.close_result.set(result.clone());
                let _ = ack.send(result);
                return;
            }
        }
    }
}

/// Append one line, rotating the file when the period changed or the size
/// limit was exceeded. Blocking file operations run on the blocking pool; the
/// line is moved into the task so the (potentially large) payload is written
/// without an extra copy.
async fn write_event(shared: &Arc<WriterShared>, line: String) -> Result<()> {
    let task = shared.clone();
    tokio::task::spawn_blocking(move || {
        let mut state = task.state.lock().expect("ThinkingData log state poisoned");
        let period = format_with(task.rotate_mode.date_format());
        match write_line(&task, &mut state, &line, &period) {
            Err(err) => {
                remember_error(&mut state, err.clone());
                Err(err)
            }
            Ok(()) => Ok(()),
        }
    })
    .await
    .map_err(|err| TDError::msg(format!("log writer failed: {err}")))?
}

/// Sync the current file; retains and returns the first remembered error.
async fn sync_file(shared: &Arc<WriterShared>) -> Result<()> {
    let task = shared.clone();
    tokio::task::spawn_blocking(move || {
        let mut state = task.state.lock().expect("ThinkingData log state poisoned");
        if let Some(file) = &state.file
            && let Err(err) = file.sync_all()
        {
            remember_error(&mut state, TDError::msg(format!("sync log file: {err}")));
        }
        match &state.write_err {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    })
    .await
    .map_err(|err| TDError::msg(format!("log writer failed: {err}")))?
}

/// Sync, close and seal the writer; retains and returns the first error.
async fn close_writer(shared: &Arc<WriterShared>) -> Result<()> {
    let task = shared.clone();
    tokio::task::spawn_blocking(move || {
        let mut state = task.state.lock().expect("ThinkingData log state poisoned");
        if let Some(file) = state.file.take() {
            if let Err(err) = file.sync_all() {
                remember_error(&mut state, TDError::msg(format!("sync log file: {err}")));
            }
            drop(file);
        }
        match &state.write_err {
            Some(err) => Err(err.clone()),
            None => Ok(()),
        }
    })
    .await
    .map_err(|err| TDError::msg(format!("log writer failed: {err}")))?
}

fn remember_error(state: &mut WriterState, err: TDError) {
    if state.write_err.is_none() {
        state.write_err = Some(err);
    }
}

/// Append one line, mirroring the Go writer's rotation logic.
fn write_line(
    shared: &WriterShared,
    state: &mut WriterState,
    line: &str,
    period: &str,
) -> Result<()> {
    let mut file_name = construct_file_name(
        &shared.directory,
        &shared.file_name_prefix,
        period,
        shared.max_file_size,
        state.file_index,
    );
    if let Some(file) = &state.file {
        if state.file_name == file_name && shared.max_file_size > 0 {
            let size = file
                .metadata()
                .map_err(|err| TDError::msg(format!("stat log file: {err}")))?
                .len();
            if size > shared.max_file_size {
                if state.file_index == usize::MAX {
                    return Err(TDError::msg("log file index overflow"));
                }
                state.file_index += 1;
                file_name = construct_file_name(
                    &shared.directory,
                    &shared.file_name_prefix,
                    period,
                    shared.max_file_size,
                    state.file_index,
                );
            }
        }
        if state.file_name != file_name {
            let previous = state.file.take();
            if let Some(previous) = previous {
                previous
                    .sync_all()
                    .map_err(|err| TDError::msg(format!("sync rotated log file: {err}")))?;
                drop(previous);
            }
        }
    }
    if state.file.is_none() {
        let (file, name) = open_writable(
            &shared.directory,
            &shared.file_name_prefix,
            shared.rotate_mode.date_format(),
            shared.max_file_size,
            &mut state.file_index,
        )?;
        state.file = Some(file);
        state.file_name = name;
    }
    let file = state.file.as_mut().expect("log file is opened above");
    file.write_all(line.as_bytes())
        .and_then(|_| file.write_all(b"\n"))
        .map_err(|err| TDError::msg(format!("write log file: {err}")))
}

/// Base file name (no directory) for a paged log file.
fn paged_base_name(prefix: &str, period: &str, index: usize) -> String {
    format!("{}log.{period}_{index}", dotted_prefix(prefix))
}

fn dotted_prefix(prefix: &str) -> String {
    if prefix.is_empty() {
        String::new()
    } else {
        format!("{prefix}.")
    }
}

fn construct_file_name(
    directory: &str,
    prefix: &str,
    period: &str,
    max_file_size: u64,
    index: usize,
) -> String {
    let dotted = dotted_prefix(prefix);
    if max_file_size > 0 {
        format!("{directory}/{dotted}log.{period}_{index}")
    } else {
        format!("{directory}/{dotted}log.{period}")
    }
}

/// Resume the latest numbered file for this period and prefix on restart.
/// Older partial files and gaps must not move the append position backwards.
fn restore_file_index(
    directory: &str,
    prefix: &str,
    period: &str,
    file_index: &mut usize,
) -> Result<()> {
    let entries =
        read_dir(directory).map_err(|err| TDError::msg(format!("read log directory: {err}")))?;
    let name_prefix = paged_base_name(prefix, period, 0)
        .strip_suffix('0')
        .map(str::to_string)
        .unwrap_or_default();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with(&name_prefix) {
            continue;
        }
        let suffix = &name[name_prefix.len()..];
        // Only accept names generated by `construct_file_name`, not backups.
        if let Ok(index) = suffix.parse::<usize>()
            && index.to_string() == suffix
            && index > *file_index
        {
            *file_index = index;
        }
    }
    Ok(())
}

/// Open the first candidate file that still has room, including files left
/// behind by a previous process.
fn open_writable(
    directory: &str,
    prefix: &str,
    date_format: &str,
    max_file_size: u64,
    file_index: &mut usize,
) -> Result<(File, String)> {
    let dotted = dotted_prefix(prefix);
    loop {
        let period = format_with(date_format);
        let name = if max_file_size > 0 {
            format!("{directory}/{dotted}log.{period}_{file_index}")
        } else {
            format!("{directory}/{dotted}log.{period}")
        };
        let file = open_append(Path::new(&name))?;
        if max_file_size == 0 {
            return Ok((file, name));
        }
        let size = file
            .metadata()
            .map_err(|err| TDError::msg(format!("stat log file: {err}")))?
            .len();
        if size <= max_file_size {
            return Ok((file, name));
        }
        drop(file);
        if *file_index == usize::MAX {
            return Err(TDError::msg("log file index overflow"));
        }
        *file_index += 1;
    }
}

fn open_append(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.append(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o664);
    }
    options
        .open(path)
        .map_err(|err| TDError::msg(format!("open log file {}: {err}", path.display())))
}

#[cfg(feature = "blocking")]
impl Consumer for LogConsumer {
    /// Serialize and append the event, awaiting the writer task's result.
    /// Unlike the Go SDK — which only admits events onto a channel and
    /// reports write errors from `flush`/`close` — this port reports file
    /// errors immediately.
    fn add(&self, data: &Data) -> Result<()> {
        crate::consumer::shared_runtime().block_on(self.do_add(data))
    }

    /// Sync the current file to disk and return the first retained error.
    fn flush(&self) -> Result<()> {
        crate::consumer::shared_runtime().block_on(self.do_flush())
    }

    /// Stop accepting events, sync and close the current file.
    fn close(&self) -> Result<()> {
        crate::consumer::shared_runtime().block_on(self.do_close())
    }
}

#[cfg(feature = "async")]
#[async_trait::async_trait]
impl AsyncConsumer for LogConsumer {
    /// Serialize and enqueue the event; the streaming writer task performs
    /// the file I/O, so this method never blocks the caller's task.
    async fn add_async(&self, data: &Data) -> Result<()> {
        self.do_add(data).await
    }

    /// Sync the current file to disk and return the first retained error.
    async fn flush_async(&self) -> Result<()> {
        self.do_flush().await
    }

    /// Stop accepting events, sync and close the current file.
    async fn close_async(&self) -> Result<()> {
        self.do_close().await
    }
}

#[cfg(all(test, feature = "blocking"))]
mod tests {
    use super::*;
    use crate::DataType;
    use serde_json::json;

    fn temp_dir(tag: &str) -> String {
        let dir = std::env::temp_dir().join(format!(
            "thinkingdata-log-test-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir.to_string_lossy().into_owned()
    }

    fn event() -> Data {
        let mut data = Data::new(DataType::Track);
        data.account_id = "acc".to_string();
        data.event_name = "purchase".to_string();
        data.properties.insert("price".to_string(), json!(9.9));
        data
    }

    #[test]
    fn writes_one_json_object_per_line() {
        let dir = temp_dir("lines");
        let consumer = LogConsumer::new(&dir).unwrap();
        consumer.add(&event()).unwrap();
        consumer.close().unwrap();

        let mut entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        entries.sort();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].starts_with("log.2"), "{entries:?}");

        let content = std::fs::read_to_string(Path::new(&dir).join(&entries[0])).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed["#type"], "track");
        assert_eq!(parsed["properties"]["price"], 9.9);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn file_name_prefix_is_applied() {
        let dir = temp_dir("prefix");
        let consumer = LogConsumer::with_config(LogConsumerConfig {
            directory: dir.clone(),
            file_name_prefix: "ta".to_string(),
            ..LogConsumerConfig::default()
        })
        .unwrap();
        consumer.add(&event()).unwrap();
        consumer.close().unwrap();

        let entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries.len(), 1);
        assert!(entries[0].starts_with("ta.log."), "{entries:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn size_paging_creates_indexed_files() {
        let dir = temp_dir("paging");
        let consumer = LogConsumer::with_config(LogConsumerConfig {
            directory: dir.clone(),
            file_size_mb: 1,
            ..LogConsumerConfig::default()
        })
        .unwrap();
        let mut big = event();
        big.properties
            .insert("padding".to_string(), json!("x".repeat(1024 * 1024 + 128)));
        consumer.add(&big).unwrap();
        consumer.add(&event()).unwrap();
        consumer.close().unwrap();

        let mut entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        entries.sort();
        assert_eq!(entries.len(), 2, "{entries:?}");
        assert!(entries[0].ends_with("_0"), "{entries:?}");
        assert!(entries[1].ends_with("_1"), "{entries:?}");

        // The second event must have landed in file _1.
        let second = std::fs::read_to_string(Path::new(&dir).join(&entries[1])).unwrap();
        assert!(second.contains("\"purchase\""), "{second}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restore_file_index_resumes_at_max() {
        let dir = temp_dir("restore");
        std::fs::create_dir_all(&dir).unwrap();
        let period = format_with(RotateMode::Daily.date_format());
        std::fs::write(Path::new(&dir).join(format!("log.{period}_0")), "old\n").unwrap();
        std::fs::write(Path::new(&dir).join(format!("log.{period}_3")), "old\n").unwrap();
        std::fs::write(Path::new(&dir).join(format!("log.{period}_backup")), "").unwrap();

        let mut index = 0usize;
        restore_file_index(&dir, "", &period, &mut index).unwrap();
        assert_eq!(index, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn add_after_close_fails() {
        let dir = temp_dir("closed");
        let consumer = LogConsumer::new(&dir).unwrap();
        consumer.close().unwrap();
        let err = consumer.add(&event()).unwrap_err();
        assert!(err.to_string().contains("closed"), "{err}");
        // close is idempotent and repeats the retained result.
        consumer.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn go_rotate_mode_validation() {
        assert_eq!(RotateMode::from_go(0).unwrap(), RotateMode::Daily);
        assert_eq!(RotateMode::from_go(1).unwrap(), RotateMode::Hourly);
        assert!(RotateMode::from_go(2).is_err());
    }

    #[test]
    fn local_time_is_used_for_period() {
        // Sanity: the daily/hourly patterns produce the documented shapes.
        let daily = format_with(RotateMode::Daily.date_format());
        assert_eq!(daily.len(), 10, "{daily}");
        let hourly = format_with(RotateMode::Hourly.date_format());
        assert_eq!(hourly.len(), 13, "{hourly}");
    }
}
