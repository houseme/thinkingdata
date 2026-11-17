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

//! SDK diagnostics logging.
//!
//! Logging is disabled by default ([`TDLogLevel::Off`]). Raise the level with
//! [`set_log_level`], or route diagnostics into your own logging stack by
//! installing a [`TDLogger`] implementation with [`set_custom_logger`]:
//!
//! ```
//! use std::sync::{Arc, Mutex};
//! use thinkingdata_core::log::{set_custom_logger, set_log_level, TDLogger, TDLogLevel};
//!
//! struct Collect(Mutex<Vec<String>>);
//! impl TDLogger for Collect {
//!     fn print(&self, message: &str) {
//!         self.0.lock().unwrap().push(message.to_string());
//!     }
//! }
//!
//! set_custom_logger(Arc::new(Collect(Mutex::new(Vec::new()))));
//! set_log_level(TDLogLevel::Info);
//! assert_eq!(thinkingdata_core::log::get_log_level(), TDLogLevel::Info);
//! set_log_level(TDLogLevel::Off);
//! ```

use std::sync::{Arc, Mutex, atomic::AtomicU8, atomic::Ordering};

/// Prefix prepended to every diagnostic message.
pub const SDK_LOG_PREFIX: &str = "[ThinkingData]";

/// The log output level, ordered from quietest to most verbose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum TDLogLevel {
    /// Disable all logging (default).
    Off = 1,
    /// Errors only.
    Error = 2,
    /// Errors and warnings.
    Warning = 3,
    /// Errors, warnings and informational messages.
    Info = 4,
    /// Everything, including per-event payloads.
    Debug = 5,
}

impl TDLogLevel {
    fn as_u8(self) -> u8 {
        self as u8
    }

    fn from_u8(value: u8) -> TDLogLevel {
        match value {
            2 => TDLogLevel::Error,
            3 => TDLogLevel::Warning,
            4 => TDLogLevel::Info,
            5 => TDLogLevel::Debug,
            _ => TDLogLevel::Off,
        }
    }

    fn tag(self) -> &'static str {
        match self {
            TDLogLevel::Error => "[Error] ",
            TDLogLevel::Warning => "[Warning] ",
            TDLogLevel::Info => "[Info] ",
            TDLogLevel::Debug | TDLogLevel::Off => "[Info] ",
        }
    }
}

/// A destination for SDK diagnostic messages.
///
/// `print` may be called concurrently from multiple threads. Panics inside a
/// custom logger are contained so logging can never interrupt event delivery.
pub trait TDLogger: Send + Sync {
    /// Receive one formatted diagnostic line (trailing newline included).
    fn print(&self, message: &str);
}

static LOG_LEVEL: AtomicU8 = AtomicU8::new(1); // TDLogLevel::Off
static CUSTOM_LOGGER: Mutex<Option<Arc<dyn TDLogger>>> = Mutex::new(None);

/// Set the log output level.
///
/// The default is [`TDLogLevel::Off`]. Note that constructing a
/// [`DebugConsumer`](crate::consumer::DebugConsumer) raises the global level
/// to [`TDLogLevel::Debug`], matching the official Go SDK behavior.
pub fn set_log_level(level: TDLogLevel) {
    LOG_LEVEL.store(level.as_u8(), Ordering::SeqCst);
}

/// Get the current log output level.
pub fn get_log_level() -> TDLogLevel {
    TDLogLevel::from_u8(LOG_LEVEL.load(Ordering::SeqCst))
}

/// Install a custom logger that receives every formatted diagnostic message.
///
/// Pass an [`Arc`] wrapping your implementation; call [`clear_custom_logger`]
/// to remove it again.
pub fn set_custom_logger(logger: Arc<dyn TDLogger>) {
    let mut guard = CUSTOM_LOGGER.lock().expect("ThinkingData logger poisoned");
    *guard = Some(logger);
}

/// Remove a previously installed custom logger.
pub fn clear_custom_logger() {
    let mut guard = CUSTOM_LOGGER.lock().expect("ThinkingData logger poisoned");
    *guard = None;
}

/// Emit one diagnostic message if `level` is enabled.
///
/// The custom logger is invoked outside of any lock, mirroring the Go SDK's
/// rule that user code never runs while SDK configuration locks are held.
pub(crate) fn td_log(level: TDLogLevel, message: &str) {
    if level > get_log_level() || level == TDLogLevel::Off {
        return;
    }
    let line = format!("{SDK_LOG_PREFIX}{}{message}\n", level.tag());
    let logger = {
        let guard = CUSTOM_LOGGER.lock().expect("ThinkingData logger poisoned");
        guard.clone()
    };
    if let Some(logger) = logger {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            logger.print(&line);
        }));
        let _ = result;
    } else {
        // Default destination: stdout, timestamped like the Go SDK.
        let now = crate::utils::format_now();
        print!("[{now}]{line}");
    }
}

// `debug`/`warning` are part of the internal log API; exercised by tests and
// kept for parity with the Go SDK's tdLogDebug/tdLogWarning helpers.
#[allow(dead_code)]
pub(crate) fn td_log_debug(message: &str) {
    td_log(TDLogLevel::Debug, message);
}

pub(crate) fn td_log_info(message: &str) {
    td_log(TDLogLevel::Info, message);
}

#[allow(dead_code)]
pub(crate) fn td_log_warning(message: &str) {
    td_log(TDLogLevel::Warning, message);
}

pub(crate) fn td_log_error(message: &str) {
    td_log(TDLogLevel::Error, message);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;

    // The global log level and custom logger are process-wide; serialize every
    // test that touches them so parallel execution cannot interfere.
    static LOG_TEST_LOCK: Mutex<()> = Mutex::new(());

    struct CountingLogger {
        lines: Mutex<Vec<String>>,
        panics: bool,
        calls: AtomicUsize,
    }

    impl CountingLogger {
        fn new(panics: bool) -> Arc<Self> {
            Arc::new(Self {
                lines: Mutex::new(Vec::new()),
                panics,
                calls: AtomicUsize::new(0),
            })
        }
    }

    impl TDLogger for CountingLogger {
        fn print(&self, message: &str) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.panics {
                panic!("logger panic");
            }
            self.lines.lock().unwrap().push(message.to_string());
        }
    }

    struct Guard;

    impl Drop for Guard {
        fn drop(&mut self) {
            clear_custom_logger();
            set_log_level(TDLogLevel::Off);
        }
    }

    #[test]
    fn respects_level_threshold() {
        let _guard = Guard;
        let _serial = LOG_TEST_LOCK.lock().unwrap();
        let logger = CountingLogger::new(false);
        set_custom_logger(logger.clone());
        set_log_level(TDLogLevel::Warning);

        td_log_debug("hidden");
        td_log_warning("shown");

        let lines = logger.lines.lock().unwrap();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("[Warning] shown"));
    }

    #[test]
    fn contains_panic_from_custom_logger() {
        let _guard = Guard;
        let _serial = LOG_TEST_LOCK.lock().unwrap();
        let logger = CountingLogger::new(true);
        set_custom_logger(logger.clone());
        set_log_level(TDLogLevel::Error);
        td_log_error("boom");
        assert_eq!(logger.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn default_level_is_off() {
        let _serial = LOG_TEST_LOCK.lock().unwrap();
        assert_eq!(get_log_level(), TDLogLevel::Off);
    }
}
