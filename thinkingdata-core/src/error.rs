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

//! Error type shared by the whole SDK.

use std::fmt;

/// The result type used across the SDK.
pub type Result<T> = std::result::Result<T, TDError>;

/// Errors returned by the ThinkingData SDK.
///
/// External error sources (HTTP, IO, JSON, URL) are preserved as formatted
/// messages so the error itself stays [`Clone`], which the consumers need to
/// publish results — such as the outcome of `close` — to concurrent callers.
#[derive(Debug, Clone, thiserror::Error)]
pub enum TDError {
    /// A generic error message, mirroring the error strings of the official SDKs.
    #[error("{0}")]
    Message(String),

    /// An HTTP transport or response error.
    #[error("http request failed: {0}")]
    Http(String),

    /// A file system error.
    #[error("io operation failed: {0}")]
    Io(String),

    /// A JSON serialization error.
    #[error("json serialization failed: {0}")]
    Json(String),

    /// The configured server URL is invalid.
    #[error("invalid server url: {0}")]
    InvalidServerUrl(String),

    /// `event_name` is required for every `track*` data type.
    #[error("the event name must be provided")]
    EmptyEventName,

    /// `event_id` is required for `track_update` and `track_overwrite`.
    #[error("the event id must be provided")]
    EmptyEventId,

    /// `first_check_id` is required for first events.
    #[error("the 'firstCheckId' must be provided")]
    EmptyFirstCheckId,

    /// The event name does not match `^[a-zA-Z#][A-Za-z0-9_]{0,49}$`.
    #[error("invalid event name: {0}")]
    InvalidEventName(String),

    /// A property key does not match `^[a-zA-Z#][A-Za-z0-9_]{0,49}$`.
    /// Only reported by stringent consumers (debug consumer).
    #[error("invalid property key: {0}")]
    InvalidPropertyKey(String),

    /// A `user_add` property value is not a number.
    #[error("invalid property value: only numbers is supported by UserAdd")]
    InvalidUserAddValue,

    /// Both `account_id` and `distinct_id` were empty.
    #[error("invalid parameters: account_id and distinct_id cannot be empty at the same time")]
    EmptyIds,

    /// `user_unset` was called without any keys.
    #[error("invalid params for UserUnset: keys is nil")]
    EmptyUnsetKeys,

    /// The batch consumer has been closed and no longer accepts data.
    #[error("batch consumer has been closed")]
    ConsumerClosed,
}

impl From<reqwest::Error> for TDError {
    fn from(err: reqwest::Error) -> Self {
        TDError::Http(err.to_string())
    }
}

impl From<std::io::Error> for TDError {
    fn from(err: std::io::Error) -> Self {
        TDError::Io(err.to_string())
    }
}

impl From<serde_json::Error> for TDError {
    fn from(err: serde_json::Error) -> Self {
        TDError::Json(err.to_string())
    }
}

impl From<url::ParseError> for TDError {
    fn from(err: url::ParseError) -> Self {
        TDError::InvalidServerUrl(err.to_string())
    }
}

impl TDError {
    /// Shorthand for [`TDError::Message`] with a formatted string.
    pub fn msg(message: impl fmt::Display) -> Self {
        TDError::Message(message.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clone_preserves_message() {
        let err = TDError::InvalidEventName("bad name".to_string());
        let cloned = err.clone();
        assert_eq!(cloned.to_string(), "invalid event name: bad name");
    }

    #[test]
    fn from_io_error_wraps_message() {
        let err: TDError = std::io::Error::new(std::io::ErrorKind::NotFound, "missing").into();
        assert!(err.to_string().contains("missing"));
    }
}
