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

//! The analytics facades: [`TDAnalytics`] (blocking) and [`TDAnalyticsAsync`]
//! (async).
//!
//! Both facades share one implementation strategy: they validate inputs,
//! merge super properties, hoist preset keys (`#ip`, `#uuid`, `#time`, …)
//! into the wire format and hand the finished [`Data`] snapshot to the
//! injected consumer. Only the delivery call differs (`add` vs
//! `add_async`).
//!
//! ```
//! use thinkingdata_core::{TDAnalytics, consumer::LogConsumer};
//!
//! # fn main() -> Result<(), thinkingdata_core::TDError> {
//! # let dir = std::env::temp_dir().join("thinkingdata-core-doctest-analytics");
//! let consumer = LogConsumer::new(dir.to_str().unwrap())?;
//! let ta = TDAnalytics::new(consumer)?;
//!
//! // Super properties are applied to every subsequent event…
//! ta.set_super_properties(serde_json::json!({ "channel": "rust" }))?;
//!
//! ta.track("account_id_123", "purchase", serde_json::json!({ "price": 99.0 }))?;
//! ta.user_set("account_id_123", serde_json::json!({ "vip": true }))?;
//!
//! ta.close()?;
//! # Ok(())
//! # }
//! ```

use std::sync::{Arc, RwLock};

use serde_json::{Map, Value};

use crate::SDK_VERSION;
use crate::data::{
    Data, DataType, IntoPropertyMap, LIB_NAME, apply_preset_properties, format_properties,
};
use crate::error::{Result, TDError};
use crate::utils::is_valid_key;

#[cfg(feature = "async")]
use crate::consumer::AsyncConsumer;
#[cfg(feature = "blocking")]
use crate::consumer::Consumer;

/// A function producing dynamic super properties, evaluated once per event
/// outside of any lock.
type DynamicSuperProperties = Arc<dyn Fn() -> Map<String, Value> + Send + Sync>;

/// Shared super-property state used by both facades.
#[derive(Default)]
struct SuperProperties {
    static_props: RwLock<Map<String, Value>>,
    dynamic_props: RwLock<Option<DynamicSuperProperties>>,
}

impl SuperProperties {
    /// Snapshot of the static super properties.
    fn get(&self) -> Map<String, Value> {
        self.static_props
            .read()
            .expect("ThinkingData super properties poisoned")
            .clone()
    }

    /// Snapshot of the dynamic super properties; the user callback is invoked
    /// outside of the lock so it may reenter the SDK.
    fn get_dynamic(&self) -> Map<String, Value> {
        let action = self
            .dynamic_props
            .read()
            .expect("ThinkingData super properties poisoned")
            .clone();
        if let Some(action) = action {
            action()
        } else {
            Map::new()
        }
    }

    /// Validate and merge static super properties.
    fn set(&self, properties: Map<String, Value>) -> Result<()> {
        for key in properties.keys() {
            if !is_valid_key(key) {
                return Err(TDError::InvalidPropertyKey(key.clone()));
            }
        }
        let mut guard = self
            .static_props
            .write()
            .expect("ThinkingData super properties poisoned");
        for (key, value) in properties {
            guard.insert(key, value);
        }
        Ok(())
    }
}

/// Validate event parameters shared by every `track*` operation.
fn validate_event(data_type: DataType, event_name: &str, event_id: &str) -> Result<()> {
    if event_name.is_empty() {
        crate::log::td_log_error("the event name must be provided");
        return Err(TDError::EmptyEventName);
    }
    // eventId must not be null unless the type is plain `track`.
    if event_id.is_empty() && data_type != DataType::Track {
        crate::log::td_log_error("the event id must be provided");
        return Err(TDError::EmptyEventId);
    }
    Ok(())
}

/// Compose the property map of a track event, in the Go SDK's precedence
/// order: super properties < dynamic super properties < preset `#lib` fields
/// < custom properties.
fn compose_track_properties(
    super_properties: &Map<String, Value>,
    dynamic: &Map<String, Value>,
    custom: Map<String, Value>,
) -> Map<String, Value> {
    let mut properties = super_properties.clone();
    for (key, value) in dynamic {
        properties.insert(key.clone(), value.clone());
    }
    properties.insert("#lib".to_string(), Value::String(LIB_NAME.to_string()));
    properties.insert(
        "#lib_version".to_string(),
        Value::String(SDK_VERSION.to_string()),
    );
    for (key, value) in custom {
        properties.insert(key, value);
    }
    properties
}

/// Build the final [`Data`] snapshot: hoist preset keys out of `properties`
/// and validate names/keys/values according to the consumer's stringency.
fn build_data(
    account_id: &str,
    distinct_id: &str,
    data_type: DataType,
    event_name: &str,
    event_id: &str,
    stringent: bool,
    mut properties: Map<String, Value>,
) -> Result<Data> {
    if account_id.is_empty() && distinct_id.is_empty() {
        let message =
            "invalid parameters: account_id and distinct_id cannot be empty at the same time";
        crate::log::td_log_error(message);
        return Err(TDError::EmptyIds);
    }

    let mut data = Data::new(data_type);
    data.account_id = account_id.to_string();
    data.distinct_id = distinct_id.to_string();
    data.event_name = event_name.to_string();
    data.event_id = event_id.to_string();
    apply_preset_properties(&mut data, &mut properties);
    data.properties = properties;
    format_properties(&mut data, stringent)?;
    Ok(data)
}

/// Reject `user_unset` calls without keys.
fn validate_unset_keys(keys: &[impl AsRef<str>]) -> Result<()> {
    if keys.is_empty() {
        crate::log::td_log_info("invalid params for UserUnset: keys is nil");
        return Err(TDError::EmptyUnsetKeys);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Blocking facade
// ---------------------------------------------------------------------------

/// The blocking analytics facade of the SDK, ported from the Go SDK's
/// `TDAnalytics`.
///
/// It is cheap to clone; every clone shares the same consumer and super
/// property state. See the [crate documentation](crate) for a quick start.
#[cfg(feature = "blocking")]
#[derive(Clone)]
pub struct TDAnalytics {
    consumer: Arc<dyn Consumer>,
    super_properties: Arc<SuperProperties>,
}

#[cfg(feature = "blocking")]
impl TDAnalytics {
    /// Create an analytics instance delegating delivery to `consumer`.
    pub fn new(consumer: impl Consumer + 'static) -> Result<Self> {
        crate::log::td_log_info("init SDK success");
        Ok(Self {
            consumer: Arc::new(consumer),
            super_properties: Arc::new(SuperProperties::default()),
        })
    }

    /// The injected consumer.
    pub fn consumer(&self) -> &Arc<dyn Consumer> {
        &self.consumer
    }

    /// Set common properties applied to every subsequent `track*` event.
    ///
    /// Keys are validated against the key pattern; an invalid key returns an
    /// error instead of silently corrupting the stream.
    pub fn set_super_properties(&self, properties: impl IntoPropertyMap) -> Result<()> {
        self.super_properties.set(properties.into_property_map()?)
    }

    /// Snapshot of the currently registered super properties.
    pub fn super_properties(&self) -> Map<String, Value> {
        self.super_properties.get()
    }

    /// Remove every registered super property.
    pub fn clear_super_properties(&self) {
        self.super_properties
            .static_props
            .write()
            .expect("ThinkingData super properties poisoned")
            .clear();
    }

    /// Register a callback producing dynamic common properties. It is
    /// evaluated once per event, outside of any lock; avoid heavy work in it.
    pub fn set_dynamic_super_properties<F, P>(&self, action: F)
    where
        F: Fn() -> P + Send + Sync + 'static,
        P: IntoPropertyMap,
    {
        let action = Arc::new(move || action().into_property_map().unwrap_or_default());
        *self
            .super_properties
            .dynamic_props
            .write()
            .expect("ThinkingData super properties poisoned") = Some(action);
    }

    /// Snapshot of the dynamic super properties.
    pub fn dynamic_super_properties(&self) -> Map<String, Value> {
        self.super_properties.get_dynamic()
    }

    /// Report an ordinary event attributed to `account_id`.
    pub fn track(
        &self,
        account_id: &str,
        event_name: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_with_distinct_id(account_id, "", event_name, properties)
    }

    /// Report an ordinary event with explicit account and distinct IDs.
    pub fn track_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        event_name: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_impl(
            account_id,
            distinct_id,
            DataType::Track,
            event_name,
            "",
            properties,
        )
    }

    /// Report a first-check event; `first_check_id` deduplicates it.
    pub fn track_first(
        &self,
        account_id: &str,
        event_name: &str,
        first_check_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_first_with_distinct_id(account_id, "", event_name, first_check_id, properties)
    }

    /// Report a first-check event with explicit account and distinct IDs.
    pub fn track_first_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        event_name: &str,
        first_check_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        if first_check_id.is_empty() {
            let message = "the 'firstCheckId' must be provided";
            crate::log::td_log_info(message);
            return Err(TDError::EmptyFirstCheckId);
        }
        let mut map = properties.into_property_map()?;
        map.insert(
            "#first_check_id".to_string(),
            Value::String(first_check_id.to_string()),
        );
        self.track_impl(
            account_id,
            distinct_id,
            DataType::Track,
            event_name,
            "",
            map,
        )
    }

    /// Report an updatable event identified by `event_id`.
    pub fn track_update(
        &self,
        account_id: &str,
        event_name: &str,
        event_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_update_with_distinct_id(account_id, "", event_name, event_id, properties)
    }

    /// Report an updatable event with explicit account and distinct IDs.
    pub fn track_update_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        event_name: &str,
        event_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_impl(
            account_id,
            distinct_id,
            DataType::TrackUpdate,
            event_name,
            event_id,
            properties,
        )
    }

    /// Report an overridable event identified by `event_id`.
    pub fn track_overwrite(
        &self,
        account_id: &str,
        event_name: &str,
        event_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_overwrite_with_distinct_id(account_id, "", event_name, event_id, properties)
    }

    /// Report an overridable event with explicit account and distinct IDs.
    pub fn track_overwrite_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        event_name: &str,
        event_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_impl(
            account_id,
            distinct_id,
            DataType::TrackOverwrite,
            event_name,
            event_id,
            properties,
        )
    }

    fn track_impl(
        &self,
        account_id: &str,
        distinct_id: &str,
        data_type: DataType,
        event_name: &str,
        event_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        validate_event(data_type, event_name, event_id)?;
        let custom = properties.into_property_map()?;
        let properties = compose_track_properties(
            &self.super_properties.get(),
            &self.super_properties.get_dynamic(),
            custom,
        );
        let data = build_data(
            account_id,
            distinct_id,
            data_type,
            event_name,
            event_id,
            self.consumer.is_stringent(),
            properties,
        )?;
        self.consumer.add(&data)
    }

    /// Set user properties, overwriting existing values.
    pub fn user_set(&self, account_id: &str, properties: impl IntoPropertyMap) -> Result<()> {
        self.user_set_with_distinct_id(account_id, "", properties)
    }

    /// Set user properties with explicit account and distinct IDs.
    pub fn user_set_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserSet,
            properties.into_property_map()?,
        )
    }

    /// Set user properties only where they were not set before.
    pub fn user_set_once(&self, account_id: &str, properties: impl IntoPropertyMap) -> Result<()> {
        self.user_set_once_with_distinct_id(account_id, "", properties)
    }

    /// `user_set_once` with explicit account and distinct IDs.
    pub fn user_set_once_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserSetOnce,
            properties.into_property_map()?,
        )
    }

    /// Accumulate numeric user properties.
    pub fn user_add(&self, account_id: &str, properties: impl IntoPropertyMap) -> Result<()> {
        self.user_add_with_distinct_id(account_id, "", properties)
    }

    /// `user_add` with explicit account and distinct IDs.
    pub fn user_add_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserAdd,
            properties.into_property_map()?,
        )
    }

    /// Append values to array-typed user properties.
    pub fn user_append(&self, account_id: &str, properties: impl IntoPropertyMap) -> Result<()> {
        self.user_append_with_distinct_id(account_id, "", properties)
    }

    /// `user_append` with explicit account and distinct IDs.
    pub fn user_append_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserAppend,
            properties.into_property_map()?,
        )
    }

    /// Append unique values to array-typed user properties.
    pub fn user_uniq_append(
        &self,
        account_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_uniq_append_with_distinct_id(account_id, "", properties)
    }

    /// `user_uniq_append` with explicit account and distinct IDs.
    pub fn user_uniq_append_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserUniqAppend,
            properties.into_property_map()?,
        )
    }

    /// Clear the listed user properties.
    pub fn user_unset<S: AsRef<str>>(&self, account_id: &str, keys: &[S]) -> Result<()> {
        self.user_unset_with_distinct_id(account_id, "", keys)
    }

    /// Clear the listed user properties with explicit account and distinct IDs.
    pub fn user_unset_with_distinct_id<S: AsRef<str>>(
        &self,
        account_id: &str,
        distinct_id: &str,
        keys: &[S],
    ) -> Result<()> {
        validate_unset_keys(keys)?;
        let mut properties = Map::new();
        for key in keys {
            properties.insert(key.as_ref().to_string(), Value::from(0));
        }
        self.user_impl(account_id, distinct_id, DataType::UserUnset, properties)
    }

    /// Clear user properties using a property map (values are ignored by the
    /// receiver, but preset keys keep their values, matching the Lua SDK).
    pub fn user_unset_with_properties(
        &self,
        account_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_unset_with_properties_with_distinct_id(account_id, "", properties)
    }

    /// `user_unset_with_properties` with explicit account and distinct IDs.
    pub fn user_unset_with_properties_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        let mut map = properties.into_property_map()?;
        if map.is_empty() {
            crate::log::td_log_info("invalid params for UserUnset: properties is nil");
            return Err(TDError::EmptyUnsetKeys);
        }
        for (key, value) in map.iter_mut() {
            if !key.starts_with('#') {
                *value = Value::from(0);
            }
        }
        self.user_impl(account_id, distinct_id, DataType::UserUnset, map)
    }

    /// Delete the user; this operation cannot be undone.
    pub fn user_del(&self, account_id: &str) -> Result<()> {
        self.user_del_with_distinct_id(account_id, "")
    }

    /// Delete the user with explicit account and distinct IDs.
    pub fn user_del_with_distinct_id(&self, account_id: &str, distinct_id: &str) -> Result<()> {
        self.user_impl(account_id, distinct_id, DataType::UserDel, Map::new())
    }

    /// Delete the user together with final property values.
    pub fn user_del_with_properties(
        &self,
        account_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_del_with_properties_with_distinct_id(account_id, "", properties)
    }

    /// `user_del_with_properties` with explicit account and distinct IDs.
    pub fn user_del_with_properties_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserDel,
            properties.into_property_map()?,
        )
    }

    fn user_impl(
        &self,
        account_id: &str,
        distinct_id: &str,
        data_type: DataType,
        properties: Map<String, Value>,
    ) -> Result<()> {
        let data = build_data(
            account_id,
            distinct_id,
            data_type,
            "",
            "",
            self.consumer.is_stringent(),
            properties,
        )?;
        self.consumer.add(&data)
    }

    /// Deliver everything currently buffered by the consumer.
    pub fn flush(&self) -> Result<()> {
        self.consumer.flush()
    }

    /// Flush and shut the consumer down.
    pub fn close(&self) -> Result<()> {
        self.consumer.close()
    }
}

// ---------------------------------------------------------------------------
// Async facade
// ---------------------------------------------------------------------------

/// The asynchronous analytics facade, mirroring [`TDAnalytics`] with async
/// methods over the [`AsyncConsumer`] trait. Available with the `async`
/// feature.
///
/// ```
/// # #[cfg(feature = "async")]
/// # async fn demo() -> Result<(), thinkingdata_core::TDError> {
/// use thinkingdata_core::{TDAnalyticsAsync, consumer::BatchConsumer};
///
/// let consumer = BatchConsumer::new("https://receiver.thinkingdata.io", "your_app_id")?;
/// let ta = TDAnalyticsAsync::new(consumer)?;
///
/// ta.set_super_properties(serde_json::json!({ "channel": "rust" }))?;
/// ta.track("account_id_123", "purchase", serde_json::json!({ "price": 99.0 })).await?;
/// ta.close().await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "async")]
#[derive(Clone)]
pub struct TDAnalyticsAsync {
    consumer: Arc<dyn AsyncConsumer>,
    super_properties: Arc<SuperProperties>,
}

#[cfg(feature = "async")]
impl TDAnalyticsAsync {
    /// Create an analytics instance delegating delivery to `consumer`.
    pub fn new(consumer: impl AsyncConsumer + 'static) -> Result<Self> {
        crate::log::td_log_info("init SDK success");
        Ok(Self {
            consumer: Arc::new(consumer),
            super_properties: Arc::new(SuperProperties::default()),
        })
    }

    /// The injected consumer.
    pub fn consumer(&self) -> &Arc<dyn AsyncConsumer> {
        &self.consumer
    }

    /// Set common properties applied to every subsequent `track*` event.
    pub fn set_super_properties(&self, properties: impl IntoPropertyMap) -> Result<()> {
        self.super_properties.set(properties.into_property_map()?)
    }

    /// Snapshot of the currently registered super properties.
    pub fn super_properties(&self) -> Map<String, Value> {
        self.super_properties.get()
    }

    /// Remove every registered super property.
    pub fn clear_super_properties(&self) {
        self.super_properties
            .static_props
            .write()
            .expect("ThinkingData super properties poisoned")
            .clear();
    }

    /// Register a callback producing dynamic common properties. It is
    /// evaluated once per event, outside of any lock; avoid heavy work in it.
    pub fn set_dynamic_super_properties<F, P>(&self, action: F)
    where
        F: Fn() -> P + Send + Sync + 'static,
        P: IntoPropertyMap,
    {
        let action = Arc::new(move || action().into_property_map().unwrap_or_default());
        *self
            .super_properties
            .dynamic_props
            .write()
            .expect("ThinkingData super properties poisoned") = Some(action);
    }

    /// Snapshot of the dynamic super properties.
    pub fn dynamic_super_properties(&self) -> Map<String, Value> {
        self.super_properties.get_dynamic()
    }

    /// Report an ordinary event attributed to `account_id`.
    pub async fn track(
        &self,
        account_id: &str,
        event_name: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_with_distinct_id(account_id, "", event_name, properties)
            .await
    }

    /// Report an ordinary event with explicit account and distinct IDs.
    pub async fn track_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        event_name: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_impl(
            account_id,
            distinct_id,
            DataType::Track,
            event_name,
            "",
            properties,
        )
        .await
    }

    /// Report a first-check event; `first_check_id` deduplicates it.
    pub async fn track_first(
        &self,
        account_id: &str,
        event_name: &str,
        first_check_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_first_with_distinct_id(account_id, "", event_name, first_check_id, properties)
            .await
    }

    /// Report a first-check event with explicit account and distinct IDs.
    pub async fn track_first_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        event_name: &str,
        first_check_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        if first_check_id.is_empty() {
            let message = "the 'firstCheckId' must be provided";
            crate::log::td_log_info(message);
            return Err(TDError::EmptyFirstCheckId);
        }
        let mut map = properties.into_property_map()?;
        map.insert(
            "#first_check_id".to_string(),
            Value::String(first_check_id.to_string()),
        );
        self.track_impl(
            account_id,
            distinct_id,
            DataType::Track,
            event_name,
            "",
            map,
        )
        .await
    }

    /// Report an updatable event identified by `event_id`.
    pub async fn track_update(
        &self,
        account_id: &str,
        event_name: &str,
        event_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_update_with_distinct_id(account_id, "", event_name, event_id, properties)
            .await
    }

    /// Report an updatable event with explicit account and distinct IDs.
    pub async fn track_update_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        event_name: &str,
        event_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_impl(
            account_id,
            distinct_id,
            DataType::TrackUpdate,
            event_name,
            event_id,
            properties,
        )
        .await
    }

    /// Report an overridable event identified by `event_id`.
    pub async fn track_overwrite(
        &self,
        account_id: &str,
        event_name: &str,
        event_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_overwrite_with_distinct_id(account_id, "", event_name, event_id, properties)
            .await
    }

    /// Report an overridable event with explicit account and distinct IDs.
    pub async fn track_overwrite_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        event_name: &str,
        event_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.track_impl(
            account_id,
            distinct_id,
            DataType::TrackOverwrite,
            event_name,
            event_id,
            properties,
        )
        .await
    }

    async fn track_impl(
        &self,
        account_id: &str,
        distinct_id: &str,
        data_type: DataType,
        event_name: &str,
        event_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        validate_event(data_type, event_name, event_id)?;
        let custom = properties.into_property_map()?;
        let properties = compose_track_properties(
            &self.super_properties.get(),
            &self.super_properties.get_dynamic(),
            custom,
        );
        let data = build_data(
            account_id,
            distinct_id,
            data_type,
            event_name,
            event_id,
            self.consumer.is_stringent(),
            properties,
        )?;
        self.consumer.add_async(&data).await
    }

    /// Set user properties, overwriting existing values.
    pub async fn user_set(&self, account_id: &str, properties: impl IntoPropertyMap) -> Result<()> {
        self.user_set_with_distinct_id(account_id, "", properties)
            .await
    }

    /// Set user properties with explicit account and distinct IDs.
    pub async fn user_set_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserSet,
            properties.into_property_map()?,
        )
        .await
    }

    /// Set user properties only where they were not set before.
    pub async fn user_set_once(
        &self,
        account_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_set_once_with_distinct_id(account_id, "", properties)
            .await
    }

    /// `user_set_once` with explicit account and distinct IDs.
    pub async fn user_set_once_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserSetOnce,
            properties.into_property_map()?,
        )
        .await
    }

    /// Accumulate numeric user properties.
    pub async fn user_add(&self, account_id: &str, properties: impl IntoPropertyMap) -> Result<()> {
        self.user_add_with_distinct_id(account_id, "", properties)
            .await
    }

    /// `user_add` with explicit account and distinct IDs.
    pub async fn user_add_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserAdd,
            properties.into_property_map()?,
        )
        .await
    }

    /// Append values to array-typed user properties.
    pub async fn user_append(
        &self,
        account_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_append_with_distinct_id(account_id, "", properties)
            .await
    }

    /// `user_append` with explicit account and distinct IDs.
    pub async fn user_append_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserAppend,
            properties.into_property_map()?,
        )
        .await
    }

    /// Append unique values to array-typed user properties.
    pub async fn user_uniq_append(
        &self,
        account_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_uniq_append_with_distinct_id(account_id, "", properties)
            .await
    }

    /// `user_uniq_append` with explicit account and distinct IDs.
    pub async fn user_uniq_append_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserUniqAppend,
            properties.into_property_map()?,
        )
        .await
    }

    /// Clear the listed user properties.
    pub async fn user_unset<S: AsRef<str>>(&self, account_id: &str, keys: &[S]) -> Result<()> {
        self.user_unset_with_distinct_id(account_id, "", keys).await
    }

    /// Clear the listed user properties with explicit account and distinct IDs.
    pub async fn user_unset_with_distinct_id<S: AsRef<str>>(
        &self,
        account_id: &str,
        distinct_id: &str,
        keys: &[S],
    ) -> Result<()> {
        validate_unset_keys(keys)?;
        let mut properties = Map::new();
        for key in keys {
            properties.insert(key.as_ref().to_string(), Value::from(0));
        }
        self.user_impl(account_id, distinct_id, DataType::UserUnset, properties)
            .await
    }

    /// Clear user properties using a property map (values are ignored by the
    /// receiver, but preset keys keep their values, matching the Lua SDK).
    pub async fn user_unset_with_properties(
        &self,
        account_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_unset_with_properties_with_distinct_id(account_id, "", properties)
            .await
    }

    /// `user_unset_with_properties` with explicit account and distinct IDs.
    pub async fn user_unset_with_properties_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        let mut map = properties.into_property_map()?;
        if map.is_empty() {
            crate::log::td_log_info("invalid params for UserUnset: properties is nil");
            return Err(TDError::EmptyUnsetKeys);
        }
        for (key, value) in map.iter_mut() {
            if !key.starts_with('#') {
                *value = Value::from(0);
            }
        }
        self.user_impl(account_id, distinct_id, DataType::UserUnset, map)
            .await
    }

    /// Delete the user; this operation cannot be undone.
    pub async fn user_del(&self, account_id: &str) -> Result<()> {
        self.user_del_with_distinct_id(account_id, "").await
    }

    /// Delete the user with explicit account and distinct IDs.
    pub async fn user_del_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
    ) -> Result<()> {
        self.user_impl(account_id, distinct_id, DataType::UserDel, Map::new())
            .await
    }

    /// Delete the user together with final property values.
    pub async fn user_del_with_properties(
        &self,
        account_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_del_with_properties_with_distinct_id(account_id, "", properties)
            .await
    }

    /// `user_del_with_properties` with explicit account and distinct IDs.
    pub async fn user_del_with_properties_with_distinct_id(
        &self,
        account_id: &str,
        distinct_id: &str,
        properties: impl IntoPropertyMap,
    ) -> Result<()> {
        self.user_impl(
            account_id,
            distinct_id,
            DataType::UserDel,
            properties.into_property_map()?,
        )
        .await
    }

    async fn user_impl(
        &self,
        account_id: &str,
        distinct_id: &str,
        data_type: DataType,
        properties: Map<String, Value>,
    ) -> Result<()> {
        let data = build_data(
            account_id,
            distinct_id,
            data_type,
            "",
            "",
            self.consumer.is_stringent(),
            properties,
        )?;
        self.consumer.add_async(&data).await
    }

    /// Deliver everything currently buffered by the consumer.
    pub async fn flush(&self) -> Result<()> {
        self.consumer.flush_async().await
    }

    /// Flush and shut the consumer down.
    pub async fn close(&self) -> Result<()> {
        self.consumer.close_async().await
    }
}
