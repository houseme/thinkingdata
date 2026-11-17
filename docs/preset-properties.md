# Preset Properties & System Fields (TE v5.0)

English | [中文](preset-properties.zh-CN.md)

Summarizes the official [preset properties documentation](https://docs-v2.thinkingdata.cn/?code=preset_properties&lan=zh-CN&version=5.0)
(TE 5.0) and how the `thinkingdata` Rust SDK interacts with it.

## 1. Rules

- **Preset properties** are generated or obtained by TE. All of them start with `#` and are event
  properties with fixed Chinese display names and meanings.
- **System fields** are structural fields of the data (`#account_id`, `#event_time`, …) or fields
  with special purposes in the database; they are rarely used directly in analysis models.
- **Any `#`-prefixed property outside the list below is illegal and will not be stored** — do not
  invent custom `#` keys.
- Except for `#ip`, you should not set preset properties yourself unless you need to align
  multi-source data (client SDK + server transfer) under TE staff guidance. Types must match the
  table exactly, otherwise the value is dropped (null).

## 2. Preset properties

The SDK ships this list as a constant, so you can validate keys programmatically:

```rust
use thinkingdata_core::{PRESET_PROPERTIES, is_preset_property};

assert!(is_preset_property("#lib"));
assert_eq!(PRESET_PROPERTIES.len(), 48);
```

| Property | Chinese name | Type | Description |
|---|---|---|---|
| `#ip` | 客户端IP | string | User IP address; TE derives geo information from it |
| `#country` | 国家地区 | string | Country/region, derived from IP |
| `#country_code` | 国家地区代码 | string | ISO 3166-1 alpha-2 (two uppercase letters), derived from IP |
| `#province` | 省份 | string | Province, derived from IP |
| `#city` | 城市 | string | City, derived from IP |
| `#os_version` | 操作系统版本 | string | e.g. iOS 11.2.2, Android 8.0.0 |
| `#manufacturer` | 生产商 | string | Device manufacturer, e.g. Apple, vivo |
| `#os` | 操作系统 | string | e.g. Android, iOS |
| `#device_id` | 设备号 | string | Device ID (IDFV/UUID on iOS, androidID on Android) |
| `#screen_height` | 屏幕高度 | number | e.g. 1920 |
| `#screen_width` | 屏幕宽度 | number | e.g. 1080 |
| `#device_model` | 设备型号 | string | e.g. iPhone 8 |
| `#device_type` | 设备类型 | string | e.g. iPad, iPhone |
| `#app_version` | app版本 | string | Your app's version |
| `#bundle_id` | 应用包名 | string | App bundle ID or process name |
| `#lib` | SDK 类型 | string | SDK type, e.g. Android, iOS — **this SDK sends `Rust`** |
| `#lib_version` | SDK 版本 | string | SDK version — **this SDK sends its crate version** |
| `#network_type` | 网络类型 | string | Network state at event time, e.g. WIFI, 3G, 4G |
| `#carrier` | 运营商 | string | Mobile carrier, e.g. 中国移动, 中国电信 |
| `#browser` | 浏览器 | string | Browser type, e.g. Chrome, Firefox |
| `#browser_version` | 浏览器版本 | string | e.g. Chrome 61.0, Firefox 57.0 |
| `#duration` | 事件时长 | number | Duration recorded by timers, in seconds |
| `#url` | 页面地址 | string | Auto-collection: current page URL (`location.href` on web) |
| `#url_path` | 页面路径 | string | Auto-collection: current page path (`location.pathname`) |
| `#referrer` | 前向地址 | string | Auto-collection: previous page address (`document.referrer`) |
| `#referrer_host` | 前向域名 | string | Auto-collection: host of `referrer` |
| `#title` | 页面标题 | string | Auto-collection: page title (`document.title`) |
| `#screen_name` | 页面名称 | string | Auto-collection: page name (Activity/ViewController class name) |
| `#element_id` | 元素 ID | string | Auto-collection: control ID |
| `#element_type` | 元素类型 | string | Auto-collection: control type |
| `#resume_from_background` | 是否从后台唤醒 | bool | Auto-collection: app resumed from background |
| `#element_selector` | 元素选择器 | string | Auto-collection: control viewPath |
| `#element_position` | 元素位置 | string | Auto-collection: control position |
| `#element_content` | 元素内容 | string | Auto-collection: control content |
| `#scene` | 场景值 | number | WeChat mini-program launch scene |
| `#mp_platform` | 小程序平台 | string | Platform hosting the mini-program |
| `#app_crashed_reason` | 异常信息 | string | Auto-collection: crash stack trace |
| `#zone_offset` | 时区偏移 | number | UTC offset in hours of the data time |
| `#system_language` | 系统默认语言 | string | ISO 639-1 (two lowercase letters), e.g. zh, en |
| `#install_time` | App安装时间 | time | App install time, from the system |
| `#simulator` | 是否为模拟器 | number | Whether the device is a simulator (true/false) |
| `#ram` | 内存(GB) | string | Remaining/total RAM in GB, e.g. `1.4/2.4` |
| `#disk` | 硬盘(GB) | string | Remaining/total disk in GB, e.g. `30/200` |
| `#fps` | FPS | number | Current frame rate, e.g. 60 |
| `#background_duration` | 后台停留时长 | number | Background time between two `start` events, in seconds |
| `#start_reason` | 启动原因 | string | Present when the app was started non-launcher (e.g. deeplink) |
| `#ua` | 用户当前的代理信息 | string | User agent string |
| `#utm` | 广告系列来源属性 | string | Campaign source info (source, medium, …) |

Server-side SDKs (like this one) typically only supply `#ip`; everything else is derived by TE or
collected by client SDKs. Pass them in `properties` when needed:

```rust
ta.track("acc", "purchase", serde_json::json!({
    "#ip": "192.168.171.111",   // hoisted to the wire header
    "#screen_width": 1080,      // stays in properties
}))?;
```

## 3. System fields in the event table

| Field | Chinese name | Type | Description |
|---|---|---|---|
| `$part_event` | 事件分区字段 | string | Partition field, from `#event_name` |
| `$part_date` | 日期分区字段 | time | Partition field, from `#event_time` |
| `#app_id` | 项目ID | string | Project ID the event belongs to |
| `#user_id` | 用户唯一 ID | number | TE-internal unique user ID |
| `#account_id` | 账户 ID | string | Same as the data's `#account_id` |
| `#distinct_id` | 访客 ID | string | Same as the data's `#distinct_id` |
| `#event_name` | 事件名称 | string | Same as the data's `#event_name` |
| `#event_time` | 事件时间 | time | Same as the data's `#time` |
| `#server_time` | 服务端时间 | time | Time the server received the data |
| `#dw_create_time` | 首次入库时间 | time | First storage time |
| `#dw_update_time` | 入库更新时间 | time | Storage update time (null for non-updatable events) |
| `#kafka_offset` | kafka偏移值 | number | Kafka offset |
| `#uuid` | UUID | string | Data identification ID |

## 4. System fields in the user table

| Field | Chinese name | Type | Description |
|---|---|---|---|
| `#user_id` | 用户唯一 ID | number | TE-internal unique user ID |
| `#account_id` | 账户 ID | string | Account ID |
| `#distinct_id` | 访客 ID | string | Distinct ID |
| `#active_time` | 激活时间 | time | `#time` of the user's first stored data |
| `#reg_time` | 注册时间 | time | `#time` of the user's first stored data containing an account ID |
| `#update_time` | 更新时间 | time | `#time` of the last received user-property data |
| `#server_time` | 服务端时间 | time | Server time of the last received user-property data |
| `#dw_update_time` | 入库更新时间 | time | Latest user-data update time |
| `#event_date` | 最新的事件日期 | number | Latest stored event date for the user |
| `#user_operation` | 用户操作类型 | string | Operation type of the user-property data |
| `#kafka_offset` | kafka偏移值 | number | Kafka offset |
| `#uuid` | UUID | string | Identification ID of the user-property data |

These system fields are maintained by TE; you never send them.

## 5. References

- Official preset properties (v5.0): <https://docs-v2.thinkingdata.cn/?code=preset_properties&lan=zh-CN&version=5.0>
- Data rules: [data-format.md](data-format.md)
