# 预置属性与系统字段(TE v5.0)

[English](preset-properties.md) | 中文

总结自官方[预置属性文档](https://docs-v2.thinkingdata.cn/?code=preset_properties&lan=zh-CN&version=5.0)(TE 5.0),并说明 `thinkingdata` Rust SDK 与之的关系。

## 1. 规则

- **预置属性**由 TE 生成或取得,全部以 `#` 开头,均为事件属性,中文名与含义固定。
- **系统字段**指数据中的结构字段(如 `#account_id`、`#event_time`)或数据库中有特殊用途的字段,在分析模型中通常不直接使用。
- **除下表罗列的预置属性外,任何以 `#` 开头的属性都被视为非法字段,无法入库**——不要自造 `#` 开头的自定义属性。
- 除 `#ip` 外,不建议直接上报预置属性;仅当客户端 SDK 与其他传输方式混用、需要对齐多端属性时,在 TE 工作人员指导下设置,并务必保证属性类型与表格一致,类型不一致则值被丢弃(null)。

## 2. 预置属性

SDK 内置了这份清单,可用于程序化校验:

```rust
use thinkingdata_core::{PRESET_PROPERTIES, is_preset_property};

assert!(is_preset_property("#lib"));
assert_eq!(PRESET_PROPERTIES.len(), 48);
```

| 属性名 | 中文名 | 属性类型 | 说明 |
|---|---|---|---|
| `#ip` | 客户端IP | 文本 | 用户的 IP 地址,TE 将以此获取地理位置信息 |
| `#country` | 国家地区 | 文本 | 用户所在国家或地区,根据 IP 生成 |
| `#country_code` | 国家地区代码 | 文本 | ISO 3166-1 alpha-2(两位大写字母),根据 IP 生成 |
| `#province` | 省份 | 文本 | 用户所在省份,根据 IP 生成 |
| `#city` | 城市 | 文本 | 用户所在城市,根据 IP 生成 |
| `#os_version` | 操作系统版本 | 文本 | 如 iOS 11.2.2、Android 8.0.0 |
| `#manufacturer` | 生产商 | 文本 | 设备制造商,如 Apple、vivo |
| `#os` | 操作系统 | 文本 | 如 Android、iOS |
| `#device_id` | 设备号 | 文本 | 设备 ID(iOS 取 IDFV 或 UUID,Android 取 androidID) |
| `#screen_height` | 屏幕高度 | 数值 | 如 1920 |
| `#screen_width` | 屏幕宽度 | 数值 | 如 1080 |
| `#device_model` | 设备型号 | 文本 | 如 iPhone 8 |
| `#device_type` | 设备类型 | 文本 | 如 iPad、iPhone |
| `#app_version` | app版本 | 文本 | APP 版本 |
| `#bundle_id` | 应用包名 | 文本 | APP 包名或进程名 |
| `#lib` | SDK 类型 | 文本 | 接入 SDK 的类型——**本 SDK 上报 `Rust`** |
| `#lib_version` | SDK 版本 | 文本 | 接入 SDK 的版本——**本 SDK 上报 crate 版本** |
| `#network_type` | 网络类型 | 文本 | 事件发生时的网络状态,如 WIFI、3G、4G |
| `#carrier` | 运营商 | 文本 | 网络运营商,如中国移动、中国电信 |
| `#browser` | 浏览器 | 文本 | 浏览器类型,如 Chrome、Firefox |
| `#browser_version` | 浏览器版本 | 文本 | 如 Chrome 61.0、Firefox 57.0 |
| `#duration` | 事件时长 | 数值 | 计时功能记录的时长,单位秒 |
| `#url` | 页面地址 | 文本 | 自动采集:当前页面地址(web 为 `location.href`) |
| `#url_path` | 页面路径 | 文本 | 自动采集:当前页面路径(`location.pathname`) |
| `#referrer` | 前向地址 | 文本 | 自动采集:跳转前页面地址(`document.referrer`) |
| `#referrer_host` | 前向域名 | 文本 | 自动采集:referrer 的 host |
| `#title` | 页面标题 | 文本 | 自动采集:页面标题(`document.title`) |
| `#screen_name` | 页面名称 | 文本 | 自动采集:页面名称(Activity/ViewController 类名) |
| `#element_id` | 元素 ID | 文本 | 自动采集:控件 ID |
| `#element_type` | 元素类型 | 文本 | 自动采集:控件类型 |
| `#resume_from_background` | 是否从后台唤醒 | 布尔 | 自动采集:应用是否从后台唤醒 |
| `#element_selector` | 元素选择器 | 文本 | 自动采集:控件 viewPath |
| `#element_position` | 元素位置 | 文本 | 自动采集:控件位置信息 |
| `#element_content` | 元素内容 | 文本 | 自动采集:控件上的内容 |
| `#scene` | 场景值 | 数值 | 微信小程序启动场景值 |
| `#mp_platform` | 小程序平台 | 文本 | 应用所在平台 |
| `#app_crashed_reason` | 异常信息 | 文本 | 自动采集:APP 崩溃堆栈 |
| `#zone_offset` | 时区偏移 | 数值 | 数据时间相对 UTC 的偏移小时数 |
| `#system_language` | 系统默认语言 | 文本 | ISO 639-1(两位小写字母),如 zh、en |
| `#install_time` | App安装时间 | 时间 | 应用安装时间,来源于系统 |
| `#simulator` | 是否为模拟器 | 数值 | 设备是否为模拟器(true/false) |
| `#ram` | 内存(GB) | 文本 | 剩余/总内存,如 `1.4/2.4` |
| `#disk` | 硬盘(GB) | 文本 | 剩余/总存储,如 `30/200` |
| `#fps` | FPS | 数值 | 当前图像帧率,如 60 |
| `#background_duration` | 后台停留时长 | 数值 | 两次 start 事件间应用进入后台的时长,单位秒 |
| `#start_reason` | 启动原因 | 文本 | 非 launcher 方式启动时存在(如 deeplink) |
| `#ua` | 用户当前的代理信息 | 文本 | User Agent 字符串 |
| `#utm` | 广告系列来源属性 | 文本 | 广告来源、媒介等信息 |

服务端 SDK(如本 SDK)通常只需提供 `#ip`,其余由 TE 推导或由客户端 SDK 采集。需要时放入 `properties` 即可:

```rust
ta.track("acc", "purchase", serde_json::json!({
    "#ip": "192.168.171.111",   // 提升到线上报文头部
    "#screen_width": 1080,      // 留在 properties 内层
}))?;
```

## 3. 事件表中的系统字段

| 字段名 | 中文名 | 类型 | 说明 |
|---|---|---|---|
| `$part_event` | 事件分区字段 | 文本 | 取自 `#event_name` |
| `$part_date` | 日期分区字段 | 时间 | 取自 `#event_time` |
| `#app_id` | 项目ID | 文本 | 事件所属项目 ID |
| `#user_id` | 用户唯一 ID | 数值 | TE 系统内唯一用户标识 |
| `#account_id` | 账户 ID | 文本 | 同数据中的 `#account_id` |
| `#distinct_id` | 访客 ID | 文本 | 同数据中的 `#distinct_id` |
| `#event_name` | 事件名称 | 文本 | 同数据中的 `#event_name` |
| `#event_time` | 事件时间 | 时间 | 同数据中的 `#time` |
| `#server_time` | 服务端时间 | 时间 | 服务端收到数据的时间 |
| `#dw_create_time` | 首次入库时间 | 时间 | 首次入库时间 |
| `#dw_update_time` | 入库更新时间 | 时间 | 入库更新时间(非可更新事件为 null) |
| `#kafka_offset` | kafka偏移值 | 数值 | Kafka 偏移值 |
| `#uuid` | UUID | 文本 | 数据识别 ID |

## 4. 用户表中的系统字段

| 字段名 | 中文名 | 类型 | 说明 |
|---|---|---|---|
| `#user_id` | 用户唯一 ID | 数值 | TE 系统内唯一用户标识 |
| `#account_id` | 账户 ID | 文本 | 账户 ID |
| `#distinct_id` | 访客 ID | 文本 | 访客 ID |
| `#active_time` | 激活时间 | 时间 | 用户首条数据入库时的 `#time` |
| `#reg_time` | 注册时间 | 时间 | 用户首条含账户 ID 数据入库时的 `#time` |
| `#update_time` | 更新时间 | 时间 | 最后一条用户属性数据的 `#time` |
| `#server_time` | 服务端时间 | 时间 | 最后一条用户属性数据的服务器时间 |
| `#dw_update_time` | 入库更新时间 | 时间 | 用户数据最新更新时间 |
| `#event_date` | 最新的事件日期 | 数值 | 该用户最新一条事件入库日期 |
| `#user_operation` | 用户操作类型 | 文本 | 用户属性数据的操作类型 |
| `#kafka_offset` | kafka偏移值 | 数值 | 用户属性数据的 Kafka 偏移值 |
| `#uuid` | UUID | 文本 | 用户属性数据的识别 ID |

系统字段均由 TE 维护,无需上报。

## 5. 参考链接

- 官方预置属性(v5.0):<https://docs-v2.thinkingdata.cn/?code=preset_properties&lan=zh-CN&version=5.0>
- 数据规则:[data-format.zh-CN.md](data-format.zh-CN.md)
