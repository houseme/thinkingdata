# 数据规则(TE v5.0)

[English](data-format.md) | 中文

本文档描述 ThinkingData(TE)接收端要求的数据格式,以及 `thinkingdata` Rust SDK 如何生成该格式。内容总结自 TE 5.0 官方[数据规则](https://docs-v2.thinkingdata.cn/?code=data_format&lan=zh-CN&version=5.0),并将每条规则映射到 Rust API。

## 1. 结构:一行一条 JSON

接收端接受 NDJSON:**一行一条 JSON 数据**,每行对应用户的一次行为(事件)或一次用户属性操作。

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

用户属性操作的数据结构相同,只是 `#type` 替换为 `user_set`、`user_setOnce`、`user_add`、`user_unset`、`user_append`、`user_uniq_append` 或 `user_del`。

每条数据分为两部分:

1. **数据信息部分(Header)**——与 `properties` 同层的字段(均以 `#` 开头)。
2. **数据主体部分(Body)**——`properties` 对象:事件属性或需要设置的用户属性。

## 2. 数据信息部分字段

| 字段 | 含义 | 是否必须 | 说明 |
|---|---|---|---|
| `#account_id` | 账号 ID(登录态身份) | 与 `#distinct_id` 至少传一个 | 长度 ≤ 128 字符 |
| `#distinct_id` | 访客 ID(匿名身份) | | 长度 ≤ 128 字符 |
| `#type` | 数据类型 | 必须 | 见下表 |
| `#event_name` | 事件名称 | `#type` = `track` 时必须 | 字母开头,仅小写字母/数字/`_`,≤ 50 字符 |
| `#time` | 触发时间 | 必须 | `yyyy-MM-dd HH:mm:ss` 或 `yyyy-MM-dd HH:mm:ss.SSS` |
| `#ip` | 触发地点 | 可选 | TE 据此计算地理位置;若传入 `#country`/`#province`/`#city`,以传入值为准 |
| `#uuid` | 数据唯一 ID | 可选 | 标准_uuid 格式;接收端据此对近几小时数据去重 |

除以上字段外,本 Rust SDK 与官方 Go SDK 一致,还会把以下 4 个官方 SDK 头部字段从 `properties` 中提升到顶层(在 properties 中传入即可):

| 字段 | 含义 |
|---|---|
| `#app_id` | 本条数据所属项目 ID 覆盖 |
| `#first_check_id` | 首日事件去重标识(`track_first` 使用) |
| `#transaction_property` | 可更新/可覆盖事件的交易属性 |
| `#import_tool_id` | 导入工具标识 |

其余以 `#` 开头的属性必须放在 `properties` 内层,且只有[预置属性](preset-properties.zh-CN.md)合法——未知 `#` 开头的字段无法入库。

### SDK 如何填充数据信息部分

- `#time`——默认取当前本地时间(基于 `jiff`);若在 properties 中传入 `#time`,字符串原样使用,其他类型回退为当前时间。
- `#uuid`——默认生成随机 UUID v4;传入 `#uuid` 则使用传入值。
- `#ip`、`#app_id`、`#first_check_id`、`#transaction_property`、`#import_tool_id`——从传入的 properties 中提取并移出属性表。
- 序列化后的报文中内嵌的 RFC 3339 时间戳(如嵌套对象里的)会在提交前重写为 `yyyy-MM-dd HH:mm:ss.SSS`,与 Go SDK 的 `parseTime` 一致。

## 3. 数据类型

| `#type` | 操作 | Rust API(blocking/async 同名) |
|---|---|---|
| `track` | 写入一个事件 | `track`、`track_first`(写入 `#first_check_id`)、`track_update`(`#event_id`)、`track_overwrite`(`#event_id`) |
| `user_set` | 覆盖用户属性(不存在则新建) | `user_set` |
| `user_setOnce` | 初始化用户属性(已有值则忽略) | `user_set_once` |
| `user_add` | 数值型用户属性累加(未赋值先置 0;负值相当于相减) | `user_add`——值必须为数字,SDK 强制校验 |
| `user_unset` | 清空列出的用户属性(置 NULL;不存在的属性不会新建) | `user_unset`(线上传 0 值)、`user_unset_with_properties` |
| `user_append` | 为列表型用户属性追加元素 | `user_append` |
| `user_uniq_append` | 追加并对全列表去重(保持原有元素顺序) | `user_uniq_append` |
| `user_del` | 删除该用户(事件数据保留) | `user_del`、`user_del_with_properties` |

每个操作都有 `*_with_distinct_id` 变体,参数形如 `(account_id, distinct_id, …)`,可同时指定两个身份。

用户表操作按**数据到达顺序**执行,与 `#time` 无关;重传历史 `user_set` 数据仍会照常覆盖。用户属性操作不会产生行为记录,因此无法在行为分析模型(除 SQL 查询)中直接查询。

## 4. 属性取值类型与限制

属性类型由**第一次**收到该属性时的值决定,后续类型不一致的值将被丢弃(该条数据其余符合类型的属性保留),TE 不会做类型兼容转换。

| TE 类型 | 示例 | 限制 | JSON 表示 |
|---|---|---|---|
| 数值 | `123`、`1.23` | −9E15 … 9E15 | number |
| 文本 | `"ABC"`、`"上海"` | ≤ 2KB | string |
| 时间 | `"2019-01-01 00:00:00.000"` | `yyyy-MM-dd HH:mm:ss[.SSS]`;日期可用 `yyyy-MM-dd 00:00:00` | string |
| 布尔 | `true`、`false` | — | bool |
| 列表 | `["a","1","true"]` | ≤ 500 个元素;元素转为字符串,每个 ≤ 255 字节 | array |
| 对象 | `{"hero_name":"刘备","hero_level":22}` | ≤ 100 个子属性;子属性遵循上述规则 | object |
| 对象组 | `[{...},{...}]` | ≤ 500 个对象;每个对象 ≤ 100 个子属性 | 对象数组 |

### SDK 的预检校验

```rust
use thinkingdata_core::utils::{check_value_limits, is_documented_time_format};
use thinkingdata_core::PRESET_PROPERTIES;

// 按文档限制校验单个取值(字符串 2KB、数值 ±9E15、列表 500 元素、
// 列表元素 255 字节、对象 100 子属性)。
check_value_limits("price", &serde_json::json!(9.9))?;

// 或校验整条数据:#time 格式、ID 长度(≤128)以及所有属性取值。
data.check_limits()?;
```

debug consumer 会自动执行 `check_limits`(它是**严格模式** consumer),开发期即可发现非法 key 与超限取值,而不是被接收端静默丢弃。batch 与 log consumer 保持与官方 SDK 一致,交由接收端处理。

## 5. 命名规则

- **事件名**:字母开头;仅小写字母、数字和 `_`;≤ 50 字符;不能带空格。(SDK 的 key 校验模式与 Go SDK 一致,也接受大写;TE 后台属性名不区分大小写,`Purchase` 与 `purchase` 会视为同一属性。)
- **属性名**:同上;仅预置属性可以 `#` 开头。
- `#account_id` / `#distinct_id`:≤ 128 字符(TE 3.1 及之后创建的项目)。
- 编码:始终使用 UTF-8。

## 6. 数量与时间限制

| 限制 | 建议不超过 | 硬件上限 |
|---|---|---|
| 事件种类 | 100 | 500 |
| 事件属性 | 300 | 1000 |
| 用户属性 | 100 | 500 |

- 服务端数据接收时间范围:`[服务器时间 − 3 年, + 3 天]`(客户端:− 10 天);超期数据不录入。
- 接收端会对近几个小时内相同 `#uuid` 的数据去重(解决网络抖动导致的短时重复,非全量去重)。

## 7. 常见问题

1. **后台没有收到数据**——检查 APPID、接收 URL 及 `/sync_server` 后缀;使用 LogBus 时确保一行一条 JSON,数据信息部分 key 以 `#` 开头且无遗漏。
2. **属性缺失**——首次上报时的类型决定属性类型,后续类型不一致的值被丢弃;可在后台元数据管理中查看属性类型。
3. **`#` 开头的自定义 key 丢失**——只有[预置属性](preset-properties.zh-CN.md)能以 `#` 开头,其余被视为非法字段。
4. **行为模型中看不到用户属性操作**——它们只修改用户表。
5. **超过三年的数据**不会被录入。

## 8. 本 SDK 使用的接收端接口

| Consumer | 接口 | 报文 |
|---|---|---|
| `BatchConsumer` | `{server_url}/sync_server` | 事件 JSON 数组;默认 gzip 压缩;头 `appid`、`compress`、`TA-Integration-*`;响应 `{"code": 0}`(`-1` 数据格式错误、`-2` APP ID 不存在、`-3` IP 传输有误);非 200 重试 3 次;失败批次进入缓存 |
| `DebugConsumer` | `{server_url}/data_debug` | 表单字段 `data`、`appid`、`source=server`、`dryRun`、可选 `deviceId`;响应 `{"errorLevel": n}` |
| `LogConsumer` | 本地 NDJSON 文件 | 后续由 LogBus 读取 |

## 9. 参考链接

- 官方数据规则(v5.0):<https://docs-v2.thinkingdata.cn/?code=data_format&lan=zh-CN&version=5.0>
- 官方预置属性(v5.0):<https://docs-v2.thinkingdata.cn/?code=preset_properties&lan=zh-CN&version=5.0>
- Go SDK(线上行为参考):<https://github.com/ThinkingDataAnalytics/go-sdk>
