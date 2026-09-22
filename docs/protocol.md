# 三段协议

## 客户端 ↔ 逻辑服务器

TCP 是字节流。每条消息是 `[4 字节无符号网络序长度][Protobuf 正文]`，长度不含头部，
最大正文 1 MiB。接收方累计 input 后循环拆帧：不完整则等待，完整则取出，剩余字节继续解析。

权威定义是 [protocol.proto](../protocol.proto)。ClientEnvelope 必须携带 1–128 字节 request_id
和一个 payload。客户端请求对应的响应保留 request_id；频道广播不对应某一个请求，request_id 为空。

| 请求 | 关键字段 | 结果 |
|---|---|---|
| login | nickname、channel_id，均为 1–64 字节 | login_response，包含 history |
| send_message | content，1–8192 字节 | send_message_response；频道成员收到 chat_message |
| quit | 无附加字段 | quit_response；其他成员收到 system_message |

长度校验按 UTF-8 字节数，不是中文字数。sender_id、channel_id、timestamp_ms 由逻辑服务器填写，
发送者不能用 send_message 请求伪造昵称。

常见业务错误：INVALID_LOGIN、ALREADY_LOGGED_IN、NICKNAME_TAKEN、NOT_LOGGED_IN、
INVALID_MESSAGE、DATA_UNAVAILABLE、UNKNOWN_REQUEST、INTERNAL。
无法解析的 Protobuf、空请求编号、未设置 payload 或超长帧会关闭连接。

## 逻辑服务器 ↔ 数据服务器

同样采用 4 字节长度前缀，正文改为 JSON。

```json
{"version":1,"request_id":"r1","action":"get_channel_history","channel_id":"lobby"}
```

```json
{"version":1,"request_id":"r2","action":"append_message","channel_id":"lobby","sender_id":"alice","content":"hello","timestamp_ms":1780000000000}
```

```json
{"version":1,"request_id":"r2","ok":true,"data":{"stored":true},"error":null}
```

```json
{"version":1,"request_id":"r2","ok":false,"data":{},"error":{"code":"REDIS_UNAVAILABLE","message":"无法连接 Redis"}}
```

查询成功时 data 是 `{"messages":[...]}`。DataProxy 同步等待，并检查响应版本与 request_id。
每个工作线程持有自己的 DataProxy，没有多请求共用一个 socket 的异步响应匹配表。

## 数据服务器 ↔ Redis

Redis key：`chat:channel:<channel_id>:messages`，value 为 List，每项是完整聊天消息的 JSON。
LRANGE 读取历史；EVAL 脚本原子执行 RPUSH 与 LTRIM，保留最近 100 条。

请求编号用于关联响应，**不是幂等键**。自动重试已取消，但如果发送完成后响应丢失，
调用方仍不能知道写入是否成功。若将来加入自动重试，应先设计幂等键、去重范围和有效期。
