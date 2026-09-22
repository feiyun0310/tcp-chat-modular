# 初学者阅读路线

不要先逐行读完 epoll。先跟着一个请求找到“谁调用谁、数据保存在哪里”。

1. **protocol.proto**：画出 ClientEnvelope 与 ServerEnvelope，区分请求响应和主动广播。
2. **logicserver.cpp**：只看三个对象如何组装：Registry、ChatService、TcpServer；
   发送回调使业务层不需要知道 epoll，worker 下标决定使用哪个 DataProxy。
3. **ChatService::Handle → OnLogin**：看参数校验、昵称占用、历史查询、失败回滚、Ready。
4. **Registry**：画出 users 和 channels 两张索引，找出每次插入/删除如何维护一致性。
5. **DataProxy → HistoryService → RedisStore**：对照 JSON 请求与 Redis key，跟完一次保存和查询。
6. **TcpServer::Impl::Read → Queue::Push → Worker**：理解网络线程如何交出一条完整请求。
7. **Send → outgoing → Drain → Flush**：理解工作线程为何不能直接修改 Connection.output。
8. **Drop → PushControl → OnDisconnected → Shutdown**：理解 fd 复用、清理顺序、线程退出。
9. **tests/**：看半包、连续请求、断线和存储失败如何被构造出来，再自己增加一个测试。

## 对照原来的函数

| 原单文件实现 | 当前模块 |
|---|---|
| 两个服务器中的 Queue | frame/blocking_queue.h |
| Session/Connection 与 DConn/Conn | frame/connection.h |
| 两份 epoll 循环、Worker、Wake、输出队列 | frame/tcp_server.cpp |
| logicserver.cpp 的 Registry | business/chat_registry.cpp |
| logicserver.cpp 的 Handle/BroadcastSystem/Error | business/chat_service.cpp |
| logicserver.cpp 的 DataProxy | storage/data_proxy.cpp |
| dataserver.cpp 的 Handle/Ok/Fail | business/history_service.cpp |
| dataserver.cpp 的 RedisProxy | storage/redis_store.cpp |

## 给别人讲这个项目时

先用 1 分钟讲三段链路，再用数据结构图讲 Session、Connection、Registry 和 Redis List。
随后拿一个登录请求讲网络线程与工作线程如何交接，最后讲发送消息与断线。
重点解释两个设计理由：为什么同一连接固定分片、为什么断线也要进入工作队列。
用集成测试结果展示行为，不把“用了 epoll”直接等同于“已经证明高并发性能”。

## 建议的小练习

- 增加频道人数查询：先改 proto，再改 ChatService，最后加测试；通常不需要改 TcpServer。
- 增加历史条数参数：明确限制，改数据协议与 Redis 查询，检查响应大小。
- 给队列增加指标：只改框架的统计，不把昵称、频道等业务状态塞进通用队列。
