# 版本变化

## 模块化与可验证版本

- 逻辑服务器、数据服务器共用 TcpServer 与长度前缀实现；入口文件负责组装模块。
- 提取 ChatService、Registry、HistoryService、DataProxy、RedisStore。
- 按连接分片顺序执行请求，把断线清理放到同一工作队列。
- 增加登录 ready 阶段，避免历史读取期间参与广播；读取和格式错误时撤销登记。
- 增加有界队列、输出上限、连接上限、环境配置、连接/收发等待超时。
- 校验数据响应 request_id；移除自动重试；Redis 追加与裁剪改为原子 Lua。
- 修正数据服务器异常字段类型导致异常处理再次抛出的路径。
- 客户端退出等待响应，服务器停止时先回收工作线程再关闭 eventfd。
- 添加构建与测试脚本、GitHub Actions 和当前版本架构学习文档。

原始代码来自 [tcp-game-chat-redis 的原始提交](https://github.com/feiyun0310/tcp-game-chat-redis/commit/9372bfdf01e8f391dc08a06b41142a6502b8161f)。
本独立仓库仅保留改进版；旧代码在原仓库 Git 历史中，旧学习笔记归档在 docs/legacy/。
