# TCP 频道聊天室 · C++ / epoll / Protobuf / Redis

[![Linux build and tests](https://github.com/feiyun0310/tcp-chat-modular/actions/workflows/ci.yml/badge.svg)](https://github.com/feiyun0310/tcp-chat-modular/actions/workflows/ci.yml)

一个可以运行、测试和逐层阅读的 Linux C++ 服务端项目。客户端加入频道后可以聊天、
接收成员加入/离开通知，并在再次登录时读取频道最近 100 条消息。

项目重点是把 **TCP 框架、聊天业务、数据访问** 分开，并说明网络线程与工作线程如何协作。
保留客户端 → 逻辑服务器 → 数据服务器 → Redis 的原有链路，同时让两个服务器共用网络框架。

```mermaid
flowchart LR
    C[终端客户端] <-->|长度前缀 + Protobuf| L[逻辑服务器]
    L <-->|长度前缀 + JSON| D[数据服务器]
    D <-->|RESP / Lua| R[(Redis 最近 100 条消息)]
```

## 这版完成了什么

- **框架复用**：epoll、连接、拆包、发送缓冲区、eventfd、工作队列集中在 `frame/`。
- **业务边界清楚**：`ChatService` 处理登录/发消息/退出；`HistoryService` 校验历史查询与保存请求。
- **同一连接保持顺序**：按 Session ID 固定分配工作线程，请求与断线事件进入同一个 FIFO。
- **状态生命周期完整**：登录失败回滚；历史就绪后才参与广播；断线清理排在在途请求之后。
- **资源和失败处理**：有界队列、连接数和帧大小限制、慢接收端输出上限、下游连接/收发超时。
- **避免隐式重复写入**：不自动重试不确定的写请求；Redis 用 Lua 原子执行追加与裁剪。
- **可重复验证**：构建脚本、单元测试、独立 Redis 集成测试和 GitHub Actions。

## 快速运行

环境：Linux，C++17，g++，Protobuf 编译器与 C++ 开发库，nlohmann/json，Redis。
CI 使用 Ubuntu 24.04。Windows 请在 Linux 环境（如 WSL）运行，代码使用 epoll/eventfd。

```bash
sudo apt-get update
sudo apt-get install -y g++ protobuf-compiler libprotobuf-dev nlohmann-json3-dev redis-server python3-protobuf
git clone https://github.com/feiyun0310/tcp-chat-modular.git
cd tcp-chat-modular
bash build.sh
```

先确认 `redis-cli ping` 返回 `PONG`；如果 Redis 尚未运行，在一个终端执行 `redis-server`。
再分别打开终端运行：

```bash
./bin/dataserver   # 数据服务器：127.0.0.1:8888
./bin/logicserver  # 逻辑服务器：127.0.0.1:9999
./bin/client      # 客户端，按提示输入昵称和频道
```

打开第二个客户端，用不同昵称加入相同频道。输入消息后两个客户端都能收到广播；
输入 `/quit` 退出，客户端会等待退出响应，最多等待 3 秒。

```text
客户端 A                         客户端 B
昵称: alice                      昵称: bob
频道: lobby                      频道: lobby
hello                            [时间] alice: hello
/quit                            [系统] alice 离开了频道
```

`protocol.pb.h/.cc` 和 Python 协议文件由 `build.sh` 生成到 `build/generated/`，
不提交生成文件；构建时使用本机配套的 protoc 和 Protobuf 开发库。

## 运行测试

```bash
PYTHON=/usr/bin/python3 bash test.sh
```

测试会启动独立 Redis 和服务器，使用动态端口和临时目录，结束后关闭它们；不会清空已有 Redis。
单元测试检查队列、拆包和聊天业务；集成测试使用真实 TCP、Protobuf、JSON 与 Redis。
详细覆盖范围和限制见 [测试说明](docs/testing.md)，执行结果以 [Actions](https://github.com/feiyun0310/tcp-chat-modular/actions) 为准。

## 从哪里读代码

```text
.
├── client.cpp                    客户端交互和响应显示
├── protocol.proto                客户端协议
├── logicserver.cpp               组装框架、ChatService、DataProxy
├── dataserver.cpp                组装框架、HistoryService、RedisStore
├── frame/
│   ├── tcp_server.h/.cpp         网络循环、固定分片工作线程、退出清理
│   ├── socket_io.h/.cpp          帧编码、拆包、阻塞连接和收发
│   ├── connection.h             Session、Connection
│   ├── blocking_queue.h         有界 FIFO 工作队列
│   └── config.h                 环境变量读取与校验
├── business/
│   ├── chat_registry.h/.cpp      在线用户、频道成员、登录就绪状态
│   ├── chat_service.h/.cpp       登录、发消息、退出、断线
│   └── history_service.h/.cpp    数据服务器请求校验和响应
├── storage/
│   ├── history_store.h          聊天业务依赖的存储接口
│   ├── data_proxy.h/.cpp        通过 TCP 调用数据服务器
│   └── redis_store.h/.cpp       Redis RESP 与消息存取
├── tests/                       单元测试与真实服务集成测试
├── docs/                        架构、协议、学习路线、测试与演进
├── build.sh / test.sh
└── .github/workflows/ci.yml
```

初学者建议先读 `protocol.proto → logicserver.cpp → ChatService → Registry → DataProxy`，
走通一个登录请求后再进入网络框架。详见 [学习路线](docs/learning-guide.md)。

## 配置

| 环境变量 | 默认值 | 用途 |
|---|---|---|
| `CHAT_LOGIC_BIND` | `127.0.0.1` | 逻辑服务器监听 IPv4 地址 |
| `CHAT_LOGIC_HOST` | `127.0.0.1` | 客户端连接的逻辑服务器地址 |
| `CHAT_LOGIC_PORT` | `9999` | 逻辑服务器端口 |
| `CHAT_DATA_BIND` | `127.0.0.1` | 数据服务器监听地址 |
| `CHAT_DATA_HOST` | `127.0.0.1` | 逻辑服务器访问的数据服务器地址 |
| `CHAT_DATA_PORT` | `8888` | 数据服务器端口 |
| `CHAT_REDIS_HOST` | `127.0.0.1` | Redis 地址 |
| `CHAT_REDIS_PORT` | `6379` | Redis 端口 |
| `CHAT_WORKERS` | `4` | 每个服务器的工作线程数，范围 1–32 |
| `CHAT_IO_TIMEOUT_MS` | `2000` | 下游连接超时及每次阻塞收发等待上限 |

地址使用 IPv4 字面量，当前不做 DNS 解析。端口范围 1–65535，非法配置会报错并退出。
例如换一个聊天端口，需要同时给逻辑服务器与客户端设置 `CHAT_LOGIC_PORT=19999`。

## 设计边界

这是面向学习和作品展示的单实例频道聊天服务。没有账号鉴权、TLS、跨逻辑服务器的用户状态共享，
也没有消息去重、离线投递和端到端 exactly-once 保证；昵称唯一只表示当前实例内的在线状态。
同连接请求有序，不保证不同连接的全局消息顺序。同步数据访问会阻塞该连接所在工作线程，
因此同一分片上的其他连接也会等待。超时是连接/单次 socket 等待上限，不是整个业务的绝对截止时间。
目前按完全断开处理 TCP 半关闭；队列或输出缓冲区超限时关闭连接，不承诺保留在途消息。

Redis 的持久化取决于部署时的 Redis 配置，本项目的 `RPUSH` 成功不等于磁盘持久化完成。
本仓库没有虚构吞吐量、并发量或线上可用性指标。

继续阅读：[架构与线程流程](docs/architecture.md) · [协议](docs/protocol.md) ·
[测试](docs/testing.md) · [版本变化](CHANGELOG.md)

## 项目来源

本仓库是 [tcp-game-chat-redis](https://github.com/feiyun0310/tcp-game-chat-redis) 的独立改进版，
基于已通过测试的 [模块化提交](https://github.com/feiyun0310/tcp-game-chat-redis/commit/252de38752edcd82a0d0afe7db7385a504ffad78) 整理。
原仓库与旧版实现保留，当前项目在本仓库独立维护。
