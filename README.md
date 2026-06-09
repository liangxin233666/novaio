# NovaIO

NovaIO 是一个基于 C++ 协程和 Linux `io_uring` 的异步网络运行时原型，面向高并发后台接入层、长连接网关和 RPC/HTTP 服务场景。项目目标是把 `io_uring` 完成事件、用户态协程调度、跨核唤醒、定时器和协程同步原语打通，让业务代码可以用接近同步的 `co_await accept/recv/send` 风格表达异步 I/O。

## 核心能力

- 自研 `Task<T>` / `Task<void>` / `DetachedTask` 协程抽象，支持 continuation 恢复、异常传播和协程帧自定义分配。
- 每核一个 `Scheduler`，每个调度线程独立持有本地队列、`io_uring`、五级时间轮和线程本地 `mimalloc heap`。
- 基于 `io_uring` 封装异步 `accept/recv/send/close`，使用 `user_data` 低位 tag 统一分发普通 I/O、multishot 事件、跨核唤醒和 inbox 消息。
- 引入 Direct FD、Provided Buffer Ring、multishot accept/recv 和 `msg_ring`，降低 fd 查找、buffer 分配、重复 SQE 提交和跨线程唤醒成本。
- 提供 `CoMutex`、`Event`、`when_all`、时间轮、metrics、综合测试和 wrk 压测脚本。

## 工程化入口

依赖：

- LLVM/Clang 22+
- libc++
- liburing
- mimalloc
- libnuma
- Ninja

构建与测试：

```bash
cmake --preset dev
cmake --build --preset dev
ctest --preset dev
```

如果环境没有安装独立 `ctest` 命令，可以直接运行：

```bash
./out/dev/test_framework
timeout 10s ./out/dev/comprehensive_test
```

性能构建：

```bash
cmake --preset release
cmake --build --preset release
```

Sanitizer 构建：

```bash
cmake --preset asan
cmake --build --preset asan
ctest --preset asan
```

HTTP server 示例：

```bash
NOVAIO_CORES=8 NOVAIO_LISTENERS=8 ./out/release/http_server
```

本机 wrk 对比脚本：

```bash
WRK_THREADS=8 WRK_CONNECTIONS=512 WRK_DURATION=10s NODE_WORKERS=8 ./scripts/bench/wrk_compare.sh
```
