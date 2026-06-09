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

## 面试讲法

一句话：

> 我实现了一个 C++ 协程网络 runtime。每个调度线程持有独立的 `io_uring`、本地任务队列、时间轮和线程本地 allocator；业务协程发起 I/O 后挂起，内核完成事件到达后 runtime 通过 CQE `user_data` 找回上下文并恢复 coroutine handle。

建议重点讲四条主线：

- 协程链路：`promise_type`、`initial_suspend`、`final_suspend`、continuation、异常传播、协程帧分配。
- 调度链路：每核 scheduler、本地队列、批量 resume、runnext fast path、跨核 inbox + `msg_ring` 唤醒。
- I/O 链路：SQE 提交、CQE 分发、Direct FD、Provided Buffer Ring、multishot、buffer 归还。
- 工程链路：CMake Presets、CTest、Sanitizer、metrics、wrk 对比压测和生产化短板复盘。

更完整的 STAR 话术见：

- `NOVAIO_RESUME_STAR.md`
- `NOVAIO_PROJECT_ONEPAGE_STAR.md`
- `NOVAIO_INTERVIEW_GUIDE.md`
- `docs/ENGINEERING_UPGRADE.md`
