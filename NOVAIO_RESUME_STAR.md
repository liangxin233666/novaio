# NovaIO 简历项目经历 STAR 版本

## 压测说明

本机初步压测使用 `wrk`，脚本位于：

- `scripts/bench/wrk_compare.sh`
- `scripts/bench/node_http_server.js`

当前 16 逻辑线程 / 8 物理核心机器上的公平压测采用物理核心对齐参数：

```text
physical cores  : 8
server workers  : 8
wrk threads     : 8
wrk connections : 512
wrk duration    : 10s
Node workers    : 8
NovaIO server   : build/http_server
Baseline        : Node.js/libuv HTTP cluster
```

压测结果：

```text
NovaIO:
  Requests/sec : 113,024.42
  Avg latency  : 4.51 ms
  Requests     : 1,140,646 / 10.09s

Node/libuv cluster:
  Requests/sec : 77,482.68
  Avg latency  : 8.62 ms
  Requests     : 782,453 / 10.10s
```

结论表述建议：

> 在本机 8 物理核环境下，服务端 worker 数对齐为 `8`，使用 `wrk -t8 -c512 -d10s` 初步压测，NovaIO HTTP server 达到约 `11.3w req/s`，相比 8 worker Node/libuv HTTP cluster 的约 `7.7w req/s`，吞吐提升约 `46%`。该数据用于展示本机原型验证结果，不等同于严格生产 benchmark。

## 简历项目经历：完整版

**NovaIO：基于 C++ 协程与 io_uring 的高性能异步网络运行时**

技术栈：`C++20/26`、`Coroutine`、`io_uring`、`liburing`、`mimalloc`、`多线程调度`、`时间轮`、`无锁队列`、`Linux 网络编程`

**项目背景（Situation）**：传统 epoll + 回调模型在高并发网络服务中容易出现回调链复杂、连接状态分散、跨线程调度成本高和内存分配抖动等问题；企业级后台接入层还需要处理大量连接、异步收发、心跳超时和跨线程任务投递。

**项目任务（Task）**：设计并实现一个基于 C++ 协程和 Linux io_uring 的异步网络 runtime，使业务逻辑能够用 `co_await recv -> co_await send` 的同步风格表达异步 I/O，同时探索高并发场景下的调度、内存和 I/O 优化手段。

**核心行动（Action）**：

- 自研 `Task<T>` / `Task<void>` / `DetachedTask` 协程抽象，实现 `promise_type`、`initial_suspend`、`final_suspend`、continuation 恢复和异常传播，打通协程调用链。
- 设计每核一个 `Scheduler` 的调度模型，每个调度线程独立维护本地任务队列、`io_uring`、时间轮和线程本地 `mimalloc heap`，并结合 CPU affinity、NUMA affinity 降低共享竞争和延迟抖动。
- 基于 io_uring 实现异步 `accept/recv/send/close`，通过 SQE 提交 I/O 操作，通过 CQE 的 `user_data` 找回 `IoContext` 并恢复挂起协程；使用低位 tag 统一分发普通 I/O、multishot 事件、跨核协程唤醒和 inbox 消息。
- 引入 Direct FD、Provided Buffer Ring、multishot accept/recv 和 msg_ring：Direct FD 减少 fd table 查找，Buffer Ring 预注册 `2048` 个 `2KB` buffer 降低 recv 分配开销，multishot 减少重复提交 SQE，msg_ring 解决跨 Scheduler 唤醒问题。
- 实现五级层次时间轮，支持 `sleep_for`、超时唤醒和协程定时恢复；实现 `CoMutex`、`Event`、`when_all` 等协程同步和组合并发能力。
- 构建综合测试与 wrk 压测脚本，覆盖协程返回值与异常传播、时间轮、`when_all`、`CoMutex`、`Event`、SPSC 队列、序列化、Direct FD、Provided Buffer Ring 和 TCP echo 链路。

**结果（Result）**：

- 打通了从协程调度、异步 I/O、跨核投递、定时器到同步原语的完整 runtime 链路。
- 综合测试覆盖 `9` 项核心功能并全部通过，包括协程异常传播、时间轮、同步原语、Direct FD、Provided Buffer Ring 和 TCP echo。
- 本机 8 物理核环境下，服务端 worker 数对齐为 `8`，使用 `wrk -t8 -c512 -d10s` 初步压测，NovaIO HTTP server 达到约 `11.3w req/s`；同参数下 8 worker Node/libuv HTTP cluster 约 `7.7w req/s`，NovaIO 吞吐高约 `46%`。
- 复盘并明确生产化改进方向，包括 MPSC 队列替换、I/O cancel 语义、连接生命周期管理、buffer RAII、背压、限流、可观测性和 p99 延迟分析。

## 简历项目经历：精简版

**NovaIO：基于 C++ 协程与 io_uring 的异步网络运行时**

**S - 情境**：面向高并发后台接入层、长连接网关和异步网络服务，传统 epoll + 回调模型容易出现回调链复杂、连接状态分散、跨线程唤醒成本高、buffer 分配频繁和延迟抖动等问题。

**T - 任务**：独立设计一个基于 C++ 协程和 Linux io_uring 的异步网络 runtime，让网络 I/O 可以用 `co_await accept/recv/send` 的同步风格编写，同时验证每核调度、零拷贝倾向 buffer 管理、跨核唤醒和多线程扩展能力。

**A - 行动**：

- 做了什么：自研 `Task<T>`、`Task<void>`、`DetachedTask` 协程抽象，实现 `promise_type`、`initial_suspend`、`final_suspend`、continuation 恢复、异常传播和协程帧自定义分配，打通从业务协程到 runtime 调度的执行链路。
- 亮点：设计每核一个 `Scheduler` 的多线程调度模型，每个线程独立维护本地任务队列、`io_uring`、五级时间轮和线程本地 `mimalloc heap`，并结合 CPU affinity / NUMA affinity 减少共享竞争、跨 NUMA 内存访问和延迟抖动。
- 难点：基于 io_uring 封装异步 `accept/recv/send/close`，通过 SQE 提交请求，通过 CQE `user_data` 找回 `IoContext` 并恢复挂起协程；同时用低位 tag 区分普通 I/O、multishot 事件、跨核协程唤醒和 inbox 消息，解决同一个 CQE 分发入口下的多类型事件调度问题。
- 优化：引入 Direct FD、Provided Buffer Ring、multishot accept/recv 和 `msg_ring`；Direct FD 降低 fd table 查找成本，Buffer Ring 预注册 buffer 减少 recv 分配，multishot 减少重复提交 SQE，`msg_ring` 用于唤醒目标 Scheduler，降低跨线程任务投递延迟。
- 完整性：实现 `CoMutex`、`Event`、`when_all`、基础 metrics 和综合测试，覆盖协程返回值、异常传播、时间轮、同步原语、SPSC 队列、Direct FD、Provided Buffer Ring 和 TCP echo 链路。

**R - 结果**：综合测试 `9` 项核心功能全部通过；本机 8 物理核环境下，使用 `wrk -t8 -c512 -d10s` 初步压测，NovaIO HTTP server 达到约 `11.3w req/s`，相比 8 worker Node/libuv HTTP cluster 的约 `7.7w req/s` 吞吐高约 `46%`。项目最终形成了一个包含协程调度、异步 I/O、跨核投递、定时器和同步原语的完整 runtime 原型，也沉淀了 MPSC 队列、I/O cancel、背压、资源生命周期和可观测性的生产化改进方向。

## 面试 STAR 口述版

**S - 情境**

传统回调式网络编程在高并发后台服务中会出现回调链复杂、状态分散、线程竞争和内存分配抖动。尤其是长连接网关、消息推送、RPC 接入层这类场景，需要同时处理大量连接、异步收发、心跳超时和跨线程任务投递。

**T - 任务**

我希望实现一个基于 C++ 协程和 io_uring 的异步网络 runtime，让业务代码能用同步风格编写异步 I/O，同时验证每核调度、Direct FD、Buffer Ring、multishot 和 msg_ring 在高并发网络服务中的可行性。

**A - 行动**

我自研了 `Task<T>` 协程抽象和每核 Scheduler；用 io_uring SQE/CQE 绑定 I/O 操作和协程上下文；通过 `user_data` 保存 `IoContext`，I/O 完成后恢复 coroutine handle；用 Direct FD 减少 fd 查找，用 Provided Buffer Ring 管理 recv buffer，用 multishot accept/recv 减少重复提交，用 msg_ring 实现跨 Scheduler 唤醒；同时实现五级时间轮、协程锁、Event 和 when_all。

**R - 结果**

最终打通了协程调度、异步 I/O、跨核投递、定时器和同步原语的完整链路。综合测试中 `9` 项核心功能全部通过。本机 8 物理核公平对齐压测下，NovaIO HTTP server 达到约 `11.3w req/s`，同参数 8 worker Node/libuv HTTP cluster 约 `7.7w req/s`，吞吐高约 `46%`。同时我也总结了生产化短板，包括 MPSC 队列、I/O cancel、资源生命周期、背压和可观测性。

## 简历里推荐使用的最终版本

如果简历只能放 4 到 5 行，建议用这一版：

**NovaIO：基于 C++ 协程与 io_uring 的高性能异步网络运行时**

- **S**：面向高并发后台接入层中回调链复杂、跨线程唤醒成本高、buffer 分配频繁的问题，设计 NovaIO 异步网络运行时。
- **T**：目标是用 C++ 协程 + io_uring 实现同步风格的异步 I/O，并验证多线程每核调度、低开销 I/O 提交、定时器和协程同步原语的完整链路。
- **A**：自研 `Task<T>` / `DetachedTask` 协程抽象和每核 `Scheduler`，每线程独立维护本地队列、`io_uring`、五级时间轮和 `mimalloc heap`，结合 CPU/NUMA affinity 降低共享竞争。
- **A**：封装异步 `accept/recv/send/close`，通过 CQE `user_data` 恢复 `IoContext` 对应协程；引入 Direct FD、Provided Buffer Ring、multishot accept/recv 和 `msg_ring`，优化 fd 查找、buffer 分配、重复 SQE 提交和跨核唤醒。
- **R**：综合测试 `9` 项核心功能全部通过；本机 8 物理核下 `wrk -t8 -c512 -d10s` 初步压测约 `11.3w req/s`，相比 8 worker Node/libuv HTTP cluster 的约 `7.7w req/s` 吞吐高约 `46%`。
