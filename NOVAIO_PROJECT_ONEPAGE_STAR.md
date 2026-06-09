# NovaIO 项目经历一页式 STAR 总结

## 项目一句话

NovaIO 是我独立设计并实现的一个基于 `C++ 协程` 和 `Linux io_uring` 的高性能异步网络运行时，目标是让高并发网络服务可以用 `co_await accept/recv/send` 的同步代码风格完成异步 I/O，同时验证每核调度、低开销 I/O 提交、跨核唤醒、定时器和协程同步原语在后台接入层场景中的可行性。

## STAR 总览

| STAR | 面试官关注点 | 我的项目回答 |
| --- | --- | --- |
| S - 情境 | 为什么做这个项目 | 高并发后台接入层、长连接网关、RPC/HTTP 服务通常要处理大量连接、异步收发、超时和跨线程任务投递。传统 `epoll + 回调` 模型容易出现回调链复杂、连接状态分散、跨线程唤醒成本高、buffer 频繁分配和延迟抖动。 |
| T - 任务 | 我要解决什么问题 | 设计一个 C++ 异步网络 runtime：业务层用同步风格写异步 I/O；底层用 io_uring 提交内核 I/O；运行时负责协程调度、多线程扩展、定时器、同步原语和跨核唤醒。 |
| A - 行动 | 我具体做了什么 | 自研 `Task<T>` 协程抽象、每核 `Scheduler`、io_uring I/O 封装、Direct FD、Provided Buffer Ring、multishot accept/recv、msg_ring、五级时间轮、`CoMutex`、`Event`、`when_all` 和压测脚本。 |
| R - 结果 | 最终效果有多好 | 综合测试 `9` 项核心功能全部通过；本机 8 物理核下 `wrk -t8 -c512 -d10s` 初步压测约 `11.3w req/s`，相比 8 worker Node/libuv HTTP cluster 的约 `7.7w req/s`，吞吐高约 `46%`。 |

## 我做了什么

### 1. 自研 C++ 协程 runtime

我没有只调用现成异步库，而是自己实现了协程运行时的核心链路：

- 实现 `Task<T>`、`Task<void>`、`DetachedTask`。
- 实现 `promise_type`、`initial_suspend`、`final_suspend`。
- 支持 continuation 恢复，让一个协程 `co_await` 另一个协程后能正确回到调用方。
- 支持异常传播，协程内部异常不会丢失。
- 对协程帧做自定义分配，配合线程本地 `mimalloc heap` 降低分配开销。

这部分说明我理解 C++ 协程不是“语法糖”，而是掌握了协程帧、promise、awaiter、coroutine handle、挂起点和恢复点这些底层机制。

### 2. 实现每核一个 Scheduler 的多线程调度模型

我设计了每个调度线程独立持有一套运行时组件：

- 本地任务队列。
- 跨线程 inbox 队列。
- 独立 `io_uring` 实例。
- 独立五级时间轮。
- 线程本地 `mimalloc heap`。

同时在调度线程启动时设置：

- `CPU affinity`：让 scheduler 尽量固定在指定 CPU 上运行，减少线程迁移。
- `NUMA affinity`：让线程尽量使用本 NUMA 节点内存，降低跨 NUMA 访问延迟。

调度循环里会处理定时器、inbox、就绪协程和 I/O 完成事件，使 CPU 计算任务、网络 I/O 和定时器事件都能在同一个 runtime 中统一调度。

### 3. 基于 io_uring 封装异步网络 I/O

我封装了异步：

- `accept`
- `recv`
- `send`
- `close`

实现方式是：

- 提交 I/O 时从 SQ ring 获取 SQE。
- 用 `io_uring_prep_*` 填充内核需要执行的操作。
- 在 `sqe->user_data` 里绑定 `IoContext` 或事件 tag。
- 当前协程挂起。
- 内核完成 I/O 后向 CQ ring 写入 CQE。
- runtime 轮询 CQE，根据 `user_data` 找回上下文。
- 设置结果并恢复对应协程。

这让业务代码可以写成接近同步的形式，但底层实际是异步非阻塞 I/O。

### 4. 引入 io_uring 高级特性做性能优化

我不是只用了最基础的 io_uring，而是结合了多个高级能力：

| 技术点 | 我怎么用 | 解决什么问题 |
| --- | --- | --- |
| Direct FD | `accept_direct` 让新连接进入 io_uring fixed file table，后续 I/O 使用 `IOSQE_FIXED_FILE` | 减少普通 fd table 查找开销 |
| Provided Buffer Ring | 预注册一批 recv buffer，recv 时用 `IOSQE_BUFFER_SELECT` 让内核选择 buffer | 降低每次 recv 动态分配 buffer 的开销 |
| multishot accept | 一次提交 accept SQE，后续多个连接到来都能返回 CQE | 减少反复提交 accept 请求的开销 |
| multishot recv | 一次提交 recv 请求，多个数据到来可以多次返回 CQE | 减少重复提交 recv SQE 的成本 |
| msg_ring | 一个 scheduler 可以向另一个 scheduler 的 io_uring 发送消息 | 解决跨线程投递任务后目标线程可能睡眠的问题 |

这些优化点体现的是：我不仅会用异步 I/O API，还会围绕 fd 查找、buffer 分配、SQE 提交次数和跨核唤醒这些真实性能瓶颈做设计。

### 5. 实现 runtime 基础组件

除了网络 I/O，我还补齐了 runtime 需要的基础设施：

- 五级层次时间轮：支持 `sleep_for` 和定时唤醒。
- `CoMutex`：协程友好的 mutex，等待时挂起协程而不是阻塞线程。
- `Event`：用于协程间事件通知。
- `when_all`：支持多个协程并发执行并等待全部完成。
- 基础 metrics：统计任务执行、队列处理等 runtime 状态。
- 综合测试：覆盖协程、队列、时间轮、同步原语、Direct FD、Provided Buffer Ring 和 TCP echo 链路。

这说明项目不是单点 demo，而是一个具备运行时完整性的原型。

## 亮点是什么

### 亮点 1：同步风格写异步 I/O

业务层不需要写复杂回调链，而是可以通过协程写出：

```cpp
auto client = co_await accept();
auto data = co_await recv(client);
co_await send(client, data);
```

优点是：

- 代码结构接近同步逻辑，可读性更好。
- 连接状态保存在协程栈/协程帧里，不需要手动拆散到多个回调上下文。
- 异常可以沿协程链路传播，更符合普通 C++ 代码习惯。

### 亮点 2：每核调度，减少共享竞争

每个 scheduler 都有自己的队列、io_uring 和 allocator heap，避免所有线程争抢一个全局事件循环或全局任务队列。

优点是：

- 队列竞争更少。
- I/O 完成事件本地处理。
- 内存分配更贴近本线程。
- 更容易做 CPU/NUMA 亲和性优化。

### 亮点 3：io_uring 高级能力组合使用

Direct FD、Provided Buffer Ring、multishot 和 msg_ring 分别优化不同环节：

- Direct FD 优化 fd 查找。
- Buffer Ring 优化 recv buffer 分配。
- multishot 优化 SQE 重复提交。
- msg_ring 优化跨核唤醒。

这些点组合起来，说明我的优化不是只盯着单个 API，而是围绕完整请求链路降低开销。

### 亮点 4：有完整测试和对比数据

我不是只写功能代码，还补了综合测试和压测脚本，并和成熟框架 Node/libuv 做了同等 worker 数的本机对比。

当前本机公平对齐参数：

```text
physical cores  : 8
server workers  : 8
wrk threads     : 8
wrk connections : 512
wrk duration    : 10s
Node workers    : 8
```

结果：

```text
NovaIO HTTP server      : 约 113,024 req/s，平均延迟约 4.51 ms
Node/libuv HTTP cluster : 约 77,483 req/s，平均延迟约 8.62 ms
```

这个结果可以说明我的原型在本机场景下具备较好的性能潜力，但我会明确说这是本机初步压测，不把它包装成严格生产 benchmark。

## 难点是什么

### 难点 1：协程生命周期和恢复链路

难点在于协程不是普通函数调用，挂起之后什么时候恢复、由谁恢复、异常怎么传播、父子协程怎么衔接都要自己处理。

我解决的是：

- `initial_suspend` 控制协程创建后是否立即执行。
- `final_suspend` 控制协程结束后如何恢复 continuation。
- `coroutine_handle` 保存和恢复执行现场。
- `promise_type` 保存返回值或异常。
- `DetachedTask` 用于 fire-and-forget 型任务。

这部分如果处理不好，容易出现协程提前销毁、重复恢复、异常丢失或内存泄漏。

### 难点 2：SQE/CQE 与协程上下文绑定

io_uring 的内核完成事件只返回 CQE，本身不知道业务协程是谁。因此我需要在提交 SQE 时把业务上下文编码进去。

我的做法是：

- 普通 I/O：`user_data` 绑定 `IoContext*`。
- multishot：`user_data` 绑定 `MultishotOp*`。
- 跨核协程唤醒：`user_data` 绑定 coroutine handle。
- inbox 通知：使用独立 tag 表示只需要唤醒事件循环。

我用 `user_data` 的低位 tag 区分事件类型，高位存放指针或句柄。这样同一个 CQE 分发入口就能处理普通 I/O、multishot、跨核恢复和 inbox 通知。

### 难点 3：跨线程任务投递和唤醒

多线程 runtime 不能只把任务塞进目标队列，因为目标 scheduler 可能正在 `io_uring_submit_and_wait` 中睡眠。

我做了两步：

- 先把任务放到目标 scheduler 的 inbox。
- 再用 `io_uring msg_ring` 给目标 ring 发消息，把目标 scheduler 唤醒。

这样可以避免任务已经入队但目标线程迟迟不醒的问题。

### 难点 4：buffer 生命周期管理

Provided Buffer Ring 的难点不是“注册 buffer”本身，而是 buffer 被内核选中之后，用户态必须根据 CQE flags 拿到 buffer id，并在处理完数据后归还 buffer。

我实现了：

- 初始化时预注册固定数量 buffer。
- recv 时设置 `IOSQE_BUFFER_SELECT` 和 buffer group id。
- CQE 返回后解析 buffer id。
- 数据处理完成后把 buffer 放回 ring。

如果这个流程错了，会出现 buffer 泄漏、重复归还或读到错误数据。

### 难点 5：多线程性能和公平压测

这个项目本身支持多线程，但压测时不能简单把 worker 开到逻辑线程数就说更公平。我的机器是 16 逻辑线程 / 8 物理核心，使用 16 个服务端 worker 可能引入超线程资源竞争。

所以我采用：

- 服务端 worker 对齐物理核心数：8。
- Node/libuv cluster 也开 8 worker。
- wrk threads 也设为 8。
- 连接数设为 512，避免过低压不满，也避免超过物理资源导致抖动过大。

这样对比更容易向面试官解释，也更符合工程压测的公平性。

## 最终效果是什么

### 功能效果

- 实现了一个完整的 C++ 协程异步网络 runtime 原型。
- 打通了协程调度、异步 I/O、跨核投递、定时器、同步原语和 HTTP server 示例。
- 综合测试 `9` 项核心功能全部通过。
- 能够支撑多线程 HTTP server 压测。

### 性能效果

本机 8 物理核公平对齐压测：

```text
NovaIO:
  Requests/sec : 113,024.42
  Avg latency  : 4.51 ms

Node/libuv cluster:
  Requests/sec : 77,482.68
  Avg latency  : 8.62 ms
```

可以在简历中写：

> 在本机 8 物理核环境下，服务端 worker 数对齐为 8，使用 `wrk -t8 -c512 -d10s` 初步压测，NovaIO HTTP server 达到约 `11.3w req/s`，相比 8 worker Node/libuv HTTP cluster 的约 `7.7w req/s`，吞吐高约 `46%`。

### 能力体现

这个项目能体现我具备：

- C++ 协程底层机制理解。
- Linux io_uring 异步 I/O 能力。
- 多线程调度与跨核唤醒设计能力。
- 网络服务性能优化意识。
- 对测试、公平压测和生产化短板的工程判断。

## 面试时可以这样讲

如果面试官让我用 1 分钟介绍这个项目，我会这样说：

> 我做了一个叫 NovaIO 的 C++ 异步网络运行时，主要解决高并发后台服务里 epoll 回调链复杂、跨线程唤醒和 buffer 分配开销的问题。项目底层基于 Linux io_uring，上层用 C++ 协程把异步 accept、recv、send 封装成 `co_await` 的同步写法。我自己实现了 `Task<T>` 协程抽象、每核一个 Scheduler 的多线程调度模型、五级时间轮、协程锁、Event 和 when_all。I/O 层用了 Direct FD、Provided Buffer Ring、multishot accept/recv 和 msg_ring，分别优化 fd 查找、buffer 分配、重复 SQE 提交和跨核唤醒。最后综合测试 9 项核心功能全部通过，本机 8 物理核下用 wrk 压测约 11.3w req/s，相比同样 8 worker 的 Node/libuv HTTP cluster 约 7.7w req/s，吞吐高约 46%。这个项目让我比较系统地理解了 C++ 协程、io_uring 和多线程 runtime 的设计取舍。

## 简历可直接使用版本

**NovaIO：基于 C++ 协程与 io_uring 的高性能异步网络运行时**

- **S**：面向高并发后台接入层中回调链复杂、跨线程唤醒成本高、buffer 分配频繁的问题，设计 NovaIO 异步网络运行时。
- **T**：目标是用 C++ 协程 + io_uring 实现同步风格异步 I/O，并验证每核调度、低开销 I/O 提交、定时器和协程同步原语的完整链路。
- **A**：自研 `Task<T>` / `DetachedTask` 协程抽象和每核 `Scheduler`，每线程独立维护本地队列、`io_uring`、五级时间轮和 `mimalloc heap`，结合 CPU/NUMA affinity 降低共享竞争。
- **A**：封装异步 `accept/recv/send/close`，通过 CQE `user_data` 恢复 `IoContext` 对应协程；引入 Direct FD、Provided Buffer Ring、multishot accept/recv 和 `msg_ring`，优化 fd 查找、buffer 分配、重复 SQE 提交和跨核唤醒。
- **R**：综合测试 `9` 项核心功能全部通过；本机 8 物理核下 `wrk -t8 -c512 -d10s` 初步压测约 `11.3w req/s`，相比 8 worker Node/libuv HTTP cluster 的约 `7.7w req/s`，吞吐高约 `46%`。

## 面试官追问时的态度

如果面试官问“这个项目哪里还不够生产化”，可以直接回答：

- 目前是高性能 runtime 原型，不是完整生产框架。
- inbox 当前更适合 SPSC/MPSC 方向继续完善，需要进一步加强多生产者并发安全。
- I/O cancel 目前还可以继续做到真正的 io_uring cancel 语义。
- buffer 归还可以进一步 RAII 化，减少业务层忘记归还的风险。
- 需要补充更完整的 p99/p999 延迟、长连接、慢客户端、背压、限流和可观测性测试。

这样回答的好处是：既能突出项目亮点，也能说明我知道生产系统和实验原型之间的差距。
