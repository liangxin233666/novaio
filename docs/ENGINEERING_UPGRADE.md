# NovaIO 工程化改造说明

## 改造目标

这次改造把 NovaIO 从“能手动编译运行的 runtime 原型”升级为“面试官可以按标准命令构建、测试和追问设计取舍的工程项目”。

核心目标有三个：

- 构建可复现：用现代 CMake target、Presets 和 CTest 固化构建与测试入口。
- 行为可验证：综合测试覆盖协程、时间轮、同步原语、队列、序列化和 io_uring echo 链路。
- 问题可讲清：把高性能 runtime 的关键工程取舍沉淀成能在腾讯二面、三面展开讨论的材料。

## 主要改造点

### 1. 现代 CMake target 化

原先 `CMakeLists.txt` 只定义了 `INTERFACE` target，源码、示例和测试没有进入统一构建图。改造后：

- `novaio_core` 是真实 `STATIC` library，包含 `src/comutex.cpp`、`src/io_engine.cpp`、`src/runtime.cpp`、`src/scheduler.cpp`、`src/time_wheel.cpp`。
- examples 和 tests 都通过 `target_link_libraries(... novaio_core)` 复用同一套 runtime。
- liburing、mimalloc、libnuma、pthread 由 CMake 查找并挂到 target 上。
- libc++、mold、x86-64-v3、metrics、sanitizer 都变成显式构建选项。

面试可讲：

> 我没有继续依赖手工命令编译，而是把 runtime 核心、测试和示例纳入同一个 CMake target graph。这样依赖、编译选项和链接选项都在 target 级别传递，避免不同二进制使用不一致的 ABI 或优化参数。

### 2. CMake Presets + CTest

新增 `CMakePresets.json`：

- `dev`：Debug + metrics，适合日常开发。
- `release`：Release + x86-64-v3，适合 wrk 压测。
- `asan`：AddressSanitizer + UBSan，适合排查生命周期和未定义行为。

标准命令：

```bash
cmake --preset dev
cmake --build --preset dev
ctest --preset dev
```

面试可讲：

> 高性能项目不能只看 benchmark，还要有 Debug、Release 和 Sanitizer 三套构建。Debug 用来定位逻辑问题，Release 用来验证性能路径，Sanitizer 用来检查协程生命周期、越界和 UB 风险。

### 3. Event 从单等待者升级为多等待者广播

原 `Event` 只用一个 atomic 保存一个 coroutine handle。第二个协程等待同一个未触发事件时，CAS 失败后会直接继续执行，属于同步原语语义 bug。

改造后：

- `Event` 使用 one-shot signaled 状态。
- 每个等待者以 awaiter 节点挂入等待链表。
- `set()` 一次性摘下全部等待者并恢复。
- 如果等待协程属于其他 scheduler，通过 `Runtime::dispatch_on()` 投递回原 scheduler，避免跨线程直接 resume。

面试可讲：

> 协程同步原语要特别关注等待者生命周期和恢复线程。awaiter 节点存放在协程帧里，协程挂起期间地址稳定；set 时摘链表再恢复，避免持锁 resume；跨 scheduler 恢复走 inbox + msg_ring，保持调度模型一致。

### 4. IO metrics 埋点修正

原 metrics 有 `io_events_reaped` 字段，但 CQE 分发入口没有记录，所以综合测试中 IO 指标一直是 0。

改造后在 `IoEngine::poll()` 的 CQE 遍历入口记录 IO 事件数。

面试可讲：

> 可观测性埋点要放在 runtime 边界，而不是业务示例里。CQE 分发处是所有 I/O 完成事件的统一入口，在这里计数能覆盖普通 I/O、multishot、inbox 和跨核唤醒等事件。

### 5. io_uring 初始化降级

原实现直接使用 `IORING_SETUP_SINGLE_ISSUER`、`IORING_SETUP_DEFER_TASKRUN` 和 `IORING_SETUP_COOP_TASKRUN`，如果内核、容器或权限策略不支持，就会直接启动失败。

改造后：

- 优先使用高级 setup flags。
- 失败后降级到基础 `io_uring` setup。
- queue depth 从 `4096` 逐级降到 `1024`、`256`。

面试可讲：

> 高性能参数不能写死成唯一启动路径。我的策略是 feature negotiation：能启用高级特性就启用，环境不支持时保留基础可用性。这样本地测试、容器环境和生产机器之间的差异不会直接导致 runtime 无法启动。

### 6. NUMA affinity 的 sanitizer 友好实现

ASan 发现 `numa_node_of_cpu()` / `numa_node_to_cpus()` 在当前 libnuma 版本下有内部泄漏报告。改造后不再用 libnuma 查询 CPU 所属 node，而是通过 Linux sysfs `/sys/devices/system/cpu/cpuN/nodeX` 获取 NUMA node，再把 node id 传给 `mi_heap_set_numa_affinity()`。

面试可讲：

> sanitizer 报告不应该简单忽略。这里我把不可控的 libnuma 查询路径替换成 sysfs 读取，既保留 NUMA affinity 能力，也让 ASan/LSan 构建保持干净。

## 测试覆盖

当前 CTest 覆盖：

- `novaio.test_framework`：基础协程、`CoMutex`、`Event`、`when_all`。
- `novaio.comprehensive`：返回值和异常传播、时间轮级联、跨线程 mutex、单事件唤醒、多等待者事件广播、SPSC 队列、序列化、Direct FD、Provided Buffer Ring 和 TCP echo。

建议在面试中主动说明：

> 这个项目目前有功能测试和本机压测，但还不是生产级 benchmark。后续我会补 p99 延迟、连接生命周期压力测试、I/O cancel、背压、MPSC inbox 和 CI 环境下的依赖安装矩阵。

## 可能追问

### 为什么 Event 不直接跨线程 resume？

因为 coroutine handle 本身只是恢复入口，不代表可以在任意线程恢复。NovaIO 的设计是每个 scheduler 拥有自己的本地队列、time wheel、allocator 和 io_uring。跨线程直接 resume 会破坏线程归属，可能让协程帧分配、I/O 提交和本地状态跑到错误线程。

### 为什么 set 时先摘链表再恢复？

避免在持锁状态下执行用户协程。协程恢复后可能继续调用 Event、Runtime 或其他同步原语，如果还持有 Event 的 mutex，容易引入死锁和长临界区。

### 为什么 metrics 做成构建选项？

metrics 对高性能 runtime 有价值，但也会给热路径带来额外写入。把它做成 CMake option 后，Debug 可以默认开启，严格 benchmark 可以关闭，避免测量污染。

### 为什么保留 x86-64-v3 选项？

项目压测目标是现代 x86 Linux 服务器。`x86-64-v3` 可以启用更现代的指令集基线，但它不适合所有机器，所以放到 release preset 和独立 option 里，而不是无条件全局开启。
