# 执行引擎：ExecPlan 是怎么真正跑起来的

> NovaRocks 技术分析系列 · 第 1 篇

上一篇结束时，我们手里有了一个 `ExecPlan { arena, root }`——一棵执行节点树加一个表达式 arena。但它只是一张"怎么跑"的蓝图：哪个算子在哪、表达式长什么样，数据本身还一行没动。

这一篇就讲蓝图怎么变成运转的机器。它要回答一连串环环相扣的问题：数据以什么形态在算子间流动？算子之间靠什么协议协作？谁来把成百上千个算子实例并行调度起来？当一个算子暂时吃不下、或者上游还没产出时，调度器怎么知道、又怎么不空转？跨节点 shuffle 时数据如何搬运、如何施加背压？以及——当一个查询被取消，那些阻塞在等待上的算子如何被干净地唤醒，而不是变成永远醒不过来的僵尸？

这是 NovaRocks 的执行内核。内容多，我们走"全景"高度，把骨架和它一以贯之的几个取舍讲透；个别子系统（比如某个具体的 join 算子内部）留给后续。

## 列式的载体：Chunk

一切从数据的形态说起。NovaRocks 是 Arrow-first 的，批量数据的载体叫 `Chunk`——本质是 Arrow `RecordBatch` 的一层包装：

```rust
// src/exec/chunk/chunk_impl.rs:31
/// A chunk of data, consisting of multiple rows.
/// Phase 2: Wrapper around Arrow RecordBatch.
#[derive(Debug, Clone)]
pub struct Chunk {
    pub batch: RecordBatch,
    chunk_schema: ChunkSchemaRef,
    accounting: Option<Arc<ChunkAccounting>>,
}
```

三个字段各司其职。`batch` 是真正的列式数据；`accounting` 是可选的内存计量句柄，让一批数据占多少内存能被查询级的内存追踪器看见；而 `chunk_schema` 是这层包装真正的价值所在——它维护着 NovaRocks 自己的 schema 元数据：

```rust
// src/exec/chunk/schema.rs:385
pub struct ChunkSchema {
    slots: Vec<ChunkSlotSchema>,
    arrow_schema: SchemaRef,
    slot_ids: Vec<SlotId>,
    index_by_slot: HashMap<SlotId, usize>,
}
```

`index_by_slot` 就是上一篇 layout 推断在执行期的落地——把 StarRocks 计划里的 `slot_id` 映射到 `RecordBatch` 里的物理列下标。算子拿到一个 `Chunk`，要取"第 5 号 slot 那一列"，靠的就是这张表。`ChunkSchema` 还顺手解决了一个 Arrow 类型系统的表达力问题：有些 StarRocks 逻辑类型在 Arrow 里会"塌缩"（比如 `JSON` 落成 `Utf8`），于是它在 Arrow field 的 metadata 里挂一个 `nr_logical_type` 把原始逻辑类型记下来，避免一来一回丢了语义。

为什么不自己造一套列式内存格式，而是包 Arrow？因为 Arrow 给了现成的、零拷贝友好的列式表示和一整套 compute kernel，还天然是跨系统交换格式（这对后面接 Iceberg/Parquet 极其省事）。代价是要维护"Arrow schema ↔ NovaRocks slot 语义"这层映射——而 `Chunk`/`ChunkSchema` 正是这层映射的归属地。

## 算子契约：push / pull

数据有了载体，算子之间怎么传？NovaRocks 用一对 trait 定义了算子契约。基础的 `Operator` 管生命周期（`name / prepare / close / cancel / is_finished`），而真正负责数据流动的是 `ProcessorOperator`，它定义了一套 push/pull 语义：

```rust
// src/exec/pipeline/operator.rs:100
pub trait ProcessorOperator: Operator {
    fn need_input(&self) -> bool;
    fn has_output(&self) -> bool;
    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String>;
    fn pull_chunk(&mut self, state: &RuntimeState) -> Result<Option<Chunk>, String>;
    fn set_finishing(&mut self, state: &RuntimeState) -> Result<(), String>;

    /// Dependency that must be ready before the operator can make progress.
    /// This is used for build-side readiness (join, runtime filters, etc.).
    fn precondition_dependency(&self) -> Option<DependencyHandle> { None }
}
```

读懂这五个方法，就读懂了半个执行引擎：`need_input()` 问"你还能再吃一批吗"，`has_output()` 问"你有产出了吗"，`push_chunk` 喂进一批、`pull_chunk` 取走一批，`set_finishing` 告诉算子"上游没有更多数据了"。算子不主动拉线程、不自己 sleep，它只是诚实地回答"我现在能不能吃 / 能不能吐"——调度的活儿交给上层。`precondition_dependency` 则是给"必须等某个前置条件就绪才能动"的算子（典型是 hash join 的 probe 侧要等 build 侧建好表）留的钩子，后面会看到它怎么用。

## Pipeline：把算子串成可调度的 driver

谁来串这些算子、谁来推动数据流？`ExecPlan` 先被编译成一张 pipeline 图：

```rust
// src/exec/pipeline/builder.rs:136
pub(crate) fn build_pipeline_graph_for_exec_plan_with_dop(
    plan: &ExecPlan,
    _debug: bool,
    dep_manager: DependencyManager,
    _exchange_finst_id: Option<(i64, i64)>,
    pipeline_dop: i32,
    runtime_filter_hub: Arc<RuntimeFilterHub>,
) -> Result<PipelineGraph, String> {
```

`PipelineGraph` 里是若干条 pipeline，每条在运行期实例化成多个 `PipelineDriver`（`pipeline_dop` 就是并行度——同一条 pipeline 被复制成 N 份并行跑）。一个 driver 持有一串算子：source → 若干 processor → sink。

driver 是个状态机，状态定义本身就把执行模型讲清楚了：

```rust
// src/exec/pipeline/driver.rs:50
/// **State machine (high level)**
/// ```text
///              (scheduled)                 (time slice ends)
///   Ready ───────────────────► Running ─────────────────────► Ready
///                               │  ├─ blocks on I/O/deps ───► Blocked(reason)
///                               │  ├─ completes normally ───► Finished
///                               ├─ canceled ────────────────► Canceled
///                               └─ fatal error ─────────────► Failed(err)
/// ```
pub enum DriverState {
    Ready, Running, Blocked(BlockedReason), PendingFinish,
    Finished, Canceled, Failed(String),
}
```

推动它的核心是一个协作式的执行函数 `process(time_slice)`——它不会霸占线程，而是跑一个时间片就让出（回到 `Ready` 重新排队，或转入 `Blocked`）。`process` 里判断该不该阻塞的逻辑非常能说明问题：

```rust
// src/exec/pipeline/driver.rs:485
if let Some(sink) = self.operators.last()
    && !sink.is_finished()
{
    let Some(proc) = sink.as_processor_ref() else { /* ... */ };
    if !proc.need_input() {
        return self.block_or_fail(BlockedReason::OutputFull);
    }
}
```

也就是说：如果链尾的 sink 暂时吃不下（`need_input()` 为假），driver 就以 `OutputFull` 阻塞；如果链头的 source 暂时没产出（`has_output()` 为假），就以 `InputEmpty` 阻塞；如果在等某个 `Dependency`（比如 join 的 build 侧还没就绪），就以 `Dependency` 阻塞。`BlockedReason` 这三种状态，正是整个调度的语汇。

## 调度：一个协作式的工作线程池

driver 被谁驱动？`src/exec/pipeline/global_driver_executor.rs` 里的全局执行器——一个固定大小的工作线程池，每个线程跑同一个 `worker_loop`：

```rust
// src/exec/pipeline/global_driver_executor.rs:356
fn worker_loop(shared: Arc<ExecutorShared>, poller: BlockedDriverPoller) {
    loop {
        let mut task = {
            let mut queue = shared.queue.lock().expect("global executor queue lock");
            while queue.is_empty() && !shared.shutdown.load(Ordering::Acquire) {
                queue = shared.cv.wait(queue).expect("queue condvar wait");
            }
            if shared.shutdown.load(Ordering::Acquire) { return; }
            queue.pop_front()
        };
        // ...
        let state = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            task.driver.process(task.time_slice)
        }))
        .unwrap_or_else(|payload| {
            DriverState::Failed(format!("panic in driver execution: {/* msg */}"))
        });
        // ... 根据 state 决定重新入队 / 挂起到 poller / 结束 ...
    }
}
```

几个设计点一目了然：所有 worker 共享一个就绪 driver 队列，队列空时用条件变量挂起（不忙等），关停时统一退出；每次只给 driver 一个时间片去 `process`，跑完按返回的 `DriverState` 决定重新入队、挂起还是收尾；尤其值得一提的是 **`catch_unwind` 把单个 driver 的 panic 隔离成 `Failed`**——一个算子炸了，受影响的是那一个查询，而不是把整个 worker 线程乃至进程拖垮。这对一个"大量 AI 协作、还在快速演进"的引擎是很务实的健壮性兜底。被阻塞的 driver 则交给 `BlockedDriverPoller`，等它阻塞的条件满足后再被放回就绪队列。

## 不靠忙等：依赖与事件唤醒

协作式调度最怕的是忙等——一个阻塞的 driver 反复被调度、反复发现还不能跑。NovaRocks 用 `Dependency` 把"阻塞/就绪"做成了事件：

```rust
// src/exec/pipeline/dependency.rs:44
pub struct Dependency {
    id: usize,
    name: String,
    ready: AtomicBool,
    observable: Arc<Observable>,
}

// src/exec/pipeline/dependency.rs:91
pub fn set_ready(&self) {
    let prev = self.ready.swap(true, Ordering::AcqRel);
    if !prev {
        let notify = self.observable.defer_notify();
        notify.arm();
    }
}
```

一个 driver 阻塞在某个 `Dependency` 上时不会空转；当条件满足（比如 build 侧建好了哈希表）调用 `set_ready()`，挂在 `observable` 上的通知被触发，被阻塞的 driver 才会被重新唤醒、放回就绪队列。`InputEmpty`/`OutputFull` 也同理——它们由上下游算子的状态变化驱动唤醒，而不是轮询。这把"阻塞"从忙等变成了事件驱动，这也是协作式调度能高效跑大量 driver 的前提。

## 跨节点：Exchange 与背压

到目前为止都是单机内一条 driver 链上的事。但分析型查询要在多个 fragment（乃至多个 BE）之间 shuffle 数据，这就是 exchange。每个 exchange 接收端用一把三元组 key 标识：

```rust
// src/runtime/exchange.rs:37
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub struct ExchangeKey {
    pub finst_id_hi: i64,
    pub finst_id_lo: i64,
    pub node_id: i32,
}
```

发送端最容易出事的是"产得比发得快"——内存会被未发出的数据撑爆。NovaRocks 在发送侧记一笔在途账：

```rust
// src/service/exchange_sender.rs:31
pub struct ExchangeSendTracker {
    inflight_tasks: AtomicUsize,
    inflight_bytes: AtomicUsize,
}
```

`inflight_bytes` 一旦超过配置上限（`exchange_io_max_inflight_bytes`），发送侧的算子就会以 `OutputFull` 让出——这就和前面 driver 的阻塞机制接上了：**背压不是某个模块的特性，而是同一套"能不能吐 / 能不能吃"语义贯穿单机算子链和跨节点通道的结果。**

接收端是另一半。发送端通过 `push_chunks(key, sender_id, ..., eos)` 把数据连同 EOS 标记推过来；下游的 exchange source 则在 `take_all_chunks_blocking` 里阻塞，直到**所有** sender 都发完 EOS 才算输入结束。这里的协同最容易写出 bug——而最隐蔽的一种，是查询被取消后接收端永远等不到 EOS。NovaRocks 的取消路径把这个口子封死了：

```rust
// src/runtime/exchange.rs:337
pub fn cancel_fragment(finst_id_hi: i64, finst_id_lo: i64) {
    let mut guard = exchange().lock().expect("exchange lock");
    let keys: Vec<ExchangeKey> = guard.keys().copied()
        .filter(|k| k.finst_id_hi == finst_id_hi && k.finst_id_lo == finst_id_lo)
        .collect();
    for k in keys {
        mark_key_canceled(k);
        if let Some(r) = guard.get(&k).cloned() {
            let notify = r.observable.defer_notify();
            let mut st = r.mu.lock().expect("exchange receiver lock");
            st.canceled = true;
            r.cv.notify_all();      // 唤醒所有阻塞在该 key 上的接收者
            drop(st);
            notify.arm();
        }
        guard.remove(&k);
    }
}
```

取消时，找出该 fragment 的所有 exchange key，把接收端标记为 `canceled`、用 `cv.notify_all()` 加 observable 通知把所有阻塞的等待者叫醒，再把 key 清掉。被叫醒的接收者看到 `canceled` 就干净退出，而不是继续傻等一个永远不会到来的 EOS。这类"等待 / EOS / 取消"的三方协同，是分布式执行里最容易留 bug 的角落；把它收敛到统一的 key + 事件唤醒模型里，是这一层设计的重点。

## 取舍与对照

回头看，这套执行引擎有几个一以贯之的选择：

- **协作式调度而非线程绑定算子**。每个 driver 跑一个时间片就让出，由固定大小的工作线程池复用少量线程驱动大量 driver。好处是高并发下不被线程数和上下文切换拖垮，代价是算子必须写成"可中断、可重入"的状态机——这正是 `need_input/has_output` 契约存在的理由。这也是 StarRocks pipeline 执行模型的核心思想，NovaRocks 用 Rust 的 trait + 协作循环把它重新落地了一遍。
- **背压用一套语义贯穿到底**。单机算子链的"吃不下/吐不出"和跨节点 exchange 的"在途字节超限"，最终都收敛成同一种 `OutputFull` 阻塞，而不是各做各的限流。
- **事件驱动而非轮询**。`Dependency` + `Observable` 让阻塞的 driver 真正睡下、被精确唤醒；取消同样走唤醒而非超时兜底，避免 CPU 空转在"还不能跑"的判断上，也避免取消后的僵尸等待。
- **panic 隔离**。`catch_unwind` 把单个 driver 的崩溃降级成该查询的 `Failed`，不殃及线程池——对一个仍在高速演进的引擎是值钱的兜底。
- **Arrow-first 的连带收益**。`Chunk` 包 `RecordBatch`，让执行层和后面的 Parquet/Iceberg 连接器共享同一套列式表示，少了一层格式转换；代价是要在 `ChunkSchema` 里维护 slot 映射与逻辑类型保真。

## 小结：下一站，自己的 SQL 大脑

这一篇我们把蓝图跑了起来：数据以 `Chunk` 流动，算子用 push/pull 契约协作，pipeline 把它们编译成可被工作线程池协作调度的 driver，`Dependency` 让阻塞变成事件，exchange 把这套语义延伸到跨节点的 shuffle、背压与取消唤醒。

但有个问题一直没回答：这些 `ExecPlan` 是谁产出的？前两篇走的都是 StarRocks FE 下发 thrift 计划那条线。可 NovaRocks 还能完全脱离 FE 自己跑 SQL——那它必须自带一颗大脑：解析、分析、优化、codegen。下一篇就进入 standalone 模式的 SQL 栈与优化器，看 NovaRocks 如何把一句 SQL 自己变成能在今天这套引擎上跑的计划。
