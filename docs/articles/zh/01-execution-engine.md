# 执行引擎：ExecPlan 是怎么真正跑起来的

> NovaRocks 技术分析系列 · 第 1 篇

上一篇结束时，我们手里有了一个 `ExecPlan { arena, root }`——一棵执行节点树加一个表达式 arena。但它只是一张"怎么跑"的蓝图：哪个算子在哪、表达式长什么样。数据本身还一行没动。

这一篇就讲蓝图怎么变成运转的机器：数据以什么形态在算子间流动、算子之间靠什么协议协作、谁来并行调度它们、跨节点时数据又如何搬运并施加背压。这是 NovaRocks 的执行内核。内容多，我们走"全景"高度——把骨架和关键取舍讲透，少数子系统（比如具体某个 join 算子）留给后续。

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

三个字段各司其职：`batch` 是真正的列式数据；`chunk_schema` 维护 NovaRocks 自己的 schema 元数据——其中关键的一项，是把 StarRocks 计划里的 `slot_id` 映射到 `RecordBatch` 里的物理列下标（这就是上一篇 layout 推断要解决的问题在执行期的落地）；`accounting` 是可选的内存计量句柄，让一批数据占用多少内存可被查询级的内存追踪器看见。

为什么不自己造一套列式内存格式，而是包 Arrow？因为 Arrow 给了现成的、零拷贝友好的列式表示和一整套 compute kernel，还天然是跨系统交换格式（这对后面接 Iceberg/Parquet 极其省事）。代价是要维护"Arrow schema ↔ NovaRocks slot 语义"这层映射，而 `Chunk` 正是这层映射的归属地。

## 算子契约：push / pull

数据有了载体，算子之间怎么传？NovaRocks 用一对 trait 定义了算子契约。基础的 `Operator` 管生命周期：

```rust
// src/exec/pipeline/operator.rs:55
pub trait Operator: Send {
    fn name(&self) -> &str;
    fn prepare(&mut self) -> Result<(), String> { Ok(()) }
    fn close(&mut self) -> Result<(), String> { Ok(()) }
    fn cancel(&mut self) {}
    fn is_finished(&self) -> bool { false }
    // ... as_processor_ref / as_processor_mut ...
}
```

真正负责数据流动的是 `ProcessorOperator`——它定义了一套 push/pull 语义：

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

读懂这四个方法，就读懂了半个执行引擎：`need_input()` 问"你还能再吃一批吗"，`has_output()` 问"你有产出了吗"，`push_chunk` 喂进一批、`pull_chunk` 取走一批，`set_finishing` 告诉算子"上游没有更多数据了"。算子不主动拉线程、不自己 sleep，它只是诚实地回答"我现在能不能吃/能不能吐"——调度的活儿交给上层。

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

`PipelineGraph` 里是若干条 pipeline，每条 pipeline 在运行期实例化成多个 `PipelineDriver`（`pipeline_dop` 就是并行度）。一个 driver 持有一串算子：source → 若干 processor → sink。推动它的核心是一个协作式的执行循环 `process(time_slice)`——它不会霸占线程，而是跑一个时间片就让出。循环里判断该不该阻塞的逻辑非常能说明问题：

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

driver 被调度执行的地方是 `src/exec/pipeline/global_driver_executor.rs` 里的全局执行器：一个工作线程池不断取出"就绪"的 driver、给它一个时间片去 `process`，跑完根据返回的 `DriverState` 决定是重新入队、还是挂起、还是结束。

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

一个 driver 阻塞在某个 `Dependency` 上时不会空转；当条件满足（比如 build 侧建好了哈希表）调用 `set_ready()`，挂在 `observable` 上的通知被触发，被阻塞的 driver 才会被重新唤醒、重新入队。`InputEmpty`/`OutputFull` 同理——它们也由上下游算子状态变化驱动唤醒，而不是轮询。这把"阻塞"从忙等变成了事件驱动。

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

// ...
pub fn on_enqueue(&self, bytes: usize) {
    self.inflight_tasks.fetch_add(1, Ordering::AcqRel);
    self.inflight_bytes.fetch_add(bytes, Ordering::AcqRel);
}
pub fn inflight_bytes(&self) -> usize {
    self.inflight_bytes.load(Ordering::Acquire)
}
```

`inflight_bytes` 一旦超过配置上限（`exchange_io_max_inflight_bytes`），发送侧的算子就会以 `OutputFull` 让出——这就和前面 driver 的阻塞机制接上了：背压不是某个模块的特性，而是同一套"能不能吐/能不能吃"语义贯穿单机算子链和跨节点通道的结果。

接收端是另一半。下游的 exchange source 必须等上游所有 sender 都发完（EOS）才算输入结束；在此之前它阻塞等待。而当查询被取消时，exchange 会清掉对应的 key 并唤醒所有卡在等待上的接收者——否则一个被取消的查询会留下永远等不到 EOS 的僵尸 driver。分布式执行里最容易写出 bug 的，正是这类"等待 / EOS / 取消"三者的协同；把它收敛到统一的 key + 事件唤醒模型里，是这一层设计的重点。

## 取舍与对照

回头看，这套执行引擎有几个一以贯之的选择：

- **协作式调度而非线程绑定算子**。每个 driver 跑一个时间片就让出，由全局线程池复用少量线程驱动大量 driver。好处是高并发下不被线程数和上下文切换拖垮，代价是算子必须写成"可中断、可重入"的状态机——这正是 `need_input/has_output` 契约存在的理由。这也是 StarRocks pipeline 执行模型的核心思想，NovaRocks 用 Rust 的 trait + 协作循环把它重新落地了一遍。
- **背压用一套语义贯穿到底**。单机算子链的"吃不下/吐不出"和跨节点 exchange 的"在途字节超限"，最终都收敛成同一种 `OutputFull` 阻塞，而不是各做各的限流。
- **事件驱动而非轮询**。`Dependency` + `Observable` 让阻塞的 driver 真正睡下、被精确唤醒，避免 CPU 空转在"还不能跑"的判断上。
- **Arrow-first 的连带收益**。`Chunk` 包 `RecordBatch`，让执行层和后面的 Parquet/Iceberg 连接器共享同一套列式表示，少了一层格式转换。

## 小结：下一站，自己的 SQL 大脑

这一篇我们把蓝图跑了起来：数据以 `Chunk` 流动，算子用 push/pull 契约协作，pipeline 把它们编译成可被全局线程池协作调度的 driver，`Dependency` 让阻塞变成事件，exchange 把这套语义延伸到跨节点的 shuffle 与背压。

但有个问题一直没回答：这些 `ExecPlan` 是谁产出的？前两篇走的都是 StarRocks FE 下发 thrift 计划那条线。可 NovaRocks 还能完全脱离 FE 自己跑 SQL——那它必须自带一颗大脑：解析、分析、优化、codegen。下一篇就进入 standalone 模式的 SQL 栈与优化器，看 NovaRocks 如何把一句 SQL 自己变成能在今天这套引擎上跑的计划。
