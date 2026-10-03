# NovaRocks 对 bytes 1.11.0 的窄补丁

## 上游来源与文件核对

来源是 Cargo 已锁定的 crates.io `bytes = 1.11.0` registry 包，保留 MIT 上游许可。准确原始文件清单和 SHA256 存于 [UPSTREAM.json](UPSTREAM.json)，上游 VCS 信息保留于 `.cargo_vcs_info.json`。

- Registry：`registry+https://github.com/rust-lang/crates.io-index`。
- 包 checksum / 缓存 `bytes-1.11.0.crate` 的实际 SHA256：`b35204fbdc0b3f4446b89fc1ac2cf84a8a68971995d0bf2e925ec7cd960f9cb3`。
- 包内记录的上游 Git commit：`a7952fb4478f6dc226f623b217432fbc6f8dad24`。
- 对本机 exact registry 原始包逐一计算 `UPSTREAM.json.files` 的 **49 个文件** SHA256，全部匹配；其中 46 个 vendored 原始文件仍逐字节一致，修改的原始文件仅如下三项。

| 原始文件 | 补丁内容 | 原始 SHA256 |
| --- | --- | --- |
| `Cargo.toml` | 仅注册两个新增 integration test target；依赖和 features 不变 | `9aa6832fcb96aaeadbba051f5524dad18fca65c650fa3a12cffff02109d3e073` |
| `src/bytes.rs` | 新增 guarded owner 构造、配套私有 vtable/销毁路径及一个大小 getter | `54568a31252a7cc4f1dcee26d2d34dc6e0af9c9d8bf09522b694c3bd6a2a04bc` |
| `src/bytes_mut.rs` | 仅新增共享分配 metadata 大小 getter | `b93d48655775bab059ca7b91ce8d5a553916375d85ede1d04586ee781283afb6` |

本地新增文件为 `UPSTREAM.json`、本文、`tests/test_owned_exit_guard.rs` 和 `tests/test_owned_exit_guard_alloc.rs`。`Cargo.toml.orig`、上游 `Cargo.lock`、所有既有测试及 bench 文件均保持原始 hash。这里的 hash 核对针对源码文件，不包含 Cargo 的 `target/` 构建产物。

## 新增 API 与分配边界

只新增三个公开 API：

```rust
Bytes::from_owner_with_exit_guard<T, G>(owner: T, exit_guard: G) -> Bytes
// T: AsRef<[u8]> + Send + 'static; G: Send + 'static

Bytes::owner_with_exit_guard_metadata_size<T, G>() -> usize
BytesMut::shared_allocation_metadata_size() -> usize
```

原 `Bytes::from_owner` 函数、`Owned<T>`、原 owner vtable 和既有转换语义保持不变。新构造使用独立 `#[repr(C)] GuardedOwned<T, G>`，其首字段为 `AtomicUsize` 引用计数，后两字段分别为 `ManuallyDrop<T>` 和 `ManuallyDrop<G>`；已有 clone/slice 操作沿新 vtable 保留同一个 owner wrapper。

两个 getter 都不分配：`owner_with_exit_guard_metadata_size` 返回 `Layout::new::<GuardedOwned<T, G>>().size()`，包含内联 `T`、`G`、引用计数及布局 padding；`shared_allocation_metadata_size` 返回既有 `Shared` 类型的 `size_of`，供 `BytesMut` split/freeze 形成共享 Box 前预授。它们仅报告这些类型的请求分配大小，不包含 `T`/`G` 内部持有的堆对象、原始 byte buffer、allocator overhead 或 RSS。调用方仍须在构造前覆盖完整 backing 与共存峰值；getter 不创建预算或治理 authority，也不是任意 `Bytes` allocation 的反查接口。

## pin、AsRef 与最后 holder 的退出顺序

`T` 先移入永久 Box 地址，再安装 `Bytes` cleanup vtable，最后只调用一次 `T::as_ref()` 并保存 slice 的 ptr/len。之后 clone/slice 不再次调用 `AsRef`。`T` 从这一调用到析构都保持该地址，支持包含 `PhantomPinned` 的 owner；`G` 不被 pin、不被借用或暴露，在最后引用退出时可以移动到独立局部变量。

最后一个引用正常退出时，准确顺序是：

```text
ManuallyDrop::take(G) 到独立局部变量
恢复 Box<GuardedOwned<T, G>>
T::drop（仍在原 Box 地址）
Box allocation 实际 dealloc
G::drop
```

`G` 局部变量先于 Box 声明，因此 `T::drop` 发生 unwind 时，Box 先销毁并实际释放 allocation，随后才销毁 `G`；`ManuallyDrop<T>` 防止第二次调用 owner destructor。`AsRef` panic 时，由此前已安装的 `Bytes` cleanup 进入同一退出路径。单次 unwind 的上述次序有回归 oracle；`panic=abort` 或析构中的双重 panic 遵循 Rust 原有进程中止语义，不作中止后的析构承诺。

这保证 guard 不会仅因最后业务引用逻辑退出，就在 wrapper 的真实 deallocation 前归还它覆盖的能力。它不证明 owner 内部所有外部对象都已退出，也不改变这些对象自己的释放合同。

## 转换、no_std 与回归范围

guarded owner 转为 `Vec<u8>` 时，仍先用 `slice.to_vec()` 创建独立 buffer，然后释放本次 owner 引用；转为 `BytesMut` 沿同一 deep-copy 路径。其他 aliases 继续保留旧 owner/guard，新 buffer 不继承 guard。转换入口必须独立预授新 allocation，并覆盖 old+new 峰值，不能把 guarded `Bytes` 当作所有复制路径都自动带预算。

源码仍为 `#![no_std]`，新实现只用既有 `core` / `alloc` 与 crate atomics，不增加 `std` 依赖或更改默认 `std` feature。新增 integration tests 使用 `std` 测试设施；allocator probe 以 `cfg(not(miri))` 排除 Miri，不把 System allocator deallocation oracle 当作 Miri 的证明。实际 no_std 配置与执行收据由主 agent 汇总。

`test_owned_exit_guard.rs` 覆盖 pinned 地址、AsRef 一次、最后 slice/clone、并发最后退出、空值/高 alignment、AsRef 与 owner Drop 的单次 panic，以及 Vec/BytesMut 独立复制。`test_owned_exit_guard_alloc.rs` 用实际 System allocator dealloc 事件要求 `owner → wrapper dealloc → guard`，同时覆盖正常和 unwind 路径，并核对既有 Shared Box 的大小 getter。把 guard 提前到 Box dealloc 前的 oracle mutation，在 `/tmp/m07-bytes-exit-guard-mutant.log` 中因 `guard returned before the actual wrapper deallocation` 失败；恢复后的 allocator 日志单独保留。测试数量、最终命令和整体收据由主 agent 补入，本文件不把这些模块回归等同于完整 Native HTTP/H2、transport alias、进程内存或产品验收。

## 已核对的上游 Clippy 问题

没有修复、改写或在源码中压制上游告警。`/tmp/m07-bytes-exit-guard-clippy.log` 的严格 `-D warnings` 失败来自以下未改动代码：

| 类别 | 原代码位置 |
| --- | --- |
| `missing_safety_doc` | `src/buf/buf_mut.rs` 的 `unsafe trait BufMut` |
| `len_without_is_empty` | `src/buf/uninit_slice.rs` 的公开 `len` |
| `missing_safety_doc` | `src/bytes_mut.rs` 的 `unsafe fn set_len` |
| `needless_return` | `src/bytes_mut.rs` 的既有空输入 return 分支 |

前两个文件保持原始 hash；`src/bytes_mut.rs` 与 exact registry 的完整 diff 仅有新增 getter，以上代码未变，只因插入发生行号移动。

`/tmp/m07-bytes-exit-guard-clippy-default.log` 的默认 all-target 检查还遇到三个未修改 bench 的 `#![feature(test)]` 在 stable 上报 `E0554`，以及上游 `tests/test_bytes.rs` 的 `#[should_panic]` 反向 slice `5..3` 触发 `reversed_empty_ranges`。该日志中的其他测试/内建 test-module 告警也来自 hash 未变的上游文件或 `src/bytes_mut.rs` 未改动区域。不能据此称默认 all-target 或严格 Clippy 通过；单独 focused 日志与这些原始失败日志分开保存。


## Registry advisory 身份与待审计状态

原 registry `bytes 1.11.0` 的已知 `RUSTSEC-2026-0007` 继续记录为待源代码审计/修复，不宣称 M07 修复此 advisory。当前 path patch 的 lock entry 没有 registry source/checksum；cargo-deny 0.20.2 的 RustSec 匹配跳过 source=None。因此活动 deny ignore 已按 `unused-ignored-advisory=deny` 移除，级别没有降低。这只是 package source 身份迁移，不是安全结论；恢复 registry identity 时无旧 ignore 可以掩盖检查。

`BytesMut::reserve` 的原 `v_capacity >= new_cap + offset` 仍保留。当前 bytes_mut diff 只增加原 Shared metadata getter，exit-guard/Miri 收据不证明此 reserve 分支已修复。原始 registry provenance 仍见 `UPSTREAM.json`；本地 vendor 的源代码责任不由 cargo-deny registry PASS 自动覆盖。
