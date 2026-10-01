# P04 真实 fresh metadata owner

ArrowMetadataOwner 只从有限 Vec<(String,String)> 构造私有 immutable map，没有任意 HashMap 入口或 mutation 权限。reserve 前检查原 Vec spare、String 实际 capacity 与新 table 共存上界；按 move 只计一批 strings。builder 不授予钱包，调用者须事前覆盖完整 construction peak。

构造后只对本 owner 新建且从未删除的 map 颁发 table+key/value heap 收据；move 到新 Field/Schema 后按准确 Arc identity 校验。共享 Arc 保留同一实际 owner，结构 clone、Arc::make_mut 和 metadata 相等的未知大 map 不继承证明。该收据只涵盖附着的 metadata map，排除 Arc/Field/name/DataType/nested/Fields/receipt scaffolds；这些是后续完整 source proof 的独立义务。未接入任何生产 codec、LocalProgram、Chunk 或 Native producer。

stdlib 来源已核验：Rust1.92.0 ded5c06cf21d2b93bffd5d884aa6e96934ee4234，library Cargo.lock std hashbrown0.15.5 checksum9229cfe53dfd69f0609a49f65461bd93001ea1ef889cd5529dd176593f5338a1，raw/mod.rs SHA256a88cd291be71cd2f6119a1f8b4030a0429b6b1f8771b7f2a8b140c1824788bed。仅fresh后capacity推导bucket；reserve前按请求数另算bucket，15请求必须32buckets。控制组宽16保守覆盖当前ARM8/x86SSE16；升级toolchain必须重新核验allocator oracle。该声明是allocator请求layout上界，不是allocator内部/RSS计费。

5个真实allocator/owner测试通过：13个growth边界同时比较真实 table allocation 与预付/最终上界，preflight及超限/entry拒绝零分配，重复key不发布收据，实际Vec/String spare在reserve前拒绝，准确Field/Schema owner的alias/clone/mutation/unknown map区分。Types92 lib测试、strict Clippy -D warnings通过。第一次Clippy因多余的ptr_arg expect不触发而失败，删除不必要expect后复跑通过；原日志保留。readonly agent复核无阻断并补明确scope文档。

此模块提供过程内 allocation provenance facts，不包含query/async/global registry/semantic authority，不新增钱包，也不宣称MEM记账或完整Root input证明。P04仍executing；接线必须覆盖真实codec、expected/actual schema与nested carrier origins，不通过迭代未知map补造证明。
