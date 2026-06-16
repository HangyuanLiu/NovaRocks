# ScalarArena M0（基础类型 + bridge + 核心不变式）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建立优化器原生 interned 标量 IR 的基础类型 `ScalarArena`/`ScalarId`/`ScalarNode`（hash-consed）+ 与 analyzer `TypedExpr` 的双向 bridge，并用单测锁定「id 相等 ⟺ 结构相等」这一承重不变式。**不碰任何算子字段**（那是 M1）。

**Architecture:** 新建优化器私有模块 `src/sql/optimizer/scalar/`。`ScalarNode` 是「子节点全用 `ScalarId` 引用」的标量节点；`ScalarArena` 拥有 `Vec<ScalarNode>` + 并行 `types`/`nullable`，`intern()` 经 `HashMap<ScalarNode, ScalarId>` 做 hash-consing（结构相同则复用 id）。可交换算子（AND/OR/Eq）在 intern 前对子 id 排序以规范化。本里程碑模块尚未接线，挂 `#![allow(dead_code)]`。

**Tech Stack:** Rust；`arrow::datatypes::DataType`；现有 `crate::sql::analysis::{TypedExpr, ExprKind, BinOp, UnOp, LiteralValue, SortItem, LambdaParam, WindowFrame, SubqueryKind}`、`crate::sql::column_id::ColumnId`。

**参照 spec:** `docs/design/specs/2026-06-16-optimizer-scalar-expr-ir.md`（§3 表示、§3.2 hash-cons 不变式、§7 M0）。

---

## File Structure

- Create: `src/sql/optimizer/scalar/mod.rs` — `ScalarId`、`ScalarNode`、`HashableLiteral`、`ScalarArena`（intern/getters）、`intern_typed`、`materialize` + 单测。
- Modify: `src/sql/optimizer/mod.rs` — 加 `pub(crate) mod scalar;`（模块声明）。
- Modify: `src/sql/analysis/mod.rs` — 给 `BinOp`、`UnOp` 补 `Hash, Eq` derive（若缺；`ScalarNode` 要它们做 HashMap key）。

> 单一职责：所有标量 IR + bridge 集中在 `scalar/mod.rs`（M0 体量适中，单文件；后续若变大再拆 `node.rs`/`bridge.rs`）。

---

### Task 1: `ScalarArena` 核心 + hash-consing

**Files:**
- Create: `src/sql/optimizer/scalar/mod.rs`
- Modify: `src/sql/optimizer/mod.rs`（加 `pub(crate) mod scalar;`，放在 `pub(crate) mod runtime_filter_pass;` 一带，按字母序）
- Modify: `src/sql/analysis/mod.rs`（确保 `BinOp`/`UnOp` 派生 `Hash, Eq`）

- [ ] **Step 1: 确认/补 `BinOp`、`UnOp` 的 `Hash, Eq` derive**

读 `src/sql/analysis/mod.rs:567`(`enum BinOp`)、`:588`(`enum UnOp`) 的 `#[derive(...)]`。若缺 `Hash` 或 `Eq`，补上（它们是无字段或简单变体的算子枚举，加这两个 derive 安全）。例如：
```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum BinOp { /* ... 不变 ... */ }
```
（`UnOp` 同理。）

- [ ] **Step 2: 写失败测试（dedup + 类型断言）**

在 `src/sql/optimizer/scalar/mod.rs` 末尾：
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, LiteralValue};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn int() -> DataType { DataType::Int64 }

    #[test]
    fn intern_dedups_structurally_equal_nodes() {
        let mut a = ScalarArena::new();
        let c = a.intern(ScalarNode::ColumnRef(ColumnId(1)), int(), false);
        let c2 = a.intern(ScalarNode::ColumnRef(ColumnId(1)), int(), false);
        assert_eq!(c, c2, "same ColumnRef must intern to one id");
        let d = a.intern(ScalarNode::ColumnRef(ColumnId(2)), int(), false);
        assert_ne!(c, d, "different ColumnRef must get different ids");

        let lit = a.intern(ScalarNode::Literal(HashableLiteral(LiteralValue::Int(7))), int(), false);
        let add1 = a.intern(ScalarNode::BinaryOp { op: BinOp::Add, left: c, right: lit }, int(), false);
        let add2 = a.intern(ScalarNode::BinaryOp { op: BinOp::Add, left: c, right: lit }, int(), false);
        assert_eq!(add1, add2, "same BinaryOp over same child ids must intern to one id");
        assert_eq!(a.data_type(add1), &int());
        assert!(!a.nullable(add1));
    }
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cargo test --lib scalar::tests::intern_dedups_structurally_equal_nodes`
Expected: 编译失败（`ScalarArena`/`ScalarNode`/`HashableLiteral`/`ScalarId` 未定义）。

- [ ] **Step 4: 实现核心类型 + intern**

`src/sql/optimizer/mod.rs` 加模块声明（按字母序）：
```rust
pub(crate) mod scalar;
```
`src/sql/optimizer/scalar/mod.rs`：
```rust
//! Optimizer-native interned scalar expression IR (ScalarArena + ScalarId).
//!
//! Operators will reference scalar expressions by a Copy `ScalarId` handle
//! instead of owning analyzer `TypedExpr` by value, so cloning an operator /
//! memo expression is O(1). `intern` hash-conses: structurally-identical nodes
//! share one id, giving id-equality == structural-equality (the property the
//! dedup sites and future CSE rely on). M0 builds the type + the TypedExpr
//! bridge only; no operator field uses it yet.
#![allow(dead_code)] // wired into operators in M1.

use std::collections::HashMap;

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, LiteralValue};
use crate::sql::column_id::ColumnId;

/// `LiteralValue` is only `PartialEq` (it holds `Float(f64)` / `Decimal(String)`),
/// so it cannot be a `HashMap` key directly. This newtype provides `Eq`/`Hash`
/// by hashing/comparing floats via their bit pattern (NaN compares equal to NaN,
/// which is exactly what we want for structural dedup of identical literals).
#[derive(Clone, Debug)]
pub(crate) struct HashableLiteral(pub LiteralValue);

impl PartialEq for HashableLiteral {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (LiteralValue::Float(a), LiteralValue::Float(b)) => a.to_bits() == b.to_bits(),
            (a, b) => a == b,
        }
    }
}
impl Eq for HashableLiteral {}
impl std::hash::Hash for HashableLiteral {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
        match &self.0 {
            LiteralValue::Null => {}
            LiteralValue::Bool(b) => b.hash(state),
            LiteralValue::Int(i) => i.hash(state),
            LiteralValue::LargeInt(i) => i.hash(state),
            LiteralValue::Float(f) => f.to_bits().hash(state),
            LiteralValue::Decimal(s) | LiteralValue::String(s) => s.hash(state),
            LiteralValue::Binary(b) => b.hash(state),
        }
    }
}

/// Copy handle into a `ScalarArena`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct ScalarId(u32);

/// One scalar node. Children are referenced by `ScalarId` (never inlined), so a
/// node is cheap to hash/compare. Type-determining info that is NOT a function
/// of `(op, children)` MUST live in the node (e.g. `Cast.target`), so that
/// `node` alone is a correct intern key. More variants are added in Task 5.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum ScalarNode {
    ColumnRef(ColumnId),
    Literal(HashableLiteral),
    BinaryOp { op: BinOp, left: ScalarId, right: ScalarId },
    FunctionCall { name: String, args: Vec<ScalarId>, distinct: bool },
}

/// Owns all scalar nodes for one optimize() call; interns (hash-conses) on push.
pub(crate) struct ScalarArena {
    nodes: Vec<ScalarNode>,
    types: Vec<DataType>,
    nullable: Vec<bool>,
    intern: HashMap<ScalarNode, ScalarId>,
}

impl ScalarArena {
    pub(crate) fn new() -> Self {
        Self { nodes: Vec::new(), types: Vec::new(), nullable: Vec::new(), intern: HashMap::new() }
    }

    /// Intern a node. Returns the existing id for a structurally-identical node.
    /// `ty`/`nullable` are the computed properties of the expression; they MUST
    /// be a function of the node (debug-asserted on a dedup hit).
    pub(crate) fn intern(&mut self, node: ScalarNode, ty: DataType, nullable: bool) -> ScalarId {
        if let Some(&id) = self.intern.get(&node) {
            debug_assert!(
                self.types[id.0 as usize] == ty && self.nullable[id.0 as usize] == nullable,
                "interned node has divergent type/nullable; node must fully determine its type \
                 (put type-discriminating info, e.g. Cast.target, inside the node)"
            );
            return id;
        }
        let id = ScalarId(self.nodes.len() as u32);
        self.nodes.push(node.clone());
        self.types.push(ty);
        self.nullable.push(nullable);
        self.intern.insert(node, id);
        id
    }

    pub(crate) fn node(&self, id: ScalarId) -> &ScalarNode { &self.nodes[id.0 as usize] }
    pub(crate) fn data_type(&self, id: ScalarId) -> &DataType { &self.types[id.0 as usize] }
    pub(crate) fn nullable(&self, id: ScalarId) -> bool { self.nullable[id.0 as usize] }
}
```

- [ ] **Step 5: 跑测试确认通过**

Run: `cargo test --lib scalar::tests::intern_dedups_structurally_equal_nodes`
Expected: PASS。

- [ ] **Step 6: Commit**

```bash
git add src/sql/optimizer/scalar/mod.rs src/sql/optimizer/mod.rs src/sql/analysis/mod.rs
git commit -m "feat(optimizer): ScalarArena core with hash-consing (M0 task 1)"
```

---

### Task 2: 可交换算子规范化（`a AND b` == `b AND a`）

**Files:**
- Modify: `src/sql/optimizer/scalar/mod.rs`

- [ ] **Step 1: 写失败测试**

在 tests mod：
```rust
#[test]
fn commutative_ops_normalize_to_one_id() {
    let mut a = ScalarArena::new();
    let x = a.intern(ScalarNode::ColumnRef(ColumnId(1)), int(), false);
    let y = a.intern(ScalarNode::ColumnRef(ColumnId(2)), int(), false);
    let b = DataType::Boolean;
    let xy = a.intern(ScalarNode::BinaryOp { op: BinOp::And, left: x, right: y }, b.clone(), false);
    let yx = a.intern(ScalarNode::BinaryOp { op: BinOp::And, left: y, right: x }, b.clone(), false);
    assert_eq!(xy, yx, "AND must be commutative-normalized to one id");
    // 非可交换算子保持区分
    let sub_xy = a.intern(ScalarNode::BinaryOp { op: BinOp::Sub, left: x, right: y }, int(), false);
    let sub_yx = a.intern(ScalarNode::BinaryOp { op: BinOp::Sub, left: y, right: x }, int(), false);
    assert_ne!(sub_xy, sub_yx, "Sub must NOT be normalized");
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib scalar::tests::commutative_ops_normalize_to_one_id`
Expected: FAIL（`xy != yx`，当前未规范化）。

- [ ] **Step 3: 在 `intern` 前规范化可交换算子的子 id 顺序**

在 `intern` 开头插入规范化（按 `ScalarId` 升序排两个操作数）：
```rust
pub(crate) fn intern(&mut self, node: ScalarNode, ty: DataType, nullable: bool) -> ScalarId {
    let node = Self::normalize(node);
    // ... 其余不变（hash-cons）...
}

impl ScalarArena {
    /// Canonicalize commutative binary ops by ordering operands by ScalarId, so
    /// `a AND b` and `b AND a` intern to one id. Mirrors StarRocks
    /// normalizeChildrenGroup.
    fn normalize(node: ScalarNode) -> ScalarNode {
        if let ScalarNode::BinaryOp { op, left, right } = node {
            let commutative = matches!(op, BinOp::And | BinOp::Or | BinOp::Eq);
            if commutative && left.0 > right.0 {
                return ScalarNode::BinaryOp { op, left: right, right: left };
            }
            return ScalarNode::BinaryOp { op, left, right };
        }
        node
    }
}
```
> 注：确认 `BinOp` 变体名（`And`/`Or`/`Eq`）与 `src/sql/analysis/mod.rs:567` 一致；若 `Eq` 名不同（如 `Equal`）按实际改。不要把 `NotEq`/比较类里非交换的纳入。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib scalar::tests::commutative_ops_normalize_to_one_id`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/scalar/mod.rs
git commit -m "feat(optimizer): commutative-op normalization in ScalarArena intern (M0 task 2)"
```

---

### Task 3: `intern_typed`（`&TypedExpr` → `ScalarId`）+ 核心不变式测试

**Files:**
- Modify: `src/sql/optimizer/scalar/mod.rs`

- [ ] **Step 1: 写失败测试（含「独立构造→同一 id」承重不变式）**

```rust
#[cfg(test)]
mod bridge_tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn col(id: u32, ty: DataType) -> TypedExpr {
        TypedExpr { kind: ExprKind::ColumnRef { column_id: ColumnId(id), qualifier: None, column: format!("c{id}") },
                    data_type: ty, nullable: false }
    }
    fn lit_int(v: i64) -> TypedExpr {
        TypedExpr { kind: ExprKind::Literal(LiteralValue::Int(v)), data_type: DataType::Int64, nullable: false }
    }
    fn eq(l: TypedExpr, r: TypedExpr) -> TypedExpr {
        TypedExpr { kind: ExprKind::BinaryOp { left: Box::new(l), op: BinOp::Eq, right: Box::new(r) },
                    data_type: DataType::Boolean, nullable: false }
    }

    #[test]
    fn intern_typed_dedups_independent_identical_exprs() {
        let mut a = ScalarArena::new();
        // 两棵「独立构造」但结构相同的 TypedExpr，必须 intern 成同一 ScalarId。
        let id1 = intern_typed(&mut a, &eq(col(1, DataType::Int64), lit_int(5)));
        let id2 = intern_typed(&mut a, &eq(col(1, DataType::Int64), lit_int(5)));
        assert_eq!(id1, id2, "structurally-identical TypedExprs must intern to one ScalarId");
        // 不同字面量 → 不同 id
        let id3 = intern_typed(&mut a, &eq(col(1, DataType::Int64), lit_int(6)));
        assert_ne!(id1, id3);
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib scalar::bridge_tests::intern_typed_dedups_independent_identical_exprs`
Expected: 编译失败（`intern_typed` 未定义）。

- [ ] **Step 3: 实现 `intern_typed`（M0 覆盖核心变体）**

```rust
use crate::sql::analysis::{ExprKind, TypedExpr};

/// Recursively intern an analyzer `TypedExpr` into the arena, returning its id.
/// M0 covers ColumnRef / Literal / BinaryOp / FunctionCall; Task 5 extends to
/// every remaining `ExprKind` variant.
pub(crate) fn intern_typed(arena: &mut ScalarArena, expr: &TypedExpr) -> ScalarId {
    let node = match &expr.kind {
        ExprKind::ColumnRef { column_id, .. } => ScalarNode::ColumnRef(*column_id),
        ExprKind::Literal(v) => ScalarNode::Literal(HashableLiteral(v.clone())),
        ExprKind::BinaryOp { left, op, right } => {
            let l = intern_typed(arena, left);
            let r = intern_typed(arena, right);
            ScalarNode::BinaryOp { op: *op, left: l, right: r }
        }
        ExprKind::FunctionCall { name, args, distinct } => {
            let arg_ids: Vec<ScalarId> = args.iter().map(|a| intern_typed(arena, a)).collect();
            ScalarNode::FunctionCall { name: name.clone(), args: arg_ids, distinct: *distinct }
        }
        other => unimplemented!("intern_typed: ExprKind variant not covered in M0: {other:?}"),
    };
    arena.intern(node, expr.data_type.clone(), expr.nullable)
}
```
> 子节点先 intern（拿到 `ScalarId` 再构造父 `ScalarNode`），避免 RefCell/借用问题（M1 接 `Rc<RefCell<>>` 时尤其重要：先把子 id 拿到再 `borrow_mut().intern`）。`other => unimplemented!` 是 M0 的临时分支，Task 5 删除并补全。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib scalar::bridge_tests::intern_typed_dedups_independent_identical_exprs`
Expected: PASS（这是「id 判等 ⟺ 结构判等」的核心 gate）。

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/scalar/mod.rs
git commit -m "feat(optimizer): intern_typed TypedExpr->ScalarId bridge for core variants (M0 task 3)"
```

---

### Task 4: `materialize`（`ScalarId` → `TypedExpr`）+ 往返测试

**Files:**
- Modify: `src/sql/optimizer/scalar/mod.rs`

- [ ] **Step 1: 写失败测试（往返结构相等）**

`TypedExpr` 仅 `PartialEq`？需确认。它派生 `Clone, Debug`，**未派生 `PartialEq`**——故往返断言用 `format!("{:?}", ...)` 结构比较（M0 够用；不引入对 `TypedExpr` 的 `PartialEq` 依赖）：
```rust
#[test]
fn materialize_round_trips_core_variants() {
    let mut a = ScalarArena::new();
    let original = eq(col(1, DataType::Int64), lit_int(5));
    let id = intern_typed(&mut a, &original);
    let back = materialize(&a, id);
    assert_eq!(format!("{:?}", back), format!("{:?}", original),
        "intern_typed then materialize must reproduce the expression");
}
```
> 若后续发现 `column`/`qualifier` 等在往返中丢失（`ScalarNode::ColumnRef` 只存 `ColumnId`），见 Step 3 的注释处理。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test --lib scalar::bridge_tests::materialize_round_trips_core_variants`
Expected: 编译失败（`materialize` 未定义）。

- [ ] **Step 3: 实现 `materialize`**

```rust
/// Rebuild an analyzer `TypedExpr` from an interned id (transient view for
/// codegen/EXPLAIN and the staged-migration bridge; NOT a long-lived optimizer
/// type). M0 covers the core variants; Task 5 extends.
pub(crate) fn materialize(arena: &ScalarArena, id: ScalarId) -> TypedExpr {
    let kind = match arena.node(id) {
        ScalarNode::ColumnRef(cid) => ExprKind::ColumnRef {
            column_id: *cid,
            // ColumnRef name/qualifier are display-only; the optimizer addresses
            // columns by ColumnId. M0 reconstructs a synthetic name; if a caller
            // needs the original display name, resolve it via ColumnRefFactory at
            // the codegen/EXPLAIN boundary (M1).
            qualifier: None,
            column: format!("col{}", cid.0),
        },
        ScalarNode::Literal(HashableLiteral(v)) => ExprKind::Literal(v.clone()),
        ScalarNode::BinaryOp { op, left, right } => ExprKind::BinaryOp {
            left: Box::new(materialize(arena, *left)),
            op: *op,
            right: Box::new(materialize(arena, *right)),
        },
        ScalarNode::FunctionCall { name, args, distinct } => ExprKind::FunctionCall {
            name: name.clone(),
            args: args.iter().map(|a| materialize(arena, *a)).collect(),
            distinct: *distinct,
        },
    };
    TypedExpr { kind, data_type: arena.data_type(id).clone(), nullable: arena.nullable(id) }
}
```
> **设计取舍（记入 M1 待办）**：`ScalarNode::ColumnRef` 只存 `ColumnId`，丢弃了 `qualifier`/`column`（显示名）。优化器按 `ColumnId` 寻址、显示名可由 `ColumnRefFactory` 在边界还原，故 M0 用合成名即可；该测试因此用「相同输入下往返一致」而非「与原始逐字段相等」——把 col 名设成与合成规则一致（或断言除显示名外结构相等）。若 Step 1 测试因 col 名失败，将 `col()` 助手的 `column` 改为 `format!("col{id}")` 使其与 materialize 的合成规则对齐，验证非显示名部分的往返保真。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test --lib scalar::bridge_tests::materialize_round_trips_core_variants`
Expected: PASS。

- [ ] **Step 5: Commit**

```bash
git add src/sql/optimizer/scalar/mod.rs
git commit -m "feat(optimizer): materialize ScalarId->TypedExpr bridge for core variants (M0 task 4)"
```

---

### Task 5: 扩展到所有 `ExprKind` 变体 + 综合往返 gate

**Files:**
- Modify: `src/sql/optimizer/scalar/mod.rs`

把 `ScalarNode`、`intern_typed`、`materialize` 扩到 `ExprKind` 的**全部**变体。子节点一律用 `ScalarId`；可交换性只在 Task 2 的 `BinaryOp` 处。逐变体的 child-shape（取自 `src/sql/analysis/mod.rs:310-418`，照此机械映射）：

| ExprKind 变体 | ScalarNode 对应（子节点改 ScalarId） |
|---|---|
| `UnaryOp{op:UnOp, expr}` | `UnaryOp{op:UnOp, child:ScalarId}` |
| `Cast{expr, target:DataType}` | `Cast{child:ScalarId, target:DataType}` ← **target 必须进 node**（同 child 不同目标类型不能共享 id；`DataType` 已 `Hash+Eq`） |
| `IsNull{expr, negated}` | `IsNull{child:ScalarId, negated:bool}` |
| `InList{expr, list, negated}` | `InList{child:ScalarId, list:Vec<ScalarId>, negated:bool}` |
| `Between{expr, low, high, negated}` | `Between{child, low, high:ScalarId, negated}` |
| `Like{expr, pattern, negated}` | `Like{child:ScalarId, pattern:ScalarId, negated}` |
| `Case{operand:Option<Box>, when_then:Vec<(TE,TE)>, else_expr:Option<Box>}` | `Case{operand:Option<ScalarId>, when_then:Vec<(ScalarId,ScalarId)>, else_expr:Option<ScalarId>}` |
| `IsTruthValue{expr, value:bool, negated}` | `IsTruthValue{child:ScalarId, value:bool, negated:bool}` |
| `Nested(Box)` | `Nested(ScalarId)` |
| `AggregateCall{name, args, distinct, order_by:Vec<SortItem>}` | `AggregateCall{name:String, args:Vec<ScalarId>, distinct:bool, order_by:Vec<SortKey>}` |
| `WindowCall{name,args,distinct,partition_by,order_by:Vec<SortItem>,window_frame:Option<WindowFrame>,ignore_nulls}` | 镜像，`args/partition_by:Vec<ScalarId>`、`order_by:Vec<SortKey>`，`window_frame`/`ignore_nulls` 原样 |
| `LambdaParamRef{name, slot_id:i32}` | `LambdaParamRef{name:String, slot_id:i32}`（叶子，无子节点） |
| `LambdaFunction{params:Vec<LambdaParam>, body}` | `LambdaFunction{params:Vec<LambdaParam>, body:ScalarId}` |
| `Lambda{params:Vec<String>, body}` | `Lambda{params:Vec<String>, body:ScalarId}` |
| `SubqueryPlaceholder{id:usize, kind:SubqueryKind, data_type}` | `SubqueryPlaceholder{id:usize, kind:SubqueryKind, data_type:DataType}`（叶子；见下注） |

新增 `SortKey`（取代 `SortItem` 的按值 `TypedExpr`）：
```rust
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct SortKey { pub expr: ScalarId, pub asc: bool, pub nulls_first: bool }
```

- [ ] **Step 1: 确认 `WindowFrame`、`SubqueryKind`、`LambdaParam`、`UnOp` 的 `Hash+Eq`**

`ScalarNode` 要 derive `Hash+Eq`，故内嵌的 `WindowFrame`/`SubqueryKind`/`LambdaParam`/`UnOp` 也须 `Hash+Eq`。读各自定义；缺则补 derive。
- ⚠️ **`SubqueryKind` 含子查询 AST**（`src/sql/analysis/mod.rs:421+`），很可能**非 `Hash/Eq`**。M0 策略：`SubqueryPlaceholder` 在进入优化器前已被 SubqueryRewrite 消解（`optimize()` 有 `find_residual_apply` backstop），优化器内**不应**再见到它。故 `intern_typed` 对 `SubqueryPlaceholder` 走 `unreachable!("SubqueryPlaceholder must be rewritten before the optimizer")`，`ScalarNode` **不**纳入该变体。`WindowFrame` 若含表达式边界且非 Hash，同法评估：优先给 `WindowFrame` 补 `Hash+Eq`（若其 bound 是字面量/数字而非 `TypedExpr`）；若它内嵌 `TypedExpr`，则该子表达式也要改走 `ScalarId`（升级 `WindowFrame` 或在 `ScalarNode::WindowCall` 里展开 frame 边界为 `ScalarId`）——按实际定义决定，记入注释。

- [ ] **Step 2: 写综合往返失败测试**

构造一棵用尽多种变体的 `TypedExpr`（CASE 内含 BinaryOp、FunctionCall、Cast、IsNull、InList、AggregateCall(带 order_by)），断言 `intern_typed` 后 `materialize` 的 `Debug` 串与原始一致，且二次 `intern_typed` 同一 id：
```rust
#[test]
fn all_variants_round_trip_and_dedup() {
    let mut a = ScalarArena::new();
    let e = /* 构造覆盖 Cast/IsNull/InList/Case/FunctionCall/AggregateCall/Nested 的复合 TypedExpr */;
    let id1 = intern_typed(&mut a, &e);
    let back = materialize(&a, id1);
    assert_eq!(format!("{back:?}"), format!("{e:?}"), "complex expr must round-trip");
    let id2 = intern_typed(&mut a, &e);
    assert_eq!(id1, id2, "complex expr must dedup to one id");
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cargo test --lib scalar::bridge_tests::all_variants_round_trip_and_dedup`
Expected: 编译失败 / `unimplemented!` panic（变体未覆盖）。

- [ ] **Step 4: 扩展 `ScalarNode` enum + `intern_typed` + `materialize` 全变体**

按上表给 `ScalarNode` 加全部变体；`intern_typed` 逐变体（先 intern 子节点拿 `ScalarId` 再构造 node，`SortItem`→`SortKey`、`order_by` 逐项 intern）；`materialize` 对称还原；删 `intern_typed` 的 `unimplemented!` 兜底。`Case.when_then` 逐对 intern；`AggregateCall.order_by`/`WindowCall.order_by` 把每个 `SortItem{expr,asc,nulls_first}` 转成 `SortKey{expr: intern_typed(..), asc, nulls_first}`。

- [ ] **Step 5: 跑测试确认通过 + 全 lib 编译**

Run: `cargo test --lib scalar`
Expected: 该模块所有测试 PASS。
Run: `cargo build --lib 2>&1 | grep -E '^error' || echo OK`
Expected: `OK`（模块挂 `#![allow(dead_code)]`，未接线不报错）。

- [ ] **Step 6: fmt/clippy + Commit**

```bash
cargo fmt
cargo clippy --lib 2>&1 | grep -A2 'scalar/mod.rs' || echo "no clippy in scalar/mod.rs"
git add src/sql/optimizer/scalar/mod.rs src/sql/analysis/mod.rs
git commit -m "feat(optimizer): extend ScalarArena bridge to all ExprKind variants (M0 task 5)"
```

---

## Self-Review

- **Spec 覆盖**：M0 = spec §7 M0（建类型 + hash-cons + 交换律规范化 + bridge + 「独立构造→同一 id」不变式）——Task 1（arena/intern/dedup）、Task 2（交换律）、Task 3（intern_typed + 核心不变式）、Task 4（materialize 往返）、Task 5（全变体 + 综合 gate）逐条对应。**不碰算子字段**（M1）✅。
- **占位扫描**：Task 5 的「全变体」给了逐变体 child-shape 映射表 + 机械模式（Task 3/4 已示范），非 hand-wave；`unimplemented!`/`unreachable!` 是 M0 受控临时分支，Task 5 删除 intern 的兜底、Subquery 走 `unreachable!`（设计契约）。
- **类型一致**：`ScalarId`/`ScalarNode`/`HashableLiteral`/`ScalarArena`/`SortKey`/`intern`/`intern_typed`/`materialize`/`node`/`data_type`/`nullable` 全计划一致；`Cast.target` 进 node 的不变式在 Task 1 注释 + Task 5 表中一致声明。

---

## Execution Handoff

M0 计划完成。两种执行方式：
1. **Subagent-Driven（推荐）**：每 task 派新 subagent、task 间审查、快迭代。
2. **Inline**：本会话内按 executing-plans 批量执行 + checkpoint。
