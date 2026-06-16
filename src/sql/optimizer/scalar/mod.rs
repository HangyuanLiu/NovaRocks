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
use std::hash::{Hash, Hasher};

use arrow::datatypes::DataType;

use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
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

impl Hash for HashableLiteral {
    fn hash<H: Hasher>(&self, state: &mut H) {
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
    BinaryOp {
        op: BinOp,
        left: ScalarId,
        right: ScalarId,
    },
    FunctionCall {
        name: String,
        args: Vec<ScalarId>,
        distinct: bool,
    },
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
        Self {
            nodes: Vec::new(),
            types: Vec::new(),
            nullable: Vec::new(),
            intern: HashMap::new(),
        }
    }

    /// Intern a node. Returns the existing id for a structurally-identical node.
    /// `ty`/`nullable` are the computed properties of the expression; they MUST
    /// be a function of the node (debug-asserted on a dedup hit).
    pub(crate) fn intern(&mut self, node: ScalarNode, ty: DataType, nullable: bool) -> ScalarId {
        let node = Self::normalize(node);
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

    /// Canonicalize commutative binary ops by ordering operands by ScalarId, so
    /// `a AND b` and `b AND a` intern to one id. Mirrors StarRocks
    /// normalizeChildrenGroup.
    fn normalize(node: ScalarNode) -> ScalarNode {
        if let ScalarNode::BinaryOp { op, left, right } = node {
            let commutative = matches!(op, BinOp::And | BinOp::Or | BinOp::Eq);
            if commutative && left.0 > right.0 {
                return ScalarNode::BinaryOp {
                    op,
                    left: right,
                    right: left,
                };
            }
            return ScalarNode::BinaryOp { op, left, right };
        }
        node
    }

    pub(crate) fn node(&self, id: ScalarId) -> &ScalarNode {
        &self.nodes[id.0 as usize]
    }

    pub(crate) fn data_type(&self, id: ScalarId) -> &DataType {
        &self.types[id.0 as usize]
    }

    pub(crate) fn nullable(&self, id: ScalarId) -> bool {
        self.nullable[id.0 as usize]
    }
}

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
            ScalarNode::BinaryOp {
                op: *op,
                left: l,
                right: r,
            }
        }
        ExprKind::FunctionCall {
            name,
            args,
            distinct,
        } => {
            let arg_ids: Vec<ScalarId> = args.iter().map(|a| intern_typed(arena, a)).collect();
            ScalarNode::FunctionCall {
                name: name.clone(),
                args: arg_ids,
                distinct: *distinct,
            }
        }
        other => unimplemented!("intern_typed: ExprKind variant not covered in M0: {other:?}"),
    };
    arena.intern(node, expr.data_type.clone(), expr.nullable)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{BinOp, LiteralValue};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn int() -> DataType {
        DataType::Int64
    }

    #[test]
    fn intern_dedups_structurally_equal_nodes() {
        let mut a = ScalarArena::new();
        let c = a.intern(ScalarNode::ColumnRef(ColumnId(1)), int(), false);
        let c2 = a.intern(ScalarNode::ColumnRef(ColumnId(1)), int(), false);
        assert_eq!(c, c2, "same ColumnRef must intern to one id");
        let d = a.intern(ScalarNode::ColumnRef(ColumnId(2)), int(), false);
        assert_ne!(c, d, "different ColumnRef must get different ids");

        let lit = a.intern(
            ScalarNode::Literal(HashableLiteral(LiteralValue::Int(7))),
            int(),
            false,
        );
        let add1 = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Add,
                left: c,
                right: lit,
            },
            int(),
            false,
        );
        let add2 = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Add,
                left: c,
                right: lit,
            },
            int(),
            false,
        );
        assert_eq!(
            add1, add2,
            "same BinaryOp over same child ids must intern to one id"
        );
        assert_eq!(a.data_type(add1), &int());
        assert!(!a.nullable(add1));
    }

    #[test]
    fn commutative_ops_normalize_to_one_id() {
        let mut a = ScalarArena::new();
        let x = a.intern(ScalarNode::ColumnRef(ColumnId(1)), int(), false);
        let y = a.intern(ScalarNode::ColumnRef(ColumnId(2)), int(), false);
        let b = DataType::Boolean;
        let xy = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left: x,
                right: y,
            },
            b.clone(),
            false,
        );
        let yx = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left: y,
                right: x,
            },
            b.clone(),
            false,
        );
        assert_eq!(xy, yx, "AND must be commutative-normalized to one id");

        let sub_xy = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Sub,
                left: x,
                right: y,
            },
            int(),
            false,
        );
        let sub_yx = a.intern(
            ScalarNode::BinaryOp {
                op: BinOp::Sub,
                left: y,
                right: x,
            },
            int(),
            false,
        );
        assert_ne!(sub_xy, sub_yx, "Sub must NOT be normalized");
    }
}

#[cfg(test)]
mod bridge_tests {
    use super::*;
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};
    use crate::sql::column_id::ColumnId;
    use arrow::datatypes::DataType;

    fn col(id: u32, ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: format!("c{id}"),
            },
            data_type: ty,
            nullable: false,
        }
    }

    fn lit_int(v: i64) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(v)),
            data_type: DataType::Int64,
            nullable: false,
        }
    }

    fn eq(l: TypedExpr, r: TypedExpr) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::BinaryOp {
                left: Box::new(l),
                op: BinOp::Eq,
                right: Box::new(r),
            },
            data_type: DataType::Boolean,
            nullable: false,
        }
    }

    #[test]
    fn intern_typed_dedups_independent_identical_exprs() {
        let mut a = ScalarArena::new();
        // Independently constructed but structurally identical TypedExpr trees
        // must intern to the same ScalarId.
        let id1 = intern_typed(&mut a, &eq(col(1, DataType::Int64), lit_int(5)));
        let id2 = intern_typed(&mut a, &eq(col(1, DataType::Int64), lit_int(5)));
        assert_eq!(
            id1, id2,
            "structurally-identical TypedExprs must intern to one ScalarId"
        );

        let id3 = intern_typed(&mut a, &eq(col(1, DataType::Int64), lit_int(6)));
        assert_ne!(id1, id3);
    }
}
