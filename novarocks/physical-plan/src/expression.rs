// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::DataType;

use novarocks_type_contract::{
    AggregateStateFormatId, FunctionArgumentEvaluation, FunctionFailureBehavior, FunctionId,
    FunctionKind, FunctionOverloadId, FunctionVolatility,
};

use crate::{ExprId, FunctionArgumentType, ValueId, ValueType};

#[derive(Clone, Debug, PartialEq)]
pub struct BoundFunction {
    pub function_id: FunctionId,
    pub overload: FunctionOverloadId,
    pub kind: FunctionKind,
    pub argument_types: Box<[FunctionArgumentType]>,
    pub result_type: ValueType,
    pub volatility: FunctionVolatility,
    pub argument_evaluation: FunctionArgumentEvaluation,
    pub failure_behavior: FunctionFailureBehavior,
}

/// Exact binding for a function whose result is a relation rather than a
/// scalar value. Keeping this separate prevents outer-input pass-through
/// columns from being mistaken for function result columns.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundTableFunction {
    pub function_id: FunctionId,
    pub overload: FunctionOverloadId,
    pub argument_types: Box<[FunctionArgumentType]>,
    pub result_types: Box<[ValueType]>,
    pub volatility: FunctionVolatility,
    pub argument_evaluation: FunctionArgumentEvaluation,
    pub failure_behavior: FunctionFailureBehavior,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregatePhase {
    Single,
    Partial {
        sequence: crate::AggregateSequenceId,
    },
    Intermediate {
        sequence: crate::AggregateSequenceId,
    },
    Final {
        sequence: crate::AggregateSequenceId,
    },
}

impl AggregatePhase {
    pub const fn sequence(self) -> Option<crate::AggregateSequenceId> {
        match self {
            Self::Single => None,
            Self::Partial { sequence }
            | Self::Intermediate { sequence }
            | Self::Final { sequence } => Some(sequence),
        }
    }

    pub const fn consumes_logical_arguments(self) -> bool {
        matches!(self, Self::Single | Self::Partial { .. })
    }

    pub const fn produces_final_result(self) -> bool {
        matches!(self, Self::Single | Self::Final { .. })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateBinding {
    pub function: BoundFunction,
    pub phase: AggregatePhase,
    /// Number of logical SQL arguments at the front of `argument_types`.
    /// Remaining channels are aggregate-owned ORDER BY inputs.
    pub logical_argument_count: u32,
    pub intermediate_type: ValueType,
    pub state_format: AggregateStateFormatId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NullOrdering {
    First,
    Last,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SortExpr {
    pub expr: ExprId,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    BitwiseNot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Eq,
    EqForNull,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    BitAnd,
    BitOr,
    BitXor,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LiteralValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64Bits(u64),
    LargeInt(i128),
    Decimal128(i128),
    Utf8(Box<str>),
    Binary(Box<[u8]>),
    Date32(i32),
    Time64(i64),
    Timestamp(i64),
    IntervalMonthDayNano(i128),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WindowBound {
    UnboundedPreceding,
    Preceding(ExprId),
    CurrentRow,
    Following(ExprId),
    UnboundedFollowing,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start: WindowBound,
    pub end: WindowBound,
    pub exclusion: WindowFrameExclusion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowFrameExclusion {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExprKind {
    Value(ValueId),
    LambdaParameter {
        lambda: ExprId,
        ordinal: u32,
    },
    Literal(LiteralValue),
    Unary {
        op: UnaryOperator,
        expr: ExprId,
    },
    Binary {
        left: ExprId,
        op: BinaryOperator,
        right: ExprId,
    },
    FunctionCall {
        function: BoundFunction,
        args: Box<[ExprId]>,
    },
    Lambda {
        parameter_types: Box<[ValueType]>,
        body: ExprId,
    },
    Cast {
        expr: ExprId,
        target: DataType,
    },
    IsNull {
        expr: ExprId,
        negated: bool,
    },
    InList {
        expr: ExprId,
        list: Box<[ExprId]>,
        negated: bool,
    },
    Between {
        expr: ExprId,
        low: ExprId,
        high: ExprId,
        negated: bool,
    },
    Like {
        expr: ExprId,
        pattern: ExprId,
        negated: bool,
    },
    Case {
        operand: Option<ExprId>,
        when_then: Box<[(ExprId, ExprId)]>,
        else_expr: Option<ExprId>,
    },
    IsTruthValue {
        expr: ExprId,
        value: bool,
        negated: bool,
    },
    WindowCall {
        function: BoundFunction,
        distinct: bool,
        args: Box<[ExprId]>,
        function_order_by: Box<[SortExpr]>,
        frame: Option<WindowFrame>,
        ignore_nulls: bool,
        aggregate_binding: Option<AggregateBinding>,
    },
}

impl ExprKind {
    pub(crate) fn expression_references(&self, output: &mut Vec<ExprId>) {
        match self {
            Self::Value(_) | Self::LambdaParameter { .. } | Self::Literal(_) => {}
            Self::Unary { expr, .. }
            | Self::Cast { expr, .. }
            | Self::IsNull { expr, .. }
            | Self::IsTruthValue { expr, .. } => output.push(*expr),
            Self::Binary { left, right, .. } => output.extend([*left, *right]),
            Self::FunctionCall { args, .. } => {
                output.extend(args.iter().copied());
            }
            Self::Lambda { body, .. } => output.push(*body),
            Self::InList { expr, list, .. } => {
                output.push(*expr);
                output.extend(list.iter().copied());
            }
            Self::Between {
                expr, low, high, ..
            } => output.extend([*expr, *low, *high]),
            Self::Like { expr, pattern, .. } => output.extend([*expr, *pattern]),
            Self::Case {
                operand,
                when_then,
                else_expr,
            } => {
                output.extend(*operand);
                for (when, then) in when_then {
                    output.extend([*when, *then]);
                }
                output.extend(*else_expr);
            }
            Self::WindowCall {
                args,
                function_order_by,
                frame,
                ..
            } => {
                output.extend(args.iter().copied());
                output.extend(function_order_by.iter().map(|item| item.expr));
                if let Some(frame) = frame {
                    for bound in [&frame.start, &frame.end] {
                        if let WindowBound::Preceding(expr) | WindowBound::Following(expr) = bound {
                            output.push(*expr);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExprNode {
    pub id: ExprId,
    /// Physical node whose evaluation scope owns this expression.
    ///
    /// Expressions may be shared by multiple roots of the same node, but they
    /// never cross node scopes. This makes value visibility a local, linear
    /// validation instead of a repeated transitive graph walk.
    pub owner: crate::NodeId,
    /// Innermost enclosing lambda. `None` denotes the physical node scope.
    pub lambda_scope: Option<ExprId>,
    pub ty: ValueType,
    pub kind: ExprKind,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExprArena {
    nodes: BTreeMap<ExprId, ExprNode>,
}

impl ExprArena {
    pub fn get(&self, id: ExprId) -> Option<&ExprNode> {
        self.nodes.get(&id)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&ExprId, &ExprNode)> {
        self.nodes.iter()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub(crate) fn insert(&mut self, node: ExprNode) -> Option<ExprNode> {
        self.nodes.insert(node.id, node)
    }
}

/// Return whether every expression has replica-deterministic semantics.
///
/// Missing expression definitions fail closed. `allow_values` is explicit
/// because some ownership checks require closed expressions, while relational
/// operators may safely read identical input values on every replica.
pub fn expressions_are_replica_deterministic(
    arena: &ExprArena,
    expressions: impl IntoIterator<Item = ExprId>,
    allow_values: bool,
) -> bool {
    let mut pending = expressions.into_iter().collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    while let Some(expression) = pending.pop() {
        if !visited.insert(expression) {
            continue;
        }
        let Some(expression) = arena.get(expression) else {
            return false;
        };
        let immutable = match &expression.kind {
            ExprKind::Value(_) => allow_values,
            ExprKind::FunctionCall { function, .. } | ExprKind::WindowCall { function, .. } => {
                function.volatility == FunctionVolatility::Immutable
            }
            ExprKind::Literal(_)
            | ExprKind::LambdaParameter { .. }
            | ExprKind::Unary { .. }
            | ExprKind::Binary { .. }
            | ExprKind::Lambda { .. }
            | ExprKind::Cast { .. }
            | ExprKind::IsNull { .. }
            | ExprKind::InList { .. }
            | ExprKind::Between { .. }
            | ExprKind::Like { .. }
            | ExprKind::Case { .. }
            | ExprKind::IsTruthValue { .. } => true,
        };
        if !immutable {
            return false;
        }
        expression.kind.expression_references(&mut pending);
    }
    true
}
