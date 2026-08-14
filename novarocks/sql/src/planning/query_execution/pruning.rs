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

/// Immutable min/max predicate projection for native scan preparation.
/// SQL owns expression inspection; Core maps this neutral DTO to its execution
/// runtime representation without observing the SQL expression tree.
#[derive(Clone, Debug, PartialEq)]
pub enum NativeMinMaxPredicate {
    Eq {
        column: String,
        value: NativeMinMaxPredicateValue,
    },
    Lt {
        column: String,
        value: NativeMinMaxPredicateValue,
    },
    Le {
        column: String,
        value: NativeMinMaxPredicateValue,
    },
    Gt {
        column: String,
        value: NativeMinMaxPredicateValue,
    },
    Ge {
        column: String,
        value: NativeMinMaxPredicateValue,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum NativeMinMaxPredicateValue {
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    ByteArray(Vec<u8>),
    Date32(i32),
    DateTimeMicros(i64),
    DateTimeNanos(i64),
}

pub fn native_scan_min_max_predicates(
    predicates: &[crate::analysis::TypedExpr],
) -> Vec<NativeMinMaxPredicate> {
    let mut out = Vec::new();
    for predicate in predicates {
        collect_native_min_max_predicates(predicate, &mut out);
    }
    out
}

fn collect_native_min_max_predicates(
    expr: &crate::analysis::TypedExpr,
    out: &mut Vec<NativeMinMaxPredicate>,
) {
    use crate::analysis::{BinOp, ExprKind};

    match &expr.kind {
        ExprKind::Nested(inner) => collect_native_min_max_predicates(inner, out),
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_native_min_max_predicates(left, out);
            collect_native_min_max_predicates(right, out);
        }
        ExprKind::BinaryOp { left, op, right } => {
            if let Some(predicate) = native_min_max_comparison(left, *op, right) {
                out.push(predicate);
            } else if let Some(predicate) =
                native_min_max_comparison(right, reverse_comparison(*op), left)
            {
                out.push(predicate);
            }
        }
        _ => {}
    }
}

fn reverse_comparison(op: crate::analysis::BinOp) -> crate::analysis::BinOp {
    use crate::analysis::BinOp;
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::Le => BinOp::Ge,
        BinOp::Gt => BinOp::Lt,
        BinOp::Ge => BinOp::Le,
        other => other,
    }
}

fn native_min_max_comparison(
    column: &crate::analysis::TypedExpr,
    op: crate::analysis::BinOp,
    literal: &crate::analysis::TypedExpr,
) -> Option<NativeMinMaxPredicate> {
    use crate::analysis::{BinOp, ExprKind};

    let ExprKind::ColumnRef { column: name, .. } = &column.kind else {
        return None;
    };
    if column.data_type != literal.data_type {
        return None;
    }
    let value = native_min_max_literal(literal)?;
    Some(match op {
        BinOp::Eq => NativeMinMaxPredicate::Eq {
            column: name.clone(),
            value,
        },
        BinOp::Lt => NativeMinMaxPredicate::Lt {
            column: name.clone(),
            value,
        },
        BinOp::Le => NativeMinMaxPredicate::Le {
            column: name.clone(),
            value,
        },
        BinOp::Gt => NativeMinMaxPredicate::Gt {
            column: name.clone(),
            value,
        },
        BinOp::Ge => NativeMinMaxPredicate::Ge {
            column: name.clone(),
            value,
        },
        _ => return None,
    })
}

fn native_min_max_literal(expr: &crate::analysis::TypedExpr) -> Option<NativeMinMaxPredicateValue> {
    use crate::analysis::{ExprKind, LiteralValue};
    use arrow::datatypes::{DataType, TimeUnit};

    let ExprKind::Literal(literal) = &expr.kind else {
        return None;
    };
    match (&expr.data_type, literal) {
        (DataType::Boolean, LiteralValue::Bool(value)) => {
            Some(NativeMinMaxPredicateValue::Boolean(*value))
        }
        (DataType::Int8 | DataType::Int16 | DataType::Int32, LiteralValue::Int(value)) => {
            i32::try_from(*value)
                .ok()
                .map(NativeMinMaxPredicateValue::Int32)
        }
        (DataType::Int64, LiteralValue::Int(value)) => {
            Some(NativeMinMaxPredicateValue::Int64(*value))
        }
        (DataType::Float32, LiteralValue::Float(value)) if value.is_finite() => {
            Some(NativeMinMaxPredicateValue::Float(*value as f32))
        }
        (DataType::Float64, LiteralValue::Float(value)) if value.is_finite() => {
            Some(NativeMinMaxPredicateValue::Double(*value))
        }
        (DataType::Utf8 | DataType::LargeUtf8, LiteralValue::String(value)) => Some(
            NativeMinMaxPredicateValue::ByteArray(value.as_bytes().to_vec()),
        ),
        (DataType::Binary | DataType::LargeBinary, LiteralValue::Binary(value)) => {
            Some(NativeMinMaxPredicateValue::ByteArray(value.clone()))
        }
        (DataType::Date32, LiteralValue::Int(value)) => i32::try_from(*value)
            .ok()
            .map(NativeMinMaxPredicateValue::Date32),
        (DataType::Timestamp(TimeUnit::Microsecond, _), LiteralValue::Int(value)) => {
            Some(NativeMinMaxPredicateValue::DateTimeMicros(*value))
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), LiteralValue::Int(value)) => {
            Some(NativeMinMaxPredicateValue::DateTimeNanos(*value))
        }
        _ => None,
    }
}
