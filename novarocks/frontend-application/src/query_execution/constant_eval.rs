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

//! Frontend adapter that answers the SQL compiler's constant-evaluation port
//! with the real execution kernels.
//!
//! The Frontend is the only owner allowed to bridge these two crates:
//! `novarocks-sql` must never depend on `novarocks-execution`, so a folded
//! literal can only stay bit-identical to runtime output if the Frontend
//! builds a one-node `ExprArena` and runs it through `ExprArena::eval`.
//!
//! This adapter is a dumb per-node calculator. Recursion, volatility gating,
//! foldable-shape policy, and the fail-open decision all live on the SQL side;
//! here the only decisions are "can this literal/node shape be represented
//! faithfully?" (`Ok(None)` when not) and "what did the kernel return?".

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, RecordBatchOptions, StringArray,
};
use arrow::compute::kernels::cast_utils::parse_decimal;
use arrow::datatypes::{DataType, Decimal128Type, Decimal256Type, DecimalType};
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema};
use novarocks_execution::exec::expr::function::lookup_function;
use novarocks_execution::exec::expr::{
    ExprArena, ExprId, ExprNode, LiteralValue as ExecLiteralValue,
};
use novarocks_functions::validate_function_value_type_observed;
use novarocks_sql::compiler::{
    BinOp, FoldNodeKind, FoldRequest, LiteralValue as SqlLiteralValue, SqlConstantEvaluationError,
    SqlConstantEvaluator, UnOp,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl, ValueLogicalType,
    ValueTypeVisit, field_logical_type, validate_value_type_structure_observed,
};
use novarocks_types::largeint;
use std::sync::Arc;

/// Zero-sized, stateless evaluator: it owns no session, catalog, or runtime
/// state, so one process-lifetime instance serves every compilation.
#[derive(Debug)]
struct ExecutionConstantEvaluator;

static EXECUTION_CONSTANT_EVALUATOR: ExecutionConstantEvaluator = ExecutionConstantEvaluator;

/// The Frontend-owned constant evaluator handed to the SQL compiler.
///
/// Frontend is the only crate that sees both the SQL compiler boundary and the
/// execution kernels, so it owns this adapter. Every compile request built here
/// passes it, which is what lets the optimizer fold constants with exactly the
/// semantics the runtime would have produced.
// Design: ADR-0100 (docs/adr/ADR-0100-constant-folding-reuses-execution-kernels-through-an-injected-port.md)
pub(crate) fn constant_evaluator() -> &'static dyn SqlConstantEvaluator {
    &EXECUTION_CONSTANT_EVALUATOR
}

impl SqlConstantEvaluator for ExecutionConstantEvaluator {
    fn eval_scalar(
        &self,
        request: &FoldRequest,
        control: &dyn PureCompileControl,
    ) -> Result<Option<SqlLiteralValue>, SqlConstantEvaluationError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let evaluated = (|| -> Result<Option<SqlLiteralValue>, SqlConstantEvaluationError> {
            // Validate every authored type before literal conversion or arena
            // publication. A decline must not hide an invalid later argument.
            let mut supported = legacy_value_type_supported(&request.result_type, &mut work)?;
            for arg in &request.args {
                work.step()?;
                supported &= legacy_value_type_supported(&arg.value_type, &mut work)?;
                if matches!(arg.value, SqlLiteralValue::Null) && !arg.value_type.nullable {
                    supported = false;
                }
                if matches!(arg.value, SqlLiteralValue::LargeInt(_))
                    && arg.value_type.logical_type != ValueLogicalType::LargeInt
                {
                    supported = false;
                }
            }
            if !supported {
                return Ok(None);
            }
            let mut arena = ExprArena::default();
            let mut arg_ids: Vec<ExprId> = Vec::with_capacity(request.args.len());
            for arg in &request.args {
                work.step()?;
                let Some(literal) = sql_literal_to_exec(&arg.value, &arg.value_type.data_type)
                else {
                    return Ok(None);
                };
                arg_ids.push(
                    arena.push_typed(ExprNode::Literal(literal), arg.value_type.data_type.clone()),
                );
            }

            work.step()?;
            let Some(root_node) = root_node_for(&request.kind, &arg_ids) else {
                return Ok(None);
            };
            let root = arena.push_typed(root_node, request.result_type.data_type.clone());

            let chunk = single_row_chunk()?;
            // The legacy kernel and literal/type cloning remain opaque here.
            // Flush the adapter's work and observe the original request before
            // entering it; this is not an internal kernel work/MEM guarantee.
            work.flush()?;
            let output = arena.eval(root, &chunk)?;
            work.step()?;
            let result = read_back_row0(&output, &request.result_type.data_type)?;
            if matches!(result, Some(SqlLiteralValue::Null)) && !request.result_type.nullable {
                return Ok(None);
            }
            Ok(result)
        })();
        // Success, conversion/shape declines and ordinary kernel/readback
        // errors all observe completion. Control failures remain typed and
        // never become a fail-open legacy evaluation String.
        if matches!(&evaluated, Err(SqlConstantEvaluationError::Control(_))) {
            // A delegated owner may return a typed resource/control refusal
            // without latching this adapter's scope. Keep that primary cause.
            return evaluated;
        }
        work.finish()?;
        evaluated
    }
}

/// The legacy arena carries Arrow types only. Its one exact non-Physical
/// scalar representation is the existing LARGEINT literal/kernel protocol;
/// all other semantic domains need the full-value evaluator migration.
fn legacy_value_type_supported(
    value_type: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlConstantEvaluationError> {
    work.step()?;
    value_type
        .logical_type
        .validate_carrier(&value_type.data_type)?;
    // The function owner supplies the actual type/metadata resource limits.
    // Its borrowed walk observes entries, not opaque cloning or kernel work.
    validate_function_value_type_observed(value_type, work)?;
    let mut nested_physical = true;
    validate_value_type_structure_observed::<SqlConstantEvaluationError>(
        &value_type.data_type,
        |visit| {
            work.step()?;
            if let ValueTypeVisit::Field(field) = visit {
                nested_physical &= field_logical_type(field)? == ValueLogicalType::Physical;
            }
            Ok(())
        },
    )?;
    Ok(nested_physical
        && match value_type.logical_type {
            ValueLogicalType::Physical => {
                !matches!(value_type.data_type, DataType::FixedSizeBinary(_))
            }
            ValueLogicalType::LargeInt => true,
            _ => false,
        })
}

#[cfg(test)]
fn fixture_value_type(value: &SqlLiteralValue, data_type: DataType) -> FunctionValueType {
    FunctionValueType {
        data_type,
        nullable: matches!(value, SqlLiteralValue::Null),
        logical_type: if matches!(value, SqlLiteralValue::LargeInt(_)) {
            ValueLogicalType::LargeInt
        } else {
            ValueLogicalType::Physical
        },
    }
}

#[cfg(test)]
struct TestControl;
#[cfg(test)]
impl PureCompileControl for TestControl {
    fn checkpoint(
        &self,
        _: CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}

/// A schemaless chunk with exactly one row.
///
/// Constant folding never reads a slot, so the chunk carries no columns; the
/// explicit row count is what makes every literal kernel materialize a
/// length-1 array.
fn single_row_chunk() -> Result<Chunk, String> {
    let chunk_schema = Arc::new(ChunkSchema::empty());
    let batch = RecordBatch::try_new_with_options(
        chunk_schema.arrow_schema_ref(),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|error| format!("constant folding failed to build a 1-row chunk: {error}"))?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

/// Maps one SQL fold node onto the execution expression it must reuse.
///
/// Returns `None` for any shape without a direct execution counterpart: the
/// adapter must never emulate a missing kernel.
fn root_node_for(kind: &FoldNodeKind, args: &[ExprId]) -> Option<ExprNode> {
    match kind {
        FoldNodeKind::BinaryOp(op, decimal_overflow_policy) => {
            let [lhs, rhs] = args else {
                return None;
            };
            let (lhs, rhs) = (*lhs, *rhs);
            Some(match op {
                BinOp::Add => ExprNode::Add(lhs, rhs, *decimal_overflow_policy),
                BinOp::Sub => ExprNode::Sub(lhs, rhs, *decimal_overflow_policy),
                BinOp::Mul => ExprNode::Mul(lhs, rhs, *decimal_overflow_policy),
                BinOp::Div => ExprNode::Div(lhs, rhs, *decimal_overflow_policy),
                BinOp::Mod => ExprNode::Mod(lhs, rhs, *decimal_overflow_policy),
                BinOp::Eq => ExprNode::Eq(lhs, rhs),
                BinOp::Ne => ExprNode::Ne(lhs, rhs),
                BinOp::Lt => ExprNode::Lt(lhs, rhs),
                BinOp::Le => ExprNode::Le(lhs, rhs),
                BinOp::Gt => ExprNode::Gt(lhs, rhs),
                BinOp::Ge => ExprNode::Ge(lhs, rhs),
                BinOp::EqForNull => ExprNode::EqForNull(lhs, rhs),
                BinOp::And => ExprNode::And(lhs, rhs),
                BinOp::Or => ExprNode::Or(lhs, rhs),
            })
        }
        FoldNodeKind::UnaryOp(op) => {
            let [child] = args else {
                return None;
            };
            match op {
                UnOp::Not => Some(ExprNode::Not(*child)),
                // Execution has no negation or bitwise-not expression node.
                // Emulating either here would reimplement semantics the
                // Frontend does not own, so decline instead.
                UnOp::Negate | UnOp::BitwiseNot => None,
            }
        }
        FoldNodeKind::Cast(decimal_overflow_policy) => {
            let [child] = args else {
                return None;
            };
            // The cast target is the node's own data type, which the caller
            // attaches through `push_typed(.., out_type)`.
            Some(ExprNode::Cast(*child, *decimal_overflow_policy))
        }
        FoldNodeKind::Function { name } => {
            let kind = lookup_function(name)?;
            Some(ExprNode::FunctionCall {
                kind,
                args: args.to_vec(),
            })
        }
    }
}

/// Turns one already-folded SQL literal into the execution literal the kernels
/// expect, driven by the argument's declared Arrow type.
///
/// Every arm is exact: a value that cannot round-trip through the target
/// representation yields `None` so the caller keeps the original expression.
fn sql_literal_to_exec(value: &SqlLiteralValue, data_type: &DataType) -> Option<ExecLiteralValue> {
    match (value, data_type) {
        // A typed NULL literal keeps its declared type: `ExprArena::eval`
        // materializes `new_null_array(out_type)` for a `Null` literal.
        (SqlLiteralValue::Null, _) => Some(ExecLiteralValue::Null),
        (SqlLiteralValue::Bool(v), DataType::Boolean) => Some(ExecLiteralValue::Bool(*v)),
        (SqlLiteralValue::Int(v), DataType::Int8) => {
            i8::try_from(*v).ok().map(ExecLiteralValue::Int8)
        }
        (SqlLiteralValue::Int(v), DataType::Int16) => {
            i16::try_from(*v).ok().map(ExecLiteralValue::Int16)
        }
        (SqlLiteralValue::Int(v), DataType::Int32) => {
            i32::try_from(*v).ok().map(ExecLiteralValue::Int32)
        }
        (SqlLiteralValue::Int(v), DataType::Int64) => Some(ExecLiteralValue::Int64(*v)),
        (SqlLiteralValue::Int(v), DataType::Date32) => {
            i32::try_from(*v).ok().map(ExecLiteralValue::Date32)
        }
        (SqlLiteralValue::LargeInt(v), dt) if largeint::is_largeint_data_type(dt) => {
            Some(ExecLiteralValue::LargeInt(*v))
        }
        (SqlLiteralValue::Float(v), DataType::Float64) => Some(ExecLiteralValue::Float64(*v)),
        (SqlLiteralValue::Float(v), DataType::Float32) => {
            // Accept only a FLOAT literal that survives the f64 -> f32 round
            // trip. Everything this adapter reads back from a Float32 column
            // does survive it, so the guard rejects exactly the widened values
            // the runtime would never have produced.
            let narrowed = *v as f32;
            (f64::from(narrowed).to_bits() == v.to_bits())
                .then_some(ExecLiteralValue::Float32(narrowed))
        }
        (SqlLiteralValue::String(v), DataType::Utf8) => Some(ExecLiteralValue::Utf8(v.clone())),
        (SqlLiteralValue::Binary(v), DataType::Binary) => Some(ExecLiteralValue::Binary(v.clone())),
        (SqlLiteralValue::Decimal(text), DataType::Decimal128(precision, scale)) => {
            exact_decimal_text(text, *scale)?;
            parse_decimal::<Decimal128Type>(text, *precision, *scale)
                .ok()
                .map(|value| ExecLiteralValue::Decimal128 {
                    value,
                    precision: *precision,
                    scale: *scale,
                })
        }
        (SqlLiteralValue::Decimal(text), DataType::Decimal256(precision, scale)) => {
            exact_decimal_text(text, *scale)?;
            parse_decimal::<Decimal256Type>(text, *precision, *scale)
                .ok()
                .map(|value| ExecLiteralValue::Decimal256 {
                    value,
                    precision: *precision,
                    scale: *scale,
                })
        }
        _ => None,
    }
}

/// Guards `parse_decimal`, which silently truncates surplus fraction digits
/// and accepts e-notation. Both would fold to a value the runtime never
/// produced, so only plain notation that fits the column scale is accepted.
fn exact_decimal_text(text: &str, scale: i8) -> Option<()> {
    let scale = usize::try_from(scale).ok()?;
    if text.contains(['e', 'E']) {
        return None;
    }
    match text.split_once('.') {
        Some((_, fraction)) => (fraction.len() <= scale).then_some(()),
        None => Some(()),
    }
}

/// Reads the single produced row back into the SQL literal vocabulary.
///
/// `Err` means the kernel produced something that contradicts the frozen node
/// type; `Ok(None)` means the output type has no faithful SQL literal form.
fn read_back_row0(
    output: &ArrayRef,
    out_type: &DataType,
) -> Result<Option<SqlLiteralValue>, String> {
    if output.len() != 1 {
        return Err(format!(
            "constant folding produced {} rows, expected exactly 1",
            output.len()
        ));
    }
    if !is_readable_output_type(out_type) {
        return Ok(None);
    }
    if output.data_type() != out_type {
        return Err(format!(
            "constant folding produced {:?}, expected {:?}",
            output.data_type(),
            out_type
        ));
    }
    if output.is_null(0) {
        return Ok(Some(SqlLiteralValue::Null));
    }

    let literal = match out_type {
        DataType::Boolean => SqlLiteralValue::Bool(downcast::<BooleanArray>(output)?.value(0)),
        DataType::Int8 => SqlLiteralValue::Int(i64::from(downcast::<Int8Array>(output)?.value(0))),
        DataType::Int16 => {
            SqlLiteralValue::Int(i64::from(downcast::<Int16Array>(output)?.value(0)))
        }
        DataType::Int32 => {
            SqlLiteralValue::Int(i64::from(downcast::<Int32Array>(output)?.value(0)))
        }
        DataType::Int64 => SqlLiteralValue::Int(downcast::<Int64Array>(output)?.value(0)),
        DataType::Date32 => {
            SqlLiteralValue::Int(i64::from(downcast::<Date32Array>(output)?.value(0)))
        }
        DataType::FixedSizeBinary(_) => SqlLiteralValue::LargeInt(largeint::i128_from_be_bytes(
            downcast::<FixedSizeBinaryArray>(output)?.value(0),
        )?),
        DataType::Float32 => {
            SqlLiteralValue::Float(f64::from(downcast::<Float32Array>(output)?.value(0)))
        }
        DataType::Float64 => SqlLiteralValue::Float(downcast::<Float64Array>(output)?.value(0)),
        // `StringArray::value` converts without revalidating, so a kernel is
        // able to hand back bytes that are not valid UTF-8. The plan encodes a
        // literal string as a protobuf string field, which would re-encode
        // those bytes, so decline rather than fold something the runtime would
        // not reproduce. (Byte-carrying string families such as `aes_encrypt`
        // are valid UTF-8 and are excluded by the SQL-side foldable list.)
        DataType::Utf8 => {
            let Some(text) = utf8_round_trippable(downcast::<StringArray>(output)?.value(0)) else {
                return Ok(None);
            };
            SqlLiteralValue::String(text)
        }
        DataType::LargeUtf8 => {
            let Some(text) = utf8_round_trippable(downcast::<LargeStringArray>(output)?.value(0))
            else {
                return Ok(None);
            };
            SqlLiteralValue::String(text)
        }
        DataType::Binary => {
            SqlLiteralValue::Binary(downcast::<BinaryArray>(output)?.value(0).to_vec())
        }
        DataType::LargeBinary => {
            SqlLiteralValue::Binary(downcast::<LargeBinaryArray>(output)?.value(0).to_vec())
        }
        // Arrow renders the unscaled value exactly at the column's scale, so
        // the folded text matches what the runtime would have printed — but
        // only while the value still fits the declared precision. A decimal
        // kernel is allowed to return a result wider than its own declared
        // precision (an overflowed multiply or a rounding cast such as
        // `CAST(99999.999 AS DECIMAL(7,2))` -> 100000.00), and rendering that
        // through the declared precision drops the leading digit. Decline the
        // fold instead, so the runtime keeps producing whatever it produces
        // today for out-of-range decimals.
        DataType::Decimal128(precision, _) => {
            let array = downcast::<Decimal128Array>(output)?;
            if !Decimal128Type::is_valid_decimal_precision(array.value(0), *precision) {
                return Ok(None);
            }
            SqlLiteralValue::Decimal(array.value_as_string(0))
        }
        DataType::Decimal256(precision, _) => {
            let array = downcast::<Decimal256Array>(output)?;
            if !Decimal256Type::is_valid_decimal_precision(array.value(0), *precision) {
                return Ok(None);
            }
            SqlLiteralValue::Decimal(array.value_as_string(0))
        }
        // `is_readable_output_type` already filtered everything else.
        _ => return Ok(None),
    };
    Ok(Some(literal))
}

/// Returns the string only when its bytes really are valid UTF-8.
///
/// `StringArray::value` converts without revalidating, so a kernel that stores
/// raw bytes in a Utf8 array yields a `&str` whose bytes would change the first
/// time they are re-encoded.
fn utf8_round_trippable(value: &str) -> Option<String> {
    std::str::from_utf8(value.as_bytes())
        .ok()
        .map(str::to_string)
}

/// Output types with an exact SQL literal representation.
fn is_readable_output_type(out_type: &DataType) -> bool {
    match out_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Date32
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary => true,
        // LARGEINT is the only FixedSizeBinary the SQL literal vocabulary can
        // express; any other width is opaque bytes with no literal form.
        DataType::FixedSizeBinary(_) => largeint::is_largeint_data_type(out_type),
        // A negative scale would render a text this adapter refuses to read
        // back in, so never fold into one.
        DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) => *scale >= 0,
        _ => false,
    }
}

fn downcast<T: 'static>(output: &ArrayRef) -> Result<&T, String> {
    output.as_any().downcast_ref::<T>().ok_or_else(|| {
        format!(
            "constant folding could not read a {:?} result as {}",
            output.data_type(),
            std::any::type_name::<T>()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_sql::compiler::FoldArg;

    fn arg(value: SqlLiteralValue, data_type: DataType) -> FoldArg {
        FoldArg {
            value_type: fixture_value_type(&value, data_type),
            value,
        }
    }

    fn fold(
        kind: FoldNodeKind,
        args: Vec<FoldArg>,
        out_type: DataType,
    ) -> Result<Option<SqlLiteralValue>, SqlConstantEvaluationError> {
        constant_evaluator().eval_scalar(
            &FoldRequest {
                kind,
                args,
                result_type: FunctionValueType::new(out_type, true),
            },
            &TestControl,
        )
    }

    #[test]
    fn folds_int32_addition() {
        let folded = fold(
            FoldNodeKind::BinaryOp(
                BinOp::Add,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            vec![
                arg(SqlLiteralValue::Int(1), DataType::Int32),
                arg(SqlLiteralValue::Int(1), DataType::Int32),
            ],
            DataType::Int32,
        );
        assert_eq!(folded, Ok(Some(SqlLiteralValue::Int(2))));
    }

    #[test]
    fn folds_date_format_of_date32_literal() {
        // 18262 is 2020-01-01 in days since the epoch. The kernel translates
        // the MySQL format through `mysql_format_to_chrono`, so `%Y-%m-%d`
        // renders as chrono `%Y-%m-%d`.
        let folded = fold(
            FoldNodeKind::Function {
                name: "date_format".to_string(),
            },
            vec![
                arg(SqlLiteralValue::Int(18262), DataType::Date32),
                arg(
                    SqlLiteralValue::String("%Y-%m-%d".to_string()),
                    DataType::Utf8,
                ),
            ],
            DataType::Utf8,
        );
        assert_eq!(
            folded,
            Ok(Some(SqlLiteralValue::String("2020-01-01".to_string())))
        );
    }

    #[test]
    fn folds_cast_of_utf8_to_date32() {
        // 1970-01-01 -> 2020-01-01 spans 50 years with 12 leap days
        // (1972..=2016 step 4, 2000 included), i.e. 50 * 365 + 12 = 18262.
        let folded = fold(
            FoldNodeKind::Cast(novarocks_type_contract::DecimalOverflowPolicy::OutputNull),
            vec![arg(
                SqlLiteralValue::String("2020-01-01".to_string()),
                DataType::Utf8,
            )],
            DataType::Date32,
        );
        assert_eq!(folded, Ok(Some(SqlLiteralValue::Int(18262))));
    }

    #[test]
    fn folds_decimal_multiplication_keeping_scale() {
        // 1.25 * 4.00 with an output scale equal to the sum of the input
        // scales, so the kernel neither rescales nor rounds: 125 * 400 = 50000
        // at scale 4.
        let folded = fold(
            FoldNodeKind::BinaryOp(
                BinOp::Mul,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            vec![
                arg(
                    SqlLiteralValue::Decimal("1.25".to_string()),
                    DataType::Decimal128(10, 2),
                ),
                arg(
                    SqlLiteralValue::Decimal("4.00".to_string()),
                    DataType::Decimal128(10, 2),
                ),
            ],
            DataType::Decimal128(20, 4),
        );
        assert_eq!(
            folded,
            Ok(Some(SqlLiteralValue::Decimal("5.0000".to_string())))
        );
    }

    #[test]
    fn folds_checked_decimal_cast_overflow_to_null() {
        // Half-up carry produces 100000.00, requiring precision 8; the frozen
        // (7,2) cast therefore returns NULL under OutputNull.
        let folded = fold(
            FoldNodeKind::Cast(novarocks_type_contract::DecimalOverflowPolicy::OutputNull),
            vec![arg(
                SqlLiteralValue::Decimal("99999.999".to_string()),
                DataType::Decimal128(8, 3),
            )],
            DataType::Decimal128(7, 2),
        );
        assert_eq!(folded, Ok(Some(SqlLiteralValue::Null)));
    }

    #[test]
    fn folds_decimal_cast_that_still_fits_its_precision() {
        // Same rounding shape as above, one digit of headroom: the guard must
        // not reject a result the declared precision can hold.
        let folded = fold(
            FoldNodeKind::Cast(novarocks_type_contract::DecimalOverflowPolicy::OutputNull),
            vec![arg(
                SqlLiteralValue::Decimal("99999.999".to_string()),
                DataType::Decimal128(8, 3),
            )],
            DataType::Decimal128(8, 2),
        );
        assert_eq!(
            folded,
            Ok(Some(SqlLiteralValue::Decimal("100000.00".to_string())))
        );
    }

    #[test]
    fn declines_unknown_function() {
        let folded = fold(
            FoldNodeKind::Function {
                name: "no_such_novarocks_function".to_string(),
            },
            vec![arg(SqlLiteralValue::Int(1), DataType::Int32)],
            DataType::Int32,
        );
        assert_eq!(folded, Ok(None));
    }

    #[test]
    fn declines_unary_negate() {
        let folded = fold(
            FoldNodeKind::UnaryOp(UnOp::Negate),
            vec![arg(SqlLiteralValue::Int(1), DataType::Int32)],
            DataType::Int32,
        );
        assert_eq!(folded, Ok(None));

        // `NOT` is the one unary op with a direct execution node.
        let folded_not = fold(
            FoldNodeKind::UnaryOp(UnOp::Not),
            vec![arg(SqlLiteralValue::Bool(true), DataType::Boolean)],
            DataType::Boolean,
        );
        assert_eq!(folded_not, Ok(Some(SqlLiteralValue::Bool(false))));
    }

    #[test]
    fn declines_unmappable_argument_literal() {
        // A string value carried on an INT slot has no faithful execution
        // literal: the adapter must not parse or coerce it.
        let mismatched_kind = fold(
            FoldNodeKind::BinaryOp(
                BinOp::Add,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            vec![
                arg(SqlLiteralValue::String("7".to_string()), DataType::Int32),
                arg(SqlLiteralValue::Int(1), DataType::Int32),
            ],
            DataType::Int32,
        );
        assert_eq!(mismatched_kind, Ok(None));

        // An INT literal that does not fit its declared width is equally
        // unmappable.
        let out_of_range = fold(
            FoldNodeKind::BinaryOp(
                BinOp::Add,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            vec![
                arg(SqlLiteralValue::Int(i64::MAX), DataType::Int32),
                arg(SqlLiteralValue::Int(1), DataType::Int32),
            ],
            DataType::Int32,
        );
        assert_eq!(out_of_range, Ok(None));

        // More fraction digits than the column scale would be truncated, so
        // the decimal is declined rather than folded to a different value.
        let lossy_decimal = fold(
            FoldNodeKind::BinaryOp(
                BinOp::Add,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            vec![
                arg(
                    SqlLiteralValue::Decimal("1.239".to_string()),
                    DataType::Decimal128(10, 2),
                ),
                arg(
                    SqlLiteralValue::Decimal("1.00".to_string()),
                    DataType::Decimal128(10, 2),
                ),
            ],
            DataType::Decimal128(10, 2),
        );
        assert_eq!(lossy_decimal, Ok(None));
    }

    #[test]
    fn division_by_zero_folds_to_null() {
        // Observed behavior: `arithmetic::eval_div` nullifies zero divisors
        // (matching StarRocks), so the kernel yields NULL rather than `Err`.
        // The adapter therefore folds `1 / 0` to a typed NULL literal instead
        // of surfacing an evaluation failure.
        let folded = fold(
            FoldNodeKind::BinaryOp(
                BinOp::Div,
                novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            ),
            vec![
                arg(SqlLiteralValue::Int(1), DataType::Int32),
                arg(SqlLiteralValue::Int(0), DataType::Int32),
            ],
            DataType::Float64,
        );
        assert_eq!(folded, Ok(Some(SqlLiteralValue::Null)));
    }
}

#[cfg(test)]
mod full_value_type_tests {
    use super::*;
    use arrow::datatypes::Field;
    use novarocks_sql::compiler::FoldArg;
    use novarocks_type_contract::{DecimalOverflowPolicy, NR_LOGICAL_TYPE_KEY, ValueTypeError};

    fn cast(
        source: FunctionValueType,
        result_type: FunctionValueType,
        value: SqlLiteralValue,
    ) -> FoldRequest {
        FoldRequest {
            kind: FoldNodeKind::Cast(DecimalOverflowPolicy::OutputNull),
            args: vec![FoldArg {
                value,
                value_type: source,
            }],
            result_type,
        }
    }

    #[test]
    fn authored_semantic_roots_decline_instead_of_carrier_retagging() {
        for (carrier, logical, value) in [
            (
                DataType::Utf8,
                ValueLogicalType::Json,
                SqlLiteralValue::String("{}".into()),
            ),
            (
                DataType::LargeBinary,
                ValueLogicalType::Variant,
                SqlLiteralValue::Binary(vec![1]),
            ),
            (
                DataType::Binary,
                ValueLogicalType::Hll,
                SqlLiteralValue::Binary(vec![1]),
            ),
            (
                DataType::Binary,
                ValueLogicalType::Bitmap,
                SqlLiteralValue::Binary(vec![1]),
            ),
            (
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Uuid,
                SqlLiteralValue::Null,
            ),
        ] {
            let semantic = FunctionValueType {
                data_type: carrier.clone(),
                nullable: true,
                logical_type: logical,
            };
            let physical = FunctionValueType::new(carrier, true);
            for request in [
                cast(semantic.clone(), physical.clone(), value.clone()),
                cast(physical, semantic, value),
            ] {
                assert_eq!(
                    constant_evaluator().eval_scalar(&request, &TestControl),
                    Ok(None)
                );
            }
        }
    }

    #[test]
    fn nested_logical_identity_declines_and_invalid_later_types_are_not_hidden() {
        let child = Field::new("item", DataType::Utf8, true)
            .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "json".into())].into());
        let nested = FunctionValueType::new(DataType::List(Arc::new(child)), true);
        let mut request = cast(nested.clone(), nested, SqlLiteralValue::Null);
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &TestControl),
            Ok(None)
        );
        request.args.push(FoldArg {
            value: SqlLiteralValue::Int(1),
            value_type: FunctionValueType {
                data_type: DataType::Int64,
                nullable: false,
                logical_type: ValueLogicalType::Json,
            },
        });
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &TestControl),
            Err(SqlConstantEvaluationError::InvalidType(
                ValueTypeError::InvalidLogicalCarrier(ValueLogicalType::Json)
            ))
        );
        let bad = Field::new("item", DataType::Utf8, true)
            .with_metadata([(NR_LOGICAL_TYPE_KEY.into(), "unknown-domain".into())].into());
        request.result_type = FunctionValueType::new(DataType::List(Arc::new(bad)), true);
        assert!(matches!(
            constant_evaluator().eval_scalar(&request, &TestControl),
            Err(SqlConstantEvaluationError::Preparation(_))
        ));
    }

    #[test]
    fn nonnullable_input_and_actual_null_output_decline() {
        let physical = FunctionValueType::new(DataType::Int32, false);
        let request = cast(
            physical.clone(),
            FunctionValueType::new(DataType::Int32, true),
            SqlLiteralValue::Null,
        );
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &TestControl),
            Ok(None)
        );
        let request = FoldRequest {
            kind: FoldNodeKind::BinaryOp(BinOp::Div, DecimalOverflowPolicy::OutputNull),
            args: vec![
                FoldArg {
                    value: SqlLiteralValue::Int(1),
                    value_type: physical.clone(),
                },
                FoldArg {
                    value: SqlLiteralValue::Int(0),
                    value_type: physical,
                },
            ],
            result_type: FunctionValueType::new(DataType::Float64, false),
        };
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &TestControl),
            Ok(None)
        );
    }

    #[test]
    fn exact_largeint_intrinsic_round_trips_without_fixed16_inference() {
        let ty = FunctionValueType {
            data_type: DataType::FixedSizeBinary(16),
            nullable: false,
            logical_type: ValueLogicalType::LargeInt,
        };
        let value = i128::from(i64::MAX) + 17;
        let request = cast(ty.clone(), ty.clone(), SqlLiteralValue::LargeInt(value));
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &TestControl),
            Ok(Some(SqlLiteralValue::LargeInt(value)))
        );
        let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), false);
        for request in [
            cast(physical.clone(), ty, SqlLiteralValue::LargeInt(value)),
            cast(physical.clone(), physical, SqlLiteralValue::LargeInt(value)),
        ] {
            assert_eq!(
                constant_evaluator().eval_scalar(&request, &TestControl),
                Ok(None)
            );
        }
    }
}

#[cfg(test)]
mod overflow_policy_tests {
    use super::*;
    use novarocks_sql::compiler::FoldArg;
    use novarocks_type_contract::DecimalOverflowPolicy::{OutputNull, ReportError};
    #[test]
    fn real_execution_fold_distinguishes_checked_decimal_error_from_null() {
        let args = vec![
            FoldArg {
                value: SqlLiteralValue::Decimal(
                    "99999999999999999999999999999999999999".to_string(),
                ),
                value_type: FunctionValueType::new(DataType::Decimal128(38, 0), false),
            },
            FoldArg {
                value: SqlLiteralValue::Int(1),
                value_type: FunctionValueType::new(DataType::Int64, false),
            },
        ];
        for policy in [OutputNull, ReportError] {
            let request = FoldRequest {
                kind: FoldNodeKind::BinaryOp(BinOp::Add, policy),
                args: args.clone(),
                result_type: FunctionValueType::new(DataType::Decimal128(38, 0), true),
            };
            let result = constant_evaluator().eval_scalar(&request, &TestControl);
            if policy == OutputNull {
                assert_eq!(result, Ok(Some(SqlLiteralValue::Null)));
            } else {
                let SqlConstantEvaluationError::Evaluation(message) = result.unwrap_err() else {
                    panic!("ordinary decimal overflow must remain an evaluation error");
                };
                assert!(message.contains("'add' operation involving decimal values overflows"));
            }
            let request = FoldRequest {
                kind: FoldNodeKind::Cast(policy),
                args: vec![args[0].clone()],
                result_type: FunctionValueType::new(DataType::Decimal128(9, 0), true),
            };
            let result = constant_evaluator().eval_scalar(&request, &TestControl);
            if policy == OutputNull {
                assert_eq!(result, Ok(Some(SqlLiteralValue::Null)));
            } else {
                let SqlConstantEvaluationError::Evaluation(message) = result.unwrap_err() else {
                    panic!("ordinary decimal overflow must remain an evaluation error");
                };
                assert!(message.contains("overflows"));
            }
        }
    }
}

#[cfg(test)]
mod request_control_tests {
    use super::*;
    use novarocks_sql::compiler::FoldArg;
    use novarocks_type_contract::{
        CompileControlError, DecimalOverflowPolicy, MAX_UNOBSERVED_COMPILE_WORK,
    };
    use std::sync::Mutex;

    #[derive(Default)]
    struct Control {
        checks: Mutex<Vec<u32>>,
        fail_at: Option<(usize, CompileControlError)>,
    }
    impl Control {
        fn checks(&self) -> Vec<u32> {
            self.checks.lock().unwrap().clone()
        }
        fn failing(at: usize, error: CompileControlError) -> Self {
            Self {
                checks: Mutex::default(),
                fail_at: Some((at, error)),
            }
        }
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::FunctionSpecialization);
            assert!(units <= MAX_UNOBSERVED_COMPILE_WORK);
            let mut checks = self.checks.lock().unwrap();
            let index = checks.len();
            checks.push(units);
            if let Some((at, error)) = self.fail_at
                && index == at
            {
                Err(error)
            } else {
                Ok(())
            }
        }
    }
    fn arg(value: SqlLiteralValue, data_type: DataType) -> FoldArg {
        FoldArg {
            value_type: fixture_value_type(&value, data_type),
            value,
        }
    }
    fn request(kind: FoldNodeKind, args: Vec<FoldArg>, out_type: DataType) -> FoldRequest {
        FoldRequest {
            kind,
            args,
            result_type: FunctionValueType::new(out_type, true),
        }
    }
    fn all_errors() -> [CompileControlError; 3] {
        [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ]
    }
    fn assert_stopped(
        request: &FoldRequest,
        checks: &[u32],
        at: usize,
        error: CompileControlError,
    ) {
        let control = Control::failing(at, error);
        assert_eq!(
            constant_evaluator().eval_scalar(request, &control),
            Err(SqlConstantEvaluationError::Control(error))
        );
        assert_eq!(control.checks(), checks[..=at]);
    }

    #[test]
    fn real_wide_arguments_observe_entry_interior_and_decline_completion() {
        let request = request(
            FoldNodeKind::Function {
                name: "no_such_novarocks_function".into(),
            },
            vec![arg(SqlLiteralValue::Int(7), DataType::Int64); 320],
            DataType::Int64,
        );
        let control = Control::default();
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &control),
            Ok(None)
        );
        let checks = control.checks();
        assert_eq!(checks[0], 0);
        let interior = checks.iter().position(|units| *units == 256).unwrap();
        let finish = checks.len() - 1;
        assert!(finish > interior);
        assert!((1..256).contains(&checks[finish]));
        for error in all_errors() {
            for at in [0, interior, finish] {
                assert_stopped(&request, &checks, at, error);
            }
        }
    }

    #[test]
    fn authored_type_metadata_interior_and_decline_tail_keep_original_control() {
        use arrow::datatypes::Field;
        use novarocks_type_contract::NR_LOGICAL_TYPE_KEY;
        let mut metadata = (0..255)
            .map(|index| (format!("provider.fact.{index}"), "value".into()))
            .collect::<std::collections::HashMap<_, _>>();
        metadata.insert(NR_LOGICAL_TYPE_KEY.into(), "json".into());
        let field = Field::new("item", DataType::Utf8, true).with_metadata(metadata);
        let mut request = request(
            FoldNodeKind::Cast(DecimalOverflowPolicy::OutputNull),
            vec![],
            DataType::Null,
        );
        request.result_type = FunctionValueType::new(DataType::List(Arc::new(field)), true);
        let control = Control::default();
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &control),
            Ok(None)
        );
        let checks = control.checks();
        assert_eq!(checks[0], 0);
        let interior = checks.iter().position(|units| *units == 256).unwrap();
        let tail = checks.len() - 1;
        assert!(tail > interior);
        assert!(checks[tail] > 0);
        for error in all_errors() {
            for at in [0, interior, tail] {
                assert_stopped(&request, &checks, at, error);
            }
        }
    }

    #[test]
    fn shared_type_resource_refusal_cannot_be_replaced_by_later_control() {
        use arrow::datatypes::Field;
        use novarocks_type_contract::MAX_ARROW_FIELD_METADATA_VALUE_BYTES;
        let field = Field::new("item", DataType::Utf8, true).with_metadata(
            [(
                "provider.fact".into(),
                "x".repeat(MAX_ARROW_FIELD_METADATA_VALUE_BYTES + 1),
            )]
            .into(),
        );
        let mut request = request(
            FoldNodeKind::Cast(DecimalOverflowPolicy::OutputNull),
            vec![],
            DataType::Null,
        );
        request.result_type = FunctionValueType::new(DataType::List(Arc::new(field)), true);
        let control = Control::failing(1, CompileControlError::Cancelled);
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &control),
            Err(SqlConstantEvaluationError::Control(
                CompileControlError::ResourceExhausted
            ))
        );
        assert_eq!(control.checks(), [0]);
    }

    #[test]
    fn literal_conversion_decline_still_observes_completion() {
        let request = request(
            FoldNodeKind::Cast(DecimalOverflowPolicy::OutputNull),
            vec![arg(SqlLiteralValue::String("7".into()), DataType::Int32)],
            DataType::Int64,
        );
        let control = Control::default();
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &control),
            Ok(None)
        );
        let checks = control.checks();
        assert_eq!(checks.len(), 2);
        assert_eq!(checks[0], 0);
        assert!(checks[1] > 0);
        for error in all_errors() {
            assert_stopped(&request, &checks, 1, error);
        }
    }

    #[test]
    fn output_tail_cancellation_never_returns_the_completed_kernel_value() {
        let request = request(
            FoldNodeKind::BinaryOp(BinOp::Add, DecimalOverflowPolicy::OutputNull),
            vec![arg(SqlLiteralValue::Int(1), DataType::Int32); 2],
            DataType::Int32,
        );
        let control = Control::default();
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &control),
            Ok(Some(SqlLiteralValue::Int(2)))
        );
        let checks = control.checks();
        assert_eq!(checks[0], 0);
        assert_eq!(checks.len(), 3);
        assert!(checks[1] > 0); // Actual argument/root work flushes before eval.
        assert!(checks[2] > 0); // The actual produced row observes completion.
        for error in all_errors() {
            for at in [1, 2] {
                assert_stopped(&request, &checks, at, error);
            }
        }
    }

    #[test]
    fn ordinary_kernel_errors_remain_evaluation_errors_and_observe_completion() {
        let request = request(
            FoldNodeKind::BinaryOp(BinOp::Add, DecimalOverflowPolicy::ReportError),
            vec![
                arg(
                    SqlLiteralValue::Decimal("99999999999999999999999999999999999999".into()),
                    DataType::Decimal128(38, 0),
                ),
                arg(SqlLiteralValue::Int(1), DataType::Int64),
            ],
            DataType::Decimal128(38, 0),
        );
        let control = Control::default();
        let SqlConstantEvaluationError::Evaluation(message) = constant_evaluator()
            .eval_scalar(&request, &control)
            .unwrap_err()
        else {
            panic!("the actual decimal kernel error must not become request control");
        };
        assert!(message.contains("'add' operation involving decimal values overflows"));
        let checks = control.checks();
        assert_eq!(checks.len(), 3);
        assert_eq!(checks[2], 0); // A failed opaque call still observes exit.
        for error in all_errors() {
            assert_stopped(&request, &checks, 2, error);
        }
    }

    #[test]
    fn unreadable_output_decline_observes_the_same_original_control() {
        let request = request(
            FoldNodeKind::Cast(DecimalOverflowPolicy::OutputNull),
            vec![arg(SqlLiteralValue::Int(7), DataType::Int32)],
            DataType::UInt32,
        );
        let control = Control::default();
        assert_eq!(
            constant_evaluator().eval_scalar(&request, &control),
            Ok(None)
        );
        let checks = control.checks();
        assert_eq!(checks.len(), 3);
        for error in all_errors() {
            assert_stopped(&request, &checks, checks.len() - 1, error);
        }
    }
}
