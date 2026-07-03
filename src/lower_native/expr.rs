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

//! Proto expression lowering.

use arrow::datatypes::DataType;
use arrow_buffer::i256;

use super::decode_type;
use super::layout::Layout;
use crate::common::ids::SlotId;
use crate::exec::expr::function::{FunctionKind, function_metadata, lookup_function};
use crate::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use crate::proto::{common, expr};
use crate::thrift::types;

#[allow(dead_code)]
pub(crate) fn lower_proto_expr(
    e: &expr::Expr,
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<ExprId, String> {
    let data_type = decode_expr_type(e)?;
    let kind = e
        .kind
        .as_ref()
        .ok_or_else(|| "Expr.kind missing".to_string())?;

    match kind {
        expr::expr::Kind::ColumnRef(column) => {
            let slot_id = input_layout.resolve_column_id(column.column_id)?;
            Ok(arena.push_typed(ExprNode::SlotId(slot_id), data_type))
        }
        expr::expr::Kind::Literal(literal) => {
            let value = lower_literal(literal, &data_type)?;
            Ok(arena.push_typed(ExprNode::Literal(value), data_type))
        }
        expr::expr::Kind::BinaryOp(binary) => {
            lower_binary_op(binary, arena, input_layout, data_type)
        }
        expr::expr::Kind::UnaryOp(unary) => lower_unary_op(unary, arena, input_layout, data_type),
        expr::expr::Kind::FunctionCall(call) => {
            lower_function_call(call, arena, input_layout, data_type)
        }
        expr::expr::Kind::AggregateCall(_) => Err(
            "native scalar expr lowering does not lower AggregateCall; aggregate node handles it"
                .to_string(),
        ),
        expr::expr::Kind::WindowCall(_) => Err(
            "native scalar expr lowering does not lower WindowCall; analytic/window node handles it"
                .to_string(),
        ),
        expr::expr::Kind::Cast(cast) => lower_cast(cast, arena, input_layout, data_type),
        expr::expr::Kind::IsNull(is_null) => {
            lower_is_null(is_null, arena, input_layout, data_type)
        }
        expr::expr::Kind::InList(in_list) => {
            lower_in_list(in_list, arena, input_layout, data_type)
        }
        expr::expr::Kind::Between(between) => {
            lower_between(between, arena, input_layout, data_type)
        }
        expr::expr::Kind::Like(like) => lower_like(like, arena, input_layout, data_type),
        expr::expr::Kind::CaseExpr(case_expr) => {
            lower_case(case_expr, arena, input_layout, data_type)
        }
        expr::expr::Kind::IsTruth(is_truth) => {
            lower_is_truth(is_truth, arena, input_layout, data_type)
        }
        expr::expr::Kind::LambdaParamRef(param) => {
            let slot_id = SlotId::try_from(param.slot_id)?;
            Ok(arena.push_typed(ExprNode::SlotId(slot_id), data_type))
        }
        expr::expr::Kind::Lambda(_) => Err(
            "native scalar expr lowering does not lower Lambda; lambda parameter slot ids are not carried by LambdaExpr"
                .to_string(),
        ),
        expr::expr::Kind::Nested(nested) => lower_nested(nested, arena, input_layout, data_type),
    }
}

fn decode_expr_type(e: &expr::Expr) -> Result<DataType, String> {
    let desc = e
        .r#type
        .as_ref()
        .ok_or_else(|| "Expr.type missing".to_string())?;
    decode_type(desc).map_err(|err| format!("Expr.type decode failed: {err}"))
}

fn lower_required_child(
    child: &Option<Box<expr::Expr>>,
    field_name: &str,
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<ExprId, String> {
    let child = child
        .as_ref()
        .ok_or_else(|| format!("{field_name} missing"))?;
    lower_proto_expr(child, arena, input_layout)
}

fn lower_required_unboxed_child(
    child: &Option<expr::Expr>,
    field_name: &str,
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<ExprId, String> {
    let child = child
        .as_ref()
        .ok_or_else(|| format!("{field_name} missing"))?;
    lower_proto_expr(child, arena, input_layout)
}

fn lower_expr_list(
    values: &[expr::Expr],
    arena: &mut ExprArena,
    input_layout: &Layout,
) -> Result<Vec<ExprId>, String> {
    values
        .iter()
        .map(|value| lower_proto_expr(value, arena, input_layout))
        .collect()
}

fn lower_binary_op(
    binary: &expr::BinaryOpExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let op = expr::BinaryOp::try_from(binary.op)
        .map_err(|_| format!("unknown BinaryOp {}", binary.op))?;
    let left = lower_required_child(&binary.left, "BinaryOp.left", arena, input_layout)?;
    let right = lower_required_child(&binary.right, "BinaryOp.right", arena, input_layout)?;
    let node = match op {
        expr::BinaryOp::Unspecified => {
            return Err("BinaryOp.op is unspecified".to_string());
        }
        expr::BinaryOp::Add => ExprNode::Add(left, right),
        expr::BinaryOp::Sub => ExprNode::Sub(left, right),
        expr::BinaryOp::Mul => ExprNode::Mul(left, right),
        expr::BinaryOp::Div => ExprNode::Div(left, right),
        expr::BinaryOp::Mod => ExprNode::Mod(left, right),
        expr::BinaryOp::Eq => ExprNode::Eq(left, right),
        expr::BinaryOp::Ne => ExprNode::Ne(left, right),
        expr::BinaryOp::Lt => ExprNode::Lt(left, right),
        expr::BinaryOp::Le => ExprNode::Le(left, right),
        expr::BinaryOp::Gt => ExprNode::Gt(left, right),
        expr::BinaryOp::Ge => ExprNode::Ge(left, right),
        expr::BinaryOp::EqForNull => ExprNode::EqForNull(left, right),
        expr::BinaryOp::And => ExprNode::And(left, right),
        expr::BinaryOp::Or => ExprNode::Or(left, right),
    };
    Ok(arena.push_typed(node, data_type))
}

fn lower_unary_op(
    unary: &expr::UnaryOpExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let op =
        expr::UnaryOp::try_from(unary.op).map_err(|_| format!("unknown UnaryOp {}", unary.op))?;
    let operand = lower_required_child(&unary.operand, "UnaryOp.operand", arena, input_layout)?;
    match op {
        expr::UnaryOp::Unspecified => Err("UnaryOp.op is unspecified".to_string()),
        expr::UnaryOp::Not => Ok(arena.push_typed(ExprNode::Not(operand), data_type)),
        expr::UnaryOp::Negate => {
            let zero_type = arena
                .data_type(operand)
                .cloned()
                .unwrap_or_else(|| data_type.clone());
            let zero = push_zero_literal(arena, &zero_type)?;
            Ok(arena.push_typed(ExprNode::Sub(zero, operand), data_type))
        }
        expr::UnaryOp::BitwiseNot => {
            let kind = lookup_function("bitnot")
                .ok_or_else(|| "BITWISE_NOT requires bitnot function support".to_string())?;
            validate_function_arity("bitnot", kind, 1)?;
            Ok(arena.push_typed(
                ExprNode::FunctionCall {
                    kind,
                    args: vec![operand],
                },
                data_type,
            ))
        }
    }
}

fn lower_function_call(
    call: &expr::FunctionCall,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    if call.distinct {
        return Err(format!(
            "native scalar expr lowering does not support DISTINCT FunctionCall '{}'",
            call.function_name
        ));
    }
    let kind = lookup_function(&call.function_name).ok_or_else(|| {
        format!(
            "unsupported native scalar function '{}'",
            call.function_name
        )
    })?;
    let args = lower_expr_list(&call.args, arena, input_layout)?;
    validate_function_arity(&call.function_name, kind, args.len())?;
    Ok(arena.push_typed(ExprNode::FunctionCall { kind, args }, data_type))
}

fn lower_cast(
    cast: &expr::CastExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let operand = cast
        .operand
        .as_ref()
        .ok_or_else(|| "Cast.operand missing".to_string())?;
    let child = lower_proto_expr(operand, arena, input_layout)?;
    let target = cast
        .target
        .as_ref()
        .ok_or_else(|| "Cast.target missing".to_string())?;
    let target_type =
        decode_type(target).map_err(|err| format!("Cast.target decode failed: {err}"))?;
    if target_type != data_type {
        return Err(format!(
            "Cast target type {target_type:?} does not match Expr.type {data_type:?}"
        ));
    }

    if matches!(data_type, DataType::LargeBinary) {
        let child_type = arena
            .data_type(child)
            .ok_or_else(|| "CAST child missing data type".to_string())?;
        if !is_encoded_variant_payload_source(child_type) {
            return Err("CAST to VARIANT is not supported".to_string());
        }
    }
    if let Some(child_type) = arena.data_type(child)
        && matches!(child_type, DataType::LargeBinary)
        && !matches!(data_type, DataType::LargeBinary)
    {
        let supported = matches!(
            data_type,
            DataType::Boolean
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::Date32
                | DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        if !supported {
            return Err("CAST from VARIANT is not supported".to_string());
        }
    }

    let target_primitive = thrift_primitive_from_type_desc(target, "Cast.target")?;
    let source_primitive = operand
        .r#type
        .as_ref()
        .map(|desc| thrift_primitive_from_type_desc(desc, "Cast.operand"))
        .transpose()?
        .flatten();
    let node = if target_primitive == Some(types::TPrimitiveType::TIME) {
        if source_primitive == Some(types::TPrimitiveType::DATETIME) {
            ExprNode::CastTimeFromDatetime(child)
        } else {
            ExprNode::CastTime(child)
        }
    } else {
        ExprNode::Cast(child)
    };
    Ok(arena.push_typed(node, data_type))
}

fn lower_is_null(
    is_null: &expr::IsNullExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let child = lower_required_child(&is_null.operand, "IsNull.operand", arena, input_layout)?;
    let node = if is_null.negated {
        ExprNode::IsNotNull(child)
    } else {
        ExprNode::IsNull(child)
    };
    Ok(arena.push_typed(node, data_type))
}

fn lower_in_list(
    in_list: &expr::InListExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let child = lower_required_child(&in_list.operand, "InList.operand", arena, input_layout)?;
    let values = lower_expr_list(&in_list.list, arena, input_layout)?;
    Ok(arena.push_typed(
        ExprNode::In {
            child,
            values,
            is_not_in: in_list.negated,
        },
        data_type,
    ))
}

fn lower_between(
    between: &expr::BetweenExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let operand = lower_required_child(&between.operand, "Between.operand", arena, input_layout)?;
    let low = lower_required_child(&between.low, "Between.low", arena, input_layout)?;
    let high = lower_required_child(&between.high, "Between.high", arena, input_layout)?;
    if between.negated {
        let lt_low = arena.push_typed(ExprNode::Lt(operand, low), DataType::Boolean);
        let gt_high = arena.push_typed(ExprNode::Gt(operand, high), DataType::Boolean);
        Ok(arena.push_typed(ExprNode::Or(lt_low, gt_high), data_type))
    } else {
        let ge_low = arena.push_typed(ExprNode::Ge(operand, low), DataType::Boolean);
        let le_high = arena.push_typed(ExprNode::Le(operand, high), DataType::Boolean);
        let in_range = arena.push_typed(ExprNode::And(ge_low, le_high), DataType::Boolean);
        Ok(in_range)
    }
}

fn lower_like(
    like: &expr::LikeExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let operand = lower_required_child(&like.operand, "Like.operand", arena, input_layout)?;
    let pattern = lower_required_child(&like.pattern, "Like.pattern", arena, input_layout)?;
    let like_id = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Like,
            args: vec![operand, pattern],
        },
        DataType::Boolean,
    );
    if like.negated {
        Ok(arena.push_typed(ExprNode::Not(like_id), data_type))
    } else {
        Ok(like_id)
    }
}

fn lower_case(
    case_expr: &expr::CaseExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let mut children = Vec::with_capacity(
        usize::from(case_expr.operand.is_some())
            + case_expr.when_then.len() * 2
            + usize::from(case_expr.else_expr.is_some()),
    );
    if let Some(operand) = &case_expr.operand {
        children.push(lower_proto_expr(operand, arena, input_layout)?);
    }
    for (idx, branch) in case_expr.when_then.iter().enumerate() {
        children.push(lower_required_unboxed_child(
            &branch.when,
            &format!("Case.when_then[{idx}].when"),
            arena,
            input_layout,
        )?);
        children.push(lower_required_unboxed_child(
            &branch.then,
            &format!("Case.when_then[{idx}].then"),
            arena,
            input_layout,
        )?);
    }
    if let Some(else_expr) = &case_expr.else_expr {
        children.push(lower_proto_expr(else_expr, arena, input_layout)?);
    }
    Ok(arena.push_typed(
        ExprNode::Case {
            has_case_expr: case_expr.operand.is_some(),
            has_else_expr: case_expr.else_expr.is_some(),
            children,
        },
        data_type,
    ))
}

fn lower_is_truth(
    is_truth: &expr::IsTruthExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    if !matches!(data_type, DataType::Boolean) {
        return Err(format!("IsTruth must return Boolean, got {data_type:?}"));
    }
    let child = lower_required_child(&is_truth.operand, "IsTruth.operand", arena, input_layout)?;
    if is_truth.value && !is_truth.negated {
        Ok(child)
    } else {
        Ok(arena.push_typed(ExprNode::Not(child), DataType::Boolean))
    }
}

fn lower_nested(
    nested: &expr::NestedExpr,
    arena: &mut ExprArena,
    input_layout: &Layout,
    data_type: DataType,
) -> Result<ExprId, String> {
    let inner = nested
        .inner
        .as_ref()
        .ok_or_else(|| "NestedExpr.inner missing".to_string())?;
    let inner_type = decode_expr_type(inner)?;
    if inner_type != data_type {
        return Err(format!(
            "NestedExpr type {data_type:?} does not match inner type {inner_type:?}"
        ));
    }
    lower_proto_expr(inner, arena, input_layout)
}

fn is_encoded_variant_payload_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::Null
    )
}

fn thrift_primitive_from_type_desc(
    desc: &common::TypeDesc,
    context: &str,
) -> Result<Option<types::TPrimitiveType>, String> {
    let Some(common::type_desc::Kind::Scalar(scalar)) = desc.kind.as_ref() else {
        return Ok(None);
    };
    let primitive = common::PrimitiveType::try_from(scalar.r#type)
        .map_err(|_| format!("{context} has unknown primitive type {}", scalar.r#type))?;
    let thrift = match primitive {
        common::PrimitiveType::Unspecified => {
            return Err(format!("{context} primitive type is unspecified"));
        }
        common::PrimitiveType::NullType => types::TPrimitiveType::NULL_TYPE,
        common::PrimitiveType::Boolean => types::TPrimitiveType::BOOLEAN,
        common::PrimitiveType::Tinyint => types::TPrimitiveType::TINYINT,
        common::PrimitiveType::Smallint => types::TPrimitiveType::SMALLINT,
        common::PrimitiveType::Int => types::TPrimitiveType::INT,
        common::PrimitiveType::Bigint => types::TPrimitiveType::BIGINT,
        common::PrimitiveType::Largeint => types::TPrimitiveType::LARGEINT,
        common::PrimitiveType::Float => types::TPrimitiveType::FLOAT,
        common::PrimitiveType::Double => types::TPrimitiveType::DOUBLE,
        common::PrimitiveType::Decimal32 => types::TPrimitiveType::DECIMAL32,
        common::PrimitiveType::Decimal64 => types::TPrimitiveType::DECIMAL64,
        common::PrimitiveType::Decimal128 => types::TPrimitiveType::DECIMAL128,
        common::PrimitiveType::Decimal256 => types::TPrimitiveType::DECIMAL256,
        common::PrimitiveType::Date => types::TPrimitiveType::DATE,
        common::PrimitiveType::Datetime => types::TPrimitiveType::DATETIME,
        common::PrimitiveType::Time => types::TPrimitiveType::TIME,
        common::PrimitiveType::Varchar => types::TPrimitiveType::VARCHAR,
        common::PrimitiveType::Char => types::TPrimitiveType::CHAR,
        common::PrimitiveType::Varbinary => types::TPrimitiveType::VARBINARY,
        common::PrimitiveType::Binary => types::TPrimitiveType::BINARY,
        common::PrimitiveType::Json => types::TPrimitiveType::JSON,
        common::PrimitiveType::Hll => types::TPrimitiveType::HLL,
        common::PrimitiveType::Bitmap | common::PrimitiveType::Object => {
            types::TPrimitiveType::OBJECT
        }
        common::PrimitiveType::Percentile => types::TPrimitiveType::PERCENTILE,
        common::PrimitiveType::Variant => types::TPrimitiveType::VARIANT,
    };
    Ok(Some(thrift))
}

fn validate_function_arity(name: &str, kind: FunctionKind, arg_count: usize) -> Result<(), String> {
    let metadata = function_metadata(kind);
    if arg_count < metadata.min_args || arg_count > metadata.max_args {
        return Err(format!(
            "function '{}' expects {} to {} arguments, got {}",
            name, metadata.min_args, metadata.max_args, arg_count
        ));
    }
    Ok(())
}

fn lower_literal(
    literal: &expr::LiteralExpr,
    data_type: &DataType,
) -> Result<LiteralValue, String> {
    let value = literal
        .value
        .as_ref()
        .ok_or_else(|| "LiteralExpr.value missing".to_string())?;
    let value = value
        .value
        .as_ref()
        .ok_or_else(|| "LiteralValue.value missing".to_string())?;
    use common::literal_value::Value;
    match value {
        Value::NullValue(true) => Ok(LiteralValue::Null),
        Value::NullValue(false) => Err("LiteralValue.null_value must be true".to_string()),
        Value::BoolValue(value) => {
            require_type(
                data_type,
                matches!(data_type, DataType::Boolean),
                "bool literal",
            )?;
            Ok(LiteralValue::Bool(*value))
        }
        Value::IntValue(value) => lower_int_literal(*value, data_type),
        Value::LargeintValue(bytes) => {
            require_type(
                data_type,
                crate::common::largeint::is_largeint_data_type(data_type),
                "largeint literal",
            )?;
            Ok(LiteralValue::LargeInt(
                crate::common::largeint::i128_from_be_bytes(bytes)?,
            ))
        }
        Value::FloatValue(value) => match data_type {
            DataType::Float32 => Ok(LiteralValue::Float32(*value as f32)),
            DataType::Float64 => Ok(LiteralValue::Float64(*value)),
            _ => Err(format!("float literal cannot be lowered as {data_type:?}")),
        },
        Value::StringValue(value) => {
            require_type(
                data_type,
                matches!(data_type, DataType::Utf8 | DataType::LargeUtf8),
                "string literal",
            )?;
            Ok(LiteralValue::Utf8(value.clone()))
        }
        Value::BinaryValue(value) => {
            require_type(
                data_type,
                matches!(data_type, DataType::Binary | DataType::LargeBinary),
                "binary literal",
            )?;
            Ok(LiteralValue::Binary(value.clone()))
        }
        Value::Date32Value(value) => {
            require_type(
                data_type,
                matches!(data_type, DataType::Date32),
                "date32 literal",
            )?;
            Ok(LiteralValue::Date32(*value))
        }
        Value::DecimalValue(decimal) => lower_decimal_literal(decimal, data_type),
    }
}

fn require_type(data_type: &DataType, ok: bool, context: &str) -> Result<(), String> {
    if ok {
        Ok(())
    } else {
        Err(format!("{context} cannot be lowered as {data_type:?}"))
    }
}

fn lower_int_literal(value: i64, data_type: &DataType) -> Result<LiteralValue, String> {
    match data_type {
        DataType::Int8 => i8::try_from(value)
            .map(LiteralValue::Int8)
            .map_err(|_| format!("int literal {value} is outside Int8 range")),
        DataType::Int16 => i16::try_from(value)
            .map(LiteralValue::Int16)
            .map_err(|_| format!("int literal {value} is outside Int16 range")),
        DataType::Int32 => i32::try_from(value)
            .map(LiteralValue::Int32)
            .map_err(|_| format!("int literal {value} is outside Int32 range")),
        DataType::Int64 => Ok(LiteralValue::Int64(value)),
        DataType::Date32 => i32::try_from(value)
            .map(LiteralValue::Date32)
            .map_err(|_| format!("date32 int literal {value} is outside i32 range")),
        _ => Err(format!("int literal cannot be lowered as {data_type:?}")),
    }
}

fn lower_decimal_literal(
    decimal: &common::DecimalLiteral,
    data_type: &DataType,
) -> Result<LiteralValue, String> {
    let precision = u8::try_from(decimal.precision)
        .map_err(|_| format!("invalid decimal precision {}", decimal.precision))?;
    let scale = i8::try_from(decimal.scale)
        .map_err(|_| format!("invalid decimal scale {}", decimal.scale))?;
    validate_decimal_parts(precision, scale)?;

    match data_type {
        DataType::Decimal128(expected_precision, expected_scale) => {
            validate_decimal_type_match(precision, scale, *expected_precision, *expected_scale)?;
            let bytes = decimal_bytes::<16>(&decimal.value, "Decimal128")?;
            Ok(LiteralValue::Decimal128 {
                value: i128::from_be_bytes(bytes),
                precision,
                scale,
            })
        }
        DataType::Decimal256(expected_precision, expected_scale) => {
            validate_decimal_type_match(precision, scale, *expected_precision, *expected_scale)?;
            let bytes = decimal_bytes::<32>(&decimal.value, "Decimal256")?;
            Ok(LiteralValue::Decimal256 {
                value: i256::from_be_bytes(bytes),
                precision,
                scale,
            })
        }
        _ => Err(format!(
            "decimal literal requires Decimal128/Decimal256 type, got {data_type:?}"
        )),
    }
}

fn validate_decimal_parts(precision: u8, scale: i8) -> Result<(), String> {
    if precision == 0 || precision > 76 {
        return Err(format!(
            "decimal precision {precision} must be between 1 and 76"
        ));
    }
    if scale < 0 || scale > precision as i8 {
        return Err(format!(
            "decimal scale {scale} must be between 0 and precision {precision}"
        ));
    }
    Ok(())
}

fn validate_decimal_type_match(
    precision: u8,
    scale: i8,
    expected_precision: u8,
    expected_scale: i8,
) -> Result<(), String> {
    if precision == expected_precision && scale == expected_scale {
        Ok(())
    } else {
        Err(format!(
            "decimal literal precision/scale ({precision},{scale}) does not match Expr.type ({expected_precision},{expected_scale})"
        ))
    }
}

fn decimal_bytes<const N: usize>(value: &[u8], label: &str) -> Result<[u8; N], String> {
    value
        .try_into()
        .map_err(|_| format!("{label} literal requires {N} bytes, got {}", value.len()))
}

fn push_zero_literal(arena: &mut ExprArena, data_type: &DataType) -> Result<ExprId, String> {
    let literal = match data_type {
        DataType::Int8 => LiteralValue::Int8(0),
        DataType::Int16 => LiteralValue::Int16(0),
        DataType::Int32 => LiteralValue::Int32(0),
        DataType::Int64 => LiteralValue::Int64(0),
        DataType::Float32 => LiteralValue::Float32(0.0),
        DataType::Float64 => LiteralValue::Float64(0.0),
        DataType::Decimal128(precision, scale) => LiteralValue::Decimal128 {
            value: 0,
            precision: *precision,
            scale: *scale,
        },
        DataType::Decimal256(precision, scale) => LiteralValue::Decimal256 {
            value: i256::ZERO,
            precision: *precision,
            scale: *scale,
        },
        dt if crate::common::largeint::is_largeint_data_type(dt) => LiteralValue::LargeInt(0),
        _ => {
            return Err(format!(
                "NEGATE is not supported for data type {data_type:?}"
            ));
        }
    };
    Ok(arena.push_typed(ExprNode::Literal(literal), data_type.clone()))
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow_buffer::i256;
    use std::sync::Arc;

    use super::*;
    use crate::common::ids::SlotId;
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::{ExprArena, ExprNode, LiteralValue, function::FunctionKind};
    use crate::proto::{common, expr};
    use crate::sql::codegen::proto_encode::types::encode_type;

    fn type_desc(data_type: &DataType) -> common::TypeDesc {
        encode_type(data_type).expect("encode type")
    }

    fn scalar_expr(data_type: DataType, kind: expr::expr::Kind) -> expr::Expr {
        expr::Expr {
            r#type: Some(type_desc(&data_type)),
            nullable: true,
            kind: Some(kind),
        }
    }

    fn int_lit(value: i64) -> expr::Expr {
        scalar_expr(
            DataType::Int64,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            }),
        )
    }

    fn string_lit(value: &str) -> expr::Expr {
        scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            }),
        )
    }

    fn bool_lit(value: bool) -> expr::Expr {
        scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::BoolValue(value)),
                }),
            }),
        )
    }

    fn col(column_id: u32, data_type: DataType) -> expr::Expr {
        scalar_expr(
            data_type,
            expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            }),
        )
    }

    fn layout_for_slots(slots: &[u32]) -> super::super::layout::Layout {
        super::super::layout::Layout::for_slots(slots.iter().copied().map(SlotId::new))
    }

    fn lower_with_slots(e: &expr::Expr, slots: &[u32]) -> (ExprArena, crate::exec::expr::ExprId) {
        let mut arena = ExprArena::default();
        let layout = layout_for_slots(slots);
        let id = lower_proto_expr(e, &mut arena, &layout).expect("lower proto expr");
        (arena, id)
    }

    fn lower(e: &expr::Expr) -> (ExprArena, crate::exec::expr::ExprId) {
        lower_with_slots(e, &[1, 7, 42])
    }

    fn lower_err_with_slots(e: &expr::Expr, slots: &[u32]) -> String {
        let mut arena = ExprArena::default();
        let layout = layout_for_slots(slots);
        lower_proto_expr(e, &mut arena, &layout).unwrap_err()
    }

    fn make_i64_chunk(slot: SlotId, values: Vec<Option<i64>>) -> Chunk {
        let field = Field::new("c0", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[slot],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    #[test]
    fn lowers_column_ref_to_slot_id_with_decoded_type() {
        let (arena, id) = lower(&col(42, DataType::Int32));

        assert!(matches!(
            arena.node(id),
            Some(ExprNode::SlotId(slot)) if *slot == SlotId::new(42)
        ));
        assert_eq!(arena.data_type(id), Some(&DataType::Int32));
    }

    #[test]
    fn column_ref_missing_from_layout_fails() {
        let err = lower_err_with_slots(&col(42, DataType::Int32), &[7]);

        assert!(err.contains("ColumnRef column_id=42 not found in input layout"));
    }

    #[test]
    fn lowers_typed_literals() {
        let cases = vec![
            scalar_expr(
                DataType::Int32,
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::NullValue(true)),
                    }),
                }),
            ),
            bool_lit(true),
            int_lit(123),
            scalar_expr(
                DataType::Float64,
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::FloatValue(1.25)),
                    }),
                }),
            ),
            string_lit("abc"),
            scalar_expr(
                DataType::Binary,
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::BinaryValue(vec![1, 2, 3])),
                    }),
                }),
            ),
            scalar_expr(
                DataType::Date32,
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::Date32Value(20_000)),
                    }),
                }),
            ),
            scalar_expr(
                DataType::FixedSizeBinary(16),
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::LargeintValue(
                            (-12_345i128).to_be_bytes().to_vec(),
                        )),
                    }),
                }),
            ),
            scalar_expr(
                DataType::Decimal128(10, 2),
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::DecimalValue(
                            common::DecimalLiteral {
                                value: 12345i128.to_be_bytes().to_vec(),
                                precision: 10,
                                scale: 2,
                            },
                        )),
                    }),
                }),
            ),
            scalar_expr(
                DataType::Decimal256(40, 3),
                expr::expr::Kind::Literal(expr::LiteralExpr {
                    value: Some(common::LiteralValue {
                        value: Some(common::literal_value::Value::DecimalValue(
                            common::DecimalLiteral {
                                value: i256::from_i128(123_456).to_be_bytes().to_vec(),
                                precision: 40,
                                scale: 3,
                            },
                        )),
                    }),
                }),
            ),
        ];

        for expr in cases {
            let (arena, id) = lower(&expr);
            assert!(matches!(arena.node(id), Some(ExprNode::Literal(_))));
        }

        let (arena, id) = lower(&scalar_expr(
            DataType::Decimal128(10, 2),
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::DecimalValue(
                        common::DecimalLiteral {
                            value: 12345i128.to_be_bytes().to_vec(),
                            precision: 10,
                            scale: 2,
                        },
                    )),
                }),
            }),
        ));
        assert!(matches!(
            arena.node(id),
            Some(ExprNode::Literal(LiteralValue::Decimal128 {
                value: 12345,
                precision: 10,
                scale: 2
            }))
        ));
    }

    #[test]
    fn cast_rejects_target_type_mismatch() {
        let expr = scalar_expr(
            DataType::Float64,
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(col(1, DataType::Int64))),
                target: Some(type_desc(&DataType::Utf8)),
            })),
        );

        let err = lower_err_with_slots(&expr, &[1]);
        assert!(err.contains("Cast target type Utf8 does not match Expr.type Float64"));
    }

    #[test]
    fn cast_selects_time_special_case_nodes() {
        let time_type = DataType::Time64(TimeUnit::Microsecond);
        let datetime_type = DataType::Timestamp(TimeUnit::Microsecond, None);
        let datetime_to_time = scalar_expr(
            time_type.clone(),
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(col(1, datetime_type))),
                target: Some(type_desc(&time_type)),
            })),
        );
        let int_to_time = scalar_expr(
            time_type.clone(),
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(col(7, DataType::Int64))),
                target: Some(type_desc(&time_type)),
            })),
        );

        let (arena, id) = lower_with_slots(&datetime_to_time, &[1, 7]);
        assert!(matches!(
            arena.node(id),
            Some(ExprNode::CastTimeFromDatetime(_))
        ));

        let (arena, id) = lower_with_slots(&int_to_time, &[1, 7]);
        assert!(matches!(arena.node(id), Some(ExprNode::CastTime(_))));
    }

    #[test]
    fn cast_preserves_variant_guards() {
        let scalar_to_variant = scalar_expr(
            DataType::LargeBinary,
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(col(1, DataType::Int64))),
                target: Some(type_desc(&DataType::LargeBinary)),
            })),
        );
        let variant_to_decimal = scalar_expr(
            DataType::Decimal128(10, 2),
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(col(1, DataType::LargeBinary))),
                target: Some(type_desc(&DataType::Decimal128(10, 2))),
            })),
        );

        let err = lower_err_with_slots(&scalar_to_variant, &[1]);
        assert!(err.contains("CAST to VARIANT is not supported"));
        let err = lower_err_with_slots(&variant_to_decimal, &[1]);
        assert!(err.contains("CAST from VARIANT is not supported"));
    }

    #[test]
    fn lowers_recursive_binary_cast_and_function_call() {
        let add = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                op: expr::BinaryOp::Add as i32,
                left: Some(Box::new(col(7, DataType::Int64))),
                right: Some(Box::new(int_lit(5))),
            })),
        );
        let cast = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                operand: Some(Box::new(add)),
                target: Some(type_desc(&DataType::Utf8)),
            })),
        );
        let call = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                function_name: "upper".to_string(),
                args: vec![cast],
                distinct: false,
            }),
        );

        let (arena, id) = lower(&call);
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(id) else {
            panic!("expected function call");
        };
        assert_eq!(*kind, FunctionKind::Upper);
        assert_eq!(args.len(), 1);
        let Some(ExprNode::Cast(add_id)) = arena.node(args[0]) else {
            panic!("expected cast arg");
        };
        assert!(matches!(
            arena.node(*add_id),
            Some(ExprNode::Add(left, right))
                if matches!(arena.node(*left), Some(ExprNode::SlotId(SlotId(7))))
                    && matches!(arena.node(*right), Some(ExprNode::Literal(LiteralValue::Int64(5))))
        ));
    }

    #[test]
    fn nested_requires_outer_and_inner_type_match() {
        let nested = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::Nested(Box::new(expr::NestedExpr {
                inner: Some(Box::new(string_lit("x"))),
            })),
        );

        let err = lower_err_with_slots(&nested, &[]);
        assert!(err.contains("NestedExpr type Int64 does not match inner type Utf8"));
    }

    #[test]
    fn not_between_lowers_to_or_of_lt_and_gt() {
        let between = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::Between(Box::new(expr::BetweenExpr {
                operand: Some(Box::new(col(1, DataType::Int64))),
                low: Some(Box::new(int_lit(10))),
                high: Some(Box::new(int_lit(20))),
                negated: true,
            })),
        );

        let (arena, id) = lower_with_slots(&between, &[1]);
        let Some(ExprNode::Or(left, right)) = arena.node(id) else {
            panic!("expected NOT BETWEEN to lower as OR");
        };
        assert!(matches!(arena.node(*left), Some(ExprNode::Lt(_, _))));
        assert!(matches!(arena.node(*right), Some(ExprNode::Gt(_, _))));
    }

    #[test]
    fn numeric_is_false_uses_not_path() {
        let is_false = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::IsTruth(Box::new(expr::IsTruthExpr {
                operand: Some(Box::new(col(1, DataType::Int64))),
                value: false,
                negated: false,
            })),
        );

        let (arena, id) = lower_with_slots(&is_false, &[1]);
        let Some(ExprNode::Not(child)) = arena.node(id) else {
            panic!("expected numeric IS FALSE to lower through NOT");
        };
        assert!(matches!(
            arena.node(*child),
            Some(ExprNode::SlotId(SlotId(1)))
        ));

        let chunk = make_i64_chunk(SlotId::new(1), vec![Some(0), Some(1)]);
        let out = arena.eval(id, &chunk).expect("eval");
        let out = out.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(out.value(0));
        assert!(!out.value(1));
    }

    #[test]
    fn lowers_case_in_like_and_nested() {
        let in_pred = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::InList(Box::new(expr::InListExpr {
                operand: Some(Box::new(col(1, DataType::Utf8))),
                list: vec![string_lit("a"), string_lit("b")],
                negated: false,
            })),
        );
        let like = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::Like(Box::new(expr::LikeExpr {
                operand: Some(Box::new(col(1, DataType::Utf8))),
                pattern: Some(Box::new(string_lit("x%"))),
                negated: false,
            })),
        );
        let case_expr = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Nested(Box::new(expr::NestedExpr {
                inner: Some(Box::new(scalar_expr(
                    DataType::Utf8,
                    expr::expr::Kind::CaseExpr(Box::new(expr::CaseExpr {
                        operand: None,
                        when_then: vec![
                            expr::WhenThen {
                                when: Some(in_pred),
                                then: Some(string_lit("in")),
                            },
                            expr::WhenThen {
                                when: Some(like),
                                then: Some(string_lit("like")),
                            },
                        ],
                        else_expr: Some(Box::new(string_lit("miss"))),
                    })),
                ))),
            })),
        );

        let (arena, id) = lower(&case_expr);
        let Some(ExprNode::Case {
            has_case_expr,
            has_else_expr,
            children,
        }) = arena.node(id)
        else {
            panic!("expected CASE after nested lowering");
        };
        assert!(!has_case_expr);
        assert!(has_else_expr);
        assert_eq!(children.len(), 5);
        assert!(matches!(arena.node(children[0]), Some(ExprNode::In { .. })));
        assert!(matches!(
            arena.node(children[2]),
            Some(ExprNode::FunctionCall {
                kind: FunctionKind::Like,
                ..
            })
        ));
    }

    #[test]
    fn fails_fast_for_aggregate_and_window_calls() {
        let aggregate = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::AggregateCall(expr::AggregateCall {
                function_name: "count".to_string(),
                args: vec![int_lit(1)],
                distinct: false,
                order_by: vec![],
            }),
        );
        let window = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::WindowCall(expr::WindowCall {
                function_name: "rank".to_string(),
                args: vec![],
                distinct: false,
                partition_by: vec![],
                order_by: vec![],
                frame: None,
                ignore_nulls: false,
            }),
        );

        for (expr, needle) in [(aggregate, "AggregateCall"), (window, "WindowCall")] {
            let mut arena = ExprArena::default();
            let layout = layout_for_slots(&[]);
            let err = lower_proto_expr(&expr, &mut arena, &layout).unwrap_err();
            assert!(err.contains(needle), "{err}");
        }
    }
}
