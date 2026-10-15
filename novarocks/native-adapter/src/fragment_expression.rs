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

//! Fragment-native proto expression lowering.

use arrow::datatypes::DataType;

use self::error::NativeExpressionDecodeError;
use crate::fragment_type_decode::NativeTypeDecoder;
use novarocks_execution::exec::chunk::SlotLayout;
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::expr;
use novarocks_types::SlotId;

use crate::fragment_error::NativeFragmentDecodeError;

mod binary;
mod case;
mod cast;
mod collection;
mod error;
mod function_call;
mod lambda;
mod literal;
mod min_max;
mod nested;
mod predicate;
mod unary;

/// Immutable input-slot layout used solely by Native expression decoding.
/// It carries no runtime or connector state.
#[derive(Clone, Debug, Default)]
pub struct NativeExpressionInputLayout {
    slots: Vec<SlotId>,
}

impl NativeExpressionInputLayout {
    pub fn from_slot_ids(slots: impl IntoIterator<Item = SlotId>) -> Self {
        let mut layout = Self::default();
        for slot in slots {
            if !layout.slots.contains(&slot) {
                layout.slots.push(slot);
            }
        }
        layout
    }

    pub(crate) fn resolve_column_id(
        &self,
        column_id: u32,
        path: FieldPath,
    ) -> Result<SlotId, novarocks_proto_codec::ProtocolError> {
        let slot = SlotId::new(column_id);
        if self.slots.contains(&slot) {
            Ok(slot)
        } else {
            Err(novarocks_proto_codec::ProtocolError::new(
                path.field("column_id"),
                novarocks_proto_codec::ProtocolErrorKind::InvalidValue,
                format!("ColumnRef column_id={column_id} not found in input layout"),
            ))
        }
    }
}

/// Test convenience over the same typed owner used by production.
#[cfg(test)]
pub(crate) fn decode_expr(
    e: &expr::Expr,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
) -> Result<ExprId, NativeExpressionDecodeError> {
    decode_expr_at(e, FieldPath::root("expr"), arena, input_layout)
}

#[cfg(test)]
pub fn decode_expr_at(
    e: &expr::Expr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,
) -> Result<ExprId, NativeExpressionDecodeError> {
    crate::fragment_expression::fixture_sources::with_decoder(|decoder| {
        decode_expr_at_with_types(e, path, arena, input_layout, decoder)
    })
}

#[cfg(test)]
pub fn validate_proto_expr_shape_at(
    e: &expr::Expr,
    path: FieldPath,
) -> Result<(), NativeExpressionDecodeError> {
    crate::fragment_expression::fixture_sources::with_decoder(|decoder| {
        validate_proto_expr_shape_at_with_types(e, path, decoder)
    })
}

#[cfg(test)]
pub fn decode_expr_for_slot_layout(
    e: &expr::Expr,
    path: FieldPath,
    arena: &mut ExprArena,
    layout: &SlotLayout,
) -> Result<ExprId, NativeFragmentDecodeError> {
    crate::fragment_expression::fixture_sources::with_decoder(|decoder| {
        decode_expr_for_slot_layout_with_types(e, path, arena, layout, decoder)
    })
}

fn decode_type_with_decoder(
    desc: &novarocks_proto_models::common::TypeDesc,
    path: FieldPath,
    type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
) -> Result<DataType, NativeExpressionDecodeError> {
    type_decoder.data_type(desc, path).map_err(Into::into)
}

pub(crate) fn decode_expr_at_with_types(
    e: &expr::Expr,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<ExprId, NativeExpressionDecodeError> {
    validate_proto_expr_shape_at_with_types(e, path.clone(), type_decoder)?;
    let data_type = decode_expr_type_at(e, path.clone(), type_decoder)?;
    let declared = type_decoder.value_type(
        e.r#type.as_ref().expect("validated expression type"),
        path.clone().field("type"),
    )?;
    let kind = e.kind.as_ref().expect("validated native expression kind");

    let id = match kind {
        expr::expr::Kind::ColumnRef(column) => {
            let slot_id = input_layout
                .resolve_column_id(column.column_id, path.clone().field("column_ref"))?;
            Ok(arena.push_typed(ExprNode::SlotId(slot_id), data_type))
        }
        expr::expr::Kind::ConstantReference(reference) => {
            let value = type_decoder.constant_value(
                reference,
                e,
                path.clone().field("constant_reference"),
            )?;
            Ok(arena.push_typed(ExprNode::Constant(value), data_type))
        }
        expr::expr::Kind::Literal(literal) => {
            let literal_path = path.clone().field("literal");
            type_decoder.reject_strong_inline_literal(literal_path.clone())?;
            let value = literal::lower_literal_at(literal, &data_type)
                .map_err(|error| error.into_protocol(literal_path))?;
            Ok(arena.push_typed(ExprNode::Literal(value), data_type))
        }
        expr::expr::Kind::BinaryOp(binary) => binary::lower_binary_op(
            binary,
            path.clone().field("binary_op"),
            arena,
            input_layout,
            data_type,
            type_decoder,
        ),
        expr::expr::Kind::UnaryOp(unary) => unary::lower_unary_op(
            unary,
            path.clone().field("unary_op"),
            arena,
            input_layout,
            data_type,
            type_decoder,
        ),
        expr::expr::Kind::FunctionCall(call) => function_call::lower_function_call(
            e,
            call,
            path.clone().field("function_call"),
            arena,
            input_layout,
            data_type,
            declared.as_ref(),
            type_decoder,
        ),
        expr::expr::Kind::AggregateCall(_) => Err(NativeExpressionDecodeError::unsupported(
            path.clone().field("aggregate_call"),
            "native scalar expression lowering does not lower AggregateCall",
        )),
        expr::expr::Kind::WindowCall(_) => Err(NativeExpressionDecodeError::unsupported(
            path.clone().field("window_call"),
            "native scalar expression lowering does not lower WindowCall",
        )),
        expr::expr::Kind::Cast(cast) => cast::lower_cast_with_binding(
            cast,
            path.clone().field("cast"),
            arena,
            input_layout,
            data_type,
            declared.as_ref(),
            type_decoder,
        ),
        expr::expr::Kind::IsNull(is_null) => predicate::lower_is_null(
            is_null,
            path.clone().field("is_null"),
            arena,
            input_layout,
            data_type,
            type_decoder,
        ),
        expr::expr::Kind::InList(in_list) => predicate::lower_in_list(
            in_list,
            path.clone().field("in_list"),
            arena,
            input_layout,
            data_type,
            type_decoder,
        ),
        expr::expr::Kind::Between(between) => predicate::lower_between(
            between,
            path.clone().field("between"),
            arena,
            input_layout,
            data_type,
            declared.as_ref(),
            type_decoder,
        ),
        expr::expr::Kind::Like(like) => predicate::lower_like(
            like,
            path.clone().field("like"),
            arena,
            input_layout,
            data_type,
            declared.as_ref(),
            type_decoder,
        ),
        expr::expr::Kind::CaseExpr(case_expr) => case::lower_case(
            case_expr,
            path.clone().field("case_expr"),
            arena,
            input_layout,
            data_type,
            type_decoder,
        ),
        expr::expr::Kind::IsTruth(is_truth) => predicate::lower_is_truth(
            is_truth,
            path.clone().field("is_truth"),
            arena,
            input_layout,
            data_type,
            declared.as_ref(),
            type_decoder,
        ),
        expr::expr::Kind::LambdaParamRef(param) => {
            let slot_id = SlotId::try_from(param.slot_id).map_err(|error| {
                NativeExpressionDecodeError::out_of_range(
                    path.clone().field("lambda_param_ref").field("slot_id"),
                    error,
                )
            })?;
            Ok(arena.push_typed(ExprNode::SlotId(slot_id), data_type))
        }
        expr::expr::Kind::Lambda(lambda) => lambda::lower_lambda(
            lambda,
            path.clone().field("lambda"),
            arena,
            input_layout,
            data_type,
            type_decoder,
        ),
        expr::expr::Kind::Nested(nested) => nested::lower_nested(
            nested,
            path.clone().field("nested"),
            arena,
            input_layout,
            data_type,
            type_decoder,
        ),
    }?;

    type_decoder.save_expression_binding(e, path, id, arena)?;
    Ok(id)
}

pub(crate) fn decode_expr_for_slot_layout_with_types(
    e: &expr::Expr,
    path: FieldPath,
    arena: &mut ExprArena,
    layout: &SlotLayout,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<ExprId, NativeFragmentDecodeError> {
    let input = NativeExpressionInputLayout::from_slot_ids(layout.order().iter().copied());
    decode_expr_at_with_types(e, path, arena, &input, type_decoder)
        .map_err(|error| error.into_native())
}

fn decode_expr_type_at(
    e: &expr::Expr,
    path: FieldPath,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<DataType, NativeExpressionDecodeError> {
    let desc = e.r#type.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("type"),
            "native Expr requires type",
        )
    })?;
    decode_type_with_decoder(desc, path.field("type"), type_decoder)
}

pub(crate) fn decode_decimal_overflow_policy(
    value: i32,
    path: FieldPath,
) -> Result<novarocks_type_contract::DecimalOverflowPolicy, NativeExpressionDecodeError> {
    use novarocks_type_contract::DecimalOverflowPolicy as Policy;
    match expr::DecimalOverflowPolicy::try_from(value) {
        Ok(expr::DecimalOverflowPolicy::OutputNull) => Ok(Policy::OutputNull),
        Ok(expr::DecimalOverflowPolicy::ReportError) => Ok(Policy::ReportError),
        Ok(expr::DecimalOverflowPolicy::Unspecified) => Err(
            NativeExpressionDecodeError::invalid_enum(path, "DecimalOverflowPolicy is unspecified"),
        ),
        Err(_) => Err(NativeExpressionDecodeError::invalid_enum(
            path,
            format!("unknown DecimalOverflowPolicy {value}"),
        )),
    }
}

pub(crate) fn require_binary_decimal_policy(
    op: expr::BinaryOp,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
    path: FieldPath,
) -> Result<(), NativeExpressionDecodeError> {
    if !matches!(
        op,
        expr::BinaryOp::Add
            | expr::BinaryOp::Sub
            | expr::BinaryOp::Mul
            | expr::BinaryOp::Div
            | expr::BinaryOp::Mod
    ) && policy != novarocks_type_contract::DecimalOverflowPolicy::OutputNull
    {
        return Err(NativeExpressionDecodeError::invalid_value(
            path,
            "non-arithmetic binary expression requires OutputNull decimal policy",
        ));
    }
    Ok(())
}

pub(crate) fn validate_proto_expr_shape_at_with_types(
    e: &expr::Expr,
    path: FieldPath,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<(), NativeExpressionDecodeError> {
    let type_desc = e.r#type.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("type"),
            "native Expr requires type",
        )
    })?;
    decode_type_with_decoder(type_desc, path.clone().field("type"), type_decoder)?;
    let kind = e.kind.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("kind"),
            "native Expr requires kind",
        )
    })?;

    fn required_boxed(
        owner_path: FieldPath,
        child: &Option<Box<expr::Expr>>,
        field: &'static str,
        type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
    ) -> Result<(), NativeExpressionDecodeError> {
        let child_path = owner_path.field(field);
        let child = child.as_deref().ok_or_else(|| {
            NativeExpressionDecodeError::missing(
                child_path.clone(),
                format!("native Expr requires {field}"),
            )
        })?;
        validate_proto_expr_shape_at_with_types(child, child_path, type_decoder)
    }
    fn list(
        owner_path: FieldPath,
        values: &[expr::Expr],
        field: &'static str,
        type_decoder: &mut NativeTypeDecoder<'_, '_, '_>,
    ) -> Result<(), NativeExpressionDecodeError> {
        for (index, value) in values.iter().enumerate() {
            validate_proto_expr_shape_at_with_types(
                value,
                owner_path.clone().field(field).index(index),
                type_decoder,
            )?;
        }
        Ok(())
    }

    match kind {
        expr::expr::Kind::BinaryOp(binary) => {
            let path = path.clone().field("binary_op");
            let policy = decode_decimal_overflow_policy(
                binary.decimal_overflow_policy,
                path.clone().field("decimal_overflow_policy"),
            )?;
            let op = expr::BinaryOp::try_from(binary.op).map_err(|_| {
                NativeExpressionDecodeError::invalid_enum(
                    path.clone().field("op"),
                    format!("unknown BinaryOp {}", binary.op),
                )
            })?;
            if op == expr::BinaryOp::Unspecified {
                return Err(NativeExpressionDecodeError::invalid_enum(
                    path.clone().field("op"),
                    "BinaryOp.op is unspecified",
                ));
            }
            require_binary_decimal_policy(
                op,
                policy,
                path.clone().field("decimal_overflow_policy"),
            )?;
            required_boxed(path.clone(), &binary.left, "left", type_decoder)?;
            required_boxed(path, &binary.right, "right", type_decoder)?;
        }
        expr::expr::Kind::UnaryOp(unary) => {
            let path = path.clone().field("unary_op");
            let op = expr::UnaryOp::try_from(unary.op).map_err(|_| {
                NativeExpressionDecodeError::invalid_enum(
                    path.clone().field("op"),
                    format!("unknown UnaryOp {}", unary.op),
                )
            })?;
            if op == expr::UnaryOp::Unspecified {
                return Err(NativeExpressionDecodeError::invalid_enum(
                    path.clone().field("op"),
                    "UnaryOp.op is unspecified",
                ));
            }
            required_boxed(path, &unary.operand, "operand", type_decoder)?;
        }
        expr::expr::Kind::FunctionCall(call) => {
            let path = path.clone().field("function_call");
            validate_function_name_at(&call.function_name, path.clone().field("function_name"))?;
            list(path, &call.args, "args", type_decoder)?;
        }
        expr::expr::Kind::AggregateCall(call) => {
            let path = path.clone().field("aggregate_call");
            validate_function_name_at(&call.function_name, path.clone().field("function_name"))?;
            list(path.clone(), &call.args, "args", type_decoder)?;
            validate_sort_items_at(&call.order_by, path.clone().field("order_by"), type_decoder)?;
        }
        expr::expr::Kind::WindowCall(call) => {
            let path = path.clone().field("window_call");
            validate_function_name_at(&call.function_name, path.clone().field("function_name"))?;
            list(path.clone(), &call.args, "args", type_decoder)?;
            list(
                path.clone(),
                &call.partition_by,
                "partition_by",
                type_decoder,
            )?;
            validate_sort_items_at(&call.order_by, path.clone().field("order_by"), type_decoder)?;
            if let Some(frame) = &call.frame {
                validate_window_frame_at(frame, path.clone().field("frame"))?;
            }
        }
        expr::expr::Kind::Cast(cast) => {
            let path = path.clone().field("cast");
            let policy = decode_decimal_overflow_policy(
                cast.decimal_overflow_policy,
                path.clone().field("decimal_overflow_policy"),
            )?;
            let operand = cast.operand.as_ref().ok_or_else(|| {
                NativeExpressionDecodeError::missing(
                    path.clone().field("operand"),
                    "native Cast requires operand",
                )
            })?;
            let target = cast.target.as_ref().ok_or_else(|| {
                NativeExpressionDecodeError::missing(
                    path.clone().field("target"),
                    "native Cast requires target",
                )
            })?;
            let source_type =
                decode_expr_type_at(operand, path.clone().field("operand"), type_decoder)?;
            let target_type =
                decode_type_with_decoder(target, path.clone().field("target"), type_decoder)?;
            if !novarocks_type_contract::decimal_error_policy_cast_supported(
                &source_type,
                &target_type,
                policy,
            ) {
                return Err(NativeExpressionDecodeError::unsupported(
                    path.clone().field("decimal_overflow_policy"),
                    "ReportError decimal CAST does not support nested type conversion",
                ));
            }
            required_boxed(path.clone(), &cast.operand, "operand", type_decoder)?;
        }
        expr::expr::Kind::IsNull(is_null) => required_boxed(
            path.clone().field("is_null"),
            &is_null.operand,
            "operand",
            type_decoder,
        )?,
        expr::expr::Kind::InList(in_list) => {
            let path = path.clone().field("in_list");
            required_boxed(path.clone(), &in_list.operand, "operand", type_decoder)?;
            if in_list.list.is_empty() {
                return Err(NativeExpressionDecodeError::invalid_value(
                    path.clone().field("list"),
                    "InList.list is empty",
                ));
            }
            list(path, &in_list.list, "list", type_decoder)?;
        }
        expr::expr::Kind::Between(between) => {
            let path = path.clone().field("between");
            required_boxed(path.clone(), &between.operand, "operand", type_decoder)?;
            required_boxed(path.clone(), &between.low, "low", type_decoder)?;
            required_boxed(path, &between.high, "high", type_decoder)?;
        }
        expr::expr::Kind::Like(like) => {
            let path = path.clone().field("like");
            required_boxed(path.clone(), &like.operand, "operand", type_decoder)?;
            required_boxed(path, &like.pattern, "pattern", type_decoder)?;
        }
        expr::expr::Kind::CaseExpr(case_expr) => {
            let path = path.clone().field("case_expr");
            if let Some(operand) = &case_expr.operand {
                validate_proto_expr_shape_at_with_types(
                    operand,
                    path.clone().field("operand"),
                    type_decoder,
                )?;
            }
            if case_expr.when_then.is_empty() {
                return Err(NativeExpressionDecodeError::invalid_value(
                    path.clone().field("when_then"),
                    "CaseExpr.when_then is empty",
                ));
            }
            for (index, branch) in case_expr.when_then.iter().enumerate() {
                let branch_path = path.clone().field("when_then").index(index);
                let when = branch.when.as_ref().ok_or_else(|| {
                    NativeExpressionDecodeError::missing(
                        branch_path.clone().field("when"),
                        "native CaseExpr branch requires when",
                    )
                })?;
                validate_proto_expr_shape_at_with_types(
                    when,
                    branch_path.clone().field("when"),
                    type_decoder,
                )?;
                let then = branch.then.as_ref().ok_or_else(|| {
                    NativeExpressionDecodeError::missing(
                        branch_path.clone().field("then"),
                        "native CaseExpr branch requires then",
                    )
                })?;
                validate_proto_expr_shape_at_with_types(
                    then,
                    branch_path.field("then"),
                    type_decoder,
                )?;
            }
            if let Some(else_expr) = &case_expr.else_expr {
                validate_proto_expr_shape_at_with_types(
                    else_expr,
                    path.clone().field("else_expr"),
                    type_decoder,
                )?;
            }
        }
        expr::expr::Kind::IsTruth(is_truth) => required_boxed(
            path.clone().field("is_truth"),
            &is_truth.operand,
            "operand",
            type_decoder,
        )?,
        expr::expr::Kind::Lambda(lambda) => {
            let path = path.clone().field("lambda");
            if lambda.params.is_empty() {
                return Err(NativeExpressionDecodeError::invalid_value(
                    path.clone().field("params"),
                    "LambdaExpr.params is empty",
                ));
            }
            let mut slots = std::collections::BTreeSet::new();
            for (index, param) in lambda.params.iter().enumerate() {
                let param_path = path.clone().field("params").index(index);
                if param.slot_id <= 0 {
                    return Err(NativeExpressionDecodeError::out_of_range(
                        param_path.clone().field("slot_id"),
                        "Lambda parameter slot_id must be positive",
                    ));
                }
                if !slots.insert(param.slot_id) {
                    return Err(NativeExpressionDecodeError::inconsistent(
                        param_path.field("slot_id"),
                        format!("duplicate Lambda parameter slot_id={}", param.slot_id),
                    ));
                }
                let param_type = param.r#type.as_ref().ok_or_else(|| {
                    NativeExpressionDecodeError::missing(
                        param_path.clone().field("type"),
                        "native Lambda parameter requires type",
                    )
                })?;
                decode_type_with_decoder(param_type, param_path.field("type"), type_decoder)?;
            }
            required_boxed(path, &lambda.body, "body", type_decoder)?;
        }
        expr::expr::Kind::Nested(nested) => required_boxed(
            path.clone().field("nested"),
            &nested.inner,
            "inner",
            type_decoder,
        )?,
        expr::expr::Kind::ColumnRef(column) => {
            let path = path.clone().field("column_ref");
            if column.column_id == 0 {
                return Err(NativeExpressionDecodeError::out_of_range(
                    path.clone().field("column_id"),
                    "ColumnRef.column_id must be positive",
                ));
            }
        }
        expr::expr::Kind::ConstantReference(reference) => {
            if reference.pool_id.is_none() {
                return Err(NativeExpressionDecodeError::missing(
                    path.clone().field("constant_reference").field("pool_id"),
                    "constant pool reference is absent",
                ));
            }
        }
        expr::expr::Kind::Literal(literal) => {
            let path = path.clone().field("literal");
            type_decoder.reject_strong_inline_literal(path.clone())?;
            let value = literal.value.as_ref().ok_or_else(|| {
                NativeExpressionDecodeError::missing(
                    path.clone().field("value"),
                    "native LiteralExpr requires value",
                )
            })?;
            if value.value.is_none() {
                return Err(NativeExpressionDecodeError::missing(
                    path.clone().field("value").field("value"),
                    "native LiteralValue requires value",
                ));
            }
        }
        expr::expr::Kind::LambdaParamRef(param) => {
            let path = path.clone().field("lambda_param_ref");
            if param.slot_id <= 0 {
                return Err(NativeExpressionDecodeError::out_of_range(
                    path.clone().field("slot_id"),
                    "LambdaParamRef.slot_id must be positive",
                ));
            }
        }
    }
    Ok(())
}

fn validate_function_name_at(
    name: &str,
    path: FieldPath,
) -> Result<(), NativeExpressionDecodeError> {
    if name.is_empty() {
        return Err(NativeExpressionDecodeError::invalid_value(
            path,
            "function_name is empty",
        ));
    }
    Ok(())
}

fn validate_sort_items_at(
    items: &[expr::SortItem],
    path: FieldPath,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<(), NativeExpressionDecodeError> {
    for (index, item) in items.iter().enumerate() {
        let expression_path = path.clone().index(index).field("expr");
        let expression = item.expr.as_ref().ok_or_else(|| {
            NativeExpressionDecodeError::missing(
                expression_path.clone(),
                "native SortItem requires expr",
            )
        })?;
        validate_proto_expr_shape_at_with_types(expression, expression_path, type_decoder)?;
    }
    Ok(())
}

fn validate_window_frame_at(
    frame: &expr::WindowFrame,
    path: FieldPath,
) -> Result<(), NativeExpressionDecodeError> {
    let frame_type = expr::WindowFrameType::try_from(frame.frame_type).map_err(|_| {
        NativeExpressionDecodeError::invalid_enum(
            path.clone().field("frame_type"),
            format!("unknown WindowFrameType {}", frame.frame_type),
        )
    })?;
    if frame_type == expr::WindowFrameType::Unspecified {
        return Err(NativeExpressionDecodeError::invalid_enum(
            path.clone().field("frame_type"),
            "WindowFrame.frame_type is unspecified",
        ));
    }
    let start = frame.start.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("start"),
            "native WindowFrame requires start",
        )
    })?;
    validate_window_bound_at(start, path.clone().field("start"))?;
    let end = frame.end.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("end"),
            "native WindowFrame requires end",
        )
    })?;
    validate_window_bound_at(end, path.field("end"))?;
    Ok(())
}

fn validate_window_bound_at(
    bound: &expr::WindowBound,
    path: FieldPath,
) -> Result<(), NativeExpressionDecodeError> {
    match bound.bound.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(
            path.clone().field("bound"),
            "native WindowBound requires bound",
        )
    })? {
        expr::window_bound::Bound::UnboundedPreceding(true)
        | expr::window_bound::Bound::CurrentRow(true)
        | expr::window_bound::Bound::UnboundedFollowing(true) => Ok(()),
        expr::window_bound::Bound::UnboundedPreceding(false)
        | expr::window_bound::Bound::CurrentRow(false)
        | expr::window_bound::Bound::UnboundedFollowing(false) => Err(
            NativeExpressionDecodeError::invalid_value(path, "WindowBound marker must be true"),
        ),
        expr::window_bound::Bound::Preceding(offset)
        | expr::window_bound::Bound::Following(offset)
            if offset >= &0 =>
        {
            Ok(())
        }
        expr::window_bound::Bound::Preceding(offset)
        | expr::window_bound::Bound::Following(offset) => {
            Err(NativeExpressionDecodeError::out_of_range(
                path,
                format!("WindowBound offset must be nonnegative, got {offset}"),
            ))
        }
    }
}

fn lower_required_child(
    child: &Option<Box<expr::Expr>>,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<ExprId, NativeExpressionDecodeError> {
    let child = child.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(path.clone(), "native Expr child is required")
    })?;
    decode_expr_at_with_types(child, path, arena, input_layout, type_decoder)
}

fn lower_required_unboxed_child(
    child: &Option<expr::Expr>,
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<ExprId, NativeExpressionDecodeError> {
    let child = child.as_ref().ok_or_else(|| {
        NativeExpressionDecodeError::missing(path.clone(), "native Expr child is required")
    })?;
    decode_expr_at_with_types(child, path, arena, input_layout, type_decoder)
}

fn lower_expr_list(
    values: &[expr::Expr],
    path: FieldPath,
    arena: &mut ExprArena,
    input_layout: &NativeExpressionInputLayout,

    type_decoder: &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
) -> Result<Vec<ExprId>, NativeExpressionDecodeError> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| {
            decode_expr_at_with_types(
                value,
                path.clone().index(index),
                arena,
                input_layout,
                type_decoder,
            )
        })
        .collect()
}

#[cfg(test)]
#[path = "fragment_expression/fixture_sources.rs"]
pub(crate) mod fixture_sources;

#[cfg(test)]
#[path = "fragment_expression/fixture_scalar_sources.rs"]
mod fixture_scalar_sources;

#[cfg(test)]
pub(crate) mod tests {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Fields, Schema};
    use std::sync::Arc;

    use super::*;
    use novarocks_execution::exec::chunk::Chunk;
    use novarocks_execution::exec::expr::{
        ExprArena, ExprNode, LiteralValue, function::FunctionKind,
    };
    use novarocks_proto_models::{common, expr};
    use novarocks_types::SlotId;

    use super::fixture_sources;
    pub(crate) use fixture_sources::{
        Declaration, json_type, largeint_type, type_desc, variant_type,
    };

    pub(crate) fn decode_fixture_expr(
        e: &expr::Expr,
        arena: &mut ExprArena,
        layout: &NativeExpressionInputLayout,
    ) -> Result<ExprId, NativeExpressionDecodeError> {
        fixture_sources::decode(e, FieldPath::root("expr"), arena, layout)
    }

    pub(crate) fn scalar_expr(data_type: impl Declaration, kind: expr::expr::Kind) -> expr::Expr {
        let declared = data_type.declared();
        let (declared, kind) = match kind {
            expr::expr::Kind::Literal(literal) => {
                // This literal's actual constant source is immutable and
                // non-null unless its independently authored value is NULL.
                let nullable = matches!(
                    literal
                        .value
                        .as_ref()
                        .and_then(|value| value.value.as_ref()),
                    Some(common::literal_value::Value::NullValue(true))
                );
                let declared = declared.with_nullable(nullable).unwrap();
                let reference = fixture_sources::constant_reference(&declared, &literal);
                (declared, expr::expr::Kind::ConstantReference(reference))
            }
            kind => (declared, kind),
        };
        expr::Expr {
            original_definition_id: None,
            r#type: Some(fixture_sources::type_desc_with_nullable(
                &declared,
                declared.nullable(),
            )),
            nullable: declared.nullable(),
            kind: Some(kind),
        }
    }

    pub(crate) fn with_fixture_decoder<T>(
        operation: impl FnOnce(
            &mut crate::fragment_type_decode::NativeTypeDecoder<'_, '_, '_>,
        ) -> Result<T, NativeExpressionDecodeError>,
    ) -> Result<T, NativeExpressionDecodeError> {
        fixture_sources::with_decoder(operation)
    }

    pub(crate) fn validate_fixture_expression(
        e: &expr::Expr,
        path: FieldPath,
    ) -> Result<(), NativeExpressionDecodeError> {
        with_fixture_decoder(|decoder| {
            super::validate_proto_expr_shape_at_with_types(e, path, decoder)
        })
    }

    pub(crate) fn fixture_constant_is_null(value: &novarocks_functions::ConstantValue) -> bool {
        fixture_sources::constant_is_null(value)
    }

    pub(crate) fn assert_complete_frozen(arena: ExprArena) {
        let frozen = arena
            .into_immutable()
            .expect("complete Native expression freeze");
        assert!(
            frozen
                .nodes()
                .iter()
                .all(|node| node.value_type().is_some()),
            "every original and synthetic node retains its complete authored binding"
        );
    }

    pub(crate) fn int_lit(value: i64) -> expr::Expr {
        scalar_expr(
            DataType::Int64,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::IntValue(value)),
                }),
            }),
        )
    }

    pub(crate) fn string_lit(value: &str) -> expr::Expr {
        scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::StringValue(value.to_string())),
                }),
            }),
        )
    }

    pub(crate) fn bool_lit(value: bool) -> expr::Expr {
        scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::BoolValue(value)),
                }),
            }),
        )
    }

    pub(crate) fn null_lit(data_type: DataType) -> expr::Expr {
        scalar_expr(
            data_type,
            expr::expr::Kind::Literal(expr::LiteralExpr {
                value: Some(common::LiteralValue {
                    value: Some(common::literal_value::Value::NullValue(true)),
                }),
            }),
        )
    }

    pub(crate) fn col(column_id: u32, data_type: impl Declaration) -> expr::Expr {
        scalar_expr(
            data_type,
            expr::expr::Kind::ColumnRef(expr::ColumnRef {
                column_id,
                qualifier: None,
                column: None,
            }),
        )
    }

    pub(crate) fn map_string_json_type() -> novarocks_type_contract::FunctionValueType {
        fixture_sources::map_json_type()
    }

    pub(crate) fn layout_for_slots(slots: &[u32]) -> NativeExpressionInputLayout {
        NativeExpressionInputLayout::from_slot_ids(slots.iter().copied().map(SlotId::new))
    }

    pub(crate) fn lower_with_slots(
        e: &expr::Expr,
        slots: &[u32],
    ) -> (ExprArena, novarocks_execution::exec::expr::ExprId) {
        let mut arena = ExprArena::default();
        let layout = layout_for_slots(slots);
        let id = decode_fixture_expr(e, &mut arena, &layout).expect("lower proto expr");
        (arena, id)
    }

    pub(crate) fn lower(e: &expr::Expr) -> (ExprArena, novarocks_execution::exec::expr::ExprId) {
        lower_with_slots(e, &[1, 7, 42])
    }

    pub(crate) fn lower_err_with_slots(e: &expr::Expr, slots: &[u32]) -> String {
        let mut arena = ExprArena::default();
        let layout = layout_for_slots(slots);
        decode_fixture_expr(e, &mut arena, &layout)
            .unwrap_err()
            .to_string()
    }

    pub(crate) fn make_i64_chunk(slot: SlotId, values: Vec<Option<i64>>) -> Chunk {
        let field = Field::new("c0", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        let chunk_schema =
            novarocks_execution::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
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

        assert_eq!(
            err,
            "native protocol error at expr.column_ref.column_id (invalid value): ColumnRef column_id=42 not found in input layout"
        );
    }

    #[test]
    fn input_layout_preserves_unknown_column_error_contract() {
        let error = NativeExpressionInputLayout::from_slot_ids([SlotId::new(7)])
            .resolve_column_id(9, FieldPath::root("expr").field("column_ref"))
            .expect_err("unknown slot must fail");

        assert_eq!(
            error.to_string(),
            "native protocol error at expr.column_ref.column_id (invalid value): ColumnRef column_id=9 not found in input layout"
        );
    }

    #[test]
    fn lowers_recursive_binary_cast_and_function_call() {
        let add = scalar_expr(
            DataType::Int64,
            expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                allow_throw_exception: None,
                decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
                op: expr::BinaryOp::Add as i32,
                left: Some(Box::new(col(7, DataType::Int64))),
                right: Some(Box::new(int_lit(5))),
            })),
        );
        let cast = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                allow_throw_exception: None,
                decimal_overflow_policy: expr::DecimalOverflowPolicy::OutputNull as i32,
                operand: Some(Box::new(add)),
                target: Some(type_desc(&DataType::Utf8)),
            })),
        );
        let mut call = scalar_expr(
            DataType::Utf8,
            expr::expr::Kind::FunctionCall(expr::FunctionCall {
                frozen_binding: None,
                original_request_id: None,
                function_name: "upper".to_string(),
                args: vec![cast],
                distinct: false,
            }),
        );

        let source = super::fixture_scalar_sources::ScalarSource::new(
            "upper",
            &[novarocks_functions::FunctionArgument::Value {
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
                constant: None,
            }],
        );
        source.attach(&mut call, 700);
        let (arena, id) = source.lower(&call, &[1, 7, 42], 700);
        let Some(ExprNode::FunctionCall { kind, args }) = arena.node(id) else {
            panic!("expected function call");
        };
        assert_eq!(*kind, FunctionKind::Upper);
        assert_eq!(args.len(), 1);
        let Some(ExprNode::Cast(add_id, _)) = arena.node(args[0]) else {
            panic!("expected cast arg");
        };
        assert!(matches!(
            arena.node(*add_id),
            Some(ExprNode::Add(left, right, novarocks_type_contract::DecimalOverflowPolicy::OutputNull))
                if matches!(arena.node(*left), Some(ExprNode::SlotId(SlotId(7))))
                    && matches!(arena.node(*right), Some(ExprNode::Constant(value)) if value.try_i64().unwrap() == Some(5))
        ));
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
        assert_complete_frozen(arena);
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
            let err = decode_fixture_expr(&expr, &mut arena, &layout).unwrap_err();
            assert!(err.contains(needle), "{err}");
        }
    }

    fn policy_cast(child: expr::Expr, policy: i32) -> expr::Expr {
        scalar_expr(
            DataType::Decimal128(38, 0),
            expr::expr::Kind::Cast(Box::new(expr::CastExpr {
                allow_throw_exception: None,
                operand: Some(Box::new(child)),
                target: Some(type_desc(&DataType::Decimal128(38, 0))),
                decimal_overflow_policy: policy,
            })),
        )
    }

    #[test]
    fn native_decimal_policy_survives_nested_scope_prost_decode_and_freeze() {
        use novarocks_type_contract::DecimalOverflowPolicy as Policy;
        use prost::Message;
        let null_cast = policy_cast(int_lit(1), expr::DecimalOverflowPolicy::OutputNull as i32);
        let error_cast = policy_cast(null_cast, expr::DecimalOverflowPolicy::ReportError as i32);
        let wire = scalar_expr(
            DataType::Decimal128(38, 0),
            expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                allow_throw_exception: None,
                op: expr::BinaryOp::Add as i32,
                left: Some(Box::new(error_cast)),
                right: Some(Box::new(policy_cast(
                    int_lit(1),
                    expr::DecimalOverflowPolicy::OutputNull as i32,
                ))),
                decimal_overflow_policy: expr::DecimalOverflowPolicy::ReportError as i32,
            })),
        );
        let decoded = expr::Expr::decode(wire.encode_to_vec().as_slice()).unwrap();
        let (arena, id) = lower_with_slots(&decoded, &[]);
        let Some(ExprNode::Add(left, right, Policy::ReportError)) = arena.node(id) else {
            panic!("missing arithmetic policy");
        };
        assert_eq!(
            arena.decimal_overflow_policy(*left),
            Some(Policy::ReportError)
        );
        assert_eq!(
            arena.decimal_overflow_policy(*right),
            Some(Policy::OutputNull)
        );
        let Some(ExprNode::Cast(inner, Policy::ReportError)) = arena.node(*left) else {
            panic!("missing outer cast");
        };
        assert_eq!(
            arena.decimal_overflow_policy(*inner),
            Some(Policy::OutputNull)
        );
        let frozen = arena.into_immutable().unwrap();
        let policies = frozen
            .nodes()
            .iter()
            .filter_map(|node| node.kind().decimal_overflow_policy())
            .collect::<Vec<_>>();
        assert_eq!(
            policies,
            vec![
                Policy::OutputNull,
                Policy::ReportError,
                Policy::OutputNull,
                Policy::ReportError
            ]
        );
        assert!(!frozen.allow_throw_exception());
    }

    #[test]
    fn native_rejects_absent_zero_unknown_and_irrelevant_error_policy() {
        use prost::Message;
        for policy in [0, 99, -1] {
            for wire in [
                policy_cast(int_lit(1), policy),
                scalar_expr(
                    DataType::Int64,
                    expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                        allow_throw_exception: None,
                        op: expr::BinaryOp::Add as i32,
                        left: Some(Box::new(int_lit(1))),
                        right: Some(Box::new(int_lit(1))),
                        decimal_overflow_policy: policy,
                    })),
                ),
            ] {
                // For zero, prost omits the scalar field: this covers genuinely
                // absent policy bytes, not just a hand-constructed zero object.
                let wire = expr::Expr::decode(wire.encode_to_vec().as_slice()).unwrap();
                let mut arena = ExprArena::default();
                let error =
                    decode_fixture_expr(&wire, &mut arena, &NativeExpressionInputLayout::default())
                        .unwrap_err();
                assert!(
                    error.to_string().contains("decimal_overflow_policy"),
                    "{error}"
                );
            }
        }
        let wire = scalar_expr(
            DataType::Boolean,
            expr::expr::Kind::BinaryOp(Box::new(expr::BinaryOpExpr {
                allow_throw_exception: None,
                op: expr::BinaryOp::Eq as i32,
                left: Some(Box::new(int_lit(1))),
                right: Some(Box::new(int_lit(1))),
                decimal_overflow_policy: expr::DecimalOverflowPolicy::ReportError as i32,
            })),
        );
        let error = decode_fixture_expr(
            &wire,
            &mut ExprArena::default(),
            &NativeExpressionInputLayout::default(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("requires OutputNull"));
    }
}

#[cfg(test)]
#[path = "fragment_expression/fixture_aggregate_sources.rs"]
pub(crate) mod fixture_aggregate_sources;
