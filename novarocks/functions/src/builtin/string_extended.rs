// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Selected string encodings, fixed-width numeric rendering and chained Murmur3.
//! Every variable-size traversal observes the original evaluation control.
pub use super::murmur::{
    format_decimal_with_scale, format_decimal256_with_scale, format_float32_for_varchar,
    format_float64_for_varchar, format_timestamp_for_varchar, murmur_hash3_32,
};
pub use super::string_extended_owner::Operation as StringOperation;
use crate::{
    EvaluatedArgument, FunctionArgumentType, KernelEvaluationControl, KernelFailure, RowDataError,
    ScalarCallContract, ScalarCallInput, SelectedValues, Selection,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use StringOperation as Operation;
use arrow_array::{Array, ArrayRef, BinaryArray, Int32Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

pub(super) fn validate_profile(
    op: Operation,
    contract: &ScalarCallContract,
    mut step: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), KernelFailure> {
    let types = &contract.selected().argument_types;
    let arity = match op {
        Operation::Unhex | Operation::Money => types.len() == 1,
        Operation::ToBinary => types.len() == 1 || types.len() == 2,
        Operation::Murmur => !types.is_empty(),
        Operation::RegexpReplace => types.len() == 3,
    };
    if !arity {
        return Err(invalid(
            "extended string call differs from its exact selected arity",
        ));
    }
    for ty in types {
        let FunctionArgumentType::Value(ty) = ty else {
            return Err(invalid("extended string requires value arguments"));
        };
        step()?;
        let physical = ty.logical_type == ValueLogicalType::Physical;
        let admitted = match op {
            Operation::Unhex | Operation::ToBinary | Operation::RegexpReplace => {
                physical && ty.data_type == DataType::Utf8
            }
            Operation::Money => {
                physical
                    && matches!(
                        ty.data_type,
                        DataType::Int64 | DataType::Float64 | DataType::Decimal128(..)
                    )
            }
            Operation::Murmur => {
                (physical
                    && matches!(
                        ty.data_type,
                        DataType::Utf8
                            | DataType::LargeUtf8
                            | DataType::Binary
                            | DataType::LargeBinary
                            | DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::UInt8
                            | DataType::UInt16
                            | DataType::UInt32
                            | DataType::UInt64
                            | DataType::Boolean
                            | DataType::Float32
                            | DataType::Float64
                            | DataType::Decimal128(..)
                            | DataType::Decimal256(..)
                            | DataType::Timestamp(_, None)
                    ))
                    || (ty.logical_type == ValueLogicalType::LargeInt
                        && ty.data_type == DataType::FixedSizeBinary(16))
            }
        };
        if !admitted {
            return Err(invalid(
                "extended string call has no exact installed selected profile",
            ));
        }
    }
    let expected = match op {
        Operation::Unhex | Operation::ToBinary => DataType::Binary,
        Operation::Money | Operation::RegexpReplace => DataType::Utf8,
        Operation::Murmur => DataType::Int32,
    };
    step()?;
    if contract.result_type().logical_type != ValueLogicalType::Physical
        || contract.result_type().data_type != expected
    {
        return Err(invalid(
            "extended string call differs from its exact selected result profile",
        ));
    }
    Ok(())
}

fn reserve<T>(n: usize, work: &mut EvaluationCheckpoints<'_>) -> Result<Vec<T>, KernelFailure> {
    Layout::array::<T>(n).map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    let mut v = Vec::new();
    v.try_reserve_exact(n)
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()?;
    Ok(v)
}
#[derive(Clone, Copy)]
pub(super) enum Row {
    Null,
    Value,
    Error(&'static str),
}
/// Carrier-neutral shared arguments. The shell owns binding and projection.
#[derive(Clone, Copy)]
pub struct StringCoreInput<'call, 'a> {
    pub arguments: &'a [EvaluatedArgument<'a>],
    pub selection: Selection<'a>,
    /// V1 captures the full first raw diagnostic before owner-boundary truncation.
    pub error_boundary: Option<&'call dyn Fn(&str) -> Result<(), KernelFailure>>,
}
impl<'a> StringCoreInput<'_, 'a> {
    fn arguments(self) -> &'a [EvaluatedArgument<'a>] {
        self.arguments
    }
    fn selection(self) -> Selection<'a> {
        self.selection
    }
}
fn argument_row(
    input: &StringCoreInput<'_, '_>,
    i: usize,
    ordinal: usize,
    batch: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    let argument = &input.arguments()[i];
    let row = argument.value_row(ordinal, batch);
    work.step()?;
    if row >= argument.array().len() {
        return Err(internal(
            "extended string selected argument row out of bounds",
        ));
    }
    Ok(row)
}
fn visit_row(
    op: Operation,
    input: &StringCoreInput<'_, '_>,
    ordinal: usize,
    batch: usize,
    work: &mut EvaluationCheckpoints<'_>,
    mut emit: impl FnMut(u8) -> Result<(), KernelFailure>,
) -> Result<Row, KernelFailure> {
    if matches!(op, Operation::Murmur) && input.arguments().is_empty() {
        for byte in 104_729u32.to_le_bytes() {
            emit(byte)?;
            work.step()?;
        }
        return Ok(Row::Value);
    }
    let row = argument_row(input, 0, ordinal, batch, work)?;
    let array = input.arguments()[0].array();
    if array.is_null(row) {
        return Ok(Row::Null);
    }
    match op {
        Operation::Unhex | Operation::ToBinary => {
            let text = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| internal("binary input is not Utf8"))?
                .value(row);
            let format = if input.arguments().len() == 2 {
                let at = argument_row(input, 1, ordinal, batch, work)?;
                let strings = input.arguments()[1]
                    .array()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| internal("binary format is not Utf8"))?;
                if strings.is_null(at) {
                    None
                } else {
                    Some(strings.value(at))
                }
            } else {
                None
            };
            super::string_binary::visit(text, format, matches!(op, Operation::Unhex), work, emit)
        }
        Operation::Money => super::string_money::visit(array.as_ref(), row, work, emit),
        Operation::RegexpReplace => {
            Err(internal("regexp_replace must use its shared selected core"))
        }
        Operation::Murmur => {
            let mut seed = 104_729u32;
            for i in 0..input.arguments().len() {
                let row = argument_row(input, i, ordinal, batch, work)?;
                let array = input.arguments()[i].array();
                if array.is_null(row) {
                    return Ok(Row::Null);
                }
                let Some(next) = super::murmur::hash_array_selected(
                    array,
                    row,
                    seed,
                    work,
                    input.error_boundary,
                )?
                else {
                    return Ok(Row::Null);
                };
                seed = next;
            }
            for byte in seed.to_le_bytes() {
                emit(byte)?;
                work.step()?;
            }
            Ok(Row::Value)
        }
    }
}

pub(super) fn evaluate<'a>(
    op: Operation,
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    validate_profile(op, input.contract(), || work.step())?;
    if input.arguments().len() != input.contract().selected().argument_types.len() {
        return Err(invalid("extended string checked argument count differs"));
    }
    for (argument, ty) in input
        .arguments()
        .iter()
        .zip(&input.contract().selected().argument_types)
    {
        let FunctionArgumentType::Value(ty) = ty else {
            return Err(invalid("extended string requires value arguments"));
        };
        if argument.array().data_type() != &ty.data_type {
            return Err(invalid(
                "extended string actual argument carrier differs from selected contract",
            ));
        }
        for (ordinal, batch) in input.selection().iter().enumerate() {
            let row = argument.value_row(ordinal, batch);
            work.step()?;
            if row >= argument.array().len() {
                return Err(internal(
                    "extended string selected argument row out of bounds",
                ));
            }
            if argument.array().is_null(row) && !ty.nullable {
                return Err(internal(
                    "extended string non-null argument contains selected SQL NULL",
                ));
            }
        }
        work.step()?;
    }
    work.finish()?;
    evaluate_selected(
        op,
        StringCoreInput {
            arguments: input.arguments(),
            selection: input.selection(),
            error_boundary: None,
        },
        control,
    )
}
/// Both v1 full selections and prepared selected owners enter this same computation.
pub fn evaluate_selected<'a>(
    op: StringOperation,
    input: StringCoreInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    if matches!(op, Operation::RegexpReplace) {
        return super::string_regexp_replace::evaluate_selected(input, control);
    }
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let target = match op {
        Operation::Unhex | Operation::ToBinary => DataType::Binary,
        Operation::Money | Operation::RegexpReplace => DataType::Utf8,
        Operation::Murmur => DataType::Int32,
    };
    let result = (|| {
        let selection = input.selection();
        let mut total = 0usize;
        let mut errors = 0usize;
        for (ordinal, batch) in selection.iter().enumerate() {
            let mut length = 0usize;
            let row = visit_row(op, &input, ordinal, batch, &mut work, |_| {
                length = length
                    .checked_add(1)
                    .ok_or(KernelFailure::ResourceExhausted)?;
                Ok(())
            })?;
            match row {
                Row::Value => {
                    total = total
                        .checked_add(length)
                        .ok_or(KernelFailure::ResourceExhausted)?
                }
                Row::Error(message) => {
                    if let Some(boundary) = input.error_boundary {
                        boundary(message)?;
                    }
                    errors += 1;
                }
                Row::Null => {}
            }
            work.step()?;
        }
        i32::try_from(total).map_err(|_| KernelFailure::ResourceExhausted)?;
        let mut bytes = reserve::<u8>(total, &mut work)?;
        let mut offsets = reserve::<i32>(
            selection
                .len()
                .checked_add(1)
                .ok_or(KernelFailure::ResourceExhausted)?,
            &mut work,
        )?;
        let mut row_errors = reserve::<RowDataError>(errors, &mut work)?;
        let mut hashes = reserve::<Option<i32>>(
            if matches!(op, Operation::Murmur) {
                selection.len()
            } else {
                0
            },
            &mut work,
        )?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        offsets.push(0);
        let mut has_null = false;
        for (ordinal, batch) in selection.iter().enumerate() {
            let start = bytes.len();
            let row = visit_row(op, &input, ordinal, batch, &mut work, |byte| {
                if bytes.len() >= total {
                    return Err(internal("extended string exceeded measured output extent"));
                }
                bytes.push(byte);
                Ok(())
            })?;
            let valid = matches!(row, Row::Value);
            if !valid {
                bytes.truncate(start);
            }
            if let Row::Error(message) = row {
                work.flush()?;
                row_errors.push(RowDataError::new(ordinal, message));
                work.flush()?;
            }
            if matches!(op, Operation::Murmur) {
                hashes.push(if valid {
                    Some(i32::from_le_bytes(
                        bytes[start..]
                            .try_into()
                            .map_err(|_| internal("Murmur3 output width differs"))?,
                    ))
                } else {
                    None
                });
            }
            validity.append(valid);
            has_null |= !valid;
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total || row_errors.len() != errors {
            return Err(internal("extended string measured output extent differs"));
        }
        work.flush()?;
        let array: ArrayRef = match op {
            Operation::Murmur => Arc::new(Int32Array::from(hashes)),
            Operation::Money | Operation::RegexpReplace => Arc::new(StringArray::new(
                OffsetBuffer::new(offsets.into()),
                Buffer::from(bytes),
                has_null.then(|| NullBuffer::new(validity.finish())),
            )),
            _ => Arc::new(BinaryArray::new(
                OffsetBuffer::new(offsets.into()),
                Buffer::from(bytes),
                has_null.then(|| NullBuffer::new(validity.finish())),
            )),
        };
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target,
            array,
            row_errors.into_boxed_slice(),
            || work.step(),
        )
    })();
    if matches!(
        &result,
        Err(KernelFailure::Cancelled
            | KernelFailure::DeadlineExceeded
            | KernelFailure::ResourceExhausted)
    ) {
        return result;
    }
    work.finish()?;
    result
}

/// V1 host adapter: arguments have already been evaluated and expanded.
/// Data errors remain batch failures with the original first-row message.
pub fn evaluate_legacy(
    op: StringOperation,
    arrays: &[ArrayRef],
    rows: usize,
) -> Result<ArrayRef, String> {
    let arguments: Vec<_> = arrays.iter().map(EvaluatedArgument::Column).collect();
    let raw_error = std::cell::RefCell::new(None);
    let error_boundary = |message: &str| {
        *raw_error.borrow_mut() = Some(message.to_string());
        Err(KernelFailure::InstanceFailed)
    };
    let result = evaluate_selected(
        op,
        StringCoreInput {
            arguments: &arguments,
            selection: Selection::all(rows),
            error_boundary: Some(&error_boundary),
        },
        &LegacyStringControl,
    );
    if let Some(message) = raw_error.into_inner() {
        return Err(message);
    }
    let values = result.map_err(|error| error.to_string())?;
    if let Some(error) = values.errors().first() {
        return Err(error.message().to_string());
    }
    Ok(Arc::clone(values.values()))
}
pub(super) struct LegacyStringControl;
impl KernelEvaluationControl for LegacyStringControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        Err(internal("legacy string computation must not wait"))
    }
}

/// V1 knows the actual Arrow carrier before its residual carrier projection.
pub fn murmur_supported_carrier(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Boolean
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
            | DataType::Decimal256(..)
            | DataType::Timestamp(_, None)
            | DataType::FixedSizeBinary(16)
    )
}

#[cfg(test)]
mod shared_legacy_tests {
    use super::*;
    use arrow_array::{Float64Array, Int32Array, StructArray};
    use arrow_schema::Field;

    #[test]
    fn shared_string_legacy_preserves_long_arrow_projection_error() {
        let field = Arc::new(Field::new("long-field-".repeat(128), DataType::Int32, true));
        let values: ArrayRef = Arc::new(StructArray::from(vec![(
            field,
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )]));
        let options = arrow_cast::CastOptions {
            safe: false,
            format_options: arrow_cast::display::FormatOptions::default(),
        };
        let raw =
            arrow_cast::cast_with_options(values.as_ref(), &DataType::Utf8, &options).unwrap_err();
        let expected = format!("cast to Utf8 failed for murmur_hash3_32: {raw}");
        assert!(
            expected.len() > crate::MAX_ROW_ERROR_MESSAGE_BYTES,
            "{expected}"
        );
        assert_eq!(
            evaluate_legacy(StringOperation::Murmur, &[values], 1).unwrap_err(),
            expected
        );
    }

    #[test]
    fn shared_string_legacy_masks_unsupported_projection_after_preceding_null() {
        let field = Arc::new(Field::new("invalid-projection", DataType::Int32, true));
        let values: ArrayRef = Arc::new(StructArray::from(vec![(
            field,
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )]));
        let null: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
        let output = evaluate_legacy(StringOperation::Murmur, &[null, values], 1).unwrap();
        assert_eq!(output.null_count(), 1);
    }

    #[test]
    fn shared_string_legacy_unknown_long_binary_format_keeps_hex_default() {
        let input: ArrayRef = Arc::new(StringArray::from(vec!["00FF", "xx"]));
        let format = "unsupported-format-".repeat(128);
        let formats: ArrayRef = Arc::new(StringArray::from(vec![format.as_str(), format.as_str()]));
        let output = evaluate_legacy(StringOperation::ToBinary, &[input, formats], 2).unwrap();
        let binary = output.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(binary.value(0), [0, 255]);
        assert!(binary.is_null(1));
    }

    #[test]
    fn shared_string_legacy_money_failfast_keeps_first_original_error() {
        let input: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN, f64::MAX]));
        assert_eq!(
            evaluate_legacy(StringOperation::Money, &[input], 2).unwrap_err(),
            "money_format input must be finite"
        );
    }
}
