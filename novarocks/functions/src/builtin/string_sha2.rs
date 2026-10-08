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

//! Selected SHA2 over the exact installed Utf8 and Int64 profile.
//! Digest state and output are fixed-width; no per-row String is allocated.
//! Library digest calls have original-control opaque checkpoints. Checked
//! output layouts are representation gates, not a formal memory grant.

use super::sha2_shared::{DigestKind, Observation};
use crate::{
    FunctionArgumentType, KernelEvaluationControl, KernelFailure, ScalarCallInput, SelectedValues,
    kernel_control::{internal, invalid},
    kernel_input::EvaluationCheckpoints,
};
use arrow_array::{Array, ArrayRef, Int64Array, StringArray};
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::DataType;
use novarocks_type_contract::ValueLogicalType;
use std::{alloc::Layout, sync::Arc};

fn digest_bytes(bits: i64) -> Option<usize> {
    DigestKind::for_bits(bits).map(DigestKind::bytes)
}
fn output_capacity(rows: usize, bytes: usize) -> Result<(), KernelFailure> {
    i32::try_from(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    let offsets = Layout::array::<i32>(
        rows.checked_add(1)
            .ok_or(KernelFailure::ResourceExhausted)?,
    )
    .map_err(|_| KernelFailure::ResourceExhausted)?
    .size();
    let bitmap = rows
        .checked_add(63)
        .map(|n| n / 64)
        .and_then(|n| n.checked_mul(8))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bytes).map_err(|_| KernelFailure::ResourceExhausted)?;
    Layout::array::<u8>(bitmap).map_err(|_| KernelFailure::ResourceExhausted)?;
    bytes
        .checked_add(offsets)
        .and_then(|n| n.checked_add(bitmap))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(())
}
fn append_digest(
    kind: DigestKind,
    text: &[u8],
    bytes: &mut Vec<u8>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<(), KernelFailure> {
    kind.visit(
        text,
        &mut |observation| match observation {
            Observation::Step => work.step(),
            Observation::OpaqueBoundary => work.flush(),
        },
        &mut |byte| bytes.push(byte),
    )
}

pub(super) fn evaluate_string_sha2<'a>(
    input: ScalarCallInput<'_, 'a>,
    control: &dyn KernelEvaluationControl,
) -> Result<SelectedValues<'a>, KernelFailure> {
    control.checkpoint(0)?;
    let mut work = EvaluationCheckpoints::new(control);
    let result = (|| {
        let types = input.contract().selected().argument_types.as_ref();
        let arguments = input.arguments();
        if types.len() != 2 || arguments.len() != 2 {
            return Err(invalid("sha2 requires its exact two checked arguments"));
        }
        for (ty, expected) in types.iter().zip([DataType::Utf8, DataType::Int64]) {
            let FunctionArgumentType::Value(ty) = ty else {
                return Err(invalid("sha2 requires value arguments"));
            };
            let exact = ty.logical_type == ValueLogicalType::Physical && ty.data_type == expected;
            work.step()?;
            if !exact {
                return Err(invalid(
                    "sha2 differs from its exact installed argument profile",
                ));
            }
        }
        let target = input.contract().result_type();
        if target.logical_type != ValueLogicalType::Physical
            || target.data_type != DataType::Utf8
            || !target.nullable
        {
            return Err(invalid(
                "sha2 differs from its exact installed result profile",
            ));
        }
        let strings = arguments[0]
            .array()
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| internal("sha2 selected source is not Utf8"))?;
        let lengths = arguments[1]
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal("sha2 selected bit length is not Int64"))?;
        let selected = |ordinal,
                        batch_row,
                        work: &mut EvaluationCheckpoints<'_>|
         -> Result<Option<(&str, i64)>, KernelFailure> {
            let mut rows = [0; 2];
            let mut null = false;
            for (i, argument) in arguments.iter().enumerate() {
                let row = argument.value_row(ordinal, batch_row);
                rows[i] = row;
                work.step()?;
                if row >= argument.array().len() {
                    return Err(internal("sha2 selected argument row is out of bounds"));
                }
                if argument.array().is_null(row) {
                    let FunctionArgumentType::Value(ty) = &types[i] else {
                        return Err(invalid("sha2 requires value arguments"));
                    };
                    if !ty.nullable {
                        return Err(internal(
                            "sha2 non-null argument contains selected SQL NULL",
                        ));
                    }
                    null = true;
                }
            }
            if null {
                Ok(None)
            } else {
                Ok(Some((strings.value(rows[0]), lengths.value(rows[1]))))
            }
        };
        let selection = input.selection();
        output_capacity(selection.len(), 0)?;
        let mut total = 0usize;
        for (ordinal, row) in selection.iter().enumerate() {
            if let Some((_, bits)) = selected(ordinal, row, &mut work)?
                && let Some(width) = digest_bytes(bits)
            {
                total = total
                    .checked_add(width * 2)
                    .ok_or(KernelFailure::ResourceExhausted)?;
            }
            work.step()?;
        }
        output_capacity(selection.len(), total)?;
        work.flush()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(total)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(selection.len() + 1)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut validity = BooleanBufferBuilder::new(selection.len());
        work.flush()?;
        offsets.push(0i32);
        let mut has_null = false;
        for (ordinal, row) in selection.iter().enumerate() {
            let value = selected(ordinal, row, &mut work)?;
            match value.filter(|(_, bits)| digest_bytes(*bits).is_some()) {
                None => {
                    validity.append(false);
                    has_null = true;
                }
                Some((text, bits)) => {
                    let width = digest_bytes(bits).unwrap() * 2;
                    if width > total - bytes.len() {
                        return Err(internal("sha2 exceeded its measured output extent"));
                    }
                    let kind = DigestKind::for_bits(bits)
                        .ok_or_else(|| internal("sha2 measured an unsupported bit length"))?;
                    append_digest(kind, text.as_bytes(), &mut bytes, &mut work)?;
                    validity.append(true);
                }
            }
            offsets.push(i32::try_from(bytes.len()).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        if bytes.len() != total {
            return Err(internal("sha2 differs from its measured output extent"));
        }
        work.flush()?;
        let array = Arc::new(StringArray::new(
            OffsetBuffer::new(offsets.into()),
            Buffer::from(bytes),
            has_null.then(|| NullBuffer::new(validity.finish())),
        )) as ArrayRef;
        work.flush()?;
        SelectedValues::try_new_observed::<KernelFailure>(
            selection,
            &target.data_type,
            array,
            Box::default(),
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

#[cfg(test)]
#[path = "string_sha2_tests.rs"]
mod tests;
