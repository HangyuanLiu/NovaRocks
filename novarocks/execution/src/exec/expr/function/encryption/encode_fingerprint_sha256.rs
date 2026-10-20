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
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprId, expression_contract};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use novarocks_types::largeint;
use sha2::{Digest, Sha256};
use std::sync::Arc;

const MARKER_NULL: u8 = 0;
const MARKER_INT8: u8 = 1;
const MARKER_INT16: u8 = 2;
const MARKER_INT32: u8 = 3;
const MARKER_INT64: u8 = 4;
const MARKER_INT128: u8 = 5;
const MARKER_FLOAT: u8 = 6;
const MARKER_DOUBLE: u8 = 7;
const MARKER_STRING: u8 = 8;
const MARKER_DECIMAL: u8 = 9;
const MARKER_DATE: u8 = 10;
const MARKER_DATETIME: u8 = 11;

#[inline]
fn update_null(digests: &mut [Sha256], row: usize) {
    digests[row].update([MARKER_NULL]);
}

#[inline]
fn update_marker_and_bytes(digests: &mut [Sha256], row: usize, marker: u8, bytes: &[u8]) {
    digests[row].update([marker]);
    digests[row].update(bytes);
}

fn encode_string_like(
    arr: &ArrayRef,
    digests: &mut [Sha256],
    is_binary: bool,
) -> ExecutionResult<()> {
    if is_binary {
        let arr = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
            expression_contract("encode_fingerprint_sha256: failed to downcast BinaryArray")
        })?;
        for row in 0..arr.len() {
            if arr.is_null(row) {
                update_null(digests, row);
            } else {
                update_marker_and_bytes(digests, row, MARKER_STRING, arr.value(row));
            }
        }
    } else {
        let arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
            expression_contract("encode_fingerprint_sha256: failed to downcast StringArray")
        })?;
        for row in 0..arr.len() {
            if arr.is_null(row) {
                update_null(digests, row);
            } else {
                update_marker_and_bytes(digests, row, MARKER_STRING, arr.value(row).as_bytes());
            }
        }
    }
    Ok(())
}

fn encode_fixed_size_binary(arr: &FixedSizeBinaryArray, digests: &mut [Sha256], marker: u8) {
    for row in 0..arr.len() {
        if arr.is_null(row) {
            update_null(digests, row);
        } else {
            update_marker_and_bytes(digests, row, marker, arr.value(row));
        }
    }
}

fn encode_timestamp(arr: &ArrayRef, digests: &mut [Sha256]) -> ExecutionResult<()> {
    let casted = super::common::timestamp_as_i64(arr, "encode_fingerprint_sha256")?;
    for row in 0..casted.len() {
        if casted.is_null(row) {
            update_null(digests, row);
        } else {
            update_marker_and_bytes(
                digests,
                row,
                MARKER_DATETIME,
                &casted.value(row).to_le_bytes(),
            );
        }
    }
    Ok(())
}

fn encode_array(arr: &ArrayRef, digests: &mut [Sha256]) -> ExecutionResult<()> {
    if arr.len() != digests.len() {
        return Err(expression_contract(
            "encode_fingerprint_sha256: prepared argument length mismatch",
        ));
    }
    match arr.data_type() {
        DataType::Null => {
            for row in 0..arr.len() {
                update_null(digests, row);
            }
        }
        DataType::Utf8 => {
            encode_string_like(arr, digests, false)?;
        }
        DataType::Binary => {
            encode_string_like(arr, digests, true)?;
        }
        DataType::Boolean => {
            let arr = arr.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast BooleanArray")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(digests, row, MARKER_INT8, &[arr.value(row) as u8]);
                }
            }
        }
        DataType::Int8 => {
            let arr = arr.as_any().downcast_ref::<Int8Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast Int8Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT8,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::UInt8 => {
            let arr = arr.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast UInt8Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT8,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Int16 => {
            let arr = arr.as_any().downcast_ref::<Int16Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast Int16Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT16,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::UInt16 => {
            let arr = arr.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast UInt16Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT16,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Int32 => {
            let arr = arr.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast Int32Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT32,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::UInt32 => {
            let arr = arr.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast UInt32Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT32,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Date32 => {
            let arr = arr.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast Date32Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_DATE,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Int64 => {
            let arr = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast Int64Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT64,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::UInt64 => {
            let arr = arr.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast UInt64Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_INT64,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Float32 => {
            let arr = arr.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast Float32Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_FLOAT,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Float64 => {
            let arr = arr.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                expression_contract("encode_fingerprint_sha256: failed to downcast Float64Array")
            })?;
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    update_marker_and_bytes(
                        digests,
                        row,
                        MARKER_DOUBLE,
                        &arr.value(row).to_le_bytes(),
                    );
                }
            }
        }
        DataType::Timestamp(_, _) => {
            encode_timestamp(arr, digests)?;
        }
        DataType::Decimal128(precision, _) => {
            let arr = arr
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    expression_contract(
                        "encode_fingerprint_sha256: failed to downcast Decimal128Array",
                    )
                })?;
            let (marker, byte_width) = if *precision <= 9 {
                (MARKER_INT32, 4usize)
            } else if *precision <= 18 {
                (MARKER_INT64, 8usize)
            } else {
                (MARKER_DECIMAL, 16usize)
            };
            for row in 0..arr.len() {
                if arr.is_null(row) {
                    update_null(digests, row);
                } else {
                    let bytes = arr.value(row).to_le_bytes();
                    update_marker_and_bytes(digests, row, marker, &bytes[..byte_width]);
                }
            }
        }
        DataType::FixedSizeBinary(width) if *width == largeint::LARGEINT_BYTE_WIDTH => {
            let arr = largeint::as_fixed_size_binary_array(arr, "encode_fingerprint_sha256")?;
            encode_fixed_size_binary(arr, digests, MARKER_INT128);
        }
        // Preserve this function's declared compatibility semantics: an
        // unsupported complex/object-like column contributes no bytes, even
        // when NULL. This is not a query-level execution fallback.
        _ => {}
    }

    Ok(())
}

pub fn eval_encode_fingerprint_sha256<__Eval: crate::exec::expr::ExprEvaluation + ?Sized>(
    arena: &__Eval,
    expr: ExprId,
    args: &[ExprId],
    chunk: &Chunk,
) -> ExecutionResult<ArrayRef> {
    if args.is_empty() {
        return Err(expression_contract(
            "encode_fingerprint_sha256 requires at least 1 argument",
        ));
    }
    check_output_extent(chunk.len())?;
    let mut digests = digest_states(chunk.len())?;
    for arg in args {
        let arr = arena.eval(*arg, chunk)?;
        encode_array(&arr, &mut digests)?;
    }

    let mut builder = novarocks_functions::aggregate_scalar::BinaryResultBuilder::new();
    for digest in digests {
        builder.append_value(digest.finalize())?;
    }

    let out = Arc::new(builder.finish()?) as ArrayRef;
    super::common::cast_output(out, arena.data_type(expr), "encode_fingerprint_sha256")
        .map_err(ExecutionFailure::from)
}

#[derive(Debug)]
enum FingerprintFailure {
    OutputExtent {
        required: u128,
    },
    Layout {
        required: u128,
        source: std::alloc::LayoutError,
    },
    Reservation {
        required: u128,
        source: std::collections::TryReserveError,
    },
}
impl std::fmt::Display for FingerprintFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutputExtent { required } => write!(
                f,
                "SHA256 fingerprint output requires {required} Binary bytes, exceeding i32 offsets"
            ),
            Self::Layout { required, source } => write!(
                f,
                "fingerprint state layout requires {required} bytes: {source}"
            ),
            Self::Reservation { required, source } => write!(
                f,
                "fingerprint state reservation requires {required} bytes: {source}"
            ),
        }
    }
}
impl std::error::Error for FingerprintFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::OutputExtent { .. } => None,
            Self::Layout { source, .. } => Some(source),
            Self::Reservation { source, .. } => Some(source),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for FingerprintFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, RepresentationBoundary, RepresentationFacts,
            RepresentationLayout, ResourceDimension, ResourceFacts, domain,
        };
        match self {
            Self::OutputExtent { required } => FailureFact::new(
                FailureClass::Representation,
                ExecutionReason::FingerprintOutputExtent.reason(),
            )
            .with_representation(RepresentationFacts {
                boundary: RepresentationBoundary::SingleOutput,
                layout: RepresentationLayout {
                    domain: domain::KERNEL,
                    code: std::num::NonZeroU16::new(61).unwrap(),
                },
                required: u64::try_from(*required).ok(),
                extent_limit: Some(i32::MAX as u64),
                representable_prefix: None,
            }),
            Self::Layout { required, .. } | Self::Reservation { required, .. } => FailureFact::new(
                FailureClass::Resource,
                if matches!(self, Self::Layout { .. }) {
                    ExecutionReason::FingerprintStateLayout
                } else {
                    ExecutionReason::FingerprintStateAllocation
                }
                .reason(),
            )
            .with_resource(ResourceFacts {
                request_scope: None,
                constraint_scope: None,
                dimension: ResourceDimension::Bytes,
                requested: u64::try_from(*required).ok(),
                required_growth: None,
                limit: None,
                live: None,
                committed: None,
            }),
        }
    }
}
fn check_output_extent(rows: usize) -> ExecutionResult<()> {
    let required = rows as u128 * 32;
    if required > i32::MAX as u128 {
        return Err(ExecutionFailure::from_domain(
            FingerprintFailure::OutputExtent { required },
        ));
    }
    Ok(())
}
fn digest_states(rows: usize) -> ExecutionResult<Vec<Sha256>> {
    let required = rows as u128 * std::mem::size_of::<Sha256>() as u128;
    std::alloc::Layout::array::<Sha256>(rows).map_err(|source| {
        ExecutionFailure::from_domain(FingerprintFailure::Layout { required, source })
    })?;
    let mut states = Vec::new();
    states.try_reserve_exact(rows).map_err(|source| {
        ExecutionFailure::from_domain(FingerprintFailure::Reservation { required, source })
    })?;
    states.extend((0..rows).map(|_| Sha256::new()));
    Ok(states)
}
#[cfg(test)]
mod source_properties {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    #[test]
    fn fingerprint_original_marker_bytes_and_nullable_sha256_oracles_are_preserved() {
        let input: ArrayRef = Arc::new(StringArray::from(vec![Some("abc"), None]));
        let mut states = digest_states(2).unwrap();
        encode_array(&input, &mut states).unwrap();
        let values = states
            .into_iter()
            .map(|state| hex::encode(state.finalize()))
            .collect::<Vec<_>>();
        // Python hashlib.sha256 of original wire bytes 08 61 62 63 and 00.
        assert_eq!(
            values,
            [
                "5a4eb9341eb46a7aa15be680888769ee984b1e23e4d1e798ae6c761deefce69e",
                "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d"
            ]
        );
    }
    #[test]
    fn fingerprint_ignores_declared_complex_columns_and_refuses_output_extent() {
        let field = Arc::new(arrow::datatypes::Field::new(
            "ResourceExhausted",
            DataType::Int64,
            false,
        ));
        let weak_field = Arc::downgrade(&field);
        let input: ArrayRef = Arc::new(arrow::array::StructArray::new(
            vec![field].into(),
            vec![Arc::new(Int64Array::from(vec![1]))],
            None,
        ));
        let weak = Arc::downgrade(&input);
        let mut states = digest_states(1).unwrap();
        let empty = states[0].clone().finalize();
        encode_array(&input, &mut states).expect("declared complex columns contribute no bytes");
        assert_eq!(states.remove(0).finalize(), empty);
        drop(input);
        assert!(weak.upgrade().is_none());
        assert!(weak_field.upgrade().is_none());
        let maximum_rows = i32::MAX as usize / 32;
        check_output_extent(maximum_rows).unwrap();
        let failure = check_output_extent(maximum_rows + 1).unwrap_err();
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert!(fact.resource().is_none());
        let extent = fact.representation().unwrap();
        assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
        assert_eq!(extent.required, Some((maximum_rows as u64 + 1) * 32));
        assert_eq!(extent.representable_prefix, None);
    }
}
