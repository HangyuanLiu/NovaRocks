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

use crate::datasketches_hll_failure::HllObservation;
use crate::hll::{MURMUR_SEED, murmur_hash64a_observed};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
    LargeStringArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, TimeUnit};

/// Original diagnostics are formatted by the same borrowed author.
pub enum SketchHashFailure<'a> {
    Downcast(&'static str),
    Unsupported(&'a DataType),
}
impl SketchHashFailure<'_> {
    pub fn message<'a>(&'a self, context: &'a str) -> SketchHashMessage<'a> {
        SketchHashMessage {
            failure: self,
            context,
        }
    }
}
#[derive(Debug)]
pub struct SketchHashContractFailure {
    detail: novarocks_failure_contract::BoundedDetail,
    downcast: bool,
}
impl SketchHashFailure<'_> {
    pub fn into_kernel(self, context: &str) -> crate::KernelFailure {
        let downcast = matches!(self, Self::Downcast(_));
        let detail =
            novarocks_failure_contract::BoundedDetail::truncating_display(self.message(context));
        crate::KernelFailure::from_domain(SketchHashContractFailure { detail, downcast })
    }
}
impl std::fmt::Display for SketchHashContractFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.detail.as_str())
    }
}
impl std::error::Error for SketchHashContractFailure {}
impl novarocks_failure_contract::FailureProjection for SketchHashContractFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        FailureFact::new(
            FailureClass::Invariant,
            FailureReason::new(
                domain::KERNEL,
                std::num::NonZeroU16::new(if self.downcast { 306 } else { 307 }).unwrap(),
            ),
        )
    }
}
impl novarocks_failure_contract::OuterFailureProjection for SketchHashContractFailure {}
pub struct SketchHashMessage<'a> {
    failure: &'a SketchHashFailure<'a>,
    context: &'a str,
}
impl std::fmt::Display for SketchHashMessage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.failure {
            SketchHashFailure::Downcast(kind) => {
                write!(f, "{}: failed to downcast {}", self.context, kind)
            }
            SketchHashFailure::Unsupported(ty) => write!(
                f,
                "{}: unsupported sketch hash input type {:?}",
                self.context, ty
            ),
        }
    }
}
pub trait SketchHashFailureSink {
    type Error;
    fn hash_contract(&mut self, failure: SketchHashFailure<'_>) -> Self::Error;
    fn observe_hash(&mut self, event: HllObservation) -> Result<(), Self::Error>;
}
struct LegacySketchHashFailure<'a>(&'a str);
impl SketchHashFailureSink for LegacySketchHashFailure<'_> {
    type Error = crate::KernelFailure;
    fn hash_contract(&mut self, failure: SketchHashFailure<'_>) -> crate::KernelFailure {
        failure.into_kernel(self.0)
    }
    fn observe_hash(&mut self, _: HllObservation) -> Result<(), crate::KernelFailure> {
        Ok(())
    }
}
pub fn prehash_array_value(
    array: &ArrayRef,
    row: usize,
    context: &str,
) -> Result<Option<u64>, crate::KernelFailure> {
    prehash_array_value_with_failure(array, row, &mut LegacySketchHashFailure(context))
}
pub fn prehash_array_value_with_failure<F: SketchHashFailureSink>(
    array: &ArrayRef,
    row: usize,
    sink: &mut F,
) -> Result<Option<u64>, F::Error> {
    macro_rules! hash_from_bytes {
        ($arr:expr, $value:expr) => {{
            if $arr.is_null(row) {
                return Ok(None);
            }
            return Ok(Some(murmur_hash64a_observed(
                $value.as_ref(),
                MURMUR_SEED,
                &mut || sink.observe_hash(HllObservation::Step),
            )?));
        }};
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| sink.hash_contract(SketchHashFailure::Downcast("BooleanArray")))?;
            if arr.is_null(row) {
                Ok(None)
            } else {
                Ok(Some(murmur_hash64a_observed(
                    &[if arr.value(row) { 1 } else { 0 }],
                    MURMUR_SEED,
                    &mut || sink.observe_hash(HllObservation::Step),
                )?))
            }
        }
        DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| sink.hash_contract(SketchHashFailure::Downcast("Int8Array")))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| sink.hash_contract(SketchHashFailure::Downcast("Int16Array")))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| sink.hash_contract(SketchHashFailure::Downcast("Int32Array")))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| sink.hash_contract(SketchHashFailure::Downcast("Int64Array")))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| sink.hash_contract(SketchHashFailure::Downcast("Float32Array")))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| sink.hash_contract(SketchHashFailure::Downcast("Float64Array")))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| sink.hash_contract(SketchHashFailure::Downcast("Date32Array")))?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampSecondArray>()
                    .ok_or_else(|| {
                        sink.hash_contract(SketchHashFailure::Downcast("TimestampSecondArray"))
                    })?;
                hash_from_bytes!(arr, arr.value(row).to_le_bytes())
            }
            TimeUnit::Millisecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .ok_or_else(|| {
                        sink.hash_contract(SketchHashFailure::Downcast("TimestampMillisecondArray"))
                    })?;
                hash_from_bytes!(arr, arr.value(row).to_le_bytes())
            }
            TimeUnit::Microsecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| {
                        sink.hash_contract(SketchHashFailure::Downcast("TimestampMicrosecondArray"))
                    })?;
                hash_from_bytes!(arr, arr.value(row).to_le_bytes())
            }
            TimeUnit::Nanosecond => {
                let arr = array
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .ok_or_else(|| {
                        sink.hash_contract(SketchHashFailure::Downcast("TimestampNanosecondArray"))
                    })?;
                hash_from_bytes!(arr, arr.value(row).to_le_bytes())
            }
        },
        DataType::Decimal128(_, _) => {
            let arr = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    sink.hash_contract(SketchHashFailure::Downcast("Decimal128Array"))
                })?;
            hash_from_bytes!(arr, arr.value(row).to_le_bytes())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| sink.hash_contract(SketchHashFailure::Downcast("StringArray")))?;
            hash_from_bytes!(arr, arr.value(row).as_bytes())
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    sink.hash_contract(SketchHashFailure::Downcast("LargeStringArray"))
                })?;
            hash_from_bytes!(arr, arr.value(row).as_bytes())
        }
        DataType::Binary => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| sink.hash_contract(SketchHashFailure::Downcast("BinaryArray")))?;
            hash_from_bytes!(arr, arr.value(row))
        }
        DataType::LargeBinary => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| {
                    sink.hash_contract(SketchHashFailure::Downcast("LargeBinaryArray"))
                })?;
            hash_from_bytes!(arr, arr.value(row))
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| {
                    sink.hash_contract(SketchHashFailure::Downcast("FixedSizeBinaryArray"))
                })?;
            hash_from_bytes!(arr, arr.value(row))
        }
        other => Err(sink.hash_contract(SketchHashFailure::Unsupported(other))),
    }
}

#[cfg(test)]
mod contract_tests {
    use super::*;
    use arrow_array::{Int64Array, StructArray};
    use arrow_schema::Field;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::sync::Arc;
    #[test]
    fn sketch_hash_unsupported_metadata_is_not_row_data_and_does_not_pin_fields() {
        let field = Arc::new(Field::new(
            "large field".repeat(100_000),
            DataType::Int64,
            false,
        ));
        let weak = Arc::downgrade(&field);
        let input: ArrayRef = Arc::new(StructArray::from(vec![(
            field,
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )]));
        let failure = prehash_array_value(&input, 0, "hash-contract").unwrap_err();
        assert_eq!(failure.failure_fact().class(), FailureClass::Invariant);
        assert!(failure.to_string().len() <= 512);
        drop(input);
        assert!(weak.upgrade().is_none());
    }
}
