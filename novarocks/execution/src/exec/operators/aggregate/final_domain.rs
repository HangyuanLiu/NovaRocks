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

use std::error::Error;
use std::fmt;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::exec::hash_table::key_column::KeyColumn;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FinalAggregateDomainError {
    MembershipKeyCount {
        actual: usize,
    },
    FinalKeyMaterialization(crate::runtime::fragment::ExecutionFailure),
    FinalKeyStructure(novarocks_failure_contract::BoundedDetail),
    FinalKeyUtf8(std::str::Utf8Error),
    OffsetCountExtent {
        rows: usize,
    },
    FinalKeyTypeMismatch {
        expected: novarocks_failure_contract::BoundedDetail,
        actual: novarocks_failure_contract::BoundedDetail,
    },
    FinalKeyRowCountMismatch {
        expected: usize,
        actual: usize,
    },
}

impl fmt::Display for FinalAggregateDomainError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MembershipKeyCount { actual } => write!(
                formatter,
                "final aggregate domain requires exactly one membership key column, got {actual}"
            ),
            Self::FinalKeyMaterialization(detail) => {
                write!(
                    formatter,
                    "failed to materialize final aggregate key column: {detail}"
                )
            }
            Self::FinalKeyStructure(detail) => {
                write!(formatter, "invalid final aggregate key column: {detail}")
            }
            Self::FinalKeyUtf8(source) => write!(
                formatter,
                "final aggregate Utf8 key bytes are invalid: {source}"
            ),
            Self::OffsetCountExtent { rows } => write!(
                formatter,
                "final aggregate Utf8 offset count exceeds usize for {rows} rows"
            ),
            Self::FinalKeyTypeMismatch { expected, actual } => write!(
                formatter,
                "final aggregate key materialization type mismatch: expected {expected:?}, got {actual:?}"
            ),
            Self::FinalKeyRowCountMismatch { expected, actual } => write!(
                formatter,
                "final aggregate key materialization row count mismatch: expected {expected}, got {actual}"
            ),
        }
    }
}

impl Error for FinalAggregateDomainError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FinalKeyMaterialization(source) => Some(source),
            Self::FinalKeyUtf8(source) => Some(source),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for FinalAggregateDomainError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{
            FailureClass, FailureFact, ResourceDimension, ResourceFacts,
        };
        let reason = match self {
            Self::FinalKeyMaterialization(source) => return source.failure_fact(),
            Self::MembershipKeyCount { .. } => ExecutionReason::FinalAggregateKeyCount,
            Self::FinalKeyStructure(_) => ExecutionReason::FinalAggregateKeyStructure,
            Self::FinalKeyTypeMismatch { .. } => ExecutionReason::FinalAggregateKeyType,
            Self::FinalKeyRowCountMismatch { .. } => ExecutionReason::FinalAggregateKeyRows,
            Self::FinalKeyUtf8(_) => ExecutionReason::FinalAggregateUtf8,
            Self::OffsetCountExtent { rows } => {
                return FailureFact::new(
                    FailureClass::Resource,
                    ExecutionReason::FinalAggregateOffsetExtent.reason(),
                )
                .with_resource(ResourceFacts {
                    request_scope: None,
                    constraint_scope: None,
                    dimension: ResourceDimension::Items,
                    requested: u64::try_from(*rows as u128 + 1).ok(),
                    required_growth: None,
                    limit: u64::try_from(usize::MAX).ok(),
                    live: None,
                    committed: None,
                });
            }
        };
        FailureFact::new(FailureClass::Invariant, reason.reason())
    }
}
impl From<FinalAggregateDomainError> for crate::runtime::fragment::ExecutionFailure {
    fn from(source: FinalAggregateDomainError) -> Self {
        Self::from_domain(source)
    }
}
fn structure(detail: impl fmt::Display) -> FinalAggregateDomainError {
    FinalAggregateDomainError::FinalKeyStructure(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    )
}

/// Materializes the one install-frozen membership key from finalized aggregate
/// state. Execution owns typed final-domain encoding and resource accounting;
/// Core only verifies the aggregate kernel shape and transfers its Arrow column.
pub(crate) fn extract_final_aggregate_key(
    final_key_columns: &[KeyColumn],
) -> Result<(DataType, ArrayRef), FinalAggregateDomainError> {
    let [final_key_column] = final_key_columns else {
        return Err(FinalAggregateDomainError::MembershipKeyCount {
            actual: final_key_columns.len(),
        });
    };
    let expected_rows = final_key_row_count(final_key_column)?;
    let expected_type = final_key_column.data_type();
    let array = final_key_column
        .to_array()
        .map_err(FinalAggregateDomainError::FinalKeyMaterialization)?;
    if array.len() != expected_rows {
        return Err(FinalAggregateDomainError::FinalKeyRowCountMismatch {
            expected: expected_rows,
            actual: array.len(),
        });
    }
    if array.data_type() != &expected_type {
        return Err(FinalAggregateDomainError::FinalKeyTypeMismatch {
            expected: novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
                "{expected_type:?}"
            )),
            actual: novarocks_failure_contract::BoundedDetail::truncating_display(format_args!(
                "{:?}",
                array.data_type()
            )),
        });
    }
    Ok((expected_type, array))
}

fn final_key_row_count(final_key_column: &KeyColumn) -> Result<usize, FinalAggregateDomainError> {
    fn parallel_row_count(
        values: usize,
        nulls: usize,
        key_type: &str,
    ) -> Result<usize, FinalAggregateDomainError> {
        if values != nulls {
            return Err(structure(format_args!(
                "{key_type} values/null bitmap length mismatch: values={values} nulls={nulls}"
            )));
        }
        Ok(values)
    }

    match final_key_column {
        KeyColumn::Int8 { values, nulls } => parallel_row_count(values.len(), nulls.len(), "Int8"),
        KeyColumn::Int16 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Int16")
        }
        KeyColumn::Int32 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Int32")
        }
        KeyColumn::Int64 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Int64")
        }
        KeyColumn::Float32 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Float32")
        }
        KeyColumn::Float64 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Float64")
        }
        KeyColumn::Boolean { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Boolean")
        }
        KeyColumn::Utf8 {
            offsets,
            data,
            nulls,
        } => {
            let expected_offsets = nulls.len().checked_add(1).ok_or_else(|| {
                FinalAggregateDomainError::OffsetCountExtent { rows: nulls.len() }
            })?;
            if offsets.len() != expected_offsets
                || offsets.first() != Some(&0)
                || offsets.last() != Some(&data.len())
            {
                return Err(structure(
                    "Utf8 offsets do not match the key data and null bitmap",
                ));
            }
            for window in offsets.windows(2) {
                let start = window[0];
                let end = window[1];
                if start > end || end > data.len() {
                    return Err(structure("Utf8 offsets are not monotonic and in-bounds"));
                }
                std::str::from_utf8(&data[start..end])
                    .map_err(FinalAggregateDomainError::FinalKeyUtf8)?;
            }
            Ok(nulls.len())
        }
        KeyColumn::Date32 { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "Date32")
        }
        KeyColumn::Timestamp { values, nulls, .. } => {
            parallel_row_count(values.len(), nulls.len(), "Timestamp")
        }
        KeyColumn::Decimal128 { values, nulls, .. } => {
            parallel_row_count(values.len(), nulls.len(), "Decimal128")
        }
        KeyColumn::Decimal256 { values, nulls, .. } => {
            parallel_row_count(values.len(), nulls.len(), "Decimal256")
        }
        KeyColumn::LargeIntBinary { values, nulls } => {
            parallel_row_count(values.len(), nulls.len(), "LargeInt")
        }
        KeyColumn::ListUtf8 { values } => Ok(values.len()),
        KeyColumn::ListInt32 { values } => Ok(values.len()),
        KeyColumn::Complex { keys, .. } => Ok(keys.len()),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, Int64Array};
    use arrow::datatypes::DataType;

    use super::{FinalAggregateDomainError, extract_final_aggregate_key};
    use crate::exec::hash_table::key_column::KeyColumn;

    #[test]
    fn final_aggregate_key_transfers_the_validated_arrow_column() {
        let columns = vec![KeyColumn::int64_for_test(vec![7, -2, 7], vec![1, 1, 1])];

        let (data_type, array) = extract_final_aggregate_key(&columns).expect("valid key column");

        assert_eq!(data_type, DataType::Int64);
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array")
                .values(),
            &[7, -2, 7]
        );
    }

    #[test]
    fn final_aggregate_key_rejects_invalid_kernel_shape_before_execution_encoding() {
        assert_eq!(
            extract_final_aggregate_key(&[]),
            Err(FinalAggregateDomainError::MembershipKeyCount { actual: 0 })
        );
        let multiple = vec![
            KeyColumn::int64_for_test(vec![1], vec![1]),
            KeyColumn::int64_for_test(vec![2], vec![1]),
        ];
        assert_eq!(
            extract_final_aggregate_key(&multiple),
            Err(FinalAggregateDomainError::MembershipKeyCount { actual: 2 })
        );
        let malformed = vec![KeyColumn::int64_for_test(vec![1, 2], vec![1])];
        assert!(matches!(
            extract_final_aggregate_key(&malformed),
            Err(FinalAggregateDomainError::FinalKeyStructure(_))
        ));
    }
    #[test]
    fn malformed_retained_key_preserves_original_utf8_error_without_retaining_storage() {
        use crate::exec::expr::agg::{AggregateAllocator, AggregateVec};
        use crate::runtime::mem_tracker::MemTracker;
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        let tracker = MemTracker::new_root("final aggregate key source");
        let weak = std::sync::Arc::downgrade(&tracker);
        let allocator = AggregateAllocator::new(tracker.clone());
        let mut offsets = AggregateVec::new_in(allocator.clone());
        offsets.extend_from_slice(&[0usize, 1]);
        let mut data = AggregateVec::new_in(allocator.clone());
        data.push(0xffu8);
        let mut nulls = AggregateVec::new_in(allocator.clone());
        nulls.push(1u8);
        let columns = vec![KeyColumn::Utf8 {
            offsets,
            data,
            nulls,
        }];
        assert!(tracker.current() > 0);
        let error = extract_final_aggregate_key(&columns).unwrap_err();
        let original = std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<std::str::Utf8Error>()
            .unwrap();
        assert_eq!(original.valid_up_to(), 0);
        assert_eq!(original.error_len(), Some(1));
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Invariant);
        assert_eq!(fact.reason().code(), 409);
        drop(columns);
        assert_eq!(tracker.current(), 0);
        drop(allocator);
        drop(tracker);
        assert!(weak.upgrade().is_none());
        assert_eq!(error.failure_fact(), fact);
    }
}
