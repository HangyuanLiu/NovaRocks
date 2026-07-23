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

use arrow::array::{Array, StringArray};
use arrow::datatypes::DataType;

use crate::exec::hash_table::key_column::KeyColumn;
use crate::runtime_filter::exec::membership_delta::{
    MembershipDeltaEncoder, MembershipEncodingError, MembershipEncodingOutcome,
};
use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FinalAggregateDomainError {
    MembershipKeyCount {
        actual: usize,
    },
    FinalKeyMaterialization(String),
    FinalKeyStructure(String),
    FinalKeyTypeMismatch {
        expected: DataType,
        actual: DataType,
    },
    FinalKeyRowCountMismatch {
        expected: usize,
        actual: usize,
    },
    MembershipEncoding(MembershipEncodingError),
    ResourceOrSize,
    SplitDomain {
        shards: usize,
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
            Self::FinalKeyTypeMismatch { expected, actual } => write!(
                formatter,
                "final aggregate key materialization type mismatch: expected {expected:?}, got {actual:?}"
            ),
            Self::FinalKeyRowCountMismatch { expected, actual } => write!(
                formatter,
                "final aggregate key materialization row count mismatch: expected {expected}, got {actual}"
            ),
            Self::MembershipEncoding(error) => error.fmt(formatter),
            Self::ResourceOrSize => write!(
                formatter,
                "final aggregate domain cannot retain the exact membership domain within resources"
            ),
            Self::SplitDomain { shards } => write!(
                formatter,
                "final aggregate domain unexpectedly split into {shards} membership shards"
            ),
        }
    }
}

impl Error for FinalAggregateDomainError {}

/// Encodes the one install-frozen membership key from finalized aggregate state.
///
/// Callers must invoke this while the final aggregate's `KeyTable` still owns its
/// key columns. The returned domain is independent of that state and retains
/// null membership explicitly for the `NullSafeEqual` contract.
/// `max_contribution_bytes` must be the installed membership contribution limit.
pub(crate) fn extract_final_aggregate_domain(
    final_key_columns: &[KeyColumn],
    max_contribution_bytes: usize,
) -> Result<ValueDomainDelta, FinalAggregateDomainError> {
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
            expected: expected_type,
            actual: array.data_type().clone(),
        });
    }
    ensure_contribution_fits(array.as_ref(), &expected_type, max_contribution_bytes)?;

    let outcome =
        MembershipDeltaEncoder::encode(array.as_ref(), &expected_type, max_contribution_bytes)
            .map_err(FinalAggregateDomainError::MembershipEncoding)?;
    let MembershipEncodingOutcome::Deltas(mut deltas) = outcome else {
        return Err(FinalAggregateDomainError::ResourceOrSize);
    };
    if deltas.len() != 1 {
        return Err(FinalAggregateDomainError::SplitDomain {
            shards: deltas.len(),
        });
    }
    Ok(deltas.pop().expect("one final aggregate domain delta"))
}

fn ensure_contribution_fits(
    array: &dyn Array,
    data_type: &DataType,
    max_contribution_bytes: usize,
) -> Result<(), FinalAggregateDomainError> {
    let empty = MembershipValues::empty_for_data_type(data_type).ok_or_else(|| {
        FinalAggregateDomainError::MembershipEncoding(MembershipEncodingError::UnsupportedType(
            data_type.clone(),
        ))
    })?;
    let mut upper_bound = ValueDomainDelta::new(empty, false)
        .canonical_encoded_len()
        .map_err(|error| {
            FinalAggregateDomainError::MembershipEncoding(MembershipEncodingError::InvalidArray {
                data_type: data_type.clone(),
                detail: error.to_string(),
            })
        })?;
    let non_null_rows = array.len().checked_sub(array.null_count()).ok_or_else(|| {
        FinalAggregateDomainError::MembershipEncoding(MembershipEncodingError::InvalidArray {
            data_type: data_type.clone(),
            detail: "Arrow null count exceeds array length".to_string(),
        })
    })?;

    match data_type {
        DataType::Boolean | DataType::Int8 => {
            add_fixed_contribution(&mut upper_bound, non_null_rows, 1)?
        }
        DataType::Int16 => add_fixed_contribution(&mut upper_bound, non_null_rows, 2)?,
        DataType::Int32 | DataType::Float32 | DataType::Date32 => {
            add_fixed_contribution(&mut upper_bound, non_null_rows, 4)?
        }
        DataType::Int64 | DataType::Float64 | DataType::Timestamp(_, _) => {
            add_fixed_contribution(&mut upper_bound, non_null_rows, 8)?
        }
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            add_fixed_contribution(&mut upper_bound, non_null_rows, 16)?
        }
        DataType::Decimal128(_, _) => add_fixed_contribution(&mut upper_bound, non_null_rows, 16)?,
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    FinalAggregateDomainError::MembershipEncoding(
                        MembershipEncodingError::InvalidArray {
                            data_type: data_type.clone(),
                            detail: "Arrow physical array does not match its declared data type"
                                .to_string(),
                        },
                    )
                })?;
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    continue;
                }
                let scalar_bytes = 8usize
                    .checked_add(typed.value(row).len())
                    .ok_or(FinalAggregateDomainError::ResourceOrSize)?;
                upper_bound = upper_bound
                    .checked_add(scalar_bytes)
                    .ok_or(FinalAggregateDomainError::ResourceOrSize)?;
            }
        }
        other => {
            return Err(FinalAggregateDomainError::MembershipEncoding(
                MembershipEncodingError::UnsupportedType(other.clone()),
            ));
        }
    }
    if upper_bound > max_contribution_bytes {
        return Err(FinalAggregateDomainError::ResourceOrSize);
    }
    Ok(())
}

fn add_fixed_contribution(
    upper_bound: &mut usize,
    non_null_rows: usize,
    scalar_bytes: usize,
) -> Result<(), FinalAggregateDomainError> {
    let values_bytes = non_null_rows
        .checked_mul(scalar_bytes)
        .ok_or(FinalAggregateDomainError::ResourceOrSize)?;
    *upper_bound = upper_bound
        .checked_add(values_bytes)
        .ok_or(FinalAggregateDomainError::ResourceOrSize)?;
    Ok(())
}

fn final_key_row_count(final_key_column: &KeyColumn) -> Result<usize, FinalAggregateDomainError> {
    fn parallel_row_count(
        values: usize,
        nulls: usize,
        key_type: &str,
    ) -> Result<usize, FinalAggregateDomainError> {
        if values != nulls {
            return Err(FinalAggregateDomainError::FinalKeyStructure(format!(
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
                FinalAggregateDomainError::FinalKeyStructure(
                    "Utf8 null bitmap length overflows offset count".to_string(),
                )
            })?;
            if offsets.len() != expected_offsets {
                return Err(FinalAggregateDomainError::FinalKeyStructure(format!(
                    "Utf8 offsets/null bitmap length mismatch: offsets={} nulls={}",
                    offsets.len(),
                    nulls.len()
                )));
            }
            if offsets.first() != Some(&0) {
                return Err(FinalAggregateDomainError::FinalKeyStructure(
                    "Utf8 offsets must start at zero".to_string(),
                ));
            }
            if offsets.last() != Some(&data.len()) {
                return Err(FinalAggregateDomainError::FinalKeyStructure(format!(
                    "Utf8 final offset/data length mismatch: final_offset={} data_len={}",
                    offsets.last().copied().unwrap_or_default(),
                    data.len()
                )));
            }
            for window in offsets.windows(2) {
                let start = window[0];
                let end = window[1];
                if start > end || end > data.len() {
                    return Err(FinalAggregateDomainError::FinalKeyStructure(format!(
                        "Utf8 offsets are not monotonic and in-bounds: start={start} end={end} data_len={}",
                        data.len()
                    )));
                }
                std::str::from_utf8(&data[start..end]).map_err(|error| {
                    FinalAggregateDomainError::FinalKeyStructure(format!(
                        "Utf8 key bytes are invalid: {error}"
                    ))
                })?;
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
        KeyColumn::Complex {
            keys,
            nulls,
            values,
            ..
        } => {
            let rows = parallel_row_count(keys.len(), nulls.len(), "Complex")?;
            parallel_row_count(rows, values.len(), "Complex")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, TimeUnit};

    use super::{FinalAggregateDomainError, extract_final_aggregate_domain};
    use crate::exec::hash_table::key_column::KeyColumn;
    use crate::runtime_filter::exec::membership_delta::MembershipEncodingError;
    use crate::runtime_filter::port::value_domain::{MembershipValues, ValueDomainDelta};

    const TEST_MAX_CONTRIBUTION_BYTES: usize = 64 * 1024;

    fn int64_column(values: Vec<i64>, nulls: Vec<u8>) -> KeyColumn {
        KeyColumn::Int64 { values, nulls }
    }

    fn extract(
        final_key_columns: &[KeyColumn],
    ) -> Result<ValueDomainDelta, FinalAggregateDomainError> {
        extract_final_aggregate_domain(final_key_columns, TEST_MAX_CONTRIBUTION_BYTES)
    }

    #[test]
    fn final_aggregate_domain_uses_final_key_columns() {
        let final_key_columns = vec![int64_column(vec![7, 2, 7], vec![1, 1, 1])];

        let domain = extract(&final_key_columns).expect("final key column must encode");

        assert_eq!(
            domain,
            ValueDomainDelta::new(MembershipValues::int64([2, 7]), false)
        );
    }

    #[test]
    fn domain_is_captured_before_group_state_drop() {
        let final_key_columns = vec![int64_column(vec![41], vec![1])];

        let domain = extract(&final_key_columns)
            .expect("final key column must encode before state is released");
        drop(final_key_columns);

        assert_eq!(
            domain,
            ValueDomainDelta::new(MembershipValues::int64([41]), false)
        );
    }

    #[test]
    fn duplicate_keys_are_deduplicated() {
        let final_key_columns = vec![int64_column(vec![9, 9, 9, -2], vec![1, 1, 1, 1])];

        let domain = extract(&final_key_columns).expect("duplicate final keys must encode");

        assert_eq!(
            domain,
            ValueDomainDelta::new(MembershipValues::int64([-2, 9]), false)
        );
    }

    #[test]
    fn null_safe_key_preserves_explicit_null() {
        let final_key_columns = vec![int64_column(vec![5, 0, 5], vec![1, 0, 1])];

        let domain = extract(&final_key_columns).expect("nullable final key must encode");

        assert_eq!(
            domain,
            ValueDomainDelta::new(MembershipValues::int64([5]), true)
        );
    }

    #[test]
    fn unsupported_or_multi_key_contract_fails_fast() {
        let zero_key_error = extract(&[]).expect_err("zero membership keys must fail");
        assert_eq!(
            zero_key_error,
            FinalAggregateDomainError::MembershipKeyCount { actual: 0 }
        );

        let unsupported = vec![KeyColumn::Decimal256 {
            values: vec![],
            nulls: vec![],
            precision: 10,
            scale: 2,
        }];
        let unsupported_error = extract(&unsupported).expect_err("Decimal256 must be unsupported");
        assert_eq!(
            unsupported_error,
            FinalAggregateDomainError::MembershipEncoding(
                MembershipEncodingError::UnsupportedType(DataType::Decimal256(10, 2))
            )
        );

        let multi_key = vec![
            int64_column(vec![1], vec![1]),
            int64_column(vec![2], vec![1]),
        ];
        let multi_key_error = extract(&multi_key).expect_err("multiple membership keys must fail");
        assert_eq!(
            multi_key_error,
            FinalAggregateDomainError::MembershipKeyCount { actual: 2 }
        );

        let malformed = vec![int64_column(vec![1, 2], vec![1])];
        assert!(matches!(
            extract(&malformed),
            Err(FinalAggregateDomainError::FinalKeyStructure(_))
        ));

        let malformed_null_only_utf8 = vec![KeyColumn::Utf8 {
            offsets: vec![99, 0],
            data: vec![],
            nulls: vec![0],
        }];
        assert!(matches!(
            extract(&malformed_null_only_utf8),
            Err(FinalAggregateDomainError::FinalKeyStructure(_))
        ));
    }

    #[test]
    fn contribution_limit_rejects_domain_before_membership_collection() {
        let baseline = ValueDomainDelta::new(MembershipValues::int64([]), false)
            .canonical_encoded_len()
            .expect("empty domain length");
        let final_key_columns = vec![int64_column(vec![1, 2], vec![1, 1])];

        let error = extract_final_aggregate_domain(&final_key_columns, baseline + 8)
            .expect_err("two exact Int64 keys exceed one-key contribution bound");

        assert_eq!(error, FinalAggregateDomainError::ResourceOrSize);
    }

    #[test]
    fn supported_membership_types_preserve_exact_domains() {
        let timestamp_timezone: Arc<str> = Arc::from("Asia/Shanghai");
        let cases = vec![
            (
                KeyColumn::Boolean {
                    values: vec![1, 0],
                    nulls: vec![1, 1],
                },
                ValueDomainDelta::new(MembershipValues::boolean([false, true]), false),
            ),
            (
                KeyColumn::Int8 {
                    values: vec![-1, 2],
                    nulls: vec![1, 1],
                },
                ValueDomainDelta::new(MembershipValues::int8([-1, 2]), false),
            ),
            (
                KeyColumn::Int16 {
                    values: vec![-2, 3],
                    nulls: vec![1, 1],
                },
                ValueDomainDelta::new(MembershipValues::int16([-2, 3]), false),
            ),
            (
                KeyColumn::Int32 {
                    values: vec![-3, 4],
                    nulls: vec![1, 1],
                },
                ValueDomainDelta::new(MembershipValues::int32([-3, 4]), false),
            ),
            (
                int64_column(vec![-4, 5], vec![1, 1]),
                ValueDomainDelta::new(MembershipValues::int64([-4, 5]), false),
            ),
            (
                KeyColumn::LargeIntBinary {
                    values: vec![i128::MIN + 1, i128::MAX],
                    nulls: vec![1, 1],
                },
                ValueDomainDelta::new(
                    MembershipValues::large_int([i128::MIN + 1, i128::MAX]),
                    false,
                ),
            ),
            (
                KeyColumn::Float32 {
                    values: vec![f32::NAN, -0.0, 0.0, 1.5],
                    nulls: vec![1, 1, 1, 1],
                },
                ValueDomainDelta::new(MembershipValues::float32([f32::NAN, 0.0, 1.5]), false),
            ),
            (
                KeyColumn::Float64 {
                    values: vec![f64::NAN, -0.0, 0.0, 2.5],
                    nulls: vec![1, 1, 1, 1],
                },
                ValueDomainDelta::new(MembershipValues::float64([f64::NAN, 0.0, 2.5]), false),
            ),
            (
                KeyColumn::Utf8 {
                    offsets: vec![0, 5, 9],
                    data: b"alphabeta".to_vec(),
                    nulls: vec![1, 1],
                },
                ValueDomainDelta::new(MembershipValues::utf8(["alpha", "beta"]), false),
            ),
            (
                KeyColumn::Date32 {
                    values: vec![-7, 8],
                    nulls: vec![1, 1],
                },
                ValueDomainDelta::new(MembershipValues::date32([-7, 8]), false),
            ),
            (
                KeyColumn::Timestamp {
                    values: vec![-10, 20],
                    nulls: vec![1, 1],
                    unit: TimeUnit::Microsecond,
                    tz: Some("Asia/Shanghai".to_string()),
                },
                ValueDomainDelta::new(
                    MembershipValues::timestamp(
                        TimeUnit::Microsecond,
                        Some(timestamp_timezone),
                        [-10, 20],
                    ),
                    false,
                ),
            ),
            (
                KeyColumn::Decimal128 {
                    values: vec![-5678, 1234],
                    nulls: vec![1, 1],
                    precision: 18,
                    scale: 2,
                },
                ValueDomainDelta::new(
                    MembershipValues::decimal128(18, 2, [-5678, 1234])
                        .expect("valid Decimal128 values"),
                    false,
                ),
            ),
        ];

        for (final_key_column, expected) in cases {
            assert_eq!(
                extract(&[final_key_column]).expect("supported final key type must encode"),
                expected
            );
        }
    }
}
