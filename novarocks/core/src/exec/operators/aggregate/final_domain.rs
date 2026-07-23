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

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use arrow::array::{
    Array, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray, Float32Array,
    Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, TimeUnit};

use crate::exec::hash_table::key_column::KeyColumn;
use crate::runtime_filter::exec::membership_delta::{
    MembershipDeltaEncoder, MembershipEncodingError, MembershipEncodingOutcome,
};
use crate::runtime_filter::port::value_domain::{
    CanonicalF32, CanonicalF64, MembershipValues, ValueDomainDelta,
};

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
///
/// `max_domain_canonical_bytes` bounds only the canonical `ValueDomainDelta`
/// payload. It excludes `FinalDomainShard` and completion-fence envelope bytes;
/// callers must reserve those bytes before passing the remaining domain budget.
pub(crate) fn extract_final_aggregate_domain(
    final_key_columns: &[KeyColumn],
    max_domain_canonical_bytes: usize,
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
    ensure_distinct_domain_fits(array.as_ref(), &expected_type, max_domain_canonical_bytes)?;

    let outcome =
        MembershipDeltaEncoder::encode(array.as_ref(), &expected_type, max_domain_canonical_bytes)
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

fn ensure_distinct_domain_fits(
    array: &dyn Array,
    data_type: &DataType,
    max_domain_canonical_bytes: usize,
) -> Result<(), FinalAggregateDomainError> {
    let empty = MembershipValues::empty_for_data_type(data_type).ok_or_else(|| {
        FinalAggregateDomainError::MembershipEncoding(MembershipEncodingError::UnsupportedType(
            data_type.clone(),
        ))
    })?;
    let mut canonical_len = ValueDomainDelta::new(empty, false)
        .canonical_encoded_len()
        .map_err(|error| invalid_array(data_type, error.to_string()))?;
    if canonical_len > max_domain_canonical_bytes {
        return Err(FinalAggregateDomainError::ResourceOrSize);
    }

    match data_type {
        DataType::Boolean => {
            let typed = downcast_array::<BooleanArray>(array, data_type)?;
            ensure_distinct_values(
                (0..typed.len()).filter_map(|row| (!typed.is_null(row)).then(|| typed.value(row))),
                |_| Ok(1),
                &mut canonical_len,
                max_domain_canonical_bytes,
            )?;
        }
        DataType::Int8 => {
            let typed = downcast_array::<Int8Array>(array, data_type)?;
            ensure_fixed_width_distinct(typed, 1, &mut canonical_len, max_domain_canonical_bytes)?;
        }
        DataType::Int16 => {
            let typed = downcast_array::<Int16Array>(array, data_type)?;
            ensure_fixed_width_distinct(typed, 2, &mut canonical_len, max_domain_canonical_bytes)?;
        }
        DataType::Int32 => {
            let typed = downcast_array::<Int32Array>(array, data_type)?;
            ensure_fixed_width_distinct(typed, 4, &mut canonical_len, max_domain_canonical_bytes)?;
        }
        DataType::Int64 => {
            let typed = downcast_array::<Int64Array>(array, data_type)?;
            ensure_fixed_width_distinct(typed, 8, &mut canonical_len, max_domain_canonical_bytes)?;
        }
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            let typed = downcast_array::<FixedSizeBinaryArray>(array, data_type)?;
            let mut distinct = BTreeSet::new();
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    continue;
                }
                let value = typed
                    .value(row)
                    .try_into()
                    .map(i128::from_be_bytes)
                    .map_err(|_| {
                        invalid_array(data_type, "LargeInt scalar is not 16 bytes".to_string())
                    })?;
                reserve_distinct(
                    &mut distinct,
                    value,
                    16,
                    &mut canonical_len,
                    max_domain_canonical_bytes,
                )?;
            }
        }
        DataType::Float32 => {
            let typed = downcast_array::<Float32Array>(array, data_type)?;
            ensure_distinct_values(
                (0..typed.len()).filter_map(|row| {
                    (!typed.is_null(row)).then(|| CanonicalF32::new(typed.value(row)))
                }),
                |_| Ok(4),
                &mut canonical_len,
                max_domain_canonical_bytes,
            )?;
        }
        DataType::Float64 => {
            let typed = downcast_array::<Float64Array>(array, data_type)?;
            ensure_distinct_values(
                (0..typed.len()).filter_map(|row| {
                    (!typed.is_null(row)).then(|| CanonicalF64::new(typed.value(row)))
                }),
                |_| Ok(8),
                &mut canonical_len,
                max_domain_canonical_bytes,
            )?;
        }
        DataType::Utf8 => {
            let typed = downcast_array::<StringArray>(array, data_type)?;
            ensure_distinct_values(
                (0..typed.len()).filter_map(|row| (!typed.is_null(row)).then(|| typed.value(row))),
                |value| {
                    8usize
                        .checked_add(value.len())
                        .ok_or(FinalAggregateDomainError::ResourceOrSize)
                },
                &mut canonical_len,
                max_domain_canonical_bytes,
            )?;
        }
        DataType::Date32 => {
            let typed = downcast_array::<Date32Array>(array, data_type)?;
            ensure_fixed_width_distinct(typed, 4, &mut canonical_len, max_domain_canonical_bytes)?;
        }
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => ensure_timestamp_distinct::<TimestampSecondArray>(
                array,
                data_type,
                &mut canonical_len,
                max_domain_canonical_bytes,
            )?,
            TimeUnit::Millisecond => ensure_timestamp_distinct::<TimestampMillisecondArray>(
                array,
                data_type,
                &mut canonical_len,
                max_domain_canonical_bytes,
            )?,
            TimeUnit::Microsecond => ensure_timestamp_distinct::<TimestampMicrosecondArray>(
                array,
                data_type,
                &mut canonical_len,
                max_domain_canonical_bytes,
            )?,
            TimeUnit::Nanosecond => ensure_timestamp_distinct::<TimestampNanosecondArray>(
                array,
                data_type,
                &mut canonical_len,
                max_domain_canonical_bytes,
            )?,
        },
        DataType::Decimal128(precision, scale) => {
            let typed = downcast_array::<Decimal128Array>(array, data_type)?;
            let precision = *precision;
            let scale = *scale;
            let mut distinct = BTreeSet::new();
            for row in 0..typed.len() {
                if typed.is_null(row) {
                    continue;
                }
                let value = typed.value(row);
                MembershipValues::validate_decimal128_scalar(precision, scale, value).map_err(
                    |error| {
                        FinalAggregateDomainError::MembershipEncoding(
                            MembershipEncodingError::InvalidDecimal {
                                precision,
                                scale,
                                detail: error.to_string(),
                            },
                        )
                    },
                )?;
                reserve_distinct(
                    &mut distinct,
                    value,
                    16,
                    &mut canonical_len,
                    max_domain_canonical_bytes,
                )?;
            }
        }
        other => {
            return Err(FinalAggregateDomainError::MembershipEncoding(
                MembershipEncodingError::UnsupportedType(other.clone()),
            ));
        }
    }
    Ok(())
}

fn invalid_array(data_type: &DataType, detail: String) -> FinalAggregateDomainError {
    FinalAggregateDomainError::MembershipEncoding(MembershipEncodingError::InvalidArray {
        data_type: data_type.clone(),
        detail,
    })
}

fn downcast_array<'a, T: Array + 'static>(
    array: &'a dyn Array,
    data_type: &DataType,
) -> Result<&'a T, FinalAggregateDomainError> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        invalid_array(
            data_type,
            "Arrow physical array does not match its declared data type".to_string(),
        )
    })
}

fn ensure_distinct_values<T, I, F>(
    values: I,
    mut scalar_bytes: F,
    canonical_len: &mut usize,
    max_domain_canonical_bytes: usize,
) -> Result<(), FinalAggregateDomainError>
where
    T: Ord,
    I: IntoIterator<Item = T>,
    F: FnMut(&T) -> Result<usize, FinalAggregateDomainError>,
{
    let mut distinct = BTreeSet::new();
    for value in values {
        let value_bytes = scalar_bytes(&value)?;
        reserve_distinct(
            &mut distinct,
            value,
            value_bytes,
            canonical_len,
            max_domain_canonical_bytes,
        )?;
    }
    Ok(())
}

fn reserve_distinct<T: Ord>(
    distinct: &mut BTreeSet<T>,
    value: T,
    scalar_bytes: usize,
    canonical_len: &mut usize,
    max_domain_canonical_bytes: usize,
) -> Result<(), FinalAggregateDomainError> {
    if distinct.contains(&value) {
        return Ok(());
    }
    let candidate_len = canonical_len
        .checked_add(scalar_bytes)
        .ok_or(FinalAggregateDomainError::ResourceOrSize)?;
    if candidate_len > max_domain_canonical_bytes {
        return Err(FinalAggregateDomainError::ResourceOrSize);
    }
    distinct.insert(value);
    *canonical_len = candidate_len;
    Ok(())
}

fn ensure_fixed_width_distinct<A>(
    array: &A,
    scalar_bytes: usize,
    canonical_len: &mut usize,
    max_domain_canonical_bytes: usize,
) -> Result<(), FinalAggregateDomainError>
where
    A: Array + FinalDomainValueAt,
{
    ensure_distinct_values(
        (0..array.len()).filter_map(|row| (!array.is_null(row)).then(|| array.value_at(row))),
        |_| Ok(scalar_bytes),
        canonical_len,
        max_domain_canonical_bytes,
    )
}

fn ensure_timestamp_distinct<A>(
    array: &dyn Array,
    data_type: &DataType,
    canonical_len: &mut usize,
    max_domain_canonical_bytes: usize,
) -> Result<(), FinalAggregateDomainError>
where
    A: Array + FinalDomainValueAt<Value = i64> + 'static,
{
    ensure_fixed_width_distinct(
        downcast_array::<A>(array, data_type)?,
        8,
        canonical_len,
        max_domain_canonical_bytes,
    )
}

trait FinalDomainValueAt {
    type Value: Ord;

    fn value_at(&self, row: usize) -> Self::Value;
}

macro_rules! final_domain_value_at {
    ($array:ty, $value:ty) => {
        impl FinalDomainValueAt for $array {
            type Value = $value;

            fn value_at(&self, row: usize) -> Self::Value {
                self.value(row)
            }
        }
    };
}

final_domain_value_at!(Int8Array, i8);
final_domain_value_at!(Int16Array, i16);
final_domain_value_at!(Int32Array, i32);
final_domain_value_at!(Int64Array, i64);
final_domain_value_at!(Date32Array, i32);
final_domain_value_at!(TimestampSecondArray, i64);
final_domain_value_at!(TimestampMillisecondArray, i64);
final_domain_value_at!(TimestampMicrosecondArray, i64);
final_domain_value_at!(TimestampNanosecondArray, i64);

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

    const TEST_MAX_DOMAIN_CANONICAL_BYTES: usize = 64 * 1024;

    fn int64_column(values: Vec<i64>, nulls: Vec<u8>) -> KeyColumn {
        KeyColumn::Int64 { values, nulls }
    }

    fn extract(
        final_key_columns: &[KeyColumn],
    ) -> Result<ValueDomainDelta, FinalAggregateDomainError> {
        extract_final_aggregate_domain(final_key_columns, TEST_MAX_DOMAIN_CANONICAL_BYTES)
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
    fn duplicate_keys_fit_exact_domain_budget() {
        let baseline = ValueDomainDelta::new(MembershipValues::int64([]), false)
            .canonical_encoded_len()
            .expect("empty domain length");
        let final_key_columns = vec![int64_column(vec![1, 1], vec![1, 1])];

        let domain = extract_final_aggregate_domain(&final_key_columns, baseline + 8)
            .expect("duplicate rows must consume one canonical scalar frame");

        assert_eq!(
            domain,
            ValueDomainDelta::new(MembershipValues::int64([1]), false)
        );
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
