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

//! Canonical comparator identity for SQL ordered-bound planning.

use arrow::datatypes::{DataType, TimeUnit};
use sha2::{Digest, Sha256};

use super::contract::{ComparatorDigest, NullOrder, OrderKeyContract, SortDirection};

pub(crate) const COMPARATOR_ALGORITHM_VERSION: u16 = 1;
const COMPARATOR_DOMAIN: &[u8] = b"novarocks.runtime-filter.comparator";
const LARGEINT_BYTE_WIDTH: i32 = 16;
const DECIMAL128_MAX_PRECISION: u8 = 38;
const DECIMAL128_MAX_SCALE: i8 = 38;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ComparatorDigestError {
    EmptyKeys,
    UnsupportedSchema,
    LengthOverflow,
}

/// Returns the v1 canonical comparator identity emitted by the SQL planner.
///
/// The domain tag, version and per-key layout intentionally match the frozen
/// ordered-bound contract. Runtime validates this identity after wire decode.
pub(crate) fn comparator_digest_for_plan(
    keys: &[OrderKeyContract],
) -> Result<ComparatorDigest, ComparatorDigestError> {
    if keys.is_empty() {
        return Err(ComparatorDigestError::EmptyKeys);
    }
    let mut canonical = Vec::with_capacity(64);
    canonical.extend_from_slice(COMPARATOR_DOMAIN);
    canonical.extend_from_slice(&COMPARATOR_ALGORITHM_VERSION.to_be_bytes());
    encode_keys(keys, &mut canonical)?;
    Ok(ComparatorDigest::new(Sha256::digest(canonical).into()))
}

fn encode_keys(
    keys: &[OrderKeyContract],
    output: &mut Vec<u8>,
) -> Result<(), ComparatorDigestError> {
    let count = u32::try_from(keys.len()).map_err(|_| ComparatorDigestError::LengthOverflow)?;
    output.extend_from_slice(&count.to_be_bytes());
    for key in keys {
        encode_schema(&key.data_type, output)?;
        output.push(match key.direction {
            SortDirection::Ascending => 1,
            SortDirection::Descending => 2,
        });
        output.push(match key.null_order {
            NullOrder::First => 1,
            NullOrder::Last => 2,
        });
    }
    Ok(())
}

fn encode_schema(data_type: &DataType, output: &mut Vec<u8>) -> Result<(), ComparatorDigestError> {
    match data_type {
        DataType::Boolean => output.push(1),
        DataType::Int8 => output.push(2),
        DataType::Int16 => output.push(3),
        DataType::Int32 => output.push(4),
        DataType::Int64 => output.push(5),
        DataType::FixedSizeBinary(width) if *width == LARGEINT_BYTE_WIDTH => output.push(6),
        DataType::Utf8 => output.push(9),
        DataType::Date32 => output.push(10),
        DataType::Timestamp(unit, timezone) => {
            output.extend_from_slice(&[
                11,
                match unit {
                    TimeUnit::Second => 1,
                    TimeUnit::Millisecond => 2,
                    TimeUnit::Microsecond => 3,
                    TimeUnit::Nanosecond => 4,
                },
            ]);
            match timezone {
                Some(timezone) => {
                    output.push(1);
                    let len = u32::try_from(timezone.len())
                        .map_err(|_| ComparatorDigestError::LengthOverflow)?;
                    output.extend_from_slice(&len.to_be_bytes());
                    output.extend_from_slice(timezone.as_bytes());
                }
                None => output.push(0),
            }
        }
        DataType::Decimal128(precision, scale)
            if *precision != 0
                && *precision <= DECIMAL128_MAX_PRECISION
                && *scale <= DECIMAL128_MAX_SCALE
                && (*scale <= 0 || (*scale as u8) <= *precision) =>
        {
            output.extend_from_slice(&[12, *precision, *scale as u8]);
        }
        _ => return Err(ComparatorDigestError::UnsupportedSchema),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, TimeUnit};

    use super::*;

    fn key(data_type: DataType, direction: SortDirection, null_order: NullOrder) -> OrderKeyContract {
        OrderKeyContract {
            data_type,
            direction,
            null_order,
        }
    }

    #[test]
    fn digest_is_stable_for_the_same_v1_contract() {
        let keys = vec![
            key(DataType::Int64, SortDirection::Ascending, NullOrder::Last),
            key(
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                SortDirection::Descending,
                NullOrder::First,
            ),
        ];
        assert_eq!(
            comparator_digest_for_plan(&keys),
            comparator_digest_for_plan(&keys)
        );
    }

    #[test]
    fn digest_binds_ordering_and_null_semantics() {
        let baseline = comparator_digest_for_plan(&[key(
            DataType::Int64,
            SortDirection::Ascending,
            NullOrder::Last,
        )])
        .unwrap();
        assert_ne!(
            baseline,
            comparator_digest_for_plan(&[key(
                DataType::Int64,
                SortDirection::Descending,
                NullOrder::Last,
            )])
            .unwrap()
        );
        assert_ne!(
            baseline,
            comparator_digest_for_plan(&[key(
                DataType::Int64,
                SortDirection::Ascending,
                NullOrder::First,
            )])
            .unwrap()
        );
    }

    #[test]
    fn unsupported_or_empty_contracts_fail_without_a_digest() {
        assert_eq!(
            comparator_digest_for_plan(&[]),
            Err(ComparatorDigestError::EmptyKeys)
        );
        assert_eq!(
            comparator_digest_for_plan(&[key(
                DataType::Float64,
                SortDirection::Ascending,
                NullOrder::Last,
            )]),
            Err(ComparatorDigestError::UnsupportedSchema)
        );
    }
}
