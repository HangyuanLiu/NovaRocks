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

/// The comparator this plan's ordered runtime filter is compared under.
///
/// Both the wire contract and the facts an attempt deploys from name the
/// comparator by digest, and they must name the same one. Deriving it once
/// here is what makes that true by construction rather than by review.
pub fn runtime_filter_comparator_digest(
    filter: &novarocks_physical_plan::RuntimeFilter,
) -> Result<Option<[u8; 32]>, String> {
    let novarocks_physical_plan::RuntimeFilterDomain::Ordered { key, .. } = &filter.domain else {
        return Ok(None);
    };
    let (comparator_digest, _) =
        runtime_filter_order_digests(&key.ty.data_type(), key.direction, key.null_ordering)?;
    Ok(Some(comparator_digest))
}

fn runtime_filter_order_digests(
    data_type: &arrow::datatypes::DataType,
    direction: novarocks_physical_plan::SortDirection,
    null_ordering: novarocks_physical_plan::NullOrdering,
) -> Result<([u8; 32], [u8; 32]), String> {
    runtime_filter_order_digests_observed(data_type, direction, null_ordering, || Ok(()))
}

/// Feed the same canonical bytes into both fixed-size digest states. No
/// timezone-dependent temporary Vec or assumed fixed key size is allocated.
pub(crate) fn runtime_filter_order_digests_observed<E: From<String>>(
    data_type: &arrow::datatypes::DataType,
    direction: novarocks_physical_plan::SortDirection,
    null_ordering: novarocks_physical_plan::NullOrdering,
    observe: impl FnMut() -> Result<(), E>,
) -> Result<([u8; 32], [u8; 32]), E> {
    use novarocks_local_program::{FilterNullOrder, FilterSortDirection};
    novarocks_local_program::single_key_ordered_filter_digests_observed(
        match direction {
            novarocks_physical_plan::SortDirection::Ascending => FilterSortDirection::Ascending,
            novarocks_physical_plan::SortDirection::Descending => FilterSortDirection::Descending,
        },
        match null_ordering {
            novarocks_physical_plan::NullOrdering::First => FilterNullOrder::First,
            novarocks_physical_plan::NullOrdering::Last => FilterNullOrder::Last,
        },
        |emit| visit_runtime_filter_key_type(data_type, &mut |bytes| emit(bytes)),
        observe,
    )
}

pub(crate) fn visit_runtime_filter_key_type<E: From<String>>(
    data_type: &arrow::datatypes::DataType,
    emit: &mut impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    use arrow::datatypes::{DataType, TimeUnit};
    match data_type {
        DataType::Boolean => emit(&[1])?,
        DataType::Int8 => emit(&[2])?,
        DataType::Int16 => emit(&[3])?,
        DataType::Int32 => emit(&[4])?,
        DataType::Int64 => emit(&[5])?,
        DataType::FixedSizeBinary(16) => emit(&[6])?,
        DataType::Utf8 => emit(&[9])?,
        DataType::Date32 => emit(&[10])?,
        DataType::Timestamp(unit, timezone) => {
            emit(&[
                11,
                match unit {
                    TimeUnit::Second => 1,
                    TimeUnit::Millisecond => 2,
                    TimeUnit::Microsecond => 3,
                    TimeUnit::Nanosecond => 4,
                },
            ])?;
            if let Some(timezone) = timezone {
                let length = u32::try_from(timezone.len()).map_err(|_| {
                    E::from("runtime-filter timezone length exceeds u32".to_string())
                })?;
                emit(&[1])?;
                emit(&length.to_be_bytes())?;
                emit(timezone.as_bytes())?;
            } else {
                emit(&[0])?;
            }
        }
        DataType::Decimal128(precision, scale)
            if *precision != 0
                && *precision <= 38
                && *scale <= 38
                && (*scale <= 0 || (*scale as u8) <= *precision) =>
        {
            emit(&[12, *precision, *scale as u8])?;
        }
        _ => {
            return Err(E::from(
                "runtime-filter ordered key type is unsupported".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    #[test]
    fn ordered_runtime_filter_digest_matches_canonical_int64_golden() {
        let (comparator, order) = runtime_filter_order_digests(
            &DataType::Int64,
            novarocks_physical_plan::SortDirection::Ascending,
            novarocks_physical_plan::NullOrdering::Last,
        )
        .unwrap();
        assert_eq!(
            hex::encode(comparator),
            "ef75ae29c8dd2bce7ddb857cfff1d89b8afa8c30950bd3afb669ffb52d8755d8"
        );
        assert_eq!(
            hex::encode(order),
            "ee277431b71d9a36652809581e72c910c527d050916ec4c96ac2f4b4636549bb"
        );
        assert!(
            runtime_filter_order_digests(
                &DataType::Float64,
                novarocks_physical_plan::SortDirection::Ascending,
                novarocks_physical_plan::NullOrdering::Last,
            )
            .is_err()
        );
    }

    #[test]
    fn ordered_runtime_filter_digest_streams_long_timezone_bytes_and_original_refusals() {
        #[derive(Debug, Eq, PartialEq)]
        enum Failure {
            Invalid(String),
            Control(novarocks_type_contract::CompileControlError),
        }
        impl From<String> for Failure {
            fn from(value: String) -> Self {
                Self::Invalid(value)
            }
        }
        use novarocks_type_contract::CompileControlError;
        let timezone = "UTC".repeat(80);
        let ty = DataType::Timestamp(
            arrow::datatypes::TimeUnit::Nanosecond,
            Some(timezone.clone().into()),
        );
        let direction = novarocks_physical_plan::SortDirection::Descending;
        let nulls = novarocks_physical_plan::NullOrdering::First;
        let mut count = 0;
        let baseline = runtime_filter_order_digests_observed(&ty, direction, nulls, || {
            count += 1;
            Ok::<_, Failure>(())
        })
        .unwrap();
        assert_eq!(
            baseline,
            runtime_filter_order_digests(&ty, direction, nulls).unwrap()
        );
        assert_eq!(count, 13 + timezone.len());
        for reason in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for stop in 0..count {
                let mut seen = 0;
                let result = runtime_filter_order_digests_observed(&ty, direction, nulls, || {
                    let ordinal = seen;
                    seen += 1;
                    if ordinal == stop {
                        Err(Failure::Control(reason.clone()))
                    } else {
                        Ok(())
                    }
                });
                assert_eq!(result, Err(Failure::Control(reason.clone())));
                assert_eq!(seen, stop + 1);
            }
        }
    }
}
