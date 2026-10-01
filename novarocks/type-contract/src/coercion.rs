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

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};

fn is_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn wider_decimal_type(
    left_precision: u8,
    left_scale: i8,
    left_is_256: bool,
    right_precision: u8,
    right_scale: i8,
    right_is_256: bool,
) -> DataType {
    let scale = left_scale.max(right_scale);
    let left_int_digits = i16::from(left_precision) - i16::from(left_scale);
    let right_int_digits = i16::from(right_precision) - i16::from(right_scale);
    let precision =
        (left_int_digits.max(right_int_digits).max(0) + i16::from(scale)).clamp(1, 76) as u8;

    if left_is_256 || right_is_256 || precision > 38 {
        DataType::Decimal256(precision, scale)
    } else {
        DataType::Decimal128(precision, scale)
    }
}

/// Existing carrier-only common type for comparisons, CASE and UNION.
///
/// This preserves the legacy Arrow carrier rules, including their fallback
/// and field reconstruction behavior. It does not establish or merge complete
/// logical identities. Callers own structural admission for recursive types.
pub fn wider_type(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        return a.clone();
    }
    match (a, b) {
        (DataType::Null, other) | (other, DataType::Null) => other.clone(),
        (l, r)
            if (crate::is_largeint_data_type(l) && is_integer(r))
                || (is_integer(l) && crate::is_largeint_data_type(r)) =>
        {
            DataType::FixedSizeBinary(crate::LARGEINT_BYTE_WIDTH)
        }
        (DataType::List(left_field), DataType::List(right_field)) => {
            DataType::List(Arc::new(Field::new(
                left_field.name(),
                wider_type(left_field.data_type(), right_field.data_type()),
                left_field.is_nullable() || right_field.is_nullable(),
            )))
        }
        (DataType::Map(left_entries, _), DataType::Map(right_entries, _)) => {
            wider_map_type(left_entries, right_entries)
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields))
            if left_fields.len() == right_fields.len() =>
        {
            if let Some(fields) = wider_struct_fields_by_name(left_fields, right_fields) {
                return DataType::Struct(fields);
            }
            DataType::Struct(Fields::from(
                left_fields
                    .iter()
                    .zip(right_fields.iter())
                    .map(|(left_field, right_field)| {
                        Arc::new(Field::new(
                            left_field.name(),
                            wider_type(left_field.data_type(), right_field.data_type()),
                            left_field.is_nullable() || right_field.is_nullable(),
                        ))
                    })
                    .collect::<Vec<_>>(),
            ))
        }
        // VARCHAR wins before DECIMAL, matching StarRocks TypeManager:
        // getAssignmentCompatibleType handles string pairs before decimal
        // pairs, and ARRAY/MAP/STRUCT common types recurse through this rule.
        (DataType::Utf8, _) | (_, DataType::Utf8) => DataType::Utf8,
        (DataType::LargeUtf8, _) | (_, DataType::LargeUtf8) => DataType::Utf8,
        // Decimal + Decimal -> wider Decimal. Promote to Decimal256 when the
        // common precision exceeds Decimal128 or either side is already wide.
        (DataType::Decimal128(p1, s1), DataType::Decimal128(p2, s2)) => {
            wider_decimal_type(*p1, *s1, false, *p2, *s2, false)
        }
        (DataType::Decimal128(p1, s1), DataType::Decimal256(p2, s2)) => {
            wider_decimal_type(*p1, *s1, false, *p2, *s2, true)
        }
        (DataType::Decimal256(p1, s1), DataType::Decimal128(p2, s2)) => {
            wider_decimal_type(*p1, *s1, true, *p2, *s2, false)
        }
        (DataType::Decimal256(p1, s1), DataType::Decimal256(p2, s2)) => {
            wider_decimal_type(*p1, *s1, true, *p2, *s2, true)
        }
        // Decimal + Integer -> Decimal. Keep the existing decimal metadata;
        // integer literal narrowing happens before this common-type step.
        (
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
        )
        | (
            DataType::Int64 | DataType::Int32 | DataType::Int16 | DataType::Int8,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
        ) => match (a, b) {
            (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => {
                DataType::Decimal128(*p, *s)
            }
            (DataType::Decimal256(p, s), _) | (_, DataType::Decimal256(p, s)) => {
                DataType::Decimal256(*p, *s)
            }
            _ => unreachable!(),
        },
        // Decimal + Float -> Float64 (StarRocks FE: promote to Double).
        (
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
            DataType::Float64 | DataType::Float32,
        )
        | (
            DataType::Float64 | DataType::Float32,
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _),
        ) => DataType::Float64,
        // Decimal + other -> Decimal.
        (DataType::Decimal128(p, s), _) | (_, DataType::Decimal128(p, s)) => {
            DataType::Decimal128(*p, *s)
        }
        (DataType::Decimal256(p, s), _) | (_, DataType::Decimal256(p, s)) => {
            DataType::Decimal256(*p, *s)
        }
        // DATE + DATETIME -> DATETIME (StarRocks: only DATETIME signatures exist
        // for comparison/greatest/least/coalesce with mixed date+datetime input).
        (DataType::Timestamp(u, tz), DataType::Date32)
        | (DataType::Date32, DataType::Timestamp(u, tz)) => DataType::Timestamp(*u, tz.clone()),
        (DataType::Float64, _) | (_, DataType::Float64) => DataType::Float64,
        (DataType::Float32, _) | (_, DataType::Float32) => DataType::Float64,
        (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
        (DataType::Int32, _) | (_, DataType::Int32) => DataType::Int64,
        (DataType::Int16, _) | (_, DataType::Int16) => DataType::Int16,
        _ => a.clone(),
    }
}

fn wider_struct_fields_by_name(left_fields: &Fields, right_fields: &Fields) -> Option<Fields> {
    let right_by_name = right_fields
        .iter()
        .map(|field| (field.name().as_str(), field))
        .collect::<std::collections::HashMap<_, _>>();
    if left_fields
        .iter()
        .any(|field| !right_by_name.contains_key(field.name().as_str()))
    {
        return None;
    }
    Some(Fields::from(
        left_fields
            .iter()
            .map(|left_field| {
                let right_field = right_by_name.get(left_field.name().as_str())?;
                Some(Arc::new(Field::new(
                    left_field.name(),
                    wider_type(left_field.data_type(), right_field.data_type()),
                    left_field.is_nullable() || right_field.is_nullable(),
                )))
            })
            .collect::<Option<Vec<_>>>()?,
    ))
}

fn wider_map_type(left_entries: &Field, right_entries: &Field) -> DataType {
    let DataType::Struct(left_fields) = left_entries.data_type() else {
        return DataType::Map(Arc::new(left_entries.clone()), false);
    };
    let DataType::Struct(right_fields) = right_entries.data_type() else {
        return DataType::Map(Arc::new(left_entries.clone()), false);
    };
    if left_fields.len() != 2 || right_fields.len() != 2 {
        return DataType::Map(Arc::new(left_entries.clone()), false);
    }

    let key_type = wider_type(left_fields[0].data_type(), right_fields[0].data_type());
    let value_type = wider_type(left_fields[1].data_type(), right_fields[1].data_type());
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new(
                        "key",
                        key_type,
                        left_fields[0].is_nullable() || right_fields[0].is_nullable(),
                    )),
                    Arc::new(Field::new(
                        "value",
                        value_type,
                        left_fields[1].is_nullable() || right_fields[1].is_nullable(),
                    )),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;

    fn list(name: &str, ty: DataType, nullable: bool) -> DataType {
        DataType::List(Arc::new(Field::new(name, ty, nullable)))
    }

    fn map(key: DataType, value: DataType, key_nullable: bool, value_nullable: bool) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", key, key_nullable),
                        Field::new("value", value, value_nullable),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    #[test]
    fn decimal_precision_scale_promotion_and_existing_clamp() {
        for (left, right, expected) in [
            (
                DataType::Decimal128(10, 2),
                DataType::Decimal128(18, 4),
                DataType::Decimal128(18, 4),
            ),
            (
                DataType::Decimal128(38, 0),
                DataType::Decimal128(38, 20),
                DataType::Decimal256(58, 20),
            ),
            (
                DataType::Decimal256(76, 0),
                DataType::Decimal128(38, 38),
                DataType::Decimal256(76, 38),
            ),
            (
                DataType::Decimal128(10, -2),
                DataType::Decimal128(12, 3),
                DataType::Decimal128(15, 3),
            ),
            (
                DataType::Decimal256(39, 9),
                DataType::Int64,
                DataType::Decimal256(39, 9),
            ),
            (
                DataType::Decimal128(18, 6),
                DataType::Float32,
                DataType::Float64,
            ),
            (DataType::Decimal128(18, 6), DataType::Utf8, DataType::Utf8),
        ] {
            assert_eq!(wider_type(&left, &right), expected);
            assert_eq!(wider_type(&right, &left), expected);
        }
    }

    #[test]
    fn list_recursion_keeps_left_name_and_joins_nullability() {
        let left = list("left_item", DataType::Int32, false);
        let right = list("right_item", DataType::Int64, true);
        assert_eq!(
            wider_type(&left, &right),
            list("left_item", DataType::Int64, true)
        );
        assert_eq!(
            wider_type(&right, &left),
            list("right_item", DataType::Int64, true)
        );
    }

    #[test]
    fn struct_names_align_reordered_fields_before_type_merge() {
        let left = DataType::Struct(
            vec![
                Field::new("number", DataType::Int32, false),
                Field::new("text", DataType::Utf8, false),
            ]
            .into(),
        );
        let right = DataType::Struct(
            vec![
                Field::new("text", DataType::Utf8, true),
                Field::new("number", DataType::Int64, false),
            ]
            .into(),
        );
        let expected = DataType::Struct(
            vec![
                Field::new("number", DataType::Int64, false),
                Field::new("text", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(wider_type(&left, &right), expected);
    }

    #[test]
    fn struct_unmatched_names_keep_existing_positional_fallback() {
        let left = DataType::Struct(vec![Field::new("left", DataType::Int32, false)].into());
        let right = DataType::Struct(vec![Field::new("right", DataType::Int64, true)].into());
        assert_eq!(
            wider_type(&left, &right),
            DataType::Struct(vec![Field::new("left", DataType::Int64, true)].into())
        );
    }

    #[test]
    fn map_merges_both_channels_and_their_nullability() {
        let left = map(DataType::Int32, DataType::Decimal128(10, 2), false, false);
        let right = map(DataType::Int64, DataType::Decimal128(18, 4), true, true);
        assert_eq!(
            wider_type(&left, &right),
            map(DataType::Int64, DataType::Decimal128(18, 4), true, true)
        );
    }

    #[test]
    fn malformed_map_keeps_existing_left_carrier_fallback() {
        let entries = Arc::new(Field::new("opaque", DataType::Int32, true));
        let left = DataType::Map(Arc::clone(&entries), true);
        let right = map(DataType::Int64, DataType::Utf8, false, true);
        assert_eq!(wider_type(&left, &right), DataType::Map(entries, false));
    }

    #[test]
    fn null_largeint_and_timestamp_follow_original_carrier_rules() {
        let largeint = DataType::FixedSizeBinary(crate::LARGEINT_BYTE_WIDTH);
        assert_eq!(wider_type(&largeint, &DataType::Int64), largeint);
        let timestamp = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        assert_eq!(wider_type(&DataType::Date32, &timestamp), timestamp);
        let nested = list("item", DataType::Utf8, true);
        assert_eq!(wider_type(&DataType::Null, &nested), nested);
    }

    #[test]
    fn carrier_reconstruction_does_not_claim_logical_metadata_merge() {
        let tagged =
            Arc::new(Field::new("item", DataType::Utf8, false).with_metadata(
                [(crate::NR_LOGICAL_TYPE_KEY.to_string(), "json".to_string())].into(),
            ));
        let left = DataType::List(Arc::clone(&tagged));
        // Identical carriers retain the existing exact object shape.
        assert_eq!(wider_type(&left, &left), left);
        // Reconstruction in the legacy carrier rule deliberately does not
        // constitute a semantic-domain preservation proof.
        let right = list("item", DataType::Null, true);
        assert_eq!(
            wider_type(&left, &right),
            list("item", DataType::Utf8, true)
        );
    }
}
