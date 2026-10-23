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

//! Lossless execution-carrier adaptation to an authoritative Iceberg SDK schema.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, Int8Array, Int16Array, Int32Array,
    LargeBinaryArray, LargeStringArray, ListArray, MapArray, StringBuilder, StructArray,
    make_array,
};
use arrow::datatypes::{DataType, Field, Int32Type, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

/// The target has already been projected into input order by the frozen-schema
/// owner. This boundary does not resolve fields, fill columns, or cast values.
pub(super) fn adapt_batch(input: &RecordBatch, target: &SchemaRef) -> Result<RecordBatch, String> {
    validate_batch_schema(input.schema().as_ref(), target.as_ref())?;
    let source = input.schema();
    let columns = input
        .columns()
        .iter()
        .zip(source.fields())
        .zip(target.fields())
        .map(|((array, from), to)| {
            if !from.name().eq_ignore_ascii_case(to.name()) {
                return Err(format!(
                    "Iceberg write adaptation field name mismatch: input `{}` target `{}`",
                    from.name(),
                    to.name()
                ));
            }
            if !to.is_nullable() && array.null_count() != 0 {
                return Err(format!(
                    "Iceberg write adaptation required column `{}` contains NULL",
                    to.name()
                ));
            }
            let adapted = adapt_array(array, from, to, to.name(), true)?;
            if !to.is_nullable() && adapted.null_count() != 0 {
                return Err(format!(
                    "Iceberg write adaptation required column `{}` contains NULL",
                    to.name()
                ));
            }
            Ok(adapted)
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new_with_options(
        target.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
    )
    .map_err(|error| format!("Iceberg write adaptation batch failed: {error}"))
}

fn nested_nullability(from: &Field, to: &Field, path: &str) -> Result<(), String> {
    if from.is_nullable() != to.is_nullable() {
        return Err(format!(
            "Iceberg write adaptation nested nullability mismatch at `{path}`: input={} target={}",
            from.is_nullable(),
            to.is_nullable()
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Adaptation {
    Identity,
    Int8,
    Int16,
    Binary,
    Timezone,
    Dictionary,
    Struct,
    List,
    Map,
}

fn adaptation(
    from: &DataType,
    to: &DataType,
    path: &str,
    top_level: bool,
) -> Result<Adaptation, String> {
    Ok(match (from, to) {
        (x, y) if x == y => Adaptation::Identity,
        (DataType::Int8, DataType::Int32) if top_level => Adaptation::Int8,
        (DataType::Int16, DataType::Int32) if top_level => Adaptation::Int16,
        (DataType::Binary, DataType::LargeBinary) => Adaptation::Binary,
        (DataType::Timestamp(u, None), DataType::Timestamp(v, Some(tz)))
            if u == v && tz.as_ref() == "+00:00" =>
        {
            Adaptation::Timezone
        }
        (DataType::Dictionary(k, v), DataType::Utf8)
            if **k == DataType::Int32
                && matches!(v.as_ref(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            Adaptation::Dictionary
        }
        (DataType::Struct(_), DataType::Struct(_)) => Adaptation::Struct,
        (DataType::List(_), DataType::List(_)) => Adaptation::List,
        (DataType::Map(_, s1), DataType::Map(_, s2)) if s1 == s2 => Adaptation::Map,
        _ => {
            return Err(format!(
                "unsupported Iceberg write adaptation at `{path}`: {from:?} to {to:?}"
            ));
        }
    })
}

/// Pure preparation shares the exact conversion vocabulary with batch writing.
/// Required actual NULLs and string offset bounds need arrays and remain checked
/// by `adapt_batch` before publishing bytes.
pub(crate) fn validate_batch_schema(
    input: &arrow::datatypes::Schema,
    target: &arrow::datatypes::Schema,
) -> Result<(), String> {
    if input.fields().len() != target.fields().len() {
        return Err(format!(
            "Iceberg write adaptation column count mismatch: input={} target={}",
            input.fields().len(),
            target.fields().len()
        ));
    }
    for (from, to) in input.fields().iter().zip(target.fields()) {
        if !from.name().eq_ignore_ascii_case(to.name()) {
            return Err(format!(
                "Iceberg write adaptation field name mismatch: input `{}` target `{}`",
                from.name(),
                to.name()
            ));
        }
        validate_field_schema(from, to, to.name(), true, false)?;
    }
    Ok(())
}

fn validate_field_schema(
    from: &Field,
    to: &Field,
    path: &str,
    top_level: bool,
    canonical_struct_roles: bool,
) -> Result<(), String> {
    if !top_level {
        nested_nullability(from, to, path)?;
    }
    match adaptation(from.data_type(), to.data_type(), path, top_level)? {
        Adaptation::Struct => {
            let (DataType::Struct(sources), DataType::Struct(targets)) =
                (from.data_type(), to.data_type())
            else {
                unreachable!()
            };
            if sources.len() != targets.len() {
                return Err(format!(
                    "Iceberg write adaptation struct child count mismatch at `{path}`"
                ));
            }
            for (source, target) in sources.iter().zip(targets) {
                if !canonical_struct_roles && !source.name().eq_ignore_ascii_case(target.name()) {
                    return Err(format!(
                        "Iceberg write adaptation struct field name mismatch at `{path}.{}`: input `{}` target `{}`",
                        target.name(),
                        source.name(),
                        target.name()
                    ));
                }
                validate_field_schema(
                    source,
                    target,
                    &format!("{path}.{}", target.name()),
                    false,
                    false,
                )?;
            }
        }
        Adaptation::List | Adaptation::Map => {
            let (source, target, canonical_struct_roles) = match (from.data_type(), to.data_type())
            {
                (DataType::List(s), DataType::List(t)) => (s, t, false),
                (DataType::Map(s, _), DataType::Map(t, _)) => (s, t, true),
                _ => unreachable!(),
            };
            validate_field_schema(
                source,
                target,
                &format!("{path}.{}", target.name()),
                false,
                canonical_struct_roles,
            )?;
        }
        _ => (),
    }
    Ok(())
}

fn adapt_array(
    array: &ArrayRef,
    from: &Field,
    to: &Field,
    path: &str,
    top_level: bool,
) -> Result<ArrayRef, String> {
    if !top_level {
        nested_nullability(from, to, path)?;
    }
    let target = to.data_type();
    match adaptation(array.data_type(), target, path, top_level)? {
        Adaptation::Identity => Ok(array.clone()),
        Adaptation::Int8 => {
            let a = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .expect("Int8 array");
            Ok(Arc::new(Int32Array::from_iter(
                a.iter().map(|v| v.map(i32::from)),
            )))
        }
        Adaptation::Int16 => {
            let a = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .expect("Int16 array");
            Ok(Arc::new(Int32Array::from_iter(
                a.iter().map(|v| v.map(i32::from)),
            )))
        }
        Adaptation::Binary => {
            let a = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("Binary array");
            Ok(Arc::new(LargeBinaryArray::new(
                arrow::buffer::OffsetBuffer::new(
                    a.value_offsets()
                        .iter()
                        .map(|v| i64::from(*v))
                        .collect::<Vec<_>>()
                        .into(),
                ),
                a.values().clone(),
                a.nulls().cloned(),
            )))
        }
        Adaptation::Timezone => {
            let data = array
                .to_data()
                .into_builder()
                .data_type(target.clone())
                .build()
                .map_err(|error| {
                    format!("Iceberg write adaptation timezone label at `{path}` failed: {error}")
                })?;
            Ok(make_array(data))
        }
        Adaptation::Dictionary => {
            let dictionary = array
                .as_any()
                .downcast_ref::<DictionaryArray<Int32Type>>()
                .expect("Int32 dictionary");
            let unpacked =
                arrow::compute::take(dictionary.values().as_ref(), dictionary.keys(), None)
                    .map_err(|error| {
                        format!("Iceberg write adaptation dictionary at `{path}` failed: {error}")
                    })?;
            // LargeUtf8 dictionaries are an admitted execution encoding. Convert
            // their expanded strings explicitly; overflow must be an error, never
            // truncation or a new NULL from Arrow's relaxed scalar casts.
            let unpacked = if matches!(unpacked.data_type(), DataType::LargeUtf8)
                && *target == DataType::Utf8
            {
                checked_string_offsets(&unpacked, path)?
            } else {
                unpacked
            };
            let unpacked_field = from.clone().with_data_type(unpacked.data_type().clone());
            adapt_array(&unpacked, &unpacked_field, to, path, top_level)
        }
        Adaptation::Struct => {
            let (DataType::Struct(source_fields), DataType::Struct(target_fields)) =
                (array.data_type(), target)
            else {
                unreachable!()
            };
            let a = array
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("Struct array");
            if source_fields.len() != target_fields.len() {
                return Err(format!(
                    "Iceberg write adaptation struct child count mismatch at `{path}`"
                ));
            }
            let columns = a
                .columns()
                .iter()
                .zip(source_fields)
                .zip(target_fields)
                .map(|((child, source), target)| {
                    adapt_array(
                        child,
                        source,
                        target,
                        &format!("{path}.{}", target.name()),
                        false,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            StructArray::try_new_with_length(
                target_fields.clone(),
                columns,
                a.nulls().cloned(),
                a.len(),
            )
            .map(|a| Arc::new(a) as ArrayRef)
            .map_err(|error| format!("Iceberg write adaptation struct at `{path}` failed: {error}"))
        }
        Adaptation::List => {
            let (DataType::List(source), DataType::List(target)) = (array.data_type(), target)
            else {
                unreachable!()
            };
            let a = array
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("List array");
            let values = adapt_array(
                a.values(),
                source,
                target,
                &format!("{path}.{}", target.name()),
                false,
            )?;
            ListArray::try_new(
                target.clone(),
                a.offsets().clone(),
                values,
                a.nulls().cloned(),
            )
            .map(|a| Arc::new(a) as ArrayRef)
            .map_err(|error| format!("Iceberg write adaptation list at `{path}` failed: {error}"))
        }
        Adaptation::Map => {
            let (DataType::Map(source, _), DataType::Map(target, target_sorted)) =
                (array.data_type(), target)
            else {
                unreachable!()
            };
            let a = array
                .as_any()
                .downcast_ref::<MapArray>()
                .expect("Map array");
            let entries = adapt_array(
                &(Arc::new(a.entries().clone()) as ArrayRef),
                source,
                target,
                &format!("{path}.{}", target.name()),
                false,
            )?;
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("adapted map entries");
            MapArray::try_new(
                target.clone(),
                a.offsets().clone(),
                entries.clone(),
                a.nulls().cloned(),
                *target_sorted,
            )
            .map(|a| Arc::new(a) as ArrayRef)
            .map_err(|error| format!("Iceberg write adaptation map at `{path}` failed: {error}"))
        }
    }
}

fn checked_utf8_total(total: usize, additional: usize, path: &str) -> Result<usize, String> {
    total
        .checked_add(additional)
        .filter(|total| *total <= i32::MAX as usize)
        .ok_or_else(|| {
            format!("Iceberg write adaptation string offsets exceed i32 range at `{path}`")
        })
}

fn checked_string_offsets(array: &ArrayRef, path: &str) -> Result<ArrayRef, String> {
    let a = array
        .as_any()
        .downcast_ref::<LargeStringArray>()
        .expect("LargeUtf8 array");
    let total = a.iter().flatten().try_fold(0, |total, value| {
        checked_utf8_total(total, value.len(), path)
    })?;
    let mut builder = StringBuilder::with_capacity(a.len(), total);
    for value in a.iter() {
        builder.append_option(value);
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Decimal128Array, Int64Array, LargeStringArray, StringArray, TimestampMicrosecondArray,
        TimestampNanosecondArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::{Fields, Schema, TimeUnit};
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

    fn field(name: &str, dtype: DataType, nullable: bool, id: i32) -> Field {
        Field::new(name, dtype, nullable).with_metadata(std::collections::HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    fn adapt_one(array: ArrayRef, target: Field) -> Result<RecordBatch, String> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            target.name(),
            array.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        adapt_batch(&batch, &Arc::new(Schema::new(vec![target])))
    }

    fn dictionary(large: bool, nullable: bool) -> ArrayRef {
        let values: ArrayRef = if large {
            Arc::new(LargeStringArray::from(vec![
                Some("alpha"),
                None,
                Some("東京"),
            ]))
        } else {
            Arc::new(StringArray::from(vec![Some("alpha"), None, Some("東京")]))
        };
        let keys = if nullable {
            Int32Array::from(vec![Some(2), None, Some(0), Some(1)])
        } else {
            Int32Array::from(vec![2, 0, 2, 0])
        };
        Arc::new(DictionaryArray::try_new(keys, values).unwrap())
    }

    #[test]
    fn top_level_narrow_integers_widen_without_losing_values_or_nulls() {
        for array in [
            Arc::new(Int8Array::from(vec![Some(i8::MIN), None, Some(i8::MAX)])) as ArrayRef,
            Arc::new(Int16Array::from(vec![Some(i16::MIN), None, Some(i16::MAX)])) as ArrayRef,
        ] {
            let expected = if *array.data_type() == DataType::Int8 {
                vec![Some(-128), None, Some(127)]
            } else {
                vec![Some(-32768), None, Some(32767)]
            };
            let batch = adapt_one(array, field("n", DataType::Int32, true, 1)).unwrap();
            let output = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(output.iter().collect::<Vec<_>>(), expected);
            assert_eq!(
                batch
                    .schema()
                    .field(0)
                    .metadata()
                    .get(PARQUET_FIELD_ID_META_KEY),
                Some(&"1".to_string())
            );
        }
    }

    #[test]
    fn timezone_relabel_preserves_micros_nanos_nulls_and_slices() {
        for array in [
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(-1),
                None,
                Some(42),
            ])) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(vec![
                Some(-1),
                None,
                Some(42),
            ])) as ArrayRef,
        ] {
            let unit = match array.data_type() {
                DataType::Timestamp(unit, _) => *unit,
                _ => unreachable!(),
            };
            let input = array.slice(1, 2);
            let output = adapt_one(
                input.clone(),
                field(
                    "ts",
                    DataType::Timestamp(unit, Some("+00:00".into())),
                    true,
                    9,
                ),
            )
            .unwrap();
            assert_eq!(
                input.to_data().buffers(),
                output.column(0).to_data().buffers()
            );
            assert!(output.column(0).is_null(0));
            assert_eq!(input.to_data().nulls(), output.column(0).to_data().nulls());
        }
    }

    #[test]
    fn admitted_dictionaries_expand_at_root_and_keep_logical_nulls() {
        for large in [false, true] {
            let output =
                adapt_one(dictionary(large, true), field("s", DataType::Utf8, true, 2)).unwrap();
            let strings = output
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(
                strings.iter().collect::<Vec<_>>(),
                vec![Some("東京"), None, Some("alpha"), None]
            );
        }
    }

    #[test]
    fn binary_widens_in_sliced_list_and_map_with_authoritative_nested_fields() {
        let binary: ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&[0xff, 0][..]),
            None,
            Some(&[][..]),
            Some(&[1][..]),
        ]));
        let source_child = Arc::new(Field::new("item", DataType::Binary, true));
        let list: ArrayRef = Arc::new(ListArray::new(
            source_child,
            OffsetBuffer::new(vec![0, 1, 3, 4].into()),
            binary.clone(),
            Some(NullBuffer::from(vec![true, false, true])),
        ));
        let target_child = Arc::new(field("element", DataType::LargeBinary, true, 11));
        let output = adapt_one(
            list.slice(1, 2),
            field("l", DataType::List(target_child.clone()), true, 10),
        )
        .unwrap();
        let list = output
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.value_offsets(), &[1, 3, 4]);
        assert!(list.is_null(0));
        assert_eq!(list.data_type(), &DataType::List(target_child));
        let values = list
            .values()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), &[0xff, 0]);
        assert!(values.is_null(1));
        assert_eq!(values.value(2), &[] as &[u8]);

        let sources: Fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Binary, true)),
        ]
        .into();
        let entries = StructArray::new(
            sources.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])), binary],
            None,
        );
        let map: ArrayRef = Arc::new(MapArray::new(
            Arc::new(Field::new("entries", DataType::Struct(sources), false)),
            OffsetBuffer::new(vec![0, 2, 4].into()),
            entries,
            None,
            false,
        ));
        let targets: Fields = vec![
            Arc::new(field("key", DataType::Int32, false, 21)),
            Arc::new(field("value", DataType::LargeBinary, true, 22)),
        ]
        .into();
        let output = adapt_one(
            map,
            field(
                "m",
                DataType::Map(
                    Arc::new(Field::new(
                        "key_value",
                        DataType::Struct(targets.clone()),
                        false,
                    )),
                    false,
                ),
                true,
                20,
            ),
        )
        .unwrap();
        let map = output
            .column(0)
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        assert_eq!(map.entries().fields(), &targets);
        assert_eq!(map.value_offsets(), &[0, 2, 4]);
        assert_eq!(
            map.values()
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(0),
            &[0xff, 0]
        );
    }

    #[test]
    fn dictionaries_expand_recursively_in_struct_list_and_map_keys() {
        for large in [false, true] {
            let dict = dictionary(large, true);
            let source_fields: Fields =
                vec![Arc::new(Field::new("s", dict.data_type().clone(), true))].into();
            let structure: ArrayRef = Arc::new(StructArray::new(
                source_fields,
                vec![dict.clone()],
                Some(NullBuffer::from(vec![true, false, true, true])),
            ));
            let target_fields: Fields = vec![Arc::new(field("s", DataType::Utf8, true, 31))].into();
            let output = adapt_one(
                structure,
                field("obj", DataType::Struct(target_fields), true, 30),
            )
            .unwrap();
            let structure = output
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert!(structure.is_null(1));
            assert_eq!(
                structure
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "東京"
            );
            let list: ArrayRef = Arc::new(ListArray::new(
                Arc::new(Field::new("item", dict.data_type().clone(), true)),
                OffsetBuffer::new(vec![0, 2, 4].into()),
                dict,
                None,
            ));
            let output = adapt_one(
                list,
                field(
                    "l",
                    DataType::List(Arc::new(field("element", DataType::Utf8, true, 41))),
                    true,
                    40,
                ),
            )
            .unwrap();
            let list = output
                .column(0)
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            assert!(list.values().is_null(3));

            let keys = dictionary(large, false);
            let fields: Fields = vec![
                Arc::new(Field::new("key", keys.data_type().clone(), false)),
                Arc::new(Field::new("value", DataType::Int32, true)),
            ]
            .into();
            let entries = StructArray::new(
                fields.clone(),
                vec![
                    keys,
                    Arc::new(Int32Array::from(vec![Some(1), None, Some(3), Some(4)])),
                ],
                None,
            );
            let map: ArrayRef = Arc::new(MapArray::new(
                Arc::new(Field::new("entries", DataType::Struct(fields), false)),
                OffsetBuffer::new(vec![0, 4].into()),
                entries,
                None,
                false,
            ));
            let fields: Fields = vec![
                Arc::new(field("key", DataType::Utf8, false, 51)),
                Arc::new(field("value", DataType::Int32, true, 52)),
            ]
            .into();
            let output = adapt_one(
                map,
                field(
                    "m",
                    DataType::Map(
                        Arc::new(Field::new("key_value", DataType::Struct(fields), false)),
                        false,
                    ),
                    true,
                    50,
                ),
            )
            .unwrap();
            let map = output
                .column(0)
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap();
            assert_eq!(
                map.keys()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some("東京"), Some("alpha"), Some("東京"), Some("alpha")]
            );
        }
    }

    #[test]
    fn closed_set_rejects_value_casts_and_unsupported_carriers_with_path() {
        for (array, dtype) in [
            (
                Arc::new(Int64Array::from(vec![Some(i64::MAX), None])) as ArrayRef,
                DataType::Int32,
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![100, 200])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ) as ArrayRef,
                DataType::Decimal128(9, 2),
            ),
            (
                Arc::new(
                    Decimal128Array::from(vec![100, 200])
                        .with_precision_and_scale(10, 2)
                        .unwrap(),
                ) as ArrayRef,
                DataType::Decimal128(10, 1),
            ),
            (
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1), None]).with_timezone("+08:00"),
                ) as ArrayRef,
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            (
                Arc::new(arrow::array::NullArray::new(2)) as ArrayRef,
                DataType::Utf8,
            ),
            (
                Arc::new(LargeStringArray::from(vec![Some("a"), None])) as ArrayRef,
                DataType::Utf8,
            ),
            (
                Arc::new(arrow::array::StringViewArray::from(vec![Some("a"), None])) as ArrayRef,
                DataType::Utf8,
            ),
            (
                Arc::new(
                    DictionaryArray::try_new(
                        Int16Array::from(vec![0, 0]),
                        Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                    )
                    .unwrap(),
                ) as ArrayRef,
                DataType::Utf8,
            ),
        ] {
            let error = adapt_one(array, field("bad", dtype, true, 60)).unwrap_err();
            assert!(
                error.contains("unsupported Iceberg write adaptation at `bad`"),
                "{error}"
            );
        }
    }

    #[test]
    fn required_root_checks_array_nulls_including_dictionary_value_nulls() {
        for array in [
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
            dictionary(false, true),
        ] {
            let dtype = if matches!(array.data_type(), DataType::Dictionary(..)) {
                DataType::Utf8
            } else {
                DataType::Int32
            };
            let error = adapt_one(array, field("id", dtype, false, 1)).unwrap_err();
            assert!(error.contains("id"), "{error}");
        }
        // A non-null dictionary key can still select a NULL dictionary value.
        let array: ArrayRef = Arc::new(
            DictionaryArray::try_new(
                Int32Array::from(vec![0]),
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            )
            .unwrap(),
        );
        assert!(adapt_one(array, field("id", DataType::Utf8, false, 1)).is_err());
    }

    #[test]
    fn nested_nullability_and_map_sort_mismatches_and_nested_narrow_ints_fail() {
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(Int32Array::from(vec![1])),
            None,
        ));
        let error = adapt_one(
            array,
            field(
                "l",
                DataType::List(Arc::new(field("element", DataType::Int32, false, 2))),
                true,
                1,
            ),
        )
        .unwrap_err();
        assert!(
            error.contains("nested nullability mismatch at `l.element`"),
            "{error}"
        );
        let array: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Int8, true)),
            OffsetBuffer::new(vec![0, 1].into()),
            Arc::new(Int8Array::from(vec![1])),
            None,
        ));
        assert!(
            adapt_one(
                array,
                field(
                    "l",
                    DataType::List(Arc::new(field("element", DataType::Int32, true, 2))),
                    true,
                    1
                )
            )
            .unwrap_err()
            .contains("l.element")
        );
        let fields: Fields = vec![
            Arc::new(Field::new("key", DataType::Int32, false)),
            Arc::new(Field::new("value", DataType::Int32, true)),
        ]
        .into();
        let entries = StructArray::new(
            fields.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(Int32Array::from(vec![2])),
            ],
            None,
        );
        let entry_field = Arc::new(Field::new("entries", DataType::Struct(fields), false));
        let array: ArrayRef = Arc::new(MapArray::new(
            entry_field.clone(),
            OffsetBuffer::new(vec![0, 1].into()),
            entries,
            None,
            false,
        ));
        let error =
            adapt_one(array, field("m", DataType::Map(entry_field, true), true, 1)).unwrap_err();
        assert!(error.contains("at `m`"), "{error}");
    }

    #[test]
    fn string_offset_overflow_is_checked_before_allocating() {
        assert_eq!(
            checked_utf8_total(i32::MAX as usize - 1, 1, "s").unwrap(),
            i32::MAX as usize
        );
        for (total, additional) in [(i32::MAX as usize, 1), (usize::MAX, 1)] {
            assert!(
                checked_utf8_total(total, additional, "s")
                    .unwrap_err()
                    .contains("offsets exceed i32 range at `s`")
            );
        }
    }

    #[test]
    fn reordered_top_level_names_do_not_rebind_values_and_identical_arrays_are_shared() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "input",
                DataType::Int32,
                true,
            )])),
            vec![values.clone()],
        )
        .unwrap();
        let error = adapt_batch(
            &input,
            &Arc::new(Schema::new(vec![field(
                "different",
                DataType::Int32,
                true,
                1,
            )])),
        )
        .unwrap_err();
        assert!(error.contains("field name mismatch"));
        let output = adapt_batch(
            &input,
            &Arc::new(Schema::new(vec![field("input", DataType::Int32, true, 1)])),
        )
        .unwrap();
        assert!(Arc::ptr_eq(&values, output.column(0)));
    }
    #[test]
    fn struct_child_names_are_semantic_and_must_not_rebind_equal_carriers() {
        let sources: Fields = vec![
            Arc::new(Field::new("right", DataType::Int32, true)),
            Arc::new(Field::new("left", DataType::Int32, true)),
        ]
        .into();
        let targets: Fields = vec![
            Arc::new(field("left", DataType::Int32, true, 2)),
            Arc::new(field("right", DataType::Int32, true, 3)),
        ]
        .into();
        let input_schema = Schema::new(vec![Field::new(
            "s",
            DataType::Struct(sources.clone()),
            true,
        )]);
        let target_schema = Schema::new(vec![field("s", DataType::Struct(targets), true, 1)]);
        let error = validate_batch_schema(&input_schema, &target_schema).unwrap_err();
        assert!(
            error.contains("struct field name mismatch at `s.left`"),
            "{error}"
        );
        let array = Arc::new(StructArray::new(
            sources,
            vec![
                Arc::new(Int32Array::from(vec![2])),
                Arc::new(Int32Array::from(vec![1])),
            ],
            None,
        ));
        let input = RecordBatch::try_new(Arc::new(input_schema), vec![array]).unwrap();
        assert!(
            adapt_batch(&input, &Arc::new(target_schema))
                .unwrap_err()
                .contains("struct field name mismatch at `s.left`")
        );
    }
}
