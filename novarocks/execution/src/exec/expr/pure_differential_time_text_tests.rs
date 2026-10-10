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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! Original v1 differential oracle for each actual TIME declared overload.
use super::*;
use arrow::array::{Date32Array, StringArray, TimestampMicrosecondArray};
use temporal::SourceExpression;
fn strings(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn profile(name: &str, array: ArrayRef) -> ScalarDiffSpec {
    let rows = array.len();
    let spec = ScalarDiffSpec::new(name).column(array);
    if name == "time_format" {
        spec.column(strings(
            (0..rows)
                .map(|i| {
                    if i == 1 || i == 3 {
                        None
                    } else {
                        Some("%H:%i:%s %f %h %%")
                    }
                })
                .collect(),
        ))
    } else {
        spec
    }
}
fn check(name: &str, array: ArrayRef) {
    if std::env::var_os("UEA5E_TIME_OWNERS_DISABLED").is_some() {
        match run_scalar_differential(&profile(name, array.clone())) {
            Err(DifferentialFailure::MissingPureImplementation {
                overload,
                legacy: LegacyStatus::Available,
                ..
            }) => {
                eprintln!("TIME original missing overload: {}", overload.as_str());
                return;
            }
            other => {
                panic!("expected genuine independently selected missing-owner baseline: {other:?}")
            }
        }
    }
    let summary =
        assert_scalar_matches_v1(profile(name, array.clone()).sparse_selections(8, 0xA719));
    assert_eq!(summary.rows, array.len());
    assert_scalar_matches_v1(profile(name, array.slice(0, 0)));
    for source in [array.slice(0, 1), array.slice(1, 1)] {
        for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
            let mut spec = ScalarDiffSpec::new(name)
                .constant_array(source.clone())
                .constant_rows(7)
                .legacy_constants(form);
            if name == "time_format" {
                spec = spec.constant_array(strings(vec![Some("%H:%i:%s %f %h %%")]));
            }
            assert_scalar_matches_v1(spec.sparse_selections(5, 0x7F09));
        }
    }
}
#[test]
fn pure_differential_time_profile_to_sec_utf8() {
    check(
        "time_to_sec",
        strings(vec![
            Some("12:34:56"),
            None,
            Some("bad"),
            Some("-25:00:03.125"),
            Some("00:00:00"),
        ]),
    );
}
#[test]
fn pure_differential_time_profile_to_sec_date32() {
    check(
        "time_to_sec",
        Arc::new(Date32Array::from(vec![
            Some(0),
            None,
            Some(1),
            Some(-1),
            Some(i32::MAX),
        ])),
    );
}
#[test]
fn pure_differential_time_profile_to_sec_timestamp() {
    check(
        "time_to_sec",
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(45_296_000_000),
            None,
            Some(-1),
            Some(0),
            Some(i64::MAX),
        ])),
    );
}
#[test]
fn pure_differential_time_profile_format_utf8() {
    check(
        "time_format",
        strings(vec![
            Some("12:34:56"),
            None,
            Some("bad"),
            Some("-25:00:03.125"),
            Some("00:00:00"),
        ]),
    );
}
#[test]
fn pure_differential_time_profile_format_date32() {
    check(
        "time_format",
        Arc::new(Date32Array::from(vec![
            Some(0),
            None,
            Some(1),
            Some(-1),
            Some(i32::MAX),
        ])),
    );
}
#[test]
fn pure_differential_time_profile_format_timestamp() {
    check(
        "time_format",
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(45_296_000_000),
            None,
            Some(-1),
            Some(0),
            Some(i64::MAX),
        ])),
    );
}
#[test]
fn identity_cast_whole_invocation_error_uses_executed_typed_phase_not_single_row_guesses() {
    for array in [
        Arc::new(Date32Array::from(vec![Some(0), Some(i32::MAX), None])) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(vec![
            Some(0),
            Some(i64::MAX),
            None,
        ])) as ArrayRef,
    ] {
        let summary = assert_scalar_matches_v1(
            ScalarDiffSpec::new("time_to_sec")
                .column(array.clone())
                .temporal_source(SourceExpression {
                    input: DiffArgument::Column {
                        value_type: FunctionValueType::new(array.data_type().clone(), true),
                        values: array.clone(),
                    },
                    sec_to_time: false,
                    cast_chain: vec![array.data_type().clone()],
                })
                .sparse_selections(12, 0x87AA),
        );
        assert!(summary.legacy_batch_errors > 0);
        assert!(summary.attributed_row_errors >= 3);
    }
}
#[test]
fn utf8_cast_override_fresh_occurrences_match_actual_v1() {
    let source = strings(vec![
        Some("12:34:56"),
        None,
        Some("bad"),
        Some("2026-10-08 07:08:09"),
    ]);
    let timestamp = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
    // The normal binding channel is genuinely materialized by the old cast.
    // Both evaluated paths below still demand that original CAST afresh.
    let normal = materialize_cast(source.clone(), timestamp.clone());
    let spec = ScalarDiffSpec::new("time_format")
        .column(normal)
        .column(strings(vec![Some("%f"), Some("%H"), None, Some("%f")]))
        .temporal_source(SourceExpression {
            input: DiffArgument::Column {
                value_type: FunctionValueType::new(DataType::Utf8, true),
                values: source,
            },
            sec_to_time: false,
            cast_chain: vec![timestamp],
        });
    assert_scalar_matches_v1(spec.sparse_selections(8, 0xCE71));
}
#[test]
fn immediate_utf8_cast_source_fills_original_seconds_without_deepest_phase() {
    let source = strings(vec![
        Some("12:34:56"),
        None,
        Some("bad"),
        Some("2026-10-08"),
    ]);
    let normal = materialize_cast(source.clone(), DataType::Date32);
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("time_to_sec")
            .column(normal)
            .temporal_source(SourceExpression {
                input: DiffArgument::Column {
                    value_type: FunctionValueType::new(DataType::Utf8, true),
                    values: source,
                },
                sec_to_time: false,
                cast_chain: vec![DataType::Date32],
            })
            .sparse_selections(8, 0xC173),
    );
}

#[test]
fn roundtrip_bypass_unreachable_transformed_child_matches_actual_v1() {
    let array = Arc::new(arrow::array::Int64Array::from(vec![
        Some(45296),
        None,
        Some(-90),
        Some(i64::MAX),
    ])) as ArrayRef;
    let timestamp = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
    // This typed normal channel is only a logical request carrier; the real
    // original source author intentionally never evaluates the transformed child.
    let spec = ScalarDiffSpec::new("time_to_sec")
        .column(Arc::new(TimestampMicrosecondArray::from(vec![Some(0); 4])))
        .temporal_source(SourceExpression {
            input: DiffArgument::Column {
                value_type: FunctionValueType::new(DataType::Int64, true),
                values: array,
            },
            sec_to_time: true,
            cast_chain: vec![timestamp],
        });
    assert_scalar_matches_v1(spec.sparse_selections(8, 0x190F));
}
fn materialize_cast(array: ArrayRef, target: DataType) -> ArrayRef {
    let mut arena = ExprArena::default();
    let slot = SlotId::new(1);
    let input = arena.push_typed(ExprNode::SlotId(slot), array.data_type().clone());
    let expr = arena.push_typed(
        ExprNode::Cast(input, DecimalOverflowPolicy::OutputNull),
        target,
    );
    let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
        "source",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    let chunk_schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(schema.as_ref(), &[slot]).unwrap();
    arena
        .eval(expr, &Chunk::new_with_chunk_schema(batch, chunk_schema))
        .unwrap()
}
#[test]
fn all_six_actual_profiles_owner_status_supports_registration_disabled_before_baseline() {
    let disabled = std::env::var_os("UEA5E_TIME_OWNERS_DISABLED").is_some();
    let arrays = [
        strings(vec![Some("12:34:56")]),
        Arc::new(Date32Array::from(vec![0])) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(vec![0])) as ArrayRef,
    ];
    let mut overloads = std::collections::BTreeSet::new();
    for name in ["time_to_sec", "time_format"] {
        for array in &arrays {
            let spec = profile(name, array.clone());
            if disabled {
                match run_scalar_differential(&spec) {
                    Err(DifferentialFailure::MissingPureImplementation {
                        overload,
                        legacy: LegacyStatus::Available,
                        ..
                    }) => {
                        overloads.insert(overload.as_str().to_owned());
                    }
                    other => panic!("expected genuine missing-owner baseline: {other:?}"),
                }
            } else {
                let summary = assert_scalar_matches_v1(spec);
                overloads.insert(summary.overload.as_str().to_owned());
            }
        }
    }
    assert_eq!(overloads.len(), 6);
}
