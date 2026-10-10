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

//! Immutable actual v1 FROM_UNIXTIME/HOUR values, carrier drift, errors and panics.
//! Independent original dispatcher goldens; timezone authority is explicit in each case.
use super::{ExprArena, ExprNode};
use crate::exec::chunk::{Chunk, ChunkSchema};
use crate::exec::expr::function::{FunctionKind, eval_date_function};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Int64Array, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Local, TimeZone, Timelike};
use novarocks_types::SlotId;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Arc,
};
fn chunk(columns: Vec<ArrayRef>) -> Chunk {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, c)| Field::new(format!("c{}", i + 1), c.data_type().clone(), true))
        .collect::<Vec<_>>();
    let slots = (1..=columns.len())
        .map(|i| SlotId::new(i as u32))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap();
    let schema =
        ChunkSchema::try_ref_from_schema_and_slot_ids(batch.schema().as_ref(), &slots).unwrap();
    Chunk::new_with_chunk_schema(batch, schema)
}
fn eval(
    name: &'static str,
    columns: Vec<ArrayRef>,
    result: DataType,
    zone: Option<&str>,
) -> Result<ArrayRef, String> {
    let mut arena = ExprArena::default();
    arena.set_session_time_zone(zone.map(str::to_owned));
    let args = columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            arena.push_typed(
                ExprNode::SlotId(SlotId::new(i as u32 + 1)),
                c.data_type().clone(),
            )
        })
        .collect::<Vec<_>>();
    let call = arena.push_typed(
        ExprNode::FunctionCall {
            kind: FunctionKind::Date(name),
            args: args.clone(),
        },
        result,
    );
    eval_date_function(name, &arena, call, &args, &chunk(columns))
}

fn ints(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}
fn text(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}
fn strings(output: ArrayRef) -> Vec<Option<String>> {
    assert_eq!(output.data_type(), &DataType::Utf8);
    output
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|s| s.map(str::to_owned))
        .collect()
}
#[test]
fn original_from_unixtime_seconds_bounds_null_empty_and_explicit_utc_are_frozen() {
    let values = ints(vec![
        Some(0),
        Some(1),
        Some(253402243199),
        Some(253402243200),
        Some(-1),
        None,
        Some(i64::MAX),
    ]);
    assert_eq!(
        strings(eval("from_unixtime", vec![values], DataType::Utf8, Some("UTC")).unwrap()),
        vec![
            Some("1970-01-01 00:00:00".into()),
            Some("1970-01-01 00:00:01".into()),
            Some("9999-12-31 07:59:59".into()),
            None,
            None,
            None,
            None
        ]
    );
    assert!(
        strings(
            eval(
                "from_unixtime",
                vec![ints(vec![])],
                DataType::Utf8,
                Some("UTC")
            )
            .unwrap()
        )
        .is_empty()
    );
}
#[test]
fn original_from_unixtime_actual_session_zone_fixed_named_and_explicit_argument_override() {
    for (zone, expected) in [
        ("+10:00", "1970-01-01 10:00:01"),
        ("-05:00", "1969-12-31 19:00:01"),
        ("Asia/Shanghai", "1970-01-01 08:00:01"),
        ("America/New_York", "1969-12-31 19:00:01"),
    ] {
        assert_eq!(
            strings(
                eval(
                    "from_unixtime",
                    vec![ints(vec![Some(1)])],
                    DataType::Utf8,
                    Some(zone)
                )
                .unwrap()
            ),
            vec![Some(expected.into())]
        );
    }
    assert_eq!(
        strings(
            eval(
                "from_unixtime",
                vec![
                    ints(vec![Some(1), Some(1), Some(1), Some(1)]),
                    text(vec![Some("%Y-%m-%d %H:%i:%s"); 4]),
                    text(vec![Some("UTC"), Some("+10:00"), Some("bad-zone"), None])
                ],
                DataType::Utf8,
                Some("-05:00")
            )
            .unwrap()
        ),
        vec![
            Some("1970-01-01 00:00:01".into()),
            Some("1970-01-01 10:00:01".into()),
            None,
            None
        ]
    );
}
#[test]
fn original_from_unixtime_normalizer_exact_128_129_and_rendered_size_null_limits() {
    let f128 = ":".repeat(128);
    let f129 = ":".repeat(129);
    let expanded = "%Y".repeat(64);
    let format = text(vec![
        Some("yyyy-MM-dd HH:mm:ss"),
        Some("yyyy-MM-dd"),
        Some("yyyyMMdd"),
        Some("%Y-%m-%d %H:%i:%S"),
        Some(""),
        Some("plain"),
        Some("%"),
        Some("%Q"),
        Some(&f128),
        Some(&f129),
        Some(&expanded),
        None,
    ]);
    let actual = strings(
        eval(
            "from_unixtime",
            vec![ints(vec![Some(1); 12]), format],
            DataType::Utf8,
            Some("UTC"),
        )
        .unwrap(),
    );
    assert_eq!(
        actual,
        vec![
            Some("1970-01-01 00:00:01".into()),
            Some("1970-01-01".into()),
            Some("19700101".into()),
            Some("1970-01-01 00:00:01".into()),
            None,
            None,
            None,
            None,
            Some(f128),
            None,
            None,
            None
        ]
    );
}
#[test]
fn original_from_unixtime_format_percent_bug_panics_only_after_original_masks() {
    for fmt in ["%%", "%Y%%"] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| eval(
                "from_unixtime",
                vec![ints(vec![Some(0)]), text(vec![Some(fmt)])],
                DataType::Utf8,
                Some("UTC")
            )))
            .is_err()
        );
    }
    assert_eq!(
        strings(
            eval(
                "from_unixtime",
                vec![
                    ints(vec![None, Some(-1), Some(253402243200), Some(0)]),
                    text(vec![Some("%%"); 4]),
                    text(vec![
                        Some("UTC"),
                        Some("UTC"),
                        Some("UTC"),
                        Some("bad-zone")
                    ])
                ],
                DataType::Utf8,
                Some("UTC")
            )
            .unwrap()
        ),
        vec![None, None, None, None]
    );
    assert_eq!(
        strings(
            eval(
                "from_unixtime",
                vec![ints(vec![Some(0)]), text(vec![Some("%%%%")])],
                DataType::Utf8,
                Some("UTC")
            )
            .unwrap()
        ),
        vec![Some("%".into())]
    );
}
#[test]
fn original_from_unixtime_raw_output_projection_all_units_timezone_and_formatted_date() {
    let source = ints(vec![Some(1), None]);
    assert_eq!(
        eval(
            "from_unixtime",
            vec![source.clone()],
            DataType::Date32,
            Some("UTC")
        )
        .unwrap()
        .to_data(),
        Date32Array::from(vec![Some(0), None]).to_data()
    );
    for (unit, value) in [
        (TimeUnit::Second, 1),
        (TimeUnit::Millisecond, 1000),
        (TimeUnit::Microsecond, 1000000),
        (TimeUnit::Nanosecond, 1000000000),
    ] {
        let ty = DataType::Timestamp(unit, Some("UTC".into()));
        let actual = eval(
            "from_unixtime",
            vec![source.clone()],
            ty.clone(),
            Some("UTC"),
        )
        .unwrap();
        let expected: ArrayRef = match unit {
            TimeUnit::Second => {
                Arc::new(TimestampSecondArray::from(vec![Some(value), None]).with_timezone("UTC"))
            }
            TimeUnit::Millisecond => Arc::new(
                TimestampMillisecondArray::from(vec![Some(value), None]).with_timezone("UTC"),
            ),
            TimeUnit::Microsecond => Arc::new(
                TimestampMicrosecondArray::from(vec![Some(value), None]).with_timezone("UTC"),
            ),
            TimeUnit::Nanosecond => Arc::new(
                TimestampNanosecondArray::from(vec![Some(value), None]).with_timezone("UTC"),
            ),
        };
        assert_eq!(actual.to_data(), expected.to_data());
    }
    assert_eq!(
        eval(
            "from_unixtime",
            vec![source, text(vec![Some("yyyy-MM-dd"), None])],
            DataType::Date32,
            Some("UTC")
        )
        .unwrap()
        .to_data(),
        Date32Array::from(vec![Some(0), None]).to_data()
    );
    assert_eq!(
        strings(
            eval(
                "from_unixtime_ms",
                vec![ints(vec![Some(1001), Some(-1), None])],
                DataType::Utf8,
                Some("UTC")
            )
            .unwrap()
        ),
        vec![Some("1970-01-01 00:00:01".into()), None, None]
    );
}
#[test]
fn original_from_unixtime_declared_datetime_date_carriers_errors_and_utf8_numeric_profile() {
    for source in [
        Arc::new(Date32Array::from(vec![0])) as ArrayRef,
        Arc::new(TimestampMicrosecondArray::from(vec![0])),
        Arc::new(BooleanArray::from(vec![true])),
    ] {
        assert_eq!(
            eval(
                "from_unixtime",
                vec![source, text(vec![Some("%Y")])],
                DataType::Utf8,
                Some("UTC")
            )
            .unwrap_err(),
            "from_unixtime expects int"
        );
    }
    assert_eq!(
        strings(
            eval(
                "from_unixtime",
                vec![
                    text(vec![Some("0"), Some(" 1.9 "), Some("bad"), None]),
                    text(vec![Some("%Y-%m-%d %H:%i:%s"); 4])
                ],
                DataType::Utf8,
                Some("UTC")
            )
            .unwrap()
        ),
        vec![
            Some("1970-01-01 00:00:00".into()),
            Some("1970-01-01 00:00:01".into()),
            None,
            None
        ]
    );
    assert_eq!(
        eval(
            "from_unixtime",
            vec![ints(vec![Some(0)]), ints(vec![Some(0)])],
            DataType::Utf8,
            Some("UTC")
        )
        .unwrap_err(),
        "from_unixtime expects string format"
    );
    assert_eq!(
        eval(
            "from_unixtime",
            vec![
                ints(vec![Some(0)]),
                text(vec![Some("%Y")]),
                ints(vec![Some(0)])
            ],
            DataType::Utf8,
            Some("UTC")
        )
        .unwrap_err(),
        "from_unixtime expects string timezone"
    );
    assert_eq!(
        eval(
            "from_unixtime",
            vec![ints(vec![Some(0)])],
            DataType::Int32,
            Some("UTC")
        )
        .unwrap_err(),
        "from_unixtime unsupported output type: Int32"
    );
}
#[test]
fn original_from_unixtime_local_fallback_uses_real_process_author_not_session_default_guess() {
    let expected = Local
        .timestamp_opt(0, 0)
        .single()
        .unwrap()
        .naive_local()
        .format("%Y-%m-%d %H:%M:%S")
        .to_string();
    for zone in [None, Some("local"), Some("bad-zone")] {
        assert_eq!(
            strings(
                eval(
                    "from_unixtime",
                    vec![ints(vec![Some(0)])],
                    DataType::Utf8,
                    zone
                )
                .unwrap()
            ),
            vec![Some(expected.clone())]
        );
    }
}
#[test]
fn original_hour_from_unixtime_ignores_session_zone_and_retains_actual_int64_carrier() {
    let input = vec![
        Some(0),
        Some(5 * 3600),
        None,
        Some(-1),
        Some(253402243199),
        Some(253402243200),
    ];
    let expected = Int64Array::from(
        input
            .iter()
            .map(|v| {
                v.and_then(|s| {
                    (0..=253402243199)
                        .contains(&s)
                        .then(|| Local.timestamp_opt(s, 0).single().map(|d| d.hour() as i64))
                        .flatten()
                })
            })
            .collect::<Vec<_>>(),
    );
    for zone in [None, Some("UTC"), Some("+14:00"), Some("bad-zone")] {
        let actual = eval(
            "hour_from_unixtime",
            vec![ints(input.clone())],
            DataType::Int32,
            zone,
        )
        .unwrap();
        assert_eq!(actual.to_data(), expected.to_data());
    }
    assert_eq!(
        eval(
            "hour_from_unixtime",
            vec![Arc::new(Date32Array::from(vec![0]))],
            DataType::Int32,
            Some("UTC")
        )
        .unwrap_err(),
        "hour_from_unixtime expects int"
    );
}
