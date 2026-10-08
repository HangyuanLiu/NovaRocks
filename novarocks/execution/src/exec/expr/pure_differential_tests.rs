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

//! The harness proved on owners that already exist, and the to-do inventory
//! of owners that do not.

use super::aggregate::{
    AggregateDiffSpec, AggregateDiffSummary, aggregate_pure_owner_status,
    assert_aggregate_matches_v1,
};
use super::generate::{InputGenerator, InputProfile, TextProfile, sql_date_range};
use super::*;
use arrow::array::{
    Float64Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::datatypes::TimeUnit;

const ROWS: usize = 256;

fn value(data_type: DataType) -> FunctionValueType {
    FunctionValueType::new(data_type, true)
}

fn largeint(nullable: bool) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        nullable,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}

fn timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

fn generated(seed: u64, value_type: &FunctionValueType, profile: &InputProfile) -> ArrayRef {
    InputGenerator::new(seed).column(value_type, ROWS, profile)
}

fn int64_constant(value: i64) -> ConstantValue {
    constant(
        FunctionValueType::new(DataType::Int64, false),
        Arc::new(Int64Array::from(vec![value])),
    )
}

/// One evidence line per proven case; run with `--nocapture` to collect.
fn scalar_ledger(summary: ScalarDiffSummary) -> ScalarDiffSummary {
    println!(
        "scalar {} [{}] legacy `{}` -> {:?} (nullable {}): rows={} selections={} \
         legacy_batch_errors={} attributed_row_errors={} null_results={}",
        summary.function.as_str(),
        summary.overload.as_str(),
        summary.legacy_name,
        summary.result_type.data_type,
        summary.result_type.nullable,
        summary.rows,
        summary.selections,
        summary.legacy_batch_errors,
        summary.attributed_row_errors,
        summary.null_results,
    );
    summary
}

fn aggregate_ledger(summary: AggregateDiffSummary) -> AggregateDiffSummary {
    println!(
        "aggregate {} [{}] -> {:?}: pure state {:?}, legacy state {:?}, rows={} groups={} \
         partitions={} matched_failures={} null_results={}",
        summary.function.as_str(),
        summary.overload.as_str(),
        summary.result_type.data_type,
        summary.pure_state_type.data_type,
        summary.legacy_intermediate_type,
        summary.rows,
        summary.groups,
        summary.partitions,
        summary.matched_failures,
        summary.null_results,
    );
    summary
}

// ---------------------------------------------------------------------------
// Generators
// ---------------------------------------------------------------------------

#[test]
fn pure_differential_generators_are_seed_deterministic() {
    let profile = InputProfile::default();
    for value_type in [
        value(DataType::Int8),
        value(DataType::Int64),
        value(DataType::UInt32),
        value(DataType::Float32),
        value(DataType::Float64),
        value(DataType::Decimal128(18, 3)),
        value(DataType::Decimal256(60, 10)),
        largeint(true),
        value(DataType::Utf8),
        value(DataType::Binary),
        value(DataType::Boolean),
        value(DataType::Date32),
        value(timestamp()),
    ] {
        let first = generated(7, &value_type, &profile);
        let second = generated(7, &value_type, &profile);
        let other = generated(8, &value_type, &profile);
        assert_eq!(first.to_data(), second.to_data(), "{value_type:?}");
        assert_ne!(first.to_data(), other.to_data(), "{value_type:?}");
        assert!(novarocks_type_contract::arrow_data_types_exact(
            first.data_type(),
            &value_type.data_type
        ));
    }
}

#[test]
fn pure_differential_generators_cover_domain_boundaries_and_null_density() {
    let profile = InputProfile::default().with_null_ratio(0.0);
    let rows = 4096;
    let column = |value_type: FunctionValueType, profile: &InputProfile| {
        InputGenerator::new(11).column(&value_type, rows, profile)
    };

    let ints = column(value(DataType::Int8), &profile);
    let ints = ints.as_primitive::<Int8Type>();
    for boundary in [i8::MIN, -1, 0, 1, i8::MAX] {
        assert!(ints.values().contains(&boundary), "Int8 misses {boundary}");
    }
    let wide = column(value(DataType::Int64), &profile);
    let wide = wide.as_primitive::<Int64Type>();
    assert!(wide.values().contains(&i64::MIN) && wide.values().contains(&i64::MAX));

    let floats = column(value(DataType::Float64), &profile);
    let floats = floats.as_primitive::<Float64Type>();
    let bits = floats
        .values()
        .iter()
        .map(|v| v.to_bits())
        .collect::<Vec<_>>();
    assert!(floats.values().iter().any(|v| v.is_nan()));
    assert!(bits.contains(&(-0.0f64).to_bits()) && bits.contains(&0.0f64.to_bits()));
    assert!(floats.values().contains(&f64::INFINITY));
    assert!(floats.values().contains(&f64::NEG_INFINITY));
    assert!(floats.values().iter().any(|v| v.is_subnormal()));

    let decimals = column(value(DataType::Decimal128(9, 2)), &profile);
    let decimals = decimals.as_primitive::<Decimal128Type>();
    assert!(decimals.values().iter().all(|v| v.abs() <= 999_999_999));
    assert!(decimals.values().contains(&999_999_999) && decimals.values().contains(&-999_999_999));
    assert!(decimals.values().contains(&50), "the 0.5 tie is drawn");

    let large = column(largeint(false), &profile);
    let large = large.as_fixed_size_binary();
    assert!((0..large.len()).any(|row| large.value(row) == i128::MIN.to_be_bytes()));

    let text = column(value(DataType::Utf8), &profile);
    let text = text.as_string::<i32>();
    assert!((0..text.len()).any(|row| text.value(row).is_empty()));
    assert!((0..text.len()).any(|row| text.value(row).len() > text.value(row).chars().count()));
    let dates_text = column(
        value(DataType::Utf8),
        &profile.clone().with_text(TextProfile::DateText),
    );
    let dates_text = dates_text.as_string::<i32>();
    assert!((0..dates_text.len()).any(|row| dates_text.value(row) == "2000-02-29"));

    let (min_day, max_day) = sql_date_range();
    assert_eq!((min_day, max_day), (-719_528, 2_932_896));
    let dates = column(value(DataType::Date32), &profile);
    let dates = dates.as_primitive::<Date32Type>();
    assert!(
        dates
            .values()
            .iter()
            .all(|d| (min_day..=max_day).contains(d))
    );
    assert!(dates.values().contains(&min_day) && dates.values().contains(&max_day));

    let stamps = column(value(timestamp()), &profile);
    let stamps = stamps
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    assert!(stamps.values().contains(&253_402_300_799_999_999));

    // NULL density follows the ratio; a non-nullable type never gets NULL.
    let nullable = column(
        value(DataType::Int32),
        &InputProfile::default().with_null_ratio(0.5),
    );
    let ratio = nullable.null_count() as f64 / rows as f64;
    assert!((0.45..0.55).contains(&ratio), "{ratio}");
    let required = column(
        FunctionValueType::new(DataType::Int32, false),
        &InputProfile::default().with_null_ratio(0.5),
    );
    assert_eq!(required.null_count(), 0);
    let none = column(
        value(DataType::Utf8),
        &InputProfile::default()
            .with_null_ratio(0.0)
            .with_boundary_ratio(0.0),
    );
    assert_eq!(none.null_count(), 0);
}

#[test]
fn pure_differential_selections_and_groups_are_well_formed() {
    let mut generator = InputGenerator::new(3);
    for rows in [2, 3, 17, 256] {
        for _ in 0..20 {
            let selected = generator.selection(rows, 0.5);
            assert!(!selected.is_empty() && selected.len() < rows);
            assert!(selected.windows(2).all(|pair| pair[0] < pair[1]));
            assert!(selected.iter().all(|row| *row < rows));
        }
    }
    let groups = generator.group_ids(100, 7);
    assert_eq!(groups.len(), 100);
    for group in 0..7 {
        assert!(groups.contains(&group));
    }
}

// ---------------------------------------------------------------------------
// The error rule on synthetic outcomes
// ---------------------------------------------------------------------------

fn ints(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn facts<'a>(result: &'a FunctionValueType, rows: &'a [usize]) -> SelectionComparison<'a> {
    SelectionComparison {
        result_type: result,
        floats: FloatComparison::Exact,
        messages: ErrorMessageCheck::LegacyContainsPure,
        rows,
    }
}

#[test]
fn pure_differential_error_rule_accepts_exact_attribution_and_rejects_every_violation() {
    let result = value(DataType::Int64);
    let rows = [0, 1, 2];
    // Legacy fails the batch because of row 1 only; pure raises at ordinal 1.
    let legacy_row = |row: usize| match row {
        1 => Err("overflow in row: value 9".to_string()),
        row => Ok(ints(&[Some(row as i64 * 10)])),
    };
    let exact = PureSelection {
        values: ints(&[Some(0), None, Some(20)]),
        errors: vec![RowDataError::new(1, "overflow in row")],
    };
    let mut found = Vec::new();
    let outcome = compare_selection(
        facts(&result, &rows),
        Err("batch failed: overflow in row: value 9".into()),
        legacy_row,
        &exact,
        &mut found,
    );
    assert!(found.is_empty(), "{found:?}");
    assert!(outcome.legacy_batch_error);
    assert_eq!(outcome.attributed_row_errors, 1);

    // A swallowed required error.
    let swallowed = PureSelection {
        values: ints(&[Some(0), None, Some(20)]),
        errors: Vec::new(),
    };
    let mut found = Vec::new();
    compare_selection(
        facts(&result, &rows),
        Err("batch failed".into()),
        legacy_row,
        &swallowed,
        &mut found,
    );
    assert!(
        found
            .iter()
            .any(|line| line.contains("required error swallowed")),
        "{found:?}"
    );

    // An extra visible error on a row the legacy call evaluates.
    let extra = PureSelection {
        values: ints(&[None, None, Some(20)]),
        errors: vec![
            RowDataError::new(0, "invented"),
            RowDataError::new(1, "overflow in row"),
        ],
    };
    let mut found = Vec::new();
    compare_selection(
        facts(&result, &rows),
        Err("batch failed".into()),
        legacy_row,
        &extra,
        &mut found,
    );
    assert!(
        found
            .iter()
            .any(|line| line.contains("row 0: pure raised `invented`")),
        "{found:?}"
    );

    // A pure row error where the legacy batch succeeded.
    let mut found = Vec::new();
    compare_selection(
        facts(&result, &rows),
        Ok(ints(&[Some(0), Some(10), Some(20)])),
        legacy_row,
        &exact,
        &mut found,
    );
    assert!(
        found
            .iter()
            .any(|line| line.contains("where the legacy batch succeeded")),
        "{found:?}"
    );

    // A diagnostic that does not carry the pure message.
    let mut found = Vec::new();
    compare_selection(
        facts(&result, &rows),
        Err("batch failed".into()),
        |row| match row {
            1 => Err("something else".to_string()),
            row => Ok(ints(&[Some(row as i64 * 10)])),
        },
        &exact,
        &mut found,
    );
    assert!(
        found.iter().any(|line| line.contains("does not contain")),
        "{found:?}"
    );

    // A batch error no single row reproduces.
    let mut found = Vec::new();
    compare_selection(
        facts(&result, &rows),
        Err("batch-only failure".into()),
        |row| Ok(ints(&[Some(row as i64 * 10)])),
        &PureSelection {
            values: ints(&[Some(0), Some(10), Some(20)]),
            errors: Vec::new(),
        },
        &mut found,
    );
    assert!(
        found.iter().any(|line| line.contains("unattributable")),
        "{found:?}"
    );

    // Values, NULLs and carrier types are compared exactly.
    let mut found = Vec::new();
    compare_selection(
        facts(&result, &rows),
        Ok(ints(&[Some(0), None, Some(21)])),
        legacy_row,
        &PureSelection {
            values: ints(&[Some(0), Some(10), Some(20)]),
            errors: Vec::new(),
        },
        &mut found,
    );
    assert_eq!(found.len(), 2, "{found:?}");
    let mut found = Vec::new();
    compare_selection(
        facts(&result, &rows),
        Ok(Arc::new(Int32Array::from(vec![0, 10, 20]))),
        legacy_row,
        &PureSelection {
            values: ints(&[Some(0), Some(10), Some(20)]),
            errors: Vec::new(),
        },
        &mut found,
    );
    assert!(
        found.iter().any(|line| line.contains("result type")),
        "{found:?}"
    );
}

#[test]
fn pure_differential_float_comparison_is_exact_unless_tolerance_is_requested() {
    let left: ArrayRef = Arc::new(Float64Array::from(vec![0.0, f64::NAN, 1.0]));
    let right: ArrayRef = Arc::new(Float64Array::from(vec![
        -0.0,
        -f64::NAN,
        1.0 + f64::EPSILON,
    ]));
    assert!(compare_value(&left, 0, &right, 0, FloatComparison::Exact).is_err());
    assert_eq!(
        compare_value(&left, 1, &right, 1, FloatComparison::Exact),
        Ok(false)
    );
    assert!(compare_value(&left, 2, &right, 2, FloatComparison::Exact).is_err());
    let tolerance = FloatComparison::Tolerance {
        absolute: 0.0,
        relative: 1e-12,
    };
    assert_eq!(compare_value(&left, 2, &right, 2, tolerance), Ok(false));
}

// ---------------------------------------------------------------------------
// Proofs on existing owners
// ---------------------------------------------------------------------------

#[test]
fn pure_differential_abs_matches_v1_for_every_selected_profile() {
    let profile = InputProfile::default();
    for (seed, source) in [
        value(DataType::Int8),
        value(DataType::Int16),
        value(DataType::Int32),
        value(DataType::Int64),
        largeint(true),
        value(DataType::Float32),
        value(DataType::Float64),
        value(DataType::Decimal128(18, 3)),
    ]
    .into_iter()
    .enumerate()
    {
        for nullable in [true, false] {
            let source = FunctionValueType {
                nullable,
                ..source.clone()
            };
            let values = generated(100 + seed as u64, &source, &profile);
            let summary = scalar_ledger(assert_scalar_matches_v1(
                ScalarDiffSpec::new("abs")
                    .typed_column(source.clone(), values)
                    .sparse_selections(3, seed as u64),
            ));
            assert_eq!(summary.selections, 4);
            assert_eq!(summary.result_type.nullable, nullable);
        }
    }
    // The frozen widening return of the smallest integer profile.
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("abs")
            .column(Arc::new(arrow::array::Int8Array::from(vec![
                Some(i8::MIN),
                None,
                Some(-7),
            ])))
            .expect_result_type(value(DataType::Int16)),
    );
}

#[test]
fn pure_differential_round_matches_v1_with_and_without_constant_scale() {
    let profile = InputProfile::default();
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for (seed, source) in [
            value(DataType::Float64),
            value(DataType::Decimal128(18, 4)),
            value(DataType::Int64),
        ]
        .into_iter()
        .enumerate()
        {
            let values = generated(200 + seed as u64, &source, &profile);
            scalar_ledger(assert_scalar_matches_v1(
                ScalarDiffSpec::new("round")
                    .typed_column(source.clone(), values.clone())
                    .decimal_overflow(policy),
            ));
            for scale in [-2, 0, 2] {
                scalar_ledger(assert_scalar_matches_v1(
                    ScalarDiffSpec::new("round")
                        .typed_column(source.clone(), values.clone())
                        .constant(int64_constant(scale))
                        .decimal_overflow(policy),
                ));
            }
        }
    }
}

#[test]
fn pure_differential_numeric_binary_matches_v1_for_signed_and_float_pairs() {
    let profile = InputProfile::default();
    let sources = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ];
    let mut seed = 300;
    for name in ["atan2", "fmod", "pow"] {
        for left in &sources {
            for right in &sources {
                seed += 1;
                let left_type = value(left.clone());
                let right_type = value(right.clone());
                scalar_ledger(assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .typed_column(left_type.clone(), generated(seed, &left_type, &profile))
                        .typed_column(
                            right_type.clone(),
                            generated(seed + 1000, &right_type, &profile),
                        )
                        .sparse_selections(1, seed),
                ));
            }
        }
    }
}

#[test]
fn pure_differential_crc32_matches_v1_over_mixed_text() {
    for (seed, text) in [TextProfile::Mixed, TextProfile::Ascii, TextProfile::Numeric]
        .into_iter()
        .enumerate()
    {
        let profile = InputProfile::default().with_text(text);
        let source = value(DataType::Utf8);
        scalar_ledger(assert_scalar_matches_v1(
            ScalarDiffSpec::new("crc32").typed_column(
                source.clone(),
                generated(400 + seed as u64, &source, &profile),
            ),
        ));
    }
}

#[test]
fn pure_differential_makedate_matches_v1_across_valid_and_invalid_parts() {
    let mut generator = InputGenerator::new(500);
    let mut years = Vec::with_capacity(ROWS);
    let mut days = Vec::with_capacity(ROWS);
    for _ in 0..ROWS {
        let rng = generator.rng();
        years.push((!rng.chance(0.1)).then(|| rng.range_i64(-3, 10_003)));
        days.push((!rng.chance(0.1)).then(|| rng.range_i64(-2, 370)));
    }
    years.extend([
        Some(0),
        Some(9999),
        Some(2000),
        Some(1900),
        Some(i64::MIN),
        Some(i64::MAX),
    ]);
    days.extend([Some(1), Some(365), Some(366), Some(366), Some(1), Some(1)]);
    scalar_ledger(assert_scalar_matches_v1(
        ScalarDiffSpec::new("makedate")
            .column(ints(&years))
            .column(ints(&days)),
    ));
}

#[test]
fn pure_differential_calendar_parts_match_v1_for_every_source_carrier() {
    let names = [
        "year",
        "month",
        "day",
        "dayofmonth",
        "hour",
        "minute",
        "second",
        "dayofweek",
        "yearweek",
        "dayofyear",
        "weekofyear",
        "quarter",
    ];
    let datetime_text = InputProfile::default().with_text(TextProfile::DateTimeText);
    let date_text = InputProfile::default().with_text(TextProfile::DateText);
    let sources = [
        (value(timestamp()), InputProfile::default()),
        (value(DataType::Date32), InputProfile::default()),
        (value(DataType::Utf8), datetime_text),
        (value(DataType::Utf8), date_text),
    ];
    for (index, name) in names.into_iter().enumerate() {
        for (offset, (source, profile)) in sources.iter().enumerate() {
            let seed = 600 + (index * 10 + offset) as u64;
            match run_scalar_differential(
                &ScalarDiffSpec::new(name)
                    .typed_column(source.clone(), generated(seed, source, profile))
                    .sparse_selections(1, seed),
            ) {
                Ok(summary) => {
                    scalar_ledger(summary);
                }
                // A carrier SQL does not bind for this name is not a lane.
                Err(DifferentialFailure::Resolution { error, .. }) => {
                    println!("skip {name} over {:?}: {error}", source.data_type);
                }
                Err(failure) => panic!("{name} over {:?}: {failure}", source.data_type),
            }
        }
    }
}

#[test]
fn pure_differential_threads_constant_forms_semantics_and_comparison_options() {
    let profile = InputProfile::default();
    let source = value(DataType::Float64);
    let values = generated(450, &source, &profile);
    // The same constant through a legacy literal node and a pool node.
    for form in [LegacyConstantForm::Literal, LegacyConstantForm::Pool] {
        scalar_ledger(assert_scalar_matches_v1(
            ScalarDiffSpec::new("round")
                .typed_column(source.clone(), Arc::clone(&values))
                .constant_array(Arc::new(Int64Array::from(vec![1])))
                .legacy_constants(form),
        ));
    }
    // An all-constant call sized by `constant_rows`.
    let summary = scalar_ledger(assert_scalar_matches_v1(
        ScalarDiffSpec::new("crc32")
            .constant_array(Arc::new(StringArray::from(vec!["NovaRocks"])))
            .constant_rows(5),
    ));
    assert_eq!(summary.rows, 5);
    // Statement parameters reach both paths; owners without dependencies
    // receive no environment references.
    scalar_ledger(assert_scalar_matches_v1(
        ScalarDiffSpec::new("abs")
            .typed_column(source.clone(), Arc::clone(&values))
            .semantics(DiffSemantics {
                allow_throw_exception: true,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                time_zone: None,
                extra: Vec::new(),
            })
            .time_zone("Asia/Shanghai")
            .parameter(SemanticParameterValue::StatementStartUtc(
                1_700_000_000_000_000,
            )),
    ));
    let failure = run_scalar_differential(
        &ScalarDiffSpec::new("abs")
            .typed_column(source.clone(), Arc::clone(&values))
            .parameter(SemanticParameterValue::AllowThrowException(true)),
    )
    .unwrap_err();
    assert!(
        matches!(failure, DifferentialFailure::InvalidSpec(_)),
        "{failure}"
    );
    scalar_ledger(assert_scalar_matches_v1(
        ScalarDiffSpec::new("atan2")
            .typed_column(source.clone(), Arc::clone(&values))
            .typed_column(source.clone(), generated(451, &source, &profile))
            .float_comparison(FloatComparison::Tolerance {
                absolute: 0.0,
                relative: 0.0,
            }),
    ));
}

#[test]
fn pure_differential_url_decode_attributes_legacy_batch_errors_to_pure_rows() {
    let values: ArrayRef = Arc::new(StringArray::from(vec![
        Some("plain"),
        Some("a%20b"),
        Some("%zz"),
        None,
        Some("%E4%B8%AD"),
        Some("%C3"),
        Some("tail%"),
        Some("+%2B"),
    ]));
    for allow in [false, true] {
        let summary = scalar_ledger(assert_scalar_matches_v1(
            ScalarDiffSpec::new("url_decode")
                .column(Arc::clone(&values))
                .allow_throw_exception(allow)
                .sparse_selections(6, 9),
        ));
        // At least one selection exercised the row attribution path.
        assert!(summary.legacy_batch_errors > 0, "{summary:?}");
        assert!(summary.attributed_row_errors > 0, "{summary:?}");
    }
    // Row identity alone, without diagnostic text, is the relaxed check.
    assert_scalar_matches_v1(
        ScalarDiffSpec::new("url_decode")
            .column(values)
            .ignore_error_messages(),
    );
}

#[test]
fn pure_differential_aggregates_match_v1_single_and_two_phase() {
    let profile = InputProfile::default();
    let rows = ROWS;
    let mut generator = InputGenerator::new(700);
    // Group 8 has no rows and is emitted from empty states.
    let groups = generator.group_ids(rows, 8);
    let cases: Vec<(&str, FunctionValueType)> = vec![
        ("sum", value(DataType::Int8)),
        ("sum", value(DataType::Int32)),
        ("sum", value(DataType::Boolean)),
        ("sum", value(DataType::Decimal128(20, 2))),
        ("sum", value(DataType::Float64)),
        ("count", value(DataType::Utf8)),
        ("count", value(DataType::Float64)),
        ("min", value(DataType::Int32)),
        ("max", value(DataType::Int64)),
        ("min", value(DataType::Float64)),
        ("max", value(DataType::Utf8)),
        ("min", value(DataType::Date32)),
        ("max", value(timestamp())),
        ("max", value(DataType::Decimal128(18, 3))),
    ];
    for (index, (name, source)) in cases.into_iter().enumerate() {
        let values = generated(710 + index as u64, &source, &profile);
        // Float SUM is an unordered accumulation; its exact equality is not a
        // contract, so it is compared with a relative tolerance.
        let floats = if name == "sum" && source.data_type == DataType::Float64 {
            FloatComparison::Tolerance {
                absolute: 0.0,
                relative: 1e-9,
            }
        } else {
            FloatComparison::Exact
        };
        let summary = aggregate_ledger(assert_aggregate_matches_v1(
            AggregateDiffSpec::new(name)
                .typed_column(source.clone(), values)
                .grouped(groups.clone(), 9)
                .partitions(3, index as u64)
                .float_comparison(floats),
        ));
        assert_eq!(summary.groups, 9);
    }
    // BIGINT SUM over INT-range values compares values; full-range BIGINT
    // overflows and is covered by the overflow case below.
    let narrow = generated(798, &value(DataType::Int32), &profile);
    let wide = arrow::compute::cast(&narrow, &DataType::Int64).unwrap();
    aggregate_ledger(assert_aggregate_matches_v1(
        AggregateDiffSpec::new("sum")
            .column(Arc::clone(&wide))
            .grouped(groups.clone(), 9),
    ));
    // count(*), a scalar aggregate over one global group, and a constant
    // argument broadcast by the legacy packing and read once by the pure path.
    aggregate_ledger(assert_aggregate_matches_v1(
        AggregateDiffSpec::new("count")
            .constant_rows(rows)
            .grouped(groups.clone(), 9),
    ));
    aggregate_ledger(assert_aggregate_matches_v1(
        AggregateDiffSpec::new("sum").column(wide),
    ));
    aggregate_ledger(assert_aggregate_matches_v1(
        AggregateDiffSpec::new("sum")
            .constant(int64_constant(3))
            .constant_rows(rows)
            .grouped(groups, 9)
            .semantics(DiffSemantics {
                allow_throw_exception: true,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                ..DiffSemantics::default()
            }),
    ));
}

#[test]
fn pure_differential_sum_overflow_is_a_failure_on_both_paths() {
    let values = ints(&[Some(i64::MAX), Some(1), Some(i64::MAX), None]);
    let spec = AggregateDiffSpec::new("sum")
        .column(values)
        .partitions(2, 1);
    let summary = assert_aggregate_matches_v1(spec.clone().ignore_error_messages());
    assert_eq!(summary.matched_failures, 2, "{summary:?}");
    let strict = assert_aggregate_matches_v1(spec);
    assert_eq!(strict.matched_failures, 2, "{strict:?}");
}

// ---------------------------------------------------------------------------
// Missing-owner inventory
// ---------------------------------------------------------------------------

fn column_argument(data_type: DataType) -> DiffArgument {
    let value_type = value(data_type);
    let values = InputGenerator::new(1).column(&value_type, 8, &InputProfile::default());
    DiffArgument::Column { value_type, values }
}

#[test]
fn pure_differential_reports_missing_owners_for_the_census() {
    let scalar_census: Vec<(&str, Vec<DiffArgument>)> = vec![
        (
            "ifnull",
            vec![
                column_argument(DataType::Int64),
                column_argument(DataType::Int64),
            ],
        ),
        ("murmur_hash3_32", vec![column_argument(DataType::Utf8)]),
        (
            "days_add",
            vec![
                column_argument(timestamp()),
                column_argument(DataType::Int64),
            ],
        ),
        ("hour_from_unixtime", vec![column_argument(DataType::Int64)]),
        ("time_to_sec", vec![column_argument(timestamp())]),
        ("unhex", vec![column_argument(DataType::Utf8)]),
        ("to_binary", vec![column_argument(DataType::Utf8)]),
        ("money_format", vec![column_argument(DataType::Int64)]),
    ];
    let mut inventory = Vec::new();
    for (name, arguments) in scalar_census {
        let mut spec = ScalarDiffSpec::new(name);
        spec.arguments = arguments;
        let failure = run_scalar_differential(&spec).expect_err(name);
        let Some(overload) = failure.missing_overload() else {
            panic!("{name}: expected MissingPureImplementation, got {failure}");
        };
        assert_eq!(
            scalar_pure_owner_status(name, &spec.arguments)
                .unwrap_err()
                .missing_overload(),
            Some(overload)
        );
        inventory.push(failure.to_string());
    }
    let aggregate_census: Vec<(&str, Vec<DataType>)> =
        vec![("group_concat", vec![DataType::Utf8, DataType::Utf8])];
    for (name, types) in aggregate_census {
        let mut spec = AggregateDiffSpec::new(name);
        spec.arguments = types.into_iter().map(column_argument).collect();
        let failure = super::aggregate::run_aggregate_differential(&spec).expect_err(name);
        let Some(overload) = failure.missing_overload() else {
            panic!("{name}: expected MissingPureImplementation, got {failure}");
        };
        assert_eq!(
            aggregate_pure_owner_status(&spec)
                .unwrap_err()
                .missing_overload(),
            Some(overload)
        );
        inventory.push(failure.to_string());
    }
    println!("pure owner census ({} missing):", inventory.len());
    for line in &inventory {
        println!("  {line}");
    }
}

#[test]
fn pure_differential_owner_inventory_covers_the_builtin_catalogue() {
    let inventory = pure_owner_inventory();
    let installed = inventory.iter().filter(|row| row.installed).count();
    let installed_named = |name: &str, kind: CatalogKind| {
        inventory
            .iter()
            .filter(|row| row.name == name && row.kind == kind)
            .map(|row| row.installed)
            .collect::<Vec<_>>()
    };
    assert!(
        installed_named("abs", CatalogKind::Scalar)
            .iter()
            .all(|installed| *installed)
    );
    assert!(
        installed_named("sum", CatalogKind::Aggregate)
            .iter()
            .all(|installed| *installed)
    );
    assert_eq!(installed_named("ifnull", CatalogKind::Scalar), vec![false]);
    assert!(
        installed_named("avg", CatalogKind::Aggregate)
            .iter()
            .all(|installed| *installed)
    );
    let mut missing = std::collections::BTreeMap::<String, usize>::new();
    for row in inventory.iter().filter(|row| !row.installed) {
        *missing
            .entry(format!("{:?}:{}", row.kind, row.name))
            .or_default() += 1;
    }
    println!(
        "pure owner inventory: {installed} of {} declared overloads installed; {} functions missing",
        inventory.len(),
        missing.len()
    );
    for (function, overloads) in &missing {
        println!("  missing {function} ({overloads} overloads)");
    }
}

#[test]
fn pure_differential_rejects_uncoerced_arguments_and_wrong_pins() {
    // makedate coerces its parts to BIGINT; the spec must supply BIGINT.
    let failure = run_scalar_differential(
        &ScalarDiffSpec::new("makedate")
            .column(Arc::new(Int32Array::from(vec![2000])))
            .column(Arc::new(Int32Array::from(vec![60]))),
    )
    .unwrap_err();
    assert!(
        matches!(failure, DifferentialFailure::CoercedArgument { .. }),
        "{failure}"
    );
    let failure = run_scalar_differential(
        &ScalarDiffSpec::new("abs")
            .column(Arc::new(Int16Array::from(vec![1])))
            .expect_result_type(value(DataType::Int16)),
    )
    .unwrap_err();
    assert!(
        matches!(failure, DifferentialFailure::ResultTypePin { .. }),
        "{failure}"
    );
}

#[test]
fn pure_differential_bit_family_matches_v1_every_integer_profile() {
    let profile = InputProfile::default();
    for (index, source) in [
        value(DataType::Int8),
        value(DataType::Int16),
        value(DataType::Int32),
        value(DataType::Int64),
        largeint(true),
    ]
    .into_iter()
    .enumerate()
    {
        for nullable in [true, false] {
            let source = FunctionValueType {
                nullable,
                ..source.clone()
            };
            let left = generated(0xb170 + index as u64, &source, &profile);
            let right = generated(0xb270 + index as u64, &source, &profile);
            for name in ["bitand", "bitor", "bitxor", "bitnot"] {
                let mut spec = ScalarDiffSpec::new(name)
                    .typed_column(source.clone(), left.clone())
                    .sparse_selections(3, index as u64);
                if name != "bitnot" {
                    spec = spec.typed_column(source.clone(), right.clone());
                }
                scalar_ledger(assert_scalar_matches_v1(spec));
            }
            let counts = generated(0xb370 + index as u64, &value(DataType::Int64), &profile);
            for name in [
                "bit_shift_left",
                "bit_shift_right",
                "bit_shift_right_logical",
            ] {
                scalar_ledger(assert_scalar_matches_v1(
                    ScalarDiffSpec::new(name)
                        .typed_column(source.clone(), left.clone())
                        .typed_column(value(DataType::Int64), counts.clone())
                        .sparse_selections(3, index as u64),
                ));
            }
        }
    }
}
