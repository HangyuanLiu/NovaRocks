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

//! Numeric DISTINCT v1 update, sparse Partial and Final merge differential.
//! The shared harness compares Single and Partial -> Final over the same
//! groups, using original-batch Selection for pure partials and gathered v1
//! input. Exact floating comparison keeps the original hash iteration order.
use super::aggregate::{
    AggregateDiffSpec, assert_aggregate_matches_v1, run_aggregate_differential,
};
use super::{DiffSemantics, DifferentialFailure, FloatComparison, HarnessControl};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Decimal256Array, Float64Array, Int64Array,
    new_empty_array, new_null_array,
};
use arrow::datatypes::DataType;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;

const NAMES: [&str; 2] = ["multi_distinct_sum", "multi_distinct_avg"];
fn integer_rows(nullable: bool) -> (ArrayRef, Vec<usize>) {
    let pattern = [
        Some(1),
        Some(1),
        Some(2),
        Some(-3),
        Some(-3),
        Some(4),
        Some(0),
        Some(0),
        None,
        None,
        Some(20),
        Some(-20),
    ];
    let groups = [0, 0, 0, 1, 1, 1, 2, 2, 3, 4, 5, 5];
    let values = (0..516)
        .map(|row| pattern[row % pattern.len()].or_else(|| (!nullable).then_some(9)))
        .collect::<Vec<_>>();
    let mapping = (0..516).map(|row| groups[row % groups.len()]).collect();
    (Arc::new(Int64Array::from(values)), mapping)
}
fn check(name: &str, values: ArrayRef, nullable: bool, mapping: Vec<usize>, seed: u64) {
    let source_type = values.data_type().clone();
    let summary = assert_aggregate_matches_v1(
        AggregateDiffSpec::new(name)
            .typed_column(
                FunctionValueType::new(source_type.clone(), nullable),
                values,
            )
            .grouped(mapping, 8)
            .partitions(7, seed)
            .float_comparison(FloatComparison::Exact),
    );
    assert_eq!(summary.matched_failures, 0, "{name} {source_type:?}");
    assert_eq!(summary.pure_state_type.data_type, DataType::Binary);
    assert_eq!(summary.legacy_intermediate_type, DataType::Binary);
    assert!(summary.null_results >= 4, "two empty groups in both shapes");
    println!(
        "numeric DISTINCT {} [{}] source={source_type:?} nullable={nullable} result={:?} rows={} groups={} partitions={} null_results={}",
        summary.function.as_str(),
        summary.overload.as_str(),
        summary.result_type.data_type,
        summary.rows,
        summary.groups,
        summary.partitions,
        summary.null_results
    );
}

#[test]
fn pure_differential_distinct_numeric_all_integer_float_widths_nullable_profiles() {
    for nullable in [false, true] {
        let (integers, groups) = integer_rows(nullable);
        for (ordinal, ty) in [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ]
        .into_iter()
        .enumerate()
        {
            let values = arrow::compute::cast(&integers, &ty).unwrap();
            for name in NAMES {
                check(
                    name,
                    values.clone(),
                    nullable,
                    groups.clone(),
                    901 + ordinal as u64,
                );
            }
        }
    }
}

#[test]
fn pure_differential_distinct_numeric_boolean_sum_freezes_legacy_binding_drift_and_pure_refusal() {
    for values in [
        Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
        new_empty_array(&DataType::Boolean),
        new_null_array(&DataType::Boolean, 3),
    ] {
        assert_binding_drift_refused("multi_distinct_sum", values, DataType::Int64);
    }
}

#[test]
fn pure_differential_distinct_numeric_decimal128_precisions_scales_and_rounding() {
    for precision in [9, 18, 38] {
        for scale in [0, 2, 6] {
            for nullable in [false, true] {
                let (integers, groups) = integer_rows(nullable);
                let source = integers.as_any().downcast_ref::<Int64Array>().unwrap();
                let values: ArrayRef = Arc::new(
                    Decimal128Array::from(
                        (0..source.len())
                            .map(|row| {
                                if arrow::array::Array::is_null(source, row) {
                                    None
                                } else {
                                    Some(source.value(row) as i128 * 1001)
                                }
                            })
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
                );
                for name in NAMES {
                    check(
                        name,
                        values.clone(),
                        nullable,
                        groups.clone(),
                        911 + scale as u64,
                    );
                }
            }
        }
    }
}

#[test]
fn pure_differential_distinct_numeric_decimal256_sum_freezes_legacy_binding_drift_and_pure_refusal()
{
    for precision in [40, 60, 76] {
        for scale in [0, 2, 6] {
            let ty = DataType::Decimal256(precision, scale);
            for values in [
                Arc::new(
                    Decimal256Array::from(vec![Some(arrow_buffer::i256::from_i128(1001)), None])
                        .with_precision_and_scale(precision, scale)
                        .unwrap(),
                ) as ArrayRef,
                new_empty_array(&ty),
                new_null_array(&ty, 3),
            ] {
                assert_binding_drift_refused("multi_distinct_sum", values, ty.clone());
            }
        }
    }
}

#[test]
fn pure_differential_distinct_numeric_decimal256_avg_precisions_scales_nullable_empty_and_null() {
    for precision in [40, 60, 76] {
        for scale in [0, 2, 6] {
            let ty = DataType::Decimal256(precision, scale);
            for nullable in [false, true] {
                let (integers, groups) = integer_rows(nullable);
                let source = integers.as_any().downcast_ref::<Int64Array>().unwrap();
                let values: ArrayRef = Arc::new(
                    Decimal256Array::from(
                        (0..source.len())
                            .map(|row| {
                                if source.is_null(row) {
                                    None
                                } else {
                                    Some(arrow_buffer::i256::from_i128(
                                        source.value(row) as i128 * 1001,
                                    ))
                                }
                            })
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
                );
                check(
                    "multi_distinct_avg",
                    values,
                    nullable,
                    groups,
                    921 + scale as u64,
                );
            }
            let empty = assert_aggregate_matches_v1(
                AggregateDiffSpec::new("multi_distinct_avg")
                    .column(new_empty_array(&ty))
                    .partitions(11, 922),
            );
            assert_eq!(empty.matched_failures, 0);
            assert_eq!(empty.null_results, 2, "{ty:?} global empty");
            let null = assert_aggregate_matches_v1(
                AggregateDiffSpec::new("multi_distinct_avg")
                    .column(new_null_array(&ty, 8))
                    .grouped(vec![0; 8], 3)
                    .partitions(11, 923),
            );
            assert_eq!(null.matched_failures, 0);
            assert_eq!(null.null_results, 6, "{ty:?} all NULL and empty groups");
        }
    }
}

#[test]
fn pure_differential_distinct_numeric_special_float_keys_and_hash_order_are_exact() {
    let values: ArrayRef = Arc::new(Float64Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(-0.0),
        None,
        Some(f64::NAN),
        Some(f64::from_bits(0x7ff8_0000_0000_0123)),
        None,
        Some(f64::INFINITY),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(1e16),
        Some(1.0),
        Some(-1e16),
        Some(1.0),
        None,
    ]));
    for ty in [DataType::Float32, DataType::Float64] {
        for name in NAMES {
            assert_aggregate_matches_v1(
                AggregateDiffSpec::new(name)
                    .column(arrow::compute::cast(&values, &ty).unwrap())
                    .grouped(vec![0, 0, 0, 0, 1, 1, 1, 2, 2, 3, 4, 4, 4, 4, 5], 7)
                    .partitions(5, 931)
                    .float_comparison(FloatComparison::Exact),
            );
        }
    }
}

#[test]
fn pure_differential_distinct_numeric_all_null_and_global_empty_every_domain() {
    for ty in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(18, 2),
    ] {
        for name in NAMES {
            let empty = assert_aggregate_matches_v1(
                AggregateDiffSpec::new(name)
                    .column(new_empty_array(&ty))
                    .partitions(11, 939),
            );
            assert_eq!(empty.null_results, 2, "{name} {ty:?} global empty");
            let null = assert_aggregate_matches_v1(
                AggregateDiffSpec::new(name)
                    .column(new_null_array(&ty, 8))
                    .grouped(vec![0; 8], 3)
                    .partitions(11, 940),
            );
            assert_eq!(
                null.null_results, 6,
                "{name} {ty:?} all NULL and empty groups"
            );
        }
    }
}

#[test]
fn pure_differential_distinct_numeric_constant_broadcast_and_null_under_both_policies() {
    for policy in [
        DecimalOverflowPolicy::OutputNull,
        DecimalOverflowPolicy::ReportError,
    ] {
        for name in NAMES {
            for value in [Some(7i64), None] {
                let constant = super::constant(
                    FunctionValueType::new(DataType::Int64, value.is_none()),
                    Arc::new(Int64Array::from(vec![value])),
                );
                let result = assert_aggregate_matches_v1(
                    AggregateDiffSpec::new(name)
                        .constant(constant)
                        .constant_rows(513)
                        .grouped((0..513).map(|row| row % 5).collect(), 6)
                        .partitions(7, 951)
                        .semantics(DiffSemantics {
                            decimal_overflow_policy: policy,
                            ..DiffSemantics::default()
                        }),
                );
                assert_eq!(result.matched_failures, 0);
                assert_eq!(result.null_results, if value.is_none() { 12 } else { 2 });
            }
        }
    }
}

/// Both actual paths are checked independently: the differential harness
/// returns before pure preparation when the legacy binding already drifts.
fn assert_binding_drift_refused(name: &str, values: ArrayRef, raw_output: DataType) {
    use novarocks_functions::builtin::catalogue::builtin_engine_function_catalog;
    use novarocks_functions::{
        AggregateKernelPhase, AggregatePreparationOptions, CallArgumentUses, CallEffectInput,
        FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionResultType,
        FunctionSpecializationFailure, KernelFailure, PureCallPreparation, ScopedExpressionEffects,
    };
    use novarocks_type_contract::{CallProofScope, ExpressionUseId, SemanticParameters};
    let source = FunctionValueType::new(values.data_type().clone(), true);
    let spec = AggregateDiffSpec::new(name).typed_column(source.clone(), values);
    let failure = run_aggregate_differential(&spec).unwrap_err();
    match failure {
        DifferentialFailure::LegacyUnavailable {
            name: actual,
            reason,
        } => {
            assert_eq!(actual, name);
            let expected = format!(
                "bind aggregate `{name}`: prepare aggregate `{name}` overload `builtin.aggregate/{name}/derived-v1`: legacy aggregate type drift: implementation intermediate=Binary, output={raw_output:?}; catalog intermediate=Binary, output=Float64"
            );
            assert_eq!(reason, expected);
        }
        other => panic!("expected frozen legacy binding drift for {name}: {other}"),
    }
    let catalog = builtin_engine_function_catalog();
    let arguments = [FunctionArgument::Value {
        value_type: source.clone(),
        constant: None,
    }];
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_user(name, FunctionKind::Aggregate, request, &HarnessControl)
        .unwrap();
    assert!(
        matches!(&bound.selected.result_type, FunctionResultType::Scalar(result) if result.data_type == DataType::Float64)
    );
    let context = super::harness_context();
    let uses = [Some(ExpressionUseId::new(1))];
    let parameters = SemanticParameters::try_new([]).unwrap();
    let selected = Arc::new(bound.selected);
    let input = CallEffectInput {
        context,
        argument_uses: CallArgumentUses::SelectedChannels(&uses),
        function_id: &bound.function_id,
        kind: FunctionKind::Aggregate,
        selected: &selected,
        request: FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: 1,
            expected_result_type: None,
        },
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        proof_scope: CallProofScope::Domain(context.domain),
    };
    let failure = match catalog.prepare_fresh_selected(
        input,
        selected.clone(),
        PureCallPreparation::Aggregate {
            arguments: ScopedExpressionEffects::pure_value(context),
            options: AggregatePreparationOptions {
                phase: AggregateKernelPhase::Single,
                distinct: false,
                order_keys: Arc::from([]),
                state_input_type: None,
            },
        },
        &HarnessControl,
    ) {
        Err(error) => error,
        Ok(_) => panic!("{name} must refuse its unsupported legacy drift profile"),
    };
    match failure {
        FunctionSpecializationFailure::Kernel(KernelFailure::InvalidProgram(diagnostic)) => {
            assert_eq!(
                diagnostic.message(),
                format!(
                    "builtin.aggregate/{name}/v1 has no installed numeric DISTINCT input profile for {:?}",
                    source.data_type
                )
            )
        }
        other => panic!("expected typed named InvalidProgram for {name}: {other}"),
    }
}
