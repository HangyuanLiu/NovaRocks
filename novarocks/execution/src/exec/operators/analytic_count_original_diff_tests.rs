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
//! Installed COUNT AggregateWindowV1 vs original analytic count and frame author.
//! Mount as a cfg(test) child of analytic_shared, no production visibility changes.
use super::*;
use arrow::array::{Int64Array, StringArray};
use arrow::array::{DictionaryArray, Int8Array, NullArray};
use arrow::datatypes::Int8Type;
use novarocks_functions as f;
use novarocks_type_contract as t;
use std::time::Duration;
pub(super) struct C;
impl t::PureCompileControl for C {
    fn checkpoint(&self, _: t::CompilePhase, _: u32) -> Result<(), t::CompileControlError> {
        Ok(())
    }
}
impl f::KernelEvaluationControl for C {
    fn checkpoint(&self, _: u32) -> Result<(), f::KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), f::KernelFailure> {
        panic!("COUNT never waits")
    }
}
pub(super) fn prepare(
    source: Option<&ArrayRef>,
    running: bool,
    ignore: bool,
) -> Arc<dyn f::PreparedWindowKernel> {
    let catalog = f::builtin::catalogue::builtin_engine_function_catalog();
    let arguments = source
        .into_iter()
        .map(|a| f::FunctionArgument::Value {
            value_type: t::FunctionValueType::new(a.data_type().clone(), true),
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = f::FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    };
    let resolved = catalog
        .resolve_bound_user("count", t::FunctionKind::Aggregate, request, &C)
        .unwrap();
    let selected = Arc::new(resolved.selected);
    let context = t::ExpressionEffectContext {
        use_id: t::ExpressionUseId::new(31),
        domain: t::EvaluationDomainId::new(9),
        demand: t::EvaluationDemand::Value,
    };
    let uses = (0..arguments.len())
        .map(|_| Some(t::ExpressionUseId::new(32)))
        .collect::<Vec<_>>();
    let parameters = t::SemanticParameters::try_new([]).unwrap();
    let input = f::CallEffectInput {
        context,
        argument_uses: f::CallArgumentUses::SelectedChannels(&uses),
        function_id: &resolved.function_id,
        kind: t::FunctionKind::Aggregate,
        selected: &selected,
        request: f::FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: arguments.len(),
            expected_result_type: None,
        },
        environment: &[],
        parameters: &parameters,
        decimal_overflow_policy: t::DecimalOverflowPolicy::OutputNull,
        proof_scope: t::CallProofScope::Domain(context.domain),
    };
    let frame = running.then_some(t::WindowFrame {
        units: t::WindowFrameUnits::Rows,
        start: t::WindowBound::Preceding(1),
        end: t::WindowBound::CurrentRow,
        exclusion: t::WindowFrameExclusion::NoOthers,
    });
    let value = catalog
        .prepare_fresh_selected(
            input,
            selected.clone(),
            f::PureCallPreparation::AggregateWindow {
                arguments: f::ScopedExpressionEffects::pure_value(context),
                options: f::AggregateWindowPreparationOptions {
                    aggregate: f::AggregatePreparationOptions {
                        state_interpretation: None,
                        phase: f::AggregateKernelPhase::Single,
                        distinct: false,
                        order_keys: Arc::from([]),
                        state_input_type: None,
                    },
                    window: f::WindowCallOptions::try_new(frame, ignore, &C).unwrap(),
                },
            },
            &C,
        )
        .unwrap();
    assert_eq!(
        value.implementation().abi,
        f::PureKernelAbi::AggregateWindowV1
    );
    match value.into_prepared() {
        f::PreparedPureKernel::Window(p) => p,
        _ => panic!("COUNT actual window ABI"),
    }
}
pub(super) fn compare(source: Option<ArrayRef>, n: usize) {
    for running in [false, true] {
        for ignore in [false, true] {
            let window = running.then_some(WindowFrame {
                start: Some(WindowBoundary::Preceding(1)),
                end: Some(WindowBoundary::CurrentRow),
                window_type: WindowType::Rows,
            });
            let partitions = if n == 0 {
                vec![]
            } else if n < 3 {
                vec![(0, n)]
            } else {
                vec![(0, 2), (2, n)]
            };
            let ctx = PartitionWindowContext::new(&partitions, &[], window.as_ref()).unwrap();
            let old_args = source.iter().cloned().collect::<Vec<_>>();
            let legacy = compute_count(&old_args, &ctx, n).unwrap();
            let legacy = legacy.as_any().downcast_ref::<Int64Array>().unwrap();
            let prepared = prepare(source.as_ref(), running, ignore);
            for (part, (start, end)) in partitions.iter().copied().enumerate() {
                let rows = end - start;
                let arrays = source
                    .iter()
                    .map(|v| v.slice(start, rows))
                    .collect::<Vec<_>>();
                let args = arrays
                    .iter()
                    .map(f::EvaluatedArgument::Column)
                    .collect::<Vec<_>>();
                let peers = ctx
                    .peer_groups(part)
                    .unwrap()
                    .iter()
                    .map(|(s, e)| f::WindowRowRange {
                        start: s - start,
                        end: e - start,
                    })
                    .collect::<Vec<_>>();
                let frames = ctx
                    .frames(part)
                    .unwrap()
                    .iter()
                    .map(|(s, e)| f::WindowRowRange {
                        start: s - start,
                        end: e - start,
                    })
                    .collect::<Vec<_>>();
                let full =
                    f::FullPartitionWindowInput::try_new(prepared.contract(), rows, &args, &[], &C)
                        .unwrap();
                let input = f::WindowPartitionInput::try_new(full, &peers, &frames, &C).unwrap();
                let mut evaluator =
                    f::WindowEvaluationPartition::begin(prepared.clone(), input, &C).unwrap();
                for ordinals in [
                    (0..rows).collect::<Vec<_>>(),
                    (0..rows).filter(|i| i % 2 == 0).collect::<Vec<_>>(),
                    vec![],
                ] {
                    let selection = f::Selection::try_sparse(rows, &ordinals).unwrap();
                    let out = evaluator.evaluate(selection, rows, &C).unwrap();
                    assert!(out.errors().is_empty());
                    assert_eq!(out.values().data_type(), &DataType::Int64);
                    assert_eq!(out.values().null_count(), 0);
                    let actual = out.values().as_any().downcast_ref::<Int64Array>().unwrap();
                    for (i, row) in ordinals.iter().copied().enumerate() {
                        assert_eq!(actual.value(i), legacy.value(start + row));
                    }
                }
            }
        }
    }
}
#[test]
fn pure_differential_count_over_original_star_actual_signature_frames_sparse_empty() {
    for n in [0, 1, 5, 319] {
        compare(None, n);
    }
}
#[test]
fn pure_differential_count_over_original_expr_actual_signature_frames_sparse_null_slice() {
    let source: ArrayRef = Arc::new(Int64Array::from(vec![
        Some(99),
        Some(1),
        None,
        Some(3),
        None,
        Some(5),
        Some(77),
    ]));
    for input in [source.slice(1, 5), source.slice(2, 0), source.slice(0, 1)] {
        let n = input.len();
        compare(Some(input), n);
    }
}
#[test]
fn pure_differential_count_over_original_bare_null_profile() {
    compare(Some(Arc::new(NullArray::new(5))), 5);
}
#[test]
fn pure_differential_count_over_original_dictionary_value_null_profile() {
    let input: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(1), Some(0)]),
            Arc::new(StringArray::from(vec![Some("ok"), None])),
        )
        .unwrap(),
    );
    compare(Some(input), 5);
}
