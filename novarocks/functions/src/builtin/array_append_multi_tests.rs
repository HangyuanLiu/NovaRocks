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

//! Actual ARRAY_APPEND invocation must preflight complete backing before construction.
use super::*;
use crate::builtin::array_append_core::{
    AppendFailure, AppendOutputFacts, ArrayAppendInputs, append_observed_guarded,
};
use arrow_array::types::Int8Type;
use arrow_array::{DictionaryArray, Int8Array, StringViewArray};
fn dictionary(text: &str, count: usize) -> ArrayRef {
    Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![0]),
            Arc::new(StringArray::from(vec![text; count])),
        )
        .unwrap(),
    )
}
fn null_parent(value: ArrayRef) -> ArrayRef {
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", value.data_type().clone(), true)),
        OffsetBuffer::new(vec![0, 1].into()),
        value,
        Some(NullBuffer::from(vec![false])),
    ))
}
#[test]
fn append_multi_constructor_full_dictionary_domain_is_refused_even_when_parent_skips_every_extend()
{
    let a = null_parent(dictionary("first", 64));
    let target = dictionary("second", 64);
    let p = prepared(
        &[source(&a), source(&target)],
        crate::binding_test_control(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Column(&target),
    ];
    let mut k = ScalarEvaluationInstance::instantiate(p).unwrap();
    let control = Control::default();
    assert_eq!(
        k.evaluate(Selection::all(1), &args, &control).unwrap_err(),
        KernelFailure::ResourceExhausted
    );
    assert!(!control.trace.lock().unwrap().is_empty());
    let after = Control::default();
    assert_eq!(
        k.evaluate(Selection::all(1), &args, &after).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    assert!(after.trace.lock().unwrap().is_empty());
}
#[test]
fn append_multi_view_backing_remapping_and_dictionary_domains_preserve_original_real_output() {
    let children: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("original-child-long-view"),
        None,
    ]));
    let a = list(children, vec![0, 1, 2]);
    let target: ArrayRef = Arc::new(StringViewArray::from(vec![
        Some("original-target-long-view"),
        Some("short"),
    ]));
    let p = prepared(
        &[source(&a), source(&target)],
        crate::binding_test_control(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Column(&target),
    ];
    let out = ScalarEvaluationInstance::instantiate(p)
        .unwrap()
        .evaluate(Selection::all(2), &args, &Control::default())
        .unwrap();
    let out = out.values().as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(out.value_offsets(), &[0, 2, 4]);
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("original-child-long-view"),
            Some("original-target-long-view"),
            None,
            Some("short")
        ]
    );
    assert_eq!(out.values().to_data().buffers().len(), 3);
    let child = dictionary("first", 3);
    let a = list(child, vec![0, 1]);
    let target = dictionary("second", 2);
    let p = prepared(
        &[source(&a), source(&target)],
        crate::binding_test_control(),
    )
    .unwrap();
    let args = [
        EvaluatedArgument::Column(&a),
        EvaluatedArgument::Column(&target),
    ];
    let out = ScalarEvaluationInstance::instantiate(p)
        .unwrap()
        .evaluate(Selection::all(1), &args, &Control::default())
        .unwrap();
    let out = out.values().as_any().downcast_ref::<ListArray>().unwrap();
    let values = out
        .values()
        .as_any()
        .downcast_ref::<DictionaryArray<Int8Type>>()
        .unwrap();
    assert_eq!(values.values().len(), 5);
    assert_eq!(values.keys().values().as_ref(), &[0, 3]);
}
#[test]
fn append_multi_real_plan_and_copy_share_null_offset_and_selected_row_author() {
    let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
    let a: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(vec![0, 2, 3, 4].into()),
        values,
        Some(NullBuffer::from(vec![true, false, true])),
    ));
    let target: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
    let mut observer = |_| Ok::<(), KernelFailure>(());
    let inputs = ArrayAppendInputs::new(
        a,
        target,
        AppendOutputFacts::OriginalInputList,
        &mut observer,
    )
    .unwrap();
    let rows = [0, 1, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let mut emitted = vec![];
    inputs
        .extensions_observed(
            selection,
            |_, row| Ok(inputs.legacy_rows(row)),
            |source, start, len| {
                emitted.push((source, start, len));
                Ok(())
            },
            &mut observer,
        )
        .unwrap();
    assert_eq!(emitted, vec![(0, 0, 2), (1, 0, 1), (0, 3, 1), (1, 2, 1)]);
    let mut checked = false;
    let out = append_observed_guarded(
        &inputs,
        selection,
        |_, row| Ok(inputs.legacy_rows(row)),
        |inputs| {
            checked = true;
            let plan = emitted
                .iter()
                .map(
                    |&(source, start, len)| crate::selected_copy::ExtendSegment {
                        source,
                        start,
                        len,
                        repeats: 1,
                        nulls: 0,
                    },
                )
                .collect::<Vec<_>>();
            crate::selected_copy::preflight_extend_multi(
                &inputs.constructor_sources(),
                &plan,
                0,
                |_| Ok(()),
            )
            .map_err(copy_error)
        },
        |_, _, _, _| Ok(()),
        &mut observer,
    )
    .unwrap();
    assert!(checked);
    let out = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(out.value_offsets(), &[0, 3, 3, 5]);
    assert!(out.is_null(1));
    assert_eq!(
        out.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(1), Some(2), Some(10), Some(4), Some(30)]
    );
}
#[test]
fn append_multi_constructor_guard_refusal_precedes_original_dictionary_panic_with_seven_exact_causes()
 {
    let a = null_parent(dictionary("first", 64));
    let target = dictionary("second", 64);
    let mut noop = |_| Ok::<(), KernelFailure>(());
    let inputs =
        ArrayAppendInputs::new(a, target, AppendOutputFacts::OriginalInputList, &mut noop).unwrap();
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
        KernelFailure::Internal(KernelDiagnostic::new("original internal")),
        KernelFailure::Operational(KernelDiagnostic::new("original operational")),
        KernelFailure::InstanceFailed,
    ] {
        let mut observed = 0;
        let result = append_observed_guarded(
            &inputs,
            Selection::all(1),
            |_, row| Ok(inputs.legacy_rows(row)),
            |_| Err(cause.clone()),
            |_, _, _, _| Ok(()),
            &mut |_| {
                observed += 1;
                Ok(())
            },
        );
        assert!(matches!(result,Err(AppendFailure::Control(ref actual)) if actual==&cause));
        assert_eq!(observed, 0); // no original to_data/new/extend boundary is entered
    }
}
#[test]
fn append_multi_every_actual_view_and_dictionary_callback_preserves_seven_causes_and_failed_latch()
{
    let dictionaries = list(dictionary("first", 3), vec![0, 1]);
    let dictionary_target = dictionary("second", 2);
    let views = list(
        Arc::new(StringViewArray::from(vec![Some(
            "child-original-long-view",
        )])),
        vec![0, 1],
    );
    let view_target: ArrayRef = Arc::new(StringViewArray::from(vec![Some(
        "target-original-long-view",
    )]));
    for (a, target) in [(dictionaries, dictionary_target), (views, view_target)] {
        let p = prepared(
            &[source(&a), source(&target)],
            crate::binding_test_control(),
        )
        .unwrap();
        let args = [
            EvaluatedArgument::Column(&a),
            EvaluatedArgument::Column(&target),
        ];
        let good = Control::default();
        ScalarEvaluationInstance::instantiate(p.clone())
            .unwrap()
            .evaluate(Selection::all(1), &args, &good)
            .unwrap();
        let trace = good.trace.lock().unwrap().clone();
        for at in 0..trace.len() {
            for cause in [
                KernelFailure::Cancelled,
                KernelFailure::DeadlineExceeded,
                KernelFailure::ResourceExhausted,
                KernelFailure::InvalidProgram(KernelDiagnostic::new("original invalid")),
                KernelFailure::Internal(KernelDiagnostic::new("original internal")),
                KernelFailure::Operational(KernelDiagnostic::new("original operational")),
                KernelFailure::InstanceFailed,
            ] {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let mut k = ScalarEvaluationInstance::instantiate(p.clone()).unwrap();
                assert_eq!(
                    k.evaluate(Selection::all(1), &args, &control).unwrap_err(),
                    cause
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                let after = Control::default();
                assert_eq!(
                    k.evaluate(Selection::all(1), &args, &after).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                assert!(after.trace.lock().unwrap().is_empty());
            }
        }
    }
}
