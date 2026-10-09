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

//! The actual typed core consumer, with fresh mutable state and host per trial.
use super::*;
use arrow_array::{Array, StructArray};

fn quantile_packet() -> ArrayRef {
    let value = Arc::new(Float64Array::from(vec![-0.0])) as ArrayRef;
    let rates = Arc::new(ListArray::from_iter_primitive::<
        arrow_array::types::Float64Type,
        _,
        _,
    >([Some(
        (0..512).map(|n| Some(n as f64 / 511.)).collect::<Vec<_>>(),
    )])) as ArrayRef;
    Arc::new(StructArray::new(
        vec![
            Field::new("value", value.data_type().clone(), true),
            Field::new("rate", rates.data_type().clone(), true),
        ]
        .into(),
        vec![value, rates],
        None,
    ))
}
fn dense_payload() -> Vec<u8> {
    // The original constructor and original add author produce this state.
    // Compression 16 is a legitimate bounded-v4 decoded state, not a SQL default.
    let mut state = digest::PercentileState::new_in(16, allocator_api2::alloc::Global);
    for n in 0..1024 {
        digest::add_value(&mut state, n as f64 - 512.).unwrap();
    }
    digest::encode_state(&state)
}
fn run_update(
    k: &ApproxPercentileKernel,
    values: &[ArrayRef],
    control: &Control,
    refusal: Option<(usize, KernelFailure)>,
) -> (Result<(), EvaluationFailure>, Arc<Host>, usize) {
    let host = Arc::new(Host::default());
    let setup = Control::default();
    let mut state = k
        .create_state_with_allocator(Some(host.clone()), &setup)
        .unwrap();
    let arguments = values
        .iter()
        .map(EvaluatedArgument::Column)
        .collect::<Vec<_>>();
    let input = SelectedAggregateUpdateInput::try_new(
        &k.contract,
        Selection::all(1),
        &arguments,
        &[],
        &setup,
    )
    .unwrap();
    let prepared = k
        .prepare_update_evaluation(input, &[17], Some(host.clone()), &setup)
        .unwrap();
    let before = host.ledger.lock().unwrap().attempts;
    if let Some((at, cause)) = refusal {
        arm_refusal(&host, at, cause);
    }
    let result = k.update_row_evaluation(&mut state, &prepared, 0, control);
    if result.is_err() {
        assert!(state.failed);
        let before = control.trace.lock().unwrap().clone();
        assert_eq!(
            k.update_row_evaluation(&mut state, &prepared, 0, control)
                .unwrap_err(),
            EvaluationFailure::Kernel(KernelFailure::InstanceFailed)
        );
        assert_eq!(*control.trace.lock().unwrap(), before);
    }
    let attempts = host.ledger.lock().unwrap().attempts - before;
    drop(prepared);
    drop(state);
    (result, host, attempts)
}
#[test]
fn percentile_union_core_control_owned_quantile_and_dense_payload_every_callback_seven_causes() {
    let dense = dense_payload();
    for values in [
        vec![quantile_packet()],
        vec![Arc::new(BinaryArray::from(vec![dense.as_slice()])) as ArrayRef],
    ] {
        let k = kernel(
            "percentile_union",
            types(&values),
            AggregateKernelPhase::Single,
        );
        let observed = Control::default();
        let (result, host, _) = run_update(&k, &values, &observed, None);
        result.unwrap();
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        let trace = observed.trace.into_inner().unwrap();
        assert!(
            trace.contains(&256),
            "actual original owned work crosses its quantum"
        );
        for at in 0..trace.len() {
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(vec![]),
                    refusal: Some((at, cause.clone())),
                };
                let (result, host, _) = run_update(&k, &values, &control, None);
                assert_eq!(result.unwrap_err(), EvaluationFailure::Kernel(cause));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
                assert_eq!(host.ledger.lock().unwrap().bytes, 0);
            }
        }
    }
}
#[test]
fn percentile_union_core_control_every_real_quantile_and_decode_allocation_seven_causes() {
    let dense = dense_payload();
    for values in [
        vec![quantile_packet()],
        vec![Arc::new(BinaryArray::from(vec![dense.as_slice()])) as ArrayRef],
    ] {
        let k = kernel(
            "percentile_union",
            types(&values),
            AggregateKernelPhase::Single,
        );
        let (result, host, attempts) = run_update(&k, &values, &Control::default(), None);
        result.unwrap();
        assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        assert!(attempts >= 3, "actual scratch/digest allocation frontiers");
        for at in 0..attempts {
            for cause in causes() {
                let (result, host, _) =
                    run_update(&k, &values, &Control::default(), Some((at, cause.clone())));
                assert_eq!(result.unwrap_err(), EvaluationFailure::Kernel(cause));
                assert_eq!(host.ledger.lock().unwrap().bytes, 0);
            }
        }
    }
}
#[test]
fn percentile_union_core_control_legacy_quantile_scratch_and_host_scratch_same_bytes() {
    let values = vec![quantile_packet()];
    let k = kernel(
        "percentile_union",
        types(&values),
        AggregateKernelPhase::Single,
    );
    let host = Arc::new(Host::default());
    let mut state = k
        .create_state_with_allocator(Some(host.clone()), &Control::default())
        .unwrap();
    update_selected(
        &k,
        &mut state,
        host.clone(),
        &values,
        Selection::all(1),
        &[17],
        &Control::default(),
    )
    .unwrap();
    let structure = values[0].as_any().downcast_ref::<StructArray>().unwrap();
    let channels = core::UnweightedInput::try_new(structure, Diagnostic::UnweightedUpdate).unwrap();
    let mut original = digest::PercentileState::default();
    channels
        .update_addresses(&mut original, 0, 0, 0, Diagnostic::UnweightedUpdate)
        .unwrap();
    assert_eq!(
        digest::encode_state(&state.core),
        digest::encode_state(&original)
    );
    assert!(host.ledger.lock().unwrap().attempts >= 4);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}
#[test]
fn percentile_union_core_control_serializer_original_bytes_every_callback_seven_causes() {
    let bytes = dense_payload();
    let mut state = digest::decode_state(&bytes).unwrap();
    digest::set_quantiles(
        &mut state,
        &(0..512).map(|n| n as f64 / 511.).collect::<Vec<_>>(),
    )
    .unwrap();
    let original = digest::encode_state(&state);
    let run = |control: &Control| {
        let mut work = EvaluationCheckpoints::new(control);
        let result = digest::encode_state_observed(&state, &mut |event| match event {
            digest::PercentileEncodeObservation::Step => work.step(),
            digest::PercentileEncodeObservation::OpaqueBegin
            | digest::PercentileEncodeObservation::OpaqueEnd => work.flush(),
        });
        if result.is_ok() {
            work.finish()?;
        }
        result
    };
    let observed = Control::default();
    assert_eq!(run(&observed).unwrap(), original);
    let trace = observed.trace.into_inner().unwrap();
    assert!(trace.contains(&256));
    for at in 0..trace.len() {
        for cause in causes() {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(run(&control).unwrap_err(), cause);
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn percentile_union_core_control_typed_core_errors_keep_original_full_text_and_no_tail() {
    use crate::approx_percentile_failure::{
        ApproxPercentileDataRecipe, ApproxPercentileFailureSink, ApproxPercentileObservation,
    };
    struct TextSink {
        trace: usize,
    }
    impl ApproxPercentileFailureSink for TextSink {
        type Error = String;
        fn data(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> String {
            recipe.to_string()
        }
        fn allocation(&mut self, recipe: ApproxPercentileDataRecipe<'_>) -> String {
            recipe.to_string()
        }
        fn observe(&mut self, _: ApproxPercentileObservation) -> Result<(), String> {
            self.trace += 1;
            Ok(())
        }
    }
    for q in [f64::NAN, -1., 2., f64::INFINITY] {
        let expected =
            digest::set_quantile(&mut digest::PercentileState::default(), q).unwrap_err();
        let mut sink = TextSink { trace: 0 };
        let error =
            digest::set_quantile_with_sink(&mut digest::PercentileState::default(), q, &mut sink)
                .unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(sink.trace, 0);
    }
    let mut legacy = digest::PercentileState::default();
    let mut typed = digest::PercentileState::default();
    digest::set_quantiles(&mut legacy, &[-0., 0.5, 1.]).unwrap();
    let mut sink = TextSink { trace: 0 };
    digest::set_quantiles_with_sink(&mut typed, &[-0., 0.5, 1.], &mut sink).unwrap();
    for value in [-0., 0., -1., 1., f64::NAN] {
        digest::add_value(&mut legacy, value).unwrap();
        digest::add_value_with_sink(&mut typed, value, &mut sink).unwrap();
    }
    assert_eq!(digest::encode_state(&legacy), digest::encode_state(&typed));
    let expected = digest::set_quantiles(&mut legacy, &[0., 0.75, 1.]).unwrap_err();
    assert_eq!(
        digest::set_quantiles_with_sink(&mut typed, &[0., 0.75, 1.], &mut sink).unwrap_err(),
        expected
    );
}
