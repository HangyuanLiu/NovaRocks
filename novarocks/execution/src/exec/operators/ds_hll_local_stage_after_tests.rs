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

//! Actual local lifecycle of one Single call; the original numerical oracle
//! was measured by the unchanged before probes, not invented from the owner.
use super::*;
#[test]
fn ds_hll_local_stage_after_real_factory_100k_and_exact_identity() {
    use novarocks_functions::AggregateKernelPhase as P;
    let tracker = MemTracker::new_root("DsHllLocalStageAfter");
    {
        let program = program(false);
        let source = input(&program);
        let single = factory(&program, P::Single);
        assert!(single.requires_local_update_stages());
        let original = single.calls[0].contract.clone();
        let (partial, final_stage) = single.into_local_update_stages().unwrap();
        assert!(!partial.completes_groups());
        assert!(final_stage.completes_groups());
        assert_eq!(partial.calls[0].contract.phase(), P::Partial);
        assert_eq!(final_stage.calls[0].contract.phase(), P::Final);
        assert!(Arc::ptr_eq(
            partial.calls[0].contract.call(),
            original.call()
        ));
        assert!(Arc::ptr_eq(
            final_stage.calls[0].contract.call(),
            original.call()
        ));
        assert_eq!(partial.output.slots()[0].data_type(), &DataType::Binary);
        assert_eq!(final_stage.output, factory(&program, P::Single).output);
        assert!(
            final_stage.sites.is_empty(),
            "no fabricated expression root"
        );
        let left = run(
            &partial,
            &[Chunk::new_like(source.batch.slice(0, 50_000), &source)],
            tracker.clone(),
        );
        let right = run(
            &partial,
            &[Chunk::new_like(source.batch.slice(50_000, 50_000), &source)],
            tracker.clone(),
        );
        for payload in [&left, &right] {
            let bytes = payload
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(0);
            assert_eq!(bytes[3], 17);
            assert_eq!(bytes[5], 0, "original raw partial HIP flag");
            assert_eq!(payload.chunk_schema(), partial.output.as_ref());
        }
        for reverse in [false, true] {
            let inputs = if reverse {
                [right.clone(), left.clone()]
            } else {
                [left.clone(), right.clone()]
            };
            let result = run(&final_stage, &inputs, tracker.clone());
            assert_eq!(
                result
                    .batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                100_091
            );
            assert_eq!(result.chunk_schema(), final_stage.output.as_ref());
        }
    }
    assert_eq!(tracker.current(), 0);
}

#[test]
fn ds_hll_local_stage_after_dop_one_original_raw_and_empty_two_driver_lifecycle() {
    use novarocks_functions::AggregateKernelPhase as P;
    let tracker = MemTracker::new_root("DsHllLocalStageSingleAndEmpty");
    {
        let program = program(false);
        let source = input(&program);
        let single = factory(&program, P::Single);
        let state = RuntimeState::default();
        let mut operator = single.create(1, 0);
        operator.set_mem_tracker(tracker.clone());
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, source).unwrap();
        processor.set_finishing(&state).unwrap();
        let result = processor.pull_chunk(&state).unwrap().unwrap();
        assert_eq!(
            result
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            100_093
        );
        drop(operator);
        assert_eq!(tracker.current(), 0);
        let (partial, final_stage) = single.into_local_update_stages().unwrap();
        let left = run(&partial, &[], tracker.clone());
        let right = run(&partial, &[], tracker.clone());
        assert_eq!(left.len(), 1);
        assert_eq!(right.len(), 1);
        let result = run(&final_stage, &[left, right], tracker.clone());
        assert_eq!(result.len(), 1);
        assert_eq!(
            result
                .batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            0
        );
    }
    assert_eq!(tracker.current(), 0);
}
#[test]
fn ds_hll_local_stage_after_actual_partial_and_final_cancel_first_cause_no_replay() {
    use novarocks_functions::AggregateKernelPhase as P;
    let tracker = MemTracker::new_root("DsHllLocalStageNoReplay");
    for final_phase in [false, true] {
        let program = program(false);
        let source = input(&program);
        let (partial, final_stage) = factory(&program, P::Single)
            .into_local_update_stages()
            .unwrap();
        let source = Chunk::new_like(source.batch.slice(0, 50_000), &source);
        let partial_output = run(&partial, &[source.clone()], tracker.clone());
        let phase = if final_phase { &final_stage } else { &partial };
        let first = if final_phase {
            partial_output.clone()
        } else {
            source.clone()
        };
        let state = RuntimeState::default();
        let mut operator = phase.create(if final_phase { 1 } else { 2 }, 0);
        operator.set_mem_tracker(tracker.clone());
        let processor = operator.as_processor_mut().unwrap();
        processor.push_chunk(&state, first.clone()).unwrap();
        assert!(tracker.current() > 0);
        phase.error.set_error("local aggregate actual task stop");
        let primary = processor.push_chunk(&state, first.clone()).unwrap_err();
        assert_eq!(
            primary.cause(),
            &crate::runtime::fragment::ExecutionFailureCause::Kernel(KernelFailure::Cancelled)
        );
        assert_eq!(tracker.current(), 0);
        assert!(processor.is_finished());
        assert!(!processor.need_input());
        assert!(!processor.has_output());
        assert_eq!(processor.push_chunk(&state, first).unwrap_err(), primary);
        assert_eq!(processor.set_finishing(&state).unwrap_err(), primary);
        assert_eq!(processor.pull_chunk(&state).unwrap_err(), primary);
        drop(operator);
        assert_eq!(tracker.current(), 0);
    }
}
