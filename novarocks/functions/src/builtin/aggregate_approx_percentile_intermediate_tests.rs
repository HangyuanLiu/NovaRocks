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

//! One complete Intermediate phase chain; original frozen raw/diff leaves remain unchanged.
use super::*;
#[test]
fn approximate_percentile_owner_all_arities_partial_intermediate_final_original_state_bytes() {
    for (name, weighted, arities) in [
        ("percentile_approx", false, [2, 3]),
        ("percentile_approx_weighted", true, [3, 4]),
    ] {
        for arity in arities {
            let mut args = vec![Arc::new(Float64Array::from(vec![3.])) as ArrayRef];
            if weighted {
                args.push(Arc::new(Int64Array::from(vec![2])) as ArrayRef);
            }
            args.push(Arc::new(Float64Array::from(vec![0.5])) as ArrayRef);
            if args.len() < arity {
                args.push(Arc::new(Int64Array::from(vec![2048])) as ArrayRef);
            }
            let partial = kernel(name, types(&args), AggregateKernelPhase::Partial);
            let intermediate = kernel(name, types(&args), AggregateKernelPhase::Intermediate);
            let final_kernel = kernel(name, types(&args), AggregateKernelPhase::Final);
            let host = Arc::new(Host::default());
            let control = Control::default();
            let mut partial_state = partial
                .create_state_with_allocator(Some(host.clone()), &control)
                .unwrap();
            update_selected(
                &partial,
                &mut partial_state,
                host.clone(),
                &args,
                Selection::all(1),
                &[17],
                &control,
            )
            .unwrap();
            let emit = |kernel: &ApproxPercentileKernel, state: &ApproxPercentileState| {
                let allocator: Arc<dyn AggregateStateAllocator> = host.clone();
                let indices = [17];
                let context = AggregateEmissionContext::from_host(
                    &kernel.contract,
                    &indices,
                    32,
                    Some(&allocator),
                );
                assert_eq!(context.phase(), AggregateInvocationPhase::Intermediate);
                kernel
                    .build_intermediate_evaluation_with_context(
                        std::iter::once(state),
                        &context,
                        &control,
                    )
                    .unwrap()
            };
            let partial_payload = emit(&partial, &partial_state);
            let mut intermediate_state = intermediate
                .create_state_with_allocator(Some(host.clone()), &control)
                .unwrap();
            merge_selected(
                &intermediate,
                &mut intermediate_state,
                host.clone(),
                &partial_payload,
                Selection::all(1),
                &[17],
                &control,
            )
            .unwrap();
            let intermediate_payload = emit(&intermediate, &intermediate_state);
            assert_eq!(
                partial_payload
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0),
                intermediate_payload
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0)
            );
            let mut final_state = final_kernel
                .create_state_with_allocator(Some(host.clone()), &control)
                .unwrap();
            merge_selected(
                &final_kernel,
                &mut final_state,
                host.clone(),
                &intermediate_payload,
                Selection::all(1),
                &[17],
                &control,
            )
            .unwrap();
            let output = final_output(&final_kernel, &final_state, host.clone()).unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
                    .to_bits(),
                3f64.to_bits()
            );
            drop(output);
            drop(final_state);
            drop(intermediate_state);
            drop(partial_state);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}
