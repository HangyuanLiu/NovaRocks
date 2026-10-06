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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::*;
use novarocks_type_contract::{
    CompilePhase, PureCompileControl, owned_resources::copy::copy_string,
};
use std::{alloc::Layout, sync::Mutex};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop);
        }
        trace.push(units);
        match self.refusal {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn append(
    context: &mut ValidationContext,
    resources: &mut ControlResourceCounter,
    admit: &mut DiagnosticAdmission<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), ControlResourceError> {
    let error = ValidationError::categorized_in(
        ValidationErrorCategory::ResourceLimit,
        "p\0",
        "mλ",
        resources,
        admit,
        work,
    )?;
    context.push_in(error, resources, admit, work)
}

#[test]
fn borrowed_diagnostics_512_candidates_keep_original_content_and_full_request_invoice() {
    let mut original = ValidationContext::new();
    let mut context = ValidationContext::new();
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut resources = ControlResourceCounter::default();
    for _ in 0..512 {
        original.push(ValidationError::resource_limit("p\0", "mλ"));
        append(&mut context, &mut resources, &mut |_| Ok(()), &mut work).unwrap();
    }
    assert_eq!(context.errors, original.errors);
    assert_eq!(context.errors.len(), 129);
    assert_eq!(context.errors.capacity(), 256);
    let result =
        ValidationErrors::from_collector_in(context, &mut resources, &mut |_| Ok(()), &mut work)
            .unwrap();
    assert_eq!(result, ValidationErrors::from_collector(original));
    // Independent locked-Vec invoice: full growth requests 4+8+16+32+64+128+256
    // and the real 129-element final trim, plus every candidate's two byte Boxes.
    let header = Layout::new::<ValidationError>().size();
    assert_eq!(
        resources.facts().allocation_requests_upper_bound,
        512 * 2 + 2 + 7 + 1
    );
    assert_eq!(
        resources.facts().allocation_request_bytes_upper_bound,
        512 * 5
            + "validation".len()
            + "additional validation errors were truncated".len()
            + (508 + 129) * header
    );
    work.finish().unwrap();
}

#[test]
fn borrowed_diagnostics_130th_candidate_still_copies_without_second_sentinel_or_vec_request() {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut resources = ControlResourceCounter::default();
    let mut context = ValidationContext::new();
    for _ in 0..129 {
        append(&mut context, &mut resources, &mut |_| Ok(()), &mut work).unwrap();
    }
    let before = resources.facts();
    let pointer = context.errors.as_ptr();
    append(&mut context, &mut resources, &mut |_| Ok(()), &mut work).unwrap();
    let after = resources.facts();
    assert_eq!(
        after.allocation_requests_upper_bound - before.allocation_requests_upper_bound,
        2
    );
    assert_eq!(
        after.allocation_request_bytes_upper_bound - before.allocation_request_bytes_upper_bound,
        5
    );
    assert_eq!(context.errors.len(), 129);
    assert_eq!(context.errors.as_ptr(), pointer);
    context
        .mark_truncated_in(&mut resources, &mut |_| Ok(()), &mut work)
        .unwrap();
    assert_eq!(resources.facts(), after);
}

#[test]
fn borrowed_diagnostic_both_known_boxes_refuse_before_real_pending_copy_observation() {
    for cause in CAUSES {
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((3, cause)),
        };
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let source = "x".repeat(255);
        let copied = copy_string::<ControlResourceError>(&source, &mut work).unwrap();
        assert_eq!(copied.as_bytes(), [b'x'; 255]);
        assert_ne!(copied.as_ptr(), source.as_ptr());
        let mut resources = ControlResourceCounter::default();
        let result = ValidationError::categorized_in(
            ValidationErrorCategory::StructuralInvariant,
            "p\0",
            "mλ",
            &mut resources,
            &mut |facts| {
                assert_eq!(facts.allocation_requests_upper_bound, 2);
                assert_eq!(facts.allocation_request_bytes_upper_bound, 5);
                if facts.allocation_request_bytes_upper_bound > 4 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            },
            &mut work,
        );
        assert_eq!(result, Err(CompileControlError::ResourceExhausted.into()));
        assert_eq!(control.trace(), [0, 0, 1]);
    }
}

#[test]
fn borrowed_sentinel_admits_both_spellings_and_actual_collector_growth_before_observation() {
    for axis in 0..2 {
        for cause in CAUSES {
            let mut context = ValidationContext::new();
            for _ in 0..128 {
                context.push(ValidationError::resource_limit("p\0", "mλ"));
            }
            assert_eq!(context.errors.len(), 128);
            assert_eq!(context.errors.capacity(), 128);
            // The incoming context is real, built before this caller scope.
            // Admit its actual backing and closed byte payloads once.
            let mut resources = ControlResourceCounter::default();
            resources
                .buffer::<ValidationError>(context.errors.capacity(), 1)
                .unwrap();
            for error in &context.errors {
                resources.buffer::<u8>(error.path().len(), 1).unwrap();
                resources.buffer::<u8>(error.message().len(), 1).unwrap();
            }
            let before = resources.facts();
            let requested_bytes = "validation".len()
                + "additional validation errors were truncated".len()
                + Layout::array::<ValidationError>(256).unwrap().size();
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((1, cause)),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let result = context.mark_truncated_in(
                &mut resources,
                &mut |facts| {
                    let over = if axis == 0 {
                        facts.allocation_requests_upper_bound
                            > before.allocation_requests_upper_bound + 3 - 1
                    } else {
                        facts.allocation_request_bytes_upper_bound
                            > before.allocation_request_bytes_upper_bound + requested_bytes - 1
                    };
                    if over {
                        Err(CompileControlError::ResourceExhausted)
                    } else {
                        Ok(())
                    }
                },
                &mut work,
            );
            assert_eq!(result, Err(CompileControlError::ResourceExhausted.into()));
            assert_eq!(control.trace(), [0]);
            assert_eq!(context.errors.len(), 128);
            assert_eq!(context.errors.capacity(), 128);
        }
    }
}

fn small(control: &Control) -> Result<ValidationErrors, ControlResourceError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let mut resources = ControlResourceCounter::default();
    let mut context = ValidationContext::new();
    append(&mut context, &mut resources, &mut |_| Ok(()), &mut work)?;
    context.mark_truncated_in(&mut resources, &mut |_| Ok(()), &mut work)?;
    let result =
        ValidationErrors::from_collector_in(context, &mut resources, &mut |_| Ok(()), &mut work)?;
    work.finish()?;
    Ok(result)
}
#[test]
fn borrowed_diagnostic_each_actual_small_callback_preserves_three_causes_and_caller_tail() {
    let baseline = Control::default();
    let result = small(&baseline).unwrap();
    assert_eq!(result.errors().len(), 2);
    assert_eq!(
        result.errors()[0].category(),
        ValidationErrorCategory::ResourceLimit
    );
    assert_eq!(result.errors()[1].path(), "validation");
    let trace = baseline.trace();
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause)),
            };
            assert_eq!(small(&control), Err(cause.into()));
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[test]
fn borrowed_diagnostic_final_trim_uses_actual_empty_spare_and_equal_capacity() {
    for spare in [false, true] {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut resources = ControlResourceCounter::default();
        let mut context = ValidationContext::new();
        // This fixture owns the original spare backing before the measured
        // operation and admits that actual backing once, not as a source-B default.
        context
            .errors
            .try_reserve_exact(if spare { 7 } else { 0 })
            .unwrap();
        resources
            .buffer::<ValidationError>(context.errors.capacity(), 1)
            .unwrap();
        let before = resources.facts();
        let result = ValidationErrors::from_collector_in(
            context,
            &mut resources,
            &mut |_| Ok(()),
            &mut work,
        )
        .unwrap();
        assert!(result.errors().is_empty());
        assert_eq!(
            resources.facts().allocation_requests_upper_bound,
            before.allocation_requests_upper_bound
        );
        assert_eq!(
            resources.facts().allocation_request_bytes_upper_bound,
            before.allocation_request_bytes_upper_bound
        );
        assert_eq!(
            resources.facts().cumulative_work_upper_bound,
            before.cumulative_work_upper_bound + 1
        );
    }
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut resources = ControlResourceCounter::default();
    let mut context = ValidationContext::new();
    context.errors.try_reserve_exact(1).unwrap();
    resources.buffer::<ValidationError>(1, 1).unwrap();
    append(&mut context, &mut resources, &mut |_| Ok(()), &mut work).unwrap();
    let pointer = context.errors.as_ptr();
    let before = resources.facts();
    let result =
        ValidationErrors::from_collector_in(context, &mut resources, &mut |_| Ok(()), &mut work)
            .unwrap();
    assert_eq!(result.errors().as_ptr(), pointer);
    assert_eq!(
        resources.facts().allocation_requests_upper_bound,
        before.allocation_requests_upper_bound
    );
    assert_eq!(
        resources.facts().allocation_request_bytes_upper_bound,
        before.allocation_request_bytes_upper_bound
    );
    assert_eq!(
        resources.facts().cumulative_work_upper_bound,
        before.cumulative_work_upper_bound + 1
    );
}

#[test]
fn borrowed_diagnostic_preserves_exact_utf8_null_bytes_and_parent_control_first_cause() {
    let path = "λ\0".repeat(320);
    let message = "診断\0".repeat(320);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut resources = ControlResourceCounter::default();
    let error = ValidationError::categorized_in(
        ValidationErrorCategory::UnsupportedCapability,
        &path,
        &message,
        &mut resources,
        &mut |_| Ok(()),
        &mut work,
    )
    .unwrap();
    assert_eq!(
        error,
        ValidationError::unsupported_capability(&path, &message)
    );
    assert_ne!(error.path.as_ptr(), path.as_ptr());
    assert_ne!(error.message.as_ptr(), message.as_ptr());
    assert_eq!(
        resources.facts().allocation_request_bytes_upper_bound,
        path.len() + message.len()
    );
    // Already-spelled payload copying remains one opaque library operation;
    // this fixture makes no formatter/allocator-internal 256-work claim.
    let control = Control {
        trace: Mutex::default(),
        refusal: Some((1, CompileControlError::Cancelled)),
    };
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut resources = ControlResourceCounter::default();
    // An actual parent refusal is Control; SourceModel remains separately typed
    // in source/numeric authors and is never classified by diagnostic text.
    let result = ValidationError::categorized_in(
        ValidationErrorCategory::StructuralInvariant,
        &path,
        &message,
        &mut resources,
        &mut |_| Err(CompileControlError::ResourceExhausted),
        &mut work,
    );
    assert_eq!(result, Err(CompileControlError::ResourceExhausted.into()));
    assert_eq!(control.trace(), [0]);
}

#[test]
fn borrowed_empty_spellings_and_reused_collector_admit_actual_work_before_observation() {
    for operation in 0..3 {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((1, cause)),
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let mut resources = ControlResourceCounter::default();
            // Original objects exist before the measured caller operation. The
            // collector's actual spare backing is funded once by this owner.
            let mut context = ValidationContext::new();
            context.errors.try_reserve_exact(1).unwrap();
            resources
                .buffer::<ValidationError>(context.errors.capacity(), 1)
                .unwrap();
            let before = resources.facts();
            let known_work = if operation == 0 { 4 } else { 1 };
            let mut admit = |facts: &ControlOwnedResourceFacts| {
                assert_eq!(
                    facts.allocation_requests_upper_bound,
                    before.allocation_requests_upper_bound
                );
                assert_eq!(
                    facts.allocation_request_bytes_upper_bound,
                    before.allocation_request_bytes_upper_bound
                );
                assert_eq!(
                    facts.cumulative_work_upper_bound,
                    before.cumulative_work_upper_bound + known_work
                );
                if facts.cumulative_work_upper_bound
                    > before.cumulative_work_upper_bound + known_work - 1
                {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            };
            let result = match operation {
                0 => ValidationError::categorized_in(
                    ValidationErrorCategory::StructuralInvariant,
                    "",
                    "",
                    &mut resources,
                    &mut admit,
                    &mut work,
                )
                .map(|_| ()),
                1 => context.push_in(
                    ValidationError::new("", ""),
                    &mut resources,
                    &mut admit,
                    &mut work,
                ),
                _ => ValidationErrors::from_collector_in(
                    context,
                    &mut resources,
                    &mut admit,
                    &mut work,
                )
                .map(|_| ()),
            };
            assert_eq!(result, Err(CompileControlError::ResourceExhausted.into()));
            assert_eq!(control.trace(), [0]);
        }
    }
}
