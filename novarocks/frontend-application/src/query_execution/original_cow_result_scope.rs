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

//! COW-only projection of an existing Internal result admission.

use super::internal_result_cpu::INTERNAL_PEAK_BYTES;
use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_spi::connector::{
    ConnectorOperationControl, ConnectorOperationControlView, ConnectorOriginalResultScope,
    ConnectorOriginalResultScopeCheck, ConnectorRequestContext, OriginalResultCheckClass as Class,
    OriginalResultCheckError as Failure,
};
use novarocks_workload_control::{ResultWindowClass, WorkError};
use std::{fmt, sync::Arc, time::Instant};

// No ActivityLease. The executing command/job owns activity until actual exit.
struct OriginalCowResultScope {
    control: ConnectorOperationControlView,
    binding: QueryResultCapacityBinding,
}
#[derive(Debug)]
struct InvalidOriginal(&'static str);
impl fmt::Display for InvalidOriginal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for InvalidOriginal {}
impl ConnectorOriginalResultScopeCheck for OriginalCowResultScope {
    fn original_deadline(&self) -> Instant {
        self.control.deadline()
    }
    fn check_active(&self) -> Result<(), Failure> {
        self.control
            .check_active()
            .map_err(|e| Failure::new(Class::Control, e))?;
        self.binding
            .scope()
            .check()
            .map_err(|e| Failure::new(Class::Admission, e))?;
        Ok(())
    }
    fn check_before_growth(&self, total: u64) -> Result<(), Failure> {
        self.check_active()?;
        // The 1024 MiB physical window does not enlarge the 1008 MiB profile.
        if total > INTERNAL_PEAK_BYTES {
            return Err(Failure::new(
                Class::Coverage,
                WorkError::Capacity("COW Internal construction profile"),
            ));
        }
        self.binding
            .window_alias()
            .check_backing_total(total)
            .map_err(|e| Failure::new(Class::Coverage, e))?;
        self.check_active()
    }
}

/// existing_upper is the caller's actual prospective receipt before this Arc
/// grows. Add this newly allocated carrier once, before Arc::new; the returned
/// scalar is a conservative existing caller upper for provider coexistence.
/// It is not a provider recipe and never covers later provider graphs by fiat.
pub(crate) fn from_original_binding(
    binding: &QueryResultCapacityBinding,
    context: &ConnectorRequestContext,
    existing_upper: u64,
) -> Result<(ConnectorOriginalResultScope, u64), Failure> {
    context
        .check_active()
        .map_err(|e| Failure::new(Class::Control, e))?;
    binding
        .scope()
        .check()
        .map_err(|e| Failure::new(Class::Admission, e))?;
    let window = binding.window_alias();
    if !window.is_for_scope(binding.scope()) || binding.class() != ResultWindowClass::Internal {
        return Err(Failure::new(
            Class::Identity,
            InvalidOriginal("COW requires its original Internal result binding"),
        ));
    }
    window
        .check_backing_total(INTERNAL_PEAK_BYTES)
        .map_err(|e| Failure::new(Class::Coverage, e))?;
    let caller_with_scope_upper = existing_upper
        .checked_add(
            carrier_header_upper()
                .ok_or_else(|| Failure::new(Class::Coverage, WorkError::ArithmeticOverflow))?,
        )
        .ok_or_else(|| Failure::new(Class::Coverage, WorkError::ArithmeticOverflow))?;
    if caller_with_scope_upper > INTERNAL_PEAK_BYTES {
        return Err(Failure::new(
            Class::Coverage,
            WorkError::Capacity("COW Internal construction profile"),
        ));
    }
    window
        .check_backing_total(caller_with_scope_upper)
        .map_err(|e| Failure::new(Class::Coverage, e))?;
    let owner = Arc::new(OriginalCowResultScope {
        control: context.original_operation_control(),
        binding: binding.clone(),
    });
    let original = ConnectorOriginalResultScope::from_original(owner);
    original.check_active()?;
    Ok((original, caller_with_scope_upper))
}

pub(crate) fn carrier_header_upper() -> Option<u64> {
    let alignment =
        std::mem::align_of::<OriginalCowResultScope>().max(std::mem::align_of::<usize>());
    2usize
        .checked_mul(std::mem::size_of::<usize>())?
        .checked_add(alignment - 1)?
        .checked_add(std::mem::size_of::<OriginalCowResultScope>())?
        .checked_add(alignment - 1)?
        .try_into()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorCowBeginCause, ConnectorCowBeginFailure, ConnectorStopOwner,
    };
    use std::{
        error::Error,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(10),
            ConnectorStopOwner::new().view(),
            16 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .unwrap()
    }
    struct Canary {
        capacity: novarocks_workload_control::ResultCapacityHandle,
        held_during_cause_exit: Arc<AtomicBool>,
    }
    impl fmt::Debug for Canary {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("original source Debug must never run")
        }
    }
    impl fmt::Display for Canary {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("original source Display must never run")
        }
    }
    impl Error for Canary {}
    impl Drop for Canary {
        fn drop(&mut self) {
            self.held_during_cause_exit.store(
                self.capacity.snapshot().held_positions == [0, 0, 1, 0],
                Ordering::SeqCst,
            );
        }
    }
    #[test]
    fn failure_and_last_neutral_alias_keep_actual_window_without_activity() {
        let (_control, root, binding, capacity) =
            super::super::internal_result_cpu::admitted_internal_fixture();
        // This fixture has no result payload/header/provider graph. Existing
        // result backing really is empty; the new carrier is counted by factory.
        let (scope, carrier_upper) = from_original_binding(&binding, &context(), 0).unwrap();
        let counted_carrier = carrier_upper > 0;
        let last = scope.retention_guard();
        let held = Arc::new(AtomicBool::new(false));
        let source = Box::new(Canary {
            capacity: capacity.clone(),
            held_during_cause_exit: held.clone(),
        });
        let raw_pointer = source.as_ref() as *const Canary;
        let failure = ConnectorCowBeginFailure::new(
            ConnectorCowBeginCause::OriginalResult(Failure::new(Class::Control, source)),
            scope,
        );
        // The closed envelope may be printed but never formats the raw source.
        let presentation = format!("{failure} {failure:?}");
        let actual_canary = failure
            .source()
            .and_then(|e| e.source())
            .and_then(|e| e.source())
            .and_then(|e| e.downcast_ref::<Box<Canary>>())
            .is_some_and(|e| e.as_ref() as *const Canary == raw_pointer);
        // A neutral holder cannot self-wait. Rescue scope exits before assertions
        // even if the observer proves an accidental activity lease regression.
        let activity_binding = binding.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let observer = std::thread::spawn(move || {
            activity_binding.wait_result_activities_exited();
            done_tx.send(()).unwrap();
        });
        let no_activity = done_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        drop(binding);
        root.owner.complete();
        root.business.release();
        let before = capacity.snapshot().held_positions;
        drop(failure);
        let after_failure = capacity.snapshot().held_positions;
        drop(last);
        observer.join().unwrap();
        let final_count = capacity.snapshot().held_positions;
        assert!(counted_carrier);
        assert!(no_activity);
        assert!(actual_canary);
        assert!(presentation.contains("copy-on-write begin failed"));
        assert!(held.load(Ordering::SeqCst));
        assert_eq!(before, [0, 0, 1, 0]);
        assert_eq!(after_failure, [0, 0, 1, 0]);
        assert_eq!(final_count, [0; 4]);
    }
    #[test]
    fn same_control_projection_preserves_original_deadline_and_stop_error() {
        let (_control, root, binding, capacity) =
            super::super::internal_result_cpu::admitted_internal_fixture();
        let stop = ConnectorStopOwner::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        let context = ConnectorRequestContext::try_new(
            deadline,
            stop.view(),
            16 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .unwrap();
        let (scope, _) = from_original_binding(&binding, &context, 0).unwrap();
        let copy = scope.clone();
        let same = scope.is_same_original(&copy);
        let actual_deadline = scope.original_deadline();
        stop.request_stop();
        let original = context.check_active().err();
        let retained = copy.check_active().err();
        let same_error = retained
            .as_ref()
            .and_then(|e| e.source())
            .and_then(|e| e.downcast_ref::<novarocks_spi::connector::ConnectorError>())
            .zip(original.as_ref())
            .is_some_and(|(a, b)| a == b);
        let class = retained.as_ref().map(|e| e.class());
        drop(copy);
        drop(scope);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert!(same);
        assert_eq!(actual_deadline, deadline);
        assert!(same_error);
        assert_eq!(class, Some(Class::Control));
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}
