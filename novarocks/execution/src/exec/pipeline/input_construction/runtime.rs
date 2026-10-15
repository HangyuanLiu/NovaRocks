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

use super::{ConstructionFailure, InputConstructionRequestHost, InputConstructionWorkControl};
use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
use std::{alloc::Layout, sync::Arc};

/// Existing logical tracker adapter. Its finite ancestor chain contains no
/// query context or memory authority; these are observations, not physical facts.
pub struct TrackedInputConstructionHost {
    tracker: Arc<MemTracker>,
    _creation: Option<TrackedArcCharge>,
}
impl TrackedInputConstructionHost {
    pub fn new(tracker: Arc<MemTracker>) -> Self {
        Self {
            tracker,
            _creation: None,
        }
    }
    fn observe_bytes(&self, bytes: usize) -> Result<(), ConstructionFailure> {
        if bytes == 0 {
            return Ok(());
        }
        let bytes = i64::try_from(bytes).map_err(|_| {
            ConstructionFailure::Control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            )
        })?;
        if let Err(error) = self.tracker.consume_and_check_limit_typed(bytes) {
            self.tracker.release(bytes);
            return Err(ConstructionFailure::Memory(error));
        }
        Ok(())
    }
}
impl InputConstructionRequestHost for TrackedInputConstructionHost {
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure> {
        self.observe_bytes(layout.size())
    }
    fn observe_opaque_bound(&self, bytes_upper: usize) -> Result<(), ConstructionFailure> {
        self.observe_bytes(bytes_upper)
    }
    fn release_requested_bytes(&self, bytes: usize) {
        if bytes != 0 {
            self.tracker
                .release(i64::try_from(bytes).expect("successful construction requests fit i64"));
        }
    }
}

/// Only the independently retained cancellation/error signal is borrowed.
pub struct RuntimeInputConstructionControl {
    error: Arc<RuntimeErrorState>,
    _creation: Option<TrackedArcCharge>,
}
impl RuntimeInputConstructionControl {
    pub fn new(error: Arc<RuntimeErrorState>) -> Self {
        Self {
            error,
            _creation: None,
        }
    }
}

struct TrackedArcCharge {
    tracker: Arc<MemTracker>,
    bytes: usize,
}
impl Drop for TrackedArcCharge {
    fn drop(&mut self) {
        self.tracker
            .release(i64::try_from(self.bytes).expect("successful metadata request fits i64"));
    }
}
fn metadata_request<T>(tracker: &Arc<MemTracker>) -> Result<TrackedArcCharge, ConstructionFailure> {
    let layout = novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<T>())
        .map_err(|error| {
        ConstructionFailure::InvalidSource(format!("runtime input metadata request: {error:?}"))
    })?;
    let bytes = i64::try_from(layout.size()).map_err(|_| {
        ConstructionFailure::Control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        )
    })?;
    if let Err(error) = tracker.consume_and_check_limit_typed(bytes) {
        tracker.release(bytes);
        return Err(ConstructionFailure::Memory(error));
    }
    Ok(TrackedArcCharge {
        tracker: tracker.clone(),
        bytes: layout.size(),
    })
}
struct TrackedArrowModel {
    _creation: TrackedArcCharge,
}
impl super::InputConstructionOperationModel for TrackedArrowModel {
    fn observe(
        &self,
        step: crate::exec::chunk::ArrayNormalizationStep<'_>,
        request: &mut dyn FnMut(Layout) -> Result<(), ConstructionFailure>,
        opaque: &mut dyn FnMut(super::OpaqueConstructionBound) -> Result<(), ConstructionFailure>,
        work: &mut dyn FnMut(usize) -> Result<(), ConstructionFailure>,
    ) -> Result<(), ConstructionFailure> {
        super::LockedArrowInputConstructionModel.observe(step, request, opaque, work)
    }
}

/// Production convenience constructor. Every newly allocated adapter, factory
/// and creator holder is observed before its own Arc construction and retains
/// its independent late-drop responsibility. No process-default tracker,
/// QueryMemoryBinding or entire RuntimeState is substituted for these inputs.
pub fn try_runtime_input_construction_factory(
    profile: super::InputConstructionProfile,
    tracker: Arc<MemTracker>,
    error: Arc<RuntimeErrorState>,
) -> Result<Arc<dyn super::InputConstructionFactory>, ConstructionFailure> {
    if let Some(error) = error.error() {
        return Err(ConstructionFailure::Caller(Box::new(error)));
    }
    let host_charge = metadata_request::<TrackedInputConstructionHost>(&tracker)?;
    let host = Arc::new(TrackedInputConstructionHost {
        tracker: tracker.clone(),
        _creation: Some(host_charge),
    });
    let control_charge = metadata_request::<RuntimeInputConstructionControl>(&tracker)?;
    let control = Arc::new(RuntimeInputConstructionControl {
        error,
        _creation: Some(control_charge),
    });
    let model_charge = metadata_request::<TrackedArrowModel>(&tracker)?;
    let model = Arc::new(TrackedArrowModel {
        _creation: model_charge,
    });
    super::ObservedInputConstructionFactory::try_new(profile, host, control, model)
        .map(|factory| factory as Arc<dyn super::InputConstructionFactory>)
}
impl InputConstructionWorkControl for RuntimeInputConstructionControl {
    fn checkpoint(&self, _: usize) -> Result<(), ConstructionFailure> {
        match self.error.error() {
            Some(error) => Err(ConstructionFailure::Caller(Box::new(error))),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use novarocks_type_contract::ValueTypeStorageLifetime;
    fn profile() -> super::super::InputConstructionProfile {
        super::super::InputConstructionProfile {
            max_requests: 256,
            max_request_bytes: i64::MAX as usize,
            max_opaque_requests: 256,
            max_opaque_bytes: i64::MAX as usize,
            max_work: usize::MAX,
            max_depth: 64,
        }
    }
    fn source() -> Chunk {
        let storage = ValueTypeStorageLifetime::pure();
        Chunk::try_new_typed_with_columns_owned_observed::<String, _>(
            Arc::new(ChunkSchema::try_new_typed(vec![], storage.clone()).unwrap()),
            vec![],
            1,
            storage,
            |_| Ok(()),
        )
        .unwrap()
    }
    #[test]
    fn production_metadata_is_observed_before_creation_and_all_late_drops_balance() {
        let tracker = MemTracker::new_root("construction metadata");
        let error = Arc::new(RuntimeErrorState::default());
        let error_weak = Arc::downgrade(&error);
        let factory =
            try_runtime_input_construction_factory(profile(), tracker.clone(), error).unwrap();
        assert!(tracker.current() > 0);
        let scope = factory.begin_input_construction(&source()).unwrap();
        let hold = scope.storage_lifetime();
        drop(factory);
        assert!(error_weak.upgrade().is_some());
        drop(scope);
        assert!(error_weak.upgrade().is_none());
        assert!(tracker.current() > 0);
        drop(hold);
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn failed_first_adapter_request_creates_no_retained_charge() {
        let tracker = MemTracker::new_root("construction metadata");
        tracker.install_limit_once(1).unwrap();
        assert!(
            try_runtime_input_construction_factory(
                profile(),
                tracker.clone(),
                Arc::new(RuntimeErrorState::default())
            )
            .is_err()
        );
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn runtime_control_returns_the_original_failure_payload() {
        let error = Arc::new(RuntimeErrorState::default());
        let original: crate::runtime::fragment::ExecutionFailure =
            novarocks_functions::KernelFailure::DeadlineExceeded.into();
        error.set_error(original.clone());
        let control = RuntimeInputConstructionControl::new(error);
        assert_eq!(
            control.checkpoint(0).unwrap_err(),
            ConstructionFailure::Caller(Box::new(original))
        );
    }
}
