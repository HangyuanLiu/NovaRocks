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

//! Checked whole-invocation lifecycle. Only an explicitly prepared complete
//! carrier owner may enter this path; old row-bounded partitions stay separate.
use crate::kernel_control::{internal, invalid};
use crate::window_invocation_data::finish_window_lifecycle;
use crate::{
    AggregateStateAllocator, KernelEvaluationControl, KernelFailure, PreparedWindowKernel,
    WindowCallContract, WindowEvaluationFailure, WindowInvocationContext, WindowInvocationInput,
    WindowInvocationScope, WindowKernelPartition, WindowPartitionRetention, WindowResultCarrier,
};
use std::sync::Arc;

pub struct WindowEvaluationInvocation<'input> {
    // Destroy the borrowed runtime instance before the source and host owners.
    instance: Box<dyn WindowKernelPartition + 'input>,
    prepared: Arc<dyn PreparedWindowKernel>,
    contract: Arc<WindowCallContract>,
    input: WindowInvocationInput<'input>,
    context: WindowInvocationContext,
    retained_upper_bound: usize,
    first_failure: Option<WindowEvaluationFailure>,
    finished: bool,
}
impl<'input> WindowEvaluationInvocation<'input> {
    pub fn begin(
        prepared: Arc<dyn PreparedWindowKernel>,
        input: WindowInvocationInput<'input>,
        context: WindowInvocationContext,
        allocator: Option<Arc<dyn AggregateStateAllocator>>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, WindowEvaluationFailure> {
        control.checkpoint(0)?;
        if context.scope() != WindowInvocationScope::CompleteInvocation
            || !prepared.has_original_invocation_handoff()
            || !prepared.has_complete_carrier_handoff()
            || prepared.partition_retention(input.full_input().invocation_rows())?
                != WindowPartitionRetention::InputBackedFrozen
            || !std::ptr::eq(prepared.contract().as_ref(), input.full_input().contract())
        {
            return Err(invalid(
                "window whole invocation differs from its exact source capability",
            )
            .into());
        }
        let contract = Arc::clone(prepared.contract());
        let instance =
            Arc::clone(&prepared).begin_invocation_evaluated(input, context, allocator, control)?;
        // This observation freezes retaining stock AFTER the actual host
        // construction grant. It is never used to authorize that operation.
        let retained_upper_bound = instance.retained_bytes();
        size_of::<Self>()
            .checked_add(retained_upper_bound)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let result = Self {
            instance,
            prepared,
            contract,
            input,
            context,
            retained_upper_bound,
            first_failure: None,
            finished: false,
        };
        result.validate_metadata()?;
        result.validate_retained()?;
        control.checkpoint(0)?;
        Ok(result)
    }
    fn validate_metadata(&self) -> Result<(), KernelFailure> {
        if !Arc::ptr_eq(self.prepared.contract(), &self.contract)
            || !self.prepared.has_original_invocation_handoff()
            || !self.prepared.has_complete_carrier_handoff()
            || self
                .prepared
                .partition_retention(self.input.full_input().invocation_rows())?
                != WindowPartitionRetention::InputBackedFrozen
        {
            return Err(internal(
                "window whole invocation changed its immutable metadata",
            ));
        }
        Ok(())
    }
    fn validate_retained(&self) -> Result<(), KernelFailure> {
        if self.instance.retained_bytes() > self.retained_upper_bound {
            return Err(internal(
                "window whole invocation exceeded frozen retaining stock",
            ));
        }
        Ok(())
    }
    fn latch<T>(
        &mut self,
        result: Result<T, WindowEvaluationFailure>,
    ) -> Result<T, WindowEvaluationFailure> {
        if let Err(cause) = &result {
            self.first_failure = Some(cause.clone());
        }
        result
    }
    pub fn complete_carrier(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<WindowResultCarrier, WindowEvaluationFailure> {
        if let Some(cause) = &self.first_failure {
            return Err(cause.clone());
        }
        if self.finished {
            return Err(KernelFailure::InstanceFailed.into());
        }
        let result = (|| {
            control.checkpoint(0)?;
            self.validate_metadata()?;
            self.validate_retained()?;
            let carrier = self
                .instance
                .complete_carrier_evaluated(control)?
                .ok_or_else(|| invalid("whole window owner omitted its complete carrier"))?;
            carrier.validate_call_metadata(
                &self.contract,
                self.input.full_input().invocation_rows(),
            )?;
            carrier.validate_source_scope(self.context)?;
            control.checkpoint(0)?;
            Ok(carrier)
        })();
        let result = finish_window_lifecycle(result, || self.validate_retained());
        let result = finish_window_lifecycle(result, || self.validate_metadata());
        self.latch(result)
    }
    pub fn finish(
        &mut self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), WindowEvaluationFailure> {
        if let Some(cause) = &self.first_failure {
            return Err(cause.clone());
        }
        if self.finished {
            return Err(KernelFailure::InstanceFailed.into());
        }
        self.finished = true;
        let result = (|| {
            control.checkpoint(0)?;
            self.validate_metadata()?;
            self.validate_retained()?;
            self.instance.finish_evaluated(control)?;
            control.checkpoint(0)?;
            Ok(())
        })();
        let result = finish_window_lifecycle(result, || self.validate_retained());
        let result = finish_window_lifecycle(result, || self.validate_metadata());
        self.latch(result)
    }
}
