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

//! Owned scalar staging under the same actual opaque host authority.
use crate::aggregate_scalar::{self as scalar, AggScalarValue as V, ScalarStateError, ScalarWork};
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::scalar_output_operation::ScalarOutputFailure;
use crate::scalar_output_resources::ScalarOutputResources;
use crate::{AggregateStateAllocator, KernelFailure};
use arrow_array::ArrayRef;
use std::sync::Arc;

pub(crate) struct WindowOutputScalars {
    values: Vec<Option<V>>,
    charge: OpaqueRetainedCharge,
    heap_bytes: usize,
}
impl WindowOutputScalars {
    pub(crate) fn try_new(
        rows: usize,
        host: Arc<dyn AggregateStateAllocator>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let mut charge = OpaqueRetainedCharge::try_new(host)?;
        let bytes = rows
            .checked_mul(size_of::<Option<V>>())
            .ok_or(KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let mut reservation = charge.reserve_operation(bytes)?;
        work.flush()?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(rows)
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        work.flush()?;
        let retained = values
            .capacity()
            .checked_mul(size_of::<Option<V>>())
            .ok_or(KernelFailure::ResourceExhausted)?;
        charge.reconcile_under_reservation(retained, &mut reservation)?;
        Ok(Self {
            values,
            charge,
            heap_bytes: 0,
        })
    }
    pub(crate) fn push_original_read(
        &mut self,
        values: &ArrayRef,
        row: usize,
        resources: ScalarOutputResources,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), ScalarOutputFailure> {
        let bytes = resources
            .operation_upper_bound()
            .map_err(|cause| match cause {
                ScalarStateError::Kernel(cause) => ScalarOutputFailure::Kernel(cause),
                _ => unreachable!("resource facts contain no value-data error"),
            })?;
        work.flush()?;
        let mut reservation = self.charge.reserve_operation(bytes)?;
        work.flush()?;
        let value = match scalar::scalar_from_array(values, row, &mut ScalarWork::new(Some(work))) {
            Ok(value) => value,
            Err(ScalarStateError::Kernel(cause)) => return Err(cause.into()),
            Err(ScalarStateError::OutputAllocation(_)) => {
                return Err(KernelFailure::ResourceExhausted.into());
            }
            Err(ScalarStateError::Legacy(message)) => {
                return Err(ScalarOutputFailure::OriginalData {
                    message,
                    reservation,
                });
            }
        };
        // All original scalar copies have completed under the real operation
        // lease. Cached retained growth is from their actual owned capacities.
        let heap = ScalarOutputResources::owned_heap_bytes(
            std::slice::from_ref(&value),
            &mut ScalarWork::new(Some(work)),
        )
        .map_err(|cause| match cause {
            ScalarStateError::Kernel(cause) => ScalarOutputFailure::Kernel(cause),
            _ => unreachable!("graph capacity facts contain no value-data error"),
        })?;
        let heap_bytes = self
            .heap_bytes
            .checked_add(heap)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let retained = self
            .values
            .capacity()
            .checked_mul(size_of::<Option<V>>())
            .and_then(|bytes| bytes.checked_add(heap_bytes))
            .ok_or(KernelFailure::ResourceExhausted)?;
        if self.values.len() == self.values.capacity() {
            return Err(crate::kernel_control::invalid(
                "window scalar staging exceeds its exact frame/output extent",
            )
            .into());
        }
        self.charge
            .reconcile_under_reservation(retained, &mut reservation)?;
        self.values.push(value);
        self.heap_bytes = heap_bytes;
        work.step()?;
        Ok(())
    }
    pub(crate) fn values(&self) -> &[Option<V>] {
        &self.values
    }
    pub(crate) fn into_parts(self) -> (Vec<Option<V>>, OpaqueRetainedCharge) {
        (self.values, self.charge)
    }
}
