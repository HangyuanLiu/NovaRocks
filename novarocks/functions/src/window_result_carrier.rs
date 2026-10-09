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

//! Narrow full-partition carrier handoff to the original output-layout owner.
//! Actual legacy carriers may reveal a final layout Data error; that is not a
//! license to weaken SelectedValues or FixedZero scalar/window validation.
use crate::window_invocation_data::{WindowDataSlot, WindowInvocationData, WindowInvocationPhase};
use crate::opaque_memory::OpaqueReservation;
use crate::{KernelFailure, WindowCallContract};
use arrow_array::ArrayRef;
use std::sync::Arc;

pub struct WindowResultCarrier {
    values: ArrayRef,
    contract: Arc<WindowCallContract>,
    input_rows: usize,
    source: WindowDataSlot,
    original_carrier_stock: usize,
}
impl WindowResultCarrier {
    pub(crate) fn from_actual_complete_input(
        values: ArrayRef,
        contract: Arc<WindowCallContract>,
        input_rows: usize,
        source: WindowDataSlot,
        original_carrier_stock: usize,
    ) -> Self {
        Self {
            values,
            contract,
            input_rows,
            source,
            original_carrier_stock,
        }
    }
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }
    pub fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }
    pub fn invocation_rows(&self) -> usize {
        self.input_rows
    }
    pub fn source_context(&self) -> crate::WindowInvocationContext {
        self.source.context()
    }
    pub fn validate_source_scope(
        &self,
        context: crate::WindowInvocationContext,
    ) -> Result<(), KernelFailure> {
        if self.source.context() != context {
            return Err(crate::kernel_control::invalid(
                "complete window carrier has another source occurrence/domain",
            ));
        }
        Ok(())
    }
    /// Observation of the original builder's actual owned carrier, not a grant.
    pub fn original_carrier_stock(&self) -> usize {
        self.original_carrier_stock
    }
    /// The host already ran the ONE original output validator. This projection
    /// preserves its full text and the actual output ordinal, without reading
    /// or parsing text to infer an error category or source.
    pub fn original_output_validation_data(
        &self,
        output_ordinal: usize,
        message: String,
        reservation: OpaqueReservation,
    ) -> WindowInvocationData {
        self.source.publish_at_output(
            WindowInvocationPhase::OutputValidation,
            None,
            None,
            Some(output_ordinal),
            message,
            reservation,
        )
    }
    /// Check the immutable author receipt and invocation domain. Actual Arrow
    /// carrier validation belongs to the ONE original analytic output author;
    /// rejecting its returned carrier here would turn original layout Data
    /// into an InvalidProgram before that original author can run.
    pub fn validate_call_metadata(
        &self,
        contract: &Arc<WindowCallContract>,
        rows: usize,
    ) -> Result<(), KernelFailure> {
        if !Arc::ptr_eq(&self.contract, contract) || self.input_rows != rows {
            return Err(crate::kernel_control::invalid(
                "complete window carrier differs from its exact call/domain",
            ));
        }
        Ok(())
    }
}
