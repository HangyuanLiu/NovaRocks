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

//! One public Arrow operation invoice shared by grouped keys and scan filters.
//! It is a declared policy, not proof of private Arrow capacities or RSS.

use novarocks_functions::opaque_memory::{OpaqueReservation, OpaqueRetainedCharge};
use novarocks_functions::{AggregateStateAllocator, KernelFailure};
use std::sync::Arc;

/// Declared public work, never inferred private allocation geometry.
#[derive(Clone, Copy)]
pub(crate) struct ArrowWorkShape {
    pub(crate) rows: usize,
    pub(crate) cells: usize,
    pub(crate) payload_bytes: usize,
    pub(crate) fields: usize,
    pub(crate) buffers: usize,
}

/// GroupedTopNArrowBudgetV1 from the accepted plan. Overflow is a fatal
/// resource refusal; it cannot turn into a zero budget or a row data error.
fn invoice(shape: ArrowWorkShape) -> Result<usize, KernelFailure> {
    let checked = || {
        16_384usize
            .checked_add(64usize.checked_mul(shape.rows.checked_add(shape.cells)?)?)?
            .checked_add(2usize.checked_mul(shape.payload_bytes)?)?
            .checked_add(2_048usize.checked_mul(shape.fields)?)?
            .checked_add(1_024usize.checked_mul(shape.buffers)?)
    };
    checked().ok_or(KernelFailure::ResourceExhausted)
}

/// Kept alive until the actual public operation and its temporary results
/// have returned/dropped. This same host also adopts its new returned stock.
pub(crate) struct ArrowWorkBudget {
    pub(crate) host: Arc<dyn AggregateStateAllocator>,
    _reservation: OpaqueReservation,
}
impl ArrowWorkBudget {
    pub(crate) fn admit(
        host: Arc<dyn AggregateStateAllocator>,
        shape: ArrowWorkShape,
    ) -> Result<Self, KernelFailure> {
        let authority = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let reservation = authority.reserve_operation(invoice(shape)?)?;
        Ok(Self {
            host,
            _reservation: reservation,
        })
    }
}
