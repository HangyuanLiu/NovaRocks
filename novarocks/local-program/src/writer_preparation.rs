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

//! Independently held immutable writer preparation; no live runtime capability.

use novarocks_functions::{AggregateKernelPhase, PreparedAggregateHandle};
use novarocks_type_contract::{
    ValueTypeStorageLifetime,
    owned_resources::layout::{LayoutResourceError, arc_layout},
};
use novarocks_types::SlotId;
use std::{alloc::Layout, fmt, sync::Arc};

/// One exact prepared immutable writer call. It lends no mutable state,
/// function catalog, task/query owner or new capacity authority.
#[derive(Clone, Debug)]
pub struct PreparedWriterAggregateCall {
    handle: PreparedAggregateHandle,
    input: SlotId,
    output: SlotId,
    output_position: usize,
    _storage: ValueTypeStorageLifetime,
}
impl PreparedWriterAggregateCall {
    pub fn new(
        handle: PreparedAggregateHandle,
        input: SlotId,
        output: SlotId,
        output_position: usize,
        storage: ValueTypeStorageLifetime,
    ) -> Self {
        Self {
            handle,
            input,
            output,
            output_position,
            _storage: storage,
        }
    }
    pub fn handle(&self) -> &PreparedAggregateHandle {
        &self.handle
    }
    pub fn input(&self) -> SlotId {
        self.input
    }
    pub fn output(&self) -> SlotId {
        self.output
    }
    pub fn output_position(&self) -> usize {
        self.output_position
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PreparedWriterCallsError {
    InvalidPhase,
    InvalidCall,
}
impl fmt::Display for PreparedWriterCallsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid prepared writer source: {self:?}")
    }
}
impl std::error::Error for PreparedWriterCallsError {}
struct PreparedWriterCallsBacking {
    calls: Box<[PreparedWriterAggregateCall]>,
    phase: AggregateKernelPhase,
    // Actual source/new headers drop before their creator responsibility.
    _storage: ValueTypeStorageLifetime,
}
/// Cheap clones retain one independently created call collection. There is no
/// naked owning collection/handle release API and no whole LocalProgram hold.
#[derive(Clone)]
pub struct PreparedWriterAggregateCalls(Arc<PreparedWriterCallsBacking>);
impl fmt::Debug for PreparedWriterAggregateCalls {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedWriterAggregateCalls")
            .field("phase", &self.phase())
            .field("calls", &self.calls())
            .finish()
    }
}
impl PreparedWriterAggregateCalls {
    /// The caller already observed the actual Box source. This constructor
    /// checks the unchanged writer phase law and observes its true Arc header.
    pub fn try_new_observed<E: From<LayoutResourceError> + From<PreparedWriterCallsError>>(
        calls: Box<[PreparedWriterAggregateCall]>,
        phase: AggregateKernelPhase,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(Layout) -> Result<(), E>,
        mut work: impl FnMut() -> Result<(), E>,
    ) -> Result<Self, E> {
        if !matches!(
            phase,
            AggregateKernelPhase::Partial | AggregateKernelPhase::Final
        ) {
            return Err(PreparedWriterCallsError::InvalidPhase.into());
        }
        for call in &calls {
            work()?;
            let contract = call.handle.contract();
            if contract.phase() != phase
                || contract.logical_argument_types().len() != 1
                || contract.distinct()
                || !contract.order_keys().is_empty()
            {
                return Err(PreparedWriterCallsError::InvalidCall.into());
            }
        }
        work()?;
        request(arc_layout(Layout::new::<PreparedWriterCallsBacking>())?)?;
        Ok(Self(Arc::new(PreparedWriterCallsBacking {
            calls,
            phase,
            _storage: storage,
        })))
    }
    pub fn calls(&self) -> &[PreparedWriterAggregateCall] {
        &self.0.calls
    }
    pub fn phase(&self) -> AggregateKernelPhase {
        self.0.phase
    }
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0._storage.clone()
    }
}
