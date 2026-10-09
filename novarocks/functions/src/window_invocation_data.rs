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

//! Window-specific whole-invocation Data; not an aggregate emission receipt.
//! The exact call and metadata slot are admitted before original computation.

use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::aggregate_invocation_backing::HostShared;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueReservation;
use crate::{KernelFailure, WindowCallContract};
use std::{
    fmt,
    sync::{Arc, OnceLock},
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowInvocationPhase {
    FrameUpdate,
    FrameFinal,
    FrameScalarRead,
    PartitionAssembly,
    InvocationAssembly,
    OutputEmission,
    OutputValidation,
}
struct OriginalData {
    phase: WindowInvocationPhase,
    frame_ordinal: Option<usize>,
    input_row: Option<usize>,
    output_ordinal: Option<usize>,
    frame_origin: Option<crate::WindowFrameOrigin>,
    message: String,
    // The real reservation remains live after Data. Original message backing
    // is destroyed first. Moving this admission requires no new host request,
    // formatter, cloned text, checkpoint, or fallible error footer.
    reservation: OpaqueReservation,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowInvocationScope {
    Partition { ordinal: usize },
    CompleteInvocation,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowInvocationContext {
    call_ordinal: usize,
    scope: WindowInvocationScope,
}
impl WindowInvocationContext {
    pub const fn partition(call_ordinal: usize, partition_ordinal: usize) -> Self {
        Self {
            call_ordinal,
            scope: WindowInvocationScope::Partition {
                ordinal: partition_ordinal,
            },
        }
    }
    pub const fn complete_invocation(call_ordinal: usize) -> Self {
        Self {
            call_ordinal,
            scope: WindowInvocationScope::CompleteInvocation,
        }
    }
    pub const fn call_ordinal(self) -> usize {
        self.call_ordinal
    }
    pub const fn scope(self) -> WindowInvocationScope {
        self.scope
    }
    pub const fn partition_ordinal(self) -> Option<usize> {
        match self.scope {
            WindowInvocationScope::Partition { ordinal } => Some(ordinal),
            WindowInvocationScope::CompleteInvocation => None,
        }
    }
}

struct WindowDataInner {
    context: WindowInvocationContext,
    contract: Arc<WindowCallContract>,
    input_rows: usize,
    data: OnceLock<OriginalData>,
}
#[derive(Clone)]
pub struct WindowInvocationData(HostShared<WindowDataInner>);
impl WindowInvocationData {
    pub fn window_contract(&self) -> &WindowCallContract {
        &self.0.contract
    }
    pub fn call_ordinal(&self) -> usize {
        self.0.context.call_ordinal()
    }
    pub fn invocation_rows(&self) -> usize {
        self.0.input_rows
    }
    pub fn scope(&self) -> WindowInvocationScope {
        self.0.context.scope()
    }
    pub fn partition_ordinal(&self) -> Option<usize> {
        self.data()
            .frame_origin
            .map(|origin| origin.partition_ordinal)
            .or(self.0.context.partition_ordinal())
    }
    pub fn partition_rows(&self) -> Option<usize> {
        if let Some(origin) = self.data().frame_origin {
            Some(origin.partition.end - origin.partition.start)
        } else if self.0.context.partition_ordinal().is_some() {
            Some(self.0.input_rows)
        } else {
            None
        }
    }
    pub fn partition_frame_ordinal(&self) -> Option<usize> {
        self.data()
            .frame_origin
            .map(|origin| origin.output_row - origin.partition.start)
    }

    fn data(&self) -> &OriginalData {
        self.0.data.get().expect("published original Window Data")
    }
    pub fn phase(&self) -> WindowInvocationPhase {
        self.data().phase
    }
    pub fn frame_ordinal(&self) -> Option<usize> {
        self.data().frame_ordinal
    }
    pub fn output_ordinal(&self) -> Option<usize> {
        self.data().output_ordinal
    }
    pub fn input_row(&self) -> Option<usize> {
        self.data().input_row
    }
    pub fn message(&self) -> &str {
        &self.data().message
    }
    pub fn same_backing(&self, other: &Self) -> bool {
        self.0.ptr_eq(&other.0)
    }
    pub fn retained_bytes(&self) -> usize {
        self.0.block_bytes() + self.data().message.capacity()
    }
    pub fn retained_admission_bytes(&self) -> usize {
        self.data().reservation.remaining_bytes()
    }
}
impl PartialEq for WindowInvocationData {
    fn eq(&self, other: &Self) -> bool {
        self.same_backing(other)
    }
}
impl Eq for WindowInvocationData {}
impl fmt::Debug for WindowInvocationData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowInvocationData")
            .field("input_rows", &self.invocation_rows())
            .field("scope", &self.scope())
            .field("partition_ordinal", &self.partition_ordinal())
            .field("phase", &self.phase())
            .field("frame_ordinal", &self.frame_ordinal())
            .field("input_row", &self.input_row())
            .field("output_ordinal", &self.output_ordinal())
            .field("message", &self.message())
            .finish()
    }
}
impl fmt::Display for WindowInvocationData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}
impl std::error::Error for WindowInvocationData {}

/// One per real invocation, allocated before first original frame computation.
/// The empty slot is never an error or a default source receipt.
#[derive(Clone)]
pub(crate) struct WindowDataSlot(HostShared<WindowDataInner>);
impl WindowDataSlot {
    pub(crate) fn context(&self) -> WindowInvocationContext {
        self.0.context
    }

    pub(crate) fn prepare(
        contract: Arc<WindowCallContract>,
        context: WindowInvocationContext,
        partition_rows: usize,
        allocator: HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        Ok(Self(HostShared::try_new(
            WindowDataInner {
                context,
                contract,
                input_rows: partition_rows,
                data: OnceLock::new(),
            },
            allocator,
            work,
        )?))
    }
    pub(crate) fn retained_envelope(&self) -> usize {
        self.0.block_bytes()
            + self
                .0
                .data
                .get()
                .map_or(0, |data| data.reservation.remaining_bytes())
    }
    pub(crate) fn publish(
        &self,
        phase: WindowInvocationPhase,
        frame_ordinal: Option<usize>,
        input_row: Option<usize>,
        message: String,
        reservation: OpaqueReservation,
    ) -> WindowInvocationData {
        self.publish_with_origin(
            phase,
            frame_ordinal,
            input_row,
            None,
            None,
            message,
            reservation,
        )
    }
    pub(crate) fn publish_frame(
        &self,
        phase: WindowInvocationPhase,
        origin: crate::WindowFrameOrigin,
        input_row: Option<usize>,
        message: String,
        reservation: OpaqueReservation,
    ) -> WindowInvocationData {
        self.publish_with_origin(
            phase,
            Some(origin.output_row),
            input_row,
            None,
            Some(origin),
            message,
            reservation,
        )
    }
    pub(crate) fn publish_at_output(
        &self,
        phase: WindowInvocationPhase,
        frame_ordinal: Option<usize>,
        input_row: Option<usize>,
        output_ordinal: Option<usize>,
        message: String,
        reservation: OpaqueReservation,
    ) -> WindowInvocationData {
        self.publish_with_origin(
            phase,
            frame_ordinal,
            input_row,
            output_ordinal,
            None,
            message,
            reservation,
        )
    }
    fn publish_with_origin(
        &self,
        phase: WindowInvocationPhase,
        frame_ordinal: Option<usize>,
        input_row: Option<usize>,
        output_ordinal: Option<usize>,
        frame_origin: Option<crate::WindowFrameOrigin>,
        message: String,
        reservation: OpaqueReservation,
    ) -> WindowInvocationData {
        if self.0.data.get().is_some() {
            return WindowInvocationData(self.0.clone());
        }
        // The original analytic formatter runs under this already-granted
        // operation reservation, not through a new fallible host request.
        // Stage and call ordinal come from the actual invoking host/context.
        let message = match phase {
            WindowInvocationPhase::OutputValidation => message,
            WindowInvocationPhase::FrameUpdate => crate::window_format::window_failure_message(
                self.0.context.call_ordinal(),
                &crate::aggregate_format::AggregateFailureStage::Update.message(&message),
            ),
            WindowInvocationPhase::FrameFinal => crate::window_format::window_failure_message(
                self.0.context.call_ordinal(),
                &crate::aggregate_format::AggregateFailureStage::BuildFinal.message(&message),
            ),
            WindowInvocationPhase::FrameScalarRead
            | WindowInvocationPhase::PartitionAssembly
            | WindowInvocationPhase::InvocationAssembly
            | WindowInvocationPhase::OutputEmission => {
                crate::window_format::window_failure_message(
                    self.0.context.call_ordinal(),
                    &message,
                )
            }
        };
        // First publication wins. A failed instance never starts another
        // operation; an accidental repeated projection keeps original Data.
        let _ = self.0.data.set(OriginalData {
            phase,
            frame_ordinal,
            input_row,
            output_ordinal,
            frame_origin,
            message,
            reservation,
        });
        WindowInvocationData(self.0.clone())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WindowEvaluationFailure {
    Kernel(KernelFailure),
    InvocationData(WindowInvocationData),
}
impl From<KernelFailure> for WindowEvaluationFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl From<WindowInvocationData> for WindowEvaluationFailure {
    fn from(cause: WindowInvocationData) -> Self {
        Self::InvocationData(cause)
    }
}
impl fmt::Display for WindowEvaluationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Kernel(cause) => cause.fmt(f),
            Self::InvocationData(cause) => cause.fmt(f),
        }
    }
}
impl std::error::Error for WindowEvaluationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Kernel(cause) => cause,
            Self::InvocationData(cause) => cause,
        })
    }
}
pub(crate) fn finish_window_lifecycle<T>(
    result: Result<T, WindowEvaluationFailure>,
    post: impl FnOnce() -> Result<(), KernelFailure>,
) -> Result<T, WindowEvaluationFailure> {
    match result {
        Err(data @ WindowEvaluationFailure::InvocationData(_)) => Err(data),
        Ok(value) => post().map(|()| value).map_err(Into::into),
        Err(WindowEvaluationFailure::Kernel(cause)) => {
            crate::aggregate_kernel::finish_lifecycle(Err(cause), post()).map_err(Into::into)
        }
    }
}
