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

//! BE-local consumer input construction. These installed capabilities never
//! form part of a frontend wire declaration or create a memory grant authority.

use std::alloc::Layout;

use arrow::datatypes::DataType;
use novarocks_type_contract::ValueTypeStorageLifetime;
use novarocks_types::SlotId;

use crate::exec::chunk::{ArrayNormalizationStep, ArrayPathStep, Chunk, ChunkSchema};
use crate::runtime::fragment::ExecutionFailure;

#[path = "input_construction/arrow_model.rs"]
pub mod arrow_model;
#[path = "input_construction/runtime.rs"]
pub mod runtime;
#[path = "input_construction/scoped.rs"]
pub mod scoped;
pub use arrow_model::LockedArrowInputConstructionModel;
pub use runtime::{
    ReceiverInputConstructionStopOwner, try_receiver_input_construction_factory,
    try_runtime_input_construction_factory,
};
pub use scoped::{
    InputConstructionOperationModel, InputConstructionProfile, InputConstructionRequestHost,
    InputConstructionWorkControl, ObservedInputConstructionFactory, OpaqueConstructionBound,
};

/// Construction failures retain the original caller payload. Source and
/// admission failures have explicit variants and are never inferred from text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConstructionFailure {
    MissingOwner,
    Memory(crate::runtime::mem_tracker::MemTrackerLimitExceeded),
    InvalidSource(String),
    Control(novarocks_type_contract::CompileControlError),
    Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError),
    Caller(Box<ExecutionFailure>),
}
impl ConstructionFailure {
    pub fn detail(&self) -> &str {
        match self {
            Self::MissingOwner => "typed input construction owner is missing",
            Self::Memory(error) => error.detail(),
            Self::InvalidSource(error) => error,
            Self::Control(novarocks_type_contract::CompileControlError::Cancelled) => {
                "input construction was cancelled"
            }
            Self::Control(novarocks_type_contract::CompileControlError::DeadlineExceeded) => {
                "input construction deadline was exceeded"
            }
            Self::Control(novarocks_type_contract::CompileControlError::ResourceExhausted) => {
                "input construction work resources were exhausted"
            }
            Self::Caller(error) => error.detail(),
            Self::Resource(_) => "input construction storage layout cannot be represented",
        }
    }
}
impl From<String> for ConstructionFailure {
    fn from(error: String) -> Self {
        Self::InvalidSource(error)
    }
}
impl From<novarocks_type_contract::CompileControlError> for ConstructionFailure {
    fn from(error: novarocks_type_contract::CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<ExecutionFailure> for ConstructionFailure {
    fn from(error: ExecutionFailure) -> Self {
        Self::Caller(Box::new(error))
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for ConstructionFailure
{
    fn from(error: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}
impl std::fmt::Display for ConstructionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingOwner => f.write_str("typed input construction owner is missing"),
            Self::Memory(error) => error.fmt(f),
            Self::InvalidSource(error) => f.write_str(error),
            Self::Control(error) => error.fmt(f),
            Self::Caller(error) => error.fmt(f),
            Self::Resource(error) => write!(f, "input construction storage layout: {error:?}"),
        }
    }
}
impl std::error::Error for ConstructionFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Memory(error) => Some(error),
            Self::Caller(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

/// Preparation installs this factory once. Each actual transformation obtains
/// an independent construction scope. The scope's Box/holder creation itself
/// is observed by the original factory before allocation; a caller cannot reset
/// a shared current scope or reuse a preparation-wide cumulative byte allowance.
pub trait InputConstructionFactory: Send + Sync {
    fn observe_input_work(&self, units: usize) -> Result<(), ConstructionFailure>;
    /// Begin one actual construction from an existing explicit binding. This
    /// does not retain the schema or fabricate an input chunk for source and
    /// aggregate authors. The scope owns only newly constructed storage.
    fn begin_typed_construction(
        &self,
        schema: &ChunkSchema,
    ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure>;
    /// Begin schema construction from an already frozen complete declaration.
    /// No runtime ChunkSchema is manufactured before the creating owner exists.
    fn begin_compiled_layout_construction(
        &self,
        layout: &novarocks_local_program::StaticLayout,
    ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure>;
    /// Create the runtime schema through the same actual preparation owner.
    /// The resulting schema retains only its finite storage responsibility;
    /// it does not keep this factory or the whole compiled program alive.
    fn create_compiled_chunk_schema(
        &self,
        layout: &novarocks_local_program::StaticLayout,
    ) -> Result<crate::exec::chunk::ChunkSchemaRef, ConstructionFailure> {
        let owner = self.begin_compiled_layout_construction(layout)?;
        ChunkSchema::from_compiled_layout_owned_observed::<ConstructionFailure>(
            layout,
            owner.storage_lifetime(),
            |layout| owner.request(layout),
            |units| {
                owner.observe(
                    0,
                    &[],
                    ArrayNormalizationStep::InspectConstructionWork { units },
                )
            },
        )
    }
    fn begin_input_construction(
        &self,
        source: &Chunk,
    ) -> Result<Box<dyn InputConstructionOwner>, ConstructionFailure> {
        self.begin_typed_construction(source.chunk_schema())
    }
}

/// One actual encoded occurrence. Slot identity and output occurrence are both
/// explicit; a parent path cannot authorize an independently encoded child.
#[derive(Clone, Copy, Debug)]
pub struct EncodedValueOccurrence<'a> {
    pub slot: SlotId,
    pub occurrence: usize,
    pub path: &'a [ArrayPathStep],
    pub carrier: &'a DataType,
}

/// Installed by the actual pipeline preparation owner. Implementations check
/// their finite construction request counts/bytes and cooperative control
/// before each operation. They retain only their constructed storage's real
/// responsibility, never QueryContext, MemoryAuthority, or a whole input Chunk.
/// Observation grants no capacity and publishes no allocator facts.
pub trait InputConstructionOwner: Send + Sync {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime;

    /// One concrete constructor request, before the allocation it describes.
    fn request(&self, layout: Layout) -> Result<(), ConstructionFailure>;

    /// A locked library's operation bound remains opaque; it is never turned
    /// into a fabricated Layout or a unique allocator fact.
    fn observe_opaque_upper_bound(&self, bytes_upper: usize) -> Result<(), ConstructionFailure>;

    /// Actual borrowed work and representation operation. Decode/rebuild events
    /// precede their allocations; the owner validates their concrete shapes and
    /// requests before allowing them. It must not estimate unknown backing from
    /// ArrayRef size or derive semantics from Field metadata.
    fn observe(
        &self,
        occurrence: usize,
        path: &[ArrayPathStep],
        step: ArrayNormalizationStep<'_>,
    ) -> Result<(), ConstructionFailure>;
}
