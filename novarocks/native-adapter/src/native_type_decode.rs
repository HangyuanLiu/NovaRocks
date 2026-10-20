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

//! Carrier-neutral native execution type-table decoding and construction work.
//!
//! Runtime-filter domains and legacy fragment envelopes borrow this same owner.
//! No fragment tree, original-request actor, or function catalog is retained here.

use crate::native_type_construction::{
    NativeTypeConstruction, NativeTypeConstructionError, NativeTypeFactory,
};
use novarocks_functions::FunctionValueType;
use novarocks_plan_codec::{
    execution_type_table::{
        DecodedExecutionTypeTable, ExecutionTypeTableLimits,
        decode_execution_type_table_with_factory_in,
    },
    host_projection_v2::ProjectionFailure,
    value_type_table::LogicalTypeCodecError,
};
use novarocks_proto_codec::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::{common, plan};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ValueTypeStorageLifetime};
use std::{alloc::Layout, error::Error, fmt};

/// Exact protocol or creator cause, independent of any fragment-tree decoder.
#[derive(Debug)]
pub(crate) enum NativeTypeDecodeError {
    Protocol(ProtocolError),
    Construction(NativeTypeConstructionError),
}
impl NativeTypeDecodeError {
    pub(crate) fn construction_cause(&self) -> Option<&NativeTypeConstructionError> {
        match self {
            Self::Construction(cause) => Some(cause),
            Self::Protocol(_) => None,
        }
    }
    fn invalid_value(path: FieldPath, detail: impl fmt::Display) -> Self {
        ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail.to_string()).into()
    }
}
impl fmt::Display for NativeTypeDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(cause) => cause.fmt(f),
            Self::Construction(cause) => cause.fmt(f),
        }
    }
}
impl Error for NativeTypeDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protocol(cause) => Some(cause),
            Self::Construction(cause) => Some(cause),
        }
    }
}
impl From<ProtocolError> for NativeTypeDecodeError {
    fn from(cause: ProtocolError) -> Self {
        Self::Protocol(cause)
    }
}
impl From<NativeTypeConstructionError> for NativeTypeDecodeError {
    fn from(cause: NativeTypeConstructionError) -> Self {
        Self::Construction(cause)
    }
}
impl From<novarocks_type_contract::owned_resources::layout::LayoutResourceError>
    for NativeTypeDecodeError
{
    fn from(cause: novarocks_type_contract::owned_resources::layout::LayoutResourceError) -> Self {
        NativeTypeConstructionError::Resource(cause).into()
    }
}
impl From<novarocks_type_contract::ValueTypeError> for NativeTypeDecodeError {
    fn from(cause: novarocks_type_contract::ValueTypeError) -> Self {
        Self::invalid_value(FieldPath::root("type"), cause)
    }
}
impl From<novarocks_local_program::FilterMembershipSchemaError> for NativeTypeDecodeError {
    fn from(cause: novarocks_local_program::FilterMembershipSchemaError) -> Self {
        match cause {
            novarocks_local_program::FilterMembershipSchemaError::Resource(cause) => cause.into(),
            cause => {
                Self::invalid_value(FieldPath::root("runtime_filter").field("membership"), cause)
            }
        }
    }
}

/// No mutable host state resides in the immutable namespace or decode context.
pub(crate) struct NativeTypeDecodeResources<'a> {
    pub(crate) creation: &'a NativeTypeConstruction<'a>,
    pub(crate) work: CompileCheckpoints<'a>,
}
impl<'a> NativeTypeDecodeResources<'a> {
    pub(crate) fn try_new(
        creation: &'a NativeTypeConstruction<'a>,
    ) -> Result<Self, NativeTypeDecodeError> {
        Ok(Self {
            creation,
            work: CompileCheckpoints::try_new(creation, CompilePhase::LowerProgram)
                .map_err(NativeTypeConstructionError::Control)?,
        })
    }
    pub(crate) fn storage(&self) -> ValueTypeStorageLifetime {
        self.creation.storage_lifetime()
    }
    pub(crate) fn request(&mut self, layout: Layout) -> Result<(), NativeTypeDecodeError> {
        self.work
            .flush()
            .map_err(NativeTypeConstructionError::Control)?;
        self.creation.request_layout(layout)?;
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<(), NativeTypeDecodeError> {
        self.work
            .finish()
            .map_err(NativeTypeConstructionError::Control)?;
        Ok(())
    }
}

pub(crate) fn table_error(error: LogicalTypeCodecError, path: FieldPath) -> NativeTypeDecodeError {
    match error {
        LogicalTypeCodecError::Control(error) => NativeTypeConstructionError::Control(error).into(),
        LogicalTypeCodecError::ResourceSource(_) => NativeTypeConstructionError::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
        )
        .into(),
        error => NativeTypeDecodeError::invalid_value(path, error),
    }
}
pub(crate) fn table_failure(
    error: ProjectionFailure<LogicalTypeCodecError, NativeTypeConstructionError>,
    path: FieldPath,
) -> NativeTypeDecodeError {
    match error {
        ProjectionFailure::Codec(error) => table_error(error, path),
        ProjectionFailure::Host(error) => error.into(),
    }
}

/// The retained source namespace and the successful decoder's actual request
/// provenance. This upper includes temporary construction requests; it is not
/// a precise retained size, a capacity grant, or a second memory authority.
pub(crate) struct NativeSourceTypes {
    pub(crate) execution: DecodedExecutionTypeTable,
    pub(crate) construction_request_bytes_upper_bound: usize,
}

/// Decode one complete namespace with the actual host factory. Finite grammar
/// limits never replace the creator's exact request and control observations.
pub(crate) fn decode_type_owner(
    table: &plan::ExecutionTypeTable,
    path: FieldPath,
    resources: &mut NativeTypeDecodeResources<'_>,
) -> Result<NativeSourceTypes, NativeTypeDecodeError> {
    let limits = ExecutionTypeTableLimits::for_validated_root_count(table.value_bindings.len())
        .map_err(NativeTypeConstructionError::Control)?;
    let factory = NativeTypeFactory::new(resources.creation);
    let execution =
        decode_execution_type_table_with_factory_in(table, limits, &factory, &mut resources.work)
            .map_err(|error| table_failure(error, path))?;
    Ok(NativeSourceTypes {
        execution,
        construction_request_bytes_upper_bound: factory.construction_request_bytes_upper_bound(),
    })
}

/// Resolve only within this exact owner. The returned binding is borrowed;
/// copying it is a separate observed operation, never an inline type rebuild.
pub(crate) fn resolve_type<'a>(
    table: &'a DecodedExecutionTypeTable,
    desc: &common::TypeDesc,
    path: FieldPath,
    resources: &mut NativeTypeDecodeResources<'_>,
) -> Result<&'a FunctionValueType, NativeTypeDecodeError> {
    table
        .type_reference_observed(desc, &mut resources.work)
        .map_err(|error| table_error(error, path))
}

pub(crate) fn clone_type(
    source: &FunctionValueType,
    resources: &mut NativeTypeDecodeResources<'_>,
) -> Result<FunctionValueType, NativeTypeDecodeError> {
    source
        .clone_observed(|| {
            resources
                .work
                .step()
                .map_err(NativeTypeConstructionError::Control)
        })
        .map_err(Into::into)
}

pub(crate) struct NativeTypeTableDecoder<'types, 'borrow, 'creator> {
    pub(crate) types: &'types DecodedExecutionTypeTable,
    pub(crate) resources: &'borrow mut NativeTypeDecodeResources<'creator>,
}
impl NativeTypeTableDecoder<'_, '_, '_> {
    pub(crate) fn data_type(
        &mut self,
        desc: &common::TypeDesc,
        path: FieldPath,
    ) -> Result<arrow::datatypes::DataType, NativeTypeDecodeError> {
        let value = resolve_type(self.types, desc, path, self.resources)?;
        // Normalized layouts contain shared children/timezones and no
        // Dictionary/REE Box, so this projection has no heap request.
        self.resources
            .work
            .step()
            .map_err(NativeTypeConstructionError::Control)?;
        Ok(value.data_type().clone())
    }
    pub(crate) fn value_type(
        &mut self,
        desc: &common::TypeDesc,
        path: FieldPath,
    ) -> Result<Option<FunctionValueType>, NativeTypeDecodeError> {
        let source = resolve_type(self.types, desc, path, self.resources)?;
        Ok(Some(clone_type(source, self.resources)?))
    }
    pub(crate) fn bindings_match(
        &mut self,
        actual: &novarocks_type_contract::TypedValueBinding,
        expected: &novarocks_type_contract::TypedValueBinding,
        ignore_root_nullability: bool,
    ) -> Result<bool, NativeTypeDecodeError> {
        if !ignore_root_nullability {
            self.resources
                .work
                .step()
                .map_err(NativeTypeConstructionError::Control)?;
            if actual.nullable() != expected.nullable() {
                return Ok(false);
            }
        }
        let semantics = FunctionValueType::borrowed_value_domains_equal_observed::<
            NativeTypeDecodeError,
        >(actual.value_type(), expected.value_type(), || {
            self.resources
                .work
                .step()
                .map_err(|cause| NativeTypeConstructionError::Control(cause).into())
        })?;
        if !semantics {
            return Ok(false);
        }
        actual.layout().equals_observed(expected.layout(), || {
            self.resources
                .work
                .step()
                .map_err(|cause| NativeTypeConstructionError::Control(cause).into())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution::runtime::mem_tracker::MemTracker;
    use novarocks_type_contract::{CompileControlError, PureCompileControl};
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    struct Control {
        calls: Mutex<Vec<(CompilePhase, u32)>>,
        refuse: AtomicBool,
        cause: CompileControlError,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.calls.lock().unwrap().push((phase, units));
            if self.refuse.load(Ordering::SeqCst) {
                Err(self.cause)
            } else {
                Ok(())
            }
        }
    }
    fn control(cause: CompileControlError) -> Control {
        Control {
            calls: Mutex::new(Vec::new()),
            refuse: AtomicBool::new(false),
            cause,
        }
    }

    #[test]
    fn request_flushes_pending_work_before_creation_and_finish_observes_the_tail() {
        let control = control(CompileControlError::Cancelled);
        let tracker = MemTracker::new_root("shared native type work");
        let creation = NativeTypeConstruction::try_with_control(tracker, &control).unwrap();
        let mut resources = NativeTypeDecodeResources::try_new(&creation).unwrap();
        resources.work.step().unwrap();
        control.calls.lock().unwrap().clear();
        resources.request(Layout::new::<u8>()).unwrap();
        assert_eq!(
            control.calls.lock().unwrap()[0],
            (CompilePhase::LowerProgram, 1)
        );
        control.calls.lock().unwrap().clear();
        resources.work.step().unwrap();
        resources.finish().unwrap();
        assert_eq!(
            *control.calls.lock().unwrap(),
            [(CompilePhase::LowerProgram, 1)]
        );
    }

    #[test]
    fn request_and_finish_preserve_each_exact_control_cause() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = control(cause);
            let tracker = MemTracker::new_root("shared native type control");
            let creation =
                NativeTypeConstruction::try_with_control(tracker.clone(), &control).unwrap();
            let mut resources = NativeTypeDecodeResources::try_new(&creation).unwrap();
            resources.work.step().unwrap();
            let before = tracker.current();
            control.refuse.store(true, Ordering::SeqCst);
            let failure = resources.request(Layout::new::<u64>()).unwrap_err();
            assert_eq!(
                failure.construction_cause(),
                Some(&NativeTypeConstructionError::Control(cause))
            );
            assert_eq!(
                tracker.current(),
                before,
                "refused work cannot reach creator allocation"
            );
            let failure = resources.finish().unwrap_err();
            assert_eq!(
                failure.construction_cause(),
                Some(&NativeTypeConstructionError::Control(cause))
            );
        }
    }
}
