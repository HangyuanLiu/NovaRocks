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
use novarocks_execution_contract::failure::{FailureFact, FailureProjection, SharedFailureCause};
use novarocks_functions::FunctionValueType;
use novarocks_plan_codec::{
    execution_type_table::{
        DecodedExecutionTypeTable, ExecutionTypeTableLimits,
        decode_execution_type_table_with_factory_in,
    },
    host_projection_v2::ProjectionFailure,
    value_type_table::LogicalTypeCodecError,
};
use novarocks_proto_codec::{FieldPath, ProtocolError};
use novarocks_proto_models::{common, plan};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
use std::{alloc::Layout, error::Error, fmt};

/// Exact protocol, creator, or field-scoped domain cause, independent of any fragment-tree decoder.
#[derive(Debug)]
pub(crate) enum NativeTypeDecodeError {
    Protocol(ProtocolError),
    Construction(NativeTypeConstructionError),
    Source {
        path: FieldPath,
        cause: SharedFailureCause,
    },
}
impl NativeTypeDecodeError {
    #[cfg(test)]
    pub(crate) fn construction_cause(&self) -> Option<&NativeTypeConstructionError> {
        match self {
            Self::Construction(cause) => Some(cause),
            Self::Protocol(_) | Self::Source { .. } => None,
        }
    }
    pub(crate) fn from_domain<E>(path: FieldPath, cause: E) -> Self
    where
        E: Error + FailureProjection + Send + Sync + 'static,
    {
        Self::Source {
            path,
            cause: SharedFailureCause::new(cause),
        }
    }
}
impl fmt::Display for NativeTypeDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(cause) => cause.fmt(f),
            Self::Construction(cause) => cause.fmt(f),
            Self::Source { path, cause } => {
                write!(f, "native type decode error at {path}: {cause}")
            }
        }
    }
}
impl Error for NativeTypeDecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protocol(cause) => Some(cause),
            Self::Construction(cause) => Some(cause),
            Self::Source { cause, .. } => Some(cause),
        }
    }
}
impl FailureProjection for NativeTypeDecodeError {
    fn failure_fact(&self) -> FailureFact {
        match self {
            Self::Protocol(cause) => cause.failure_fact(),
            Self::Construction(cause) => cause.failure_fact(),
            Self::Source { cause, .. } => cause.failure_fact(),
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
        Self::from_domain(FieldPath::root("type"), cause)
    }
}
impl From<novarocks_local_program::FilterMembershipSchemaError> for NativeTypeDecodeError {
    fn from(cause: novarocks_local_program::FilterMembershipSchemaError) -> Self {
        Self::from_domain(FieldPath::root("runtime_filter").field("membership"), cause)
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
    NativeTypeDecodeError::from_domain(path, error)
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

pub(crate) struct NativeSourceTypes {
    pub(crate) execution: DecodedExecutionTypeTable,
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
    Ok(NativeSourceTypes { execution })
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
                Err(self.cause.clone())
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

    fn assert_domain_source<E>(
        original: E,
        expected_path: FieldPath,
        convert: impl FnOnce(E) -> NativeTypeDecodeError,
    ) -> NativeTypeDecodeError
    where
        E: Error + FailureProjection + Send + Sync + 'static,
    {
        let expected = original.failure_fact();
        let decoded = convert(original);
        assert_eq!(decoded.failure_fact(), expected);
        let NativeTypeDecodeError::Source { path, cause } = &decoded else {
            panic!("the original domain cause must not become a protocol error");
        };
        assert_eq!(path, &expected_path);
        assert!(decoded.to_string().contains(&expected_path.to_string()));
        let retained = cause.downcast_ref::<E>().unwrap();
        assert!(std::ptr::eq(
            retained,
            decoded
                .source()
                .unwrap()
                .source()
                .unwrap()
                .downcast_ref::<E>()
                .unwrap(),
        ));
        decoded
    }

    #[test]
    fn intrinsic_value_and_membership_conversions_keep_the_original_domain_and_path() {
        use novarocks_local_program::FilterMembershipSchemaError;
        use novarocks_type_contract::{
            ValueTypeError, owned_resources::layout::LayoutResourceError,
        };
        for original in [
            ValueTypeError::TooDeep,
            ValueTypeError::InvalidType("memory exhausted unsupported cancelled"),
        ] {
            assert_domain_source(
                original,
                FieldPath::root("type"),
                NativeTypeDecodeError::from,
            );
        }
        for original in [
            FilterMembershipSchemaError::DigestMismatch,
            FilterMembershipSchemaError::Resource(LayoutResourceError::Arithmetic),
            FilterMembershipSchemaError::TimezoneLengthOverflow {
                required: usize::MAX,
                source: u32::try_from(u64::MAX).unwrap_err(),
            },
        ] {
            assert_domain_source(
                original,
                FieldPath::root("runtime_filter").field("membership"),
                NativeTypeDecodeError::from,
            );
        }
    }

    #[test]
    fn type_table_codec_causes_keep_the_whole_author_fact_and_original_source_chain() {
        use novarocks_type_contract::ExecutionValueTypeError;
        for original in [
            LogicalTypeCodecError::InvalidShape("memory exhausted unsupported cancelled"),
            LogicalTypeCodecError::ResourceSource("original source model"),
            LogicalTypeCodecError::ValueType(ExecutionValueTypeError::Depth),
            LogicalTypeCodecError::ValueType(ExecutionValueTypeError::Invalid("unsupported")),
            LogicalTypeCodecError::Control(CompileControlError::Cancelled),
            LogicalTypeCodecError::Control(CompileControlError::DeadlineExceeded),
            LogicalTypeCodecError::Control(CompileControlError::ResourceExhausted),
        ] {
            let path = FieldPath::root("runtime_filter")
                .field("type_table")
                .index(3);
            assert_domain_source(original, path.clone(), |original| {
                table_error(original, path)
            });
        }
    }

    #[test]
    fn protocol_and_host_construction_project_the_original_facts_without_text_classification() {
        use novarocks_execution_contract::failure::FailureClass;
        use novarocks_proto_codec::ProtocolErrorKind;
        let protocol = ProtocolError::new(
            FieldPath::root("type"),
            ProtocolErrorKind::InvalidValue,
            "memory exhausted cancelled unsupported",
        );
        let expected = protocol.failure_fact();
        let decoded = NativeTypeDecodeError::from(protocol);
        assert_eq!(decoded.failure_fact(), expected);
        assert_eq!(decoded.failure_fact().class(), FailureClass::Data);
        assert!(
            decoded
                .source()
                .unwrap()
                .downcast_ref::<ProtocolError>()
                .is_some()
        );
        for original in [
            NativeTypeConstructionError::Control(CompileControlError::Cancelled),
            NativeTypeConstructionError::Control(CompileControlError::DeadlineExceeded),
            NativeTypeConstructionError::Control(CompileControlError::ResourceExhausted),
        ] {
            let expected = original.failure_fact();
            let decoded = table_failure(
                ProjectionFailure::Host(original.clone()),
                FieldPath::root("type"),
            );
            assert_eq!(decoded.failure_fact(), expected);
            assert_eq!(decoded.construction_cause(), Some(&original));
            assert_eq!(
                decoded
                    .source()
                    .unwrap()
                    .downcast_ref::<NativeTypeConstructionError>(),
                Some(&original),
            );
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
            let control = control(cause.clone());
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
                Some(&NativeTypeConstructionError::Control(cause.clone()))
            );
            assert_eq!(
                tracker.current(),
                before,
                "refused work cannot reach creator allocation"
            );
            let failure = resources.finish().unwrap_err();
            assert_eq!(
                failure.construction_cause(),
                Some(&NativeTypeConstructionError::Control(cause.clone()))
            );
        }
    }
}
