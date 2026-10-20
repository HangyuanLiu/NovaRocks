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

//! Legacy arena constant broadcasting. Format checks belong to the shared
//! neutral selected-copy author; this adapter retains the existing old path.

use arrow::array::{ArrayRef, UInt32Array};
use novarocks_functions::{ConstantValue, selected_copy::preflight_broadcast};

pub(super) fn broadcast(
    value: &ConstantValue,
    rows: usize,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    broadcast_body(value, rows, None, None)
}
/// Same immutable ordinal/index/take operation under its actual constructor.
/// The caller keeps the source pool's finite storage through any array aliases.
pub(crate) fn broadcast_with_construction(
    value: &ConstantValue,
    rows: usize,
    temporary: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    retained: &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    control: &dyn novarocks_functions::KernelEvaluationControl,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    broadcast_body(value, rows, Some((temporary, retained)), Some(control))
}
fn broadcast_body(
    value: &ConstantValue,
    rows: usize,
    owners: Option<(
        &dyn crate::exec::pipeline::operator::InputConstructionOwner,
        &dyn crate::exec::pipeline::operator::InputConstructionOwner,
    )>,
    control: Option<&dyn novarocks_functions::KernelEvaluationControl>,
) -> crate::runtime::fragment::ExecutionResult<ArrayRef> {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_functions::array_projection::ArrayNormalizationStep;
    use novarocks_functions::selected_copy::{
        CopyIndexView, preflight_primitive_vec_adoption_construction_observed,
        preflight_take_structure_construction,
    };
    use novarocks_type_contract::ValueSchemaCreation;
    let source = value.pool().array();
    if let Some(control) = control {
        control.checkpoint(0)?;
    }
    if rows == 0 {
        if let Some((_, owner)) = owners {
            owner.observe(
                0,
                &[],
                ArrayNormalizationStep::SliceArray {
                    source,
                    offset: value.ordinal() as usize,
                    length: 0,
                },
            )?;
        }
        let output = source.slice(value.ordinal() as usize, 0);
        if let Some((_, owner)) = owners {
            owner.observe(0, &[], ArrayNormalizationStep::OpaqueOperationFinished)?;
        }
        return Ok(output);
    }
    // Explicit legacy expansion preserves its old range preflight. Strong
    // callers use the same actual indices and sole structural take author.
    if owners.is_none() {
        preflight_broadcast(source.as_ref(), value.ordinal(), rows, |_| Ok(()))
            .map_err(constant_copy_failure)?;
    }
    let mut indices = constant_indices(rows, owners.map(|(owner, _)| owner))?;
    indices.resize(rows, value.ordinal());
    if let Some((temporary, _)) = owners {
        let mut request = |layout| temporary.request(layout);
        let mut opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(temporary.storage_lifetime(), &mut request, &mut opaque);
        let mut work = |boundary: bool| {
            temporary.observe(
                0,
                &[],
                ArrayNormalizationStep::InspectConstructionWork {
                    units: usize::from(!boundary),
                },
            )
        };
        preflight_primitive_vec_adoption_construction_observed(&mut creation, &mut work)
            .map_err(copy_failure)?;
    }
    let indices = UInt32Array::from(indices);
    if let Some((temporary, retained)) = owners {
        let mut temporary_request = |layout| temporary.request(layout);
        let mut temporary_opaque = |bytes| temporary.observe_opaque_upper_bound(bytes);
        let mut retained_request = |layout| retained.request(layout);
        let mut retained_opaque = |bytes| retained.observe_opaque_upper_bound(bytes);
        let mut temporary_creation = ValueSchemaCreation::new(
            temporary.storage_lifetime(),
            &mut temporary_request,
            &mut temporary_opaque,
        );
        let mut retained_creation = ValueSchemaCreation::new(
            retained.storage_lifetime(),
            &mut retained_request,
            &mut retained_opaque,
        );
        let mut work = |boundary: bool| {
            temporary.observe(
                0,
                &[],
                ArrayNormalizationStep::InspectConstructionWork {
                    units: usize::from(!boundary),
                },
            )
        };
        preflight_take_structure_construction(
            source.as_ref(),
            CopyIndexView::UInt32(&indices),
            &mut temporary_creation,
            &mut retained_creation,
            &mut work,
        )
        .map_err(copy_failure)?;
    }
    let output = arrow::compute::take(source.as_ref(), &indices, None).map_err(|source| {
        crate::runtime::fragment::ExecutionFailure::from_domain(ConstantBroadcastFailure::Arrow(
            source,
        ))
    })?;
    if let Some(control) = control {
        control.checkpoint(0)?;
    }
    if !novarocks_type_contract::arrow_data_types_exact(output.data_type(), source.data_type()) {
        return Err(ConstructionFailure::InvalidSource(
            "constant broadcast changed the exact Arrow carrier".to_owned(),
        )
        .into());
    }
    Ok(output)
}

fn constant_indices(
    rows: usize,
    owner: Option<&dyn crate::exec::pipeline::operator::InputConstructionOwner>,
) -> crate::runtime::fragment::ExecutionResult<Vec<u32>> {
    let layout = std::alloc::Layout::array::<u32>(rows).map_err(|source| {
        crate::runtime::fragment::ExecutionFailure::from_domain(
            ConstantBroadcastFailure::IndexBacking { rows, source },
        )
    })?;
    if let Some(owner) = owner {
        owner.request(layout)?;
    }
    let mut indices = Vec::new();
    indices.try_reserve_exact(rows).map_err(|source| {
        crate::runtime::fragment::ExecutionFailure::from_domain(
            novarocks_functions::allocation_failure::AllocationRefusal::new(
                "reserve constant broadcast indices",
                source,
            ),
        )
    })?;
    Ok(indices)
}

fn copy_failure(
    failure: novarocks_functions::selected_copy::CopyConstructionError<
        crate::exec::pipeline::operator::ConstructionFailure,
    >,
) -> crate::runtime::fragment::ExecutionFailure {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_functions::selected_copy::CopyConstructionError;
    match failure {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(source) => constant_copy_failure(source),
    }
}
fn constant_copy_failure(
    source: novarocks_functions::selected_copy::CopyError,
) -> crate::runtime::fragment::ExecutionFailure {
    match source {
        novarocks_functions::selected_copy::CopyError::Control(cause) => cause.into(),
        source => crate::runtime::fragment::ExecutionFailure::from_domain(
            ConstantBroadcastFailure::Copy(source),
        ),
    }
}

/// This boundary broadcasts one checked constant-pool value. A refusal of
/// its whole-copy geometry is distinct from the actual creator's capacity
/// error and from the caller's original control failure.
#[derive(Debug)]
enum ConstantBroadcastFailure {
    IndexBacking {
        rows: usize,
        source: std::alloc::LayoutError,
    },
    Copy(novarocks_functions::selected_copy::CopyError),
    Arrow(arrow::error::ArrowError),
}
impl std::fmt::Display for ConstantBroadcastFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IndexBacking { rows, source } => write!(
                f,
                "constant broadcast index backing requires {} bytes: {source}",
                *rows as u128 * std::mem::size_of::<u32>() as u128,
            ),
            Self::Copy(source) => source.fmt(f),
            Self::Arrow(source) => write!(f, "constant pool broadcast failed: {source}"),
        }
    }
}
impl std::error::Error for ConstantBroadcastFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IndexBacking { source, .. } => Some(source),
            Self::Copy(source) => Some(source),
            Self::Arrow(source) => Some(source),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for ConstantBroadcastFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use crate::runtime::fragment::ExecutionReason;
        use novarocks_failure_contract::{FailureClass, FailureFact};
        match self {
            Self::IndexBacking { rows, .. } => {
                novarocks_functions::allocation_failure::rust_array_backing_failure_fact(
                    ExecutionReason::ConstantBroadcastIndexBacking.reason(),
                    *rows as u128 * std::mem::size_of::<u32>() as u128,
                    novarocks_failure_contract::RepresentationBoundary::SingleOutput,
                )
            }
            Self::Copy(source) => source.failure_fact(),
            Self::Arrow(_) => FailureFact::new(
                FailureClass::Invariant,
                ExecutionReason::ConstantBroadcastArrow.reason(),
            ),
        }
    }
}

#[cfg(test)]
mod failure_projection_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, RepresentationBoundary};
    use novarocks_functions::{KernelFailure, selected_copy::CopyError};
    #[test]
    fn constant_index_backing_retains_actual_layout_source_before_any_allocation() {
        use std::error::Error;
        let failure = constant_indices(usize::MAX, None).unwrap_err();
        let fact = failure.failure_fact();
        assert_eq!(fact.class(), FailureClass::Representation);
        assert_eq!(
            fact.reason(),
            crate::runtime::fragment::ExecutionReason::ConstantBroadcastIndexBacking.reason()
        );
        let extent = fact.representation().unwrap();
        assert_eq!(extent.required, None);
        assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
        assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
        assert!(fact.resource().is_none());
        let mut source = failure.source();
        let mut original_layout = false;
        while let Some(actual) = source {
            original_layout |= actual.is::<std::alloc::LayoutError>();
            source = actual.source();
        }
        assert!(original_layout);
        assert_eq!(constant_indices(0, None).unwrap().capacity(), 0);
    }
    #[test]
    fn constant_adapter_forwards_real_copy_geometry_instead_of_inventing_its_boundary() {
        let source = arrow::array::StringArray::from(vec![Some("xy")]);
        let error = novarocks_functions::selected_copy::preflight_broadcast(
            &source,
            0,
            i32::MAX as usize,
            |_| Ok(()),
        )
        .unwrap_err();
        let expected = error.failure_fact();
        let failure = constant_copy_failure(error);
        assert_eq!(failure.failure_fact(), expected);
        assert_eq!(expected.class(), FailureClass::Representation);
        assert_eq!(
            expected.representation().unwrap().boundary,
            RepresentationBoundary::SingleOutput
        );
        let source = arrow::array::Int64Array::from(vec![1]);
        let error =
            novarocks_functions::selected_copy::preflight_broadcast(&source, 0, usize::MAX, |_| {
                Ok(())
            })
            .unwrap_err();
        let expected = error.failure_fact();
        let failure = constant_copy_failure(error);
        assert_eq!(failure.failure_fact(), expected);
        assert_eq!(expected.class(), FailureClass::Representation);
        let extent = expected.representation().unwrap();
        assert_eq!(extent.layout.code.get(), 80);
        assert_eq!(extent.required, None);
        assert_eq!(extent.extent_limit, Some(isize::MAX as u64));
        assert_eq!(extent.boundary, RepresentationBoundary::SingleOutput);
        assert!(expected.resource().is_none());

        let failure = constant_copy_failure(CopyError::Control(KernelFailure::ResourceExhausted));
        assert_eq!(
            failure.failure_fact(),
            KernelFailure::ResourceExhausted.failure_fact()
        );
        assert_eq!(failure.failure_fact().representation(), None);
    }
}
