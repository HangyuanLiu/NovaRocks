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

pub(super) fn broadcast(value: &ConstantValue, rows: usize) -> Result<ArrayRef, String> {
    broadcast_body(value, rows, None, None).map_err(|error| error.to_string())
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
            .map_err(|error| error.to_string())?;
    }
    let mut indices = if let Some((owner, _)) = owners {
        owner.request(
            std::alloc::Layout::array::<u32>(rows)
                .map_err(|_| novarocks_functions::KernelFailure::ResourceExhausted)?,
        )?;
        let mut indices = Vec::new();
        indices
            .try_reserve_exact(rows)
            .map_err(|_| novarocks_functions::KernelFailure::ResourceExhausted)?;
        indices
    } else {
        Vec::with_capacity(rows)
    };
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
    let output = arrow::compute::take(source.as_ref(), &indices, None)
        .map_err(|error| format!("constant pool broadcast failed: {error}"))?;
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

fn copy_failure(
    failure: novarocks_functions::selected_copy::CopyConstructionError<
        crate::exec::pipeline::operator::ConstructionFailure,
    >,
) -> crate::runtime::fragment::ExecutionFailure {
    use crate::exec::pipeline::operator::ConstructionFailure;
    use novarocks_functions::selected_copy::{CopyConstructionError, CopyError};
    match failure {
        CopyConstructionError::Creator(cause) => cause.into(),
        CopyConstructionError::Resource(cause) => ConstructionFailure::Resource(cause).into(),
        CopyConstructionError::Copy(CopyError::Control(cause)) => cause.into(),
        CopyConstructionError::Copy(CopyError::Extent) => {
            novarocks_functions::KernelFailure::ResourceExhausted.into()
        }
        CopyConstructionError::Copy(CopyError::Invalid(message)) => {
            ConstructionFailure::InvalidSource(message.to_owned()).into()
        }
        CopyConstructionError::Copy(CopyError::Unsupported(ty)) => {
            ConstructionFailure::InvalidSource(format!(
                "constant broadcast carrier {ty:?} is unsupported"
            ))
            .into()
        }
    }
}
