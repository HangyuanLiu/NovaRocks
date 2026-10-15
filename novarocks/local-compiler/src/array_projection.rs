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

//! The pure constant creator uses the runtime's sole descriptor normalization
//! body and construction geometry. It acquires no allocation capacity.
use crate::FragmentCompileError;
use arrow_array::ArrayRef;
use novarocks_functions::array_projection::{
    ArrayProjectionOpaqueBound, normalize_execution_array_observed,
    observe_array_normalization_construction,
};
use novarocks_local_program::LayoutCompileError;
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, FunctionValueType};

pub(crate) fn normalize_constant_column_with_creation<H>(
    source: &ArrayRef,
    ty: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
    creation: &mut novarocks_type_contract::ValueSchemaCreation<'_, H>,
) -> Result<ArrayRef, crate::FragmentCreationError<H>> {
    let normalized =
        normalize_execution_array_observed::<crate::FragmentCreationError<H>, _>(source, |step| {
            work.flush()?;
            let control = work.control();
            // Request and opaque bounds reborrow the same real creator through
            // sequential construction callbacks without erasing its refusal.
            let mut observe = |units: usize| -> Result<(), crate::FragmentCreationError<H>> {
                if units == 0 {
                    work.flush()?;
                    control.checkpoint(CompilePhase::LowerProgram, 0)?;
                } else {
                    for _ in 0..units {
                        work.step()?;
                    }
                }
                Ok(())
            };
            creation.with_request_ports(|exact, upper| {
                let mut request =
                    |layout| exact(layout).map_err(crate::FragmentCreationError::Creator);
                let mut opaque = |bound: ArrayProjectionOpaqueBound| {
                    upper(bound.bytes_upper).map_err(crate::FragmentCreationError::Creator)
                };
                observe_array_normalization_construction(
                    step,
                    &mut request,
                    &mut opaque,
                    &mut observe,
                )
            })
        })?;
    if !ty
        .typed_binding()
        .layout()
        .matches_projection_observed::<LayoutCompileError>(normalized.data_type(), |units| {
            for _ in 0..units {
                work.step()?;
            }
            Ok(())
        })?
    {
        return Err(FragmentCompileError::Invalid(
            "normalized Values constant differs from its complete output binding",
        )
        .into());
    }
    work.flush()?;
    Ok(normalized)
}
