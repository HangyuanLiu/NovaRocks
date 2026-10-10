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

//! Shared single-owner refinement handoff to dedicated lifecycle preparations.

use crate::{
    CallEffectInput, CallEffectRefinementError, FunctionBindingError, FunctionEffectOwner,
    KernelFailure, RefinedCallEffects, ScopedExpressionEffects,
};
use novarocks_type_contract::{CallEffects, CompileControlError, CompilePhase, PureCompileControl};
use std::fmt;

#[derive(Debug)]
pub enum FunctionSpecializationFailure {
    Binding(FunctionBindingError),
    Effects(novarocks_type_contract::EffectContractError),
    Control(CompileControlError),
    Kernel(KernelFailure),
    /// The selected overload has no pure implementation installed in this
    /// catalog; it names the overload so the missing owner is identifiable.
    MissingPureImplementation(crate::FunctionOverloadId),
    InvalidInput(&'static str),
}
impl fmt::Display for FunctionSpecializationFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(e) => fmt::Display::fmt(e, f),
            Self::Effects(e) => fmt::Display::fmt(e, f),
            Self::Control(e) => fmt::Display::fmt(e, f),
            Self::Kernel(e) => fmt::Display::fmt(e, f),
            Self::MissingPureImplementation(overload) => write!(
                f,
                "selected overload `{}` has no installed pure implementation",
                overload.as_str()
            ),
            Self::InvalidInput(e) => f.write_str(e),
        }
    }
}
impl std::error::Error for FunctionSpecializationFailure {}

pub(crate) fn refine_once_for_specialization<
    'a,
    O: FunctionEffectOwner<Error = FunctionBindingError> + ?Sized,
>(
    owner: &O,
    input: CallEffectInput<'a>,
    frozen: Option<&CallEffects>,
    arguments: ScopedExpressionEffects,
    control: &dyn PureCompileControl,
) -> Result<(RefinedCallEffects<'a>, ScopedExpressionEffects), FunctionSpecializationFailure> {
    control
        .checkpoint(CompilePhase::FunctionSpecialization, 0)
        .map_err(FunctionSpecializationFailure::Control)?;
    let receipt = match frozen {
        Some(frozen) => crate::validate_frozen_call_effects(owner, input, frozen, control),
        None => crate::refine_call_effects(owner, input, control),
    }
    .map_err(|error| match error {
        CallEffectRefinementError::Owner(error) => FunctionSpecializationFailure::from(error),
        CallEffectRefinementError::Control(error) => FunctionSpecializationFailure::Control(error),
        CallEffectRefinementError::Contract(error) => FunctionSpecializationFailure::Effects(error),
        CallEffectRefinementError::InvalidInput(error) => {
            FunctionSpecializationFailure::InvalidInput(error)
        }
    })?;
    let effects = receipt
        .compose_for_use(input, arguments)
        .map_err(FunctionSpecializationFailure::Effects)?;
    Ok((receipt, effects))
}

impl From<CompileControlError> for FunctionSpecializationFailure {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}

impl From<FunctionBindingError> for FunctionSpecializationFailure {
    fn from(error: FunctionBindingError) -> Self {
        match error {
            FunctionBindingError::Control(error) => Self::Control(error),
            other => Self::Binding(other),
        }
    }
}
