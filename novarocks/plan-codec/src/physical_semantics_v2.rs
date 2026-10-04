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

//! Typed projection of the v2 semantic components after resource-safe DTO
//! admission. This is neither raw-byte preflight nor a complete package codec.
//! Decoded parts must enter the same checked `FragmentPackage` constructor to
//! prove parameter closure and actual pruning consumers. Exact installed owner
//! refinement, implication and allocation admission remain separate checks.

use novarocks_physical_plan::{
    ExpressionRootError, Fragment, FragmentPackage, FrozenCallError, FrozenFragmentCalls,
    FrozenFragmentPruning, FrozenPruningError, PhysicalRootUses, PredicateSourceError,
    PruningStructureError, RootUseBindingError,
};
use novarocks_proto_models::physical_semantics_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ExpressionControlFlowError,
    PureCompileControl, SemanticParameterError, SemanticParameters,
};
use std::fmt;

use crate::physical_control_v2::ControlCodecError;

mod calls;
mod parameter_resources;
mod parameters;
mod pruning;

pub use parameter_resources::{
    ParameterProjectionFacts, ParameterProjectionLimits, PreparedSemanticParametersDecode,
    decode_semantic_parameters, prepare_semantic_parameters_decode,
};

pub(crate) use calls::{
    decode_policy as decode_decimal_policy, encode_policy as encode_decimal_policy,
};
pub(crate) use parameters::{decode_reference, encode_reference};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SemanticsCodecError {
    Control(CompileControlError),
    InvalidShape(&'static str),
    Parameter(SemanticParameterError),
    Calls(FrozenCallError),
    Pruning(FrozenPruningError),
    ControlComponent(ControlCodecError),
}
impl fmt::Display for SemanticsCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::InvalidShape(message) => f.write_str(message),
            Self::Parameter(error) => error.fmt(f),
            Self::Calls(error) => error.fmt(f),
            Self::Pruning(error) => error.fmt(f),
            Self::ControlComponent(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for SemanticsCodecError {}
impl From<CompileControlError> for SemanticsCodecError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl From<SemanticParameterError> for SemanticsCodecError {
    fn from(error: SemanticParameterError) -> Self {
        Self::Parameter(error)
    }
}
impl From<FrozenCallError> for SemanticsCodecError {
    fn from(error: FrozenCallError) -> Self {
        match error {
            FrozenCallError::Control(error)
            | FrozenCallError::Roots(RootUseBindingError::Control(error))
            | FrozenCallError::Roots(RootUseBindingError::Roots(ExpressionRootError::Control(
                error,
            ))) => Self::Control(error),
            error => Self::Calls(error),
        }
    }
}
impl From<FrozenPruningError> for SemanticsCodecError {
    fn from(error: FrozenPruningError) -> Self {
        match error {
            FrozenPruningError::Control(error)
            | FrozenPruningError::Structure(PruningStructureError::Control(error))
            | FrozenPruningError::Structure(PruningStructureError::Source(
                PredicateSourceError::Control(error),
            )) => Self::Control(error),
            error => Self::Pruning(error),
        }
    }
}
impl From<ControlCodecError> for SemanticsCodecError {
    fn from(error: ControlCodecError) -> Self {
        match error {
            ControlCodecError::Control(error)
            | ControlCodecError::Flow(ExpressionControlFlowError::Control(error))
            | ControlCodecError::Roots(RootUseBindingError::Control(error))
            | ControlCodecError::Roots(RootUseBindingError::Roots(ExpressionRootError::Control(
                error,
            ))) => Self::Control(error),
            error => Self::ControlComponent(error),
        }
    }
}

fn required_id(value: Option<u32>, message: &'static str) -> Result<u32, SemanticsCodecError> {
    value.ok_or(SemanticsCodecError::InvalidShape(message))
}

fn finish_projection<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, SemanticsCodecError>,
) -> Result<T, SemanticsCodecError> {
    if matches!(&result, Err(SemanticsCodecError::Control(_))) {
        return result;
    }
    // Ordinary failures also owe observation of their completed tail. A
    // previously captured control refusal must keep its original category.
    work.finish()?;
    result
}

/// The three exact DTO fields of a fragment package. Envelope field presence
/// and raw resource admission belong to the complete package codec.
#[derive(Clone, Debug, PartialEq)]
pub struct EncodedFragmentSemantics {
    pub parameters: wire::SemanticParameters,
    pub calls: wire::FrozenCalls,
    pub pruning: wire::FrozenPruning,
}

/// Structural component results, without a claim of package closure or owner
/// authentication. No alternate package constructor is provided here.
pub struct DecodedFragmentSemantics {
    parameters: SemanticParameters,
    calls: FrozenFragmentCalls,
    pruning: FrozenFragmentPruning,
}
impl DecodedFragmentSemantics {
    pub fn into_parts(
        self,
    ) -> (
        SemanticParameters,
        FrozenFragmentCalls,
        FrozenFragmentPruning,
    ) {
        (self.parameters, self.calls, self.pruning)
    }
}

pub fn encode_fragment_semantics(
    package: &FragmentPackage,
    control: &dyn PureCompileControl,
) -> Result<EncodedFragmentSemantics, SemanticsCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = (|| {
        let parameters = parameters::encode_parameters(package.parameters(), &mut work)?;
        let calls = calls::encode_calls(package.calls(), &mut work)?;
        let pruning = pruning::encode_pruning(package.pruning(), &mut work)?;
        Ok(EncodedFragmentSemantics {
            parameters,
            calls,
            pruning,
        })
    })();
    finish_projection(work, result)
}

pub fn decode_fragment_semantics(
    fragment: &Fragment,
    uses: &PhysicalRootUses,
    encoded: &EncodedFragmentSemantics,
    control: &dyn PureCompileControl,
) -> Result<DecodedFragmentSemantics, SemanticsCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)?;
    let result = (|| {
        let parameters = parameters::decode_parameters(&encoded.parameters, &mut work)?;
        let calls = calls::decode_calls(fragment, uses, &encoded.calls, &mut work, control)?;
        let pruning = pruning::decode_pruning(fragment.id(), &encoded.pruning, &mut work, control)?;
        Ok(DecodedFragmentSemantics {
            parameters,
            calls,
            pruning,
        })
    })();
    finish_projection(work, result)
}

#[cfg(test)]
mod tests;
