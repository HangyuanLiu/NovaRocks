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

//! Signature-only projection from original checked binding sources and the
//! same emitted type table. This does not authorize occurrence effects,
//! installed owners, a decoder, or a formal MEM allocation grant.

use crate::physical_type_v2::{EncodedTypeTable, TypeCodecError};
use novarocks_physical_plan::{BoundFunction, BoundTableFunction};
use novarocks_proto_models::physical_package_v2 as wire;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::fmt;

mod encode;
mod materialize;
pub(crate) use materialize::{
    Model as MaterializationModel, add, boxed, cap, completed, finish, mul, reserve,
};
mod read;
mod signature_copy;
pub use materialize::{
    MaterializedFunctionBinding, MaterializedFunctionBindings,
    PreparedFunctionBindingsMaterialization, materialize_function_bindings,
    prepare_function_bindings_materialization,
};
pub use read::{PreparedFunctionBindingHeaders, prepare_function_binding_headers};
pub(crate) use signature_copy::{
    copy_scalar_signature_observed, copy_table_signature_observed, preflight_scalar_signature_copy,
    preflight_table_signature_copy, preflight_table_signature_copy_counts,
    preflight_table_signature_copy_types,
};

#[derive(Debug)]
pub enum BindingCodecError {
    Control(CompileControlError),
    Type(TypeCodecError),
    InvalidShape(&'static str),
}
impl From<CompileControlError> for BindingCodecError {
    fn from(cause: CompileControlError) -> Self {
        Self::Control(cause)
    }
}
impl From<TypeCodecError> for BindingCodecError {
    fn from(error: TypeCodecError) -> Self {
        match error {
            TypeCodecError::Control(cause) => Self::Control(cause),
            other => Self::Type(other),
        }
    }
}
impl fmt::Display for BindingCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::Type(error) => error.fmt(formatter),
            Self::InvalidShape(message) => formatter.write_str(message),
        }
    }
}
impl std::error::Error for BindingCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Type(error) => Some(error),
            Self::InvalidShape(_) => None,
        }
    }
}
pub(crate) struct VerifiedSignature {
    matches: bool,
    work: usize,
}
impl VerifiedSignature {
    pub(crate) fn matches(&self) -> bool {
        self.matches
    }
    pub(crate) fn work_upper_bound(&self) -> usize {
        self.work
    }
}
pub(crate) use encode::verify_scalar_signature;

#[derive(Clone, Copy)]
pub enum BindingSource<'a> {
    Scalar(&'a BoundFunction),
    Table(&'a BoundTableFunction),
}
#[derive(Clone, Copy)]
pub enum ArgumentTypeIds<'a> {
    Value(u32),
    Lambda { parameters: &'a [u32], result: u32 },
}
#[derive(Clone, Copy)]
pub enum ResultTypeIds<'a> {
    Scalar(u32),
    Relation(&'a [u32]),
}
#[derive(Clone, Copy)]
pub struct FunctionBindingInput<'a> {
    pub id: u32,
    pub source: BindingSource<'a>,
    pub arguments: &'a [ArgumentTypeIds<'a>],
    pub result: ResultTypeIds<'a>,
}
/// Every envelope is supplied by the caller. Input IDs must be strictly
/// ascending; this encoder contract does not constrain a future decoder.
#[derive(Clone, Copy, Debug)]
pub struct BindingProjectionLimits {
    pub max_definitions: usize,
    pub max_type_references: usize,
    pub max_request_bytes: usize,
    pub max_allocation_requests: usize,
    pub max_coexisting_source_and_request_bytes: usize,
    pub max_work: usize,
}
#[derive(Clone, Copy, Debug)]
pub struct BindingProjectionFacts {
    pub definition_count: usize,
    pub type_reference_count: usize,
    pub allocation_requests_upper_bound: usize,
    pub request_bytes_upper_bound: usize,
    pub coexisting_source_and_request_bytes_upper_bound: usize,
    pub cumulative_work_upper_bound: usize,
}
pub struct EncodedFunctionBindings<'loan, 'source> {
    definitions: Vec<wire::FunctionBindingDefinition>,
    inputs: &'loan [FunctionBindingInput<'source>],
    types: &'loan EncodedTypeTable<'source>,
    facts: BindingProjectionFacts,
}
impl<'loan, 'source> EncodedFunctionBindings<'loan, 'source> {
    pub fn as_wire(&self) -> &[wire::FunctionBindingDefinition] {
        &self.definitions
    }
    pub fn into_wire(self) -> Vec<wire::FunctionBindingDefinition> {
        self.definitions
    }
    pub fn facts(&self) -> &BindingProjectionFacts {
        &self.facts
    }
    pub(crate) fn type_sources(&self) -> &'loan EncodedTypeTable<'source> {
        self.types
    }
    pub(crate) fn source_counts(&self) -> usize {
        self.inputs.len()
    }
    pub(crate) fn scalar_binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source BoundFunction>, BindingCodecError> {
        for input in self.inputs {
            let matches = input.id == id;
            work.step()?;
            if matches {
                return Ok(match input.source {
                    BindingSource::Scalar(source) => Some(source),
                    BindingSource::Table(_) => None,
                });
            }
        }
        Ok(None)
    }
    /// Resolve only an actual borrowed source in this emitted namespace.
    /// If the source is intentionally aliased by multiple definitions, the
    /// first original ascending ID is sufficient for the same exact signature.
    /// The caller admits count-sized work and owns this scope's entry/tail.
    pub(crate) fn table_source_id_observed(
        &self,
        source: &BoundTableFunction,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<u32, BindingCodecError> {
        for input in self.inputs {
            let same = matches!(input.source, BindingSource::Table(actual) if std::ptr::eq(actual, source));
            work.step()?;
            if same {
                return Ok(input.id);
            }
        }
        Err(BindingCodecError::InvalidShape(
            "table signature is not an original emitted source",
        ))
    }
    /// Borrow an original relational signature. The caller admits the linear
    /// work from facts().definition_count and owns this meter's entry/tail.
    pub fn table_binding_observed(
        &self,
        id: u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<&'source BoundTableFunction>, BindingCodecError> {
        for input in self.inputs {
            let matches = input.id == id;
            work.step()?;
            if matches {
                return Ok(match input.source {
                    BindingSource::Table(source) => Some(source),
                    BindingSource::Scalar(_) => None,
                });
            }
        }
        Ok(None)
    }
}
pub fn encode_function_bindings<'loan, 'source>(
    types: &'loan EncodedTypeTable<'source>,
    inputs: &'loan [FunctionBindingInput<'source>],
    source_retained_bytes: usize,
    limits: BindingProjectionLimits,
    control: &dyn PureCompileControl,
) -> Result<EncodedFunctionBindings<'loan, 'source>, BindingCodecError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
    let result = encode::encode(types, inputs, source_retained_bytes, limits, &mut work);
    if let Err(BindingCodecError::Control(cause)) = &result {
        return Err(BindingCodecError::Control(*cause));
    }
    work.finish()?;
    let (definitions, facts) = result?;
    Ok(EncodedFunctionBindings {
        definitions,
        inputs,
        types,
        facts,
    })
}

#[cfg(test)]
mod tests;
