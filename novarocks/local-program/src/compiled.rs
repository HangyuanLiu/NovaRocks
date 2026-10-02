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

//! Final pure compilation product. The graph is owned exactly once by the
//! mandatory checked expression/control/type/channel/lexical chain. Provider
//! recipes and provenance are checked against that same graph before exposure.
//! Execution's construction bridge consumes LocalProgramGraph separately and
//! must be retired when the production compiler is connected.

use crate::{
    CompiledOriginsError, DiagnosticSourceNodeId, LocalOperatorProvenance, LocalProgramGraph,
    ProgramCallSite, ProgramComparisonSite, ProgramLexicalBindings, ProgramNodeId,
    ProgramPrimitiveError, ProgramProvenance, ProgramStateTemplate, ProviderLinkError,
};
use novarocks_connector_contract::ConnectorWriteRecipe;
use novarocks_type_contract::{CompileControlError, PureCompileControl};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

#[derive(Clone, Debug)]
pub struct LocalProgram {
    checked: ProgramLexicalBindings,
    provenance: ProgramProvenance,
    writes: BTreeMap<ProgramNodeId, ConnectorWriteRecipe>,
    arithmetic: BTreeMap<crate::ProgramUseRef, novarocks_functions::PreparedArithmeticRecipe>,
    comparisons: BTreeMap<ProgramComparisonSite, novarocks_functions::PreparedComparisonRecipe>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalProgramCompileError {
    Control(CompileControlError),
    MissingSink,
    Origins(CompiledOriginsError),
    Provider(ProviderLinkError),
    Primitive(ProgramPrimitiveError),
}
impl From<ProgramPrimitiveError> for LocalProgramCompileError {
    fn from(error: ProgramPrimitiveError) -> Self {
        match error {
            ProgramPrimitiveError::Control(cause) => Self::Control(cause),
            other => Self::Primitive(other),
        }
    }
}
impl From<CompiledOriginsError> for LocalProgramCompileError {
    fn from(error: CompiledOriginsError) -> Self {
        match error {
            CompiledOriginsError::Control(cause) => Self::Control(cause),
            other => Self::Origins(other),
        }
    }
}
impl From<ProviderLinkError> for LocalProgramCompileError {
    fn from(error: ProviderLinkError) -> Self {
        match error {
            ProviderLinkError::Control(cause) => Self::Control(cause),
            other => Self::Provider(other),
        }
    }
}
impl fmt::Display for LocalProgramCompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(f),
            Self::Origins(error) => error.fmt(f),
            Self::Provider(error) => error.fmt(f),
            Self::Primitive(error) => error.fmt(f),
            Self::MissingSink => f.write_str("compiled local program requires an exact sink"),
        }
    }
}
impl std::error::Error for LocalProgramCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            Self::Origins(error) => Some(error),
            Self::Provider(error) => Some(error),
            Self::Primitive(error) => Some(error),
            Self::MissingSink => None,
        }
    }
}
impl LocalProgram {
    /// Accept only the complete checked chain, not an AST and optional receipts.
    /// Provenance is freshly authored against its actual local graph. Compiler
    /// lowering remains responsible for physical-token/source correspondence,
    /// output guarantees and the existence of synthetic non-node entities.
    pub fn try_new(
        checked: ProgramLexicalBindings,
        operators: Vec<LocalOperatorProvenance>,
        allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
        writes: BTreeMap<ProgramNodeId, ConnectorWriteRecipe>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, LocalProgramCompileError> {
        let mut work = novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::LowerProgram,
        )
        .map_err(LocalProgramCompileError::Control)?;
        let graph = checked
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot()
            .program();
        let has_sink = graph.sink().is_some();
        work.step().map_err(LocalProgramCompileError::Control)?;
        work.flush().map_err(LocalProgramCompileError::Control)?;
        if !has_sink {
            return Err(LocalProgramCompileError::MissingSink);
        }
        let provenance =
            crate::compiled_origins::compile_origins(graph, operators, allowed_sources, control)?;
        crate::provider_links::validate_provider_links(&checked, &writes, control)?;
        let arithmetic = crate::primitives::compile_arithmetic(&checked, control)?;
        let comparisons = crate::primitives::compile_comparisons(&checked, control)?;
        // Each delegated author finishes its completed work and propagates a
        // first control refusal directly; no control object enters the product.
        Ok(Self {
            checked,
            provenance,
            writes,
            comparisons,
            arithmetic,
        })
    }
    pub const fn checked(&self) -> &ProgramLexicalBindings {
        &self.checked
    }
    pub fn graph(&self) -> &LocalProgramGraph {
        self.checked
            .channels()
            .expressions()
            .resolved_calls()
            .snapshot()
            .program()
    }
    pub const fn provenance(&self) -> &ProgramProvenance {
        &self.provenance
    }
    pub fn write_recipes(&self) -> &BTreeMap<ProgramNodeId, ConnectorWriteRecipe> {
        &self.writes
    }
    pub fn comparison_recipe(
        &self,
        site: ProgramComparisonSite,
    ) -> Option<&novarocks_functions::PreparedComparisonRecipe> {
        self.comparisons.get(&site)
    }
    pub fn arithmetic_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedArithmeticRecipe> {
        self.arithmetic.get(&site)
    }
    /// Borrow the exact checked implementation's lifecycle; never rebuild a
    /// second state declaration from a name or legacy expression tag.
    pub fn state_template(&self, site: ProgramCallSite) -> Option<ProgramStateTemplate<'_>> {
        self.checked
            .channels()
            .expressions()
            .resolved_calls()
            .calls()
            .get(&site)
            .map(|call| call.state_template())
    }
}

#[cfg(test)]
mod kernel_tests;
#[cfg(test)]
mod read_tests;
#[cfg(test)]
mod tests;
