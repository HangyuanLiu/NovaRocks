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
    BindingRequirement, CompiledOriginsError, DiagnosticSourceNodeId, LocalOperatorProvenance,
    LocalProgramGraph, ProgramCallSite, ProgramComparisonSite, ProgramLexicalBindings,
    ProgramNodeId, ProgramNodeKind, ProgramPrimitiveError, ProgramProvenance, ProgramStateTemplate,
    ProviderLinkError,
};
use novarocks_connector_contract::ConnectorWriteRecipe;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

/// Compiled-only receiver address of one actual `ExchangeSource` node. A
/// compiled node has no legacy native identity, so the program carries the
/// physical routing facts explicitly instead of deriving them from a node ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompiledExchangeInput {
    /// Physical NodeId of the receiving ExchangeSource; senders address it.
    pub receiver_node: u32,
    /// Physical EdgeId of the inbound stream edge.
    pub edge: u32,
    /// Physical FragmentId of the sending fragment.
    pub source_fragment: u32,
}

/// Compiled-only physical address of one actual provider `Scan` node. A
/// compiled node has no legacy native identity, so the program names the
/// physical node that keys the task's runtime split queue, range scope and
/// scan assignment instead of deriving it from a node ID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompiledScanInput {
    /// Physical NodeId of the scan; runtime splits are addressed to it.
    pub scan_node: u32,
}

#[derive(Clone, Debug)]
pub struct LocalProgram {
    checked: ProgramLexicalBindings,
    provenance: ProgramProvenance,
    writes: BTreeMap<ProgramNodeId, ConnectorWriteRecipe>,
    exchange_inputs: BTreeMap<ProgramNodeId, CompiledExchangeInput>,
    scan_inputs: BTreeMap<ProgramNodeId, CompiledScanInput>,
    arithmetic: BTreeMap<crate::ProgramUseRef, novarocks_functions::PreparedArithmeticRecipe>,
    casts: BTreeMap<crate::ProgramUseRef, novarocks_functions::PreparedCastRecipe>,
    comparisons: BTreeMap<ProgramComparisonSite, novarocks_functions::PreparedComparisonRecipe>,
    null_safe_comparisons:
        BTreeMap<crate::ProgramUseRef, novarocks_functions::PreparedNullSafeComparisonRecipe>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LocalProgramCompileError {
    Control(CompileControlError),
    MissingSink,
    /// The compiled exchange addresses, the actual `ExchangeSource` nodes and
    /// their `ExchangeInput` requirements are not the same node set.
    ExchangeInputMismatch(ProgramNodeId),
    /// Two compiled exchange inputs name the same physical receiver.
    DuplicateExchangeReceiver(u32),
    /// The compiled scan addresses, the actual `Scan` nodes and their `Scan`
    /// requirements are not the same node set.
    ScanInputMismatch(ProgramNodeId),
    /// Two compiled scan inputs name the same physical scan node.
    DuplicateScanNode(u32),
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
            Self::ExchangeInputMismatch(node) => write!(
                f,
                "compiled exchange input addresses differ from the exchange sources at local node {}",
                node.index()
            ),
            Self::DuplicateExchangeReceiver(receiver) => write!(
                f,
                "compiled exchange inputs share physical receiver node {receiver}"
            ),
            Self::ScanInputMismatch(node) => write!(
                f,
                "compiled scan input addresses differ from the scan nodes at local node {}",
                node.index()
            ),
            Self::DuplicateScanNode(scan) => {
                write!(f, "compiled scan inputs share physical scan node {scan}")
            }
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
            Self::MissingSink
            | Self::ExchangeInputMismatch(_)
            | Self::DuplicateExchangeReceiver(_)
            | Self::ScanInputMismatch(_)
            | Self::DuplicateScanNode(_) => None,
        }
    }
}
impl LocalProgram {
    /// Accept only the complete checked chain, not an AST and optional receipts.
    /// Provenance is freshly authored against its actual local graph. Compiler
    /// lowering remains responsible for physical-token/source correspondence,
    /// output guarantees and the existence of synthetic non-node entities.
    /// `exchange_inputs` addresses exactly the graph's `ExchangeSource` nodes,
    /// which are exactly its `ExchangeInput` requirements; receivers are unique.
    /// `scan_inputs` likewise addresses exactly its `Scan` nodes, which are
    /// exactly its `Scan` requirements; physical scan nodes are unique.
    pub fn try_new(
        checked: ProgramLexicalBindings,
        operators: Vec<LocalOperatorProvenance>,
        allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
        writes: BTreeMap<ProgramNodeId, ConnectorWriteRecipe>,
        exchange_inputs: BTreeMap<ProgramNodeId, CompiledExchangeInput>,
        scan_inputs: BTreeMap<ProgramNodeId, CompiledScanInput>,
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
        validate_exchange_inputs(graph, &exchange_inputs, control)?;
        validate_scan_inputs(graph, &scan_inputs, control)?;
        let arithmetic = crate::primitives::compile_arithmetic(&checked, control)?;
        let casts = crate::primitives::compile_casts(&checked, control)?;
        let comparisons = crate::primitives::compile_comparisons(&checked, control)?;
        let null_safe_comparisons =
            crate::primitives::compile_null_safe_comparisons(&checked, control)?;
        // Each delegated author finishes its completed work and propagates a
        // first control refusal directly; no control object enters the product.
        Ok(Self {
            checked,
            provenance,
            writes,
            exchange_inputs,
            scan_inputs,
            comparisons,
            null_safe_comparisons,
            arithmetic,
            casts,
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
    /// Exact receiver address of every `ExchangeSource` node in this program.
    pub fn exchange_inputs(&self) -> &BTreeMap<ProgramNodeId, CompiledExchangeInput> {
        &self.exchange_inputs
    }
    /// Exact physical scan address of every `Scan` node in this program.
    pub fn scan_inputs(&self) -> &BTreeMap<ProgramNodeId, CompiledScanInput> {
        &self.scan_inputs
    }
    pub fn comparison_recipe(
        &self,
        site: ProgramComparisonSite,
    ) -> Option<&novarocks_functions::PreparedComparisonRecipe> {
        self.comparisons.get(&site)
    }
    pub fn null_safe_comparison_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedNullSafeComparisonRecipe> {
        self.null_safe_comparisons.get(&site)
    }
    pub fn arithmetic_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedArithmeticRecipe> {
        self.arithmetic.get(&site)
    }
    pub fn cast_recipe(
        &self,
        site: crate::ProgramUseRef,
    ) -> Option<&novarocks_functions::PreparedCastRecipe> {
        self.casts.get(&site)
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

/// One compiled-only source address family: which nodes it addresses, which
/// requirement declares such a node, the physical identity that must be unique
/// and the errors naming a coverage gap or a shared physical identity.
struct AddressFamily<A> {
    addressed: fn(&ProgramNodeKind) -> bool,
    declared: fn(&BindingRequirement) -> Option<ProgramNodeId>,
    physical: fn(&A) -> u32,
    mismatch: fn(ProgramNodeId) -> LocalProgramCompileError,
    duplicate: fn(u32) -> LocalProgramCompileError,
}

/// Require one address per actual exchange receiver and per declared exchange
/// input, and no address elsewhere. A first control refusal stays primary.
fn validate_exchange_inputs(
    graph: &LocalProgramGraph,
    exchange_inputs: &BTreeMap<ProgramNodeId, CompiledExchangeInput>,
    control: &dyn PureCompileControl,
) -> Result<(), LocalProgramCompileError> {
    validate_addresses(
        graph,
        exchange_inputs,
        AddressFamily {
            addressed: |kind| matches!(kind, ProgramNodeKind::ExchangeSource { .. }),
            declared: |requirement| match requirement {
                BindingRequirement::ExchangeInput { node, .. } => Some(*node),
                _ => None,
            },
            physical: |input| input.receiver_node,
            mismatch: LocalProgramCompileError::ExchangeInputMismatch,
            duplicate: LocalProgramCompileError::DuplicateExchangeReceiver,
        },
        control,
    )
}

/// Require one address per actual scan and per declared scan requirement, and
/// no address elsewhere. A first control refusal stays primary.
fn validate_scan_inputs(
    graph: &LocalProgramGraph,
    scan_inputs: &BTreeMap<ProgramNodeId, CompiledScanInput>,
    control: &dyn PureCompileControl,
) -> Result<(), LocalProgramCompileError> {
    validate_addresses(
        graph,
        scan_inputs,
        AddressFamily {
            addressed: |kind| matches!(kind, ProgramNodeKind::Scan { .. }),
            declared: |requirement| match requirement {
                BindingRequirement::Scan { node, .. } => Some(*node),
                _ => None,
            },
            physical: |input| input.scan_node,
            mismatch: LocalProgramCompileError::ScanInputMismatch,
            duplicate: LocalProgramCompileError::DuplicateScanNode,
        },
        control,
    )
}

fn validate_addresses<A>(
    graph: &LocalProgramGraph,
    addresses: &BTreeMap<ProgramNodeId, A>,
    family: AddressFamily<A>,
    control: &dyn PureCompileControl,
) -> Result<(), LocalProgramCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)
        .map_err(LocalProgramCompileError::Control)?;
    let result = validate_addresses_core(graph, addresses, &family, &mut work);
    if matches!(result, Err(LocalProgramCompileError::Control(_))) {
        return result;
    }
    work.finish().map_err(LocalProgramCompileError::Control)?;
    result
}

fn validate_addresses_core<A>(
    graph: &LocalProgramGraph,
    addresses: &BTreeMap<ProgramNodeId, A>,
    family: &AddressFamily<A>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), LocalProgramCompileError> {
    let mut required = BTreeSet::new();
    for requirement in graph.requirements().entries() {
        if let Some(node) = (family.declared)(requirement) {
            required.insert(node);
        }
        work.step().map_err(LocalProgramCompileError::Control)?;
    }
    // A requirement naming no actual node cannot be hidden by the node walk.
    for node in &required {
        let present = node.index() < graph.nodes().len();
        work.step().map_err(LocalProgramCompileError::Control)?;
        if !present {
            return Err((family.mismatch)(*node));
        }
    }
    let mut physical = BTreeSet::new();
    for (index, node) in graph.nodes().iter().enumerate() {
        let id = ProgramNodeId::new(index);
        let addressed = (family.addressed)(node.kind());
        let declared = required.contains(&id);
        let address = addresses.get(&id);
        work.step().map_err(LocalProgramCompileError::Control)?;
        match (addressed, declared, address) {
            (true, true, Some(input)) => {
                let identity = (family.physical)(input);
                let unique = physical.insert(identity);
                work.step().map_err(LocalProgramCompileError::Control)?;
                if !unique {
                    return Err((family.duplicate)(identity));
                }
            }
            (false, false, None) => {}
            _ => return Err((family.mismatch)(id)),
        }
    }
    // An address for a node outside the graph cannot be hidden either.
    for id in addresses.keys() {
        let present = id.index() < graph.nodes().len();
        work.step().map_err(LocalProgramCompileError::Control)?;
        if !present {
            return Err((family.mismatch)(*id));
        }
    }
    Ok(())
}

#[cfg(test)]
mod kernel_tests;
#[cfg(test)]
mod read_tests;
#[cfg(test)]
mod tests;
