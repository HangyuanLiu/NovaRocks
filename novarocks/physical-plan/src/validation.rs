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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use novarocks_connector_contract::{
    ConnectorCodecCategory, ConnectorEncodedPayload, ConnectorReadRelationKind,
    ConnectorReadWorkSource, MAX_CONNECTOR_WRITE_TARGETS,
};

use crate::resource::{
    CutResourcePreflight, CutResourceUsage, MAX_ANNOTATION_BYTES, MAX_ANNOTATION_KEY_BYTES,
    MAX_ANNOTATION_VALUE_BYTES, MAX_ANNOTATIONS, MAX_PLAN_DERIVED_CUT_BYTES,
    MAX_PLAN_DERIVED_CUT_ITEMS, validate_fragment_cut_resources, validate_fragment_resources,
    validate_plan_resources,
};
use crate::{
    AggregatePhase, AnnotationSubject, ArtifactInputField, ArtifactSourceBinding, CoverageSet,
    CutImport, CutValue, Distribution, Edge, EdgeId, ExprId, ExprKind, Fragment, FragmentCuts,
    FragmentId, FragmentSink, FunctionKind, InboundFragmentCut, NodeId, NodeKind,
    OutboundFragmentCut, PLAN_CONTRACT_REVISION, PhysicalNode, PhysicalPlan,
    ProviderColumnReference, ProviderReadReference, Relation, RequiredContracts, RowMultiplicity,
    RuntimeFilterEndpoint, SealedArtifactRef, SealedArtifactSinkSpec, ValueDef, ValueId,
    ValueOrigin, ValueType,
};

pub const MAX_PLAN_FRAGMENTS: usize = 16_384;
pub const MAX_FRAGMENT_NODES: usize = 4_096;
pub const MAX_FRAGMENT_VALUES: usize = 65_536;
pub const MAX_FRAGMENT_EXPRESSIONS: usize = 262_144;
pub const MAX_EXPRESSION_SEMANTIC_DEPTH: usize = 256;
pub const MAX_PLAN_EDGES: usize = 65_536;
pub const MAX_PLAN_RUNTIME_FILTERS: usize = 65_536;
pub const MAX_RUNTIME_FILTER_COVERAGE_DEPTH: usize = 256;
pub const MAX_RUNTIME_FILTER_ARTIFACT_BYTES: u64 = 1 << 30;
pub const MAX_RUNTIME_FILTER_DEADLINE_MS: u64 = 86_400_000;
pub const MAX_RUNTIME_FILTER_RETRIES: u32 = 100;
pub const MAX_RUNTIME_FILTER_ENDPOINTS: usize = 4_096;
pub const MAX_RUNTIME_FILTER_COVERAGE_NODES: usize = 16_384;
pub const MAX_RUNTIME_FILTER_LINEAGE_STEPS: usize = 4_096;
pub const MAX_PLAN_ARTIFACT_REFS: usize = 65_536;
pub const MAX_PROVIDER_PRIVATE_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_METADATA_COVERAGE_EVIDENCE_BYTES: usize = 1024 * 1024;
pub const MAX_ARTIFACT_REFERENCE_BYTES: u32 = 16 * 1024 * 1024;
pub const MAX_UNPIVOT_MAPPINGS: usize = 4_096;
pub const MAX_UNPIVOT_CONSTANTS: usize = 16_384;
pub const MAX_UNPIVOT_COLLECTION_ITEMS: usize = 4_096;
pub const MAX_UNPIVOT_LITERAL_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_UNPIVOT_OUTPUT_ROWS: u64 = 1 << 30;
pub const MAX_UNPIVOT_OUTPUT_BYTES: u64 = 1 << 30;
pub const MAX_PARTITION_COUNT: u32 = 1 << 20;
pub const MAX_SCAN_BATCH_ROWS: u64 = 1 << 30;
pub const MAX_SCAN_BATCH_BYTES: u64 = 1 << 30;
pub const MAX_PLAN_SEMANTIC_TRACE_WORK: usize = 1 << 20;
pub const MAX_PIPELINE_DOP: u32 = 1 << 20;

#[derive(Clone, Debug, Default)]
struct ValuePortIndex {
    occurrences: BTreeMap<ValueId, usize>,
}

impl ValuePortIndex {
    fn new(values: &[ValueId]) -> Self {
        let mut occurrences = BTreeMap::new();
        for value in values {
            *occurrences.entry(*value).or_default() += 1;
        }
        Self { occurrences }
    }

    fn contains(&self, value: &ValueId) -> bool {
        self.occurrences.contains_key(value)
    }
}

struct FragmentValidationIndexes {
    output_ports: BTreeMap<NodeId, Arc<ValuePortIndex>>,
    visible_inputs: BTreeMap<NodeId, VisibleInputIndex>,
}

enum VisibleInputIndex {
    Empty,
    One(Arc<ValuePortIndex>),
    Many(Box<[Arc<ValuePortIndex>]>),
}

impl VisibleInputIndex {
    fn contains(&self, value: &ValueId) -> bool {
        match self {
            Self::Empty => false,
            Self::One(port) => port.contains(value),
            Self::Many(ports) => ports.iter().any(|port| port.contains(value)),
        }
    }
}

impl FragmentValidationIndexes {
    fn new(fragment: &Fragment) -> Self {
        let output_ports = fragment
            .nodes()
            .values()
            .map(|node| (node.id, Arc::new(ValuePortIndex::new(&node.output.columns))))
            .collect::<BTreeMap<_, _>>();
        let mut visible_inputs = BTreeMap::new();
        for node in fragment.nodes().values() {
            let visible = if let NodeKind::Scan {
                provider_outputs, ..
            } = &node.kind
            {
                VisibleInputIndex::One(Arc::new(ValuePortIndex::new(
                    &provider_outputs
                        .iter()
                        .map(|(_, value)| *value)
                        .collect::<Vec<_>>(),
                )))
            } else {
                let ports = node
                    .inputs
                    .iter()
                    .filter_map(|input| output_ports.get(input).cloned())
                    .collect::<Vec<_>>();
                match ports.as_slice() {
                    [] => VisibleInputIndex::Empty,
                    [port] => VisibleInputIndex::One(port.clone()),
                    _ => VisibleInputIndex::Many(ports.into_boxed_slice()),
                }
            };
            visible_inputs.insert(node.id, visible);
        }
        Self {
            output_ports,
            visible_inputs,
        }
    }

    fn output(&self, node: NodeId) -> Option<&ValuePortIndex> {
        self.output_ports.get(&node).map(Arc::as_ref)
    }

    fn visible_input(&self, node: NodeId) -> Option<&VisibleInputIndex> {
        self.visible_inputs.get(&node)
    }
}

#[derive(Clone, Debug, Default)]
struct ValueMappingIndex {
    by_destination: BTreeMap<ValueId, BTreeMap<Option<ValueId>, usize>>,
}

impl ValueMappingIndex {
    fn from_pairs(mapping: &[(ValueId, ValueId)]) -> Self {
        Self::from_pairs_iter(mapping.iter().copied())
    }

    fn from_pairs_iter(mapping: impl IntoIterator<Item = (ValueId, ValueId)>) -> Self {
        let mut index = Self::default();
        for (source, destination) in mapping {
            index.insert(Some(source), destination);
        }
        index
    }

    fn insert(&mut self, source: Option<ValueId>, destination: ValueId) {
        *self
            .by_destination
            .entry(destination)
            .or_default()
            .entry(source)
            .or_default() += 1;
    }

    fn contains(&self, source: ValueId, destination: ValueId) -> bool {
        self.by_destination
            .get(&destination)
            .is_some_and(|sources| sources.contains_key(&Some(source)))
    }

    fn has_destination(&self, destination: ValueId) -> bool {
        self.by_destination.contains_key(&destination)
    }

    fn resolve(&self, destination: ValueId, allow_identical_duplicates: bool) -> Option<ValueId> {
        let sources = self.by_destination.get(&destination)?;
        if sources.len() != 1 {
            return None;
        }
        let (source, occurrences) = sources.first_key_value()?;
        if !allow_identical_duplicates && *occurrences != 1 {
            return None;
        }
        *source
    }
}

struct SemanticTraceWorkBudget {
    remaining: usize,
}

impl SemanticTraceWorkBudget {
    const fn new() -> Self {
        Self {
            remaining: MAX_PLAN_SEMANTIC_TRACE_WORK,
        }
    }

    fn charge(&mut self, work: usize) -> bool {
        let Some(remaining) = self.remaining.checked_sub(work) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

#[derive(Default)]
struct SemanticTraceIndexes {
    ports: BTreeMap<(FragmentId, NodeId), ValuePortIndex>,
    edges: BTreeMap<EdgeId, ValueMappingIndex>,
    projects: BTreeMap<(FragmentId, NodeId), ValueMappingIndex>,
    unions: BTreeMap<(FragmentId, NodeId, usize), ValueMappingIndex>,
    aggregate_sequences:
        BTreeMap<(FragmentId, NodeId), BTreeMap<crate::AggregateSequenceId, Option<usize>>>,
}

#[derive(Default)]
struct RuntimeFilterLineageIndexes {
    parents: BTreeMap<FragmentId, BTreeMap<NodeId, Option<NodeId>>>,
    ports: BTreeMap<(FragmentId, NodeId), ValuePortIndex>,
    projects: BTreeMap<(FragmentId, NodeId), ValueMappingIndex>,
    apply_ports: BTreeMap<(FragmentId, NodeId, RuntimeFilterApplyPortKey), Option<ValuePortIndex>>,
    scan_provider_ports: BTreeMap<(FragmentId, NodeId), ValuePortIndex>,
    build_frontiers: BTreeMap<(FragmentId, NodeId), RuntimeFilterFrontierIndex>,
}

struct RuntimeFilterFrontierIndex {
    build: BTreeSet<EdgeId>,
    non_build: BTreeSet<EdgeId>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RuntimeFilterApplyPortKey {
    NodeInput(u32),
    NodeOutput,
    ScanSource,
}

impl From<crate::RuntimeFilterApplyPoint> for RuntimeFilterApplyPortKey {
    fn from(value: crate::RuntimeFilterApplyPoint) -> Self {
        match value {
            crate::RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
                Self::NodeInput(input_ordinal)
            }
            crate::RuntimeFilterApplyPoint::NodeOutput => Self::NodeOutput,
            crate::RuntimeFilterApplyPoint::ScanSource => Self::ScanSource,
        }
    }
}

impl RuntimeFilterLineageIndexes {
    fn apply_port_contains_all(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        apply_point: crate::RuntimeFilterApplyPoint,
        values: &[ValueId],
    ) -> Option<bool> {
        if apply_point == crate::RuntimeFilterApplyPoint::ScanSource {
            return matches!(node.kind, NodeKind::Scan { .. }).then(|| {
                values
                    .iter()
                    .all(|value| self.scan_provider_contains(fragment, node, *value))
            });
        }
        let port = self
            .apply_ports
            .entry((fragment.id(), node.id, apply_point.into()))
            .or_insert_with(|| {
                let available = match apply_point {
                    crate::RuntimeFilterApplyPoint::NodeInput { input_ordinal } => {
                        usize::try_from(input_ordinal)
                            .ok()
                            .and_then(|ordinal| node.inputs.get(ordinal))
                            .and_then(|input| fragment.nodes().get(input))
                            .map(|input| input.output.columns.as_ref())
                    }
                    crate::RuntimeFilterApplyPoint::NodeOutput => {
                        Some(node.output.columns.as_ref())
                    }
                    crate::RuntimeFilterApplyPoint::ScanSource => unreachable!(),
                };
                available.map(ValuePortIndex::new)
            })
            .as_ref()?;
        Some(values.iter().all(|value| port.contains(value)))
    }

    fn scan_provider_contains(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        value: ValueId,
    ) -> bool {
        let NodeKind::Scan {
            provider_outputs, ..
        } = &node.kind
        else {
            return false;
        };
        self.scan_provider_ports
            .entry((fragment.id(), node.id))
            .or_insert_with(|| {
                ValuePortIndex::new(
                    &provider_outputs
                        .iter()
                        .map(|(_, value)| *value)
                        .collect::<Vec<_>>(),
                )
            })
            .contains(&value)
    }

    fn build_frontier(
        &mut self,
        fragment: &Fragment,
        root: NodeId,
        inbound_edges: &BTreeSet<EdgeId>,
    ) -> &RuntimeFilterFrontierIndex {
        self.build_frontiers
            .entry((fragment.id(), root))
            .or_insert_with(|| {
                let build = collect_subtree_exchange_edges(fragment, root);
                let non_build = inbound_edges.difference(&build).copied().collect();
                RuntimeFilterFrontierIndex { build, non_build }
            })
    }

    fn port_contains(&mut self, fragment: &Fragment, node: &PhysicalNode, value: ValueId) -> bool {
        self.ports
            .entry((fragment.id(), node.id))
            .or_insert_with(|| ValuePortIndex::new(&node.output.columns))
            .contains(&value)
    }

    fn project_source(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        expressions: &[(ExprId, ValueId)],
        output: ValueId,
    ) -> Option<Option<ValueId>> {
        let index = self
            .projects
            .entry((fragment.id(), node.id))
            .or_insert_with(|| {
                let mut index = ValueMappingIndex::default();
                for (expression, output) in expressions {
                    index.insert(expression_value(fragment, *expression), *output);
                }
                index
            });
        if index.has_destination(output) {
            Some(index.resolve(output, false))
        } else {
            None
        }
    }
}

impl SemanticTraceIndexes {
    fn aggregate_sequence_call<'a>(
        &mut self,
        fragment: FragmentId,
        node: NodeId,
        calls: &'a [crate::AggregateCall],
        sequence: crate::AggregateSequenceId,
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<&'a crate::AggregateCall> {
        let key = (fragment, node);
        if let Entry::Vacant(entry) = self.aggregate_sequences.entry(key) {
            if !budget.charge(calls.len()) {
                return None;
            }
            let mut by_sequence = BTreeMap::new();
            for (ordinal, call) in calls.iter().enumerate() {
                let Some(call_sequence) = call.binding.phase.sequence() else {
                    continue;
                };
                if matches!(call.binding.phase, AggregatePhase::Final { .. }) {
                    continue;
                }
                match by_sequence.entry(call_sequence) {
                    Entry::Vacant(entry) => {
                        entry.insert(Some(ordinal));
                    }
                    Entry::Occupied(mut entry) => {
                        entry.insert(None);
                    }
                }
            }
            entry.insert(by_sequence);
        }
        let ordinal = self
            .aggregate_sequences
            .get(&key)?
            .get(&sequence)?
            .as_ref()?;
        calls.get(*ordinal)
    }

    fn ensure_port(
        &mut self,
        key: (FragmentId, NodeId),
        values: &[ValueId],
        budget: &mut SemanticTraceWorkBudget,
    ) -> bool {
        if let Entry::Vacant(entry) = self.ports.entry(key) {
            if !budget.charge(values.len()) {
                return false;
            }
            entry.insert(ValuePortIndex::new(values));
        }
        true
    }

    fn map_edge_values(
        &mut self,
        edge: EdgeId,
        mapping: &[(ValueId, ValueId)],
        values: &[ValueId],
        allow_identical_duplicates: bool,
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<Vec<ValueId>> {
        if let Entry::Vacant(entry) = self.edges.entry(edge) {
            if !budget.charge(mapping.len()) {
                return None;
            }
            entry.insert(ValueMappingIndex::from_pairs(mapping));
        }
        if !budget.charge(values.len()) {
            return None;
        }
        let index = self.edges.get(&edge)?;
        values
            .iter()
            .map(|value| index.resolve(*value, allow_identical_duplicates))
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn map_union_values(
        &mut self,
        fragment: FragmentId,
        node: NodeId,
        input_ordinal: usize,
        outputs: &[ValueId],
        input: &[ValueId],
        values: &[ValueId],
        allow_identical_duplicates: bool,
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<Vec<ValueId>> {
        if outputs.len() != input.len() {
            return None;
        }
        let key = (fragment, node, input_ordinal);
        if let Entry::Vacant(entry) = self.unions.entry(key) {
            if !budget.charge(outputs.len()) {
                return None;
            }
            let mut index = ValueMappingIndex::default();
            for (destination, source) in outputs.iter().copied().zip(input.iter().copied()) {
                index.insert(Some(source), destination);
            }
            entry.insert(index);
        }
        if !budget.charge(values.len()) {
            return None;
        }
        let index = self.unions.get(&key)?;
        values
            .iter()
            .map(|value| index.resolve(*value, allow_identical_duplicates))
            .collect()
    }

    fn map_project_values(
        &mut self,
        fragment: &Fragment,
        node: &PhysicalNode,
        child: &PhysicalNode,
        expressions: &[(ExprId, ValueId)],
        values: &[ValueId],
        budget: &mut SemanticTraceWorkBudget,
    ) -> Option<Vec<ValueId>> {
        let child_key = (fragment.id(), child.id);
        if !self.ensure_port(child_key, &child.output.columns, budget) {
            return None;
        }
        let project_key = (fragment.id(), node.id);
        if let Entry::Vacant(entry) = self.projects.entry(project_key) {
            if !budget.charge(expressions.len()) {
                return None;
            }
            let mut index = ValueMappingIndex::default();
            for (expression, output) in expressions {
                index.insert(expression_value(fragment, *expression), *output);
            }
            entry.insert(index);
        }
        if !budget.charge(values.len()) {
            return None;
        }
        let child_values = self.ports.get(&child_key)?;
        let expression_sources = self.projects.get(&project_key)?;
        values
            .iter()
            .map(|expected| {
                if child_values.contains(expected) {
                    Some(*expected)
                } else {
                    expression_sources.resolve(*expected, false)
                }
            })
            .collect()
    }

    fn port_contains_all(
        &mut self,
        fragment: FragmentId,
        node: &PhysicalNode,
        values: impl IntoIterator<Item = ValueId>,
        value_count: usize,
        budget: &mut SemanticTraceWorkBudget,
    ) -> bool {
        let key = (fragment, node.id);
        if !self.ensure_port(key, &node.output.columns, budget) || !budget.charge(value_count) {
            return false;
        }
        let Some(port) = self.ports.get(&key) else {
            return false;
        };
        values.into_iter().all(|value| port.contains(&value))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationError {
    pub path: Box<str>,
    pub message: Box<str>,
}

impl ValidationError {
    fn new(path: impl AsRef<str>, message: impl AsRef<str>) -> Self {
        Self {
            path: path.as_ref().into(),
            message: message.as_ref().into(),
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.path, self.message)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationErrors(Box<[ValidationError]>);

pub const MAX_VALIDATION_ERRORS: usize = 128;

#[derive(Default)]
pub(crate) struct ValidationErrorCollector {
    errors: Vec<ValidationError>,
    truncated: bool,
}

impl ValidationErrorCollector {
    pub(crate) const fn new() -> Self {
        Self {
            errors: Vec::new(),
            truncated: false,
        }
    }

    pub(crate) fn push(&mut self, error: ValidationError) {
        if self.errors.len() < MAX_VALIDATION_ERRORS {
            self.errors.push(error);
        } else {
            self.mark_truncated();
        }
    }

    pub(crate) fn is_saturated(&self) -> bool {
        self.errors.len() >= MAX_VALIDATION_ERRORS
    }

    pub(crate) fn mark_truncated(&mut self) {
        if !self.truncated {
            self.truncated = true;
            self.errors.push(ValidationError::new(
                "validation",
                "additional validation errors were truncated",
            ));
        }
    }

    fn into_vec(self) -> Vec<ValidationError> {
        self.errors
    }
}

impl std::ops::Deref for ValidationErrorCollector {
    type Target = Vec<ValidationError>;

    fn deref(&self) -> &Self::Target {
        &self.errors
    }
}

impl ValidationErrors {
    pub fn errors(&self) -> &[ValidationError] {
        &self.0
    }

    fn from_collector(errors: ValidationErrorCollector) -> Self {
        Self(errors.into_vec().into_boxed_slice())
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "physical plan has {} validation error(s)",
            self.0.len()
        )?;
        for error in &self.0 {
            write!(formatter, "; {error}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ValidationErrors {}

pub fn validate_fragment(fragment: &Fragment, cuts: &FragmentCuts) -> Result<(), ValidationErrors> {
    let mut errors = ValidationErrorCollector::new();
    validate_fragment_into(fragment, &mut errors);
    validate_fragment_cut_resources(fragment, cuts, &mut errors);
    if !errors.is_empty() {
        return Err(ValidationErrors::from_collector(errors));
    }
    validate_fragment_cuts_into(fragment, cuts, true, &mut errors);
    validate_fragment_partition_identities(fragment, cuts, &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors))
    }
}

fn validate_fragment_partition_identities(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    errors: &mut ValidationErrorCollector,
) {
    let mut spaces = BTreeMap::new();
    let mut counts = BTreeMap::new();
    for node in fragment.nodes().values() {
        for (ordinal, properties) in node.required_inputs.iter().enumerate() {
            register_partition_identity(
                &properties.distribution,
                &format!("nodes[{}].required_inputs[{ordinal}]", node.id.get()),
                &mut spaces,
                &mut counts,
                errors,
            );
        }
        register_partition_identity(
            &node.output_properties.distribution,
            &format!("nodes[{}].output_properties", node.id.get()),
            &mut spaces,
            &mut counts,
            errors,
        );
        if let NodeKind::TableWriter { target } = &node.kind {
            register_partition_identity(
                &target.required_distribution,
                &format!("nodes[{}].writer.required_distribution", node.id.get()),
                &mut spaces,
                &mut counts,
                errors,
            );
        }
    }
    for (direction, ordinal, partitioning) in cuts
        .inbound
        .iter()
        .enumerate()
        .flat_map(|(ordinal, cut)| {
            [
                ("inbound.source", ordinal, &cut.partitioning.source),
                (
                    "inbound.destination",
                    ordinal,
                    &cut.partitioning.destination,
                ),
            ]
        })
        .chain(cuts.outbound.iter().enumerate().flat_map(|(ordinal, cut)| {
            [
                ("outbound.source", ordinal, &cut.partitioning.source),
                (
                    "outbound.destination",
                    ordinal,
                    &cut.partitioning.destination,
                ),
            ]
        }))
    {
        register_partition_identity(
            partitioning,
            &format!("cuts.{direction}[{ordinal}].partitioning"),
            &mut spaces,
            &mut counts,
            errors,
        );
    }
}

pub(crate) fn validate_fragment_definition(fragment: &Fragment) -> Result<(), ValidationErrors> {
    let mut errors = ValidationErrorCollector::new();
    validate_fragment_into(fragment, &mut errors);
    validate_fragment_partition_identities(fragment, &FragmentCuts::default(), &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors))
    }
}

/// Derive the explicit cut contract used to validate one fragment without the
/// rest of the plan graph.
pub fn fragment_cuts(plan: &PhysicalPlan, fragment_id: FragmentId) -> Option<FragmentCuts> {
    let derivation = FragmentCutDerivation::new(plan)?;
    derivation.derive(plan, fragment_id)
}

/// Derive every independently verifiable fragment cut in one indexed pass.
pub fn derive_fragment_cuts(plan: &PhysicalPlan) -> Option<BTreeMap<FragmentId, FragmentCuts>> {
    let mut errors = ValidationErrorCollector::new();
    let derivation = FragmentCutDerivation::new(plan)?;
    let mut total_items = 0usize;
    let mut total_bytes = 0usize;
    for fragment in plan.fragments().keys().copied() {
        let usage = preflight_fragment_cut_resources(plan, fragment, &derivation, &mut errors)?;
        total_items = total_items.saturating_add(usage.items);
        total_bytes = total_bytes.saturating_add(usage.bytes);
    }
    if !errors.is_empty()
        || total_items > MAX_PLAN_DERIVED_CUT_ITEMS
        || total_bytes > MAX_PLAN_DERIVED_CUT_BYTES
    {
        return None;
    }
    plan.fragments()
        .keys()
        .copied()
        .map(|fragment| Some((fragment, derivation.derive_preflighted(plan, fragment)?)))
        .collect()
}

struct FragmentCutDerivation {
    provenance: PlanSourceProvenance,
    inbound: BTreeMap<FragmentId, Vec<EdgeId>>,
    outbound: BTreeMap<FragmentId, Vec<EdgeId>>,
    change_stream_writers: BTreeMap<EdgeId, crate::ChangeStreamWriterCut>,
    proof_hulls: Vec<RuntimeFilterProofHull>,
    proof_hull_by_fragment: BTreeMap<FragmentId, usize>,
}

struct RuntimeFilterProofHull {
    fragments: BTreeSet<FragmentId>,
    edges: BTreeSet<EdgeId>,
    filters: BTreeSet<crate::RuntimeFilterId>,
}

#[derive(Default)]
struct RuntimeFilterBuildDependencyClosure {
    fragments: BTreeSet<FragmentId>,
    edges: BTreeSet<EdgeId>,
    sites: BTreeSet<(FragmentId, NodeId)>,
}

#[derive(Default)]
struct RuntimeFilterBuildDependencyCache {
    by_root: BTreeMap<(FragmentId, NodeId), RuntimeFilterBuildDependencyClosure>,
    source_sinks: Option<SourceSinkEdgeIndex>,
}

struct RuntimeFilterBuildExpansion<'a> {
    fragments: &'a mut BTreeSet<FragmentId>,
    edges: &'a mut BTreeSet<EdgeId>,
    dependency_sites: &'a mut BTreeSet<(FragmentId, NodeId)>,
    new_dependency_sites: &'a mut Vec<(FragmentId, NodeId)>,
    expanded_build_roots: &'a mut BTreeSet<(FragmentId, NodeId)>,
    cache: &'a mut RuntimeFilterBuildDependencyCache,
    work_budget: &'a mut SemanticTraceWorkBudget,
}

impl FragmentCutDerivation {
    fn new(plan: &PhysicalPlan) -> Option<Self> {
        let provenance = source_provenance_index(plan)?;
        let mut inbound = BTreeMap::<FragmentId, Vec<EdgeId>>::new();
        let mut outbound = BTreeMap::<FragmentId, Vec<EdgeId>>::new();
        for edge in plan.edges().values() {
            inbound
                .entry(edge.destination.fragment)
                .or_default()
                .push(edge.id);
            outbound
                .entry(edge.source.fragment)
                .or_default()
                .push(edge.id);
        }
        let mut change_stream_writers = BTreeMap::new();
        for fragment in plan.fragments().values() {
            let FragmentSink::Router { routes, .. } = fragment.sink() else {
                continue;
            };
            for route in routes {
                let Some(edge) = plan.edges().get(&route.edge) else {
                    continue;
                };
                if edge.kind != crate::EdgeKind::ChangeStreamRouter
                    || edge.source.fragment != fragment.id()
                {
                    continue;
                }
                if let Some(proof) = change_stream_writer_cut(route, edge) {
                    change_stream_writers.insert(edge.id, proof);
                }
            }
        }
        let mut proof_hulls = Vec::new();
        let mut proof_hull_by_fragment = BTreeMap::new();
        let mut proof_hull_by_filter_set = BTreeMap::<Box<[crate::RuntimeFilterId]>, usize>::new();
        let mut build_dependency_cache = RuntimeFilterBuildDependencyCache::default();
        let mut proof_work_budget = SemanticTraceWorkBudget::new();
        for fragment in plan.fragments().values() {
            let mut key = fragment.runtime_filters().to_vec();
            key.sort_unstable();
            let key = key.into_boxed_slice();
            let index = if let Some(index) = proof_hull_by_filter_set.get(&key) {
                *index
            } else {
                let mut fragments = BTreeSet::new();
                let mut edges = BTreeSet::new();
                let filters = extend_runtime_filter_proof_hull(
                    plan,
                    fragment.runtime_filters().iter().copied(),
                    &mut fragments,
                    &mut edges,
                    &mut build_dependency_cache,
                    &mut proof_work_budget,
                )?;
                for edge in &edges {
                    let edge = plan.edges().get(edge)?;
                    fragments.extend([edge.source.fragment, edge.destination.fragment]);
                }
                let index = proof_hulls.len();
                proof_hulls.push(RuntimeFilterProofHull {
                    fragments,
                    edges,
                    filters,
                });
                proof_hull_by_filter_set.insert(key, index);
                index
            };
            proof_hull_by_fragment.insert(fragment.id(), index);
        }
        Some(Self {
            provenance,
            inbound,
            outbound,
            change_stream_writers,
            proof_hulls,
            proof_hull_by_fragment,
        })
    }

    fn derive(&self, plan: &PhysicalPlan, fragment_id: FragmentId) -> Option<FragmentCuts> {
        let mut errors = ValidationErrorCollector::new();
        preflight_fragment_cut_resources(plan, fragment_id, self, &mut errors)?;
        if !errors.is_empty() {
            return None;
        }
        self.derive_preflighted(plan, fragment_id)
    }

    fn derive_preflighted(
        &self,
        plan: &PhysicalPlan,
        fragment_id: FragmentId,
    ) -> Option<FragmentCuts> {
        fragment_cuts_with_provenance(
            plan,
            fragment_id,
            &self.provenance,
            self.inbound
                .get(&fragment_id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            self.outbound
                .get(&fragment_id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            self,
            self.proof_hull(fragment_id)?,
        )
    }

    fn proof_hull(&self, fragment_id: FragmentId) -> Option<&RuntimeFilterProofHull> {
        self.proof_hulls
            .get(*self.proof_hull_by_fragment.get(&fragment_id)?)
    }

    fn change_stream_writer(&self, edge: EdgeId) -> Option<crate::ChangeStreamWriterCut> {
        self.change_stream_writers.get(&edge).cloned()
    }
}

fn preflight_fragment_cut_resources(
    plan: &PhysicalPlan,
    fragment_id: FragmentId,
    derivation: &FragmentCutDerivation,
    errors: &mut ValidationErrorCollector,
) -> Option<CutResourceUsage> {
    let fragment = plan.fragments().get(&fragment_id)?;
    let inbound = derivation
        .inbound
        .get(&fragment_id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let outbound = derivation
        .outbound
        .get(&fragment_id)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let path = format!("fragments[{}].cuts.preflight", fragment_id.get());
    let mut usage = CutResourcePreflight::new();
    usage.add_items(inbound.len() + outbound.len());
    for (edge_id, is_outbound) in inbound
        .iter()
        .map(|edge| (edge, false))
        .chain(outbound.iter().map(|edge| (edge, true)))
    {
        let edge = plan.edges().get(edge_id)?;
        let source = plan.fragments().get(&edge.source.fragment)?;
        let source_binding_count = derivation.provenance.binding_count(edge.source.fragment)?;
        usage.add_items(edge.destination.receive_mapping.len() * if is_outbound { 2 } else { 1 });
        usage.add_items(source_binding_count);
        usage.add_distribution(&edge.partitioning.source);
        usage.add_distribution(&edge.partitioning.destination);
        for binding in derivation.provenance.binding_refs(edge.source.fragment)? {
            usage.add_source(binding, &path);
        }
        for (source_value, _) in &edge.destination.receive_mapping {
            let ty = &source.values().get(source_value)?.ty;
            usage.add_value_type(ty, &path, errors);
            if is_outbound {
                usage.add_value_type(ty, &path, errors);
            }
        }
        if let Some(proof) = derivation.change_stream_writer(edge.id) {
            usage.add_items(proof.fields.len());
        }
        if let Some(proof) = writer_result_cut(plan, edge) {
            usage.add_items(proof.fields.len());
            for field in &proof.fields {
                usage.add_bytes(field.name.len());
                usage.add_value_type(&field.ty, &path, errors);
            }
        }
    }
    let artifacts = fragment
        .nodes()
        .values()
        .filter_map(|node| match &node.kind {
            NodeKind::Scan { relation, .. } => Some(relation.artifact_inputs()),
            _ => None,
        })
        .flatten()
        .map(|requirement| requirement.artifact)
        .collect::<BTreeSet<_>>();
    usage.add_items(artifacts.len());
    for artifact in artifacts {
        usage.add_artifact(plan.artifact_refs().get(&artifact)?, &path, errors);
    }
    usage.add_items(fragment.runtime_filters().len());
    for filter in fragment.runtime_filters() {
        usage.add_filter(plan.runtime_filters().get(filter)?, &path, errors);
    }
    let proof_hull = derivation.proof_hull(fragment_id)?;
    usage.add_items(proof_hull.fragments.len() + proof_hull.edges.len() + proof_hull.filters.len());
    for proof_fragment in &proof_hull.fragments {
        usage.add_fragment(plan.fragments().get(proof_fragment)?, errors);
    }
    for proof_edge in &proof_hull.edges {
        usage.add_edge(plan.edges().get(proof_edge)?);
    }
    for proof_filter in &proof_hull.filters {
        usage.add_filter(plan.runtime_filters().get(proof_filter)?, &path, errors);
    }
    Some(usage.validate(&format!("{path}.resources"), errors))
}

fn fragment_cuts_with_provenance(
    plan: &PhysicalPlan,
    fragment_id: FragmentId,
    provenance: &PlanSourceProvenance,
    inbound_edges: &[EdgeId],
    outbound_edges: &[EdgeId],
    derivation: &FragmentCutDerivation,
    proof_hull: &RuntimeFilterProofHull,
) -> Option<FragmentCuts> {
    let fragment = plan.fragments().get(&fragment_id)?;
    let inbound = inbound_edges
        .iter()
        .map(|edge| plan.edges().get(edge))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .map(|edge| {
            let source = plan.fragments().get(&edge.source.fragment)?;
            let source_bindings = provenance.bindings(edge.source.fragment)?;
            let has_source_free_rows = provenance.has_source_free_rows(edge.source.fragment)?;
            let imports = edge
                .destination
                .receive_mapping
                .iter()
                .map(|(source_value, destination)| {
                    Some(CutImport {
                        source: CutValue {
                            value: *source_value,
                            ty: source.values().get(source_value)?.ty.clone(),
                        },
                        destination: *destination,
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(InboundFragmentCut {
                edge: edge.id,
                kind: edge.kind,
                source_fragment: edge.source.fragment,
                destination_node: edge.destination.node,
                imports: imports.into_boxed_slice(),
                partitioning: edge.partitioning.clone(),
                source_bindings: source_bindings.into_boxed_slice(),
                has_source_free_rows,
                change_stream_writer: derivation.change_stream_writer(edge.id),
                writer_result: writer_result_cut(plan, edge),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let outbound = outbound_edges
        .iter()
        .map(|edge| plan.edges().get(edge))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .map(|edge| {
            let source_bindings = provenance.bindings(edge.source.fragment)?;
            let has_source_free_rows = provenance.has_source_free_rows(edge.source.fragment)?;
            let projection = edge
                .source
                .projection
                .iter()
                .map(|value| {
                    Some(CutValue {
                        value: *value,
                        ty: fragment.values().get(value)?.ty.clone(),
                    })
                })
                .collect::<Option<Vec<_>>>()?;
            Some(OutboundFragmentCut {
                edge: edge.id,
                kind: edge.kind,
                destination_fragment: edge.destination.fragment,
                projection: projection.into_boxed_slice(),
                destination_imports: edge
                    .destination
                    .receive_mapping
                    .iter()
                    .map(|(source, destination)| {
                        Some(CutImport {
                            source: CutValue {
                                value: *source,
                                ty: fragment.values().get(source)?.ty.clone(),
                            },
                            destination: *destination,
                        })
                    })
                    .collect::<Option<Vec<_>>>()?
                    .into_boxed_slice(),
                partitioning: edge.partitioning.clone(),
                source_bindings: source_bindings.into_boxed_slice(),
                has_source_free_rows,
                change_stream_writer: derivation.change_stream_writer(edge.id),
                writer_result: writer_result_cut(plan, edge),
            })
        })
        .collect::<Option<Vec<_>>>()?;
    let mut artifact_refs = BTreeMap::new();
    for requirement in fragment
        .nodes()
        .values()
        .filter_map(|node| match &node.kind {
            NodeKind::Scan { relation, .. } => Some(relation.artifact_inputs()),
            _ => None,
        })
        .flatten()
    {
        artifact_refs.insert(
            requirement.artifact,
            plan.artifact_refs().get(&requirement.artifact)?.clone(),
        );
    }
    let runtime_filters = fragment
        .runtime_filters()
        .iter()
        .map(|id| plan.runtime_filters().get(id).cloned())
        .collect::<Option<Vec<_>>>()?;
    let runtime_filter_proof = crate::RuntimeFilterProofGraph {
        fragments: proof_hull
            .fragments
            .iter()
            .copied()
            .map(|id| plan.fragments().get(&id).cloned())
            .collect::<Option<Vec<_>>>()?
            .into_boxed_slice(),
        edges: proof_hull
            .edges
            .iter()
            .copied()
            .map(|id| plan.edges().get(&id).cloned())
            .collect::<Option<Vec<_>>>()?
            .into_boxed_slice(),
        filters: proof_hull
            .filters
            .iter()
            .copied()
            .map(|id| plan.runtime_filters().get(&id).cloned())
            .collect::<Option<Vec<_>>>()?
            .into_boxed_slice(),
    };
    Some(FragmentCuts {
        inbound: inbound.into_boxed_slice(),
        outbound: outbound.into_boxed_slice(),
        artifact_refs: artifact_refs.into_values().collect(),
        runtime_filters: runtime_filters.into_boxed_slice(),
        runtime_filter_proof,
    })
}

fn change_stream_writer_cut(
    route: &crate::ChangeStreamRoute,
    edge: &Edge,
) -> Option<crate::ChangeStreamWriterCut> {
    if route.input_mapping.len() != edge.destination.receive_mapping.len() {
        return None;
    }
    let fields = route
        .input_mapping
        .iter()
        .zip(edge.destination.receive_mapping.iter())
        .map(|((token, route_source), (mapped_source, destination))| {
            (route_source == mapped_source).then_some(crate::ChangeStreamWriterCutField {
                token: *token,
                source: *route_source,
                destination: *destination,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(crate::ChangeStreamWriterCut {
        route_id: route.route_id,
        write_target_ordinal: route.write_target_ordinal,
        fields: fields.into_boxed_slice(),
    })
}

fn writer_result_cut(plan: &PhysicalPlan, edge: &Edge) -> Option<crate::WriterResultCut> {
    if edge.kind != crate::EdgeKind::Stream {
        return None;
    }
    let source = plan.fragments().get(&edge.source.fragment)?;
    let root = source.nodes().get(&source.root())?;
    let NodeKind::TableWriter { target } = &root.kind else {
        return None;
    };
    if !matches!(source.sink(), FragmentSink::Stream { edge: sink_edge } if *sink_edge == edge.id)
        || target.output_schema.fields.len() != edge.destination.receive_mapping.len()
        || root.output.columns.as_ref() != edge.source.projection.as_ref()
    {
        return None;
    }
    let fields = target
        .output_schema
        .fields
        .iter()
        .zip(&edge.destination.receive_mapping)
        .map(|(field, (mapped_source, destination))| {
            (field.value == *mapped_source).then_some(crate::WriterResultCutField {
                source: field.value,
                destination: *destination,
                name: field.name.clone(),
                ty: field.ty.clone(),
                role: field.role,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some(crate::WriterResultCut {
        write_target_ordinal: target.write_target_ordinal,
        schema_revision: target.output_schema.revision,
        fields: fields.into_boxed_slice(),
    })
}

fn extend_runtime_filter_build_dependencies(
    plan: &PhysicalPlan,
    producer: &crate::RuntimeFilterProducer,
    expansion: &mut RuntimeFilterBuildExpansion<'_>,
) -> bool {
    let Some(fragment) = plan.fragments().get(&producer.endpoint.fragment) else {
        return false;
    };
    let Some(join) = fragment.nodes().get(&producer.endpoint.node) else {
        return false;
    };
    let NodeKind::HashJoin { build_side, .. } = &join.kind else {
        return false;
    };
    let Some(build_root) = usize::try_from(build_side.input_ordinal())
        .ok()
        .and_then(|ordinal| join.inputs.get(ordinal))
        .copied()
    else {
        return false;
    };

    let root = (fragment.id(), build_root);
    if !expansion.expanded_build_roots.insert(root) {
        return true;
    }
    if expansion.cache.source_sinks.is_none() {
        expansion.cache.source_sinks = Some(SourceSinkEdgeIndex::new(plan));
    }

    if let Entry::Vacant(entry) = expansion.cache.by_root.entry(root) {
        let mut closure = RuntimeFilterBuildDependencyClosure::default();
        let mut pending = vec![root];
        while let Some((fragment_id, node_id)) = pending.pop() {
            if !closure.sites.insert((fragment_id, node_id)) {
                continue;
            }
            if !expansion.work_budget.charge(1) {
                return false;
            }
            closure.fragments.insert(fragment_id);
            let Some(fragment) = plan.fragments().get(&fragment_id) else {
                return false;
            };
            let Some(node) = fragment.nodes().get(&node_id) else {
                return false;
            };
            if let NodeKind::ExchangeSource { edge, .. } = &node.kind {
                let Some(edge_contract) = plan.edges().get(edge) else {
                    return false;
                };
                if edge_contract.destination.fragment != fragment_id
                    || edge_contract.destination.node != node_id
                {
                    return false;
                }
                closure.edges.insert(*edge);
                closure.fragments.extend([
                    edge_contract.source.fragment,
                    edge_contract.destination.fragment,
                ]);
                let Some(source) = plan.fragments().get(&edge_contract.source.fragment) else {
                    return false;
                };
                if !expansion
                    .cache
                    .source_sinks
                    .as_ref()
                    .is_some_and(|sinks| sinks.owns(edge_contract))
                {
                    return false;
                }
                pending.push((source.id(), source.root()));
            }
            pending.extend(node.inputs.iter().map(|input| (fragment_id, *input)));
        }
        entry.insert(closure);
    }

    let Some(closure) = expansion.cache.by_root.get(&root) else {
        return false;
    };
    let merge_work = closure
        .sites
        .len()
        .saturating_add(closure.fragments.len())
        .saturating_add(closure.edges.len());
    if !expansion.work_budget.charge(merge_work) {
        return false;
    }
    for site in &closure.sites {
        if expansion.dependency_sites.insert(*site) {
            expansion.new_dependency_sites.push(*site);
        }
    }
    expansion
        .fragments
        .extend(closure.fragments.iter().copied());
    expansion.edges.extend(closure.edges.iter().copied());
    true
}

fn extend_runtime_filter_proof_hull(
    plan: &PhysicalPlan,
    seed_filters: impl IntoIterator<Item = crate::RuntimeFilterId>,
    fragments: &mut BTreeSet<FragmentId>,
    edges: &mut BTreeSet<EdgeId>,
    build_dependency_cache: &mut RuntimeFilterBuildDependencyCache,
    work_budget: &mut SemanticTraceWorkBudget,
) -> Option<BTreeSet<crate::RuntimeFilterId>> {
    let mut seeds = BTreeSet::new();
    for filter in seed_filters {
        if !work_budget.charge(1) {
            return None;
        }
        seeds.insert(filter);
    }
    let mut included = BTreeSet::new();
    let mut expanded = BTreeSet::new();
    let mut include_queue = seeds.iter().copied().collect::<Vec<_>>();
    let mut active_queue = include_queue.clone();
    let mut dependency_sites = BTreeSet::new();
    let mut expanded_build_roots = BTreeSet::new();
    let mut blocking_at = BTreeMap::<(FragmentId, NodeId), Vec<crate::RuntimeFilterId>>::new();

    while !include_queue.is_empty() || !active_queue.is_empty() {
        while let Some(filter_id) = include_queue.pop() {
            if !included.insert(filter_id) {
                continue;
            }
            let filter = plan.runtime_filters().get(&filter_id)?;
            let static_work = filter
                .equality_witnesses
                .len()
                .saturating_add(filter.producers.len())
                .saturating_add(filter.consumers.len())
                .saturating_add(filter.producers.iter().fold(0usize, |work, producer| {
                    work.saturating_add(producer.progress.build_edges.len())
                        .saturating_add(producer.progress.non_build_edges.len())
                }))
                .saturating_add(filter.consumers.iter().fold(0usize, |work, consumer| {
                    work.saturating_add(match &consumer.target {
                        crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => 0,
                        crate::RuntimeFilterConsumerTarget::ScanField { lineage, .. } => {
                            lineage.len()
                        }
                    })
                }));
            if !work_budget.charge(static_work) {
                return None;
            }
            fragments.extend(
                filter
                    .equality_witnesses
                    .iter()
                    .map(|witness| witness.fragment),
            );
            fragments.extend(
                filter
                    .producers
                    .iter()
                    .map(|producer| producer.endpoint.fragment),
            );
            fragments.extend(
                filter
                    .consumers
                    .iter()
                    .map(|consumer| consumer.endpoint.fragment),
            );
            for producer in &filter.producers {
                edges.extend(
                    producer
                        .progress
                        .build_edges
                        .iter()
                        .chain(&producer.progress.non_build_edges)
                        .copied(),
                );
            }
            for consumer in &filter.consumers {
                if consumer.activation == crate::RuntimeFilterConsumerActivation::BlockingSnapshot {
                    let site = (consumer.endpoint.fragment, consumer.endpoint.node);
                    blocking_at.entry(site).or_default().push(filter_id);
                    if dependency_sites.contains(&site) {
                        active_queue.push(filter_id);
                    }
                }
                let crate::RuntimeFilterConsumerTarget::ScanField { lineage, .. } =
                    &consumer.target
                else {
                    continue;
                };
                for step in lineage {
                    match step {
                        crate::RuntimeFilterLineageStep::FilterPassThrough { fragment, .. }
                        | crate::RuntimeFilterLineageStep::SortPassThrough { fragment, .. }
                        | crate::RuntimeFilterLineageStep::ProjectIdentity { fragment, .. }
                        | crate::RuntimeFilterLineageStep::AggregateGroupKey { fragment, .. }
                        | crate::RuntimeFilterLineageStep::UnionAllBranch { fragment, .. } => {
                            fragments.insert(*fragment);
                        }
                        crate::RuntimeFilterLineageStep::ExchangeMapping { edge, .. } => {
                            edges.insert(*edge);
                        }
                    }
                }
            }
        }

        while let Some(filter_id) = active_queue.pop() {
            if !expanded.insert(filter_id) {
                continue;
            }
            let filter = plan.runtime_filters().get(&filter_id)?;
            if !work_budget.charge(filter.producers.len()) {
                return None;
            }
            for producer in &filter.producers {
                let mut new_sites = Vec::new();
                let mut expansion = RuntimeFilterBuildExpansion {
                    fragments,
                    edges,
                    dependency_sites: &mut dependency_sites,
                    new_dependency_sites: &mut new_sites,
                    expanded_build_roots: &mut expanded_build_roots,
                    cache: build_dependency_cache,
                    work_budget,
                };
                if !extend_runtime_filter_build_dependencies(plan, producer, &mut expansion) {
                    return None;
                }
                for site in new_sites {
                    if let Some(blocked_filters) = blocking_at.get(&site) {
                        if !work_budget.charge(blocked_filters.len()) {
                            return None;
                        }
                        active_queue.extend(blocked_filters.iter().copied());
                    }
                    let dependency_fragment = plan.fragments().get(&site.0)?;
                    if !work_budget.charge(dependency_fragment.runtime_filters().len()) {
                        return None;
                    }
                    include_queue.extend(dependency_fragment.runtime_filters().iter().copied());
                }
            }
        }
    }

    Some(included)
}

#[derive(Clone, Default, PartialEq)]
struct SourceBindingIndex {
    bindings: BTreeSet<ArtifactSourceBinding>,
}

impl SourceBindingIndex {
    fn insert(&mut self, binding: ArtifactSourceBinding) {
        self.bindings.insert(binding);
    }

    fn len(&self) -> usize {
        self.bindings.len()
    }

    fn values(&self) -> impl Iterator<Item = &ArtifactSourceBinding> {
        self.bindings.iter()
    }
}

#[derive(Clone, Default)]
struct SourceProvenance {
    bindings: SourceBindingIndex,
    has_source_free_rows: bool,
}

#[derive(Default)]
struct SourceBindingRegistry {
    ids: BTreeMap<Arc<ArtifactSourceBinding>, u32>,
    bindings: Vec<Arc<ArtifactSourceBinding>>,
}

impl SourceBindingRegistry {
    fn intern(&mut self, binding: ArtifactSourceBinding) -> Option<usize> {
        if let Some(id) = self.ids.get(&binding) {
            return Some(*id as usize);
        }
        let id = u32::try_from(self.bindings.len()).ok()?;
        let binding = Arc::new(binding);
        self.ids.insert(Arc::clone(&binding), id);
        self.bindings.push(binding);
        Some(id as usize)
    }
}

#[derive(Clone, Default)]
struct CompactSourceBindingSet {
    ids: Arc<[u32]>,
}

impl CompactSourceBindingSet {
    fn from_ids(ids: &[usize]) -> Option<Self> {
        Some(Self {
            ids: ids
                .iter()
                .map(|id| u32::try_from(*id).ok())
                .collect::<Option<Vec<_>>>()?
                .into(),
        })
    }

    fn union(local: &Self, parents: impl IntoIterator<Item = Self>) -> Self {
        let mut sets = parents
            .into_iter()
            .filter(|set| !set.ids.is_empty())
            .collect::<Vec<_>>();
        if !local.ids.is_empty() {
            sets.push(local.clone());
        }
        if sets.is_empty() {
            return Self::default();
        }
        while sets.len() > 1 {
            let mut merged = Vec::with_capacity(sets.len().div_ceil(2));
            let mut pairs = sets.chunks_exact(2);
            for pair in &mut pairs {
                merged.push(Self::merge_sorted(&pair[0], &pair[1]));
            }
            if let [remainder] = pairs.remainder() {
                merged.push(remainder.clone());
            }
            sets = merged;
        }
        sets.pop()
            .expect("one non-empty source binding set remains")
    }

    fn merge_sorted(left: &Self, right: &Self) -> Self {
        if Arc::ptr_eq(&left.ids, &right.ids) {
            return left.clone();
        }
        let mut ids = Vec::with_capacity(left.ids.len().saturating_add(right.ids.len()));
        let (mut left_index, mut right_index) = (0, 0);
        while left_index < left.ids.len() && right_index < right.ids.len() {
            match left.ids[left_index].cmp(&right.ids[right_index]) {
                std::cmp::Ordering::Less => {
                    ids.push(left.ids[left_index]);
                    left_index += 1;
                }
                std::cmp::Ordering::Greater => {
                    ids.push(right.ids[right_index]);
                    right_index += 1;
                }
                std::cmp::Ordering::Equal => {
                    ids.push(left.ids[left_index]);
                    left_index += 1;
                    right_index += 1;
                }
            }
        }
        ids.extend_from_slice(&left.ids[left_index..]);
        ids.extend_from_slice(&right.ids[right_index..]);
        Self { ids: ids.into() }
    }

    fn ids(&self) -> impl Iterator<Item = usize> + '_ {
        self.ids.iter().map(|id| *id as usize)
    }

    fn len(&self) -> usize {
        self.ids.len()
    }
}

fn charge_provenance_cut_items(
    current: &mut usize,
    source_binding_count: usize,
    outgoing_count: usize,
) -> Option<()> {
    *current = current.saturating_add(
        source_binding_count
            .saturating_mul(outgoing_count)
            .saturating_mul(2),
    );
    (*current <= MAX_PLAN_DERIVED_CUT_ITEMS).then_some(())
}

struct PlanSourceProvenance {
    registry: SourceBindingRegistry,
    by_fragment: BTreeMap<FragmentId, CompactSourceBindingSet>,
    source_free_fragments: BTreeSet<FragmentId>,
}

impl PlanSourceProvenance {
    fn binding_refs(
        &self,
        fragment: FragmentId,
    ) -> Option<impl Iterator<Item = &ArtifactSourceBinding>> {
        Some(self.by_fragment.get(&fragment)?.ids().map(|id| {
            self.registry
                .bindings
                .get(id)
                .expect("compact provenance ids are interned")
                .as_ref()
        }))
    }

    fn bindings(&self, fragment: FragmentId) -> Option<Vec<ArtifactSourceBinding>> {
        Some(self.binding_refs(fragment)?.cloned().collect())
    }

    fn binding_count(&self, fragment: FragmentId) -> Option<usize> {
        Some(self.by_fragment.get(&fragment)?.len())
    }

    fn has_source_free_rows(&self, fragment: FragmentId) -> Option<bool> {
        self.by_fragment
            .contains_key(&fragment)
            .then(|| self.source_free_fragments.contains(&fragment))
    }
}

fn source_provenance_index(plan: &PhysicalPlan) -> Option<PlanSourceProvenance> {
    let mut registry = SourceBindingRegistry::default();
    let mut local_ids = BTreeMap::<FragmentId, Vec<usize>>::new();
    let mut local_source_free = BTreeSet::new();
    let mut indegree = plan
        .fragments()
        .keys()
        .copied()
        .map(|fragment| (fragment, 0usize))
        .collect::<BTreeMap<_, _>>();
    let mut outgoing = BTreeMap::<FragmentId, Vec<FragmentId>>::new();
    let mut incoming = BTreeMap::<FragmentId, Vec<FragmentId>>::new();
    for edge in plan.edges().values() {
        *indegree.get_mut(&edge.destination.fragment)? += 1;
        outgoing
            .entry(edge.source.fragment)
            .or_default()
            .push(edge.destination.fragment);
        incoming
            .entry(edge.destination.fragment)
            .or_default()
            .push(edge.source.fragment);
    }
    for fragment in plan.fragments().values() {
        let mut ids = Vec::new();
        for node in fragment.nodes().values() {
            match &node.kind {
                NodeKind::Scan { relation, .. } => {
                    ids.push(registry.intern(relation.source_binding())?);
                }
                NodeKind::Values { rows } if !rows.is_empty() => {
                    local_source_free.insert(fragment.id());
                }
                NodeKind::GenerateSeries { .. } => {
                    local_source_free.insert(fragment.id());
                }
                NodeKind::TableFunction { .. } if node.inputs.is_empty() => {
                    local_source_free.insert(fragment.id());
                }
                _ => {}
            }
        }
        ids.sort_unstable();
        ids.dedup();
        local_ids.insert(fragment.id(), ids);
    }
    let local_sets = local_ids
        .iter()
        .map(|(fragment, ids)| Some((*fragment, CompactSourceBindingSet::from_ids(ids)?)))
        .collect::<Option<BTreeMap<_, _>>>()?;
    let mut by_fragment = BTreeMap::new();
    let mut source_free_fragments = BTreeSet::new();
    let mut cut_binding_items = 0_usize;
    let mut ready = indegree
        .iter()
        .filter_map(|(fragment, degree)| (*degree == 0).then_some(*fragment))
        .collect::<BTreeSet<_>>();
    let mut visited = 0usize;
    while let Some(source) = ready.pop_first() {
        visited += 1;
        if !by_fragment.contains_key(&source) {
            let parents = incoming
                .get(&source)
                .into_iter()
                .flatten()
                .map(|parent| by_fragment.get(parent).cloned())
                .collect::<Option<Vec<_>>>()?;
            let combined = CompactSourceBindingSet::union(local_sets.get(&source)?, parents);
            by_fragment.insert(source, combined);
            if local_source_free.contains(&source)
                || incoming
                    .get(&source)
                    .into_iter()
                    .flatten()
                    .any(|parent| source_free_fragments.contains(parent))
            {
                source_free_fragments.insert(source);
            }
        }
        let source_binding_count = by_fragment.get(&source)?.len();
        let outgoing_count = outgoing.get(&source).map_or(0, Vec::len);
        charge_provenance_cut_items(&mut cut_binding_items, source_binding_count, outgoing_count)?;
        for destination in outgoing.get(&source).into_iter().flatten() {
            let degree = indegree.get_mut(destination)?;
            *degree = degree.checked_sub(1)?;
            if *degree == 0 {
                ready.insert(*destination);
            }
        }
    }
    (visited == plan.fragments().len()).then_some(PlanSourceProvenance {
        registry,
        by_fragment,
        source_free_fragments,
    })
}

pub fn validate_plan(plan: &PhysicalPlan) -> Result<(), ValidationErrors> {
    let mut errors = ValidationErrorCollector::new();
    validate_plan_resources(plan, &mut errors);
    if !errors.is_empty() {
        return Err(ValidationErrors::from_collector(errors));
    }
    if plan.required().plan_contract_revision != PLAN_CONTRACT_REVISION {
        errors.push(ValidationError::new(
            "required.plan_contract_revision",
            format!(
                "expected {PLAN_CONTRACT_REVISION}, got {}",
                plan.required().plan_contract_revision
            ),
        ));
    }
    bounded_count(
        &mut errors,
        "fragments",
        plan.fragments().len(),
        MAX_PLAN_FRAGMENTS,
    );
    bounded_count(&mut errors, "edges", plan.edges().len(), MAX_PLAN_EDGES);
    bounded_count(
        &mut errors,
        "runtime_filters",
        plan.runtime_filters().len(),
        MAX_PLAN_RUNTIME_FILTERS,
    );
    bounded_count(
        &mut errors,
        "artifact_refs",
        plan.artifact_refs().len(),
        MAX_PLAN_ARTIFACT_REFS,
    );
    if plan.fragments().is_empty() {
        errors.push(ValidationError::new("fragments", "plan has no fragments"));
    }

    for fragment in plan.fragments().values() {
        validate_fragment_into(fragment, &mut errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return Err(ValidationErrors::from_collector(errors));
        }
    }
    let mut root_port_indexes = BTreeMap::new();
    for edge in plan.edges().values() {
        validate_edge(plan, edge, &mut root_port_indexes, &mut errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return Err(ValidationErrors::from_collector(errors));
        }
    }
    macro_rules! run_validation_stage {
        ($stage:expr) => {{
            $stage;
            if errors.is_saturated() {
                errors.mark_truncated();
                return Err(ValidationErrors::from_collector(errors));
            }
        }};
    }
    run_validation_stage!(validate_fragment_graph(plan, &mut errors));
    run_validation_stage!(validate_sinks(plan, &mut errors));
    run_validation_stage!(validate_writer_flows(plan, &mut errors));
    run_validation_stage!(validate_result(plan, &mut errors));
    run_validation_stage!(validate_runtime_filters(plan, &mut errors));
    run_validation_stage!(validate_artifact_refs(plan, &mut errors));
    run_validation_stage!(validate_artifact_inputs(plan, &mut errors));
    run_validation_stage!(validate_annotations(plan, &mut errors));
    run_validation_stage!(validate_cross_fragment_value_origins(plan, &mut errors));
    run_validation_stage!(validate_partition_identities(plan, &mut errors));
    run_validation_stage!(validate_aggregate_sequences(plan, &mut errors));
    run_validation_stage!(validate_topn_reductions(plan, &mut errors));

    if errors.is_empty() {
        let Some(derivation) = FragmentCutDerivation::new(plan) else {
            errors.push(ValidationError::new(
                "fragments.cuts",
                "cannot index the complete fragment cut graph",
            ));
            return Err(ValidationErrors::from_collector(errors));
        };
        let mut total_cut_items = 0usize;
        let mut total_cut_bytes = 0usize;
        for fragment in plan.fragments().values() {
            let Some(usage) =
                preflight_fragment_cut_resources(plan, fragment.id(), &derivation, &mut errors)
            else {
                errors.push(ValidationError::new(
                    format!("fragments[{}].cuts", fragment.id().get()),
                    "cannot preflight the complete fragment cuts",
                ));
                return Err(ValidationErrors::from_collector(errors));
            };
            total_cut_items = total_cut_items.saturating_add(usage.items);
            total_cut_bytes = total_cut_bytes.saturating_add(usage.bytes);
            if total_cut_items > MAX_PLAN_DERIVED_CUT_ITEMS
                || total_cut_bytes > MAX_PLAN_DERIVED_CUT_BYTES
            {
                errors.push(ValidationError::new(
                    "fragments.cuts.resources",
                    "aggregate derived fragment cuts exceed the plan publication budget",
                ));
            }
            if !errors.is_empty() {
                return Err(ValidationErrors::from_collector(errors));
            }
            let Some(cuts) = derivation.derive_preflighted(plan, fragment.id()) else {
                errors.push(ValidationError::new(
                    format!("fragments[{}].cuts", fragment.id().get()),
                    "cannot derive complete fragment cuts",
                ));
                return Err(ValidationErrors::from_collector(errors));
            };
            validate_fragment_cuts_into(fragment, &cuts, false, &mut errors);
            validate_fragment_partition_identities(fragment, &cuts, &mut errors);
            if !errors.is_empty() {
                return Err(ValidationErrors::from_collector(errors));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ValidationErrors::from_collector(errors))
    }
}

fn validate_aggregate_sequences(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    type CallRef = (FragmentId, NodeId, crate::AggregateCallId);

    #[derive(Default)]
    struct SequenceMembers {
        partials: BTreeSet<CallRef>,
        intermediates: BTreeSet<CallRef>,
        finals: Vec<CallRef>,
    }

    let mut sequences: BTreeMap<crate::AggregateSequenceId, SequenceMembers> = BTreeMap::new();
    let mut calls_by_ref = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for call in calls {
                let call_ref = (fragment.id(), node.id, call.id);
                calls_by_ref.insert(call_ref, (fragment, node, call));
                let Some(sequence) = call.binding.phase.sequence() else {
                    continue;
                };
                let members = sequences.entry(sequence).or_default();
                match call.binding.phase {
                    AggregatePhase::Single => unreachable!("single phase has no sequence"),
                    AggregatePhase::Partial { .. } => {
                        members.partials.insert(call_ref);
                    }
                    AggregatePhase::Intermediate { .. } => {
                        members.intermediates.insert(call_ref);
                    }
                    AggregatePhase::Final { .. } => members.finals.push(call_ref),
                }
            }
        }
    }

    let mut trace_budget = SemanticTraceWorkBudget::new();
    let mut trace_indexes = SemanticTraceIndexes::default();
    for (sequence, members) in sequences {
        let path = format!("aggregate_sequences[{}]", sequence.get());
        if members.finals.len() != 1 {
            errors.push(ValidationError::new(
                &path,
                "aggregate sequence must have exactly one final call",
            ));
            continue;
        }
        if members.partials.is_empty() {
            errors.push(ValidationError::new(
                &path,
                "aggregate sequence final has no partial producer",
            ));
            continue;
        }
        let final_ref = members.finals[0];
        let Some(&(final_fragment, final_node, final_call)) = calls_by_ref.get(&final_ref) else {
            continue;
        };
        let NodeKind::Aggregate { group_by, .. } = &final_node.kind else {
            continue;
        };
        let Some(values) = aggregate_state_inputs(final_fragment, group_by, final_call) else {
            errors.push(ValidationError::new(
                &path,
                "aggregate final inputs are not direct grouping/state values",
            ));
            continue;
        };
        let Some(input) = final_node.inputs.first().copied() else {
            continue;
        };
        let partial_shape = members.partials.iter().find_map(|call_ref| {
            calls_by_ref.get(call_ref).map(|(_, _, call)| {
                (
                    call.distinct,
                    call.order_by
                        .iter()
                        .map(|item| (item.direction, item.null_ordering))
                        .collect::<Vec<_>>(),
                )
            })
        });
        let Some(partial_shape) = partial_shape else {
            continue;
        };
        let mut matched_partials = BTreeSet::new();
        let mut matched_intermediates = BTreeSet::new();
        let valid = trace_aggregate_sequence_inputs(
            plan,
            sequence,
            (final_fragment.id(), input),
            values,
            &final_call.binding,
            &partial_shape,
            &mut matched_partials,
            &mut matched_intermediates,
            &mut trace_budget,
            &mut trace_indexes,
        );
        if !valid
            || matched_partials != members.partials
            || matched_intermediates != members.intermediates
        {
            errors.push(ValidationError::new(
                &path,
                "aggregate state paths do not reduce exactly into their matching final",
            ));
        }
    }
}

fn aggregate_state_inputs(
    fragment: &Fragment,
    group_by: &[(ExprId, ValueId)],
    call: &crate::AggregateCall,
) -> Option<Vec<ValueId>> {
    let mut values = group_by
        .iter()
        .map(|(expression, _)| expression_value(fragment, *expression))
        .collect::<Option<Vec<_>>>()?;
    if call.arguments.len() != 1 || !call.order_by.is_empty() {
        return None;
    }
    values.push(expression_value(fragment, call.arguments[0])?);
    Some(values)
}

fn aggregate_outputs(group_by: &[(ExprId, ValueId)], call: &crate::AggregateCall) -> Vec<ValueId> {
    group_by
        .iter()
        .map(|(_, output)| *output)
        .chain(std::iter::once(call.output))
        .collect()
}

fn aggregate_bindings_match(
    expected: &crate::AggregateBinding,
    actual: &crate::AggregateBinding,
) -> bool {
    expected.function == actual.function
        && expected.logical_argument_count == actual.logical_argument_count
        && expected.intermediate_type == actual.intermediate_type
        && expected.state_format == actual.state_format
        && expected.phase.sequence() == actual.phase.sequence()
}

#[allow(clippy::too_many_arguments)]
fn trace_aggregate_sequence_inputs(
    plan: &PhysicalPlan,
    sequence: crate::AggregateSequenceId,
    start: (FragmentId, NodeId),
    initial_values: Vec<ValueId>,
    expected_binding: &crate::AggregateBinding,
    partial_shape: &(bool, Vec<(crate::SortDirection, crate::NullOrdering)>),
    matched_partials: &mut BTreeSet<(FragmentId, NodeId, crate::AggregateCallId)>,
    matched_intermediates: &mut BTreeSet<(FragmentId, NodeId, crate::AggregateCallId)>,
    trace_budget: &mut SemanticTraceWorkBudget,
    trace_indexes: &mut SemanticTraceIndexes,
) -> bool {
    let mut pending = vec![(start, initial_values)];
    let mut visited = BTreeSet::new();
    while let Some((node_ref, expected_values)) = pending.pop() {
        if !trace_budget.charge(expected_values.len().saturating_add(1))
            || !visited.insert((node_ref.0, node_ref.1, expected_values.clone()))
        {
            return false;
        }
        let Some(fragment) = plan.fragments().get(&node_ref.0) else {
            return false;
        };
        let Some(node) = fragment.nodes().get(&node_ref.1) else {
            return false;
        };
        match &node.kind {
            NodeKind::Aggregate { group_by, calls } => {
                let Some(call) = trace_indexes.aggregate_sequence_call(
                    fragment.id(),
                    node.id,
                    calls,
                    sequence,
                    trace_budget,
                ) else {
                    return false;
                };
                if !aggregate_bindings_match(expected_binding, &call.binding)
                    || aggregate_outputs(group_by, call) != expected_values
                {
                    return false;
                }
                let call_ref = (fragment.id(), node.id, call.id);
                match call.binding.phase {
                    AggregatePhase::Partial { .. } => {
                        let shape = (
                            call.distinct,
                            call.order_by
                                .iter()
                                .map(|item| (item.direction, item.null_ordering))
                                .collect::<Vec<_>>(),
                        );
                        if shape != *partial_shape || !matched_partials.insert(call_ref) {
                            return false;
                        }
                    }
                    AggregatePhase::Intermediate { .. } => {
                        if !matched_intermediates.insert(call_ref) {
                            return false;
                        }
                        let Some(values) = aggregate_state_inputs(fragment, group_by, call) else {
                            return false;
                        };
                        let Some(input) = node.inputs.first().copied() else {
                            return false;
                        };
                        pending.push(((fragment.id(), input), values));
                    }
                    AggregatePhase::Single | AggregatePhase::Final { .. } => return false,
                }
            }
            NodeKind::ExchangeSource { edge, .. } => {
                let Some(edge) = plan.edges().get(edge) else {
                    return false;
                };
                if edge.kind != crate::EdgeKind::Stream
                    || edge.destination.fragment != node_ref.0
                    || edge.destination.node != node_ref.1
                {
                    return false;
                }
                let Some(values) = trace_indexes.map_edge_values(
                    edge.id,
                    &edge.destination.receive_mapping,
                    &expected_values,
                    false,
                    trace_budget,
                ) else {
                    return false;
                };
                let Some(source) = plan.fragments().get(&edge.source.fragment) else {
                    return false;
                };
                pending.push(((source.id(), source.root()), values));
            }
            NodeKind::Project { expressions } => {
                let Some(input) = node.inputs.first().copied() else {
                    return false;
                };
                let Some(child) = fragment.nodes().get(&input) else {
                    return false;
                };
                let Some(values) = trace_indexes.map_project_values(
                    fragment,
                    node,
                    child,
                    expressions,
                    &expected_values,
                    trace_budget,
                ) else {
                    return false;
                };
                pending.push(((fragment.id(), input), values));
            }
            NodeKind::SetOp {
                kind: crate::SetOperationKind::UnionAll,
                input_mappings,
            } => {
                if node.inputs.len() != input_mappings.len() || node.inputs.is_empty() {
                    return false;
                }
                for (input_ordinal, (input, mapping)) in
                    node.inputs.iter().zip(input_mappings).enumerate()
                {
                    let Some(mapped) = trace_indexes.map_union_values(
                        fragment.id(),
                        node.id,
                        input_ordinal,
                        &node.output.columns,
                        mapping,
                        &expected_values,
                        false,
                        trace_budget,
                    ) else {
                        return false;
                    };
                    pending.push(((fragment.id(), *input), mapped));
                }
            }
            _ => return false,
        }
    }
    true
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PartitionSpaceDefinition {
    Hash(crate::HashPartitionScheme),
    Bucket(crate::BucketPartitionScheme),
}

fn validate_partition_identities(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    let mut spaces = BTreeMap::new();
    let mut counts = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            for (ordinal, properties) in node.required_inputs.iter().enumerate() {
                register_partition_identity(
                    &properties.distribution,
                    &format!(
                        "fragments[{}].nodes[{}].required_inputs[{ordinal}]",
                        fragment.id().get(),
                        node.id.get()
                    ),
                    &mut spaces,
                    &mut counts,
                    errors,
                );
            }
            register_partition_identity(
                &node.output_properties.distribution,
                &format!(
                    "fragments[{}].nodes[{}].output_properties",
                    fragment.id().get(),
                    node.id.get()
                ),
                &mut spaces,
                &mut counts,
                errors,
            );
            if let NodeKind::TableWriter { target } = &node.kind {
                register_partition_identity(
                    &target.required_distribution,
                    &format!(
                        "fragments[{}].nodes[{}].writer.required_distribution",
                        fragment.id().get(),
                        node.id.get()
                    ),
                    &mut spaces,
                    &mut counts,
                    errors,
                );
            }
        }
    }
    for edge in plan.edges().values() {
        register_partition_identity(
            &edge.partitioning.source,
            &format!("edges[{}].partitioning.source", edge.id.get()),
            &mut spaces,
            &mut counts,
            errors,
        );
        register_partition_identity(
            &edge.partitioning.destination,
            &format!("edges[{}].partitioning.destination", edge.id.get()),
            &mut spaces,
            &mut counts,
            errors,
        );
    }
}

fn register_partition_identity(
    distribution: &Distribution,
    path: &str,
    spaces: &mut BTreeMap<novarocks_type_contract::PartitionSpaceId, PartitionSpaceDefinition>,
    counts: &mut BTreeMap<
        novarocks_type_contract::PartitionCountParameterId,
        crate::PartitionCountDomain,
    >,
    errors: &mut ValidationErrorCollector,
) {
    let (space, definition) = match distribution {
        Distribution::Hash { scheme, .. } => {
            if counts
                .insert(scheme.count.id, scheme.count.admissible)
                .is_some_and(|existing| existing != scheme.count.admissible)
            {
                errors.push(ValidationError::new(
                    path,
                    "partition-count parameter identity has conflicting admissible domains",
                ));
            }
            (scheme.space, PartitionSpaceDefinition::Hash(scheme.clone()))
        }
        Distribution::BucketShuffle { scheme, .. } => (
            scheme.space,
            PartitionSpaceDefinition::Bucket(scheme.clone()),
        ),
        Distribution::Unconstrained
        | Distribution::Singleton
        | Distribution::RoundRobin
        | Distribution::Broadcast => return,
    };
    if spaces
        .insert(space, definition.clone())
        .is_some_and(|existing| existing != definition)
    {
        errors.push(ValidationError::new(
            path,
            "partition-space identity has conflicting definitions",
        ));
    }
}

fn validate_topn_reductions(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    type NodeRef = (FragmentId, NodeId);

    let mut partials: BTreeMap<crate::TopNSequenceId, BTreeSet<NodeRef>> = BTreeMap::new();
    let mut finals: BTreeMap<crate::TopNSequenceId, Vec<NodeRef>> = BTreeMap::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::TopN { phase, .. } = &node.kind {
                match *phase {
                    crate::TopNPhase::Single => {}
                    crate::TopNPhase::Partial { sequence } => {
                        partials
                            .entry(sequence)
                            .or_default()
                            .insert((fragment.id(), node.id));
                    }
                    crate::TopNPhase::Final { sequence } => {
                        finals
                            .entry(sequence)
                            .or_default()
                            .push((fragment.id(), node.id));
                    }
                }
            }
        }
    }

    let sequences = partials
        .keys()
        .chain(finals.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut trace_budget = SemanticTraceWorkBudget::new();
    let mut trace_indexes = SemanticTraceIndexes::default();
    for sequence in sequences {
        let sequence_path = format!("topn_sequences[{}]", sequence.get());
        let expected_partials = partials.get(&sequence).cloned().unwrap_or_default();
        let sequence_finals = finals.get(&sequence).map(Vec::as_slice).unwrap_or_default();
        if sequence_finals.len() != 1 {
            errors.push(ValidationError::new(
                &sequence_path,
                "TopN sequence must have exactly one final node",
            ));
            continue;
        }
        if expected_partials.is_empty() {
            errors.push(ValidationError::new(
                &sequence_path,
                "TopN sequence final has no partial producer",
            ));
            continue;
        }
        let (final_fragment_id, final_node_id) = sequence_finals[0];
        let Some(final_fragment) = plan.fragments().get(&final_fragment_id) else {
            continue;
        };
        let Some(final_node) = final_fragment.nodes().get(&final_node_id) else {
            continue;
        };
        let NodeKind::TopN {
            order_by,
            limit,
            offset,
            phase: crate::TopNPhase::Final { .. },
        } = &final_node.kind
        else {
            continue;
        };
        let Some(required_partial_limit) = limit.checked_add(*offset) else {
            continue;
        };
        let Some(expected_ordering) = derive_ordering(final_fragment, &[], order_by) else {
            continue;
        };
        let Some(input) = final_node.inputs.first().copied() else {
            continue;
        };
        let mut matched = BTreeSet::new();
        let all_paths_match = trace_topn_reduction_inputs(
            plan,
            sequence,
            (final_fragment_id, input),
            expected_ordering,
            required_partial_limit,
            &mut matched,
            &mut trace_budget,
            &mut trace_indexes,
        );
        if !all_paths_match || matched != expected_partials {
            errors.push(ValidationError::new(
                &sequence_path,
                "TopN partial paths do not reduce exactly into their matching final",
            ));
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn trace_topn_reduction_inputs(
    plan: &PhysicalPlan,
    sequence: crate::TopNSequenceId,
    start: (FragmentId, NodeId),
    initial_ordering: Vec<crate::OrderingKey>,
    required_partial_limit: u64,
    matched: &mut BTreeSet<(FragmentId, NodeId)>,
    trace_budget: &mut SemanticTraceWorkBudget,
    trace_indexes: &mut SemanticTraceIndexes,
) -> bool {
    let mut pending = vec![(start, initial_ordering)];
    let mut visited = BTreeSet::new();
    while let Some((node_ref, expected_ordering)) = pending.pop() {
        if !trace_budget.charge(expected_ordering.len().saturating_add(1))
            || !visited.insert((node_ref.0, node_ref.1, expected_ordering.clone()))
        {
            return false;
        }
        let Some(fragment) = plan.fragments().get(&node_ref.0) else {
            return false;
        };
        let Some(node) = fragment.nodes().get(&node_ref.1) else {
            return false;
        };
        match &node.kind {
            NodeKind::TopN {
                limit,
                offset,
                phase:
                    crate::TopNPhase::Partial {
                        sequence: partial_sequence,
                    },
                ..
            } => {
                if *partial_sequence != sequence
                    || *offset != 0
                    || *limit != required_partial_limit
                    || node.output_properties.ordering.as_ref() != expected_ordering.as_slice()
                {
                    return false;
                }
                if !matched.insert(node_ref) {
                    return false;
                }
            }
            NodeKind::ExchangeSource { edge, .. } => {
                let Some(edge) = plan.edges().get(edge) else {
                    return false;
                };
                if edge.kind != crate::EdgeKind::Stream
                    || edge.destination.fragment != node_ref.0
                    || edge.destination.node != node_ref.1
                    || edge.partitioning.source != Distribution::Singleton
                    || edge.partitioning.destination != Distribution::Singleton
                {
                    return false;
                }
                let Some(mapped_values) = trace_indexes.map_edge_values(
                    edge.id,
                    &edge.destination.receive_mapping,
                    &expected_ordering
                        .iter()
                        .map(|key| key.value)
                        .collect::<Vec<_>>(),
                    true,
                    trace_budget,
                ) else {
                    return false;
                };
                let mapped = expected_ordering
                    .iter()
                    .zip(mapped_values)
                    .map(|(key, value)| crate::OrderingKey {
                        value,
                        direction: key.direction,
                        null_ordering: key.null_ordering,
                    })
                    .collect();
                let Some(source) = plan.fragments().get(&edge.source.fragment) else {
                    return false;
                };
                pending.push(((source.id(), source.root()), mapped));
            }
            NodeKind::Project { .. } => {
                let Some(input) = node.inputs.first().copied() else {
                    return false;
                };
                let Some(child) = fragment.nodes().get(&input) else {
                    return false;
                };
                if !trace_indexes.port_contains_all(
                    fragment.id(),
                    child,
                    expected_ordering.iter().map(|key| key.value),
                    expected_ordering.len(),
                    trace_budget,
                ) {
                    return false;
                }
                pending.push(((fragment.id(), input), expected_ordering));
            }
            NodeKind::SetOp {
                kind: crate::SetOperationKind::UnionAll,
                input_mappings,
            } => {
                if node.inputs.len() != input_mappings.len() || node.inputs.is_empty() {
                    return false;
                }
                let expected_values = expected_ordering
                    .iter()
                    .map(|key| key.value)
                    .collect::<Vec<_>>();
                for (input_ordinal, (input, mapping)) in
                    node.inputs.iter().zip(input_mappings).enumerate()
                {
                    let mapped_values = trace_indexes.map_union_values(
                        fragment.id(),
                        node.id,
                        input_ordinal,
                        &node.output.columns,
                        mapping,
                        &expected_values,
                        true,
                        trace_budget,
                    );
                    let Some(mapped_values) = mapped_values else {
                        return false;
                    };
                    let mapped = expected_ordering
                        .iter()
                        .zip(mapped_values)
                        .map(|(key, value)| crate::OrderingKey {
                            value,
                            direction: key.direction,
                            null_ordering: key.null_ordering,
                        })
                        .collect();
                    pending.push(((fragment.id(), *input), mapped));
                }
            }
            _ => return false,
        }
    }
    true
}

fn validate_fragment_graph(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    let mut indegree = plan
        .fragments()
        .keys()
        .copied()
        .map(|fragment| (fragment, 0_usize))
        .collect::<BTreeMap<_, _>>();
    let mut successors: BTreeMap<FragmentId, Vec<FragmentId>> = BTreeMap::new();
    for edge in plan.edges().values() {
        if indegree.contains_key(&edge.source.fragment)
            && let Some(degree) = indegree.get_mut(&edge.destination.fragment)
        {
            *degree += 1;
            successors
                .entry(edge.source.fragment)
                .or_default()
                .push(edge.destination.fragment);
        }
    }
    let mut ready = indegree
        .iter()
        .filter_map(|(fragment, degree)| (*degree == 0).then_some(*fragment))
        .collect::<Vec<_>>();
    let mut visited = 0_usize;
    while let Some(fragment) = ready.pop() {
        visited += 1;
        if let Some(next) = successors.get(&fragment) {
            for successor in next {
                if let Some(degree) = indegree.get_mut(successor) {
                    *degree -= 1;
                    if *degree == 0 {
                        ready.push(*successor);
                    }
                }
            }
        }
    }
    if visited != plan.fragments().len() {
        errors.push(ValidationError::new(
            "edges",
            "fragment exchange graph contains a cycle",
        ));
    }
}

fn validate_fragment_cuts_into(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    validate_runtime_filter_proof: bool,
    errors: &mut ValidationErrorCollector,
) {
    let path = format!("fragments[{}].cuts", fragment.id().get());
    let local_provenance = fragment_source_provenance(fragment, cuts);
    bounded_count(
        errors,
        &format!("{path}.inbound"),
        cuts.inbound.len(),
        MAX_PLAN_EDGES,
    );
    bounded_count(
        errors,
        &format!("{path}.outbound"),
        cuts.outbound.len(),
        MAX_PLAN_EDGES,
    );
    bounded_count(
        errors,
        &format!("{path}.artifact_refs"),
        cuts.artifact_refs.len(),
        MAX_PLAN_ARTIFACT_REFS,
    );
    bounded_count(
        errors,
        &format!("{path}.runtime_filters"),
        cuts.runtime_filters.len(),
        MAX_PLAN_RUNTIME_FILTERS,
    );
    let mut inbound_ids = BTreeSet::new();
    for cut in &cuts.inbound {
        bounded_count(
            errors,
            &format!("{path}.inbound.imports"),
            cut.imports.len(),
            MAX_FRAGMENT_VALUES,
        );
        bounded_count(
            errors,
            &format!("{path}.inbound.source_bindings"),
            cut.source_bindings.len(),
            MAX_PLAN_ARTIFACT_REFS,
        );
        for source in &cut.source_bindings {
            validate_read_reference(&source.source, &path, errors);
            if source.selection_digest == [0; 32] {
                errors.push(ValidationError::new(
                    &path,
                    "upstream source binding has a zero selection digest",
                ));
            }
        }
        if !inbound_ids.insert(cut.edge) {
            errors.push(ValidationError::new(&path, "duplicate inbound edge"));
        }
        if cut.source_fragment == fragment.id() {
            errors.push(ValidationError::new(
                &path,
                "inbound cut has an invalid peer identity",
            ));
        }
        match fragment.nodes().get(&cut.destination_node) {
            Some(node)
                if matches!(
                    &node.kind,
                    NodeKind::ExchangeSource { edge, imports }
                        if *edge == cut.edge
                            && imports.len() == cut.imports.len()
                            && imports.iter().zip(&cut.imports).all(
                                |((source, destination), cut)| {
                                    *source == cut.source.value && *destination == cut.destination
                                }
                            )
                ) =>
            {
                if node.output_properties.distribution != cut.partitioning.destination
                    || node.output_properties.row_multiplicity
                        != cut.partitioning.destination_multiplicity
                    || !node.output_properties.ordering.is_empty()
                {
                    errors.push(ValidationError::new(
                        &path,
                        "exchange source properties differ from its inbound cut",
                    ));
                }
            }
            Some(_) => errors.push(ValidationError::new(
                &path,
                "inbound cut does not match its exchange source node",
            )),
            None => errors.push(ValidationError::new(
                &path,
                "inbound cut destination node is not defined",
            )),
        }
        validate_distribution(
            fragment,
            &cut.partitioning.destination,
            "inbound_cut.destination_partitioning",
            errors,
        );
        validate_mapped_partitioning(
            &cut.partitioning,
            &cut.imports
                .iter()
                .map(|import| (import.source.value, import.destination))
                .collect::<Vec<_>>(),
            &path,
            errors,
        );
        for import in &cut.imports {
            match fragment.values().get(&import.destination) {
                Some(value)
                    if value.ty == import.source.ty
                        && import_origin_matches(
                            &value.origin,
                            cut.edge,
                            cut.kind,
                            cut.source_fragment,
                            import.source.value,
                        ) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "inbound cut type or destination origin is inconsistent",
                )),
                None => errors.push(ValidationError::new(
                    &path,
                    "inbound cut destination value is not defined",
                )),
            }
        }
        validate_inbound_change_stream_writer(fragment, cut, &path, errors);
        validate_inbound_writer_result_structure(fragment, cut, &path, errors);
    }
    let expected_inbound_list = fragment
        .nodes()
        .values()
        .filter_map(|node| match node.kind {
            NodeKind::ExchangeSource { edge, .. } => Some(edge),
            _ => None,
        })
        .collect::<Vec<_>>();
    let expected_inbound = expected_inbound_list
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if expected_inbound.len() != expected_inbound_list.len() {
        errors.push(ValidationError::new(
            &path,
            "more than one exchange source claims the same inbound edge",
        ));
    }
    if inbound_ids != expected_inbound {
        errors.push(ValidationError::new(
            &path,
            "inbound cuts differ from the fragment exchange sources",
        ));
    }
    let mut inbound_indexes = BTreeMap::new();
    for cut in &cuts.inbound {
        inbound_indexes.entry(cut.edge).or_insert_with(|| {
            (
                cut,
                ValueMappingIndex::from_pairs_iter(
                    cut.imports
                        .iter()
                        .map(|import| (import.source.value, import.destination)),
                ),
            )
        });
    }
    for value in fragment.values().values() {
        let found = match value.origin {
            ValueOrigin::ExchangeImport { edge, source_value } => {
                inbound_indexes.get(&edge).is_some_and(|(cut, imports)| {
                    cut.kind != crate::EdgeKind::CteMulticast
                        && imports.contains(source_value, value.id)
                })
            }
            ValueOrigin::CteImport {
                edge,
                producer_fragment,
                producer_value,
            } => inbound_indexes.get(&edge).is_some_and(|(cut, imports)| {
                cut.kind == crate::EdgeKind::CteMulticast
                    && cut.source_fragment == producer_fragment
                    && imports.contains(producer_value, value.id)
            }),
            _ => continue,
        };
        if !found {
            errors.push(ValidationError::new(
                &path,
                "cross-fragment import is absent from the inbound cuts",
            ));
        }
    }

    let root_values = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| ValuePortIndex::new(&root.output.columns));
    let root_multiplicity = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| root.output_properties.row_multiplicity);
    let mut outbound_ids = BTreeSet::new();
    for cut in &cuts.outbound {
        bounded_count(
            errors,
            &format!("{path}.outbound.projection"),
            cut.projection.len(),
            MAX_FRAGMENT_VALUES,
        );
        bounded_count(
            errors,
            &format!("{path}.outbound.source_bindings"),
            cut.source_bindings.len(),
            MAX_PLAN_ARTIFACT_REFS,
        );
        for source in &cut.source_bindings {
            validate_read_reference(&source.source, &path, errors);
            if source.selection_digest == [0; 32] {
                errors.push(ValidationError::new(
                    &path,
                    "outbound source binding has a zero selection digest",
                ));
            }
        }
        if cut.has_source_free_rows != local_provenance.has_source_free_rows
            || !same_source_bindings(&cut.source_bindings, &local_provenance.bindings)
        {
            errors.push(ValidationError::new(
                &path,
                "outbound source provenance differs from the fragment's exact inputs",
            ));
        }
        if !outbound_ids.insert(cut.edge) {
            errors.push(ValidationError::new(&path, "duplicate outbound edge"));
        }
        if cut.destination_fragment == fragment.id() {
            errors.push(ValidationError::new(
                &path,
                "outbound cut has an invalid peer",
            ));
        }
        for projected in &cut.projection {
            match fragment.values().get(&projected.value) {
                Some(value) if value.ty != projected.ty => errors.push(ValidationError::new(
                    &path,
                    "outbound cut type differs from its source value",
                )),
                Some(_) => {}
                None => errors.push(ValidationError::new(
                    &path,
                    "outbound cut source value is not defined",
                )),
            }
        }
        if cut.destination_imports.len() != cut.projection.len()
            || cut
                .projection
                .iter()
                .zip(&cut.destination_imports)
                .any(|(projected, import)| projected != &import.source)
        {
            errors.push(ValidationError::new(
                &path,
                "outbound cut projection differs from its destination import mapping",
            ));
        }
        validate_mapped_partitioning(
            &cut.partitioning,
            &cut.destination_imports
                .iter()
                .map(|import| (import.source.value, import.destination))
                .collect::<Vec<_>>(),
            &path,
            errors,
        );
        if root_values.as_ref().is_some_and(|root_values| {
            cut.projection
                .iter()
                .any(|projected| !root_values.contains(&projected.value))
        }) {
            errors.push(ValidationError::new(
                &path,
                "outbound cut projects a value absent from the fragment root output",
            ));
        }
        if root_multiplicity
            .is_some_and(|multiplicity| multiplicity != cut.partitioning.source_multiplicity)
        {
            errors.push(ValidationError::new(
                &path,
                "outbound cut row multiplicity differs from the fragment root",
            ));
        }
        validate_outbound_writer_result(fragment, cut, &path, errors);
        validate_distribution(
            fragment,
            &cut.partitioning.source,
            "outbound_cut.source_partitioning",
            errors,
        );
        if root_values.as_ref().is_some_and(|root_values| {
            distribution_values(&cut.partitioning.source)
                .iter()
                .any(|value| !root_values.contains(value))
        }) {
            errors.push(ValidationError::new(
                &path,
                "outbound partition key is absent from the fragment root output",
            ));
        }
    }
    let sink_edges = match fragment.sink() {
        FragmentSink::Stream { edge } => vec![*edge],
        FragmentSink::Multicast { edges } => edges.to_vec(),
        FragmentSink::Router { routes, .. } => routes.iter().map(|route| route.edge).collect(),
        FragmentSink::Result | FragmentSink::SealedArtifact(_) | FragmentSink::Noop => Vec::new(),
    };
    let sink_edge_ids = sink_edges.iter().copied().collect::<BTreeSet<_>>();
    if sink_edge_ids.len() != sink_edges.len() {
        errors.push(ValidationError::new(
            &path,
            "fragment sink destinations contain duplicate edge occurrences",
        ));
    }
    if sink_edge_ids != outbound_ids {
        errors.push(ValidationError::new(
            &path,
            "outbound cuts differ from the fragment sink destinations",
        ));
    }
    let outbound_by_edge = cuts
        .outbound
        .iter()
        .map(|cut| (cut.edge, cut))
        .collect::<BTreeMap<_, _>>();
    if let FragmentSink::Router { routes, .. } = fragment.sink() {
        for route in routes {
            let cut = outbound_by_edge.get(&route.edge).copied();
            if cut.is_none_or(|cut| {
                !cut.projection
                    .iter()
                    .map(|value| value.value)
                    .eq(route.input_mapping.iter().map(|(_, value)| *value))
            }) {
                errors.push(ValidationError::new(
                    &path,
                    "router edge projection differs from its exact route input sequence",
                ));
            }
            if let Some(cut) = cut {
                validate_router_partitioning(route, &cut.partitioning.source, &path, errors);
                let proof_matches = cut.change_stream_writer.as_ref().is_some_and(|proof| {
                    proof.route_id == route.route_id
                        && proof.write_target_ordinal == route.write_target_ordinal
                        && proof.fields.len() == route.input_mapping.len()
                        && proof.fields.len() == cut.destination_imports.len()
                        && proof
                            .fields
                            .iter()
                            .zip(&route.input_mapping)
                            .zip(&cut.destination_imports)
                            .all(|((proof, (token, source)), import)| {
                                proof.token == *token
                                    && proof.source == *source
                                    && proof.source == import.source.value
                                    && proof.destination == import.destination
                            })
                });
                if !proof_matches {
                    errors.push(ValidationError::new(
                        &path,
                        "router outbound cut lacks its exact destination writer proof",
                    ));
                }
            }
        }
    }
    for cut in &cuts.outbound {
        if cut.kind != crate::EdgeKind::ChangeStreamRouter && cut.change_stream_writer.is_some() {
            errors.push(ValidationError::new(
                &path,
                "non-router outbound cut carries a change-stream writer proof",
            ));
        }
    }
    validate_fragment_writer_results(fragment, cuts, &path, errors);
    if let FragmentSink::SealedArtifact(spec) = fragment.sink()
        && (local_provenance.has_source_free_rows
            || local_provenance.bindings.len() != 1
            || local_provenance
                .bindings
                .values()
                .any(|source| source != &spec.source))
    {
        errors.push(ValidationError::new(
            &path,
            "artifact inputs are not derived exclusively from the exact source binding",
        ));
    }
    validate_fragment_artifact_cuts(fragment, cuts, &path, errors);
    if validate_runtime_filter_proof {
        validate_fragment_runtime_filter_cuts(fragment, cuts, &path, errors);
    }
}

fn fragment_source_provenance(fragment: &Fragment, cuts: &FragmentCuts) -> SourceProvenance {
    let mut provenance = SourceProvenance::default();
    for node in fragment.nodes().values() {
        match &node.kind {
            NodeKind::Scan { relation, .. } => {
                provenance.bindings.insert(relation.source_binding());
            }
            NodeKind::Values { rows } if !rows.is_empty() => {
                provenance.has_source_free_rows = true;
            }
            NodeKind::GenerateSeries { .. } => {
                provenance.has_source_free_rows = true;
            }
            NodeKind::TableFunction { .. } if node.inputs.is_empty() => {
                provenance.has_source_free_rows = true;
            }
            _ => {}
        }
    }
    for cut in &cuts.inbound {
        for binding in &cut.source_bindings {
            provenance.bindings.insert(binding.clone());
        }
        provenance.has_source_free_rows |= cut.has_source_free_rows;
    }
    provenance
}

fn same_source_bindings(left: &[ArtifactSourceBinding], right: &SourceBindingIndex) -> bool {
    let mut left_index = SourceBindingIndex::default();
    for binding in left {
        left_index.insert(binding.clone());
    }
    &left_index == right
}

fn validate_inbound_change_stream_writer(
    fragment: &Fragment,
    cut: &InboundFragmentCut,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if cut.kind != crate::EdgeKind::ChangeStreamRouter {
        if cut.change_stream_writer.is_some() {
            errors.push(ValidationError::new(
                path,
                "non-router inbound cut carries a change-stream writer proof",
            ));
        }
        return;
    }
    let Some(proof) = &cut.change_stream_writer else {
        errors.push(ValidationError::new(
            path,
            "router inbound cut lacks its destination writer proof",
        ));
        return;
    };
    let writer = fragment.nodes().get(&fragment.root());
    let target = writer.and_then(|writer| match &writer.kind {
        NodeKind::TableWriter { target } if writer.inputs.as_ref() == [cut.destination_node] => {
            Some(target)
        }
        _ => None,
    });
    let Some(target) = target else {
        errors.push(ValidationError::new(
            path,
            "router inbound cut receiver is not the direct root table writer input",
        ));
        return;
    };
    if proof.route_id == crate::ConnectorWriteRouteId::from_bytes([0; 32])
        || proof.write_target_ordinal != target.write_target_ordinal
        || proof.fields.len() != cut.imports.len()
        || proof.fields.len() != target.target_fields.len()
        || !proof
            .fields
            .iter()
            .zip(&cut.imports)
            .zip(&target.target_fields)
            .all(|((proof, import), target)| {
                proof.source == import.source.value
                    && proof.destination == import.destination
                    && proof.token == target.token
                    && proof.destination == target.input
            })
    {
        errors.push(ValidationError::new(
            path,
            "router inbound cut proof differs from its exact table writer contract",
        ));
    }
}

fn validate_inbound_writer_result_structure(
    fragment: &Fragment,
    cut: &InboundFragmentCut,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if cut.kind != crate::EdgeKind::Stream {
        if cut.writer_result.is_some() {
            errors.push(ValidationError::new(
                path,
                "non-stream inbound cut carries a writer result proof",
            ));
        }
        return;
    }
    let Some(proof) = &cut.writer_result else {
        return;
    };
    let matches = proof.schema_revision == crate::WRITER_MULTIPLEX_SCHEMA_REVISION
        && proof.fields.len() == cut.imports.len()
        && proof
            .fields
            .iter()
            .zip(&cut.imports)
            .all(|(field, import)| {
                field.source == import.source.value
                    && field.destination == import.destination
                    && field.ty == import.source.ty
                    && fragment
                        .values()
                        .get(&field.destination)
                        .is_some_and(|value| value.ty == field.ty)
            });
    if !matches {
        errors.push(ValidationError::new(
            path,
            "inbound writer result proof differs from its stream import contract",
        ));
    }
}

fn validate_outbound_writer_result(
    fragment: &Fragment,
    cut: &OutboundFragmentCut,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if cut.kind != crate::EdgeKind::Stream {
        if cut.writer_result.is_some() {
            errors.push(ValidationError::new(
                path,
                "non-stream outbound cut carries a writer result proof",
            ));
        }
        return;
    }
    let root = fragment.nodes().get(&fragment.root());
    let target = root.and_then(|root| match &root.kind {
        NodeKind::TableWriter { target } => Some(target),
        _ => None,
    });
    match (target, &cut.writer_result) {
        (None, None) => {}
        (None, Some(_)) => errors.push(ValidationError::new(
            path,
            "non-writer stream carries a writer result proof",
        )),
        (Some(_), None) => errors.push(ValidationError::new(
            path,
            "table writer stream lacks its writer result proof",
        )),
        (Some(target), Some(proof)) => {
            let fields_match = proof.write_target_ordinal == target.write_target_ordinal
                && proof.schema_revision == target.output_schema.revision
                && proof.fields.len() == target.output_schema.fields.len()
                && proof.fields.len() == cut.destination_imports.len()
                && proof
                    .fields
                    .iter()
                    .zip(&target.output_schema.fields)
                    .zip(&cut.destination_imports)
                    .all(|((proof, field), import)| {
                        proof.source == field.value
                            && proof.destination == import.destination
                            && import.source.value == field.value
                            && proof.name == field.name
                            && proof.ty == field.ty
                            && proof.ty == import.source.ty
                            && proof.role == field.role
                    });
            if !fields_match {
                errors.push(ValidationError::new(
                    path,
                    "outbound writer result proof differs from its exact table writer schema",
                ));
            }
        }
    }
}

fn validate_fragment_writer_results(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let inbound_by_edge = cuts
        .inbound
        .iter()
        .map(|cut| (cut.edge, cut))
        .collect::<BTreeMap<_, _>>();
    let mut consumed_inbound = BTreeMap::<EdgeId, usize>::new();
    let mut consumed_writers = BTreeMap::<NodeId, usize>::new();
    if let Some(root) = fragment.nodes().get(&fragment.root())
        && matches!(root.kind, NodeKind::TableWriter { .. })
    {
        for cut in &cuts.outbound {
            if cut.writer_result.is_some() {
                *consumed_writers.entry(root.id).or_default() += 1;
            }
        }
    }
    for finish_node in fragment
        .nodes()
        .values()
        .filter(|node| matches!(node.kind, NodeKind::TableFinish(_)))
    {
        if finish_node.id != fragment.root() {
            errors.push(ValidationError::new(
                path,
                "table finish must be the root of its fragment",
            ));
        }
        let NodeKind::TableFinish(finish) = &finish_node.kind else {
            unreachable!();
        };
        let finish_values = finish
            .input_schema
            .fields
            .iter()
            .map(|field| field.value)
            .collect::<Box<[_]>>();
        let mut pending = finish_node
            .inputs
            .iter()
            .map(|input| (*input, finish_values.clone()))
            .collect::<Vec<_>>();
        let mut visited = BTreeSet::new();
        let mut ordinals = Vec::new();
        while let Some((node_id, expected_values)) = pending.pop() {
            if !visited.insert(node_id) {
                errors.push(ValidationError::new(
                    path,
                    "writer relation reaches table finish through more than one local path",
                ));
                continue;
            }
            let Some(node) = fragment.nodes().get(&node_id) else {
                continue;
            };
            match &node.kind {
                NodeKind::TableWriter { target } => {
                    *consumed_writers.entry(node.id).or_default() += 1;
                    ordinals.push(target.write_target_ordinal);
                    if !writer_schema_matches_finish_values(
                        &target.output_schema,
                        &finish.input_schema,
                        &expected_values,
                    ) {
                        errors.push(ValidationError::new(
                            path,
                            "local table writer fields do not map exactly to its table finish input roles",
                        ));
                    }
                }
                NodeKind::ExchangeSource { edge, .. } => {
                    let proof = inbound_by_edge
                        .get(edge)
                        .and_then(|cut| cut.writer_result.as_ref());
                    let Some(proof) = proof else {
                        errors.push(ValidationError::new(
                            path,
                            "table finish stream lacks an upstream writer result proof",
                        ));
                        continue;
                    };
                    *consumed_inbound.entry(*edge).or_default() += 1;
                    ordinals.push(proof.write_target_ordinal);
                    if !writer_result_proof_matches_finish(
                        proof,
                        &finish.input_schema,
                        &expected_values,
                    ) {
                        errors.push(ValidationError::new(
                            path,
                            "upstream writer result fields do not map exactly to its table finish input roles",
                        ));
                    }
                }
                NodeKind::SetOp {
                    kind: crate::SetOperationKind::UnionAll,
                    input_mappings,
                } => {
                    if node.output.columns.as_ref() != expected_values.as_ref()
                        || input_mappings.len() != node.inputs.len()
                        || input_mappings
                            .iter()
                            .any(|mapping| mapping.len() != expected_values.len())
                    {
                        errors.push(ValidationError::new(
                            path,
                            "writer UnionAll does not preserve the exact finish field occurrences",
                        ));
                        continue;
                    }
                    pending.extend(
                        node.inputs
                            .iter()
                            .copied()
                            .zip(input_mappings.iter().cloned()),
                    );
                }
                _ => errors.push(ValidationError::new(
                    path,
                    "table finish input contains a non-preserving writer relation node",
                )),
            }
        }
        ordinals.sort_unstable();
        if ordinals.as_slice() != finish.expected_target_ordinals.as_ref() {
            errors.push(ValidationError::new(
                path,
                "table finish expected targets differ from its fragment cut writer proofs",
            ));
        }
    }
    for cut in &cuts.inbound {
        if cut.writer_result.is_some() && consumed_inbound.get(&cut.edge).copied() != Some(1) {
            errors.push(ValidationError::new(
                path,
                "inbound writer result proof must feed exactly one local table finish",
            ));
        }
    }
    for writer in fragment
        .nodes()
        .values()
        .filter(|node| matches!(node.kind, NodeKind::TableWriter { .. }))
    {
        if consumed_writers.get(&writer.id).copied() != Some(1) {
            errors.push(ValidationError::new(
                path,
                "table writer must feed exactly one local finish or writer result stream",
            ));
        }
    }
}

fn writer_result_proof_matches_finish(
    proof: &crate::WriterResultCut,
    finish: &crate::WriterRelationSchema,
    expected_values: &[ValueId],
) -> bool {
    proof.schema_revision == finish.revision
        && proof.fields.len() == finish.fields.len()
        && proof
            .fields
            .iter()
            .zip(&finish.fields)
            .zip(expected_values)
            .all(|((proof, finish), expected)| {
                proof.destination == *expected
                    && proof.name == finish.name
                    && proof.ty == finish.ty
                    && proof.role == finish.role
            })
}

fn writer_schema_matches_finish_values(
    writer: &crate::WriterRelationSchema,
    finish: &crate::WriterRelationSchema,
    expected_values: &[ValueId],
) -> bool {
    writer_schema_shapes_match(writer, finish)
        && writer.fields.len() == expected_values.len()
        && writer
            .fields
            .iter()
            .zip(expected_values)
            .all(|(field, expected)| field.value == *expected)
}

fn validate_fragment_artifact_cuts(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let mut supplied = BTreeMap::new();
    for artifact in &cuts.artifact_refs {
        if supplied.insert(artifact.id, artifact).is_some() {
            errors.push(ValidationError::new(
                path,
                "duplicate artifact reference in fragment cuts",
            ));
        }
        validate_artifact_ref(artifact, errors);
    }
    let requirements = fragment
        .nodes()
        .values()
        .filter_map(|node| match &node.kind {
            NodeKind::Scan { relation, .. } => Some(relation.artifact_inputs()),
            _ => None,
        })
        .flatten()
        .collect::<Vec<_>>();
    let expected = requirements
        .iter()
        .map(|requirement| requirement.artifact)
        .collect::<BTreeSet<_>>();
    if supplied.keys().copied().collect::<BTreeSet<_>>() != expected {
        errors.push(ValidationError::new(
            path,
            "artifact references in fragment cuts differ from relation requirements",
        ));
    }
    for requirement in requirements {
        let Some(artifact) = supplied.get(&requirement.artifact) else {
            continue;
        };
        if artifact.kind != requirement.kind
            || artifact.format != requirement.format
            || artifact.schema != requirement.schema
            || artifact.source != requirement.source
            || artifact.coverage != requirement.required_coverage
        {
            errors.push(ValidationError::new(
                path,
                format!(
                    "artifact {} differs from the relation's exact input requirement",
                    requirement.artifact.get()
                ),
            ));
        }
    }
}

fn validate_fragment_runtime_filter_cuts(
    fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let mut lineage_indexes = RuntimeFilterLineageIndexes::default();
    let inbound_edges = cuts
        .inbound
        .iter()
        .map(|cut| cut.edge)
        .collect::<BTreeSet<_>>();
    let expected = fragment
        .runtime_filters()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if expected.len() != fragment.runtime_filters().len() {
        errors.push(ValidationError::new(
            path,
            "fragment has duplicate runtime-filter identities",
        ));
    }
    let supplied = cuts
        .runtime_filters
        .iter()
        .map(|filter| filter.id)
        .collect::<BTreeSet<_>>();
    if supplied.len() != cuts.runtime_filters.len() || supplied != expected {
        errors.push(ValidationError::new(
            path,
            "runtime filters in fragment cuts differ from fragment attachments",
        ));
    }
    validate_runtime_filter_proof_graph(fragment, cuts, path, errors);
    for filter in &cuts.runtime_filters {
        if !validate_runtime_filter_shape(filter, path, errors) {
            continue;
        }
        let witnesses = runtime_filter_witness_index(&filter.equality_witnesses);
        for witness in &filter.equality_witnesses {
            if witness.fragment == fragment.id() {
                validate_runtime_filter_equality_witness(
                    fragment,
                    witness,
                    &filter.domain,
                    path,
                    errors,
                );
            }
        }
        let mut local_endpoint_count = 0_usize;
        for producer in &filter.producers {
            if producer.endpoint.fragment == fragment.id() {
                local_endpoint_count += 1;
                validate_runtime_filter_endpoint_in_fragment(
                    fragment,
                    &producer.endpoint,
                    &filter.domain,
                    path,
                    errors,
                );
                validate_apply_point_in_fragment(
                    fragment,
                    &mut lineage_indexes,
                    &producer.endpoint,
                    producer.apply_point,
                    path,
                    errors,
                );
                validate_runtime_filter_producer_target(
                    fragment,
                    &witnesses,
                    producer,
                    &filter.domain,
                    filter.reduction,
                    path,
                    errors,
                );
                validate_runtime_filter_producer_progress(
                    fragment,
                    producer,
                    &inbound_edges,
                    &mut lineage_indexes,
                    path,
                    errors,
                );
            }
        }
        for consumer in &filter.consumers {
            if consumer.endpoint.fragment == fragment.id() {
                local_endpoint_count += 1;
                validate_runtime_filter_endpoint_in_fragment(
                    fragment,
                    &consumer.endpoint,
                    &filter.domain,
                    path,
                    errors,
                );
                validate_apply_point_in_fragment(
                    fragment,
                    &mut lineage_indexes,
                    &consumer.endpoint,
                    consumer.apply_point,
                    path,
                    errors,
                );
                validate_runtime_filter_consumer_semantics(
                    fragment,
                    &witnesses,
                    consumer,
                    &mut lineage_indexes,
                    path,
                    errors,
                );
            }
        }
        if local_endpoint_count == 0 {
            errors.push(ValidationError::new(
                path,
                "attached runtime filter has no endpoint in this fragment",
            ));
        }
    }
}

fn validate_runtime_filter_proof_graph(
    local_fragment: &Fragment,
    cuts: &FragmentCuts,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let mut lineage_indexes = RuntimeFilterLineageIndexes::default();
    let mut root_port_indexes = BTreeMap::new();
    let proof_fragments = cuts
        .runtime_filter_proof
        .fragments
        .iter()
        .map(|fragment| (fragment.id(), fragment.clone()))
        .collect::<BTreeMap<_, _>>();
    let proof_edges = cuts
        .runtime_filter_proof
        .edges
        .iter()
        .map(|edge| (edge.id, edge.clone()))
        .collect::<BTreeMap<_, _>>();
    let proof_filters = cuts
        .runtime_filter_proof
        .filters
        .iter()
        .map(|filter| (filter.id, filter.clone()))
        .collect::<BTreeMap<_, _>>();
    if proof_fragments.len() != cuts.runtime_filter_proof.fragments.len()
        || proof_edges.len() != cuts.runtime_filter_proof.edges.len()
        || proof_filters.len() != cuts.runtime_filter_proof.filters.len()
    {
        errors.push(ValidationError::new(
            path,
            "runtime-filter proof graph contains duplicate fragment, edge, or filter identities",
        ));
        return;
    }
    if cuts.runtime_filters.is_empty() {
        if !proof_fragments.is_empty() || !proof_edges.is_empty() || !proof_filters.is_empty() {
            errors.push(ValidationError::new(
                path,
                "runtime-filter proof graph is non-empty without an attached filter",
            ));
        }
        return;
    }

    let proof_plan = PhysicalPlan::from(crate::PhysicalPlanParts {
        version: crate::PlanVersionId::try_new([1; 16])
            .expect("the proof-only plan version is non-zero"),
        fragments: proof_fragments.clone(),
        edges: proof_edges.clone(),
        runtime_filters: proof_filters.clone(),
        result_port: None,
        artifact_refs: BTreeMap::new(),
        required: RequiredContracts::default(),
        annotations: Box::default(),
    });
    let attachments = runtime_filter_attachment_index(&proof_plan);
    let inbound_edges = runtime_filter_inbound_edge_index(&proof_plan);
    if cuts
        .runtime_filters
        .iter()
        .any(|filter| proof_filters.get(&filter.id) != Some(filter))
    {
        errors.push(ValidationError::new(
            path,
            "runtime-filter proof graph changes a locally attached filter contract",
        ));
        return;
    }
    let mut required_fragments = BTreeSet::new();
    let mut required_edges = BTreeSet::new();
    let mut build_dependency_cache = RuntimeFilterBuildDependencyCache::default();
    let mut proof_work_budget = SemanticTraceWorkBudget::new();
    let Some(required_filters) = extend_runtime_filter_proof_hull(
        &proof_plan,
        cuts.runtime_filters.iter().map(|filter| filter.id),
        &mut required_fragments,
        &mut required_edges,
        &mut build_dependency_cache,
        &mut proof_work_budget,
    ) else {
        errors.push(ValidationError::new(
            path,
            "runtime-filter proof graph omits a join-build execution dependency",
        ));
        return;
    };
    for edge in &required_edges {
        let Some(edge) = proof_edges.get(edge) else {
            continue;
        };
        required_fragments.extend([edge.source.fragment, edge.destination.fragment]);
    }
    if proof_fragments.keys().copied().collect::<BTreeSet<_>>() != required_fragments
        || proof_edges.keys().copied().collect::<BTreeSet<_>>() != required_edges
        || proof_filters.keys().copied().collect::<BTreeSet<_>>() != required_filters
        || proof_fragments.get(&local_fragment.id()) != Some(local_fragment)
    {
        errors.push(ValidationError::new(
            path,
            "runtime-filter proof graph differs from the exact referenced subgraph",
        ));
        return;
    }

    for fragment in proof_fragments.values() {
        validate_fragment_into(fragment, errors);
    }
    for edge in proof_plan.edges().values() {
        validate_edge(&proof_plan, edge, &mut root_port_indexes, errors);
    }
    validate_runtime_filter_proof_edge_source_sinks(&proof_plan, path, errors);
    for filter in proof_plan.runtime_filters().values() {
        if !validate_runtime_filter_shape(filter, path, errors) {
            continue;
        }
        let witnesses = runtime_filter_witness_index(&filter.equality_witnesses);
        for witness in &filter.equality_witnesses {
            match proof_plan.fragments().get(&witness.fragment) {
                Some(fragment) => validate_runtime_filter_equality_witness(
                    fragment,
                    witness,
                    &filter.domain,
                    path,
                    errors,
                ),
                None => errors.push(ValidationError::new(
                    path,
                    "runtime-filter proof graph omits an equality fragment",
                )),
            }
        }
        for producer in &filter.producers {
            validate_runtime_filter_attachment(
                &proof_plan,
                &attachments,
                filter.id,
                &producer.endpoint,
                path,
                errors,
            );
            if let Some(fragment) = proof_plan.fragments().get(&producer.endpoint.fragment) {
                validate_runtime_filter_endpoint_in_fragment(
                    fragment,
                    &producer.endpoint,
                    &filter.domain,
                    path,
                    errors,
                );
                validate_apply_point_in_fragment(
                    fragment,
                    &mut lineage_indexes,
                    &producer.endpoint,
                    producer.apply_point,
                    path,
                    errors,
                );
                validate_runtime_filter_producer_target(
                    fragment,
                    &witnesses,
                    producer,
                    &filter.domain,
                    filter.reduction,
                    path,
                    errors,
                );
                validate_runtime_filter_producer_progress(
                    fragment,
                    producer,
                    inbound_edges
                        .get(&fragment.id())
                        .unwrap_or(&BTreeSet::new()),
                    &mut lineage_indexes,
                    path,
                    errors,
                );
            }
        }
        for consumer in &filter.consumers {
            validate_runtime_filter_attachment(
                &proof_plan,
                &attachments,
                filter.id,
                &consumer.endpoint,
                path,
                errors,
            );
            if let Some(fragment) = proof_plan.fragments().get(&consumer.endpoint.fragment) {
                validate_runtime_filter_endpoint_in_fragment(
                    fragment,
                    &consumer.endpoint,
                    &filter.domain,
                    path,
                    errors,
                );
                validate_apply_point_in_fragment(
                    fragment,
                    &mut lineage_indexes,
                    &consumer.endpoint,
                    consumer.apply_point,
                    path,
                    errors,
                );
                validate_runtime_filter_consumer_semantics(
                    fragment,
                    &witnesses,
                    consumer,
                    &mut lineage_indexes,
                    path,
                    errors,
                );
            }
            validate_runtime_filter_consumer_lineage(
                &proof_plan,
                &witnesses,
                consumer,
                path,
                &mut lineage_indexes,
                errors,
            );
        }
    }
    validate_runtime_filter_wait_graph(&proof_plan, path, errors);
}

fn bounded_count(errors: &mut ValidationErrorCollector, path: &str, actual: usize, maximum: usize) {
    if actual > maximum {
        errors.push(ValidationError::new(
            path,
            format!("contains {actual} items, exceeding {maximum}"),
        ));
    }
}

fn validate_fragment_into(fragment: &Fragment, errors: &mut ValidationErrorCollector) {
    let prefix = format!("fragments[{}]", fragment.id().get());
    let previous_errors = errors.len();
    validate_fragment_resources(fragment, errors);
    if errors.len() != previous_errors {
        return;
    }
    bounded_count(
        errors,
        &format!("{prefix}.nodes"),
        fragment.nodes().len(),
        MAX_FRAGMENT_NODES,
    );
    bounded_count(
        errors,
        &format!("{prefix}.values"),
        fragment.values().len(),
        MAX_FRAGMENT_VALUES,
    );
    bounded_count(
        errors,
        &format!("{prefix}.expressions"),
        fragment.expressions().len(),
        MAX_FRAGMENT_EXPRESSIONS,
    );
    if !fragment.nodes().contains_key(&fragment.root()) {
        errors.push(ValidationError::new(
            format!("{prefix}.root"),
            format!("node {} is not defined", fragment.root().get()),
        ));
    }
    let dop = fragment.dop_domain();
    if dop.min == 0 || dop.min > dop.max || dop.max > MAX_PIPELINE_DOP {
        errors.push(ValidationError::new(
            format!("{prefix}.dop_domain"),
            "DOP bounds must be non-zero, ordered and bounded",
        ));
    }
    if dop.requires_power_of_two
        && dop
            .min
            .checked_next_power_of_two()
            .is_none_or(|first| first > dop.max)
    {
        errors.push(ValidationError::new(
            format!("{prefix}.dop_domain"),
            "power-of-two DOP domain has no admissible member",
        ));
    }
    let mut aggregate_calls = BTreeMap::new();
    for node in fragment.nodes().values() {
        let NodeKind::Aggregate { calls, .. } = &node.kind else {
            continue;
        };
        for call in calls {
            if aggregate_calls
                .insert(call.id, call.binding.phase)
                .is_some()
            {
                errors.push(ValidationError::new(
                    format!("{prefix}.aggregate_calls[{}]", call.id.get()),
                    "aggregate call identity must be unique within its fragment",
                ));
            }
            if errors.is_saturated() {
                errors.mark_truncated();
                return;
            }
        }
    }
    for value in fragment.values().values() {
        validate_value(fragment, value, &aggregate_calls, errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return;
        }
    }
    let indexes = FragmentValidationIndexes::new(fragment);
    let window_roots = fragment
        .nodes()
        .values()
        .filter_map(|node| match &node.kind {
            NodeKind::Window(spec) => Some(spec.expressions.iter().map(|item| item.expression)),
            _ => None,
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let operator_roots = fragment
        .nodes()
        .values()
        .flat_map(|node| {
            let mut roots = Vec::new();
            node.kind.expression_references(&mut roots);
            roots
        })
        .collect::<BTreeSet<_>>();
    let mut expression_parents: BTreeMap<ExprId, Vec<ExpressionParentReference>> = BTreeMap::new();
    for (_, parent) in fragment.expressions().iter() {
        let mut references = Vec::new();
        expression_parent_references(parent, &mut references);
        for (child, role) in references {
            expression_parents
                .entry(child)
                .or_default()
                .push(ExpressionParentReference {
                    parent: parent.id,
                    role,
                });
        }
    }
    for (_, expression) in fragment.expressions().iter() {
        validate_expression(
            fragment,
            expression,
            &indexes.visible_inputs,
            &window_roots,
            &operator_roots,
            expression_parents
                .get(&expression.id)
                .map(Vec::as_slice)
                .unwrap_or_default(),
            errors,
        );
        if errors.is_saturated() {
            errors.mark_truncated();
            return;
        }
    }
    validate_expression_acyclic(fragment, errors);
    validate_lambda_scope_acyclic(fragment, errors);
    validate_expression_reachability(fragment, errors);
    for node in fragment.nodes().values() {
        validate_node(fragment, node, &indexes, errors);
        if errors.is_saturated() {
            errors.mark_truncated();
            return;
        }
    }
    validate_node_graph(fragment, errors);
    validate_fragment_sink(fragment, errors);
}

fn validate_fragment_sink(fragment: &Fragment, errors: &mut ValidationErrorCollector) {
    let path = format!("fragments[{}].sink", fragment.id().get());
    let root_output = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| root.output.columns.as_ref())
        .unwrap_or_default();
    let root_values = root_output.iter().copied().collect::<BTreeSet<_>>();
    let root_multiplicity = fragment
        .nodes()
        .get(&fragment.root())
        .map(|root| root.output_properties.row_multiplicity);
    let mut edge_ids = BTreeSet::new();
    match fragment.sink() {
        FragmentSink::Multicast { edges } => {
            if edges.is_empty() {
                errors.push(ValidationError::new(
                    &path,
                    "multi-destination sink has no edges",
                ));
            }
            for edge in edges {
                if !edge_ids.insert(*edge) {
                    errors.push(ValidationError::new(
                        &path,
                        "sink contains a duplicate edge destination",
                    ));
                }
            }
        }
        FragmentSink::Router { effect, routes } => {
            if root_multiplicity != Some(RowMultiplicity::SingleCopy) {
                errors.push(ValidationError::new(
                    &path,
                    "router requires single-copy row ownership",
                ));
            }
            require_value(fragment, *effect, &path, errors);
            if !root_values.contains(effect) {
                errors.push(ValidationError::new(
                    &path,
                    "router effect is absent from the fragment root output",
                ));
            }
            let change_events = change_event_source(fragment, *effect);
            if change_events.is_none() {
                errors.push(ValidationError::new(
                    &path,
                    "router effect is not the exact output of a change-event expansion",
                ));
            }
            if routes.is_empty() {
                errors.push(ValidationError::new(
                    &path,
                    "multi-destination sink has no edges",
                ));
            }
            let mut route_ids = BTreeSet::new();
            for (ordinal, route) in routes.iter().enumerate() {
                if route.route_id == crate::ConnectorWriteRouteId::from_bytes([0; 32])
                    || !route_ids.insert(route.route_id)
                    || usize::try_from(route.write_target_ordinal.get()).ok() != Some(ordinal)
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router routes require unique identities and dense target ordinals in route order",
                    ));
                }
                let mut effects = BTreeSet::new();
                if route.accepted_effects.is_empty()
                    || route
                        .accepted_effects
                        .iter()
                        .any(|effect| !effects.insert(*effect))
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router route requires unique accepted effects",
                    ));
                }
                let mut tokens = BTreeSet::new();
                let mut input_values = BTreeSet::new();
                if route.input_mapping.is_empty()
                    || route.input_mapping.iter().any(|(token, value)| {
                        !tokens.insert(*token) || !root_values.contains(value)
                    })
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router route requires unique input tokens and root-output values",
                    ));
                }
                input_values.extend(route.input_mapping.iter().map(|(_, value)| *value));
                if route.input_mapping.iter().any(|(_, value)| value == effect) {
                    errors.push(ValidationError::new(
                        &path,
                        "router data input cannot contain its generated effect value",
                    ));
                }
                let mut partition_values = BTreeSet::new();
                if route
                    .partition_by
                    .iter()
                    .any(|value| !partition_values.insert(*value) || !input_values.contains(value))
                {
                    errors.push(ValidationError::new(
                        &path,
                        "router partition values must be unique route inputs",
                    ));
                }
                if !edge_ids.insert(route.edge) {
                    errors.push(ValidationError::new(
                        &path,
                        "sink contains a duplicate edge destination",
                    ));
                }
            }
            let covered_effects = routes
                .iter()
                .flat_map(|route| route.accepted_effects.iter().copied())
                .collect::<BTreeSet<_>>();
            if change_events.is_some_and(|events| {
                events
                    .iter()
                    .any(|event| !covered_effects.contains(&event.effect))
            }) {
                errors.push(ValidationError::new(
                    &path,
                    "router routes do not cover every emitted change-event effect",
                ));
            }
        }
        FragmentSink::SealedArtifact(spec) => validate_artifact_sink(fragment, spec, errors),
        FragmentSink::Result => {
            if root_multiplicity != Some(RowMultiplicity::SingleCopy) {
                errors.push(ValidationError::new(
                    &path,
                    "result sink requires single-copy row ownership",
                ));
            }
        }
        FragmentSink::Stream { .. } | FragmentSink::Noop => {}
    }
}

fn change_event_source(fragment: &Fragment, effect: ValueId) -> Option<&[crate::ChangeEventSpec]> {
    let value = fragment.values().get(&effect)?;
    if value.ty.data_type != DataType::Int8 || value.ty.nullable {
        return None;
    }
    let ValueOrigin::NodeOutput {
        node,
        output_ordinal,
    } = value.origin
    else {
        return None;
    };
    let source = fragment.nodes().get(&node)?;
    let ordinal = usize::try_from(output_ordinal).ok()?;
    if source.output.columns.get(ordinal) != Some(&effect) {
        return None;
    }
    match &source.kind {
        NodeKind::ChangeEventExpand {
            events,
            effect_output,
        } if *effect_output == effect => Some(events),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExpressionParentRole {
    BoundLambdaArgument,
    Other,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExpressionParentReference {
    parent: ExprId,
    role: ExpressionParentRole,
}

fn expression_parent_references(
    parent: &crate::ExprNode,
    output: &mut Vec<(ExprId, ExpressionParentRole)>,
) {
    match &parent.kind {
        ExprKind::FunctionCall { function, args } => {
            for (ordinal, argument) in args.iter().copied().enumerate() {
                let role = if matches!(
                    function.argument_types.get(ordinal),
                    Some(crate::FunctionArgumentType::Lambda { .. })
                ) {
                    ExpressionParentRole::BoundLambdaArgument
                } else {
                    ExpressionParentRole::Other
                };
                output.push((argument, role));
            }
        }
        ExprKind::WindowCall {
            function,
            args,
            function_order_by,
            frame,
            ..
        } => {
            for (ordinal, argument) in args.iter().copied().enumerate() {
                let role = if matches!(
                    function.argument_types.get(ordinal),
                    Some(crate::FunctionArgumentType::Lambda { .. })
                ) {
                    ExpressionParentRole::BoundLambdaArgument
                } else {
                    ExpressionParentRole::Other
                };
                output.push((argument, role));
            }
            output.extend(
                function_order_by
                    .iter()
                    .map(|item| (item.expr, ExpressionParentRole::Other)),
            );
            if let Some(frame) = frame {
                for bound in [&frame.start, &frame.end] {
                    if let crate::WindowBound::Preceding(expression)
                    | crate::WindowBound::Following(expression) = bound
                    {
                        output.push((*expression, ExpressionParentRole::Other));
                    }
                }
            }
        }
        _ => {
            let mut references = Vec::new();
            parent.kind.expression_references(&mut references);
            output.extend(
                references
                    .into_iter()
                    .map(|reference| (reference, ExpressionParentRole::Other)),
            );
        }
    }
}

fn validate_distribution(
    fragment: &Fragment,
    distribution: &Distribution,
    label: &str,
    errors: &mut ValidationErrorCollector,
) {
    let path = format!("fragments[{}].{label}", fragment.id().get());
    let (keys, algorithm) = match distribution {
        Distribution::Hash { keys, scheme } => {
            if scheme.definition.algorithm
                != novarocks_type_contract::PartitionHashAlgorithm::NativeExchangeV1
            {
                errors.push(ValidationError::new(
                    &path,
                    "ordinary hash partitioning uses the wrong hash algorithm",
                ));
            }
            validate_partition_count_domain(&scheme.count.admissible, &path, errors);
            (Some(keys), Some(scheme.definition.algorithm))
        }
        Distribution::BucketShuffle { keys, scheme } => {
            if scheme.bucket_count == 0 || scheme.bucket_count > MAX_PARTITION_COUNT {
                errors.push(ValidationError::new(
                    &path,
                    "bucket partition count must be within the supported bound",
                ));
            }
            if scheme.hash != novarocks_type_contract::PartitionHashAlgorithm::NativeBucketCrc32V1 {
                errors.push(ValidationError::new(
                    &path,
                    "bucket partitioning uses the wrong hash algorithm",
                ));
            }
            if scheme.layout != novarocks_type_contract::BucketLayoutAlgorithm::DenseZeroBasedV1
                || scheme.ordinal_domain.first_ordinal != 0
                || scheme.ordinal_domain.ordinal_count != scheme.bucket_count
                || scheme.ordinal_domain.evidence_digest == [0; 32]
            {
                errors.push(ValidationError::new(
                    &path,
                    "bucket partitioning lacks a complete dense ordinal-domain proof",
                ));
            }
            (Some(keys), Some(scheme.hash))
        }
        Distribution::Unconstrained
        | Distribution::Singleton
        | Distribution::RoundRobin
        | Distribution::Broadcast => (None, None),
    };
    if let Some(keys) = keys {
        if keys.is_empty() {
            errors.push(ValidationError::new(
                &path,
                "keyed distribution has no keys",
            ));
        }
        for value in keys.iter() {
            require_value(fragment, *value, &path, errors);
            if let (Some(algorithm), Some(value)) = (algorithm, fragment.values().get(value))
                && !algorithm.supports_partition_key(&value.ty.data_type)
            {
                errors.push(ValidationError::new(
                    &path,
                    "keyed distribution uses a data type outside its hash algorithm domain",
                ));
            }
        }
    }
}

fn validate_partition_count_domain(
    domain: &crate::PartitionCountDomain,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if domain.min == 0 || domain.min > domain.max || domain.max > MAX_PARTITION_COUNT {
        errors.push(ValidationError::new(
            path,
            "partition-count domain must be non-zero, ordered and bounded",
        ));
    }
    if domain.requires_power_of_two
        && domain
            .min
            .checked_next_power_of_two()
            .is_none_or(|first| first > domain.max)
    {
        errors.push(ValidationError::new(
            path,
            "power-of-two partition-count domain has no admissible member",
        ));
    }
}

fn validate_value(
    fragment: &Fragment,
    value: &ValueDef,
    aggregate_calls: &BTreeMap<crate::AggregateCallId, AggregatePhase>,
    errors: &mut ValidationErrorCollector,
) {
    let path = format!(
        "fragments[{}].values[{}]",
        fragment.id().get(),
        value.id.get()
    );
    match &value.origin {
        ValueOrigin::ProviderField { scan_node, field } => {
            match fragment.nodes().get(scan_node) {
                Some(node) if matches!(node.kind, NodeKind::Scan { .. }) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "provider field origin does not reference a scan node",
                )),
                None => errors.push(ValidationError::new(
                    &path,
                    format!("scan node {} is not defined", scan_node.get()),
                )),
            }
            validate_column_reference(None, field, &path, errors);
        }
        ValueOrigin::Expr { node, expr } => {
            require_node(fragment, *node, &path, errors);
            if let Some(expression) = fragment.expressions().get(*expr) {
                if expression.ty != value.ty {
                    errors.push(ValidationError::new(
                        &path,
                        "expression origin type differs from value type",
                    ));
                }
            } else {
                errors.push(ValidationError::new(
                    &path,
                    format!("expression {} is not defined", expr.get()),
                ));
            }
        }
        ValueOrigin::NullExtended { node, of } => {
            match fragment.nodes().get(node) {
                Some(owner)
                    if matches!(
                        owner.kind,
                        NodeKind::HashJoin { .. }
                            | NodeKind::NestLoopJoin { .. }
                            | NodeKind::Repeat { .. }
                    ) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "NULL-extended value owner cannot produce NULL extensions",
                )),
                None => require_node(fragment, *node, &path, errors),
            }
            if let Some(source) = fragment.values().get(of) {
                if source.ty.data_type != value.ty.data_type || !value.ty.nullable {
                    errors.push(ValidationError::new(
                        &path,
                        "null-extended value must preserve the data type and be nullable",
                    ));
                }
            } else {
                errors.push(ValidationError::new(
                    &path,
                    format!("source value {} is not defined", of.get()),
                ));
            }
        }
        ValueOrigin::AggregateState { call, phase } => match aggregate_calls.get(call) {
            Some(actual)
                if actual == phase
                    && matches!(
                        phase,
                        AggregatePhase::Partial { .. } | AggregatePhase::Intermediate { .. }
                    ) => {}
            Some(_) => errors.push(ValidationError::new(
                &path,
                "aggregate state origin differs from the call phase",
            )),
            None => errors.push(ValidationError::new(
                &path,
                format!("aggregate call {} is not defined", call.get()),
            )),
        },
        ValueOrigin::AggregateResult { call } => match aggregate_calls.get(call) {
            Some(AggregatePhase::Single | AggregatePhase::Final { .. }) => {}
            Some(_) => errors.push(ValidationError::new(
                &path,
                "aggregate result origin references an intermediate-state call",
            )),
            None => errors.push(ValidationError::new(
                &path,
                format!("aggregate call {} is not defined", call.get()),
            )),
        },
        ValueOrigin::NodeOutput {
            node,
            output_ordinal,
        } => match fragment.nodes().get(node) {
            Some(owner)
                if usize::try_from(*output_ordinal)
                    .ok()
                    .and_then(|ordinal| owner.output.columns.get(ordinal))
                    == Some(&value.id) => {}
            Some(_) => errors.push(ValidationError::new(
                &path,
                "node-output origin does not match the declared output ordinal",
            )),
            None => require_node(fragment, *node, &path, errors),
        },
        ValueOrigin::ExchangeImport { .. } | ValueOrigin::CteImport { .. } => {
            // Cross-fragment definitions are checked with the complete graph.
        }
        ValueOrigin::WriterDerived { writer_node, .. } => match fragment.nodes().get(writer_node) {
            Some(node)
                if matches!(
                    node.kind,
                    NodeKind::TableWriter { .. } | NodeKind::TableFinish(_)
                ) => {}
            Some(_) => errors.push(ValidationError::new(
                &path,
                "writer-derived value does not reference a writer node",
            )),
            None => errors.push(ValidationError::new(
                &path,
                format!("writer node {} is not defined", writer_node.get()),
            )),
        },
    }
}

fn validate_expression(
    fragment: &Fragment,
    expression: &crate::ExprNode,
    expression_scopes: &BTreeMap<NodeId, VisibleInputIndex>,
    window_roots: &BTreeSet<ExprId>,
    operator_roots: &BTreeSet<ExprId>,
    parents: &[ExpressionParentReference],
    errors: &mut ValidationErrorCollector,
) {
    let path = format!(
        "fragments[{}].expressions[{}]",
        fragment.id().get(),
        expression.id.get()
    );
    let owner = fragment.nodes().get(&expression.owner);
    if owner.is_none() {
        errors.push(ValidationError::new(
            &path,
            format!("owner node {} is not defined", expression.owner.get()),
        ));
    }
    if let Some(scope) = expression.lambda_scope {
        match fragment.expressions().get(scope) {
            Some(scope_node)
                if scope_node.owner == expression.owner
                    && matches!(scope_node.kind, ExprKind::Lambda { .. }) => {}
            _ => errors.push(ValidationError::new(
                &path,
                "expression lambda scope is not a lambda owned by the same physical node",
            )),
        }
    }
    let mut references = Vec::new();
    expression.kind.expression_references(&mut references);
    for reference in references {
        match fragment.expressions().get(reference) {
            Some(child) if child.owner != expression.owner => errors.push(ValidationError::new(
                &path,
                "expression dependency crosses a physical-node scope",
            )),
            Some(child)
                if !expression_reference_scope_allowed(fragment, expression, reference, child) =>
            {
                errors.push(ValidationError::new(
                    &path,
                    "expression dependency crosses a lambda lexical scope",
                ));
            }
            Some(_) => {}
            None => errors.push(ValidationError::new(
                &path,
                format!("expression {} is not defined", reference.get()),
            )),
        }
    }
    match &expression.kind {
        ExprKind::Value(value) => match fragment.values().get(value) {
            Some(definition) if definition.ty != expression.ty => {
                errors.push(ValidationError::new(
                    &path,
                    "value-reference type differs from the value definition",
                ))
            }
            Some(_) => {
                if owner.is_some()
                    && expression_scopes
                        .get(&expression.owner)
                        .is_some_and(|scope| !scope.contains(value))
                {
                    errors.push(ValidationError::new(
                        &path,
                        "value reference is outside its owner node input scope",
                    ));
                }
            }
            None => errors.push(ValidationError::new(
                &path,
                format!("value {} is not defined", value.get()),
            )),
        },
        ExprKind::Literal(literal) => validate_literal_type(literal, &expression.ty, &path, errors),
        ExprKind::LambdaParameter { lambda, ordinal } => {
            match fragment.expressions().get(*lambda) {
                Some(crate::ExprNode {
                    kind:
                        ExprKind::Lambda {
                            parameter_types, ..
                        },
                    ..
                }) if usize::try_from(*ordinal)
                    .ok()
                    .and_then(|ordinal| parameter_types.get(ordinal))
                    == Some(&expression.ty) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "lambda parameter does not reference a matching lambda owner",
                )),
                None => errors.push(ValidationError::new(
                    &path,
                    "lambda parameter owner is not defined",
                )),
            }
            if expression.lambda_scope != Some(*lambda) {
                errors.push(ValidationError::new(
                    &path,
                    "lambda parameter is not declared in its lambda's lexical scope",
                ));
            }
        }
        ExprKind::Lambda {
            parameter_types,
            body,
        } => {
            let invalid_parent = parents
                .iter()
                .find(|parent| parent.role != ExpressionParentRole::BoundLambdaArgument);
            if operator_roots.contains(&expression.id)
                || parents.is_empty()
                || invalid_parent.is_some()
            {
                let detail = invalid_parent.map_or_else(String::new, |parent| {
                    format!(
                        "; expression {} references it outside a bound lambda argument position",
                        parent.parent.get()
                    )
                });
                errors.push(ValidationError::new(
                    &path,
                    format!(
                        "lambda must appear only as an exact bound-function lambda argument{detail}"
                    ),
                ));
            }
            if parameter_types.is_empty() {
                errors.push(ValidationError::new(&path, "lambda has no parameters"));
            }
            if let Some(body) = fragment.expressions().get(*body)
                && body.ty != expression.ty
            {
                errors.push(ValidationError::new(
                    &path,
                    "lambda type differs from its body type",
                ));
            }
        }
        ExprKind::Unary { op, expr } => {
            if let Some(input) = fragment.expressions().get(*expr) {
                let valid = match op {
                    crate::UnaryOperator::Plus | crate::UnaryOperator::Minus => {
                        is_numeric(&input.ty.data_type) && input.ty == expression.ty
                    }
                    crate::UnaryOperator::Not => {
                        input.ty.data_type == DataType::Boolean
                            && expression.ty.data_type == DataType::Boolean
                            && expression.ty.nullable == input.ty.nullable
                    }
                    crate::UnaryOperator::BitwiseNot => {
                        is_integer(&input.ty.data_type) && input.ty == expression.ty
                    }
                };
                if !valid {
                    errors.push(ValidationError::new(
                        &path,
                        "unary expression types are inconsistent with its operator",
                    ));
                }
            }
        }
        ExprKind::Binary { left, op, right } => {
            if let (Some(left), Some(right)) = (
                fragment.expressions().get(*left),
                fragment.expressions().get(*right),
            ) {
                validate_binary_types(left, *op, right, expression, &path, errors);
            }
        }
        ExprKind::FunctionCall { function, args } => {
            if function.kind != FunctionKind::Scalar {
                errors.push(ValidationError::new(
                    &path,
                    "scalar call has non-scalar binding",
                ));
            }
            validate_function_call(fragment, expression, function, args, &path, errors);
        }
        ExprKind::WindowCall {
            function,
            distinct,
            args,
            function_order_by,
            frame,
            aggregate_binding,
            ..
        } => {
            if !window_roots.contains(&expression.id)
                || !parents.is_empty()
                || expression.lambda_scope.is_some()
            {
                let detail = parents.first().map_or_else(String::new, |parent| {
                    format!("; expression {} also references it", parent.parent.get())
                });
                errors.push(ValidationError::new(
                    &path,
                    format!(
                        "window call must be a top-level expression of its owning Window node{detail}"
                    ),
                ));
            }
            match function.kind {
                FunctionKind::Window => {
                    if aggregate_binding.is_some() || *distinct || !function_order_by.is_empty() {
                        errors.push(ValidationError::new(
                            &path,
                            "window function carries aggregate-only semantics",
                        ));
                    }
                    validate_function_call(fragment, expression, function, args, &path, errors);
                }
                FunctionKind::Aggregate => match aggregate_binding {
                    Some(binding)
                        if binding.function == *function
                            && binding.phase == AggregatePhase::Single =>
                    {
                        validate_aggregate_arguments(
                            fragment,
                            binding,
                            args,
                            function_order_by,
                            &path,
                            errors,
                        );
                        if expression.ty != function.result_type {
                            errors.push(ValidationError::new(
                                &path,
                                "aggregate window type differs from its bound result type",
                            ));
                        }
                    }
                    _ => errors.push(ValidationError::new(
                        &path,
                        "aggregate window requires the exact single-phase aggregate binding",
                    )),
                },
                _ => errors.push(ValidationError::new(
                    &path,
                    "window call has invalid function kind",
                )),
            }
            if let Some(frame) = frame {
                validate_window_frame(fragment, frame, &path, errors);
            }
        }
        ExprKind::Cast { expr, target } => {
            if &expression.ty.data_type != target
                || fragment
                    .expressions()
                    .get(*expr)
                    .is_some_and(|input| input.ty.nullable != expression.ty.nullable)
            {
                errors.push(ValidationError::new(
                    &path,
                    "cast result type or nullability differs from its explicit input and target",
                ));
            }
        }
        ExprKind::IsNull { .. } | ExprKind::IsTruthValue { .. } => {
            require_non_nullable_boolean(&expression.ty, &path, errors);
        }
        ExprKind::InList { expr, list, .. } => {
            require_boolean_result(&expression.ty, &path, errors);
            if let Some(input) = fragment.expressions().get(*expr) {
                let nullable = input.ty.nullable
                    || list.iter().any(|candidate| {
                        fragment
                            .expressions()
                            .get(*candidate)
                            .is_some_and(|candidate| candidate.ty.nullable)
                    });
                if expression.ty.nullable != nullable {
                    errors.push(ValidationError::new(
                        &path,
                        "IN-list result nullability differs from its operands",
                    ));
                }
                for candidate in list {
                    if fragment
                        .expressions()
                        .get(*candidate)
                        .is_some_and(|candidate| candidate.ty.data_type != input.ty.data_type)
                    {
                        errors.push(ValidationError::new(
                            &path,
                            "IN-list candidate type differs from its input",
                        ));
                    }
                }
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            require_boolean_result(&expression.ty, &path, errors);
            if let Some(input) = fragment.expressions().get(*expr) {
                let nullable = [*low, *high].into_iter().any(|bound| {
                    fragment
                        .expressions()
                        .get(bound)
                        .is_some_and(|bound| bound.ty.nullable)
                }) || input.ty.nullable;
                if expression.ty.nullable != nullable {
                    errors.push(ValidationError::new(
                        &path,
                        "BETWEEN result nullability differs from its operands",
                    ));
                }
                for bound in [*low, *high] {
                    if fragment
                        .expressions()
                        .get(bound)
                        .is_some_and(|bound| bound.ty.data_type != input.ty.data_type)
                    {
                        errors.push(ValidationError::new(
                            &path,
                            "BETWEEN bound type differs from its input",
                        ));
                    }
                }
            }
        }
        ExprKind::Like { expr, pattern, .. } => {
            require_boolean_result(&expression.ty, &path, errors);
            let nullable = [*expr, *pattern].into_iter().any(|id| {
                fragment
                    .expressions()
                    .get(id)
                    .is_some_and(|value| value.ty.nullable)
            });
            if expression.ty.nullable != nullable {
                errors.push(ValidationError::new(
                    &path,
                    "LIKE result nullability differs from its operands",
                ));
            }
            if [*expr, *pattern].into_iter().any(|id| {
                fragment
                    .expressions()
                    .get(id)
                    .is_some_and(|value| !is_utf8(&value.ty.data_type))
            }) {
                errors.push(ValidationError::new(
                    &path,
                    "LIKE operands must use a UTF-8 type",
                ));
            }
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => validate_case_types(
            fragment, expression, *operand, when_then, *else_expr, &path, errors,
        ),
    }
}

fn validate_literal_type(
    literal: &crate::LiteralValue,
    ty: &ValueType,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let valid = match literal {
        crate::LiteralValue::Null => ty.nullable,
        crate::LiteralValue::Boolean(_) => ty.data_type == DataType::Boolean && !ty.nullable,
        crate::LiteralValue::Int64(_) => ty.data_type == DataType::Int64 && !ty.nullable,
        crate::LiteralValue::UInt64(_) => ty.data_type == DataType::UInt64 && !ty.nullable,
        crate::LiteralValue::Float64Bits(_) => ty.data_type == DataType::Float64 && !ty.nullable,
        crate::LiteralValue::LargeInt(_) => {
            novarocks_type_contract::is_largeint_data_type(&ty.data_type) && !ty.nullable
        }
        crate::LiteralValue::Decimal128(_) => {
            matches!(ty.data_type, DataType::Decimal128(_, _)) && !ty.nullable
        }
        crate::LiteralValue::Utf8(_) => is_utf8(&ty.data_type) && !ty.nullable,
        crate::LiteralValue::Binary(_) => {
            matches!(
                ty.data_type,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView
            ) && !ty.nullable
        }
        crate::LiteralValue::Date32(_) => ty.data_type == DataType::Date32 && !ty.nullable,
        crate::LiteralValue::Time64(_) => {
            matches!(
                ty.data_type,
                DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
            ) && !ty.nullable
        }
        crate::LiteralValue::Timestamp(_) => {
            matches!(ty.data_type, DataType::Timestamp(_, _)) && !ty.nullable
        }
        crate::LiteralValue::IntervalMonthDayNano(_) => {
            ty.data_type == DataType::Interval(IntervalUnit::MonthDayNano) && !ty.nullable
        }
    };
    if !valid {
        errors.push(ValidationError::new(
            path,
            "literal representation differs from its declared type",
        ));
    }
}

fn expression_reference_scope_allowed(
    fragment: &Fragment,
    parent: &crate::ExprNode,
    child_id: ExprId,
    child: &crate::ExprNode,
) -> bool {
    if matches!(parent.kind, ExprKind::Lambda { body, .. } if body == child_id) {
        return child.lambda_scope == Some(parent.id);
    }
    if let ExprKind::LambdaParameter { lambda, .. } = child.kind {
        return lambda_scope_contains(fragment, parent.lambda_scope, lambda);
    }
    child.lambda_scope == parent.lambda_scope
}

fn lambda_scope_contains(fragment: &Fragment, mut scope: Option<ExprId>, expected: ExprId) -> bool {
    let mut visited = BTreeSet::new();
    while let Some(id) = scope {
        if id == expected {
            return true;
        }
        if !visited.insert(id) {
            return false;
        }
        scope = fragment
            .expressions()
            .get(id)
            .and_then(|expression| expression.lambda_scope);
    }
    false
}

fn validate_binary_types(
    left: &crate::ExprNode,
    op: crate::BinaryOperator,
    right: &crate::ExprNode,
    output: &crate::ExprNode,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let same_inputs = left.ty.data_type == right.ty.data_type;
    let nullable = left.ty.nullable || right.ty.nullable;
    let valid = match op {
        crate::BinaryOperator::Add
        | crate::BinaryOperator::Subtract
        | crate::BinaryOperator::Multiply
        | crate::BinaryOperator::Divide
        | crate::BinaryOperator::Modulo => {
            let operation = match op {
                crate::BinaryOperator::Add => novarocks_type_contract::ArithmeticOperator::Add,
                crate::BinaryOperator::Subtract => {
                    novarocks_type_contract::ArithmeticOperator::Subtract
                }
                crate::BinaryOperator::Multiply => {
                    novarocks_type_contract::ArithmeticOperator::Multiply
                }
                crate::BinaryOperator::Divide => {
                    novarocks_type_contract::ArithmeticOperator::Divide
                }
                crate::BinaryOperator::Modulo => {
                    novarocks_type_contract::ArithmeticOperator::Modulo
                }
                _ => unreachable!(),
            };
            novarocks_type_contract::arithmetic_result_type_with_op(
                &left.ty.data_type,
                &right.ty.data_type,
                operation,
            )
            .as_ref()
            .is_some_and(|expected| expected == &output.ty.data_type)
                && output.ty.nullable == nullable
        }
        crate::BinaryOperator::Eq
        | crate::BinaryOperator::NotEq
        | crate::BinaryOperator::Lt
        | crate::BinaryOperator::LtEq
        | crate::BinaryOperator::Gt
        | crate::BinaryOperator::GtEq => {
            same_inputs
                && output.ty.data_type == DataType::Boolean
                && output.ty.nullable == nullable
        }
        crate::BinaryOperator::EqForNull => {
            same_inputs && output.ty.data_type == DataType::Boolean && !output.ty.nullable
        }
        crate::BinaryOperator::And | crate::BinaryOperator::Or => {
            left.ty.data_type == DataType::Boolean
                && right.ty.data_type == DataType::Boolean
                && output.ty.data_type == DataType::Boolean
                && output.ty.nullable == nullable
        }
        crate::BinaryOperator::BitAnd
        | crate::BinaryOperator::BitOr
        | crate::BinaryOperator::BitXor => {
            same_inputs
                && is_integer(&left.ty.data_type)
                && output.ty.data_type == left.ty.data_type
                && output.ty.nullable == nullable
        }
    };
    if !valid {
        errors.push(ValidationError::new(
            path,
            "binary expression types are inconsistent with its operator",
        ));
    }
}

fn validate_case_types(
    fragment: &Fragment,
    expression: &crate::ExprNode,
    operand: Option<ExprId>,
    when_then: &[(ExprId, ExprId)],
    else_expr: Option<ExprId>,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if when_then.is_empty() {
        errors.push(ValidationError::new(
            path,
            "CASE expression has no branches",
        ));
    }
    let operand_type = operand.and_then(|id| fragment.expressions().get(id).map(|node| &node.ty));
    for (when, then) in when_then {
        if let Some(when) = fragment.expressions().get(*when) {
            let valid = operand_type
                .map(|operand| operand.data_type == when.ty.data_type)
                .unwrap_or(when.ty.data_type == DataType::Boolean);
            if !valid {
                errors.push(ValidationError::new(path, "CASE condition type is invalid"));
            }
        }
        if fragment
            .expressions()
            .get(*then)
            .is_some_and(|then| then.ty.data_type != expression.ty.data_type)
        {
            errors.push(ValidationError::new(
                path,
                "CASE result type differs from its output",
            ));
        }
    }
    if let Some(else_expr) = else_expr
        && fragment
            .expressions()
            .get(else_expr)
            .is_some_and(|otherwise| otherwise.ty.data_type != expression.ty.data_type)
    {
        errors.push(ValidationError::new(
            path,
            "CASE ELSE type differs from its output",
        ));
    }
    let result_nullable = else_expr.is_none()
        || when_then.iter().any(|(_, then)| {
            fragment
                .expressions()
                .get(*then)
                .is_some_and(|then| then.ty.nullable)
        })
        || else_expr.is_some_and(|otherwise| {
            fragment
                .expressions()
                .get(otherwise)
                .is_some_and(|otherwise| otherwise.ty.nullable)
        });
    if expression.ty.nullable != result_nullable {
        errors.push(ValidationError::new(
            path,
            "CASE result nullability differs from its branches",
        ));
    }
}

fn validate_window_frame(
    fragment: &Fragment,
    frame: &crate::WindowFrame,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if matches!(frame.start, crate::WindowBound::UnboundedFollowing)
        || matches!(frame.end, crate::WindowBound::UnboundedPreceding)
        || window_bound_position(fragment, &frame.start)
            > window_bound_position(fragment, &frame.end)
    {
        errors.push(ValidationError::new(
            path,
            "window frame start follows its end",
        ));
    }
    for bound in [&frame.start, &frame.end] {
        let expression = match bound {
            crate::WindowBound::Preceding(expression)
            | crate::WindowBound::Following(expression) => Some(*expression),
            crate::WindowBound::UnboundedPreceding
            | crate::WindowBound::CurrentRow
            | crate::WindowBound::UnboundedFollowing => None,
        };
        let Some(expression) = expression else {
            continue;
        };
        if window_row_offset(fragment, expression) == Some(0) {
            errors.push(ValidationError::new(
                path,
                "zero window-frame offset must be canonicalized to CURRENT ROW",
            ));
        }
        let valid = fragment
            .expressions()
            .get(expression)
            .is_some_and(|expression| {
                !expression.ty.nullable
                    && (frame.units == crate::WindowFrameUnits::Range
                        || window_offset_literal_is_valid(&expression.kind, frame.units))
            });
        if !valid {
            errors.push(ValidationError::new(
                path,
                "window frame offset must be a non-negative non-null literal of the required domain",
            ));
        }
    }
    if frame.units == crate::WindowFrameUnits::Range && frame_has_offset(frame) {
        errors.push(ValidationError::new(
            path,
            "RANGE window offsets require typed order-key arithmetic not represented by contract revision 1",
        ));
    }
    let offsets_are_ordered = match (&frame.start, &frame.end) {
        (crate::WindowBound::Preceding(start), crate::WindowBound::Preceding(end)) => {
            window_row_offset(fragment, *start)
                .zip(window_row_offset(fragment, *end))
                .is_none_or(|(start, end)| start >= end)
        }
        (crate::WindowBound::Following(start), crate::WindowBound::Following(end)) => {
            window_row_offset(fragment, *start)
                .zip(window_row_offset(fragment, *end))
                .is_none_or(|(start, end)| start <= end)
        }
        _ => true,
    };
    if !offsets_are_ordered {
        errors.push(ValidationError::new(
            path,
            "window frame start follows its end after comparing exact offsets",
        ));
    }
}

fn frame_has_offset(frame: &crate::WindowFrame) -> bool {
    [&frame.start, &frame.end].iter().any(|bound| {
        matches!(
            bound,
            crate::WindowBound::Preceding(_) | crate::WindowBound::Following(_)
        )
    })
}

fn window_bound_position(fragment: &Fragment, bound: &crate::WindowBound) -> u8 {
    match bound {
        crate::WindowBound::UnboundedPreceding => 0,
        crate::WindowBound::Preceding(expression)
            if window_row_offset(fragment, *expression) == Some(0) =>
        {
            2
        }
        crate::WindowBound::Preceding(_) => 1,
        crate::WindowBound::CurrentRow => 2,
        crate::WindowBound::Following(expression)
            if window_row_offset(fragment, *expression) == Some(0) =>
        {
            2
        }
        crate::WindowBound::Following(_) => 3,
        crate::WindowBound::UnboundedFollowing => 4,
    }
}

fn window_offset_literal_is_valid(kind: &ExprKind, units: crate::WindowFrameUnits) -> bool {
    match (units, kind) {
        (
            crate::WindowFrameUnits::Rows | crate::WindowFrameUnits::Groups,
            ExprKind::Literal(crate::LiteralValue::UInt64(_)),
        ) => true,
        (
            crate::WindowFrameUnits::Rows | crate::WindowFrameUnits::Groups,
            ExprKind::Literal(crate::LiteralValue::Int64(value)),
        ) => *value >= 0,
        _ => false,
    }
}

fn window_row_offset(fragment: &Fragment, expression: ExprId) -> Option<u64> {
    match &fragment.expressions().get(expression)?.kind {
        ExprKind::Literal(crate::LiteralValue::UInt64(value)) => Some(*value),
        ExprKind::Literal(crate::LiteralValue::Int64(value)) => u64::try_from(*value).ok(),
        _ => None,
    }
}

fn require_boolean_result(ty: &ValueType, path: &str, errors: &mut ValidationErrorCollector) {
    if ty.data_type != DataType::Boolean {
        errors.push(ValidationError::new(
            path,
            "expression result is not Boolean",
        ));
    }
}

fn require_non_nullable_boolean(ty: &ValueType, path: &str, errors: &mut ValidationErrorCollector) {
    if ty.data_type != DataType::Boolean || ty.nullable {
        errors.push(ValidationError::new(
            path,
            "expression result must be non-nullable Boolean",
        ));
    }
}

fn is_utf8(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_integer(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

fn is_numeric(ty: &DataType) -> bool {
    is_integer(ty)
        || matches!(
            ty,
            DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
        )
}

fn validate_function_call(
    fragment: &Fragment,
    expression: &crate::ExprNode,
    function: &crate::BoundFunction,
    args: &[ExprId],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    validate_function_arguments(fragment, &function.argument_types, args, path, errors);
    if expression.ty != function.result_type {
        errors.push(ValidationError::new(
            path,
            "function expression type differs from the bound result type",
        ));
    }
}

fn validate_function_arguments(
    fragment: &Fragment,
    expected: &[crate::FunctionArgumentType],
    args: &[ExprId],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if expected.len() != args.len() {
        errors.push(ValidationError::new(
            path,
            format!(
                "bound function expects {} arguments, got {}",
                expected.len(),
                args.len()
            ),
        ));
        return;
    }
    for (ordinal, (expected, argument)) in expected.iter().zip(args).enumerate() {
        let valid = fragment.expressions().get(*argument).is_some_and(|actual| {
            match (expected, &actual.kind) {
                (crate::FunctionArgumentType::Value(_), ExprKind::Lambda { .. }) => false,
                (crate::FunctionArgumentType::Value(expected), _) => &actual.ty == expected,
                (
                    crate::FunctionArgumentType::Lambda {
                        parameter_types,
                        result_type,
                    },
                    ExprKind::Lambda {
                        parameter_types: actual_parameters,
                        ..
                    },
                ) => actual_parameters == parameter_types && &actual.ty == result_type,
                (crate::FunctionArgumentType::Lambda { .. }, _) => false,
            }
        });
        if !valid {
            errors.push(ValidationError::new(
                path,
                format!("function argument {ordinal} shape differs from its bound signature"),
            ));
        }
    }
}

fn validate_expression_acyclic(fragment: &Fragment, errors: &mut ValidationErrorCollector) {
    let path = format!("fragments[{}].expressions", fragment.id().get());
    let mut remaining_dependencies = BTreeMap::new();
    let mut dependents: BTreeMap<ExprId, Vec<ExprId>> = BTreeMap::new();
    let mut depths = BTreeMap::new();
    let mut ready = Vec::new();

    for (id, expression) in fragment.expressions().iter() {
        let mut references = Vec::new();
        expression.kind.expression_references(&mut references);
        references.retain(|reference| fragment.expressions().get(*reference).is_some());
        references.sort_unstable();
        references.dedup();
        remaining_dependencies.insert(*id, references.len());
        if references.is_empty() {
            ready.push(*id);
            depths.insert(*id, 1_usize);
        }
        for reference in references {
            dependents.entry(reference).or_default().push(*id);
        }
    }

    let mut processed = 0_usize;
    while let Some(id) = ready.pop() {
        processed += 1;
        let depth = depths.get(&id).copied().unwrap_or(1);
        if let Some(users) = dependents.get(&id) {
            for user in users {
                let candidate = depth.saturating_add(1);
                depths
                    .entry(*user)
                    .and_modify(|current| *current = (*current).max(candidate))
                    .or_insert(candidate);
                if let Some(remaining) = remaining_dependencies.get_mut(user) {
                    *remaining -= 1;
                    if *remaining == 0 {
                        ready.push(*user);
                    }
                }
            }
        }
    }

    if processed != fragment.expressions().len() {
        errors.push(ValidationError::new(
            &path,
            "expression graph contains a cycle",
        ));
    }
    if depths.values().copied().max().unwrap_or(0) > MAX_EXPRESSION_SEMANTIC_DEPTH {
        errors.push(ValidationError::new(
            &path,
            format!("expression semantic depth exceeds {MAX_EXPRESSION_SEMANTIC_DEPTH}"),
        ));
    }
}

fn validate_lambda_scope_acyclic(fragment: &Fragment, errors: &mut ValidationErrorCollector) {
    let path = format!("fragments[{}].expressions", fragment.id().get());
    let mut complete = BTreeSet::new();
    for (start, _) in fragment.expressions().iter() {
        if complete.contains(start) {
            continue;
        }
        let mut current = Some(*start);
        let mut local = BTreeSet::new();
        while let Some(id) = current {
            if complete.contains(&id) {
                break;
            }
            if !local.insert(id) {
                errors.push(ValidationError::new(
                    &path,
                    "lambda lexical-scope graph contains a cycle",
                ));
                break;
            }
            current = fragment
                .expressions()
                .get(id)
                .and_then(|expression| expression.lambda_scope);
        }
        complete.extend(local);
    }
}

fn validate_expression_reachability(fragment: &Fragment, errors: &mut ValidationErrorCollector) {
    let path = format!("fragments[{}].expressions", fragment.id().get());
    let mut pending = Vec::new();
    for node in fragment.nodes().values() {
        node.kind.expression_references(&mut pending);
        if let NodeKind::Scan { derived_values, .. } = &node.kind {
            pending.extend(derived_values.iter().filter_map(|value| {
                fragment
                    .values()
                    .get(value)
                    .and_then(|definition| match definition.origin {
                        ValueOrigin::Expr { node: owner, expr } if owner == node.id => Some(expr),
                        _ => None,
                    })
            }));
        }
    }
    let mut reachable = BTreeSet::new();
    while let Some(id) = pending.pop() {
        if !reachable.insert(id) {
            continue;
        }
        if let Some(expression) = fragment.expressions().get(id) {
            expression.kind.expression_references(&mut pending);
        }
    }
    if reachable.len() != fragment.expressions().len() {
        errors.push(ValidationError::new(
            &path,
            "expression arena contains definitions unreachable from operator roots",
        ));
    }
}

fn validate_node(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    errors: &mut ValidationErrorCollector,
) {
    let path = format!(
        "fragments[{}].nodes[{}]",
        fragment.id().get(),
        node.id.get()
    );
    if node.output.node != node.id {
        errors.push(ValidationError::new(
            &path,
            "output port belongs to a different node",
        ));
    }
    for input in &node.inputs {
        require_node(fragment, *input, &path, errors);
    }
    if node.inputs.len() != node.required_inputs.len() {
        errors.push(ValidationError::new(
            &path,
            "required input properties must have one entry per input",
        ));
    }
    for (ordinal, properties) in node.required_inputs.iter().enumerate() {
        validate_distribution(
            fragment,
            &properties.distribution,
            "node.required_input",
            errors,
        );
        for key in &properties.ordering {
            require_value(fragment, key.value, &path, errors);
        }
        if let Some(input) = node
            .inputs
            .get(ordinal)
            .and_then(|input| fragment.nodes().get(input))
        {
            if let Some(input_values) = indexes.output(input.id) {
                validate_property_keys_on_port(
                    fragment,
                    properties,
                    input_values,
                    &format!("{path}.required_inputs[{ordinal}]"),
                    errors,
                );
            }
            if !properties_satisfy(&input.output_properties, properties) {
                errors.push(ValidationError::new(
                    &path,
                    format!("input {ordinal} does not provide its required physical properties"),
                ));
            }
        }
    }
    validate_distribution(
        fragment,
        &node.output_properties.distribution,
        "node.output",
        errors,
    );
    for key in &node.output_properties.ordering {
        require_value(fragment, key.value, &path, errors);
    }
    for value in &node.output.columns {
        require_value(fragment, *value, &path, errors);
    }
    if let Some(output_values) = indexes.output(node.id) {
        validate_property_keys_on_port(
            fragment,
            &node.output_properties,
            output_values,
            &format!("{path}.output_properties"),
            errors,
        );
    }
    validate_node_output_properties(fragment, node, &path, errors);
    let mut expressions = Vec::new();
    node.kind.expression_references(&mut expressions);
    for expression in expressions {
        match fragment.expressions().get(expression) {
            Some(expression) if expression.owner != node.id => errors.push(ValidationError::new(
                &path,
                "node references an expression owned by another physical node",
            )),
            Some(expression) if expression.lambda_scope.is_some() => errors.push(
                ValidationError::new(&path, "node expression root is inside a lambda scope"),
            ),
            Some(_) => {}
            None => errors.push(ValidationError::new(
                &path,
                format!("expression {} is not defined", expression.get()),
            )),
        }
    }
    validate_node_arity(node, &path, errors);
    validate_node_semantics(fragment, node, indexes, &path, errors);
    validate_node_output_closure(fragment, node, indexes, &path, errors);
}

fn validate_node_output_closure(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let input_columns = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
        .map(|input| input.output.columns.as_ref())
        .unwrap_or_default();
    let input_values = indexes
        .visible_input(node.id)
        .expect("every fragment node has one indexed visible-input port");
    let aggregate_call_ids = match &node.kind {
        NodeKind::Aggregate { calls, .. } => {
            calls.iter().map(|call| call.id).collect::<BTreeSet<_>>()
        }
        _ => BTreeSet::new(),
    };
    let repeat_grouping_values = match &node.kind {
        NodeKind::Repeat {
            grouping_values, ..
        } => grouping_values.iter().copied().collect::<BTreeSet<_>>(),
        _ => BTreeSet::new(),
    };
    if matches!(
        node.kind,
        NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. }
    ) {
        validate_join_output_closure(fragment, node, indexes, path, errors);
        return;
    }
    let exact = match &node.kind {
        NodeKind::Scan {
            provider_outputs,
            derived_values,
            ..
        } => {
            validate_scan_output_coverage(node, provider_outputs, derived_values, path, errors);
            None
        }
        NodeKind::Filter { .. }
        | NodeKind::Sort { .. }
        | NodeKind::TopN { .. }
        | NodeKind::Limit { .. }
        | NodeKind::AssertOneRow(_) => Some(input_columns.to_vec()),
        NodeKind::Project { expressions } => {
            Some(expressions.iter().map(|(_, value)| *value).collect())
        }
        NodeKind::Aggregate { group_by, calls } => Some(
            group_by
                .iter()
                .map(|(_, value)| *value)
                .chain(calls.iter().map(|call| call.output))
                .collect(),
        ),
        NodeKind::Window(spec) => Some(
            input_columns
                .iter()
                .copied()
                .chain(spec.expressions.iter().map(|expression| expression.output))
                .collect(),
        ),
        NodeKind::Repeat {
            grouping_values,
            grouping_outputs,
            ..
        } => {
            let replacements = grouping_values.iter().copied().collect::<BTreeMap<_, _>>();
            Some(
                input_columns
                    .iter()
                    .map(|value| replacements.get(value).copied().unwrap_or(*value))
                    .chain(grouping_outputs.iter().map(|output| output.output))
                    .collect(),
            )
        }
        NodeKind::Unpivot { spec } => Some(
            spec.passthrough
                .iter()
                .map(|(_, output)| *output)
                .chain(std::iter::once(spec.value_output))
                .chain(spec.literal_outputs.iter().copied())
                .collect(),
        ),
        NodeKind::TableFunction { outputs, .. } => {
            Some(outputs.iter().map(|output| output.value()).collect())
        }
        NodeKind::ExchangeSource { .. } => None,
        NodeKind::TableWriter { target } => Some(
            target
                .output_schema
                .fields
                .iter()
                .map(|field| field.value)
                .collect(),
        ),
        NodeKind::TableFinish(spec) => Some(
            spec.output_schema
                .fields
                .iter()
                .map(|field| field.value)
                .collect(),
        ),
        NodeKind::HashJoin { .. }
        | NodeKind::NestLoopJoin { .. }
        | NodeKind::SetOp { .. }
        | NodeKind::Values { .. }
        | NodeKind::GenerateSeries { .. }
        | NodeKind::ChangeEventExpand { .. } => None,
    };
    if let Some(exact) = exact
        && exact.as_slice() != node.output.columns.as_ref()
    {
        errors.push(ValidationError::new(
            path,
            "node output port differs from its exact produced/pass-through sequence",
        ));
    }
    for (ordinal, value) in node.output.columns.iter().enumerate() {
        if input_values.contains(value) {
            continue;
        }
        let owned = fragment.values().get(value).is_some_and(|definition| {
            value_origin_allowed(
                node,
                definition,
                ordinal,
                input_values,
                &aggregate_call_ids,
                &repeat_grouping_values,
            )
        });
        if !owned {
            errors.push(ValidationError::new(
                path,
                format!("output value {} is not produced by this node", value.get()),
            ));
        }
    }
}

fn validate_scan_output_coverage(
    node: &PhysicalNode,
    provider_outputs: &[(ProviderColumnReference, ValueId)],
    derived_values: &[ValueId],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let expected = ValuePortIndex::new(
        &provider_outputs
            .iter()
            .map(|(_, value)| *value)
            .chain(derived_values.iter().copied())
            .collect::<Vec<_>>(),
    );
    let actual = ValuePortIndex::new(&node.output.columns);
    if actual.occurrences != expected.occurrences {
        errors.push(ValidationError::new(
            path,
            "scan output port does not exactly cover its provider and derived value occurrences",
        ));
    }
}

fn validate_join_output_closure(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let Some(left) = node.inputs.first().and_then(|id| fragment.nodes().get(id)) else {
        return;
    };
    let Some(right) = node.inputs.get(1).and_then(|id| fragment.nodes().get(id)) else {
        return;
    };
    let (kind, declared_nulls) = match &node.kind {
        NodeKind::HashJoin {
            kind,
            null_extended,
            ..
        }
        | NodeKind::NestLoopJoin {
            kind,
            null_extended,
            ..
        } => (*kind, null_extended.as_ref()),
        _ => return,
    };
    let left_values = indexes
        .output(left.id)
        .expect("every fragment node has one indexed output port");
    let right_values = indexes
        .output(right.id)
        .expect("every fragment node has one indexed output port");
    let declared = declared_nulls.iter().copied().collect::<BTreeSet<_>>();
    if declared.len() != declared_nulls.len() {
        errors.push(ValidationError::new(
            path,
            "join NULL-extended value identities are duplicated",
        ));
    }
    let output_nulls = node
        .output
        .columns
        .iter()
        .filter_map(|value| {
            fragment.values().get(value).and_then(|definition| {
                matches!(
                    definition.origin,
                    ValueOrigin::NullExtended { node: owner, .. } if owner == node.id
                )
                .then_some(*value)
            })
        })
        .collect::<BTreeSet<_>>();
    if output_nulls != declared {
        errors.push(ValidationError::new(
            path,
            "join NULL-extended declarations differ from its output values",
        ));
    }
    for value in &node.output.columns {
        let allowed_original = match kind {
            crate::JoinKind::Cross | crate::JoinKind::Inner => {
                left_values.contains(value) || right_values.contains(value)
            }
            crate::JoinKind::LeftOuter
            | crate::JoinKind::LeftSemi
            | crate::JoinKind::LeftAnti
            | crate::JoinKind::NullAwareLeftAnti => left_values.contains(value),
            crate::JoinKind::RightOuter
            | crate::JoinKind::RightSemi
            | crate::JoinKind::RightAnti => right_values.contains(value),
            crate::JoinKind::FullOuter => false,
        };
        if allowed_original {
            continue;
        }
        let valid_null_extension = declared.contains(value)
            && fragment.values().get(value).is_some_and(|definition| {
                matches!(
                    definition.origin,
                    ValueOrigin::NullExtended { node: owner, of }
                        if owner == node.id
                            && match kind {
                                crate::JoinKind::LeftOuter => right_values.contains(&of),
                                crate::JoinKind::RightOuter => left_values.contains(&of),
                                crate::JoinKind::FullOuter => {
                                    left_values.contains(&of) || right_values.contains(&of)
                                }
                                crate::JoinKind::Cross
                                | crate::JoinKind::Inner
                                | crate::JoinKind::LeftSemi
                                | crate::JoinKind::LeftAnti
                                | crate::JoinKind::NullAwareLeftAnti
                                | crate::JoinKind::RightSemi
                                | crate::JoinKind::RightAnti => false,
                            }
                )
            });
        if !valid_null_extension {
            errors.push(ValidationError::new(
                path,
                format!(
                    "join output value {} is not valid for its join kind",
                    value.get()
                ),
            ));
        }
    }
}

fn value_origin_allowed(
    node: &PhysicalNode,
    definition: &ValueDef,
    ordinal: usize,
    input_values: &VisibleInputIndex,
    aggregate_call_ids: &BTreeSet<crate::AggregateCallId>,
    repeat_grouping_values: &BTreeSet<(ValueId, ValueId)>,
) -> bool {
    match (&node.kind, &definition.origin) {
        (NodeKind::Scan { .. }, ValueOrigin::ProviderField { scan_node, .. }) => {
            *scan_node == node.id
        }
        (
            NodeKind::Scan { .. } | NodeKind::Project { .. } | NodeKind::Window { .. },
            ValueOrigin::Expr { node: owner, .. },
        ) => *owner == node.id,
        (
            NodeKind::Aggregate { .. },
            ValueOrigin::AggregateState { call, .. } | ValueOrigin::AggregateResult { call },
        ) => aggregate_call_ids.contains(call),
        (
            NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. },
            ValueOrigin::NullExtended { node: owner, of },
        ) => *owner == node.id && input_values.contains(of),
        (
            NodeKind::ExchangeSource {
                edge: node_edge, ..
            },
            ValueOrigin::ExchangeImport { edge, .. } | ValueOrigin::CteImport { edge, .. },
        ) => node_edge == edge,
        (
            NodeKind::TableWriter { .. } | NodeKind::TableFinish(_),
            ValueOrigin::WriterDerived { writer_node, .. },
        ) => *writer_node == node.id,
        (NodeKind::Repeat { .. }, ValueOrigin::NullExtended { node: owner, of }) => {
            *owner == node.id && repeat_grouping_values.contains(&(*of, definition.id))
        }
        (
            NodeKind::SetOp { .. },
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal,
            },
        ) => {
            *owner == node.id
                && usize::try_from(*output_ordinal)
                    .ok()
                    .and_then(|ordinal| node.output.columns.get(ordinal))
                    == Some(&definition.id)
        }
        (
            NodeKind::Values { .. }
            | NodeKind::Repeat { .. }
            | NodeKind::Unpivot { .. }
            | NodeKind::GenerateSeries { .. }
            | NodeKind::TableFunction { .. }
            | NodeKind::ChangeEventExpand { .. },
            ValueOrigin::NodeOutput {
                node: owner,
                output_ordinal,
            },
        ) => *owner == node.id && usize::try_from(*output_ordinal).ok() == Some(ordinal),
        _ => false,
    }
}

fn validate_node_arity(node: &PhysicalNode, path: &str, errors: &mut ValidationErrorCollector) {
    let valid = match &node.kind {
        NodeKind::Scan { .. }
        | NodeKind::Values { .. }
        | NodeKind::GenerateSeries { .. }
        | NodeKind::ExchangeSource { .. } => node.inputs.is_empty(),
        NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. } => node.inputs.len() == 2,
        NodeKind::SetOp { .. } => node.inputs.len() >= 2,
        NodeKind::TableFunction { .. } => node.inputs.len() <= 1,
        _ => node.inputs.len() == 1,
    };
    if !valid {
        errors.push(ValidationError::new(path, "node has invalid input arity"));
    }
}

fn validate_node_semantics(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    match &node.kind {
        NodeKind::Scan {
            relation,
            read_budget,
            provider_outputs,
            residuals,
            derived_values,
        } => {
            validate_relation(fragment, relation, path, errors);
            if read_budget.max_batch_rows == 0
                || read_budget.max_batch_rows > MAX_SCAN_BATCH_ROWS
                || read_budget.max_batch_bytes == 0
                || read_budget.max_batch_bytes > MAX_SCAN_BATCH_BYTES
            {
                errors.push(ValidationError::new(
                    path,
                    "scan read budget must be non-zero and within the supported bounds",
                ));
            }
            if provider_outputs.len() != relation.schema().len() {
                errors.push(ValidationError::new(
                    path,
                    "provider output mapping does not cover the exact relation schema",
                ));
            }
            for ((column, value), field) in provider_outputs.iter().zip(relation.schema()) {
                if column != &field.column {
                    errors.push(ValidationError::new(
                        path,
                        "provider output column differs from the relation schema ordinal",
                    ));
                }
                match fragment.values().get(value) {
                    Some(definition)
                        if definition.ty == field.ty
                            && matches!(
                                &definition.origin,
                                ValueOrigin::ProviderField { scan_node, field: origin }
                                    if *scan_node == node.id && origin == column
                            ) => {}
                    Some(_) => errors.push(ValidationError::new(
                        path,
                        "provider output value has inconsistent type or origin",
                    )),
                    None => require_value(fragment, *value, path, errors),
                }
            }
            for value in derived_values {
                match fragment.values().get(value) {
                    Some(definition) if matches!(definition.origin, ValueOrigin::Expr { node: owner, .. } if owner == node.id) =>
                        {}
                    Some(_) => errors.push(ValidationError::new(
                        path,
                        "derived scan value does not have this scan as its expression owner",
                    )),
                    None => require_value(fragment, *value, path, errors),
                }
            }
            for residual in residuals {
                require_boolean_expression(fragment, *residual, path, errors);
            }
            validate_scan_predicate_contract(fragment, node.id, relation, residuals, path, errors);
        }
        NodeKind::Filter { predicate } => {
            require_boolean_expression(fragment, *predicate, path, errors);
            require_passthrough_output(fragment, node, path, errors);
        }
        NodeKind::Project { expressions } => {
            let input = node.inputs.first().and_then(|id| fragment.nodes().get(id));
            let input_values = input.and_then(|input| indexes.output(input.id));
            for (expression, value) in expressions {
                match (
                    fragment.expressions().get(*expression),
                    fragment.values().get(value),
                ) {
                    (Some(expression_node), Some(value_def))
                        if expression_node.ty == value_def.ty
                            && (matches!(
                                value_def.origin,
                                ValueOrigin::Expr { node: owner, expr: source }
                                    if owner == node.id && source == expression_node.id
                            ) || matches!(
                                expression_node.kind,
                                ExprKind::Value(source)
                                    if source == *value
                                        && input_values
                                            .as_ref()
                                            .is_some_and(|input| input.contains(&source))
                            )) => {}
                    (Some(_), Some(_)) => errors.push(ValidationError::new(
                        path,
                        "project output has inconsistent expression, type or owner",
                    )),
                    _ => {}
                }
            }
        }
        NodeKind::Aggregate { group_by, calls } => {
            for (expression_id, output) in group_by {
                match (
                    fragment.expressions().get(*expression_id),
                    fragment.values().get(output),
                ) {
                    (Some(expression_node), Some(value))
                        if expression_node.ty == value.ty
                            && (matches!(expression_node.kind, ExprKind::Value(source) if source == *output)
                                || matches!(
                                    value.origin,
                                    ValueOrigin::Expr { node: owner, expr }
                                        if owner == node.id && expr == *expression_id
                                )) => {}
                    (Some(_), Some(_)) => errors.push(ValidationError::new(
                        path,
                        "aggregate grouping output has inconsistent expression, type or origin",
                    )),
                    _ => {}
                }
            }
            let mut ids = BTreeSet::new();
            let phase_kind = calls
                .first()
                .map(|call| std::mem::discriminant(&call.binding.phase));
            for call in calls {
                if !ids.insert(call.id) {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate call identity is duplicated",
                    ));
                }
                if call.binding.function.kind != FunctionKind::Aggregate {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate node has non-aggregate binding",
                    ));
                }
                if phase_kind
                    .is_some_and(|expected| expected != std::mem::discriminant(&call.binding.phase))
                {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate node mixes incompatible execution phases",
                    ));
                }
                validate_aggregate_value_inputs(
                    fragment,
                    &call.binding,
                    &call.arguments,
                    &call.order_by,
                    path,
                    errors,
                );
                if !call.binding.phase.consumes_logical_arguments() && call.distinct {
                    errors.push(ValidationError::new(
                        path,
                        "state-consuming aggregate phase cannot apply DISTINCT again",
                    ));
                }
                if let Some(output) = fragment.values().get(&call.output) {
                    let expected = match call.binding.phase {
                        AggregatePhase::Single | AggregatePhase::Final { .. } => {
                            &call.binding.function.result_type
                        }
                        AggregatePhase::Partial { .. } | AggregatePhase::Intermediate { .. } => {
                            &call.binding.intermediate_type
                        }
                    };
                    if &output.ty != expected {
                        errors.push(ValidationError::new(
                            path,
                            "aggregate output type differs from phase output",
                        ));
                    }
                    let expected_origin = match call.binding.phase {
                        AggregatePhase::Single | AggregatePhase::Final { .. } => matches!(
                            output.origin,
                            ValueOrigin::AggregateResult { call: id } if id == call.id
                        ),
                        AggregatePhase::Partial { .. } | AggregatePhase::Intermediate { .. } => {
                            matches!(
                                output.origin,
                                ValueOrigin::AggregateState { call: id, phase }
                                    if id == call.id && phase == call.binding.phase
                            )
                        }
                    };
                    if !expected_origin {
                        errors.push(ValidationError::new(
                            path,
                            "aggregate output origin differs from the call phase",
                        ));
                    }
                }
            }
        }
        NodeKind::HashJoin {
            kind,
            keys,
            build_side,
            distribution,
            residual,
            null_extended,
            ..
        } => {
            if *kind == crate::JoinKind::Cross {
                errors.push(ValidationError::new(
                    path,
                    "cross join cannot use the hash-join node contract",
                ));
            }
            let broadcast_build_is_safe = match kind {
                crate::JoinKind::Inner => true,
                crate::JoinKind::LeftOuter
                | crate::JoinKind::LeftSemi
                | crate::JoinKind::LeftAnti
                | crate::JoinKind::NullAwareLeftAnti => *build_side == crate::JoinSide::Right,
                crate::JoinKind::RightOuter
                | crate::JoinKind::RightSemi
                | crate::JoinKind::RightAnti => *build_side == crate::JoinSide::Left,
                crate::JoinKind::FullOuter | crate::JoinKind::Cross => false,
            };
            if *distribution == crate::JoinDistribution::BroadcastBuild && !broadcast_build_is_safe
            {
                errors.push(ValidationError::new(
                    path,
                    "broadcast-build join cannot publish build-side rows from replicated workers",
                ));
            }
            if keys.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "hash join must have at least one key",
                ));
            }
            for key in keys {
                if let (Some(left), Some(right)) = (
                    fragment.expressions().get(key.left),
                    fragment.expressions().get(key.right),
                ) && left.ty.data_type != right.ty.data_type
                {
                    errors.push(ValidationError::new(
                        path,
                        "hash join key pair does not have one exact execution data type",
                    ));
                }
            }
            if let Some(residual) = residual {
                require_boolean_expression(fragment, *residual, path, errors);
            }
            if let (Some(left), Some(right)) = (
                node.inputs.first().and_then(|id| fragment.nodes().get(id)),
                node.inputs.get(1).and_then(|id| fragment.nodes().get(id)),
            ) {
                validate_expression_values_on_port(
                    fragment,
                    &keys.iter().map(|key| key.left).collect::<Vec<_>>(),
                    indexes
                        .output(left.id)
                        .expect("every fragment node has one indexed output port"),
                    path,
                    errors,
                );
                validate_expression_values_on_port(
                    fragment,
                    &keys.iter().map(|key| key.right).collect::<Vec<_>>(),
                    indexes
                        .output(right.id)
                        .expect("every fragment node has one indexed output port"),
                    path,
                    errors,
                );
            }
            validate_null_extended(fragment, node.id, null_extended, path, errors);
            if node.required_inputs.len() == 2 {
                let left_keys = keys
                    .iter()
                    .filter_map(|key| expression_value(fragment, key.left))
                    .collect::<Vec<_>>();
                let right_keys = keys
                    .iter()
                    .filter_map(|key| expression_value(fragment, key.right))
                    .collect::<Vec<_>>();
                let compatible = match (
                    distribution,
                    &node.required_inputs[0],
                    &node.required_inputs[1],
                ) {
                    (
                        crate::JoinDistribution::Partitioned,
                        crate::PhysicalProperties {
                            distribution:
                                Distribution::Hash {
                                    keys: left,
                                    scheme: left_scheme,
                                },
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                        crate::PhysicalProperties {
                            distribution:
                                Distribution::Hash {
                                    keys: right,
                                    scheme: right_scheme,
                                },
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                    ) => {
                        left_scheme == right_scheme
                            && left.as_ref() == left_keys
                            && right.as_ref() == right_keys
                    }
                    (
                        crate::JoinDistribution::Colocated,
                        crate::PhysicalProperties {
                            distribution:
                                Distribution::BucketShuffle {
                                    keys: left,
                                    scheme: left_scheme,
                                },
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                        crate::PhysicalProperties {
                            distribution:
                                Distribution::BucketShuffle {
                                    keys: right,
                                    scheme: right_scheme,
                                },
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                    ) => {
                        left_scheme == right_scheme
                            && left.as_ref() == left_keys
                            && right.as_ref() == right_keys
                    }
                    (crate::JoinDistribution::BroadcastBuild, left, right) => match build_side {
                        crate::JoinSide::Left => {
                            left.distribution == Distribution::Broadcast
                                && left.row_multiplicity == RowMultiplicity::Replicated
                                && right.row_multiplicity == RowMultiplicity::SingleCopy
                        }
                        crate::JoinSide::Right => {
                            right.distribution == Distribution::Broadcast
                                && right.row_multiplicity == RowMultiplicity::Replicated
                                && left.row_multiplicity == RowMultiplicity::SingleCopy
                        }
                    },
                    (
                        crate::JoinDistribution::Singleton,
                        crate::PhysicalProperties {
                            distribution: Distribution::Singleton,
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                        crate::PhysicalProperties {
                            distribution: Distribution::Singleton,
                            row_multiplicity: RowMultiplicity::SingleCopy,
                            ..
                        },
                    ) => true,
                    _ => false,
                };
                if !compatible {
                    errors.push(ValidationError::new(
                        path,
                        "hash join lacks the exact partition-space proof required by its distribution mode",
                    ));
                }
            }
        }
        NodeKind::NestLoopJoin {
            kind,
            distribution,
            predicate,
            null_extended,
        } => {
            if *kind == crate::JoinKind::Cross && (predicate.is_some() || !null_extended.is_empty())
            {
                errors.push(ValidationError::new(
                    path,
                    "cross join cannot carry a predicate or NULL-extension outputs",
                ));
            }
            if let Some(predicate) = predicate {
                require_boolean_expression(fragment, *predicate, path, errors);
            }
            validate_null_extended(fragment, node.id, null_extended, path, errors);
            if nest_loop_join_output_distribution(fragment, node, *kind, *distribution, *predicate)
                .is_none()
            {
                errors.push(ValidationError::new(
                    path,
                    "nested-loop join lacks a complete singleton or right-broadcast placement proof",
                ));
            }
        }
        NodeKind::Limit {
            limit: None,
            offset: 0,
        } => errors.push(ValidationError::new(
            path,
            "limit node has neither a limit nor an offset",
        )),
        NodeKind::Sort { order_by, mode } => {
            require_passthrough_output(fragment, node, path, errors);
            validate_ordering_expressions(fragment, node, indexes, order_by, path, errors);
            if order_by.is_empty() {
                errors.push(ValidationError::new(path, "sort order is empty"));
            }
            match mode {
                crate::SortMode::Global => {}
                crate::SortMode::Analytic { partition_by } => {
                    validate_partition_ordering(
                        fragment,
                        node,
                        indexes,
                        partition_by,
                        path,
                        errors,
                    );
                }
                crate::SortMode::PartitionTopN {
                    partition_by,
                    limit,
                    ..
                } => {
                    validate_partition_ordering(
                        fragment,
                        node,
                        indexes,
                        partition_by,
                        path,
                        errors,
                    );
                    if *limit == 0 {
                        errors.push(ValidationError::new(
                            path,
                            "partition TopN sort requires a non-zero limit",
                        ));
                    }
                }
            }
        }
        NodeKind::TopN {
            order_by,
            limit,
            offset,
            phase,
        } => {
            require_passthrough_output(fragment, node, path, errors);
            validate_ordering_expressions(fragment, node, indexes, order_by, path, errors);
            if order_by.is_empty() {
                errors.push(ValidationError::new(path, "TopN order is empty"));
            }
            if limit.checked_add(*offset).is_none() {
                errors.push(ValidationError::new(
                    path,
                    "TopN limit and offset overflow the row-count domain",
                ));
            }
            if matches!(phase, crate::TopNPhase::Partial { .. }) && *offset != 0 {
                errors.push(ValidationError::new(
                    path,
                    "partial TopN cannot apply an offset before global completion",
                ));
            }
        }
        NodeKind::Limit { .. } => {
            require_passthrough_output(fragment, node, path, errors);
            let singleton = node
                .required_inputs
                .first()
                .is_some_and(|required| required.distribution == Distribution::Singleton)
                && node
                    .inputs
                    .first()
                    .and_then(|input| fragment.nodes().get(input))
                    .is_some_and(|input| {
                        input.output_properties.distribution == Distribution::Singleton
                    });
            if !singleton {
                errors.push(ValidationError::new(
                    path,
                    "global limit requires a singleton input",
                ));
            }
        }
        NodeKind::SetOp {
            kind,
            input_mappings,
        } => {
            if input_mappings.len() != node.inputs.len()
                || input_mappings
                    .iter()
                    .any(|mapping| mapping.len() != node.output.columns.len())
            {
                errors.push(ValidationError::new(
                    path,
                    "set operation mappings must match every input and output ordinal",
                ));
            }
            for (input_ordinal, mapping) in input_mappings.iter().enumerate() {
                let Some(input_values) = node
                    .inputs
                    .get(input_ordinal)
                    .and_then(|input| indexes.output(*input))
                else {
                    continue;
                };
                for (output_ordinal, input_value) in mapping.iter().enumerate() {
                    if !input_values.contains(input_value) {
                        errors.push(ValidationError::new(
                            path,
                            "set operation mapping references a value outside its input port",
                        ));
                    }
                    if let Some(output_value) = node.output.columns.get(output_ordinal)
                        && let (Some(input_value), Some(output_value)) = (
                            fragment.values().get(input_value),
                            fragment.values().get(output_value),
                        )
                        && input_value.ty != output_value.ty
                    {
                        errors.push(ValidationError::new(
                            path,
                            "set operation input type differs from its output ordinal",
                        ));
                    }
                }
            }
            if *kind != crate::SetOperationKind::UnionAll
                && set_operation_output_distribution(fragment, node, *kind).is_none()
            {
                errors.push(ValidationError::new(
                    path,
                    "INTERSECT and EXCEPT require singleton inputs or one exact shared partition space over every comparison ordinal",
                ));
            }
        }
        NodeKind::Values { rows } => {
            if rows
                .iter()
                .any(|row| row.len() != node.output.columns.len())
            {
                errors.push(ValidationError::new(
                    path,
                    "VALUES row width differs from output width",
                ));
            }
            for row in rows {
                for (ordinal, expression) in row.iter().enumerate() {
                    if let Some(output) = node.output.columns.get(ordinal)
                        && let (Some(expression), Some(output)) = (
                            fragment.expressions().get(*expression),
                            fragment.values().get(output),
                        )
                        && expression.ty != output.ty
                    {
                        errors.push(ValidationError::new(
                            path,
                            "VALUES expression type differs from its output ordinal",
                        ));
                    }
                }
            }
        }
        NodeKind::ExchangeSource { edge, imports } => {
            for (source, destination) in imports {
                match fragment.values().get(destination) {
                    Some(value)
                        if matches!(
                            value.origin,
                            ValueOrigin::ExchangeImport {
                                edge: value_edge,
                                source_value
                            } if value_edge == *edge && source_value == *source
                        ) || matches!(
                            value.origin,
                            ValueOrigin::CteImport {
                                edge: value_edge,
                                producer_value,
                                ..
                            } if value_edge == *edge && producer_value == *source
                        ) => {}
                    Some(_) => errors.push(ValidationError::new(
                        path,
                        "exchange import mapping differs from destination value origin",
                    )),
                    None => require_value(fragment, *destination, path, errors),
                }
            }
            if !imports
                .iter()
                .map(|(_, destination)| destination)
                .eq(node.output.columns.iter())
            {
                errors.push(ValidationError::new(
                    path,
                    "exchange output occurrences differ from its exact import sequence",
                ));
            }
        }
        NodeKind::TableWriter { target } => {
            let child = node
                .inputs
                .first()
                .and_then(|child| fragment.nodes().get(child));
            let child_output = child
                .map(|child| child.output.columns.as_ref())
                .unwrap_or_default();
            let child_values = indexes
                .visible_input(node.id)
                .expect("every fragment node has one indexed visible-input port");
            if target.input.as_ref() != child_output {
                errors.push(ValidationError::new(
                    path,
                    "table writer input differs from its exact child output",
                ));
            }
            for value in &target.input {
                require_value(fragment, *value, path, errors);
            }
            validate_distribution(fragment, &target.required_distribution, "writer", errors);
            if target.required_distribution == Distribution::Broadcast
                || child.is_none_or(|child| {
                    child.output_properties.row_multiplicity != RowMultiplicity::SingleCopy
                })
                || node
                    .required_inputs
                    .first()
                    .is_none_or(|required| required.row_multiplicity != RowMultiplicity::SingleCopy)
            {
                errors.push(ValidationError::new(
                    path,
                    "table writer requires a non-replicated owned input distribution",
                ));
            }
            if node
                .required_inputs
                .first()
                .is_none_or(|required| required.distribution != target.required_distribution)
            {
                errors.push(ValidationError::new(
                    path,
                    "table writer has two different input distribution contracts",
                ));
            }
            validate_encoded_payload(
                None,
                &target.handle,
                &[ConnectorCodecCategory::WriteHandle],
                path,
                errors,
            );
            validate_writer_schema(
                fragment,
                &target.output_schema,
                WriterRelationContract::Multiplex,
                Some(node.id),
                path,
                errors,
            );
            let mut target_tokens = BTreeSet::new();
            for field in &target.target_fields {
                if !target_tokens.insert(&field.token) {
                    errors.push(ValidationError::new(
                        path,
                        "table writer repeats a target field token",
                    ));
                }
                match fragment.values().get(&field.input) {
                    Some(value) if value.ty != field.ty => errors.push(ValidationError::new(
                        path,
                        "writer target field type differs from its input value",
                    )),
                    Some(_) => {}
                    None => require_value(fragment, field.input, path, errors),
                }
                if !child_values.contains(&field.input) {
                    errors.push(ValidationError::new(
                        path,
                        "table writer target field is absent from its exact child output",
                    ));
                }
            }
            validate_writer_aggregates(fragment, &target.partial_aggregates, path, errors);
            validate_writer_aggregate_ports(
                fragment,
                &target.partial_aggregates,
                node.id,
                child_values,
                path,
                errors,
            );
        }
        NodeKind::TableFinish(spec) => {
            if spec.expected_target_ordinals.is_empty()
                || spec
                    .expected_target_ordinals
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
            {
                errors.push(ValidationError::new(
                    path,
                    "table finish target ordinals must be non-empty and strictly increasing",
                ));
            }
            if spec.expected_target_ordinals.len() > MAX_CONNECTOR_WRITE_TARGETS {
                errors.push(ValidationError::new(
                    path,
                    "table finish target count exceeds the connector contract bound",
                ));
            }
            validate_writer_schema(
                fragment,
                &spec.input_schema,
                WriterRelationContract::Multiplex,
                None,
                path,
                errors,
            );
            validate_writer_schema(
                fragment,
                &spec.output_schema,
                WriterRelationContract::RootResult,
                Some(node.id),
                path,
                errors,
            );
            validate_writer_aggregates(fragment, &spec.final_aggregates, path, errors);
            let child_output = node
                .inputs
                .first()
                .and_then(|child| fragment.nodes().get(child))
                .map(|child| child.output.columns.as_ref())
                .unwrap_or_default();
            let finish_input_is_single_copy = node
                .inputs
                .first()
                .and_then(|child| fragment.nodes().get(child))
                .is_some_and(|child| {
                    child.output_properties.distribution == Distribution::Singleton
                        && child.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                })
                && node.required_inputs.first().is_some_and(|required| {
                    required.distribution == Distribution::Singleton
                        && required.row_multiplicity == RowMultiplicity::SingleCopy
                });
            if !finish_input_is_single_copy {
                errors.push(ValidationError::new(
                    path,
                    "table finish requires singleton writer results with single-copy ownership",
                ));
            }
            let input_schema_values = spec
                .input_schema
                .fields
                .iter()
                .map(|field| field.value)
                .collect::<Vec<_>>();
            if input_schema_values.as_slice() != child_output {
                errors.push(ValidationError::new(
                    path,
                    "table finish input schema differs from its exact child output",
                ));
            }
            let child_values = indexes
                .visible_input(node.id)
                .expect("every fragment node has one indexed visible-input port");
            if spec
                .final_aggregates
                .iter()
                .any(|call| !child_values.contains(&call.input))
            {
                errors.push(ValidationError::new(
                    path,
                    "table finish aggregate input is absent from its exact child output",
                ));
            }
            validate_writer_aggregate_ports(
                fragment,
                &spec.final_aggregates,
                node.id,
                child_values,
                path,
                errors,
            );
            if spec.final_aggregates.is_empty() != spec.grouped_unpivot.is_none() {
                errors.push(ValidationError::new(
                    path,
                    "table finish final aggregates and grouped Unpivot must be both absent or both present",
                ));
            }
            if let Some(unpivot) = &spec.grouped_unpivot {
                validate_writer_grouped_unpivot(fragment, node.id, spec, unpivot, path, errors);
            }
        }
        NodeKind::TableFunction {
            function,
            arguments,
            outputs,
            left_outer,
        } => {
            if function.result_types.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "table function has an empty relation result schema",
                ));
            }
            validate_function_arguments(
                fragment,
                &function.argument_types,
                arguments,
                path,
                errors,
            );
            if *left_outer && node.inputs.len() != 1 {
                errors.push(ValidationError::new(
                    path,
                    "left-outer table function requires one outer input",
                ));
            }
            let input_values = indexes
                .visible_input(node.id)
                .expect("every fragment node has one indexed visible-input port");
            let mut seen_results = BTreeSet::new();
            for (output_ordinal, output) in outputs.iter().enumerate() {
                match *output {
                    crate::TableFunctionOutput::PassThrough(value) => {
                        if !input_values.contains(&value) {
                            errors.push(ValidationError::new(
                                path,
                                "table function pass-through value is absent from its outer input",
                            ));
                        }
                    }
                    crate::TableFunctionOutput::FunctionResult {
                        result_ordinal,
                        value,
                    } => {
                        let Ok(result_ordinal) = usize::try_from(result_ordinal) else {
                            errors.push(ValidationError::new(
                                path,
                                "table function result ordinal is outside the host range",
                            ));
                            continue;
                        };
                        if !seen_results.insert(result_ordinal) {
                            errors.push(ValidationError::new(
                                path,
                                "table function result ordinal is duplicated",
                            ));
                        }
                        let Some(expected) = function.result_types.get(result_ordinal) else {
                            errors.push(ValidationError::new(
                                path,
                                "table function result ordinal is outside its bound schema",
                            ));
                            continue;
                        };
                        if let Some(definition) = fragment.values().get(&value) {
                            let mut expected = expected.clone();
                            expected.nullable |= *left_outer;
                            if definition.ty != expected {
                                errors.push(ValidationError::new(
                                    path,
                                    "table function result type differs from its bound schema",
                                ));
                            }
                            if !matches!(
                                definition.origin,
                                ValueOrigin::NodeOutput { node: owner, output_ordinal: origin_ordinal }
                                    if owner == node.id
                                        && usize::try_from(origin_ordinal).ok() == Some(output_ordinal)
                            ) {
                                errors.push(ValidationError::new(
                                    path,
                                    "table function result value is not owned by its output occurrence",
                                ));
                            }
                        }
                    }
                }
            }
            if seen_results.len() != function.result_types.len()
                || !(0..function.result_types.len()).all(|ordinal| seen_results.contains(&ordinal))
            {
                errors.push(ValidationError::new(
                    path,
                    "table function outputs do not cover its bound relation result schema exactly",
                ));
            }
            if outputs.len() != node.output.columns.len() {
                errors.push(ValidationError::new(
                    path,
                    "table function output mapping differs from its output port",
                ));
            } else {
                for (mapped, value) in outputs.iter().zip(&node.output.columns) {
                    if mapped.value() != *value {
                        errors.push(ValidationError::new(
                            path,
                            "table function output mapping differs from its output port",
                        ));
                    }
                }
            }
            for value in &node.output.columns {
                if fragment.values().get(value).is_none() {
                    errors.push(ValidationError::new(
                        path,
                        "table function output value is not defined",
                    ));
                }
            }
        }
        NodeKind::AssertOneRow(spec) => {
            require_passthrough_output(fragment, node, path, errors);
            match spec {
                crate::RowCountAssertionSpec::Global { subject, .. } => {
                    if subject.is_empty() {
                        errors.push(ValidationError::new(
                            path,
                            "global row-count assertion subject is empty",
                        ));
                    }
                }
                crate::RowCountAssertionSpec::PerKeyAtMostOne {
                    keys,
                    labels,
                    message,
                } => {
                    if keys.is_empty() || keys.len() != labels.len() {
                        errors.push(ValidationError::new(
                            path,
                            "keyed row-count assertion requires matching non-empty keys and labels",
                        ));
                    }
                    if labels.iter().any(|label| label.is_empty()) || message.is_empty() {
                        errors.push(ValidationError::new(
                            path,
                            "keyed row-count assertion labels and message must be non-empty",
                        ));
                    }
                    let input_values = indexes
                        .visible_input(node.id)
                        .expect("every fragment node has one indexed visible-input port");
                    for value in keys {
                        require_value(fragment, *value, path, errors);
                        if !input_values.contains(value) {
                            errors.push(ValidationError::new(
                                path,
                                "keyed row-count assertion key is absent from its exact child port",
                            ));
                        }
                    }
                }
            }
        }
        NodeKind::Window(spec) => {
            if !spec.partition_by.is_empty() {
                validate_partition_ordering(
                    fragment,
                    node,
                    indexes,
                    &spec.partition_by,
                    path,
                    errors,
                );
            }
            validate_ordering_expressions(fragment, node, indexes, &spec.order_by, path, errors);
            if spec.expressions.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "window node has no function calls",
                ));
            }
            for item in &spec.expressions {
                if let Some(expression) = fragment.expressions().get(item.expression)
                    && let ExprKind::WindowCall {
                        frame: Some(frame), ..
                    } = &expression.kind
                    && frame_has_offset(frame)
                {
                    if frame.units == crate::WindowFrameUnits::Range && spec.order_by.len() != 1 {
                        errors.push(ValidationError::new(
                            path,
                            "RANGE window frame with an offset requires exactly one order key",
                        ));
                    }
                    if frame.units == crate::WindowFrameUnits::Groups && spec.order_by.is_empty() {
                        errors.push(ValidationError::new(
                            path,
                            "GROUPS window frame with an offset requires an order key",
                        ));
                    }
                }
                match (
                    fragment.expressions().get(item.expression),
                    fragment.values().get(&item.output),
                ) {
                    (Some(expression), Some(output))
                        if expression.ty == output.ty
                            && expression.owner == node.id
                            && expression.lambda_scope.is_none()
                            && matches!(expression.kind, ExprKind::WindowCall { .. })
                            && matches!(
                                output.origin,
                                ValueOrigin::Expr { node: owner, expr }
                                    if owner == node.id && expr == item.expression
                            ) => {}
                    (Some(_), Some(_)) => errors.push(ValidationError::new(
                        path,
                        "window output has inconsistent expression, type or origin",
                    )),
                    _ => {}
                }
            }
        }
        NodeKind::Repeat {
            grouping_sets,
            grouping_values,
            grouping_outputs,
        } => {
            let input = node.inputs.first().and_then(|id| fragment.nodes().get(id));
            let input_values = indexes
                .visible_input(node.id)
                .expect("every fragment node has one indexed visible-input port");
            for value in grouping_sets.iter().flatten().chain(
                grouping_outputs
                    .iter()
                    .flat_map(|output| output.arguments.iter()),
            ) {
                if input.is_some() && !input_values.contains(value) {
                    errors.push(ValidationError::new(
                        path,
                        "repeat grouping value is absent from its input port",
                    ));
                }
            }
            let mut grouping_occurrences = BTreeMap::<ValueId, usize>::new();
            for grouping_set in grouping_sets {
                for value in grouping_set.iter().copied().collect::<BTreeSet<_>>() {
                    *grouping_occurrences.entry(value).or_default() += 1;
                }
            }
            let nullable_inputs = grouping_occurrences
                .into_iter()
                .filter_map(|(value, count)| (count < grouping_sets.len()).then_some(value))
                .collect::<BTreeSet<_>>();
            let mappings = grouping_values
                .iter()
                .map(|(input, output)| (*input, *output))
                .collect::<BTreeMap<_, _>>();
            if mappings.len() != grouping_values.len()
                || mappings.keys().copied().collect::<BTreeSet<_>>() != nullable_inputs
            {
                errors.push(ValidationError::new(
                    path,
                    "repeat nullable grouping mappings do not cover the exact nullable inputs",
                ));
            }
            for (input, output) in grouping_values {
                match fragment.values().get(output) {
                    Some(definition)
                        if matches!(
                            definition.origin,
                            ValueOrigin::NullExtended { node: owner, of }
                                if owner == node.id && of == *input
                        ) => {}
                    Some(_) => errors.push(ValidationError::new(
                        path,
                        "repeat grouping output is not the declared NULL extension",
                    )),
                    None => require_value(fragment, *output, path, errors),
                }
            }
        }
        NodeKind::GenerateSeries { start, stop, step } => {
            if node.output.columns.len() != 1 {
                errors.push(ValidationError::new(
                    path,
                    "generate-series requires exactly one output value",
                ));
            }
            let output_type = node
                .output
                .columns
                .first()
                .and_then(|value| fragment.values().get(value))
                .map(|value| &value.ty);
            for argument in [Some(*start), Some(*stop), *step].into_iter().flatten() {
                if let (Some(output_type), Some(argument)) =
                    (output_type, fragment.expressions().get(argument))
                    && &argument.ty != output_type
                {
                    errors.push(ValidationError::new(
                        path,
                        "generate-series argument type differs from its output",
                    ));
                }
            }
        }
        NodeKind::ChangeEventExpand {
            events,
            effect_output,
        } => {
            require_value(fragment, *effect_output, path, errors);
            let effect_is_exact = fragment.values().get(effect_output).is_some_and(|value| {
                value.ty.data_type == DataType::Int8
                    && !value.ty.nullable
                    && matches!(
                        value.origin,
                        ValueOrigin::NodeOutput {
                            node: owner,
                            output_ordinal,
                        } if owner == node.id
                            && usize::try_from(output_ordinal)
                                .ok()
                                .and_then(|ordinal| node.output.columns.get(ordinal))
                                == Some(effect_output)
                    )
            });
            if !effect_is_exact {
                errors.push(ValidationError::new(
                    path,
                    "change-event effect output must be an exact non-null Int8 node output",
                ));
            }
            if events.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "change-event expansion has no events",
                ));
            }
            let output_values = indexes
                .output(node.id)
                .expect("every fragment node has one indexed output port");
            for event in events {
                if let Some(predicate) = event.predicate {
                    require_boolean_expression(fragment, predicate, path, errors);
                }
                let mut assigned_outputs = BTreeMap::new();
                for (output, expression) in &event.assignments {
                    require_value(fragment, *output, path, errors);
                    if *output == *effect_output {
                        errors.push(ValidationError::new(
                            path,
                            "change-event assignment targets the generated effect output",
                        ));
                    }
                    if !output_values.contains(output) {
                        errors.push(ValidationError::new(
                            path,
                            "change-event assignment output is absent from the node output port",
                        ));
                    }
                    if assigned_outputs
                        .insert(*output, expression.is_some())
                        .is_some()
                    {
                        errors.push(ValidationError::new(
                            path,
                            "change-event event contains a duplicate assignment output",
                        ));
                    }
                    if let Some(expression) = expression
                        && let (Some(output), Some(expression)) = (
                            fragment.values().get(output),
                            fragment.expressions().get(*expression),
                        )
                        && output.ty != expression.ty
                    {
                        errors.push(ValidationError::new(
                            path,
                            "change-event assignment type differs from its output",
                        ));
                    }
                }
                for output in node
                    .output
                    .columns
                    .iter()
                    .copied()
                    .filter(|output| output != effect_output)
                {
                    let assigned_non_null = assigned_outputs.get(&output).copied().unwrap_or(false);
                    if !assigned_non_null
                        && fragment
                            .values()
                            .get(&output)
                            .is_some_and(|value| !value.ty.nullable)
                    {
                        errors.push(ValidationError::new(
                            path,
                            "change-event event leaves a non-null output without an expression",
                        ));
                    }
                }
            }
        }
        NodeKind::Unpivot { spec } => validate_unpivot(fragment, node, indexes, spec, path, errors),
    }
}

fn validate_aggregate_arguments(
    fragment: &Fragment,
    binding: &crate::AggregateBinding,
    args: &[ExprId],
    order_by: &[crate::SortExpr],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    match binding.phase {
        AggregatePhase::Single | AggregatePhase::Partial { .. } => {
            let logical_count =
                usize::try_from(binding.logical_argument_count).unwrap_or(usize::MAX);
            if logical_count != args.len()
                || binding.function.argument_types.len() != args.len() + order_by.len()
            {
                errors.push(ValidationError::new(
                    path,
                    "aggregate logical/ORDER BY channel counts differ from the bound signature",
                ));
            } else {
                if binding.function.argument_types[logical_count..]
                    .iter()
                    .any(|argument| matches!(argument, crate::FunctionArgumentType::Lambda { .. }))
                {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate ORDER BY update channels must be scalar values",
                    ));
                }
                let inputs = args
                    .iter()
                    .copied()
                    .chain(order_by.iter().map(|item| item.expr))
                    .collect::<Vec<_>>();
                validate_function_arguments(
                    fragment,
                    &binding.function.argument_types,
                    &inputs,
                    path,
                    errors,
                );
            }
        }
        AggregatePhase::Intermediate { .. } | AggregatePhase::Final { .. } => {
            if args.len() != 1 || !order_by.is_empty() {
                errors.push(ValidationError::new(
                    path,
                    "state-consuming aggregate phase requires exactly one state input",
                ));
            } else if let Some(argument) = fragment.expressions().get(args[0])
                && argument.ty != binding.intermediate_type
            {
                errors.push(ValidationError::new(
                    path,
                    "aggregate state input differs from its bound intermediate type",
                ));
            }
        }
    }
}

fn validate_aggregate_value_inputs(
    fragment: &Fragment,
    binding: &crate::AggregateBinding,
    args: &[ExprId],
    order_by: &[crate::SortExpr],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    validate_aggregate_arguments(fragment, binding, args, order_by, path, errors);
}

fn validate_ordering_expressions(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    ordering: &[crate::SortExpr],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let input_values = node.inputs.first().and_then(|input| indexes.output(*input));
    for key in ordering {
        match expression_value(fragment, key.expr) {
            Some(value) if input_values.is_some_and(|input| input.contains(&value)) => {}
            _ => errors.push(ValidationError::new(
                path,
                "physical ordering key must be a direct value from the exact child port",
            )),
        }
    }
}

fn validate_partition_ordering(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    partition_by: &[crate::SortExpr],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if partition_by.is_empty() {
        errors.push(ValidationError::new(
            path,
            "partitioned ordering requires at least one partition key",
        ));
    }
    validate_ordering_expressions(fragment, node, indexes, partition_by, path, errors);
}

fn remap_properties_through_values(
    input: &crate::PhysicalProperties,
    values: &BTreeMap<ValueId, ValueId>,
) -> crate::PhysicalProperties {
    let remap_keys = |keys: &[ValueId]| {
        keys.iter()
            .map(|key| values.get(key).copied())
            .collect::<Option<Vec<_>>>()
            .map(Vec::into_boxed_slice)
    };
    let distribution = match &input.distribution {
        Distribution::Singleton => Distribution::Singleton,
        Distribution::Broadcast => Distribution::Broadcast,
        Distribution::Hash { keys, scheme } => remap_keys(keys)
            .map(|keys| Distribution::Hash {
                keys,
                scheme: scheme.clone(),
            })
            .unwrap_or(Distribution::Unconstrained),
        Distribution::BucketShuffle { keys, scheme } => remap_keys(keys)
            .map(|keys| Distribution::BucketShuffle {
                keys,
                scheme: scheme.clone(),
            })
            .unwrap_or(Distribution::Unconstrained),
        Distribution::Unconstrained | Distribution::RoundRobin => Distribution::Unconstrained,
    };
    let ordering = input
        .ordering
        .iter()
        .map_while(|key| {
            Some(crate::OrderingKey {
                value: values.get(&key.value).copied()?,
                direction: key.direction,
                null_ordering: key.null_ordering,
            })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    crate::PhysicalProperties {
        distribution,
        row_multiplicity: input.row_multiplicity,
        ordering,
    }
}

fn properties_satisfy(
    actual: &crate::PhysicalProperties,
    required: &crate::PhysicalProperties,
) -> bool {
    let distribution = match &required.distribution {
        Distribution::Unconstrained => true,
        required => &actual.distribution == required,
    };
    distribution
        && actual.row_multiplicity == required.row_multiplicity
        && actual.ordering.len() >= required.ordering.len()
        && actual.ordering[..required.ordering.len()] == *required.ordering
}

fn nest_loop_join_output_distribution(
    fragment: &Fragment,
    node: &PhysicalNode,
    kind: crate::JoinKind,
    distribution: crate::NestLoopJoinDistribution,
    predicate: Option<ExprId>,
) -> Option<Distribution> {
    let inputs = node
        .inputs
        .iter()
        .map(|input| fragment.nodes().get(input))
        .collect::<Option<Vec<_>>>()?;
    if inputs.len() != 2 || node.required_inputs.len() != 2 {
        return None;
    }
    match distribution {
        crate::NestLoopJoinDistribution::Singleton => {
            let complete = inputs
                .iter()
                .zip(&node.required_inputs)
                .all(|(input, required)| {
                    input.output_properties.distribution == Distribution::Singleton
                        && required.distribution == Distribution::Singleton
                        && input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                        && required.row_multiplicity == RowMultiplicity::SingleCopy
                });
            complete.then_some(Distribution::Singleton)
        }
        crate::NestLoopJoinDistribution::BroadcastRight => {
            if !matches!(
                kind,
                crate::JoinKind::Cross
                    | crate::JoinKind::Inner
                    | crate::JoinKind::LeftOuter
                    | crate::JoinKind::LeftSemi
                    | crate::JoinKind::LeftAnti
                    | crate::JoinKind::NullAwareLeftAnti
            ) || inputs[1].output_properties.distribution != Distribution::Broadcast
                || node.required_inputs[1].distribution != Distribution::Broadcast
                || inputs[1].output_properties.row_multiplicity != RowMultiplicity::Replicated
                || node.required_inputs[1].row_multiplicity != RowMultiplicity::Replicated
                || inputs[0].output_properties.row_multiplicity != RowMultiplicity::SingleCopy
                || node.required_inputs[0].row_multiplicity != RowMultiplicity::SingleCopy
                || node.required_inputs[0].distribution != inputs[0].output_properties.distribution
            {
                return None;
            }
            let mut output = inputs[0].output_properties.distribution.clone();
            if output == Distribution::Broadcast
                && predicate.is_some_and(|predicate| {
                    !expressions_are_replica_deterministic(
                        fragment,
                        std::iter::once(predicate),
                        true,
                    )
                })
            {
                output = Distribution::Unconstrained;
            }
            Some(output)
        }
    }
}

fn set_operation_output_distribution(
    fragment: &Fragment,
    node: &PhysicalNode,
    kind: crate::SetOperationKind,
) -> Option<Distribution> {
    let NodeKind::SetOp { input_mappings, .. } = &node.kind else {
        return None;
    };
    let inputs = node
        .inputs
        .iter()
        .map(|input| fragment.nodes().get(input))
        .collect::<Option<Vec<_>>>()?;
    if inputs.len() < 2
        || inputs.len() != input_mappings.len()
        || inputs.len() != node.required_inputs.len()
        || inputs
            .iter()
            .any(|input| input.output_properties.row_multiplicity != RowMultiplicity::SingleCopy)
        || node
            .required_inputs
            .iter()
            .any(|required| required.row_multiplicity != RowMultiplicity::SingleCopy)
    {
        return None;
    }
    if kind == crate::SetOperationKind::UnionAll {
        return Some(
            if inputs
                .iter()
                .all(|input| input.output_properties.distribution == Distribution::Singleton)
            {
                Distribution::Singleton
            } else {
                Distribution::Unconstrained
            },
        );
    }
    let exact_inputs = inputs
        .iter()
        .zip(&node.required_inputs)
        .all(|(input, required)| required.distribution == input.output_properties.distribution);
    if !exact_inputs {
        return None;
    }
    if inputs
        .iter()
        .all(|input| input.output_properties.distribution == Distribution::Singleton)
    {
        return Some(Distribution::Singleton);
    }

    let comparison_pattern = occurrence_equivalence_pattern(input_mappings.first()?);
    if input_mappings
        .iter()
        .skip(1)
        .any(|mapping| occurrence_equivalence_pattern(mapping) != comparison_pattern)
    {
        return None;
    }
    let mut output_sources = BTreeMap::new();
    for (output, source) in node.output.columns.iter().zip(input_mappings.first()?) {
        if output_sources
            .insert(*output, *source)
            .is_some_and(|existing| existing != *source)
        {
            return None;
        }
    }
    let representative_ordinals = comparison_pattern
        .iter()
        .enumerate()
        .filter_map(|(ordinal, representative)| (*representative == ordinal).then_some(ordinal))
        .collect::<Vec<_>>();

    let hash_scheme =
        inputs
            .iter()
            .zip(input_mappings)
            .try_fold(None, |expected, (input, mapping)| {
                match &input.output_properties.distribution {
                    Distribution::Hash { keys, scheme }
                        if mapping_keys_match(keys, mapping, &representative_ordinals) =>
                    {
                        match expected {
                            None => Some(Some(scheme)),
                            Some(expected) if expected == scheme => Some(Some(expected)),
                            Some(_) => None,
                        }
                    }
                    _ => None,
                }
            });
    if let Some(Some(scheme)) = hash_scheme {
        return Some(Distribution::Hash {
            keys: representative_ordinals
                .iter()
                .map(|ordinal| node.output.columns[*ordinal])
                .collect(),
            scheme: scheme.clone(),
        });
    }

    let bucket_scheme =
        inputs
            .iter()
            .zip(input_mappings)
            .try_fold(None, |expected, (input, mapping)| {
                match &input.output_properties.distribution {
                    Distribution::BucketShuffle { keys, scheme }
                        if mapping_keys_match(keys, mapping, &representative_ordinals) =>
                    {
                        match expected {
                            None => Some(Some(scheme)),
                            Some(expected) if expected == scheme => Some(Some(expected)),
                            Some(_) => None,
                        }
                    }
                    _ => None,
                }
            });
    bucket_scheme
        .flatten()
        .map(|scheme| Distribution::BucketShuffle {
            keys: representative_ordinals
                .iter()
                .map(|ordinal| node.output.columns[*ordinal])
                .collect(),
            scheme: scheme.clone(),
        })
}

fn occurrence_equivalence_pattern(values: &[ValueId]) -> Vec<usize> {
    let mut representatives = BTreeMap::new();
    values
        .iter()
        .enumerate()
        .map(|(ordinal, value)| *representatives.entry(*value).or_insert(ordinal))
        .collect()
}

fn mapping_keys_match(keys: &[ValueId], mapping: &[ValueId], ordinals: &[usize]) -> bool {
    keys.iter()
        .copied()
        .eq(ordinals.iter().map(|ordinal| mapping[*ordinal]))
}

fn validate_node_output_properties(
    fragment: &Fragment,
    node: &PhysicalNode,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let empty = crate::PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    };
    let expected = match &node.kind {
        NodeKind::Scan { relation, .. } => {
            if relation.provided_properties().row_multiplicity != RowMultiplicity::SingleCopy
                || relation.provided_properties().distribution == Distribution::Broadcast
            {
                errors.push(ValidationError::new(
                    path,
                    "scan output requires single-copy provider work ownership",
                ));
            }
            Some(relation.provided_properties())
        }
        NodeKind::Filter { predicate } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            let expected = crate::derive_filter_output_properties(
                &input.output_properties,
                expressions_are_replica_deterministic(fragment, std::iter::once(*predicate), true),
            );
            if node.output_properties != expected {
                errors.push(ValidationError::new(
                    path,
                    "filter output properties exceed its deterministic predicate proof",
                ));
            }
            return;
        }
        NodeKind::Limit { .. } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            if input.output_properties.row_multiplicity != RowMultiplicity::SingleCopy
                || node.output_properties
                    != (crate::PhysicalProperties {
                        distribution: Distribution::Singleton,
                        row_multiplicity: RowMultiplicity::SingleCopy,
                        ordering: input.output_properties.ordering.clone(),
                    })
            {
                errors.push(ValidationError::new(
                    path,
                    "limit output properties exceed its replica-equivalence proof",
                ));
            }
            return;
        }
        NodeKind::Project { expressions } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            let expected = crate::derive_project_output_properties(
                &input.output_properties,
                &node.output.columns,
                expressions_are_replica_deterministic(
                    fragment,
                    expressions.iter().map(|(expression, _)| *expression),
                    true,
                ),
            );
            if node.output_properties != expected {
                errors.push(ValidationError::new(
                    path,
                    "project output properties differ from the guarantees preserved by its output",
                ));
            }
            return;
        }
        NodeKind::Sort { order_by, mode } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            let required = node.required_inputs.first();
            let partition_by = match mode {
                crate::SortMode::Global => &[][..],
                crate::SortMode::Analytic { partition_by }
                | crate::SortMode::PartitionTopN { partition_by, .. } => partition_by,
            };
            let partition_values = direct_order_values(fragment, partition_by);
            let expected_ordering = derive_ordering(fragment, partition_by, order_by);
            let distribution_valid = match mode {
                crate::SortMode::Global => {
                    required.is_some_and(|required| {
                        required.distribution == Distribution::Singleton
                            && required.ordering.is_empty()
                    }) && input.output_properties.distribution == Distribution::Singleton
                        && node.output_properties.distribution == Distribution::Singleton
                }
                crate::SortMode::Analytic { .. } | crate::SortMode::PartitionTopN { .. } => {
                    partition_values.is_some_and(|keys| {
                        required.is_some_and(|required| {
                            required.ordering.is_empty()
                                && required.distribution == input.output_properties.distribution
                        }) && input.output_properties.distribution
                            == node.output_properties.distribution
                            && distribution_colocates_by(
                                &input.output_properties.distribution,
                                &keys,
                            )
                    })
                }
            };
            let multiplicity_valid = match mode {
                crate::SortMode::Global => {
                    required.is_some_and(|required| {
                        required.row_multiplicity == RowMultiplicity::SingleCopy
                    }) && input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                        && node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                }
                crate::SortMode::Analytic { .. } | crate::SortMode::PartitionTopN { .. } => {
                    required.is_some_and(|required| {
                        required.row_multiplicity == input.output_properties.row_multiplicity
                    }) && node.output_properties.row_multiplicity
                        == input.output_properties.row_multiplicity
                }
            };
            if !distribution_valid || !multiplicity_valid {
                errors.push(ValidationError::new(
                    path,
                    "sort mode lacks its exact input and output distribution contract",
                ));
            }
            if expected_ordering.as_deref() != Some(node.output_properties.ordering.as_ref()) {
                errors.push(ValidationError::new(
                    path,
                    "sort output ordering differs from its exact partition and order keys",
                ));
            }
            return;
        }
        NodeKind::TopN {
            order_by, phase, ..
        } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            let required = node.required_inputs.first();
            let distribution_valid = match phase {
                crate::TopNPhase::Single | crate::TopNPhase::Final { .. } => {
                    required.is_some_and(|required| {
                        required.distribution == Distribution::Singleton
                            && required.ordering.is_empty()
                    }) && input.output_properties.distribution == Distribution::Singleton
                        && node.output_properties.distribution == Distribution::Singleton
                }
                crate::TopNPhase::Partial { .. } => {
                    required.is_some_and(|required| {
                        required.distribution == input.output_properties.distribution
                            && required.ordering.is_empty()
                    }) && node.output_properties.distribution
                        == input.output_properties.distribution
                }
            };
            let multiplicity_valid = match phase {
                crate::TopNPhase::Single | crate::TopNPhase::Final { .. } => {
                    required.is_some_and(|required| {
                        required.row_multiplicity == RowMultiplicity::SingleCopy
                    }) && input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                        && node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                }
                crate::TopNPhase::Partial { .. } => {
                    required.is_some_and(|required| {
                        required.row_multiplicity == input.output_properties.row_multiplicity
                    }) && node.output_properties.row_multiplicity
                        == input.output_properties.row_multiplicity
                }
            };
            if !distribution_valid || !multiplicity_valid {
                errors.push(ValidationError::new(
                    path,
                    "TopN phase lacks its exact distribution contract",
                ));
            }
            if derive_ordering(fragment, &[], order_by).as_deref()
                != Some(node.output_properties.ordering.as_ref())
            {
                errors.push(ValidationError::new(
                    path,
                    "TopN output ordering differs from its exact order keys",
                ));
            }
            return;
        }
        NodeKind::Window(spec) => {
            validate_window_properties(fragment, node, spec, path, errors);
            return;
        }
        NodeKind::AssertOneRow(spec) => {
            validate_assertion_properties(fragment, node, spec, path, errors);
            return;
        }
        NodeKind::TableFunction {
            function,
            arguments,
            outputs,
            ..
        } => {
            validate_table_function_properties(
                fragment, node, function, arguments, outputs, path, errors,
            );
            return;
        }
        NodeKind::ExchangeSource { .. } => return,
        NodeKind::Values { .. } => {
            let NodeKind::Values { rows } = &node.kind else {
                unreachable!();
            };
            let valid = node.output_properties.ordering.is_empty()
                && match (&node.output_properties.distribution, rows.is_empty()) {
                    (Distribution::Unconstrained, true) | (Distribution::Singleton, _) => {
                        node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
                    }
                    (Distribution::Broadcast, _) => {
                        node.output_properties.row_multiplicity == RowMultiplicity::Replicated
                            && expressions_are_replica_deterministic(
                                fragment,
                                rows.iter().flat_map(|row| row.iter().copied()),
                                false,
                            )
                    }
                    (
                        Distribution::Unconstrained
                        | Distribution::RoundRobin
                        | Distribution::Hash { .. }
                        | Distribution::BucketShuffle { .. },
                        _,
                    ) => false,
                };
            if !valid {
                errors.push(ValidationError::new(
                    path,
                    "VALUES distribution and row multiplicity lack an exact placement proof",
                ));
            }
            return;
        }
        NodeKind::Aggregate { group_by, calls } => {
            let input = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input));
            let output_values = node.output.columns.iter().copied().collect::<BTreeSet<_>>();
            let distribution = input
                .map(|input| match &input.output_properties.distribution {
                    Distribution::Singleton => Distribution::Singleton,
                    Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. }
                        if keys.iter().all(|key| output_values.contains(key)) =>
                    {
                        input.output_properties.distribution.clone()
                    }
                    Distribution::Unconstrained
                    | Distribution::RoundRobin
                    | Distribution::Broadcast
                    | Distribution::Hash { .. }
                    | Distribution::BucketShuffle { .. } => Distribution::Unconstrained,
                })
                .unwrap_or(Distribution::Unconstrained);
            let consumes_complete_groups = calls.is_empty()
                || calls.iter().any(|call| {
                    matches!(
                        call.binding.phase,
                        AggregatePhase::Single | AggregatePhase::Final { .. }
                    )
                });
            if consumes_complete_groups {
                let required = node.required_inputs.first();
                let grouping_values = group_by
                    .iter()
                    .map(|(expression, _)| expression_value(fragment, *expression))
                    .collect::<Option<Vec<_>>>();
                let colocated = input.is_some_and(|input| {
                    required.is_some_and(|required| {
                        required.distribution == input.output_properties.distribution
                    }) && if group_by.is_empty() {
                        input.output_properties.distribution == Distribution::Singleton
                    } else {
                        grouping_values.as_deref().is_some_and(|keys| {
                            distribution_colocates_by(&input.output_properties.distribution, keys)
                        })
                    }
                });
                if !colocated {
                    errors.push(ValidationError::new(
                        path,
                        "aggregate finalization lacks complete group co-location",
                    ));
                }
            }
            let single_copy = input.is_some_and(|input| {
                input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
            }) && node
                .required_inputs
                .first()
                .is_some_and(|required| required.row_multiplicity == RowMultiplicity::SingleCopy);
            if !single_copy
                || node.output_properties
                    != (crate::PhysicalProperties {
                        distribution,
                        row_multiplicity: RowMultiplicity::SingleCopy,
                        ordering: Box::default(),
                    })
            {
                errors.push(ValidationError::new(
                    path,
                    "aggregate output properties differ from its proven input distribution",
                ));
            }
            return;
        }
        NodeKind::HashJoin {
            kind,
            keys,
            build_side,
            distribution,
            residual,
            ..
        } => {
            let inputs = node
                .inputs
                .iter()
                .filter_map(|input| fragment.nodes().get(input))
                .collect::<Vec<_>>();
            if inputs.len() != 2 {
                return;
            }
            let mut output_distribution = match (kind, distribution, build_side) {
                (_, crate::JoinDistribution::Singleton, _) => Distribution::Singleton,
                (crate::JoinKind::Inner, _, crate::JoinSide::Left) => {
                    inputs[1].output_properties.distribution.clone()
                }
                (
                    crate::JoinKind::Inner
                    | crate::JoinKind::LeftOuter
                    | crate::JoinKind::LeftSemi
                    | crate::JoinKind::LeftAnti
                    | crate::JoinKind::NullAwareLeftAnti,
                    _,
                    _,
                ) => inputs[0].output_properties.distribution.clone(),
                (
                    crate::JoinKind::RightOuter
                    | crate::JoinKind::RightSemi
                    | crate::JoinKind::RightAnti,
                    _,
                    _,
                ) => inputs[1].output_properties.distribution.clone(),
                (crate::JoinKind::FullOuter | crate::JoinKind::Cross, _, _) => {
                    Distribution::Unconstrained
                }
            };
            if output_distribution == Distribution::Broadcast
                && (inputs
                    .iter()
                    .any(|input| input.output_properties.distribution != Distribution::Broadcast)
                    || !expressions_are_replica_deterministic(
                        fragment,
                        keys.iter()
                            .flat_map(|key| [key.left, key.right])
                            .chain(residual.iter().copied()),
                        true,
                    ))
            {
                output_distribution = Distribution::Unconstrained;
            }
            if node.output_properties
                != (crate::PhysicalProperties {
                    distribution: output_distribution,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                })
            {
                errors.push(ValidationError::new(
                    path,
                    "hash join output properties differ from its preserved partition side",
                ));
            }
            return;
        }
        NodeKind::NestLoopJoin {
            kind,
            distribution,
            predicate,
            ..
        } => {
            let Some(distribution) = nest_loop_join_output_distribution(
                fragment,
                node,
                *kind,
                *distribution,
                *predicate,
            ) else {
                return;
            };
            if node.output_properties
                != (crate::PhysicalProperties {
                    distribution,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                })
            {
                errors.push(ValidationError::new(
                    path,
                    "nested-loop join output properties differ from its execution placement",
                ));
            }
            return;
        }
        NodeKind::SetOp { kind, .. } => {
            let Some(distribution) = set_operation_output_distribution(fragment, node, *kind)
            else {
                return;
            };
            if node.output_properties
                != (crate::PhysicalProperties {
                    distribution,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                })
            {
                errors.push(ValidationError::new(
                    path,
                    "set operation output properties differ from its equality co-location proof",
                ));
            }
            return;
        }
        NodeKind::Repeat { .. } | NodeKind::Unpivot { .. } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            let value_mapping = match &node.kind {
                NodeKind::Unpivot { spec } => {
                    spec.passthrough.iter().copied().collect::<BTreeMap<_, _>>()
                }
                NodeKind::Repeat { .. } => input
                    .output
                    .columns
                    .iter()
                    .copied()
                    .filter(|value| node.output.columns.contains(value))
                    .map(|value| (value, value))
                    .collect::<BTreeMap<_, _>>(),
                _ => unreachable!(),
            };
            let expected =
                remap_properties_through_values(&input.output_properties, &value_mapping);
            if node.output_properties != expected {
                errors.push(ValidationError::new(
                    path,
                    "row-expanding operator output properties differ from its exact passthrough mapping",
                ));
            }
            return;
        }
        NodeKind::ChangeEventExpand { .. } => {
            let Some(input) = node
                .inputs
                .first()
                .and_then(|input| fragment.nodes().get(input))
            else {
                return;
            };
            if node.output_properties
                != (crate::PhysicalProperties {
                    distribution: Distribution::Unconstrained,
                    row_multiplicity: input.output_properties.row_multiplicity,
                    ordering: Box::default(),
                })
            {
                errors.push(ValidationError::new(
                    path,
                    "change-event expansion must declare unconstrained output properties",
                ));
            }
            return;
        }
        NodeKind::GenerateSeries { .. } => {
            if node.output_properties
                != (crate::PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                })
            {
                errors.push(ValidationError::new(
                    path,
                    "generate-series requires singleton placement with single-copy row ownership",
                ));
            }
            return;
        }
        NodeKind::TableWriter { .. } => Some(&empty),
        NodeKind::TableFinish(_) => {
            let finish = crate::PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            };
            if node.output_properties != finish {
                errors.push(ValidationError::new(
                    path,
                    "table finish output requires singleton placement with single-copy ownership",
                ));
            }
            return;
        }
    };
    if expected.is_some_and(|expected| expected != &node.output_properties) {
        errors.push(ValidationError::new(
            path,
            "node output properties are not proven by its operator semantics",
        ));
    }
}

fn expressions_are_replica_deterministic(
    fragment: &Fragment,
    expressions: impl IntoIterator<Item = ExprId>,
    allow_values: bool,
) -> bool {
    crate::expressions_are_replica_deterministic(fragment.expressions(), expressions, allow_values)
}

fn direct_order_values(fragment: &Fragment, ordering: &[crate::SortExpr]) -> Option<Vec<ValueId>> {
    ordering
        .iter()
        .map(|item| expression_value(fragment, item.expr))
        .collect()
}

fn derive_ordering(
    fragment: &Fragment,
    partition_by: &[crate::SortExpr],
    order_by: &[crate::SortExpr],
) -> Option<Vec<crate::OrderingKey>> {
    partition_by
        .iter()
        .chain(order_by)
        .map(|item| {
            expression_value(fragment, item.expr).map(|value| crate::OrderingKey {
                value,
                direction: item.direction,
                null_ordering: item.null_ordering,
            })
        })
        .collect()
}

fn distribution_colocates_by(distribution: &Distribution, keys: &[ValueId]) -> bool {
    let key_index = ValuePortIndex::new(keys);
    match distribution {
        Distribution::Singleton => true,
        Distribution::Hash {
            keys: distribution_keys,
            ..
        }
        | Distribution::BucketShuffle {
            keys: distribution_keys,
            ..
        } => distribution_keys.iter().all(|key| key_index.contains(key)),
        Distribution::Unconstrained | Distribution::RoundRobin | Distribution::Broadcast => false,
    }
}

fn validate_window_properties(
    fragment: &Fragment,
    node: &PhysicalNode,
    spec: &crate::WindowSpec,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let Some(input) = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
    else {
        return;
    };
    let Some(required) = node.required_inputs.first() else {
        return;
    };
    let expected_ordering = derive_ordering(fragment, &spec.partition_by, &spec.order_by);
    let partition_values = direct_order_values(fragment, &spec.partition_by);
    let distribution_valid = if spec.partition_by.is_empty() {
        required.distribution == Distribution::Singleton
            && input.output_properties.distribution == Distribution::Singleton
            && node.output_properties.distribution == Distribution::Singleton
    } else {
        partition_values.is_some_and(|keys| {
            properties_satisfy(&input.output_properties, required)
                && input.output_properties.distribution == node.output_properties.distribution
                && distribution_colocates_by(&input.output_properties.distribution, &keys)
        })
    };
    let single_copy = required.row_multiplicity == RowMultiplicity::SingleCopy
        && input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
        && node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy;
    if !distribution_valid || !single_copy {
        errors.push(ValidationError::new(
            path,
            "window partition lacks its exact input and output distribution contract",
        ));
    }
    if expected_ordering.as_deref().is_none_or(|expected| {
        required.ordering.len() < expected.len()
            || required.ordering[..expected.len()] != *expected
            || input.output_properties.ordering.len() < expected.len()
            || input.output_properties.ordering[..expected.len()] != *expected
    }) || input.output_properties != node.output_properties
    {
        errors.push(ValidationError::new(
            path,
            "window child and output properties differ from its exact partition ordering",
        ));
    }
}

fn validate_assertion_properties(
    fragment: &Fragment,
    node: &PhysicalNode,
    spec: &crate::RowCountAssertionSpec,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let Some(input) = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
    else {
        return;
    };
    let Some(required) = node.required_inputs.first() else {
        return;
    };
    let distribution_valid = match spec {
        crate::RowCountAssertionSpec::Global { .. } => {
            required.distribution == Distribution::Singleton
                && input.output_properties.distribution == Distribution::Singleton
                && node.output_properties.distribution == Distribution::Singleton
        }
        crate::RowCountAssertionSpec::PerKeyAtMostOne { keys, .. } => {
            properties_satisfy(&input.output_properties, required)
                && input.output_properties.distribution == node.output_properties.distribution
                && distribution_colocates_by(&input.output_properties.distribution, keys)
        }
    };
    let single_copy = required.row_multiplicity == RowMultiplicity::SingleCopy
        && input.output_properties.row_multiplicity == RowMultiplicity::SingleCopy
        && node.output_properties.row_multiplicity == RowMultiplicity::SingleCopy;
    if !distribution_valid || !single_copy || !required.ordering.is_empty() {
        errors.push(ValidationError::new(
            path,
            "row-count assertion lacks its exact distribution contract",
        ));
    }
    if node.output_properties != input.output_properties {
        errors.push(ValidationError::new(
            path,
            "row-count assertion does not preserve its child properties",
        ));
    }
}

fn validate_table_function_properties(
    fragment: &Fragment,
    node: &PhysicalNode,
    function: &crate::BoundTableFunction,
    arguments: &[ExprId],
    outputs: &[crate::TableFunctionOutput],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let Some(input) = node
        .inputs
        .first()
        .and_then(|input| fragment.nodes().get(input))
    else {
        if node.output_properties
            != (crate::PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            })
        {
            errors.push(ValidationError::new(
                path,
                "standalone table function requires singleton placement with single-copy ownership",
            ));
        }
        return;
    };
    let passthrough = outputs
        .iter()
        .filter_map(|output| match output {
            crate::TableFunctionOutput::PassThrough(value) => Some(*value),
            crate::TableFunctionOutput::FunctionResult { .. } => None,
        })
        .collect::<BTreeSet<_>>();
    let distribution = match &input.output_properties.distribution {
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. }
            if keys.iter().all(|key| passthrough.contains(key)) =>
        {
            input.output_properties.distribution.clone()
        }
        Distribution::Hash { .. } | Distribution::BucketShuffle { .. } => {
            Distribution::Unconstrained
        }
        Distribution::Broadcast
            if function.volatility != crate::FunctionVolatility::Immutable
                || !expressions_are_replica_deterministic(
                    fragment,
                    arguments.iter().copied(),
                    true,
                ) =>
        {
            Distribution::Unconstrained
        }
        distribution => distribution.clone(),
    };
    let ordering_len = input
        .output_properties
        .ordering
        .iter()
        .take_while(|key| passthrough.contains(&key.value))
        .count();
    let ordering = Box::from(&input.output_properties.ordering[..ordering_len]);
    if node.output_properties
        != (crate::PhysicalProperties {
            distribution,
            row_multiplicity: input.output_properties.row_multiplicity,
            ordering,
        })
    {
        errors.push(ValidationError::new(
            path,
            "table function output properties differ from its explicit passthrough guarantees",
        ));
    }
}

fn validate_property_keys_on_port(
    fragment: &Fragment,
    properties: &crate::PhysicalProperties,
    port_values: &ValuePortIndex,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if properties.distribution == Distribution::Broadcast
        && properties.row_multiplicity != RowMultiplicity::Replicated
    {
        errors.push(ValidationError::new(
            path,
            "broadcast physical properties require replicated row multiplicity",
        ));
    }
    let mut keys = properties
        .ordering
        .iter()
        .map(|key| key.value)
        .collect::<Vec<_>>();
    match &properties.distribution {
        Distribution::Hash {
            keys: partition_keys,
            ..
        }
        | Distribution::BucketShuffle {
            keys: partition_keys,
            ..
        } => keys.extend(partition_keys.iter().copied()),
        Distribution::Unconstrained
        | Distribution::Singleton
        | Distribution::RoundRobin
        | Distribution::Broadcast => {}
    }
    for key in keys {
        require_value(fragment, key, path, errors);
        if !port_values.contains(&key) {
            errors.push(ValidationError::new(
                path,
                format!(
                    "physical property key {} is absent from the port",
                    key.get()
                ),
            ));
        }
    }
}

fn validate_expression_values_on_port(
    fragment: &Fragment,
    roots: &[ExprId],
    port_values: &ValuePortIndex,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let mut visited = BTreeSet::new();
    let mut pending = roots.to_vec();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let Some(expression) = fragment.expressions().get(id) else {
            continue;
        };
        if let ExprKind::Value(value) = expression.kind
            && !port_values.contains(&value)
        {
            errors.push(ValidationError::new(
                path,
                format!(
                    "join key expression references value {} from the wrong input",
                    value.get()
                ),
            ));
        }
        expression.kind.expression_references(&mut pending);
    }
}

fn import_origin_matches(
    origin: &ValueOrigin,
    edge: EdgeId,
    kind: crate::EdgeKind,
    source_fragment: FragmentId,
    source_value: ValueId,
) -> bool {
    match (kind, origin) {
        (
            crate::EdgeKind::CteMulticast,
            ValueOrigin::CteImport {
                edge: value_edge,
                producer_fragment,
                producer_value,
            },
        ) => {
            *value_edge == edge
                && *producer_fragment == source_fragment
                && *producer_value == source_value
        }
        (
            crate::EdgeKind::Stream | crate::EdgeKind::ChangeStreamRouter,
            ValueOrigin::ExchangeImport {
                edge: value_edge,
                source_value: value_source,
            },
        ) => *value_edge == edge && *value_source == source_value,
        _ => false,
    }
}

fn expression_value(fragment: &Fragment, expression: ExprId) -> Option<ValueId> {
    match &fragment.expressions().get(expression)?.kind {
        ExprKind::Value(value) => Some(*value),
        _ => None,
    }
}

#[derive(Clone, Copy)]
enum WriterRelationContract {
    Multiplex,
    RootResult,
}

fn validate_writer_schema(
    fragment: &Fragment,
    schema: &crate::WriterRelationSchema,
    contract: WriterRelationContract,
    owner: Option<NodeId>,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let expected_revision = match contract {
        WriterRelationContract::Multiplex => crate::WRITER_MULTIPLEX_SCHEMA_REVISION,
        WriterRelationContract::RootResult => crate::ROOT_WRITE_RESULT_SCHEMA_REVISION,
    };
    let valid_width = match contract {
        WriterRelationContract::Multiplex => schema.fields.len() >= 4,
        WriterRelationContract::RootResult => schema.fields.len() == 8,
    };
    if schema.revision != expected_revision || !valid_width {
        errors.push(ValidationError::new(
            path,
            "writer relation schema has an unsupported revision or width",
        ));
    }
    let mut names = BTreeSet::new();
    let mut values = BTreeSet::new();
    for (ordinal, field) in schema.fields.iter().enumerate() {
        if field.name.is_empty()
            || !names.insert(field.name.as_ref())
            || !values.insert(field.value)
        {
            errors.push(ValidationError::new(
                path,
                "writer relation schema contains an empty/duplicate field or value",
            ));
        }
        match fragment.values().get(&field.value) {
            Some(value) if value.ty != field.ty => errors.push(ValidationError::new(
                path,
                "writer relation field type differs from its value",
            )),
            Some(_) => {}
            None => require_value(fragment, field.value, path, errors),
        }
        if !writer_relation_field_matches(contract, ordinal, field, schema.fields.len()) {
            errors.push(ValidationError::new(
                path,
                "writer relation field differs from its closed role contract",
            ));
        }
        if let Some(owner) = owner {
            let expected = match (field.role, contract) {
                (crate::WriterRelationFieldRole::Kind, _) => crate::WriterDerivedKind::RelationKind,
                (crate::WriterRelationFieldRole::TargetOrdinal, _) => {
                    crate::WriterDerivedKind::WriteTargetOrdinal
                }
                (crate::WriterRelationFieldRole::RowCount, _) => {
                    crate::WriterDerivedKind::AffectedRows
                }
                (crate::WriterRelationFieldRole::CommitFragment, _) => {
                    crate::WriterDerivedKind::CommitFragment
                }
                (crate::WriterRelationFieldRole::Auxiliary, _) => {
                    crate::WriterDerivedKind::RelationAuxiliary
                }
            };
            if !fragment.values().get(&field.value).is_some_and(|value| {
                matches!(
                    value.origin,
                    ValueOrigin::WriterDerived { writer_node, kind }
                        if writer_node == owner && kind == expected
                )
            }) {
                errors.push(ValidationError::new(
                    path,
                    "writer relation field origin differs from its role and owner",
                ));
            }
        }
    }
}

fn writer_relation_field_matches(
    contract: WriterRelationContract,
    actual_ordinal: usize,
    field: &crate::WriterRelationField,
    field_count: usize,
) -> bool {
    let ordinal = match contract {
        WriterRelationContract::Multiplex => match field.role {
            crate::WriterRelationFieldRole::Kind => 0,
            crate::WriterRelationFieldRole::TargetOrdinal => 1,
            crate::WriterRelationFieldRole::RowCount => 2,
            crate::WriterRelationFieldRole::CommitFragment => 3,
            crate::WriterRelationFieldRole::Auxiliary => {
                return actual_ordinal >= 4
                    && field_count > 4
                    && field.ty.nullable
                    && field.ty.data_type != DataType::Null;
            }
        },
        WriterRelationContract::RootResult => match field.role {
            crate::WriterRelationFieldRole::Kind => 0,
            crate::WriterRelationFieldRole::TargetOrdinal => 1,
            crate::WriterRelationFieldRole::RowCount => 2,
            crate::WriterRelationFieldRole::CommitFragment => 3,
            crate::WriterRelationFieldRole::Auxiliary => match field.name.as_ref() {
                "input_fields" => 4,
                "blob_type" => 5,
                "body" => 6,
                "properties" => 7,
                _ => return false,
            },
        },
    };
    if actual_ordinal != ordinal {
        return false;
    }
    match (contract, ordinal) {
        (_, 0) => {
            field.name.as_ref() == "kind"
                && field.role == crate::WriterRelationFieldRole::Kind
                && field.ty == crate::ValueType::new(DataType::Int8, false)
        }
        (WriterRelationContract::Multiplex, 1) => {
            field.name.as_ref() == "write_target_ordinal"
                && field.role == crate::WriterRelationFieldRole::TargetOrdinal
                && field.ty == crate::ValueType::new(DataType::Int32, false)
        }
        (WriterRelationContract::RootResult, 1) => {
            field.name.as_ref() == "write_target_ordinal"
                && field.role == crate::WriterRelationFieldRole::TargetOrdinal
                && field.ty == crate::ValueType::new(DataType::Int32, true)
        }
        (_, 2) => {
            field.name.as_ref() == "row_count"
                && field.role == crate::WriterRelationFieldRole::RowCount
                && field.ty == crate::ValueType::new(DataType::Int64, true)
        }
        (_, 3) => {
            field.name.as_ref() == "commit_fragment"
                && field.role == crate::WriterRelationFieldRole::CommitFragment
                && field.ty == crate::ValueType::new(DataType::Binary, true)
        }
        (WriterRelationContract::RootResult, 4) => {
            matches!(
                &field.ty.data_type,
                DataType::List(item)
                    if item.name() == "item"
                        && item.data_type() == &DataType::Int32
                        && !item.is_nullable()
                        && item.metadata().is_empty()
            ) && field.ty.nullable
        }
        (WriterRelationContract::RootResult, 5) => {
            field.ty == crate::ValueType::new(DataType::Utf8, true)
        }
        (WriterRelationContract::RootResult, 6) => {
            field.ty == crate::ValueType::new(DataType::Binary, true)
        }
        (WriterRelationContract::RootResult, 7) => {
            matches!(
                &field.ty.data_type,
                DataType::Map(entries, false)
                    if entries.name() == "entries"
                        && !entries.is_nullable()
                        && entries.metadata().is_empty()
                        && matches!(entries.data_type(), DataType::Struct(fields)
                            if fields.len() == 2
                                && fields[0].name() == "key"
                                && fields[0].data_type() == &DataType::Utf8
                                && !fields[0].is_nullable()
                                && fields[0].metadata().is_empty()
                                && fields[1].name() == "value"
                                && fields[1].data_type() == &DataType::Utf8
                                && !fields[1].is_nullable()
                                && fields[1].metadata().is_empty())
            ) && field.ty.nullable
        }
        _ => false,
    }
}

fn validate_writer_aggregates(
    fragment: &Fragment,
    calls: &[crate::WriterAggregateCall],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    for call in calls {
        if call.binding.function.kind != FunctionKind::Aggregate {
            errors.push(ValidationError::new(
                path,
                "writer aggregate has non-aggregate binding",
            ));
        }
        if call.binding.logical_argument_count != 1
            || call.binding.function.argument_types.len() != 1
        {
            errors.push(ValidationError::new(
                path,
                "writer aggregate carrier supports exactly one logical argument",
            ));
        }
        require_value(fragment, call.input, path, errors);
        require_value(fragment, call.output, path, errors);
        let expected_input = match call.binding.phase {
            AggregatePhase::Single | AggregatePhase::Partial { .. } => {
                match call.binding.function.argument_types.first() {
                    Some(crate::FunctionArgumentType::Value(value)) => Some(value),
                    Some(crate::FunctionArgumentType::Lambda { .. }) | None => {
                        errors.push(ValidationError::new(
                            path,
                            "writer aggregate requires one scalar value argument",
                        ));
                        None
                    }
                }
            }
            AggregatePhase::Intermediate { .. } | AggregatePhase::Final { .. } => {
                Some(&call.binding.intermediate_type)
            }
        };
        if let (Some(expected), Some(actual)) = (expected_input, fragment.values().get(&call.input))
            && expected != &actual.ty
        {
            errors.push(ValidationError::new(
                path,
                "writer aggregate input type differs from its phase contract",
            ));
        }
        let expected_output = match call.binding.phase {
            AggregatePhase::Single | AggregatePhase::Final { .. } => {
                &call.binding.function.result_type
            }
            AggregatePhase::Partial { .. } | AggregatePhase::Intermediate { .. } => {
                &call.binding.intermediate_type
            }
        };
        if let Some(actual) = fragment.values().get(&call.output)
            && &actual.ty != expected_output
        {
            errors.push(ValidationError::new(
                path,
                "writer aggregate output type differs from its phase contract",
            ));
        }
    }
}

fn validate_writer_aggregate_ports(
    fragment: &Fragment,
    calls: &[crate::WriterAggregateCall],
    owner: NodeId,
    child_values: &VisibleInputIndex,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let mut outputs = BTreeSet::new();
    for call in calls {
        if !child_values.contains(&call.input) {
            errors.push(ValidationError::new(
                path,
                "writer aggregate input is absent from its exact child output",
            ));
        }
        if !outputs.insert(call.output) {
            errors.push(ValidationError::new(
                path,
                "writer aggregate output is duplicated",
            ));
        }
        if !fragment.values().get(&call.output).is_some_and(|value| {
            matches!(
                value.origin,
                ValueOrigin::WriterDerived { writer_node, .. } if writer_node == owner
            )
        }) {
            errors.push(ValidationError::new(
                path,
                "writer aggregate output is not owned by this writer node",
            ));
        }
    }
}

fn validate_unpivot(
    fragment: &Fragment,
    node: &PhysicalNode,
    indexes: &FragmentValidationIndexes,
    spec: &crate::UnpivotSpec,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if spec.mappings.is_empty() {
        errors.push(ValidationError::new(path, "unpivot requires mappings"));
    }
    if !validate_unpivot_resource_limits(
        fragment,
        spec.mappings.len(),
        spec.mappings
            .iter()
            .map(|mapping| mapping.constants.as_ref()),
        spec.max_output_rows,
        spec.max_output_bytes,
        path,
        errors,
    ) {
        return;
    }
    let child_values = indexes
        .visible_input(node.id)
        .expect("every fragment node has one indexed visible-input port");
    let mut output_roles = BTreeSet::new();
    for (input, output) in &spec.passthrough {
        require_value(fragment, *input, path, errors);
        require_value(fragment, *output, path, errors);
        if !child_values.contains(input) {
            errors.push(ValidationError::new(
                path,
                "unpivot passthrough input is absent from its exact child output",
            ));
        }
        if !output_roles.insert(*output) {
            errors.push(ValidationError::new(
                path,
                "unpivot output roles contain a duplicate value",
            ));
        }
        if let (Some(input), Some(output)) =
            (fragment.values().get(input), fragment.values().get(output))
            && input.ty != output.ty
        {
            errors.push(ValidationError::new(
                path,
                "unpivot passthrough changes the value type",
            ));
        }
    }
    require_value(fragment, spec.value_output, path, errors);
    if !output_roles.insert(spec.value_output) {
        errors.push(ValidationError::new(
            path,
            "unpivot output roles contain a duplicate value",
        ));
    }
    for value in &spec.literal_outputs {
        require_value(fragment, *value, path, errors);
        if !output_roles.insert(*value) {
            errors.push(ValidationError::new(
                path,
                "unpivot output roles contain a duplicate value",
            ));
        }
    }
    if output_roles.len() != node.output.columns.len()
        || !node
            .output
            .columns
            .iter()
            .all(|output| output_roles.contains(output))
    {
        errors.push(ValidationError::new(
            path,
            "unpivot output roles do not exactly cover the node output port",
        ));
    }
    let mut value_nullable = false;
    let mut literal_nullable = vec![false; spec.literal_outputs.len()];
    for mapping in &spec.mappings {
        require_value(fragment, mapping.input, path, errors);
        if !child_values.contains(&mapping.input) {
            errors.push(ValidationError::new(
                path,
                "unpivot mapping input is absent from its exact child output",
            ));
        }
        if let (Some(input), Some(output)) = (
            fragment.values().get(&mapping.input),
            fragment.values().get(&spec.value_output),
        ) {
            if input.ty.data_type != output.ty.data_type {
                errors.push(ValidationError::new(
                    path,
                    "unpivot mapping input type differs from its value output",
                ));
            }
            value_nullable |= input.ty.nullable;
        }
        if mapping.constants.len() != spec.literal_outputs.len() {
            errors.push(ValidationError::new(
                path,
                "unpivot constant width differs from literal output width",
            ));
        }
        for (index, (constant, output)) in mapping
            .constants
            .iter()
            .zip(&spec.literal_outputs)
            .enumerate()
        {
            if let Some(nullable) =
                validate_unpivot_constant(fragment, constant, *output, "unpivot", path, errors)
            {
                literal_nullable[index] |= nullable;
            }
        }
    }
    if fragment
        .values()
        .get(&spec.value_output)
        .is_some_and(|output| output.ty.nullable != value_nullable)
    {
        errors.push(ValidationError::new(
            path,
            "unpivot value output nullability differs from its mapping inputs",
        ));
    }
    for (output, nullable) in spec.literal_outputs.iter().zip(literal_nullable) {
        if fragment
            .values()
            .get(output)
            .is_some_and(|output| output.ty.nullable != nullable)
        {
            errors.push(ValidationError::new(
                path,
                "unpivot literal output nullability differs from its constants",
            ));
        }
    }
}

fn validate_writer_grouped_unpivot(
    fragment: &Fragment,
    owner: NodeId,
    finish: &crate::WriterFinishSpec,
    spec: &crate::WriterGroupedUnpivotSpec,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if finish.final_aggregates.is_empty() || spec.mappings.is_empty() {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot requires final aggregates and mappings",
        ));
    }
    if !validate_unpivot_resource_limits(
        fragment,
        spec.mappings.len(),
        spec.mappings
            .iter()
            .map(|mapping| mapping.constants.as_ref()),
        spec.max_output_rows,
        spec.max_output_bytes,
        path,
        errors,
    ) {
        return;
    }

    let input_values = finish
        .input_schema
        .fields
        .iter()
        .map(|field| field.value)
        .collect::<BTreeSet<_>>();
    let output_fields = finish
        .output_schema
        .fields
        .iter()
        .map(|field| (field.value, field.role))
        .collect::<BTreeMap<_, _>>();
    let aggregate_outputs = finish
        .final_aggregates
        .iter()
        .map(|call| call.output)
        .collect::<BTreeSet<_>>();
    let write_targets = finish
        .expected_target_ordinals
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let statistics_targets = spec
        .statistics_target_ordinals
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if spec.statistics_target_ordinals.is_empty()
        || statistics_targets.len() != spec.statistics_target_ordinals.len()
        || spec
            .statistics_target_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot statistics target ordinals must be non-empty and strictly increasing",
        ));
    }
    if !statistics_targets.is_subset(&write_targets) {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot statistics targets are not write targets",
        ));
    }

    if !input_values.contains(&spec.grouping_input) {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot grouping input is absent from the finish input schema",
        ));
    }
    if finish
        .input_schema
        .fields
        .iter()
        .find(|field| field.value == spec.grouping_input)
        .is_none_or(|field| field.role != crate::WriterRelationFieldRole::TargetOrdinal)
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot grouping input is not the finish target ordinal field",
        ));
    }
    for (label, value) in [
        ("grouping input", spec.grouping_input),
        ("grouping output", spec.grouping_output),
    ] {
        match fragment.values().get(&value) {
            Some(definition) if definition.ty == crate::ValueType::new(DataType::Int32, false) => {}
            Some(_) => errors.push(ValidationError::new(
                path,
                format!("writer grouped Unpivot {label} must be non-null Int32"),
            )),
            None => require_value(fragment, value, path, errors),
        }
    }
    match fragment.values().get(&spec.passthrough_output) {
        Some(definition) if definition.ty.data_type == DataType::Int32 => {}
        Some(_) => errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot passthrough output must be Int32",
        )),
        None => require_value(fragment, spec.passthrough_output, path, errors),
    }
    if !fragment
        .values()
        .get(&spec.grouping_output)
        .is_some_and(|value| {
            matches!(
                value.origin,
                ValueOrigin::WriterDerived {
                    writer_node,
                    kind: crate::WriterDerivedKind::GroupingKey,
                } if writer_node == owner
            )
        })
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot grouping output is not owned by this finish node",
        ));
    }
    if output_fields.get(&spec.passthrough_output)
        != Some(&crate::WriterRelationFieldRole::TargetOrdinal)
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot passthrough output is not the finish target ordinal field",
        ));
    }
    if !fragment
        .values()
        .get(&spec.passthrough_output)
        .is_some_and(|value| {
            matches!(
                value.origin,
                ValueOrigin::WriterDerived {
                    writer_node,
                    kind: crate::WriterDerivedKind::WriteTargetOrdinal,
                } if writer_node == owner
            )
        })
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot passthrough output is not this finish node's target ordinal",
        ));
    }
    if output_fields.get(&spec.value_output) != Some(&crate::WriterRelationFieldRole::Auxiliary)
        || spec.literal_outputs.iter().any(|value| {
            output_fields.get(value) != Some(&crate::WriterRelationFieldRole::Auxiliary)
        })
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot value and literal outputs are not auxiliary finish fields",
        ));
    }
    let unpivot_auxiliary_outputs = std::iter::once(spec.value_output)
        .chain(spec.literal_outputs.iter().copied())
        .collect::<BTreeSet<_>>();
    let schema_auxiliary_outputs = finish
        .output_schema
        .fields
        .iter()
        .filter_map(|field| {
            (field.role == crate::WriterRelationFieldRole::Auxiliary).then_some(field.value)
        })
        .collect::<BTreeSet<_>>();
    if unpivot_auxiliary_outputs.len() != spec.literal_outputs.len() + 1
        || unpivot_auxiliary_outputs != schema_auxiliary_outputs
    {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot roles do not exactly cover distinct auxiliary finish fields",
        ));
    }

    let mut mapped_targets = BTreeSet::new();
    let mut mapped_inputs = BTreeSet::new();
    let mut mapping_keys = BTreeSet::new();
    let mut previous_target = None;
    for mapping in &spec.mappings {
        if previous_target.is_some_and(|previous| previous > mapping.write_target_ordinal) {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot mappings are not ordered by target ordinal",
            ));
        }
        previous_target = Some(mapping.write_target_ordinal);
        if !statistics_targets.contains(&mapping.write_target_ordinal) {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot mapping names an unexpected write target",
            ));
        }
        mapped_targets.insert(mapping.write_target_ordinal);
        if !mapping_keys.insert((mapping.write_target_ordinal, mapping.input)) {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot repeats a target and aggregate output mapping",
            ));
        }
        if !aggregate_outputs.contains(&mapping.input) {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot mapping input is not a final aggregate output",
            ));
        }
        if let (Some(input), Some(output)) = (
            fragment.values().get(&mapping.input),
            fragment.values().get(&spec.value_output),
        ) && input.ty.data_type != output.ty.data_type
        {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot aggregate type differs from its value output",
            ));
        }
        mapped_inputs.insert(mapping.input);
        if mapping.constants.len() != spec.literal_outputs.len() {
            errors.push(ValidationError::new(
                path,
                "writer grouped Unpivot constant width differs from its literal output width",
            ));
        }
        for (constant, output) in mapping.constants.iter().zip(&spec.literal_outputs) {
            validate_unpivot_constant(
                fragment,
                constant,
                *output,
                "writer grouped Unpivot",
                path,
                errors,
            );
        }
    }
    if mapped_targets != statistics_targets {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot mappings do not cover every statistics target",
        ));
    }
    if mapped_inputs != aggregate_outputs {
        errors.push(ValidationError::new(
            path,
            "writer grouped Unpivot mappings do not cover every final aggregate output",
        ));
    }
}

fn validate_unpivot_resource_limits<'a>(
    fragment: &Fragment,
    mapping_count: usize,
    constants: impl IntoIterator<Item = &'a [crate::UnpivotConstant]>,
    max_output_rows: u64,
    max_output_bytes: u64,
    path: &str,
    errors: &mut ValidationErrorCollector,
) -> bool {
    if max_output_rows == 0
        || max_output_bytes == 0
        || max_output_rows > MAX_UNPIVOT_OUTPUT_ROWS
        || max_output_bytes > MAX_UNPIVOT_OUTPUT_BYTES
    {
        errors.push(ValidationError::new(
            path,
            "unpivot row/byte bounds are zero or exceed the contract maximum",
        ));
        return false;
    }
    if mapping_count > MAX_UNPIVOT_MAPPINGS {
        errors.push(ValidationError::new(
            path,
            "unpivot mapping count exceeds the contract maximum",
        ));
        return false;
    }

    let mut constant_count = 0_usize;
    let mut collection_items = 0_usize;
    let mut literal_bytes = 0_usize;
    for constants in constants {
        constant_count = match constant_count.checked_add(constants.len()) {
            Some(count) if count <= MAX_UNPIVOT_CONSTANTS => count,
            _ => {
                errors.push(ValidationError::new(
                    path,
                    "unpivot constant count exceeds the contract maximum",
                ));
                return false;
            }
        };
        for constant in constants {
            let (items, bytes) = match constant {
                crate::UnpivotConstant::Scalar(expression) => {
                    (0, unpivot_scalar_literal_bytes(fragment, *expression))
                }
                crate::UnpivotConstant::Int32List(values) => (
                    values.len(),
                    values.len().saturating_mul(std::mem::size_of::<i32>()),
                ),
                crate::UnpivotConstant::Utf8Map(entries) => (
                    entries.len(),
                    entries.iter().fold(0_usize, |total, (key, value)| {
                        total.saturating_add(key.len()).saturating_add(value.len())
                    }),
                ),
            };
            collection_items = collection_items.saturating_add(items);
            literal_bytes = literal_bytes.saturating_add(bytes);
            if collection_items > MAX_UNPIVOT_COLLECTION_ITEMS
                || literal_bytes > MAX_UNPIVOT_LITERAL_BYTES
            {
                errors.push(ValidationError::new(
                    path,
                    "unpivot literal collections exceed the contract budget",
                ));
                return false;
            }
        }
    }
    true
}

fn unpivot_scalar_literal_bytes(fragment: &Fragment, expression: ExprId) -> usize {
    let Some(ExprKind::Literal(literal)) = fragment
        .expressions()
        .get(expression)
        .map(|expression| &expression.kind)
    else {
        return 0;
    };
    match literal {
        crate::LiteralValue::Utf8(value) => value.len(),
        crate::LiteralValue::Binary(value) => value.len(),
        crate::LiteralValue::Null => 0,
        crate::LiteralValue::Boolean(_) => std::mem::size_of::<bool>(),
        crate::LiteralValue::Int64(_)
        | crate::LiteralValue::UInt64(_)
        | crate::LiteralValue::Float64Bits(_)
        | crate::LiteralValue::Time64(_)
        | crate::LiteralValue::Timestamp(_) => std::mem::size_of::<u64>(),
        crate::LiteralValue::Date32(_) => std::mem::size_of::<u32>(),
        crate::LiteralValue::LargeInt(_)
        | crate::LiteralValue::Decimal128(_)
        | crate::LiteralValue::IntervalMonthDayNano(_) => std::mem::size_of::<u128>(),
    }
}

fn validate_unpivot_constant(
    fragment: &Fragment,
    constant: &crate::UnpivotConstant,
    output: ValueId,
    context: &str,
    path: &str,
    errors: &mut ValidationErrorCollector,
) -> Option<bool> {
    let output_type = fragment.values().get(&output).map(|value| &value.ty)?;
    let (matches, nullable) = match constant {
        crate::UnpivotConstant::Scalar(expression) => match fragment.expressions().get(*expression)
        {
            Some(expression) => (
                matches!(expression.kind, ExprKind::Literal(_))
                    && expression.ty.data_type == output_type.data_type,
                expression.ty.nullable,
            ),
            None => {
                errors.push(ValidationError::new(
                    path,
                    format!("{context} scalar constant expression is not defined"),
                ));
                return None;
            }
        },
        crate::UnpivotConstant::Int32List(_) => (
            matches!(
                &output_type.data_type,
                DataType::List(field)
                    if field.name() == "item"
                        && field.data_type() == &DataType::Int32
                        && !field.is_nullable()
                        && field.metadata().is_empty()
            ),
            false,
        ),
        crate::UnpivotConstant::Utf8Map(_) => (
            matches!(
                &output_type.data_type,
                DataType::Map(entries, false)
                    if entries.name() == "entries"
                        && !entries.is_nullable()
                        && entries.metadata().is_empty()
                        && matches!(entries.data_type(), DataType::Struct(fields)
                            if fields.len() == 2
                                && fields[0].name() == "key"
                                && fields[0].data_type() == &DataType::Utf8
                                && !fields[0].is_nullable()
                                && fields[0].metadata().is_empty()
                                && fields[1].name() == "value"
                                && fields[1].data_type() == &DataType::Utf8
                                && !fields[1].is_nullable()
                                && fields[1].metadata().is_empty())
            ),
            false,
        ),
    };
    if !matches {
        errors.push(ValidationError::new(
            path,
            format!("{context} constant type differs from its literal output"),
        ));
    }
    if let crate::UnpivotConstant::Utf8Map(entries) = constant
        && (entries.iter().any(|(key, _)| key.is_empty())
            || entries
                .windows(2)
                .any(|pair| pair[0].0.as_ref() >= pair[1].0.as_ref()))
    {
        errors.push(ValidationError::new(
            path,
            format!("{context} map keys must be non-empty and strictly increasing"),
        ));
    }
    Some(nullable)
}

fn validate_relation(
    fragment: &Fragment,
    relation: &Relation,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    validate_read_reference(relation.read(), path, errors);
    if relation.work_source() == ConnectorReadWorkSource::WholeRelation
        && relation.read().relation.kind() != ConnectorReadRelationKind::SystemTable
    {
        errors.push(ValidationError::new(
            path,
            "whole-relation work is valid only for a system-table relation",
        ));
    }
    if relation.work_source() == ConnectorReadWorkSource::WholeRelation
        && (!matches!(
            relation.provided_properties().distribution,
            Distribution::Singleton
        ) || fragment.dop_domain().min != 1
            || fragment.dop_domain().max != 1)
    {
        errors.push(ValidationError::new(
            path,
            "whole-relation work requires singleton distribution and exactly one driver",
        ));
    }
    let relation_source = relation.source_binding();
    if relation_source.selection_digest == [0; 32] {
        errors.push(ValidationError::new(
            path,
            "relation selection digest is zero",
        ));
    }
    if relation.schema().is_empty() {
        errors.push(ValidationError::new(path, "relation schema is empty"));
    }
    for field in relation.schema() {
        validate_column_reference(Some(relation.read()), &field.column, path, errors);
    }
    for guarantee in relation.predicate_guarantees() {
        require_boolean_expression(fragment, guarantee.predicate, path, errors);
    }
    let mut artifact_ids = BTreeSet::new();
    for requirement in relation.artifact_inputs() {
        if !artifact_ids.insert(requirement.artifact) {
            errors.push(ValidationError::new(
                path,
                "relation has duplicate artifact input requirements",
            ));
        }
        if requirement.format.revision == 0 || requirement.schema.is_empty() {
            errors.push(ValidationError::new(
                path,
                "artifact input requirement has an invalid format or empty schema",
            ));
        }
        validate_read_reference(&requirement.source.source, path, errors);
        validate_coverage(&requirement.required_coverage, path, errors);
        if requirement.source != relation_source
            || requirement.required_coverage.selection_digest != requirement.source.selection_digest
        {
            errors.push(ValidationError::new(
                path,
                "artifact input requirement is not bound to the relation's exact source selection",
            ));
        }
    }
    validate_distribution(
        fragment,
        &relation.provided_properties().distribution,
        "relation.provided_properties",
        errors,
    );
    for key in &relation.provided_properties().ordering {
        require_value(fragment, key.value, path, errors);
    }
    if let Relation::Metadata(metadata) = relation
        && (metadata.coverage_evidence.is_empty()
            || metadata.coverage_evidence.len() > MAX_METADATA_COVERAGE_EVIDENCE_BYTES)
    {
        errors.push(ValidationError::new(
            path,
            "metadata relation coverage evidence must be bounded and non-empty",
        ));
    }
}

fn validate_scan_predicate_contract(
    fragment: &Fragment,
    scan: NodeId,
    relation: &Relation,
    residuals: &[ExprId],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let mut guarantees = BTreeMap::new();
    for guarantee in relation.predicate_guarantees() {
        match guarantees.entry(guarantee.predicate) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(guarantee.kind);
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                let message = if *entry.get() == guarantee.kind {
                    "relation contains a duplicate guarantee for one predicate"
                } else {
                    "relation contains conflicting guarantees for one predicate"
                };
                errors.push(ValidationError::new(path, message));
            }
        }
        if fragment
            .expressions()
            .get(guarantee.predicate)
            .is_some_and(|expression| expression.owner != scan)
        {
            errors.push(ValidationError::new(
                path,
                "relation predicate guarantee is not owned by its scan",
            ));
        }
        if !expressions_are_replica_deterministic(
            fragment,
            std::iter::once(guarantee.predicate),
            true,
        ) {
            errors.push(ValidationError::new(
                path,
                "relation predicate guarantee must be replica deterministic",
            ));
        }
    }

    let mut residual_set = BTreeSet::new();
    for residual in residuals {
        if !residual_set.insert(*residual) {
            errors.push(ValidationError::new(
                path,
                "scan contains a duplicate residual predicate",
            ));
        }
        if fragment
            .expressions()
            .get(*residual)
            .is_some_and(|expression| expression.owner != scan)
        {
            errors.push(ValidationError::new(
                path,
                "scan residual predicate is not owned by its scan",
            ));
        }
    }

    for (predicate, kind) in guarantees {
        if kind == crate::PredicateGuaranteeKind::PruningOnly && !residual_set.contains(&predicate)
        {
            errors.push(ValidationError::new(
                path,
                "pruning-only relation predicate must be evaluated by the scan residual",
            ));
        }
    }
}

fn validate_read_reference(
    reference: &ProviderReadReference,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let descriptor = reference.binding.descriptor();
    let catalog = reference.binding.catalog_handle();
    if &descriptor.instance_id != catalog.catalog_name() {
        errors.push(ValidationError::new(
            path,
            "provider instance and catalog handle identify different bindings",
        ));
    }
    validate_encoded_payload(
        Some(reference),
        reference.relation.table(),
        &[ConnectorCodecCategory::ReadTable],
        path,
        errors,
    );
    validate_encoded_payload(
        Some(reference),
        reference.relation.view(),
        &[ConnectorCodecCategory::ReadView],
        path,
        errors,
    );
}

fn validate_column_reference(
    relation: Option<&ProviderReadReference>,
    column: &ProviderColumnReference,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    validate_encoded_payload(
        relation,
        &column.column_payload,
        &[ConnectorCodecCategory::ReadColumn],
        path,
        errors,
    );
}

fn validate_encoded_payload(
    relation: Option<&ProviderReadReference>,
    payload: &ConnectorEncodedPayload,
    allowed_categories: &[ConnectorCodecCategory],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if payload.payload().is_empty() || payload.payload().len() > MAX_PROVIDER_PRIVATE_PAYLOAD_BYTES
    {
        errors.push(ValidationError::new(
            path,
            "provider-private payload must be bounded and non-empty",
        ));
    }
    if !allowed_categories.contains(&payload.header().category()) {
        errors.push(ValidationError::new(
            path,
            "provider-private payload has the wrong category",
        ));
    }
    if let Some(relation) = relation
        && (payload.header().provider_id() != &relation.binding.descriptor().provider_id
            || payload.header().catalog() != relation.binding.catalog_handle())
    {
        errors.push(ValidationError::new(
            path,
            "provider-private payload header differs from the exact relation binding",
        ));
    }
}

fn validate_null_extended(
    fragment: &Fragment,
    owner: NodeId,
    values: &[ValueId],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    for value in values {
        match fragment.values().get(value) {
            Some(value) if matches!(value.origin, ValueOrigin::NullExtended { node, .. } if node == owner) =>
                {}
            Some(_) => errors.push(ValidationError::new(
                path,
                "join null-extension list contains a value with another origin",
            )),
            None => require_value(fragment, *value, path, errors),
        }
    }
}

fn require_passthrough_output(
    fragment: &Fragment,
    node: &PhysicalNode,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if let Some(input) = node.inputs.first().and_then(|id| fragment.nodes().get(id))
        && input.output.columns != node.output.columns
    {
        errors.push(ValidationError::new(
            path,
            "pass-through node changed value identities or output order",
        ));
    }
}

fn require_boolean_expression(
    fragment: &Fragment,
    expression: ExprId,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    match fragment.expressions().get(expression) {
        Some(expression) if expression.ty.data_type == DataType::Boolean => {}
        Some(_) => errors.push(ValidationError::new(
            path,
            "predicate expression is not Boolean",
        )),
        None => errors.push(ValidationError::new(
            path,
            format!("expression {} is not defined", expression.get()),
        )),
    }
}

fn validate_node_graph(fragment: &Fragment, errors: &mut ValidationErrorCollector) {
    let path = format!("fragments[{}].nodes", fragment.id().get());
    let mut remaining_inputs = BTreeMap::new();
    let mut dependents: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
    let mut ready = Vec::new();
    for (id, node) in fragment.nodes() {
        let inputs = node
            .inputs
            .iter()
            .copied()
            .filter(|input| fragment.nodes().contains_key(input))
            .collect::<BTreeSet<_>>();
        remaining_inputs.insert(*id, inputs.len());
        if inputs.is_empty() {
            ready.push(*id);
        }
        for input in inputs {
            dependents.entry(input).or_default().push(*id);
        }
    }
    let mut processed = 0_usize;
    while let Some(id) = ready.pop() {
        processed += 1;
        if let Some(users) = dependents.get(&id) {
            for user in users {
                if let Some(remaining) = remaining_inputs.get_mut(user) {
                    *remaining -= 1;
                    if *remaining == 0 {
                        ready.push(*user);
                    }
                }
            }
        }
    }
    if processed != fragment.nodes().len() {
        errors.push(ValidationError::new(&path, "node graph contains a cycle"));
    }

    let mut visited = BTreeSet::new();
    let mut pending = vec![fragment.root()];
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        if let Some(node) = fragment.nodes().get(&id) {
            pending.extend(node.inputs.iter().copied());
        }
    }
    if visited.len() != fragment.nodes().len() {
        errors.push(ValidationError::new(
            &path,
            "fragment contains nodes unreachable from its root",
        ));
    }
}

fn validate_edge(
    plan: &PhysicalPlan,
    edge: &Edge,
    root_port_indexes: &mut BTreeMap<FragmentId, ValuePortIndex>,
    errors: &mut ValidationErrorCollector,
) {
    let path = format!("edges[{}]", edge.id.get());
    let Some(source) = plan.fragments().get(&edge.source.fragment) else {
        errors.push(ValidationError::new(
            &path,
            "source fragment is not defined",
        ));
        return;
    };
    let Some(destination) = plan.fragments().get(&edge.destination.fragment) else {
        errors.push(ValidationError::new(
            &path,
            "destination fragment is not defined",
        ));
        return;
    };
    if edge.source.fragment == edge.destination.fragment {
        errors.push(ValidationError::new(
            &path,
            "edge cannot connect a fragment to itself",
        ));
    }
    if edge.source.projection.len() != edge.destination.receive_mapping.len() {
        errors.push(ValidationError::new(
            &path,
            "source projection and receive mapping have different widths",
        ));
    }
    if let Some(root) = source.nodes().get(&source.root()) {
        let root_values = root_port_indexes
            .entry(source.id())
            .or_insert_with(|| ValuePortIndex::new(&root.output.columns));
        if edge
            .source
            .projection
            .iter()
            .any(|value| !root_values.contains(value))
        {
            errors.push(ValidationError::new(
                &path,
                "edge projects a value absent from the source fragment root output",
            ));
        }
        if root.output_properties.row_multiplicity != edge.partitioning.source_multiplicity {
            errors.push(ValidationError::new(
                &path,
                "edge source row multiplicity differs from the source fragment root",
            ));
        }
    }
    validate_distribution(
        source,
        &edge.partitioning.source,
        "edge.source_partitioning",
        errors,
    );
    validate_distribution(
        destination,
        &edge.partitioning.destination,
        "edge.destination_partitioning",
        errors,
    );
    for (ordinal, (projected, (mapped_source, imported))) in edge
        .source
        .projection
        .iter()
        .zip(edge.destination.receive_mapping.iter())
        .enumerate()
    {
        if projected != mapped_source {
            errors.push(ValidationError::new(
                &path,
                format!("receive mapping source differs at ordinal {ordinal}"),
            ));
        }
        match (
            source.values().get(projected),
            destination.values().get(imported),
        ) {
            (Some(source_value), Some(destination_value)) => {
                if source_value.ty != destination_value.ty {
                    errors.push(ValidationError::new(
                        &path,
                        format!("source and destination types differ at ordinal {ordinal}"),
                    ));
                }
                if !import_origin_matches(
                    &destination_value.origin,
                    edge.id,
                    edge.kind,
                    edge.source.fragment,
                    *projected,
                ) {
                    errors.push(ValidationError::new(
                        &path,
                        format!("destination value origin differs at ordinal {ordinal}"),
                    ));
                }
            }
            _ => errors.push(ValidationError::new(
                &path,
                format!("mapping references an undefined value at ordinal {ordinal}"),
            )),
        }
    }
    validate_edge_partitioning(edge, &path, errors);
    match destination.nodes().get(&edge.destination.node) {
        Some(node)
            if matches!(
                &node.kind,
                NodeKind::ExchangeSource { edge: node_edge, imports }
                    if *node_edge == edge.id
                        && imports.as_ref() == edge.destination.receive_mapping.as_ref()
            ) =>
        {
            if node.output_properties.distribution != edge.partitioning.destination
                || node.output_properties.row_multiplicity
                    != edge.partitioning.destination_multiplicity
                || !node.output_properties.ordering.is_empty()
            {
                errors.push(ValidationError::new(
                    &path,
                    "exchange receiver properties differ from the edge destination contract",
                ));
            }
        }
        Some(_) => errors.push(ValidationError::new(
            &path,
            "destination node is not the exact exchange receiver",
        )),
        None => errors.push(ValidationError::new(
            &path,
            "destination node is not defined",
        )),
    }
}

fn validate_edge_partitioning(edge: &Edge, path: &str, errors: &mut ValidationErrorCollector) {
    validate_mapped_partitioning(
        &edge.partitioning,
        &edge.destination.receive_mapping,
        path,
        errors,
    );
}

fn validate_mapped_partitioning(
    partitioning: &crate::EdgePartitioning,
    mapping: &[(ValueId, ValueId)],
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let layout_valid = match (&partitioning.source, &partitioning.destination) {
        (Distribution::Unconstrained, Distribution::Unconstrained)
        | (Distribution::Singleton, Distribution::Singleton)
        | (Distribution::RoundRobin, Distribution::RoundRobin)
        | (Distribution::Broadcast, Distribution::Broadcast) => true,
        (
            Distribution::Hash {
                keys: source_keys,
                scheme: source_scheme,
            },
            Distribution::Hash {
                keys: destination_keys,
                scheme: destination_scheme,
            },
        ) => {
            source_scheme == destination_scheme
                && mapped_partition_keys_match(source_keys, destination_keys, mapping)
        }
        (
            Distribution::BucketShuffle {
                keys: source_keys,
                scheme: source_scheme,
            },
            Distribution::BucketShuffle {
                keys: destination_keys,
                scheme: destination_scheme,
            },
        ) => {
            source_scheme == destination_scheme
                && mapped_partition_keys_match(source_keys, destination_keys, mapping)
        }
        _ => false,
    };
    let multiplicity_valid = if partitioning.destination == Distribution::Broadcast {
        partitioning.source_multiplicity == RowMultiplicity::SingleCopy
            && partitioning.destination_multiplicity == RowMultiplicity::Replicated
    } else {
        partitioning.source_multiplicity == partitioning.destination_multiplicity
    };
    if !layout_valid || !multiplicity_valid {
        errors.push(ValidationError::new(
            path,
            "edge source and destination partitioning or row multiplicity are inconsistent",
        ));
    }
}

fn mapped_partition_keys_match(
    source_keys: &[ValueId],
    destination_keys: &[ValueId],
    mapping: &[(ValueId, ValueId)],
) -> bool {
    let mapping = ValueMappingIndex::from_pairs(mapping);
    source_keys.len() == destination_keys.len()
        && source_keys
            .iter()
            .zip(destination_keys)
            .all(|(source, destination)| mapping.contains(*source, *destination))
}

fn distribution_values(distribution: &Distribution) -> &[ValueId] {
    match distribution {
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. } => keys,
        Distribution::Unconstrained
        | Distribution::Singleton
        | Distribution::RoundRobin
        | Distribution::Broadcast => &[],
    }
}

#[derive(Default)]
struct SourceSinkEdgeIndex {
    owned: BTreeMap<EdgeId, bool>,
}

impl SourceSinkEdgeIndex {
    fn new(plan: &PhysicalPlan) -> Self {
        let mut index = Self::default();
        for source in plan.fragments().values() {
            match source.sink() {
                FragmentSink::Stream { edge } => index.record(
                    *edge,
                    plan.edges().get(edge).is_some_and(|contract| {
                        contract.source.fragment == source.id()
                            && contract.kind == crate::EdgeKind::Stream
                    }),
                ),
                FragmentSink::Multicast { edges } => {
                    for edge in edges {
                        index.record(
                            *edge,
                            plan.edges().get(edge).is_some_and(|contract| {
                                contract.source.fragment == source.id()
                                    && contract.kind == crate::EdgeKind::CteMulticast
                            }),
                        );
                    }
                }
                FragmentSink::Router { routes, .. } => {
                    for route in routes {
                        index.record(
                            route.edge,
                            plan.edges().get(&route.edge).is_some_and(|contract| {
                                contract.source.fragment == source.id()
                                    && contract.kind == crate::EdgeKind::ChangeStreamRouter
                                    && contract
                                        .source
                                        .projection
                                        .iter()
                                        .eq(route.input_mapping.iter().map(|(_, value)| value))
                            }),
                        );
                    }
                }
                FragmentSink::Result | FragmentSink::SealedArtifact(_) | FragmentSink::Noop => {}
            }
        }
        index
    }

    fn record(&mut self, edge: EdgeId, valid: bool) {
        self.owned
            .entry(edge)
            .and_modify(|owned| *owned = false)
            .or_insert(valid);
    }

    fn owns(&self, edge: &Edge) -> bool {
        self.owned.get(&edge.id) == Some(&true)
    }
}

fn validate_runtime_filter_proof_edge_source_sinks(
    plan: &PhysicalPlan,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let source_sinks = SourceSinkEdgeIndex::new(plan);
    for edge in plan.edges().values() {
        if !source_sinks.owns(edge) {
            errors.push(ValidationError::new(
                format!("{path}.edges[{}]", edge.id.get()),
                "runtime-filter proof edge is not owned by its exact source fragment sink",
            ));
        }
    }
    for source in plan.fragments().values() {
        let FragmentSink::Router { routes, .. } = source.sink() else {
            continue;
        };
        for route in routes {
            let Some(edge) = plan.edges().get(&route.edge) else {
                continue;
            };
            if !source_sinks.owns(edge) {
                continue;
            }
            let edge_path = format!("{path}.edges[{}]", edge.id.get());
            validate_router_writer_contract(plan, route, edge, &edge_path, errors);
            validate_router_partitioning(route, &edge.partitioning.source, &edge_path, errors);
        }
    }
}

fn validate_sinks(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    let mut referenced = BTreeSet::new();
    for fragment in plan.fragments().values() {
        let path = format!("fragments[{}].sink", fragment.id().get());
        let edges: &[EdgeId] = match fragment.sink() {
            FragmentSink::Stream { edge } => std::slice::from_ref(edge),
            FragmentSink::Multicast { edges } => edges,
            FragmentSink::Router { routes, .. } => {
                for route in routes {
                    if !referenced.insert(route.edge) {
                        errors.push(ValidationError::new(
                            &path,
                            "edge is referenced by more than one sink",
                        ));
                    }
                    match plan.edges().get(&route.edge) {
                        Some(edge)
                            if edge.source.fragment == fragment.id()
                                && edge.kind == crate::EdgeKind::ChangeStreamRouter =>
                        {
                            if !edge
                                .source
                                .projection
                                .iter()
                                .eq(route.input_mapping.iter().map(|(_, value)| value))
                            {
                                errors.push(ValidationError::new(
                                    &path,
                                    "router edge projection differs from its exact route input sequence",
                                ));
                            }
                            validate_router_writer_contract(plan, route, edge, &path, errors);
                            validate_router_partitioning(
                                route,
                                &edge.partitioning.source,
                                &path,
                                errors,
                            );
                        }
                        Some(_) => errors.push(ValidationError::new(
                            &path,
                            "sink edge belongs to another source fragment",
                        )),
                        None => {
                            errors.push(ValidationError::new(&path, "sink edge is not defined"))
                        }
                    }
                }
                &[]
            }
            FragmentSink::Result | FragmentSink::SealedArtifact(_) | FragmentSink::Noop => &[],
        };
        for edge_id in edges {
            if !referenced.insert(*edge_id) {
                errors.push(ValidationError::new(
                    &path,
                    "edge is referenced by more than one sink",
                ));
            }
            match plan.edges().get(edge_id) {
                Some(edge)
                    if edge.source.fragment == fragment.id()
                        && edge_kind_matches_sink(fragment.sink(), edge.kind) => {}
                Some(_) => errors.push(ValidationError::new(
                    &path,
                    "sink edge belongs to another source fragment",
                )),
                None => errors.push(ValidationError::new(&path, "sink edge is not defined")),
            }
        }
    }
    for edge in plan.edges().values() {
        if !referenced.contains(&edge.id) {
            errors.push(ValidationError::new(
                format!("edges[{}]", edge.id.get()),
                "edge is not owned by its source fragment sink",
            ));
        }
    }
}

fn validate_router_writer_contract(
    plan: &PhysicalPlan,
    route: &crate::ChangeStreamRoute,
    edge: &Edge,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let Some(destination) = plan.fragments().get(&edge.destination.fragment) else {
        return;
    };
    let Some(writer) = destination.nodes().get(&destination.root()) else {
        return;
    };
    let NodeKind::TableWriter { target } = &writer.kind else {
        errors.push(ValidationError::new(
            path,
            "router edge destination fragment root is not its exact table writer",
        ));
        return;
    };
    if writer.inputs.as_ref() != [edge.destination.node] {
        errors.push(ValidationError::new(
            path,
            "router edge receiver is not the direct table writer input",
        ));
    }
    if route.write_target_ordinal != target.write_target_ordinal {
        errors.push(ValidationError::new(
            path,
            "router write target ordinal differs from its destination table writer",
        ));
    }
    if target.required_distribution != edge.partitioning.destination {
        errors.push(ValidationError::new(
            path,
            "router edge destination distribution differs from its table writer requirement",
        ));
    }
    let fields_match = route.input_mapping.len() == edge.destination.receive_mapping.len()
        && route.input_mapping.len() == target.target_fields.len()
        && route
            .input_mapping
            .iter()
            .zip(edge.destination.receive_mapping.iter())
            .zip(target.target_fields.iter())
            .all(
                |(((route_token, route_value), (mapped_source, imported)), target_field)| {
                    route_value == mapped_source
                        && route_token == &target_field.token
                        && imported == &target_field.input
                },
            );
    if !fields_match {
        errors.push(ValidationError::new(
            path,
            "router field mapping differs from its destination table writer contract",
        ));
    }
}

fn validate_router_partitioning(
    route: &crate::ChangeStreamRoute,
    source_distribution: &Distribution,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let matches = match source_distribution {
        Distribution::Singleton => route.partition_by.is_empty(),
        Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. } => {
            !route.partition_by.is_empty() && keys.as_ref() == route.partition_by.as_ref()
        }
        Distribution::Unconstrained | Distribution::RoundRobin | Distribution::Broadcast => false,
    };
    if !matches {
        errors.push(ValidationError::new(
            path,
            "router partition values differ from its exact edge distribution",
        ));
    }
}

fn edge_kind_matches_sink(sink: &FragmentSink, kind: crate::EdgeKind) -> bool {
    match sink {
        FragmentSink::Stream { .. } => kind == crate::EdgeKind::Stream,
        FragmentSink::Multicast { .. } => kind == crate::EdgeKind::CteMulticast,
        FragmentSink::Router { .. } => kind == crate::EdgeKind::ChangeStreamRouter,
        FragmentSink::Result | FragmentSink::SealedArtifact(_) | FragmentSink::Noop => false,
    }
}

fn validate_writer_flows(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    let writers = plan
        .fragments()
        .values()
        .flat_map(|fragment| {
            fragment.nodes().values().filter_map(|node| {
                matches!(node.kind, NodeKind::TableWriter { .. })
                    .then_some((fragment.id(), node.id))
            })
        })
        .collect::<BTreeSet<_>>();
    let mut writer_finish_counts = BTreeMap::<(FragmentId, NodeId), usize>::new();

    for fragment in plan.fragments().values() {
        for finish_node in fragment
            .nodes()
            .values()
            .filter(|node| matches!(node.kind, NodeKind::TableFinish(_)))
        {
            let path = format!(
                "fragments[{}].nodes[{}].writer_flow",
                fragment.id().get(),
                finish_node.id.get()
            );
            if finish_node.id != fragment.root() {
                errors.push(ValidationError::new(
                    &path,
                    "table finish must be the root of its fragment",
                ));
            }
            let NodeKind::TableFinish(finish) = &finish_node.kind else {
                unreachable!();
            };
            let finish_values = finish
                .input_schema
                .fields
                .iter()
                .map(|field| field.value)
                .collect::<Box<[_]>>();
            let mut pending = finish_node
                .inputs
                .iter()
                .copied()
                .map(|node| (fragment.id(), node, finish_values.clone()))
                .collect::<Vec<_>>();
            let mut visited = BTreeSet::new();
            let mut finish_writers = Vec::new();
            while let Some((fragment_id, node_id, expected_values)) = pending.pop() {
                if !visited.insert((fragment_id, node_id)) {
                    errors.push(ValidationError::new(
                        &path,
                        "writer relation reaches table finish through more than one path",
                    ));
                    continue;
                }
                let Some(flow_fragment) = plan.fragments().get(&fragment_id) else {
                    continue;
                };
                let Some(node) = flow_fragment.nodes().get(&node_id) else {
                    continue;
                };
                match &node.kind {
                    NodeKind::TableWriter { target } => {
                        finish_writers.push((fragment_id, node_id, target));
                        *writer_finish_counts
                            .entry((fragment_id, node_id))
                            .or_default() += 1;
                        if !writer_schema_matches_finish_values(
                            &target.output_schema,
                            &finish.input_schema,
                            &expected_values,
                        ) {
                            errors.push(ValidationError::new(
                                &path,
                                "table writer fields do not map exactly to its table finish input roles",
                            ));
                        }
                    }
                    NodeKind::ExchangeSource { edge, .. } => match plan.edges().get(edge) {
                        Some(edge_contract) if edge_contract.kind == crate::EdgeKind::Stream => {
                            let source_values = edge_contract
                                .destination
                                .receive_mapping
                                .iter()
                                .zip(&expected_values)
                                .map(|((source, destination), expected)| {
                                    (*destination == *expected).then_some(*source)
                                })
                                .collect::<Option<Box<[_]>>>();
                            if let Some(source) =
                                plan.fragments().get(&edge_contract.source.fragment)
                            {
                                let Some(source_root) = source.nodes().get(&source.root()) else {
                                    continue;
                                };
                                match (&source_root.kind, source_values) {
                                    (NodeKind::TableWriter { target }, Some(source_values)) => {
                                        finish_writers.push((source.id(), source_root.id, target));
                                        *writer_finish_counts
                                            .entry((source.id(), source_root.id))
                                            .or_default() += 1;
                                        if !writer_schema_matches_finish_values(
                                            &target.output_schema,
                                            &finish.input_schema,
                                            &source_values,
                                        ) {
                                            errors.push(ValidationError::new(
                                                &path,
                                                "streamed table writer fields do not map exactly to its table finish input roles",
                                            ));
                                        }
                                    }
                                    _ => errors.push(ValidationError::new(
                                        &path,
                                        "table finish stream source fields do not map exactly to its table finish input roles",
                                    )),
                                }
                            }
                        }
                        _ => errors.push(ValidationError::new(
                            &path,
                            "table finish writer relation uses a non-stream exchange",
                        )),
                    },
                    NodeKind::SetOp {
                        kind: crate::SetOperationKind::UnionAll,
                        input_mappings,
                    } => {
                        if node.output.columns.as_ref() != expected_values.as_ref()
                            || input_mappings.len() != node.inputs.len()
                            || input_mappings
                                .iter()
                                .any(|mapping| mapping.len() != expected_values.len())
                        {
                            errors.push(ValidationError::new(
                                &path,
                                "writer UnionAll does not preserve the exact finish field occurrences",
                            ));
                            continue;
                        }
                        pending.extend(
                            node.inputs
                                .iter()
                                .copied()
                                .zip(input_mappings.iter().cloned())
                                .map(|(input, mapping)| (fragment_id, input, mapping)),
                        );
                    }
                    _ => errors.push(ValidationError::new(
                        &path,
                        "table finish input contains a non-preserving writer relation node",
                    )),
                }
            }
            let actual_ordinals = finish_writers
                .iter()
                .map(|(_, _, target)| target.write_target_ordinal)
                .collect::<BTreeSet<_>>();
            if actual_ordinals.len() != finish_writers.len()
                || !actual_ordinals
                    .iter()
                    .copied()
                    .eq(finish.expected_target_ordinals.iter().copied())
            {
                errors.push(ValidationError::new(
                    &path,
                    "table finish expected targets differ from its exact upstream writers",
                ));
            }
            for (_, _, target) in finish_writers {
                if !writer_schema_shapes_match(&target.output_schema, &finish.input_schema) {
                    errors.push(ValidationError::new(
                        &path,
                        "table writer output schema differs from its table finish input schema",
                    ));
                }
            }
        }
    }

    for writer in writers {
        if writer_finish_counts.get(&writer).copied() != Some(1) {
            errors.push(ValidationError::new(
                format!("fragments[{}].nodes[{}]", writer.0.get(), writer.1.get()),
                "table writer must feed exactly one table finish",
            ));
        }
    }
}

fn writer_schema_shapes_match(
    writer: &crate::WriterRelationSchema,
    finish: &crate::WriterRelationSchema,
) -> bool {
    writer.revision == finish.revision
        && writer.fields.len() == finish.fields.len()
        && writer
            .fields
            .iter()
            .zip(&finish.fields)
            .all(|(writer, finish)| {
                writer.name == finish.name && writer.ty == finish.ty && writer.role == finish.role
            })
}

fn validate_result(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    let result_sinks = plan
        .fragments()
        .values()
        .filter(|fragment| matches!(fragment.sink(), FragmentSink::Result))
        .collect::<Vec<_>>();
    match (plan.result_port(), result_sinks.as_slice()) {
        (None, []) => {}
        (None, _) => errors.push(ValidationError::new(
            "result_port",
            "result sink has no result port",
        )),
        (Some(_), []) => errors.push(ValidationError::new(
            "result_port",
            "result port has no result sink",
        )),
        (Some(_), [_, _, ..]) => errors.push(ValidationError::new(
            "result_port",
            "plan has more than one result sink",
        )),
        (Some(result), [fragment]) => {
            if result.fragment != fragment.id() {
                errors.push(ValidationError::new(
                    "result_port.fragment",
                    "result port belongs to another fragment",
                ));
            }
            if result.output.node != fragment.root() {
                errors.push(ValidationError::new(
                    "result_port.output",
                    "result output is not the result fragment root",
                ));
            }
            match fragment.nodes().get(&result.output.node) {
                Some(node) if node.output == result.output => {}
                Some(_) => errors.push(ValidationError::new(
                    "result_port.output",
                    "result output differs from the node output port",
                )),
                None => errors.push(ValidationError::new(
                    "result_port.output",
                    "result node is not defined",
                )),
            }
            if result.fields.len() != result.output.columns.len() {
                errors.push(ValidationError::new(
                    "result_port.fields",
                    "result schema width differs from output width",
                ));
            }
            for (ordinal, (field, value)) in
                result.fields.iter().zip(&result.output.columns).enumerate()
            {
                if field.value != *value {
                    errors.push(ValidationError::new(
                        "result_port.fields",
                        format!("result value differs at ordinal {ordinal}"),
                    ));
                }
                if field.name.is_empty() {
                    errors.push(ValidationError::new(
                        "result_port.fields",
                        format!("result name is empty at ordinal {ordinal}"),
                    ));
                }
                if let Some(definition) = fragment.values().get(value)
                    && definition.ty != field.ty
                {
                    errors.push(ValidationError::new(
                        "result_port.fields",
                        format!("result type differs at ordinal {ordinal}"),
                    ));
                }
            }
        }
    }
}

fn validate_runtime_filters(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    let mut lineage_indexes = RuntimeFilterLineageIndexes::default();
    let attachments = runtime_filter_attachment_index(plan);
    let inbound_edges = runtime_filter_inbound_edge_index(plan);
    for filter in plan.runtime_filters().values() {
        let path = format!("runtime_filters[{}]", filter.id.get());
        if !validate_runtime_filter_shape(filter, &path, errors) {
            continue;
        }
        let witnesses = runtime_filter_witness_index(&filter.equality_witnesses);
        for witness in &filter.equality_witnesses {
            match plan.fragments().get(&witness.fragment) {
                Some(fragment) => validate_runtime_filter_equality_witness(
                    fragment,
                    witness,
                    &filter.domain,
                    &path,
                    errors,
                ),
                None => errors.push(ValidationError::new(
                    &path,
                    "runtime filter equality witness fragment is not defined",
                )),
            }
        }
        for producer in &filter.producers {
            validate_runtime_filter_attachment(
                plan,
                &attachments,
                filter.id,
                &producer.endpoint,
                &path,
                errors,
            );
            validate_runtime_filter_endpoint(
                plan,
                &producer.endpoint,
                &filter.domain,
                &path,
                errors,
            );
            validate_apply_point(
                plan,
                &mut lineage_indexes,
                &producer.endpoint,
                producer.apply_point,
                &path,
                errors,
            );
            if let Some(fragment) = plan.fragments().get(&producer.endpoint.fragment) {
                validate_runtime_filter_producer_target(
                    fragment,
                    &witnesses,
                    producer,
                    &filter.domain,
                    filter.reduction,
                    &path,
                    errors,
                );
                validate_runtime_filter_producer_progress(
                    fragment,
                    producer,
                    inbound_edges
                        .get(&fragment.id())
                        .unwrap_or(&BTreeSet::new()),
                    &mut lineage_indexes,
                    &path,
                    errors,
                );
            }
        }
        for consumer in &filter.consumers {
            validate_runtime_filter_attachment(
                plan,
                &attachments,
                filter.id,
                &consumer.endpoint,
                &path,
                errors,
            );
            validate_runtime_filter_endpoint(
                plan,
                &consumer.endpoint,
                &filter.domain,
                &path,
                errors,
            );
            validate_apply_point(
                plan,
                &mut lineage_indexes,
                &consumer.endpoint,
                consumer.apply_point,
                &path,
                errors,
            );
            if let Some(fragment) = plan.fragments().get(&consumer.endpoint.fragment) {
                validate_runtime_filter_consumer_semantics(
                    fragment,
                    &witnesses,
                    consumer,
                    &mut lineage_indexes,
                    &path,
                    errors,
                );
            }
            validate_runtime_filter_consumer_lineage(
                plan,
                &witnesses,
                consumer,
                &path,
                &mut lineage_indexes,
                errors,
            );
        }
    }
    validate_runtime_filter_wait_graph(plan, "runtime_filters", errors);
    for fragment in plan.fragments().values() {
        for id in fragment.runtime_filters() {
            if !plan.runtime_filters().contains_key(id) {
                errors.push(ValidationError::new(
                    format!("fragments[{}].runtime_filters", fragment.id().get()),
                    format!("runtime filter {} is not defined", id.get()),
                ));
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RuntimeFilterWaitNode {
    Physical(FragmentId, NodeId),
    Filter(crate::RuntimeFilterId),
}

fn validate_runtime_filter_wait_graph(
    plan: &PhysicalPlan,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let mut dependencies =
        BTreeMap::<RuntimeFilterWaitNode, BTreeSet<RuntimeFilterWaitNode>>::new();
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            let current = RuntimeFilterWaitNode::Physical(fragment.id(), node.id);
            let current_dependencies = dependencies.entry(current).or_default();
            current_dependencies.extend(
                node.inputs
                    .iter()
                    .map(|input| RuntimeFilterWaitNode::Physical(fragment.id(), *input)),
            );
            if let NodeKind::ExchangeSource { edge, .. } = &node.kind
                && let Some(edge) = plan.edges().get(edge)
                && let Some(source) = plan.fragments().get(&edge.source.fragment)
            {
                current_dependencies
                    .insert(RuntimeFilterWaitNode::Physical(source.id(), source.root()));
            }
        }
    }
    for filter in plan.runtime_filters().values() {
        let filter_node = RuntimeFilterWaitNode::Filter(filter.id);
        dependencies.entry(filter_node).or_default();
        for producer in &filter.producers {
            let Some(fragment) = plan.fragments().get(&producer.endpoint.fragment) else {
                continue;
            };
            let Some(join) = fragment.nodes().get(&producer.endpoint.node) else {
                continue;
            };
            let NodeKind::HashJoin { build_side, .. } = &join.kind else {
                continue;
            };
            let Some(build_root) = usize::try_from(build_side.input_ordinal())
                .ok()
                .and_then(|ordinal| join.inputs.get(ordinal))
            else {
                continue;
            };
            dependencies
                .entry(filter_node)
                .or_default()
                .insert(RuntimeFilterWaitNode::Physical(fragment.id(), *build_root));
        }
        for consumer in &filter.consumers {
            if consumer.activation == crate::RuntimeFilterConsumerActivation::BlockingSnapshot {
                dependencies
                    .entry(RuntimeFilterWaitNode::Physical(
                        consumer.endpoint.fragment,
                        consumer.endpoint.node,
                    ))
                    .or_default()
                    .insert(filter_node);
            }
        }
    }

    let mut remaining = dependencies
        .iter()
        .map(|(node, dependencies)| (*node, dependencies.len()))
        .collect::<BTreeMap<_, _>>();
    let mut dependents = BTreeMap::<RuntimeFilterWaitNode, Vec<RuntimeFilterWaitNode>>::new();
    for (node, node_dependencies) in &dependencies {
        for dependency in node_dependencies {
            dependents.entry(*dependency).or_default().push(*node);
        }
    }
    let mut ready = remaining
        .iter()
        .filter_map(|(node, count)| (*count == 0).then_some(*node))
        .collect::<Vec<_>>();
    while let Some(node) = ready.pop() {
        let Some(count) = remaining.remove(&node) else {
            continue;
        };
        debug_assert_eq!(count, 0);
        if let Some(nodes) = dependents.get(&node) {
            for dependent in nodes {
                if let Some(count) = remaining.get_mut(dependent) {
                    *count -= 1;
                    if *count == 0 {
                        ready.push(*dependent);
                    }
                }
            }
        }
    }
    if remaining
        .keys()
        .any(|node| matches!(node, RuntimeFilterWaitNode::Filter(_)))
    {
        errors.push(ValidationError::new(
            path,
            "blocking runtime-filter waits form a cycle with physical execution dependencies",
        ));
    }
}

fn validate_runtime_filter_attachment(
    plan: &PhysicalPlan,
    attachments: &BTreeMap<FragmentId, BTreeSet<crate::RuntimeFilterId>>,
    id: crate::RuntimeFilterId,
    endpoint: &RuntimeFilterEndpoint,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if let Some(fragment) = plan.fragments().get(&endpoint.fragment)
        && !attachments
            .get(&fragment.id())
            .is_some_and(|filters| filters.contains(&id))
    {
        errors.push(ValidationError::new(
            path,
            "runtime filter endpoint fragment does not attach the filter",
        ));
    }
}

type RuntimeFilterWitnessIndex<'a> = BTreeMap<
    crate::RuntimeFilterEqualityWitnessId,
    Option<&'a crate::RuntimeFilterEqualityWitness>,
>;

fn runtime_filter_witness_index(
    witnesses: &[crate::RuntimeFilterEqualityWitness],
) -> RuntimeFilterWitnessIndex<'_> {
    let mut index = BTreeMap::new();
    for witness in witnesses {
        index
            .entry(witness.id)
            .and_modify(|existing| *existing = None)
            .or_insert(Some(witness));
    }
    index
}

fn runtime_filter_attachment_index(
    plan: &PhysicalPlan,
) -> BTreeMap<FragmentId, BTreeSet<crate::RuntimeFilterId>> {
    plan.fragments()
        .values()
        .map(|fragment| {
            (
                fragment.id(),
                fragment.runtime_filters().iter().copied().collect(),
            )
        })
        .collect()
}

fn runtime_filter_inbound_edge_index(
    plan: &PhysicalPlan,
) -> BTreeMap<FragmentId, BTreeSet<EdgeId>> {
    let mut index = BTreeMap::<FragmentId, BTreeSet<EdgeId>>::new();
    for edge in plan.edges().values() {
        index
            .entry(edge.destination.fragment)
            .or_default()
            .insert(edge.id);
    }
    index
}

fn validate_runtime_filter_shape(
    filter: &crate::RuntimeFilter,
    path: &str,
    errors: &mut ValidationErrorCollector,
) -> bool {
    if filter.producers.len() > MAX_RUNTIME_FILTER_ENDPOINTS
        || filter.consumers.len() > MAX_RUNTIME_FILTER_ENDPOINTS
        || filter.equality_witnesses.len() > MAX_RUNTIME_FILTER_ENDPOINTS
        || filter
            .producers
            .len()
            .saturating_add(filter.consumers.len())
            > MAX_RUNTIME_FILTER_ENDPOINTS
    {
        errors.push(ValidationError::new(
            path,
            "runtime filter endpoint count exceeds the contract maximum",
        ));
        return false;
    }
    let lineage_steps = filter.consumers.iter().fold(0_usize, |total, consumer| {
        total.saturating_add(match &consumer.target {
            crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => 0,
            crate::RuntimeFilterConsumerTarget::ScanField { lineage, .. } => lineage.len(),
        })
    });
    if lineage_steps > MAX_RUNTIME_FILTER_LINEAGE_STEPS {
        errors.push(ValidationError::new(
            path,
            "runtime filter lineage exceeds the contract maximum",
        ));
        return false;
    }
    if !matches!(
        (filter.kind, &filter.domain),
        (
            crate::RuntimeFilterKind::Bloom | crate::RuntimeFilterKind::InList,
            crate::RuntimeFilterDomain::Membership { .. }
        ) | (
            crate::RuntimeFilterKind::MinMax,
            crate::RuntimeFilterDomain::Ordered { .. }
        )
    ) {
        errors.push(ValidationError::new(
            path,
            "runtime filter kind differs from its logical domain",
        ));
    }
    if let crate::RuntimeFilterDomain::Ordered {
        key, comparator, ..
    } = &filter.domain
    {
        if key.ty.data_type == DataType::Null {
            errors.push(ValidationError::new(
                path,
                "ordered runtime-filter key has Null type",
            ));
        }
        if !comparator.supports_order_key(&key.ty.data_type) {
            errors.push(ValidationError::new(
                path,
                "ordered runtime-filter key type is unsupported by its comparison algorithm",
            ));
        }
    }
    if filter.producers.is_empty() {
        errors.push(ValidationError::new(
            path,
            "runtime filter has no producers",
        ));
    }
    if filter.consumers.is_empty() {
        errors.push(ValidationError::new(
            path,
            "runtime filter has no consumers",
        ));
    }
    let equality_witnesses = filter
        .equality_witnesses
        .iter()
        .map(|witness| witness.id)
        .collect::<BTreeSet<_>>();
    if equality_witnesses.len() != filter.equality_witnesses.len() {
        errors.push(ValidationError::new(
            path,
            "runtime filter equality witness identities are not unique",
        ));
    }
    let equality_anchors = filter
        .equality_witnesses
        .iter()
        .map(|witness| {
            (
                witness.fragment,
                witness.join,
                witness.key_ordinal,
                witness.domain_side,
            )
        })
        .collect::<BTreeSet<_>>();
    if equality_anchors.len() != filter.equality_witnesses.len() {
        errors.push(ValidationError::new(
            path,
            "runtime filter equality witnesses duplicate one hash-join key direction",
        ));
    }
    let producer_equalities = filter
        .producers
        .iter()
        .map(|producer| match &producer.target {
            crate::RuntimeFilterProducerTarget::JoinBuildKey { equality } => *equality,
        })
        .collect::<BTreeSet<_>>();
    let consumer_equalities = filter
        .consumers
        .iter()
        .map(|consumer| match &consumer.target {
            crate::RuntimeFilterConsumerTarget::JoinProbeKey { equality }
            | crate::RuntimeFilterConsumerTarget::ScanField { equality, .. } => *equality,
        })
        .collect::<BTreeSet<_>>();
    if !consumer_equalities.is_subset(&producer_equalities) {
        errors.push(ValidationError::new(
            path,
            "runtime filter consumer equality witness has no producer for the same join key",
        ));
    }
    let referenced_equalities = producer_equalities
        .union(&consumer_equalities)
        .copied()
        .collect::<BTreeSet<_>>();
    if equality_witnesses != referenced_equalities {
        errors.push(ValidationError::new(
            path,
            "runtime filter equality witness references differ from their definitions",
        ));
    }
    if filter.policy.max_contribution_bytes == 0
        || filter.policy.max_artifact_bytes == 0
        || filter.policy.deadline_ms == 0
        || filter.policy.max_retries == 0
    {
        errors.push(ValidationError::new(
            path,
            "runtime filter policy has a zero byte, time, or retry bound",
        ));
    }
    if filter.policy.max_contribution_bytes > filter.policy.max_artifact_bytes
        || filter.policy.max_artifact_bytes > MAX_RUNTIME_FILTER_ARTIFACT_BYTES
        || filter.policy.deadline_ms > MAX_RUNTIME_FILTER_DEADLINE_MS
        || filter.policy.max_retries > MAX_RUNTIME_FILTER_RETRIES
    {
        errors.push(ValidationError::new(
            path,
            "runtime filter policy exceeds its resource bounds",
        ));
    }
    match (&filter.domain, filter.reduction) {
        (
            crate::RuntimeFilterDomain::Membership { .. },
            crate::RuntimeFilterReduction::SetUnion,
        )
        | (
            crate::RuntimeFilterDomain::Ordered { .. },
            crate::RuntimeFilterReduction::UnionOrderedHull,
        ) => {}
        _ => errors.push(ValidationError::new(
            path,
            "runtime filter domain and reduction are inconsistent",
        )),
    }
    let producer_witnesses = filter
        .producers
        .iter()
        .map(|producer| producer.witness)
        .collect::<BTreeSet<_>>();
    if producer_witnesses.len() != filter.producers.len() {
        errors.push(ValidationError::new(
            path,
            "runtime filter producer witnesses are not unique",
        ));
    }
    let (availability, availability_safe) = validate_runtime_filter_coverage(
        &filter.availability_coverage,
        "availability",
        path,
        errors,
    );
    if availability != producer_witnesses {
        errors.push(ValidationError::new(
            path,
            "runtime filter availability coverage differs from its producer witnesses",
        ));
    }
    let (terminal, terminal_safe) =
        validate_runtime_filter_coverage(&filter.terminal_coverage, "terminal", path, errors);
    if !terminal.is_subset(&producer_witnesses) {
        errors.push(ValidationError::new(
            path,
            "runtime filter terminal coverage references an unknown producer witness",
        ));
    }
    for producer in &filter.producers {
        if producer.contribution_kinds.is_empty() {
            errors.push(ValidationError::new(
                path,
                "runtime filter producer has no contribution kinds",
            ));
        }
    }
    for consumer in &filter.consumers {
        if consumer.capabilities.is_empty() {
            errors.push(ValidationError::new(
                path,
                "runtime filter consumer has no artifact capabilities",
            ));
        }
    }
    validate_runtime_filter_matrix(filter, availability_safe && terminal_safe, path, errors);
    true
}

fn validate_runtime_filter_matrix(
    filter: &crate::RuntimeFilter,
    coverage_comparison_safe: bool,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if coverage_comparison_safe && filter.availability_coverage != filter.terminal_coverage {
        errors.push(ValidationError::new(
            path,
            "complete-once runtime filter has different availability and terminal coverage",
        ));
    }

    let (expected_contributions, expected_completion, required_capabilities, exact_capabilities) =
        match (&filter.domain, filter.reduction) {
            (
                crate::RuntimeFilterDomain::Membership { null_semantics, .. },
                crate::RuntimeFilterReduction::SetUnion,
            ) => {
                if !coverage_is_all_of_only(&filter.availability_coverage)
                    || (coverage_comparison_safe
                        && filter.availability_coverage != filter.terminal_coverage)
                {
                    errors.push(ValidationError::new(
                        path,
                        "membership runtime filter requires complete-once identical AllOf coverage",
                    ));
                }
                let has_final_shard = filter.producers.iter().any(|producer| {
                    producer
                        .contribution_kinds
                        .contains(&crate::RuntimeFilterContributionKind::FinalDomainShard)
                });
                if has_final_shard
                    && (*null_semantics != crate::RuntimeFilterNullSemantics::NullSafeEqual
                        || !coverage_is_all_of_only(&filter.availability_coverage))
                {
                    errors.push(ValidationError::new(
                        path,
                        "fenced final-domain runtime filter requires null-safe AllOf coverage",
                    ));
                }
                let contributions = if has_final_shard {
                    BTreeSet::from([
                        crate::RuntimeFilterContributionKind::FinalDomainShard,
                        crate::RuntimeFilterContributionKind::ProducerClosed,
                    ])
                } else {
                    BTreeSet::from([
                        crate::RuntimeFilterContributionKind::ValueDomainDelta,
                        crate::RuntimeFilterContributionKind::ProducerClosed,
                    ])
                };
                (
                    contributions,
                    if has_final_shard {
                        crate::RuntimeFilterCompletion::FencedCommittedDomain
                    } else {
                        crate::RuntimeFilterCompletion::ProducerClosed
                    },
                    BTreeSet::from([
                        crate::RuntimeFilterArtifactCapability::Membership,
                        crate::RuntimeFilterArtifactCapability::EmptyDomain,
                    ]),
                    false,
                )
            }
            (
                crate::RuntimeFilterDomain::Ordered { .. },
                crate::RuntimeFilterReduction::UnionOrderedHull,
            ) => {
                if !coverage_is_all_of_only(&filter.availability_coverage)
                    || (coverage_comparison_safe
                        && filter.availability_coverage != filter.terminal_coverage)
                {
                    errors.push(ValidationError::new(
                        path,
                        "ordered hull runtime filter requires complete-once identical AllOf coverage",
                    ));
                }
                (
                    BTreeSet::from([
                        crate::RuntimeFilterContributionKind::FinalOrderedHullShard,
                        crate::RuntimeFilterContributionKind::ProducerClosed,
                    ]),
                    crate::RuntimeFilterCompletion::FencedCommittedDomain,
                    BTreeSet::from([
                        crate::RuntimeFilterArtifactCapability::OrderedRange,
                        crate::RuntimeFilterArtifactCapability::EmptyDomain,
                    ]),
                    true,
                )
            }
            _ => return,
        };

    for producer in &filter.producers {
        let contributions = producer
            .contribution_kinds
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if contributions.len() != producer.contribution_kinds.len()
            || producer
                .contribution_kinds
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || contributions != expected_contributions
        {
            errors.push(ValidationError::new(
                path,
                "runtime filter producer contributions differ from the channel matrix",
            ));
        }
        if producer.completion != expected_completion {
            errors.push(ValidationError::new(
                path,
                "runtime filter producer completion differs from the channel matrix",
            ));
        }
    }
    for consumer in &filter.consumers {
        let capabilities = consumer
            .capabilities
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let invalid = capabilities.len() != consumer.capabilities.len()
            || consumer
                .capabilities
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || if exact_capabilities {
                capabilities != required_capabilities
            } else {
                !required_capabilities.is_subset(&capabilities)
            };
        if invalid {
            errors.push(ValidationError::new(
                path,
                "runtime filter consumer capabilities differ from the channel matrix",
            ));
        }
    }
}

fn coverage_is_all_of_only(coverage: &crate::RuntimeFilterCoverage) -> bool {
    let Some(crate::RuntimeFilterCoverageNode::AllOf { .. }) = usize::try_from(coverage.root)
        .ok()
        .and_then(|root| coverage.nodes.get(root))
    else {
        return false;
    };
    let mut pending = vec![coverage.root];
    let mut visited = BTreeSet::new();
    while let Some(current) = pending.pop() {
        if !visited.insert(current) {
            continue;
        }
        match usize::try_from(current)
            .ok()
            .and_then(|index| coverage.nodes.get(index))
        {
            Some(crate::RuntimeFilterCoverageNode::Witness(_)) => {}
            Some(crate::RuntimeFilterCoverageNode::AllOf { children }) => {
                pending.extend(children.iter().copied());
            }
            Some(crate::RuntimeFilterCoverageNode::AnyOf { .. }) | None => return false,
        }
    }
    true
}

fn validate_runtime_filter_coverage(
    coverage: &crate::RuntimeFilterCoverage,
    label: &str,
    path: &str,
    errors: &mut ValidationErrorCollector,
) -> (BTreeSet<crate::RuntimeFilterWitnessId>, bool) {
    if coverage.nodes.is_empty() || coverage.nodes.len() > MAX_RUNTIME_FILTER_COVERAGE_NODES {
        errors.push(ValidationError::new(
            path,
            format!(
                "runtime filter {label} coverage requires 1..={} arena nodes",
                MAX_RUNTIME_FILTER_COVERAGE_NODES
            ),
        ));
        return (BTreeSet::new(), false);
    }
    let Ok(root) = usize::try_from(coverage.root) else {
        errors.push(ValidationError::new(
            path,
            format!("runtime filter {label} coverage root is outside its arena"),
        ));
        return (BTreeSet::new(), false);
    };
    if root >= coverage.nodes.len() {
        errors.push(ValidationError::new(
            path,
            format!("runtime filter {label} coverage root is outside its arena"),
        ));
        return (BTreeSet::new(), false);
    }

    let mut witnesses = Vec::new();
    let mut safe = true;
    let mut depths = Vec::with_capacity(coverage.nodes.len());
    let mut child_references = 0_usize;
    for (index, node) in coverage.nodes.iter().enumerate() {
        match node {
            crate::RuntimeFilterCoverageNode::Witness(_) => depths.push(1_usize),
            crate::RuntimeFilterCoverageNode::AllOf { children }
            | crate::RuntimeFilterCoverageNode::AnyOf { children } => {
                if children.is_empty() {
                    errors.push(ValidationError::new(
                        path,
                        format!("runtime filter {label} coverage has an empty composite"),
                    ));
                }
                child_references = child_references.saturating_add(children.len());
                if child_references > MAX_RUNTIME_FILTER_COVERAGE_NODES {
                    safe = false;
                    errors.push(ValidationError::new(
                        path,
                        format!(
                            "runtime filter {label} coverage exceeds {} child references",
                            MAX_RUNTIME_FILTER_COVERAGE_NODES
                        ),
                    ));
                    break;
                }
                if children.windows(2).any(|pair| pair[0] >= pair[1]) {
                    errors.push(ValidationError::new(
                        path,
                        format!(
                            "runtime filter {label} coverage children are not strictly ordered"
                        ),
                    ));
                }
                let child_depth = children
                    .iter()
                    .filter_map(|child| usize::try_from(*child).ok())
                    .filter_map(|child| depths.get(child))
                    .copied()
                    .max()
                    .unwrap_or(0);
                if children
                    .iter()
                    .any(|child| usize::try_from(*child).map_or(true, |child| child >= index))
                {
                    safe = false;
                    errors.push(ValidationError::new(
                        path,
                        format!(
                            "runtime filter {label} coverage child does not precede its parent"
                        ),
                    ));
                }
                let depth = child_depth.saturating_add(1);
                if depth > MAX_RUNTIME_FILTER_COVERAGE_DEPTH {
                    safe = false;
                    errors.push(ValidationError::new(
                        path,
                        format!(
                            "runtime filter {label} coverage exceeds semantic depth {}",
                            MAX_RUNTIME_FILTER_COVERAGE_DEPTH
                        ),
                    ));
                }
                depths.push(depth);
            }
        }
    }
    let mut reachable = BTreeSet::new();
    let mut pending = vec![coverage.root];
    while let Some(current) = pending.pop() {
        if !reachable.insert(current) {
            continue;
        }
        match usize::try_from(current)
            .ok()
            .and_then(|index| coverage.nodes.get(index))
        {
            Some(crate::RuntimeFilterCoverageNode::Witness(witness)) => {
                witnesses.push(*witness);
            }
            Some(crate::RuntimeFilterCoverageNode::AllOf { children })
            | Some(crate::RuntimeFilterCoverageNode::AnyOf { children }) => {
                pending.extend(children.iter().copied());
            }
            None => safe = false,
        }
    }
    if reachable.len() != coverage.nodes.len() {
        safe = false;
        errors.push(ValidationError::new(
            path,
            format!("runtime filter {label} coverage arena contains unreachable nodes"),
        ));
    }
    let unique = witnesses.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != witnesses.len() {
        errors.push(ValidationError::new(
            path,
            format!("runtime filter {label} coverage repeats a producer witness"),
        ));
    }
    (unique, safe)
}

fn validate_runtime_filter_producer_progress(
    fragment: &Fragment,
    producer: &crate::RuntimeFilterProducer,
    inbound_edges: &BTreeSet<EdgeId>,
    indexes: &mut RuntimeFilterLineageIndexes,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let crate::RuntimeFilterProducerProgress {
        build_edges,
        non_build_edges,
    } = &producer.progress;
    let Some(node) = fragment.nodes().get(&producer.endpoint.node) else {
        return;
    };
    let NodeKind::HashJoin { build_side, .. } = &node.kind else {
        errors.push(ValidationError::new(
            path,
            "runtime filter join-build progress does not belong to a hash-join build producer",
        ));
        return;
    };
    if !matches!(
        producer.target,
        crate::RuntimeFilterProducerTarget::JoinBuildKey { .. }
    ) || node.inputs.len() != 2
    {
        errors.push(ValidationError::new(
            path,
            "runtime filter join-build progress does not belong to a hash-join build producer",
        ));
        return;
    }
    for (label, edges) in [("build", build_edges), ("non-build", non_build_edges)] {
        if edges.windows(2).any(|pair| pair[0] >= pair[1]) {
            errors.push(ValidationError::new(
                path,
                format!("runtime filter {label} frontier edges are not strictly increasing"),
            ));
        }
        if edges.iter().any(|edge| !inbound_edges.contains(edge)) {
            errors.push(ValidationError::new(
                path,
                format!("runtime filter {label} frontier names a non-inbound edge"),
            ));
        }
    }
    let declared_build = build_edges.iter().copied().collect::<BTreeSet<_>>();
    let declared_non_build = non_build_edges.iter().copied().collect::<BTreeSet<_>>();
    if !declared_build.is_disjoint(&declared_non_build) {
        errors.push(ValidationError::new(
            path,
            "runtime filter build and non-build frontier edges overlap",
        ));
    }
    let expected = indexes.build_frontier(
        fragment,
        node.inputs[usize::try_from(build_side.input_ordinal()).unwrap()],
        inbound_edges,
    );
    if declared_build != expected.build || declared_non_build != expected.non_build {
        errors.push(ValidationError::new(
            path,
            "runtime filter join-build frontier differs from the exact join input exchange cuts",
        ));
    }
}

fn collect_subtree_exchange_edges(fragment: &Fragment, root: NodeId) -> BTreeSet<EdgeId> {
    let mut edges = BTreeSet::new();
    let mut visited = BTreeSet::new();
    let mut pending = vec![root];
    while let Some(node_id) = pending.pop() {
        if !visited.insert(node_id) {
            continue;
        }
        let Some(node) = fragment.nodes().get(&node_id) else {
            continue;
        };
        if let NodeKind::ExchangeSource { edge, .. } = node.kind {
            edges.insert(edge);
        }
        pending.extend(node.inputs.iter().copied());
    }
    edges
}

fn validate_runtime_filter_endpoint(
    plan: &PhysicalPlan,
    endpoint: &RuntimeFilterEndpoint,
    domain: &crate::RuntimeFilterDomain,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    match plan.fragments().get(&endpoint.fragment) {
        Some(fragment) => {
            validate_runtime_filter_endpoint_in_fragment(fragment, endpoint, domain, path, errors)
        }
        None => errors.push(ValidationError::new(
            path,
            "runtime filter fragment is not defined",
        )),
    }
}

fn validate_runtime_filter_endpoint_in_fragment(
    fragment: &Fragment,
    endpoint: &RuntimeFilterEndpoint,
    domain: &crate::RuntimeFilterDomain,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    require_node(fragment, endpoint.node, path, errors);
    let expected = match domain {
        crate::RuntimeFilterDomain::Membership { ty, .. } => std::slice::from_ref(ty),
        // Checked below because the ordered domain wraps its scalar type.
        crate::RuntimeFilterDomain::Ordered { .. } => &[],
    };
    let expected_len = match domain {
        crate::RuntimeFilterDomain::Membership { .. } => expected.len(),
        crate::RuntimeFilterDomain::Ordered { .. } => 1,
    };
    if endpoint.values.len() != expected_len {
        errors.push(ValidationError::new(
            path,
            "runtime filter endpoint width differs from its logical domain",
        ));
    }
    for (ordinal, value_id) in endpoint.values.iter().enumerate() {
        match fragment.values().get(value_id) {
            Some(value) => {
                let expected_type = match domain {
                    crate::RuntimeFilterDomain::Membership { ty, .. } => Some(ty),
                    crate::RuntimeFilterDomain::Ordered { key, .. } => {
                        (ordinal == 0).then_some(&key.ty)
                    }
                };
                if expected_type.is_some_and(|expected| expected != &value.ty) {
                    errors.push(ValidationError::new(
                        path,
                        "runtime filter endpoint type differs from its domain",
                    ));
                }
            }
            None => require_value(fragment, *value_id, path, errors),
        }
    }
}

fn validate_apply_point(
    plan: &PhysicalPlan,
    indexes: &mut RuntimeFilterLineageIndexes,
    endpoint: &RuntimeFilterEndpoint,
    apply_point: crate::RuntimeFilterApplyPoint,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if let Some(fragment) = plan.fragments().get(&endpoint.fragment) {
        validate_apply_point_in_fragment(fragment, indexes, endpoint, apply_point, path, errors);
    }
}

fn validate_apply_point_in_fragment(
    fragment: &Fragment,
    indexes: &mut RuntimeFilterLineageIndexes,
    endpoint: &RuntimeFilterEndpoint,
    apply_point: crate::RuntimeFilterApplyPoint,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if let Some(node) = fragment.nodes().get(&endpoint.node) {
        match indexes.apply_port_contains_all(fragment, node, apply_point, &endpoint.values) {
            Some(true) => {}
            Some(false) => errors.push(ValidationError::new(
                path,
                "runtime filter endpoint value is absent from its exact apply port",
            )),
            None => errors.push(ValidationError::new(
                path,
                "runtime filter apply point is invalid for its node",
            )),
        }
    }
}

fn runtime_filter_equality_witness<'a>(
    witnesses: &RuntimeFilterWitnessIndex<'a>,
    id: crate::RuntimeFilterEqualityWitnessId,
) -> Option<&'a crate::RuntimeFilterEqualityWitness> {
    witnesses.get(&id).copied().flatten()
}

fn equality_key_value(
    fragment: &Fragment,
    witness: &crate::RuntimeFilterEqualityWitness,
    side: crate::JoinSide,
) -> Option<ValueId> {
    let node = fragment.nodes().get(&witness.join)?;
    let NodeKind::HashJoin { keys, .. } = &node.kind else {
        return None;
    };
    let key = keys.get(usize::try_from(witness.key_ordinal).ok()?)?;
    expression_value(
        fragment,
        match side {
            crate::JoinSide::Left => key.left,
            crate::JoinSide::Right => key.right,
        },
    )
}

fn validate_runtime_filter_equality_witness(
    fragment: &Fragment,
    witness: &crate::RuntimeFilterEqualityWitness,
    domain: &crate::RuntimeFilterDomain,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let valid = fragment.nodes().get(&witness.join).is_some_and(|node| {
        let NodeKind::HashJoin {
            kind,
            keys,
            build_side,
            ..
        } = &node.kind
        else {
            return false;
        };
        if witness.fragment != fragment.id() || *build_side != witness.domain_side {
            return false;
        }
        let Some(key) = usize::try_from(witness.key_ordinal)
            .ok()
            .and_then(|ordinal| keys.get(ordinal))
        else {
            return false;
        };
        let domain_type = match witness.domain_side {
            crate::JoinSide::Left => fragment.expressions().get(key.left).map(|expr| &expr.ty),
            crate::JoinSide::Right => fragment.expressions().get(key.right).map(|expr| &expr.ty),
        };
        let domain_matches = match (domain, domain_type) {
            (crate::RuntimeFilterDomain::Membership { ty, null_semantics }, Some(actual)) => {
                ty == actual
                    && *null_semantics
                        == if key.null_safe {
                            crate::RuntimeFilterNullSemantics::NullSafeEqual
                        } else {
                            crate::RuntimeFilterNullSemantics::NeverMatches
                        }
            }
            (
                crate::RuntimeFilterDomain::Ordered {
                    key: domain_key, ..
                },
                Some(actual),
            ) => !key.null_safe && domain_key.ty == *actual,
            (_, None) => false,
        };
        domain_matches
            && matches!(
                (kind, witness.domain_side),
                (crate::JoinKind::Inner, _)
                    | (crate::JoinKind::LeftOuter, crate::JoinSide::Left)
                    | (crate::JoinKind::RightOuter, crate::JoinSide::Right)
                    | (crate::JoinKind::LeftSemi, crate::JoinSide::Right)
                    | (crate::JoinKind::RightSemi, crate::JoinSide::Left)
            )
    });
    if !valid {
        errors.push(ValidationError::new(
            path,
            "runtime filter equality witness does not prove a safe hash-join key direction",
        ));
    }
}

fn validate_runtime_filter_producer_target(
    fragment: &Fragment,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    producer: &crate::RuntimeFilterProducer,
    _domain: &crate::RuntimeFilterDomain,
    reduction: crate::RuntimeFilterReduction,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    let valid = match producer.target {
        crate::RuntimeFilterProducerTarget::JoinBuildKey { equality }
            if matches!(
                reduction,
                crate::RuntimeFilterReduction::SetUnion
                    | crate::RuntimeFilterReduction::UnionOrderedHull
            ) =>
        {
            runtime_filter_equality_witness(witnesses, equality).is_some_and(|witness| {
                witness.fragment == fragment.id()
                    && witness.join == producer.endpoint.node
                    && producer.apply_point
                        == (crate::RuntimeFilterApplyPoint::NodeInput {
                            input_ordinal: witness.domain_side.input_ordinal(),
                        })
                    && equality_key_value(fragment, witness, witness.domain_side)
                        .is_some_and(|value| producer.endpoint.values.as_ref() == [value])
            })
        }
        _ => false,
    };
    if !valid {
        errors.push(ValidationError::new(
            path,
            "runtime filter producer target does not match its equality witness and exact build key",
        ));
    }
}

fn validate_runtime_filter_consumer_semantics(
    fragment: &Fragment,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    consumer: &crate::RuntimeFilterConsumer,
    indexes: &mut RuntimeFilterLineageIndexes,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if let crate::RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete { late_apply } =
        consumer.activation
    {
        let supported = match consumer.target {
            crate::RuntimeFilterConsumerTarget::JoinProbeKey { .. } => matches!(
                late_apply,
                crate::LateApplyGranularity::Row | crate::LateApplyGranularity::Batch
            ),
            crate::RuntimeFilterConsumerTarget::ScanField { .. } => matches!(
                late_apply,
                crate::LateApplyGranularity::RowGroup
                    | crate::LateApplyGranularity::Split
                    | crate::LateApplyGranularity::File
            ),
        };
        if !supported {
            errors.push(ValidationError::new(
                path,
                "runtime filter late-apply granularity is unsupported at its consumer target",
            ));
        }
    }

    let valid = match &consumer.target {
        crate::RuntimeFilterConsumerTarget::JoinProbeKey { equality } => {
            runtime_filter_equality_witness(witnesses, *equality).is_some_and(|witness| {
                let probe_side = witness.domain_side.opposite();
                witness.fragment == fragment.id()
                    && witness.join == consumer.endpoint.node
                    && consumer.apply_point
                        == (crate::RuntimeFilterApplyPoint::NodeInput {
                            input_ordinal: probe_side.input_ordinal(),
                        })
                    && equality_key_value(fragment, witness, probe_side)
                        .is_some_and(|value| consumer.endpoint.values.as_ref() == [value])
            })
        }
        crate::RuntimeFilterConsumerTarget::ScanField { equality, .. } => {
            runtime_filter_equality_witness(witnesses, *equality).is_some()
                && consumer.apply_point == crate::RuntimeFilterApplyPoint::ScanSource
                && fragment
                    .nodes()
                    .get(&consumer.endpoint.node)
                    .is_some_and(|node| {
                        consumer.endpoint.values.len() == 1
                            && indexes.scan_provider_contains(
                                fragment,
                                node,
                                consumer.endpoint.values[0],
                            )
                    })
        }
    };
    if !valid {
        errors.push(ValidationError::new(
            path,
            "runtime filter consumer target is not locally valid for its equality witness",
        ));
    }
}

fn validate_runtime_filter_consumer_lineage(
    plan: &PhysicalPlan,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    consumer: &crate::RuntimeFilterConsumer,
    path: &str,
    indexes: &mut RuntimeFilterLineageIndexes,
    errors: &mut ValidationErrorCollector,
) {
    let crate::RuntimeFilterConsumerTarget::ScanField { equality, lineage } = &consumer.target
    else {
        return;
    };
    if lineage.len() > MAX_RUNTIME_FILTER_LINEAGE_STEPS
        || runtime_filter_scan_lineage_is_valid(
            plan, witnesses, consumer, *equality, lineage, indexes,
        )
        .is_none()
    {
        errors.push(ValidationError::new(
            path,
            "runtime filter scan consumer is not connected to its exact probe key by a safe lineage",
        ));
    }
}

fn runtime_filter_scan_lineage_is_valid(
    plan: &PhysicalPlan,
    witnesses: &RuntimeFilterWitnessIndex<'_>,
    consumer: &crate::RuntimeFilterConsumer,
    equality: crate::RuntimeFilterEqualityWitnessId,
    lineage: &[crate::RuntimeFilterLineageStep],
    indexes: &mut RuntimeFilterLineageIndexes,
) -> Option<()> {
    let witness = runtime_filter_equality_witness(witnesses, equality)?;
    let witness_fragment = plan.fragments().get(&witness.fragment)?;
    let join = witness_fragment.nodes().get(&witness.join)?;
    let probe_side = witness.domain_side.opposite();
    let probe_value = equality_key_value(witness_fragment, witness, probe_side)?;
    let probe_input = join
        .inputs
        .get(usize::try_from(probe_side.input_ordinal()).ok()?)
        .copied()?;
    if !node_has_exact_parent(
        &mut indexes.parents,
        witness_fragment,
        probe_input,
        witness.join,
    ) {
        return None;
    }

    let mut position = (witness.fragment, probe_input, probe_value);
    let mut visited = BTreeSet::from([position]);
    for step in lineage {
        let next = match *step {
            crate::RuntimeFilterLineageStep::FilterPassThrough {
                fragment,
                node,
                input_ordinal,
            } => {
                if (fragment, node) != (position.0, position.1) {
                    return None;
                }
                let fragment = plan.fragments().get(&fragment)?;
                let node = fragment.nodes().get(&node)?;
                if !matches!(node.kind, NodeKind::Filter { .. })
                    || node.inputs.len() != 1
                    || input_ordinal != 0
                    || !indexes.port_contains(fragment, node, position.2)
                {
                    return None;
                }
                let child = node.inputs[0];
                let child_node = fragment.nodes().get(&child)?;
                if !indexes.port_contains(fragment, child_node, position.2)
                    || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
                {
                    return None;
                }
                (fragment.id(), child, position.2)
            }
            crate::RuntimeFilterLineageStep::SortPassThrough {
                fragment,
                node,
                input_ordinal,
            } => {
                if (fragment, node) != (position.0, position.1) {
                    return None;
                }
                let fragment = plan.fragments().get(&fragment)?;
                let node = fragment.nodes().get(&node)?;
                let safe_sort = matches!(
                    &node.kind,
                    NodeKind::Sort {
                        order_by,
                        mode: crate::SortMode::Global | crate::SortMode::Analytic { .. },
                    } if order_by
                        .iter()
                        .all(|item| expression_value(fragment, item.expr).is_some())
                );
                let child = node
                    .inputs
                    .get(usize::try_from(input_ordinal).ok()?)
                    .copied()?;
                let child_node = fragment.nodes().get(&child)?;
                if !safe_sort
                    || node.inputs.len() != 1
                    || input_ordinal != 0
                    || !indexes.port_contains(fragment, node, position.2)
                    || !indexes.port_contains(fragment, child_node, position.2)
                    || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
                {
                    return None;
                }
                (fragment.id(), child, position.2)
            }
            crate::RuntimeFilterLineageStep::ProjectIdentity {
                fragment,
                node,
                output_ordinal,
            } => {
                if (fragment, node) != (position.0, position.1) {
                    return None;
                }
                let fragment = plan.fragments().get(&fragment)?;
                let node = fragment.nodes().get(&node)?;
                let NodeKind::Project { expressions } = &node.kind else {
                    return None;
                };
                if node.inputs.len() != 1
                    || node
                        .output
                        .columns
                        .get(usize::try_from(output_ordinal).ok()?)
                        .copied()
                        != Some(position.2)
                {
                    return None;
                }
                let child = node.inputs[0];
                let child_node = fragment.nodes().get(&child)?;
                let source = match indexes.project_source(fragment, node, expressions, position.2) {
                    Some(Some(value)) => value,
                    None if indexes.port_contains(fragment, child_node, position.2) => position.2,
                    _ => return None,
                };
                if !indexes.port_contains(fragment, child_node, source)
                    || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
                {
                    return None;
                }
                (fragment.id(), child, source)
            }
            crate::RuntimeFilterLineageStep::AggregateGroupKey {
                fragment,
                node,
                group_key_ordinal,
            } => {
                if (fragment, node) != (position.0, position.1) {
                    return None;
                }
                let fragment = plan.fragments().get(&fragment)?;
                let node = fragment.nodes().get(&node)?;
                let NodeKind::Aggregate { group_by, .. } = &node.kind else {
                    return None;
                };
                let (expression, output) = group_by
                    .get(usize::try_from(group_key_ordinal).ok()?)
                    .copied()?;
                let source = expression_value(fragment, expression)?;
                let child = *node.inputs.first()?;
                let child_node = fragment.nodes().get(&child)?;
                if node.inputs.len() != 1
                    || output != position.2
                    || !indexes.port_contains(fragment, child_node, source)
                    || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
                {
                    return None;
                }
                (fragment.id(), child, source)
            }
            crate::RuntimeFilterLineageStep::UnionAllBranch {
                fragment,
                node,
                input_ordinal,
                output_ordinal,
            } => {
                if (fragment, node) != (position.0, position.1) {
                    return None;
                }
                let fragment = plan.fragments().get(&fragment)?;
                let node = fragment.nodes().get(&node)?;
                let NodeKind::SetOp {
                    kind: crate::SetOperationKind::UnionAll,
                    input_mappings,
                } = &node.kind
                else {
                    return None;
                };
                let input_ordinal = usize::try_from(input_ordinal).ok()?;
                let output_ordinal = usize::try_from(output_ordinal).ok()?;
                if node.output.columns.get(output_ordinal).copied() != Some(position.2) {
                    return None;
                }
                let child = *node.inputs.get(input_ordinal)?;
                let source = *input_mappings.get(input_ordinal)?.get(output_ordinal)?;
                let child_node = fragment.nodes().get(&child)?;
                if !indexes.port_contains(fragment, child_node, source)
                    || !node_has_exact_parent(&mut indexes.parents, fragment, child, node.id)
                {
                    return None;
                }
                (fragment.id(), child, source)
            }
            crate::RuntimeFilterLineageStep::ExchangeMapping {
                edge,
                mapping_ordinal,
            } => {
                let fragment = plan.fragments().get(&position.0)?;
                let node = fragment.nodes().get(&position.1)?;
                let NodeKind::ExchangeSource {
                    edge: node_edge,
                    imports,
                } = &node.kind
                else {
                    return None;
                };
                let edge_contract = plan.edges().get(&edge)?;
                let ordinal = usize::try_from(mapping_ordinal).ok()?;
                let (source, destination) =
                    *edge_contract.destination.receive_mapping.get(ordinal)?;
                if *node_edge != edge
                    || edge_contract.kind != crate::EdgeKind::Stream
                    || edge_contract.destination.fragment != position.0
                    || edge_contract.destination.node != position.1
                    || destination != position.2
                    || imports.get(ordinal).copied() != Some((source, destination))
                    || edge_contract.source.projection.get(ordinal).copied() != Some(source)
                {
                    return None;
                }
                let source_fragment = plan.fragments().get(&edge_contract.source.fragment)?;
                let source_root = source_fragment.nodes().get(&source_fragment.root())?;
                if !indexes.port_contains(source_fragment, source_root, source)
                    || !matches!(source_fragment.sink(), FragmentSink::Stream { edge: sink_edge }
                        if *sink_edge == edge)
                {
                    return None;
                }
                (source_fragment.id(), source_fragment.root(), source)
            }
        };
        if !visited.insert(next) {
            return None;
        }
        position = next;
    }

    if position
        != (
            consumer.endpoint.fragment,
            consumer.endpoint.node,
            *consumer.endpoint.values.first()?,
        )
        || consumer.endpoint.values.len() != 1
        || consumer.apply_point != crate::RuntimeFilterApplyPoint::ScanSource
    {
        return None;
    }
    let fragment = plan.fragments().get(&position.0)?;
    let node = fragment.nodes().get(&position.1)?;
    indexes
        .scan_provider_contains(fragment, node, position.2)
        .then_some(())
}

fn node_has_exact_parent(
    indexes: &mut BTreeMap<FragmentId, BTreeMap<NodeId, Option<NodeId>>>,
    fragment: &Fragment,
    child: NodeId,
    expected_parent: NodeId,
) -> bool {
    let parents = indexes.entry(fragment.id()).or_insert_with(|| {
        let mut parents = BTreeMap::new();
        for node in fragment.nodes().values() {
            for child in &node.inputs {
                parents
                    .entry(*child)
                    .and_modify(|parent| *parent = None)
                    .or_insert(Some(node.id));
            }
        }
        parents
    });
    parents.get(&child).copied().flatten() == Some(expected_parent)
}

fn validate_artifact_sink(
    fragment: &Fragment,
    spec: &SealedArtifactSinkSpec,
    errors: &mut ValidationErrorCollector,
) {
    let path = format!("fragments[{}].sink.sealed_artifact", fragment.id().get());
    if spec.format.revision == 0
        || spec.input.is_empty()
        || spec.max_reference_bytes == 0
        || spec.max_reference_bytes > MAX_ARTIFACT_REFERENCE_BYTES
        || spec.source.selection_digest == [0; 32]
    {
        errors.push(ValidationError::new(
            &path,
            "sealed artifact sink requires a format revision, input and reference budget",
        ));
    }
    validate_read_reference(&spec.source.source, &path, errors);
    validate_coverage(&spec.required_coverage, &path, errors);
    if spec.required_coverage.selection_digest != spec.source.selection_digest {
        errors.push(ValidationError::new(
            &path,
            "artifact coverage is not bound to the exact source selection",
        ));
    }
    for ArtifactInputField { value, ty } in &spec.input {
        match fragment.values().get(value) {
            Some(definition) if definition.ty != *ty => errors.push(ValidationError::new(
                &path,
                "artifact input type differs from its value definition",
            )),
            Some(_) => {}
            None => require_value(fragment, *value, &path, errors),
        }
    }
    for value in &spec.partition_by {
        require_value(fragment, *value, &path, errors);
    }
    for key in &spec.order_by {
        require_value(fragment, key.value, &path, errors);
    }
    for value in &spec.group_boundaries {
        require_value(fragment, *value, &path, errors);
    }
    let Some(root) = fragment.nodes().get(&fragment.root()) else {
        return;
    };
    let input_values = spec
        .input
        .iter()
        .map(|field| field.value)
        .collect::<Vec<_>>();
    let input_value_index = ValuePortIndex::new(&input_values);
    if input_values.as_slice() != root.output.columns.as_ref() {
        errors.push(ValidationError::new(
            &path,
            "artifact input schema differs from the fragment root output",
        ));
    }
    if !spec.partition_by.is_empty() {
        match &root.output_properties.distribution {
            Distribution::Hash { keys, .. } | Distribution::BucketShuffle { keys, .. }
                if keys.as_ref() == spec.partition_by.as_ref() => {}
            _ => errors.push(ValidationError::new(
                &path,
                "artifact partition requirement is not guaranteed by the fragment output",
            )),
        }
    } else if root.output_properties.distribution != Distribution::Singleton {
        errors.push(ValidationError::new(
            &path,
            "unpartitioned sealed artifact requires singleton placement",
        ));
    }
    if root.output_properties.row_multiplicity != RowMultiplicity::SingleCopy {
        errors.push(ValidationError::new(
            &path,
            "sealed artifact requires single-copy row ownership",
        ));
    }
    let required_order = spec
        .order_by
        .iter()
        .map(|key| crate::OrderingKey {
            value: key.value,
            direction: key.direction,
            null_ordering: key.null_ordering,
        })
        .collect::<Vec<_>>();
    if root.output_properties.ordering.len() < required_order.len()
        || root.output_properties.ordering[..required_order.len()] != required_order
    {
        errors.push(ValidationError::new(
            &path,
            "artifact ordering requirement is not guaranteed by the fragment root",
        ));
    }
    if spec
        .group_boundaries
        .iter()
        .any(|value| !input_value_index.contains(value))
    {
        errors.push(ValidationError::new(
            &path,
            "artifact group boundary is absent from the exact input schema",
        ));
    }
}

fn validate_artifact_refs(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    for artifact in plan.artifact_refs().values() {
        validate_artifact_ref(artifact, errors);
    }
}

fn validate_artifact_ref(artifact: &SealedArtifactRef, errors: &mut ValidationErrorCollector) {
    let path = format!("artifact_refs[{}]", artifact.id.get());
    if artifact.format.revision == 0 || artifact.schema.is_empty() {
        errors.push(ValidationError::new(
            &path,
            "artifact reference requires a format revision and schema",
        ));
    }
    if artifact.location.is_empty() || artifact.location.len() > 4096 {
        errors.push(ValidationError::new(
            &path,
            "artifact location must be bounded and non-empty",
        ));
    }
    if artifact.content_digest == [0; 32]
        || artifact.schema_digest == [0; 32]
        || artifact.source.selection_digest == [0; 32]
    {
        errors.push(ValidationError::new(
            &path,
            "artifact evidence digests must be non-zero",
        ));
    }
    validate_read_reference(&artifact.source.source, &path, errors);
    validate_coverage(&artifact.coverage, &path, errors);
    if artifact.coverage.selection_digest != artifact.source.selection_digest {
        errors.push(ValidationError::new(
            &path,
            "artifact coverage is not bound to the exact source selection",
        ));
    }
}

pub(crate) fn validate_coverage(
    coverage: &CoverageSet,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if coverage.domain.is_empty()
        || coverage.domain.len() > 1024
        || coverage.selection_digest == [0; 32]
        || coverage.ranges.is_empty()
    {
        errors.push(ValidationError::new(
            path,
            "coverage requires a bounded domain and at least one range",
        ));
        return;
    }
    for (index, range) in coverage.ranges.iter().enumerate() {
        if let (Some(start), Some(end)) = (&range.start, &range.end)
            && start.as_ref() >= end.as_ref()
        {
            errors.push(ValidationError::new(
                path,
                format!("coverage range {index} is empty or reversed"),
            ));
        }
        if index > 0 {
            let previous = &coverage.ranges[index - 1];
            let ordered = match (&previous.end, &range.start) {
                (Some(previous_end), Some(current_start)) => {
                    previous_end.as_ref() <= current_start.as_ref()
                }
                (Some(_), None) | (None, _) => false,
            };
            if !ordered {
                errors.push(ValidationError::new(
                    path,
                    format!("coverage ranges overlap or are out of order at {index}"),
                ));
            }
        }
    }
    if coverage.complete_input {
        let spans_complete_domain = coverage
            .ranges
            .first()
            .is_some_and(|range| range.start.is_none())
            && coverage
                .ranges
                .last()
                .is_some_and(|range| range.end.is_none())
            && coverage.ranges.windows(2).all(|pair| {
                matches!(
                    (&pair[0].end, &pair[1].start),
                    (Some(previous_end), Some(next_start))
                        if previous_end.as_ref() == next_start.as_ref()
                )
            });
        if !spans_complete_domain {
            errors.push(ValidationError::new(
                path,
                "complete coverage must form one gap-free unbounded domain",
            ));
        }
    }
}

fn validate_artifact_inputs(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    for fragment in plan.fragments().values() {
        for node in fragment.nodes().values() {
            if let NodeKind::Scan { relation, .. } = &node.kind {
                let path = format!(
                    "fragments[{}].nodes[{}].relation.artifact_inputs",
                    fragment.id().get(),
                    node.id.get()
                );
                for requirement in relation.artifact_inputs() {
                    match plan.artifact_refs().get(&requirement.artifact) {
                        Some(artifact)
                            if artifact.kind == requirement.kind
                                && artifact.format == requirement.format
                                && artifact.schema == requirement.schema
                                && artifact.source == requirement.source
                                && artifact.coverage == requirement.required_coverage => {}
                        Some(_) => errors.push(ValidationError::new(
                            &path,
                            format!(
                                "artifact {} differs from the relation's exact input requirement",
                                requirement.artifact.get()
                            ),
                        )),
                        None => errors.push(ValidationError::new(
                            &path,
                            format!(
                                "artifact reference {} is not defined",
                                requirement.artifact.get()
                            ),
                        )),
                    }
                }
            }
        }
    }
}

fn validate_annotations(plan: &PhysicalPlan, errors: &mut ValidationErrorCollector) {
    bounded_count(
        errors,
        "annotations",
        plan.annotations().len(),
        MAX_ANNOTATIONS,
    );
    let total_bytes = plan
        .annotations()
        .iter()
        .fold(0_usize, |total, annotation| {
            total
                .saturating_add(annotation.key.len())
                .saturating_add(annotation.value.len())
        });
    if total_bytes > MAX_ANNOTATION_BYTES {
        errors.push(ValidationError::new(
            "annotations",
            format!("contains {total_bytes} bytes, exceeding {MAX_ANNOTATION_BYTES}"),
        ));
    }
    for (index, annotation) in plan.annotations().iter().enumerate() {
        let valid = match annotation.subject {
            AnnotationSubject::Plan => true,
            AnnotationSubject::Fragment(fragment) => plan.fragments().contains_key(&fragment),
            AnnotationSubject::Node(fragment, node) => plan
                .fragments()
                .get(&fragment)
                .is_some_and(|fragment| fragment.nodes().contains_key(&node)),
            AnnotationSubject::Value(fragment, value) => plan
                .fragments()
                .get(&fragment)
                .is_some_and(|fragment| fragment.values().contains_key(&value)),
        };
        if !valid
            || annotation.key.is_empty()
            || annotation.key.len() > MAX_ANNOTATION_KEY_BYTES
            || annotation.value.len() > MAX_ANNOTATION_VALUE_BYTES
        {
            errors.push(ValidationError::new(
                format!("annotations[{index}]"),
                "annotation has an unknown subject or invalid key/value size",
            ));
        }
    }
}

fn validate_cross_fragment_value_origins(
    plan: &PhysicalPlan,
    errors: &mut ValidationErrorCollector,
) {
    let receive_mappings = plan
        .edges()
        .iter()
        .map(|(id, edge)| {
            (
                *id,
                edge.destination
                    .receive_mapping
                    .iter()
                    .copied()
                    .collect::<BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    for fragment in plan.fragments().values() {
        for value in fragment.values().values() {
            let path = format!(
                "fragments[{}].values[{}]",
                fragment.id().get(),
                value.id.get()
            );
            match value.origin {
                ValueOrigin::ExchangeImport { edge, source_value } => match plan.edges().get(&edge)
                {
                    Some(edge_contract)
                        if edge_contract.destination.fragment == fragment.id()
                            && receive_mappings.get(&edge).is_some_and(|mapping| {
                                mapping.contains(&(source_value, value.id))
                            }) => {}
                    Some(_) => errors.push(ValidationError::new(
                        &path,
                        "exchange import is not present in the edge receive mapping",
                    )),
                    None => {
                        errors.push(ValidationError::new(&path, "exchange edge is not defined"))
                    }
                },
                ValueOrigin::CteImport {
                    edge,
                    producer_fragment,
                    producer_value,
                } => match plan.edges().get(&edge) {
                    Some(edge_contract)
                        if edge_contract.kind == crate::EdgeKind::CteMulticast
                            && edge_contract.source.fragment == producer_fragment
                            && edge_contract.destination.fragment == fragment.id()
                            && receive_mappings.get(&edge).is_some_and(|mapping| {
                                mapping.contains(&(producer_value, value.id))
                            }) => {}
                    Some(_) => errors.push(ValidationError::new(
                        &path,
                        "CTE import is not present in the exact CTE edge mapping",
                    )),
                    None => errors.push(ValidationError::new(&path, "CTE edge is not defined")),
                },
                _ => {}
            }
        }
    }
}

fn require_node(
    fragment: &Fragment,
    node: NodeId,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if !fragment.nodes().contains_key(&node) {
        errors.push(ValidationError::new(
            path,
            format!("node {} is not defined", node.get()),
        ));
    }
}

fn require_value(
    fragment: &Fragment,
    value: ValueId,
    path: &str,
    errors: &mut ValidationErrorCollector,
) {
    if !fragment.values().contains_key(&value) {
        errors.push(ValidationError::new(
            path,
            format!("value {} is not defined", value.get()),
        ));
    }
}

#[allow(dead_code)]
fn _assert_required_contract_is_copy(_: RequiredContracts) {}

#[allow(dead_code)]
fn _assert_maps_are_deterministic(_: BTreeMap<FragmentId, Fragment>) {}

#[cfg(test)]
mod validation_error_tests {
    use super::*;

    #[test]
    fn validation_diagnostics_have_a_fixed_cardinality_and_display_bound() {
        let mut collector = ValidationErrorCollector::new();
        for ordinal in 0..(MAX_VALIDATION_ERRORS * 4) {
            collector.push(ValidationError::new(
                format!("expressions[{ordinal}]"),
                "invalid expression",
            ));
        }
        let errors = ValidationErrors::from_collector(collector);
        assert_eq!(errors.errors().len(), MAX_VALIDATION_ERRORS + 1);
        assert_eq!(
            errors
                .errors()
                .iter()
                .filter(|error| error.message.contains("truncated"))
                .count(),
            1
        );
        assert!(errors.to_string().len() < 16 * 1024);
    }

    #[test]
    fn binary_arithmetic_uses_the_type_contract_largeint_domain() {
        let expression = |id, data_type| crate::ExprNode {
            id: ExprId::new(id),
            owner: NodeId::new(1),
            lambda_scope: None,
            ty: ValueType::new(data_type, false),
            kind: ExprKind::Literal(crate::LiteralValue::Null),
        };
        let largeint = DataType::FixedSizeBinary(novarocks_type_contract::LARGEINT_BYTE_WIDTH);
        let left = expression(1, largeint.clone());
        let right = expression(2, DataType::Int64);
        let output = expression(3, largeint);
        let mut errors = ValidationErrorCollector::new();
        validate_binary_types(
            &left,
            crate::BinaryOperator::Add,
            &right,
            &output,
            "binary",
            &mut errors,
        );
        assert!(errors.is_empty());
    }

    #[test]
    fn occurrence_indexes_preserve_duplicate_mapping_semantics() {
        let source = ValueId::new(1);
        let other_source = ValueId::new(2);
        let destination = ValueId::new(3);
        let repeated =
            ValueMappingIndex::from_pairs(&[(source, destination), (source, destination)]);
        assert_eq!(repeated.resolve(destination, false), None);
        assert_eq!(repeated.resolve(destination, true), Some(source));
        assert!(repeated.contains(source, destination));

        let conflicting =
            ValueMappingIndex::from_pairs(&[(source, destination), (other_source, destination)]);
        assert_eq!(conflicting.resolve(destination, false), None);
        assert_eq!(conflicting.resolve(destination, true), None);

        let port = ValuePortIndex::new(&[source, source, other_source]);
        assert_eq!(port.occurrences.get(&source), Some(&2));
        assert!(port.contains(&other_source));
    }

    #[test]
    fn semantic_trace_mapping_index_is_charged_once_and_each_lookup_is_bounded() {
        let mapping = (0_u32..1024)
            .map(|ordinal| (ValueId::new(ordinal), ValueId::new(ordinal + 2048)))
            .collect::<Vec<_>>();
        let expected = [mapping[0].1, mapping[512].1, mapping[1023].1];
        let mut indexes = SemanticTraceIndexes::default();
        let mut budget = SemanticTraceWorkBudget::new();
        let initial = budget.remaining;

        assert!(
            indexes
                .map_edge_values(EdgeId::new(1), &mapping, &expected, false, &mut budget)
                .is_some()
        );
        assert_eq!(initial - budget.remaining, mapping.len() + expected.len());

        let after_first = budget.remaining;
        assert!(
            indexes
                .map_edge_values(EdgeId::new(1), &mapping, &expected, false, &mut budget)
                .is_some()
        );
        assert_eq!(after_first - budget.remaining, expected.len());
    }

    #[test]
    fn source_provenance_fanout_shares_the_complete_binding_set() {
        let source = CompactSourceBindingSet::from_ids(&(0..4096).collect::<Vec<_>>()).unwrap();
        let empty = CompactSourceBindingSet::from_ids(&[]).unwrap();

        let fanout = (0..1024)
            .map(|_| CompactSourceBindingSet::union(&empty, [source.clone()]))
            .collect::<Vec<_>>();

        assert!(fanout.iter().all(|set| Arc::ptr_eq(&set.ids, &source.ids)));
    }

    #[test]
    fn source_provenance_rejects_high_fanout_before_destination_materialization() {
        let mut items = 0;
        assert!(charge_provenance_cut_items(&mut items, 4096, 8193).is_none());
        assert!(items > MAX_PLAN_DERIVED_CUT_ITEMS);
    }

    #[test]
    fn runtime_filter_witness_index_fails_closed_on_duplicate_or_missing_identity() {
        let witness = crate::RuntimeFilterEqualityWitness {
            id: crate::RuntimeFilterEqualityWitnessId::new(1),
            fragment: FragmentId::new(1),
            join: NodeId::new(1),
            key_ordinal: 0,
            domain_side: crate::JoinSide::Right,
        };
        let witnesses = [witness, witness];
        let duplicated = runtime_filter_witness_index(&witnesses);

        assert!(
            runtime_filter_equality_witness(
                &duplicated,
                crate::RuntimeFilterEqualityWitnessId::new(1)
            )
            .is_none()
        );
        assert!(
            runtime_filter_equality_witness(
                &duplicated,
                crate::RuntimeFilterEqualityWitnessId::new(2)
            )
            .is_none()
        );
    }

    #[test]
    fn runtime_filter_indexes_build_one_large_port_and_frontier_per_shared_site() {
        let mut builder = crate::FragmentBuilder::new(FragmentId::new(901));
        let node = builder.reserve_node_id().unwrap();
        let values = (0..4096)
            .map(|ordinal| {
                builder
                    .add_value(
                        ValueType::new(DataType::Int64, false),
                        ValueOrigin::NodeOutput {
                            node,
                            output_ordinal: ordinal,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        builder
            .insert_node(PhysicalNode {
                id: node,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: crate::PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
                output: crate::OutputPort {
                    node,
                    columns: values.clone().into_boxed_slice(),
                },
                kind: NodeKind::Values {
                    rows: Box::default(),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                node,
                FragmentSink::Noop,
                crate::PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .unwrap();
        let node = fragment.nodes().get(&node).unwrap();
        let mut indexes = RuntimeFilterLineageIndexes::default();

        assert_eq!(
            indexes.apply_port_contains_all(
                &fragment,
                node,
                crate::RuntimeFilterApplyPoint::NodeOutput,
                &[values[0], values[4095]],
            ),
            Some(true)
        );
        assert_eq!(
            indexes.apply_port_contains_all(
                &fragment,
                node,
                crate::RuntimeFilterApplyPoint::NodeOutput,
                &[values[2048]],
            ),
            Some(true)
        );
        assert_eq!(indexes.apply_ports.len(), 1);

        let inbound = BTreeSet::new();
        assert!(
            indexes
                .build_frontier(&fragment, node.id, &inbound)
                .build
                .is_empty()
        );
        assert!(
            indexes
                .build_frontier(&fragment, node.id, &inbound)
                .build
                .is_empty()
        );
        assert_eq!(indexes.build_frontiers.len(), 1);
    }

    #[test]
    fn wide_identity_project_semantics_remain_valid() {
        let mut builder = crate::FragmentBuilder::new(FragmentId::new(902));
        let input = builder.reserve_node_id().unwrap();
        let value_type = ValueType::new(DataType::Int64, false);
        let input_values = (0..4096)
            .map(|ordinal| {
                builder
                    .add_value(
                        value_type.clone(),
                        ValueOrigin::NodeOutput {
                            node: input,
                            output_ordinal: ordinal,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let singleton = crate::PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        builder
            .insert_node(PhysicalNode {
                id: input,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: singleton.clone(),
                output: crate::OutputPort {
                    node: input,
                    columns: input_values.clone().into_boxed_slice(),
                },
                kind: NodeKind::Values {
                    rows: Box::default(),
                },
            })
            .unwrap();
        let project = builder.reserve_node_id().unwrap();
        let expressions = input_values
            .iter()
            .map(|input_value| {
                let expression = builder
                    .add_expression(project, value_type.clone(), ExprKind::Value(*input_value))
                    .unwrap();
                (expression, *input_value)
            })
            .collect::<Vec<_>>();
        let output_values = input_values.clone();
        builder
            .insert_node(PhysicalNode {
                id: project,
                inputs: Box::from([input]),
                required_inputs: Box::from([singleton.clone()]),
                output_properties: singleton,
                output: crate::OutputPort {
                    node: project,
                    columns: output_values.clone().into_boxed_slice(),
                },
                kind: NodeKind::Project {
                    expressions: expressions.clone().into_boxed_slice(),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                project,
                FragmentSink::Noop,
                crate::PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .expect("a wide identity project is semantically valid");
        validate_fragment(&fragment, &FragmentCuts::default())
            .expect("independent validation accepts the wide identity project");

        let project_node = fragment.nodes().get(&project).unwrap();
        let input_node = fragment.nodes().get(&input).unwrap();
        let mut indexes = SemanticTraceIndexes::default();
        let mut budget = SemanticTraceWorkBudget::new();
        assert_eq!(
            indexes
                .map_project_values(
                    &fragment,
                    project_node,
                    input_node,
                    &expressions,
                    &output_values,
                    &mut budget,
                )
                .as_deref(),
            Some(input_values.as_slice())
        );
    }

    #[test]
    fn wide_hash_distribution_keys_validate_one_exact_colocation_mapping() {
        let source_keys = (1..=4096).map(ValueId::new).collect::<Vec<_>>();
        let destination_keys = (5001..=9096).map(ValueId::new).collect::<Vec<_>>();
        let mapping = source_keys
            .iter()
            .copied()
            .zip(destination_keys.iter().copied())
            .collect::<Vec<_>>();
        let scheme = crate::HashPartitionScheme {
            space: novarocks_type_contract::PartitionSpaceId::try_new([17; 32]).unwrap(),
            count: crate::PartitionCountParameter {
                id: novarocks_type_contract::PartitionCountParameterId::try_new([18; 32]).unwrap(),
                admissible: crate::PartitionCountDomain {
                    min: 1,
                    max: 4096,
                    requires_power_of_two: true,
                },
            },
            definition: crate::HashDefinition::native_exchange(),
        };
        let partitioning = crate::EdgePartitioning {
            source: Distribution::Hash {
                keys: source_keys.clone().into_boxed_slice(),
                scheme: scheme.clone(),
            },
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Hash {
                keys: destination_keys.clone().into_boxed_slice(),
                scheme,
            },
            destination_multiplicity: RowMultiplicity::SingleCopy,
        };
        let mut errors = ValidationErrorCollector::new();

        validate_mapped_partitioning(&partitioning, &mapping, "wide_hash", &mut errors);

        assert!(errors.is_empty());
        assert!(mapped_partition_keys_match(
            &source_keys,
            &destination_keys,
            &mapping
        ));
        assert!(distribution_colocates_by(
            &partitioning.source,
            &source_keys
        ));
    }

    #[test]
    fn aggregate_sequence_index_builds_once_for_many_distinct_lookups_and_ambiguity() {
        let binding = |sequence| crate::AggregateBinding {
            function: crate::BoundFunction {
                function_id: crate::FunctionId::try_new("builtin/test_sum/v1").unwrap(),
                overload: crate::FunctionOverloadId::try_new("i64").unwrap(),
                kind: crate::FunctionKind::Aggregate,
                argument_types: Box::from([crate::FunctionArgumentType::Value(ValueType::new(
                    DataType::Int64,
                    false,
                ))]),
                result_type: ValueType::new(DataType::Int64, false),
                volatility: crate::FunctionVolatility::Immutable,
                argument_evaluation: crate::FunctionArgumentEvaluation::Eager,
                failure_behavior: crate::FunctionFailureBehavior::Propagate,
            },
            phase: AggregatePhase::Partial { sequence },
            logical_argument_count: 1,
            intermediate_type: ValueType::new(DataType::Binary, false),
            state_format: crate::AggregateStateFormatId::try_new("test_sum/state-v1").unwrap(),
        };
        let mut calls = (1..=4096)
            .map(|id| crate::AggregateCall {
                id: crate::AggregateCallId::new(id),
                binding: binding(crate::AggregateSequenceId::new(id)),
                arguments: Box::default(),
                distinct: false,
                order_by: Box::default(),
                output: ValueId::new(id),
            })
            .collect::<Vec<_>>();
        calls.push(crate::AggregateCall {
            id: crate::AggregateCallId::new(4097),
            binding: binding(crate::AggregateSequenceId::new(4096)),
            arguments: Box::default(),
            distinct: false,
            order_by: Box::default(),
            output: ValueId::new(4097),
        });
        let mut indexes = SemanticTraceIndexes::default();
        let mut budget = SemanticTraceWorkBudget::new();
        let initial_work = budget.remaining;

        for id in 1..4096 {
            let call = indexes
                .aggregate_sequence_call(
                    FragmentId::new(903),
                    NodeId::new(1),
                    &calls,
                    crate::AggregateSequenceId::new(id),
                    &mut budget,
                )
                .expect("each unique non-final sequence resolves to its exact call");
            assert_eq!(call.id, crate::AggregateCallId::new(id));
        }
        assert!(
            indexes
                .aggregate_sequence_call(
                    FragmentId::new(903),
                    NodeId::new(1),
                    &calls,
                    crate::AggregateSequenceId::new(4096),
                    &mut budget,
                )
                .is_none(),
            "a repeated sequence must remain ambiguous"
        );
        assert_eq!(indexes.aggregate_sequences.len(), 1);
        assert_eq!(initial_work - budget.remaining, calls.len());
    }

    #[test]
    fn overlapping_runtime_filter_hulls_charge_each_revisited_static_contribution() {
        let runtime_filter = |id, witness_count| crate::RuntimeFilter {
            id: crate::RuntimeFilterId::new(id),
            kind: crate::RuntimeFilterKind::InList,
            domain: crate::RuntimeFilterDomain::Membership {
                ty: ValueType::new(DataType::Int64, false),
                null_semantics: crate::RuntimeFilterNullSemantics::NeverMatches,
            },
            reduction: crate::RuntimeFilterReduction::SetUnion,
            availability_coverage: crate::RuntimeFilterCoverage {
                nodes: Box::default(),
                root: 0,
            },
            terminal_coverage: crate::RuntimeFilterCoverage {
                nodes: Box::default(),
                root: 0,
            },
            equality_witnesses: (0..witness_count)
                .map(|ordinal| crate::RuntimeFilterEqualityWitness {
                    id: crate::RuntimeFilterEqualityWitnessId::new(ordinal + 1),
                    fragment: FragmentId::new(1),
                    join: NodeId::new(1),
                    key_ordinal: ordinal,
                    domain_side: crate::JoinSide::Right,
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            producers: Box::default(),
            consumers: Box::default(),
            policy: crate::RuntimeFilterPolicy {
                max_contribution_bytes: 1,
                max_artifact_bytes: 1,
                deadline_ms: 1,
                max_retries: 1,
            },
        };
        let common = crate::RuntimeFilterId::new(1);
        let mut filters = BTreeMap::from([(common, runtime_filter(1, 4096))]);
        for id in 2..=257 {
            filters.insert(crate::RuntimeFilterId::new(id), runtime_filter(id, 0));
        }
        let plan = PhysicalPlan::from(crate::PhysicalPlanParts {
            version: crate::PlanVersionId::try_new([1; 16]).unwrap(),
            fragments: BTreeMap::new(),
            edges: BTreeMap::new(),
            runtime_filters: filters,
            result_port: None,
            artifact_refs: BTreeMap::new(),
            required: RequiredContracts::default(),
            annotations: Box::default(),
        });
        let mut cache = RuntimeFilterBuildDependencyCache::default();
        let mut budget = SemanticTraceWorkBudget::new();
        let mut rejected = false;

        for id in 2..=257 {
            let mut fragments = BTreeSet::new();
            let mut edges = BTreeSet::new();
            if extend_runtime_filter_proof_hull(
                &plan,
                [common, crate::RuntimeFilterId::new(id)],
                &mut fragments,
                &mut edges,
                &mut cache,
                &mut budget,
            )
            .is_none()
            {
                rejected = true;
                break;
            }
        }

        assert!(
            rejected,
            "overlapping but distinct proof hulls must not rescan a wide shared filter beyond the work budget"
        );
    }

    #[test]
    fn fragment_validation_indexes_share_one_wide_child_port_across_project_fanout() {
        let mut builder = crate::FragmentBuilder::new(FragmentId::new(904));
        let source = builder.reserve_node_id().unwrap();
        let value_type = ValueType::new(DataType::Int64, false);
        let source_values = (0..4096)
            .map(|ordinal| {
                builder
                    .add_value(
                        value_type.clone(),
                        ValueOrigin::NodeOutput {
                            node: source,
                            output_ordinal: ordinal,
                        },
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let singleton = crate::PhysicalProperties {
            distribution: Distribution::Singleton,
            row_multiplicity: RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        builder
            .insert_node(PhysicalNode {
                id: source,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: singleton.clone(),
                output: crate::OutputPort {
                    node: source,
                    columns: source_values.clone().into_boxed_slice(),
                },
                kind: NodeKind::Values {
                    rows: Box::default(),
                },
            })
            .unwrap();

        let mut projects = Vec::new();
        let mut mappings = Vec::new();
        for value in source_values.iter().take(256) {
            let project = builder.reserve_node_id().unwrap();
            let expression = builder
                .add_expression(project, value_type.clone(), ExprKind::Value(*value))
                .unwrap();
            builder
                .insert_node(PhysicalNode {
                    id: project,
                    inputs: Box::from([source]),
                    required_inputs: Box::from([singleton.clone()]),
                    output_properties: singleton.clone(),
                    output: crate::OutputPort {
                        node: project,
                        columns: Box::from([*value]),
                    },
                    kind: NodeKind::Project {
                        expressions: Box::from([(expression, *value)]),
                    },
                })
                .unwrap();
            projects.push(project);
            mappings.push(Box::from([*value]));
        }

        let union = builder.reserve_node_id().unwrap();
        let union_output = builder
            .add_value(
                value_type,
                ValueOrigin::NodeOutput {
                    node: union,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        builder
            .insert_node(PhysicalNode {
                id: union,
                inputs: projects.clone().into_boxed_slice(),
                required_inputs: vec![singleton.clone(); projects.len()].into_boxed_slice(),
                output_properties: singleton,
                output: crate::OutputPort {
                    node: union,
                    columns: Box::from([union_output]),
                },
                kind: NodeKind::SetOp {
                    kind: crate::SetOperationKind::UnionAll,
                    input_mappings: mappings.into_boxed_slice(),
                },
            })
            .unwrap();
        let fragment = builder
            .finish_definition(
                union,
                FragmentSink::Noop,
                crate::PipelineDopDomain {
                    min: 1,
                    max: 1,
                    requires_power_of_two: false,
                },
            )
            .expect("the wide shared-child project fanout is semantically valid");

        let indexes = FragmentValidationIndexes::new(&fragment);
        let source_output = indexes.output_ports.get(&source).unwrap();
        assert!(projects.iter().all(|project| {
            matches!(
                indexes.visible_inputs.get(project),
                Some(VisibleInputIndex::One(port)) if Arc::ptr_eq(port, source_output)
            )
        }));
    }
}
