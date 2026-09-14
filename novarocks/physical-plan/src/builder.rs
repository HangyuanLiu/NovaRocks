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

use arrow_schema::DataType;

use crate::{
    ArtifactRefId, Edge, EdgeId, ExprArena, ExprId, ExprKind, ExprNode, Fragment, FragmentId,
    FragmentParts, FragmentSink, NodeId, NodeKind, OutputPort, PhysicalNode, PhysicalPlan,
    PhysicalPlanParts, PipelineDopDomain, PlanAnnotation, PlanVersionId, RequiredContracts,
    ResultPort, RuntimeFilter, RuntimeFilterId, SealedArtifactRef, ValidationErrors, ValueDef,
    ValueId, ValueOrigin, ValueType, validate_fragment_definition, validate_plan,
};

/// Mutable construction state. It cannot be encoded, scheduled or viewed as a
/// complete plan; `finish` consumes it and publishes only validated output.
pub struct FragmentBuilder {
    id: FragmentId,
    next_value: u32,
    next_expr: u32,
    next_node: u32,
    values: BTreeMap<ValueId, ValueDef>,
    expressions: ExprArena,
    nodes: BTreeMap<NodeId, PhysicalNode>,
    runtime_filters: BTreeSet<RuntimeFilterId>,
}

impl FragmentBuilder {
    pub fn new(id: FragmentId) -> Self {
        Self {
            id,
            next_value: 0,
            next_expr: 0,
            next_node: 0,
            values: BTreeMap::new(),
            expressions: ExprArena::default(),
            nodes: BTreeMap::new(),
            runtime_filters: BTreeSet::new(),
        }
    }

    pub const fn id(&self) -> FragmentId {
        self.id
    }

    pub const fn expressions(&self) -> &ExprArena {
        &self.expressions
    }

    pub fn reserve_node_id(&mut self) -> Result<NodeId, BuildError> {
        let id = NodeId::new(self.next_node);
        self.next_node = self
            .next_node
            .checked_add(1)
            .ok_or(BuildError::IdentitySpaceExhausted("node"))?;
        Ok(id)
    }

    pub fn reserve_expression_id(&mut self) -> Result<ExprId, BuildError> {
        let id = ExprId::new(self.next_expr);
        self.next_expr = self
            .next_expr
            .checked_add(1)
            .ok_or(BuildError::IdentitySpaceExhausted("expression"))?;
        Ok(id)
    }

    pub fn add_value(&mut self, ty: ValueType, origin: ValueOrigin) -> Result<ValueId, BuildError> {
        let id = ValueId::new(self.next_value);
        self.next_value = self
            .next_value
            .checked_add(1)
            .ok_or(BuildError::IdentitySpaceExhausted("value"))?;
        self.insert_value(ValueDef { id, ty, origin })?;
        Ok(id)
    }

    pub fn insert_value(&mut self, value: ValueDef) -> Result<(), BuildError> {
        let id = value.id;
        let next_value = self.next_value.max(
            id.get()
                .checked_add(1)
                .ok_or(BuildError::IdentitySpaceExhausted("value"))?,
        );
        match self.values.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(value);
                self.next_value = next_value;
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateValue(id)),
        }
        Ok(())
    }

    pub fn add_expression(
        &mut self,
        owner: NodeId,
        ty: ValueType,
        kind: ExprKind,
    ) -> Result<ExprId, BuildError> {
        self.add_expression_in_scope(owner, None, ty, kind)
    }

    pub fn add_expression_in_scope(
        &mut self,
        owner: NodeId,
        lambda_scope: Option<ExprId>,
        ty: ValueType,
        kind: ExprKind,
    ) -> Result<ExprId, BuildError> {
        let id = self.reserve_expression_id()?;
        self.insert_expression(ExprNode {
            id,
            owner,
            lambda_scope,
            ty,
            kind,
        })?;
        Ok(id)
    }

    pub fn insert_expression(&mut self, expression: ExprNode) -> Result<(), BuildError> {
        let id = expression.id;
        let next_expr = self.next_expr.max(
            id.get()
                .checked_add(1)
                .ok_or(BuildError::IdentitySpaceExhausted("expression"))?,
        );
        match self.expressions.get(id) {
            Some(_) => return Err(BuildError::DuplicateExpression(id)),
            None => {
                self.expressions.insert(expression);
                self.next_expr = next_expr;
            }
        }
        Ok(())
    }

    /// Adds a filter over `input`, retaining rows for which every predicate
    /// holds.
    ///
    /// The caller supplies only what it actually decides: which rows to keep.
    /// The output port, the input requirement and the output properties all
    /// follow from the input and the predicates, so building them by hand was
    /// four separate chances to state something the contract then had to catch.
    /// `node` is reserved first because the predicates are owned by it.
    pub fn add_filter(
        &mut self,
        node: NodeId,
        input: NodeId,
        predicates: Box<[ExprId]>,
    ) -> Result<(), BuildError> {
        if predicates.is_empty() {
            return Err(BuildError::FilterWithoutPredicate(node));
        }
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        let columns = source.output.columns.clone();
        let input_properties = source.output_properties.clone();
        for predicate in &predicates {
            let expression = self
                .expressions
                .get(*predicate)
                .ok_or(BuildError::UndefinedExpression(*predicate))?;
            if expression.owner != node {
                return Err(BuildError::ExpressionOutsideOwner {
                    expr: *predicate,
                    owner: expression.owner,
                    node,
                });
            }
            if expression.ty.data_type != DataType::Boolean {
                return Err(BuildError::PredicateIsNotBoolean(*predicate));
            }
        }
        let replica_deterministic = crate::expressions_are_replica_deterministic(
            &self.expressions,
            predicates.iter().copied(),
            true,
        );
        let output_properties =
            crate::derive_filter_output_properties(&input_properties, replica_deterministic);
        self.insert_node(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([crate::passthrough_requirement(&input_properties)]),
            output_properties,
            output: OutputPort { node, columns },
            kind: NodeKind::Filter { predicates },
        })
    }

    /// Adds a global limit over `input`.
    ///
    /// A global limit is only meaningful over a single stream of single-copy
    /// rows, so the requirement is checked here instead of being restated by
    /// the caller and then re-derived by the contract. Row order survives;
    /// nothing else about the input can.
    pub fn add_limit(
        &mut self,
        node: NodeId,
        input: NodeId,
        limit: Option<u64>,
        offset: u64,
    ) -> Result<(), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        if source.output_properties.distribution != crate::Distribution::Singleton
            || source.output_properties.row_multiplicity != crate::RowMultiplicity::SingleCopy
        {
            return Err(BuildError::LimitInputIsNotGlobal(node));
        }
        let columns = source.output.columns.clone();
        let output_properties = crate::PhysicalProperties {
            distribution: crate::Distribution::Singleton,
            row_multiplicity: crate::RowMultiplicity::SingleCopy,
            ordering: source.output_properties.ordering.clone(),
        };
        self.insert_node(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([crate::PhysicalProperties {
                distribution: crate::Distribution::Singleton,
                row_multiplicity: crate::RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            }]),
            output_properties,
            output: OutputPort { node, columns },
            kind: NodeKind::Limit { limit, offset },
        })
    }

    /// Adds a cardinality assertion over `input`.
    ///
    /// The assertion counts rows, so it needs each logical row to exist once;
    /// it constrains nothing else and changes nothing it passes on.
    pub fn add_assert_one_row(
        &mut self,
        node: NodeId,
        input: NodeId,
        spec: crate::RowCountAssertionSpec,
    ) -> Result<(), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        if source.output_properties.row_multiplicity != crate::RowMultiplicity::SingleCopy {
            return Err(BuildError::AssertionOverReplicatedRows(node));
        }
        let columns = source.output.columns.clone();
        let output_properties = source.output_properties.clone();
        let required = crate::PhysicalProperties {
            distribution: output_properties.distribution.clone(),
            row_multiplicity: crate::RowMultiplicity::SingleCopy,
            ordering: Box::default(),
        };
        self.insert_node(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([required]),
            output_properties,
            output: OutputPort { node, columns },
            kind: NodeKind::AssertOneRow(spec),
        })
    }

    /// Adds a sort over `input`.
    ///
    /// The ordering a sort establishes is not an independent fact to be stated
    /// alongside the sort keys - it *is* the sort keys, read as values. Having
    /// the caller supply both invited them to disagree, so the builder derives
    /// the ordering and only the keys are supplied.
    ///
    /// A global sort needs one single-copy stream. An analytic sort leaves the
    /// input layout alone, because it orders within partitions that the layout
    /// already colocates.
    pub fn add_sort(
        &mut self,
        node: NodeId,
        input: NodeId,
        order_by: Box<[crate::SortExpr]>,
        mode: crate::SortMode,
    ) -> Result<(), BuildError> {
        if order_by.is_empty() {
            return Err(BuildError::OrderingWithoutKeys(node));
        }
        let partition_by: &[crate::SortExpr] = match &mode {
            crate::SortMode::Global => &[],
            crate::SortMode::Analytic { partition_by }
            | crate::SortMode::PartitionTopN { partition_by, .. } => partition_by,
        };
        let ordering = crate::ordering_keys(&self.expressions, partition_by, &order_by)
            .ok_or(BuildError::OrderingKeyIsNotAValue(node))?
            .into_boxed_slice();
        let (output_properties, required) =
            self.passthrough_ordering_properties(node, input, &mode, ordering)?;
        let source = self.nodes.get(&input).expect("checked above");
        let columns = source.output.columns.clone();
        self.insert_node(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([required]),
            output_properties,
            output: OutputPort { node, columns },
            kind: NodeKind::Sort { order_by, mode },
        })
    }

    /// Adds a top-N over `input`.
    ///
    /// Whether one stream is required follows from the phase rather than being
    /// stated beside it: a partial phase runs where the rows already are and
    /// its result is reduced later, while every other phase produces the answer
    /// and so needs one single-copy stream.
    pub fn add_top_n(
        &mut self,
        node: NodeId,
        input: NodeId,
        order_by: Box<[crate::SortExpr]>,
        limit: u64,
        offset: u64,
        phase: crate::TopNPhase,
    ) -> Result<(), BuildError> {
        let require_singleton = !matches!(phase, crate::TopNPhase::Partial { .. });
        if order_by.is_empty() {
            return Err(BuildError::OrderingWithoutKeys(node));
        }
        let ordering = crate::ordering_keys(&self.expressions, &[], &order_by)
            .ok_or(BuildError::OrderingKeyIsNotAValue(node))?
            .into_boxed_slice();
        let mode = if require_singleton {
            crate::SortMode::Global
        } else {
            crate::SortMode::Analytic {
                partition_by: Box::default(),
            }
        };
        let (output_properties, required) =
            self.passthrough_ordering_properties(node, input, &mode, ordering)?;
        let source = self.nodes.get(&input).expect("checked above");
        let columns = source.output.columns.clone();
        self.insert_node(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([required]),
            output_properties,
            output: OutputPort { node, columns },
            kind: NodeKind::TopN {
                order_by,
                limit,
                offset,
                phase,
            },
        })
    }

    /// Adds a projection over `input`.
    ///
    /// `output` stays a caller decision because an output port is an ordered
    /// list of occurrences that may repeat a value, which the expression list
    /// cannot express: each expression defines its value exactly once. What
    /// follows from those two is the input requirement and which of the input's
    /// distribution and ordering survive, so the builder derives both.
    pub fn add_project(
        &mut self,
        node: NodeId,
        input: NodeId,
        expressions: Box<[(ExprId, ValueId)]>,
        output: Box<[ValueId]>,
    ) -> Result<(), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        let input_properties = source.output_properties.clone();
        for (expression, value) in &expressions {
            let node_expression = self
                .expressions
                .get(*expression)
                .ok_or(BuildError::UndefinedExpression(*expression))?;
            if node_expression.owner != node {
                return Err(BuildError::ExpressionOutsideOwner {
                    expr: *expression,
                    owner: node_expression.owner,
                    node,
                });
            }
            if !self.values.contains_key(value) {
                return Err(BuildError::UndefinedValue(*value));
            }
        }
        for value in &output {
            if !self.values.contains_key(value) {
                return Err(BuildError::UndefinedValue(*value));
            }
        }
        let replica_deterministic = crate::expressions_are_replica_deterministic(
            &self.expressions,
            expressions.iter().map(|(expression, _)| *expression),
            true,
        );
        let output_properties = crate::derive_project_output_properties(
            &input_properties,
            &output,
            replica_deterministic,
        );
        self.insert_node(PhysicalNode {
            id: node,
            inputs: Box::from([input]),
            required_inputs: Box::from([crate::passthrough_requirement(&input_properties)]),
            output_properties,
            output: OutputPort {
                node,
                columns: output,
            },
            kind: NodeKind::Project { expressions },
        })
    }

    /// Output and input properties for an operator that orders rows it passes
    /// through.
    fn passthrough_ordering_properties(
        &self,
        node: NodeId,
        input: NodeId,
        mode: &crate::SortMode,
        ordering: Box<[crate::OrderingKey]>,
    ) -> Result<(crate::PhysicalProperties, crate::PhysicalProperties), BuildError> {
        let source = self
            .nodes
            .get(&input)
            .ok_or(BuildError::UndefinedInput { node, input })?;
        let input_properties = &source.output_properties;
        if matches!(mode, crate::SortMode::Global) {
            if input_properties.distribution != crate::Distribution::Singleton
                || input_properties.row_multiplicity != crate::RowMultiplicity::SingleCopy
            {
                return Err(BuildError::GlobalOrderOverManyStreams(node));
            }
            return Ok((
                crate::PhysicalProperties {
                    distribution: crate::Distribution::Singleton,
                    row_multiplicity: crate::RowMultiplicity::SingleCopy,
                    ordering,
                },
                crate::PhysicalProperties {
                    distribution: crate::Distribution::Singleton,
                    row_multiplicity: crate::RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
            ));
        }
        Ok((
            crate::PhysicalProperties {
                distribution: input_properties.distribution.clone(),
                row_multiplicity: input_properties.row_multiplicity,
                ordering,
            },
            crate::PhysicalProperties {
                distribution: input_properties.distribution.clone(),
                row_multiplicity: input_properties.row_multiplicity,
                ordering: Box::default(),
            },
        ))
    }

    /// Properties a node in this fragment produces.
    ///
    /// Callers that let the builder derive properties still need to read them
    /// back to describe the node to their own caller.
    pub fn node_output_properties(&self, node: NodeId) -> Option<&crate::PhysicalProperties> {
        self.nodes.get(&node).map(|node| &node.output_properties)
    }

    pub fn insert_node(&mut self, node: PhysicalNode) -> Result<(), BuildError> {
        let id = node.id;
        let next_node = self.next_node.max(
            id.get()
                .checked_add(1)
                .ok_or(BuildError::IdentitySpaceExhausted("node"))?,
        );
        match self.nodes.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(node);
                self.next_node = next_node;
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateNode(id)),
        }
        Ok(())
    }

    pub fn attach_runtime_filter(&mut self, id: RuntimeFilterId) -> Result<(), BuildError> {
        if !self.runtime_filters.insert(id) {
            return Err(BuildError::DuplicateRuntimeFilter(id));
        }
        Ok(())
    }

    /// Finish a locally valid fragment definition.
    ///
    /// Cross-fragment imports, exchange properties, runtime-filter endpoints,
    /// and upstream provenance remain incomplete until `PlanBuilder::finish`
    /// derives and validates the exact cuts. This result is therefore a plan
    /// construction input, not an executable publication.
    pub fn finish_definition(
        self,
        root: NodeId,
        sink: FragmentSink,
        dop_domain: PipelineDopDomain,
    ) -> Result<Fragment, ValidationErrors> {
        let fragment = Fragment::from(FragmentParts {
            id: self.id,
            root,
            values: self.values,
            expressions: self.expressions,
            nodes: self.nodes,
            sink,
            dop_domain,
            runtime_filters: self.runtime_filters.into_iter().collect(),
        });
        validate_fragment_definition(&fragment)?;
        Ok(fragment)
    }
}

pub struct PlanBuilder {
    version: PlanVersionId,
    next_edge: u32,
    fragments: BTreeMap<FragmentId, Fragment>,
    edges: BTreeMap<EdgeId, Edge>,
    runtime_filters: BTreeMap<RuntimeFilterId, RuntimeFilter>,
    result_port: Option<ResultPort>,
    artifact_refs: BTreeMap<ArtifactRefId, SealedArtifactRef>,
    required: RequiredContracts,
    annotations: Vec<PlanAnnotation>,
}

impl PlanBuilder {
    pub fn new(version: PlanVersionId) -> Self {
        Self {
            version,
            next_edge: 0,
            fragments: BTreeMap::new(),
            edges: BTreeMap::new(),
            runtime_filters: BTreeMap::new(),
            result_port: None,
            artifact_refs: BTreeMap::new(),
            required: RequiredContracts::default(),
            annotations: Vec::new(),
        }
    }

    pub fn with_required_contracts(mut self, required: RequiredContracts) -> Self {
        self.required = required;
        self
    }

    pub fn add_fragment(&mut self, fragment: Fragment) -> Result<(), BuildError> {
        let id = fragment.id();
        match self.fragments.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(fragment);
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateFragment(id)),
        }
        Ok(())
    }

    pub fn reserve_edge_id(&mut self) -> Result<EdgeId, BuildError> {
        let id = EdgeId::new(self.next_edge);
        self.next_edge = self
            .next_edge
            .checked_add(1)
            .ok_or(BuildError::IdentitySpaceExhausted("edge"))?;
        Ok(id)
    }

    pub fn add_edge(&mut self, edge: Edge) -> Result<(), BuildError> {
        let id = edge.id;
        let next_edge = self.next_edge.max(
            id.get()
                .checked_add(1)
                .ok_or(BuildError::IdentitySpaceExhausted("edge"))?,
        );
        match self.edges.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(edge);
                self.next_edge = next_edge;
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateEdge(id)),
        }
        Ok(())
    }

    pub fn add_runtime_filter(&mut self, filter: RuntimeFilter) -> Result<(), BuildError> {
        let id = filter.id;
        match self.runtime_filters.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(filter);
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateRuntimeFilter(id)),
        }
        Ok(())
    }

    pub fn set_result_port(&mut self, result: ResultPort) -> Result<(), BuildError> {
        if self.result_port.is_some() {
            return Err(BuildError::DuplicateResultPort);
        }
        self.result_port = Some(result);
        Ok(())
    }

    pub fn add_artifact_ref(&mut self, artifact: SealedArtifactRef) -> Result<(), BuildError> {
        let id = artifact.id;
        match self.artifact_refs.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(artifact);
            }
            Entry::Occupied(_) => return Err(BuildError::DuplicateArtifactRef(id)),
        }
        Ok(())
    }

    pub fn add_annotation(&mut self, annotation: PlanAnnotation) {
        self.annotations.push(annotation);
    }

    pub fn finish(self) -> Result<PhysicalPlan, ValidationErrors> {
        let plan = PhysicalPlan::from(PhysicalPlanParts {
            version: self.version,
            fragments: self.fragments,
            edges: self.edges,
            runtime_filters: self.runtime_filters,
            result_port: self.result_port,
            artifact_refs: self.artifact_refs,
            required: self.required,
            annotations: self.annotations.into_boxed_slice(),
        });
        validate_plan(&plan)?;
        Ok(plan)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BuildError {
    IdentitySpaceExhausted(&'static str),
    DuplicateFragment(FragmentId),
    DuplicateNode(NodeId),
    DuplicateValue(ValueId),
    DuplicateExpression(ExprId),
    DuplicateEdge(EdgeId),
    DuplicateRuntimeFilter(RuntimeFilterId),
    DuplicateArtifactRef(ArtifactRefId),
    DuplicateResultPort,
    /// A node names an input that has not been inserted yet. Fragments are
    /// built bottom up, so this is always an ordering mistake.
    UndefinedInput {
        node: NodeId,
        input: NodeId,
    },
    UndefinedExpression(ExprId),
    UndefinedValue(ValueId),
    /// Expressions belong to exactly one node's evaluation scope, so using one
    /// under a different node is a scope violation rather than a type error.
    ExpressionOutsideOwner {
        expr: ExprId,
        owner: NodeId,
        node: NodeId,
    },
    PredicateIsNotBoolean(ExprId),
    FilterWithoutPredicate(NodeId),
    /// A global limit was built over rows that are not one single-copy stream,
    /// where "the first N rows" has no single meaning.
    LimitInputIsNotGlobal(NodeId),
    /// A cardinality assertion was built over replicated rows, where counting
    /// them counts execution copies rather than logical rows.
    AssertionOverReplicatedRows(NodeId),
    OrderingWithoutKeys(NodeId),
    /// A sort key reads a computation rather than a value, so no downstream
    /// operator could rely on the ordering it claims.
    OrderingKeyIsNotAValue(NodeId),
    /// A global order was built over rows spread across more than one stream,
    /// where "the first N in order" has no single meaning.
    GlobalOrderOverManyStreams(NodeId),
}

impl fmt::Display for BuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdentitySpaceExhausted(kind) => {
                write!(formatter, "{kind} identity space exhausted")
            }
            Self::DuplicateFragment(id) => write!(formatter, "duplicate fragment {}", id.get()),
            Self::DuplicateNode(id) => write!(formatter, "duplicate node {}", id.get()),
            Self::DuplicateValue(id) => write!(formatter, "duplicate value {}", id.get()),
            Self::DuplicateExpression(id) => write!(formatter, "duplicate expression {}", id.get()),
            Self::DuplicateEdge(id) => write!(formatter, "duplicate edge {}", id.get()),
            Self::DuplicateRuntimeFilter(id) => {
                write!(formatter, "duplicate runtime filter {}", id.get())
            }
            Self::UndefinedInput { node, input } => write!(
                formatter,
                "node {} names undefined input {}",
                node.get(),
                input.get()
            ),
            Self::UndefinedExpression(id) => {
                write!(formatter, "expression {} is not defined", id.get())
            }
            Self::UndefinedValue(id) => write!(formatter, "value {} is not defined", id.get()),
            Self::ExpressionOutsideOwner { expr, owner, node } => write!(
                formatter,
                "expression {} belongs to node {}, not node {}",
                expr.get(),
                owner.get(),
                node.get()
            ),
            Self::PredicateIsNotBoolean(id) => {
                write!(
                    formatter,
                    "predicate expression {} is not boolean",
                    id.get()
                )
            }
            Self::FilterWithoutPredicate(id) => {
                write!(formatter, "filter node {} has no predicate", id.get())
            }
            Self::LimitInputIsNotGlobal(id) => write!(
                formatter,
                "limit node {} requires one single-copy singleton input",
                id.get()
            ),
            Self::AssertionOverReplicatedRows(id) => write!(
                formatter,
                "assertion node {} cannot count replicated rows",
                id.get()
            ),
            Self::OrderingWithoutKeys(id) => {
                write!(formatter, "ordering node {} has no sort key", id.get())
            }
            Self::OrderingKeyIsNotAValue(id) => write!(
                formatter,
                "ordering node {} has a sort key that is not a value reference",
                id.get()
            ),
            Self::GlobalOrderOverManyStreams(id) => write!(
                formatter,
                "node {} requires one single-copy singleton input for a global order",
                id.get()
            ),
            Self::DuplicateArtifactRef(id) => {
                write!(formatter, "duplicate artifact reference {}", id.get())
            }
            Self::DuplicateResultPort => formatter.write_str("result port is already set"),
        }
    }
}

impl std::error::Error for BuildError {}
