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

use crate::{
    ArtifactRefId, Edge, EdgeId, ExprArena, ExprId, ExprKind, ExprNode, Fragment, FragmentId,
    FragmentParts, FragmentSink, NodeId, PhysicalNode, PhysicalPlan, PhysicalPlanParts,
    PipelineDopDomain, PlanAnnotation, PlanVersionId, RequiredContracts, ResultPort, RuntimeFilter,
    RuntimeFilterId, SealedArtifactRef, ValidationErrors, ValueDef, ValueId, ValueOrigin,
    ValueType, validate_fragment_definition, validate_plan,
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
            Self::DuplicateArtifactRef(id) => {
                write!(formatter, "duplicate artifact reference {}", id.get())
            }
            Self::DuplicateResultPort => formatter.write_str("result port is already set"),
        }
    }
}

impl std::error::Error for BuildError {}
