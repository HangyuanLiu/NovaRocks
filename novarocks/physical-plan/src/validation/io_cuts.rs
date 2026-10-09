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

//! Borrowed IO cut facts owned by the physical plan boundary.
//!
//! This view shares the production provenance and writer-cut kernels. It does
//! not materialize independently verifiable fragments, artifact payloads, or
//! runtime-filter proof graphs. Auxiliary allocations are preflighted against
//! the caller's explicit byte ceiling before any index is built.

use super::index::{PlanSourceProvenance, source_provenance_index_bounded};
use crate::{
    ChangeStreamRoute, ChangeStreamWriterCutField, ConnectorWriteRouteId, Edge, EdgeKind, Fragment,
    FragmentId, FragmentSink, NodeKind, PhysicalPlan, SourceBindingRef, ValueId, ValueType,
    WriteTargetOrdinal, WriterRelationFieldRole, WriterTarget,
};

pub struct FragmentIoCutIndex<'plan> {
    plan: &'plan PhysicalPlan,
    provenance: PlanSourceProvenance<'plan>,
}

impl<'plan> FragmentIoCutIndex<'plan> {
    pub fn try_new(plan: &'plan PhysicalPlan, maximum_index_bytes: usize) -> Option<Self> {
        let provenance = source_provenance_index_bounded(plan, maximum_index_bytes)?;
        // These are precisely the value lookups made by the owned IO cuts.
        // Validate once so iteration below returns facts, never guessed types.
        for edge in plan.edges().values() {
            let source = plan.fragments().get(&edge.source.fragment)?;
            for value in edge.source.projection.iter().chain(
                edge.destination
                    .receive_mapping
                    .iter()
                    .map(|(source, _)| source),
            ) {
                source.values().get(value)?;
            }
        }
        Some(Self { plan, provenance })
    }

    pub fn fragment(&self, fragment: FragmentId) -> Option<FragmentIoCutView<'_, 'plan>> {
        self.plan.fragments().get(&fragment)?;
        Some(FragmentIoCutView {
            index: self,
            fragment,
        })
    }

    fn edge(&self, edge: &'plan Edge) -> FragmentIoEdgeCut<'_, 'plan> {
        let source_fragment = self
            .plan
            .fragments()
            .get(&edge.source.fragment)
            .expect("IO cut index validated source fragments");
        let change_stream_writer = if edge.kind == EdgeKind::ChangeStreamRouter {
            match source_fragment.sink() {
                FragmentSink::Router { routes, .. } => routes
                    .iter()
                    .rev()
                    .filter(|route| route.edge == edge.id)
                    .find_map(|route| change_stream_writer_cut_ref(route, edge)),
                _ => None,
            }
        } else {
            None
        };
        FragmentIoEdgeCut {
            edge,
            source_fragment,
            has_source_free_rows: self
                .provenance
                .has_source_free_rows(edge.source.fragment)
                .expect("IO cut index covered every fragment"),
            change_stream_writer,
            writer_result: writer_result_cut_ref(self.plan, edge),
            index: self,
        }
    }
}

pub struct FragmentIoCutView<'index, 'plan> {
    index: &'index FragmentIoCutIndex<'plan>,
    fragment: FragmentId,
}

impl<'index, 'plan> FragmentIoCutView<'index, 'plan> {
    pub fn inbound(&self) -> impl Iterator<Item = FragmentIoEdgeCut<'index, 'plan>> + '_ {
        self.index
            .plan
            .edges()
            .values()
            .filter(move |edge| edge.destination.fragment == self.fragment)
            .map(|edge| self.index.edge(edge))
    }

    pub fn outbound(&self) -> impl Iterator<Item = FragmentIoEdgeCut<'index, 'plan>> + '_ {
        self.index
            .plan
            .edges()
            .values()
            .filter(move |edge| edge.source.fragment == self.fragment)
            .map(|edge| self.index.edge(edge))
    }
}

pub struct FragmentIoEdgeCut<'index, 'plan> {
    pub edge: &'plan Edge,
    pub source_fragment: &'plan Fragment,
    pub has_source_free_rows: bool,
    pub change_stream_writer: Option<ChangeStreamWriterCutRef<'plan>>,
    pub writer_result: Option<WriterResultCutRef<'plan>>,
    index: &'index FragmentIoCutIndex<'plan>,
}

impl<'plan> FragmentIoEdgeCut<'_, 'plan> {
    pub fn imports(&self) -> impl ExactSizeIterator<Item = CutImportRef<'plan>> + '_ {
        self.edge
            .destination
            .receive_mapping
            .iter()
            .map(|(source, destination)| CutImportRef {
                source: self.value(*source),
                destination: *destination,
            })
    }

    pub fn projection(&self) -> impl ExactSizeIterator<Item = CutValueRef<'plan>> + '_ {
        self.edge
            .source
            .projection
            .iter()
            .map(|value| self.value(*value))
    }

    fn value(&self, value: ValueId) -> CutValueRef<'plan> {
        CutValueRef {
            value,
            ty: &self
                .source_fragment
                .values()
                .get(&value)
                .expect("IO cut index validated every value")
                .ty,
        }
    }

    pub fn source_bindings(&self) -> impl Iterator<Item = SourceBindingRef<'plan>> + '_ {
        self.index
            .provenance
            .binding_refs(self.edge.source.fragment)
            .expect("IO cut index covered every fragment")
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CutValueRef<'a> {
    pub value: ValueId,
    pub ty: &'a ValueType,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CutImportRef<'a> {
    pub source: CutValueRef<'a>,
    pub destination: ValueId,
}

#[derive(Clone, Copy, Debug)]
pub struct ChangeStreamWriterCutRef<'a> {
    pub route_id: ConnectorWriteRouteId,
    pub write_target_ordinal: WriteTargetOrdinal,
    route: &'a ChangeStreamRoute,
    edge: &'a Edge,
}

impl ChangeStreamWriterCutRef<'_> {
    pub fn fields(&self) -> impl ExactSizeIterator<Item = ChangeStreamWriterCutField> + '_ {
        self.route
            .input_mapping
            .iter()
            .zip(&self.edge.destination.receive_mapping)
            .map(
                |((token, source), (_, destination))| ChangeStreamWriterCutField {
                    token: *token,
                    source: *source,
                    destination: *destination,
                },
            )
    }
}

pub(crate) fn change_stream_writer_cut_ref<'a>(
    route: &'a ChangeStreamRoute,
    edge: &'a Edge,
) -> Option<ChangeStreamWriterCutRef<'a>> {
    if route.input_mapping.len() != edge.destination.receive_mapping.len()
        || !route
            .input_mapping
            .iter()
            .zip(&edge.destination.receive_mapping)
            .all(|((_, source), (mapped, _))| source == mapped)
    {
        return None;
    }
    Some(ChangeStreamWriterCutRef {
        route_id: route.route_id,
        write_target_ordinal: route.write_target_ordinal,
        route,
        edge,
    })
}

#[derive(Clone, Copy, Debug)]
pub struct WriterResultCutRef<'a> {
    pub write_target_ordinal: WriteTargetOrdinal,
    pub schema_revision: u32,
    target: &'a WriterTarget,
    edge: &'a Edge,
}

impl<'a> WriterResultCutRef<'a> {
    pub fn fields(&self) -> impl ExactSizeIterator<Item = WriterResultCutFieldRef<'a>> + '_ {
        self.target
            .output_schema
            .fields
            .iter()
            .zip(&self.edge.destination.receive_mapping)
            .map(|(field, (_, destination))| WriterResultCutFieldRef {
                source: field.value,
                destination: *destination,
                name: &field.name,
                ty: &field.ty,
                role: field.role,
            })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct WriterResultCutFieldRef<'a> {
    pub source: ValueId,
    pub destination: ValueId,
    pub name: &'a str,
    pub ty: &'a ValueType,
    pub role: WriterRelationFieldRole,
}

pub(crate) fn writer_result_cut_ref<'a>(
    plan: &'a PhysicalPlan,
    edge: &'a Edge,
) -> Option<WriterResultCutRef<'a>> {
    if edge.kind != EdgeKind::Stream {
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
        || !target
            .output_schema
            .fields
            .iter()
            .zip(&edge.destination.receive_mapping)
            .all(|(field, (mapped, _))| field.value == *mapped)
    {
        return None;
    }
    Some(WriterResultCutRef {
        write_target_ordinal: target.write_target_ordinal,
        schema_revision: target.output_schema.revision,
        target,
        edge,
    })
}
