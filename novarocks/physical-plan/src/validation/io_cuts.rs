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

//! Borrowed writer-cut facts shared by the original fragment-cut author.
//! RootResult is terminal and has no IO edge cut. Retired artifact/provenance
//! registries are not reconstructed for borrowed explain views.

use crate::{
    ChangeStreamRoute, ChangeStreamWriterCutField, ConnectorWriteRouteId, Edge, EdgeKind,
    FragmentSink, NodeKind, PhysicalPlan, ValueId, ValueType, WriteTargetOrdinal,
    WriterRelationFieldRole, WriterTarget,
};

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
