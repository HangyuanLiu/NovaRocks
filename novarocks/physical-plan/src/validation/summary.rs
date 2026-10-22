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

//! Exact summary field and cut validation. These facts grant no runtime capability.

use super::*;
use crate::{
    DeliveryRequirement, ExchangePayloadContract, FragmentOutputPort, FragmentSink,
    LogicalChangeCountField, LogicalSummaryColumns, LogicalSummaryOutput, ValueDef, ValueOrigin,
};
use arrow_schema::DataType;

pub(crate) fn validate_logical_summary_value(
    fragment: &Fragment,
    value: &ValueDef,
    field: LogicalChangeCountField,
    errors: &mut ValidationContext,
) {
    let path = format!(
        "fragments[{}].values[{}].origin",
        fragment.id().get(),
        value.id.get()
    );
    let declared = match fragment.sink() {
        FragmentSink::Router { summary, .. } => summary
            .columns
            .fields()
            .into_iter()
            .any(|(role, id)| role == field && id == value.id),
        _ => false,
    };
    if !declared || value.ty.nullable() || value.ty.data_type() != &DataType::Int64 {
        errors.push(ValidationError::new(
            &path,
            "logical summary origin must be an exact declared non-null Int64 counter",
        ));
    }
}

/// Run after the fragment's ordinary resource and value-definition stages.
pub(crate) fn validate_logical_summary_output(
    fragment: &Fragment,
    summary: &LogicalSummaryOutput,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].sink.summary", fragment.id().get());
    let ids = summary.columns.values();
    if ids[0] == ids[1] || ids[0] == ids[2] || ids[1] == ids[2] {
        errors.push(ValidationError::new(
            &path,
            "logical summary counter identities must be distinct",
        ));
    }
    for (field, id) in summary.columns.fields() {
        match fragment.values().get(&id) {
            Some(value)
                if value.origin == (ValueOrigin::LogicalSummary { field })
                    && !value.ty.nullable()
                    && value.ty.data_type() == &DataType::Int64 => {}
            _ => errors.push(ValidationError::new(
                &path,
                "logical summary output requires its own exact non-null Int64 field origins",
            )),
        }
    }
    if fragment
        .nodes()
        .get(&fragment.root())
        .is_some_and(|node| node.output.columns.iter().any(|id| ids.contains(id)))
    {
        errors.push(ValidationError::new(
            &path,
            "logical summary counters cannot be root relational output values",
        ));
    }
}

/// Full-graph caller checks the edge against the independent sink output.
pub(crate) fn validate_logical_summary_edge(
    fragment: &Fragment,
    summary: &LogicalSummaryOutput,
    edge: &crate::Edge,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].sink.summary", fragment.id().get());
    if edge.id != summary.edge
        || edge.source.fragment != fragment.id()
        || edge.source.port != FragmentOutputPort::LogicalSummary
        || edge.role != crate::OutputRole::LogicalSummary
        || edge.delivery != DeliveryRequirement::MustDrain
        || edge.source.projection.as_ref() != summary.columns.values().as_slice()
    {
        errors.push(ValidationError::new(&path,
            "summary edge must project the exact independent summary output with MustDrain delivery"));
    }
}

/// One fixed-width summary cut has no dynamic field list or provider schema.
/// Callers still validate its exact edge identity and endpoint consumption.
pub(crate) fn validate_logical_summary_cut(
    fragment: &Fragment,
    source_port: FragmentOutputPort,
    payload: &ExchangePayloadContract,
    delivery: DeliveryRequirement,
    imports: &[crate::CutImport],
    outbound: bool,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].cuts.summary", fragment.id().get());
    if !matches!(payload, ExchangePayloadContract::Relational)
        && delivery != DeliveryRequirement::MustDrain
    {
        errors.push(ValidationError::new(
            &path,
            "write output roles require MustDrain delivery",
        ));
    }
    let ExchangePayloadContract::LogicalSummary(proof) = payload else {
        if source_port == FragmentOutputPort::LogicalSummary {
            errors.push(ValidationError::new(
                &path,
                "summary source port requires an exact summary payload",
            ));
        }
        return;
    };
    if source_port != FragmentOutputPort::LogicalSummary
        || delivery != DeliveryRequirement::MustDrain
    {
        errors.push(ValidationError::new(
            &path,
            "summary cut requires its own source port and MustDrain delivery",
        ));
    }
    let sources = proof.source.values();
    let destinations = proof.destination.values();
    if sources[0] == sources[1]
        || sources[0] == sources[2]
        || sources[1] == sources[2]
        || destinations[0] == destinations[1]
        || destinations[0] == destinations[2]
        || destinations[1] == destinations[2]
    {
        errors.push(ValidationError::new(
            &path,
            "summary cut requires three distinct source and destination fields",
        ));
    }
    if imports.len() != 3
        || imports
            .iter()
            .zip(sources.into_iter().zip(destinations))
            .any(|(import, (source, destination))| {
                import.source.value != source
                    || import.destination != destination
                    || import.source.ty.nullable()
                    || import.source.ty.data_type() != &DataType::Int64
            })
    {
        errors.push(ValidationError::new(
            &path,
            "summary cut mapping must preserve the exact ordered non-null Int64 counters",
        ));
    }
    let columns: LogicalSummaryColumns = if outbound {
        proof.source
    } else {
        proof.destination
    };
    for id in columns.values() {
        if fragment
            .values()
            .get(&id)
            .is_none_or(|value| value.ty.nullable() || value.ty.data_type() != &DataType::Int64)
        {
            errors.push(ValidationError::new(
                &path,
                "summary cut local field is absent or has an inexact type",
            ));
        }
    }
}

/// Coverage is addressed by the declared summary input, never by writer
/// fields, total row count or a provider target ordinal.
pub(crate) fn validate_fragment_summary_inputs(
    fragment: &Fragment,
    cuts: &crate::FragmentCuts,
    errors: &mut ValidationContext,
) {
    let path = format!("fragments[{}].cuts.summary", fragment.id().get());
    let mut used = std::collections::BTreeSet::new();
    for node in fragment.nodes().values() {
        let crate::NodeKind::TableFinish(finish) = &node.kind else {
            continue;
        };
        let Some(port) = finish.inputs.logical_summary else {
            continue;
        };
        let exact = node
            .inputs
            .get(port as usize)
            .and_then(|id| fragment.nodes().get(id))
            .and_then(|source| {
                let crate::NodeKind::ExchangeSource { edge, .. } = source.kind else {
                    return None;
                };
                let cut = cuts
                    .inbound
                    .iter()
                    .find(|cut| cut.edge == edge && cut.destination_node == source.id)?;
                let ExchangePayloadContract::LogicalSummary(proof) = cut.payload else {
                    return None;
                };
                Some((edge, proof.destination))
            });
        match exact {
            Some((edge, columns))
                if finish.logical_summary == Some(columns) && used.insert(edge) => {}
            _ => errors.push(ValidationError::new(
                &path,
                "logical finish requires one exact independently bound summary source",
            )),
        }
    }
    for cut in &cuts.inbound {
        if matches!(cut.payload, ExchangePayloadContract::LogicalSummary(_))
            && !used.contains(&cut.edge)
        {
            errors.push(ValidationError::new(
                &path,
                "summary cut is not consumed exactly once by a logical finish",
            ));
        }
    }
}
