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

use super::*;
use crate::validation::source_provenance_index_bytes;
use std::collections::BTreeMap;

fn writer_owned(proof: WriterResultCutRef<'_>) -> WriterResultCut {
    WriterResultCut {
        write_target_ordinal: proof.write_target_ordinal,
        schema_revision: proof.schema_revision,
        fields: proof
            .fields()
            .map(|field| WriterResultCutField {
                source: field.source,
                destination: field.destination,
                name: field.name.into(),
                ty: field.ty.clone(),
                role: field.role,
            })
            .collect(),
    }
}

fn assert_oracle(plan: &PhysicalPlan) {
    let index = FragmentIoCutIndex::try_new(plan, 4 * 1024 * 1024).unwrap();
    let owned = derive_fragment_cuts(plan).unwrap();
    for (fragment, cuts) in &owned {
        let view = index.fragment(*fragment).unwrap();
        let inbound = view.inbound().collect::<Vec<_>>();
        let outbound = view.outbound().collect::<Vec<_>>();
        assert_eq!(inbound.len(), cuts.inbound.len());
        assert_eq!(outbound.len(), cuts.outbound.len());
        for (borrowed, owned) in inbound.iter().zip(&cuts.inbound) {
            assert_eq!(borrowed.edge.id, owned.edge);
            assert_eq!(borrowed.edge.kind, owned.kind);
            assert_eq!(borrowed.edge.source.fragment, owned.source_fragment);
            assert_eq!(borrowed.edge.destination.node, owned.destination_node);
            assert_eq!(&borrowed.edge.partitioning, &owned.partitioning);
            assert_eq!(
                borrowed
                    .imports()
                    .map(|import| CutImport {
                        source: CutValue {
                            value: import.source.value,
                            ty: import.source.ty.clone()
                        },
                        destination: import.destination,
                    })
                    .collect::<Vec<_>>()
                    .as_slice(),
                owned.imports.as_ref()
            );
            assert_eq!(
                borrowed
                    .source_bindings()
                    .map(SourceBindingRef::to_owned)
                    .collect::<Vec<_>>()
                    .as_slice(),
                owned.source_bindings.as_ref()
            );
            assert_eq!(borrowed.has_source_free_rows, owned.has_source_free_rows);
            assert_eq!(
                borrowed
                    .change_stream_writer
                    .map(|proof| ChangeStreamWriterCut {
                        route_id: proof.route_id,
                        write_target_ordinal: proof.write_target_ordinal,
                        fields: proof.fields().collect(),
                    }),
                owned.change_stream_writer
            );
            assert_eq!(
                borrowed.writer_result.map(writer_owned),
                owned.writer_result
            );
        }
        for (borrowed, owned) in outbound.iter().zip(&cuts.outbound) {
            assert_eq!(borrowed.edge.id, owned.edge);
            assert_eq!(borrowed.edge.kind, owned.kind);
            assert_eq!(
                borrowed.edge.destination.fragment,
                owned.destination_fragment
            );
            assert_eq!(&borrowed.edge.partitioning, &owned.partitioning);
            assert_eq!(
                borrowed
                    .projection()
                    .map(|value| CutValue {
                        value: value.value,
                        ty: value.ty.clone(),
                    })
                    .collect::<Vec<_>>()
                    .as_slice(),
                owned.projection.as_ref()
            );
            assert_eq!(
                borrowed
                    .imports()
                    .map(|import| CutImport {
                        source: CutValue {
                            value: import.source.value,
                            ty: import.source.ty.clone()
                        },
                        destination: import.destination,
                    })
                    .collect::<Vec<_>>()
                    .as_slice(),
                owned.destination_imports.as_ref()
            );
            assert_eq!(
                borrowed
                    .source_bindings()
                    .map(SourceBindingRef::to_owned)
                    .collect::<Vec<_>>()
                    .as_slice(),
                owned.source_bindings.as_ref()
            );
            assert_eq!(borrowed.has_source_free_rows, owned.has_source_free_rows);
            assert_eq!(
                borrowed
                    .change_stream_writer
                    .map(|proof| ChangeStreamWriterCut {
                        route_id: proof.route_id,
                        write_target_ordinal: proof.write_target_ordinal,
                        fields: proof.fields().collect(),
                    }),
                owned.change_stream_writer
            );
            assert_eq!(
                borrowed.writer_result.map(writer_owned),
                owned.writer_result
            );
            for value in borrowed.projection() {
                assert!(std::ptr::eq(
                    value.ty,
                    &borrowed
                        .source_fragment
                        .values()
                        .get(&value.value)
                        .unwrap()
                        .ty
                ));
            }
        }
    }
}

fn raw_plan(fragments: Vec<Fragment>, edges: Vec<Edge>) -> PhysicalPlan {
    crate::plan::PhysicalPlanParts {
        version: version(),
        fragments: fragments
            .into_iter()
            .map(|fragment| (fragment.id(), fragment))
            .collect(),
        edges: edges.into_iter().map(|edge| (edge.id, edge)).collect(),
        runtime_filters: BTreeMap::new(),
        result_port: None,
        artifact_refs: BTreeMap::new(),
        required: RequiredContracts::default(),
        annotations: Box::default(),
    }
    .into()
}

fn edge(source: &Fragment, destination: &Fragment, id: u32) -> Edge {
    let source_value = source.nodes().get(&source.root()).unwrap().output.columns[0];
    let destination_value = destination
        .nodes()
        .get(&destination.root())
        .unwrap()
        .output
        .columns[0];
    Edge {
        id: EdgeId::new(id),
        kind: EdgeKind::Stream,
        source: EdgeSource {
            fragment: source.id(),
            projection: Box::from([source_value]),
        },
        destination: EdgeDestination {
            fragment: destination.id(),
            node: destination.root(),
            receive_mapping: Box::from([(source_value, destination_value)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Unconstrained,
            destination: Distribution::Unconstrained,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    }
}

#[test]
fn borrowed_io_cuts_match_owned_router_writer_and_source_free_facts() {
    let plan = super::sink_contract::finish_router_writer_plan(
        super::sink_contract::RouterWriterShape::Valid,
    )
    .unwrap();
    assert_oracle(&plan);
    let index = FragmentIoCutIndex::try_new(&plan, 4 * 1024 * 1024).unwrap();
    let view = index.fragment(FragmentId::new(722)).unwrap();
    let cut = view.outbound().next().unwrap();
    let proof = cut.writer_result.unwrap();
    let NodeKind::TableWriter { target } = &cut
        .source_fragment
        .nodes()
        .get(&cut.source_fragment.root())
        .unwrap()
        .kind
    else {
        unreachable!()
    };
    for (field, original) in proof.fields().zip(target.output_schema.fields.iter()) {
        assert!(std::ptr::eq(field.ty, &original.ty));
        assert_eq!(field.name.as_ptr(), original.name.as_ptr());
    }
    assert!(cut.has_source_free_rows);
    assert_oracle(&broadcast_edge_plan(RowMultiplicity::Replicated).unwrap());
}

#[test]
fn borrowed_io_cuts_preserve_first_visit_order_dedup_and_mixed_dag_source_free_rows() {
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 81),
    };
    let first = metadata_relation(&binding, column.clone());
    let mut second = first.clone();
    let Relation::Metadata(relation) = &mut second else {
        unreachable!()
    };
    relation.read.input_version = ExactInputVersion::try_new(vec![1]).unwrap();
    let a = finish_scan_relation_at(
        first.clone(),
        FragmentId::new(1),
        ProviderReadOccurrenceId::new(1),
    )
    .unwrap();
    let duplicate =
        finish_scan_relation_at(first, FragmentId::new(2), ProviderReadOccurrenceId::new(2))
            .unwrap();
    let b = finish_scan_relation_at(second, FragmentId::new(3), ProviderReadOccurrenceId::new(3))
        .unwrap();
    let (source_free, _) = literal_fragment(FragmentId::new(4), FragmentSink::Noop, false);
    let (join, _) = literal_fragment(FragmentId::new(5), FragmentSink::Noop, false);
    let (last, _) = literal_fragment(FragmentId::new(6), FragmentSink::Noop, false);
    let edges = vec![
        edge(&a, &join, 1),
        edge(&duplicate, &join, 2),
        edge(&b, &join, 3),
        edge(&source_free, &join, 4),
        edge(&join, &last, 5),
        edge(&a, &last, 6),
    ];
    // A kernel fixture permits multiple independent source nodes without
    // introducing unrelated union-expression or physical-property machinery.
    let plan = raw_plan(vec![a, duplicate, b, source_free, join, last], edges);
    assert_oracle(&plan);
    let index = FragmentIoCutIndex::try_new(&plan, 4 * 1024 * 1024).unwrap();
    let view = index.fragment(FragmentId::new(6)).unwrap();
    let cut = view
        .inbound()
        .find(|cut| cut.edge.id == EdgeId::new(5))
        .unwrap();
    let bindings = cut.source_bindings().collect::<Vec<_>>();
    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].source.input_version.as_bytes(), &[9]);
    assert_eq!(bindings[1].source.input_version.as_bytes(), &[1]);
    assert!(cut.has_source_free_rows);
}

#[test]
fn an_index_above_the_callers_budget_is_refused_before_construction() {
    let (plan, _) = super::artifact_provenance_contract::provenance_cut_fixture();
    let bytes = source_provenance_index_bytes(&plan).unwrap();
    assert!(bytes < 4 * 1024 * 1024);
    assert!(FragmentIoCutIndex::try_new(&plan, bytes - 1).is_none());
    assert!(FragmentIoCutIndex::try_new(&plan, bytes).is_some());
    assert!(FragmentIoCutIndex::try_new(&plan, 0).is_none());
}

#[test]
fn large_provider_payloads_remain_borrowed_and_do_not_increase_the_auxiliary_index() {
    let (base, destination) = super::artifact_provenance_contract::provenance_cut_fixture();
    let original_bytes = source_provenance_index_bytes(&base).unwrap();
    let mut fragments = base.fragments().values().cloned().collect::<Vec<_>>();
    let source = fragments
        .iter()
        .position(|fragment| fragment.id() != destination)
        .unwrap();
    let old = &fragments[source];
    let mut nodes = old.nodes().clone();
    let NodeKind::Scan { relation, .. } = &mut nodes.get_mut(&old.root()).unwrap().kind else {
        unreachable!()
    };
    let Relation::Metadata(metadata) = relation.as_mut() else {
        unreachable!()
    };
    let binding = &metadata.read.binding;
    let large = ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            binding.descriptor().provider_id.clone(),
            binding.catalog_handle().clone(),
            ConnectorCodecCategory::ReadTable,
            ConnectorCodecRevision::try_new(1).unwrap(),
        ),
        vec![7; 2 * 1024 * 1024].into(),
    );
    metadata.read.relation = ConnectorReadRelationPayload::new(
        ConnectorReadRelationKind::SystemTable,
        large,
        encoded(binding, ConnectorCodecCategory::ReadView, 2),
    );
    metadata.read.input_version = ExactInputVersion::try_new(vec![9; 4096]).unwrap();
    let replacement = crate::plan::FragmentParts {
        id: old.id(),
        root: old.root(),
        values: old.values().clone(),
        expressions: old.expressions().clone(),
        nodes,
        sink: old.sink().clone(),
        dop_domain: old.dop_domain(),
        runtime_filters: old.runtime_filters().into(),
    }
    .into();
    fragments[source] = replacement;
    let plan = raw_plan(fragments, base.edges().values().cloned().collect());
    assert_eq!(
        source_provenance_index_bytes(&plan).unwrap(),
        original_bytes
    );
    let index = FragmentIoCutIndex::try_new(&plan, original_bytes).unwrap();
    let view = index.fragment(destination).unwrap();
    let cut = view.inbound().next().unwrap();
    let borrowed = cut.source_bindings().next().unwrap();
    let NodeKind::Scan { relation, .. } = &cut
        .source_fragment
        .nodes()
        .get(&cut.source_fragment.root())
        .unwrap()
        .kind
    else {
        unreachable!()
    };
    let original = relation.source_binding_ref();
    assert!(std::ptr::eq(borrowed.source, original.source));
    assert!(std::ptr::eq(
        borrowed.selection_digest,
        original.selection_digest
    ));
    assert_eq!(
        borrowed.source.input_version.as_bytes().as_ptr(),
        original.source.input_version.as_bytes().as_ptr()
    );
}

#[test]
fn cyclic_or_missing_source_graphs_are_refused_by_the_shared_provenance_kernel() {
    let (a, _) = literal_fragment(FragmentId::new(1), FragmentSink::Noop, false);
    let (b, _) = literal_fragment(FragmentId::new(2), FragmentSink::Noop, false);
    let cyclic = raw_plan(
        vec![a.clone(), b.clone()],
        vec![edge(&a, &b, 1), edge(&b, &a, 2)],
    );
    assert!(FragmentIoCutIndex::try_new(&cyclic, 4 * 1024 * 1024).is_none());
    let missing = raw_plan(vec![b.clone()], vec![edge(&a, &b, 1)]);
    assert!(FragmentIoCutIndex::try_new(&missing, 4 * 1024 * 1024).is_none());
}
