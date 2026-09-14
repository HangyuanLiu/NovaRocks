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

fn provenance_cut_fixture() -> (PhysicalPlan, FragmentId) {
    let edge = EdgeId::new(811);
    let source_fragment = FragmentId::new(811);
    let destination_fragment = FragmentId::new(812);
    let binding = connector_binding();
    let column = ProviderColumnReference {
        column_payload: encoded(&binding, ConnectorCodecCategory::ReadColumn, 81),
    };
    let relation = metadata_relation(&binding, column.clone());

    let mut source_builder = FragmentBuilder::new(source_fragment);
    let scan = source_builder.reserve_node_id().unwrap();
    let source_value = source_builder
        .add_value(
            relation.schema()[0].ty.clone(),
            ValueOrigin::ProviderField {
                scan_node: scan,
                field: column.clone(),
            },
        )
        .unwrap();
    source_builder
        .insert_node_unchecked(PhysicalNode {
            id: scan,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node: scan,
                columns: Box::from([source_value]),
            },
            kind: NodeKind::Scan {
                occurrence: ProviderReadOccurrenceId::new(0),
                relation: Box::new(relation),
                read_budget: scan_budget(),
                provider_outputs: Box::from([(column, source_value)]),
                residuals: Box::default(),
                derived_values: Box::default(),
            },
        })
        .unwrap();
    let source = source_builder
        .finish_definition(scan, FragmentSink::Stream { edge }, dop())
        .unwrap();

    let mut destination_builder = FragmentBuilder::new(destination_fragment);
    let exchange = destination_builder.reserve_node_id().unwrap();
    let destination_value = destination_builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    destination_builder
        .insert_node_unchecked(PhysicalNode {
            id: exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: unconstrained(),
            output: OutputPort {
                node: exchange,
                columns: Box::from([destination_value]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, destination_value)]),
            },
        })
        .unwrap();
    let destination = destination_builder
        .finish_definition(exchange, FragmentSink::Noop, dop())
        .unwrap();

    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_edge(Edge {
        id: edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            fragment: source_fragment,
            projection: Box::from([source_value]),
        },
        destination: EdgeDestination {
            fragment: destination_fragment,
            node: exchange,
            receive_mapping: Box::from([(source_value, destination_value)]),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Unconstrained,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Unconstrained,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    (plan.finish().unwrap(), destination_fragment)
}

#[test]
fn equal_selection_digests_from_different_sources_are_distinct_valid_provenance() {
    let (plan, destination_fragment) = provenance_cut_fixture();
    let fragment = plan.fragments().get(&destination_fragment).unwrap();
    let mut cuts = fragment_cuts(&plan, destination_fragment).unwrap();
    let original = cuts.inbound[0].source_bindings[0].clone();
    let mut other_source = original.clone();
    other_source.source.input_version = ExactInputVersion::try_new(vec![10]).unwrap();
    assert_eq!(other_source.selection_digest, original.selection_digest);
    assert_ne!(other_source, original);

    cuts.inbound[0].source_bindings = Box::from([original, other_source]);

    validate_fragment(fragment, &cuts).unwrap();
}

#[test]
fn provenance_index_deduplicates_identical_complete_bindings() {
    let (plan, destination_fragment) = provenance_cut_fixture();
    let fragment = plan.fragments().get(&destination_fragment).unwrap();
    let mut cuts = fragment_cuts(&plan, destination_fragment).unwrap();
    let binding = cuts.inbound[0].source_bindings[0].clone();
    cuts.inbound[0].source_bindings = Box::from([binding.clone(), binding]);

    validate_fragment(fragment, &cuts).unwrap();
}
