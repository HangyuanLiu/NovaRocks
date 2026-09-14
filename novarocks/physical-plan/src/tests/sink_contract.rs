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

use novarocks_connector_contract::{
    ConnectorCodecCategory, ConnectorRowMutationEffect, ConnectorWriteFieldToken,
};

use super::*;

fn literal_root() -> (FragmentBuilder, NodeId, ValueId, ValueId) {
    let mut builder = FragmentBuilder::new(FragmentId::new(721));
    let node = builder.reserve_node_id().unwrap();
    let effect_expression = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(1)),
        )
        .unwrap();
    let input_expression = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Literal(LiteralValue::Int64(2)),
        )
        .unwrap();
    let effect = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let input = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 1,
            },
        )
        .unwrap();
    builder
        .insert_node(PhysicalNode {
            id: node,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: singleton(),
            output: OutputPort {
                node,
                columns: Box::from([effect, input]),
            },
            kind: NodeKind::Values {
                rows: Box::from([Box::from([effect_expression, input_expression])]),
            },
        })
        .unwrap();
    (builder, node, effect, input)
}

fn change_event_root() -> (FragmentBuilder, NodeId, ValueId, ValueId) {
    change_event_root_with_assignment(AssignmentShape::Valid)
}

#[derive(Clone, Copy)]
enum AssignmentShape {
    Valid,
    EffectTarget,
    OffPortTarget,
    DuplicateTarget,
}

fn change_event_root_with_assignment(
    shape: AssignmentShape,
) -> (FragmentBuilder, NodeId, ValueId, ValueId) {
    let (mut builder, source, _, source_value) = literal_root();
    let node = builder.reserve_node_id().unwrap();
    let assignment = builder
        .add_expression(
            node,
            ty(DataType::Int64, false),
            ExprKind::Value(source_value),
        )
        .unwrap();
    let effect = builder
        .add_value(
            ty(DataType::Int8, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let input = builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::NodeOutput {
                node,
                output_ordinal: 1,
            },
        )
        .unwrap();
    let assignments = match shape {
        AssignmentShape::Valid => vec![(input, Some(assignment))],
        AssignmentShape::EffectTarget => {
            vec![(effect, None), (input, Some(assignment))]
        }
        AssignmentShape::OffPortTarget => {
            vec![(source_value, Some(assignment)), (input, Some(assignment))]
        }
        AssignmentShape::DuplicateTarget => {
            vec![(input, Some(assignment)), (input, Some(assignment))]
        }
    };
    builder
        .insert_node(PhysicalNode {
            id: node,
            inputs: Box::from([source]),
            required_inputs: Box::from([unconstrained()]),
            output_properties: unconstrained(),
            output: OutputPort {
                node,
                columns: Box::from([effect, input]),
            },
            kind: NodeKind::ChangeEventExpand {
                events: Box::from([ChangeEventSpec {
                    predicate: None,
                    effect: ConnectorRowMutationEffect::Insert,
                    assignments: assignments.into_boxed_slice(),
                }]),
                effect_output: effect,
            },
        })
        .unwrap();
    (builder, node, effect, input)
}

#[test]
fn change_event_assignments_reject_the_generated_effect_output() {
    let (builder, root, _, _) = change_event_root_with_assignment(AssignmentShape::EffectTarget);
    let errors = builder
        .finish_definition(root, FragmentSink::Noop, dop())
        .expect_err("the generated effect output cannot be assigned by an event");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("assignment targets the generated effect output")
    }));
}

#[test]
fn change_event_assignments_reject_outputs_absent_from_the_node_port() {
    let (builder, root, _, _) = change_event_root_with_assignment(AssignmentShape::OffPortTarget);
    let errors = builder
        .finish_definition(root, FragmentSink::Noop, dop())
        .expect_err("an event assignment must target the node output port");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("assignment output is absent from the node output port")
    }));
}

#[test]
fn change_event_assignments_reject_duplicate_outputs() {
    let (builder, root, _, _) = change_event_root_with_assignment(AssignmentShape::DuplicateTarget);
    let errors = builder
        .finish_definition(root, FragmentSink::Noop, dop())
        .expect_err("one event cannot assign one output twice");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("event contains a duplicate assignment output")
    }));
}

fn route(
    route_id: ConnectorWriteRouteId,
    write_target_ordinal: WriteTargetOrdinal,
    edge: EdgeId,
    input: ValueId,
) -> ChangeStreamRoute {
    ChangeStreamRoute {
        route_id,
        write_target_ordinal,
        accepted_effects: Box::from([ConnectorRowMutationEffect::Insert]),
        input_mapping: Box::from([(ConnectorWriteFieldToken::from_bytes([3; 32]), input)]),
        partition_by: Box::from([input]),
        edge,
    }
}

#[derive(Clone, Copy)]
enum RouterWriterShape {
    Valid,
    OrdinalDrift,
    TokenDrift,
    PartitionDrift,
    UnionValid,
    UnionAuxiliarySwap,
}

fn finish_router_writer_plan(shape: RouterWriterShape) -> Result<PhysicalPlan, ValidationErrors> {
    finish_router_writer_fixture(shape).1
}

fn finish_router_writer_fixture(
    shape: RouterWriterShape,
) -> (Fragment, Result<PhysicalPlan, ValidationErrors>) {
    let edge = EdgeId::new(901);
    let finish_edge = EdgeId::new(904);
    let (source_builder, source_root, effect, source_value) = change_event_root();
    let route_token = ConnectorWriteFieldToken::from_bytes([3; 32]);
    let edge_distribution = if matches!(shape, RouterWriterShape::PartitionDrift) {
        Distribution::RoundRobin
    } else {
        Distribution::Singleton
    };
    let source = source_builder
        .finish_definition(
            source_root,
            FragmentSink::Router {
                effect,
                routes: Box::from([ChangeStreamRoute {
                    route_id: write_route_id(9),
                    write_target_ordinal: write_target_ordinal(0),
                    accepted_effects: Box::from([ConnectorRowMutationEffect::Insert]),
                    input_mapping: Box::from([(route_token, source_value)]),
                    partition_by: Box::default(),
                    edge,
                }]),
            },
            dop(),
        )
        .unwrap();

    let mut destination_builder = FragmentBuilder::new(FragmentId::new(722));
    let exchange = destination_builder.reserve_node_id().unwrap();
    let imported = destination_builder
        .add_value(
            ty(DataType::Int64, false),
            ValueOrigin::ExchangeImport { edge, source_value },
        )
        .unwrap();
    destination_builder
        .insert_node(PhysicalNode {
            id: exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: edge_distribution.clone(),
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: exchange,
                columns: Box::from([imported]),
            },
            kind: NodeKind::ExchangeSource {
                edge,
                imports: Box::from([(source_value, imported)]),
            },
        })
        .unwrap();
    let writer = destination_builder.reserve_node_id().unwrap();
    let writer_output_specs = [
        (
            "kind",
            DataType::Int8,
            false,
            WriterRelationFieldRole::Kind,
            WriterDerivedKind::RelationKind,
        ),
        (
            "write_target_ordinal",
            DataType::Int32,
            false,
            WriterRelationFieldRole::TargetOrdinal,
            WriterDerivedKind::WriteTargetOrdinal,
        ),
        (
            "row_count",
            DataType::Int64,
            true,
            WriterRelationFieldRole::RowCount,
            WriterDerivedKind::AffectedRows,
        ),
        (
            "commit_fragment",
            DataType::Binary,
            true,
            WriterRelationFieldRole::CommitFragment,
            WriterDerivedKind::CommitFragment,
        ),
        (
            "aux_a",
            DataType::Binary,
            true,
            WriterRelationFieldRole::Auxiliary,
            WriterDerivedKind::RelationAuxiliary,
        ),
        (
            "aux_b",
            DataType::Binary,
            true,
            WriterRelationFieldRole::Auxiliary,
            WriterDerivedKind::RelationAuxiliary,
        ),
    ];
    let writer_output_fields = writer_output_specs
        .into_iter()
        .map(|(name, data_type, nullable, role, kind)| {
            let value_type = ty(data_type, nullable);
            let value = destination_builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::WriterDerived {
                        writer_node: writer,
                        kind,
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: name.into(),
                ty: value_type,
                role,
            }
        })
        .collect::<Vec<_>>();
    let writer_output_values = writer_output_fields
        .iter()
        .map(|field| field.value)
        .collect::<Vec<_>>();
    let writer_ordinal = if matches!(shape, RouterWriterShape::OrdinalDrift) {
        write_target_ordinal(1)
    } else {
        write_target_ordinal(0)
    };
    let binding = connector_binding();
    let writer_token = if matches!(shape, RouterWriterShape::TokenDrift) {
        ConnectorWriteFieldToken::from_bytes([4; 32])
    } else {
        route_token
    };
    destination_builder
        .insert_node(PhysicalNode {
            id: writer,
            inputs: Box::from([exchange]),
            required_inputs: Box::from([PhysicalProperties {
                distribution: edge_distribution.clone(),
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            }]),
            output_properties: unconstrained(),
            output: OutputPort {
                node: writer,
                columns: writer_output_fields
                    .iter()
                    .map(|field| field.value)
                    .collect(),
            },
            kind: NodeKind::TableWriter {
                target: WriterTarget {
                    handle: encoded(&binding, ConnectorCodecCategory::WriteHandle, 7),
                    write_target_ordinal: writer_ordinal,
                    input: Box::from([imported]),
                    required_distribution: edge_distribution.clone(),
                    target_fields: Box::from([WriterTargetField {
                        token: writer_token,
                        input: imported,
                        ty: ty(DataType::Int64, false),
                        hidden: false,
                    }]),
                    output_schema: WriterRelationSchema {
                        revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                        fields: writer_output_fields.clone().into_boxed_slice(),
                    },
                    partial_aggregates: Box::default(),
                },
            },
        })
        .unwrap();
    let destination = destination_builder
        .finish_definition(writer, FragmentSink::Stream { edge: finish_edge }, dop())
        .unwrap();

    let mut finish_builder = FragmentBuilder::new(FragmentId::new(723));
    let finish_exchange = finish_builder.reserve_node_id().unwrap();
    let finish_input_fields = writer_output_fields
        .iter()
        .map(|field| {
            let value = finish_builder
                .add_value(
                    field.ty.clone(),
                    ValueOrigin::ExchangeImport {
                        edge: finish_edge,
                        source_value: field.value,
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: field.name.clone(),
                ty: field.ty.clone(),
                role: field.role,
            }
        })
        .collect::<Vec<_>>();
    finish_builder
        .insert_node(PhysicalNode {
            id: finish_exchange,
            inputs: Box::default(),
            required_inputs: Box::default(),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: finish_exchange,
                columns: finish_input_fields
                    .iter()
                    .map(|field| field.value)
                    .collect(),
            },
            kind: NodeKind::ExchangeSource {
                edge: finish_edge,
                imports: writer_output_fields
                    .iter()
                    .zip(&finish_input_fields)
                    .map(|(source, destination)| (source.value, destination.value))
                    .collect(),
            },
        })
        .unwrap();
    let exchange_input_fields = finish_input_fields.clone();
    let mut union_writer_fragment = None;
    let mut union_writer_edge = None;
    let (finish_input_node, finish_input_fields, expected_target_ordinals) = if matches!(
        shape,
        RouterWriterShape::UnionValid | RouterWriterShape::UnionAuxiliarySwap
    ) {
        let union_edge = EdgeId::new(905);
        let mut local_builder = FragmentBuilder::new(FragmentId::new(724));
        let local_input = local_builder.reserve_node_id().unwrap();
        let local_input_value = local_builder
            .add_value(
                ty(DataType::Int64, false),
                ValueOrigin::NodeOutput {
                    node: local_input,
                    output_ordinal: 0,
                },
            )
            .unwrap();
        local_builder
            .insert_node(PhysicalNode {
                id: local_input,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
                output: OutputPort {
                    node: local_input,
                    columns: Box::from([local_input_value]),
                },
                kind: NodeKind::Values {
                    rows: Box::default(),
                },
            })
            .unwrap();
        let local_writer = local_builder.reserve_node_id().unwrap();
        let local_fields = writer_output_fields
            .iter()
            .map(|field| {
                let value = local_builder
                    .add_value(
                        field.ty.clone(),
                        ValueOrigin::WriterDerived {
                            writer_node: local_writer,
                            kind: match field.role {
                                WriterRelationFieldRole::Kind => WriterDerivedKind::RelationKind,
                                WriterRelationFieldRole::TargetOrdinal => {
                                    WriterDerivedKind::WriteTargetOrdinal
                                }
                                WriterRelationFieldRole::RowCount => {
                                    WriterDerivedKind::AffectedRows
                                }
                                WriterRelationFieldRole::CommitFragment => {
                                    WriterDerivedKind::CommitFragment
                                }
                                WriterRelationFieldRole::Auxiliary => {
                                    WriterDerivedKind::RelationAuxiliary
                                }
                            },
                        },
                    )
                    .unwrap();
                WriterRelationField {
                    value,
                    name: field.name.clone(),
                    ty: field.ty.clone(),
                    role: field.role,
                }
            })
            .collect::<Vec<_>>();
        local_builder
            .insert_node(PhysicalNode {
                id: local_writer,
                inputs: Box::from([local_input]),
                required_inputs: Box::from([PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                }]),
                output_properties: unconstrained(),
                output: OutputPort {
                    node: local_writer,
                    columns: local_fields.iter().map(|field| field.value).collect(),
                },
                kind: NodeKind::TableWriter {
                    target: WriterTarget {
                        handle: encoded(&binding, ConnectorCodecCategory::WriteHandle, 8),
                        write_target_ordinal: write_target_ordinal(1),
                        input: Box::from([local_input_value]),
                        required_distribution: Distribution::Singleton,
                        target_fields: Box::from([WriterTargetField {
                            token: ConnectorWriteFieldToken::from_bytes([8; 32]),
                            input: local_input_value,
                            ty: ty(DataType::Int64, false),
                            hidden: false,
                        }]),
                        output_schema: WriterRelationSchema {
                            revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                            fields: local_fields.clone().into_boxed_slice(),
                        },
                        partial_aggregates: Box::default(),
                    },
                },
            })
            .unwrap();
        let local_writer_values = local_fields
            .iter()
            .map(|field| field.value)
            .collect::<Box<[_]>>();
        let local_fragment = local_builder
            .finish_definition(
                local_writer,
                FragmentSink::Stream { edge: union_edge },
                dop(),
            )
            .unwrap();
        let local_exchange = finish_builder.reserve_node_id().unwrap();
        let local_import_fields = local_fields
            .iter()
            .map(|field| {
                let value = finish_builder
                    .add_value(
                        field.ty.clone(),
                        ValueOrigin::ExchangeImport {
                            edge: union_edge,
                            source_value: field.value,
                        },
                    )
                    .unwrap();
                WriterRelationField {
                    value,
                    name: field.name.clone(),
                    ty: field.ty.clone(),
                    role: field.role,
                }
            })
            .collect::<Vec<_>>();
        finish_builder
            .insert_node(PhysicalNode {
                id: local_exchange,
                inputs: Box::default(),
                required_inputs: Box::default(),
                output_properties: PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
                output: OutputPort {
                    node: local_exchange,
                    columns: local_import_fields
                        .iter()
                        .map(|field| field.value)
                        .collect(),
                },
                kind: NodeKind::ExchangeSource {
                    edge: union_edge,
                    imports: local_fields
                        .iter()
                        .zip(&local_import_fields)
                        .map(|(source, destination)| (source.value, destination.value))
                        .collect(),
                },
            })
            .unwrap();
        let union = finish_builder.reserve_node_id().unwrap();
        let union_fields = finish_input_fields
            .iter()
            .enumerate()
            .map(|(ordinal, field)| {
                let value = finish_builder
                    .add_value(
                        field.ty.clone(),
                        ValueOrigin::NodeOutput {
                            node: union,
                            output_ordinal: u32::try_from(ordinal).unwrap(),
                        },
                    )
                    .unwrap();
                WriterRelationField {
                    value,
                    name: field.name.clone(),
                    ty: field.ty.clone(),
                    role: field.role,
                }
            })
            .collect::<Vec<_>>();
        let mut local_mapping = local_import_fields
            .iter()
            .map(|field| field.value)
            .collect::<Vec<_>>();
        if matches!(shape, RouterWriterShape::UnionAuxiliarySwap) {
            local_mapping.swap(4, 5);
        }
        finish_builder
            .insert_node(PhysicalNode {
                id: union,
                inputs: Box::from([finish_exchange, local_exchange]),
                required_inputs: Box::from([
                    PhysicalProperties {
                        distribution: Distribution::Singleton,
                        row_multiplicity: RowMultiplicity::SingleCopy,
                        ordering: Box::default(),
                    },
                    PhysicalProperties {
                        distribution: Distribution::Singleton,
                        row_multiplicity: RowMultiplicity::SingleCopy,
                        ordering: Box::default(),
                    },
                ]),
                output_properties: PhysicalProperties {
                    distribution: Distribution::Singleton,
                    row_multiplicity: RowMultiplicity::SingleCopy,
                    ordering: Box::default(),
                },
                output: OutputPort {
                    node: union,
                    columns: union_fields.iter().map(|field| field.value).collect(),
                },
                kind: NodeKind::SetOp {
                    kind: SetOperationKind::UnionAll,
                    input_mappings: Box::from([
                        finish_input_fields
                            .iter()
                            .map(|field| field.value)
                            .collect(),
                        local_mapping.into_boxed_slice(),
                    ]),
                },
            })
            .unwrap();
        union_writer_fragment = Some(local_fragment);
        union_writer_edge = Some(Edge {
            id: union_edge,
            kind: EdgeKind::Stream,
            source: EdgeSource {
                fragment: FragmentId::new(724),
                projection: local_writer_values,
            },
            destination: EdgeDestination {
                fragment: FragmentId::new(723),
                node: local_exchange,
                receive_mapping: local_fields
                    .iter()
                    .zip(&local_import_fields)
                    .map(|(source, destination)| (source.value, destination.value))
                    .collect(),
            },
            partitioning: EdgePartitioning {
                source: Distribution::Singleton,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Singleton,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
        });
        (
            union,
            union_fields,
            Box::from([writer_ordinal, write_target_ordinal(1)]),
        )
    } else {
        (
            finish_exchange,
            finish_input_fields,
            Box::from([writer_ordinal]),
        )
    };
    let finish = finish_builder.reserve_node_id().unwrap();
    let list_type = DataType::List(std::sync::Arc::new(arrow_schema::Field::new(
        "item",
        DataType::Int32,
        false,
    )));
    let map_type = DataType::Map(
        std::sync::Arc::new(arrow_schema::Field::new(
            "entries",
            DataType::Struct(arrow_schema::Fields::from(vec![
                arrow_schema::Field::new("key", DataType::Utf8, false),
                arrow_schema::Field::new("value", DataType::Utf8, false),
            ])),
            false,
        )),
        false,
    );
    let root_specs = [
        (
            "kind",
            DataType::Int8,
            false,
            WriterRelationFieldRole::Kind,
            WriterDerivedKind::RelationKind,
        ),
        (
            "write_target_ordinal",
            DataType::Int32,
            true,
            WriterRelationFieldRole::TargetOrdinal,
            WriterDerivedKind::WriteTargetOrdinal,
        ),
        (
            "row_count",
            DataType::Int64,
            true,
            WriterRelationFieldRole::RowCount,
            WriterDerivedKind::AffectedRows,
        ),
        (
            "commit_fragment",
            DataType::Binary,
            true,
            WriterRelationFieldRole::CommitFragment,
            WriterDerivedKind::CommitFragment,
        ),
        (
            "input_fields",
            list_type,
            true,
            WriterRelationFieldRole::Auxiliary,
            WriterDerivedKind::RelationAuxiliary,
        ),
        (
            "blob_type",
            DataType::Utf8,
            true,
            WriterRelationFieldRole::Auxiliary,
            WriterDerivedKind::RelationAuxiliary,
        ),
        (
            "body",
            DataType::Binary,
            true,
            WriterRelationFieldRole::Auxiliary,
            WriterDerivedKind::RelationAuxiliary,
        ),
        (
            "properties",
            map_type,
            true,
            WriterRelationFieldRole::Auxiliary,
            WriterDerivedKind::RelationAuxiliary,
        ),
    ];
    let root_fields = root_specs
        .into_iter()
        .map(|(name, data_type, nullable, role, kind)| {
            let value_type = ty(data_type, nullable);
            let value = finish_builder
                .add_value(
                    value_type.clone(),
                    ValueOrigin::WriterDerived {
                        writer_node: finish,
                        kind,
                    },
                )
                .unwrap();
            WriterRelationField {
                value,
                name: name.into(),
                ty: value_type,
                role,
            }
        })
        .collect::<Vec<_>>();
    finish_builder
        .insert_node(PhysicalNode {
            id: finish,
            inputs: Box::from([finish_input_node]),
            required_inputs: Box::from([PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            }]),
            output_properties: PhysicalProperties {
                distribution: Distribution::Singleton,
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            },
            output: OutputPort {
                node: finish,
                columns: root_fields.iter().map(|field| field.value).collect(),
            },
            kind: NodeKind::TableFinish(WriterFinishSpec {
                expected_target_ordinals,
                input_schema: WriterRelationSchema {
                    revision: WRITER_MULTIPLEX_SCHEMA_REVISION,
                    fields: finish_input_fields.clone().into_boxed_slice(),
                },
                output_schema: WriterRelationSchema {
                    revision: ROOT_WRITE_RESULT_SCHEMA_REVISION,
                    fields: root_fields.into_boxed_slice(),
                },
                final_aggregates: Box::default(),
                grouped_unpivot: None,
            }),
        })
        .unwrap();
    let finish_fragment = finish_builder
        .finish_definition(finish, FragmentSink::Noop, dop())
        .unwrap();

    let mut plan = PlanBuilder::new(version());
    plan.add_fragment(source).unwrap();
    plan.add_fragment(destination).unwrap();
    plan.add_fragment(finish_fragment.clone()).unwrap();
    if let Some(fragment) = union_writer_fragment {
        plan.add_fragment(fragment).unwrap();
    }
    plan.add_edge(Edge {
        id: edge,
        kind: EdgeKind::ChangeStreamRouter,
        source: EdgeSource {
            fragment: FragmentId::new(721),
            projection: Box::from([source_value]),
        },
        destination: EdgeDestination {
            fragment: FragmentId::new(722),
            node: exchange,
            receive_mapping: Box::from([(source_value, imported)]),
        },
        partitioning: EdgePartitioning {
            source: edge_distribution.clone(),
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: edge_distribution,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    plan.add_edge(Edge {
        id: finish_edge,
        kind: EdgeKind::Stream,
        source: EdgeSource {
            fragment: FragmentId::new(722),
            projection: writer_output_values.into_boxed_slice(),
        },
        destination: EdgeDestination {
            fragment: FragmentId::new(723),
            node: finish_exchange,
            receive_mapping: writer_output_fields
                .iter()
                .zip(&exchange_input_fields)
                .map(|(source, destination)| (source.value, destination.value))
                .collect(),
        },
        partitioning: EdgePartitioning {
            source: Distribution::Singleton,
            source_multiplicity: RowMultiplicity::SingleCopy,
            destination: Distribution::Singleton,
            destination_multiplicity: RowMultiplicity::SingleCopy,
        },
    })
    .unwrap();
    if let Some(edge) = union_writer_edge {
        plan.add_edge(edge).unwrap();
    }
    (finish_fragment, plan.finish())
}

#[test]
fn router_edge_closes_its_exact_destination_writer_contract() {
    finish_router_writer_plan(RouterWriterShape::Valid)
        .expect("the exact route, edge and writer contract must validate");
}

#[test]
fn writer_union_preserves_each_finish_field_occurrence() {
    finish_router_writer_plan(RouterWriterShape::UnionValid)
        .expect("UnionAll may merge exact writer relation occurrences");
}

#[test]
fn writer_union_rejects_same_typed_auxiliary_field_swaps() {
    let errors = finish_router_writer_plan(RouterWriterShape::UnionAuxiliarySwap)
        .expect_err("UnionAll cannot swap same-typed writer field roles");
    assert!(
        errors.errors().iter().any(|error| {
            error
                .message()
                .contains("stream source fields do not map exactly to its table finish input roles")
        }),
        "{errors:?}"
    );
}

#[test]
fn independent_finish_rechecks_writer_union_field_occurrences() {
    let valid = finish_router_writer_plan(RouterWriterShape::UnionValid).unwrap();
    let valid_finish = valid.fragments().get(&FragmentId::new(723)).unwrap();
    let cuts = fragment_cuts(&valid, valid_finish.id()).unwrap();
    validate_fragment(valid_finish, &cuts).expect("exact UnionAll writer lineage must validate");

    let (swapped_finish, _) = finish_router_writer_fixture(RouterWriterShape::UnionAuxiliarySwap);
    let errors = validate_fragment(&swapped_finish, &cuts)
        .expect_err("independent validation must reject a same-typed field swap");
    assert!(errors.errors().iter().any(|error| {
        error.message().contains(
            "upstream writer result fields do not map exactly to its table finish input roles",
        )
    }));
}

#[test]
fn router_edge_rejects_a_destination_writer_ordinal_drift() {
    let errors = finish_router_writer_plan(RouterWriterShape::OrdinalDrift)
        .expect_err("a route cannot select another writer target ordinal");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("write target ordinal differs from its destination table writer")
    }));
}

#[test]
fn router_edge_rejects_a_destination_writer_field_token_drift() {
    let errors = finish_router_writer_plan(RouterWriterShape::TokenDrift)
        .expect_err("a route field token must reach the same destination writer field");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("field mapping differs from its destination table writer contract")
    }));
}

#[test]
fn router_edge_rejects_partition_authority_drift() {
    let errors = finish_router_writer_plan(RouterWriterShape::PartitionDrift)
        .expect_err("an empty route partition must freeze one singleton edge");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("partition values differ from its exact edge distribution")
    }));
}

#[test]
fn independent_router_source_rejects_a_drifted_destination_writer_proof() {
    let plan = finish_router_writer_plan(RouterWriterShape::Valid).unwrap();
    let source = plan.fragments().get(&FragmentId::new(721)).unwrap();
    let mut cuts = fragment_cuts(&plan, source.id()).unwrap();
    cuts.outbound[0]
        .change_stream_writer
        .as_mut()
        .unwrap()
        .write_target_ordinal = write_target_ordinal(1);
    let errors = validate_fragment(source, &cuts)
        .expect_err("the source fragment must recheck its peer writer proof");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("outbound cut lacks its exact destination writer proof")
    }));
}

#[test]
fn independent_router_writer_rejects_a_drifted_source_route_proof() {
    let plan = finish_router_writer_plan(RouterWriterShape::Valid).unwrap();
    let writer = plan.fragments().get(&FragmentId::new(722)).unwrap();
    let mut cuts = fragment_cuts(&plan, writer.id()).unwrap();
    cuts.inbound[0]
        .change_stream_writer
        .as_mut()
        .unwrap()
        .fields[0]
        .token = ConnectorWriteFieldToken::from_bytes([33; 32]);
    let errors = validate_fragment(writer, &cuts)
        .expect_err("the writer fragment must recheck its source route proof");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("inbound cut proof differs from its exact table writer contract")
    }));
}

#[test]
fn independent_writer_rejects_a_drifted_writer_result_proof() {
    let plan = finish_router_writer_plan(RouterWriterShape::Valid).unwrap();
    let writer = plan.fragments().get(&FragmentId::new(722)).unwrap();
    let mut cuts = fragment_cuts(&plan, writer.id()).unwrap();
    cuts.outbound[0].writer_result.as_mut().unwrap().fields[0].name = "wrong_kind".into();
    let errors = validate_fragment(writer, &cuts)
        .expect_err("the writer fragment must recheck its writer result proof");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("outbound writer result proof differs from its exact table writer schema")
    }));
}

#[test]
fn independent_finish_rejects_a_drifted_writer_result_proof() {
    let plan = finish_router_writer_plan(RouterWriterShape::Valid).unwrap();
    let finish = plan.fragments().get(&FragmentId::new(723)).unwrap();
    let mut cuts = fragment_cuts(&plan, finish.id()).unwrap();
    cuts.inbound[0]
        .writer_result
        .as_mut()
        .unwrap()
        .write_target_ordinal = write_target_ordinal(1);
    let errors = validate_fragment(finish, &cuts)
        .expect_err("the finish fragment must recheck the exact upstream writer target");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("expected targets differ from its fragment cut writer proofs")
    }));
}

#[test]
fn router_data_inputs_exclude_the_generated_effect_value() {
    let edge = EdgeId::new(902);
    let (builder, root, effect, _) = change_event_root();
    let errors = builder
        .finish_definition(
            root,
            FragmentSink::Router {
                effect,
                routes: Box::from([route(
                    write_route_id(10),
                    write_target_ordinal(0),
                    edge,
                    effect,
                )]),
            },
            dop(),
        )
        .expect_err("the effect discriminator cannot enter provider data fields");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("data input cannot contain its generated effect value")
    }));
}

#[test]
fn router_allows_two_target_fields_to_read_one_value_identity() {
    let edge = EdgeId::new(903);
    let (builder, root, effect, input) = change_event_root();
    builder
        .finish_definition(
            root,
            FragmentSink::Router {
                effect,
                routes: Box::from([ChangeStreamRoute {
                    route_id: write_route_id(11),
                    write_target_ordinal: write_target_ordinal(0),
                    accepted_effects: Box::from([ConnectorRowMutationEffect::Insert]),
                    input_mapping: Box::from([
                        (ConnectorWriteFieldToken::from_bytes([12; 32]), input),
                        (ConnectorWriteFieldToken::from_bytes([13; 32]), input),
                    ]),
                    partition_by: Box::default(),
                    edge,
                }]),
            },
            dop(),
        )
        .expect("output occurrences may reuse one semantic value identity");
}

#[test]
fn independent_fragment_rejects_duplicate_multicast_destinations() {
    let (builder, root, _, _) = literal_root();
    let errors = builder
        .finish_definition(
            root,
            FragmentSink::Multicast {
                edges: Box::from([EdgeId::new(721), EdgeId::new(721)]),
            },
            dop(),
        )
        .expect_err("one edge cannot occur twice in a multicast sink");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("sink contains a duplicate edge destination")
    }));
}

#[test]
fn independent_fragment_rejects_invalid_router_identity_and_layout() {
    let (builder, root, _, input) = literal_root();
    let errors = builder
        .finish_definition(
            root,
            FragmentSink::Router {
                effect: ValueId::new(999_721),
                routes: Box::from([ChangeStreamRoute {
                    route_id: write_route_id(0),
                    write_target_ordinal: write_target_ordinal(1),
                    accepted_effects: Box::default(),
                    input_mapping: Box::default(),
                    partition_by: Box::from([input]),
                    edge: EdgeId::new(721),
                }]),
            },
            dop(),
        )
        .expect_err("a router must be complete before fragment publication");
    let messages = errors
        .errors()
        .iter()
        .map(|error| error.message())
        .collect::<Vec<_>>();
    assert!(
        messages
            .iter()
            .any(|message| message.contains("router effect is absent"))
    );
    assert!(
        messages
            .iter()
            .any(|message| message.contains("unique identities and dense target ordinals"))
    );
    assert!(
        messages
            .iter()
            .any(|message| message.contains("unique accepted effects"))
    );
    assert!(
        messages
            .iter()
            .any(|message| message.contains("unique input tokens and root-output values"))
    );
    assert!(
        messages
            .iter()
            .any(|message| message.contains("partition values must be unique route inputs"))
    );
}

#[test]
fn independent_fragment_rejects_duplicate_router_identity() {
    let (builder, root, effect, input) = change_event_root();
    let errors = builder
        .finish_definition(
            root,
            FragmentSink::Router {
                effect,
                routes: Box::from([
                    route(
                        write_route_id(7),
                        write_target_ordinal(0),
                        EdgeId::new(721),
                        input,
                    ),
                    route(
                        write_route_id(7),
                        write_target_ordinal(1),
                        EdgeId::new(722),
                        input,
                    ),
                ]),
            },
            dop(),
        )
        .expect_err("route identities must be unique");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("unique identities and dense target ordinals")
    }));
}

#[test]
fn independent_fragment_rejects_router_cut_projection_drift() {
    let (builder, root, effect, input) = change_event_root();
    let edge = EdgeId::new(721);
    let fragment = builder
        .finish_definition(
            root,
            FragmentSink::Router {
                effect,
                routes: Box::from([route(
                    write_route_id(7),
                    write_target_ordinal(0),
                    edge,
                    input,
                )]),
            },
            dop(),
        )
        .expect("router fragment is locally complete");
    let destination = ValueId::new(30_721);
    let cuts = FragmentCuts {
        inbound: Box::default(),
        outbound: Box::from([OutboundFragmentCut {
            edge,
            kind: EdgeKind::ChangeStreamRouter,
            destination_fragment: FragmentId::new(722),
            projection: Box::from([CutValue {
                value: effect,
                ty: ty(DataType::Int8, false),
            }]),
            destination_imports: Box::from([CutImport {
                source: CutValue {
                    value: effect,
                    ty: ty(DataType::Int8, false),
                },
                destination,
            }]),
            partitioning: EdgePartitioning {
                source: Distribution::Unconstrained,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: Distribution::Unconstrained,
                destination_multiplicity: RowMultiplicity::SingleCopy,
            },
            change_stream_writer: None,
            writer_result: None,
            source_bindings: Box::default(),
            has_source_free_rows: false,
        }]),
        ..FragmentCuts::default()
    };

    let errors = validate_fragment(&fragment, &cuts)
        .expect_err("the edge projection must match the route input sequence");
    assert!(errors.errors().iter().any(|error| {
        error
            .message()
            .contains("router edge projection differs from its exact route input sequence")
    }));
}
