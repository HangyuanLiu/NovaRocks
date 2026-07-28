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

#[cfg(feature = "compat")]
use crate::connector::starrocks::sink::plan::{
    FrontendAddress, StarRocksSinkDescriptor, StarRocksSinkFactoryInput, StarRocksTableSinkProgram,
};
use crate::exec::fragment::program::{FragmentProgram, FragmentSinkSpec};
use crate::exec::fragment::sink::{
    DataStreamSinkBranchProgram, FragmentSinkProgram, IcebergChangeStreamRouterProgram,
    MultiCastDataStreamSinkProgram,
};
#[cfg(feature = "compat")]
use crate::exec::operators::OlapTableSinkFactory;
use crate::exec::operators::{
    DataStreamSinkFactory, DataStreamSinkFactoryInput, IcebergChangeStreamRouterBranchFactoryInput,
    IcebergChangeStreamRouterSinkFactory, IcebergChangeStreamRouterSinkFactoryInput,
    IcebergTableSinkFactory, MultiCastDataStreamSinkFactory, NoopSinkFactory,
    ResultBufferSinkFactory, SplitDataStreamSinkFactory,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::endpoint::FragmentDestination;
use crate::runtime::fragment::error::{
    FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::instance::{FragmentInstanceSpec, FragmentSinkAssignment};
use crate::runtime::fragment::io::ExchangeFrameTransmitter;
use crate::service::result_batch_wire::{ResultProjection, ResultSinkConfig};

pub(crate) fn materialize_fragment_sink(
    program: &FragmentProgram,
    instance: &FragmentInstanceSpec,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    materialize_fragment_sink_components(
        program.sink(),
        instance.sink_assignment(),
        instance.fragment_instance_id().get(),
        instance.runtime_options().typed_result_sink(),
        program.root_plan_node_id().get(),
        transmitter,
    )
}

pub(crate) fn materialize_fragment_sink_components(
    program: &FragmentSinkSpec,
    assignment: &FragmentSinkAssignment,
    fragment_instance_id: crate::common::types::UniqueId,
    typed_result_sink: bool,
    plan_node_id: i32,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    materialize_fragment_sink_components_with_result(
        program,
        assignment,
        fragment_instance_id,
        typed_result_sink,
        plan_node_id,
        transmitter,
        None,
    )
}

pub(crate) fn materialize_fragment_sink_components_with_result(
    program: &FragmentSinkSpec,
    assignment: &FragmentSinkAssignment,
    fragment_instance_id: crate::common::types::UniqueId,
    typed_result_sink: bool,
    plan_node_id: i32,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_override: Option<(ResultSinkConfig, Option<Vec<ResultProjection>>)>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    match (program.program(), assignment) {
        (FragmentSinkProgram::Result, FragmentSinkAssignment::None) => {
            let (config, projections) =
                result_override.unwrap_or_else(|| (ResultSinkConfig::mysql(), None));
            Ok(Box::new(ResultBufferSinkFactory::new(
                projections,
                config,
                None,
                typed_result_sink,
            )))
        }
        (FragmentSinkProgram::Noop, FragmentSinkAssignment::None) => {
            Ok(Box::new(NoopSinkFactory::new()))
        }
        (
            FragmentSinkProgram::DataStream(stream),
            FragmentSinkAssignment::StreamDestinations {
                destinations,
                sender_id,
            },
        ) => {
            let input = stream_input(stream, destinations.clone())?;
            Ok(Box::new(DataStreamSinkFactory::new(
                input,
                fragment_instance_id,
                *sender_id,
                plan_node_id,
                stream.partition_arena().clone(),
                std::sync::Arc::clone(&transmitter),
            )))
        }
        (
            FragmentSinkProgram::MultiCastDataStream(grouped),
            FragmentSinkAssignment::DestinationGroups { groups, sender_id },
        ) => materialize_multicast(
            grouped,
            groups,
            fragment_instance_id,
            *sender_id,
            plan_node_id,
            std::sync::Arc::clone(&transmitter),
        ),
        (
            FragmentSinkProgram::SplitDataStream(split),
            FragmentSinkAssignment::DestinationGroups { groups, sender_id },
        ) => materialize_split(
            split,
            groups,
            fragment_instance_id,
            *sender_id,
            plan_node_id,
            std::sync::Arc::clone(&transmitter),
        ),
        (FragmentSinkProgram::IcebergTable(table), FragmentSinkAssignment::None) => {
            IcebergTableSinkFactory::try_new(table.factory_input())
                .map(|factory| Box::new(factory) as Box<dyn OperatorFactory>)
                .map_err(materialization_error)
        }
        #[cfg(feature = "compat")]
        (
            FragmentSinkProgram::StarRocksTable(table),
            FragmentSinkAssignment::StarRocksTable(assignment),
        ) => materialize_starrocks_table(table, assignment),
        (
            FragmentSinkProgram::IcebergChangeStreamRouter(router),
            FragmentSinkAssignment::DestinationGroups { groups, sender_id },
        ) => materialize_router(
            router,
            groups,
            fragment_instance_id,
            *sender_id,
            plan_node_id,
            std::sync::Arc::clone(&transmitter),
        ),
        (static_program, dynamic_assignment) => Err(materialization_error(format!(
            "sink {} cannot be materialized with assignment {}",
            sink_program_name(static_program),
            sink_assignment_name(dynamic_assignment)
        ))),
    }
}

#[cfg(feature = "compat")]
fn materialize_starrocks_table(
    program: &StarRocksTableSinkProgram,
    assignment: &crate::runtime::fragment::instance::StarRocksTableSinkAssignment,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    let input = starrocks_factory_input(program, assignment);
    OlapTableSinkFactory::try_new(input)
        .map(|factory| Box::new(factory) as Box<dyn OperatorFactory>)
        .map_err(materialization_error)
}

#[cfg(feature = "compat")]
fn starrocks_factory_input(
    program: &StarRocksTableSinkProgram,
    assignment: &crate::runtime::fragment::instance::StarRocksTableSinkAssignment,
) -> StarRocksSinkFactoryInput {
    let descriptor = &program.descriptor;
    StarRocksSinkFactoryInput {
        name: program.name.clone(),
        descriptor: StarRocksSinkDescriptor {
            db_id: descriptor.db_id,
            table_id: descriptor.table_id,
            db_name: descriptor.db_name.clone(),
            table_name: descriptor.table_name.clone(),
            txn_id: assignment.txn_id(),
            load_id: assignment.load_id(),
            keys_type: descriptor.keys_type,
            is_lake_table: descriptor.is_lake_table,
            dynamic_overwrite: descriptor.dynamic_overwrite,
            partial_update_mode: descriptor.partial_update_mode.clone(),
            merge_condition: descriptor.merge_condition.clone(),
            null_expr_in_auto_increment: descriptor.null_expr_in_auto_increment,
            miss_auto_increment_column: descriptor.miss_auto_increment_column,
            schema: descriptor.schema.clone(),
            partition: descriptor.partition.clone(),
            location: descriptor.location.clone(),
            nodes: descriptor.nodes.clone(),
            frontend: assignment.frontend().map(|endpoint| FrontendAddress {
                hostname: endpoint.host().to_string(),
                port: endpoint.port(),
            }),
        },
        output_projection: program.output_projection.clone(),
        output_expr_slot_name_map: program.output_expr_slot_name_map.clone(),
        output_expr_slot_ids: program.output_expr_slot_ids.clone(),
        literal_partition_values: program.literal_partition_values.clone(),
    }
}

fn materialize_multicast(
    program: &MultiCastDataStreamSinkProgram,
    groups: &[Vec<FragmentDestination>],
    fragment_instance_id: crate::common::types::UniqueId,
    sender_id: Option<i32>,
    plan_node_id: i32,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    ensure_group_count(program.sinks().len(), groups.len())?;
    let sinks = program
        .sinks()
        .iter()
        .zip(groups)
        .map(|(stream, destinations)| {
            Ok((branch_input(stream, destinations.clone())?, stream.limit()))
        })
        .collect::<Result<Vec<_>, FragmentLaunchError>>()?;
    Ok(Box::new(MultiCastDataStreamSinkFactory::new(
        sinks,
        fragment_instance_id,
        sender_id,
        program.partition_arena().clone(),
        plan_node_id,
        transmitter,
    )))
}

fn materialize_split(
    program: &crate::exec::fragment::sink::SplitDataStreamSinkProgram,
    groups: &[Vec<FragmentDestination>],
    fragment_instance_id: crate::common::types::UniqueId,
    sender_id: Option<i32>,
    plan_node_id: i32,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    let sinks = program
        .sinks()
        .iter()
        .zip(groups)
        .map(|(stream, destinations)| branch_input(stream, destinations.clone()))
        .collect::<Result<Vec<_>, FragmentLaunchError>>()?;
    Ok(Box::new(SplitDataStreamSinkFactory::new(
        sinks,
        fragment_instance_id,
        sender_id,
        program.arena().clone(),
        plan_node_id,
        std::sync::Arc::new(program.arena().clone()),
        program.split_exprs().to_vec(),
        transmitter,
    )))
}

fn materialize_router(
    program: &IcebergChangeStreamRouterProgram,
    groups: &[Vec<FragmentDestination>],
    fragment_instance_id: crate::common::types::UniqueId,
    sender_id: Option<i32>,
    plan_node_id: i32,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    ensure_group_count(program.branches().len(), groups.len())?;
    let branches = program
        .branches()
        .iter()
        .zip(groups)
        .map(|(branch, destinations)| {
            Ok(IcebergChangeStreamRouterBranchFactoryInput {
                branch_id: branch.branch_id(),
                branch_kind: branch.branch_kind(),
                stream_sink: branch_input(branch.stream_sink(), destinations.clone())?,
            })
        })
        .collect::<Result<Vec<_>, FragmentLaunchError>>()?;
    let input = IcebergChangeStreamRouterSinkFactoryInput {
        change_op_slot_id: slot_id_as_i32(program.change_op_slot_id())?,
        data_route_slot_id: program
            .data_route_slot_id()
            .map(slot_id_as_i32)
            .transpose()?,
        branches,
    };
    IcebergChangeStreamRouterSinkFactory::try_new(
        input,
        fragment_instance_id,
        sender_id,
        program.partition_arena().clone(),
        plan_node_id,
        transmitter,
    )
    .map(|factory| Box::new(factory) as Box<dyn OperatorFactory>)
    .map_err(materialization_error)
}

fn stream_input(
    program: &crate::exec::fragment::sink::DataStreamSinkProgram,
    destinations: Vec<FragmentDestination>,
) -> Result<DataStreamSinkFactoryInput, FragmentLaunchError> {
    DataStreamSinkFactoryInput::try_from_static_program(
        program.dest_node_id(),
        program.output_partition_type(),
        program.output_exprs().to_vec(),
        program.output_partition_exprs().to_vec(),
        program.output_columns().to_vec(),
        destinations,
    )
    .map_err(materialization_error)
}

fn branch_input(
    program: &DataStreamSinkBranchProgram,
    destinations: Vec<FragmentDestination>,
) -> Result<DataStreamSinkFactoryInput, FragmentLaunchError> {
    DataStreamSinkFactoryInput::try_from_static_program(
        program.dest_node_id(),
        program.output_partition_type(),
        program.output_exprs().to_vec(),
        program.output_partition_exprs().to_vec(),
        program.output_columns().to_vec(),
        destinations,
    )
    .map_err(materialization_error)
}

fn ensure_group_count(expected: usize, actual: usize) -> Result<(), FragmentLaunchError> {
    if expected != actual {
        return Err(materialization_error(format!(
            "expected {expected} destination groups, got {actual}"
        )));
    }
    Ok(())
}

fn slot_id_as_i32(slot_id: crate::common::ids::SlotId) -> Result<i32, FragmentLaunchError> {
    i32::try_from(slot_id.as_u32()).map_err(|_| {
        materialization_error(format!(
            "router slot id {} exceeds operator representation",
            slot_id.as_u32()
        ))
    })
}

fn materialization_error(detail: impl Into<String>) -> FragmentLaunchError {
    FragmentLaunchError::new(
        FragmentLaunchStage::Materialize,
        FragmentLaunchErrorKind::Materialization,
        detail,
    )
}

fn sink_program_name(program: &FragmentSinkProgram) -> &'static str {
    match program {
        FragmentSinkProgram::Result => "result",
        FragmentSinkProgram::Noop => "noop",
        FragmentSinkProgram::DataStream(_) => "data_stream",
        FragmentSinkProgram::MultiCastDataStream(_) => "multi_cast_data_stream",
        FragmentSinkProgram::SplitDataStream(_) => "split_data_stream",
        #[cfg(feature = "compat")]
        FragmentSinkProgram::StarRocksTable(_) => "starrocks_table",
        FragmentSinkProgram::IcebergTable(_) => "iceberg_table",
        FragmentSinkProgram::IcebergChangeStreamRouter(_) => "iceberg_change_stream_router",
    }
}

fn sink_assignment_name(assignment: &FragmentSinkAssignment) -> &'static str {
    match assignment {
        FragmentSinkAssignment::None => "none",
        FragmentSinkAssignment::StreamDestinations { .. } => "stream_destinations",
        FragmentSinkAssignment::DestinationGroups { .. } => "destination_groups",
        FragmentSinkAssignment::StarRocksTable(_) => "starrocks_table",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::num::NonZeroUsize;

    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    #[cfg(feature = "compat")]
    use crate::connector::starrocks::lake::context::PartialUpdateWriteMode;
    #[cfg(feature = "compat")]
    use crate::connector::starrocks::schema::{
        StarRocksColumnSchema, StarRocksKeysType, StarRocksTabletSchema,
    };
    #[cfg(feature = "compat")]
    use crate::connector::starrocks::sink::plan::{
        SinkIndexDescriptor, SinkLocationDescriptor, SinkNodesDescriptor, SinkPartitionDescriptor,
        SinkSchemaDescriptor, StarRocksTableSinkDescriptor, StarRocksTableSinkProgram,
    };
    use crate::exec::chunk::Chunk;
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::{
        FragmentContractVersion, FragmentProgram, FragmentProgramOptions, FragmentSinkSpec,
        RuntimeFilterContract,
    };
    use crate::exec::fragment::sink::{
        DataStreamSinkBranchProgram, DataStreamSinkProgram, FragmentSinkProgram,
        IcebergChangeStreamRouterBranchProgram, IcebergChangeStreamRouterProgram,
        MultiCastDataStreamSinkProgram, SplitDataStreamSinkProgram,
    };
    use crate::exec::node::values::ValuesNode;
    use crate::exec::node::{ExecNode, ExecNodeKind, ExecPlan};
    use crate::exec::operators::DataStreamPartitionType;
    #[cfg(feature = "compat")]
    use crate::runtime::endpoint::RuntimeEndpoint;
    use crate::runtime::fragment::error::{FragmentLaunchErrorKind, FragmentLaunchStage};
    #[cfg(feature = "compat")]
    use crate::runtime::fragment::instance::StarRocksTableSinkAssignment;
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
        FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::fragment::io::ExchangeFrameTransmitter;
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;
    use crate::sql::common::ChangeStreamBranchKind;

    #[cfg(feature = "compat")]
    use super::starrocks_factory_input;
    use super::{materialize_fragment_sink, materialize_fragment_sink_components};

    fn test_transmitter() -> std::sync::Arc<dyn ExchangeFrameTransmitter> {
        crate::runtime::fragment::io::exchange::discard_exchange_transmitter()
    }

    fn stream_program() -> DataStreamSinkProgram {
        DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::Unpartitioned,
            Vec::new(),
            vec![SlotId::new(3)],
            None,
            ExprArena::default(),
        )
        .expect("stream program")
    }

    fn stream_branch(dest_node_id: i32) -> DataStreamSinkBranchProgram {
        DataStreamSinkBranchProgram::try_new(
            dest_node_id,
            Vec::new(),
            DataStreamPartitionType::Unpartitioned,
            Vec::new(),
            vec![SlotId::new(3)],
            None,
        )
        .expect("stream branch")
    }

    fn instance(sink_assignment: FragmentSinkAssignment) -> FragmentInstanceSpec {
        FragmentInstanceSpec::new_native(
            crate::exec::fragment::program::FragmentContractVersion::CURRENT,
            QueryId { hi: 1, lo: 2 },
            FragmentInstanceId::new(UniqueId { hi: 3, lo: 4 }),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            sink_assignment,
            FragmentRuntimeOptions::new(QueryOptions::default(), None, false),
            NonZeroUsize::new(1).expect("non-zero DOP"),
            BackendNum::try_new(0).expect("backend number"),
        )
    }

    fn fragment_program(sink: FragmentSinkProgram) -> FragmentProgram {
        FragmentProgram::new(
            ExecPlan {
                arena: ExprArena::default(),
                root: ExecNode {
                    kind: ExecNodeKind::Values(ValuesNode {
                        chunk: Chunk::default(),
                        node_id: 99,
                    }),
                },
            },
            FragmentSinkSpec::try_new(sink).expect("fragment sink"),
            FragmentProgramOptions::new(FragmentContractVersion::CURRENT),
            BTreeMap::new(),
            BTreeMap::new(),
            RuntimeFilterContract::default(),
        )
    }

    #[cfg(feature = "compat")]
    fn starrocks_table_program() -> StarRocksTableSinkProgram {
        let tablet_schema = StarRocksTabletSchema::try_new(
            Some(10),
            Some(StarRocksKeysType::Primary),
            vec![StarRocksColumnSchema {
                unique_id: 1,
                name: Some("k".to_string()),
                r#type: "BIGINT".to_string(),
                is_key: Some(true),
                is_nullable: Some(false),
                ..StarRocksColumnSchema::default()
            }],
        )
        .expect("tablet schema");
        StarRocksTableSinkProgram {
            name: "OLAP_TABLE_SINK".to_string(),
            descriptor: StarRocksTableSinkDescriptor {
                db_id: 1,
                table_id: 2,
                db_name: Some("db".to_string()),
                table_name: Some("tbl".to_string()),
                keys_type: StarRocksKeysType::Primary,
                is_lake_table: true,
                dynamic_overwrite: false,
                partial_update_mode: PartialUpdateWriteMode::Row,
                merge_condition: None,
                null_expr_in_auto_increment: false,
                miss_auto_increment_column: false,
                schema: SinkSchemaDescriptor {
                    slot_descs: Vec::new(),
                    indexes: vec![SinkIndexDescriptor {
                        index_id: 10,
                        schema_id: 10,
                        column_names: vec!["k".to_string()],
                        tablet_schema,
                        column_to_expr_value: HashMap::new(),
                        is_shadow: false,
                        where_clause: None,
                    }],
                },
                partition: SinkPartitionDescriptor {
                    enable_automatic_partition: false,
                    partition_columns: Vec::new(),
                    distributed_columns: Vec::new(),
                    partition_exprs: None,
                    partitions: Vec::new(),
                },
                location: SinkLocationDescriptor {
                    tablets: Vec::new(),
                },
                nodes: SinkNodesDescriptor { nodes: Vec::new() },
            },
            output_projection: None,
            output_expr_slot_name_map: HashMap::new(),
            output_expr_slot_ids: Vec::new(),
            literal_partition_values: None,
        }
    }

    fn assert_factory_and_operator_name(
        factory: &dyn crate::exec::pipeline::operator_factory::OperatorFactory,
        expected: &str,
    ) {
        assert_eq!(factory.name(), expected);
        assert_eq!(factory.create(1, 0).name(), expected);
    }

    #[test]
    fn data_stream_materialization_uses_fragment_root_plan_node_id() {
        let program = fragment_program(FragmentSinkProgram::DataStream(stream_program()));
        let instance = instance(FragmentSinkAssignment::StreamDestinations {
            destinations: Vec::new(),
            sender_id: None,
        });

        let factory = materialize_fragment_sink(&program, &instance, test_transmitter())
            .expect("data stream sink");

        assert_factory_and_operator_name(factory.as_ref(), "EXCHANGE_SINK (id=99)");
    }

    #[test]
    fn multicast_materialization_uses_fragment_root_plan_node_id() {
        let program = fragment_program(FragmentSinkProgram::MultiCastDataStream(
            MultiCastDataStreamSinkProgram::try_new(vec![stream_branch(17)], ExprArena::default())
                .expect("multicast program"),
        ));
        let instance = instance(FragmentSinkAssignment::DestinationGroups {
            groups: vec![Vec::new()],
            sender_id: None,
        });

        let factory = materialize_fragment_sink(&program, &instance, test_transmitter())
            .expect("multicast sink");

        assert_factory_and_operator_name(factory.as_ref(), "MULTI_CAST_DATA_STREAM_SINK (id=99)");
    }

    #[test]
    fn split_materialization_is_owned_by_fragment_sink_materializer() {
        let mut arena = ExprArena::default();
        let split_expr = arena.push_typed(
            crate::exec::expr::ExprNode::Literal(crate::exec::expr::LiteralValue::Bool(true)),
            arrow::datatypes::DataType::Boolean,
        );
        let program = fragment_program(FragmentSinkProgram::SplitDataStream(
            SplitDataStreamSinkProgram::try_new(vec![stream_branch(17)], vec![split_expr], arena)
                .expect("split program"),
        ));
        let instance = instance(FragmentSinkAssignment::DestinationGroups {
            groups: vec![Vec::new()],
            sender_id: None,
        });

        let factory =
            materialize_fragment_sink(&program, &instance, test_transmitter()).expect("split sink");

        assert_factory_and_operator_name(factory.as_ref(), "SPLIT_DATA_STREAM_SINK (id=99)");
    }

    #[cfg(feature = "compat")]
    #[test]
    fn starrocks_materializer_assembles_dynamic_identity_only_at_runtime() {
        let program = starrocks_table_program();
        let frontend = RuntimeEndpoint::new("frontend", 9020).expect("frontend");
        let assignment =
            StarRocksTableSinkAssignment::new(97, UniqueId { hi: 101, lo: 103 }, Some(frontend));

        let input = starrocks_factory_input(&program, &assignment);

        assert_eq!(input.name, "OLAP_TABLE_SINK");
        assert_eq!((input.descriptor.db_id, input.descriptor.table_id), (1, 2));
        assert_eq!(input.descriptor.txn_id, 97);
        assert_eq!(
            (input.descriptor.load_id.hi, input.descriptor.load_id.lo),
            (101, 103)
        );
        let frontend = input.descriptor.frontend.expect("frontend address");
        assert_eq!(
            (frontend.hostname.as_str(), frontend.port),
            ("frontend", 9020)
        );
    }

    #[test]
    fn iceberg_router_materialization_uses_fragment_root_plan_node_id() {
        let program = fragment_program(FragmentSinkProgram::IcebergChangeStreamRouter(
            IcebergChangeStreamRouterProgram::try_new(
                SlotId::new(1),
                None,
                vec![IcebergChangeStreamRouterBranchProgram::new(
                    7,
                    ChangeStreamBranchKind::DeleteDv,
                    stream_branch(17),
                )],
                ExprArena::default(),
            )
            .expect("router program"),
        ));
        let instance = instance(FragmentSinkAssignment::DestinationGroups {
            groups: vec![Vec::new()],
            sender_id: None,
        });

        let factory = materialize_fragment_sink(&program, &instance, test_transmitter())
            .expect("router sink");

        assert_factory_and_operator_name(
            factory.as_ref(),
            "ICEBERG_CHANGE_STREAM_ROUTER_SINK (id=99)",
        );
    }

    #[test]
    fn result_materialization_remains_anonymous_with_fragment_root_plan_node_id() {
        let program = fragment_program(FragmentSinkProgram::Result);
        let instance = instance(FragmentSinkAssignment::None);

        let factory = materialize_fragment_sink(&program, &instance, test_transmitter())
            .expect("result sink");

        assert_factory_and_operator_name(factory.as_ref(), "RESULT_BUFFER_SINK (plan_node_id=-1)");
    }

    #[test]
    fn data_stream_materialization_requires_stream_destinations() {
        let program = fragment_program(FragmentSinkProgram::DataStream(stream_program()));

        let error = match materialize_fragment_sink(
            &program,
            &instance(FragmentSinkAssignment::None),
            test_transmitter(),
        ) {
            Ok(_) => panic!("missing stream destinations must fail"),
            Err(error) => error,
        };

        assert_eq!(error.stage(), FragmentLaunchStage::Materialize);
        assert_eq!(error.kind(), FragmentLaunchErrorKind::Materialization);
    }

    #[test]
    fn grouped_materialization_rejects_mismatched_destination_count() {
        let program = fragment_program(FragmentSinkProgram::MultiCastDataStream(
            MultiCastDataStreamSinkProgram::try_new(
                vec![stream_branch(17), stream_branch(18)],
                ExprArena::default(),
            )
            .expect("multicast program"),
        ));
        let assignment = FragmentSinkAssignment::DestinationGroups {
            groups: vec![Vec::new()],
            sender_id: None,
        };

        let error =
            match materialize_fragment_sink(&program, &instance(assignment), test_transmitter()) {
                Ok(_) => panic!("one destination group must not be truncated against two branches"),
                Err(error) => error,
            };

        assert_eq!(error.stage(), FragmentLaunchStage::Materialize);
        assert_eq!(error.kind(), FragmentLaunchErrorKind::Materialization);
        assert!(error.detail().contains("expected 2 destination groups"));
    }

    #[test]
    fn result_materialization_keeps_bridge_plan_node_unset() {
        let sink = FragmentSinkSpec::try_new(FragmentSinkProgram::Result).expect("result sink");

        let factory = materialize_fragment_sink_components(
            &sink,
            &FragmentSinkAssignment::None,
            UniqueId { hi: 3, lo: 4 },
            false,
            47,
            test_transmitter(),
        )
        .expect("result sink materialization");

        assert_eq!(factory.name(), "RESULT_BUFFER_SINK (plan_node_id=-1)");
    }
}
