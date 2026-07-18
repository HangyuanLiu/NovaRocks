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

use crate::exec::fragment::program::FragmentSinkSpec;
use crate::exec::fragment::sink::{
    DataStreamSinkBranchProgram, FragmentSinkProgram, IcebergChangeStreamRouterProgram,
    MultiCastDataStreamSinkProgram,
};
use crate::exec::operators::{
    DataStreamSinkFactory, DataStreamSinkFactoryInput, IcebergChangeStreamRouterBranchFactoryInput,
    IcebergChangeStreamRouterSinkFactory, IcebergChangeStreamRouterSinkFactoryInput,
    IcebergTableSinkFactory, MultiCastDataStreamSinkFactory, NoopSinkFactory,
    ResultBufferSinkFactory,
};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::endpoint::FragmentDestination;
use crate::runtime::fragment::error::{
    FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::instance::{FragmentInstanceSpec, FragmentSinkAssignment};
use crate::service::result_batch_wire::ResultSinkConfig;

pub(crate) fn materialize_fragment_sink(
    program: &FragmentSinkSpec,
    instance: &FragmentInstanceSpec,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    materialize_fragment_sink_components(
        program,
        instance.sink_assignment(),
        instance.fragment_instance_id().get(),
        instance.runtime_options().typed_result_sink(),
        -1,
    )
}

pub(crate) fn materialize_fragment_sink_components(
    program: &FragmentSinkSpec,
    assignment: &FragmentSinkAssignment,
    fragment_instance_id: crate::common::types::UniqueId,
    typed_result_sink: bool,
    plan_node_id: i32,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    match (program.program(), assignment) {
        (FragmentSinkProgram::Result, FragmentSinkAssignment::None) => Ok(Box::new(
            ResultBufferSinkFactory::new(None, ResultSinkConfig::mysql(), None, typed_result_sink),
        )),
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
        ),
        (FragmentSinkProgram::IcebergTable(table), FragmentSinkAssignment::None) => {
            IcebergTableSinkFactory::try_new(table.factory_input())
                .map(|factory| Box::new(factory) as Box<dyn OperatorFactory>)
                .map_err(materialization_error)
        }
        (
            FragmentSinkProgram::IcebergChangeStreamRouter(router),
            FragmentSinkAssignment::DestinationGroups { groups, sender_id },
        ) => materialize_router(
            router,
            groups,
            fragment_instance_id,
            *sender_id,
            plan_node_id,
        ),
        (static_program, dynamic_assignment) => Err(materialization_error(format!(
            "sink {} cannot be materialized with assignment {}",
            sink_program_name(static_program),
            sink_assignment_name(dynamic_assignment)
        ))),
    }
}

fn materialize_multicast(
    program: &MultiCastDataStreamSinkProgram,
    groups: &[Vec<FragmentDestination>],
    fragment_instance_id: crate::common::types::UniqueId,
    sender_id: Option<i32>,
    plan_node_id: i32,
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
    )))
}

fn materialize_router(
    program: &IcebergChangeStreamRouterProgram,
    groups: &[Vec<FragmentDestination>],
    fragment_instance_id: crate::common::types::UniqueId,
    sender_id: Option<i32>,
    plan_node_id: i32,
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
        FragmentSinkProgram::IcebergTable(_) => "iceberg_table",
        FragmentSinkProgram::IcebergChangeStreamRouter(_) => "iceberg_change_stream_router",
    }
}

fn sink_assignment_name(assignment: &FragmentSinkAssignment) -> &'static str {
    match assignment {
        FragmentSinkAssignment::None => "none",
        FragmentSinkAssignment::StreamDestinations { .. } => "stream_destinations",
        FragmentSinkAssignment::DestinationGroups { .. } => "destination_groups",
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::common::ids::SlotId;
    use crate::common::types::UniqueId;
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::FragmentSinkSpec;
    use crate::exec::fragment::sink::{
        DataStreamSinkBranchProgram, DataStreamSinkProgram, FragmentSinkProgram,
        MultiCastDataStreamSinkProgram,
    };
    use crate::exec::operators::DataStreamPartitionType;
    use crate::runtime::fragment::error::{FragmentLaunchErrorKind, FragmentLaunchStage};
    use crate::runtime::fragment::instance::{
        BackendNum, ExchangeInputAssignments, FragmentInstanceId, FragmentInstanceSpec,
        FragmentRuntimeOptions, FragmentSinkAssignment, ScanAssignments,
    };
    use crate::runtime::query_context::QueryId;
    use crate::runtime::query_options::QueryOptions;
    use crate::runtime::runtime_filter_params::RuntimeFilterParams;

    use super::{materialize_fragment_sink, materialize_fragment_sink_components};

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
        FragmentInstanceSpec::new(
            crate::exec::fragment::program::FragmentContractVersion::CURRENT,
            QueryId { hi: 1, lo: 2 },
            FragmentInstanceId::new(UniqueId { hi: 3, lo: 4 }),
            ScanAssignments::default(),
            ExchangeInputAssignments::default(),
            sink_assignment,
            RuntimeFilterParams::default(),
            FragmentRuntimeOptions::new(QueryOptions::default(), None, false),
            NonZeroUsize::new(1).expect("non-zero DOP"),
            BackendNum::try_new(0).expect("backend number"),
        )
    }

    #[test]
    fn data_stream_materialization_requires_stream_destinations() {
        let sink = FragmentSinkSpec::try_new(FragmentSinkProgram::DataStream(stream_program()))
            .expect("data stream sink");

        let error = match materialize_fragment_sink(&sink, &instance(FragmentSinkAssignment::None))
        {
            Ok(_) => panic!("missing stream destinations must fail"),
            Err(error) => error,
        };

        assert_eq!(error.stage(), FragmentLaunchStage::Materialize);
        assert_eq!(error.kind(), FragmentLaunchErrorKind::Materialization);
    }

    #[test]
    fn grouped_materialization_rejects_mismatched_destination_count() {
        let sink = FragmentSinkSpec::try_new(FragmentSinkProgram::MultiCastDataStream(
            MultiCastDataStreamSinkProgram::try_new(
                vec![stream_branch(17), stream_branch(18)],
                ExprArena::default(),
            )
            .expect("multicast program"),
        ))
        .expect("two-branch multicast sink");
        let assignment = FragmentSinkAssignment::DestinationGroups {
            groups: vec![Vec::new()],
            sender_id: None,
        };

        let error = match materialize_fragment_sink(&sink, &instance(assignment)) {
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
        )
        .expect("result sink materialization");

        assert_eq!(factory.name(), "RESULT_BUFFER_SINK (plan_node_id=-1)");
    }
}
