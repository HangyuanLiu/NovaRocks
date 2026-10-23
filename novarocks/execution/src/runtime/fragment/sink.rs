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

use super::{ExecutionFailure, sink_failure};
use crate::exec::fragment::sink::DataStreamSinkFactoryInput;
use crate::exec::operators::{CompiledPartitionKeys, DataStreamSinkFactory, NoopSinkFactory};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::error::{
    FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::instance::FragmentSinkAssignment;
use crate::runtime::fragment::io::ExchangeFrameTransmitter;
use crate::runtime::fragment::io::exchange_edge::ExchangeEdgeGates;
use novarocks_local_program::{StaticSinkProgram, StaticStreamBranch};

/// Materialize the root sink of one compiled LocalProgram (local-compiler
/// output).
///
/// Partition keys are frozen compiled roots. Task-local destinations and
/// outbound edge gates are bound without constructing an expression arena.
pub(crate) fn materialize_compiled_sink(
    program: &std::sync::Arc<novarocks_local_program::LocalProgram>,
    assignment: &FragmentSinkAssignment,
    fragment_instance_id: novarocks_types::UniqueId,
    transmitter: std::sync::Arc<dyn ExchangeFrameTransmitter>,
    result_sink: Option<Box<dyn OperatorFactory>>,
    edge_gates: Option<std::sync::Arc<ExchangeEdgeGates>>,
) -> Result<Box<dyn OperatorFactory>, FragmentLaunchError> {
    let graph = program.graph();
    let sink = graph
        .sink()
        .ok_or_else(|| materialization_contract("compiled local program has no static sink"))?;
    if !matches!(
        sink,
        StaticSinkProgram::Result | StaticSinkProgram::RootResult(_)
    ) && result_sink.is_some()
    {
        return Err(materialization_contract(format!(
            "compiled {} sink cannot take a result sink",
            sink_program_name(sink)
        )));
    }
    match (sink, assignment) {
        (
            StaticSinkProgram::Result | StaticSinkProgram::RootResult(_),
            FragmentSinkAssignment::None,
        ) => result_sink
            .ok_or_else(|| materialization_contract("compiled RESULT_SINK requires a result sink")),
        (StaticSinkProgram::Noop, FragmentSinkAssignment::None) => {
            Ok(Box::new(NoopSinkFactory::new()))
        }
        (
            StaticSinkProgram::DataStream { branch, .. },
            FragmentSinkAssignment::StreamDestinations {
                destinations,
                sender_id,
            },
        ) => {
            let partition_keys = compiled_partition_keys(program, branch)?;
            if let Some(limit) = branch.limit() {
                return Err(materialization_contract(format!(
                    "compiled stream sink limit {limit} is not executable yet"
                )));
            }
            let plan_node_id =
                sink_failure::root_identity(graph.root().index()).map_err(materialization_error)?;
            // Key roots remain in their LocalProgram; only Task-local routing
            // and output columns are bound here.
            let input = DataStreamSinkFactoryInput::try_from_static_program(
                branch.dest_node_id(),
                branch.partition_type(),
                branch.contract().delivery,
                branch.output_columns().to_vec(),
                destinations.clone(),
            )
            .map_err(materialization_error)?;
            let factory = DataStreamSinkFactory::new(
                input,
                fragment_instance_id,
                *sender_id,
                plan_node_id,
                transmitter,
            );
            let factory = match partition_keys {
                Some(keys) => factory
                    .with_compiled_partition_keys(keys)
                    .map_err(materialization_error)?,
                None => factory,
            };
            // Without the gates a push sink sends the moment it has rows, and
            // the frozen edge's closed state means nothing.
            let factory = match edge_gates {
                Some(gates) => factory.with_edge_gates(gates),
                None => factory,
            };
            Ok(Box::new(factory))
        }
        (
            StaticSinkProgram::MultiCastDataStream { .. }
            | StaticSinkProgram::SplitDataStream { .. }
            | StaticSinkProgram::ChangeStreamRouter { .. },
            _,
        ) => Err(materialization_contract(format!(
            "compiled {} sink not executable yet",
            sink_program_name(sink)
        ))),
        (static_program, dynamic_assignment) => Err(materialization_contract(format!(
            "compiled sink {} cannot be materialized with assignment {}",
            sink_program_name(static_program),
            sink_assignment_name(dynamic_assignment)
        ))),
    }
}

/// The compiled partition-key roots of the one DataStream branch, in key
/// order. A partitioning that evaluates no key must carry none.
fn compiled_partition_keys(
    program: &std::sync::Arc<novarocks_local_program::LocalProgram>,
    branch: &StaticStreamBranch,
) -> Result<Option<CompiledPartitionKeys>, FragmentLaunchError> {
    let keys = branch.partition_exprs().len();
    if !branch.partition_type().requires_exprs() {
        if keys != 0 {
            return Err(materialization_contract(format!(
                "compiled {} stream sink cannot carry partition keys",
                branch.partition_type().display_name()
            )));
        }
        return Ok(None);
    }
    if keys == 0 {
        return Err(materialization_contract(format!(
            "compiled {} stream sink has no partition key",
            branch.partition_type().display_name()
        )));
    }
    let mut sites = Vec::with_capacity(keys);
    for key in 0..keys {
        let key = sink_failure::key_identity(key).map_err(materialization_error)?;
        sites.push(
            novarocks_local_program::ProgramExpressionRootSite::SinkPartition { branch: 0, key },
        );
    }
    Ok(Some(CompiledPartitionKeys::new(
        std::sync::Arc::clone(program),
        sites,
    )))
}
fn materialization_error(source: impl Into<ExecutionFailure>) -> FragmentLaunchError {
    FragmentLaunchError::from_failure(
        FragmentLaunchStage::Materialize,
        FragmentLaunchErrorKind::Materialization,
        source.into(),
    )
}

fn materialization_contract(detail: impl std::fmt::Display) -> FragmentLaunchError {
    materialization_error(sink_failure::contract(detail))
}

fn sink_program_name(program: &StaticSinkProgram) -> &'static str {
    match program {
        StaticSinkProgram::Result => "result",
        StaticSinkProgram::RootResult(_) => "root_result",
        StaticSinkProgram::Noop => "noop",
        StaticSinkProgram::DataStream { .. } => "data_stream",
        StaticSinkProgram::MultiCastDataStream { .. } => "multi_cast_data_stream",
        StaticSinkProgram::SplitDataStream { .. } => "split_data_stream",
        StaticSinkProgram::ChangeStreamRouter { .. } => "change_stream_router",
    }
}

fn sink_assignment_name(assignment: &FragmentSinkAssignment) -> &'static str {
    match assignment {
        FragmentSinkAssignment::None => "none",
        FragmentSinkAssignment::StreamDestinations { .. } => "stream_destinations",
        FragmentSinkAssignment::DestinationGroups { .. } => "destination_groups",
    }
}
