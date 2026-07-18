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

use crate::common::ids::SlotId;
use crate::connector::iceberg::sink_plan::{IcebergSinkFactoryInput, IcebergSinkPlan};
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::operators::DataStreamPartitionType;
use crate::sql::common::ChangeStreamBranchKind;

#[derive(Clone, Debug)]
pub(crate) enum FragmentSinkProgram {
    Result,
    Noop,
    DataStream(DataStreamSinkProgram),
    MultiCastDataStream(MultiCastDataStreamSinkProgram),
    IcebergTable(IcebergTableSinkProgram),
    IcebergChangeStreamRouter(IcebergChangeStreamRouterProgram),
}

#[derive(Clone, Debug)]
pub(crate) struct DataStreamSinkProgram {
    pub(crate) dest_node_id: i32,
    pub(crate) output_exprs: Vec<ExprId>,
    pub(crate) output_partition_type: DataStreamPartitionType,
    pub(crate) output_partition_exprs: Vec<ExprId>,
    pub(crate) output_columns: Vec<SlotId>,
    pub(crate) limit: Option<i64>,
    partition_arena: ExprArena,
}

impl DataStreamSinkProgram {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        dest_node_id: i32,
        output_exprs: Vec<ExprId>,
        output_partition_type: DataStreamPartitionType,
        output_partition_exprs: Vec<ExprId>,
        output_columns: Vec<SlotId>,
        limit: Option<i64>,
        partition_arena: ExprArena,
    ) -> Self {
        Self {
            dest_node_id,
            output_exprs,
            output_partition_type,
            output_partition_exprs,
            output_columns,
            limit,
            partition_arena,
        }
    }

    pub(crate) const fn partition_arena(&self) -> &ExprArena {
        &self.partition_arena
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DataStreamSinkBranchProgram {
    pub(crate) dest_node_id: i32,
    pub(crate) output_exprs: Vec<ExprId>,
    pub(crate) output_partition_type: DataStreamPartitionType,
    pub(crate) output_partition_exprs: Vec<ExprId>,
    pub(crate) output_columns: Vec<SlotId>,
    pub(crate) limit: Option<i64>,
}

impl DataStreamSinkBranchProgram {
    pub(crate) fn new(
        dest_node_id: i32,
        output_exprs: Vec<ExprId>,
        output_partition_type: DataStreamPartitionType,
        output_partition_exprs: Vec<ExprId>,
        output_columns: Vec<SlotId>,
        limit: Option<i64>,
    ) -> Self {
        Self {
            dest_node_id,
            output_exprs,
            output_partition_type,
            output_partition_exprs,
            output_columns,
            limit,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MultiCastDataStreamSinkProgram {
    pub(crate) sinks: Vec<DataStreamSinkBranchProgram>,
    partition_arena: ExprArena,
}

impl MultiCastDataStreamSinkProgram {
    pub(crate) fn new(sinks: Vec<DataStreamSinkBranchProgram>, partition_arena: ExprArena) -> Self {
        Self {
            sinks,
            partition_arena,
        }
    }

    pub(crate) const fn partition_arena(&self) -> &ExprArena {
        &self.partition_arena
    }
}

#[derive(Clone)]
pub(crate) struct IcebergTableSinkProgram {
    name: String,
    arena: ExprArena,
    plan: IcebergSinkPlan,
}

impl std::fmt::Debug for IcebergTableSinkProgram {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IcebergTableSinkProgram")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl IcebergTableSinkProgram {
    pub(crate) fn from_factory_input(input: IcebergSinkFactoryInput) -> Self {
        Self {
            name: input.name,
            arena: input.arena,
            plan: input.plan,
        }
    }

    pub(crate) fn factory_input(&self) -> IcebergSinkFactoryInput {
        IcebergSinkFactoryInput {
            name: self.name.clone(),
            arena: self.arena.clone(),
            plan: self.plan.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergChangeStreamRouterBranchProgram {
    pub(crate) branch_id: i32,
    pub(crate) branch_kind: ChangeStreamBranchKind,
    pub(crate) stream_sink: DataStreamSinkBranchProgram,
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergChangeStreamRouterProgram {
    pub(crate) change_op_slot_id: SlotId,
    pub(crate) data_route_slot_id: Option<SlotId>,
    pub(crate) branches: Vec<IcebergChangeStreamRouterBranchProgram>,
    partition_arena: ExprArena,
}

impl IcebergChangeStreamRouterProgram {
    pub(crate) fn new(
        change_op_slot_id: SlotId,
        data_route_slot_id: Option<SlotId>,
        branches: Vec<IcebergChangeStreamRouterBranchProgram>,
        partition_arena: ExprArena,
    ) -> Self {
        Self {
            change_op_slot_id,
            data_route_slot_id,
            branches,
            partition_arena,
        }
    }

    pub(crate) const fn partition_arena(&self) -> &ExprArena {
        &self.partition_arena
    }
}

#[cfg(test)]
mod tests {
    use crate::common::ids::SlotId;
    use crate::exec::expr::ExprArena;
    use crate::exec::fragment::program::{
        FragmentSinkAssignmentKind, FragmentSinkAssignmentRequirement, FragmentSinkSpec,
    };
    use crate::exec::operators::DataStreamPartitionType;

    use super::{DataStreamSinkProgram, FragmentSinkProgram};

    #[test]
    fn static_data_stream_program_contains_no_destinations() {
        let program = DataStreamSinkProgram::new(
            17,
            Vec::new(),
            DataStreamPartitionType::Unpartitioned,
            Vec::new(),
            vec![SlotId::new(3)],
            Some(9),
            ExprArena::default(),
        );

        assert_eq!(program.dest_node_id, 17);
        assert_eq!(program.output_columns, vec![SlotId::new(3)]);
        assert_eq!(program.limit, Some(9));
        assert!(
            program
                .partition_arena()
                .node(crate::exec::expr::ExprId(0))
                .is_none()
        );

        let spec = FragmentSinkSpec::try_new(FragmentSinkProgram::DataStream(program))
            .expect("static data stream sink");
        assert_eq!(
            spec.assignment_requirement(),
            FragmentSinkAssignmentRequirement::Required(
                FragmentSinkAssignmentKind::StreamDestinations
            )
        );
    }
}
