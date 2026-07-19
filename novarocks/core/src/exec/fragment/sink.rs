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

use std::collections::HashSet;

use crate::common::ids::SlotId;
use crate::connector::iceberg::sink_plan::{IcebergSinkFactoryInput, IcebergSinkPlan};
#[cfg(feature = "compat")]
use crate::connector::starrocks::sink::plan::StarRocksTableSinkProgram;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::fragment::error::{ExecPlanBuildError, ExecPlanInvariant};
use crate::exec::operators::DataStreamPartitionType;
use crate::sql::common::ChangeStreamBranchKind;

#[derive(Clone, Debug)]
pub(crate) enum FragmentSinkProgram {
    Result,
    Noop,
    DataStream(DataStreamSinkProgram),
    MultiCastDataStream(MultiCastDataStreamSinkProgram),
    SplitDataStream(SplitDataStreamSinkProgram),
    #[cfg(feature = "compat")]
    StarRocksTable(StarRocksTableSinkProgram),
    IcebergTable(IcebergTableSinkProgram),
    IcebergChangeStreamRouter(IcebergChangeStreamRouterProgram),
}

impl FragmentSinkProgram {
    pub(crate) fn validate(&self) -> Result<(), ExecPlanBuildError> {
        match self {
            Self::Result | Self::Noop => Ok(()),
            Self::DataStream(program) => program.validate(),
            Self::MultiCastDataStream(program) => program.validate(),
            Self::SplitDataStream(program) => program.validate(),
            #[cfg(feature = "compat")]
            Self::StarRocksTable(program) => program
                .validate()
                .map_err(|error| ExecPlanBuildError::new(ExecPlanInvariant::Sink, error)),
            Self::IcebergTable(program) => program.validate(),
            Self::IcebergChangeStreamRouter(program) => program.validate(),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DataStreamSinkProgram {
    dest_node_id: i32,
    output_exprs: Vec<ExprId>,
    output_partition_type: DataStreamPartitionType,
    output_partition_exprs: Vec<ExprId>,
    output_columns: Vec<SlotId>,
    limit: Option<i64>,
    partition_arena: ExprArena,
}

impl DataStreamSinkProgram {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        dest_node_id: i32,
        output_exprs: Vec<ExprId>,
        output_partition_type: DataStreamPartitionType,
        mut output_partition_exprs: Vec<ExprId>,
        output_columns: Vec<SlotId>,
        limit: Option<i64>,
        partition_arena: ExprArena,
    ) -> Result<Self, ExecPlanBuildError> {
        if !output_partition_type.requires_exprs() {
            output_partition_exprs.clear();
        }
        let program = Self {
            dest_node_id,
            output_exprs,
            output_partition_type,
            output_partition_exprs,
            output_columns,
            limit,
            partition_arena,
        };
        program.validate()?;
        Ok(program)
    }

    fn validate(&self) -> Result<(), ExecPlanBuildError> {
        validate_stream_shape(
            "DATA_STREAM_SINK",
            &self.output_exprs,
            self.output_partition_type,
            &self.output_partition_exprs,
            &self.output_columns,
        )?;
        validate_expr_ids(
            &self.partition_arena,
            &self.output_partition_exprs,
            "DATA_STREAM_SINK partition",
        )
    }

    pub(crate) const fn dest_node_id(&self) -> i32 {
        self.dest_node_id
    }

    pub(crate) fn output_exprs(&self) -> &[ExprId] {
        &self.output_exprs
    }

    pub(crate) const fn output_partition_type(&self) -> DataStreamPartitionType {
        self.output_partition_type
    }

    pub(crate) fn output_partition_exprs(&self) -> &[ExprId] {
        &self.output_partition_exprs
    }

    pub(crate) fn output_columns(&self) -> &[SlotId] {
        &self.output_columns
    }

    pub(crate) const fn partition_arena(&self) -> &ExprArena {
        &self.partition_arena
    }
}

#[derive(Clone, Debug)]
pub(crate) struct DataStreamSinkBranchProgram {
    dest_node_id: i32,
    output_exprs: Vec<ExprId>,
    output_partition_type: DataStreamPartitionType,
    output_partition_exprs: Vec<ExprId>,
    output_columns: Vec<SlotId>,
    limit: Option<i64>,
}

impl DataStreamSinkBranchProgram {
    pub(crate) fn try_new(
        dest_node_id: i32,
        output_exprs: Vec<ExprId>,
        output_partition_type: DataStreamPartitionType,
        mut output_partition_exprs: Vec<ExprId>,
        output_columns: Vec<SlotId>,
        limit: Option<i64>,
    ) -> Result<Self, ExecPlanBuildError> {
        if !output_partition_type.requires_exprs() {
            output_partition_exprs.clear();
        }
        let program = Self {
            dest_node_id,
            output_exprs,
            output_partition_type,
            output_partition_exprs,
            output_columns,
            limit,
        };
        program.validate_shape("grouped DATA_STREAM_SINK branch")?;
        Ok(program)
    }

    pub(crate) fn into_program(
        self,
        partition_arena: ExprArena,
    ) -> Result<DataStreamSinkProgram, ExecPlanBuildError> {
        DataStreamSinkProgram::try_new(
            self.dest_node_id,
            self.output_exprs,
            self.output_partition_type,
            self.output_partition_exprs,
            self.output_columns,
            self.limit,
            partition_arena,
        )
    }

    fn validate_shape(&self, context: &str) -> Result<(), ExecPlanBuildError> {
        validate_stream_shape(
            context,
            &self.output_exprs,
            self.output_partition_type,
            &self.output_partition_exprs,
            &self.output_columns,
        )
    }

    pub(crate) const fn dest_node_id(&self) -> i32 {
        self.dest_node_id
    }

    pub(crate) fn output_exprs(&self) -> &[ExprId] {
        &self.output_exprs
    }

    pub(crate) const fn output_partition_type(&self) -> DataStreamPartitionType {
        self.output_partition_type
    }

    pub(crate) fn output_partition_exprs(&self) -> &[ExprId] {
        &self.output_partition_exprs
    }

    pub(crate) fn output_columns(&self) -> &[SlotId] {
        &self.output_columns
    }

    pub(crate) const fn limit(&self) -> Option<i64> {
        self.limit
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MultiCastDataStreamSinkProgram {
    sinks: Vec<DataStreamSinkBranchProgram>,
    partition_arena: ExprArena,
}

impl MultiCastDataStreamSinkProgram {
    pub(crate) fn try_new(
        sinks: Vec<DataStreamSinkBranchProgram>,
        partition_arena: ExprArena,
    ) -> Result<Self, ExecPlanBuildError> {
        let program = Self {
            sinks,
            partition_arena,
        };
        program.validate()?;
        Ok(program)
    }

    fn validate(&self) -> Result<(), ExecPlanBuildError> {
        validate_non_empty_group("MULTI_CAST_DATA_STREAM_SINK", self.sinks.len())?;
        for (index, sink) in self.sinks.iter().enumerate() {
            sink.validate_shape(&format!("MULTI_CAST_DATA_STREAM_SINK sink[{index}]"))?;
            validate_expr_ids(
                &self.partition_arena,
                sink.output_partition_exprs(),
                &format!("MULTI_CAST_DATA_STREAM_SINK sink[{index}] partition"),
            )?;
        }
        Ok(())
    }

    pub(crate) fn sinks(&self) -> &[DataStreamSinkBranchProgram] {
        &self.sinks
    }

    pub(crate) const fn partition_arena(&self) -> &ExprArena {
        &self.partition_arena
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SplitDataStreamSinkProgram {
    sinks: Vec<DataStreamSinkBranchProgram>,
    split_exprs: Vec<ExprId>,
    arena: ExprArena,
}

impl SplitDataStreamSinkProgram {
    pub(crate) fn try_new(
        sinks: Vec<DataStreamSinkBranchProgram>,
        split_exprs: Vec<ExprId>,
        arena: ExprArena,
    ) -> Result<Self, ExecPlanBuildError> {
        let program = Self {
            sinks,
            split_exprs,
            arena,
        };
        program.validate()?;
        Ok(program)
    }

    fn validate(&self) -> Result<(), ExecPlanBuildError> {
        validate_non_empty_group("SPLIT_DATA_STREAM_SINK", self.sinks.len())?;
        if self.split_exprs.len() != self.sinks.len() {
            return Err(ExecPlanBuildError::new(
                ExecPlanInvariant::Sink,
                format!(
                    "SPLIT_DATA_STREAM_SINK split expression count {} does not match branch count {}",
                    self.split_exprs.len(),
                    self.sinks.len()
                ),
            ));
        }
        validate_expr_ids(
            &self.arena,
            &self.split_exprs,
            "SPLIT_DATA_STREAM_SINK split",
        )?;
        for (index, sink) in self.sinks.iter().enumerate() {
            sink.validate_shape(&format!("SPLIT_DATA_STREAM_SINK sink[{index}]"))?;
            validate_expr_ids(
                &self.arena,
                sink.output_partition_exprs(),
                &format!("SPLIT_DATA_STREAM_SINK sink[{index}] partition"),
            )?;
        }
        Ok(())
    }

    pub(crate) fn sinks(&self) -> &[DataStreamSinkBranchProgram] {
        &self.sinks
    }

    pub(crate) fn split_exprs(&self) -> &[ExprId] {
        &self.split_exprs
    }

    pub(crate) const fn arena(&self) -> &ExprArena {
        &self.arena
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
    pub(crate) fn try_from_factory_input(
        input: IcebergSinkFactoryInput,
    ) -> Result<Self, ExecPlanBuildError> {
        let program = Self {
            name: input.name,
            arena: input.arena,
            plan: input.plan,
        };
        program.validate()?;
        Ok(program)
    }

    fn validate(&self) -> Result<(), ExecPlanBuildError> {
        validate_expr_ids(&self.arena, &self.plan.output_exprs, "Iceberg output")?;
        validate_expr_ids(&self.arena, &self.plan.partition_exprs, "Iceberg partition")
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
    branch_id: i32,
    branch_kind: ChangeStreamBranchKind,
    stream_sink: DataStreamSinkBranchProgram,
}

impl IcebergChangeStreamRouterBranchProgram {
    pub(crate) fn new(
        branch_id: i32,
        branch_kind: ChangeStreamBranchKind,
        stream_sink: DataStreamSinkBranchProgram,
    ) -> Self {
        Self {
            branch_id,
            branch_kind,
            stream_sink,
        }
    }

    pub(crate) const fn branch_id(&self) -> i32 {
        self.branch_id
    }

    pub(crate) const fn branch_kind(&self) -> ChangeStreamBranchKind {
        self.branch_kind
    }

    pub(crate) const fn stream_sink(&self) -> &DataStreamSinkBranchProgram {
        &self.stream_sink
    }
}

#[derive(Clone, Debug)]
pub(crate) struct IcebergChangeStreamRouterProgram {
    change_op_slot_id: SlotId,
    data_route_slot_id: Option<SlotId>,
    branches: Vec<IcebergChangeStreamRouterBranchProgram>,
    partition_arena: ExprArena,
}

impl IcebergChangeStreamRouterProgram {
    pub(crate) fn try_new(
        change_op_slot_id: SlotId,
        data_route_slot_id: Option<SlotId>,
        branches: Vec<IcebergChangeStreamRouterBranchProgram>,
        partition_arena: ExprArena,
    ) -> Result<Self, ExecPlanBuildError> {
        let program = Self {
            change_op_slot_id,
            data_route_slot_id,
            branches,
            partition_arena,
        };
        program.validate()?;
        Ok(program)
    }

    fn validate(&self) -> Result<(), ExecPlanBuildError> {
        validate_non_empty_group("ICEBERG_CHANGE_STREAM_ROUTER_SINK", self.branches.len())?;
        for (index, branch) in self.branches.iter().enumerate() {
            branch.stream_sink.validate_shape(&format!(
                "ICEBERG_CHANGE_STREAM_ROUTER_SINK branch[{index}]"
            ))?;
            validate_expr_ids(
                &self.partition_arena,
                branch.stream_sink.output_partition_exprs(),
                &format!("ICEBERG_CHANGE_STREAM_ROUTER_SINK branch[{index}] partition"),
            )?;
        }
        Ok(())
    }

    pub(crate) const fn change_op_slot_id(&self) -> SlotId {
        self.change_op_slot_id
    }

    pub(crate) const fn data_route_slot_id(&self) -> Option<SlotId> {
        self.data_route_slot_id
    }

    pub(crate) fn branches(&self) -> &[IcebergChangeStreamRouterBranchProgram] {
        &self.branches
    }

    pub(crate) const fn partition_arena(&self) -> &ExprArena {
        &self.partition_arena
    }
}

fn validate_stream_shape(
    context: &str,
    output_exprs: &[ExprId],
    output_partition_type: DataStreamPartitionType,
    output_partition_exprs: &[ExprId],
    output_columns: &[SlotId],
) -> Result<(), ExecPlanBuildError> {
    if !output_exprs.is_empty() {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Expression,
            format!("{context} output_exprs are not supported"),
        ));
    }
    if !output_partition_type.requires_exprs() && !output_partition_exprs.is_empty() {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Expression,
            format!("{context} non-hash partition type must not retain partition expressions"),
        ));
    }
    let mut seen = HashSet::new();
    if let Some(slot_id) = output_columns
        .iter()
        .find(|slot_id| !seen.insert(**slot_id))
    {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Sink,
            format!("{context} duplicate output column slot id {slot_id}"),
        ));
    }
    Ok(())
}

fn validate_expr_ids(
    arena: &ExprArena,
    exprs: &[ExprId],
    context: &str,
) -> Result<(), ExecPlanBuildError> {
    if let Some(expr_id) = exprs.iter().find(|expr_id| arena.node(**expr_id).is_none()) {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Expression,
            format!(
                "{context} expression id {} is missing from its arena",
                expr_id.0
            ),
        ));
    }
    Ok(())
}

fn validate_non_empty_group(context: &str, count: usize) -> Result<(), ExecPlanBuildError> {
    if count == 0 {
        return Err(ExecPlanBuildError::new(
            ExecPlanInvariant::Sink,
            format!("{context} requires at least one static branch"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Schema};
    use parquet::basic::Compression;

    use crate::common::ids::SlotId;
    use crate::connector::iceberg::delete_file::IcebergFileFormat;
    use crate::connector::iceberg::sink_plan::{
        IcebergSinkFactoryInput, IcebergSinkMode, IcebergSinkPlan,
    };
    use crate::exec::expr::{ExprArena, ExprId, ExprNode};
    use crate::exec::fragment::error::ExecPlanInvariant;
    use crate::exec::fragment::program::{
        FragmentSinkAssignmentKind, FragmentSinkAssignmentRequirement, FragmentSinkSpec,
    };
    use crate::exec::operators::DataStreamPartitionType;
    use crate::sql::common::ChangeStreamBranchKind;

    use super::{
        DataStreamSinkBranchProgram, DataStreamSinkProgram, FragmentSinkProgram,
        IcebergChangeStreamRouterBranchProgram, IcebergChangeStreamRouterProgram,
        IcebergTableSinkProgram, MultiCastDataStreamSinkProgram,
    };

    fn iceberg_input(
        arena: ExprArena,
        output_exprs: Vec<ExprId>,
        partition_exprs: Vec<ExprId>,
    ) -> IcebergSinkFactoryInput {
        let schema = Arc::new(Schema::empty());
        IcebergSinkFactoryInput {
            name: "ICEBERG_TABLE_SINK".to_string(),
            arena,
            plan: IcebergSinkPlan {
                mode: IcebergSinkMode::Data,
                table_location: "file:///tmp/table".to_string(),
                data_location: "file:///tmp/table/data".to_string(),
                target_partition_spec_id: 0,
                target_table_metadata: None,
                target_snapshot_id: None,
                position_delete_data_file_partitions: HashMap::new(),
                position_delete_data_file_partition_index_input: None,
                object_store_s3: None,
                file_format: IcebergFileFormat::Parquet,
                report_file_format: "parquet".to_string(),
                compression: Compression::SNAPPY,
                output_schema: Arc::clone(&schema),
                target_schema: schema,
                equality_delete_columns: Vec::new(),
                row_lineage_data: false,
                output_exprs,
                partition_exprs,
                partition_source_column_names: Vec::new(),
                partition_column_names: Vec::new(),
                transform_exprs: Vec::new(),
                position_delete_binding: None,
            },
        }
    }

    #[test]
    fn static_data_stream_program_contains_no_destinations() {
        let program = DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::Unpartitioned,
            Vec::new(),
            vec![SlotId::new(3)],
            Some(9),
            ExprArena::default(),
        )
        .expect("valid stream program");

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

    #[test]
    fn data_stream_program_rejects_duplicate_output_columns() {
        let error = DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::Unpartitioned,
            Vec::new(),
            vec![SlotId::new(3), SlotId::new(3)],
            None,
            ExprArena::default(),
        )
        .expect_err("duplicate output columns must fail static construction");

        assert_eq!(error.invariant(), ExecPlanInvariant::Sink);
        assert!(error.detail().contains("duplicate output column slot id 3"));
    }

    #[test]
    fn data_stream_program_rejects_all_unsupported_output_exprs() {
        let mut arena = ExprArena::default();
        let valid_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);

        for output_expr in [valid_id, ExprId(99)] {
            let error = DataStreamSinkProgram::try_new(
                17,
                vec![output_expr],
                DataStreamPartitionType::Unpartitioned,
                Vec::new(),
                vec![SlotId::new(3)],
                None,
                arena.clone(),
            )
            .expect_err("stream output expressions are unsupported");

            assert_eq!(error.invariant(), ExecPlanInvariant::Expression);
            assert!(error.detail().contains("output_exprs are not supported"));
        }
    }

    #[test]
    fn data_stream_partition_exprs_are_normalized_and_arena_checked() {
        let mut arena = ExprArena::default();
        let valid_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);

        let random = DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::Random,
            vec![ExprId(99)],
            vec![SlotId::new(3)],
            None,
            arena.clone(),
        )
        .expect("non-hash partition expressions are normalized away");
        assert!(random.output_partition_exprs().is_empty());

        let hash = DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::HashPartitioned,
            vec![valid_id],
            vec![SlotId::new(3)],
            None,
            arena.clone(),
        )
        .expect("valid hash partition expression");
        assert_eq!(hash.output_partition_exprs(), &[valid_id]);

        let error = DataStreamSinkProgram::try_new(
            17,
            Vec::new(),
            DataStreamPartitionType::HashPartitioned,
            vec![ExprId(99)],
            vec![SlotId::new(3)],
            None,
            arena,
        )
        .expect_err("hash partition expression must belong to its arena");
        assert_eq!(error.invariant(), ExecPlanInvariant::Expression);
    }

    #[test]
    fn grouped_stream_programs_validate_partition_exprs_against_group_arena() {
        let branch = || {
            DataStreamSinkBranchProgram::try_new(
                17,
                Vec::new(),
                DataStreamPartitionType::HashPartitioned,
                vec![ExprId(99)],
                vec![SlotId::new(3)],
                None,
            )
            .expect("branch validation is completed by the group arena owner")
        };

        let multicast_error =
            MultiCastDataStreamSinkProgram::try_new(vec![branch()], ExprArena::default())
                .expect_err("multicast partition expression must belong to group arena");
        assert_eq!(multicast_error.invariant(), ExecPlanInvariant::Expression);

        let router_error = IcebergChangeStreamRouterProgram::try_new(
            SlotId::new(1),
            None,
            vec![IcebergChangeStreamRouterBranchProgram::new(
                7,
                ChangeStreamBranchKind::FreshData,
                branch(),
            )],
            ExprArena::default(),
        )
        .expect_err("router partition expression must belong to group arena");
        assert_eq!(router_error.invariant(), ExecPlanInvariant::Expression);
    }

    #[test]
    fn iceberg_program_validates_output_and_partition_expr_arena_membership() {
        for (output_exprs, partition_exprs, label) in [
            (vec![ExprId(99)], Vec::new(), "output"),
            (Vec::new(), vec![ExprId(99)], "partition"),
        ] {
            let error = IcebergTableSinkProgram::try_from_factory_input(iceberg_input(
                ExprArena::default(),
                output_exprs,
                partition_exprs,
            ))
            .expect_err("Iceberg expression ids must belong to the sink arena");

            assert_eq!(error.invariant(), ExecPlanInvariant::Expression);
            assert!(error.detail().contains(label));
        }

        let mut arena = ExprArena::default();
        let expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        IcebergTableSinkProgram::try_from_factory_input(iceberg_input(
            arena,
            vec![expr],
            vec![expr],
        ))
        .expect("valid Iceberg expression ids");
    }
}
