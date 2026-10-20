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

use crate::exec::expr::ExprId;
use crate::runtime::endpoint::FragmentDestination;
pub use novarocks_execution_contract::DataStreamPartitionType;
use novarocks_types::SlotId;
use std::collections::HashSet;

/// Construction-time input for a distributed stream sink. The private
/// operator factories consume this value but do not define the public
/// fragment-construction contract.
#[derive(Clone)]
pub struct DataStreamSinkFactoryInput {
    pub dest_node_id: i32,
    pub output_exprs: Vec<ExprId>,
    pub output_partition_type: DataStreamPartitionType,
    pub output_partition_exprs: Vec<ExprId>,
    pub output_columns: Vec<SlotId>,
    pub destinations: Vec<FragmentDestination>,
}

impl DataStreamSinkFactoryInput {
    pub fn try_from_static_program(
        dest_node_id: i32,
        output_partition_type: DataStreamPartitionType,
        output_exprs: Vec<ExprId>,
        mut output_partition_exprs: Vec<ExprId>,
        output_columns: Vec<SlotId>,
        destinations: Vec<FragmentDestination>,
    ) -> Result<Self, String> {
        if !output_exprs.is_empty() {
            return Err("DATA_STREAM_SINK output_exprs are not supported".to_string());
        }
        let mut seen = HashSet::new();
        if let Some(slot_id) = output_columns
            .iter()
            .find(|slot_id| !seen.insert(**slot_id))
        {
            return Err(format!(
                "DATA_STREAM_SINK: duplicate output_columns slot id: {slot_id}"
            ));
        }
        if !output_partition_type.requires_exprs() {
            output_partition_exprs.clear();
        }
        Ok(Self {
            dest_node_id,
            output_exprs,
            output_partition_type,
            output_partition_exprs,
            output_columns,
            destinations,
        })
    }

    pub fn try_new(
        dest_node_id: i32,
        output_partition_type: DataStreamPartitionType,
        output_exprs: Vec<ExprId>,
        output_partition_exprs: Vec<ExprId>,
        output_columns: Vec<i32>,
        destinations: Vec<FragmentDestination>,
    ) -> Result<Self, String> {
        let mut seen = HashSet::new();
        let mut parsed_output_columns = Vec::with_capacity(output_columns.len());
        for raw in output_columns {
            let slot_id = SlotId::try_from(raw).map_err(|err| {
                format!("DATA_STREAM_SINK: invalid output_columns slot id: {err}")
            })?;
            if !seen.insert(slot_id) {
                return Err(format!(
                    "DATA_STREAM_SINK: duplicate output_columns slot id: {slot_id}"
                ));
            }
            parsed_output_columns.push(slot_id);
        }
        Self::try_from_static_program(
            dest_node_id,
            output_partition_type,
            output_exprs,
            output_partition_exprs,
            parsed_output_columns,
            destinations,
        )
    }
}
