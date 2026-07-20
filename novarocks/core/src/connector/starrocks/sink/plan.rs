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

use std::collections::HashMap;
use std::sync::Arc;

use crate::common::ids::SlotId;
use crate::common::types::UniqueId;
use crate::connector::starrocks::schema::{StarRocksKeysType, StarRocksTabletSchema};
use crate::connector::starrocks::sink::partition_key::{PartitionExprPlan, PartitionKeyValue};
use crate::exec::expr::{ExprArena, ExprId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FrontendAddress {
    pub(crate) hostname: String,
    pub(crate) port: i32,
}

#[derive(Clone)]
pub(crate) struct StarRocksSinkFactoryInput {
    pub(crate) name: String,
    pub(crate) descriptor: StarRocksSinkDescriptor,
    pub(crate) output_projection: Option<SinkOutputProjectionPlan>,
    pub(crate) output_expr_slot_name_map: HashMap<String, SlotId>,
    pub(crate) output_expr_slot_ids: Vec<Option<SlotId>>,
    pub(crate) literal_partition_values: Option<Vec<String>>,
}

#[derive(Clone)]
pub(crate) struct StarRocksTableSinkProgram {
    pub(crate) name: String,
    pub(crate) descriptor: StarRocksTableSinkDescriptor,
    pub(crate) output_projection: Option<SinkOutputProjectionPlan>,
    pub(crate) output_expr_slot_name_map: HashMap<String, SlotId>,
    pub(crate) output_expr_slot_ids: Vec<Option<SlotId>>,
    pub(crate) literal_partition_values: Option<Vec<String>>,
}

impl std::fmt::Debug for StarRocksTableSinkProgram {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StarRocksTableSinkProgram")
            .field("name", &self.name)
            .field("db_id", &self.descriptor.db_id)
            .field("table_id", &self.descriptor.table_id)
            .finish_non_exhaustive()
    }
}

impl StarRocksTableSinkProgram {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("StarRocks table sink program name must not be empty".to_string());
        }
        if self.descriptor.db_id <= 0 || self.descriptor.table_id <= 0 {
            return Err(format!(
                "StarRocks table sink program requires positive db/table ids, got db_id={} table_id={}",
                self.descriptor.db_id, self.descriptor.table_id
            ));
        }
        if self.descriptor.schema.indexes.is_empty() {
            return Err(
                "StarRocks table sink program requires at least one write index".to_string(),
            );
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct StarRocksTableSinkDescriptor {
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) db_name: Option<String>,
    pub(crate) table_name: Option<String>,
    pub(crate) keys_type: StarRocksKeysType,
    pub(crate) is_lake_table: bool,
    pub(crate) dynamic_overwrite: bool,
    pub(crate) partial_update_mode:
        crate::connector::starrocks::lake::context::PartialUpdateWriteMode,
    pub(crate) merge_condition: Option<String>,
    pub(crate) null_expr_in_auto_increment: bool,
    pub(crate) miss_auto_increment_column: bool,
    pub(crate) schema: SinkSchemaDescriptor,
    pub(crate) partition: SinkPartitionDescriptor,
    pub(crate) location: SinkLocationDescriptor,
    pub(crate) nodes: SinkNodesDescriptor,
}

#[derive(Clone)]
pub(crate) struct StarRocksSinkDescriptor {
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) db_name: Option<String>,
    pub(crate) table_name: Option<String>,
    pub(crate) txn_id: i64,
    pub(crate) load_id: UniqueId,
    pub(crate) keys_type: StarRocksKeysType,
    pub(crate) is_lake_table: bool,
    pub(crate) dynamic_overwrite: bool,
    pub(crate) partial_update_mode:
        crate::connector::starrocks::lake::context::PartialUpdateWriteMode,
    pub(crate) merge_condition: Option<String>,
    pub(crate) null_expr_in_auto_increment: bool,
    pub(crate) miss_auto_increment_column: bool,
    pub(crate) schema: SinkSchemaDescriptor,
    pub(crate) partition: SinkPartitionDescriptor,
    pub(crate) location: SinkLocationDescriptor,
    pub(crate) nodes: SinkNodesDescriptor,
    pub(crate) frontend: Option<FrontendAddress>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SinkSlotDescriptor {
    pub(crate) id: Option<SlotId>,
    pub(crate) col_name: Option<String>,
    pub(crate) col_physical_name: Option<String>,
}

#[derive(Clone)]
pub(crate) struct SinkSchemaDescriptor {
    pub(crate) slot_descs: Vec<SinkSlotDescriptor>,
    pub(crate) indexes: Vec<SinkIndexDescriptor>,
}

#[derive(Clone)]
pub(crate) struct SinkIndexDescriptor {
    pub(crate) index_id: i64,
    pub(crate) schema_id: i64,
    pub(crate) column_names: Vec<String>,
    pub(crate) tablet_schema: StarRocksTabletSchema,
    pub(crate) column_to_expr_value: HashMap<String, String>,
    pub(crate) is_shadow: bool,
    pub(crate) where_clause: Option<SinkPredicatePlan>,
}

#[derive(Clone)]
pub(crate) struct SinkPredicatePlan {
    pub(crate) arena: Arc<ExprArena>,
    pub(crate) expr_id: ExprId,
}

#[derive(Clone)]
pub(crate) struct SinkOutputProjectionPlan {
    pub(crate) arena: Arc<ExprArena>,
    pub(crate) expr_ids: Vec<ExprId>,
    pub(crate) output_slot_ids: Vec<SlotId>,
    pub(crate) output_field_names: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct SinkPartitionDescriptor {
    pub(crate) enable_automatic_partition: bool,
    pub(crate) partition_columns: Vec<String>,
    pub(crate) distributed_columns: Vec<String>,
    pub(crate) partition_exprs: Option<Arc<PartitionExprPlan>>,
    pub(crate) partitions: Vec<SinkPartitionEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SinkPartitionEntry {
    pub(crate) partition_id: i64,
    pub(crate) is_shadow: bool,
    pub(crate) indexes: Vec<SinkPartitionIndex>,
    pub(crate) start_key: Option<Vec<PartitionKeyValue>>,
    pub(crate) end_key: Option<Vec<PartitionKeyValue>>,
    pub(crate) in_keys: Vec<Vec<PartitionKeyValue>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SinkPartitionIndex {
    pub(crate) index_id: i64,
    pub(crate) tablet_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SinkLocationDescriptor {
    pub(crate) tablets: Vec<SinkTabletLocation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SinkTabletLocation {
    pub(crate) tablet_id: i64,
    pub(crate) node_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SinkNodesDescriptor {
    pub(crate) nodes: Vec<SinkNodeInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SinkNodeInfo {
    pub(crate) id: i64,
    pub(crate) option: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CreatePartitionResult {
    pub(crate) partitions: Vec<SinkPartitionEntry>,
    pub(crate) tablets: Vec<SinkTabletLocation>,
    pub(crate) nodes: Vec<SinkNodeInfo>,
}
