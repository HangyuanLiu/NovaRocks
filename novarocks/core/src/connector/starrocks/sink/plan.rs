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
use crate::connector::starrocks::ports::SinkFrontendProvider;
use crate::connector::starrocks::schema::{StarRocksKeysType, StarRocksTabletSchema};
use crate::connector::starrocks::sink::partition_key::{PartitionExprPlan, PartitionKeyValue};
use crate::exec::expr::{ExprArena, ExprId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FrontendAddress {
    pub hostname: String,
    pub port: i32,
}

#[derive(Clone)]
pub struct StarRocksSinkFactoryInput {
    pub name: String,
    pub descriptor: StarRocksSinkDescriptor,
    pub output_projection: Option<SinkOutputProjectionPlan>,
    pub output_expr_slot_name_map: HashMap<String, SlotId>,
    pub output_expr_slot_ids: Vec<Option<SlotId>>,
    pub literal_partition_values: Option<Vec<String>>,
}

#[derive(Clone)]
pub struct StarRocksTableSinkProgram {
    pub name: String,
    pub descriptor: StarRocksTableSinkDescriptor,
    pub output_projection: Option<SinkOutputProjectionPlan>,
    pub output_expr_slot_name_map: HashMap<String, SlotId>,
    pub output_expr_slot_ids: Vec<Option<SlotId>>,
    pub literal_partition_values: Option<Vec<String>>,
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
    pub fn validate(&self) -> Result<(), String> {
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
pub struct StarRocksTableSinkDescriptor {
    pub db_id: i64,
    pub table_id: i64,
    pub db_name: Option<String>,
    pub table_name: Option<String>,
    pub keys_type: StarRocksKeysType,
    pub is_lake_table: bool,
    pub dynamic_overwrite: bool,
    pub partial_update_mode: crate::connector::starrocks::lake::context::PartialUpdateWriteMode,
    pub merge_condition: Option<String>,
    pub null_expr_in_auto_increment: bool,
    pub miss_auto_increment_column: bool,
    pub schema: SinkSchemaDescriptor,
    pub partition: SinkPartitionDescriptor,
    pub location: SinkLocationDescriptor,
    pub nodes: SinkNodesDescriptor,
    pub frontend_provider: Option<Arc<dyn SinkFrontendProvider>>,
    pub starlet_metadata_provider:
        Option<Arc<dyn crate::connector::starrocks::ports::StarletMetadataProvider>>,
    pub storage_metadata_provider:
        Option<Arc<dyn crate::connector::starrocks::ports::StorageMetadataProvider>>,
}

#[derive(Clone)]
pub struct StarRocksSinkDescriptor {
    pub db_id: i64,
    pub table_id: i64,
    pub db_name: Option<String>,
    pub table_name: Option<String>,
    pub txn_id: i64,
    pub load_id: UniqueId,
    pub keys_type: StarRocksKeysType,
    pub is_lake_table: bool,
    pub dynamic_overwrite: bool,
    pub partial_update_mode: crate::connector::starrocks::lake::context::PartialUpdateWriteMode,
    pub merge_condition: Option<String>,
    pub null_expr_in_auto_increment: bool,
    pub miss_auto_increment_column: bool,
    pub schema: SinkSchemaDescriptor,
    pub partition: SinkPartitionDescriptor,
    pub location: SinkLocationDescriptor,
    pub nodes: SinkNodesDescriptor,
    pub frontend: Option<FrontendAddress>,
    pub frontend_provider: Option<Arc<dyn SinkFrontendProvider>>,
    pub starlet_metadata_provider:
        Option<Arc<dyn crate::connector::starrocks::ports::StarletMetadataProvider>>,
    pub storage_metadata_provider:
        Option<Arc<dyn crate::connector::starrocks::ports::StorageMetadataProvider>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkSlotDescriptor {
    pub id: Option<SlotId>,
    pub col_name: Option<String>,
    pub col_physical_name: Option<String>,
}

#[derive(Clone)]
pub struct SinkSchemaDescriptor {
    pub slot_descs: Vec<SinkSlotDescriptor>,
    pub indexes: Vec<SinkIndexDescriptor>,
}

#[derive(Clone)]
pub struct SinkIndexDescriptor {
    pub index_id: i64,
    pub schema_id: i64,
    pub column_names: Vec<String>,
    pub tablet_schema: StarRocksTabletSchema,
    pub column_to_expr_value: HashMap<String, String>,
    pub is_shadow: bool,
    pub where_clause: Option<SinkPredicatePlan>,
}

#[derive(Clone)]
pub struct SinkPredicatePlan {
    pub arena: Arc<ExprArena>,
    pub expr_id: ExprId,
}

#[derive(Clone)]
pub struct SinkOutputProjectionPlan {
    pub arena: Arc<ExprArena>,
    pub expr_ids: Vec<ExprId>,
    pub output_slot_ids: Vec<SlotId>,
    pub output_field_names: Vec<String>,
}

#[derive(Clone)]
pub struct SinkPartitionDescriptor {
    pub enable_automatic_partition: bool,
    pub partition_columns: Vec<String>,
    pub distributed_columns: Vec<String>,
    pub partition_exprs: Option<Arc<PartitionExprPlan>>,
    pub partitions: Vec<SinkPartitionEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkPartitionEntry {
    pub partition_id: i64,
    pub is_shadow: bool,
    pub indexes: Vec<SinkPartitionIndex>,
    pub start_key: Option<Vec<PartitionKeyValue>>,
    pub end_key: Option<Vec<PartitionKeyValue>>,
    pub in_keys: Vec<Vec<PartitionKeyValue>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkPartitionIndex {
    pub index_id: i64,
    pub tablet_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkLocationDescriptor {
    pub tablets: Vec<SinkTabletLocation>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkTabletLocation {
    pub tablet_id: i64,
    pub node_ids: Vec<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkNodesDescriptor {
    pub nodes: Vec<SinkNodeInfo>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SinkNodeInfo {
    pub id: i64,
    pub option: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreatePartitionResult {
    pub partitions: Vec<SinkPartitionEntry>,
    pub tablets: Vec<SinkTabletLocation>,
    pub nodes: Vec<SinkNodeInfo>,
}
