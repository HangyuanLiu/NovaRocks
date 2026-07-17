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
use crate::connector::starrocks::sink::partition_key::{PartitionExprPlan, PartitionKeyValue};
use crate::exec::expr::{ExprArena, ExprId};
use crate::service::grpc_client::proto::starrocks::{KeysType, PUniqueId, TabletSchemaPb};

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
pub(crate) struct StarRocksSinkDescriptor {
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) db_name: Option<String>,
    pub(crate) table_name: Option<String>,
    pub(crate) txn_id: i64,
    pub(crate) load_id: PUniqueId,
    pub(crate) keys_type: KeysType,
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
    pub(crate) tablet_schema: TabletSchemaPb,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::lake::context::PartialUpdateWriteMode;

    #[test]
    fn frontend_address_is_plain_domain_endpoint() {
        let addr = FrontendAddress {
            hostname: "127.0.0.1".to_string(),
            port: 9030,
        };

        assert_eq!(addr.hostname, "127.0.0.1");
        assert_eq!(addr.port, 9030);
    }

    #[test]
    fn create_partition_result_keeps_all_metadata_groups() {
        let result = CreatePartitionResult {
            partitions: vec![SinkPartitionEntry {
                partition_id: 10,
                is_shadow: false,
                indexes: vec![SinkPartitionIndex {
                    index_id: 20,
                    tablet_ids: vec![30],
                }],
                start_key: None,
                end_key: None,
                in_keys: Vec::new(),
            }],
            tablets: vec![SinkTabletLocation {
                tablet_id: 30,
                node_ids: vec![40],
            }],
            nodes: vec![SinkNodeInfo { id: 40, option: 0 }],
        };

        assert_eq!(result.partitions[0].partition_id, 10);
        assert_eq!(result.tablets[0].tablet_id, 30);
        assert_eq!(result.nodes[0].id, 40);
    }

    #[test]
    fn domain_descriptors_keep_partial_update_and_index_metadata() {
        let mut column_to_expr_value = HashMap::new();
        column_to_expr_value.insert("v".to_string(), "coalesce(v, 0)".to_string());

        let descriptor = StarRocksSinkDescriptor {
            db_id: 1,
            table_id: 2,
            db_name: Some("db".to_string()),
            table_name: Some("tbl".to_string()),
            txn_id: 3,
            load_id: PUniqueId { hi: 4, lo: 5 },
            keys_type: KeysType::PrimaryKeys,
            is_lake_table: true,
            dynamic_overwrite: false,
            partial_update_mode: PartialUpdateWriteMode::ColumnUpdate,
            merge_condition: Some("version".to_string()),
            null_expr_in_auto_increment: false,
            miss_auto_increment_column: false,
            schema: SinkSchemaDescriptor {
                slot_descs: Vec::new(),
                indexes: vec![SinkIndexDescriptor {
                    index_id: 10,
                    schema_id: 20,
                    column_names: vec!["k".to_string(), "v".to_string()],
                    tablet_schema: TabletSchemaPb::default(),
                    column_to_expr_value,
                    is_shadow: true,
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
            frontend: None,
        };

        assert!(matches!(
            descriptor.partial_update_mode,
            PartialUpdateWriteMode::ColumnUpdate
        ));
        assert_eq!(descriptor.merge_condition.as_deref(), Some("version"));
        assert_eq!(
            descriptor.schema.indexes[0]
                .column_to_expr_value
                .get("v")
                .map(String::as_str),
            Some("coalesce(v, 0)")
        );
        assert!(descriptor.schema.indexes[0].is_shadow);
    }

    #[test]
    fn factory_input_keeps_literal_partition_values_as_domain_strings() {
        let input = StarRocksSinkFactoryInput {
            name: "OLAP_TABLE_SINK".to_string(),
            descriptor: StarRocksSinkDescriptor {
                db_id: 1,
                table_id: 2,
                db_name: Some("db".to_string()),
                table_name: Some("tbl".to_string()),
                txn_id: 3,
                load_id: PUniqueId { hi: 4, lo: 5 },
                keys_type: KeysType::PrimaryKeys,
                is_lake_table: true,
                dynamic_overwrite: false,
                partial_update_mode: PartialUpdateWriteMode::ColumnUpdate,
                merge_condition: None,
                null_expr_in_auto_increment: false,
                miss_auto_increment_column: false,
                schema: SinkSchemaDescriptor {
                    slot_descs: Vec::new(),
                    indexes: Vec::new(),
                },
                partition: SinkPartitionDescriptor {
                    enable_automatic_partition: true,
                    partition_columns: vec!["dt".to_string()],
                    distributed_columns: Vec::new(),
                    partition_exprs: None,
                    partitions: Vec::new(),
                },
                location: SinkLocationDescriptor {
                    tablets: Vec::new(),
                },
                nodes: SinkNodesDescriptor { nodes: Vec::new() },
                frontend: None,
            },
            output_projection: None,
            output_expr_slot_name_map: HashMap::new(),
            output_expr_slot_ids: Vec::new(),
            literal_partition_values: Some(vec!["2026-06-30".to_string()]),
        };

        assert_eq!(
            input.literal_partition_values.as_deref(),
            Some(&["2026-06-30".to_string()][..])
        );
    }
}
