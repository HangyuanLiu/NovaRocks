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

use crate::lower::compat::node::Lowered;
use crate::lower::compat::node::decode::QueryGlobalDictMap;
use crate::novarocks_connectors::ConnectorRegistry;
use crate::thrift::{descriptors, internal_service, plan_nodes, types};

/// Lower an OLAP_SCAN_NODE StarRocks tablet scan node to a `Lowered` ExecNode.
pub(crate) fn lower_starrocks_scan_node(
    node: &plan_nodes::TPlanNode,
    _desc_tbl: Option<&descriptors::TDescriptorTable>,
    _tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    _layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    _exec_params: Option<&internal_service::TPlanFragmentExecParams>,
    _query_opts: Option<&internal_service::TQueryOptions>,
    _connectors: &ConnectorRegistry,
    _query_global_dict_map: &QueryGlobalDictMap,
) -> Result<Lowered, String> {
    if node.num_children != 0 {
        return Err(format!(
            "OLAP_SCAN_NODE expected 0 children, got {}",
            node.num_children
        ));
    }

    let Some(olap) = node.olap_scan_node.as_ref() else {
        return Err("OLAP_SCAN_NODE missing olap_scan_node payload".to_string());
    };
    Err(format!(
        "OLAP_SCAN_NODE StarRocks OLAP direct-read requires partition_storage_paths metadata, but current OLAP thrift/descriptors only provide tuple_id={} and do not provide tablet storage paths or the schema_key needed to resolve them without guessing",
        olap.tuple_id
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lower::compat::node::decode::QueryGlobalDictMap;
    use crate::lower::compat::test_support::DescriptorTableBuilder;
    use crate::thrift::{descriptors, internal_service, plan_nodes, types};
    use arrow::datatypes::DataType;
    use std::collections::{BTreeMap, HashMap};

    fn single_int_olap_desc_table(
        tuple_id: i32,
        slot_id: i32,
        table_id: i64,
    ) -> descriptors::TDescriptorTable {
        let mut builder = DescriptorTableBuilder::new();
        builder.add_table(table_id, "db", "tbl", 1);
        builder.add_tuple(tuple_id, Some(table_id));
        builder.add_slot(slot_id, tuple_id, "status", &DataType::Utf8, true, 0);
        builder.build()
    }

    fn olap_plan_node(node_id: i32, tuple_id: i32) -> plan_nodes::TPlanNode {
        let mut node = crate::lower::compat::node::test_plan_node(
            node_id,
            plan_nodes::TPlanNodeType::OLAP_SCAN_NODE,
            0,
        );
        node.row_tuples = vec![tuple_id];
        node.olap_scan_node = Some(plan_nodes::TOlapScanNode {
            tuple_id,
            key_column_name: vec![],
            key_column_type: vec![],
            is_preaggregation: false,
            sort_column: None,
            rollup_name: None,
            sql_predicates: None,
            enable_column_expr_predicate: None,
            dict_string_id_to_int_ids: None,
            unused_output_column_name: None,
            sorted_by_keys_per_tablet: None,
            bucket_exprs: None,
            sort_key_column_names: None,
            max_parallel_scan_instance_num: None,
            column_access_paths: None,
            use_pk_index: None,
            columns_desc: None,
            output_chunk_by_bucket: None,
            output_asc_hint: None,
            partition_order_hint: None,
            enable_prune_column_after_index_filter: None,
            enable_gin_filter: None,
            schema_id: None,
            vector_search_options: None,
            sample_options: None,
            enable_topn_filter_back_pressure: None,
            back_pressure_max_rounds: None,
            back_pressure_throttle_time: None,
            back_pressure_throttle_time_upper_bound: None,
            back_pressure_num_rows: None,
            next_uniq_id: None,
            enable_global_late_materialization: None,
            partition_conjuncts: None,
        });
        node
    }

    fn olap_exec_params(node_id: i32) -> internal_service::TPlanFragmentExecParams {
        let internal_scan_range = plan_nodes::TInternalScanRange::new(
            vec![],
            "123".to_string(),
            "7".to_string(),
            "0".to_string(),
            10,
            "db".to_string(),
            None::<Vec<plan_nodes::TKeyRange>>,
            None::<String>,
            Some("tbl".to_string()),
            Some(20),
            None::<i64>,
            Some(true),
            None::<i32>,
            Some(false),
            Some(false),
            None::<i64>,
        );
        let scan_range = internal_service::TScanRangeParams::new(
            plan_nodes::TScanRange::new(
                Some(internal_scan_range),
                None::<Vec<u8>>,
                None::<plan_nodes::TBrokerScanRange>,
                None::<plan_nodes::TEsScanRange>,
                None::<plan_nodes::THdfsScanRange>,
                None::<plan_nodes::TBinlogScanRange>,
                None::<plan_nodes::TBenchmarkScanRange>,
            ),
            None::<i32>,
            Some(false),
            Some(false),
        );
        let mut per_node_scan_ranges = BTreeMap::new();
        per_node_scan_ranges.insert(node_id, vec![scan_range]);
        internal_service::TPlanFragmentExecParams {
            query_id: types::TUniqueId::new(0, 1),
            fragment_instance_id: types::TUniqueId::new(0, 1),
            per_node_scan_ranges,
            per_exch_num_senders: BTreeMap::new(),
            destinations: None,
            sender_id: None,
            num_senders: None,
            send_query_statistics_with_every_batch: None,
            use_vectorized: None,
            runtime_filter_params: None,
            instances_number: None,
            enable_exchange_pass_through: None,
            node_to_per_driver_seq_scan_ranges: None,
            enable_exchange_perf: None,
            pipeline_sink_dop: None,
            report_when_finish: None,
            exec_debug_options: None,
            per_look_up_num_fetchers: None,
            per_fetch_target_nodes: None,
        }
    }

    #[test]
    fn olap_scan_without_partition_storage_paths_fails_at_lowering() {
        let node_id = 11;
        let tuple_id = 1;
        let slot_id = 7;
        let table_id = 100;
        let node = olap_plan_node(node_id, tuple_id);
        let desc_tbl = single_int_olap_desc_table(tuple_id, slot_id, table_id);
        let tuple_slots = HashMap::from([(tuple_id, vec![slot_id])]);
        let exec_params = olap_exec_params(node_id);
        let connectors = crate::connector::ConnectorRegistry::default();
        let query_global_dict_map = QueryGlobalDictMap::new();

        let result = lower_starrocks_scan_node(
            &node,
            Some(&desc_tbl),
            &tuple_slots,
            &HashMap::new(),
            Some(&exec_params),
            None,
            &connectors,
            &query_global_dict_map,
        );

        let Err(err) = result else {
            panic!("OLAP scan without direct-read metadata should fail at lowering");
        };
        assert!(
            err.contains("OLAP_SCAN_NODE StarRocks OLAP direct-read requires partition_storage_paths metadata"),
            "unexpected error: {err}"
        );
    }
}
