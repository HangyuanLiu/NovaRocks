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

use crate::protocol::starrocks::decode::node::Lowered;
use crate::protocol::starrocks::decode::node::decode::QueryGlobalDictMap;
use novarocks::novarocks_connectors::ConnectorRegistry;
use novarocks::runtime::query_options::QueryOptions;
use novarocks::thrift::{descriptors, plan_nodes, types};

/// Lower an OLAP_SCAN_NODE StarRocks tablet scan node to a `Lowered` ExecNode.
pub(crate) fn lower_starrocks_scan_node(
    node: &plan_nodes::TPlanNode,
    _desc_tbl: Option<&descriptors::TDescriptorTable>,
    _tuple_slots: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    _layout_hints: &HashMap<types::TTupleId, Vec<types::TSlotId>>,
    _query_opts: &QueryOptions,
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
