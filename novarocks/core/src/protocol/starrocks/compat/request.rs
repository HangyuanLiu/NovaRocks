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

use crate::thrift::internal_service::{TPlanFragmentExecParams, TScanRangeParams};

/// Backfills the historical per-node scan-range view from the current per-driver view.
///
/// Older consumers expected `per_node_scan_ranges`, while current FEs may send only
/// `node_to_per_driver_seq_scan_ranges`. A concrete per-node entry always wins;
/// only an absent or placeholder entry is backfilled. This rule can be removed once
/// all supported fragment consumers read the per-driver shape directly.
pub fn backfill_per_node_scan_ranges(exec_params: &mut TPlanFragmentExecParams) {
    fn has_concrete_scan_range(ranges: &[TScanRangeParams]) -> bool {
        ranges.iter().any(|range| !range.empty.unwrap_or(false))
    }

    let Some(node_to_per_driver) = exec_params.node_to_per_driver_seq_scan_ranges.as_ref() else {
        return;
    };
    let mut to_insert = Vec::new();
    for (node_id, per_driver) in node_to_per_driver {
        let existing = exec_params.per_node_scan_ranges.get(node_id);
        if existing.is_some_and(|ranges| has_concrete_scan_range(ranges)) {
            continue;
        }
        let flattened = per_driver
            .values()
            .flat_map(|ranges| ranges.iter().cloned())
            .collect::<Vec<_>>();
        if flattened.is_empty() {
            if existing.is_none() {
                to_insert.push((*node_id, Vec::new()));
            }
            continue;
        }
        to_insert.push((*node_id, flattened));
    }
    for (node_id, ranges) in to_insert {
        exec_params.per_node_scan_ranges.insert(node_id, ranges);
    }
}
