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
use crate::connector::MinMaxPredicate;
use crate::protocol::starrocks::decode::layout::Layout;
use crate::thrift::exprs;

/// Parse a min/max conjunct TExpr into MinMaxPredicates used for pruning.
pub(crate) fn parse_min_max_conjuncts(
    expr: &exprs::TExpr,
    layout: &Layout,
) -> Result<Vec<MinMaxPredicate>, String> {
    parse_min_max_conjuncts_with_column_resolver(expr, |slot_ref| {
        get_column_name_from_slot(slot_ref, layout)
    })
}

pub(crate) use super::min_max_parser::parse_min_max_conjuncts_with_column_resolver;

fn get_column_name_from_slot(
    slot_ref: &exprs::TSlotRef,
    layout: &Layout,
) -> Result<String, String> {
    let key = (slot_ref.tuple_id, slot_ref.slot_id);
    let idx = layout
        .index
        .get(&key)
        .ok_or_else(|| format!("slot not found in layout: {:?}", key))?;

    Ok(idx.to_string())
}
