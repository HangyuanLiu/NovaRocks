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
use std::time::Duration;

use crate::exec::expr::ExprArena;
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::node::limit::LimitNode;
use crate::exec::node::sort::{SortExpression, SortNode, SortTopNType};
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::novarocks_logging::warn;

use crate::common::config::exchange_wait_ms;
use crate::protocol::starrocks::decode::expr::lower_t_expr;
use crate::protocol::starrocks::decode::layout::{Layout, chunk_schema_for_layout};
use crate::protocol::starrocks::decode::node::{Lowered, local_rf_waiting_set};
use crate::runtime::exchange;
use crate::thrift::{descriptors, plan_nodes, types};

/// Lower an EXCHANGE_NODE plan node to a `Lowered` ExecNode.
///
/// This helper encapsulates both receiver and sender exchange lowering logic.
pub(crate) fn lower_exchange_node(
    children: Vec<Lowered>,
    node: &plan_nodes::TPlanNode,
    desc_tbl: &descriptors::TDescriptorTable,
    fragment_instance_id: Option<crate::common::types::UniqueId>,
    per_exchange_count: Option<i32>,
    batch_sender_counts: &std::collections::HashMap<i32, usize>,
    arena: &mut ExprArena,
    out_layout: &Layout,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
) -> Result<Lowered, String> {
    if children.is_empty() {
        let fragment_instance_id = fragment_instance_id.ok_or_else(|| {
            "EXCHANGE_NODE missing fragment instance id for exchange receiver".to_string()
        })?;

        let expected =
            resolve_exchange_sender_count(node.node_id, per_exchange_count, batch_sender_counts)?;
        if per_exchange_count.is_none() {
            warn!(
                target: "novarocks::exec",
                node_id = node.node_id,
                expected_senders = expected,
                "EXCHANGE_NODE missing per_exch_num_senders; using sender count from batch"
            );
        }

        let key = exchange::ExchangeKey {
            finst_id_hi: fragment_instance_id.hi,
            finst_id_lo: fragment_instance_id.lo,
            node_id: node.node_id,
        };
        let exchange_timeout_ms = exchange_wait_ms();

        let expected_chunk_schema = chunk_schema_for_layout(desc_tbl, out_layout)?;
        let mut out = ExecNode {
            kind: ExecNodeKind::ExchangeSource(
                ExchangeSourceNode::new(
                    key,
                    expected,
                    Duration::from_millis(exchange_timeout_ms),
                    expected_chunk_schema,
                )
                .with_local_rf_waiting_set(local_rf_waiting_set(node)),
            ),
        };

        // Some plans (e.g. global ORDER BY) use a merging exchange without an explicit SORT_NODE.
        // Use exchange_node.sort_info (if present) to produce deterministic order.
        // For non-ordering exchange, keep LIMIT/OFFSET semantics via LimitNode.
        if let Some(exch) = node.exchange_node.as_ref() {
            let offset = match exch.offset.unwrap_or(0) {
                v if v < 0 => {
                    return Err(format!("EXCHANGE_NODE offset must be >= 0, got {v}"));
                }
                v => v as usize,
            };
            if let Some(info) = exch.sort_info.as_ref() {
                let order_by = build_sort_order_by(
                    info,
                    arena,
                    out_layout,
                    &format!("EXCHANGE_NODE node_id={}", node.node_id),
                    last_query_id,
                    fe_addr,
                )?;

                let limit = if node.limit >= 0 {
                    Some(node.limit as usize)
                } else {
                    None
                };

                out = ExecNode {
                    kind: ExecNodeKind::Sort(SortNode {
                        input: Box::new(out),
                        node_id: node.node_id,
                        use_top_n: false,
                        order_by,
                        limit,
                        offset,
                        topn_type: SortTopNType::RowNumber,
                        max_buffered_rows: None,
                        max_buffered_bytes: None,
                        partition_exprs: Vec::new(),
                        partition_limit: None,
                    }),
                };
            } else if node.limit >= 0 || offset > 0 {
                out = ExecNode {
                    kind: ExecNodeKind::Limit(LimitNode {
                        input: Box::new(out),
                        node_id: node.node_id,
                        limit: (node.limit >= 0).then_some(node.limit as usize),
                        offset,
                    }),
                };
            }
        }

        Ok(Lowered {
            node: out,
            layout: out_layout.clone(),
        })
    } else {
        // Sender Exchange (if it appears in plan tree? usually it's a sink)
        // Or maybe a pass-through?
        if children.len() != 1 {
            return Err(format!(
                "EXCHANGE_NODE expected 0 or 1 child, got {}",
                children.len()
            ));
        }
        Ok(children.into_iter().next().expect("child"))
    }
}

pub(crate) fn resolve_exchange_sender_count(
    node_id: i32,
    per_exchange_count: Option<i32>,
    batch_sender_counts: &std::collections::HashMap<i32, usize>,
) -> Result<usize, String> {
    let count = match per_exchange_count {
        Some(count) => usize::try_from(count).map_err(|_| {
            format!("EXCHANGE_NODE expected_senders must be > 0, node_id={node_id}")
        })?,
        None => batch_sender_counts
            .get(&node_id)
            .copied()
            .ok_or_else(|| format!("EXCHANGE_NODE missing sender count for node_id {node_id}"))?,
    };
    if count == 0 {
        return Err(format!(
            "EXCHANGE_NODE expected_senders must be > 0, node_id={node_id}"
        ));
    }
    Ok(count)
}

fn build_sort_order_by(
    info: &plan_nodes::TSortInfo,
    arena: &mut ExprArena,
    input_layout: &Layout,
    node_label: &str,
    last_query_id: Option<&str>,
    fe_addr: Option<&crate::protocol::starrocks::decode::StarRocksExternalDependencyDraft>,
) -> Result<Vec<SortExpression>, String> {
    let key_count = info.ordering_exprs.len();
    if info.is_asc_order.len() != key_count {
        return Err(format!(
            "{node_label} sort_info.is_asc_order length mismatch: ordering_exprs={} is_asc_order={}",
            key_count,
            info.is_asc_order.len()
        ));
    }
    if info.nulls_first.len() != key_count {
        return Err(format!(
            "{node_label} sort_info.nulls_first length mismatch: ordering_exprs={} nulls_first={}",
            key_count,
            info.nulls_first.len()
        ));
    }

    let mut order_by = Vec::with_capacity(key_count);
    for (i, expr) in info.ordering_exprs.iter().enumerate() {
        let expr_id = lower_t_expr(expr, arena, input_layout, last_query_id, fe_addr)?;
        order_by.push(SortExpression {
            expr: expr_id,
            asc: info.is_asc_order[i],
            nulls_first: info.nulls_first[i],
        });
    }
    Ok(order_by)
}
