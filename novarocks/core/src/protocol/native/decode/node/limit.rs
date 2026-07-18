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

use super::DecodedNode;
use super::common::{
    check_exact_arity, merge_limits, parse_distributed_limit, parse_optional_nonnegative_i64,
};
use crate::exec::node::limit::LimitNode;
use crate::exec::node::{ExecNode, ExecNodeKind};
use crate::proto::plan;

pub(super) fn lower_limit_node(
    node: &plan::DistributedNode,
    limit_node: &plan::LimitNode,
    mut children: Vec<DecodedNode>,
) -> Result<DecodedNode, String> {
    check_exact_arity("LimitNode", 1, children.len())?;
    let child = children.pop().expect("child");
    let payload_limit = parse_optional_nonnegative_i64(limit_node.limit, "LimitNode.limit")?;
    let outer_limit = parse_distributed_limit(node.limit, "LimitNode DistributedNode.limit")?;
    let limit = merge_limits("LimitNode", payload_limit, outer_limit)?;
    let offset =
        parse_optional_nonnegative_i64(limit_node.offset, "LimitNode.offset")?.unwrap_or(0);
    Ok(DecodedNode {
        node: ExecNode {
            kind: ExecNodeKind::Limit(LimitNode {
                input: Box::new(child.node),
                node_id: node.node_id,
                limit,
                offset,
            }),
        },
        layout: child.layout,
        output_schema: child.output_schema,
    })
}
