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

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::datatypes::DataType;

use super::super::expr::lower_proto_expr;
use crate::common::ids::SlotId;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::RuntimeFilterProbeSpec;
use crate::fs::object_store::{ObjectStoreConfig, apply_object_store_runtime_defaults};
use crate::fs::object_store_credentials::{ObjectStoreCredentials, ObjectStoreCredentialsSource};
use crate::proto::{common, plan};

pub(super) fn scan_output_columns(
    scan: &plan::ScanNode,
) -> Result<Vec<common::OutputColumn>, String> {
    if scan.columns.is_empty() {
        return Err("ScanNode columns are empty".to_string());
    }
    if scan.required_columns.is_empty() {
        return Ok(scan.columns.clone());
    }

    let required = scan
        .required_columns
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<HashSet<_>>();
    let output_columns = scan
        .columns
        .iter()
        .filter(|column| required.contains(&column.name.to_ascii_lowercase()))
        .cloned()
        .collect::<Vec<_>>();
    if output_columns.is_empty() {
        return Err(format!(
            "ScanNode required_columns {:?} do not match any scan columns",
            scan.required_columns
        ));
    }
    Ok(output_columns)
}

pub(super) fn column_def_data_type(column: &plan::ColumnDef) -> Result<DataType, String> {
    let desc = column
        .logical_type
        .as_ref()
        .or(column.data_type.as_ref())
        .ok_or_else(|| format!("column {} type missing", column.name))?;
    super::super::decode_type(desc)
}

pub(super) fn output_column_data_type(column: &common::OutputColumn) -> Result<DataType, String> {
    let desc = column
        .r#type
        .as_ref()
        .ok_or_else(|| format!("output column {} type missing", column.name))?;
    super::super::decode_type(desc)
}

pub(super) fn scan_batch_size(
    query_options: Option<&crate::runtime::query_options::QueryOptions>,
) -> Result<usize, String> {
    let Some(value) = query_options.and_then(|opts| opts.batch_size) else {
        return Ok(4096);
    };
    let batch_size = usize::try_from(value).map_err(|_| {
        format!("native ScanNode query_options.batch_size must be positive, got {value}")
    })?;
    if batch_size == 0 {
        return Err("native ScanNode query_options.batch_size must be positive".to_string());
    }
    Ok(batch_size)
}

pub(super) fn lower_scan_predicate(
    scan: &plan::ScanNode,
    arena: &mut ExprArena,
    layout: &super::super::layout::Layout,
) -> Result<Option<crate::exec::expr::ExprId>, String> {
    let mut predicate = None;
    for (idx, expr) in scan.predicates.iter().enumerate() {
        let expr_id = lower_proto_expr(expr, arena, layout)
            .map_err(|err| format!("ScanNode predicate {idx}: {err}"))?;
        predicate = Some(match predicate {
            Some(prev) => arena.push_typed(ExprNode::And(prev, expr_id), DataType::Boolean),
            None => expr_id,
        });
    }
    Ok(predicate)
}

pub(super) fn lower_node_probe_runtime_filter_specs(
    node: &plan::DistributedNode,
    arena: &mut ExprArena,
    layout: &super::super::layout::Layout,
) -> Result<Vec<RuntimeFilterProbeSpec>, String> {
    let mut specs = Vec::with_capacity(node.probe_runtime_filters.len());
    for probe in &node.probe_runtime_filters {
        let kind = super::runtime_filter_kind_or_join(probe.kind, "RuntimeFilterProbe")?;
        let probe_expr = probe.probe_expr.as_ref().ok_or_else(|| {
            format!(
                "RuntimeFilterProbe node_id={} filter_id={} probe_expr missing",
                node.node_id, probe.filter_id
            )
        })?;
        let expr_id = lower_proto_expr(probe_expr, arena, layout).map_err(|err| {
            format!(
                "RuntimeFilterProbe node_id={} filter_id={} probe_expr: {}",
                node.node_id, probe.filter_id, err
            )
        })?;
        let slot_id = first_expr_slot(arena, expr_id).ok_or_else(|| {
            format!(
                "RuntimeFilterProbe node_id={} filter_id={} probe_expr must reference a scan slot",
                node.node_id, probe.filter_id
            )
        })?;
        let data_type = arena.data_type(expr_id).cloned().ok_or_else(|| {
            format!(
                "RuntimeFilterProbe node_id={} filter_id={} probe_expr type missing",
                node.node_id, probe.filter_id
            )
        })?;
        specs.push(RuntimeFilterProbeSpec {
            filter_id: probe.filter_id,
            expr_id,
            slot_id,
            data_type,
            self_subtree: kind == plan::RuntimeFilterKind::Topn,
        });
    }
    Ok(specs)
}

fn first_expr_slot(arena: &ExprArena, expr_id: crate::exec::expr::ExprId) -> Option<SlotId> {
    let mut stack = vec![expr_id];
    while let Some(id) = stack.pop() {
        let Some(node) = arena.node(id) else {
            continue;
        };
        match node {
            ExprNode::SlotId(slot_id) => return Some(*slot_id),
            ExprNode::ArrayExpr { elements } => stack.extend(elements.iter().copied()),
            ExprNode::StructExpr { fields } => stack.extend(fields.iter().copied()),
            ExprNode::LambdaFunction {
                body,
                common_sub_exprs,
                ..
            } => {
                stack.push(*body);
                stack.extend(common_sub_exprs.iter().map(|(_, expr)| *expr));
            }
            ExprNode::DictDecode { child, .. }
            | ExprNode::Cast(child)
            | ExprNode::CastTime(child)
            | ExprNode::CastTimeFromDatetime(child)
            | ExprNode::Not(child)
            | ExprNode::IsNull(child)
            | ExprNode::IsNotNull(child)
            | ExprNode::Clone(child) => stack.push(*child),
            ExprNode::Add(left, right)
            | ExprNode::Sub(left, right)
            | ExprNode::Mul(left, right)
            | ExprNode::Div(left, right)
            | ExprNode::Mod(left, right)
            | ExprNode::Eq(left, right)
            | ExprNode::EqForNull(left, right)
            | ExprNode::Ne(left, right)
            | ExprNode::Lt(left, right)
            | ExprNode::Le(left, right)
            | ExprNode::Gt(left, right)
            | ExprNode::Ge(left, right)
            | ExprNode::And(left, right)
            | ExprNode::Or(left, right) => {
                stack.push(*left);
                stack.push(*right);
            }
            ExprNode::In { child, values, .. } => {
                stack.push(*child);
                stack.extend(values.iter().copied());
            }
            ExprNode::Case { children, .. } | ExprNode::FunctionCall { args: children, .. } => {
                stack.extend(children.iter().copied());
            }
            ExprNode::Literal(_) => {}
        }
    }
    None
}

pub(super) fn parse_scan_limit(limit: i64) -> Result<Option<usize>, String> {
    if limit == -1 {
        Ok(None)
    } else if limit < 0 {
        Err(format!("ScanNode limit must be -1 or >= 0, got {limit}"))
    } else {
        Ok(Some(limit as usize))
    }
}

pub(super) fn resolve_cloud_object_store_config(
    cloud_properties: &HashMap<String, String>,
) -> Result<Option<ObjectStoreConfig>, String> {
    let props = cloud_properties
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<BTreeMap<_, _>>();
    let Some(credentials) = ObjectStoreCredentials::optional_from_aws_s3_properties(
        ObjectStoreCredentialsSource::AwsS3Properties,
        &props,
    )?
    else {
        return Ok(None);
    };
    let mut cfg = credentials.to_object_store_config();
    apply_object_store_runtime_defaults(&mut cfg);
    Ok(Some(cfg))
}

pub(super) fn table_location_map(table: &plan::IcebergTableInfo) -> HashMap<i64, String> {
    let mut locations = HashMap::new();
    if !table.location.is_empty() {
        locations.insert(i64::from(table.schema_id), table.location.clone());
    }
    locations
}
