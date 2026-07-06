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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Date32Array, Int8Array, Int16Array, Int32Array, Int64Array,
    LargeStringArray, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::DataType;
use chrono::{Datelike, NaiveDate, NaiveDateTime};

use crate::common::ids::SlotId;
use crate::connector::starrocks::lake::{
    build_sink_tablet_schema, context::PartialUpdateWriteMode,
};
use crate::connector::starrocks::sink::frontend_wire::{
    frontend_address_from_thrift, latest_frontend_address,
};
use crate::connector::starrocks::sink::partition_key::{PartitionExprPlan, PartitionKeyValue};
use crate::connector::starrocks::sink::plan::{
    FrontendAddress, SinkIndexDescriptor, SinkLocationDescriptor, SinkNodeInfo,
    SinkNodesDescriptor, SinkOutputProjectionPlan, SinkPartitionDescriptor, SinkPartitionEntry,
    SinkPartitionIndex, SinkPredicatePlan, SinkSchemaDescriptor, SinkSlotDescriptor,
    SinkTabletLocation, StarRocksSinkDescriptor, StarRocksSinkFactoryInput,
};
use crate::exec::expr::ExprArena;
use crate::exec::node::{ExecNodeKind, ExecPlan};
use crate::lower::compact::expr::lower_t_expr;
use crate::lower::compact::layout::Layout;
use crate::service::grpc_client::proto::starrocks::{KeysType, PUniqueId};
use crate::thrift::{data_sinks, descriptors, exprs, types};
use crate::types::arrow_thrift::thrift_desc_to_arrow_type;

const LOAD_OP_COLUMN: &str = "__op";
const UNIX_EPOCH_DAY_OFFSET: i32 = 719_163;

pub(crate) fn lower_starrocks_sink_factory_input(
    sink: &data_sinks::TOlapTableSink,
    output_exprs: Option<&[exprs::TExpr]>,
    exec_plan: Option<&ExecPlan>,
    layout: Option<&Layout>,
    last_query_id: Option<&str>,
    session_time_zone: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<StarRocksSinkFactoryInput, String> {
    let keys_type = lower_keys_type(sink.keys_type)?;
    let write_indexes = resolve_sink_write_index_selections(sink)?;
    let primary_schema_id = write_indexes
        .first()
        .map(|index| index.schema_id)
        .ok_or_else(|| {
            "OLAP_TABLE_SINK cannot resolve any write index from sink schema/partition metadata"
                .to_string()
        })?;
    let output_expr_slot_name_map =
        lower_output_expr_slot_name_map(&sink.schema, primary_schema_id, output_exprs)?;
    let output_expr_slot_ids = resolve_output_expr_slot_ids_for_write(output_exprs)?;
    let output_projection = lower_output_projection(
        sink,
        output_exprs,
        layout,
        session_time_zone,
        last_query_id,
        fe_addr,
    )?;
    let slot_name_overrides = if output_expr_slot_name_map.is_empty() {
        None
    } else {
        Some(&output_expr_slot_name_map)
    };
    let output_expr_slot_id_overrides = if output_projection.is_none() {
        build_output_expr_slot_id_overrides(&sink.schema.slot_descs, slot_name_overrides)?
    } else {
        HashMap::new()
    };
    let slot_id_overrides = if output_expr_slot_id_overrides.is_empty() {
        None
    } else {
        Some(&output_expr_slot_id_overrides)
    };

    let frontend = lower_frontend_address(fe_addr);
    let schema = lower_sink_schema(&sink.schema, keys_type, session_time_zone)?;
    let partition = lower_sink_partition(&sink.partition, session_time_zone, slot_id_overrides)?;
    let location = lower_sink_location(&sink.location);
    let nodes = lower_sink_nodes(&sink.nodes_info)?;
    let literal_partition_values =
        lower_literal_partition_values(sink, primary_schema_id, output_exprs, exec_plan)?;

    Ok(StarRocksSinkFactoryInput {
        name: "OLAP_TABLE_SINK".to_string(),
        descriptor: StarRocksSinkDescriptor {
            db_id: sink.db_id,
            table_id: sink.table_id,
            db_name: sink.db_name.clone(),
            table_name: sink.table_name.clone(),
            txn_id: sink.txn_id,
            load_id: PUniqueId {
                hi: sink.load_id.hi,
                lo: sink.load_id.lo,
            },
            keys_type,
            is_lake_table: sink.is_lake_table.unwrap_or(false),
            dynamic_overwrite: sink.dynamic_overwrite.unwrap_or(false),
            partial_update_mode: PartialUpdateWriteMode::from_thrift(sink.partial_update_mode),
            merge_condition: sink
                .merge_condition
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .map(ToString::to_string),
            null_expr_in_auto_increment: sink.null_expr_in_auto_increment.unwrap_or(false),
            miss_auto_increment_column: sink.miss_auto_increment_column.unwrap_or(false),
            schema,
            partition,
            location,
            nodes,
            frontend,
        },
        output_projection,
        output_expr_slot_name_map,
        output_expr_slot_ids,
        literal_partition_values,
    })
}

fn lower_frontend_address(fe_addr: Option<&types::TNetworkAddress>) -> Option<FrontendAddress> {
    fe_addr
        .map(frontend_address_from_thrift)
        .or_else(latest_frontend_address)
}

fn lower_keys_type(keys_type: Option<types::TKeysType>) -> Result<KeysType, String> {
    match keys_type.unwrap_or(types::TKeysType::DUP_KEYS) {
        t if t == types::TKeysType::DUP_KEYS => Ok(KeysType::DupKeys),
        t if t == types::TKeysType::AGG_KEYS => Ok(KeysType::AggKeys),
        t if t == types::TKeysType::PRIMARY_KEYS => Ok(KeysType::PrimaryKeys),
        t if t == types::TKeysType::UNIQUE_KEYS => Ok(KeysType::UniqueKeys),
        other => Err(format!(
            "OLAP_TABLE_SINK does not support keys_type={other:?}"
        )),
    }
}

fn lower_sink_schema(
    schema: &descriptors::TOlapTableSchemaParam,
    keys_type: KeysType,
    session_time_zone: Option<&str>,
) -> Result<SinkSchemaDescriptor, String> {
    let slot_descs = schema
        .slot_descs
        .iter()
        .map(|slot| {
            let id = match slot.id {
                Some(id) if id >= 0 => SlotId::try_from(id).ok(),
                _ => None,
            };
            SinkSlotDescriptor {
                id,
                col_name: slot.col_name.clone(),
                col_physical_name: slot.col_physical_name.clone(),
            }
        })
        .collect();

    let indexes = schema
        .indexes
        .iter()
        .map(|index| {
            let schema_id = index.schema_id.filter(|v| *v > 0).unwrap_or(index.id);
            let tablet_schema = build_sink_tablet_schema(schema, schema_id, keys_type)?;
            Ok(SinkIndexDescriptor {
                index_id: index.id,
                schema_id,
                column_names: lower_index_column_names(index),
                tablet_schema,
                column_to_expr_value: lower_column_to_expr_value(index),
                is_shadow: index.is_shadow.unwrap_or(false),
                where_clause: lower_optional_predicate(
                    index.where_clause.as_ref(),
                    session_time_zone,
                )?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(SinkSchemaDescriptor {
        slot_descs,
        indexes,
    })
}

fn lower_optional_predicate(
    expr: Option<&exprs::TExpr>,
    session_time_zone: Option<&str>,
) -> Result<Option<SinkPredicatePlan>, String> {
    let Some(expr) = expr.filter(|expr| !expr.nodes.is_empty()) else {
        return Ok(None);
    };
    let mut arena = ExprArena::default();
    arena.set_session_time_zone(session_time_zone.map(|s| s.to_string()));
    let empty_layout = Layout {
        order: Vec::new(),
        index: HashMap::new(),
    };
    let expr_id = lower_t_expr(expr, &mut arena, &empty_layout, None, None)?;
    Ok(Some(SinkPredicatePlan {
        arena: Arc::new(arena),
        expr_id,
    }))
}

fn lower_index_column_names(index: &descriptors::TOlapTableIndexSchema) -> Vec<String> {
    let mut column_names = if let Some(param) = index.column_param.as_ref() {
        param
            .columns
            .iter()
            .map(|c| c.column_name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| name.to_ascii_lowercase())
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    if column_names.is_empty() {
        column_names = index
            .columns
            .iter()
            .map(|name| name.trim())
            .filter(|name| !name.is_empty())
            .map(|name| name.to_ascii_lowercase())
            .collect::<Vec<_>>();
    }
    column_names
}

fn lower_column_to_expr_value(
    index: &descriptors::TOlapTableIndexSchema,
) -> HashMap<String, String> {
    let mut out = HashMap::new();
    if let Some(expr_map) = index.column_to_expr_value.as_ref() {
        for (key, value) in expr_map {
            let normalized_key = key.trim().to_ascii_lowercase();
            if normalized_key.is_empty() {
                continue;
            }
            out.insert(normalized_key, value.clone());
        }
    }
    out
}

fn lower_sink_partition(
    partition: &descriptors::TOlapTablePartitionParam,
    session_time_zone: Option<&str>,
    slot_id_overrides: Option<&HashMap<SlotId, SlotId>>,
) -> Result<SinkPartitionDescriptor, String> {
    let partition_exprs = if let Some(exprs) = partition.partition_exprs.as_ref()
        && !exprs.is_empty()
    {
        Some(Arc::new(lower_partition_expr_plan(
            exprs,
            session_time_zone,
            slot_id_overrides,
        )?))
    } else {
        None
    };

    let partitions = partition
        .partitions
        .iter()
        .map(|part| {
            Ok(SinkPartitionEntry {
                partition_id: part.id,
                is_shadow: part.is_shadow_partition.unwrap_or(false),
                indexes: part
                    .indexes
                    .iter()
                    .map(|index| SinkPartitionIndex {
                        index_id: index.index_id,
                        tablet_ids: index.tablet_ids.clone(),
                    })
                    .collect(),
                start_key: lower_partition_boundary_key(
                    part.start_keys.as_deref(),
                    part.start_key.as_ref(),
                )?,
                end_key: lower_partition_boundary_key(
                    part.end_keys.as_deref(),
                    part.end_key.as_ref(),
                )?,
                in_keys: lower_partition_in_keys(part.in_keys.as_deref())?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(SinkPartitionDescriptor {
        enable_automatic_partition: partition.enable_automatic_partition.unwrap_or(false),
        partition_columns: lower_partition_columns(partition),
        distributed_columns: lower_string_list(partition.distributed_columns.as_deref()),
        partition_exprs,
        partitions,
    })
}

fn lower_partition_columns(partition: &descriptors::TOlapTablePartitionParam) -> Vec<String> {
    let mut cols = partition
        .partition_columns
        .as_ref()
        .map(|values| lower_string_list(Some(values)))
        .unwrap_or_default();
    if cols.is_empty()
        && let Some(col) = partition
            .partition_column
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
    {
        cols.push(col.to_ascii_lowercase());
    }
    cols
}

fn lower_string_list(values: Option<&[String]>) -> Vec<String> {
    values
        .map(|values| {
            values
                .iter()
                .map(|value| value.trim())
                .filter(|value| !value.is_empty())
                .map(|value| value.to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn lower_partition_expr_plan(
    exprs: &[exprs::TExpr],
    session_time_zone: Option<&str>,
    slot_id_overrides: Option<&HashMap<SlotId, SlotId>>,
) -> Result<PartitionExprPlan, String> {
    let mut arena = ExprArena::default();
    arena.set_session_time_zone(session_time_zone.map(|s| s.to_string()));
    let mut expr_ids = Vec::with_capacity(exprs.len());
    let empty_layout = Layout {
        order: Vec::new(),
        index: HashMap::new(),
    };
    for expr in exprs {
        let mut rewritten_expr = expr.clone();
        if let Some(overrides) = slot_id_overrides {
            remap_partition_expr_slot_ids(&mut rewritten_expr, overrides)?;
        }
        let expr_id = lower_t_expr(&rewritten_expr, &mut arena, &empty_layout, None, None)?;
        expr_ids.push(expr_id);
    }
    Ok(PartitionExprPlan { arena, expr_ids })
}

fn remap_partition_expr_slot_ids(
    expr: &mut exprs::TExpr,
    slot_id_overrides: &HashMap<SlotId, SlotId>,
) -> Result<(), String> {
    if slot_id_overrides.is_empty() {
        return Ok(());
    }
    for (idx, node) in expr.nodes.iter_mut().enumerate() {
        if node.node_type != exprs::TExprNodeType::SLOT_REF {
            continue;
        }
        let Some(slot_ref) = node.slot_ref.as_mut() else {
            continue;
        };
        let source_slot_id = SlotId::try_from(slot_ref.slot_id).map_err(|e| {
            format!(
                "invalid partition expr slot id {} at node {}: {}",
                slot_ref.slot_id, idx, e
            )
        })?;
        let Some(target_slot_id) = slot_id_overrides.get(&source_slot_id).copied() else {
            continue;
        };
        let target_slot_i32 = i32::try_from(target_slot_id.as_u32()).map_err(|_| {
            format!(
                "partition expr remapped slot id {} exceeds i32 range",
                target_slot_id
            )
        })?;
        slot_ref.slot_id = target_slot_i32;
    }
    Ok(())
}

fn lower_sink_location(location: &descriptors::TOlapTableLocationParam) -> SinkLocationDescriptor {
    SinkLocationDescriptor {
        tablets: location
            .tablets
            .iter()
            .map(|tablet| SinkTabletLocation {
                tablet_id: tablet.tablet_id,
                node_ids: tablet.node_ids.clone(),
            })
            .collect(),
    }
}

fn lower_sink_nodes(nodes_info: &descriptors::TNodesInfo) -> Result<SinkNodesDescriptor, String> {
    let nodes = nodes_info
        .nodes
        .iter()
        .map(|node| {
            let option = i32::try_from(node.option).map_err(|_| {
                format!("OLAP_TABLE_SINK node option out of range: {}", node.option)
            })?;
            Ok(SinkNodeInfo {
                id: node.id,
                option,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(SinkNodesDescriptor { nodes })
}

pub(crate) fn lower_partition_boundary_key(
    key_nodes: Option<&[exprs::TExprNode]>,
    legacy_key_node: Option<&exprs::TExprNode>,
) -> Result<Option<Vec<PartitionKeyValue>>, String> {
    if let Some(nodes) = key_nodes {
        if nodes.is_empty() {
            return Ok(None);
        }
        return parse_partition_key_nodes(nodes).map(Some);
    }
    if let Some(node) = legacy_key_node {
        return parse_partition_key_nodes(std::slice::from_ref(node)).map(Some);
    }
    Ok(None)
}

pub(crate) fn lower_partition_in_keys(
    in_keys: Option<&[Vec<exprs::TExprNode>]>,
) -> Result<Vec<Vec<PartitionKeyValue>>, String> {
    let Some(in_keys) = in_keys else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(in_keys.len());
    for key in in_keys {
        out.push(parse_partition_key_nodes(key)?);
    }
    Ok(out)
}

fn parse_partition_key_nodes(nodes: &[exprs::TExprNode]) -> Result<Vec<PartitionKeyValue>, String> {
    let mut out = Vec::with_capacity(nodes.len());
    for node in nodes {
        out.push(parse_partition_key_node(node)?);
    }
    Ok(out)
}

fn parse_partition_key_node(node: &exprs::TExprNode) -> Result<PartitionKeyValue, String> {
    match node.node_type {
        t if t == exprs::TExprNodeType::NULL_LITERAL => Ok(PartitionKeyValue::Null),
        t if t == exprs::TExprNodeType::BOOL_LITERAL => {
            let value = node
                .bool_literal
                .as_ref()
                .ok_or_else(|| "BOOL_LITERAL missing bool_literal payload".to_string())?
                .value;
            Ok(PartitionKeyValue::Bool(value))
        }
        t if t == exprs::TExprNodeType::INT_LITERAL => {
            let value = node
                .int_literal
                .as_ref()
                .ok_or_else(|| "INT_LITERAL missing int_literal payload".to_string())?
                .value as i128;
            Ok(PartitionKeyValue::Int(value))
        }
        t if t == exprs::TExprNodeType::LARGE_INT_LITERAL => {
            let value = node
                .large_int_literal
                .as_ref()
                .ok_or_else(|| "LARGE_INT_LITERAL missing payload".to_string())?
                .value
                .trim()
                .parse::<i128>()
                .map_err(|_| "LARGE_INT_LITERAL parse failed".to_string())?;
            Ok(PartitionKeyValue::Int(value))
        }
        t if t == exprs::TExprNodeType::DECIMAL_LITERAL => {
            let text = node
                .decimal_literal
                .as_ref()
                .ok_or_else(|| "DECIMAL_LITERAL missing decimal_literal payload".to_string())?
                .value
                .clone();
            let DataType::Decimal128(precision, scale) = thrift_desc_to_arrow_type(&node.type_)
                .ok_or_else(|| {
                    "DECIMAL_LITERAL missing or unsupported type descriptor".to_string()
                })?
            else {
                return Err("DECIMAL_LITERAL type descriptor is not decimal".to_string());
            };
            let value = parse_decimal_literal_value(&text, precision, scale)?;
            Ok(PartitionKeyValue::Decimal { value, scale })
        }
        t if t == exprs::TExprNodeType::STRING_LITERAL
            || t == exprs::TExprNodeType::DATE_LITERAL =>
        {
            let value = if t == exprs::TExprNodeType::STRING_LITERAL {
                node.string_literal
                    .as_ref()
                    .ok_or_else(|| "STRING_LITERAL missing string_literal payload".to_string())?
                    .value
                    .clone()
            } else {
                node.date_literal
                    .as_ref()
                    .ok_or_else(|| "DATE_LITERAL missing date_literal payload".to_string())?
                    .value
                    .clone()
            };
            match thrift_desc_to_arrow_type(&node.type_) {
                Some(DataType::Date32) => {
                    Ok(PartitionKeyValue::Date32(parse_date_literal_days(&value)?))
                }
                Some(DataType::Timestamp(_, _)) | Some(DataType::Time64(_)) => Ok(
                    PartitionKeyValue::TimestampMicros(parse_datetime_literal_micros(&value)?),
                ),
                Some(DataType::Binary) => Ok(PartitionKeyValue::Binary(value.into_bytes())),
                _ => Ok(PartitionKeyValue::Utf8(value)),
            }
        }
        t if t == exprs::TExprNodeType::BINARY_LITERAL => {
            let value = node
                .binary_literal
                .as_ref()
                .ok_or_else(|| "BINARY_LITERAL missing payload".to_string())?
                .value
                .clone();
            Ok(PartitionKeyValue::Binary(value))
        }
        t if t == exprs::TExprNodeType::FLOAT_LITERAL => {
            let _ = node
                .float_literal
                .as_ref()
                .ok_or_else(|| "FLOAT_LITERAL missing float_literal payload".to_string())?;
            Err("unsupported partition key literal node type: FLOAT_LITERAL".to_string())
        }
        other => Err(format!(
            "unsupported partition key literal node type: {:?}",
            other
        )),
    }
}

fn parse_date_literal_days(value: &str) -> Result<i32, String> {
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        return Ok(date.num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        return Ok(dt.date().num_days_from_ce() - UNIX_EPOCH_DAY_OFFSET);
    }
    Err(format!("invalid DATE literal '{value}'"))
}

fn parse_datetime_literal_micros(value: &str) -> Result<i64, String> {
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(dt.and_utc().timestamp_micros());
    }
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| format!("invalid DATETIME literal '{value}'"))?;
        return Ok(dt.and_utc().timestamp_micros());
    }
    Err(format!("invalid DATETIME literal '{value}'"))
}

fn parse_decimal_literal_value(value: &str, precision: u8, scale: i8) -> Result<i128, String> {
    if scale < 0 {
        return Err(format!("invalid decimal scale: {scale}"));
    }
    let mut s = value.trim();
    if s.is_empty() {
        return Err("empty DECIMAL literal".to_string());
    }

    let mut sign: i128 = 1;
    if let Some(rest) = s.strip_prefix('-') {
        sign = -1;
        s = rest;
    } else if let Some(rest) = s.strip_prefix('+') {
        s = rest;
    }
    if s.is_empty() {
        return Err("empty DECIMAL literal".to_string());
    }

    let mut iter = s.split('.');
    let int_part_raw = iter.next().unwrap_or("");
    let frac_part = iter.next().unwrap_or("");
    if iter.next().is_some() {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }
    if int_part_raw.is_empty() && frac_part.is_empty() {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }

    let int_part = if int_part_raw.is_empty() {
        "0"
    } else {
        int_part_raw
    };
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("invalid DECIMAL literal '{value}'"));
    }

    let scale_usize = scale as usize;
    if frac_part.len() > scale_usize {
        return Err(format!(
            "DECIMAL literal '{}' exceeds scale {}",
            value, scale_usize
        ));
    }

    let mut digits = String::with_capacity(int_part.len() + scale_usize);
    digits.push_str(int_part);
    digits.push_str(frac_part);
    for _ in 0..(scale_usize - frac_part.len()) {
        digits.push('0');
    }

    let digits_trim = digits.trim_start_matches('0');
    let digits_final = if digits_trim.is_empty() {
        "0"
    } else {
        digits_trim
    };
    if digits_final.len() > precision as usize {
        return Err(format!(
            "DECIMAL literal '{}' exceeds precision {}",
            value, precision
        ));
    }

    let unsigned = digits_final
        .parse::<i128>()
        .map_err(|_| format!("failed to parse DECIMAL literal '{value}'"))?;
    Ok(unsigned.saturating_mul(sign))
}

fn lower_output_projection(
    sink: &data_sinks::TOlapTableSink,
    output_exprs: Option<&[exprs::TExpr]>,
    layout: Option<&Layout>,
    session_time_zone: Option<&str>,
    last_query_id: Option<&str>,
    fe_addr: Option<&types::TNetworkAddress>,
) -> Result<Option<SinkOutputProjectionPlan>, String> {
    let Some(output_exprs) = output_exprs.filter(|exprs| !exprs.is_empty()) else {
        return Ok(None);
    };
    let has_index_where_clause = sink.schema.indexes.iter().any(|index| {
        index
            .where_clause
            .as_ref()
            .is_some_and(|expr| !expr.nodes.is_empty())
    });
    if output_exprs_are_plain_slot_refs(output_exprs) && !has_index_where_clause {
        return Ok(None);
    }
    let layout = layout.ok_or_else(|| {
        "OLAP_TABLE_SINK requires layout for output expression projection".to_string()
    })?;
    let output_slots = resolve_output_projection_slots(sink, output_exprs)?;
    if output_slots.len() != output_exprs.len() {
        return Err(format!(
            "OLAP_TABLE_SINK output projection slot count mismatch: slots={} output_exprs={}",
            output_slots.len(),
            output_exprs.len()
        ));
    }

    let mut arena = ExprArena::default();
    arena.set_session_time_zone(session_time_zone.map(|s| s.to_string()));
    let mut expr_ids = Vec::with_capacity(output_exprs.len());
    for expr in output_exprs {
        let expr_id = lower_t_expr(expr, &mut arena, layout, last_query_id, fe_addr)?;
        expr_ids.push(expr_id);
    }
    let (output_slot_ids, output_field_names): (Vec<_>, Vec<_>) = output_slots.into_iter().unzip();
    Ok(Some(SinkOutputProjectionPlan {
        arena: Arc::new(arena),
        expr_ids,
        output_slot_ids,
        output_field_names,
    }))
}

fn output_exprs_are_plain_slot_refs(output_exprs: &[exprs::TExpr]) -> bool {
    output_exprs.iter().all(|expr| {
        expr.nodes.len() == 1
            && expr.nodes.first().is_some_and(|node| {
                node.node_type == exprs::TExprNodeType::SLOT_REF && node.slot_ref.is_some()
            })
    })
}

fn resolve_output_projection_slots(
    sink: &data_sinks::TOlapTableSink,
    output_exprs: &[exprs::TExpr],
) -> Result<Vec<(SlotId, String)>, String> {
    if let Some(mapped) = resolve_slots_from_expr_output_column(sink, output_exprs)? {
        return Ok(mapped);
    }

    let collect_named_slots = |skip_load_op: bool| -> Result<Vec<(SlotId, String)>, String> {
        let mut out = Vec::new();
        for (idx, slot_desc) in sink.schema.slot_descs.iter().enumerate() {
            let Some(raw_name) = slot_desc
                .col_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            else {
                continue;
            };
            if skip_load_op && raw_name.eq_ignore_ascii_case(LOAD_OP_COLUMN) {
                continue;
            }
            let slot_id = slot_desc.id.ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK schema.slot_descs[{}] missing id while resolving output projection slots",
                    idx
                )
            })?;
            if slot_id < 0 {
                return Err(format!(
                    "OLAP_TABLE_SINK schema.slot_descs[{}] has invalid id {} while resolving output projection slots",
                    idx, slot_id
                ));
            }
            let slot_id = SlotId::try_from(slot_id)?;
            let name = if raw_name.eq_ignore_ascii_case(LOAD_OP_COLUMN) {
                LOAD_OP_COLUMN.to_string()
            } else {
                raw_name.to_string()
            };
            out.push((slot_id, name));
        }
        Ok(out)
    };

    let named_slot_count = sink
        .schema
        .slot_descs
        .iter()
        .filter(|slot| {
            slot.col_name
                .as_deref()
                .map(str::trim)
                .is_some_and(|name| !name.is_empty())
        })
        .count();
    let skip_load_op = output_exprs.len() < named_slot_count;
    let named_slots = collect_named_slots(skip_load_op)?;
    if named_slots.len() == output_exprs.len() {
        return Ok(named_slots);
    }

    let mut ordinal_slots = Vec::new();
    for (idx, slot_desc) in sink.schema.slot_descs.iter().enumerate() {
        let Some(id) = slot_desc.id.filter(|id| *id >= 0) else {
            continue;
        };
        let slot_id = SlotId::try_from(id)?;
        let name = slot_desc
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|name| {
                if name.eq_ignore_ascii_case(LOAD_OP_COLUMN) {
                    LOAD_OP_COLUMN.to_string()
                } else {
                    name.to_string()
                }
            })
            .unwrap_or_else(|| format!("col_{idx}"));
        ordinal_slots.push((slot_id, name));
    }
    if ordinal_slots.len() == output_exprs.len() {
        return Ok(ordinal_slots);
    }

    let mut slot_name_by_id = HashMap::new();
    for (slot_id, name) in &ordinal_slots {
        slot_name_by_id
            .entry(*slot_id)
            .or_insert_with(|| name.clone());
    }
    let mut expr_slots = Vec::with_capacity(output_exprs.len());
    for (idx, expr) in output_exprs.iter().enumerate() {
        let root = expr.nodes.first().ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK output_exprs[{}] is empty while resolving output projection slots",
                idx
            )
        })?;
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            return Err(format!(
                "OLAP_TABLE_SINK cannot resolve output projection slot for output_exprs[{}] node_type={:?}",
                idx, root.node_type
            ));
        }
        let slot_ref = root.slot_ref.as_ref().ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK output_exprs[{}] SLOT_REF missing slot_ref payload",
                idx
            )
        })?;
        let slot_id = SlotId::try_from(slot_ref.slot_id)?;
        let name = slot_name_by_id
            .get(&slot_id)
            .cloned()
            .unwrap_or_else(|| format!("col_{idx}"));
        expr_slots.push((slot_id, name));
    }
    Ok(expr_slots)
}

fn resolve_slots_from_expr_output_column(
    sink: &data_sinks::TOlapTableSink,
    output_exprs: &[exprs::TExpr],
) -> Result<Option<Vec<(SlotId, String)>>, String> {
    if output_exprs.is_empty() {
        return Ok(Some(Vec::new()));
    }

    let mut out = Vec::with_capacity(output_exprs.len());
    for (expr_idx, expr) in output_exprs.iter().enumerate() {
        let Some(root) = expr.nodes.first() else {
            return Ok(None);
        };
        let Some(output_column) = root.output_column else {
            return Ok(None);
        };
        if output_column < 0 {
            return Ok(None);
        }
        let output_idx = usize::try_from(output_column).map_err(|_| {
            format!(
                "OLAP_TABLE_SINK output_exprs[{}] has invalid output_column={}",
                expr_idx, output_column
            )
        })?;
        let Some(slot_desc) = sink.schema.slot_descs.get(output_idx) else {
            return Ok(None);
        };
        let Some(slot_id_i32) = slot_desc.id else {
            return Ok(None);
        };
        if slot_id_i32 < 0 {
            return Ok(None);
        }
        let slot_id = SlotId::try_from(slot_id_i32)?;
        let name = slot_desc
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|name| {
                if name.eq_ignore_ascii_case(LOAD_OP_COLUMN) {
                    LOAD_OP_COLUMN.to_string()
                } else {
                    name.to_string()
                }
            })
            .unwrap_or_else(|| format!("col_{output_idx}"));
        out.push((slot_id, name));
    }

    Ok(Some(out))
}

fn lower_output_expr_slot_name_map(
    schema: &descriptors::TOlapTableSchemaParam,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<HashMap<String, SlotId>, String> {
    build_output_expr_slot_name_map_for_write(schema, schema_id, output_exprs)
}

fn build_output_expr_slot_name_map_for_write(
    schema: &descriptors::TOlapTableSchemaParam,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<HashMap<String, SlotId>, String> {
    let Some(output_exprs) = output_exprs else {
        return Ok(HashMap::new());
    };
    if output_exprs.is_empty() {
        return Ok(HashMap::new());
    }
    let mut slot_map = HashMap::new();
    let named_slot_count = schema
        .slot_descs
        .iter()
        .filter(|slot| {
            slot.col_name
                .as_deref()
                .map(str::trim)
                .is_some_and(|name| !name.is_empty())
        })
        .count();
    let skip_load_op = output_exprs.len() < named_slot_count;
    let mut expr_iter = output_exprs.iter();
    for slot_desc in &schema.slot_descs {
        let Some(column_name) = slot_desc
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(str::to_ascii_lowercase)
        else {
            continue;
        };
        if skip_load_op && column_name == LOAD_OP_COLUMN {
            continue;
        }
        let Some(expr) = expr_iter.next() else {
            break;
        };
        let Some(root) = expr.nodes.first() else {
            continue;
        };
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            continue;
        }
        let Some(slot_ref) = root.slot_ref.as_ref() else {
            continue;
        };
        let Ok(slot_id) = SlotId::try_from(slot_ref.slot_id) else {
            continue;
        };
        slot_map.insert(column_name, slot_id);
    }
    if !slot_map.is_empty() {
        return Ok(slot_map);
    }

    let column_names = resolve_index_column_names_for_write(schema, schema_id)?;
    if column_names.is_empty() {
        return Ok(HashMap::new());
    }

    for (column_name, expr) in column_names.iter().zip(output_exprs.iter()) {
        let Some(root) = expr.nodes.first() else {
            continue;
        };
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            continue;
        }
        let Some(slot_ref) = root.slot_ref.as_ref() else {
            continue;
        };
        let Ok(slot_id) = SlotId::try_from(slot_ref.slot_id) else {
            continue;
        };
        slot_map.insert(column_name.clone(), slot_id);
    }
    Ok(slot_map)
}

fn build_output_expr_slot_id_overrides(
    slot_descs: &[descriptors::TSlotDescriptor],
    slot_name_overrides: Option<&HashMap<String, SlotId>>,
) -> Result<HashMap<SlotId, SlotId>, String> {
    let Some(slot_name_overrides) = slot_name_overrides else {
        return Ok(HashMap::new());
    };
    if slot_name_overrides.is_empty() {
        return Ok(HashMap::new());
    }

    let schema_slot_by_name = build_slot_name_map(slot_descs)?;
    let mut slot_id_overrides = HashMap::new();
    for (column_name, schema_slot_id) in schema_slot_by_name {
        let Some(output_slot_id) = slot_name_overrides.get(&column_name).copied() else {
            continue;
        };
        if output_slot_id != schema_slot_id {
            slot_id_overrides.insert(schema_slot_id, output_slot_id);
        }
    }
    Ok(slot_id_overrides)
}

fn resolve_output_expr_slot_ids_for_write(
    output_exprs: Option<&[exprs::TExpr]>,
) -> Result<Vec<Option<SlotId>>, String> {
    let Some(output_exprs) = output_exprs else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity(output_exprs.len());
    for expr in output_exprs {
        let Some(root) = expr.nodes.first() else {
            out.push(None);
            continue;
        };
        if root.node_type != exprs::TExprNodeType::SLOT_REF {
            out.push(None);
            continue;
        }
        let Some(slot_ref) = root.slot_ref.as_ref() else {
            out.push(None);
            continue;
        };
        out.push(SlotId::try_from(slot_ref.slot_id).ok());
    }
    Ok(out)
}

fn build_slot_name_map(
    slot_descs: &[descriptors::TSlotDescriptor],
) -> Result<HashMap<String, SlotId>, String> {
    let mut slot_by_name = HashMap::new();
    for slot in slot_descs {
        let Some(id) = slot.id.filter(|id| *id >= 0) else {
            continue;
        };
        let slot_id = SlotId::try_from(id)?;
        if let Some(name) = slot
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            slot_by_name.insert(name.to_ascii_lowercase(), slot_id);
        }
        if let Some(physical_name) = slot
            .col_physical_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            slot_by_name.insert(physical_name.to_ascii_lowercase(), slot_id);
        }
    }
    Ok(slot_by_name)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SinkWriteIndexSelection {
    index_id: i64,
    schema_id: i64,
}

fn resolve_sink_write_index_selections(
    sink: &data_sinks::TOlapTableSink,
) -> Result<Vec<SinkWriteIndexSelection>, String> {
    let mut schema_id_by_index_id = HashMap::<i64, i64>::new();
    let mut index_by_id = HashMap::<i64, &descriptors::TOlapTableIndexSchema>::new();
    for index in &sink.schema.indexes {
        if index.id <= 0 {
            continue;
        }
        let schema_id = index.schema_id.filter(|v| *v > 0).unwrap_or(index.id);
        if schema_id <= 0 {
            return Err(format!(
                "OLAP_TABLE_SINK schema.indexes contains non-positive schema_id/index_id: index_id={} schema_id={}",
                index.id, schema_id
            ));
        }
        schema_id_by_index_id.insert(index.id, schema_id);
        index_by_id.insert(index.id, index);
    }
    if schema_id_by_index_id.is_empty() {
        return Err("OLAP_TABLE_SINK schema.indexes has no valid index_id/schema_id".to_string());
    }

    let mut candidate_index_ids = BTreeSet::<i64>::new();
    for partition in sink
        .partition
        .partitions
        .iter()
        .filter(|part| !part.is_shadow_partition.unwrap_or(false))
    {
        for index in &partition.indexes {
            if index.index_id > 0 {
                candidate_index_ids.insert(index.index_id);
            }
        }
    }
    if candidate_index_ids.is_empty() {
        let fallback_schema_id = resolve_schema_id(&sink.schema)?;
        for index in &sink.schema.indexes {
            let schema_id = index.schema_id.filter(|v| *v > 0).unwrap_or(index.id);
            if schema_id == fallback_schema_id && index.id > 0 {
                candidate_index_ids.insert(index.id);
            }
        }
    }
    if candidate_index_ids.is_empty() {
        return Err(
            "OLAP_TABLE_SINK cannot resolve candidate write index ids from partition/schema metadata"
                .to_string(),
        );
    }

    let slot_names = sink
        .schema
        .slot_descs
        .iter()
        .filter_map(|slot| {
            slot.col_name
                .as_deref()
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(|name| name.to_ascii_lowercase())
        })
        .filter(|name| name != LOAD_OP_COLUMN)
        .collect::<HashSet<_>>();

    let mut scored = Vec::<(i64, i64, bool, usize, usize)>::new();
    for index_id in candidate_index_ids {
        let schema_id = schema_id_by_index_id
            .get(&index_id)
            .copied()
            .ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK partition index_id={} is absent in schema.indexes",
                    index_id
                )
            })?;
        let index = index_by_id.get(&index_id).copied().ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK cannot resolve schema index for index_id={}",
                index_id
            )
        })?;
        let index_columns = lower_index_column_names(index);
        let overlap = if slot_names.is_empty() {
            0
        } else {
            index_columns
                .iter()
                .filter(|name| slot_names.contains(*name))
                .count()
        };
        scored.push((
            index_id,
            schema_id,
            index.is_shadow.unwrap_or(false),
            overlap,
            index_columns.len(),
        ));
    }
    if scored.is_empty() {
        return Err("OLAP_TABLE_SINK candidate write indexes are empty".to_string());
    }

    scored.sort_by(|left, right| {
        let left_shadow = if left.2 { 1 } else { 0 };
        let right_shadow = if right.2 { 1 } else { 0 };
        left_shadow
            .cmp(&right_shadow)
            .then(right.3.cmp(&left.3))
            .then(right.4.cmp(&left.4))
            .then(left.0.cmp(&right.0))
    });
    let primary_index_id = scored
        .first()
        .map(|item| item.0)
        .ok_or_else(|| "OLAP_TABLE_SINK cannot select primary write index".to_string())?;

    let mut out = Vec::with_capacity(scored.len());
    out.push(SinkWriteIndexSelection {
        index_id: primary_index_id,
        schema_id: schema_id_by_index_id
            .get(&primary_index_id)
            .copied()
            .ok_or_else(|| {
                format!(
                    "OLAP_TABLE_SINK missing schema_id for primary write index_id={}",
                    primary_index_id
                )
            })?,
    });

    let mut rest = scored
        .into_iter()
        .filter(|item| item.0 != primary_index_id)
        .map(|item| SinkWriteIndexSelection {
            index_id: item.0,
            schema_id: item.1,
        })
        .collect::<Vec<_>>();
    rest.sort_by_key(|item| item.index_id);
    out.extend(rest);
    Ok(out)
}

fn resolve_schema_id(schema: &descriptors::TOlapTableSchemaParam) -> Result<i64, String> {
    for index in &schema.indexes {
        if let Some(schema_id) = index.schema_id.filter(|v| *v > 0) {
            return Ok(schema_id);
        }
        if index.id > 0 {
            return Ok(index.id);
        }
    }
    Err("OLAP_TABLE_SINK schema.indexes has no valid schema_id".to_string())
}

fn resolve_index_column_names_for_write(
    schema: &descriptors::TOlapTableSchemaParam,
    schema_id: i64,
) -> Result<Vec<String>, String> {
    let index = schema
        .indexes
        .iter()
        .find(|idx| idx.schema_id.filter(|v| *v > 0).unwrap_or(idx.id) == schema_id)
        .ok_or_else(|| {
            format!("OLAP_TABLE_SINK cannot resolve schema index for schema_id={schema_id}")
        })?;
    Ok(lower_index_column_names(index))
}

fn lower_literal_partition_values(
    sink: &data_sinks::TOlapTableSink,
    schema_id: i64,
    output_exprs: Option<&[exprs::TExpr]>,
    exec_plan: Option<&ExecPlan>,
) -> Result<Option<Vec<String>>, String> {
    if !sink.partition.enable_automatic_partition.unwrap_or(false) {
        return Ok(None);
    }
    if sink
        .partition
        .partition_exprs
        .as_ref()
        .is_some_and(|exprs| !exprs.is_empty())
    {
        return Ok(None);
    }

    let partition_columns = lower_partition_columns(&sink.partition);
    if partition_columns.is_empty() {
        return Ok(None);
    }

    let Some(output_exprs) = output_exprs else {
        return Ok(None);
    };
    let index_columns = resolve_index_column_names_for_write(&sink.schema, schema_id)?;
    if index_columns.is_empty() {
        return Ok(None);
    }

    let mut partition_values = Vec::with_capacity(partition_columns.len());
    for partition_col in &partition_columns {
        let Some(column_idx) = index_columns.iter().position(|name| name == partition_col) else {
            return Ok(None);
        };
        let Some(expr) = output_exprs.get(column_idx) else {
            return Ok(None);
        };
        let Some(value) = extract_partition_literal_value(expr)
            .or_else(|| extract_partition_value_from_exec_plan(expr, exec_plan))
        else {
            return Ok(None);
        };
        partition_values.push(value);
    }

    Ok(Some(partition_values))
}

fn extract_partition_literal_value(expr: &exprs::TExpr) -> Option<String> {
    for node in &expr.nodes {
        let ty = node.node_type;
        if ty == exprs::TExprNodeType::STRING_LITERAL {
            return node.string_literal.as_ref().map(|v| v.value.clone());
        }
        if ty == exprs::TExprNodeType::DATE_LITERAL {
            return node.date_literal.as_ref().map(|v| v.value.clone());
        }
        if ty == exprs::TExprNodeType::INT_LITERAL {
            return node.int_literal.as_ref().map(|v| v.value.to_string());
        }
        if ty == exprs::TExprNodeType::LARGE_INT_LITERAL {
            return node.large_int_literal.as_ref().map(|v| v.value.clone());
        }
        if ty == exprs::TExprNodeType::BOOL_LITERAL {
            return node.bool_literal.as_ref().map(|v| {
                if v.value {
                    "TRUE".to_string()
                } else {
                    "FALSE".to_string()
                }
            });
        }
    }
    None
}

fn extract_partition_value_from_exec_plan(
    expr: &exprs::TExpr,
    exec_plan: Option<&ExecPlan>,
) -> Option<String> {
    let exec_plan = exec_plan?;
    let root = expr.nodes.first()?;
    if root.node_type != exprs::TExprNodeType::SLOT_REF {
        return None;
    }
    let slot_ref = root.slot_ref.as_ref()?;
    let output_slot_id = SlotId::try_from(slot_ref.slot_id).ok()?;

    let project = match &exec_plan.root.kind {
        ExecNodeKind::Project(project) => project,
        _ => return None,
    };
    let values = match &project.input.kind {
        ExecNodeKind::Values(values) => values,
        _ => return None,
    };
    if values.chunk.is_empty() {
        return None;
    }

    let output_pos = project
        .output_chunk_schema
        .slot_ids()
        .iter()
        .position(|slot| *slot == output_slot_id)?;
    let expr_idx = project
        .output_indices
        .as_ref()
        .and_then(|indices| indices.get(output_pos).copied())
        .unwrap_or(output_pos);
    let expr_id = *project.exprs.get(expr_idx)?;
    let array = exec_plan.arena.eval(expr_id, &values.chunk).ok()?;
    if array.is_null(0) {
        return None;
    }
    scalar_partition_value_to_string(array.as_ref(), 0)
}

fn scalar_partition_value_to_string(array: &dyn Array, row: usize) -> Option<String> {
    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|v| v.value(row).to_string()),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|v| v.value(row).to_string()),
        DataType::Int8 => array
            .as_any()
            .downcast_ref::<Int8Array>()
            .map(|v| v.value(row).to_string()),
        DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|v| v.value(row).to_string()),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|v| v.value(row).to_string()),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|v| v.value(row).to_string()),
        DataType::UInt8 => array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .map(|v| v.value(row).to_string()),
        DataType::UInt16 => array
            .as_any()
            .downcast_ref::<UInt16Array>()
            .map(|v| v.value(row).to_string()),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|v| v.value(row).to_string()),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|v| v.value(row).to_string()),
        DataType::Boolean => array.as_any().downcast_ref::<BooleanArray>().map(|v| {
            if v.value(row) {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }),
        DataType::Date32 => {
            let days_since_epoch = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .map(|v| v.value(row))?;
            let days_from_ce = UNIX_EPOCH_DAY_OFFSET.checked_add(days_since_epoch)?;
            let date = NaiveDate::from_num_days_from_ce_opt(days_from_ce)?;
            Some(date.format("%Y-%m-%d").to_string())
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeMap, HashMap};

    use crate::connector::starrocks::lake::context::PartialUpdateWriteMode;
    use crate::connector::starrocks::sink::partition_key::PartitionKeyValue;
    use crate::exec::expr::ExprNode;
    use crate::lower::compact::layout::Layout;
    use crate::service::grpc_client::proto::starrocks::KeysType;
    use crate::thrift::{data_sinks, descriptors, exprs, types};

    #[test]
    fn lower_keys_type_maps_known_values_and_rejects_unknown() {
        assert_eq!(lower_keys_type(None).unwrap(), KeysType::DupKeys);
        assert_eq!(
            lower_keys_type(Some(types::TKeysType::DUP_KEYS)).unwrap(),
            KeysType::DupKeys
        );
        assert_eq!(
            lower_keys_type(Some(types::TKeysType::AGG_KEYS)).unwrap(),
            KeysType::AggKeys
        );
        assert_eq!(
            lower_keys_type(Some(types::TKeysType::PRIMARY_KEYS)).unwrap(),
            KeysType::PrimaryKeys
        );
        assert_eq!(
            lower_keys_type(Some(types::TKeysType::UNIQUE_KEYS)).unwrap(),
            KeysType::UniqueKeys
        );

        let err = lower_keys_type(Some(types::TKeysType::from(999))).unwrap_err();
        assert!(err.contains("keys_type"));
    }

    #[test]
    fn lower_partition_literals_handles_decimal_date_and_string() {
        let decimal = decimal_literal("12.30", 10, 2);
        let date = date_literal("1970-01-02", types::TPrimitiveType::DATE);
        let string = string_literal("west");

        let values = lower_partition_boundary_key(Some(&[decimal, date, string]), None)
            .unwrap()
            .unwrap();

        assert_eq!(
            values,
            vec![
                PartitionKeyValue::Decimal {
                    value: 1230,
                    scale: 2
                },
                PartitionKeyValue::Date32(1),
                PartitionKeyValue::Utf8("west".to_string())
            ]
        );
    }

    #[test]
    fn lower_output_projection_keeps_slot_ref_fast_path_unless_where_clause_exists() {
        let output_exprs = vec![slot_ref_expr(1), slot_ref_expr(2)];
        let sink_without_where = test_sink(vec![index_schema(None)], None, None);
        let fast_path = lower_starrocks_sink_factory_input(
            &sink_without_where,
            Some(&output_exprs),
            None,
            Some(&layout_for_slots(&[1, 2])),
            None,
            Some("UTC"),
            None,
        )
        .unwrap();
        assert!(fast_path.output_projection.is_none());

        let sink_with_where = test_sink(vec![index_schema(Some(bool_expr(true)))], None, None);
        let projected = lower_starrocks_sink_factory_input(
            &sink_with_where,
            Some(&output_exprs),
            None,
            Some(&layout_for_slots(&[1, 2])),
            None,
            Some("UTC"),
            None,
        )
        .unwrap();
        let projection = projected
            .output_projection
            .expect("where clause forces projection");
        assert_eq!(
            projection.output_slot_ids,
            vec![SlotId::new(1), SlotId::new(2)]
        );
        assert_eq!(
            projection.output_field_names,
            vec!["k".to_string(), "v".to_string()]
        );
    }

    #[test]
    fn lower_partition_expr_keeps_sink_slots_when_projection_materializes() {
        let output_exprs = vec![slot_ref_expr(100), slot_ref_expr(200)];
        let mut sink = test_sink(vec![index_schema(Some(bool_expr(true)))], None, None);
        sink.partition.partition_exprs = Some(vec![slot_ref_expr(1)]);

        let input = lower_starrocks_sink_factory_input(
            &sink,
            Some(&output_exprs),
            None,
            Some(&layout_for_slots(&[100, 200])),
            None,
            Some("UTC"),
            None,
        )
        .unwrap();

        assert!(input.output_projection.is_some());
        let partition_exprs = input
            .descriptor
            .partition
            .partition_exprs
            .expect("partition exprs");
        let expr_id = partition_exprs.expr_ids[0];
        match partition_exprs.arena.node(expr_id).expect("partition expr") {
            ExprNode::SlotId(slot) => assert_eq!(*slot, SlotId::new(1)),
            other => panic!("expected slot expr, got {other:?}"),
        }
    }

    #[test]
    fn lower_literal_partition_values_use_primary_write_index() {
        let mut shadow = index_schema_with_columns(70, 71, &["V", "K"], None, true);
        shadow.column_to_expr_value = Some(BTreeMap::new());
        let primary = index_schema_with_columns(72, 73, &["K", "V"], None, false);
        let mut sink = test_sink(vec![shadow, primary], None, None);
        sink.partition.enable_automatic_partition = Some(true);
        sink.partition.partitions[0].indexes = vec![
            descriptors::TOlapTableIndexTablets::new(
                70,
                vec![80],
                None::<Vec<descriptors::TOlapTableTablet>>,
            ),
            descriptors::TOlapTableIndexTablets::new(
                72,
                vec![81],
                None::<Vec<descriptors::TOlapTableTablet>>,
            ),
        ];
        let output_exprs = vec![string_expr("west"), string_expr("wrong")];

        let input = lower_starrocks_sink_factory_input(
            &sink,
            Some(&output_exprs),
            None,
            Some(&layout_for_slots(&[])),
            None,
            Some("UTC"),
            None,
        )
        .unwrap();

        assert_eq!(
            input.literal_partition_values.as_deref(),
            Some(&["west".to_string()][..])
        );
    }

    #[test]
    fn lower_descriptor_preserves_partial_update_and_index_metadata() {
        let mut expr_values = BTreeMap::new();
        expr_values.insert(" V ".to_string(), "coalesce(v, 0)".to_string());
        expr_values.insert("   ".to_string(), "ignored".to_string());

        let mut index = index_schema(None);
        index.column_to_expr_value = Some(expr_values);
        index.is_shadow = Some(true);
        let sink = test_sink(
            vec![index],
            Some("  version >= 7  ".to_string()),
            Some(types::TPartialUpdateMode::COLUMN_UPDATE_MODE),
        );

        let input =
            lower_starrocks_sink_factory_input(&sink, None, None, None, None, Some("UTC"), None)
                .unwrap();

        assert_eq!(input.name, "OLAP_TABLE_SINK");
        assert!(matches!(
            input.descriptor.partial_update_mode,
            PartialUpdateWriteMode::ColumnUpdate
        ));
        assert_eq!(
            input.descriptor.merge_condition.as_deref(),
            Some("version >= 7")
        );
        let index = &input.descriptor.schema.indexes[0];
        assert!(index.is_shadow);
        assert_eq!(index.tablet_schema.column.len(), 2);
        assert_eq!(
            index.tablet_schema.keys_type,
            Some(KeysType::PrimaryKeys as i32)
        );
        assert_eq!(
            index.column_to_expr_value.get("v").map(String::as_str),
            Some("coalesce(v, 0)")
        );
        assert!(!index.column_to_expr_value.contains_key(""));
    }

    #[test]
    fn lower_frontend_address_falls_back_to_latest_disk_report() {
        crate::service::disk_report::set_fe_addr_for_test(Some(types::TNetworkAddress::new(
            "fallback-fe".to_string(),
            9010,
        )));

        let direct = types::TNetworkAddress::new("direct-fe".to_string(), 9020);
        let direct_addr = lower_frontend_address(Some(&direct)).expect("direct frontend");
        assert_eq!(direct_addr.hostname, "direct-fe");
        assert_eq!(direct_addr.port, 9020);

        let fallback_addr = lower_frontend_address(None).expect("fallback frontend");
        assert_eq!(fallback_addr.hostname, "fallback-fe");
        assert_eq!(fallback_addr.port, 9010);

        let sink = test_sink(vec![index_schema(None)], None, None);
        let input =
            lower_starrocks_sink_factory_input(&sink, None, None, None, None, Some("UTC"), None)
                .expect("sink input");
        let descriptor_addr = input.descriptor.frontend.expect("descriptor frontend");
        assert_eq!(descriptor_addr.hostname, "fallback-fe");
        assert_eq!(descriptor_addr.port, 9010);

        crate::service::disk_report::set_fe_addr_for_test(None);
    }

    fn test_sink(
        indexes: Vec<descriptors::TOlapTableIndexSchema>,
        merge_condition: Option<String>,
        partial_update_mode: Option<types::TPartialUpdateMode>,
    ) -> data_sinks::TOlapTableSink {
        data_sinks::TOlapTableSink {
            load_id: types::TUniqueId::new(10, 20),
            txn_id: 30,
            db_id: 40,
            table_id: 50,
            tuple_id: 0,
            num_replicas: 1,
            need_gen_rollup: false,
            db_name: Some("db".to_string()),
            table_name: Some("tbl".to_string()),
            schema: descriptors::TOlapTableSchemaParam::new(
                40,
                50,
                1,
                vec![slot_desc(1, "k"), slot_desc(2, "v")],
                descriptors::TTupleDescriptor::new(Some(0), Some(0), Some(0), Some(50), Some(0)),
                indexes,
            ),
            partition: descriptors::TOlapTablePartitionParam::new(
                40,
                50,
                1,
                None::<String>,
                Some(vec!["k".to_string()]),
                vec![descriptors::TOlapTablePartition::new(
                    60,
                    None::<exprs::TExprNode>,
                    None::<exprs::TExprNode>,
                    None::<i32>,
                    vec![descriptors::TOlapTableIndexTablets::new(
                        70,
                        vec![80],
                        None::<Vec<descriptors::TOlapTableTablet>>,
                    )],
                    None::<Vec<exprs::TExprNode>>,
                    None::<Vec<exprs::TExprNode>>,
                    None::<Vec<Vec<exprs::TExprNode>>>,
                    Some(false),
                )],
                Some(vec!["k".to_string()]),
                None::<Vec<exprs::TExpr>>,
                Some(false),
                None::<descriptors::TOlapTableDistributionType>,
            ),
            location: descriptors::TOlapTableLocationParam::new(
                40,
                50,
                1,
                vec![descriptors::TTabletLocation::new(80, vec![90])],
            ),
            nodes_info: descriptors::TNodesInfo::new(
                1,
                vec![descriptors::TNodeInfo::new(
                    90,
                    0,
                    "127.0.0.1".to_string(),
                    8060,
                )],
            ),
            load_channel_timeout_s: None,
            is_lake_table: Some(true),
            txn_trace_parent: None,
            keys_type: Some(types::TKeysType::PRIMARY_KEYS),
            write_quorum_type: None,
            enable_replicated_storage: None,
            merge_condition,
            null_expr_in_auto_increment: Some(false),
            miss_auto_increment_column: Some(false),
            abort_delete: None,
            auto_increment_slot_id: None,
            partial_update_mode,
            label: None,
            enable_colocate_mv_index: None,
            automatic_bucket_size: None,
            write_txn_log: None,
            ignore_out_of_partition: None,
            encryption_meta: None,
            dynamic_overwrite: Some(false),
            enable_data_file_bundling: None,
            is_multi_statements_txn: None,
        }
    }

    fn index_schema(where_clause: Option<exprs::TExpr>) -> descriptors::TOlapTableIndexSchema {
        index_schema_with_columns(70, 71, &["K", "V"], where_clause, false)
    }

    fn index_schema_with_columns(
        index_id: i64,
        schema_id: i64,
        columns: &[&str],
        where_clause: Option<exprs::TExpr>,
        is_shadow: bool,
    ) -> descriptors::TOlapTableIndexSchema {
        descriptors::TOlapTableIndexSchema::new(
            index_id,
            columns.iter().map(|name| (*name).to_string()).collect(),
            0,
            Some(column_param(columns)),
            where_clause,
            Some(schema_id),
            None::<BTreeMap<String, String>>,
            Some(is_shadow),
        )
    }

    fn column_param(columns: &[&str]) -> descriptors::TOlapTableColumnParam {
        descriptors::TOlapTableColumnParam {
            columns: columns
                .iter()
                .enumerate()
                .map(|(idx, name)| table_column(name, idx == 0, idx as i32))
                .collect(),
            sort_key_uid: vec![0],
            short_key_column_count: 1,
        }
    }

    fn table_column(name: &str, is_key: bool, unique_id: i32) -> descriptors::TColumn {
        descriptors::TColumn {
            column_name: name.to_string(),
            column_type: Some(types::TColumnType {
                type_: types::TPrimitiveType::INT,
                len: Some(4),
                index_len: Some(4),
                precision: None,
                scale: None,
            }),
            aggregation_type: if is_key {
                None
            } else {
                Some(types::TAggregationType::REPLACE)
            },
            is_key: Some(is_key),
            is_allow_null: Some(true),
            default_value: None,
            default_expr: None,
            is_bloom_filter_column: Some(false),
            define_expr: None,
            is_auto_increment: Some(false),
            col_unique_id: Some(unique_id),
            has_bitmap_index: Some(false),
            agg_state_desc: None,
            index_len: Some(4),
            type_desc: None,
        }
    }

    fn slot_desc(id: i32, name: &str) -> descriptors::TSlotDescriptor {
        descriptors::TSlotDescriptor {
            id: Some(id),
            parent: Some(0),
            slot_type: Some(scalar_type(types::TPrimitiveType::INT, None, None)),
            column_pos: Some(id),
            byte_offset: None,
            null_indicator_byte: None,
            null_indicator_bit: None,
            col_name: Some(name.to_string()),
            slot_idx: Some(id),
            is_materialized: Some(true),
            is_output_column: Some(true),
            is_nullable: Some(true),
            col_unique_id: None,
            col_physical_name: Some(format!("{name}_phys")),
            is_virtual_column: None,
        }
    }

    fn slot_ref_expr(slot_id: i32) -> exprs::TExpr {
        let mut node = expr_node(
            exprs::TExprNodeType::SLOT_REF,
            scalar_type(types::TPrimitiveType::INT, None, None),
        );
        node.slot_ref = Some(exprs::TSlotRef::new(slot_id, 0));
        exprs::TExpr::new(vec![node])
    }

    fn bool_expr(value: bool) -> exprs::TExpr {
        let mut node = expr_node(
            exprs::TExprNodeType::BOOL_LITERAL,
            scalar_type(types::TPrimitiveType::BOOLEAN, None, None),
        );
        node.bool_literal = Some(exprs::TBoolLiteral::new(value));
        exprs::TExpr::new(vec![node])
    }

    fn decimal_literal(value: &str, precision: i32, scale: i32) -> exprs::TExprNode {
        let mut node = expr_node(
            exprs::TExprNodeType::DECIMAL_LITERAL,
            scalar_type(
                types::TPrimitiveType::DECIMAL128,
                Some(precision),
                Some(scale),
            ),
        );
        node.decimal_literal = Some(exprs::TDecimalLiteral::new(
            value.to_string(),
            None::<Vec<u8>>,
        ));
        node
    }

    fn date_literal(value: &str, primitive: types::TPrimitiveType) -> exprs::TExprNode {
        let mut node = expr_node(
            exprs::TExprNodeType::DATE_LITERAL,
            scalar_type(primitive, None, None),
        );
        node.date_literal = Some(exprs::TDateLiteral::new(value.to_string()));
        node
    }

    fn string_literal(value: &str) -> exprs::TExprNode {
        let mut node = expr_node(
            exprs::TExprNodeType::STRING_LITERAL,
            scalar_type(types::TPrimitiveType::VARCHAR, None, None),
        );
        node.string_literal = Some(exprs::TStringLiteral::new(value.to_string()));
        node
    }

    fn string_expr(value: &str) -> exprs::TExpr {
        exprs::TExpr::new(vec![string_literal(value)])
    }

    fn expr_node(node_type: exprs::TExprNodeType, type_: types::TTypeDesc) -> exprs::TExprNode {
        exprs::TExprNode {
            node_type,
            type_,
            opcode: None,
            num_children: 0,
            agg_expr: None,
            bool_literal: None,
            case_expr: None,
            date_literal: None,
            float_literal: None,
            int_literal: None,
            in_predicate: None,
            is_null_pred: None,
            like_pred: None,
            literal_pred: None,
            slot_ref: None,
            string_literal: None,
            tuple_is_null_pred: None,
            info_func: None,
            decimal_literal: None,
            output_scale: -1,
            fn_call_expr: None,
            large_int_literal: None,
            output_column: None,
            output_type: None,
            vector_opcode: None,
            fn_: None,
            vararg_start_idx: None,
            child_type: None,
            vslot_ref: None,
            used_subfield_names: None,
            binary_literal: None,
            copy_flag: None,
            check_is_out_of_bounds: None,
            use_vectorized: None,
            has_nullable_child: None,
            is_nullable: Some(true),
            child_type_desc: None,
            is_monotonic: None,
            dict_query_expr: None,
            dictionary_get_expr: None,
            is_index_only_filter: None,
            is_nondeterministic: None,
        }
    }

    fn scalar_type(
        primitive: types::TPrimitiveType,
        precision: Option<i32>,
        scale: Option<i32>,
    ) -> types::TTypeDesc {
        types::TTypeDesc::new(Some(vec![types::TTypeNode::new(
            types::TTypeNodeType::SCALAR,
            Some(types::TScalarType::new(
                primitive,
                None::<i32>,
                precision,
                scale,
                None::<i32>,
            )),
            None::<Vec<types::TStructField>>,
            None::<bool>,
        )]))
    }

    fn layout_for_slots(slots: &[i32]) -> Layout {
        let order = slots.iter().map(|slot| (0, *slot)).collect::<Vec<_>>();
        let index = order
            .iter()
            .enumerate()
            .map(|(idx, key)| (*key, idx))
            .collect::<HashMap<_, _>>();
        Layout { order, index }
    }
}
