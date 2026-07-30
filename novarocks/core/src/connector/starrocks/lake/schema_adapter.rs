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

use std::collections::{HashMap, HashSet};

use crate::connector::starrocks::schema::{
    LakeScanColumnHint, LakeScanTableSchema, StarRocksColumnSchema, StarRocksKeysType,
    StarRocksTabletSchema,
};
use novarocks_types::decimal::{LEGACY_DECIMALV2_PRECISION, LEGACY_DECIMALV2_SCALE};

// These are StarRocks storage-domain values, not generated protobuf types.
// The compat storage codec interprets them at the file boundary.
const COMPRESSION_DEFAULT: i32 = 1;
const COMPRESSION_NONE: i32 = 2;
const COMPRESSION_SNAPPY: i32 = 3;
const COMPRESSION_LZ4_FRAME: i32 = 5;
const COMPRESSION_ZLIB: i32 = 6;
const COMPRESSION_ZSTD: i32 = 7;
const COMPRESSION_GZIP: i32 = 8;
const COMPRESSION_DEFLATE: i32 = 9;
const COMPRESSION_BZIP2: i32 = 10;
const COMPRESSION_BROTLI: i32 = 12;
const PERSISTENT_INDEX_LOCAL: i32 = 0;
const PERSISTENT_INDEX_CLOUD_NATIVE: i32 = 1;
const COMPACTION_STRATEGY_DEFAULT: i32 = 0;
const COMPACTION_STRATEGY_REAL_TIME: i32 = 1;

pub(crate) fn build_sink_tablet_schema(
    schema: &crate::thrift::descriptors::TOlapTableSchemaParam,
    schema_id: i64,
    keys_type: StarRocksKeysType,
) -> Result<StarRocksTabletSchema, String> {
    if schema.slot_descs.is_empty() {
        return Err("OLAP_TABLE_SINK schema.slot_descs is empty".to_string());
    }
    let index = schema
        .indexes
        .iter()
        .find(|idx| {
            let effective_schema_id = idx.schema_id.filter(|v| *v > 0).unwrap_or(idx.id);
            effective_schema_id == schema_id
        })
        .ok_or_else(|| {
            format!(
                "OLAP_TABLE_SINK cannot find schema index by schema_id={schema_id} in schema.indexes"
            )
        })?;
    let column_param = index.column_param.as_ref().ok_or_else(|| {
        format!("OLAP_TABLE_SINK schema.indexes(schema_id={schema_id}) missing column_param")
    })?;
    if column_param.columns.is_empty() {
        return Err(format!(
            "OLAP_TABLE_SINK schema.indexes(schema_id={schema_id}) has empty column_param.columns"
        ));
    }
    let slot_descs_by_name = build_slot_descs_by_name(schema)?;

    let mut columns = Vec::with_capacity(column_param.columns.len());
    let mut max_unique_id = 0i32;
    let mut used_unique_ids = HashSet::new();
    let mut unique_id_to_index = HashMap::new();

    for (idx, col) in column_param.columns.iter().enumerate() {
        let name = col.column_name.trim().to_string();
        if name.is_empty() {
            return Err(format!(
                "schema.indexes(schema_id={schema_id}).column_param.columns[{}] has empty column_name",
                idx
            ));
        }
        let mut column_pb =
            resolve_sink_column_pb(col, &name, idx, schema_id, &slot_descs_by_name)?;

        let unique_id = resolve_sink_unique_id(col, &name, idx, &slot_descs_by_name);
        if used_unique_ids.contains(&unique_id) {
            return Err(format!(
                "duplicate col_unique_id detected in schema.indexes(schema_id={}): unique_id={}",
                schema_id, unique_id
            ));
        }
        let is_key = col.is_key.ok_or_else(|| {
            format!(
                "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing is_key",
                idx
            )
        })?;
        let is_nullable = col.is_allow_null.ok_or_else(|| {
            format!(
                "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing is_allow_null",
                idx
            )
        })?;
        let aggregation =
            map_aggregation_type_to_schema_string(col.aggregation_type, is_key, keys_type, idx)?;
        if let Some(index_len) = col.index_len {
            column_pb.index_length = Some(index_len);
        }
        normalize_column_pb_type_attrs(&mut column_pb);
        if column_pb.r#type == "VARCHAR" && column_pb.index_length.is_none() {
            column_pb.index_length = Some(10);
        }

        used_unique_ids.insert(unique_id);
        unique_id_to_index.insert(unique_id, idx);
        max_unique_id = max_unique_id.max(unique_id);

        column_pb.unique_id = unique_id;
        column_pb.name = Some(name);
        column_pb.is_key = Some(is_key);
        column_pb.aggregation = aggregation;
        column_pb.is_nullable = Some(is_nullable);
        column_pb.default_value = col.default_value.as_ref().map(|v| v.as_bytes().to_vec());
        column_pb.is_bf_column = col.is_bloom_filter_column;
        column_pb.has_bitmap_index = col.has_bitmap_index;
        column_pb.is_auto_increment = Some(col.is_auto_increment.unwrap_or(false));

        columns.push(column_pb);
    }

    if column_param.short_key_column_count < 0 {
        return Err(format!(
            "schema.indexes(schema_id={schema_id}).column_param.short_key_column_count is negative: {}",
            column_param.short_key_column_count
        ));
    }
    let num_short_key_columns = column_param.short_key_column_count;
    if num_short_key_columns as usize > columns.len() {
        return Err(format!(
            "short_key_column_count exceeds column count: short_key_column_count={} columns={}",
            num_short_key_columns,
            columns.len()
        ));
    }
    let mut sort_key_unique_ids = Vec::new();
    let mut sort_key_idxes = Vec::new();
    if !column_param.sort_key_uid.is_empty() {
        sort_key_unique_ids.reserve(column_param.sort_key_uid.len());
        sort_key_idxes.reserve(column_param.sort_key_uid.len());
        for (idx, unique_id) in column_param.sort_key_uid.iter().enumerate() {
            if *unique_id < 0 {
                return Err(format!(
                    "schema.indexes(schema_id={schema_id}).column_param.sort_key_uid[{}] is negative: {}",
                    idx, unique_id
                ));
            }
            let col_idx = unique_id_to_index.get(unique_id).ok_or_else(|| {
                format!(
                    "schema.indexes(schema_id={schema_id}).column_param.sort_key_uid[{}]={} not found in columns",
                    idx, unique_id
                )
            })?;
            sort_key_unique_ids.push(*unique_id as u32);
            sort_key_idxes.push(*col_idx as u32);
        }
    } else {
        for (idx, col) in columns.iter().enumerate() {
            if col.is_key.unwrap_or(false) {
                sort_key_unique_ids.push(col.unique_id as u32);
                sort_key_idxes.push(idx as u32);
            }
        }
    }
    if sort_key_idxes.is_empty() {
        return Err(format!(
            "schema.indexes(schema_id={schema_id}) resolved empty sort key columns"
        ));
    }

    Ok(StarRocksTabletSchema {
        keys_type: Some(keys_type),
        column: columns.clone(),
        num_short_key_columns: Some(num_short_key_columns),
        num_rows_per_row_block: None,
        bf_fpp: None,
        next_column_unique_id: Some((max_unique_id + 1) as u32),
        deprecated_is_in_memory: None,
        deprecated_id: None,
        compression_type: None,
        sort_key_idxes,
        schema_version: Some(0),
        sort_key_unique_ids,
        table_indices: Vec::new(),
        compression_level: None,
        id: Some(schema_id),
    })
}

pub(crate) fn build_create_tablet_schema(
    request: &crate::thrift::agent_service::TCreateTabletReq,
) -> Result<StarRocksTabletSchema, String> {
    let schema = &request.tablet_schema;
    if schema.columns.is_empty() {
        return Err(format!(
            "create_tablet tablet_schema.columns is empty for tablet_id={}",
            request.tablet_id
        ));
    }

    let keys_type = map_create_tablet_keys_type(schema.keys_type)?;
    let mut columns = Vec::with_capacity(schema.columns.len());
    let mut max_unique_id = 0_i32;
    let mut used_unique_ids = HashSet::with_capacity(schema.columns.len());
    let mut unique_id_to_index = HashMap::with_capacity(schema.columns.len());

    for (idx, col) in schema.columns.iter().enumerate() {
        let name = col.column_name.trim().to_string();
        if name.is_empty() {
            return Err(format!(
                "create_tablet tablet_schema.columns[{}] has empty column_name",
                idx
            ));
        }

        let mut column_pb = resolve_create_tablet_column_pb(col, idx)?;

        let unique_id = col.col_unique_id.unwrap_or(idx as i32);
        let effective_unique_id = if unique_id < 0 { idx as i32 } else { unique_id };
        if used_unique_ids.contains(&effective_unique_id) {
            return Err(format!(
                "create_tablet has duplicate col_unique_id={}",
                effective_unique_id
            ));
        }
        used_unique_ids.insert(effective_unique_id);
        unique_id_to_index.insert(effective_unique_id, idx);
        max_unique_id = max_unique_id.max(effective_unique_id);

        let is_key = col.is_key.unwrap_or(false);
        let aggregation =
            map_aggregation_type_to_schema_string(col.aggregation_type, is_key, keys_type, idx)?;
        if let Some(index_len) = col.index_len {
            column_pb.index_length = Some(index_len);
        }
        normalize_column_pb_type_attrs(&mut column_pb);
        if column_pb.r#type == "VARCHAR" && column_pb.index_length.is_none() {
            column_pb.index_length = Some(10);
        }

        column_pb.unique_id = effective_unique_id;
        column_pb.name = Some(name);
        column_pb.is_key = Some(is_key);
        column_pb.aggregation = aggregation;
        column_pb.is_nullable = Some(col.is_allow_null.unwrap_or(false));
        // For scalar types, FE sends default_value as a plain string.
        // For complex types (ARRAY/MAP/STRUCT), FE sends define_expr (TExpr) instead.
        // Convert define_expr to a JSON string to match what StarRocks BE stores in ColumnPB.
        column_pb.default_value = col
            .default_value
            .as_ref()
            .map(|v| v.as_bytes().to_vec())
            .or_else(|| {
                col.default_expr
                    .as_ref()
                    .and_then(convert_define_expr_to_json)
                    .map(|s| s.into_bytes())
            });
        column_pb.is_bf_column = col.is_bloom_filter_column;
        column_pb.has_bitmap_index = col.has_bitmap_index;
        column_pb.is_auto_increment = Some(col.is_auto_increment.unwrap_or(false));

        columns.push(column_pb);
    }

    let num_short_key_columns = i32::from(schema.short_key_column_count);
    if num_short_key_columns < 0 {
        return Err(format!(
            "create_tablet tablet_schema.short_key_column_count is negative: {}",
            num_short_key_columns
        ));
    }
    if num_short_key_columns as usize > columns.len() {
        return Err(format!(
            "create_tablet short_key_column_count exceeds column count: short_key_column_count={} columns={}",
            num_short_key_columns,
            columns.len()
        ));
    }

    let mut sort_key_idxes = Vec::new();
    if let Some(raw_sort_key_idxes) = schema.sort_key_idxes.as_ref() {
        sort_key_idxes.reserve(raw_sort_key_idxes.len());
        for (idx, value) in raw_sort_key_idxes.iter().enumerate() {
            if *value < 0 || (*value as usize) >= columns.len() {
                return Err(format!(
                    "create_tablet tablet_schema.sort_key_idxes[{}] is out of range: {}",
                    idx, value
                ));
            }
            sort_key_idxes.push(*value as u32);
        }
    }

    let mut sort_key_unique_ids = Vec::new();
    if let Some(raw_sort_key_unique_ids) = schema.sort_key_unique_ids.as_ref() {
        sort_key_unique_ids.reserve(raw_sort_key_unique_ids.len());
        for (idx, unique_id) in raw_sort_key_unique_ids.iter().enumerate() {
            if *unique_id < 0 {
                return Err(format!(
                    "create_tablet tablet_schema.sort_key_unique_ids[{}] is negative: {}",
                    idx, unique_id
                ));
            }
            if !unique_id_to_index.contains_key(unique_id) {
                return Err(format!(
                    "create_tablet tablet_schema.sort_key_unique_ids[{}]={} not found in columns",
                    idx, unique_id
                ));
            }
            sort_key_unique_ids.push(*unique_id as u32);
        }
    }

    if sort_key_idxes.is_empty() && sort_key_unique_ids.is_empty() {
        for (idx, col) in columns.iter().enumerate() {
            if col.is_key == Some(true) {
                sort_key_idxes.push(idx as u32);
                sort_key_unique_ids.push(col.unique_id as u32);
            }
        }
    }

    let fallback_next_unique_id = columns.len() as u32;
    let next_column_unique_id = max_unique_id
        .saturating_add(1)
        .max(fallback_next_unique_id as i32) as u32;
    let compression = request
        .compression_type
        .or(schema.compression_type)
        .unwrap_or(crate::thrift::types::TCompressionType::LZ4_FRAME);
    let compression_type = map_create_tablet_compression_type(compression)?;
    let compression_level = request
        .compression_level
        .or(schema.compression_level)
        .or(Some(-1));

    Ok(StarRocksTabletSchema {
        keys_type: Some(keys_type),
        column: columns,
        num_short_key_columns: Some(num_short_key_columns),
        num_rows_per_row_block: None,
        bf_fpp: schema.bloom_filter_fpp.map(|v| v.0),
        next_column_unique_id: Some(next_column_unique_id),
        deprecated_is_in_memory: schema.is_in_memory,
        deprecated_id: None,
        compression_type: Some(compression_type),
        sort_key_idxes,
        schema_version: schema.schema_version,
        sort_key_unique_ids,
        table_indices: Vec::new(),
        compression_level,
        id: schema.id,
    })
}

pub(crate) fn build_tablet_schema_from_thrift(
    schema: &crate::thrift::agent_service::TTabletSchema,
) -> Result<StarRocksTabletSchema, String> {
    if schema.columns.is_empty() {
        return Err("schema_change base_tablet_read_schema.columns is empty".to_string());
    }

    let keys_type = map_create_tablet_keys_type(schema.keys_type)?;
    let mut columns = Vec::with_capacity(schema.columns.len());
    let mut max_unique_id = 0_i32;
    let mut used_unique_ids = HashSet::with_capacity(schema.columns.len());
    let mut unique_id_to_index = HashMap::with_capacity(schema.columns.len());

    for (idx, col) in schema.columns.iter().enumerate() {
        let name = col.column_name.trim().to_string();
        if name.is_empty() {
            return Err(format!(
                "schema_change base_tablet_read_schema.columns[{}] has empty column_name",
                idx
            ));
        }

        let mut column_pb = resolve_create_tablet_column_pb(col, idx)?;

        let unique_id = col.col_unique_id.unwrap_or(idx as i32);
        let effective_unique_id = if unique_id < 0 { idx as i32 } else { unique_id };
        if used_unique_ids.contains(&effective_unique_id) {
            return Err(format!(
                "schema_change base_tablet_read_schema has duplicate col_unique_id={}",
                effective_unique_id
            ));
        }
        used_unique_ids.insert(effective_unique_id);
        unique_id_to_index.insert(effective_unique_id, idx);
        max_unique_id = max_unique_id.max(effective_unique_id);

        let is_key = col.is_key.unwrap_or(false);
        let aggregation =
            map_aggregation_type_to_schema_string(col.aggregation_type, is_key, keys_type, idx)?;
        if let Some(index_len) = col.index_len {
            column_pb.index_length = Some(index_len);
        }
        normalize_column_pb_type_attrs(&mut column_pb);
        if column_pb.r#type == "VARCHAR" && column_pb.index_length.is_none() {
            column_pb.index_length = Some(10);
        }

        column_pb.unique_id = effective_unique_id;
        column_pb.name = Some(name);
        column_pb.is_key = Some(is_key);
        column_pb.aggregation = aggregation;
        column_pb.is_nullable = Some(col.is_allow_null.unwrap_or(false));
        // For scalar types, FE sends default_value as a plain string.
        // For complex types (ARRAY/MAP/STRUCT), FE sends define_expr (TExpr) instead.
        // Convert define_expr to a JSON string to match what StarRocks BE stores in ColumnPB.
        column_pb.default_value = col
            .default_value
            .as_ref()
            .map(|v| v.as_bytes().to_vec())
            .or_else(|| {
                col.default_expr
                    .as_ref()
                    .and_then(convert_define_expr_to_json)
                    .map(|s| s.into_bytes())
            });
        column_pb.is_bf_column = col.is_bloom_filter_column;
        column_pb.has_bitmap_index = col.has_bitmap_index;
        column_pb.is_auto_increment = Some(col.is_auto_increment.unwrap_or(false));

        columns.push(column_pb);
    }

    let num_short_key_columns = i32::from(schema.short_key_column_count);
    if num_short_key_columns < 0 {
        return Err(format!(
            "schema_change base_tablet_read_schema.short_key_column_count is negative: {}",
            num_short_key_columns
        ));
    }
    if num_short_key_columns as usize > columns.len() {
        return Err(format!(
            "schema_change base_tablet_read_schema short_key_column_count exceeds column count: short_key_column_count={} columns={}",
            num_short_key_columns,
            columns.len()
        ));
    }

    let mut sort_key_idxes = Vec::new();
    if let Some(raw_sort_key_idxes) = schema.sort_key_idxes.as_ref() {
        sort_key_idxes.reserve(raw_sort_key_idxes.len());
        for (idx, value) in raw_sort_key_idxes.iter().enumerate() {
            if *value < 0 || (*value as usize) >= columns.len() {
                return Err(format!(
                    "schema_change base_tablet_read_schema.sort_key_idxes[{}] is out of range: {}",
                    idx, value
                ));
            }
            sort_key_idxes.push(*value as u32);
        }
    }

    let mut sort_key_unique_ids = Vec::new();
    if let Some(raw_sort_key_unique_ids) = schema.sort_key_unique_ids.as_ref() {
        sort_key_unique_ids.reserve(raw_sort_key_unique_ids.len());
        for (idx, unique_id) in raw_sort_key_unique_ids.iter().enumerate() {
            if *unique_id < 0 {
                return Err(format!(
                    "schema_change base_tablet_read_schema.sort_key_unique_ids[{}] is negative: {}",
                    idx, unique_id
                ));
            }
            if !unique_id_to_index.contains_key(unique_id) {
                return Err(format!(
                    "schema_change base_tablet_read_schema.sort_key_unique_ids[{}]={} not found in columns",
                    idx, unique_id
                ));
            }
            sort_key_unique_ids.push(*unique_id as u32);
        }
    }

    if sort_key_idxes.is_empty() && sort_key_unique_ids.is_empty() {
        for (idx, col) in columns.iter().enumerate() {
            if col.is_key == Some(true) {
                sort_key_idxes.push(idx as u32);
                sort_key_unique_ids.push(col.unique_id as u32);
            }
        }
    }

    let fallback_next_unique_id = columns.len() as u32;
    let next_column_unique_id = max_unique_id
        .saturating_add(1)
        .max(fallback_next_unique_id as i32) as u32;
    let compression = schema
        .compression_type
        .unwrap_or(crate::thrift::types::TCompressionType::LZ4_FRAME);
    let compression_type = map_create_tablet_compression_type(compression)?;
    let compression_level = schema.compression_level.or(Some(-1));

    Ok(StarRocksTabletSchema {
        keys_type: Some(keys_type),
        column: columns,
        num_short_key_columns: Some(num_short_key_columns),
        num_rows_per_row_block: None,
        bf_fpp: schema.bloom_filter_fpp.map(|v| v.0),
        next_column_unique_id: Some(next_column_unique_id),
        deprecated_is_in_memory: schema.is_in_memory,
        deprecated_id: None,
        compression_type: Some(compression_type),
        sort_key_idxes,
        schema_version: schema.schema_version,
        sort_key_unique_ids,
        table_indices: Vec::new(),
        compression_level,
        id: schema.id,
    })
}

pub fn build_lake_scan_table_schema_from_thrift(
    schema: &crate::thrift::agent_service::TTabletSchema,
) -> Result<LakeScanTableSchema, String> {
    let tablet_schema = build_tablet_schema_from_thrift(schema)?;
    let mut column_hints = HashMap::new();
    for column in &schema.columns {
        let normalized_name = column.column_name.trim().to_ascii_lowercase();
        if normalized_name.is_empty() {
            continue;
        }
        let unique_id = match column.col_unique_id {
            Some(value) if value >= 0 => Some(u32::try_from(value).map_err(|_| {
                format!(
                    "invalid FE table schema col_unique_id for column '{}': {}",
                    column.column_name, value
                )
            })?),
            _ => None,
        };
        let hint = LakeScanColumnHint {
            unique_id,
            default_value: column.default_value.clone(),
        };
        if let Some(existing) = column_hints.get(&normalized_name)
            && existing != &hint
        {
            return Err(format!(
                "duplicated FE table schema column with mismatched metadata: column_name={}",
                column.column_name
            ));
        }
        column_hints.insert(normalized_name, hint);
    }
    Ok(LakeScanTableSchema {
        tablet_schema,
        column_hints,
    })
}

fn resolve_create_tablet_column_pb(
    column: &crate::thrift::descriptors::TColumn,
    column_idx: usize,
) -> Result<StarRocksColumnSchema, String> {
    if let Some(type_desc) = column.type_desc.as_ref() {
        return build_create_tablet_column_pb_from_type_desc(type_desc, column_idx);
    }
    if let Some(column_type) = column.column_type.as_ref() {
        return build_create_tablet_column_pb_from_column_type(column_type, column_idx);
    }
    Err(format!(
        "create_tablet column {} missing both column_type and type_desc",
        column_idx
    ))
}

fn build_create_tablet_column_pb_from_column_type(
    column_type: &crate::thrift::types::TColumnType,
    column_idx: usize,
) -> Result<StarRocksColumnSchema, String> {
    let sr_type = map_primitive_to_starrocks_type(column_type.type_).ok_or_else(|| {
        format!(
            "create_tablet has unsupported primitive type {:?} in column {}",
            column_type.type_, column_idx
        )
    })?;
    let (precision, frac) =
        resolve_decimal_type_attrs(column_type.type_, column_type.precision, column_type.scale);
    Ok(StarRocksColumnSchema {
        unique_id: -1,
        name: None,
        r#type: sr_type.to_string(),
        is_key: Some(false),
        aggregation: Some("NONE".to_string()),
        is_nullable: Some(true),
        default_value: None,
        precision,
        frac,
        length: column_type.len,
        index_length: column_type.index_len.or(column_type.len),
        is_bf_column: None,
        referenced_column_id: None,
        referenced_column: None,
        has_bitmap_index: None,
        visible: Some(true),
        children_columns: Vec::new(),
        is_auto_increment: Some(false),
        agg_state_desc: None,
    })
}

fn build_create_tablet_column_pb_from_type_desc(
    type_desc: &crate::thrift::types::TTypeDesc,
    column_idx: usize,
) -> Result<StarRocksColumnSchema, String> {
    let nodes = type_desc.types.as_ref().ok_or_else(|| {
        format!(
            "create_tablet column {} has empty type_desc.types",
            column_idx
        )
    })?;
    if nodes.is_empty() {
        return Err(format!(
            "create_tablet column {} has empty type_desc.types",
            column_idx
        ));
    }
    let mut cursor = 0usize;
    let mut column_pb = init_create_tablet_sub_field_pb();
    type_desc_to_column_pb(nodes, &mut cursor, column_idx, "root", &mut column_pb)?;
    if cursor != nodes.len() {
        return Err(format!(
            "create_tablet column {} type_desc parse did not consume all nodes: consumed={} total={}",
            column_idx,
            cursor,
            nodes.len()
        ));
    }
    Ok(column_pb)
}

fn type_desc_to_column_pb(
    nodes: &[crate::thrift::types::TTypeNode],
    cursor: &mut usize,
    column_idx: usize,
    path: &str,
    column_pb: &mut StarRocksColumnSchema,
) -> Result<(), String> {
    let node = nodes.get(*cursor).ok_or_else(|| {
        format!(
            "create_tablet column {} type_desc parse out of bounds at path={} cursor={} total_nodes={}",
            column_idx,
            path,
            *cursor,
            nodes.len()
        )
    })?;
    *cursor += 1;

    if node.type_ == crate::thrift::types::TTypeNodeType::SCALAR {
        let scalar = node.scalar_type.as_ref().ok_or_else(|| {
            format!(
                "create_tablet column {} scalar node missing scalar_type at path={}",
                column_idx, path
            )
        })?;
        let sr_type = map_primitive_to_starrocks_type(scalar.type_).ok_or_else(|| {
            format!(
                "create_tablet column {} has unsupported primitive type {:?} at path={}",
                column_idx, scalar.type_, path
            )
        })?;
        column_pb.r#type = sr_type.to_string();
        let (precision, frac) =
            resolve_decimal_type_attrs(scalar.type_, scalar.precision, scalar.scale);
        column_pb.precision = precision;
        column_pb.frac = frac;
        column_pb.length = scalar.len;
        column_pb.index_length = scalar.len;
        return Ok(());
    }

    if node.type_ == crate::thrift::types::TTypeNodeType::ARRAY {
        column_pb.r#type = "ARRAY".to_string();
        let mut element = init_create_tablet_sub_field_pb();
        type_desc_to_column_pb(
            nodes,
            cursor,
            column_idx,
            &format!("{path}.element"),
            &mut element,
        )?;
        element.name = Some("element".to_string());
        column_pb.children_columns.push(element);
        return Ok(());
    }

    if node.type_ == crate::thrift::types::TTypeNodeType::MAP {
        column_pb.r#type = "MAP".to_string();
        let mut key = init_create_tablet_sub_field_pb();
        type_desc_to_column_pb(nodes, cursor, column_idx, &format!("{path}.key"), &mut key)?;
        key.name = Some("key".to_string());
        column_pb.children_columns.push(key);

        let mut value = init_create_tablet_sub_field_pb();
        type_desc_to_column_pb(
            nodes,
            cursor,
            column_idx,
            &format!("{path}.value"),
            &mut value,
        )?;
        value.name = Some("value".to_string());
        column_pb.children_columns.push(value);
        return Ok(());
    }

    if node.type_ == crate::thrift::types::TTypeNodeType::STRUCT {
        column_pb.r#type = "STRUCT".to_string();
        let struct_fields = node.struct_fields.as_ref().ok_or_else(|| {
            format!(
                "create_tablet column {} struct node missing struct_fields at path={}",
                column_idx, path
            )
        })?;
        for (idx, field) in struct_fields.iter().enumerate() {
            let field_name = field
                .name
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    format!(
                        "create_tablet column {} struct field {} has empty name at path={}",
                        column_idx, idx, path
                    )
                })?;
            let mut field_pb = init_create_tablet_sub_field_pb();
            type_desc_to_column_pb(
                nodes,
                cursor,
                column_idx,
                &format!("{path}.{field_name}"),
                &mut field_pb,
            )?;
            field_pb.name = Some(field_name.to_string());
            if let Some(field_id) = field.id
                && field_id >= 0
            {
                field_pb.unique_id = field_id;
            }
            column_pb.children_columns.push(field_pb);
        }
        return Ok(());
    }

    Err(format!(
        "create_tablet column {} has unsupported type_desc node {:?} at path={}",
        column_idx, node.type_, path
    ))
}

fn init_create_tablet_sub_field_pb() -> StarRocksColumnSchema {
    StarRocksColumnSchema {
        unique_id: -1,
        name: None,
        r#type: String::new(),
        is_key: Some(false),
        aggregation: Some("NONE".to_string()),
        is_nullable: Some(true),
        default_value: None,
        precision: None,
        frac: None,
        length: None,
        index_length: None,
        is_bf_column: None,
        referenced_column_id: None,
        referenced_column: None,
        has_bitmap_index: None,
        visible: Some(true),
        children_columns: Vec::new(),
        is_auto_increment: Some(false),
        agg_state_desc: None,
    }
}

fn resolve_decimal_type_attrs(
    primitive: crate::thrift::types::TPrimitiveType,
    precision: Option<i32>,
    scale: Option<i32>,
) -> (Option<i32>, Option<i32>) {
    if primitive == crate::thrift::types::TPrimitiveType::DECIMALV2 {
        return (
            Some(i32::from(LEGACY_DECIMALV2_PRECISION)),
            Some(i32::from(LEGACY_DECIMALV2_SCALE)),
        );
    }
    (precision, scale)
}

fn normalize_column_pb_type_attrs(column: &mut StarRocksColumnSchema) {
    if column.length.is_some_and(|v| v < 0) {
        column.length = None;
    }
    if column.index_length.is_some_and(|v| v < 0) {
        column.index_length = None;
    }
    if column.precision.is_some_and(|v| v < 0) {
        column.precision = None;
    }
    if column.frac.is_some_and(|v| v < 0) {
        column.frac = None;
    }
    for child in column.children_columns.iter_mut() {
        normalize_column_pb_type_attrs(child);
    }
}

fn map_create_tablet_keys_type(
    keys_type: crate::thrift::types::TKeysType,
) -> Result<StarRocksKeysType, String> {
    if keys_type == crate::thrift::types::TKeysType::DUP_KEYS {
        return Ok(StarRocksKeysType::Duplicate);
    }
    if keys_type == crate::thrift::types::TKeysType::UNIQUE_KEYS {
        return Ok(StarRocksKeysType::Unique);
    }
    if keys_type == crate::thrift::types::TKeysType::AGG_KEYS {
        return Ok(StarRocksKeysType::Aggregate);
    }
    if keys_type == crate::thrift::types::TKeysType::PRIMARY_KEYS {
        return Ok(StarRocksKeysType::Primary);
    }
    Err(format!(
        "unsupported create_tablet keys_type={:?}",
        keys_type
    ))
}

fn map_create_tablet_compression_type(
    compression_type: crate::thrift::types::TCompressionType,
) -> Result<i32, String> {
    if compression_type == crate::thrift::types::TCompressionType::DEFAULT_COMPRESSION {
        return Ok(COMPRESSION_DEFAULT);
    }
    if compression_type == crate::thrift::types::TCompressionType::NO_COMPRESSION {
        return Ok(COMPRESSION_NONE);
    }
    if compression_type == crate::thrift::types::TCompressionType::SNAPPY {
        return Ok(COMPRESSION_SNAPPY);
    }
    if compression_type == crate::thrift::types::TCompressionType::LZ4
        || compression_type == crate::thrift::types::TCompressionType::LZ4_FRAME
    {
        return Ok(COMPRESSION_LZ4_FRAME);
    }
    if compression_type == crate::thrift::types::TCompressionType::ZLIB {
        return Ok(COMPRESSION_ZLIB);
    }
    if compression_type == crate::thrift::types::TCompressionType::ZSTD {
        return Ok(COMPRESSION_ZSTD);
    }
    if compression_type == crate::thrift::types::TCompressionType::GZIP {
        return Ok(COMPRESSION_GZIP);
    }
    if compression_type == crate::thrift::types::TCompressionType::DEFLATE {
        return Ok(COMPRESSION_DEFLATE);
    }
    if compression_type == crate::thrift::types::TCompressionType::BZIP2 {
        return Ok(COMPRESSION_BZIP2);
    }
    if compression_type == crate::thrift::types::TCompressionType::BROTLI {
        return Ok(COMPRESSION_BROTLI);
    }
    Err(format!(
        "unsupported create_tablet compression_type={:?}",
        compression_type
    ))
}

pub(crate) fn map_create_tablet_persistent_index_type(
    persistent_index_type: crate::thrift::agent_service::TPersistentIndexType,
) -> Result<i32, String> {
    if persistent_index_type == crate::thrift::agent_service::TPersistentIndexType::LOCAL {
        return Ok(PERSISTENT_INDEX_LOCAL);
    }
    if persistent_index_type == crate::thrift::agent_service::TPersistentIndexType::CLOUD_NATIVE {
        return Ok(PERSISTENT_INDEX_CLOUD_NATIVE);
    }
    Err(format!(
        "unsupported create_tablet persistent_index_type={:?}",
        persistent_index_type
    ))
}

pub(crate) const DEFAULT_COMPACTION_STRATEGY: i32 = COMPACTION_STRATEGY_DEFAULT;

pub(crate) fn map_create_tablet_compaction_strategy(
    compaction_strategy: crate::thrift::agent_service::TCompactionStrategy,
) -> Result<i32, String> {
    if compaction_strategy == crate::thrift::agent_service::TCompactionStrategy::DEFAULT {
        return Ok(COMPACTION_STRATEGY_DEFAULT);
    }
    if compaction_strategy == crate::thrift::agent_service::TCompactionStrategy::REAL_TIME {
        return Ok(COMPACTION_STRATEGY_REAL_TIME);
    }
    Err(format!(
        "unsupported create_tablet compaction_strategy={:?}",
        compaction_strategy
    ))
}

fn build_slot_descs_by_name(
    schema: &crate::thrift::descriptors::TOlapTableSchemaParam,
) -> Result<HashMap<String, &crate::thrift::descriptors::TSlotDescriptor>, String> {
    let mut map = HashMap::new();
    for (idx, slot) in schema.slot_descs.iter().enumerate() {
        let name = slot
            .col_name
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| format!("schema.slot_descs[{}] missing col_name", idx))?
            .to_ascii_lowercase();
        if map.insert(name.clone(), slot).is_some() {
            return Err(format!(
                "duplicate col_name in schema.slot_descs is not supported: {}",
                name
            ));
        }
    }
    Ok(map)
}

fn resolve_sink_unique_id(
    column: &crate::thrift::descriptors::TColumn,
    column_name: &str,
    column_idx: usize,
    slot_descs_by_name: &HashMap<String, &crate::thrift::descriptors::TSlotDescriptor>,
) -> i32 {
    column
        .col_unique_id
        .filter(|v| *v >= 0)
        .or_else(|| {
            slot_descs_by_name
                .get(&column_name.to_ascii_lowercase())
                .and_then(|slot| slot.col_unique_id)
                .filter(|v| *v >= 0)
        })
        .unwrap_or(column_idx as i32)
}

fn resolve_sink_column_pb(
    column: &crate::thrift::descriptors::TColumn,
    column_name: &str,
    column_idx: usize,
    schema_id: i64,
    slot_descs_by_name: &HashMap<String, &crate::thrift::descriptors::TSlotDescriptor>,
) -> Result<StarRocksColumnSchema, String> {
    if let Some(column_type) = column.column_type.as_ref() {
        return build_create_tablet_column_pb_from_column_type(column_type, column_idx).map_err(
            |err| {
                format!(
                    "schema.indexes(schema_id={schema_id}).column_param.columns[{}] has unsupported column_type (col_name={}): {}",
                    column_idx, column_name, err
                )
            },
        );
    }

    if let Some(type_desc) = column.type_desc.as_ref() {
        return build_create_tablet_column_pb_from_type_desc(type_desc, column_idx).map_err(
            |err| {
                format!(
                    "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing column_type and has unsupported type_desc (col_name={}): {}",
                    column_idx, column_name, err
                )
            },
        );
    }

    let slot = slot_descs_by_name
        .get(&column_name.to_ascii_lowercase())
        .ok_or_else(|| {
            format!(
                "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing column_type/type_desc and no matching slot_desc by col_name={}",
                column_idx, column_name
            )
        })?;
    let slot_type = slot.slot_type.as_ref().ok_or_else(|| {
        format!(
            "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing column_type/type_desc and matched slot_desc has no slot_type (col_name={})",
            column_idx, column_name
        )
    })?;
    build_create_tablet_column_pb_from_type_desc(slot_type, column_idx).map_err(|err| {
        format!(
            "schema.indexes(schema_id={schema_id}).column_param.columns[{}] missing column_type/type_desc and matched slot_desc has unsupported slot_type (col_name={}): {}",
            column_idx, column_name, err
        )
    })
}

fn map_aggregation_type_to_schema_string(
    aggregation_type: Option<crate::thrift::types::TAggregationType>,
    is_key: bool,
    keys_type: StarRocksKeysType,
    column_idx: usize,
) -> Result<Option<String>, String> {
    if is_key {
        return Ok(None);
    }
    if aggregation_type.is_none() && keys_type == StarRocksKeysType::Duplicate {
        return Ok(None);
    }
    let agg = aggregation_type.ok_or_else(|| {
        format!(
            "missing aggregation_type for value column in schema.indexes column index {}",
            column_idx
        )
    })?;
    let name = match agg {
        crate::thrift::types::TAggregationType::SUM => "SUM",
        crate::thrift::types::TAggregationType::MAX => "MAX",
        crate::thrift::types::TAggregationType::MIN => "MIN",
        crate::thrift::types::TAggregationType::REPLACE => "REPLACE",
        crate::thrift::types::TAggregationType::HLL_UNION => "HLL_UNION",
        crate::thrift::types::TAggregationType::NONE => "NONE",
        crate::thrift::types::TAggregationType::BITMAP_UNION => "BITMAP_UNION",
        crate::thrift::types::TAggregationType::REPLACE_IF_NOT_NULL => "REPLACE_IF_NOT_NULL",
        crate::thrift::types::TAggregationType::PERCENTILE_UNION => "PERCENTILE_UNION",
        crate::thrift::types::TAggregationType::AGG_STATE_UNION => "AGG_STATE_UNION",
        other => {
            return Err(format!(
                "unsupported aggregation_type for value column in schema.indexes column index {}: {:?}",
                column_idx, other
            ));
        }
    };
    Ok(Some(name.to_string()))
}

fn map_primitive_to_starrocks_type(
    primitive: crate::thrift::types::TPrimitiveType,
) -> Option<&'static str> {
    let t = primitive;
    Some(if t == crate::thrift::types::TPrimitiveType::BOOLEAN {
        "BOOLEAN"
    } else if t == crate::thrift::types::TPrimitiveType::TINYINT {
        "TINYINT"
    } else if t == crate::thrift::types::TPrimitiveType::SMALLINT {
        "SMALLINT"
    } else if t == crate::thrift::types::TPrimitiveType::INT {
        "INT"
    } else if t == crate::thrift::types::TPrimitiveType::BIGINT {
        "BIGINT"
    } else if t == crate::thrift::types::TPrimitiveType::LARGEINT {
        "LARGEINT"
    } else if t == crate::thrift::types::TPrimitiveType::FLOAT {
        "FLOAT"
    } else if t == crate::thrift::types::TPrimitiveType::DOUBLE {
        "DOUBLE"
    } else if t == crate::thrift::types::TPrimitiveType::DATE {
        "DATE"
    } else if t == crate::thrift::types::TPrimitiveType::DATETIME
        || t == crate::thrift::types::TPrimitiveType::TIME
    {
        "DATETIME"
    } else if t == crate::thrift::types::TPrimitiveType::CHAR {
        "CHAR"
    } else if t == crate::thrift::types::TPrimitiveType::VARCHAR {
        "VARCHAR"
    } else if t == crate::thrift::types::TPrimitiveType::HLL {
        "HLL"
    } else if t == crate::thrift::types::TPrimitiveType::OBJECT {
        "OBJECT"
    } else if t == crate::thrift::types::TPrimitiveType::PERCENTILE {
        "PERCENTILE"
    } else if t == crate::thrift::types::TPrimitiveType::BINARY {
        "BINARY"
    } else if t == crate::thrift::types::TPrimitiveType::VARBINARY {
        "VARBINARY"
    } else if t == crate::thrift::types::TPrimitiveType::DECIMAL
        || t == crate::thrift::types::TPrimitiveType::DECIMALV2
    {
        // Native writer path is DecimalV3-based; map legacy decimal primitives to Decimal128.
        "DECIMAL128"
    } else if t == crate::thrift::types::TPrimitiveType::DECIMAL32 {
        "DECIMAL32"
    } else if t == crate::thrift::types::TPrimitiveType::DECIMAL64 {
        "DECIMAL64"
    } else if t == crate::thrift::types::TPrimitiveType::DECIMAL128 {
        "DECIMAL128"
    } else if t == crate::thrift::types::TPrimitiveType::DECIMAL256 {
        "DECIMAL256"
    } else if t == crate::thrift::types::TPrimitiveType::JSON {
        "JSON"
    } else {
        return None;
    })
}

/// Convert a constant TExpr (from TColumn.define_expr) into a JSON string
/// suitable for storage in ColumnPB.default_value, matching StarRocks BE behavior
/// for complex-type column defaults (ARRAY/MAP/STRUCT).
///
/// Returns None if the expression cannot be evaluated as a constant literal
/// (e.g., unsupported node type, malformed expression).
fn convert_define_expr_to_json(expr: &crate::thrift::exprs::TExpr) -> Option<String> {
    if expr.nodes.is_empty() {
        return None;
    }
    let mut idx = 0usize;
    match eval_texpr_node(&expr.nodes, &mut idx, None) {
        Ok(value) => Some(value.to_string()),
        Err(e) => {
            tracing::warn!("failed to convert define_expr to JSON: {}", e);
            None
        }
    }
}

/// Extract struct field names from a TTypeDesc, if it describes a STRUCT type.
fn extract_struct_field_names(type_desc: &crate::thrift::types::TTypeDesc) -> Option<Vec<String>> {
    let nodes = type_desc.types.as_ref()?;
    for type_node in nodes {
        if let Some(fields) = &type_node.struct_fields {
            return Some(
                fields
                    .iter()
                    .filter_map(|f| f.name.clone())
                    .collect::<Vec<_>>(),
            );
        }
    }
    None
}

/// Recursively evaluate one TExpr node (depth-first, flat array).
/// Advances `idx` past the node and all its children.
/// `struct_fields_hint`: when Some, the caller knows the expected struct field names
/// (used for positional `row(v1, v2, ...)` calls where field names aren't in the expr).
fn eval_texpr_node(
    nodes: &[crate::thrift::exprs::TExprNode],
    idx: &mut usize,
    struct_fields_hint: Option<Vec<String>>,
) -> Result<serde_json::Value, String> {
    if *idx >= nodes.len() {
        return Err(format!(
            "expr node index {} out of bounds {}",
            *idx,
            nodes.len()
        ));
    }
    let node = &nodes[*idx];
    *idx += 1;
    let num_children = node.num_children as usize;
    let nt = node.node_type;

    use crate::thrift::exprs::TExprNodeType;
    if nt == TExprNodeType::INT_LITERAL {
        let v = node
            .int_literal
            .as_ref()
            .ok_or("INT_LITERAL missing int_literal")?;
        Ok(serde_json::Value::Number(v.value.into()))
    } else if nt == TExprNodeType::LARGE_INT_LITERAL {
        let v = node
            .large_int_literal
            .as_ref()
            .ok_or("LARGE_INT_LITERAL missing large_int_literal")?;
        Ok(serde_json::Value::String(v.value.clone()))
    } else if nt == TExprNodeType::FLOAT_LITERAL {
        let v = node
            .float_literal
            .as_ref()
            .ok_or("FLOAT_LITERAL missing float_literal")?;
        let n = serde_json::Number::from_f64(v.value.0).ok_or_else(|| {
            format!(
                "FLOAT_LITERAL value {} is not JSON-representable",
                v.value.0
            )
        })?;
        Ok(serde_json::Value::Number(n))
    } else if nt == TExprNodeType::BOOL_LITERAL {
        let v = node
            .bool_literal
            .as_ref()
            .ok_or("BOOL_LITERAL missing bool_literal")?;
        Ok(serde_json::Value::Bool(v.value))
    } else if nt == TExprNodeType::STRING_LITERAL {
        let v = node
            .string_literal
            .as_ref()
            .ok_or("STRING_LITERAL missing string_literal")?;
        Ok(serde_json::Value::String(v.value.clone()))
    } else if nt == TExprNodeType::NULL_LITERAL {
        Ok(serde_json::Value::Null)
    } else if nt == TExprNodeType::DATE_LITERAL {
        let v = node
            .date_literal
            .as_ref()
            .ok_or("DATE_LITERAL missing date_literal")?;
        Ok(serde_json::Value::String(v.value.clone()))
    } else if nt == TExprNodeType::DECIMAL_LITERAL {
        let v = node
            .decimal_literal
            .as_ref()
            .ok_or("DECIMAL_LITERAL missing decimal_literal")?;
        Ok(serde_json::Value::String(v.value.clone()))
    } else if nt == TExprNodeType::BINARY_LITERAL {
        // VARBINARY default: represent as UTF-8 string (lossy)
        let v = node
            .binary_literal
            .as_ref()
            .ok_or("BINARY_LITERAL missing binary_literal")?;
        Ok(serde_json::Value::String(
            String::from_utf8_lossy(&v.value).into_owned(),
        ))
    } else if nt == TExprNodeType::CAST_EXPR {
        // CAST has one child; pass through its value.
        // For CAST-to-STRUCT, extract field names from the type and pass as hint
        // so that a positional `row(v1, v2)` child can map values to field names.
        if num_children != 1 {
            return Err(format!("CAST_EXPR expected 1 child, got {}", num_children));
        }
        let hint = extract_struct_field_names(&node.type_);
        eval_texpr_node(nodes, idx, hint)
    } else if nt == TExprNodeType::ARRAY_EXPR {
        let mut elements = Vec::with_capacity(num_children);
        for _ in 0..num_children {
            elements.push(eval_texpr_node(nodes, idx, None)?);
        }
        Ok(serde_json::Value::Array(elements))
    } else if nt == TExprNodeType::MAP_EXPR {
        // MAP_EXPR children alternate key, value, key, value, ...
        if !num_children.is_multiple_of(2) {
            return Err(format!(
                "MAP_EXPR expected even number of children, got {}",
                num_children
            ));
        }
        let mut map = serde_json::Map::new();
        for _ in 0..(num_children / 2) {
            let key = eval_texpr_node(nodes, idx, None)?;
            let val = eval_texpr_node(nodes, idx, None)?;
            let key_str = match key {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            };
            map.insert(key_str, val);
        }
        Ok(serde_json::Value::Object(map))
    } else if nt == TExprNodeType::FUNCTION_CALL {
        // STRUCT defaults may be encoded as:
        //   named_struct('f1', v1, 'f2', v2, ...) — alternating name/value children
        //   row(v1, v2, ...) — positional children; field names come from struct_fields_hint
        // The function metadata is in `fn_` (field 26 of TExprNode), not `fn_call_expr`.
        let fn_name = node
            .fn_
            .as_ref()
            .map(|f| f.name.function_name.as_str())
            .unwrap_or("");
        if fn_name == "named_struct" {
            if !num_children.is_multiple_of(2) {
                return Err(format!(
                    "named_struct expected even children, got {}",
                    num_children
                ));
            }
            let mut obj = serde_json::Map::new();
            for _ in 0..(num_children / 2) {
                let field_name_val = eval_texpr_node(nodes, idx, None)?;
                let field_val = eval_texpr_node(nodes, idx, None)?;
                let field_name = match field_name_val {
                    serde_json::Value::String(s) => s,
                    other => other.to_string(),
                };
                obj.insert(field_name, field_val);
            }
            Ok(serde_json::Value::Object(obj))
        } else if fn_name == "row" {
            // `row(v1, v2, ...)` is positional; field names must come from the hint
            // passed by the enclosing CAST_EXPR-to-STRUCT.
            if let Some(fields) = struct_fields_hint {
                if fields.len() != num_children {
                    return Err(format!(
                        "row() has {} children but struct type has {} fields",
                        num_children,
                        fields.len()
                    ));
                }
                let mut obj = serde_json::Map::new();
                for field_name in fields {
                    let val = eval_texpr_node(nodes, idx, None)?;
                    obj.insert(field_name, val);
                }
                Ok(serde_json::Value::Object(obj))
            } else {
                // No type hint: fall back to treating as named_struct (alternating name/value)
                if !num_children.is_multiple_of(2) {
                    return Err(format!(
                        "row() without type hint expected even children, got {}",
                        num_children
                    ));
                }
                let mut obj = serde_json::Map::new();
                for _ in 0..(num_children / 2) {
                    let field_name_val = eval_texpr_node(nodes, idx, None)?;
                    let field_val = eval_texpr_node(nodes, idx, None)?;
                    let field_name = match field_name_val {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                    obj.insert(field_name, field_val);
                }
                Ok(serde_json::Value::Object(obj))
            }
        } else {
            Err(format!(
                "unsupported FUNCTION_CALL '{}' in define_expr",
                fn_name
            ))
        }
    } else {
        Err(format!("unsupported TExprNodeType {:?} in define_expr", nt))
    }
}

#[cfg(test)]
mod lake_scan_tests {
    use super::*;
    use crate::thrift::{agent_service, descriptors, types};

    #[test]
    fn storage_domain_enum_values_preserve_starrocks_wire_numbers() {
        assert_eq!(
            map_create_tablet_compression_type(types::TCompressionType::LZ4_FRAME)
                .expect("map LZ4_FRAME"),
            5
        );
        assert_eq!(
            map_create_tablet_compression_type(types::TCompressionType::ZSTD).expect("map ZSTD"),
            7
        );
        assert_eq!(
            map_create_tablet_persistent_index_type(agent_service::TPersistentIndexType::LOCAL)
                .expect("map local index"),
            0
        );
        assert_eq!(
            map_create_tablet_persistent_index_type(
                agent_service::TPersistentIndexType::CLOUD_NATIVE,
            )
            .expect("map cloud-native index"),
            1
        );
        assert_eq!(
            map_create_tablet_compaction_strategy(agent_service::TCompactionStrategy::DEFAULT)
                .expect("map default compaction"),
            0
        );
        assert_eq!(
            map_create_tablet_compaction_strategy(agent_service::TCompactionStrategy::REAL_TIME)
                .expect("map real-time compaction"),
            1
        );
    }

    #[test]
    fn lake_scan_schema_preserves_missing_wire_unique_id_in_column_hint() {
        let column = descriptors::TColumn::new(
            "k".to_string(),
            Some(types::TColumnType::new(
                types::TPrimitiveType::BIGINT,
                None,
                None,
                None,
                None,
            )),
            None,
            Some(true),
            Some(false),
            Some("7".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let mut negative_unique_id_column = column.clone();
        negative_unique_id_column.column_name = "v".to_string();
        negative_unique_id_column.is_key = Some(false);
        negative_unique_id_column.default_value = Some("8".to_string());
        negative_unique_id_column.col_unique_id = Some(-1);
        let schema = agent_service::TTabletSchema::new(
            1,
            17,
            types::TKeysType::DUP_KEYS,
            types::TStorageType::COLUMN,
            vec![column, negative_unique_id_column],
            None,
            None,
            None,
            Some(91),
            None,
            None,
            Some(3),
            None,
            None,
        );

        let decoded = build_lake_scan_table_schema_from_thrift(&schema).expect("decode schema");

        assert_eq!(decoded.tablet_schema.column[0].unique_id, 0);
        assert_eq!(decoded.column_hints["k"].unique_id, None);
        assert_eq!(
            decoded.column_hints["k"].default_value.as_deref(),
            Some("7")
        );
        assert_eq!(decoded.tablet_schema.column[1].unique_id, 1);
        assert_eq!(decoded.column_hints["v"].unique_id, None);
        assert_eq!(
            decoded.column_hints["v"].default_value.as_deref(),
            Some("8")
        );
    }
}
