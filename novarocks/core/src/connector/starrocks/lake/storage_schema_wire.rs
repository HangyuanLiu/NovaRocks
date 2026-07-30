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

use crate::connector::starrocks::schema::{
    StarRocksAggStateDesc, StarRocksColumnSchema, StarRocksKeysType, StarRocksScalarType,
    StarRocksStructField, StarRocksTabletIndex, StarRocksTabletSchema, StarRocksTypeDesc,
    StarRocksTypeNode,
};
use crate::service::grpc_client::proto::starrocks::{
    AggStateDescPb, ColumnPb, KeysType, PScalarType, PStructField, PTypeDesc, PTypeNode,
    TabletIndexPb, TabletSchemaPb,
};
use prost::Message;

pub(crate) fn encode_tablet_schema_bytes(schema: &StarRocksTabletSchema) -> Vec<u8> {
    encode_tablet_schema(schema).encode_to_vec()
}

pub(crate) fn decode_tablet_schema_bytes(bytes: &[u8]) -> Result<StarRocksTabletSchema, String> {
    let schema = TabletSchemaPb::decode(bytes)
        .map_err(|error| format!("decode StarRocks tablet schema protobuf failed: {error}"))?;
    decode_tablet_schema(schema)
}

pub(crate) fn encode_tablet_schema(schema: &StarRocksTabletSchema) -> TabletSchemaPb {
    TabletSchemaPb {
        keys_type: schema.keys_type.map(encode_keys_type),
        column: schema.column.iter().map(encode_column).collect(),
        num_short_key_columns: schema.num_short_key_columns,
        num_rows_per_row_block: schema.num_rows_per_row_block,
        bf_fpp: schema.bf_fpp,
        next_column_unique_id: schema.next_column_unique_id,
        deprecated_is_in_memory: schema.deprecated_is_in_memory,
        deprecated_id: schema.deprecated_id,
        compression_type: schema.compression_type,
        sort_key_idxes: schema.sort_key_idxes.clone(),
        schema_version: schema.schema_version,
        sort_key_unique_ids: schema.sort_key_unique_ids.clone(),
        table_indices: schema
            .table_indices
            .iter()
            .map(encode_tablet_index)
            .collect(),
        compression_level: schema.compression_level,
        id: schema.id,
    }
}

pub(crate) fn decode_tablet_schema(
    schema: TabletSchemaPb,
) -> Result<StarRocksTabletSchema, String> {
    let schema = StarRocksTabletSchema {
        keys_type: schema.keys_type.map(decode_keys_type).transpose()?,
        column: schema.column.into_iter().map(decode_column).collect(),
        num_short_key_columns: schema.num_short_key_columns,
        num_rows_per_row_block: schema.num_rows_per_row_block,
        bf_fpp: schema.bf_fpp,
        next_column_unique_id: schema.next_column_unique_id,
        deprecated_is_in_memory: schema.deprecated_is_in_memory,
        deprecated_id: schema.deprecated_id,
        compression_type: schema.compression_type,
        sort_key_idxes: schema.sort_key_idxes,
        schema_version: schema.schema_version,
        sort_key_unique_ids: schema.sort_key_unique_ids,
        table_indices: schema
            .table_indices
            .into_iter()
            .map(decode_tablet_index)
            .collect(),
        compression_level: schema.compression_level,
        id: schema.id,
    };
    schema.validate()?;
    Ok(schema)
}

pub(crate) fn validate_encoded_tablet_schema(schema: &TabletSchemaPb) -> Result<(), String> {
    decode_tablet_schema(schema.clone()).map(|_| ())
}

fn encode_keys_type(value: StarRocksKeysType) -> i32 {
    match value {
        StarRocksKeysType::Duplicate => KeysType::DupKeys as i32,
        StarRocksKeysType::Unique => KeysType::UniqueKeys as i32,
        StarRocksKeysType::Aggregate => KeysType::AggKeys as i32,
        StarRocksKeysType::Primary => KeysType::PrimaryKeys as i32,
    }
}

fn decode_keys_type(value: i32) -> Result<StarRocksKeysType, String> {
    match KeysType::try_from(value) {
        Ok(KeysType::DupKeys) => Ok(StarRocksKeysType::Duplicate),
        Ok(KeysType::UniqueKeys) => Ok(StarRocksKeysType::Unique),
        Ok(KeysType::AggKeys) => Ok(StarRocksKeysType::Aggregate),
        Ok(KeysType::PrimaryKeys) => Ok(StarRocksKeysType::Primary),
        Err(_) => Err(format!("unknown StarRocks keys type {value}")),
    }
}

fn encode_column(value: &StarRocksColumnSchema) -> ColumnPb {
    ColumnPb {
        unique_id: value.unique_id,
        name: value.name.clone(),
        r#type: value.r#type.clone(),
        is_key: value.is_key,
        aggregation: value.aggregation.clone(),
        is_nullable: value.is_nullable,
        default_value: value.default_value.clone(),
        precision: value.precision,
        frac: value.frac,
        length: value.length,
        index_length: value.index_length,
        is_bf_column: value.is_bf_column,
        referenced_column_id: value.referenced_column_id,
        referenced_column: value.referenced_column.clone(),
        has_bitmap_index: value.has_bitmap_index,
        visible: value.visible,
        children_columns: value.children_columns.iter().map(encode_column).collect(),
        is_auto_increment: value.is_auto_increment,
        agg_state_desc: value.agg_state_desc.as_ref().map(encode_agg_state),
    }
}

fn decode_column(value: ColumnPb) -> StarRocksColumnSchema {
    StarRocksColumnSchema {
        unique_id: value.unique_id,
        name: value.name,
        r#type: value.r#type,
        is_key: value.is_key,
        aggregation: value.aggregation,
        is_nullable: value.is_nullable,
        default_value: value.default_value,
        precision: value.precision,
        frac: value.frac,
        length: value.length,
        index_length: value.index_length,
        is_bf_column: value.is_bf_column,
        referenced_column_id: value.referenced_column_id,
        referenced_column: value.referenced_column,
        has_bitmap_index: value.has_bitmap_index,
        visible: value.visible,
        children_columns: value
            .children_columns
            .into_iter()
            .map(decode_column)
            .collect(),
        is_auto_increment: value.is_auto_increment,
        agg_state_desc: value.agg_state_desc.map(decode_agg_state),
    }
}

fn encode_agg_state(value: &StarRocksAggStateDesc) -> AggStateDescPb {
    AggStateDescPb {
        agg_func_name: value.agg_func_name.clone(),
        arg_types: value.arg_types.iter().map(encode_type_desc).collect(),
        ret_type: value.ret_type.as_ref().map(encode_type_desc),
        is_result_nullable: value.is_result_nullable,
        func_version: value.func_version,
    }
}

fn decode_agg_state(value: AggStateDescPb) -> StarRocksAggStateDesc {
    StarRocksAggStateDesc {
        agg_func_name: value.agg_func_name,
        arg_types: value.arg_types.into_iter().map(decode_type_desc).collect(),
        ret_type: value.ret_type.map(decode_type_desc),
        is_result_nullable: value.is_result_nullable,
        func_version: value.func_version,
    }
}

fn encode_type_desc(value: &StarRocksTypeDesc) -> PTypeDesc {
    PTypeDesc {
        types: value.types.iter().map(encode_type_node).collect(),
    }
}

fn decode_type_desc(value: PTypeDesc) -> StarRocksTypeDesc {
    StarRocksTypeDesc {
        types: value.types.into_iter().map(decode_type_node).collect(),
    }
}

fn encode_type_node(value: &StarRocksTypeNode) -> PTypeNode {
    PTypeNode {
        r#type: value.r#type,
        scalar_type: value.scalar_type.map(|scalar| PScalarType {
            r#type: scalar.r#type,
            len: scalar.len,
            precision: scalar.precision,
            scale: scalar.scale,
        }),
        struct_fields: value
            .struct_fields
            .iter()
            .map(|field| PStructField {
                name: field.name.clone(),
                comment: field.comment.clone(),
            })
            .collect(),
    }
}

fn decode_type_node(value: PTypeNode) -> StarRocksTypeNode {
    StarRocksTypeNode {
        r#type: value.r#type,
        scalar_type: value.scalar_type.map(|scalar| StarRocksScalarType {
            r#type: scalar.r#type,
            len: scalar.len,
            precision: scalar.precision,
            scale: scalar.scale,
        }),
        struct_fields: value
            .struct_fields
            .into_iter()
            .map(|field| StarRocksStructField {
                name: field.name,
                comment: field.comment,
            })
            .collect(),
    }
}

fn encode_tablet_index(value: &StarRocksTabletIndex) -> TabletIndexPb {
    TabletIndexPb {
        index_id: value.index_id,
        index_name: value.index_name.clone(),
        index_type: value.index_type,
        col_unique_id: value.col_unique_id.clone(),
        index_properties: value.index_properties.clone(),
    }
}

fn decode_tablet_index(value: TabletIndexPb) -> StarRocksTabletIndex {
    StarRocksTabletIndex {
        index_id: value.index_id,
        index_name: value.index_name,
        index_type: value.index_type,
        col_unique_id: value.col_unique_id,
        index_properties: value.index_properties,
    }
}

#[cfg(test)]
mod tests {
    use crate::connector::starrocks::schema::{
        StarRocksColumnSchema, StarRocksKeysType, StarRocksTabletSchema,
    };

    use super::{decode_tablet_schema, encode_tablet_schema};

    #[test]
    fn tablet_schema_storage_wire_round_trips_domain_schema() {
        let schema = StarRocksTabletSchema::try_new(
            Some(7),
            Some(StarRocksKeysType::Primary),
            vec![StarRocksColumnSchema {
                unique_id: 1,
                name: Some("k".to_string()),
                r#type: "BIGINT".to_string(),
                is_key: Some(true),
                visible: Some(true),
                ..StarRocksColumnSchema::default()
            }],
        )
        .expect("domain schema");

        let encoded = encode_tablet_schema(&schema);
        let decoded = decode_tablet_schema(encoded).expect("decode domain schema");

        assert_eq!(decoded, schema);
    }
}
