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

use prost::Message;

use crate::connector::starrocks::lake::context::{TabletWriteContext, register_tablet_runtime};
use crate::connector::starrocks::lake::schema_adapter::{
    build_create_tablet_schema, map_create_tablet_compaction_strategy,
    map_create_tablet_persistent_index_type,
};
use crate::connector::starrocks::lake::storage_schema_wire::encode_tablet_schema;
use crate::connector::starrocks::schema::StarRocksTabletSchema;
use crate::formats::starrocks::writer::bundle_meta::{
    empty_tablet_metadata, load_latest_tablet_metadata, write_initial_meta_file,
    write_standalone_meta_file,
};
use crate::formats::starrocks::writer::io::read_bytes_if_exists;
use crate::formats::starrocks::writer::layout::{
    initial_meta_file_path, standalone_meta_file_path,
};
use crate::runtime::starlet_shard_registry::S3StoreConfig;
use crate::service::grpc_client::proto::starrocks::{
    CompactionStrategyPb, FlatJsonConfigPb, TabletMetadataPb,
};

pub(crate) fn create_lake_tablet_from_req(
    request: &crate::thrift::agent_service::TCreateTabletReq,
    tablet_root_path: &str,
    s3_config: Option<S3StoreConfig>,
) -> Result<(), String> {
    create_lake_tablet_from_req_with_schema_patch(request, tablet_root_path, s3_config, |_| Ok(()))
}

pub(crate) fn create_lake_tablet_from_req_with_schema_patch<P>(
    request: &crate::thrift::agent_service::TCreateTabletReq,
    tablet_root_path: &str,
    s3_config: Option<S3StoreConfig>,
    patch: P,
) -> Result<(), String>
where
    P: FnOnce(&mut StarRocksTabletSchema) -> Result<(), String>,
{
    let tablet_id = request.tablet_id;
    if tablet_id <= 0 {
        return Err(format!(
            "create_tablet has non-positive tablet_id={tablet_id}"
        ));
    }

    let mut tablet_schema = build_create_tablet_schema(request)?;
    patch(&mut tablet_schema)?;
    let runtime_ctx = TabletWriteContext {
        db_id: 0,
        table_id: request.table_id.unwrap_or(0),
        tablet_id,
        tablet_root_path: tablet_root_path.to_string(),
        tablet_schema: tablet_schema.clone(),
        s3_config,
        partial_update: Default::default(),
    };
    register_tablet_runtime(&runtime_ctx)?;

    let persistent_index_type = match request.persistent_index_type {
        Some(v) => Some(map_create_tablet_persistent_index_type(v)? as i32),
        None => None,
    };
    let compaction_strategy = request
        .compaction_strategy
        .map(map_create_tablet_compaction_strategy)
        .transpose()?
        .or(Some(CompactionStrategyPb::Default as i32));
    let flat_json_config = request
        .flat_json_config
        .as_ref()
        .map(|cfg| FlatJsonConfigPb {
            flat_json_enable: cfg.flat_json_enable,
            flat_json_null_factor: cfg.flat_json_null_factor.map(|v| v.0),
            flat_json_sparsity_factor: cfg.flat_json_sparsity_factor.map(|v| v.0),
            flat_json_max_column_max: cfg.flat_json_column_max,
        });

    let mut tablet_meta = empty_tablet_metadata(tablet_id);
    tablet_meta.version = Some(1);
    tablet_meta.enable_persistent_index = request.enable_persistent_index;
    tablet_meta.persistent_index_type = persistent_index_type;
    tablet_meta.gtid = Some(request.gtid.unwrap_or(0));
    tablet_meta.compaction_strategy = compaction_strategy;
    tablet_meta.flat_json_config = flat_json_config;
    seed_tablet_metadata_schema(&mut tablet_meta, &tablet_schema);

    let standalone_v1_path = standalone_meta_file_path(tablet_root_path, tablet_id, 1)?;
    if let Some(bytes) = read_bytes_if_exists(&standalone_v1_path)? {
        let existing = TabletMetadataPb::decode(bytes.as_slice()).map_err(|e| {
            format!(
                "decode existing standalone tablet metadata failed: path={}, error={}",
                standalone_v1_path, e
            )
        })?;
        if existing.schema.as_ref() == Some(&encode_tablet_schema(&tablet_schema)) {
            return Ok(());
        }
        return write_standalone_meta_file(tablet_root_path, tablet_id, 1, &tablet_meta);
    }

    let latest_version = match load_latest_tablet_metadata(tablet_root_path, tablet_id) {
        Ok((version, _)) => version,
        Err(err) if is_missing_tablet_page_in_bundle_error(&err) => 0,
        Err(err) => return Err(err),
    };
    if latest_version > 1 {
        return Ok(());
    }

    if request.enable_tablet_creation_optimization.unwrap_or(false) {
        let initial_path = initial_meta_file_path(tablet_root_path)?;
        if read_bytes_if_exists(&initial_path)?.is_some() {
            write_standalone_meta_file(tablet_root_path, tablet_id, 1, &tablet_meta)
        } else {
            write_initial_meta_file(tablet_root_path, &tablet_meta)
        }
    } else {
        write_standalone_meta_file(tablet_root_path, tablet_id, 1, &tablet_meta)
    }
}

fn seed_tablet_metadata_schema(
    metadata: &mut TabletMetadataPb,
    tablet_schema: &StarRocksTabletSchema,
) {
    let wire_schema = encode_tablet_schema(tablet_schema);
    metadata.schema = Some(wire_schema.clone());
    if let Some(schema_id) = tablet_schema.id.filter(|id| *id > 0) {
        metadata
            .historical_schemas
            .entry(schema_id)
            .or_insert(wire_schema);
    }
}

#[allow(dead_code)]
fn is_missing_tablet_page_in_bundle_error(error: &str) -> bool {
    error.contains("bundle metadata missing tablet page for tablet_id=")
        || error.contains("bundle metadata does not contain tablet page:")
}
