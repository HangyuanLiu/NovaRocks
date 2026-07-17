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
    CompactionStrategyPb, FlatJsonConfigPb, TabletMetadataPb, TabletSchemaPb,
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
    P: FnOnce(&mut TabletSchemaPb) -> Result<(), String>,
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
        if existing.schema.as_ref() == Some(&tablet_schema) {
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

fn seed_tablet_metadata_schema(metadata: &mut TabletMetadataPb, tablet_schema: &TabletSchemaPb) {
    metadata.schema = Some(tablet_schema.clone());
    if let Some(schema_id) = tablet_schema.id.filter(|id| *id > 0) {
        metadata
            .historical_schemas
            .entry(schema_id)
            .or_insert_with(|| tablet_schema.clone());
    }
}

#[allow(dead_code)]
fn is_missing_tablet_page_in_bundle_error(error: &str) -> bool {
    error.contains("bundle metadata missing tablet page for tablet_id=")
        || error.contains("bundle metadata does not contain tablet page:")
}

#[cfg(test)]
mod tests {
    use super::{create_lake_tablet_from_req, is_missing_tablet_page_in_bundle_error};
    use crate::formats::starrocks::writer::bundle_meta::{
        load_tablet_metadata_at_version, write_bundle_meta_file,
    };
    use tempfile::TempDir;

    #[test]
    fn create_tablet_missing_tablet_page_error_matches_both_forms() {
        assert!(is_missing_tablet_page_in_bundle_error(
            "bundle metadata missing tablet page for tablet_id=123"
        ));
        assert!(is_missing_tablet_page_in_bundle_error(
            "bundle metadata does not contain tablet page: tablet_id=123, path=s3://bucket/meta"
        ));
    }

    #[test]
    fn create_tablet_writes_standalone_initial_metadata_by_default() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let req = build_test_create_tablet_req(41001, false);

        create_lake_tablet_from_req(
            &req,
            temp_dir.path().to_str().expect("temp path to str"),
            None,
        )
        .expect("create tablet");

        let standalone_path = temp_dir
            .path()
            .join("meta/000000000000A029_0000000000000001.meta");
        let initial_path = temp_dir
            .path()
            .join("meta/0000000000000000_0000000000000001.meta");
        assert!(standalone_path.exists(), "expected standalone metadata");
        assert!(!initial_path.exists(), "unexpected initial raw metadata");

        let metadata = load_tablet_metadata_at_version(
            temp_dir.path().to_str().expect("temp path to str"),
            41001,
            1,
        )
        .expect("load metadata")
        .expect("metadata should exist");
        let schema = metadata.schema.expect("schema should be persisted");
        assert_eq!(schema.id, Some(88001));
        assert_eq!(metadata.historical_schemas.get(&88001), Some(&schema));
    }

    #[test]
    fn create_tablet_refreshes_stale_standalone_metadata_when_schema_differs() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let tablet_id = 41007;
        let root = temp_dir.path().to_str().expect("temp path to str");
        let first = build_test_create_tablet_req(tablet_id, false);
        create_lake_tablet_from_req(&first, root, None).expect("create first tablet");

        let mut second = build_test_create_tablet_req(tablet_id, false);
        second.tablet_schema.id = Some(88002);
        second.tablet_schema.schema_hash = 1002;
        create_lake_tablet_from_req(&second, root, None).expect("refresh stale tablet metadata");

        let metadata = load_tablet_metadata_at_version(root, tablet_id, 1)
            .expect("load refreshed tablet metadata")
            .expect("metadata should exist");
        let schema = metadata.schema.expect("schema should be persisted");
        assert_eq!(schema.id, Some(88002));
        assert_eq!(metadata.historical_schemas.get(&88002), Some(&schema));
    }

    #[test]
    fn create_tablet_writes_raw_initial_metadata_when_optimized() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let req = build_test_create_tablet_req(41002, true);

        create_lake_tablet_from_req(
            &req,
            temp_dir.path().to_str().expect("temp path to str"),
            None,
        )
        .expect("create tablet");

        let standalone_path = temp_dir
            .path()
            .join("meta/000000000000A02A_0000000000000001.meta");
        let initial_path = temp_dir
            .path()
            .join("meta/0000000000000000_0000000000000001.meta");
        assert!(!standalone_path.exists(), "unexpected standalone metadata");
        assert!(initial_path.exists(), "expected initial raw metadata");

        let metadata = load_tablet_metadata_at_version(
            temp_dir.path().to_str().expect("temp path to str"),
            41002,
            1,
        )
        .expect("load metadata")
        .expect("metadata should exist");
        let schema = metadata.schema.expect("schema should be persisted");
        assert_eq!(schema.id, Some(88001));
        assert_eq!(metadata.historical_schemas.get(&88001), Some(&schema));
    }

    #[test]
    fn create_tablet_optimized_shared_root_writes_standalone_for_following_tablets() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let first = build_test_create_tablet_req(41003, true);
        let second = build_test_create_tablet_req(41004, true);
        let root = temp_dir.path().to_str().expect("temp path to str");

        create_lake_tablet_from_req(&first, root, None).expect("create first tablet");
        create_lake_tablet_from_req(&second, root, None).expect("create second tablet");

        let initial_path = temp_dir
            .path()
            .join("meta/0000000000000000_0000000000000001.meta");
        let second_standalone_path = temp_dir
            .path()
            .join("meta/000000000000A02C_0000000000000001.meta");
        assert!(initial_path.exists(), "expected shared initial metadata");
        assert!(
            second_standalone_path.exists(),
            "expected standalone v1 metadata for later optimized tablet"
        );

        let metadata = load_tablet_metadata_at_version(root, 41004, 1)
            .expect("load second tablet metadata")
            .expect("second tablet metadata should exist");
        let schema = metadata.schema.expect("schema should be persisted");
        assert_eq!(schema.id, Some(88001));
        assert_eq!(metadata.historical_schemas.get(&88001), Some(&schema));
    }

    #[test]
    fn create_tablet_ignores_bundle_versions_missing_new_tablet_page() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let first = build_test_create_tablet_req(41005, true);
        let second = build_test_create_tablet_req(41006, true);
        let root = temp_dir.path().to_str().expect("temp path to str");

        create_lake_tablet_from_req(&first, root, None).expect("create first tablet");

        let first_meta = load_tablet_metadata_at_version(root, 41005, 1)
            .expect("load first tablet metadata")
            .expect("first tablet metadata should exist");
        let first_schema = first_meta
            .schema
            .clone()
            .expect("first tablet schema should exist");
        write_bundle_meta_file(root, 41005, 2, &first_schema, &first_meta)
            .expect("write v2 bundle metadata for first tablet");

        create_lake_tablet_from_req(&second, root, None)
            .expect("create second tablet even when v2 bundle lacks its page");

        let second_meta = load_tablet_metadata_at_version(root, 41006, 1)
            .expect("load second tablet metadata")
            .expect("second tablet metadata should exist");
        let second_schema = second_meta
            .schema
            .expect("second tablet schema should exist");
        assert_eq!(second_schema.id, Some(88001));
    }

    fn build_test_create_tablet_req(
        tablet_id: i64,
        enable_tablet_creation_optimization: bool,
    ) -> crate::thrift::agent_service::TCreateTabletReq {
        let column = crate::thrift::descriptors::TColumn {
            column_name: "c1".to_string(),
            column_type: Some(crate::thrift::types::TColumnType {
                type_: crate::thrift::types::TPrimitiveType::BIGINT,
                len: Some(8),
                index_len: Some(8),
                precision: None,
                scale: None,
            }),
            aggregation_type: None,
            is_key: Some(true),
            is_allow_null: Some(true),
            default_value: None,
            default_expr: None,
            is_bloom_filter_column: None,
            define_expr: None,
            is_auto_increment: Some(false),
            col_unique_id: Some(0),
            has_bitmap_index: Some(false),
            agg_state_desc: None,
            index_len: Some(8),
            type_desc: None,
        };
        let schema = crate::thrift::agent_service::TTabletSchema {
            short_key_column_count: 1,
            schema_hash: 1001,
            keys_type: crate::thrift::types::TKeysType::DUP_KEYS,
            storage_type: crate::thrift::types::TStorageType::COLUMN,
            columns: vec![column],
            bloom_filter_fpp: None,
            indexes: None,
            is_in_memory: None,
            id: Some(88001),
            sort_key_idxes: None,
            sort_key_unique_ids: None,
            schema_version: Some(0),
            compression_type: Some(crate::thrift::types::TCompressionType::LZ4_FRAME),
            compression_level: None,
        };
        crate::thrift::agent_service::TCreateTabletReq {
            tablet_id,
            tablet_schema: schema,
            version: None,
            version_hash: None,
            storage_medium: None,
            in_restore_mode: None,
            base_tablet_id: None,
            base_schema_hash: None,
            table_id: Some(99001),
            partition_id: Some(99101),
            allocation_term: None,
            is_eco_mode: None,
            storage_format: None,
            tablet_type: None,
            enable_persistent_index: Some(false),
            compression_type: Some(crate::thrift::types::TCompressionType::LZ4_FRAME),
            binlog_config: None,
            persistent_index_type: None,
            primary_index_cache_expire_sec: None,
            create_schema_file: Some(false),
            compression_level: None,
            enable_tablet_creation_optimization: Some(enable_tablet_creation_optimization),
            timeout_ms: None,
            gtid: Some(0),
            flat_json_config: None,
            compaction_strategy: None,
        }
    }
}
