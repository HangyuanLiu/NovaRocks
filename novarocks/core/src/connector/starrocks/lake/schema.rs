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

use std::sync::Arc;

use crate::connector::starrocks::lake::context::{TabletWriteContext, register_tablet_runtime};
use crate::connector::starrocks::lake::storage_domain::{
    StorageFlatJsonConfig, StorageTabletMetadata,
};
use crate::connector::starrocks::ports::StorageMetadataProvider;
use crate::connector::starrocks::schema::StarRocksTabletSchema;
use crate::formats::starrocks::writer::bundle_meta::{
    load_latest_tablet_metadata_with_provider, write_initial_meta_file_with_provider,
    write_standalone_meta_file_with_provider,
};
use crate::formats::starrocks::writer::io::read_bytes_if_exists;
use crate::formats::starrocks::writer::layout::{
    initial_meta_file_path, standalone_meta_file_path,
};
use crate::runtime::starlet_shard_registry::S3StoreConfig;

/// Protocol-neutral facts required to create a lake tablet. Protocol adapters
/// construct this value before entering the storage kernel.
#[derive(Clone)]
pub struct LakeCreateTabletTask {
    pub tablet_id: i64,
    pub table_id: i64,
    pub tablet_root_path: String,
    pub tablet_schema: StarRocksTabletSchema,
    pub s3_config: Option<S3StoreConfig>,
    pub enable_persistent_index: Option<bool>,
    pub persistent_index_type: Option<i32>,
    pub gtid: i64,
    pub compaction_strategy: Option<i32>,
    pub flat_json_config: Option<StorageFlatJsonConfig>,
    pub enable_tablet_creation_optimization: bool,
}

/// Executes a normalized lake tablet-create command through the explicitly
/// supplied storage metadata codec.
pub fn execute_lake_create_tablet_task(
    task: LakeCreateTabletTask,
    storage_metadata_provider: Arc<dyn StorageMetadataProvider>,
) -> Result<(), String> {
    let tablet_id = task.tablet_id;
    if tablet_id <= 0 {
        return Err(format!(
            "create_tablet has non-positive tablet_id={tablet_id}"
        ));
    }

    let runtime_ctx = TabletWriteContext {
        db_id: 0,
        table_id: task.table_id,
        tablet_id,
        tablet_root_path: task.tablet_root_path.clone(),
        tablet_schema: task.tablet_schema.clone(),
        s3_config: task.s3_config.clone(),
        storage_metadata_provider: Some(Arc::clone(&storage_metadata_provider)),
        partial_update: Default::default(),
    };
    register_tablet_runtime(&runtime_ctx)?;
    create_lake_tablet_metadata_with_provider(&task, storage_metadata_provider.as_ref())
}

fn create_lake_tablet_metadata_with_provider(
    task: &LakeCreateTabletTask,
    provider: &dyn StorageMetadataProvider,
) -> Result<(), String> {
    let tablet_id = task.tablet_id;
    let mut tablet_meta = StorageTabletMetadata {
        id: Some(tablet_id),
        version: Some(1),
        enable_persistent_index: task.enable_persistent_index,
        persistent_index_type: task.persistent_index_type,
        gtid: Some(task.gtid),
        compaction_strategy: task.compaction_strategy,
        flat_json_config: task.flat_json_config.clone(),
        ..Default::default()
    };
    seed_storage_tablet_metadata_schema(&mut tablet_meta, &task.tablet_schema);

    let standalone_v1_path = standalone_meta_file_path(&task.tablet_root_path, tablet_id, 1)?;
    if read_bytes_if_exists(&standalone_v1_path)?.is_some() {
        let (_, existing) =
            load_latest_tablet_metadata_with_provider(&task.tablet_root_path, tablet_id, provider)?;
        if existing.schema.as_ref() == Some(&task.tablet_schema) {
            return Ok(());
        }
        return write_standalone_meta_file_with_provider(
            &task.tablet_root_path,
            tablet_id,
            1,
            &tablet_meta,
            provider,
        );
    }

    let latest_version = match load_latest_tablet_metadata_with_provider(
        &task.tablet_root_path,
        tablet_id,
        provider,
    ) {
        Ok((version, _)) => version,
        Err(error) if is_missing_tablet_page_in_bundle_error(&error) => 0,
        Err(error) => return Err(error),
    };
    if latest_version > 1 {
        return Ok(());
    }

    if task.enable_tablet_creation_optimization {
        let initial_path = initial_meta_file_path(&task.tablet_root_path)?;
        if read_bytes_if_exists(&initial_path)?.is_some() {
            write_standalone_meta_file_with_provider(
                &task.tablet_root_path,
                tablet_id,
                1,
                &tablet_meta,
                provider,
            )
        } else {
            write_initial_meta_file_with_provider(&task.tablet_root_path, &tablet_meta, provider)
        }
    } else {
        write_standalone_meta_file_with_provider(
            &task.tablet_root_path,
            tablet_id,
            1,
            &tablet_meta,
            provider,
        )
    }
}

fn seed_storage_tablet_metadata_schema(
    metadata: &mut StorageTabletMetadata,
    tablet_schema: &StarRocksTabletSchema,
) {
    metadata.schema = Some(tablet_schema.clone());
    if let Some(schema_id) = tablet_schema.id.filter(|id| *id > 0) {
        metadata
            .historical_schemas
            .entry(schema_id)
            .or_insert_with(|| tablet_schema.clone());
    }
}

fn is_missing_tablet_page_in_bundle_error(error: &str) -> bool {
    error.contains("bundle metadata missing tablet page for tablet_id=")
        || error.contains("bundle metadata does not contain tablet page:")
}
