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

use crate::common::app_config::StandaloneStarRocksTableConfig as AppStarRocksTableConfig;
use crate::runtime::starlet_shard_registry::S3StoreConfig;

use novarocks_fs::parse_object_store_path_parse_only;

#[derive(Clone, Debug)]
pub(crate) struct StarRocksTableConfig {
    pub(crate) warehouse_uri: String,
    pub(crate) s3: S3StoreConfig,
    pub(crate) mv_default_storage_engine: String,
}

impl StarRocksTableConfig {
    pub(crate) fn from_app_config(config: AppStarRocksTableConfig) -> Result<Self, String> {
        let warehouse_uri = config
            .warehouse_uri
            .trim()
            .trim_end_matches('/')
            .to_string();
        if warehouse_uri.is_empty() {
            return Err("standalone StarRocks table warehouse_uri is empty".to_string());
        }
        // Only the bucket is extracted into the cluster-level S3 profile.
        // The warehouse path component lives in `warehouse_uri` and is
        // reused by `tablet_root_path` to mint absolute tablet URIs; the
        // OpenDAL operator never depends on it as a builder root.
        let (bucket, _warehouse_path) = parse_object_store_path_parse_only(&warehouse_uri)
            .map_err(|error| error.to_string())?;
        let mv_default_storage_engine = config
            .mv_default_storage_engine
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("iceberg")
            .to_string();
        if mv_default_storage_engine != "iceberg" {
            return Err(format!(
                "invalid mv_default_storage_engine `{mv_default_storage_engine}`; allowed: iceberg"
            ));
        }
        Ok(Self {
            warehouse_uri,
            s3: S3StoreConfig {
                endpoint: config.endpoint.trim().to_string(),
                bucket,
                access_key_id: config.access_key_id.trim().to_string(),
                access_key_secret: config.access_key_secret.trim().to_string(),
                region: config.region.as_ref().map(|value| value.trim().to_string()),
                enable_path_style_access: config.enable_path_style_access,
            },
            mv_default_storage_engine,
        })
    }

    pub(crate) fn tablet_root_path(&self, db_id: i64, table_id: i64, partition_id: i64) -> String {
        // All tablets in a partition share the same root so partition replacement
        // can switch visibility without rewriting tablet-internal object layout.
        format!(
            "{}/db_{db_id}/table_{table_id}/partition_{partition_id}",
            self.warehouse_uri
        )
    }
}
