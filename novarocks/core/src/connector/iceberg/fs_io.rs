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

//! Core adapters for the provider-owned Iceberg filesystem boundary.
//!
//! Concrete FileIO, storage resolution, credential normalization, and location
//! formatting live below Core. Core retains only its synchronous application
//! bridge for legacy callers that have not yet moved to explicit provider
//! resources.

use std::ops::Range;

use bytes::Bytes;
use novarocks_fs::ObjectStoreConfig;

pub use novarocks_connector_iceberg::fs_io::{
    IcebergFsAccess, build_file_io_for_location, build_storage_factory_for_location,
    format_resolved_location, normalize_hdfs_path_parse_only, reader_factory_for_table_location,
    resolve_access_for_location, resolve_access_for_locations,
};

pub(crate) fn object_store_config_from_catalog_properties(
    props: &[(String, String)],
) -> std::result::Result<Option<ObjectStoreConfig>, String> {
    use std::collections::BTreeMap;

    let props_map: BTreeMap<String, String> = props
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    novarocks_fs::object_store_config_from_aws_s3_catalog_properties(&props_map)
}

pub(crate) fn read_exact_range(
    path: &str,
    range: Range<u64>,
    object_store_config: Option<&ObjectStoreConfig>,
) -> std::result::Result<Bytes, String> {
    if range.end < range.start {
        return Err(format!(
            "invalid fs read range {}..{}",
            range.start, range.end
        ));
    }
    if range.is_empty() {
        return Ok(Bytes::new());
    }

    let access = resolve_access_for_location(path, object_store_config)?;
    let file = access
        .handle()
        .bind(0, novarocks_fs::FileIdentity::new(path, 0, None))
        .map_err(|error| error.to_string())?;
    let read_range = novarocks_fs::FileReadRange::bounded(range.start, range.end - range.start)
        .map_err(|error| error.to_string())?;
    let cancellation = novarocks_fs::FileCancellation::new();
    crate::runtime::global_async_runtime::data_block_on(async move {
        file.read(read_range, &cancellation).await
    })
    .map_err(|error| error.to_string())?
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::object_store_config_from_catalog_properties;

    #[test]
    fn catalog_properties_build_optional_object_store_config() {
        let props = vec![
            (
                "aws.s3.endpoint_url".to_string(),
                "http://localhost:9000".to_string(),
            ),
            ("aws.s3.accessKeyId".to_string(), "ak".to_string()),
            ("aws.s3.accessKeySecret".to_string(), "sk".to_string()),
            (
                "aws.s3.enable_path_style_access".to_string(),
                "true".to_string(),
            ),
            ("aws.s3.sessionToken".to_string(), "token".to_string()),
            ("aws.s3.max_retries".to_string(), "9".to_string()),
        ];

        let cfg = object_store_config_from_catalog_properties(&props)
            .expect("parse catalog properties")
            .expect("object-store config");

        assert_eq!(cfg.endpoint, "http://localhost:9000");
        assert_eq!(cfg.access_key_id, "ak");
        assert_eq!(cfg.access_key_secret, "sk");
        assert_eq!(cfg.session_token.as_deref(), Some("token"));
        assert_eq!(cfg.enable_path_style_access, Some(true));
        assert_eq!(cfg.retry_max_times, Some(9));
    }

    #[test]
    fn catalog_properties_without_complete_credentials_return_none() {
        let props = vec![(
            "aws.s3.endpoint_url".to_string(),
            "http://localhost:9000".to_string(),
        )];

        let cfg = object_store_config_from_catalog_properties(&props)
            .expect("incomplete credentials are optional");

        assert!(cfg.is_none());
    }

    #[test]
    fn read_exact_range_uses_resolved_operator_relative_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("data.bin");
        std::fs::write(&file_path, b"0123456789").expect("write test data");
        let location = format!("file://{}", file_path.display());

        let bytes = super::read_exact_range(&location, 2..6, None).expect("range read");

        assert_eq!(bytes, "2345");
    }
}
