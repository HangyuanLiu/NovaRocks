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

// FS-6 stages this facade before later migration tasks wire it into the
// StarRocks writer, metadata, and reader paths.
#![allow(dead_code)]

use opendal::Operator;

use crate::connector::starrocks::ObjectStoreProfile;
use crate::connector::starrocks::fs_access::{
    StarRocksFsAccess, resolve_runtime_path, resolve_with_profile,
};
use crate::fs::access::{FsAccessResolver, FsScheme};

#[derive(Clone, Debug)]
pub(crate) struct StarRocksFormatPathAccess {
    access: StarRocksFsAccess,
}

impl StarRocksFormatPathAccess {
    pub(crate) fn scheme(&self) -> FsScheme {
        self.access.scheme()
    }

    pub(crate) fn operator(&self) -> Operator {
        self.access.operator()
    }

    pub(crate) fn single_relative_path(&self) -> Result<&str, String> {
        self.access.single_relative_path()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct StarRocksFormatTabletAccess {
    root_location: String,
    root_relative_path: String,
    access: StarRocksFsAccess,
}

impl StarRocksFormatTabletAccess {
    pub(crate) fn scheme(&self) -> FsScheme {
        self.access.scheme()
    }

    pub(crate) fn operator(&self) -> Operator {
        self.access.operator()
    }

    pub(crate) fn join_relative_path(&self, rel_path: &str) -> String {
        join_path(&self.root_location, rel_path)
    }

    pub(crate) fn operator_relative_path(&self, rel_path: &str) -> String {
        join_path(&self.root_relative_path, rel_path)
    }
}

pub(crate) fn resolve_format_path(path: &str) -> Result<StarRocksFormatPathAccess, String> {
    let access = resolve_runtime_path(path)?;
    Ok(StarRocksFormatPathAccess { access })
}

pub(crate) fn resolve_format_tablet_access(
    tablet_root_path: &str,
    object_store_profile: Option<&ObjectStoreProfile>,
) -> Result<StarRocksFormatTabletAccess, String> {
    let access = resolve_with_profile(tablet_root_path, object_store_profile)?;
    let root_relative_path = access.single_relative_path()?.to_string();
    let root_location = normalize_root_location(tablet_root_path);
    Ok(StarRocksFormatTabletAccess {
        root_location,
        root_relative_path,
        access,
    })
}

pub(crate) fn operator_relative_path_for_tablet_root(
    tablet_root_path: &str,
    rel_path: &str,
) -> Result<String, String> {
    let location = FsAccessResolver::new().parse_location(tablet_root_path)?;
    let rel = rel_path.trim_start_matches('/');
    match location.scheme() {
        FsScheme::Local => Ok(rel.to_string()),
        FsScheme::ObjectStore => {
            let root = location.path().trim_matches('/');
            if root.is_empty() {
                Ok(rel.to_string())
            } else if rel.is_empty() {
                Ok(root.to_string())
            } else {
                Ok(format!("{root}/{rel}"))
            }
        }
        FsScheme::Hdfs => Err(format!(
            "StarRocks formats do not support hdfs tablet path yet: {tablet_root_path}"
        )),
    }
}

fn normalize_root_location(path: &str) -> String {
    let path = path.trim();
    if path == "/" {
        return "/".to_string();
    }
    path.trim_end_matches('/').to_string()
}

fn join_path(base: &str, rel_path: &str) -> String {
    let rel_path = rel_path.trim_start_matches('/');
    if base == "/" {
        if rel_path.is_empty() {
            return "/".to_string();
        }
        return format!("/{rel_path}");
    }

    let base = base.trim_end_matches('/');
    if rel_path.is_empty() {
        return base.to_string();
    }
    if base.is_empty() {
        return rel_path.to_string();
    }
    format!("{base}/{rel_path}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::starlet_shard_registry::S3StoreConfig;

    fn sample_s3_config() -> S3StoreConfig {
        S3StoreConfig {
            endpoint: "http://127.0.0.1:9000".to_string(),
            bucket: "bucket-a".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            region: None,
            enable_path_style_access: Some(true),
        }
    }

    #[test]
    fn join_relative_path_appends_to_object_store_root() {
        assert_eq!(
            join_path("s3://bucket/warehouse/tablet", "meta/1.meta"),
            "s3://bucket/warehouse/tablet/meta/1.meta"
        );
    }

    #[test]
    fn join_relative_path_appends_to_local_root() {
        assert_eq!(
            join_path("/tmp/warehouse/tablet/", "/meta/1.meta"),
            "/tmp/warehouse/tablet/meta/1.meta"
        );
    }

    #[test]
    fn join_relative_path_returns_root_when_relative_path_is_empty() {
        assert_eq!(join_path("/tmp/root", ""), "/tmp/root");
    }

    #[test]
    fn join_relative_path_returns_relative_path_when_root_is_empty() {
        assert_eq!(join_path("", "/meta/1.meta"), "meta/1.meta");
    }

    #[test]
    fn join_relative_path_preserves_local_root() {
        assert_eq!(join_path("/", "meta/1.meta"), "/meta/1.meta");
    }

    #[test]
    fn join_relative_path_preserves_empty_relative_path_at_local_root() {
        assert_eq!(join_path("/", ""), "/");
    }

    #[test]
    fn local_tablet_access_resolves_display_and_operator_paths() {
        let temp_dir = tempfile::tempdir().expect("create temp dir");
        let tablet_root = temp_dir.path().join("tablet").to_string_lossy().to_string();
        let access =
            resolve_format_tablet_access(&tablet_root, None).expect("resolve local tablet root");

        assert_eq!(access.scheme(), FsScheme::Local);
        assert_eq!(
            access.join_relative_path("meta/1.meta"),
            format!("{tablet_root}/meta/1.meta")
        );
        assert!(
            access
                .operator_relative_path("meta/1.meta")
                .ends_with("meta/1.meta")
        );
    }

    #[test]
    fn local_root_tablet_access_preserves_display_root() {
        let access = resolve_format_tablet_access("/", None).expect("resolve local root");

        assert_eq!(access.join_relative_path("meta/1.meta"), "/meta/1.meta");
    }

    #[test]
    fn local_root_tablet_access_preserves_empty_display_root() {
        let access = resolve_format_tablet_access("/", None).expect("resolve local root");

        assert_eq!(access.join_relative_path(""), "/");
    }

    #[test]
    fn object_store_tablet_access_resolves_display_and_operator_paths() {
        let profile = ObjectStoreProfile::from_s3_store_config(&sample_s3_config())
            .expect("build object-store profile");
        let access = resolve_format_tablet_access(
            "s3://bucket-a/warehouse/db_1/table_2/100",
            Some(&profile),
        )
        .expect("resolve object-store tablet root");

        assert_eq!(access.scheme(), FsScheme::ObjectStore);
        assert_eq!(
            access.join_relative_path("meta/0000000000000000_0000000000000001.meta"),
            "s3://bucket-a/warehouse/db_1/table_2/100/meta/0000000000000000_0000000000000001.meta"
        );
        assert_eq!(
            access.operator_relative_path("meta/0000000000000000_0000000000000001.meta"),
            "warehouse/db_1/table_2/100/meta/0000000000000000_0000000000000001.meta"
        );
    }

    #[test]
    fn parse_only_local_tablet_root_returns_input_relative_path() {
        let rel = operator_relative_path_for_tablet_root("/tmp/tablet", "/data/seg.dat")
            .expect("resolve local operator-relative path");

        assert_eq!(rel, "data/seg.dat");
    }

    #[test]
    fn parse_only_object_store_tablet_root_does_not_require_profile() {
        let rel = operator_relative_path_for_tablet_root(
            "s3://bucket/warehouse/db_1/table_2/100",
            "data/seg.dat",
        )
        .expect("resolve object-store operator-relative path");

        assert_eq!(rel, "warehouse/db_1/table_2/100/data/seg.dat");
    }

    #[test]
    fn parse_only_object_store_tablet_root_empty_relative_path_returns_root() {
        let rel =
            operator_relative_path_for_tablet_root("s3://bucket/warehouse/db_1/table_2/100", "")
                .expect("resolve object-store root operator-relative path");

        assert_eq!(rel, "warehouse/db_1/table_2/100");
    }

    #[test]
    fn parse_only_hdfs_tablet_root_is_unsupported() {
        let err = operator_relative_path_for_tablet_root(
            "hdfs://nn:9000/warehouse/db_1/table_2/100",
            "data/seg.dat",
        )
        .expect_err("hdfs tablet root is unsupported");

        assert!(err.contains("hdfs tablet path yet"), "err={err}");
    }
}
