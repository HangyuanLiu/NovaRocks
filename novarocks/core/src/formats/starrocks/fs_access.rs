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
use novarocks_fs::ObjectStoreConfig;
use novarocks_fs::{FsAccessResolver, FsScheme};

const BUCKET_ROOT_COMPAT_MARKER: &str = "__novarocks_tablet_root_compat__";

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
    scheme: FsScheme,
    operator: Operator,
}

impl StarRocksFormatTabletAccess {
    pub(crate) fn scheme(&self) -> FsScheme {
        self.scheme
    }

    pub(crate) fn operator(&self) -> Operator {
        self.operator.clone()
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
    let object_store_config = object_store_profile.map(ObjectStoreProfile::to_object_store_config);
    resolve_format_tablet_access_with_object_store_config(
        tablet_root_path,
        object_store_config.as_ref(),
    )
}

pub(crate) fn resolve_format_tablet_access_with_object_store_config(
    tablet_root_path: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<StarRocksFormatTabletAccess, String> {
    if is_literal_local_root(tablet_root_path) {
        if object_store_config.is_some() {
            return Err(format!(
                "local StarRocks fs path must not be resolved with object-store config; path={tablet_root_path}"
            ));
        }
        let operator = FsAccessResolver::new()
            .resolve_location("/__novarocks_local_root__", None)
            .map_err(|error| error.to_string())?
            .operator();
        return Ok(StarRocksFormatTabletAccess {
            root_location: "/".to_string(),
            root_relative_path: String::new(),
            scheme: FsScheme::Local,
            operator,
        });
    }

    if let Some(bucket_root) = parse_bucket_root_object_store_tablet_path(tablet_root_path)? {
        // FsLocation requires a non-empty object-store path. Resolve a synthetic
        // path in the same bucket to build the operator while preserving the old
        // bucket-root tablet semantics at this facade boundary.
        let handle = FsAccessResolver::new()
            .resolve_location(&bucket_root.synthetic_path, object_store_config)
            .map_err(|error| error.to_string())?;
        return Ok(StarRocksFormatTabletAccess {
            root_location: bucket_root.normalized_root,
            root_relative_path: String::new(),
            scheme: handle.scheme(),
            operator: handle.operator(),
        });
    }

    let resolver = FsAccessResolver::new();
    let location = resolver
        .parse_location(tablet_root_path)
        .map_err(|err| format!("{err}; path={tablet_root_path}"))?;
    match location.scheme() {
        FsScheme::Local => {
            if object_store_config.is_some() {
                return Err(format!(
                    "local StarRocks fs path must not be resolved with object-store config; path={tablet_root_path}"
                ));
            }
        }
        FsScheme::ObjectStore => {
            if object_store_config.is_none() {
                return Err(format!(
                    "object-store StarRocks fs path requires object-store config; path={tablet_root_path}"
                ));
            }
        }
        FsScheme::Hdfs => {
            return Err(format!(
                "HDFS StarRocks fs path is unsupported; path={tablet_root_path}"
            ));
        }
    }

    let handle = resolver
        .resolve_location(tablet_root_path, object_store_config)
        .map_err(|error| error.to_string())?;
    let root_relative_path = handle
        .paths()
        .first()
        .ok_or_else(|| "resolved tablet root has no path".to_string())?
        .operator_relative_path()
        .to_string();
    let root_location = normalize_root_location(tablet_root_path);
    Ok(StarRocksFormatTabletAccess {
        root_location,
        root_relative_path,
        scheme: handle.scheme(),
        operator: handle.operator(),
    })
}

pub(crate) fn operator_relative_path_for_tablet_root(
    tablet_root_path: &str,
    rel_path: &str,
) -> Result<String, String> {
    if is_literal_local_root(tablet_root_path) {
        return Ok(rel_path.trim_start_matches('/').to_string());
    }

    if parse_bucket_root_object_store_tablet_path(tablet_root_path)?.is_some() {
        return Ok(rel_path.trim_start_matches('/').to_string());
    }

    let location = FsAccessResolver::new()
        .parse_location(tablet_root_path)
        .map_err(|error| error.to_string())?;
    let rel = rel_path.trim_start_matches('/');
    match location.scheme() {
        FsScheme::Local => {
            let access = resolve_with_profile(tablet_root_path, None)?;
            Ok(join_path(access.single_relative_path()?, rel_path))
        }
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

fn is_literal_local_root(tablet_root_path: &str) -> bool {
    tablet_root_path.trim() == "/"
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct BucketRootObjectStoreTabletPath {
    normalized_root: String,
    synthetic_path: String,
}

fn parse_bucket_root_object_store_tablet_path(
    tablet_root_path: &str,
) -> Result<Option<BucketRootObjectStoreTabletPath>, String> {
    let tablet_root_path = tablet_root_path.trim();
    let Some((scheme, rest)) = tablet_root_path.split_once("://") else {
        return Ok(None);
    };
    let scheme = scheme.to_ascii_lowercase();
    if !matches!(scheme.as_str(), "s3" | "s3a" | "oss") {
        return Ok(None);
    }

    let (bucket, path) = rest.split_once('/').unwrap_or((rest, ""));
    if bucket.is_empty() {
        return Ok(None);
    }
    if !path.trim_matches('/').is_empty() {
        return Ok(None);
    }

    let normalized_root = format!("{scheme}://{bucket}");
    let synthetic_path = format!("{normalized_root}/{BUCKET_ROOT_COMPAT_MARKER}");
    Ok(Some(BucketRootObjectStoreTabletPath {
        normalized_root,
        synthetic_path,
    }))
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
