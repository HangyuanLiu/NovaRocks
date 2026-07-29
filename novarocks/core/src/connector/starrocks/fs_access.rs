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

use crate::runtime::starlet_shard_registry::S3StoreConfig;
use novarocks_fs::ObjectStoreConfig;
use novarocks_fs::{FsAccessHandle, FsAccessResolver, FsLocation, FsScheme, ResolvedFsPath};

use super::ObjectStoreProfile;

#[derive(Clone, Debug)]
pub(crate) struct StarRocksFsAccess {
    handle: FsAccessHandle,
}

pub(crate) fn tablet_root_scheme(tablet_root_path: &str) -> Result<FsScheme, String> {
    FsAccessResolver::new()
        .parse_location(tablet_root_path)
        .map(|location| location.scheme())
        .map_err(|error| error.to_string())
}

impl StarRocksFsAccess {
    pub(crate) fn scheme(&self) -> FsScheme {
        self.handle.scheme()
    }

    pub(crate) fn operator(&self) -> opendal::Operator {
        self.handle.operator()
    }

    pub(crate) fn paths(&self) -> &[ResolvedFsPath] {
        self.handle.paths()
    }

    pub(crate) fn single_relative_path(&self) -> Result<&str, String> {
        let paths = self.handle.paths();
        if paths.len() != 1 {
            return Err(format!(
                "expected exactly one StarRocks fs path, got {}",
                paths.len()
            ));
        }
        Ok(paths[0].operator_relative_path())
    }
}

pub(crate) fn resolve_tablet_root(
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<StarRocksFsAccess, String> {
    let tablet_root = classify_tablet_root(tablet_root_path, s3_config)?;
    let object_store_config = match tablet_root.s3_config {
        Some(config) => Some(config.to_object_store_config()),
        None => None,
    };
    resolve_with_object_store_config(tablet_root_path, object_store_config.as_ref())
}

pub(crate) fn resolve_runtime_path(path: &str) -> Result<StarRocksFsAccess, String> {
    let resolver = FsAccessResolver::new();
    let location = resolver
        .parse_location(path)
        .map_err(|error| error.to_string())?;
    match location.scheme() {
        FsScheme::Local => resolve_with_object_store_config(path, None),
        FsScheme::ObjectStore => {
            let config = crate::runtime::starlet_shard_registry::infer_s3_config_for_path(path)
                .ok_or_else(|| missing_runtime_s3_config_error(path))?
                .to_object_store_config();
            resolve_with_object_store_config(path, Some(&config))
        }
        FsScheme::Hdfs => Err(format!(
            "StarRocks formats do not support hdfs path yet: {path}"
        )),
    }
}

pub(crate) fn common_runtime_s3_config_for_paths<'a, I>(
    paths: I,
) -> Result<Option<crate::runtime::starlet_shard_registry::S3StoreConfig>, String>
where
    I: IntoIterator<Item = &'a str>,
{
    let paths = paths.into_iter().map(str::to_string).collect::<Vec<_>>();
    let first = paths
        .first()
        .ok_or_else(|| "StarRocks runtime paths are empty".to_string())?;
    let resolver = FsAccessResolver::new();
    let locations = resolver
        .parse_locations(paths.iter().map(String::as_str))
        .map_err(|error| error.to_string())?;
    let first_scheme = locations
        .first()
        .expect("non-empty runtime paths must produce locations")
        .scheme();
    if locations
        .iter()
        .any(|location| location.scheme() != first_scheme)
    {
        return Err("mixed StarRocks runtime path schemes are not allowed".to_string());
    }

    match first_scheme {
        FsScheme::Local => Ok(None),
        FsScheme::Hdfs => Err(format!(
            "StarRocks lake does not support hdfs tablet path yet: {first}"
        )),
        FsScheme::ObjectStore => {
            let mut selected: Option<crate::runtime::starlet_shard_registry::S3StoreConfig> = None;
            for path in &paths {
                let s3_config =
                    crate::runtime::starlet_shard_registry::infer_s3_config_for_path(path)
                        .ok_or_else(|| {
                            format!("missing S3 config for StarRocks object-store path={path}")
                        })?;
                match selected.as_ref() {
                    None => selected = Some(s3_config),
                    Some(prev) if prev == &s3_config => {}
                    Some(prev) => {
                        return Err(format!(
                            "inconsistent S3 config across StarRocks runtime paths: \
                             current_endpoint={} current_bucket={} previous_endpoint={} previous_bucket={}",
                            s3_config.endpoint, s3_config.bucket, prev.endpoint, prev.bucket
                        ));
                    }
                }
            }
            Ok(selected)
        }
    }
}

pub(crate) fn resolve_runtime_paths<'a, I>(paths: I) -> Result<StarRocksFsAccess, String>
where
    I: IntoIterator<Item = &'a str>,
{
    let paths = paths
        .into_iter()
        .map(|path| path.to_string())
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return Err("StarRocks runtime paths are empty".to_string());
    }

    let resolver = FsAccessResolver::new();
    let locations = resolver
        .parse_locations(paths.iter().map(String::as_str))
        .map_err(|error| error.to_string())?;
    let first = locations
        .first()
        .expect("non-empty runtime paths must produce locations");
    let scheme = first.scheme();
    if locations.iter().any(|location| location.scheme() != scheme) {
        return Err("mixed StarRocks runtime path schemes are not allowed".to_string());
    }

    match scheme {
        FsScheme::Local => resolve_with_object_store_config_many(&paths, None),
        FsScheme::ObjectStore => {
            let mut previous_config: Option<S3StoreConfig> = None;
            for path in &paths {
                let current_config =
                    crate::runtime::starlet_shard_registry::infer_s3_config_for_path(path)
                        .ok_or_else(|| missing_runtime_s3_config_error(path))?;
                if let Some(previous_config) = previous_config.as_ref() {
                    if previous_config != &current_config {
                        return Err(format!(
                            "inconsistent S3 config across StarRocks runtime paths: \
                             previous endpoint={} bucket={}, previous region={} path_style={}, \
                             current endpoint={} bucket={}, current region={} path_style={}; \
                             profile fields differ",
                            previous_config.endpoint,
                            previous_config.bucket,
                            display_region(previous_config),
                            display_path_style(previous_config),
                            current_config.endpoint,
                            current_config.bucket,
                            display_region(&current_config),
                            display_path_style(&current_config)
                        ));
                    }
                } else {
                    previous_config = Some(current_config.clone());
                }
            }

            let config = previous_config
                .expect("object-store runtime paths must infer at least one S3 config")
                .to_object_store_config();
            resolve_with_object_store_config_many(&paths, Some(&config))
        }
        FsScheme::Hdfs => Err(format!(
            "StarRocks formats do not support hdfs path yet: {}",
            paths[0]
        )),
    }
}

fn display_region(config: &S3StoreConfig) -> &str {
    config.region.as_deref().unwrap_or("<none>")
}

fn display_path_style(config: &S3StoreConfig) -> &'static str {
    match config.enable_path_style_access {
        Some(true) => "true",
        Some(false) => "false",
        None => "<unset>",
    }
}

pub(crate) fn resolve_with_profile(
    path: &str,
    profile: Option<&ObjectStoreProfile>,
) -> Result<StarRocksFsAccess, String> {
    let object_store_config = profile.map(ObjectStoreProfile::to_object_store_config);
    resolve_with_object_store_config(path, object_store_config.as_ref())
}

pub(crate) fn path_requires_object_store_profile(path: &str) -> Result<bool, String> {
    let resolver = FsAccessResolver::new();
    let location = resolver
        .parse_location(path)
        .map_err(|err| format!("{err}; path={path}"))?;
    match location.scheme() {
        FsScheme::Local => Ok(false),
        FsScheme::ObjectStore => Ok(true),
        FsScheme::Hdfs => Err(format!(
            "HDFS StarRocks fs path is unsupported; path={path}"
        )),
    }
}

pub(crate) fn object_store_profile_for_tablet_root(
    tablet_root_path: &str,
    s3_config: Option<&S3StoreConfig>,
) -> Result<Option<ObjectStoreProfile>, String> {
    let tablet_root = classify_tablet_root(tablet_root_path, s3_config)?;
    match tablet_root.location.scheme() {
        FsScheme::Local => Ok(None),
        FsScheme::ObjectStore => {
            let config = tablet_root.s3_config.ok_or_else(|| {
                format!("object-store tablet root requires S3 config; tablet_root_path={tablet_root_path}")
            })?;
            Ok(Some(ObjectStoreProfile::from_s3_store_config(config)?))
        }
        FsScheme::Hdfs => unreachable!("classify_tablet_root rejects HDFS"),
    }
}

fn missing_runtime_s3_config_error(path: &str) -> String {
    format!(
        "missing S3 config for StarRocks object-store path={path}; \
         register tablet runtime or provide shard credentials"
    )
}

fn resolve_with_object_store_config(
    path: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<StarRocksFsAccess, String> {
    let resolver = FsAccessResolver::new();
    let location = resolver
        .parse_location(path)
        .map_err(|err| format!("{err}; path={path}"))?;
    match location.scheme() {
        FsScheme::Local => {
            if object_store_config.is_some() {
                return Err(format!(
                    "local StarRocks fs path must not be resolved with S3/ObjectStoreProfile config; path={path}"
                ));
            }
        }
        FsScheme::ObjectStore => {
            if object_store_config.is_none() {
                return Err(format!(
                    "object-store StarRocks fs path requires S3/ObjectStoreProfile config; path={path}"
                ));
            }
        }
        FsScheme::Hdfs => {
            return Err(format!(
                "HDFS StarRocks fs path is unsupported; path={path}"
            ));
        }
    }

    let handle = resolver
        .resolve_location(path, object_store_config)
        .map_err(|error| error.to_string())?;
    Ok(StarRocksFsAccess { handle })
}

fn resolve_with_object_store_config_many(
    paths: &[String],
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<StarRocksFsAccess, String> {
    let handle = FsAccessResolver::new()
        .resolve_locations(paths.iter().map(String::as_str), object_store_config)
        .map_err(|error| error.to_string())?;
    Ok(StarRocksFsAccess { handle })
}

struct ClassifiedTabletRoot<'a> {
    location: FsLocation,
    s3_config: Option<&'a S3StoreConfig>,
}

fn classify_tablet_root<'a>(
    tablet_root_path: &str,
    s3_config: Option<&'a S3StoreConfig>,
) -> Result<ClassifiedTabletRoot<'a>, String> {
    let resolver = FsAccessResolver::new();
    let location = resolver
        .parse_location(tablet_root_path)
        .map_err(|err| format!("{err}; tablet_root_path={tablet_root_path}"))?;
    match location.scheme() {
        FsScheme::Local => {
            if s3_config.is_some() {
                return Err(format!(
                    "local tablet root must not be resolved with S3 config; tablet_root_path={tablet_root_path}"
                ));
            }
            Ok(ClassifiedTabletRoot {
                location,
                s3_config: None,
            })
        }
        FsScheme::ObjectStore => {
            let config = s3_config.ok_or_else(|| {
                format!("object-store tablet root requires S3 config; tablet_root_path={tablet_root_path}")
            })?;
            let bucket = location.authority().ok_or_else(|| {
                format!(
                    "object-store tablet root missing bucket; tablet_root_path={tablet_root_path}"
                )
            })?;
            if bucket != config.bucket {
                return Err(format!(
                    "object-store tablet root bucket '{bucket}' does not match S3 config bucket '{}'; tablet_root_path={tablet_root_path}",
                    config.bucket
                ));
            }
            Ok(ClassifiedTabletRoot {
                location,
                s3_config: Some(config),
            })
        }
        FsScheme::Hdfs => Err(format!(
            "HDFS tablet root is unsupported for StarRocks fs access; tablet_root_path={tablet_root_path}"
        )),
    }
}
