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

use crate::fs::access::{FsAccessHandle, FsAccessResolver, FsLocation, FsScheme, ResolvedFsPath};
use crate::fs::object_store::ObjectStoreConfig;
use crate::fs::opendal::OpendalRangeReaderFactory;
use crate::runtime::starlet_shard_registry::S3StoreConfig;

use super::ObjectStoreProfile;

#[derive(Clone, Debug)]
pub(crate) struct StarRocksFsAccess {
    handle: FsAccessHandle,
}

pub(crate) fn tablet_root_scheme(tablet_root_path: &str) -> Result<FsScheme, String> {
    FsAccessResolver::new()
        .parse_location(tablet_root_path)
        .map(|location| location.scheme())
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

    pub(crate) fn reader_factory(&self) -> Result<OpendalRangeReaderFactory, String> {
        self.handle.reader_factory()
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
    let location = resolver.parse_location(path)?;
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
    let locations = resolver.parse_locations(paths.iter().map(String::as_str))?;
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

    let handle = resolver.resolve_location(path, object_store_config)?;
    Ok(StarRocksFsAccess { handle })
}

fn resolve_with_object_store_config_many(
    paths: &[String],
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<StarRocksFsAccess, String> {
    let handle = FsAccessResolver::new()
        .resolve_locations(paths.iter().map(String::as_str), object_store_config)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::starrocks::lake::context::{
        TabletWriteContext, lock_runtime_test_state, register_tablet_runtime,
    };
    use crate::service::grpc_client::proto::starrocks::TabletSchemaPb;

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

    fn runtime_context(
        tablet_id: i64,
        tablet_root_path: &str,
        s3_config: S3StoreConfig,
    ) -> TabletWriteContext {
        TabletWriteContext {
            db_id: 1,
            table_id: 2,
            tablet_id,
            tablet_root_path: tablet_root_path.to_string(),
            tablet_schema: TabletSchemaPb::default(),
            s3_config: Some(s3_config),
            partial_update: Default::default(),
        }
    }

    #[test]
    fn local_tablet_root_rejects_s3_config() {
        let err = resolve_tablet_root("/lake/tablet-1", Some(&test_s3_config()))
            .expect_err("local tablet root must not accept S3 config");

        assert!(err.contains("local"), "{err}");
        assert!(err.contains("S3"), "{err}");
        assert!(err.contains("tablet_root_path=/lake/tablet-1"), "{err}");
    }

    #[test]
    fn object_store_tablet_root_requires_s3_config() {
        let err = resolve_tablet_root("s3://bucket-a/warehouse/tablet-1", None)
            .expect_err("object-store tablet root requires S3 config");

        assert!(err.contains("object-store"), "{err}");
        assert!(err.contains("S3"), "{err}");
        assert!(
            err.contains("tablet_root_path=s3://bucket-a/warehouse/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn tablet_root_scheme_classifies_object_store_without_s3_config() {
        let scheme = tablet_root_scheme("s3://bucket-a/warehouse/tablet-1")
            .expect("classify object-store tablet root");

        assert_eq!(scheme, FsScheme::ObjectStore);
    }

    #[test]
    fn object_store_tablet_root_resolves_relative_path() {
        let access =
            resolve_tablet_root("s3://bucket-a/warehouse/tablet-1", Some(&test_s3_config()))
                .expect("resolve object-store tablet root");

        assert_eq!(access.scheme(), FsScheme::ObjectStore);
        assert_eq!(
            access.single_relative_path().expect("single path"),
            "warehouse/tablet-1"
        );
        assert_eq!(
            access.paths()[0].operator_relative_path(),
            "warehouse/tablet-1"
        );
        let _operator = access.operator();
        let _factory = access.reader_factory().expect("range reader factory");
    }

    #[test]
    fn single_relative_path_rejects_multiple_resolved_paths() {
        let handle = FsAccessResolver::new()
            .resolve_locations(["/tmp/tablet-1", "/tmp/tablet-2"], None)
            .expect("resolve multiple local paths");
        let access = StarRocksFsAccess { handle };

        let err = access
            .single_relative_path()
            .expect_err("multiple resolved paths must be rejected");
        assert!(err.contains("expected exactly one StarRocks fs path, got 2"));
    }

    #[test]
    fn object_store_tablet_root_rejects_s3_bucket_mismatch() {
        let err = resolve_tablet_root("s3://bucket-b/warehouse/tablet-1", Some(&test_s3_config()))
            .expect_err("object-store tablet root bucket must match S3 config");

        assert!(err.contains("bucket-b"), "{err}");
        assert!(err.contains("bucket-a"), "{err}");
        assert!(
            err.contains("tablet_root_path=s3://bucket-b/warehouse/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn hdfs_tablet_root_is_unsupported() {
        let err = resolve_tablet_root("hdfs://nn:9000/starrocks/tablet-1", None)
            .expect_err("HDFS tablet root is unsupported");

        assert!(err.contains("HDFS"), "{err}");
        assert!(err.contains("unsupported"), "{err}");
        assert!(
            err.contains("tablet_root_path=hdfs://nn:9000/starrocks/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn local_path_rejects_object_store_profile() {
        let profile = super::super::ObjectStoreProfile::from_s3_store_config(&test_s3_config())
            .expect("build object-store profile");
        let err = resolve_with_profile("/lake/tablet-1", Some(&profile))
            .expect_err("local path must not accept object-store profile");

        assert!(err.contains("local"), "{err}");
        assert!(err.contains("ObjectStoreProfile"), "{err}");
        assert!(err.contains("path=/lake/tablet-1"), "{err}");
    }

    #[test]
    fn object_store_path_requires_object_store_profile() {
        let err = resolve_with_profile("s3://bucket-a/warehouse/tablet-1", None)
            .expect_err("object-store path requires object-store profile");

        assert!(err.contains("object-store"), "{err}");
        assert!(err.contains("ObjectStoreProfile"), "{err}");
        assert!(
            err.contains("path=s3://bucket-a/warehouse/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn hdfs_path_with_profile_is_unsupported() {
        let profile = super::super::ObjectStoreProfile::from_s3_store_config(&test_s3_config())
            .expect("build object-store profile");
        let err = resolve_with_profile("hdfs://nn:9000/starrocks/tablet-1", Some(&profile))
            .expect_err("HDFS path is unsupported");

        assert!(err.contains("HDFS"), "{err}");
        assert!(err.contains("unsupported"), "{err}");
        assert!(
            err.contains("path=hdfs://nn:9000/starrocks/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn object_store_profile_for_tablet_root_maps_s3_config() {
        let profile = object_store_profile_for_tablet_root(
            "s3://bucket-a/warehouse/tablet-1",
            Some(&test_s3_config()),
        )
        .expect("build profile for object-store tablet root")
        .expect("object-store root should return profile");

        assert_eq!(profile.endpoint, "http://localhost:9000");
        assert_eq!(profile.access_key_id, "ak");
    }

    #[test]
    fn object_store_profile_for_tablet_root_returns_none_for_local() {
        let profile = object_store_profile_for_tablet_root("/lake/tablet-1", None)
            .expect("local tablet root should not need profile");

        assert_eq!(profile, None);
    }

    #[test]
    fn object_store_profile_for_tablet_root_rejects_local_with_s3_config() {
        let err = object_store_profile_for_tablet_root("/lake/tablet-1", Some(&test_s3_config()))
            .expect_err("local tablet root must reject S3 config");

        assert!(err.contains("local"), "{err}");
        assert!(err.contains("S3"), "{err}");
        assert!(err.contains("tablet_root_path=/lake/tablet-1"), "{err}");
    }

    #[test]
    fn object_store_profile_for_tablet_root_rejects_hdfs() {
        let err = object_store_profile_for_tablet_root(
            "hdfs://nn:9000/starrocks/tablet-1",
            Some(&test_s3_config()),
        )
        .expect_err("HDFS tablet root is unsupported");

        assert!(err.contains("HDFS"), "{err}");
        assert!(err.contains("unsupported"), "{err}");
        assert!(
            err.contains("tablet_root_path=hdfs://nn:9000/starrocks/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn path_requires_object_store_profile_distinguishes_local_and_object_store() {
        assert!(
            !path_requires_object_store_profile("/lake/tablet-1").expect("local path should parse")
        );
        assert!(
            path_requires_object_store_profile("s3://bucket-a/warehouse/tablet-1")
                .expect("object-store path should parse")
        );
    }

    #[test]
    fn path_requires_object_store_profile_rejects_hdfs() {
        let err = path_requires_object_store_profile("hdfs://nn:9000/starrocks/tablet-1")
            .expect_err("HDFS is unsupported");

        assert!(err.contains("HDFS"), "{err}");
        assert!(err.contains("unsupported"), "{err}");
        assert!(
            err.contains("path=hdfs://nn:9000/starrocks/tablet-1"),
            "{err}"
        );
    }

    #[test]
    fn runtime_path_resolves_local_without_s3_config() {
        let path = std::env::temp_dir()
            .join("novarocks-fs-access-runtime")
            .join("1.meta");
        let path = path.to_string_lossy().to_string();

        let access = resolve_runtime_path(&path).expect("resolve local runtime path");

        assert_eq!(access.scheme(), FsScheme::Local);
        assert!(
            access
                .single_relative_path()
                .expect("relative path")
                .ends_with("1.meta")
        );
    }

    #[test]
    fn runtime_path_rejects_unknown_object_store_credentials() {
        let _guard = lock_runtime_test_state();

        let err = resolve_runtime_path("s3://missing-bucket/warehouse/tablet-1/1.meta")
            .expect_err("unknown object-store path must require runtime credentials");

        assert!(err.contains("missing S3 config"), "{err}");
    }

    #[test]
    fn runtime_path_resolves_object_store_from_registered_runtime() {
        let _guard = lock_runtime_test_state();
        let ctx = runtime_context(
            91_001,
            "s3://bucket-a/warehouse/tablet-1",
            sample_s3_config(),
        );
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let access = resolve_runtime_path("s3://bucket-a/warehouse/tablet-1/1.meta")
            .expect("resolve registered object-store runtime path");

        assert_eq!(access.scheme(), FsScheme::ObjectStore);
        assert_eq!(
            access.single_relative_path().expect("relative path"),
            "warehouse/tablet-1/1.meta"
        );
    }

    #[test]
    fn runtime_paths_resolve_object_store_from_consistent_registered_runtime() {
        let _guard = lock_runtime_test_state();
        let ctx = runtime_context(
            91_002,
            "s3://bucket-a/warehouse/tablet-1",
            sample_s3_config(),
        );
        register_tablet_runtime(&ctx).expect("register tablet runtime");

        let access = resolve_runtime_paths([
            "s3://bucket-a/warehouse/tablet-1/1.meta",
            "s3://bucket-a/warehouse/tablet-1/2.dat",
        ])
        .expect("resolve registered object-store runtime paths");

        let relative_paths = access
            .paths()
            .iter()
            .map(|path| path.operator_relative_path())
            .collect::<Vec<_>>();
        assert_eq!(access.scheme(), FsScheme::ObjectStore);
        assert_eq!(
            relative_paths,
            vec!["warehouse/tablet-1/1.meta", "warehouse/tablet-1/2.dat"]
        );
    }

    #[test]
    fn runtime_paths_reject_inconsistent_registered_runtime_configs() {
        let _guard = lock_runtime_test_state();
        let mut first_config = sample_s3_config();
        first_config.region = Some("us-east-1".to_string());
        first_config.enable_path_style_access = Some(true);
        let mut second_config = sample_s3_config();
        second_config.region = Some("us-west-2".to_string());
        second_config.enable_path_style_access = Some(false);
        register_tablet_runtime(&runtime_context(
            91_003,
            "s3://bucket-a/warehouse/tablet-1",
            first_config,
        ))
        .expect("register first tablet runtime");
        register_tablet_runtime(&runtime_context(
            91_004,
            "s3://bucket-a/warehouse/tablet-2",
            second_config,
        ))
        .expect("register second tablet runtime");

        let err = resolve_runtime_paths([
            "s3://bucket-a/warehouse/tablet-1/1.meta",
            "s3://bucket-a/warehouse/tablet-2/1.meta",
        ])
        .expect_err("inconsistent runtime S3 configs must fail");

        assert!(
            err.contains("inconsistent S3 config across StarRocks runtime paths"),
            "{err}"
        );
        assert!(
            err.contains("previous endpoint=http://127.0.0.1:9000 bucket=bucket-a"),
            "{err}"
        );
        assert!(
            err.contains("current endpoint=http://127.0.0.1:9000 bucket=bucket-a"),
            "{err}"
        );
        assert!(
            err.contains("previous region=us-east-1 path_style=true"),
            "{err}"
        );
        assert!(
            err.contains("current region=us-west-2 path_style=false"),
            "{err}"
        );
    }

    #[test]
    fn runtime_paths_reject_empty_inputs() {
        let err = resolve_runtime_paths([]).expect_err("empty runtime paths must fail");

        assert!(err.contains("StarRocks runtime paths are empty"), "{err}");
    }

    #[test]
    fn runtime_paths_reject_mixed_schemes() {
        let local_path = std::env::temp_dir()
            .join("novarocks-fs-access-runtime")
            .join("1.meta");
        let local_path = local_path.to_string_lossy().to_string();
        let paths = [
            local_path.as_str(),
            "s3://bucket-a/warehouse/tablet-1/1.meta",
        ];

        let err = resolve_runtime_paths(paths).expect_err("mixed runtime schemes must fail");

        assert!(err.contains("mixed"), "{err}");
    }

    #[test]
    fn runtime_path_rejects_hdfs() {
        let err = resolve_runtime_path("hdfs://nn:9000/starrocks/tablet-1/1.meta")
            .expect_err("hdfs runtime path must fail");

        assert!(
            err.contains("StarRocks formats do not support hdfs path yet"),
            "{err}"
        );
    }

    fn test_s3_config() -> S3StoreConfig {
        S3StoreConfig {
            endpoint: "http://localhost:9000".to_string(),
            bucket: "bucket-a".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            region: Some("us-east-1".to_string()),
            enable_path_style_access: Some(true),
        }
    }
}
