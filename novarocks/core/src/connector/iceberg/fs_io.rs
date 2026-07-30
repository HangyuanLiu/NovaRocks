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

use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use serde::{Deserialize, Serialize};

use novarocks_fs::{FsAccessHandle, FsAccessResolver, FsScheme, ObjectStoreConfig};

#[derive(Clone, Debug)]
pub(crate) struct IcebergFsAccess {
    handle: FsAccessHandle,
}

impl IcebergFsAccess {
    fn new(handle: FsAccessHandle) -> Self {
        Self { handle }
    }

    pub(crate) fn handle(&self) -> &FsAccessHandle {
        &self.handle
    }

    pub(crate) fn operator(&self) -> Operator {
        self.handle.operator()
    }

    pub(crate) fn single_relative_path(&self) -> std::result::Result<&str, String> {
        match self.handle.paths() {
            [path] => Ok(path.operator_relative_path()),
            [] => Err("fs access handle has no resolved paths".to_string()),
            paths => Err(format!(
                "fs access handle expected one resolved path, found {}",
                paths.len()
            )),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct IcebergFsStorageFactory {
    #[serde(skip, default)]
    object_store_config: Option<ObjectStoreConfig>,
}

impl IcebergFsStorageFactory {
    pub(crate) fn new(object_store_config: Option<ObjectStoreConfig>) -> Self {
        Self {
            object_store_config,
        }
    }
}

#[typetag::serde]
impl StorageFactory for IcebergFsStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(IcebergFsStorage::new(
            self.object_store_config.clone(),
        )))
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct IcebergFsStorage {
    #[serde(skip, default)]
    object_store_config: Option<ObjectStoreConfig>,
}

impl IcebergFsStorage {
    pub(crate) fn new(object_store_config: Option<ObjectStoreConfig>) -> Self {
        Self {
            object_store_config,
        }
    }

    fn resolve_path(&self, operation: &str, path: &str) -> Result<(IcebergFsAccess, String)> {
        let access =
            resolve_access_for_location(path, self.object_store_config.as_ref()).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("fs {operation}({path}) resolve path: {e}"),
                )
            })?;
        let relative_path = access.single_relative_path().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("fs {operation}({path}) resolve relative path: {e}"),
            )
        })?;
        Ok((access.clone(), relative_path.to_string()))
    }

    async fn ensure_parent_dir(op: &Operator, path: &str) -> Result<()> {
        let Some(parent) = Path::new(path).parent() else {
            return Ok(());
        };
        let mut parent = parent.to_string_lossy().replace('\\', "/");
        if parent.is_empty() || parent == "." {
            return Ok(());
        }
        if !parent.ends_with('/') {
            parent.push('/');
        }
        op.create_dir(&parent).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("create fs parent directory {parent}: {e}"),
            )
        })
    }

    fn storage_arc(&self) -> Arc<dyn Storage> {
        Arc::new(self.clone())
    }
}

#[typetag::serde]
#[async_trait]
impl Storage for IcebergFsStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (access, relative_path) = self.resolve_path("exists", path)?;
        access.operator().exists(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("fs exists({path}) through {relative_path}: {e}"),
            )
        })
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (access, relative_path) = self.resolve_path("metadata", path)?;
        let meta = access.operator().stat(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("fs metadata({path}) through {relative_path}: {e}"),
            )
        })?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (access, relative_path) = self.resolve_path("read", path)?;
        let data = access.operator().read(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("fs read({path}) through {relative_path}: {e}"),
            )
        })?;
        Ok(data.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (access, relative_path) = self.resolve_path("reader", path)?;
        Ok(Box::new(IcebergFsFileRead {
            access,
            relative_path,
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (access, relative_path) = self.resolve_path("write", path)?;
        let op = access.operator();
        Self::ensure_parent_dir(&op, &relative_path).await?;
        op.write(&relative_path, bs).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("fs write({path}) through {relative_path}: {e}"),
            )
        })?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (access, relative_path) = self.resolve_path("writer", path)?;
        let op = access.operator();
        Self::ensure_parent_dir(&op, &relative_path).await?;
        let writer = op.writer(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("fs writer({path}) through {relative_path}: {e}"),
            )
        })?;
        Ok(Box::new(IcebergFsFileWrite {
            writer: Some(writer),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (access, relative_path) = self.resolve_path("delete", path)?;
        access.operator().delete(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("fs delete({path}) through {relative_path}: {e}"),
            )
        })
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (access, relative_path) = self.resolve_path("delete_prefix", path)?;
        access
            .operator()
            .remove_all(&relative_path)
            .await
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("fs delete_prefix({path}) through {relative_path}: {e}"),
                )
            })
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(self.storage_arc(), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(self.storage_arc(), path.to_string()))
    }
}

#[derive(Debug)]
struct IcebergFsFileRead {
    access: IcebergFsAccess,
    relative_path: String,
}

#[async_trait]
impl FileRead for IcebergFsFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        if range.end < range.start {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("invalid fs read range {}..{}", range.start, range.end),
            ));
        }

        let operator = self.access.operator();
        let relative_path = self.relative_path.clone();
        operator
            .read_with(&relative_path)
            .range(range.clone())
            .await
            .map(|buffer| buffer.to_bytes())
            .map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!(
                        "fs range read({relative_path} {}..{}): {e}",
                        range.start, range.end
                    ),
                )
            })
    }
}

struct IcebergFsFileWrite {
    writer: Option<opendal::Writer>,
}

impl std::fmt::Debug for IcebergFsFileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergFsFileWrite")
            .field("open", &self.writer.is_some())
            .finish()
    }
}

#[async_trait]
impl FileWrite for IcebergFsFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "write to closed fs file"))?;
        writer
            .write(bs)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("fs write: {e}")))
    }

    async fn close(&mut self) -> Result<()> {
        let mut writer = self
            .writer
            .take()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "fs file already closed"))?;
        writer
            .close()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("fs close: {e}")))?;
        Ok(())
    }
}

pub(crate) fn build_file_io_for_location(
    location: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> FileIO {
    // FileIO construction is lazy: the SDK asks the storage to resolve paths
    // only when actual IO starts, so this helper stores credentials here and
    // leaves location validation to the per-operation FsAccessResolver call.
    let _ = location;
    FileIOBuilder::new(Arc::new(IcebergFsStorageFactory::new(
        object_store_config.cloned(),
    )))
    .build()
}

pub(crate) fn build_storage_factory_for_location(
    location: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Arc<dyn StorageFactory> {
    // StorageFactory construction is lazy for the same reason FileIO is: keep
    // credentials here and resolve concrete operators per IO call.
    let _ = location;
    Arc::new(IcebergFsStorageFactory::new(object_store_config.cloned()))
}

pub(crate) fn object_store_config_from_catalog_properties(
    props: &[(String, String)],
) -> std::result::Result<Option<ObjectStoreConfig>, String> {
    use std::collections::BTreeMap;

    let props_map: BTreeMap<String, String> = props
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let credentials =
        crate::fs::object_store_credentials::ObjectStoreCredentials::optional_from_aws_s3_properties(
            crate::fs::object_store_credentials::ObjectStoreCredentialsSource::AwsS3Properties,
            &props_map,
        )?;
    Ok(credentials.map(|credentials| credentials.to_object_store_config()))
}

pub(crate) fn resolve_access_for_location(
    location: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> std::result::Result<IcebergFsAccess, String> {
    resolve_access_for_locations(std::iter::once(location), object_store_config)
}

pub(crate) fn resolve_access_for_locations<I, S>(
    locations: I,
    object_store_config: Option<&ObjectStoreConfig>,
) -> std::result::Result<IcebergFsAccess, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let handle = FsAccessResolver::new()
        .resolve_locations(locations, object_store_config)
        .map_err(|error| error.to_string())?;
    Ok(IcebergFsAccess::new(handle))
}

pub(crate) fn format_resolved_location(
    handle: &FsAccessHandle,
    operator_relative_path: &str,
) -> std::result::Result<String, String> {
    let path = operator_relative_path.trim_start_matches('/');
    let location = handle
        .paths()
        .first()
        .map(|path| path.location())
        .ok_or_else(|| "fs access handle has no resolved paths".to_string())?;

    match location.scheme() {
        FsScheme::Local => {
            let root = handle
                .root()
                .ok_or_else(|| "local fs access handle missing root".to_string())?;
            let full_path = Path::new(root).join(path);
            Ok(format!("file://{}", full_path.display()))
        }
        FsScheme::ObjectStore => {
            let scheme = location.uri_scheme().unwrap_or("s3");
            let bucket = handle
                .authority()
                .or_else(|| location.authority())
                .ok_or_else(|| "object-store fs access handle missing bucket".to_string())?;
            Ok(format!("{scheme}://{bucket}/{path}"))
        }
        FsScheme::Hdfs => {
            let scheme = location.uri_scheme().unwrap_or("hdfs");
            let authority = handle
                .authority()
                .or_else(|| location.authority())
                .ok_or_else(|| "hdfs fs access handle missing authority".to_string())?;
            Ok(format!("{scheme}://{authority}/{path}"))
        }
    }
}

pub(crate) fn reader_factory_for_table_location(
    location: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> std::result::Result<FsAccessHandle, String> {
    let resolver = FsAccessResolver::new();
    let parsed = resolver
        .parse_location(location)
        .map_err(|e| format!("parse table fs location {location}: {e}"))?;
    resolver
        .resolve_location(parsed.original(), object_store_config)
        .map_err(|error| error.to_string())
}

pub(crate) fn normalize_hdfs_path_parse_only(path: &str) -> std::result::Result<String, String> {
    let location = FsAccessResolver::new()
        .parse_location(path)
        .map_err(|e| format!("parse hdfs location {path}: {e}"))?;
    if location.scheme() != FsScheme::Hdfs {
        return Err(format!("expected hdfs location: {path}"));
    }
    Ok(location.path().trim_start_matches('/').to_string())
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
    use bytes::Bytes;

    use super::{
        build_file_io_for_location, format_resolved_location,
        object_store_config_from_catalog_properties, read_exact_range, resolve_access_for_location,
        resolve_access_for_locations,
    };

    fn test_object_store_config() -> novarocks_fs::ObjectStoreConfig {
        novarocks_fs::ObjectStoreConfig {
            endpoint: "http://localhost:9000".to_string(),
            access_key_id: "ak".to_string(),
            access_key_secret: "sk".to_string(),
            session_token: None,
            enable_path_style_access: Some(true),
            region: Some("us-east-1".to_string()),
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        }
    }

    #[tokio::test]
    async fn file_io_round_trips_local_file_location() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("metadata.json");
        let location = format!("file://{}", file_path.display());

        let file_io = build_file_io_for_location(&location, None);
        let output = file_io.new_output(&location).expect("output file");
        output
            .write(Bytes::from_static(b"iceberg metadata"))
            .await
            .expect("write");

        let input = file_io.new_input(&location).expect("input file");
        assert!(input.exists().await.expect("exists"));
        assert_eq!(input.read().await.expect("read"), "iceberg metadata");
    }

    #[tokio::test]
    async fn file_io_writes_nested_local_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("metadata").join("00000.json");
        let location = format!("file://{}", file_path.display());

        let file_io = build_file_io_for_location(&location, None);
        file_io
            .new_output(&location)
            .expect("output file")
            .write(Bytes::from_static(b"nested metadata"))
            .await
            .expect("nested write");

        assert_eq!(
            file_io
                .new_input(&location)
                .expect("input file")
                .read()
                .await
                .expect("nested read"),
            "nested metadata"
        );
    }

    #[tokio::test]
    async fn output_file_writer_writes_and_closes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("writer").join("00000.json");
        let location = format!("file://{}", file_path.display());

        let file_io = build_file_io_for_location(&location, None);
        let mut writer = file_io
            .new_output(&location)
            .expect("output file")
            .writer()
            .await
            .expect("writer");
        writer
            .write(Bytes::from_static(b"writer payload"))
            .await
            .expect("writer write");
        writer.close().await.expect("writer close");

        assert_eq!(
            file_io
                .new_input(&location)
                .expect("input file")
                .read()
                .await
                .expect("read"),
            "writer payload"
        );
    }

    #[tokio::test]
    async fn iceberg_input_range_read_succeeds() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("data.bin");
        std::fs::write(&file_path, b"0123456789").expect("write data");
        let location = format!("file://{}", file_path.display());

        let file_io = build_file_io_for_location(&location, None);
        let reader = file_io
            .new_input(&location)
            .expect("input file")
            .reader()
            .await
            .expect("reader");

        assert_eq!(reader.read(3..7).await.expect("range read"), "3456");
    }

    #[tokio::test]
    async fn iceberg_input_file_rejects_invalid_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        let file_path = dir.path().join("data.bin");
        std::fs::write(&file_path, b"0123456789").expect("write data");
        let location = format!("file://{}", file_path.display());

        let file_io = build_file_io_for_location(&location, None);
        let reader = file_io
            .new_input(&location)
            .expect("input file")
            .reader()
            .await
            .expect("reader");

        let invalid_start = 7;
        let invalid_end = 3;
        let err = reader
            .read(invalid_start..invalid_end)
            .await
            .expect_err("invalid range should fail");

        assert!(
            err.to_string().contains("invalid fs read range 7..3"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn s3_file_io_without_credentials_fails_on_first_io() {
        let location = "s3://bucket/table/metadata.json";
        let file_io = build_file_io_for_location(location, None);
        let input = file_io.new_input(location).expect("input file");

        let err = input
            .exists()
            .await
            .expect_err("first object-store IO should fail");

        assert!(
            err.to_string()
                .contains("fs exists(s3://bucket/table/metadata.json) resolve path"),
            "{err}"
        );
        assert!(
            err.to_string()
                .contains("object-store location requires object store config"),
            "{err}"
        );
    }

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

        let bytes = read_exact_range(&location, 2..6, None).expect("range read");

        assert_eq!(bytes, "2345");
    }

    #[test]
    fn format_resolved_location_preserves_object_store_uri_scheme() {
        let cfg = test_object_store_config();

        for (location, expected) in [
            (
                "s3a://bucket/warehouse/table",
                "s3a://bucket/warehouse/table/data/a.parquet",
            ),
            (
                "oss://bucket/warehouse/table",
                "oss://bucket/warehouse/table/data/a.parquet",
            ),
        ] {
            let access =
                resolve_access_for_location(location, Some(&cfg)).expect("resolve object store");
            let formatted =
                format_resolved_location(access.handle(), "warehouse/table/data/a.parquet")
                    .expect("format location");

            assert_eq!(formatted, expected);
        }
    }

    #[test]
    fn resolves_multiple_local_locations_to_one_access_handle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let first = dir.path().join("a.bin");
        let second = dir.path().join("b.bin");
        std::fs::write(&first, b"a").expect("first");
        std::fs::write(&second, b"b").expect("second");
        let locations = [
            format!("file://{}", first.display()),
            format!("file://{}", second.display()),
        ];

        let access = resolve_access_for_locations(locations.iter().map(String::as_str), None)
            .expect("access");

        assert_eq!(
            access.handle().operator_relative_paths(),
            vec!["a.bin", "b.bin"]
        );
    }

    #[test]
    fn object_store_without_credentials_returns_resolver_error() {
        let err = resolve_access_for_location("s3://bucket/table/metadata.json", None)
            .expect_err("missing object-store config should fail");

        assert!(
            err.ends_with("object-store location requires object store config"),
            "unexpected resolver error: {err}"
        );
    }
}
