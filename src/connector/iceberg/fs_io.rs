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

use crate::fs::access::{FsAccessHandle, FsAccessResolver};
use crate::fs::object_store::ObjectStoreConfig;
use crate::fs::opendal::OpendalRangeReaderFactory;

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

    pub(crate) fn reader_factory(&self) -> std::result::Result<OpendalRangeReaderFactory, String> {
        self.handle.reader_factory()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct IcebergFsStorageFactory {
    #[serde(default)]
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
    #[serde(default)]
    object_store_config: Option<ObjectStoreConfig>,
}

impl IcebergFsStorage {
    pub(crate) fn new(object_store_config: Option<ObjectStoreConfig>) -> Self {
        Self {
            object_store_config,
        }
    }

    fn resolve_path(&self, path: &str) -> Result<(IcebergFsAccess, String)> {
        let access = resolve_access_for_location(path, self.object_store_config.as_ref())
            .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("resolve fs path: {e}")))?;
        let relative_path = access.single_relative_path().map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("resolve fs relative path: {e}"),
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
        let (access, relative_path) = self.resolve_path(path)?;
        access.operator().exists(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("fs exists({path}) through {relative_path}: {e}"),
            )
        })
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (access, relative_path) = self.resolve_path(path)?;
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
        let (access, relative_path) = self.resolve_path(path)?;
        let data = access.operator().read(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("fs read({path}) through {relative_path}: {e}"),
            )
        })?;
        Ok(data.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (access, relative_path) = self.resolve_path(path)?;
        Ok(Box::new(IcebergFsFileRead {
            access,
            relative_path,
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (access, relative_path) = self.resolve_path(path)?;
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
        let (access, relative_path) = self.resolve_path(path)?;
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
        let (access, relative_path) = self.resolve_path(path)?;
        access.operator().delete(&relative_path).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("fs delete({path}) through {relative_path}: {e}"),
            )
        })
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (access, relative_path) = self.resolve_path(path)?;
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

        let access = self.access.clone();
        let relative_path = self.relative_path.clone();
        tokio::task::spawn_blocking(move || {
            let factory = access.reader_factory().map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("build fs range reader factory: {e}"),
                )
            })?;
            let reader = factory.open(&relative_path).map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("open fs range reader({relative_path}): {e}"),
                )
            })?;
            reader
                .read_remote_range(range.start, range.end)
                .map_err(|e| {
                    Error::new(
                        ErrorKind::DataInvalid,
                        format!(
                            "fs range read({relative_path} {}..{}): {e}",
                            range.start, range.end
                        ),
                    )
                })
        })
        .await
        .map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("join fs range read task: {e}"),
            )
        })?
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
    let handle = FsAccessResolver::new().resolve_locations(locations, object_store_config)?;
    Ok(IcebergFsAccess::new(handle))
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

    let access = resolve_access_for_location(path, object_store_config)?;
    let relative_path = access.single_relative_path()?.to_string();
    let factory = access.reader_factory()?;
    let reader = factory
        .open(&relative_path)
        .map_err(|e| format!("open fs range reader({path}) through {relative_path}: {e}"))?;
    reader
        .read_remote_range(range.start, range.end)
        .map_err(|e| {
            format!(
                "fs range read({path}) through {relative_path} {}..{}: {e}",
                range.start, range.end
            )
        })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{
        build_file_io_for_location, read_exact_range, resolve_access_for_location,
        resolve_access_for_locations,
    };

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
    async fn input_file_reader_reads_range() {
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
    async fn input_file_reader_rejects_invalid_range() {
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

        let err = reader
            .read(7..3)
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
                .contains("object-store location requires object store config"),
            "{err}"
        );
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

        assert_eq!(err, "object-store location requires object store config");
    }
}
