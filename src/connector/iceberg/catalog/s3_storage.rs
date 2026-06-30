//! S3-backed storage implementation for Iceberg SDK.
//!
//! Implements the `iceberg::io::Storage` and `StorageFactory` traits using
//! OpenDAL S3 operator, allowing Iceberg metadata and data files to be stored
//! on S3-compatible object storage (MinIO, AWS S3, Aliyun OSS).

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use opendal::Operator;
use serde::{Deserialize, Serialize};

use crate::fs::object_store::ObjectStoreConfig;

// ---------------------------------------------------------------------------
// S3Storage — implements iceberg::io::Storage via OpenDAL
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct S3Storage {
    endpoint: String,
    access_key_id: String,
    access_key_secret: String,
    #[serde(default)]
    session_token: Option<String>,
    region: String,
    enable_path_style: bool,
    #[serde(default)]
    retry_max_times: Option<usize>,
    #[serde(default)]
    retry_min_delay_ms: Option<u64>,
    #[serde(default)]
    retry_max_delay_ms: Option<u64>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    io_timeout_ms: Option<u64>,
    #[serde(skip)]
    operators: std::sync::Mutex<HashMap<String, Operator>>,
}

impl S3Storage {
    fn new(
        endpoint: &str,
        access_key_id: &str,
        access_key_secret: &str,
        session_token: Option<&str>,
        region: &str,
        enable_path_style: bool,
        retry_max_times: Option<usize>,
        retry_min_delay_ms: Option<u64>,
        retry_max_delay_ms: Option<u64>,
        timeout_ms: Option<u64>,
        io_timeout_ms: Option<u64>,
    ) -> Self {
        Self {
            endpoint: endpoint.to_string(),
            access_key_id: access_key_id.to_string(),
            access_key_secret: access_key_secret.to_string(),
            session_token: session_token.map(str::to_string),
            region: region.to_string(),
            enable_path_style,
            retry_max_times,
            retry_min_delay_ms,
            retry_max_delay_ms,
            timeout_ms,
            io_timeout_ms,
            operators: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Parse an s3:// path into (bucket, key).
    fn parse_path(path: &str) -> Result<(String, String)> {
        let stripped = path
            .strip_prefix("s3://")
            .or_else(|| path.strip_prefix("s3a://"))
            .or_else(|| path.strip_prefix("oss://"))
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::DataInvalid,
                    format!("S3Storage: unsupported path scheme: {path}"),
                )
            })?;
        let slash = stripped.find('/').ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("S3Storage: path has no key: {path}"),
            )
        })?;
        let bucket = &stripped[..slash];
        let key = &stripped[slash + 1..];
        Ok((bucket.to_string(), key.to_string()))
    }

    fn get_operator(&self, bucket: &str) -> Result<Operator> {
        let mut cache = self
            .operators
            .lock()
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("lock failed: {e}")))?;
        if let Some(op) = cache.get(bucket) {
            return Ok(op.clone());
        }

        let cfg = ObjectStoreConfig {
            endpoint: self.endpoint.clone(),
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
            session_token: self.session_token.clone(),
            enable_path_style_access: Some(self.enable_path_style),
            region: Some(self.region.clone()),
            retry_max_times: self.retry_max_times,
            retry_min_delay_ms: self.retry_min_delay_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            timeout_ms: self.timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
        };
        let op = crate::fs::object_store::build_object_store_operator(bucket, &cfg)
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("build S3 operator: {e}")))?;
        cache.insert(bucket.to_string(), op.clone());
        Ok(op)
    }
}

#[typetag::serde]
#[async_trait]
impl Storage for S3Storage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (bucket, key) = Self::parse_path(path)?;
        let op = self.get_operator(&bucket)?;
        match op.exists(&key).await {
            Ok(exists) => Ok(exists),
            Err(e) => Err(Error::new(
                ErrorKind::Unexpected,
                format!("S3 exists({path}): {e}"),
            )),
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (bucket, key) = Self::parse_path(path)?;
        let op = self.get_operator(&bucket)?;
        let meta = op
            .stat(&key)
            .await
            .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("S3 metadata({path}): {e}")))?;
        Ok(FileMetadata {
            size: meta.content_length(),
        })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (bucket, key) = Self::parse_path(path)?;
        let op = self.get_operator(&bucket)?;
        let data = op
            .read(&key)
            .await
            .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("S3 read({path}): {e}")))?;
        Ok(data.to_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (bucket, key) = Self::parse_path(path)?;
        let op = self.get_operator(&bucket)?;
        Ok(Box::new(S3FileRead { operator: op, key }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (bucket, key) = Self::parse_path(path)?;
        let op = self.get_operator(&bucket)?;
        op.write(&key, bs)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("S3 write({path}): {e}")))?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (bucket, key) = Self::parse_path(path)?;
        let op = self.get_operator(&bucket)?;
        let w = op
            .writer(&key)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("S3 writer({path}): {e}")))?;
        Ok(Box::new(S3FileWrite { writer: Some(w) }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (bucket, key) = Self::parse_path(path)?;
        let op = self.get_operator(&bucket)?;
        op.delete(&key)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("S3 delete({path}): {e}")))?;
        Ok(())
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (bucket, key) = Self::parse_path(path)?;
        let op = self.get_operator(&bucket)?;
        op.remove_all(&key).await.map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("S3 delete_prefix({path}): {e}"),
            )
        })?;
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        // Build a new S3Storage with the same config for the InputFile.
        let storage = Arc::new(S3Storage::new(
            &self.endpoint,
            &self.access_key_id,
            &self.access_key_secret,
            self.session_token.as_deref(),
            &self.region,
            self.enable_path_style,
            self.retry_max_times,
            self.retry_min_delay_ms,
            self.retry_max_delay_ms,
            self.timeout_ms,
            self.io_timeout_ms,
        ));
        Ok(InputFile::new(storage, path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        let storage = Arc::new(S3Storage::new(
            &self.endpoint,
            &self.access_key_id,
            &self.access_key_secret,
            self.session_token.as_deref(),
            &self.region,
            self.enable_path_style,
            self.retry_max_times,
            self.retry_min_delay_ms,
            self.retry_max_delay_ms,
            self.timeout_ms,
            self.io_timeout_ms,
        ));
        Ok(OutputFile::new(storage, path.to_string()))
    }
}

// ---------------------------------------------------------------------------
// S3FileRead
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct S3FileRead {
    operator: Operator,
    key: String,
}

#[async_trait]
impl FileRead for S3FileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let data = self
            .operator
            .read_with(&self.key)
            .range(range)
            .await
            .map_err(|e| Error::new(ErrorKind::DataInvalid, format!("S3 range read: {e}")))?;
        Ok(data.to_bytes())
    }
}

// ---------------------------------------------------------------------------
// S3FileWrite
// ---------------------------------------------------------------------------

struct S3FileWrite {
    writer: Option<opendal::Writer>,
}

impl std::fmt::Debug for S3FileWrite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3FileWrite")
            .field("open", &self.writer.is_some())
            .finish()
    }
}

#[async_trait]
impl FileWrite for S3FileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let w = self
            .writer
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "write to closed S3 file"))?;
        w.write(bs)
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("S3 write: {e}")))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let mut w = self
            .writer
            .take()
            .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "S3 file already closed"))?;
        w.close()
            .await
            .map_err(|e| Error::new(ErrorKind::Unexpected, format!("S3 close: {e}")))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// S3StorageFactory
// ---------------------------------------------------------------------------

/// Factory that creates S3Storage instances from catalog S3 properties.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct S3StorageFactory {
    pub endpoint: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    #[serde(default)]
    pub session_token: Option<String>,
    pub region: String,
    pub enable_path_style: bool,
    #[serde(default)]
    pub retry_max_times: Option<usize>,
    #[serde(default)]
    pub retry_min_delay_ms: Option<u64>,
    #[serde(default)]
    pub retry_max_delay_ms: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub io_timeout_ms: Option<u64>,
}

impl S3StorageFactory {
    /// Build from catalog properties (aws.s3.access_key, aws.s3.secret_key, etc.).
    pub fn from_catalog_properties(
        props: &[(String, String)],
    ) -> std::result::Result<Option<Self>, String> {
        let props_map: BTreeMap<String, String> = props
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let credentials =
            crate::fs::object_store_credentials::ObjectStoreCredentials::optional_from_aws_s3_properties(
                crate::fs::object_store_credentials::ObjectStoreCredentialsSource::AwsS3Properties,
                &props_map,
            )?;
        let Some(credentials) = credentials else {
            return Ok(None);
        };

        Ok(Some(Self {
            endpoint: credentials.endpoint,
            access_key_id: credentials.access_key_id,
            access_key_secret: credentials.access_key_secret,
            session_token: credentials.session_token,
            region: credentials
                .region
                .unwrap_or_else(|| "us-east-1".to_string()),
            enable_path_style: credentials.enable_path_style_access.unwrap_or(false),
            retry_max_times: credentials.retry_max_times,
            retry_min_delay_ms: credentials.retry_min_delay_ms,
            retry_max_delay_ms: credentials.retry_max_delay_ms,
            timeout_ms: credentials.timeout_ms,
            io_timeout_ms: credentials.io_timeout_ms,
        }))
    }
}

#[typetag::serde]
impl StorageFactory for S3StorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(S3Storage::new(
            &self.endpoint,
            &self.access_key_id,
            &self.access_key_secret,
            self.session_token.as_deref(),
            &self.region,
            self.enable_path_style,
            self.retry_max_times,
            self.retry_min_delay_ms,
            self.retry_max_delay_ms,
            self.timeout_ms,
            self.io_timeout_ms,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::S3StorageFactory;

    #[test]
    fn s3_storage_factory_uses_shared_credentials_aliases() {
        let props = vec![
            (
                "aws.s3.endpoint_url".to_string(),
                "http://localhost:9000".to_string(),
            ),
            ("aws.s3.accessKeyId".to_string(), "ak".to_string()),
            ("aws.s3.accessKeySecret".to_string(), "sk".to_string()),
            (
                "aws.s3.enable_path_style_access".to_string(),
                "1".to_string(),
            ),
        ];

        let factory = S3StorageFactory::from_catalog_properties(&props)
            .expect("parse factory")
            .expect("factory");

        assert_eq!(factory.endpoint, "http://localhost:9000");
        assert_eq!(factory.access_key_id, "ak");
        assert_eq!(factory.access_key_secret, "sk");
        assert_eq!(factory.region, "us-east-1");
        assert!(factory.enable_path_style);
    }

    #[test]
    fn s3_storage_factory_preserves_session_token() {
        let props = vec![
            (
                "aws.s3.endpoint_url".to_string(),
                "http://localhost:9000".to_string(),
            ),
            ("aws.s3.accessKeyId".to_string(), "ak".to_string()),
            ("aws.s3.accessKeySecret".to_string(), "sk".to_string()),
            ("aws.s3.sessionToken".to_string(), "token".to_string()),
        ];

        let factory = S3StorageFactory::from_catalog_properties(&props)
            .expect("parse factory")
            .expect("factory");

        assert_eq!(factory.session_token.as_deref(), Some("token"));
    }

    #[test]
    fn s3_storage_factory_preserves_retry_and_timeout_policy() {
        let props = vec![
            (
                "aws.s3.endpoint_url".to_string(),
                "http://localhost:9000".to_string(),
            ),
            ("aws.s3.accessKeyId".to_string(), "ak".to_string()),
            ("aws.s3.accessKeySecret".to_string(), "sk".to_string()),
            ("aws.s3.max_retries".to_string(), "7".to_string()),
            ("aws.s3.retry_min_delay_ms".to_string(), "11".to_string()),
            ("aws.s3.retry_max_delay_ms".to_string(), "99".to_string()),
            ("aws.s3.request_timeout_ms".to_string(), "1234".to_string()),
            ("aws.s3.io_timeout_ms".to_string(), "5678".to_string()),
        ];

        let factory = S3StorageFactory::from_catalog_properties(&props)
            .expect("parse factory")
            .expect("factory");

        assert_eq!(factory.retry_max_times, Some(7));
        assert_eq!(factory.retry_min_delay_ms, Some(11));
        assert_eq!(factory.retry_max_delay_ms, Some(99));
        assert_eq!(factory.timeout_ms, Some(1234));
        assert_eq!(factory.io_timeout_ms, Some(5678));
    }

    #[test]
    fn s3_storage_factory_rejects_invalid_path_style_property() {
        let props = vec![
            (
                "aws.s3.endpoint_url".to_string(),
                "http://localhost:9000".to_string(),
            ),
            ("aws.s3.accessKeyId".to_string(), "ak".to_string()),
            ("aws.s3.accessKeySecret".to_string(), "sk".to_string()),
            (
                "aws.s3.enable_path_style_access".to_string(),
                "maybe".to_string(),
            ),
        ];

        let err = S3StorageFactory::from_catalog_properties(&props)
            .expect_err("invalid path style should fail");

        assert!(
            err.contains(
                "aws_s3_properties object-store property aws.s3.enable_path_style_access has invalid boolean value: maybe"
            ),
            "{err}"
        );
    }
}
