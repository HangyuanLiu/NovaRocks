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

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use opendal::Operator;
use opendal::layers::{ConcurrentLimitLayer, HttpClientLayer, RetryLayer, TimeoutLayer};
use url::Url;

use crate::{FileCancellation, FileError, FileErrorKind, FileReadRange, FileResult};

const DEFAULT_OBJECT_STORE_RETRY_MAX_TIMES: usize = 6;
const DEFAULT_OBJECT_STORE_RETRY_MIN_DELAY_MS: u64 = 100;
const DEFAULT_OBJECT_STORE_RETRY_MAX_DELAY_MS: u64 = 10_000;
const DEFAULT_OBJECT_STORE_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_OBJECT_STORE_IO_TIMEOUT_MS: u64 = 60_000;
const DEFAULT_OBJECT_STORE_CONCURRENT_LIMIT: usize = 1024;
const DEFAULT_OBJECT_STORE_HTTP_CONCURRENT_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FsScheme {
    Local,
    ObjectStore,
    Hdfs,
}

impl FsScheme {
    pub fn is_object_store(self) -> bool {
        self == Self::ObjectStore
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FsLocation {
    original: String,
    scheme: FsScheme,
    uri_scheme: Option<String>,
    authority: Option<String>,
    path: String,
}

impl FsLocation {
    pub fn parse(raw: impl AsRef<str>) -> FileResult<Self> {
        let original = raw.as_ref().trim();
        if original.is_empty() {
            return Err(FileError::invalid("fs location is empty"));
        }

        let Some((uri_scheme, rest)) = split_uri_scheme(original) else {
            if original.contains("://") {
                return Err(FileError::unsupported(format!(
                    "unsupported fs location scheme: {original}"
                )));
            }
            return Ok(Self::local(original, None, original));
        };
        let uri_scheme = uri_scheme.to_ascii_lowercase();

        match uri_scheme.as_str() {
            "file" => Self::parse_file(original, uri_scheme, rest),
            "s3" | "s3a" | "oss" => {
                let (authority, path) =
                    parse_authority_and_path(original, rest, true, uri_scheme.as_str())?;
                Ok(Self {
                    original: original.to_string(),
                    scheme: FsScheme::ObjectStore,
                    uri_scheme: Some(uri_scheme),
                    authority,
                    path,
                })
            }
            "hdfs" => {
                let (authority, path) = parse_authority_and_path(original, rest, true, "hdfs")?;
                Ok(Self {
                    original: original.to_string(),
                    scheme: FsScheme::Hdfs,
                    uri_scheme: Some(uri_scheme),
                    authority,
                    path,
                })
            }
            _ => Err(FileError::unsupported(format!(
                "unsupported fs location scheme: {original}"
            ))),
        }
    }

    pub fn original(&self) -> &str {
        &self.original
    }

    pub fn scheme(&self) -> FsScheme {
        self.scheme
    }

    pub fn uri_scheme(&self) -> Option<&str> {
        self.uri_scheme.as_deref()
    }

    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    fn local(original: &str, uri_scheme: Option<String>, path: &str) -> Self {
        Self {
            original: original.to_string(),
            scheme: FsScheme::Local,
            uri_scheme,
            authority: None,
            path: path.to_string(),
        }
    }

    fn parse_file(original: &str, uri_scheme: String, rest: &str) -> FileResult<Self> {
        if let Some(without_prefix) = rest.strip_prefix("//") {
            if without_prefix.starts_with('/') {
                ensure_non_empty_path(original, "file", without_prefix)?;
                return Ok(Self::local(original, Some(uri_scheme), without_prefix));
            }

            let (authority, path) = without_prefix
                .split_once('/')
                .unwrap_or((without_prefix, ""));
            if !authority.is_empty() && authority != "localhost" {
                return Err(FileError::unsupported(format!(
                    "unsupported file URI host in local path: {original}"
                )));
            }
            let path = if path.is_empty() {
                ""
            } else {
                &without_prefix[authority.len()..]
            };
            ensure_non_empty_path(original, "file", path)?;
            return Ok(Self::local(original, Some(uri_scheme), path));
        }

        ensure_non_empty_path(original, "file", rest)?;
        Ok(Self::local(original, Some(uri_scheme), rest))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedFsPath {
    location: FsLocation,
    operator_relative_path: String,
}

impl ResolvedFsPath {
    pub fn new(
        location: FsLocation,
        operator_relative_path: impl Into<String>,
    ) -> FileResult<Self> {
        let operator_relative_path = operator_relative_path.into();
        if operator_relative_path.trim().is_empty() {
            return Err(FileError::invalid("operator-relative path is empty"));
        }
        Ok(Self {
            location,
            operator_relative_path,
        })
    }

    pub fn location(&self) -> &FsLocation {
        &self.location
    }

    pub fn operator_relative_path(&self) -> &str {
        &self.operator_relative_path
    }
}

#[derive(Clone)]
pub struct FsAccessHandle {
    scheme: FsScheme,
    operator: Operator,
    authority: Option<String>,
    root: Option<String>,
    paths: Vec<ResolvedFsPath>,
}

impl FsAccessHandle {
    pub fn new(
        scheme: FsScheme,
        operator: Operator,
        authority: Option<String>,
        root: Option<String>,
        paths: Vec<ResolvedFsPath>,
    ) -> Self {
        Self {
            scheme,
            operator,
            authority,
            root,
            paths,
        }
    }

    pub fn scheme(&self) -> FsScheme {
        self.scheme
    }

    pub fn operator(&self) -> Operator {
        self.operator.clone()
    }

    pub fn authority(&self) -> Option<&str> {
        self.authority.as_deref()
    }

    pub fn root(&self) -> Option<&str> {
        self.root.as_deref()
    }

    pub fn paths(&self) -> &[ResolvedFsPath] {
        &self.paths
    }

    pub fn bind(&self, path_index: usize, identity: FileIdentity) -> FileResult<BoundFile> {
        let path = self
            .paths
            .get(path_index)
            .ok_or_else(|| {
                FileError::invalid(format!("file path index out of bounds: {path_index}"))
            })?
            .clone();
        Ok(BoundFile {
            access: self.clone(),
            path,
            identity,
        })
    }

    pub fn operator_relative_paths(&self) -> Vec<&str> {
        self.paths
            .iter()
            .map(ResolvedFsPath::operator_relative_path)
            .collect()
    }
}

impl Debug for FsAccessHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsAccessHandle")
            .field("scheme", &self.scheme)
            .field("authority", &self.authority)
            .field("root", &self.root)
            .field("paths", &self.paths)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct BoundFile {
    access: FsAccessHandle,
    path: ResolvedFsPath,
    identity: FileIdentity,
}

impl BoundFile {
    pub fn access(&self) -> &FsAccessHandle {
        &self.access
    }

    pub fn location(&self) -> &FsLocation {
        self.path.location()
    }

    pub fn operator_relative_path(&self) -> &str {
        self.path.operator_relative_path()
    }

    pub fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    pub async fn read(
        &self,
        range: FileReadRange,
        cancellation: &FileCancellation,
    ) -> FileResult<Bytes> {
        cancellation.check()?;
        let result = match range {
            FileReadRange::WholeFile => {
                self.access
                    .operator
                    .read(self.operator_relative_path())
                    .await
            }
            FileReadRange::Bounded { offset, length } => {
                let end = offset
                    .checked_add(length)
                    .ok_or_else(|| FileError::invalid("bounded file read range overflows"))?;
                self.access
                    .operator
                    .read_with(self.operator_relative_path())
                    .range(offset..end)
                    .await
            }
        };
        cancellation.check()?;
        result
            .map(|buffer| buffer.to_bytes())
            .map_err(|error| map_opendal_error("read file", error))
    }

    pub async fn stat(&self, cancellation: &FileCancellation) -> FileResult<u64> {
        cancellation.check()?;
        let metadata = self
            .access
            .operator
            .stat(self.operator_relative_path())
            .await
            .map_err(|error| map_opendal_error("stat file", error))?;
        cancellation.check()?;
        Ok(metadata.content_length())
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct FileIdentity {
    path: String,
    file_size: u64,
    modification_time: Option<i64>,
}

impl FileIdentity {
    pub fn new(path: impl Into<String>, file_size: u64, modification_time: Option<i64>) -> Self {
        Self {
            path: path.into(),
            file_size,
            modification_time,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn modification_time(&self) -> Option<i64> {
        self.modification_time
    }

    pub fn with_modification_time_override(mut self, modification_time: Option<i64>) -> Self {
        if modification_time.is_some() {
            self.modification_time = modification_time;
        }
        self
    }

    pub fn starrocks_cache_tail(&self) -> u32 {
        if self.modification_time.unwrap_or(0) > 0 {
            ((self.modification_time.unwrap_or(0) >> 9) & 0x0000_0000_FFFF_FFFF) as u32
        } else {
            self.file_size as u32
        }
    }
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub struct ObjectStoreConfig {
    pub endpoint: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub session_token: Option<String>,
    pub enable_path_style_access: Option<bool>,
    pub region: Option<String>,
    pub retry_max_times: Option<usize>,
    pub retry_min_delay_ms: Option<u64>,
    pub retry_max_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub io_timeout_ms: Option<u64>,
}

impl Debug for ObjectStoreConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectStoreConfig")
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &"<redacted>")
            .field("access_key_secret", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .field("enable_path_style_access", &self.enable_path_style_access)
            .field("region", &self.region)
            .field("retry_max_times", &self.retry_max_times)
            .field("retry_min_delay_ms", &self.retry_min_delay_ms)
            .field("retry_max_delay_ms", &self.retry_max_delay_ms)
            .field("timeout_ms", &self.timeout_ms)
            .field("io_timeout_ms", &self.io_timeout_ms)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct FsAccessResolver;

impl FsAccessResolver {
    pub fn new() -> Self {
        Self
    }

    pub fn parse_location(&self, raw: impl AsRef<str>) -> FileResult<FsLocation> {
        FsLocation::parse(raw)
    }

    pub fn parse_locations<I, S>(&self, locations: I) -> FileResult<Vec<FsLocation>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        locations
            .into_iter()
            .map(|location| self.parse_location(location))
            .collect()
    }

    pub fn resolve_location(
        &self,
        location: impl AsRef<str>,
        object_store_config: Option<&ObjectStoreConfig>,
    ) -> FileResult<FsAccessHandle> {
        self.resolve_locations(std::iter::once(location), object_store_config)
    }

    pub fn resolve_locations<I, S>(
        &self,
        locations: I,
        object_store_config: Option<&ObjectStoreConfig>,
    ) -> FileResult<FsAccessHandle>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let locations = self.parse_locations(locations)?;
        let first = locations
            .first()
            .ok_or_else(|| FileError::invalid("fs access locations are empty"))?;
        let scheme = first.scheme();
        if locations.iter().any(|location| location.scheme() != scheme) {
            return Err(FileError::invalid(
                "mixed fs location schemes are not allowed",
            ));
        }

        match scheme {
            FsScheme::Local => resolve_local_locations(locations),
            FsScheme::ObjectStore => resolve_object_store_locations(locations, object_store_config),
            FsScheme::Hdfs => resolve_hdfs_locations(locations),
        }
    }
}

fn resolve_local_locations(locations: Vec<FsLocation>) -> FileResult<FsAccessHandle> {
    let raw_paths = locations.iter().map(FsLocation::path).collect::<Vec<_>>();
    let (root, relative_paths) = normalize_local_paths(&raw_paths)?;
    let builder = opendal::services::Fs::default().root(&root);
    let operator = Operator::new(builder)
        .map_err(|error| map_opendal_error("initialize local file operator", error))?
        .finish();
    let paths = locations
        .into_iter()
        .zip(relative_paths)
        .map(|(location, relative_path)| ResolvedFsPath::new(location, relative_path))
        .collect::<FileResult<Vec<_>>>()?;
    Ok(FsAccessHandle::new(
        FsScheme::Local,
        operator,
        None,
        Some(root),
        paths,
    ))
}

fn normalize_local_paths(paths: &[&str]) -> FileResult<(String, Vec<String>)> {
    if paths.is_empty() {
        return Err(FileError::invalid("file paths are empty"));
    }

    let mut absolute = None;
    let mut directories = Vec::with_capacity(paths.len());
    for raw in paths {
        let path = Path::new(raw.trim());
        let is_absolute = path.is_absolute();
        if absolute.is_some_and(|previous| previous != is_absolute) {
            return Err(FileError::invalid(
                "mixed absolute and relative local paths are not allowed",
            ));
        }
        absolute.get_or_insert(is_absolute);
        let directory = path.parent().unwrap_or(Path::new(""));
        directories.push(if directory.as_os_str().is_empty() {
            PathBuf::from(".")
        } else {
            directory.to_path_buf()
        });
    }

    let root = common_path_prefix(&directories)
        .ok_or_else(|| FileError::invalid("failed to compute common local root"))?;
    let root_string = if root.as_os_str().is_empty() {
        ".".to_string()
    } else {
        root.to_string_lossy().to_string()
    };
    let relative_paths = paths
        .iter()
        .map(|raw| {
            let path = Path::new(raw.trim());
            let relative = if root == Path::new(".") {
                path.to_path_buf()
            } else {
                path.strip_prefix(&root)
                    .map_err(|_| {
                        FileError::invalid(format!(
                            "local path {raw} does not start with root {root_string}"
                        ))
                    })?
                    .to_path_buf()
            };
            let relative = relative.to_string_lossy().to_string();
            if relative.is_empty() {
                return Err(FileError::invalid(format!(
                    "invalid local path after stripping root: {raw}"
                )));
            }
            Ok(relative)
        })
        .collect::<FileResult<Vec<_>>>()?;
    Ok((root_string, relative_paths))
}

fn common_path_prefix(paths: &[PathBuf]) -> Option<PathBuf> {
    let mut paths = paths.iter();
    let first = paths.next()?.components().collect::<Vec<_>>();
    let mut prefix_len = first.len();
    for path in paths {
        let components = path.components().collect::<Vec<_>>();
        prefix_len = prefix_len.min(components.len());
        for index in 0..prefix_len {
            if components[index] != first[index] {
                prefix_len = index;
                break;
            }
        }
    }
    if prefix_len == 0 {
        return None;
    }
    let mut root = PathBuf::new();
    for component in &first[..prefix_len] {
        root.push(component.as_os_str());
    }
    Some(root)
}

fn resolve_object_store_locations(
    locations: Vec<FsLocation>,
    object_store_config: Option<&ObjectStoreConfig>,
) -> FileResult<FsAccessHandle> {
    let config = object_store_config
        .ok_or_else(|| FileError::invalid("object-store location requires object store config"))?;
    let bucket = locations
        .first()
        .and_then(FsLocation::authority)
        .ok_or_else(|| FileError::invalid("object-store location missing bucket"))?
        .to_string();
    if locations
        .iter()
        .any(|location| location.authority() != Some(bucket.as_str()))
    {
        return Err(FileError::invalid(
            "mixed object-store buckets are not allowed",
        ));
    }
    let operator = build_object_store_operator(&bucket, config)?;
    let paths = locations
        .into_iter()
        .map(|location| {
            let relative_path = location.path().trim_start_matches('/').to_string();
            ResolvedFsPath::new(location, relative_path)
        })
        .collect::<FileResult<Vec<_>>>()?;
    Ok(FsAccessHandle::new(
        FsScheme::ObjectStore,
        operator,
        Some(bucket),
        None,
        paths,
    ))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ObjectStoreOperatorKey {
    bucket: String,
    config: ObjectStoreConfig,
}

fn object_store_operator_cache() -> &'static Mutex<HashMap<ObjectStoreOperatorKey, Operator>> {
    static CACHE: OnceLock<Mutex<HashMap<ObjectStoreOperatorKey, Operator>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn build_object_store_operator(bucket: &str, config: &ObjectStoreConfig) -> FileResult<Operator> {
    let bucket = bucket.trim();
    if bucket.is_empty() {
        return Err(FileError::invalid("empty object-store bucket"));
    }
    let key = ObjectStoreOperatorKey {
        bucket: bucket.to_string(),
        config: config.clone(),
    };
    if let Some(operator) = object_store_operator_cache()
        .lock()
        .map_err(|_| FileError::new(FileErrorKind::Internal, "lock object store operator cache"))?
        .get(&key)
        .cloned()
    {
        return Ok(operator);
    }

    let endpoint = normalize_s3_endpoint(&config.endpoint)?;
    let use_path_style = config
        .enable_path_style_access
        .unwrap_or_else(|| !prefer_virtual_host_style(&endpoint));
    let region = config
        .region
        .as_deref()
        .filter(|region| !region.is_empty())
        .unwrap_or("us-east-1");
    let mut builder = opendal::services::S3::default()
        .endpoint(&endpoint)
        .bucket(bucket)
        .region(region)
        .access_key_id(&config.access_key_id)
        .secret_access_key(&config.access_key_secret);
    if !use_path_style {
        builder = builder.enable_virtual_host_style();
    }
    if let Some(session_token) = config.session_token.as_deref() {
        builder = builder.session_token(session_token);
    }
    let mut operator = Operator::new(builder)
        .map_err(|error| map_opendal_error("initialize object store operator", error))?
        .finish();
    if is_local_endpoint(&endpoint) {
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .map_err(|error| {
                FileError::with_source(
                    FileErrorKind::Internal,
                    "build local object store HTTP client",
                    error,
                )
            })?;
        operator = operator.layer(HttpClientLayer::new(opendal::raw::HttpClient::with(client)));
    }
    let mut timeout = TimeoutLayer::new();
    timeout = timeout.with_timeout(Duration::from_millis(
        config.timeout_ms.unwrap_or(DEFAULT_OBJECT_STORE_TIMEOUT_MS),
    ));
    timeout = timeout.with_io_timeout(Duration::from_millis(
        config
            .io_timeout_ms
            .unwrap_or(DEFAULT_OBJECT_STORE_IO_TIMEOUT_MS),
    ));
    operator = operator.layer(timeout);
    operator = operator.layer(
        ConcurrentLimitLayer::new(DEFAULT_OBJECT_STORE_CONCURRENT_LIMIT)
            .with_http_concurrent_limit(DEFAULT_OBJECT_STORE_HTTP_CONCURRENT_LIMIT),
    );
    operator = operator.layer(
        RetryLayer::new()
            .with_jitter()
            .with_min_delay(Duration::from_millis(
                config
                    .retry_min_delay_ms
                    .unwrap_or(DEFAULT_OBJECT_STORE_RETRY_MIN_DELAY_MS),
            ))
            .with_max_delay(Duration::from_millis(
                config
                    .retry_max_delay_ms
                    .unwrap_or(DEFAULT_OBJECT_STORE_RETRY_MAX_DELAY_MS),
            ))
            .with_max_times(
                config
                    .retry_max_times
                    .unwrap_or(DEFAULT_OBJECT_STORE_RETRY_MAX_TIMES),
            ),
    );
    object_store_operator_cache()
        .lock()
        .map_err(|_| FileError::new(FileErrorKind::Internal, "lock object store operator cache"))?
        .insert(key, operator.clone());
    Ok(operator)
}

fn normalize_s3_endpoint(raw_endpoint: &str) -> FileResult<String> {
    let endpoint = raw_endpoint.trim().trim_end_matches('/');
    if endpoint.is_empty() {
        return Err(FileError::invalid("empty object-store endpoint"));
    }
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return Ok(endpoint.to_string());
    }
    let scheme = if is_local_endpoint(endpoint) {
        "http"
    } else {
        "https"
    };
    Ok(format!("{scheme}://{endpoint}"))
}

fn endpoint_host(endpoint: &str) -> String {
    let mut view = endpoint.trim();
    if let Some(rest) = view.strip_prefix("http://") {
        view = rest;
    } else if let Some(rest) = view.strip_prefix("https://") {
        view = rest;
    }
    if let Some((authority, _)) = view.split_once('/') {
        view = authority;
    }
    if let Some(rest) = view.strip_prefix('[')
        && let Some((host, _)) = rest.split_once(']')
    {
        return host.to_ascii_lowercase();
    }
    view.split(':').next().unwrap_or(view).to_ascii_lowercase()
}

fn is_local_endpoint(endpoint: &str) -> bool {
    let host = endpoint_host(endpoint);
    host == "localhost" || host.parse::<IpAddr>().is_ok()
}

fn prefer_virtual_host_style(endpoint: &str) -> bool {
    let host = endpoint_host(endpoint);
    [
        ".amazonaws.com",
        ".aliyuncs.com",
        ".myhuaweicloud.com",
        ".myqcloud.com",
        ".volces.com",
        ".ivolces.com",
        ".ksyuncs.com",
        "storage.googleapis.com",
    ]
    .iter()
    .any(|suffix| host.ends_with(suffix))
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct HdfsOperatorKey {
    name_node: String,
    user: Option<String>,
}

fn hdfs_operator_cache() -> &'static Mutex<HashMap<HdfsOperatorKey, Operator>> {
    static CACHE: OnceLock<Mutex<HashMap<HdfsOperatorKey, Operator>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn resolve_hdfs_locations(locations: Vec<FsLocation>) -> FileResult<FsAccessHandle> {
    let mut parsed = locations
        .iter()
        .map(|location| parse_hdfs_path(location.original()))
        .collect::<FileResult<Vec<_>>>()?;
    let first = parsed
        .first()
        .cloned()
        .ok_or_else(|| FileError::invalid("file paths are empty"))?;
    if parsed.iter().any(|path| path.name_node != first.name_node) {
        return Err(FileError::invalid(
            "mixed hdfs namenodes are not allowed in one access handle",
        ));
    }
    if parsed.iter().any(|path| path.user != first.user) {
        return Err(FileError::invalid(
            "mixed hdfs users are not allowed in one access handle",
        ));
    }
    let operator = build_hdfs_operator(&first.name_node, first.user.as_deref())?;
    let paths = locations
        .into_iter()
        .zip(parsed.drain(..))
        .map(|(location, path)| ResolvedFsPath::new(location, path.relative_path))
        .collect::<FileResult<Vec<_>>>()?;
    Ok(FsAccessHandle::new(
        FsScheme::Hdfs,
        operator,
        Some(first.name_node.clone()),
        Some(first.name_node),
        paths,
    ))
}

#[derive(Clone)]
struct HdfsPath {
    name_node: String,
    user: Option<String>,
    relative_path: String,
}

fn parse_hdfs_path(raw: &str) -> FileResult<HdfsPath> {
    let url = Url::parse(raw).map_err(|error| {
        FileError::with_source(FileErrorKind::Invalid, "parse hdfs path", error)
    })?;
    if url.scheme() != "hdfs" {
        return Err(FileError::invalid(format!(
            "invalid hdfs path scheme: {}",
            url.scheme()
        )));
    }
    if url.password().is_some() || url.query().is_some() || url.fragment().is_some() {
        return Err(FileError::invalid(
            "hdfs path must not include password, query, or fragment",
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| FileError::invalid("hdfs path missing host"))?;
    let authority = url
        .port()
        .map(|port| format!("{host}:{port}"))
        .unwrap_or_else(|| host.to_string());
    let relative_path = url.path().trim_start_matches('/').to_string();
    if relative_path.is_empty() {
        return Err(FileError::invalid(
            "hdfs path points to namenode root and cannot be used as file path",
        ));
    }
    Ok(HdfsPath {
        name_node: format!("hdfs://{authority}"),
        user: (!url.username().is_empty()).then(|| url.username().to_string()),
        relative_path,
    })
}

fn build_hdfs_operator(name_node: &str, user: Option<&str>) -> FileResult<Operator> {
    let key = HdfsOperatorKey {
        name_node: name_node.to_string(),
        user: user.map(str::to_string),
    };
    if let Some(operator) = hdfs_operator_cache()
        .lock()
        .map_err(|_| FileError::new(FileErrorKind::Internal, "lock hdfs operator cache"))?
        .get(&key)
        .cloned()
    {
        return Ok(operator);
    }
    let mut url = Url::parse(name_node)
        .map_err(|error| FileError::with_source(FileErrorKind::Invalid, "parse namenode", error))?;
    if let Some(user) = user {
        url.set_username(user)
            .map_err(|_| FileError::invalid("invalid hdfs user"))?;
    }
    let builder = opendal::services::HdfsNative::default()
        .name_node(url.as_str().trim_end_matches('/'))
        .root("/");
    let operator = Operator::new(builder)
        .map_err(|error| map_opendal_error("initialize hdfs operator", error))?
        .finish();
    hdfs_operator_cache()
        .lock()
        .map_err(|_| FileError::new(FileErrorKind::Internal, "lock hdfs operator cache"))?
        .insert(key, operator.clone());
    Ok(operator)
}

fn map_opendal_error(operation: &str, error: opendal::Error) -> FileError {
    let kind = match error.kind() {
        opendal::ErrorKind::NotFound => FileErrorKind::NotFound,
        opendal::ErrorKind::PermissionDenied => FileErrorKind::Permission,
        opendal::ErrorKind::Unsupported => FileErrorKind::Unsupported,
        opendal::ErrorKind::RateLimited
        | opendal::ErrorKind::Unexpected
        | opendal::ErrorKind::ConditionNotMatch => FileErrorKind::Transient,
        _ => FileErrorKind::Internal,
    };
    FileError::with_source(kind, operation, error)
}

fn split_uri_scheme(value: &str) -> Option<(&str, &str)> {
    if let Some(rest) = value.strip_prefix("file:") {
        return Some(("file", rest));
    }

    let colon = value.find("://")?;
    let scheme = &value[..colon];
    if scheme.is_empty() || !scheme.as_bytes()[0].is_ascii_alphabetic() {
        return None;
    }
    if !scheme
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-'))
    {
        return None;
    }
    Some((scheme, &value[colon + 1..]))
}

fn parse_authority_and_path(
    original: &str,
    rest: &str,
    require_authority: bool,
    scheme: &str,
) -> FileResult<(Option<String>, String)> {
    let Some(without_prefix) = rest.strip_prefix("//") else {
        return Err(FileError::invalid(format!(
            "{scheme} location must use {scheme}://authority/path: {original}"
        )));
    };
    let (authority, path) = without_prefix
        .split_once('/')
        .unwrap_or((without_prefix, ""));
    if require_authority && authority.is_empty() {
        return Err(FileError::invalid(format!(
            "{scheme} location missing authority: {original}"
        )));
    }
    let path = if path.is_empty() {
        ""
    } else {
        &without_prefix[authority.len()..]
    };
    ensure_non_empty_path(original, scheme, path)?;
    Ok((
        (!authority.is_empty()).then(|| authority.to_string()),
        path.to_string(),
    ))
}

fn ensure_non_empty_path(original: &str, scheme: &str, path: &str) -> FileResult<()> {
    if path.is_empty() || path == "/" {
        return Err(FileError::invalid(format!(
            "{scheme} location missing file path: {original}"
        )));
    }
    Ok(())
}
