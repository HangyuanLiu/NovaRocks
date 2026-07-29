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

use std::fmt::{Debug, Formatter};

use opendal::Operator;

use crate::{FileError, FileResult};

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
