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

use std::collections::BTreeMap;

use novarocks::fs::access::{FsAccessHandle, FsAccessResolver, FsScheme};
use novarocks::fs::object_store::ObjectStoreConfig;
use novarocks::fs::object_store_credentials::{
    ObjectStoreCredentials, ObjectStoreCredentialsSource,
};

pub fn object_store_config_from_fs_options(
    fs_options: &BTreeMap<String, String>,
) -> Result<Option<ObjectStoreConfig>, String> {
    let has_s3a = fs_options.keys().any(|key| key.starts_with("fs.s3a."));
    if !has_s3a {
        return Ok(None);
    }
    let credentials = ObjectStoreCredentials::from_s3a_properties(
        ObjectStoreCredentialsSource::S3AProperties,
        fs_options,
    )?;
    Ok(Some(credentials.to_object_store_config()))
}

pub fn resolve_tool_location(
    location: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<FsAccessHandle, String> {
    let resolver = FsAccessResolver::new();
    let parsed = resolver.parse_location(location)?;
    match parsed.scheme() {
        FsScheme::Local => resolver.resolve_location(location, None),
        FsScheme::ObjectStore => resolver.resolve_location(location, object_store_config),
        FsScheme::Hdfs => Err(format!(
            "tools do not support hdfs location yet: {location}"
        )),
    }
}

pub fn single_relative_path(handle: &FsAccessHandle, location: &str) -> Result<String, String> {
    handle
        .paths()
        .first()
        .map(|path| path.operator_relative_path().to_string())
        .ok_or_else(|| format!("resolved empty path list for {location}"))
}

pub fn list_prefix(relative_path: &str) -> String {
    let mut prefix = relative_path.trim_start_matches('/').to_string();
    if !prefix.is_empty() && !prefix.ends_with('/') {
        prefix.push('/');
    }
    prefix
}
