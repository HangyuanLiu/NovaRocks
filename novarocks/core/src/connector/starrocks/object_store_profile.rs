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

use crate::fs::object_store::ObjectStoreConfig;
use crate::fs::object_store_credentials::{
    AWS_S3_ENDPOINT_KEYS, ObjectStoreCredentials, ObjectStoreCredentialsSource,
};
use crate::runtime::starlet_shard_registry::S3StoreConfig;

const UNSUPPORTED_OBJECT_STORE_PREFIXES: [&str; 7] = [
    "fs.s3a.",
    "fs.s3n.",
    "fs.s3.",
    "fs.oss.",
    "fs.cos.",
    "fs.obs.",
    "aliyun.oss.",
];

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreProfile {
    pub(crate) endpoint: String,
    pub(crate) access_key_id: String,
    pub(crate) access_key_secret: String,
    pub(crate) session_token: Option<String>,
    pub(crate) region: Option<String>,
    pub(crate) enable_path_style_access: Option<bool>,
    pub(crate) retry_max_times: Option<usize>,
    pub(crate) retry_min_delay_ms: Option<u64>,
    pub(crate) retry_max_delay_ms: Option<u64>,
    pub(crate) timeout_ms: Option<u64>,
    pub(crate) io_timeout_ms: Option<u64>,
}

impl ObjectStoreProfile {
    pub(crate) fn from_properties_optional(
        props: &BTreeMap<String, String>,
    ) -> Result<Option<Self>, String> {
        let mut aws_props: BTreeMap<String, String> = BTreeMap::new();
        let mut unsupported: Vec<String> = Vec::new();

        for (key, value) in props {
            if key.starts_with("aws.s3.") {
                aws_props.insert(key.clone(), value.clone());
                continue;
            }
            if is_unsupported_object_storage_key(key) {
                unsupported.push(key.clone());
            }
        }

        if !unsupported.is_empty() {
            unsupported.sort();
            return Err(format!(
                "unsupported object storage properties detected: [{}] (only aws.s3.* is supported)",
                unsupported.join(", ")
            ));
        }

        if aws_props.is_empty() {
            return Ok(None);
        }

        Ok(Some(Self::from_aws_s3_properties(&aws_props)?))
    }

    pub(crate) fn from_s3_store_config(config: &S3StoreConfig) -> Result<Self, String> {
        let endpoint = normalize_endpoint(&config.endpoint, None)?;
        let credentials = ObjectStoreCredentials::from_parts(
            ObjectStoreCredentialsSource::StarletProfile,
            &endpoint,
            &config.access_key_id,
            &config.access_key_secret,
            config.region.as_deref(),
            config.enable_path_style_access,
        )?;
        Ok(Self::from_credentials(credentials))
    }

    pub(crate) fn to_object_store_config(&self) -> ObjectStoreConfig {
        let mut cfg = ObjectStoreConfig {
            endpoint: self.endpoint.clone(),
            access_key_id: self.access_key_id.clone(),
            access_key_secret: self.access_key_secret.clone(),
            session_token: self.session_token.clone(),
            enable_path_style_access: self.enable_path_style_access,
            region: self.region.clone(),
            retry_max_times: self.retry_max_times,
            retry_min_delay_ms: self.retry_min_delay_ms,
            retry_max_delay_ms: self.retry_max_delay_ms,
            timeout_ms: self.timeout_ms,
            io_timeout_ms: self.io_timeout_ms,
        };
        crate::fs::object_store::apply_object_store_runtime_defaults(&mut cfg);
        cfg
    }

    fn from_aws_s3_properties(props: &BTreeMap<String, String>) -> Result<Self, String> {
        let endpoint = normalize_endpoint_from_properties(props)?;
        let mut normalized_props = props.clone();
        normalized_props.insert(AWS_S3_ENDPOINT_KEYS[0].to_string(), endpoint);
        for key in AWS_S3_ENDPOINT_KEYS.iter().skip(1) {
            normalized_props.remove(*key);
        }
        let credentials = ObjectStoreCredentials::from_aws_s3_properties(
            ObjectStoreCredentialsSource::StarRocksObjectStoreProfile,
            &normalized_props,
        )?;
        Ok(Self::from_credentials(credentials))
    }

    fn from_credentials(credentials: ObjectStoreCredentials) -> Self {
        Self {
            endpoint: credentials.endpoint,
            access_key_id: credentials.access_key_id,
            access_key_secret: credentials.access_key_secret,
            session_token: credentials.session_token,
            region: credentials.region,
            enable_path_style_access: credentials.enable_path_style_access,
            retry_max_times: credentials.retry_max_times,
            retry_min_delay_ms: credentials.retry_min_delay_ms,
            retry_max_delay_ms: credentials.retry_max_delay_ms,
            timeout_ms: credentials.timeout_ms,
            io_timeout_ms: credentials.io_timeout_ms,
        }
    }
}

fn normalize_endpoint_from_properties(props: &BTreeMap<String, String>) -> Result<String, String> {
    let enable_ssl = props.get("aws.s3.enable_ssl").map(|v| is_true_value(v));
    let endpoint_raw = first_nonempty_property(props, AWS_S3_ENDPOINT_KEYS)
        .ok_or_else(|| format!("missing {} for object_store mode", AWS_S3_ENDPOINT_KEYS[0]))?;
    normalize_endpoint(endpoint_raw, enable_ssl)
}

fn is_unsupported_object_storage_key(key: &str) -> bool {
    UNSUPPORTED_OBJECT_STORE_PREFIXES
        .iter()
        .any(|prefix| key.starts_with(prefix))
}

fn first_nonempty_property<'a>(
    props: &'a BTreeMap<String, String>,
    keys: &[&str],
) -> Option<&'a str> {
    keys.iter()
        .filter_map(|key| props.get(*key))
        .map(|value| value.trim())
        .find(|value| !value.is_empty())
}

fn normalize_endpoint(
    raw_endpoint: &str,
    explicit_enable_ssl: Option<bool>,
) -> Result<String, String> {
    let mut view = raw_endpoint.trim();
    if view.is_empty() {
        return Err(format!(
            "empty {} for object_store mode",
            AWS_S3_ENDPOINT_KEYS[0]
        ));
    }
    let mut inferred_enable_ssl = None;
    if let Some(rest) = view.strip_prefix("http://") {
        view = rest;
        inferred_enable_ssl = Some(false);
    } else if let Some(rest) = view.strip_prefix("https://") {
        view = rest;
        inferred_enable_ssl = Some(true);
    }
    if let Some((authority, _)) = view.split_once('/') {
        view = authority;
    }
    let host = view.trim_end_matches('/');
    if host.is_empty() {
        return Err(format!(
            "invalid {} for object_store mode: {raw_endpoint}",
            AWS_S3_ENDPOINT_KEYS[0]
        ));
    }

    let enable_ssl = explicit_enable_ssl.or(inferred_enable_ssl).unwrap_or(true);
    let scheme = if enable_ssl { "https" } else { "http" };
    Ok(format!("{scheme}://{host}"))
}

fn is_true_value(value: &str) -> bool {
    let v = value.trim();
    v.eq_ignore_ascii_case("true") || v == "1"
}
