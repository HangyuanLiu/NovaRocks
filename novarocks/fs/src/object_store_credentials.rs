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
use crate::SecretValue;
use crate::access::ObjectStoreConfig;
use crate::object_store_settings::ObjectStoreRetrySettings;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};

pub const AWS_S3_ENDPOINT_KEYS: &[&str] = &["aws.s3.endpoint", "aws.s3.endpoint_url"];
pub const AWS_S3_ACCESS_KEY_ID_KEYS: &[&str] = &["aws.s3.accessKeyId", "aws.s3.access_key"];
pub const AWS_S3_ACCESS_KEY_SECRET_KEYS: &[&str] = &["aws.s3.accessKeySecret", "aws.s3.secret_key"];
pub const AWS_S3_SESSION_TOKEN_KEYS: &[&str] = &["aws.s3.sessionToken", "aws.s3.session_token"];
pub const AWS_S3_REGION_KEYS: &[&str] = &["aws.s3.region"];
pub const AWS_S3_ENABLE_PATH_STYLE_ACCESS_KEYS: &[&str] = &["aws.s3.enable_path_style_access"];
pub const FS_S3A_ENDPOINT_KEYS: &[&str] = &["fs.s3a.endpoint"];
pub const FS_S3A_ACCESS_KEY_ID_KEYS: &[&str] = &["fs.s3a.access.key"];
pub const FS_S3A_ACCESS_KEY_SECRET_KEYS: &[&str] = &["fs.s3a.secret.key"];
pub const FS_S3A_SESSION_TOKEN_KEYS: &[&str] = &["fs.s3a.session.token"];
pub const FS_S3A_REGION_KEYS: &[&str] = &["fs.s3a.endpoint.region"];
pub const FS_S3A_ENABLE_SSL_KEYS: &[&str] = &["fs.s3a.connection.ssl.enabled"];
pub const FS_S3A_ENABLE_PATH_STYLE_ACCESS_KEYS: &[&str] = &["fs.s3a.path.style.access"];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObjectStoreCredentialsSource {
    AwsS3Properties,
    S3AProperties,
    IcebergSinkCloudProperties,
    ConnectorStartupConfig,
    StarRocksObjectStoreProfile,
    StarletProfile,
}

impl ObjectStoreCredentialsSource {
    fn label(self) -> &'static str {
        match self {
            Self::AwsS3Properties => "aws_s3_properties",
            Self::S3AProperties => "s3a_properties",
            Self::IcebergSinkCloudProperties => "iceberg_sink_cloud_properties",
            Self::ConnectorStartupConfig => "connector_startup_config",
            Self::StarRocksObjectStoreProfile => "starrocks_object_store_profile",
            Self::StarletProfile => "starlet_profile",
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ObjectStoreCredentials {
    pub endpoint: String,
    pub access_key_id: SecretValue,
    pub access_key_secret: SecretValue,
    pub session_token: Option<SecretValue>,
    pub enable_path_style_access: Option<bool>,
    pub region: Option<String>,
    pub retry_max_times: Option<usize>,
    pub retry_min_delay_ms: Option<u64>,
    pub retry_max_delay_ms: Option<u64>,
    pub timeout_ms: Option<u64>,
    pub io_timeout_ms: Option<u64>,
}

impl Debug for ObjectStoreCredentials {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ObjectStoreCredentials")
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

impl ObjectStoreCredentials {
    pub fn from_aws_s3_properties<S>(
        source: ObjectStoreCredentialsSource,
        props: &BTreeMap<S, S>,
    ) -> Result<Self, String>
    where
        S: Borrow<str> + Ord,
    {
        let enable_path_style_access = parse_optional_bool_property(
            source,
            props,
            AWS_S3_ENABLE_PATH_STYLE_ACCESS_KEYS[0],
            AWS_S3_ENABLE_PATH_STYLE_ACCESS_KEYS,
        )?;
        let endpoint = required_property(source, props, AWS_S3_ENDPOINT_KEYS, "aws.s3.endpoint")?;
        let access_key_id = required_property(
            source,
            props,
            AWS_S3_ACCESS_KEY_ID_KEYS,
            "aws.s3.access_key",
        )?;
        let access_key_secret = required_property(
            source,
            props,
            AWS_S3_ACCESS_KEY_SECRET_KEYS,
            "aws.s3.secret_key",
        )?;
        let session_token = optional_string_property(props, AWS_S3_SESSION_TOKEN_KEYS);
        let region = optional_string_property(props, AWS_S3_REGION_KEYS);
        let retry_settings = ObjectStoreRetrySettings::from_aws_s3_props(Some(props));

        Ok(Self {
            endpoint,
            access_key_id: SecretValue::new(access_key_id),
            access_key_secret: SecretValue::new(access_key_secret),
            session_token: session_token.map(SecretValue::new),
            enable_path_style_access,
            region,
            retry_max_times: retry_settings.retry_max_times,
            retry_min_delay_ms: retry_settings.retry_min_delay_ms,
            retry_max_delay_ms: retry_settings.retry_max_delay_ms,
            timeout_ms: retry_settings.timeout_ms,
            io_timeout_ms: retry_settings.io_timeout_ms,
        })
    }

    pub fn from_s3a_properties<S>(
        source: ObjectStoreCredentialsSource,
        props: &BTreeMap<S, S>,
    ) -> Result<Self, String>
    where
        S: Borrow<str> + Ord,
    {
        let endpoint_raw =
            required_property(source, props, FS_S3A_ENDPOINT_KEYS, "fs.s3a.endpoint")?;
        let enable_ssl = parse_optional_bool_property(
            source,
            props,
            FS_S3A_ENABLE_SSL_KEYS[0],
            FS_S3A_ENABLE_SSL_KEYS,
        )?;
        let endpoint = normalize_s3a_endpoint(&endpoint_raw, enable_ssl)?;
        let access_key_id = required_property(
            source,
            props,
            FS_S3A_ACCESS_KEY_ID_KEYS,
            "fs.s3a.access.key",
        )?;
        let access_key_secret = required_property(
            source,
            props,
            FS_S3A_ACCESS_KEY_SECRET_KEYS,
            "fs.s3a.secret.key",
        )?;
        let session_token = optional_string_property(props, FS_S3A_SESSION_TOKEN_KEYS);
        let region = optional_string_property(props, FS_S3A_REGION_KEYS);
        let enable_path_style_access = parse_optional_bool_property(
            source,
            props,
            FS_S3A_ENABLE_PATH_STYLE_ACCESS_KEYS[0],
            FS_S3A_ENABLE_PATH_STYLE_ACCESS_KEYS,
        )?;

        Ok(Self {
            endpoint,
            access_key_id: SecretValue::new(access_key_id),
            access_key_secret: SecretValue::new(access_key_secret),
            session_token: session_token.map(SecretValue::new),
            enable_path_style_access,
            region,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        })
    }

    pub fn optional_from_aws_s3_properties<S>(
        source: ObjectStoreCredentialsSource,
        props: &BTreeMap<S, S>,
    ) -> Result<Option<Self>, String>
    where
        S: Borrow<str> + Ord,
    {
        parse_optional_bool_property(
            source,
            props,
            AWS_S3_ENABLE_PATH_STYLE_ACCESS_KEYS[0],
            AWS_S3_ENABLE_PATH_STYLE_ACCESS_KEYS,
        )?;
        if first_nonempty_property(props, AWS_S3_ENDPOINT_KEYS).is_none()
            || first_nonempty_property(props, AWS_S3_ACCESS_KEY_ID_KEYS).is_none()
            || first_nonempty_property(props, AWS_S3_ACCESS_KEY_SECRET_KEYS).is_none()
        {
            return Ok(None);
        }
        Self::from_aws_s3_properties(source, props).map(Some)
    }

    pub fn from_parts(
        source: ObjectStoreCredentialsSource,
        endpoint: &str,
        access_key_id: SecretValue,
        access_key_secret: SecretValue,
        region: Option<&str>,
        enable_path_style_access: Option<bool>,
    ) -> Result<Self, String> {
        let endpoint = required_part(source, endpoint, "aws.s3.endpoint")?;
        let access_key_id = required_secret_part(source, access_key_id, "aws.s3.access_key")?;
        let access_key_secret =
            required_secret_part(source, access_key_secret, "aws.s3.secret_key")?;
        let region = region
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        Ok(Self {
            endpoint,
            access_key_id,
            access_key_secret,
            session_token: None,
            enable_path_style_access,
            region,
            retry_max_times: None,
            retry_min_delay_ms: None,
            retry_max_delay_ms: None,
            timeout_ms: None,
            io_timeout_ms: None,
        })
    }

    pub fn to_object_store_config(&self) -> ObjectStoreConfig {
        ObjectStoreConfig {
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
        }
    }
}

fn required_property<S>(
    source: ObjectStoreCredentialsSource,
    props: &BTreeMap<S, S>,
    keys: &[&str],
    error_key: &str,
) -> Result<String, String>
where
    S: Borrow<str> + Ord,
{
    first_nonempty_property(props, keys)
        .map(str::to_string)
        .ok_or_else(|| missing_required_error(source, error_key))
}

fn required_part(
    source: ObjectStoreCredentialsSource,
    value: &str,
    error_key: &str,
) -> Result<String, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(missing_required_error(source, error_key));
    }
    Ok(trimmed.to_string())
}

fn required_secret_part(
    source: ObjectStoreCredentialsSource,
    value: SecretValue,
    error_key: &str,
) -> Result<SecretValue, String> {
    if value.is_empty() {
        Err(missing_required_error(source, error_key))
    } else {
        Ok(value)
    }
}

fn missing_required_error(source: ObjectStoreCredentialsSource, key: &str) -> String {
    format!(
        "{} object-store credentials missing {}",
        source.label(),
        key
    )
}

fn optional_string_property<S>(props: &BTreeMap<S, S>, keys: &[&str]) -> Option<String>
where
    S: Borrow<str> + Ord,
{
    first_nonempty_property(props, keys).map(str::to_string)
}

fn first_nonempty_property<'a, S>(props: &'a BTreeMap<S, S>, keys: &[&str]) -> Option<&'a str>
where
    S: Borrow<str> + Ord,
{
    for key in keys {
        if let Some(value) = props
            .get(*key)
            .map(|value| value.borrow().trim())
            .filter(|value| !value.is_empty())
        {
            return Some(value);
        }
    }
    None
}

fn parse_optional_bool_property<S>(
    source: ObjectStoreCredentialsSource,
    props: &BTreeMap<S, S>,
    error_key: &str,
    keys: &[&str],
) -> Result<Option<bool>, String>
where
    S: Borrow<str> + Ord,
{
    let Some(value) = first_nonempty_property(props, keys) else {
        return Ok(None);
    };
    parse_bool_value(value).map(Some).ok_or_else(|| {
        format!(
            "{} object-store property {} has invalid boolean value: {}",
            source.label(),
            error_key,
            value
        )
    })
}

fn parse_bool_value(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

fn normalize_s3a_endpoint(raw_endpoint: &str, enable_ssl: Option<bool>) -> Result<String, String> {
    let mut endpoint = raw_endpoint.trim();
    if endpoint.is_empty() {
        return Err("s3a_properties object-store credentials missing fs.s3a.endpoint".to_string());
    }

    let mut inferred_enable_ssl = None;
    if let Some(rest) = endpoint.strip_prefix("http://") {
        endpoint = rest;
        inferred_enable_ssl = Some(false);
    } else if let Some(rest) = endpoint.strip_prefix("https://") {
        endpoint = rest;
        inferred_enable_ssl = Some(true);
    }

    if let Some((authority, _)) = endpoint.split_once('/') {
        endpoint = authority;
    }
    let host = endpoint.trim_end_matches('/');
    if host.is_empty() {
        return Err("s3a_properties object-store credentials missing fs.s3a.endpoint".to_string());
    }

    let scheme = if enable_ssl.or(inferred_enable_ssl).unwrap_or(true) {
        "https"
    } else {
        "http"
    };
    Ok(format!("{scheme}://{host}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn props(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn parses_aws_s3_aliases_and_trims_values() {
        let credentials = ObjectStoreCredentials::from_aws_s3_properties(
            ObjectStoreCredentialsSource::AwsS3Properties,
            &props(&[
                ("aws.s3.endpoint_url", " http://localhost:9000/ "),
                ("aws.s3.accessKeyId", " ak "),
                ("aws.s3.accessKeySecret", " sk "),
                ("aws.s3.region", " us-east-1 "),
                ("aws.s3.enable_path_style_access", " YES "),
            ]),
        )
        .expect("parse credentials");

        assert_eq!(credentials.endpoint, "http://localhost:9000/");
        assert_eq!(credentials.access_key_id.expose_secret(), "ak");
        assert_eq!(credentials.access_key_secret.expose_secret(), "sk");
        assert_eq!(credentials.region.as_deref(), Some("us-east-1"));
        assert_eq!(credentials.enable_path_style_access, Some(true));
    }

    #[test]
    fn s3a_properties_normalize_endpoint_and_path_style() {
        let credentials = ObjectStoreCredentials::from_s3a_properties(
            ObjectStoreCredentialsSource::S3AProperties,
            &props(&[
                ("fs.s3a.endpoint", "localhost:9000"),
                ("fs.s3a.access.key", "ak"),
                ("fs.s3a.secret.key", "sk"),
                ("fs.s3a.endpoint.region", "us-east-1"),
                ("fs.s3a.connection.ssl.enabled", "false"),
                ("fs.s3a.path.style.access", "true"),
            ]),
        )
        .expect("parse s3a properties");

        assert_eq!(credentials.endpoint, "http://localhost:9000");
        assert_eq!(credentials.access_key_id.expose_secret(), "ak");
        assert_eq!(credentials.access_key_secret.expose_secret(), "sk");
        assert_eq!(credentials.region.as_deref(), Some("us-east-1"));
        assert_eq!(credentials.enable_path_style_access, Some(true));
    }

    #[test]
    fn s3a_properties_ssl_setting_overrides_explicit_endpoint_scheme() {
        let https_to_http = ObjectStoreCredentials::from_s3a_properties(
            ObjectStoreCredentialsSource::S3AProperties,
            &props(&[
                ("fs.s3a.endpoint", "https://localhost:9000"),
                ("fs.s3a.access.key", "ak"),
                ("fs.s3a.secret.key", "sk"),
                ("fs.s3a.connection.ssl.enabled", "false"),
            ]),
        )
        .expect("parse s3a properties with ssl disabled");
        assert_eq!(https_to_http.endpoint, "http://localhost:9000");

        let http_to_https = ObjectStoreCredentials::from_s3a_properties(
            ObjectStoreCredentialsSource::S3AProperties,
            &props(&[
                ("fs.s3a.endpoint", "http://localhost:9000"),
                ("fs.s3a.access.key", "ak"),
                ("fs.s3a.secret.key", "sk"),
                ("fs.s3a.connection.ssl.enabled", "true"),
            ]),
        )
        .expect("parse s3a properties with ssl enabled");
        assert_eq!(http_to_https.endpoint, "https://localhost:9000");
    }

    #[test]
    fn s3a_properties_require_endpoint() {
        let err = ObjectStoreCredentials::from_s3a_properties(
            ObjectStoreCredentialsSource::S3AProperties,
            &props(&[("fs.s3a.access.key", "ak"), ("fs.s3a.secret.key", "sk")]),
        )
        .expect_err("missing endpoint must fail");

        assert!(
            err.contains("s3a_properties object-store credentials missing fs.s3a.endpoint"),
            "{err}"
        );
    }

    #[test]
    fn s3a_properties_require_access_key() {
        let err = ObjectStoreCredentials::from_s3a_properties(
            ObjectStoreCredentialsSource::S3AProperties,
            &props(&[
                ("fs.s3a.endpoint", "localhost:9000"),
                ("fs.s3a.secret.key", "sk"),
            ]),
        )
        .expect_err("missing access key must fail");

        assert!(
            err.contains("s3a_properties object-store credentials missing fs.s3a.access.key"),
            "{err}"
        );
    }

    #[test]
    fn s3a_properties_require_endpoint_access_key_and_secret() {
        let err = ObjectStoreCredentials::from_s3a_properties(
            ObjectStoreCredentialsSource::S3AProperties,
            &props(&[
                ("fs.s3a.endpoint", "localhost:9000"),
                ("fs.s3a.access.key", "ak"),
            ]),
        )
        .expect_err("missing secret key must fail");

        assert!(
            err.contains("s3a_properties object-store credentials missing fs.s3a.secret.key"),
            "{err}"
        );
    }

    #[test]
    fn optional_parse_returns_none_when_required_fields_are_absent() {
        let parsed = ObjectStoreCredentials::optional_from_aws_s3_properties(
            ObjectStoreCredentialsSource::AwsS3Properties,
            &props(&[("aws.s3.endpoint", "http://localhost:9000")]),
        )
        .expect("missing required fields are not malformed");

        assert!(parsed.is_none());
    }

    #[test]
    fn optional_parse_rejects_invalid_present_fields() {
        let err = ObjectStoreCredentials::optional_from_aws_s3_properties(
            ObjectStoreCredentialsSource::AwsS3Properties,
            &props(&[("aws.s3.enable_path_style_access", "maybe")]),
        )
        .expect_err("invalid present bool should fail even when required fields are absent");

        assert!(err.contains("invalid boolean value: maybe"), "{err}");
    }

    #[test]
    fn parse_required_rejects_missing_required_fields() {
        let err = ObjectStoreCredentials::from_aws_s3_properties(
            ObjectStoreCredentialsSource::AwsS3Properties,
            &props(&[
                ("aws.s3.endpoint", "http://localhost:9000"),
                ("aws.s3.access_key", "ak"),
            ]),
        )
        .expect_err("secret key is required");

        assert!(
            err.contains("aws_s3_properties object-store credentials missing aws.s3.secret_key"),
            "{err}"
        );
    }

    #[test]
    fn rejects_invalid_path_style_boolean() {
        let err = ObjectStoreCredentials::from_aws_s3_properties(
            ObjectStoreCredentialsSource::AwsS3Properties,
            &props(&[
                ("aws.s3.endpoint", "http://localhost:9000"),
                ("aws.s3.access_key", "ak"),
                ("aws.s3.secret_key", "sk"),
                ("aws.s3.enable_path_style_access", "maybe"),
            ]),
        )
        .expect_err("invalid bool must fail");

        assert!(
            err.contains(
                "aws_s3_properties object-store property aws.s3.enable_path_style_access has invalid boolean value: maybe"
            ),
            "{err}"
        );
    }

    #[test]
    fn parses_session_token_and_retry_aliases() {
        let credentials = ObjectStoreCredentials::from_aws_s3_properties(
            ObjectStoreCredentialsSource::AwsS3Properties,
            &props(&[
                ("aws.s3.endpoint", "http://localhost:9000"),
                ("aws.s3.access_key", "ak"),
                ("aws.s3.secret_key", "sk"),
                ("aws.s3.sessionToken", " token "),
                ("aws.s3.max_retries", "7"),
                ("aws.s3.retry_min_delay_ms", "11"),
                ("aws.s3.retry_max_delay_ms", "99"),
                ("aws.s3.request_timeout_ms", "1234"),
                ("aws.s3.io_timeout_ms", "5678"),
            ]),
        )
        .expect("parse credentials");

        assert_eq!(
            credentials
                .session_token
                .as_ref()
                .map(SecretValue::expose_secret),
            Some("token")
        );
        assert_eq!(credentials.retry_max_times, Some(7));
        assert_eq!(credentials.retry_min_delay_ms, Some(11));
        assert_eq!(credentials.retry_max_delay_ms, Some(99));
        assert_eq!(credentials.timeout_ms, Some(1234));
        assert_eq!(credentials.io_timeout_ms, Some(5678));
    }

    #[test]
    fn converts_to_credentials_only_object_store_config() {
        let credentials = ObjectStoreCredentials::from_aws_s3_properties(
            ObjectStoreCredentialsSource::AwsS3Properties,
            &props(&[
                ("aws.s3.endpoint", "http://localhost:9000"),
                ("aws.s3.access_key", "ak"),
                ("aws.s3.secret_key", "sk"),
            ]),
        )
        .expect("parse credentials");

        let cfg = credentials.to_object_store_config();

        assert_eq!(cfg.endpoint, "http://localhost:9000");
        assert_eq!(cfg.access_key_id.expose_secret(), "ak");
        assert_eq!(cfg.access_key_secret.expose_secret(), "sk");
    }

    #[test]
    fn debug_redacts_secret_canary() {
        let credentials = ObjectStoreCredentials::from_aws_s3_properties(
            ObjectStoreCredentialsSource::AwsS3Properties,
            &props(&[
                ("aws.s3.endpoint", "http://localhost:9000"),
                ("aws.s3.access_key", "nwt-1-access-canary"),
                ("aws.s3.secret_key", "nwt-1-secret-canary"),
                ("aws.s3.session_token", "nwt-1-token-canary"),
            ]),
        )
        .expect("parse credentials");

        let debug = format!("{credentials:?}");
        assert!(!debug.contains("nwt-1-access-canary"));
        assert!(!debug.contains("nwt-1-secret-canary"));
        assert!(!debug.contains("nwt-1-token-canary"));
        assert!(debug.contains("<redacted>"));
    }
}
