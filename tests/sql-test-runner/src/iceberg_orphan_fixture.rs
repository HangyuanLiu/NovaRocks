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

//! Runner-owned real orphan fixture for REST Catalog + MinIO cases.
//!
//! The fixture intentionally does not accept a location from SQL. It resolves
//! the table through REST Catalog, then writes a deterministic unreferenced
//! object through OpenDAL. This keeps object-store credentials out of server
//! state and makes the post-cleanup assertion test the same physical object.

use anyhow::{Context, Result, bail};
use opendal::Operator;
use std::env;

#[derive(Clone, Debug)]
pub(crate) struct OrphanFixture {
    operator: Operator,
    path: String,
    location: String,
}

pub(crate) fn install(table: &str) -> Result<OrphanFixture> {
    let (namespace, table_name) = table
        .trim()
        .split_once('.')
        .ok_or_else(|| anyhow::anyhow!("@iceberg_orphan_fixture requires namespace.table"))?;
    if namespace.is_empty() || table_name.is_empty() || table_name.contains('.') {
        bail!("@iceberg_orphan_fixture requires exactly namespace.table");
    }
    let rest_uri = required_env("NOVAROCKS_ICEBERG_REST_URI")?;
    let endpoint = required_env("AWS_S3_ENDPOINT")?;
    let response: serde_json::Value = reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .context("build REST Catalog fixture client")?
        .get(format!(
            "{}/v1/namespaces/{}/tables/{}",
            rest_uri.trim_end_matches('/'),
            namespace,
            table_name
        ))
        .send()
        .context("read REST Catalog table for orphan fixture")?
        .error_for_status()
        .context("REST Catalog table lookup status")?
        .json()
        .context("decode REST Catalog table response")?;
    let location = response
        .pointer("/metadata/location")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("REST Catalog table response has no metadata.location"))?
        .to_string();
    let (bucket, prefix) = parse_s3_location(&location)?;
    let access_key = required_env("AWS_S3_ACCESS_KEY_ID")?;
    let secret_key = required_env("AWS_S3_SECRET_ACCESS_KEY")?;
    let operator = Operator::new(
        opendal::services::S3::default()
            .endpoint(&endpoint)
            .bucket(bucket)
            .region("us-east-1")
            .access_key_id(&access_key)
            .secret_access_key(&secret_key),
    )
    .context("create MinIO OpenDAL fixture operator")?
    .finish();
    let path = format!(
        "{}/data/novarocks-orphan-fixture-{}-{}.bin",
        prefix.trim_end_matches('/'),
        namespace,
        table_name
    );
    runtime()?.block_on(operator.write(&path, "novarocks orphan fixture\n"))
        .with_context(|| format!("write MinIO orphan fixture {path}"))?;
    Ok(OrphanFixture {
        operator,
        location: format!("s3://{bucket}/{path}"),
        path,
    })
}

impl OrphanFixture {
    pub(crate) fn location(&self) -> &str {
        &self.location
    }

    pub(crate) fn assert_absent(&self) -> Result<()> {
        match runtime()?.block_on(self.operator.stat(&self.path)) {
            Ok(_) => bail!("orphan fixture object remains at {}", self.location),
            Err(error) if error.kind() == opendal::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).with_context(|| {
                format!("stat MinIO orphan fixture after cleanup {}", self.location)
            }),
        }
    }
}

fn required_env(key: &str) -> Result<String> {
    env::var(key).with_context(|| format!("{key} is required for @iceberg_orphan_fixture"))
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build MinIO orphan fixture runtime")
}

fn parse_s3_location(location: &str) -> Result<(&str, &str)> {
    let raw = location
        .strip_prefix("s3://")
        .ok_or_else(|| anyhow::anyhow!("REST Catalog location is not s3://: {location}"))?;
    let (bucket, prefix) = raw
        .split_once('/')
        .ok_or_else(|| anyhow::anyhow!("REST Catalog location has no table prefix: {location}"))?;
    if bucket.is_empty() || prefix.is_empty() {
        bail!("REST Catalog location is invalid: {location}");
    }
    Ok((bucket, prefix))
}
