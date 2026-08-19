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

//! Canonical provider-private control payloads for Iceberg writes.

use std::collections::BTreeMap;

use bytes::Bytes;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use serde::{Deserialize, Serialize};

const ICEBERG_WRITE_PLAN_PAYLOAD_VERSION: u16 = 1;
const ICEBERG_FIRST_REFRESH_WRITE_PLAN_PAYLOAD_VERSION: u16 = 2;
const MAX_FIRST_REFRESH_STAGING_PATH_BYTES: usize = 4 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergWritePlanPayloadV1 {
    pub version: u16,
    pub target: String,
    pub target_ref: String,
}

impl IcebergWritePlanPayloadV1 {
    pub fn encode(&self) -> Result<Bytes, ConnectorError> {
        if self.version != ICEBERG_WRITE_PLAN_PAYLOAD_VERSION
            || self.target.is_empty()
            || self.target_ref.is_empty()
        {
            return Err(invalid("invalid Iceberg write plan payload"));
        }
        canonical_json(self, "Iceberg write plan payload")
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ConnectorError> {
        let decoded: Self = decode_canonical_json(payload, "Iceberg write plan payload")?;
        if decoded.version != ICEBERG_WRITE_PLAN_PAYLOAD_VERSION
            || decoded.target.is_empty()
            || decoded.target_ref.is_empty()
        {
            return Err(invalid(
                "unsupported or incomplete Iceberg write plan payload",
            ));
        }
        if canonical_json(&decoded, "Iceberg write plan payload")?.as_ref() != payload {
            return Err(invalid(
                "Iceberg write plan payload is not canonical JSON v1",
            ));
        }
        Ok(decoded)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IcebergFirstRefreshWritePlanPayloadV2 {
    pub version: u16,
    pub target: String,
    pub target_ref: String,
    pub expected_snapshot_id: Option<i64>,
    pub staging_path: String,
    pub provenance_properties: BTreeMap<String, String>,
}

impl IcebergFirstRefreshWritePlanPayloadV2 {
    pub fn encode(&self) -> Result<Bytes, ConnectorError> {
        self.validate()?;
        canonical_json(self, "Iceberg first-refresh write plan payload")
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ConnectorError> {
        let decoded: Self =
            decode_canonical_json(payload, "Iceberg first-refresh write plan payload")?;
        decoded.validate()?;
        if canonical_json(&decoded, "Iceberg first-refresh write plan payload")?.as_ref() != payload
        {
            return Err(invalid(
                "Iceberg first-refresh write plan payload is not canonical JSON v2",
            ));
        }
        Ok(decoded)
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.version != ICEBERG_FIRST_REFRESH_WRITE_PLAN_PAYLOAD_VERSION
            || self.target.is_empty()
            || self.target_ref.is_empty()
            || self.staging_path.is_empty()
            || self.staging_path.len() > MAX_FIRST_REFRESH_STAGING_PATH_BYTES
            || self
                .expected_snapshot_id
                .is_some_and(|snapshot_id| snapshot_id < 0)
            || self
                .provenance_properties
                .iter()
                .any(|(key, value)| key.is_empty() || value.is_empty())
        {
            return Err(invalid(
                "unsupported or incomplete Iceberg first-refresh write plan payload",
            ));
        }
        Ok(())
    }
}

fn canonical_json<T: Serialize>(value: &T, subject: &str) -> Result<Bytes, ConnectorError> {
    serde_json::to_vec(value).map(Bytes::from).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::Internal,
            format!("encode {subject}: {error}"),
        )
    })
}

fn decode_canonical_json<T: for<'de> Deserialize<'de>>(
    payload: &[u8],
    subject: &str,
) -> Result<T, ConnectorError> {
    serde_json::from_slice(payload).map_err(|error| invalid(format!("decode {subject}: {error}")))
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payloads_are_canonical_and_reject_unsafe_first_refresh_facts() {
        let plan = IcebergWritePlanPayloadV1 {
            version: 1,
            target: "db.t".to_string(),
            target_ref: "main".to_string(),
        }
        .encode()
        .expect("encode plan");
        assert_eq!(
            IcebergWritePlanPayloadV1::decode(&plan)
                .expect("decode")
                .target,
            "db.t"
        );
        assert!(
            IcebergWritePlanPayloadV1::decode(
                br#"{\"target_ref\":\"main\",\"target\":\"db.t\",\"version\":1}"#
            )
            .is_err()
        );

        let error = IcebergFirstRefreshWritePlanPayloadV2 {
            version: 2,
            target: "db.mv".to_string(),
            target_ref: "staging".to_string(),
            expected_snapshot_id: Some(-1),
            staging_path: "s3://warehouse/db/mv/staging".to_string(),
            provenance_properties: BTreeMap::new(),
        }
        .encode()
        .expect_err("negative snapshot must fail closed");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }
}
