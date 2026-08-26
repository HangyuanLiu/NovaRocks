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

//! Publication-ID MV provenance carried in an Iceberg snapshot summary.

use std::collections::BTreeMap;

use crate::iceberg::spec::Snapshot;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use novarocks_spi::connector::LakePublicationId;

use crate::commit::MV_PUBLICATION_ID_PROP;

pub const MV_PUBLICATION_PROVENANCE_PROP: &str = "novarocks.mv.publication.v2";
pub const MV_PUBLICATION_PROVENANCE_VERSION: u16 = 2;
pub const MV_REFRESH_ROW_COUNT_PROP: &str = "novarocks.mv.refresh.row_count";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RefreshTechnique {
    Incremental,
    Full,
    MetadataOnly,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenanceBase {
    pub table_fqn: String,
    pub uuid: String,
    #[serde(default)]
    pub from_snapshot: Option<i64>,
    pub to_snapshot: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MvPublicationProvenanceV2 {
    pub provenance_version: u16,
    pub publication_id: LakePublicationId,
    pub technique: RefreshTechnique,
    pub bases: Vec<ProvenanceBase>,
    pub definition_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub descriptor_properties_digest_base64: Option<String>,
    pub rows: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct WaterlineBase {
    table_fqn: String,
    uuid: String,
    to_snapshot: i64,
}

impl MvPublicationProvenanceV2 {
    pub fn to_summary_properties(&self) -> Result<BTreeMap<String, String>, String> {
        let mut props = BTreeMap::new();
        props.insert(
            MV_PUBLICATION_ID_PROP.to_string(),
            self.publication_id.to_string(),
        );
        props.insert(
            MV_PUBLICATION_PROVENANCE_PROP.to_string(),
            self.to_canonical_json()?,
        );
        Ok(props)
    }

    pub fn from_snapshot_summary(snapshot: &Snapshot) -> Result<Option<Self>, String> {
        let Some(raw) = snapshot
            .summary()
            .additional_properties
            .get(MV_PUBLICATION_PROVENANCE_PROP)
        else {
            return Ok(None);
        };
        Self::from_json(raw).map(Some)
    }

    pub fn content_hash(&self) -> Result<String, String> {
        Ok(hex_encode(&Sha256::digest(
            self.to_canonical_json()?.as_bytes(),
        )))
    }

    pub fn with_rows(&self, rows: i64) -> Result<Self, String> {
        if rows < 0 {
            return Err("MV provenance row count cannot be negative".to_string());
        }
        let mut updated = self.clone();
        updated.rows = rows;
        Ok(updated)
    }

    pub fn waterline_hash(&self) -> Result<String, String> {
        waterline_hash_for(&self.bases)
    }

    pub fn to_canonical_json(&self) -> Result<String, String> {
        let value = serde_json::to_value(self)
            .map_err(|err| format!("failed to serialize MV provenance: {err}"))?;
        serde_json::to_string(&sort_json_value(value))
            .map_err(|err| format!("failed to render canonical MV provenance JSON: {err}"))
    }

    pub fn from_json(raw: &str) -> Result<Self, String> {
        let record: Self = serde_json::from_str(raw)
            .map_err(|err| format!("failed to parse MV provenance JSON: {err}"))?;
        if record.provenance_version != MV_PUBLICATION_PROVENANCE_VERSION {
            return Err(format!(
                "unsupported MV provenance version: expected {}, got {}",
                MV_PUBLICATION_PROVENANCE_VERSION, record.provenance_version
            ));
        }
        LakePublicationId::try_from_uuid(*record.publication_id.as_uuid())
            .map_err(|error| format!("invalid MV publication identity: {error}"))?;
        if record.bases.is_empty()
            || record.definition_fingerprint.is_empty()
            || record.rows < 0
            || record
                .descriptor_properties_digest_base64
                .as_ref()
                .is_some_and(|value| value.is_empty())
        {
            return Err("MV publication provenance is incomplete or invalid".to_string());
        }
        Ok(record)
    }
}

pub fn waterline_hash_for(bases: &[ProvenanceBase]) -> Result<String, String> {
    let mut waterline_bases: Vec<WaterlineBase> = bases
        .iter()
        .map(|base| WaterlineBase {
            table_fqn: base.table_fqn.clone(),
            uuid: base.uuid.clone(),
            to_snapshot: base.to_snapshot,
        })
        .collect();
    waterline_bases.sort_by(|left, right| {
        (left.table_fqn.as_str(), left.uuid.as_str())
            .cmp(&(right.table_fqn.as_str(), right.uuid.as_str()))
    });
    let value = serde_json::to_value(&waterline_bases)
        .map_err(|err| format!("failed to serialize MV provenance waterline: {err}"))?;
    let canonical_json = serde_json::to_string(&sort_json_value(value))
        .map_err(|err| format!("failed to render canonical MV provenance waterline JSON: {err}"))?;
    Ok(hex_encode(&Sha256::digest(canonical_json.as_bytes())))
}

pub(crate) fn sort_json_value(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sort_json_value).collect()),
        Value::Object(object) => {
            let mut entries = object
                .into_iter()
                .map(|(key, value)| (key, sort_json_value(value)))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            let mut sorted = Map::new();
            for (key, value) in entries {
                sorted.insert(key, value);
            }
            Value::Object(sorted)
        }
        value => value,
    }
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}
