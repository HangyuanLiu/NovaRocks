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

use super::keys::{corrupt, marker_key, slot_key, version_key};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorDocumentFormat, ConnectorDocumentVersion, ConnectorError, ConnectorMarkerKey,
    ConnectorMarkerValue, ConnectorObservedSlot, ConnectorSlotName, ConnectorSlotValue,
    DocumentIdentity,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Format {
    owner: String,
    name: String,
    version: u32,
}
impl From<&ConnectorDocumentFormat> for Format {
    fn from(f: &ConnectorDocumentFormat) -> Self {
        Self {
            owner: f.owner().into(),
            name: f.name().into(),
            version: f.version(),
        }
    }
}
impl Format {
    fn decode(self) -> Result<ConnectorDocumentFormat, ConnectorError> {
        ConnectorDocumentFormat::try_new(self.owner, self.name, self.version)
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reference {
    rel: String,
    id: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Version {
    v: u8,
    owner: String,
    name: String,
    format: Format,
    refs: Vec<Reference>,
    content: String,
    id: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Slot {
    v: u8,
    owner: String,
    name: String,
    format: Format,
    writer: Option<i64>,
    content: String,
    digest: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    v: u8,
    owner: String,
    name: String,
    binding: String,
    format: Format,
    content: String,
    digest: String,
}

fn json<T: Serialize>(value: &T) -> Result<String, ConnectorError> {
    serde_json::to_string(value).map_err(|e| {
        ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Internal,
            format!("encode document envelope: {e}"),
        )
    })
}
fn canonical<T: Serialize + for<'de> Deserialize<'de>>(
    key: &str,
    value: &str,
) -> Result<T, ConnectorError> {
    let decoded = serde_json::from_str::<T>(value).map_err(|e| corrupt(key, e))?;
    if json(&decoded)? != value {
        return Err(corrupt(key, "noncanonical envelope"));
    }
    Ok(decoded)
}
fn content(key: &str, value: &str) -> Result<Bytes, ConnectorError> {
    URL_SAFE_NO_PAD
        .decode(value)
        .map(Bytes::from)
        .map_err(|e| corrupt(key, e))
}
fn storage_version(key: &str, version: u8) -> Result<(), ConnectorError> {
    if version != 2 {
        return Err(corrupt(key, "unsupported envelope version"));
    }
    Ok(())
}

pub(crate) fn encode_version(value: &ConnectorDocumentVersion) -> Result<String, ConnectorError> {
    json(&Version {
        v: 2,
        owner: value.owner().into(),
        name: value.name().into(),
        format: value.format().into(),
        refs: value
            .references()
            .iter()
            .map(|(rel, id)| Reference {
                rel: rel.clone(),
                id: id.to_hex(),
            })
            .collect(),
        content: URL_SAFE_NO_PAD.encode(value.content()),
        id: value.identity().to_hex(),
    })
}
pub(crate) fn decode_version(
    key: &str,
    value: &str,
) -> Result<ConnectorDocumentVersion, ConnectorError> {
    let v: Version = canonical(key, value)?;
    storage_version(key, v.v)?;
    let refs = v
        .refs
        .into_iter()
        .map(|r| {
            Ok((
                r.rel,
                DocumentIdentity::parse_hex(&r.id).map_err(|e| corrupt(key, e))?,
            ))
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let version = ConnectorDocumentVersion::try_new(
        v.owner,
        v.name,
        v.format.decode().map_err(|e| corrupt(key, e))?,
        refs,
        content(key, &v.content)?,
    )
    .map_err(|e| corrupt(key, e))?;
    if v.id != version.identity().to_hex()
        || key != version_key(version.identity())
        || encode_version(&version)? != value
    {
        return Err(corrupt(key, "identity, key or reference order mismatch"));
    }
    Ok(version)
}
pub(crate) fn encode_slot(
    name: &ConnectorSlotName,
    value: &ConnectorSlotValue,
    writer: Option<i64>,
) -> Result<String, ConnectorError> {
    name.validate()?;
    value.validate()?;
    json(&Slot {
        v: 2,
        owner: name.owner.clone(),
        name: name.name.clone(),
        format: (&value.format).into(),
        writer,
        content: URL_SAFE_NO_PAD.encode(&value.content),
        digest: value.digest(name, writer).to_hex(),
    })
}
pub(crate) fn decode_slot(
    key: &str,
    value: &str,
) -> Result<(ConnectorSlotName, ConnectorObservedSlot), ConnectorError> {
    let v: Slot = canonical(key, value)?;
    storage_version(key, v.v)?;
    let name = ConnectorSlotName::try_new(v.owner, v.name).map_err(|e| corrupt(key, e))?;
    let slot = ConnectorSlotValue {
        format: v.format.decode().map_err(|e| corrupt(key, e))?,
        content: content(key, &v.content)?,
    };
    slot.validate().map_err(|e| corrupt(key, e))?;
    if key != slot_key(&name) || v.digest != slot.digest(&name, v.writer).to_hex() {
        return Err(corrupt(key, "slot identity or digest mismatch"));
    }
    Ok((
        name,
        ConnectorObservedSlot {
            value: slot,
            writer: v.writer,
        },
    ))
}
pub(crate) fn encode_marker(
    key: &ConnectorMarkerKey,
    value: &ConnectorMarkerValue,
) -> Result<String, ConnectorError> {
    key.validate()?;
    value.validate()?;
    json(&Marker {
        v: 2,
        owner: key.owner.clone(),
        name: key.name.clone(),
        binding: key.binding.clone(),
        format: (&value.format).into(),
        content: URL_SAFE_NO_PAD.encode(&value.content),
        digest: value.digest(key).to_hex(),
    })
}
pub(crate) fn decode_marker(
    key: &str,
    value: &str,
) -> Result<(ConnectorMarkerKey, ConnectorMarkerValue), ConnectorError> {
    let v: Marker = canonical(key, value)?;
    storage_version(key, v.v)?;
    let identity = ConnectorMarkerKey {
        owner: v.owner,
        name: v.name,
        binding: v.binding,
    };
    identity.validate().map_err(|e| corrupt(key, e))?;
    let marker = ConnectorMarkerValue {
        format: v.format.decode().map_err(|e| corrupt(key, e))?,
        content: content(key, &v.content)?,
    };
    marker.validate().map_err(|e| corrupt(key, e))?;
    if key != marker_key(&identity) || v.digest != marker.digest(&identity).to_hex() {
        return Err(corrupt(key, "marker identity or digest mismatch"));
    }
    Ok((identity, marker))
}
