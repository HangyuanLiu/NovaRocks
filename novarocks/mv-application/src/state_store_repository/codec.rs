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

use std::io::Cursor;
use std::sync::Arc;

use crate::management::{DeploymentOwner, ProcessIncarnation};
use crate::persistence::identity::DocumentRevision;
use crate::state_family::MV_ACCELERATOR_STATE_FAMILY;
use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};
use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorInstanceId, ConnectorTableIdentity, ConnectorTableObjectId,
};
use novarocks_state_store_api::{Key, Value};
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::persistence::codec::{
    decode_configuration, decode_definition, decode_interpretation, decode_publication,
    encode_configuration, encode_definition, encode_interpretation, encode_publication,
    preflight_current_document_set,
};
use crate::persistence::definition::{
    MV_ACCELERATOR_PROJECTION_SUBJECT, MvAcceleratorCommittedVersionRevision,
    MvAcceleratorSourceRevision,
};
use crate::persistence::dependency::MV_ACCELERATOR_DEPENDENCY_SUBJECT;
use crate::persistence::projection::{
    MvDocumentProjection, MvPublicationState, StoredMvProjection,
};
use crate::persistence::validation::PersistenceDecodeBudget;

use super::catalog::schema_catalog;
use super::key::{MvKeyKind, expected_record_kind};

const MAGIC: &[u8; 4] = b"NRMA";
/// Record version of the MV accelerator family.
///
/// Declared by the MV product descriptor, not here: a second literal could
/// disagree with the version the product publishes and orphan deployed data.
const ENVELOPE_VERSION: u8 = MV_ACCELERATOR_STATE_FAMILY.record_version();
const HEADER_BYTES_BEFORE_FINGERPRINT: usize = 12;
const OPERATION_ID_BYTES: usize = 16;
const PAYLOAD_LENGTH_BYTES: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum MvRecordKind {
    Projection = 1,
    TargetLookup = 2,
    Dependency = 3,
    Sequence = 4,
}

impl MvRecordKind {
    fn from_byte(value: u8) -> Result<Self, String> {
        match value {
            1 => Ok(Self::Projection),
            2 => Ok(Self::TargetLookup),
            3 => Ok(Self::Dependency),
            4 => Ok(Self::Sequence),
            _ => Err(format!("unknown MV Accelerator record kind {value}")),
        }
    }

    fn subject(self) -> &'static str {
        match self {
            Self::Projection => MV_ACCELERATOR_PROJECTION_SUBJECT,
            Self::TargetLookup => "mv.accelerator_target_lookup",
            Self::Dependency => MV_ACCELERATOR_DEPENDENCY_SUBJECT,
            Self::Sequence => "mv.accelerator_sequence",
        }
    }

    fn matches_key(self, key_kind: MvKeyKind) -> bool {
        match self {
            Self::Projection => key_kind == MvKeyKind::Projection,
            Self::TargetLookup => key_kind == MvKeyKind::TargetLookup,
            Self::Dependency => matches!(
                key_kind,
                MvKeyKind::DependencyDownstream | MvKeyKind::DependencyUpstream
            ),
            Self::Sequence => key_kind == MvKeyKind::Sequence,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DecodedMvRecord<T> {
    pub operation_id: Uuid,
    pub value: T,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MvSequence {
    pub last_allocated_id: i64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct StoredMvProjectionAvro {
    mv_id: i64,
    definition: serde_bytes::ByteBuf,
    interpretation: serde_bytes::ByteBuf,
    configuration: serde_bytes::ByteBuf,
    publication: Option<serde_bytes::ByteBuf>,
    metadata_version: ProviderVersionAvro,
    output_version: Option<ProviderVersionAvro>,
    storage_rows: Option<i64>,
    source_revision: MvAcceleratorSourceRevisionAvro,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct ProviderVersionAvro {
    payload: serde_bytes::ByteBuf,
    snapshot_id: Option<i64>,
}

impl From<&ConnectorCommittedVersion> for ProviderVersionAvro {
    fn from(value: &ConnectorCommittedVersion) -> Self {
        Self {
            payload: serde_bytes::ByteBuf::from(value.payload().to_vec()),
            snapshot_id: value.snapshot_id(),
        }
    }
}

impl TryFrom<ProviderVersionAvro> for ConnectorCommittedVersion {
    type Error = String;
    fn try_from(value: ProviderVersionAvro) -> Result<Self, String> {
        Self::try_new(Bytes::from(value.payload.into_vec()), value.snapshot_id)
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MvAcceleratorCommittedVersionRevisionAvro {
    digest: String,
    snapshot_id: Option<i64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct MvAcceleratorSourceRevisionAvro {
    target_catalog: String,
    target_namespace: String,
    target_table: String,
    target_object_id: ConnectorTableObjectId,
    metadata_version: MvAcceleratorCommittedVersionRevisionAvro,
    definition_revision: String,
    interpretation_revision: String,
    publication_revision: Option<String>,
    publication_output_version: Option<MvAcceleratorCommittedVersionRevisionAvro>,
    configuration_revision: String,
    deployment_owner: String,
    process_incarnation: String,
}

impl From<&MvAcceleratorCommittedVersionRevision> for MvAcceleratorCommittedVersionRevisionAvro {
    fn from(value: &MvAcceleratorCommittedVersionRevision) -> Self {
        Self {
            digest: hex::encode(value.digest()),
            snapshot_id: value.snapshot_id(),
        }
    }
}

impl TryFrom<MvAcceleratorCommittedVersionRevisionAvro> for MvAcceleratorCommittedVersionRevision {
    type Error = String;

    fn try_from(value: MvAcceleratorCommittedVersionRevisionAvro) -> Result<Self, Self::Error> {
        Self::try_from_parts(
            decode_sha256(&value.digest, "committed version digest")?,
            value.snapshot_id,
        )
    }
}

impl From<&MvAcceleratorSourceRevision> for MvAcceleratorSourceRevisionAvro {
    fn from(value: &MvAcceleratorSourceRevision) -> Self {
        Self {
            target_catalog: value.target.instance_id.as_str().to_string(),
            target_namespace: value.target.namespace.to_string(),
            target_table: value.target.table.to_string(),
            target_object_id: value.target_object_id.clone(),
            metadata_version: (&value.metadata_version).into(),
            definition_revision: hex::encode(value.definition_revision.as_bytes()),
            interpretation_revision: hex::encode(value.interpretation_revision.as_bytes()),
            publication_revision: value
                .publication_revision
                .as_ref()
                .map(|revision| hex::encode(revision.as_bytes())),
            publication_output_version: value.publication_output_version.as_ref().map(Into::into),
            configuration_revision: hex::encode(value.configuration_revision.as_bytes()),
            deployment_owner: value.deployment_owner.as_str().to_string(),
            process_incarnation: value.process_incarnation.as_str().to_string(),
        }
    }
}

impl TryFrom<MvAcceleratorSourceRevisionAvro> for MvAcceleratorSourceRevision {
    type Error = String;

    fn try_from(value: MvAcceleratorSourceRevisionAvro) -> Result<Self, Self::Error> {
        Ok(Self {
            target: ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse(&value.target_catalog)
                    .map_err(|error| format!("decode MV Accelerator target catalog: {error}"))?,
                namespace: Arc::from(value.target_namespace),
                table: Arc::from(value.target_table),
            },
            target_object_id: value.target_object_id,
            metadata_version: value.metadata_version.try_into()?,
            definition_revision: decode_document_revision(
                &value.definition_revision,
                "definition revision",
            )?,
            interpretation_revision: decode_document_revision(
                &value.interpretation_revision,
                "interpretation revision",
            )?,
            publication_revision: value
                .publication_revision
                .as_deref()
                .map(|revision| decode_document_revision(revision, "publication revision"))
                .transpose()?,
            publication_output_version: value
                .publication_output_version
                .map(TryInto::try_into)
                .transpose()?,
            configuration_revision: decode_document_revision(
                &value.configuration_revision,
                "configuration revision",
            )?,
            deployment_owner: DeploymentOwner::parse(&value.deployment_owner)
                .map_err(|error| format!("decode MV Accelerator deployment owner: {error}"))?,
            process_incarnation: ProcessIncarnation::parse(&value.process_incarnation)
                .map_err(|error| format!("decode MV Accelerator process incarnation: {error}"))?,
        })
    }
}

impl TryFrom<&StoredMvProjection> for StoredMvProjectionAvro {
    type Error = String;
    fn try_from(value: &StoredMvProjection) -> Result<Self, String> {
        if value.mv_id <= 0 {
            return Err("MV projection ID must be positive".into());
        }
        let facts = &value.facts;
        let (publication, output_version, storage_rows) = match facts.publication() {
            MvPublicationState::NeverPublished => (None, None, None),
            MvPublicationState::Published(published) => (
                Some(serde_bytes::ByteBuf::from(
                    encode_publication(published.document())
                        .map_err(|e| e.to_string())?
                        .as_bytes()
                        .to_vec(),
                )),
                Some(ProviderVersionAvro::from(published.output_version())),
                published
                    .storage_rows()
                    .map(i64::try_from)
                    .transpose()
                    .map_err(|_| "MV storage rows exceed the cache integer range")?,
            ),
        };
        Ok(Self {
            mv_id: value.mv_id,
            definition: serde_bytes::ByteBuf::from(
                encode_definition(facts.definition())
                    .map_err(|e| e.to_string())?
                    .as_bytes()
                    .to_vec(),
            ),
            interpretation: serde_bytes::ByteBuf::from(
                encode_interpretation(facts.interpretation())
                    .map_err(|e| e.to_string())?
                    .as_bytes()
                    .to_vec(),
            ),
            configuration: serde_bytes::ByteBuf::from(
                encode_configuration(facts.configuration())
                    .map_err(|e| e.to_string())?
                    .as_bytes()
                    .to_vec(),
            ),
            publication,
            output_version,
            storage_rows,
            metadata_version: facts.metadata_version().into(),
            source_revision: facts.source_revision().into(),
        })
    }
}

impl TryFrom<StoredMvProjectionAvro> for StoredMvProjection {
    type Error = String;
    fn try_from(value: StoredMvProjectionAvro) -> Result<Self, String> {
        if value.mv_id <= 0 {
            return Err("MV projection ID must be positive".into());
        }
        if value.publication.is_some() != value.output_version.is_some() {
            return Err("MV cache publication/output presence differ".into());
        }
        let budget = PersistenceDecodeBudget::default();
        preflight_current_document_set(
            &value.definition,
            &value.interpretation,
            value.publication.as_ref().map(|bytes| bytes.as_ref()),
            &value.configuration,
            budget,
        )
        .map_err(|e| e.to_string())?;
        let publication = value
            .publication
            .as_ref()
            .map(|bytes| decode_publication(bytes, budget))
            .transpose()
            .map_err(|e| e.to_string())?;
        let output = value
            .output_version
            .map(ConnectorCommittedVersion::try_from)
            .transpose()?;
        let facts = MvDocumentProjection::try_from_parts(
            value.source_revision.try_into()?,
            value.metadata_version.try_into()?,
            decode_definition(&value.definition, budget).map_err(|e| e.to_string())?,
            decode_interpretation(&value.interpretation, budget).map_err(|e| e.to_string())?,
            decode_configuration(&value.configuration, budget).map_err(|e| e.to_string())?,
            publication.zip(output),
            value
                .storage_rows
                .map(u64::try_from)
                .transpose()
                .map_err(|_| "negative MV storage row count")?,
        )?;
        Ok(Self {
            mv_id: value.mv_id,
            facts,
        })
    }
}

fn decode_sha256(value: &str, subject: &str) -> Result<[u8; 32], String> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "MV Accelerator {subject} must be canonical lowercase SHA-256 hex"
        ));
    }
    let decoded =
        hex::decode(value).map_err(|error| format!("decode MV Accelerator {subject}: {error}"))?;
    decoded.try_into().map_err(|bytes: Vec<u8>| {
        format!(
            "MV Accelerator {subject} must be 32 bytes, got {}",
            bytes.len()
        )
    })
}

fn decode_document_revision(value: &str, subject: &str) -> Result<DocumentRevision, String> {
    DocumentRevision::try_from_bytes(&decode_sha256(value, subject)?)
        .map_err(|error| format!("decode MV Accelerator {subject}: {error}"))
}

pub fn encode_projection(
    operation_id: Uuid,
    projection: &StoredMvProjection,
) -> Result<Value, String> {
    encode_record(
        MvRecordKind::Projection,
        operation_id,
        &StoredMvProjectionAvro::try_from(projection)?,
    )
}

pub fn decode_projection(
    key: &Key,
    value: &Value,
) -> Result<DecodedMvRecord<StoredMvProjection>, String> {
    let decoded: DecodedMvRecord<StoredMvProjectionAvro> = decode_record(key, value)?;
    Ok(DecodedMvRecord {
        operation_id: decoded.operation_id,
        value: decoded.value.try_into()?,
    })
}

pub fn encode_record<T>(kind: MvRecordKind, operation_id: Uuid, value: &T) -> Result<Value, String>
where
    T: Serialize,
{
    let catalog = schema_catalog()?;
    let entry = catalog.latest(kind.subject())?;
    let datum = to_value(value)
        .map_err(|error| format!("convert MV Accelerator record failed: {error}"))?;
    let payload = to_avro_datum(entry.schema(), datum).map_err(|error| {
        format!(
            "encode MV Accelerator Avro payload for {} schema {} failed: {error}",
            entry.subject(),
            entry.id()
        )
    })?;
    let fingerprint = entry.fingerprint().as_bytes();
    let fingerprint_len = u16::try_from(fingerprint.len())
        .map_err(|_| "MV Accelerator Avro fingerprint is too large".to_string())?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| "MV Accelerator Avro payload is too large".to_string())?;
    let mut envelope = Vec::with_capacity(
        HEADER_BYTES_BEFORE_FINGERPRINT
            + fingerprint.len()
            + OPERATION_ID_BYTES
            + PAYLOAD_LENGTH_BYTES
            + payload.len(),
    );
    envelope.extend_from_slice(MAGIC);
    envelope.push(ENVELOPE_VERSION);
    envelope.push(kind as u8);
    envelope.extend_from_slice(&entry.id().to_be_bytes());
    envelope.extend_from_slice(&fingerprint_len.to_be_bytes());
    envelope.extend_from_slice(fingerprint);
    envelope.extend_from_slice(operation_id.as_bytes());
    envelope.extend_from_slice(&payload_len.to_be_bytes());
    envelope.extend_from_slice(&payload);
    Value::try_from(Bytes::from(envelope))
        .map_err(|error| format!("encode MV Accelerator StateStore value failed: {error}"))
}

pub fn decode_record<T>(key: &Key, value: &Value) -> Result<DecodedMvRecord<T>, String>
where
    T: DeserializeOwned,
{
    let bytes = value.as_bytes();
    if bytes.len() < HEADER_BYTES_BEFORE_FINGERPRINT + OPERATION_ID_BYTES + PAYLOAD_LENGTH_BYTES {
        return Err("MV Accelerator envelope is truncated".to_string());
    }
    if &bytes[..4] != MAGIC {
        return Err("MV Accelerator envelope has invalid magic".to_string());
    }
    if bytes[4] != ENVELOPE_VERSION {
        return Err(format!(
            "unsupported MV Accelerator envelope version {}",
            bytes[4]
        ));
    }
    let kind = MvRecordKind::from_byte(bytes[5])?;
    let key_kind = expected_record_kind(key)?;
    if !kind.matches_key(key_kind) {
        return Err(format!(
            "MV Accelerator envelope record kind {:?} does not match key kind {:?}",
            kind, key_kind
        ));
    }
    let schema_id = i32::from_be_bytes(bytes[6..10].try_into().expect("fixed schema id slice"));
    let fingerprint_len = u16::from_be_bytes(
        bytes[10..12]
            .try_into()
            .expect("fixed fingerprint length slice"),
    ) as usize;
    let fingerprint_end = HEADER_BYTES_BEFORE_FINGERPRINT
        .checked_add(fingerprint_len)
        .ok_or_else(|| "MV Accelerator fingerprint length overflows".to_string())?;
    let operation_end = fingerprint_end
        .checked_add(OPERATION_ID_BYTES)
        .ok_or_else(|| "MV Accelerator operation ID length overflows".to_string())?;
    let payload_length_end = operation_end
        .checked_add(PAYLOAD_LENGTH_BYTES)
        .ok_or_else(|| "MV Accelerator payload length overflows".to_string())?;
    if payload_length_end > bytes.len() {
        return Err("MV Accelerator envelope is truncated before payload".to_string());
    }
    let fingerprint = std::str::from_utf8(&bytes[HEADER_BYTES_BEFORE_FINGERPRINT..fingerprint_end])
        .map_err(|_| "MV Accelerator fingerprint is not ASCII".to_string())?;
    if !fingerprint.is_ascii() {
        return Err("MV Accelerator fingerprint is not ASCII".to_string());
    }
    let operation_id = Uuid::from_slice(&bytes[fingerprint_end..operation_end])
        .map_err(|error| format!("MV Accelerator operation ID is invalid: {error}"))?;
    let payload_len = u32::from_be_bytes(
        bytes[operation_end..payload_length_end]
            .try_into()
            .expect("fixed payload length slice"),
    ) as usize;
    let payload_end = payload_length_end
        .checked_add(payload_len)
        .ok_or_else(|| "MV Accelerator payload length overflows".to_string())?;
    if payload_end != bytes.len() {
        return Err("MV Accelerator payload length does not match exact record size".to_string());
    }
    let catalog = schema_catalog()?;
    let writer = catalog.entry(kind.subject(), schema_id)?;
    if writer.fingerprint() != fingerprint {
        return Err(format!(
            "MV Accelerator Avro schema fingerprint mismatch for {} schema {}",
            kind.subject(),
            schema_id
        ));
    }
    let reader = catalog.latest(kind.subject())?;
    let payload = &bytes[payload_length_end..payload_end];
    let mut cursor = Cursor::new(payload);
    let datum =
        from_avro_datum(writer.schema(), &mut cursor, Some(reader.schema())).map_err(|error| {
            format!(
                "decode MV Accelerator Avro payload for {} writer {} reader {} failed: {error}",
                kind.subject(),
                writer.id(),
                reader.id()
            )
        })?;
    if cursor.position() != payload.len() as u64 {
        return Err("MV Accelerator Avro payload has trailing bytes".to_string());
    }
    let value = from_value(&datum)
        .map_err(|error| format!("materialize MV Accelerator Avro payload failed: {error}"))?;
    Ok(DecodedMvRecord {
        operation_id,
        value,
    })
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod codec_tests;
