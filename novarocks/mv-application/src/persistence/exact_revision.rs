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

//! Lossless bridge between UEA-1 exact scan facts and MV persistence.
//!
//! D/L/P keep provider facts opaque, but opaque must not mean bare bytes: the
//! provider identity, fact format, format version, and value together define
//! equality. This adapter stores that complete tuple in the existing opaque MV
//! identities and restores it only through the SPI's validated constructor.

use crate::persistence::identity::{NativeDataVersion, ObjectIdentity as MvObjectIdentity};
use bytes::Bytes;
use novarocks_query_application::api::{DataVersion, ObjectIdentity, ProviderFactFormat};
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExactSemanticRevision, ConnectorProviderId,
    MAX_CONNECTOR_SEMANTIC_FACT_FORMAT_BYTES, MAX_CONNECTOR_SEMANTIC_FACT_VALUE_BYTES,
};

const FACT_ENVELOPE_MAGIC: &[u8; 8] = b"NRMVFCT1";

pub fn persist_exact_query_revision(
    object: &ObjectIdentity,
    data: &DataVersion,
) -> Result<(MvObjectIdentity, NativeDataVersion), ConnectorError> {
    if object.format_identity().provider() != data.format_identity().provider() {
        return invalid("exact query revision facts belong to different providers");
    }
    let object = MvObjectIdentity::try_new(encode_fact(
        object.format_identity(),
        object.encoded_value(),
    )?)
    .map_err(|error| invalid_error(error.to_string()))?;
    let data =
        NativeDataVersion::try_new(encode_fact(data.format_identity(), data.encoded_value())?)
            .map_err(|error| invalid_error(error.to_string()))?;
    Ok((object, data))
}

/// Persist one provider-issued exact semantic revision without translating it
/// through a query-local binding receipt. Scheduler and maintenance paths
/// already hold the connector's sealed revision, so this preserves the same
/// complete provider/format/version/value tuple without inventing a second
/// identity conversion.
pub fn persist_exact_connector_revision(
    revision: &ConnectorExactSemanticRevision,
) -> Result<(MvObjectIdentity, NativeDataVersion), ConnectorError> {
    let object = revision.object_identity();
    let data = revision.data_version();
    if object.provider() != data.provider() {
        return invalid("exact connector revision facts belong to different providers");
    }
    let object = MvObjectIdentity::try_new(encode_connector_fact(object)?)
        .map_err(|error| invalid_error(error.to_string()))?;
    let data = NativeDataVersion::try_new(encode_connector_fact(data)?)
        .map_err(|error| invalid_error(error.to_string()))?;
    Ok((object, data))
}

pub fn restore_exact_query_revision(
    object: &MvObjectIdentity,
    data: &NativeDataVersion,
) -> Result<ConnectorExactSemanticRevision, ConnectorError> {
    let object = decode_fact(object.as_bytes())?;
    let data = decode_fact(data.as_bytes())?;
    if object.provider != data.provider {
        return invalid("persisted exact revision facts belong to different providers");
    }
    ConnectorExactSemanticRevision::try_from_persisted_facts(
        ConnectorProviderId::parse(&object.provider)?,
        object.format,
        object.version,
        Bytes::from(object.value),
        data.format,
        data.version,
        Bytes::from(data.value),
    )
}

fn encode_fact(format: &ProviderFactFormat, value: &[u8]) -> Result<Vec<u8>, ConnectorError> {
    let provider = format.provider().as_bytes();
    let format_name = format.format().as_bytes();
    if provider.is_empty()
        || provider.len() > u16::MAX as usize
        || format_name.is_empty()
        || format_name.len() > MAX_CONNECTOR_SEMANTIC_FACT_FORMAT_BYTES
        || value.is_empty()
        || value.len() > MAX_CONNECTOR_SEMANTIC_FACT_VALUE_BYTES
        || format.version() == 0
    {
        return invalid("exact query revision fact is outside the persistence bounds");
    }
    let mut encoded = Vec::with_capacity(
        FACT_ENVELOPE_MAGIC.len() + 8 + provider.len() + format_name.len() + value.len(),
    );
    encoded.extend_from_slice(FACT_ENVELOPE_MAGIC);
    push_len(&mut encoded, provider.len())?;
    encoded.extend_from_slice(provider);
    push_len(&mut encoded, format_name.len())?;
    encoded.extend_from_slice(format_name);
    encoded.extend_from_slice(&format.version().to_be_bytes());
    push_len(&mut encoded, value.len())?;
    encoded.extend_from_slice(value);
    Ok(encoded)
}

fn encode_connector_fact(
    fact: &novarocks_spi::connector::ConnectorSemanticFact,
) -> Result<Vec<u8>, ConnectorError> {
    encode_fact_parts(
        fact.provider().as_str(),
        fact.format(),
        fact.version(),
        fact.value().as_ref(),
    )
}

fn encode_fact_parts(
    provider: &str,
    format_name: &str,
    version: u16,
    value: &[u8],
) -> Result<Vec<u8>, ConnectorError> {
    let provider = provider.as_bytes();
    let format_name = format_name.as_bytes();
    if provider.is_empty()
        || provider.len() > u16::MAX as usize
        || format_name.is_empty()
        || format_name.len() > MAX_CONNECTOR_SEMANTIC_FACT_FORMAT_BYTES
        || value.is_empty()
        || value.len() > MAX_CONNECTOR_SEMANTIC_FACT_VALUE_BYTES
        || version == 0
    {
        return invalid("exact revision fact is outside the persistence bounds");
    }
    let mut encoded = Vec::with_capacity(
        FACT_ENVELOPE_MAGIC.len() + 8 + provider.len() + format_name.len() + value.len(),
    );
    encoded.extend_from_slice(FACT_ENVELOPE_MAGIC);
    push_len(&mut encoded, provider.len())?;
    encoded.extend_from_slice(provider);
    push_len(&mut encoded, format_name.len())?;
    encoded.extend_from_slice(format_name);
    encoded.extend_from_slice(&version.to_be_bytes());
    push_len(&mut encoded, value.len())?;
    encoded.extend_from_slice(value);
    Ok(encoded)
}

struct DecodedFact {
    provider: String,
    format: String,
    version: u16,
    value: Vec<u8>,
}

fn decode_fact(encoded: &[u8]) -> Result<DecodedFact, ConnectorError> {
    if !encoded.starts_with(FACT_ENVELOPE_MAGIC) {
        return corrupt("persisted exact revision fact has an unknown envelope");
    }
    let mut cursor = FACT_ENVELOPE_MAGIC.len();
    let provider = take_string(encoded, &mut cursor, "provider")?;
    let format = take_string(encoded, &mut cursor, "format")?;
    let version = u16::from_be_bytes(take(encoded, &mut cursor, 2)?.try_into().unwrap());
    let value_len = usize::from(u16::from_be_bytes(
        take(encoded, &mut cursor, 2)?.try_into().unwrap(),
    ));
    let value = take(encoded, &mut cursor, value_len)?.to_vec();
    if cursor != encoded.len()
        || format.len() > MAX_CONNECTOR_SEMANTIC_FACT_FORMAT_BYTES
        || value.len() > MAX_CONNECTOR_SEMANTIC_FACT_VALUE_BYTES
    {
        return corrupt("persisted exact revision fact has a non-canonical envelope");
    }
    Ok(DecodedFact {
        provider,
        format,
        version,
        value,
    })
}

fn push_len(output: &mut Vec<u8>, len: usize) -> Result<(), ConnectorError> {
    output.extend_from_slice(
        &u16::try_from(len)
            .map_err(|_| invalid_error("exact revision fact length exceeds u16"))?
            .to_be_bytes(),
    );
    Ok(())
}

fn take<'a>(encoded: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8], ConnectorError> {
    let end = cursor
        .checked_add(len)
        .filter(|end| *end <= encoded.len())
        .ok_or_else(|| corrupt_error("persisted exact revision fact is truncated"))?;
    let value = &encoded[*cursor..end];
    *cursor = end;
    Ok(value)
}

fn take_string(
    encoded: &[u8],
    cursor: &mut usize,
    subject: &str,
) -> Result<String, ConnectorError> {
    let len = usize::from(u16::from_be_bytes(
        take(encoded, cursor, 2)?.try_into().unwrap(),
    ));
    let value = std::str::from_utf8(take(encoded, cursor, len)?).map_err(|_| {
        corrupt_error(format!(
            "persisted exact revision {subject} is not valid UTF-8"
        ))
    })?;
    if value.is_empty() {
        return corrupt("persisted exact revision fact contains an empty identity");
    }
    Ok(value.to_string())
}

fn invalid<T>(message: impl Into<String>) -> Result<T, ConnectorError> {
    Err(invalid_error(message))
}

fn invalid_error(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt<T>(message: impl Into<String>) -> Result<T, ConnectorError> {
    Err(corrupt_error(message))
}

fn corrupt_error(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn format(provider: &str, name: &str, version: u16) -> ProviderFactFormat {
        ProviderFactFormat::try_new_versioned(provider, name, version).unwrap()
    }

    #[test]
    fn persisted_revision_round_trips_the_complete_fact_identity() {
        let object = ObjectIdentity::try_new(
            format("iceberg", "table-object", 3),
            Arc::<[u8]>::from(&b"object-7"[..]),
        )
        .unwrap();
        let data = DataVersion::try_new(
            format("iceberg", "snapshot", 5),
            Arc::<[u8]>::from(&b"snapshot-11"[..]),
        )
        .unwrap();

        let (stored_object, stored_data) = persist_exact_query_revision(&object, &data).unwrap();
        let restored = restore_exact_query_revision(&stored_object, &stored_data).unwrap();

        assert_eq!(restored.object_identity().provider().as_str(), "iceberg");
        assert_eq!(restored.object_identity().format(), "table-object");
        assert_eq!(restored.object_identity().version(), 3);
        assert_eq!(restored.object_identity().value().as_ref(), b"object-7");
        assert_eq!(restored.data_version().format(), "snapshot");
        assert_eq!(restored.data_version().version(), 5);
        assert_eq!(restored.data_version().value().as_ref(), b"snapshot-11");
    }

    #[test]
    fn persisted_revision_rejects_truncation_and_cross_provider_pairs() {
        let object = ObjectIdentity::try_new(
            format("iceberg", "table-object", 1),
            Arc::<[u8]>::from(&b"object"[..]),
        )
        .unwrap();
        let data = DataVersion::try_new(
            format("paimon", "snapshot", 1),
            Arc::<[u8]>::from(&b"snapshot"[..]),
        )
        .unwrap();
        assert!(persist_exact_query_revision(&object, &data).is_err());

        let data = DataVersion::try_new(
            format("iceberg", "snapshot", 1),
            Arc::<[u8]>::from(&b"snapshot"[..]),
        )
        .unwrap();
        let (stored_object, mut stored_data) =
            persist_exact_query_revision(&object, &data).unwrap();
        let mut truncated = stored_data.as_bytes().to_vec();
        truncated.pop();
        stored_data = NativeDataVersion::try_new(truncated).unwrap();
        assert!(restore_exact_query_revision(&stored_object, &stored_data).is_err());
    }
}
