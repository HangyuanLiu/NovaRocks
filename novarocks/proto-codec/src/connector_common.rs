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

//! Strict outer codec for provider-owned connector payloads.

use bytes::Bytes;
use novarocks_proto_models::{catalog as catalog_dto, connector_common as dto};
use novarocks_spi::connector::{
    CATALOG_VERSION_BYTES, CatalogHandle, CatalogVersion, ConnectorCodecCategory,
    ConnectorCodecError, ConnectorCodecErrorKind, ConnectorCodecRevision, ConnectorDecodeLedger,
    ConnectorDecodeLimits, ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorFieldPath,
    ConnectorInstanceId, ConnectorProviderId,
};
use prost::Message;

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

pub fn encode_connector_payload(value: &ConnectorEncodedPayload) -> Vec<u8> {
    encode_connector_payload_message(value).encode_to_vec()
}

/// Materialize the public protobuf envelope around provider-private bytes.
/// Purpose-specific carriers use this helper so they never know a provider's
/// private message type.
pub fn encode_connector_payload_message(
    value: &ConnectorEncodedPayload,
) -> dto::ConnectorEncodedPayload {
    let header = value.header();
    dto::ConnectorEncodedPayload {
        header: Some(dto::ConnectorEnvelopeHeader {
            provider_id: header.provider_id().as_str().to_owned(),
            catalog: Some(catalog_dto::CatalogHandle {
                catalog_name: header.catalog().catalog_name().as_str().to_owned(),
                version: header.catalog().version().as_bytes().to_vec(),
            }),
            category: encode_category(header.category()),
            codec_revision: header.codec_revision().get(),
        }),
        payload: value.payload().to_vec(),
    }
}

pub fn decode_connector_payload(
    encoded: &[u8],
    limits: ConnectorDecodeLimits,
) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
    let root = ConnectorFieldPath::root("connector_payload");
    let mut ledger = ConnectorDecodeLedger::new(limits);
    ledger.charge_raw(encoded.len())?;
    scan_message(encoded, &[(1, 2), (2, 2)], &root)?;
    let raw = dto::ConnectorEncodedPayload::decode(encoded)
        .map_err(|error| invalid(root.clone(), format!("malformed protobuf: {error}")))?;
    let raw_header = raw.header.ok_or_else(|| missing(root.field("header")))?;
    let encoded_header = extract_length_delimited(encoded, 1, &root)?;
    let header_path = root.field("header");
    scan_message(
        encoded_header,
        &[(1, 2), (2, 2), (3, 0), (4, 0)],
        &header_path,
    )?;
    let encoded_catalog = extract_length_delimited(encoded_header, 2, &header_path)?;
    let catalog_path = header_path.field("catalog");
    scan_message(encoded_catalog, &[(1, 2), (2, 2)], &catalog_path)?;

    ledger.charge_items(8)?;
    ledger.check_depth(3)?;
    ledger.charge_scalar(raw_header.provider_id.len())?;
    let provider_id = ConnectorProviderId::parse(&raw_header.provider_id).map_err(|_| {
        invalid(
            header_path.field("provider_id"),
            "invalid provider identity",
        )
    })?;
    let raw_catalog = raw_header
        .catalog
        .ok_or_else(|| missing(header_path.field("catalog")))?;
    ledger.charge_scalar(raw_catalog.catalog_name.len())?;
    ledger.charge_scalar(raw_catalog.version.len())?;
    let catalog_name =
        ConnectorInstanceId::try_from_canonical(&raw_catalog.catalog_name).map_err(|_| {
            invalid(
                catalog_path.field("catalog_name"),
                "invalid catalog identity",
            )
        })?;
    let version: [u8; CATALOG_VERSION_BYTES] = raw_catalog.version.try_into().map_err(|_| {
        invalid(
            catalog_path.field("version"),
            "catalog version must contain exactly 32 bytes",
        )
    })?;
    let category = decode_category(raw_header.category, header_path.field("category"))?;
    let revision = ConnectorCodecRevision::try_new(raw_header.codec_revision)?;
    ledger.charge_retained(
        raw.payload
            .len()
            .saturating_add(raw_header.provider_id.len())
            .saturating_add(raw_catalog.catalog_name.len())
            .saturating_add(CATALOG_VERSION_BYTES),
    )?;
    Ok(ConnectorEncodedPayload::new(
        ConnectorEnvelopeHeader::new(
            provider_id,
            CatalogHandle::new(catalog_name, CatalogVersion::from_bytes(version)),
            category,
            revision,
        ),
        Bytes::from(raw.payload),
    ))
}

/// Validate an envelope that has already been decoded as part of a larger
/// protobuf carrier. Provider payload semantics remain untouched here.
pub fn decode_connector_payload_message(
    raw: dto::ConnectorEncodedPayload,
    limits: ConnectorDecodeLimits,
) -> Result<ConnectorEncodedPayload, ConnectorCodecError> {
    let encoded = raw.encode_to_vec();
    decode_connector_payload(&encoded, limits)
}

/// Decode one envelope embedded in a purpose-specific carrier and prove that
/// it belongs to the category selected by that carrier. The installed binding
/// performs the later provider/catalog/revision comparison.
pub fn decode_embedded_connector_payload(
    raw: Option<&dto::ConnectorEncodedPayload>,
    expected_category: ConnectorCodecCategory,
    max_encoded_bytes: usize,
    path: FieldPath,
) -> Result<ConnectorEncodedPayload, ProtocolError> {
    let raw = raw.ok_or_else(|| {
        ProtocolError::new(
            path.clone(),
            ProtocolErrorKind::MissingField,
            "provider payload must be present",
        )
    })?;
    let encoded_len = raw.encoded_len();
    if encoded_len > max_encoded_bytes {
        return Err(ProtocolError::new(
            path.clone(),
            ProtocolErrorKind::OutOfRange,
            format!("provider payload exceeds {max_encoded_bytes} encoded bytes"),
        ));
    }
    let envelope_slack = 4096usize;
    let limit = max_encoded_bytes.max(1);
    let limits =
        ConnectorDecodeLimits::try_new(limit, limit.saturating_add(envelope_slack), limit, 16, 8)
            .expect("finite embedded connector limits are valid");
    let decoded = decode_connector_payload_message(raw.clone(), limits)
        .map_err(|error| connector_error(path.clone(), error))?;
    if decoded.header().category() != expected_category {
        return Err(ProtocolError::new(
            path.field("header"),
            ProtocolErrorKind::InconsistentFields,
            "connector payload category does not match its public carrier",
        ));
    }
    Ok(decoded)
}

fn connector_error(path: FieldPath, error: ConnectorCodecError) -> ProtocolError {
    let kind = match error.kind() {
        ConnectorCodecErrorKind::MissingField => ProtocolErrorKind::MissingField,
        ConnectorCodecErrorKind::InvalidEnum => ProtocolErrorKind::InvalidEnum,
        ConnectorCodecErrorKind::InvalidValue | ConnectorCodecErrorKind::UnknownField => {
            ProtocolErrorKind::InvalidValue
        }
        ConnectorCodecErrorKind::DuplicateField => ProtocolErrorKind::DuplicateField,
        ConnectorCodecErrorKind::InconsistentFields => ProtocolErrorKind::InconsistentFields,
        ConnectorCodecErrorKind::Unsupported => ProtocolErrorKind::Unsupported,
        ConnectorCodecErrorKind::Capacity => ProtocolErrorKind::Capacity,
        ConnectorCodecErrorKind::VersionMismatch => ProtocolErrorKind::VersionMismatch,
        ConnectorCodecErrorKind::CompileControl(cause) => ProtocolErrorKind::CompileControl(cause),
    };
    ProtocolError::new(path, kind, error.to_string())
}

fn encode_category(category: ConnectorCodecCategory) -> i32 {
    match category {
        ConnectorCodecCategory::ReadTable => dto::ConnectorPayloadCategory::ReadTable as i32,
        ConnectorCodecCategory::ReadView => dto::ConnectorPayloadCategory::ReadView as i32,
        ConnectorCodecCategory::ReadColumn => dto::ConnectorPayloadCategory::ReadColumn as i32,
        ConnectorCodecCategory::ReadSplit => dto::ConnectorPayloadCategory::ReadSplit as i32,
        ConnectorCodecCategory::WriteHandle => dto::ConnectorPayloadCategory::WriteHandle as i32,
        ConnectorCodecCategory::CommitFragment => {
            dto::ConnectorPayloadCategory::CommitFragment as i32
        }
    }
}

fn decode_category(
    value: i32,
    path: ConnectorFieldPath,
) -> Result<ConnectorCodecCategory, ConnectorCodecError> {
    match dto::ConnectorPayloadCategory::try_from(value) {
        Ok(dto::ConnectorPayloadCategory::ReadTable) => Ok(ConnectorCodecCategory::ReadTable),
        Ok(dto::ConnectorPayloadCategory::ReadView) => Ok(ConnectorCodecCategory::ReadView),
        Ok(dto::ConnectorPayloadCategory::ReadColumn) => Ok(ConnectorCodecCategory::ReadColumn),
        Ok(dto::ConnectorPayloadCategory::ReadSplit) => Ok(ConnectorCodecCategory::ReadSplit),
        Ok(dto::ConnectorPayloadCategory::WriteHandle) => Ok(ConnectorCodecCategory::WriteHandle),
        Ok(dto::ConnectorPayloadCategory::CommitFragment) => {
            Ok(ConnectorCodecCategory::CommitFragment)
        }
        Ok(dto::ConnectorPayloadCategory::Unspecified) | Err(_) => Err(ConnectorCodecError::new(
            path,
            ConnectorCodecErrorKind::InvalidEnum,
            "connector payload category is unknown or unspecified",
        )),
    }
}

fn scan_message(
    mut input: &[u8],
    fields: &[(u32, u8)],
    path: &ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    let mut seen = 0u64;
    while !input.is_empty() {
        let key = read_varint(&mut input, path)?;
        let field = u32::try_from(key >> 3)
            .map_err(|_| invalid(path.clone(), "protobuf field number is out of range"))?;
        let wire = (key & 7) as u8;
        let index = fields
            .iter()
            .position(|(number, _)| *number == field)
            .ok_or_else(|| {
                ConnectorCodecError::new(
                    path.field(format!("field_{field}")),
                    ConnectorCodecErrorKind::UnknownField,
                    "unknown connector envelope field",
                )
            })?;
        if wire != fields[index].1 {
            return Err(ConnectorCodecError::new(
                path.field(format!("field_{field}")),
                ConnectorCodecErrorKind::InvalidValue,
                "connector envelope field has the wrong protobuf wire type",
            ));
        }
        let bit = 1u64 << index;
        if seen & bit != 0 {
            return Err(ConnectorCodecError::new(
                path.field(format!("field_{field}")),
                ConnectorCodecErrorKind::DuplicateField,
                "connector envelope singular field appears more than once",
            ));
        }
        seen |= bit;
        skip_value(&mut input, wire, path)?;
    }
    Ok(())
}

fn extract_length_delimited<'a>(
    mut input: &'a [u8],
    wanted: u32,
    path: &ConnectorFieldPath,
) -> Result<&'a [u8], ConnectorCodecError> {
    while !input.is_empty() {
        let key = read_varint(&mut input, path)?;
        let field = (key >> 3) as u32;
        let wire = (key & 7) as u8;
        if wire != 2 {
            skip_value(&mut input, wire, path)?;
            continue;
        }
        let length = usize::try_from(read_varint(&mut input, path)?)
            .map_err(|_| invalid(path.clone(), "protobuf length is out of range"))?;
        if input.len() < length {
            return Err(invalid(path.clone(), "truncated length-delimited field"));
        }
        let (value, rest) = input.split_at(length);
        if field == wanted {
            return Ok(value);
        }
        input = rest;
    }
    Err(missing(path.field(format!("field_{wanted}"))))
}

fn skip_value(
    input: &mut &[u8],
    wire: u8,
    path: &ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    match wire {
        0 => {
            read_varint(input, path)?;
        }
        1 => take(input, 8, path)?,
        2 => {
            let length = usize::try_from(read_varint(input, path)?)
                .map_err(|_| invalid(path.clone(), "protobuf length is out of range"))?;
            take(input, length, path)?;
        }
        5 => take(input, 4, path)?,
        _ => return Err(invalid(path.clone(), "unsupported protobuf wire type")),
    }
    Ok(())
}

fn take(
    input: &mut &[u8],
    length: usize,
    path: &ConnectorFieldPath,
) -> Result<(), ConnectorCodecError> {
    if input.len() < length {
        return Err(invalid(path.clone(), "truncated protobuf field"));
    }
    *input = &input[length..];
    Ok(())
}

fn read_varint(input: &mut &[u8], path: &ConnectorFieldPath) -> Result<u64, ConnectorCodecError> {
    let mut value = 0u64;
    for shift in (0..70).step_by(7) {
        let (&byte, rest) = input
            .split_first()
            .ok_or_else(|| invalid(path.clone(), "truncated protobuf varint"))?;
        *input = rest;
        if shift == 63 && byte > 1 {
            return Err(invalid(path.clone(), "protobuf varint overflow"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(invalid(path.clone(), "protobuf varint overflow"))
}

fn missing(path: ConnectorFieldPath) -> ConnectorCodecError {
    ConnectorCodecError::new(
        path,
        ConnectorCodecErrorKind::MissingField,
        "required connector envelope field is missing",
    )
}

fn invalid(path: ConnectorFieldPath, detail: impl AsRef<str>) -> ConnectorCodecError {
    ConnectorCodecError::new(path, ConnectorCodecErrorKind::InvalidValue, detail)
}

#[cfg(test)]
mod compile_control_adapter_tests {
    use super::*;
    use novarocks_type_contract::CompileControlError;
    use std::error::Error;

    #[test]
    fn connector_error_adapter_keeps_each_compile_cause_and_both_diagnostic_paths() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let provider = ConnectorCodecError::from(cause)
                .with_path(ConnectorFieldPath::root("columns").index(17));
            let expected_detail = provider.to_string();
            let outer = FieldPath::root("fragment")
                .field("scans")
                .index(3)
                .field("provider_payload");
            let error = connector_error(outer.clone(), provider);
            assert_eq!(error.path(), &outer);
            assert_eq!(error.kind(), ProtocolErrorKind::CompileControl(cause));
            assert_eq!(error.detail(), expected_detail);
            assert!(error.detail().contains("columns[17]"));
            assert_eq!(
                error
                    .source()
                    .unwrap()
                    .downcast_ref::<CompileControlError>(),
                Some(&cause)
            );
        }
    }

    #[test]
    fn connector_error_adapter_preserves_ordinary_categories_despite_control_spelling() {
        let categories = [
            (
                ConnectorCodecErrorKind::MissingField,
                ProtocolErrorKind::MissingField,
            ),
            (
                ConnectorCodecErrorKind::InvalidEnum,
                ProtocolErrorKind::InvalidEnum,
            ),
            (
                ConnectorCodecErrorKind::InvalidValue,
                ProtocolErrorKind::InvalidValue,
            ),
            (
                ConnectorCodecErrorKind::UnknownField,
                ProtocolErrorKind::InvalidValue,
            ),
            (
                ConnectorCodecErrorKind::DuplicateField,
                ProtocolErrorKind::DuplicateField,
            ),
            (
                ConnectorCodecErrorKind::InconsistentFields,
                ProtocolErrorKind::InconsistentFields,
            ),
            (
                ConnectorCodecErrorKind::Unsupported,
                ProtocolErrorKind::Unsupported,
            ),
            (
                ConnectorCodecErrorKind::Capacity,
                ProtocolErrorKind::Capacity,
            ),
            (
                ConnectorCodecErrorKind::VersionMismatch,
                ProtocolErrorKind::VersionMismatch,
            ),
        ];
        for (provider_kind, expected_kind) in categories {
            let provider = ConnectorCodecError::new(
                ConnectorFieldPath::root("payload").field("header"),
                provider_kind,
                "pure compilation was cancelled; pure compilation deadline was exceeded; pure compilation work budget was exhausted",
            );
            let expected_detail = provider.to_string();
            let error = connector_error(FieldPath::root("scan"), provider);
            assert_eq!(error.kind(), expected_kind);
            assert_eq!(error.detail(), expected_detail);
            assert!(error.source().is_none());
        }
    }

    #[test]
    fn embedded_connector_decode_keeps_existing_missing_and_category_errors() {
        let path = FieldPath::root("fragment").field("scan");
        let missing = decode_embedded_connector_payload(
            None,
            ConnectorCodecCategory::ReadTable,
            1024,
            path.clone(),
        )
        .unwrap_err();
        assert_eq!(missing.path(), &path);
        assert_eq!(missing.kind(), ProtocolErrorKind::MissingField);
        assert!(missing.source().is_none());

        let payload = ConnectorEncodedPayload::new(
            ConnectorEnvelopeHeader::new(
                ConnectorProviderId::parse("paimon").unwrap(),
                CatalogHandle::new(
                    ConnectorInstanceId::try_from_canonical("lake").unwrap(),
                    CatalogVersion::from_bytes([4; 32]),
                ),
                ConnectorCodecCategory::ReadSplit,
                ConnectorCodecRevision::try_new(3).unwrap(),
            ),
            Bytes::from_static(b"private-split"),
        );
        let raw = encode_connector_payload_message(&payload);
        let mismatch = decode_embedded_connector_payload(
            Some(&raw),
            ConnectorCodecCategory::ReadTable,
            1024,
            path.clone(),
        )
        .unwrap_err();
        assert_eq!(mismatch.path(), &path.field("header"));
        assert_eq!(mismatch.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            mismatch.detail(),
            "connector payload category does not match its public carrier"
        );
        assert!(mismatch.source().is_none());
    }
}
