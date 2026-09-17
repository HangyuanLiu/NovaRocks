// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCommittedVersion, ConnectorDeferredDocumentHandle, ConnectorDocumentAttachment,
    ConnectorDocumentCarrier, ConnectorDocumentFormat, ConnectorDocumentId, ConnectorDocumentName,
    ConnectorDocumentOwner, ConnectorDocumentReference, ConnectorDocumentRevision,
    ConnectorDocumentStorageLimits, ConnectorError, ConnectorErrorKind, ConnectorStoredDocument,
    ConnectorStoredDocumentAttachment, MAX_CONNECTOR_DOCUMENTS,
};

use super::envelope::{
    DOCUMENT_ENVELOPE_VERSION, DOCUMENT_MANIFEST_VERSION, IcebergDocumentAttachmentV1,
    IcebergDocumentCarrierV1, IcebergDocumentEnvelopeV1, IcebergDocumentManifestV1,
    IcebergDocumentReferenceV1, MAX_DOCUMENT_MANIFEST_BYTES,
};

pub(crate) fn encode_document_manifest(
    manifest: &IcebergDocumentManifestV1,
) -> Result<Bytes, ConnectorError> {
    validate_manifest(manifest)?;
    let bytes = serde_json::to_vec(manifest)
        .map_err(|error| corrupt(format!("encode Iceberg document manifest: {error}")))?;
    if bytes.len() > MAX_DOCUMENT_MANIFEST_BYTES {
        return Err(exhausted(
            "Iceberg document manifest exceeds its byte limit",
        ));
    }
    Ok(Bytes::from(bytes))
}

pub(crate) fn decode_document_manifest(
    bytes: &[u8],
) -> Result<IcebergDocumentManifestV1, ConnectorError> {
    decode_document_manifest_with_limits(bytes, ConnectorDocumentStorageLimits::spec_default())
}

pub(crate) fn decode_document_manifest_with_limits(
    bytes: &[u8],
    limits: ConnectorDocumentStorageLimits,
) -> Result<IcebergDocumentManifestV1, ConnectorError> {
    if bytes.is_empty() || bytes.len() > MAX_DOCUMENT_MANIFEST_BYTES {
        return Err(exhausted(
            "Iceberg document manifest is empty or exceeds its byte limit",
        ));
    }
    preflight_decode(bytes, limits, "Iceberg document manifest")?;
    preflight_manifest_item_counts(bytes, limits)?;
    let manifest: IcebergDocumentManifestV1 = serde_json::from_slice(bytes)
        .map_err(|error| corrupt(format!("decode Iceberg document manifest: {error}")))?;
    validate_manifest(&manifest)?;
    validate_manifest_limits(&manifest, limits)?;
    Ok(manifest)
}

fn validate_manifest_limits(
    manifest: &IcebergDocumentManifestV1,
    limits: ConnectorDocumentStorageLimits,
) -> Result<(), ConnectorError> {
    if manifest.documents.len() > limits.max_documents() {
        return Err(exhausted(
            "Iceberg document manifest exceeds the caller document budget",
        ));
    }
    let mut references = 0usize;
    for document in &manifest.documents {
        let encoded_len = usize::try_from(document.encoded_len)
            .map_err(|_| exhausted("Iceberg document length does not fit this process"))?;
        if encoded_len > limits.max_document_bytes() {
            return Err(exhausted(
                "Iceberg document manifest exceeds the caller per-document budget",
            ));
        }
        references = references
            .checked_add(document.references.len())
            .ok_or_else(|| exhausted("Iceberg document reference accounting overflowed"))?;
    }
    if references > limits.max_references() {
        return Err(exhausted(
            "Iceberg document manifest exceeds the caller reference budget",
        ));
    }
    Ok(())
}

/// Walk only the closed manifest paths that own allocating collections. Keys
/// with the same spelling at an unknown path are deliberately ignored here so
/// serde's `deny_unknown_fields` classifies them as corrupt data rather than a
/// caller-budget violation.
fn preflight_manifest_item_counts(
    bytes: &[u8],
    limits: ConnectorDocumentStorageLimits,
) -> Result<(), ConnectorError> {
    let mut documents = 0usize;
    let mut references = 0usize;
    let root = skip_json_whitespace(bytes, 0);
    visit_json_object(bytes, root, |key, value_start, _| {
        if json_key_matches(key, b"documents")? {
            visit_json_array(bytes, value_start, |document_start, _| {
                documents = documents
                    .checked_add(1)
                    .ok_or_else(|| exhausted("Iceberg document manifest item count overflowed"))?;
                if documents > limits.max_documents() {
                    return Err(exhausted(
                        "Iceberg document manifest exceeds the caller document budget",
                    ));
                }
                scan_document_envelope(bytes, document_start, limits, &mut references)
            })?;
        }
        Ok(())
    })?;
    Ok(())
}

fn scan_document_envelope(
    bytes: &[u8],
    start: usize,
    limits: ConnectorDocumentStorageLimits,
    references: &mut usize,
) -> Result<(), ConnectorError> {
    visit_json_object(bytes, start, |key, value_start, _| {
        if json_key_matches(key, b"encoded_len")? {
            let encoded_len = json_u64(bytes, value_start)?;
            let max_document_bytes = u64::try_from(limits.max_document_bytes()).map_err(|_| {
                exhausted("Iceberg caller per-document budget does not fit the carrier format")
            })?;
            if encoded_len > max_document_bytes {
                return Err(exhausted(
                    "Iceberg document manifest exceeds the caller per-document budget",
                ));
            }
        } else if json_key_matches(key, b"references")? {
            visit_json_array(bytes, value_start, |_, _| {
                *references = references.checked_add(1).ok_or_else(|| {
                    exhausted("Iceberg document manifest reference count overflowed")
                })?;
                if *references > limits.max_references() {
                    return Err(exhausted(
                        "Iceberg document manifest exceeds the caller reference budget",
                    ));
                }
                Ok(())
            })?;
        } else if json_key_matches(key, b"carrier")? {
            scan_document_carrier(bytes, value_start, limits)?;
        } else if json_key_matches(key, b"attachment")? {
            scan_document_attachment(bytes, value_start)?;
        }
        Ok(())
    })?;
    Ok(())
}

fn scan_document_carrier(
    bytes: &[u8],
    start: usize,
    limits: ConnectorDocumentStorageLimits,
) -> Result<(), ConnectorError> {
    let mut kind = None;
    let mut content_items = None;
    let mut location_seen = false;
    visit_json_object(bytes, start, |key, value_start, _| {
        if json_key_matches(key, b"kind")? {
            if kind.is_some() || bytes.get(value_start) != Some(&b'"') {
                return Err(corrupt("Iceberg document carrier has an invalid kind"));
            }
            let end = json_string_end(bytes, value_start + 1)
                .ok_or_else(|| corrupt("Iceberg document carrier has an unterminated kind"))?;
            let raw = &bytes[value_start + 1..end];
            kind = if json_key_matches(raw, b"available")? {
                Some(CarrierKind::Available)
            } else if json_key_matches(raw, b"deferred")? {
                Some(CarrierKind::Deferred)
            } else {
                Some(CarrierKind::Unknown)
            };
        } else if json_key_matches(key, b"content")? {
            if content_items.is_some() {
                return Err(corrupt("Iceberg document carrier repeats its content"));
            }
            let mut count = 0usize;
            visit_json_array(bytes, value_start, |_, _| {
                count = count.checked_add(1).ok_or_else(|| {
                    exhausted("Iceberg inline document length accounting overflowed")
                })?;
                Ok(())
            })?;
            content_items = Some(count);
        } else if json_key_matches(key, b"location")? {
            if location_seen {
                return Err(corrupt("Iceberg document carrier repeats its location"));
            }
            location_seen = true;
        } else {
            return Err(corrupt("Iceberg document carrier has an unknown field"));
        }
        Ok(())
    })?;
    match (kind, content_items, location_seen) {
        (Some(CarrierKind::Available), Some(count), false) => {
            if count > limits.max_document_bytes() {
                return Err(exhausted(
                    "Iceberg inline document exceeds the caller per-document budget",
                ));
            }
        }
        (Some(CarrierKind::Deferred), None, true) => {}
        _ => return Err(corrupt("Iceberg document carrier shape is invalid")),
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum CarrierKind {
    Available,
    Deferred,
    Unknown,
}

fn scan_document_attachment(bytes: &[u8], start: usize) -> Result<(), ConnectorError> {
    let mut kind = None;
    let mut committed_version_seen = false;
    let mut snapshot_id_seen = false;
    visit_json_object(bytes, start, |key, value_start, _| {
        if json_key_matches(key, b"kind")? {
            if kind.is_some() || bytes.get(value_start) != Some(&b'"') {
                return Err(corrupt("Iceberg document attachment has an invalid kind"));
            }
            let end = json_string_end(bytes, value_start + 1)
                .ok_or_else(|| corrupt("Iceberg document attachment has an unterminated kind"))?;
            let raw = &bytes[value_start + 1..end];
            kind = if json_key_matches(raw, b"table-metadata")? {
                Some(AttachmentKind::TableMetadata)
            } else if json_key_matches(raw, b"exact-output")? {
                Some(AttachmentKind::ExactOutput)
            } else if json_key_matches(raw, b"commit-output")? {
                Some(AttachmentKind::CommitOutput)
            } else {
                Some(AttachmentKind::Unknown)
            };
        } else if json_key_matches(key, b"committed_version")? {
            if committed_version_seen {
                return Err(corrupt(
                    "Iceberg document attachment repeats its committed version",
                ));
            }
            committed_version_seen = true;
        } else if json_key_matches(key, b"snapshot_id")? {
            if snapshot_id_seen {
                return Err(corrupt(
                    "Iceberg document attachment repeats its snapshot id",
                ));
            }
            snapshot_id_seen = true;
        } else {
            return Err(corrupt("Iceberg document attachment has an unknown field"));
        }
        Ok(())
    })?;
    match (kind, committed_version_seen, snapshot_id_seen) {
        (Some(AttachmentKind::ExactOutput), true, true)
        | (Some(AttachmentKind::TableMetadata | AttachmentKind::CommitOutput), false, false) => {
            Ok(())
        }
        _ => Err(corrupt("Iceberg document attachment shape is invalid")),
    }
}

#[derive(Clone, Copy)]
enum AttachmentKind {
    TableMetadata,
    ExactOutput,
    CommitOutput,
    Unknown,
}

fn json_u64(bytes: &[u8], mut index: usize) -> Result<u64, ConnectorError> {
    let mut value = 0u64;
    let mut digits = 0usize;
    while let Some(byte @ b'0'..=b'9') = bytes.get(index) {
        value = value
            .checked_mul(10)
            .and_then(|value| value.checked_add(u64::from(*byte - b'0')))
            .ok_or_else(|| exhausted("Iceberg document length exceeds u64"))?;
        digits += 1;
        index += 1;
    }
    if digits == 0 {
        return Err(corrupt(
            "Iceberg document manifest has a non-integer document length",
        ));
    }
    if bytes
        .get(index)
        .is_some_and(|byte| !byte.is_ascii_whitespace() && !matches!(*byte, b',' | b'}' | b']'))
    {
        return Err(corrupt(
            "Iceberg document manifest has a non-integer document length",
        ));
    }
    Ok(value)
}

fn json_key_matches(raw: &[u8], expected: &[u8]) -> Result<bool, ConnectorError> {
    let mut raw_index = 0usize;
    let mut expected_index = 0usize;
    while raw_index < raw.len() {
        let decoded = if raw[raw_index] != b'\\' {
            let value = raw[raw_index];
            raw_index += 1;
            value
        } else {
            raw_index += 1;
            let escape = *raw.get(raw_index).ok_or_else(|| {
                corrupt("Iceberg document manifest has an incomplete JSON property escape")
            })?;
            raw_index += 1;
            match escape {
                b'"' | b'\\' | b'/' => escape,
                b'b' => 0x08,
                b'f' => 0x0c,
                b'n' => b'\n',
                b'r' => b'\r',
                b't' => b'\t',
                b'u' => {
                    let end = raw_index.checked_add(4).ok_or_else(|| {
                        corrupt("Iceberg document manifest property escape overflowed")
                    })?;
                    let digits = raw.get(raw_index..end).ok_or_else(|| {
                        corrupt("Iceberg document manifest has a short Unicode property escape")
                    })?;
                    raw_index = end;
                    let mut codepoint = 0u16;
                    for digit in digits {
                        let digit = hex_digit(*digit)?;
                        codepoint = codepoint
                            .checked_mul(16)
                            .and_then(|value| value.checked_add(u16::from(digit)))
                            .ok_or_else(|| {
                                corrupt("Iceberg document manifest property escape overflowed")
                            })?;
                    }
                    let Ok(value) = u8::try_from(codepoint) else {
                        return Ok(false);
                    };
                    value
                }
                _ => {
                    return Err(corrupt(
                        "Iceberg document manifest has an invalid JSON property escape",
                    ));
                }
            }
        };
        if expected.get(expected_index) != Some(&decoded) {
            return Ok(false);
        }
        expected_index += 1;
    }
    Ok(expected_index == expected.len())
}

fn hex_digit(byte: u8) -> Result<u8, ConnectorError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(corrupt(
            "Iceberg document manifest has an invalid Unicode property escape",
        )),
    }
}

fn json_string_end(bytes: &[u8], mut index: usize) -> Option<usize> {
    let mut escaped = false;
    while let Some(byte) = bytes.get(index) {
        if escaped {
            escaped = false;
        } else if *byte == b'\\' {
            escaped = true;
        } else if *byte == b'"' {
            return Some(index);
        }
        index += 1;
    }
    None
}

fn visit_json_object(
    bytes: &[u8],
    start: usize,
    mut visitor: impl FnMut(&[u8], usize, usize) -> Result<(), ConnectorError>,
) -> Result<usize, ConnectorError> {
    if bytes.get(start) != Some(&b'{') {
        return Err(corrupt("Iceberg document manifest expected a JSON object"));
    }
    let mut index = start + 1;
    let mut first = true;
    loop {
        index = skip_json_whitespace(bytes, index);
        if bytes.get(index) == Some(&b'}') {
            return Ok(index + 1);
        }
        if !first {
            if bytes.get(index) != Some(&b',') {
                return Err(corrupt("Iceberg document manifest object is malformed"));
            }
            index = skip_json_whitespace(bytes, index + 1);
        }
        if bytes.get(index) != Some(&b'"') {
            return Err(corrupt("Iceberg document manifest object key is malformed"));
        }
        let key_start = index + 1;
        let key_end = json_string_end(bytes, key_start).ok_or_else(|| {
            corrupt("Iceberg document manifest contains an unterminated object key")
        })?;
        index = skip_json_whitespace(bytes, key_end + 1);
        if bytes.get(index) != Some(&b':') {
            return Err(corrupt("Iceberg document manifest object key has no value"));
        }
        let value_start = skip_json_whitespace(bytes, index + 1);
        let value_end = json_value_end(bytes, value_start)?;
        visitor(&bytes[key_start..key_end], value_start, value_end)?;
        index = value_end;
        first = false;
    }
}

fn visit_json_array(
    bytes: &[u8],
    start: usize,
    mut visitor: impl FnMut(usize, usize) -> Result<(), ConnectorError>,
) -> Result<usize, ConnectorError> {
    if bytes.get(start) != Some(&b'[') {
        return Err(corrupt("Iceberg document manifest expected a JSON array"));
    }
    let mut index = start + 1;
    let mut first = true;
    loop {
        index = skip_json_whitespace(bytes, index);
        if bytes.get(index) == Some(&b']') {
            return Ok(index + 1);
        }
        if !first {
            if bytes.get(index) != Some(&b',') {
                return Err(corrupt("Iceberg document manifest array is malformed"));
            }
            index = skip_json_whitespace(bytes, index + 1);
        }
        let value_end = json_value_end(bytes, index)?;
        visitor(index, value_end)?;
        index = value_end;
        first = false;
    }
}

fn json_value_end(bytes: &[u8], start: usize) -> Result<usize, ConnectorError> {
    match bytes.get(start) {
        Some(b'"') => json_string_end(bytes, start + 1)
            .map(|end| end + 1)
            .ok_or_else(|| corrupt("Iceberg document manifest contains an unterminated string")),
        Some(b'{') | Some(b'[') => composite_json_value_end(bytes, start),
        Some(_) => {
            let mut index = start;
            while bytes.get(index).is_some_and(|byte| {
                !byte.is_ascii_whitespace() && !matches!(*byte, b',' | b'}' | b']')
            }) {
                index += 1;
            }
            if index == start {
                Err(corrupt("Iceberg document manifest value is malformed"))
            } else {
                Ok(index)
            }
        }
        None => Err(corrupt("Iceberg document manifest value is missing")),
    }
}

fn composite_json_value_end(bytes: &[u8], start: usize) -> Result<usize, ConnectorError> {
    const MAX_JSON_DEPTH: usize = 64;
    let mut stack = [0u8; MAX_JSON_DEPTH];
    stack[0] = bytes[start];
    let mut depth = 1usize;
    let mut index = start + 1;
    let mut in_string = false;
    let mut escaped = false;
    while let Some(byte) = bytes.get(index) {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        match *byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                if depth == stack.len() {
                    return Err(exhausted("Iceberg document manifest depth overflowed"));
                }
                stack[depth] = *byte;
                depth += 1;
            }
            b'}' | b']' => {
                let expected = match stack[depth - 1] {
                    b'{' => b'}',
                    b'[' => b']',
                    _ => unreachable!("JSON stack contains only composite openers"),
                };
                if *byte != expected {
                    return Err(corrupt(
                        "Iceberg document manifest delimiters are malformed",
                    ));
                }
                depth -= 1;
                if depth == 0 {
                    return Ok(index + 1);
                }
            }
            _ => {}
        }
        index += 1;
    }
    Err(corrupt(
        "Iceberg document manifest composite is unterminated",
    ))
}

fn skip_json_whitespace(bytes: &[u8], mut index: usize) -> usize {
    while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
        index += 1;
    }
    index
}

/// Reject excessive nesting before serde is allowed to allocate a decoded
/// representation. Delimiters inside JSON strings are deliberately ignored.
pub(crate) fn preflight_decode(
    bytes: &[u8],
    limits: ConnectorDocumentStorageLimits,
    subject: &str,
) -> Result<(), ConnectorError> {
    let structural_nodes = validate_json_structure(bytes, limits.max_structure_depth(), subject)?;
    // JSON strings and byte vectors cannot decode to more payload bytes than
    // the source. The second input-sized allowance covers serde's source-side
    // scratch/copies; every object or array receives a deliberately generous
    // fixed header allowance, including Vec capacity growth and enum wrappers.
    const MAX_DECODED_STRUCTURE_BYTES: usize = 512;
    let required_working_set = bytes
        .len()
        .checked_mul(2)
        .and_then(|bytes| {
            structural_nodes
                .checked_mul(MAX_DECODED_STRUCTURE_BYTES)
                .and_then(|headers| bytes.checked_add(headers))
        })
        .ok_or_else(|| exhausted(format!("{subject} decode working-set overflowed")))?;
    if required_working_set > limits.max_decode_working_set_bytes() {
        return Err(exhausted(format!(
            "{subject} exceeds the caller decode working-set budget"
        )));
    }
    Ok(())
}

fn validate_json_structure(
    bytes: &[u8],
    max_depth: usize,
    subject: &str,
) -> Result<usize, ConnectorError> {
    let mut depth = 0usize;
    let mut structural_nodes = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match *byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| exhausted("Iceberg document manifest depth overflowed"))?;
                if depth > max_depth {
                    return Err(exhausted(format!(
                        "{subject} exceeds the caller structure-depth budget"
                    )));
                }
                structural_nodes = structural_nodes.checked_add(1).ok_or_else(|| {
                    exhausted(format!("{subject} structural item count overflowed"))
                })?;
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(structural_nodes)
}

pub(crate) fn validate_manifest(
    manifest: &IcebergDocumentManifestV1,
) -> Result<(), ConnectorError> {
    if manifest.version != DOCUMENT_MANIFEST_VERSION
        || manifest.documents.is_empty()
        || manifest.documents.len() > MAX_CONNECTOR_DOCUMENTS
    {
        return Err(corrupt(
            "Iceberg document manifest has an unsupported version or item count",
        ));
    }
    let mut identities = std::collections::HashSet::with_capacity(manifest.documents.len());
    for document in &manifest.documents {
        if document.version != DOCUMENT_ENVELOPE_VERSION
            || !identities.insert((
                document.owner.as_str(),
                document.name.as_str(),
                document.revision,
            ))
        {
            return Err(corrupt(
                "Iceberg document manifest contains an invalid or duplicate envelope",
            ));
        }
        if matches!(
            document.attachment,
            IcebergDocumentAttachmentV1::CommitOutput
        ) {
            let mut unresolved = document.clone();
            unresolved.attachment = IcebergDocumentAttachmentV1::TableMetadata;
            stored_document(&unresolved)?;
        } else {
            stored_document(document)?;
        }
    }
    Ok(())
}

pub(crate) fn stored_document(
    envelope: &IcebergDocumentEnvelopeV1,
) -> Result<ConnectorStoredDocument, ConnectorError> {
    let owner = ConnectorDocumentOwner::parse(&envelope.owner)?;
    let name = ConnectorDocumentName::parse(&envelope.name)?;
    let id = ConnectorDocumentId::new(
        owner,
        name,
        ConnectorDocumentRevision::from_bytes(envelope.revision),
    );
    let format = ConnectorDocumentFormat::try_new(
        &envelope.format_owner,
        &envelope.format_name,
        envelope.format_version,
    )?;
    let references = envelope
        .references
        .iter()
        .map(|reference| {
            ConnectorDocumentReference::try_new(
                &reference.relationship,
                ConnectorDocumentId::new(
                    ConnectorDocumentOwner::parse(&reference.owner)?,
                    ConnectorDocumentName::parse(&reference.name)?,
                    ConnectorDocumentRevision::from_bytes(reference.revision),
                ),
            )
        })
        .collect::<Result<Vec<_>, ConnectorError>>()?;
    let attachment = match &envelope.attachment {
        IcebergDocumentAttachmentV1::TableMetadata => {
            ConnectorStoredDocumentAttachment::TableMetadata
        }
        IcebergDocumentAttachmentV1::ExactOutput {
            committed_version,
            snapshot_id,
        } => ConnectorStoredDocumentAttachment::ExactOutput(ConnectorCommittedVersion::try_new(
            Bytes::copy_from_slice(committed_version),
            *snapshot_id,
        )?),
        IcebergDocumentAttachmentV1::CommitOutput => {
            return Err(corrupt(
                "unresolved commit-output attachment cannot appear in stored metadata",
            ));
        }
    };
    let carrier = match &envelope.carrier {
        IcebergDocumentCarrierV1::Available { content } => {
            ConnectorDocumentCarrier::AvailableContent(Bytes::copy_from_slice(content))
        }
        IcebergDocumentCarrierV1::Deferred { location } => {
            ConnectorDocumentCarrier::DeferredContent(ConnectorDeferredDocumentHandle::try_new(
                Bytes::copy_from_slice(location.as_bytes()),
            )?)
        }
    };
    let encoded_len = usize::try_from(envelope.encoded_len)
        .map_err(|_| exhausted("Iceberg document length does not fit this process"))?;
    ConnectorStoredDocument::try_new(id, format, encoded_len, references, attachment, carrier)
        .map_err(|error| {
            if error.kind() == ConnectorErrorKind::ResourceExhausted {
                error
            } else {
                corrupt(format!("invalid Iceberg document envelope: {error}"))
            }
        })
}

pub(crate) fn document_attachment(
    attachment: &ConnectorDocumentAttachment,
) -> IcebergDocumentAttachmentV1 {
    match attachment {
        ConnectorDocumentAttachment::TableMetadata => IcebergDocumentAttachmentV1::TableMetadata,
        ConnectorDocumentAttachment::ExactOutput(version) => {
            IcebergDocumentAttachmentV1::ExactOutput {
                committed_version: version.payload().to_vec(),
                snapshot_id: version.snapshot_id(),
            }
        }
        ConnectorDocumentAttachment::CommitOutput => IcebergDocumentAttachmentV1::CommitOutput,
    }
}

pub(crate) fn document_references(
    references: &[ConnectorDocumentReference],
) -> Vec<IcebergDocumentReferenceV1> {
    references
        .iter()
        .map(|reference| IcebergDocumentReferenceV1 {
            relationship: reference.relationship().to_string(),
            owner: reference.target().owner().as_str().to_string(),
            name: reference.target().name().as_str().to_string(),
            revision: reference.target().revision().to_bytes(),
        })
        .collect()
}

pub(crate) fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

pub(crate) fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}

pub(crate) fn exhausted(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::ResourceExhausted, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(content: Vec<u8>) -> IcebergDocumentEnvelopeV1 {
        IcebergDocumentEnvelopeV1 {
            version: DOCUMENT_ENVELOPE_VERSION,
            owner: "novarocks.mv".to_string(),
            name: "definition".to_string(),
            format_owner: "novarocks.mv".to_string(),
            format_name: "definition".to_string(),
            format_version: 77,
            revision: ConnectorDocumentRevision::for_content(&content).to_bytes(),
            encoded_len: content.len() as u64,
            references: Vec::new(),
            attachment: IcebergDocumentAttachmentV1::TableMetadata,
            carrier: IcebergDocumentCarrierV1::Available { content },
        }
    }

    #[test]
    fn opaque_bytes_round_trip_without_domain_version_interpretation() {
        let content = vec![0, 255, 17, 0, 99, 128];
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![envelope(content.clone())],
        };
        let encoded = encode_document_manifest(&manifest).expect("encode");
        let decoded = decode_document_manifest(&encoded).expect("decode");
        assert_eq!(decoded, manifest);
        let stored = stored_document(&decoded.documents[0]).expect("stored");
        assert_eq!(stored.format().version(), 77);
        assert!(matches!(
            stored.carrier(),
            ConnectorDocumentCarrier::AvailableContent(bytes) if bytes.as_ref() == content
        ));
    }

    #[test]
    fn length_or_revision_corruption_is_not_reported_as_unavailable() {
        let mut invalid_length = envelope(vec![1, 2, 3]);
        invalid_length.encoded_len += 1;
        let error = stored_document(&invalid_length).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);

        let mut invalid_revision = envelope(vec![1, 2, 3]);
        invalid_revision.revision = [7; 32];
        let error = stored_document(&invalid_revision).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn unresolved_commit_output_cannot_be_projected_as_stored_metadata() {
        let mut unresolved = envelope(vec![1, 2, 3]);
        unresolved.attachment = IcebergDocumentAttachmentV1::CommitOutput;
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![unresolved.clone()],
        };
        encode_document_manifest(&manifest).expect("pre-commit carrier may be prepared");
        let error = stored_document(&unresolved).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn caller_decode_working_set_is_rejected_before_manifest_decode() {
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![envelope(vec![1, 2, 3])],
        };
        let encoded = encode_document_manifest(&manifest).unwrap();
        let limits =
            ConnectorDocumentStorageLimits::try_new(16, 16, encoded.len() - 1, 4, 4, 64).unwrap();
        assert_eq!(
            decode_document_manifest_with_limits(&encoded, limits)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted,
        );
    }

    #[test]
    fn caller_structure_depth_is_rejected_before_manifest_decode() {
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![envelope(vec![1, 2, 3])],
        };
        let encoded = encode_document_manifest(&manifest).unwrap();
        let limits = ConnectorDocumentStorageLimits::try_new(16, 16, 1024 * 1024, 4, 4, 1).unwrap();
        assert_eq!(
            decode_document_manifest_with_limits(&encoded, limits)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted,
        );
    }

    #[test]
    fn caller_document_count_is_rejected_before_dto_allocation() {
        let encoded = br#"{"version":1,"documents":[{},{}]}"#;
        let limits =
            ConnectorDocumentStorageLimits::try_new(16, 16, 1024 * 1024, 1, 4, 64).unwrap();
        assert_eq!(
            decode_document_manifest_with_limits(encoded, limits)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted,
        );
    }

    #[test]
    fn caller_reference_count_is_rejected_before_dto_allocation() {
        let encoded = br#"{"version":1,"documents":[{"references":[{},{}]}]}"#;
        let limits =
            ConnectorDocumentStorageLimits::try_new(16, 16, 1024 * 1024, 4, 1, 64).unwrap();
        assert_eq!(
            decode_document_manifest_with_limits(encoded, limits)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted,
        );
    }

    #[test]
    fn caller_encoded_length_is_rejected_before_dto_allocation() {
        let encoded = br#"{"version":1,"documents":[{"\u0065ncoded_len":17}]}"#;
        let limits =
            ConnectorDocumentStorageLimits::try_new(16, 64, 1024 * 1024, 4, 4, 64).unwrap();
        assert_eq!(
            decode_document_manifest_with_limits(encoded, limits)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted,
        );
    }

    #[test]
    fn caller_inline_content_limit_is_rejected_before_vec_allocation() {
        let content = vec![7; 17];
        let mut oversized = envelope(content);
        oversized.encoded_len = 16;
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![oversized],
        };
        let encoded = serde_json::to_string(&manifest)
            .unwrap()
            .replace("\"content\"", "\"\\u0063ontent\"");
        let limits =
            ConnectorDocumentStorageLimits::try_new(16, 64, 1024 * 1024, 4, 4, 64).unwrap();
        assert_eq!(
            decode_document_manifest_with_limits(encoded.as_bytes(), limits)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted,
        );
    }

    #[test]
    fn wrong_path_budget_key_names_remain_corrupt_data() {
        let manifest = IcebergDocumentManifestV1 {
            version: DOCUMENT_MANIFEST_VERSION,
            documents: vec![envelope(vec![1, 2, 3])],
        };
        let base = serde_json::to_value(manifest).unwrap();
        let oversized = serde_json::Value::Array(vec![serde_json::Value::from(7); 17]);

        let mut top_level = base.clone();
        top_level
            .as_object_mut()
            .unwrap()
            .insert("content".to_string(), oversized.clone());

        let mut attachment = base.clone();
        attachment["documents"][0]["attachment"]
            .as_object_mut()
            .unwrap()
            .insert("content".to_string(), oversized);

        let mut carrier = base;
        carrier["documents"][0]["carrier"]
            .as_object_mut()
            .unwrap()
            .insert(
                "references".to_string(),
                serde_json::Value::Array(vec![serde_json::json!({}); 5]),
            );

        let limits =
            ConnectorDocumentStorageLimits::try_new(16, 64, 1024 * 1024, 4, 4, 64).unwrap();
        for (path, wrong_path) in [
            ("top-level", top_level),
            ("attachment", attachment),
            ("carrier", carrier),
        ] {
            let encoded = serde_json::to_vec(&wrong_path).unwrap();
            match decode_document_manifest_with_limits(&encoded, limits) {
                Err(error) => assert_eq!(error.kind(), ConnectorErrorKind::CorruptData, "{path}"),
                Ok(_) => panic!("{path} wrong-path key was accepted"),
            }
        }
    }
}
