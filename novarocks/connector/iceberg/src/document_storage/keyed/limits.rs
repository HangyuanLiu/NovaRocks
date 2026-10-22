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

use super::{
    envelope::encode_version,
    keys::{PREFIX, record_key},
    record::encode_record,
};
use novarocks_spi::connector::{
    ConnectorDocumentVersion, ConnectorError, ConnectorErrorKind, ConnectorSnapshotRecord,
    KeyedDocumentLimits,
};
use std::collections::HashMap;

pub(crate) fn private_property_bytes(properties: &HashMap<String, String>) -> usize {
    properties
        .iter()
        .filter(|(key, _)| key.starts_with(PREFIX))
        .fold(0usize, |sum, (key, value)| {
            sum.saturating_add(key.len()).saturating_add(value.len())
        })
}
fn check(
    category: &str,
    name: &str,
    size: usize,
    limit: usize,
    retained: Option<usize>,
) -> Result<(), ConnectorError> {
    if size <= limit {
        return Ok(());
    }
    Err(ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        format!(
            "document {category} {name} encoded size {size} exceeds limit {limit}{}",
            retained
                .map(|n| format!("; retained snapshot references occupy {n} bytes"))
                .unwrap_or_default()
        ),
    ))
}
pub(crate) fn check_version(
    version: &ConnectorDocumentVersion,
    limits: KeyedDocumentLimits,
) -> Result<(), ConnectorError> {
    check(
        "version",
        version.name(),
        encode_version(version)?.len(),
        limits.version_value_bytes,
        None,
    )
}
/// Call only when a new version, slot or marker is written. Ordinary publication
/// of an existing binding must not acquire a history-dependent admission gate.
pub(crate) fn check_private_properties(
    properties: &HashMap<String, String>,
    retained_bytes: Option<usize>,
    limits: KeyedDocumentLimits,
) -> Result<(), ConnectorError> {
    check(
        "private properties",
        "table",
        private_property_bytes(properties),
        limits.private_property_bytes,
        retained_bytes,
    )
}
pub(crate) fn record_bytes(record: &ConnectorSnapshotRecord) -> Result<usize, ConnectorError> {
    Ok(record_key(&record.owner, &record.name)
        .len()
        .saturating_add(encode_record(record)?.len()))
}
pub(crate) fn check_record(
    record: &ConnectorSnapshotRecord,
    limits: KeyedDocumentLimits,
) -> Result<(), ConnectorError> {
    check(
        "snapshot record",
        &record.name,
        record_bytes(record)?,
        limits.record_bytes,
        None,
    )
}
/// A definition owner can budget a record using its declared maximum payload
/// length without allocating a dummy payload. Base64url has no padding.
#[cfg(test)]
pub(crate) fn check_record_structure(
    owner: &str,
    name: &str,
    has_binding: bool,
    max_payload_bytes: usize,
    limits: KeyedDocumentLimits,
) -> Result<(), ConnectorError> {
    let b64 =
        (max_payload_bytes / 3)
            .saturating_mul(4)
            .saturating_add(match max_payload_bytes % 3 {
                0 => 0,
                1 => 2,
                _ => 3,
            });
    let bytes = record_key(owner, name)
        .len()
        .saturating_add(3)
        .saturating_add(if has_binding { 64 } else { 1 })
        .saturating_add(b64);
    check("snapshot record", name, bytes, limits.record_bytes, None)
}
