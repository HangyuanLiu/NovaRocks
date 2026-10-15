// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Inline document updates contain only this channel's exact keys.
// Design: ADR-0178 (docs/adr/ADR-0178-inline-keyed-documents-and-snapshot-records.md)

use super::{envelope, keys, limits, record, view};
use crate::iceberg::TableUpdate;
use crate::iceberg::spec::TableMetadata;
use novarocks_spi::connector::{
    ConnectorDocumentVersion, ConnectorError, ConnectorErrorKind, DocumentIdentity, KeyedCreate,
    KeyedDocumentLimits, KeyedMarkerWrite, KeyedPublication,
};
use std::collections::{BTreeMap, HashMap};

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}
fn versions_into(
    versions: &[ConnectorDocumentVersion],
    available: &mut BTreeMap<DocumentIdentity, ConnectorDocumentVersion>,
    updates: &mut HashMap<String, String>,
) -> Result<(), ConnectorError> {
    for version in versions {
        if !available.contains_key(&version.identity()) {
            limits::check_version(version, KeyedDocumentLimits::default())?;
        }
        if let Some(previous) = available.insert(version.identity(), version.clone())
            && previous != *version
        {
            return Err(invalid("document identity has unequal semantic bytes"));
        }
        updates.insert(
            keys::version_key(version.identity()),
            envelope::encode_version(version)?,
        );
    }
    Ok(())
}
fn add_closure(
    available: &BTreeMap<DocumentIdentity, ConnectorDocumentVersion>,
    seeds: impl IntoIterator<Item = DocumentIdentity>,
    updates: &mut HashMap<String, String>,
) -> Result<(), ConnectorError> {
    for id in view::reference_closure(available, seeds)? {
        updates.insert(
            keys::version_key(id),
            envelope::encode_version(&available[&id])?,
        );
    }
    Ok(())
}

pub(crate) fn create_properties(
    documents: &KeyedCreate,
) -> Result<HashMap<String, String>, ConnectorError> {
    documents.validate()?;
    let mut properties = HashMap::new();
    let mut available = BTreeMap::new();
    versions_into(&documents.versions, &mut available, &mut properties)?;
    // Validate even unrooted versions: new semantic facts cannot introduce
    // a missing reference into a successful create.
    add_closure(&available, available.keys().copied(), &mut properties)?;
    for (name, value) in &documents.slots {
        properties.insert(
            keys::slot_key(name),
            envelope::encode_slot(name, value, None)?,
        );
    }
    add_closure(
        &available,
        documents.roots.values().copied(),
        &mut properties,
    )?;
    for (owner, id) in &documents.roots {
        properties.insert(keys::root_key(owner), id.to_hex());
    }
    limits::check_private_properties(&properties, 0, KeyedDocumentLimits::default())?;
    Ok(properties)
}

pub(crate) fn marker_properties(
    metadata: &TableMetadata,
    documents: &KeyedMarkerWrite,
) -> Result<HashMap<String, String>, ConnectorError> {
    documents.validate()?;
    view::parse_properties(metadata.properties())
        .map_err(super::super::observation::keyed_read_error)?;
    let properties = documents
        .markers
        .iter()
        .map(|(key, value)| Ok((keys::marker_key(key), envelope::encode_marker(key, value)?)))
        .collect::<Result<HashMap<_, _>, ConnectorError>>()?;
    let mut after = metadata.properties().clone();
    after.extend(properties.clone());
    limits::check_private_properties(
        &after,
        retained_version_bytes(metadata),
        KeyedDocumentLimits::default(),
    )?;
    Ok(properties)
}

/// This count is a visible-version diagnostic. Complete retained-snapshot
/// attribution is computed from an All observation only on a budget refusal.
pub(crate) fn retained_version_bytes(metadata: &TableMetadata) -> usize {
    metadata
        .properties()
        .iter()
        .filter(|(key, _)| key.starts_with("novarocks.doc.v2.rev."))
        .map(|(key, value)| key.len().saturating_add(value.len()))
        .sum()
}

#[derive(Debug)]
pub(crate) struct PublicationUpdates {
    pub summary: BTreeMap<String, String>,
    pub updates: Vec<TableUpdate>,
}
pub(crate) fn publication_updates(
    metadata: &TableMetadata,
    documents: &KeyedPublication,
) -> Result<PublicationUpdates, ConnectorError> {
    documents.validate()?;
    let parsed = view::parse_properties(metadata.properties())
        .map_err(super::super::observation::keyed_read_error)?;
    let has_new_versions = documents
        .versions
        .iter()
        .any(|version| !parsed.versions.contains_key(&version.identity()));
    let previous_records = metadata
        .current_snapshot()
        .map(|snapshot| {
            view::parse_snapshot_records(&snapshot.summary().additional_properties, true)
        })
        .transpose()
        .map_err(super::super::observation::keyed_read_error)?
        .unwrap_or_default();
    for record in &previous_records {
        view::reference_closure(&parsed.versions, record.binding)?;
    }
    let mut available = parsed.versions;
    let mut properties = HashMap::new();
    versions_into(&documents.versions, &mut available, &mut properties)?;
    // Every new version must be internally resolvable, even if it is not a root.
    add_closure(
        &available,
        documents.versions.iter().map(|v| v.identity()),
        &mut properties,
    )?;
    let mut roots: BTreeMap<_, _> = parsed
        .roots
        .into_iter()
        .filter(|(_, root)| !root.dangling)
        .map(|(owner, root)| (owner, root.identity))
        .collect();
    roots.extend(documents.roots.clone());
    let seeds = documents
        .records
        .iter()
        .filter_map(|record| record.binding)
        .chain(roots.values().copied());
    add_closure(&available, seeds, &mut properties)?;
    for (owner, id) in &documents.roots {
        properties.insert(keys::root_key(owner), id.to_hex());
    }
    for (key, value) in &documents.markers_set {
        properties.insert(keys::marker_key(key), envelope::encode_marker(key, value)?);
    }
    let mut summary = BTreeMap::new();
    for entry in &documents.records {
        let unchanged_structure = previous_records.iter().any(|previous| {
            previous.owner == entry.owner
                && previous.name == entry.name
                && previous.binding == entry.binding
        });
        if !unchanged_structure
            || entry
                .binding
                .is_some_and(|id| !metadata.properties().contains_key(&keys::version_key(id)))
        {
            limits::check_record(entry, KeyedDocumentLimits::default())?;
        }
        summary.insert(
            keys::record_key(&entry.owner, &entry.name),
            record::encode_record(entry)?,
        );
    }
    let removals: Vec<_> = documents
        .markers_remove
        .iter()
        .map(keys::marker_key)
        .collect();
    if has_new_versions || !documents.markers_set.is_empty() {
        let mut after = metadata.properties().clone();
        after.extend(properties.clone());
        for key in &removals {
            after.remove(key);
        }
        limits::check_private_properties(
            &after,
            retained_version_bytes(metadata),
            KeyedDocumentLimits::default(),
        )?;
    }
    let mut updates = Vec::new();
    if !properties.is_empty() {
        updates.push(TableUpdate::SetProperties {
            updates: properties,
        });
    }
    if !removals.is_empty() {
        updates.push(TableUpdate::RemoveProperties { removals });
    }
    Ok(PublicationUpdates { summary, updates })
}

pub(crate) fn reject_reserved_property(key: &str) -> Result<(), ConnectorError> {
    if key.starts_with(keys::PREFIX) || keys::is_legacy_key(key) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("property {key} is reserved for document storage"),
        ));
    }
    Ok(())
}
