// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};

use novarocks_spi::connector::{
    ConnectorDocumentId, ConnectorError, ConnectorErrorKind, MAX_CONNECTOR_DOCUMENT_REFERENCES,
};

use super::envelope::IcebergDocumentManifestV1;

pub(crate) fn reachable_document_ids(
    manifest: &IcebergDocumentManifestV1,
    roots: impl IntoIterator<Item = ConnectorDocumentId>,
) -> Result<HashSet<ConnectorDocumentId>, ConnectorError> {
    let documents = manifest
        .documents
        .iter()
        .map(super::codec::stored_document)
        .collect::<Result<Vec<_>, _>>()?;
    let by_id = documents
        .iter()
        .map(|document| (document.id().clone(), document))
        .collect::<HashMap<_, _>>();
    let mut reachable = HashSet::new();
    let mut pending = roots.into_iter().collect::<Vec<_>>();
    let mut expanded_edges = 0usize;
    while let Some(id) = pending.pop() {
        if !reachable.insert(id.clone()) {
            continue;
        }
        let document = by_id.get(&id).ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::CorruptData,
                "Iceberg document retention root or edge references a missing envelope",
            )
        })?;
        expanded_edges = expanded_edges
            .checked_add(document.references().len())
            .ok_or_else(|| {
                ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg document retention edge accounting overflowed",
                )
            })?;
        if expanded_edges > MAX_CONNECTOR_DOCUMENT_REFERENCES {
            return Err(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg document retention graph exceeds its edge budget",
            ));
        }
        pending.extend(
            document
                .references()
                .iter()
                .map(|reference| reference.target().clone()),
        );
    }
    Ok(reachable)
}
