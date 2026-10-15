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

//! Pure document reachability for an exact, caller-selected expiry base.

use super::view::reference_closure;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, DocumentIdentity, FullKeyedDocumentView,
};
use std::collections::BTreeSet;

/// Select property identities only. The lifecycle owner chooses snapshots and
/// the publisher owns the frozen all-ref request. This function deletes nothing.
pub(crate) fn deletable_versions(
    full: &FullKeyedDocumentView,
    removed: &BTreeSet<i64>,
) -> Result<BTreeSet<DocumentIdentity>, ConnectorError> {
    for (name, snapshot) in &full.refs {
        if removed.contains(snapshot) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("document expiry would remove observed ref {name} head {snapshot}"),
            ));
        }
    }
    if full
        .current
        .current_snapshot
        .is_some_and(|id| removed.contains(&id))
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "document expiry would remove the current snapshot",
        ));
    }
    let snapshots: BTreeSet<_> = full.snapshots.iter().map(|s| s.snapshot_id).collect();
    if let Some(snapshot) = removed.iter().find(|id| !snapshots.contains(id)) {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("document expiry selected unobserved snapshot {snapshot}"),
        ));
    }
    // Serving remains strict even though a historical missing link is a
    // diagnostic under the external rollback boundary.
    for record in &full.current.current_records {
        reference_closure(&full.current.versions, record.binding)?;
    }
    let mut live = BTreeSet::new();
    let mut pending: Vec<_> = full
        .current
        .roots
        .values()
        .map(|root| root.identity)
        .collect();
    for snapshot in &full.snapshots {
        if !removed.contains(&snapshot.snapshot_id) {
            pending.extend(snapshot.records.iter().filter_map(|record| record.binding));
        }
    }
    // Do not skip a whole dangling root or historical binding. Some of its
    // descendants can still exist and be shared by other retained semantics.
    // Missing nodes have no bytes to delete; every available reachable node is
    // retained. A set bounds traversal even for corrupt cyclic history.
    while let Some(id) = pending.pop() {
        if live.contains(&id) {
            continue;
        }
        if let Some(version) = full.current.versions.get(&id) {
            live.insert(id);
            pending.extend(version.references().values().copied());
        }
    }
    Ok(full
        .current
        .versions
        .keys()
        .filter(|id| !live.contains(id))
        .copied()
        .collect())
}

#[cfg(test)]
#[path = "retention_tests.rs"]
mod tests;
