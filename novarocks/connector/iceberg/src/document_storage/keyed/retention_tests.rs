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

use super::deletable_versions;
use crate::document_storage::keyed::{
    calculation_test_support::{Snapshot, document, full, properties, record},
    keys::root_key,
};
use bytes::Bytes;
use novarocks_spi::connector::*;
use std::collections::BTreeSet;

fn ids(versions: &[&ConnectorDocumentVersion]) -> BTreeSet<DocumentIdentity> {
    versions.iter().map(|v| v.identity()).collect()
}
fn layout(content: &'static [u8], d: &ConnectorDocumentVersion) -> ConnectorDocumentVersion {
    ConnectorDocumentVersion::try_new(
        "novarocks.test",
        "interpretation",
        ConnectorDocumentFormat::try_new("novarocks.test", "interpretation", 1).unwrap(),
        vec![("definition".into(), d.identity())],
        Bytes::from_static(content),
    )
    .unwrap()
}
fn binding(d: &ConnectorDocumentVersion, l: &ConnectorDocumentVersion) -> ConnectorDocumentVersion {
    document(
        "binding",
        vec![
            ("definition".into(), d.identity()),
            ("interpretation".into(), l.identity()),
        ],
    )
}
#[test]
fn shared_binding_lives_until_last_retained_snapshot_and_orphan_is_deleted() {
    let d = document("definition", vec![]);
    let b = document("binding", vec![("definition".into(), d.identity())]);
    let orphan = document("orphan", vec![]);
    let view = full(
        Some(30),
        vec![
            Snapshot::new(10, None).record(record(b"one", Some(b.identity()))),
            Snapshot::new(20, Some(10)).record(record(b"two", Some(b.identity()))),
            Snapshot::new(30, Some(20)),
        ],
        properties(&[d.clone(), b.clone(), orphan.clone()]),
        vec![],
    );
    assert_eq!(
        deletable_versions(&view, &BTreeSet::from([10])).unwrap(),
        ids(&[&orphan])
    );
    assert_eq!(
        deletable_versions(&view, &BTreeSet::from([10, 20])).unwrap(),
        ids(&[&d, &b, &orphan])
    );
}
#[test]
fn roots_preserve_binding_after_every_record_snapshot_expires() {
    let d = document("definition", vec![]);
    let b = document("binding", vec![("definition".into(), d.identity())]);
    let orphan = document("orphan", vec![]);
    let mut props = properties(&[d.clone(), b.clone(), orphan.clone()]);
    props.insert(root_key("novarocks.test"), b.identity().to_hex());
    let view = full(
        Some(30),
        vec![
            Snapshot::new(10, None).record(record(b"old", Some(b.identity()))),
            Snapshot::new(30, Some(10)),
        ],
        props,
        vec![],
    );
    assert_eq!(
        deletable_versions(&view, &BTreeSet::from([10])).unwrap(),
        ids(&[&orphan])
    );
}
#[test]
fn observed_ref_heads_cannot_be_removed_and_keep_their_documents() {
    let pinned = document("pinned", vec![]);
    let other = document("other", vec![]);
    let view = full(
        Some(30),
        vec![
            Snapshot::new(10, None).record(record(b"tagged", Some(pinned.identity()))),
            Snapshot::new(20, Some(10)).record(record(b"old", Some(other.identity()))),
            Snapshot::new(30, Some(20)),
        ],
        properties(&[pinned.clone(), other.clone()]),
        vec![("tagged", 10)],
    );
    let error = deletable_versions(&view, &BTreeSet::from([10])).unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    assert!(error.message().contains("tagged"));
    assert!(deletable_versions(&view, &BTreeSet::from([30])).is_err());
    assert_eq!(
        deletable_versions(&view, &BTreeSet::from([20])).unwrap(),
        ids(&[&other])
    );
}
#[test]
fn rebound_old_layout_is_live_even_when_its_first_snapshot_expires() {
    let d = document("definition", vec![]);
    let old = layout(b"old-layout", &d);
    let new = layout(b"new-layout", &d);
    let old_binding = binding(&d, &old);
    let new_binding = binding(&d, &new);
    let mut props = properties(&[
        d.clone(),
        old.clone(),
        new.clone(),
        old_binding.clone(),
        new_binding.clone(),
    ]);
    props.insert(root_key("novarocks.test"), old_binding.identity().to_hex());
    let view = full(
        Some(30),
        vec![
            Snapshot::new(10, None).record(record(b"old", Some(old_binding.identity()))),
            Snapshot::new(20, Some(10)).record(record(b"new", Some(new_binding.identity()))),
            Snapshot::new(30, Some(20)).record(record(b"old-again", Some(old_binding.identity()))),
        ],
        props,
        vec![],
    );
    assert_eq!(
        deletable_versions(&view, &BTreeSet::from([10, 20])).unwrap(),
        ids(&[&new, &new_binding])
    );
}
#[test]
fn dangling_root_preserves_available_parts_of_its_closure() {
    let missing = document("missing", vec![]);
    let available = document("available", vec![]);
    let orphan = document("orphan", vec![]);
    let broken = document(
        "binding",
        vec![
            ("a-missing".into(), missing.identity()),
            ("z-available".into(), available.identity()),
        ],
    );
    let mut props = properties(&[broken.clone(), available.clone(), orphan.clone()]);
    props.insert(root_key("novarocks.test"), broken.identity().to_hex());
    let view = full(Some(30), vec![Snapshot::new(30, Some(20))], props, vec![]);
    assert!(view.current.roots["novarocks.test"].dangling);
    assert_eq!(
        deletable_versions(&view, &BTreeSet::new()).unwrap(),
        ids(&[&orphan])
    );
}
#[test]
fn missing_historical_link_is_diagnostic_and_other_reachable_versions_stay() {
    let missing = document("missing", vec![]);
    let available = document("available", vec![]);
    let orphan = document("orphan", vec![]);
    let broken = document(
        "binding",
        vec![
            ("a-missing".into(), missing.identity()),
            ("z-available".into(), available.identity()),
        ],
    );
    let view = full(
        Some(30),
        vec![
            Snapshot::new(10, None).record(record(b"broken-history", Some(broken.identity()))),
            Snapshot::new(30, Some(10)),
        ],
        properties(&[broken.clone(), available.clone(), orphan.clone()]),
        vec![],
    );
    assert_eq!(view.diagnostics.len(), 1);
    assert_eq!(
        deletable_versions(&view, &BTreeSet::new()).unwrap(),
        ids(&[&orphan])
    );
    assert_eq!(
        deletable_versions(&view, &BTreeSet::from([10])).unwrap(),
        ids(&[&broken, &available, &orphan])
    );
}
#[test]
fn current_reference_closure_is_strict_even_during_expiry() {
    let d = document("definition", vec![]);
    let b = document("binding", vec![("definition".into(), d.identity())]);
    let mut view = full(
        Some(30),
        vec![Snapshot::new(30, None).record(record(b"current", Some(b.identity())))],
        properties(&[d.clone(), b]),
        vec![],
    );
    view.current.versions.remove(&d.identity());
    assert_eq!(
        deletable_versions(&view, &BTreeSet::new())
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::CorruptData
    );
}
#[test]
fn absent_snapshot_selection_is_rejected_without_authorizing_property_delete() {
    let orphan = document("orphan", vec![]);
    let view = full(
        Some(30),
        vec![Snapshot::new(30, None)],
        properties(&[orphan.clone()]),
        vec![],
    );
    assert_eq!(
        deletable_versions(&view, &BTreeSet::from([999]))
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert_eq!(
        deletable_versions(&view, &BTreeSet::new()).unwrap(),
        ids(&[&orphan])
    );
}

#[test]
fn snapshot_retained_byte_diagnostic_counts_shared_closure_once_and_excludes_root_only_versions() {
    let d = document("definition", vec![]);
    let b = document("binding", vec![("definition".into(), d.identity())]);
    let root_only = document("root-only", vec![]);
    let mut props = properties(&[d.clone(), b.clone(), root_only.clone()]);
    props.insert(root_key("novarocks.test"), root_only.identity().to_hex());
    let expected: usize = [d.identity(), b.identity()]
        .into_iter()
        .map(|id| {
            let key = crate::document_storage::keyed::keys::version_key(id);
            key.len() + props[&key].len()
        })
        .sum();
    let view = full(
        Some(20),
        vec![
            Snapshot::new(10, None).record(record(b"one", Some(b.identity()))),
            Snapshot::new(20, Some(10)).record(record(b"two", Some(b.identity()))),
        ],
        props,
        vec![],
    );
    let metadata = crate::document_storage::keyed::view::metadata_from_token(&view.token).unwrap();
    assert_eq!(
        super::snapshot_referenced_bytes(&metadata).unwrap(),
        expected
    );
}
