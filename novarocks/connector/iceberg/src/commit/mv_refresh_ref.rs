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

use std::collections::BTreeMap;

use novarocks_spi::connector::ConnectorMvPublicationPermit;
use uuid::Uuid;

use crate::iceberg::spec::{Snapshot, SnapshotReference, SnapshotRetention};
use crate::iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};

use super::mv_provenance::{MvProvenanceV2, MvPublicationV2Identity};
use super::mv_publication_fence::{
    MV_PUBLICATION_FENCE_REF, MvPublicationError, ObservedFence, observe_fence,
};

pub const MV_REFRESH_ID_PROP: &str = "novarocks.mv.refresh_id";
pub const MV_ID_PROP: &str = "novarocks.mv.id";
pub const MV_REFRESH_TOKEN_PROP: &str = "novarocks.mv.refresh_token";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshSnapshotMarker {
    pub refresh_id: i64,
    pub mv_id: i64,
    pub token: String,
}

impl MvRefreshSnapshotMarker {
    pub fn to_summary_properties(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (MV_REFRESH_ID_PROP.to_string(), self.refresh_id.to_string()),
            (MV_ID_PROP.to_string(), self.mv_id.to_string()),
            (MV_REFRESH_TOKEN_PROP.to_string(), self.token.clone()),
        ])
    }
}

pub fn snapshot_matches_refresh_marker(
    snapshot: &Snapshot,
    marker: &MvRefreshSnapshotMarker,
) -> bool {
    let props = &snapshot.summary().additional_properties;
    props
        .get(MV_REFRESH_ID_PROP)
        .and_then(|value| value.parse::<i64>().ok())
        == Some(marker.refresh_id)
        && props
            .get(MV_ID_PROP)
            .and_then(|value| value.parse::<i64>().ok())
            == Some(marker.mv_id)
        && props.get(MV_REFRESH_TOKEN_PROP).map(String::as_str) == Some(marker.token.as_str())
}

fn ensure_staging_ref_is_branch(
    staging_branch: &str,
    staging_ref: &SnapshotReference,
) -> Result<(), String> {
    if !staging_ref.is_branch() {
        return Err(format!(
            "iceberg mv publish: staging ref {staging_branch} is a tag, expected branch"
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshPublishPlan {
    pub namespace: String,
    pub table: String,
    pub staging_branch: String,
    pub expected_main_snapshot_id: Option<i64>,
    pub staging_snapshot_id: i64,
    pub marker: MvRefreshSnapshotMarker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshPublishOutcome {
    pub published_snapshot_id: i64,
}

pub async fn publish_staging_branch_to_main(
    catalog: &dyn Catalog,
    plan: &MvRefreshPublishPlan,
) -> Result<MvRefreshPublishOutcome, String> {
    let ident = TableIdent::from_strs([plan.namespace.as_str(), plan.table.as_str()])
        .map_err(|e| format!("iceberg mv publish: invalid table identifier: {e}"))?;
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| format!("iceberg mv publish: load table failed: {e}"))?;
    let metadata = table.metadata();
    let main_snapshot = metadata.current_snapshot().map(|s| s.snapshot_id());
    if main_snapshot != plan.expected_main_snapshot_id {
        return Err(format!(
            "iceberg mv publish: main snapshot mismatch for {}.{}: expected {:?}, current {:?}",
            plan.namespace, plan.table, plan.expected_main_snapshot_id, main_snapshot
        ));
    }
    let staging_ref = metadata.refs().get(&plan.staging_branch).ok_or_else(|| {
        format!(
            "iceberg mv publish: staging branch {} does not exist",
            plan.staging_branch
        )
    })?;
    ensure_staging_ref_is_branch(&plan.staging_branch, staging_ref)?;
    if staging_ref.snapshot_id != plan.staging_snapshot_id {
        return Err(format!(
            "iceberg mv publish: staging branch {} points to {}, expected {}",
            plan.staging_branch, staging_ref.snapshot_id, plan.staging_snapshot_id
        ));
    }
    let staging_snapshot = metadata
        .snapshot_by_id(plan.staging_snapshot_id)
        .ok_or_else(|| {
            format!(
                "iceberg mv publish: staging snapshot {} not found",
                plan.staging_snapshot_id
            )
        })?;
    if !snapshot_matches_refresh_marker(staging_snapshot, &plan.marker) {
        return Err(format!(
            "iceberg mv publish: staging snapshot {} marker mismatch",
            plan.staging_snapshot_id
        ));
    }

    let commit = build_publish_commit(ident, plan);
    catalog
        .update_table(commit)
        .await
        .map_err(|e| format!("iceberg mv publish: commit failed: {e}"))?;
    Ok(MvRefreshPublishOutcome {
        published_snapshot_id: plan.staging_snapshot_id,
    })
}

fn build_publish_commit(ident: TableIdent, plan: &MvRefreshPublishPlan) -> TableCommit {
    TableCommit::builder()
        .ident(ident)
        .updates(vec![TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            reference: crate::iceberg::spec::SnapshotReference {
                snapshot_id: plan.staging_snapshot_id,
                retention: crate::iceberg::spec::SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        }])
        .requirements(vec![
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: plan.expected_main_snapshot_id,
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: plan.staging_branch.clone(),
                snapshot_id: Some(plan.staging_snapshot_id),
            },
        ])
        .build()
}

/// Externally fenced (V2) publication plan.
///
/// Unlike [`MvRefreshPublishPlan`], identity here is the stable resource plus
/// the ownership generation carried by the permit, so the guard does not depend
/// on a numeric MV ID that a StateStore rebuild would reassign.
#[derive(Clone, Debug)]
pub struct MvRefreshPublishV2Plan {
    pub namespace: String,
    pub table: String,
    pub permit: ConnectorMvPublicationPermit,
    pub staging_branch: String,
    pub staging_snapshot_id: i64,
    pub expected_main_snapshot_id: Option<i64>,
    /// The exact fence snapshot this attempt's generation established.
    pub expected_fence_snapshot_id: i64,
}

/// The externally observed state a V2 publication is checked against.
///
/// Extracted as plain data so the guard below is decidable — and therefore
/// testable — without a live catalog.
#[derive(Clone, Debug)]
pub struct ObservedPublicationState {
    pub table_uuid: Uuid,
    pub main_snapshot_id: Option<i64>,
    /// `None` when the staging branch is absent.
    pub staging_snapshot_id: Option<i64>,
    pub staging_is_branch: bool,
    pub fence: Option<ObservedFence>,
    /// V2 identity carried by the staged snapshot, `None` when it has no V2
    /// provenance or the snapshot is missing.
    pub staged_identity: Option<MvPublicationV2Identity>,
}

/// Decides whether a staged result may be published, from observed state alone.
///
/// This encodes the same four facts the commit requires, plus the V2 marker
/// assertion. Pre-checking here turns a doomed commit into a definite
/// `KnownUncommitted` instead of an ambiguous failure; the commit requirements
/// remain the real linearization point.
pub fn decide_v2_publication(
    plan: &MvRefreshPublishV2Plan,
    observed: &ObservedPublicationState,
) -> Result<(), String> {
    let resource = plan.permit.resource();
    if observed.table_uuid != resource.target_table_uuid() {
        return Err(format!(
            "iceberg mv publish v2: table {}.{} has UUID {}, expected {}",
            plan.namespace,
            plan.table,
            observed.table_uuid,
            resource.target_table_uuid()
        ));
    }
    if observed.main_snapshot_id != plan.expected_main_snapshot_id {
        return Err(format!(
            "iceberg mv publish v2: main snapshot mismatch for {}.{}: expected {:?}, current {:?}",
            plan.namespace, plan.table, plan.expected_main_snapshot_id, observed.main_snapshot_id
        ));
    }
    let Some(staging_snapshot_id) = observed.staging_snapshot_id else {
        return Err(format!(
            "iceberg mv publish v2: staging branch {} does not exist",
            plan.staging_branch
        ));
    };
    if !observed.staging_is_branch {
        return Err(format!(
            "iceberg mv publish: staging ref {} is a tag, expected branch",
            plan.staging_branch
        ));
    }
    if staging_snapshot_id != plan.staging_snapshot_id {
        return Err(format!(
            "iceberg mv publish v2: staging branch {} points to {}, expected {}",
            plan.staging_branch, staging_snapshot_id, plan.staging_snapshot_id
        ));
    }

    let Some(fence) = &observed.fence else {
        return Err(format!(
            "iceberg mv publish v2: {MV_PUBLICATION_FENCE_REF} is absent, so no generation owns publication"
        ));
    };
    if fence.snapshot_id != plan.expected_fence_snapshot_id {
        return Err(format!(
            "iceberg mv publish v2: {MV_PUBLICATION_FENCE_REF} points to {}, expected {}; this generation has been superseded",
            fence.snapshot_id, plan.expected_fence_snapshot_id
        ));
    }
    if &fence.marker.generation()? != plan.permit.generation() {
        return Err(format!(
            "iceberg mv publish v2: {MV_PUBLICATION_FENCE_REF} snapshot {} names a different generation than the permit",
            fence.snapshot_id
        ));
    }
    if !fence.marker.matches_resource(resource) {
        return Err(format!(
            "iceberg mv publish v2: fence snapshot {} belongs to a different target",
            fence.snapshot_id
        ));
    }

    let Some(identity) = &observed.staged_identity else {
        return Err(format!(
            "iceberg mv publish v2: staging snapshot {} carries no v2 provenance",
            plan.staging_snapshot_id
        ));
    };
    if identity != &MvPublicationV2Identity::from_permit(&plan.permit) {
        return Err(format!(
            "iceberg mv publish v2: staging snapshot {} v2 identity does not match the permit",
            plan.staging_snapshot_id
        ));
    }
    Ok(())
}

/// Reads the publication-relevant state out of loaded table metadata.
pub fn observe_publication_state(
    metadata: &crate::iceberg::spec::TableMetadata,
    plan: &MvRefreshPublishV2Plan,
) -> Result<ObservedPublicationState, String> {
    let staging_ref = metadata.refs().get(&plan.staging_branch);
    let staged_identity = match metadata.snapshot_by_id(plan.staging_snapshot_id) {
        Some(snapshot) => {
            MvProvenanceV2::from_snapshot_summary(snapshot)?.map(|provenance| provenance.identity())
        }
        None => None,
    };
    Ok(ObservedPublicationState {
        table_uuid: metadata.uuid(),
        main_snapshot_id: metadata.current_snapshot().map(|s| s.snapshot_id()),
        staging_snapshot_id: staging_ref.map(|reference| reference.snapshot_id),
        staging_is_branch: staging_ref.is_none_or(SnapshotReference::is_branch),
        fence: observe_fence(metadata)?,
        staged_identity,
    })
}

/// Publishes a staged result under an established external fence.
///
/// Four facts must hold *in the commit itself*, not merely at pre-check time:
///
/// 1. the table UUID still names this fence domain,
/// 2. `main` is still the frozen expected snapshot,
/// 3. the staging branch still points at the frozen staged snapshot,
/// 4. the internal fence ref still points at the exact snapshot this
///    generation established.
///
/// (4) is what makes a superseded owner unable to publish: a takeover moves the
/// fence ref, so the older generation's commit fails even though its `main`
/// expectation is still satisfied. The V2 marker check adds a fifth, pre-commit
/// assertion that the staged snapshot was produced by *this* permit.
pub async fn publish_staging_branch_to_main_v2(
    catalog: &dyn Catalog,
    plan: &MvRefreshPublishV2Plan,
) -> Result<MvRefreshPublishOutcome, MvPublicationError> {
    let precondition = MvPublicationError::Precondition;
    let ident =
        TableIdent::from_strs([plan.namespace.as_str(), plan.table.as_str()]).map_err(|e| {
            precondition(format!(
                "iceberg mv publish v2: invalid table identifier: {e}"
            ))
        })?;
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| precondition(format!("iceberg mv publish v2: load table failed: {e}")))?;
    let metadata = table.metadata();

    let observed = observe_publication_state(metadata, plan).map_err(precondition)?;
    decide_v2_publication(plan, &observed).map_err(precondition)?;

    let commit = build_publish_commit_v2(ident, plan.permit.resource().target_table_uuid(), plan);
    catalog.update_table(commit).await.map_err(|e| {
        MvPublicationError::CommitUnknown(format!("iceberg mv publish v2: commit failed: {e}"))
    })?;
    Ok(MvRefreshPublishOutcome {
        published_snapshot_id: plan.staging_snapshot_id,
    })
}

fn build_publish_commit_v2(
    ident: TableIdent,
    target_table_uuid: Uuid,
    plan: &MvRefreshPublishV2Plan,
) -> TableCommit {
    TableCommit::builder()
        .ident(ident)
        .updates(vec![TableUpdate::SetSnapshotRef {
            ref_name: "main".to_string(),
            reference: SnapshotReference {
                snapshot_id: plan.staging_snapshot_id,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        }])
        .requirements(vec![
            TableRequirement::UuidMatch {
                uuid: target_table_uuid,
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: plan.expected_main_snapshot_id,
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: plan.staging_branch.clone(),
                snapshot_id: Some(plan.staging_snapshot_id),
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MV_PUBLICATION_FENCE_REF.to_string(),
                snapshot_id: Some(plan.expected_fence_snapshot_id),
            },
        ])
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::mv_provenance::{ProvenanceBase, RefreshTechnique};
    use crate::iceberg::spec::{Operation, SnapshotRetention, Summary};
    use novarocks_spi::connector::{
        ConnectorCommittedVersion, ConnectorMvPublicationFenceGeneration,
        ConnectorMvPublicationFenceReceipt, ConnectorMvRefreshAttemptId,
        ConnectorMvRefreshResourceIdentity, ConnectorProviderId,
    };

    #[test]
    fn marker_round_trips_through_snapshot_summary() {
        let marker = MvRefreshSnapshotMarker {
            refresh_id: 77,
            mv_id: 12,
            token: "token-77".to_string(),
        };
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: marker.to_summary_properties().into_iter().collect(),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(300)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:/tmp/manifest-list.avro".to_string())
            .with_summary(summary)
            .with_schema_id(0)
            .build();
        assert!(snapshot_matches_refresh_marker(&snapshot, &marker));
    }

    #[test]
    fn marker_rejects_missing_non_numeric_and_wrong_token_properties() {
        let marker = MvRefreshSnapshotMarker {
            refresh_id: 77,
            mv_id: 12,
            token: "token-77".to_string(),
        };
        let mut props = marker.to_summary_properties();
        props.remove(MV_REFRESH_ID_PROP);
        assert!(!snapshot_matches_refresh_marker(
            &snapshot_with_properties(props),
            &marker
        ));

        let mut props = marker.to_summary_properties();
        props.insert(MV_REFRESH_ID_PROP.to_string(), "not-a-number".to_string());
        assert!(!snapshot_matches_refresh_marker(
            &snapshot_with_properties(props),
            &marker
        ));

        let mut props = marker.to_summary_properties();
        props.insert(MV_REFRESH_TOKEN_PROP.to_string(), "other-token".to_string());
        assert!(!snapshot_matches_refresh_marker(
            &snapshot_with_properties(props),
            &marker
        ));
    }

    #[test]
    fn staging_ref_branch_check_rejects_tags() {
        let staging_ref = SnapshotReference {
            snapshot_id: 300,
            retention: SnapshotRetention::Tag {
                max_ref_age_ms: None,
            },
        };

        let err = ensure_staging_ref_is_branch("mv_refresh_77", &staging_ref).unwrap_err();
        assert_eq!(
            err,
            "iceberg mv publish: staging ref mv_refresh_77 is a tag, expected branch"
        );
    }

    #[test]
    fn publish_commit_requirements_guard_main_and_staging_refs() {
        let plan = MvRefreshPublishPlan {
            namespace: "db".to_string(),
            table: "tbl".to_string(),
            staging_branch: "mv_refresh_77".to_string(),
            expected_main_snapshot_id: Some(100),
            staging_snapshot_id: 300,
            marker: MvRefreshSnapshotMarker {
                refresh_id: 77,
                mv_id: 12,
                token: "token-77".to_string(),
            },
        };
        let ident = TableIdent::from_strs(["db", "tbl"]).unwrap();
        let mut commit = build_publish_commit(ident, &plan);
        let requirements = commit.take_requirements();

        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: Some(100),
            })
        );
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "mv_refresh_77".to_string(),
                snapshot_id: Some(300),
            })
        );
    }

    fn snapshot_with_properties(properties: BTreeMap<String, String>) -> Snapshot {
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: properties.into_iter().collect(),
        };
        Snapshot::builder()
            .with_snapshot_id(300)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:/tmp/manifest-list.avro".to_string())
            .with_summary(summary)
            .with_schema_id(0)
            .build()
    }

    fn v2_resource() -> ConnectorMvRefreshResourceIdentity {
        ConnectorMvRefreshResourceIdentity::try_new(
            ConnectorProviderId::parse("iceberg").unwrap(),
            Uuid::from_u128(0x1234),
        )
        .unwrap()
    }

    fn v2_generation(incarnation: u64, epoch: u64) -> ConnectorMvPublicationFenceGeneration {
        ConnectorMvPublicationFenceGeneration::try_new("cluster-a", incarnation, epoch, [7u8; 32])
            .unwrap()
    }

    fn v2_permit(incarnation: u64, epoch: u64) -> ConnectorMvPublicationPermit {
        let fence_version =
            ConnectorCommittedVersion::try_new(bytes::Bytes::from_static(b"fence"), Some(500))
                .unwrap();
        let receipt = ConnectorMvPublicationFenceReceipt::try_new(
            v2_resource(),
            v2_generation(incarnation, epoch),
            fence_version,
        )
        .unwrap();
        ConnectorMvPublicationPermit::try_new(ConnectorMvRefreshAttemptId::new(), receipt).unwrap()
    }

    fn v2_plan(permit: ConnectorMvPublicationPermit) -> MvRefreshPublishV2Plan {
        MvRefreshPublishV2Plan {
            namespace: "db".to_string(),
            table: "mv".to_string(),
            permit,
            staging_branch: "mv_refresh_77".to_string(),
            staging_snapshot_id: 300,
            expected_main_snapshot_id: Some(100),
            expected_fence_snapshot_id: 500,
        }
    }

    #[test]
    fn v2_publish_commit_requires_uuid_main_staging_and_exact_fence() {
        let plan = v2_plan(v2_permit(1, 1));
        let mut commit = build_publish_commit_v2(
            TableIdent::from_strs(["db", "mv"]).unwrap(),
            Uuid::from_u128(0x1234),
            &plan,
        );
        let requirements = commit.take_requirements();

        assert_eq!(
            requirements.len(),
            4,
            "publication must be guarded by exactly four facts"
        );
        assert!(requirements.contains(&TableRequirement::UuidMatch {
            uuid: Uuid::from_u128(0x1234)
        }));
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: Some(100),
            })
        );
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "mv_refresh_77".to_string(),
                snapshot_id: Some(300),
            })
        );
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: MV_PUBLICATION_FENCE_REF.to_string(),
                snapshot_id: Some(500),
            }),
            "the exact fence snapshot is the requirement that stops a superseded owner"
        );

        let updates = commit.take_updates();
        assert_eq!(updates.len(), 1, "publication only moves main");
        match &updates[0] {
            TableUpdate::SetSnapshotRef {
                ref_name,
                reference,
            } => {
                assert_eq!(ref_name, "main");
                assert_eq!(reference.snapshot_id, 300);
            }
            other => panic!("expected SetSnapshotRef, got {other:?}"),
        }
    }

    #[test]
    fn v2_provenance_identity_round_trips_and_detects_every_permit_field() {
        let permit = v2_permit(1, 1);
        let identity = MvPublicationV2Identity::from_permit(&permit);
        let record = MvProvenanceV2::new(
            &identity,
            RefreshTechnique::Incremental,
            vec![ProvenanceBase {
                table_fqn: "ice.sales.orders".to_string(),
                uuid: "uuid-orders".to_string(),
                from_snapshot: Some(100),
                to_snapshot: 200,
            }],
            "fp-abc".to_string(),
            42,
        );

        let props = record.to_summary_properties().unwrap();
        assert!(props.contains_key(super::super::mv_provenance::MV_PROVENANCE_V2_PROP));
        // V2 must not also write V1's authoritative marker keys.
        assert!(!props.contains_key(MV_REFRESH_ID_PROP));
        assert!(!props.contains_key(MV_ID_PROP));
        assert!(!props.contains_key(MV_REFRESH_TOKEN_PROP));

        let snapshot = snapshot_with_properties(props);
        let parsed = MvProvenanceV2::from_snapshot_summary(&snapshot)
            .unwrap()
            .unwrap();
        assert_eq!(parsed, record);
        assert_eq!(parsed.identity(), identity);

        // A different generation is a different identity, so a stale attempt's
        // staged snapshot cannot satisfy a newer permit's guard.
        assert_ne!(
            MvPublicationV2Identity::from_permit(&v2_permit(1, 2)),
            identity
        );
        // So is a different attempt under the same generation.
        assert_ne!(
            MvPublicationV2Identity::from_permit(&v2_permit(1, 1)),
            identity
        );
    }

    #[test]
    fn v1_marker_reading_still_works_and_ignores_v2_records() {
        let permit = v2_permit(1, 1);
        let v2 = MvProvenanceV2::new(
            &MvPublicationV2Identity::from_permit(&permit),
            RefreshTechnique::Full,
            vec![],
            "fp".to_string(),
            0,
        );
        let snapshot = snapshot_with_properties(v2.to_summary_properties().unwrap());

        // A V2 snapshot is not publishable through the V1 marker path: it never
        // claims a refresh_id/mv_id/token identity.
        let v1_marker = MvRefreshSnapshotMarker {
            refresh_id: 77,
            mv_id: 12,
            token: "token-77".to_string(),
        };
        assert!(!snapshot_matches_refresh_marker(&snapshot, &v1_marker));

        // And a V1 snapshot carries no V2 record, so the V2 guard rejects it.
        let v1_snapshot = snapshot_with_properties(v1_marker.to_summary_properties());
        assert_eq!(
            MvProvenanceV2::from_snapshot_summary(&v1_snapshot).unwrap(),
            None
        );
        assert!(snapshot_matches_refresh_marker(&v1_snapshot, &v1_marker));
    }

    fn observed_state(
        plan: &MvRefreshPublishV2Plan,
        fence_generation: &ConnectorMvPublicationFenceGeneration,
        fence_snapshot_id: i64,
    ) -> ObservedPublicationState {
        ObservedPublicationState {
            table_uuid: Uuid::from_u128(0x1234),
            main_snapshot_id: Some(100),
            staging_snapshot_id: Some(300),
            staging_is_branch: true,
            fence: Some(super::super::mv_publication_fence::ObservedFence {
                snapshot_id: fence_snapshot_id,
                marker: super::super::mv_publication_fence::MvPublicationFenceMarker::new(
                    &v2_resource(),
                    fence_generation,
                    [9; 16],
                ),
            }),
            staged_identity: Some(MvPublicationV2Identity::from_permit(&plan.permit)),
        }
    }

    #[test]
    fn v2_publication_succeeds_only_while_its_own_generation_owns_the_fence() {
        let permit = v2_permit(1, 1);
        let plan = v2_plan(permit.clone());
        let ours = v2_generation(1, 1);

        decide_v2_publication(&plan, &observed_state(&plan, &ours, 500)).unwrap();

        // Takeover linearized first: the fence ref moved, so this generation
        // cannot publish even though its frozen main expectation still holds.
        let taken_over = ObservedPublicationState {
            fence: Some(super::super::mv_publication_fence::ObservedFence {
                snapshot_id: 600,
                marker: super::super::mv_publication_fence::MvPublicationFenceMarker::new(
                    &v2_resource(),
                    &v2_generation(2, 1),
                    [10; 16],
                ),
            }),
            ..observed_state(&plan, &ours, 500)
        };
        let err = decide_v2_publication(&plan, &taken_over).unwrap_err();
        assert!(err.contains("has been superseded"), "{err}");
        assert_eq!(
            taken_over.main_snapshot_id, plan.expected_main_snapshot_id,
            "the point of this case is that main is still exactly what we froze"
        );

        // Same fence snapshot id, different generation: a fence ref that was
        // rebuilt by another owner must not be mistaken for ours.
        let impostor = observed_state(&plan, &v2_generation(1, 2), 500);
        let err = decide_v2_publication(&plan, &impostor).unwrap_err();
        assert!(
            err.contains("different generation than the permit"),
            "{err}"
        );
    }

    #[test]
    fn v2_publication_refuses_after_an_older_publication_already_advanced_main() {
        let plan = v2_plan(v2_permit(1, 1));
        let ours = v2_generation(1, 1);

        // The other owner's publication linearized first, so main is no longer
        // the snapshot we froze. The new owner must reconcile against committed
        // lake truth rather than publish over it.
        let advanced = ObservedPublicationState {
            main_snapshot_id: Some(250),
            ..observed_state(&plan, &ours, 500)
        };
        let err = decide_v2_publication(&plan, &advanced).unwrap_err();
        assert!(err.contains("main snapshot mismatch"), "{err}");
    }

    #[test]
    fn v2_publication_rejects_foreign_target_absent_fence_and_wrong_staged_identity() {
        let plan = v2_plan(v2_permit(1, 1));
        let ours = v2_generation(1, 1);

        // External DROP/recreate: same name, new table UUID, new fence domain.
        let recreated = ObservedPublicationState {
            table_uuid: Uuid::from_u128(0x9999),
            ..observed_state(&plan, &ours, 500)
        };
        assert!(
            decide_v2_publication(&plan, &recreated)
                .unwrap_err()
                .contains("expected"),
        );

        let unfenced = ObservedPublicationState {
            fence: None,
            ..observed_state(&plan, &ours, 500)
        };
        let err = decide_v2_publication(&plan, &unfenced).unwrap_err();
        assert!(err.contains("is absent"), "{err}");

        // A staged snapshot produced by a different attempt is not publishable
        // under this permit.
        let foreign_stage = ObservedPublicationState {
            staged_identity: Some(MvPublicationV2Identity::from_permit(&v2_permit(1, 1))),
            ..observed_state(&plan, &ours, 500)
        };
        let err = decide_v2_publication(&plan, &foreign_stage).unwrap_err();
        assert!(err.contains("does not match the permit"), "{err}");

        let no_provenance = ObservedPublicationState {
            staged_identity: None,
            ..observed_state(&plan, &ours, 500)
        };
        let err = decide_v2_publication(&plan, &no_provenance).unwrap_err();
        assert!(err.contains("carries no v2 provenance"), "{err}");

        let moved_stage = ObservedPublicationState {
            staging_snapshot_id: Some(301),
            ..observed_state(&plan, &ours, 500)
        };
        let err = decide_v2_publication(&plan, &moved_stage).unwrap_err();
        assert!(err.contains("points to 301"), "{err}");

        let tag_stage = ObservedPublicationState {
            staging_is_branch: false,
            ..observed_state(&plan, &ours, 500)
        };
        let err = decide_v2_publication(&plan, &tag_stage).unwrap_err();
        assert!(err.contains("is a tag"), "{err}");
    }

    #[test]
    fn v2_provenance_rejects_version_mismatch() {
        let err = MvProvenanceV2::from_json(
            r#"{"provenance_version":1,"resource_digest":"d","target_table_uuid":"u","cluster_digest":"c","control_plane_incarnation":1,"resource_epoch":1,"token_digest":"t","attempt_id":"a","permit_digest":"p","technique":"FULL","bases":[],"definition_fingerprint":"fp","rows":0}"#,
        )
        .unwrap_err();
        assert!(
            err.contains("unsupported MV provenance v2 version"),
            "{err}"
        );
    }
}
