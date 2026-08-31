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
use std::str::FromStr;

use novarocks_spi::connector::LakePublicationId;
use uuid::Uuid;

use crate::iceberg::spec::{Snapshot, SnapshotReference, SnapshotRetention};
use crate::iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};

pub const MV_PUBLICATION_ID_PROP: &str = "novarocks.mv.publication_id";
pub const MV_PUBLICATION_STAGING_REF_PREFIX: &str = "__novarocks_mv_publication_";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MvPublicationSnapshotMarker {
    pub publication_id: LakePublicationId,
}

impl MvPublicationSnapshotMarker {
    pub fn to_summary_properties(&self) -> BTreeMap<String, String> {
        BTreeMap::from([(
            MV_PUBLICATION_ID_PROP.to_string(),
            self.publication_id.to_string(),
        )])
    }
}

pub fn snapshot_matches_publication_marker(
    snapshot: &Snapshot,
    marker: &MvPublicationSnapshotMarker,
) -> bool {
    snapshot
        .summary()
        .additional_properties
        .get(MV_PUBLICATION_ID_PROP)
        .and_then(|value| LakePublicationId::from_str(value).ok())
        == Some(marker.publication_id)
}

fn ensure_staging_ref_is_branch(
    staging_branch: &str,
    staging_ref: &SnapshotReference,
) -> Result<(), String> {
    if staging_ref.is_branch() {
        return Ok(());
    }
    Err(format!(
        "iceberg mv publish: staging ref {staging_branch} is a tag, expected branch"
    ))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshPublishPlan {
    pub namespace: String,
    pub table: String,
    pub target_table_uuid: Uuid,
    pub staging_branch: String,
    pub expected_main_snapshot_id: Option<i64>,
    pub staging_snapshot_id: i64,
    pub marker: MvPublicationSnapshotMarker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshPublishOutcome {
    pub published_snapshot_id: i64,
}

pub async fn publish_staging_branch_to_main(
    catalog: &dyn Catalog,
    loaded: &crate::iceberg::table::Table,
    plan: &MvRefreshPublishPlan,
) -> Result<MvRefreshPublishOutcome, String> {
    let expected_staging_branch = format!(
        "{MV_PUBLICATION_STAGING_REF_PREFIX}{}",
        plan.marker.publication_id
    );
    if plan.staging_branch != expected_staging_branch {
        return Err(
            "iceberg mv publish: staging ref does not match the exact publication identity"
                .to_string(),
        );
    }
    let ident = TableIdent::from_strs([plan.namespace.as_str(), plan.table.as_str()])
        .map_err(|e| format!("iceberg mv publish: invalid table identifier: {e}"))?;
    if loaded.identifier() != &ident {
        return Err(
            "iceberg mv publish: request-scoped table identity does not match publish plan"
                .to_string(),
        );
    }
    let metadata = loaded.metadata();
    let main_snapshot = metadata
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());
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
    if !snapshot_matches_publication_marker(staging_snapshot, &plan.marker) {
        return Err(format!(
            "iceberg mv publish: staging snapshot {} marker mismatch",
            plan.staging_snapshot_id
        ));
    }
    catalog
        .update_table(build_publish_commit(ident, plan))
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
                uuid: plan.target_table_uuid,
            },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::iceberg::spec::{Operation, Summary};

    fn plan() -> MvRefreshPublishPlan {
        MvRefreshPublishPlan {
            namespace: "db".to_string(),
            table: "mv".to_string(),
            target_table_uuid: Uuid::from_u128(0x1234),
            staging_branch: "__novarocks_mv_publication_01890f3c-4e70-7cc0-8000-000000000077"
                .to_string(),
            expected_main_snapshot_id: Some(100),
            staging_snapshot_id: 300,
            marker: MvPublicationSnapshotMarker {
                publication_id: LakePublicationId::try_from_uuid(
                    Uuid::parse_str("01890f3c-4e70-7cc0-8000-000000000077").unwrap(),
                )
                .unwrap(),
            },
        }
    }

    #[test]
    fn publish_commit_uses_uuid_main_and_staging_cas_only() {
        let plan = plan();
        let mut commit = build_publish_commit(TableIdent::from_strs(["db", "mv"]).unwrap(), &plan);
        let requirements = commit.take_requirements();
        assert_eq!(requirements.len(), 3);
        assert!(requirements.contains(&TableRequirement::UuidMatch {
            uuid: plan.target_table_uuid,
        }));
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: Some(100),
            })
        );
        assert!(
            requirements.contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: plan.staging_branch.clone(),
                snapshot_id: Some(300),
            })
        );
    }

    #[test]
    fn marker_match_requires_one_valid_v7_publication_id() {
        let plan = plan();
        let summary = Summary {
            operation: Operation::Append,
            additional_properties: plan.marker.to_summary_properties().into_iter().collect(),
        };
        let snapshot = Snapshot::builder()
            .with_snapshot_id(300)
            .with_sequence_number(1)
            .with_timestamp_ms(1)
            .with_manifest_list("file:/tmp/manifest-list.avro".to_string())
            .with_summary(summary)
            .with_schema_id(0)
            .build();
        assert!(snapshot_matches_publication_marker(&snapshot, &plan.marker));
    }
}
