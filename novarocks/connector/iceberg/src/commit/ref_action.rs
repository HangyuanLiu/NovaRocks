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

//! Phase-1 metadata-only commit for `CREATE/DROP BRANCH|TAG`.
//!
//! Unlike the six data-commit modules, ref actions never produce a new
//! snapshot — they emit only `SetSnapshotRef` / `RemoveSnapshotRef`
//! `TableUpdate`s plus an `AssertRefSnapshotId` requirement.

#![allow(dead_code)]

use crate::commit::existing_commit::{ExistingCommitError, existing_table_commit};
use crate::iceberg::spec::{SnapshotReference, SnapshotRetention, TableMetadata};
use crate::iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct RefActionPlan {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
    pub action: RefAction,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RefAction {
    CreateBranch {
        name: String,
        snapshot_id: i64,
        replace: bool,
        if_not_exists: bool,
        expected_table_uuid: Option<Uuid>,
    },
    CreateTag {
        name: String,
        snapshot_id: i64,
        replace: bool,
        if_not_exists: bool,
        expected_table_uuid: Option<Uuid>,
    },
    DropBranch {
        name: String,
        if_exists: bool,
    },
    DropTag {
        name: String,
        if_exists: bool,
    },
    /// Internal MV publication primitive. SQL never constructs this action.
    FastForwardBranch {
        source_branch: String,
        target_branch: String,
        source_snapshot_id: i64,
        expected_target_snapshot_id: Option<i64>,
    },
}

/// Lower a provider-independent reference action into the Iceberg commit
/// primitive after validating the pinned table metadata.
pub fn lower_ref_action(
    action: novarocks_spi::connector::ConnectorRefAction,
    metadata: &crate::iceberg::spec::TableMetadata,
    namespace: &str,
    table: &str,
    catalog: &str,
) -> Result<RefActionPlan, novarocks_spi::connector::ConnectorError> {
    use novarocks_spi::connector::{
        ConnectorError, ConnectorErrorKind, ConnectorRefAction, ConnectorRefKind,
        CreateOrReplacePolicy, DropPolicy,
    };

    fn assert_kind(
        metadata: &crate::iceberg::spec::TableMetadata,
        name: &str,
        expected: ConnectorRefKind,
    ) -> Result<(), ConnectorError> {
        let Some(existing) = metadata.refs().get(name) else {
            return Ok(());
        };
        let actual = match existing.retention {
            crate::iceberg::spec::SnapshotRetention::Branch { .. } => ConnectorRefKind::Branch,
            crate::iceberg::spec::SnapshotRetention::Tag { .. } => ConnectorRefKind::Tag,
        };
        if actual == expected {
            return Ok(());
        }
        Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("Iceberg ref `{name}` has a different kind"),
        ))
    }

    let action = match action {
        ConnectorRefAction::Create {
            kind,
            name,
            snapshot_id,
            policy,
            expected_table_uuid,
        } => {
            if name.eq_ignore_ascii_case("main") {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg ref `main` is reserved",
                ));
            }
            assert_kind(metadata, &name, kind)?;
            let expected_table_uuid = expected_table_uuid
                .as_deref()
                .map(Uuid::parse_str)
                .transpose()
                .map_err(|error| {
                    ConnectorError::new(
                        ConnectorErrorKind::InvalidRequest,
                        format!("Iceberg ref create has an invalid expected table UUID: {error}"),
                    )
                })?;
            if expected_table_uuid.is_some_and(|uuid| uuid != metadata.uuid()) {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg ref create target table incarnation changed",
                ));
            }
            let snapshot_id = match snapshot_id.or_else(|| metadata.current_snapshot_id()) {
                Some(snapshot_id) if metadata.snapshot_by_id(snapshot_id).is_some() => snapshot_id,
                _ => {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::NotFound,
                        "Iceberg ref create requires an existing snapshot",
                    ));
                }
            };
            let (replace, if_not_exists) = match policy {
                CreateOrReplacePolicy::FailIfExists => (false, false),
                CreateOrReplacePolicy::NoOpIfExists => (false, true),
                CreateOrReplacePolicy::ReplaceIfExists => (true, false),
            };
            match kind {
                ConnectorRefKind::Branch => RefAction::CreateBranch {
                    name: name.to_string(),
                    snapshot_id,
                    replace,
                    if_not_exists,
                    expected_table_uuid,
                },
                ConnectorRefKind::Tag => RefAction::CreateTag {
                    name: name.to_string(),
                    snapshot_id,
                    replace,
                    if_not_exists,
                    expected_table_uuid,
                },
            }
        }
        ConnectorRefAction::Drop { kind, name, policy } => {
            if name.eq_ignore_ascii_case("main") {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg ref `main` is reserved",
                ));
            }
            assert_kind(metadata, &name, kind)?;
            let if_exists = policy == DropPolicy::NoOpIfMissing;
            match kind {
                ConnectorRefKind::Branch => RefAction::DropBranch {
                    name: name.to_string(),
                    if_exists,
                },
                ConnectorRefKind::Tag => RefAction::DropTag {
                    name: name.to_string(),
                    if_exists,
                },
            }
        }
        ConnectorRefAction::FastForwardBranch { .. } => {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Internal,
                "guarded MV publication bypassed its provider commit path",
            ));
        }
    };
    Ok(RefActionPlan {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
        action,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub enum RefActionOutcome {
    Committed,
    NoOp,
}

/// Result of an internal cleanup-only ref retirement attempt.
///
/// `Abandoned` is deliberately non-error: every mismatch means the proof
/// observed during candidate discovery has gone stale, so this GC pass leaks
/// rather than guessing whether the ref is still NovaRocks-owned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactBranchDropOutcome {
    Retired,
    Abandoned,
}

fn build_exact_branch_drop_commit(
    ident: TableIdent,
    metadata: &TableMetadata,
    name: &str,
    expected_head_snapshot_id: i64,
) -> Result<TableCommit, ExistingCommitError> {
    // The pre-read is only candidate discovery. The frozen incarnation proof
    // stays in the destructive commit so DROP/recreate abandons GC.
    existing_table_commit(
        ident,
        metadata,
        vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: name.to_string(),
            snapshot_id: Some(expected_head_snapshot_id),
        }],
        vec![TableUpdate::RemoveSnapshotRef {
            ref_name: name.to_string(),
        }],
    )
}

/// Drop one provider-owned branch only if its table incarnation and observed
/// head are unchanged. This is intentionally separate from SQL `DROP BRANCH`:
/// it has no `IF EXISTS` mode and never converts a missing or changed ref into
/// a successful cleanup result. The caller owns the table load so request-scoped
/// storage credentials cannot be bypassed by a second catalog load here.
pub async fn drop_branch_if_exact(
    catalog: &dyn Catalog,
    loaded: &crate::iceberg::table::Table,
    expected_table_uuid: &str,
    name: &str,
    expected_head_snapshot_id: i64,
) -> Result<ExactBranchDropOutcome, String> {
    let metadata = loaded.metadata();
    if metadata.uuid().to_string() != expected_table_uuid {
        return Ok(ExactBranchDropOutcome::Abandoned);
    }
    let Some(reference) = metadata.refs().get(name) else {
        return Ok(ExactBranchDropOutcome::Abandoned);
    };
    if !reference.is_branch()
        || reference.snapshot_id != expected_head_snapshot_id
        || metadata.snapshot_by_id(expected_head_snapshot_id).is_none()
    {
        return Ok(ExactBranchDropOutcome::Abandoned);
    }
    let commit = build_exact_branch_drop_commit(
        loaded.identifier().clone(),
        metadata,
        name,
        expected_head_snapshot_id,
    )
    .map_err(|error| format!("iceberg cleanup ref: {error}"))?;
    match catalog.update_table(commit).await {
        Ok(_) => Ok(ExactBranchDropOutcome::Retired),
        // A concurrently moved/deleted ref is a failed compare-and-swap proof,
        // not a reason to retry a destructive action in this GC pass.
        Err(error)
            if matches!(
                error.kind(),
                crate::iceberg::ErrorKind::PreconditionFailed
                    | crate::iceberg::ErrorKind::CatalogCommitConflicts
            ) =>
        {
            Ok(ExactBranchDropOutcome::Abandoned)
        }
        Err(error) => Err(format!("iceberg cleanup ref: exact drop failed: {error}")),
    }
}

impl RefActionPlan {
    /// Lower against the exact locked observation. Publication belongs to the journal.
    pub(crate) fn prepare(
        &self,
        loaded: &crate::iceberg::table::Table,
    ) -> Result<
        crate::commit::staging::PreparedChange,
        novarocks_spi::connector::ConnectorMutationFailure,
    > {
        use crate::commit::staging::PreparedChange;
        use novarocks_spi::connector::{
            ConnectorMutationFailure, ConnectorMutationFailureKind as Kind,
        };
        let invalid =
            |message: String| ConnectorMutationFailure::new(Kind::InvalidRequest, message);
        let missing = |message: String| ConnectorMutationFailure::new(Kind::NotFound, message);
        let ident = TableIdent::from_strs([self.namespace.as_str(), self.table.as_str()])
            .map_err(|error| invalid(format!("Invalid Iceberg ref table identifier: {error}")))?;
        if loaded.identifier() != &ident {
            return Err(invalid(
                "Request-scoped table identity does not match the ref plan".into(),
            ));
        }
        let metadata = loaded.metadata();
        let (updates, mut requirements) = match &self.action {
            RefAction::CreateBranch {
                name,
                snapshot_id,
                replace,
                if_not_exists,
                expected_table_uuid,
            }
            | RefAction::CreateTag {
                name,
                snapshot_id,
                replace,
                if_not_exists,
                expected_table_uuid,
            } => {
                if expected_table_uuid.is_some_and(|uuid| uuid != metadata.uuid()) {
                    return Err(invalid("Iceberg ref target table UUID changed".into()));
                }
                let branch = matches!(&self.action, RefAction::CreateBranch { .. });
                let existing = metadata.refs().get(name);
                if existing.is_some_and(|reference| reference.is_branch() != branch) {
                    return Err(invalid(format!(
                        "Iceberg ref '{name}' has a different kind"
                    )));
                }
                if existing.is_some() {
                    if *if_not_exists {
                        return Ok(PreparedChange::default());
                    }
                    if !*replace {
                        return Err(ConnectorMutationFailure::new(
                            Kind::AlreadyExists,
                            format!("Iceberg ref '{name}' already exists"),
                        ));
                    }
                }
                if metadata.snapshot_by_id(*snapshot_id).is_none() {
                    return Err(missing(format!(
                        "Iceberg ref snapshot {snapshot_id} does not exist"
                    )));
                }
                (
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: name.clone(),
                        reference: crate::ref_snapshot::moved_reference(
                            existing,
                            *snapshot_id,
                            branch,
                        ),
                    }],
                    vec![TableRequirement::RefSnapshotIdMatch {
                        r#ref: name.clone(),
                        snapshot_id: existing.map(|reference| reference.snapshot_id),
                    }],
                )
            }
            RefAction::DropBranch { name, if_exists } | RefAction::DropTag { name, if_exists } => {
                let Some(existing) = metadata.refs().get(name) else {
                    if *if_exists {
                        return Ok(PreparedChange::default());
                    }
                    return Err(missing(format!("Iceberg ref '{name}' does not exist")));
                };
                let branch = matches!(&self.action, RefAction::DropBranch { .. });
                if existing.is_branch() != branch {
                    return Err(invalid(format!(
                        "Iceberg ref '{name}' has a different kind"
                    )));
                }
                (
                    vec![TableUpdate::RemoveSnapshotRef {
                        ref_name: name.clone(),
                    }],
                    vec![TableRequirement::RefSnapshotIdMatch {
                        r#ref: name.clone(),
                        snapshot_id: Some(existing.snapshot_id),
                    }],
                )
            }
            RefAction::FastForwardBranch {
                source_branch,
                target_branch,
                source_snapshot_id,
                expected_target_snapshot_id,
            } => {
                let source = metadata.refs().get(source_branch).ok_or_else(|| {
                    missing(format!(
                        "Iceberg source branch '{source_branch}' does not exist"
                    ))
                })?;
                if !source.is_branch() || source.snapshot_id != *source_snapshot_id {
                    return Err(invalid(format!(
                        "Iceberg source branch '{source_branch}' changed"
                    )));
                }
                if metadata.snapshot_by_id(*source_snapshot_id).is_none() {
                    return Err(missing(format!(
                        "Iceberg source snapshot {source_snapshot_id} does not exist"
                    )));
                }
                let target = metadata.refs().get(target_branch);
                if target.is_some_and(|reference| !reference.is_branch()) {
                    return Err(invalid(format!(
                        "Iceberg target '{target_branch}' is not a branch"
                    )));
                }
                let head = if target_branch == "main" {
                    metadata.current_snapshot_id()
                } else {
                    Some(
                        target
                            .ok_or_else(|| {
                                missing(format!(
                                    "Iceberg target branch '{target_branch}' does not exist"
                                ))
                            })?
                            .snapshot_id,
                    )
                };
                if head != *expected_target_snapshot_id {
                    return Err(ConnectorMutationFailure::new(
                        Kind::Conflict,
                        "Iceberg target branch changed before publication",
                    ));
                }
                (
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: target_branch.clone(),
                        reference: crate::ref_snapshot::moved_reference(
                            target,
                            *source_snapshot_id,
                            true,
                        ),
                    }],
                    vec![
                        TableRequirement::RefSnapshotIdMatch {
                            r#ref: source_branch.clone(),
                            snapshot_id: Some(*source_snapshot_id),
                        },
                        TableRequirement::RefSnapshotIdMatch {
                            r#ref: target_branch.clone(),
                            snapshot_id: *expected_target_snapshot_id,
                        },
                    ],
                )
            }
        };
        requirements.insert(
            0,
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
        );
        Ok(PreparedChange {
            updates,
            requirements,
        })
    }

    /// A stale cleanup candidate grants no destructive request.
    pub(crate) fn prepare_exact_drop(
        loaded: &crate::iceberg::table::Table,
        expected_table_uuid: &str,
        name: &str,
        expected_head_snapshot_id: i64,
    ) -> Option<crate::commit::staging::PreparedChange> {
        let metadata = loaded.metadata();
        let reference = metadata.refs().get(name)?;
        if metadata.uuid().to_string() != expected_table_uuid
            || !reference.is_branch()
            || reference.snapshot_id != expected_head_snapshot_id
            || metadata.snapshot_by_id(expected_head_snapshot_id).is_none()
        {
            return None;
        }
        Some(crate::commit::staging::PreparedChange {
            requirements: vec![
                TableRequirement::UuidMatch {
                    uuid: metadata.uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: name.to_owned(),
                    snapshot_id: Some(expected_head_snapshot_id),
                },
            ],
            updates: vec![TableUpdate::RemoveSnapshotRef {
                ref_name: name.to_owned(),
            }],
        })
    }
}

pub async fn execute_ref_action(
    catalog: &dyn Catalog,
    loaded: &crate::iceberg::table::Table,
    plan: &RefActionPlan,
) -> Result<RefActionOutcome, String> {
    let change = plan.prepare(loaded).map_err(|error| error.to_string())?;
    if change.updates.is_empty() {
        return Ok(RefActionOutcome::NoOp);
    }
    let commit = existing_table_commit(
        loaded.identifier().clone(),
        loaded.metadata(),
        change.requirements,
        change.updates,
    )
    .map_err(|error| format!("iceberg ref: {error}"))?;
    catalog
        .update_table(commit)
        .await
        .map_err(|error| format!("iceberg ref: commit failed: {error}"))?;
    Ok(RefActionOutcome::Committed)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::*;
    use crate::commit::existing_commit::test_support::{LocalTable, local_table};
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder,
        TableMetadataBuilder, Type,
    };

    fn metadata_with_uuid(uuid: Uuid) -> TableMetadata {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec(),
            SortOrder::unsorted_order(),
            "memory://warehouse/db/orders".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .expect("metadata builder")
        .assign_uuid(uuid)
        .build()
        .expect("metadata")
        .metadata
    }

    fn plan(action: RefAction) -> RefActionPlan {
        RefActionPlan {
            catalog: "iceberg".to_string(),
            namespace: "db".to_string(),
            table: "t".to_string(),
            action,
        }
    }

    /// Runs one action against a fresh load and returns the UUID assertions of
    /// the dispatched commit together with the UUID of the metadata it used.
    async fn run_action(fixture: &LocalTable, action: RefAction) -> (Vec<Uuid>, Uuid) {
        let loaded = fixture.load().await;
        let outcome = execute_ref_action(fixture.catalog.as_ref(), &loaded, &plan(action))
            .await
            .expect("execute ref action");
        assert_eq!(outcome, RefActionOutcome::Committed);
        (
            fixture.catalog.take_single_commit_uuids(),
            loaded.metadata().uuid(),
        )
    }

    #[tokio::test]
    async fn every_ref_action_freezes_the_uuid_of_the_metadata_it_used() {
        let fixture = local_table().await;
        let first = fixture.append("first").await;

        let create_branch = RefAction::CreateBranch {
            name: "dev".to_string(),
            snapshot_id: first,
            replace: false,
            if_not_exists: false,
            expected_table_uuid: None,
        };
        let create_tag = RefAction::CreateTag {
            name: "v1".to_string(),
            snapshot_id: first,
            replace: false,
            if_not_exists: false,
            expected_table_uuid: None,
        };
        for action in [create_branch, create_tag] {
            let (asserted, loaded) = run_action(&fixture, action).await;
            assert_eq!(asserted, vec![loaded]);
        }

        let second = fixture.append("second").await;
        let fast_forward = RefAction::FastForwardBranch {
            source_branch: "main".to_string(),
            target_branch: "dev".to_string(),
            source_snapshot_id: second,
            expected_target_snapshot_id: Some(first),
        };
        let drop_tag = RefAction::DropTag {
            name: "v1".to_string(),
            if_exists: false,
        };
        let drop_branch = RefAction::DropBranch {
            name: "dev".to_string(),
            if_exists: false,
        };
        for action in [fast_forward, drop_tag, drop_branch] {
            let (asserted, loaded) = run_action(&fixture, action).await;
            assert_eq!(asserted, vec![loaded]);
        }

        let refs = fixture.load().await.metadata().refs().clone();
        assert_eq!(refs.keys().collect::<Vec<_>>(), vec!["main"]);
    }

    #[tokio::test]
    async fn ref_create_with_another_incarnation_uuid_is_not_dispatched() {
        let fixture = local_table().await;
        let first = fixture.append("first").await;
        let loaded = fixture.load().await;
        let foreign = Uuid::new_v4();
        assert_ne!(foreign, loaded.metadata().uuid());

        let error = execute_ref_action(
            fixture.catalog.as_ref(),
            &loaded,
            &plan(RefAction::CreateBranch {
                name: "dev".to_string(),
                snapshot_id: first,
                replace: false,
                if_not_exists: false,
                expected_table_uuid: Some(foreign),
            }),
        )
        .await
        .expect_err("foreign incarnation UUID");

        assert!(error.contains("table UUID"), "{error}");
        assert!(fixture.catalog.commits().is_empty());
    }

    #[tokio::test]
    async fn pure_ref_replacement_preserves_policy_and_exact_cas() {
        let fixture = local_table().await;
        let first = fixture.append("first").await;
        let second = fixture.append("second").await;
        let loaded = fixture.load().await;
        for branch in [true, false] {
            let mut value = serde_json::to_value(loaded.metadata()).unwrap();
            value["refs"]["release"] = if branch {
                serde_json::json!({"snapshot-id":first,"type":"branch","min-snapshots-to-keep":11,"max-snapshot-age-ms":12,"max-ref-age-ms":13})
            } else {
                serde_json::json!({"snapshot-id":first,"type":"tag","max-ref-age-ms":13})
            };
            let metadata: TableMetadata = serde_json::from_value(value).unwrap();
            let expected = metadata.refs()["release"].retention.clone();
            let table = crate::iceberg::table::Table::builder()
                .runtime(crate::iceberg::Runtime::try_current().unwrap())
                .identifier(loaded.identifier().clone())
                .file_io(loaded.file_io().clone())
                .metadata(metadata)
                .build()
                .unwrap();
            let duplicate = if branch {
                RefAction::CreateBranch {
                    name: "release".into(),
                    snapshot_id: second,
                    replace: false,
                    if_not_exists: false,
                    expected_table_uuid: Some(table.metadata().uuid()),
                }
            } else {
                RefAction::CreateTag {
                    name: "release".into(),
                    snapshot_id: second,
                    replace: false,
                    if_not_exists: false,
                    expected_table_uuid: Some(table.metadata().uuid()),
                }
            };
            let duplicate = plan(duplicate)
                .prepare(&table)
                .err()
                .expect("duplicate ref");
            assert_eq!(
                duplicate.kind(),
                novarocks_spi::connector::ConnectorMutationFailureKind::AlreadyExists
            );
            if branch {
                let stale = plan(RefAction::FastForwardBranch {
                    source_branch: "main".into(),
                    target_branch: "release".into(),
                    source_snapshot_id: second,
                    expected_target_snapshot_id: Some(second),
                })
                .prepare(&table)
                .err()
                .expect("stale target head");
                assert_eq!(
                    stale.kind(),
                    novarocks_spi::connector::ConnectorMutationFailureKind::Conflict
                );
            }
            let action = if branch {
                RefAction::CreateBranch {
                    name: "release".into(),
                    snapshot_id: second,
                    replace: true,
                    if_not_exists: false,
                    expected_table_uuid: Some(table.metadata().uuid()),
                }
            } else {
                RefAction::CreateTag {
                    name: "release".into(),
                    snapshot_id: second,
                    replace: true,
                    if_not_exists: false,
                    expected_table_uuid: Some(table.metadata().uuid()),
                }
            };
            let prepared = plan(action).prepare(&table).unwrap();
            let [TableUpdate::SetSnapshotRef { reference, .. }] = prepared.updates.as_slice()
            else {
                panic!("one ref update");
            };
            assert_eq!(reference.retention, expected);
            assert_eq!(reference.snapshot_id, second);
            assert!(
                prepared
                    .requirements
                    .contains(&TableRequirement::UuidMatch {
                        uuid: table.metadata().uuid()
                    })
            );
            assert!(
                prepared
                    .requirements
                    .contains(&TableRequirement::RefSnapshotIdMatch {
                        r#ref: "release".into(),
                        snapshot_id: Some(first)
                    })
            );
        }
        assert!(
            fixture.catalog.commits().is_empty(),
            "pure lowering must not dispatch"
        );
    }

    #[test]
    fn exact_branch_drop_commit_pins_table_incarnation_and_branch_head() {
        let table_uuid = Uuid::new_v4();
        let mut commit = build_exact_branch_drop_commit(
            TableIdent::from_strs(["db", "orders"]).expect("valid table identifier"),
            &metadata_with_uuid(table_uuid),
            "__novarocks_mv_refresh",
            42,
        )
        .expect("exact branch drop commit");

        assert_eq!(
            commit.take_requirements(),
            vec![
                TableRequirement::UuidMatch { uuid: table_uuid },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: "__novarocks_mv_refresh".to_string(),
                    snapshot_id: Some(42),
                },
            ]
        );
    }
}
