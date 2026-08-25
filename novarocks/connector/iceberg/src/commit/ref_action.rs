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

use crate::iceberg::spec::{SnapshotReference, SnapshotRetention};
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
    expected_table_uuid: Uuid,
    name: &str,
    expected_head_snapshot_id: i64,
) -> TableCommit {
    TableCommit::builder()
        .ident(ident)
        .updates(vec![TableUpdate::RemoveSnapshotRef {
            ref_name: name.to_string(),
        }])
        .requirements(vec![
            // The pre-read is only candidate discovery. Keep the incarnation
            // proof in the destructive commit so DROP/recreate abandons GC.
            TableRequirement::UuidMatch {
                uuid: expected_table_uuid,
            },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: name.to_string(),
                snapshot_id: Some(expected_head_snapshot_id),
            },
        ])
        .build()
}

/// Drop one provider-owned branch only if its table incarnation and observed
/// head are unchanged. This is intentionally separate from SQL `DROP BRANCH`:
/// it has no `IF EXISTS` mode and never converts a missing or changed ref into
/// a successful cleanup result.
pub async fn drop_branch_if_exact(
    catalog: &dyn Catalog,
    namespace: &str,
    table: &str,
    expected_table_uuid: &str,
    name: &str,
    expected_head_snapshot_id: i64,
) -> Result<ExactBranchDropOutcome, String> {
    let ident = TableIdent::from_strs([namespace, table])
        .map_err(|error| format!("iceberg cleanup ref: invalid table identifier: {error}"))?;
    let loaded = catalog
        .load_table(&ident)
        .await
        .map_err(|error| format!("iceberg cleanup ref: load table failed: {error}"))?;
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
    let commit =
        build_exact_branch_drop_commit(ident, metadata.uuid(), name, expected_head_snapshot_id);
    match catalog.update_table(commit).await {
        Ok(_) => Ok(ExactBranchDropOutcome::Retired),
        // A concurrently moved/deleted ref is a failed compare-and-swap proof,
        // not a reason to retry a destructive action in this GC pass.
        Err(error) if error.to_string().contains("Requirement") => {
            Ok(ExactBranchDropOutcome::Abandoned)
        }
        Err(error) => Err(format!("iceberg cleanup ref: exact drop failed: {error}")),
    }
}

pub async fn execute_ref_action(
    catalog: &dyn Catalog,
    plan: &RefActionPlan,
) -> Result<RefActionOutcome, String> {
    let ident = TableIdent::from_strs([plan.namespace.as_str(), plan.table.as_str()])
        .map_err(|e| format!("iceberg ref: invalid table identifier: {e}"))?;
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| format!("iceberg ref: load table: {e}"))?;
    let metadata = table.metadata();

    let (updates, requirements) = match &plan.action {
        RefAction::CreateBranch {
            name,
            snapshot_id,
            replace,
            if_not_exists,
            expected_table_uuid,
        } => match metadata.refs().get(name) {
            Some(_existing) if *if_not_exists => return Ok(RefActionOutcome::NoOp),
            Some(_existing) if !*replace => {
                return Err(format!("iceberg ref: branch '{name}' already exists"));
            }
            existing => {
                let parent = existing.map(|r| r.snapshot_id);
                let mut requirements = vec![TableRequirement::RefSnapshotIdMatch {
                    r#ref: name.clone(),
                    snapshot_id: parent,
                }];
                if let Some(uuid) = expected_table_uuid {
                    requirements.insert(0, TableRequirement::UuidMatch { uuid: *uuid });
                }
                (
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: name.clone(),
                        reference: SnapshotReference {
                            snapshot_id: *snapshot_id,
                            retention: SnapshotRetention::Branch {
                                min_snapshots_to_keep: None,
                                max_snapshot_age_ms: None,
                                max_ref_age_ms: None,
                            },
                        },
                    }],
                    requirements,
                )
            }
        },
        RefAction::CreateTag {
            name,
            snapshot_id,
            replace,
            if_not_exists,
            expected_table_uuid,
        } => match metadata.refs().get(name) {
            Some(_existing) if *if_not_exists => return Ok(RefActionOutcome::NoOp),
            Some(_existing) if !*replace => {
                return Err(format!("iceberg ref: tag '{name}' already exists"));
            }
            existing => {
                let parent = existing.map(|r| r.snapshot_id);
                let mut requirements = vec![TableRequirement::RefSnapshotIdMatch {
                    r#ref: name.clone(),
                    snapshot_id: parent,
                }];
                if let Some(uuid) = expected_table_uuid {
                    requirements.insert(0, TableRequirement::UuidMatch { uuid: *uuid });
                }
                (
                    vec![TableUpdate::SetSnapshotRef {
                        ref_name: name.clone(),
                        reference: SnapshotReference {
                            snapshot_id: *snapshot_id,
                            retention: SnapshotRetention::Tag {
                                max_ref_age_ms: None,
                            },
                        },
                    }],
                    requirements,
                )
            }
        },
        RefAction::DropBranch { name, if_exists } => match metadata.refs().get(name) {
            None if *if_exists => return Ok(RefActionOutcome::NoOp),
            None => return Err(format!("iceberg ref: branch '{name}' does not exist")),
            Some(existing) => (
                vec![TableUpdate::RemoveSnapshotRef {
                    ref_name: name.clone(),
                }],
                vec![TableRequirement::RefSnapshotIdMatch {
                    r#ref: name.clone(),
                    snapshot_id: Some(existing.snapshot_id),
                }],
            ),
        },
        RefAction::DropTag { name, if_exists } => match metadata.refs().get(name) {
            None if *if_exists => return Ok(RefActionOutcome::NoOp),
            None => return Err(format!("iceberg ref: tag '{name}' does not exist")),
            Some(existing) => (
                vec![TableUpdate::RemoveSnapshotRef {
                    ref_name: name.clone(),
                }],
                vec![TableRequirement::RefSnapshotIdMatch {
                    r#ref: name.clone(),
                    snapshot_id: Some(existing.snapshot_id),
                }],
            ),
        },
        RefAction::FastForwardBranch {
            source_branch,
            target_branch,
            source_snapshot_id,
            expected_target_snapshot_id,
        } => {
            let source = metadata.refs().get(source_branch).ok_or_else(|| {
                format!("iceberg ref: source branch '{source_branch}' does not exist")
            })?;
            if !source.is_branch() {
                return Err(format!(
                    "iceberg ref: source '{source_branch}' is not a branch"
                ));
            }
            if source.snapshot_id != *source_snapshot_id {
                return Err(format!(
                    "iceberg ref: source branch '{source_branch}' points to {}, expected {source_snapshot_id}",
                    source.snapshot_id
                ));
            }
            if metadata.snapshot_by_id(*source_snapshot_id).is_none() {
                return Err(format!(
                    "iceberg ref: source snapshot {source_snapshot_id} does not exist"
                ));
            }
            let current_target_snapshot_id = if target_branch == "main" {
                metadata.current_snapshot_id()
            } else {
                let target = metadata.refs().get(target_branch).ok_or_else(|| {
                    format!("iceberg ref: target branch '{target_branch}' does not exist")
                })?;
                if !target.is_branch() {
                    return Err(format!(
                        "iceberg ref: target '{target_branch}' is not a branch"
                    ));
                }
                Some(target.snapshot_id)
            };
            if current_target_snapshot_id != *expected_target_snapshot_id {
                return Err(format!(
                    "iceberg ref: target branch '{target_branch}' points to {:?}, expected {:?}",
                    current_target_snapshot_id, expected_target_snapshot_id
                ));
            }
            (
                vec![TableUpdate::SetSnapshotRef {
                    ref_name: target_branch.clone(),
                    reference: SnapshotReference {
                        snapshot_id: *source_snapshot_id,
                        retention: SnapshotRetention::Branch {
                            min_snapshots_to_keep: None,
                            max_snapshot_age_ms: None,
                            max_ref_age_ms: None,
                        },
                    },
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

    let commit = TableCommit::builder()
        .ident(ident)
        .updates(updates)
        .requirements(requirements)
        .build();

    catalog
        .update_table(commit)
        .await
        .map_err(|e| format!("iceberg ref: commit failed: {e}"))?;

    Ok(RefActionOutcome::Committed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_branch_drop_commit_pins_table_incarnation_and_branch_head() {
        let table_uuid = Uuid::new_v4();
        let mut commit = build_exact_branch_drop_commit(
            TableIdent::from_strs(["db", "orders"]).expect("valid table identifier"),
            table_uuid,
            "__novarocks_mv_refresh",
            42,
        );

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
