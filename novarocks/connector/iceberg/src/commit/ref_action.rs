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
    },
    CreateTag {
        name: String,
        snapshot_id: i64,
        replace: bool,
        if_not_exists: bool,
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
        } => {
            if name.eq_ignore_ascii_case("main") {
                return Err(ConnectorError::new(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg ref `main` is reserved",
                ));
            }
            assert_kind(metadata, &name, kind)?;
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
                },
                ConnectorRefKind::Tag => RefAction::CreateTag {
                    name: name.to_string(),
                    snapshot_id,
                    replace,
                    if_not_exists,
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
        } => match metadata.refs().get(name) {
            Some(_existing) if *if_not_exists => return Ok(RefActionOutcome::NoOp),
            Some(_existing) if !*replace => {
                return Err(format!("iceberg ref: branch '{name}' already exists"));
            }
            existing => {
                let parent = existing.map(|r| r.snapshot_id);
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
                    vec![TableRequirement::RefSnapshotIdMatch {
                        r#ref: name.clone(),
                        snapshot_id: parent,
                    }],
                )
            }
        },
        RefAction::CreateTag {
            name,
            snapshot_id,
            replace,
            if_not_exists,
        } => match metadata.refs().get(name) {
            Some(_existing) if *if_not_exists => return Ok(RefActionOutcome::NoOp),
            Some(_existing) if !*replace => {
                return Err(format!("iceberg ref: tag '{name}' already exists"));
            }
            existing => {
                let parent = existing.map(|r| r.snapshot_id);
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
                    vec![TableRequirement::RefSnapshotIdMatch {
                        r#ref: name.clone(),
                        snapshot_id: parent,
                    }],
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
