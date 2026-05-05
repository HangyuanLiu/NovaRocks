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

use iceberg::spec::{SnapshotReference, SnapshotRetention};
use iceberg::{Catalog, TableCommit, TableIdent, TableRequirement, TableUpdate};

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
