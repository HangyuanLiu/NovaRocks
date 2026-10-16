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

//! Existing-table commit construction that always freezes the table UUID.
//!
//! Every direct-path commit against an existing table is built here. The UUID
//! comes from the same metadata that produced the updates, never from a second
//! load: a reload may already observe a dropped-and-recreated table whose
//! schema and spec IDs happen to match, and the commit must not land on it.

use std::fmt;

use uuid::Uuid;

use crate::iceberg::spec::TableMetadata;
use crate::iceberg::{TableCommit, TableIdent, TableRequirement, TableUpdate};

/// Refusal to build an existing-table commit. Raised before any dispatch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ExistingCommitError {
    /// A caller-supplied UUID assertion names another table incarnation than
    /// the metadata the updates were produced from.
    ConflictingTableUuid { base: Uuid, asserted: Uuid },
    /// An existing-table commit cannot assert that the table does not exist.
    CreateRequirement,
}

impl fmt::Display for ExistingCommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConflictingTableUuid { base, asserted } => write!(
                f,
                "existing-table commit asserts table UUID {asserted}, but its updates were \
                 produced from table UUID {base}"
            ),
            Self::CreateRequirement => {
                f.write_str("existing-table commit cannot carry a table-does-not-exist requirement")
            }
        }
    }
}

impl std::error::Error for ExistingCommitError {}

/// Build a commit against an existing table, freezing `base`'s UUID.
///
/// `base` must be the metadata the caller produced `updates` (and the other
/// `requirements`) from. A `UuidMatch` already present must equal its UUID;
/// otherwise the frozen assertion is placed first so a replaced table fails
/// before any other requirement is evaluated.
pub(crate) fn existing_table_commit(
    ident: TableIdent,
    base: &TableMetadata,
    mut requirements: Vec<TableRequirement>,
    updates: Vec<TableUpdate>,
) -> Result<TableCommit, ExistingCommitError> {
    let uuid = base.uuid();
    let mut frozen = false;
    for requirement in &requirements {
        match requirement {
            TableRequirement::NotExist => return Err(ExistingCommitError::CreateRequirement),
            TableRequirement::UuidMatch { uuid: asserted } if *asserted != uuid => {
                return Err(ExistingCommitError::ConflictingTableUuid {
                    base: uuid,
                    asserted: *asserted,
                });
            }
            TableRequirement::UuidMatch { .. } => frozen = true,
            _ => {}
        }
    }
    if !frozen {
        requirements.insert(0, TableRequirement::UuidMatch { uuid });
    }
    Ok(TableCommit::builder()
        .ident(ident)
        .requirements(requirements)
        .updates(updates)
        .build())
}
