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

//! Validate parsed Iceberg reference actions against table metadata; produce a
//! `RefActionPlan` that the lower stage forwards to the executor.

#![allow(dead_code)]

use crate::analyzer::iceberg_ref::{IcebergRefKind, SqlIcebergRefMetadata};
use novarocks_parser::ast::{
    AlterIcebergTable, IcebergReferenceAction, IcebergReferenceKind, IcebergTableAction,
    LiteralKind, ReferenceAnchor,
};

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

/// Resolve the table, validate the action against current refs/snapshots,
/// and produce a `RefActionPlan`. Errors here are analyzer-time
/// (deterministic, fail-fast).
pub fn analyze_alter_iceberg_ref(
    stmt: &AlterIcebergTable,
    catalog: &str,
    namespace: &str,
    table: &str,
    table_metadata: &SqlIcebergRefMetadata,
) -> Result<RefActionPlan, String> {
    let IcebergTableAction::Reference(action) = &stmt.action else {
        return Err("iceberg ref: expected a branch or tag action".to_string());
    };
    let name = action_name(action);
    if name == "main" {
        return Err("iceberg ref: 'main' is reserved".to_string());
    }

    let action = match action {
        IcebergReferenceAction::Create {
            kind: IcebergReferenceKind::Branch,
            name,
            anchor,
            if_not_exists,
            or_replace,
            options,
        } => {
            let _ = options;
            check_kind(table_metadata, &name.value, IcebergRefKind::Branch)?;
            let snapshot_id = resolve_anchor(anchor, table_metadata, name)?;
            RefAction::CreateBranch {
                name: name.value.clone(),
                snapshot_id,
                replace: *or_replace,
                if_not_exists: *if_not_exists,
            }
        }
        IcebergReferenceAction::Create {
            kind: IcebergReferenceKind::Tag,
            name,
            anchor,
            if_not_exists,
            or_replace,
            options,
        } => {
            let _ = options;
            check_kind(table_metadata, &name.value, IcebergRefKind::Tag)?;
            let snapshot_id = resolve_anchor(anchor, table_metadata, name)?;
            RefAction::CreateTag {
                name: name.value.clone(),
                snapshot_id,
                replace: *or_replace,
                if_not_exists: *if_not_exists,
            }
        }
        IcebergReferenceAction::Drop {
            kind: IcebergReferenceKind::Branch,
            name,
            if_exists,
        } => {
            check_kind(table_metadata, &name.value, IcebergRefKind::Branch)?;
            RefAction::DropBranch {
                name: name.value.clone(),
                if_exists: *if_exists,
            }
        }
        IcebergReferenceAction::Drop {
            kind: IcebergReferenceKind::Tag,
            name,
            if_exists,
        } => {
            check_kind(table_metadata, &name.value, IcebergRefKind::Tag)?;
            RefAction::DropTag {
                name: name.value.clone(),
                if_exists: *if_exists,
            }
        }
    };

    Ok(RefActionPlan {
        catalog: catalog.to_string(),
        namespace: namespace.to_string(),
        table: table.to_string(),
        action,
    })
}

fn action_name(a: &IcebergReferenceAction) -> &str {
    match a {
        IcebergReferenceAction::Create { name, .. } | IcebergReferenceAction::Drop { name, .. } => {
            &name.value
        }
    }
}

fn resolve_anchor(
    anchor: &ReferenceAnchor,
    metadata: &SqlIcebergRefMetadata,
    ref_name: &novarocks_parser::ast::Ident,
) -> Result<i64, String> {
    match anchor {
        ReferenceAnchor::Version(literal) => {
            let LiteralKind::Number(value) = &literal.kind else {
                return Err("iceberg ref: snapshot version must be a numeric literal".to_string());
            };
            let n = value
                .parse::<i64>()
                .map_err(|_| "iceberg ref: snapshot version must fit i64".to_string())?;
            if !metadata.has_snapshot(n) {
                return Err(format!(
                    "iceberg ref: snapshot {n} not found; cannot anchor '{}'",
                    ref_name.value
                ));
            }
            Ok(n)
        }
        ReferenceAnchor::CurrentMain => match metadata.current_snapshot_id() {
            Some(snapshot_id) => Ok(snapshot_id),
            None => Err(
                "iceberg ref: cannot create branch on table without a current snapshot".to_string(),
            ),
        },
    }
}

/// If a ref of the given name exists, ensure its kind matches the expected
/// kind (branch vs tag). Mismatches are rejected.
fn check_kind(
    metadata: &SqlIcebergRefMetadata,
    name: &str,
    expected: IcebergRefKind,
) -> Result<(), String> {
    if let Some(existing) = metadata.named_ref(name) {
        let existing_kind = existing.kind.clone();
        if existing_kind != expected {
            let actual = match existing_kind {
                IcebergRefKind::Branch => "branch",
                IcebergRefKind::Tag => "tag",
            };
            let exp = match expected {
                IcebergRefKind::Branch => "branch",
                IcebergRefKind::Tag => "tag",
            };
            return Err(format!("iceberg ref: '{name}' is a {actual}, not a {exp}"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::analyzer::iceberg_ref::SqlIcebergNamedRef;
    use novarocks_parser::{
        Span,
        ast::{Ident, Literal, ObjectName},
    };

    fn metadata_empty() -> SqlIcebergRefMetadata {
        SqlIcebergRefMetadata::default()
    }

    fn metadata_with_branch(branch_name: &str) -> SqlIcebergRefMetadata {
        SqlIcebergRefMetadata::new(
            [1],
            vec![],
            BTreeMap::from([(
                branch_name.to_string(),
                SqlIcebergNamedRef {
                    snapshot_id: 1,
                    kind: IcebergRefKind::Branch,
                },
            )]),
            Some(1),
        )
    }

    fn ident(value: &str) -> Ident {
        Ident {
            value: value.to_string(),
            quoted: false,
            quote_style: None,
            span: Span::new(0, 0),
        }
    }

    fn make_stmt(action: IcebergReferenceAction) -> AlterIcebergTable {
        AlterIcebergTable {
            table: ObjectName {
                parts: vec![ident("c"), ident("s"), ident("t")],
                span: Span::new(0, 0),
            },
            action: IcebergTableAction::Reference(action),
            span: Span::new(0, 0),
        }
    }

    #[test]
    fn create_branch_main_rejected() {
        let md = metadata_empty();
        let stmt = make_stmt(IcebergReferenceAction::Create {
            kind: IcebergReferenceKind::Branch,
            name: ident("main"),
            anchor: ReferenceAnchor::CurrentMain,
            if_not_exists: false,
            or_replace: false,
            options: None,
        });
        let err = analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).unwrap_err();
        assert!(
            err.contains("'main' is reserved"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn create_branch_unknown_anchor_rejected() {
        let md = metadata_empty();
        let stmt = make_stmt(IcebergReferenceAction::Create {
            kind: IcebergReferenceKind::Branch,
            name: ident("dev"),
            anchor: ReferenceAnchor::Version(Literal {
                kind: LiteralKind::Number("99999".to_string()),
                span: Span::new(0, 0),
            }),
            if_not_exists: false,
            or_replace: false,
            options: None,
        });
        let err = analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).unwrap_err();
        assert!(
            err.contains("snapshot 99999 not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn create_tag_kind_mismatch_when_branch_exists() {
        let md = metadata_with_branch("dev");
        let stmt = make_stmt(IcebergReferenceAction::Create {
            kind: IcebergReferenceKind::Tag,
            name: ident("dev"),
            anchor: ReferenceAnchor::CurrentMain,
            if_not_exists: false,
            or_replace: false,
            options: None,
        });
        let err = analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).unwrap_err();
        assert!(
            err.contains("'dev' is a branch, not a tag"),
            "unexpected error: {err}"
        );
    }
}
