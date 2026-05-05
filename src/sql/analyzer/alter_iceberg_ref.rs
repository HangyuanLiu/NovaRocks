//! Validate parsed `AlterIcebergRefStmt` against table metadata; produce a
//! `RefActionPlan` that the lower stage forwards to the executor.

#![allow(dead_code)]

use crate::sql::analyzer::iceberg_ref::IcebergRefKind;
use crate::sql::parser::ast::{AlterIcebergRefAction, AlterIcebergRefStmt, SnapshotAnchor};

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
    stmt: &AlterIcebergRefStmt,
    catalog: &str,
    namespace: &str,
    table: &str,
    table_metadata: &iceberg::spec::TableMetadata,
) -> Result<RefActionPlan, String> {
    let name = action_name(&stmt.action);
    if name == "main" {
        return Err("iceberg ref: 'main' is reserved".to_string());
    }

    let action = match &stmt.action {
        AlterIcebergRefAction::CreateBranch {
            name,
            anchor,
            if_not_exists,
            replace,
            ignored_options,
        } => {
            warn_ignored_options(ignored_options);
            check_kind(table_metadata, name, IcebergRefKind::Branch)?;
            let snapshot_id = resolve_anchor(anchor, table_metadata, name)?;
            RefAction::CreateBranch {
                name: name.clone(),
                snapshot_id,
                replace: *replace,
                if_not_exists: *if_not_exists,
            }
        }
        AlterIcebergRefAction::CreateTag {
            name,
            anchor,
            if_not_exists,
            replace,
            ignored_options,
        } => {
            warn_ignored_options(ignored_options);
            check_kind(table_metadata, name, IcebergRefKind::Tag)?;
            let snapshot_id = resolve_anchor(anchor, table_metadata, name)?;
            RefAction::CreateTag {
                name: name.clone(),
                snapshot_id,
                replace: *replace,
                if_not_exists: *if_not_exists,
            }
        }
        AlterIcebergRefAction::DropBranch { name, if_exists } => {
            check_kind(table_metadata, name, IcebergRefKind::Branch)?;
            RefAction::DropBranch {
                name: name.clone(),
                if_exists: *if_exists,
            }
        }
        AlterIcebergRefAction::DropTag { name, if_exists } => {
            check_kind(table_metadata, name, IcebergRefKind::Tag)?;
            RefAction::DropTag {
                name: name.clone(),
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

fn action_name(a: &AlterIcebergRefAction) -> &str {
    match a {
        AlterIcebergRefAction::CreateBranch { name, .. }
        | AlterIcebergRefAction::CreateTag { name, .. }
        | AlterIcebergRefAction::DropBranch { name, .. }
        | AlterIcebergRefAction::DropTag { name, .. } => name,
    }
}

fn warn_ignored_options(opts: &[String]) {
    if !opts.is_empty() {
        tracing::warn!(
            "iceberg ref: retention options ignored in phase 1: {}",
            opts.join(" ")
        );
    }
}

fn resolve_anchor(
    anchor: &SnapshotAnchor,
    metadata: &iceberg::spec::TableMetadata,
    ref_name: &str,
) -> Result<i64, String> {
    match anchor {
        SnapshotAnchor::SnapshotId(n) => {
            if metadata.snapshot_by_id(*n).is_none() {
                return Err(format!(
                    "iceberg ref: snapshot {n} not found; cannot anchor '{ref_name}'"
                ));
            }
            Ok(*n)
        }
        SnapshotAnchor::CurrentMain => match metadata.current_snapshot() {
            Some(s) => Ok(s.snapshot_id()),
            None => Err(
                "iceberg ref: cannot create branch on table without a current snapshot".to_string(),
            ),
        },
    }
}

/// If a ref of the given name exists, ensure its kind matches the expected
/// kind (branch vs tag). Mismatches are rejected.
fn check_kind(
    metadata: &iceberg::spec::TableMetadata,
    name: &str,
    expected: IcebergRefKind,
) -> Result<(), String> {
    if let Some(existing) = metadata.refs().get(name) {
        let existing_kind = match &existing.retention {
            iceberg::spec::SnapshotRetention::Branch { .. } => IcebergRefKind::Branch,
            iceberg::spec::SnapshotRetention::Tag { .. } => IcebergRefKind::Tag,
        };
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
    use super::*;
    use iceberg::spec::{
        FormatVersion, NestedField, Operation, PartitionSpec, PrimitiveType, Schema, Snapshot,
        SnapshotReference, SnapshotRetention, SortOrder, Summary, TableMetadataBuilder, Type,
    };
    use std::collections::HashMap;

    fn base_builder() -> TableMetadataBuilder {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();

        TableMetadataBuilder::new(
            schema,
            PartitionSpec::unpartition_spec().into_unbound(),
            SortOrder::unsorted_order(),
            "memory://test/table".to_string(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
    }

    /// Build a minimal V2 TableMetadata with no snapshots.
    fn metadata_empty() -> iceberg::spec::TableMetadata {
        base_builder().build().unwrap().metadata
    }

    /// Build a TableMetadata with one snapshot and a branch ref named `branch_name`.
    fn metadata_with_branch(branch_name: &str) -> iceberg::spec::TableMetadata {
        let snapshot_id = 1_i64;
        let snapshot = Snapshot::builder()
            .with_snapshot_id(snapshot_id)
            .with_timestamp_ms(1_700_000_000_000)
            .with_sequence_number(1)
            .with_summary(Summary {
                operation: Operation::Append,
                additional_properties: HashMap::new(),
            })
            .with_manifest_list("memory://test/table/metadata/snap-1.avro".to_string())
            .with_schema_id(0)
            .build();

        let branch_ref = SnapshotReference::new(
            snapshot_id,
            SnapshotRetention::Branch {
                min_snapshots_to_keep: None,
                max_snapshot_age_ms: None,
                max_ref_age_ms: None,
            },
        );

        base_builder()
            .add_snapshot(snapshot)
            .unwrap()
            .set_ref(
                "main",
                SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::Branch {
                        min_snapshots_to_keep: None,
                        max_snapshot_age_ms: None,
                        max_ref_age_ms: None,
                    },
                ),
            )
            .unwrap()
            .set_ref(branch_name, branch_ref)
            .unwrap()
            .build()
            .unwrap()
            .metadata
    }

    fn make_stmt(action: AlterIcebergRefAction) -> AlterIcebergRefStmt {
        AlterIcebergRefStmt {
            table: crate::sql::parser::ast::ObjectName {
                parts: vec!["c".into(), "s".into(), "t".into()],
            },
            action,
        }
    }

    #[test]
    fn create_branch_main_rejected() {
        let md = metadata_empty();
        let stmt = make_stmt(AlterIcebergRefAction::CreateBranch {
            name: "main".into(),
            anchor: SnapshotAnchor::CurrentMain,
            if_not_exists: false,
            replace: false,
            ignored_options: vec![],
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
        let stmt = make_stmt(AlterIcebergRefAction::CreateBranch {
            name: "dev".into(),
            anchor: SnapshotAnchor::SnapshotId(99_999),
            if_not_exists: false,
            replace: false,
            ignored_options: vec![],
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
        let stmt = make_stmt(AlterIcebergRefAction::CreateTag {
            name: "dev".into(),
            anchor: SnapshotAnchor::CurrentMain,
            if_not_exists: false,
            replace: false,
            ignored_options: vec![],
        });
        let err = analyze_alter_iceberg_ref(&stmt, "c", "s", "t", &md).unwrap_err();
        assert!(
            err.contains("'dev' is a branch, not a tag"),
            "unexpected error: {err}"
        );
    }
}
