//! Raw AST for `ALTER TABLE … (CREATE|DROP) [OR REPLACE] [IF [NOT] EXISTS]
//! (BRANCH|TAG) <name> [AS OF VERSION <id>] [retention …]`.

use crate::sql::parser::ast::ObjectName;

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum AlterIcebergRefAction {
    CreateBranch {
        name: String,
        anchor: SnapshotAnchor,
        if_not_exists: bool,
        replace: bool,
        ignored_options: Vec<String>,
    },
    CreateTag {
        name: String,
        anchor: SnapshotAnchor,
        if_not_exists: bool,
        replace: bool,
        ignored_options: Vec<String>,
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SnapshotAnchor {
    SnapshotId(i64),
    CurrentMain,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AlterIcebergRefStmt {
    pub table: ObjectName,
    pub action: AlterIcebergRefAction,
}
