//! Resolve Iceberg time-travel clauses + DML branch suffixes into a single
//! `IcebergRefBinding` that the read and commit paths consume.

#![allow(dead_code)]

use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IcebergRefKind {
    Branch,
    Tag,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergRefBinding {
    pub snapshot_id: i64,
    pub ref_name: Option<String>,
    pub ref_kind: Option<IcebergRefKind>,
}

impl IcebergRefBinding {
    pub fn ref_repr(&self) -> String {
        match (&self.ref_name, &self.ref_kind) {
            (Some(name), Some(IcebergRefKind::Branch)) => format!("branch '{name}'"),
            (Some(name), Some(IcebergRefKind::Tag)) => format!("tag '{name}'"),
            (Some(name), None) => format!("ref '{name}'"),
            (None, _) => format!("snapshot {}", self.snapshot_id),
        }
    }
}

impl fmt::Display for IcebergRefBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.ref_repr())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IcebergDmlTarget {
    pub read_binding: IcebergRefBinding,
    pub write_ref: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_repr_branch() {
        let b = IcebergRefBinding {
            snapshot_id: 7,
            ref_name: Some("dev".into()),
            ref_kind: Some(IcebergRefKind::Branch),
        };
        assert_eq!(b.ref_repr(), "branch 'dev'");
    }

    #[test]
    fn ref_repr_tag() {
        let b = IcebergRefBinding {
            snapshot_id: 7,
            ref_name: Some("v1".into()),
            ref_kind: Some(IcebergRefKind::Tag),
        };
        assert_eq!(b.ref_repr(), "tag 'v1'");
    }

    #[test]
    fn ref_repr_snapshot_only() {
        let b = IcebergRefBinding {
            snapshot_id: 42,
            ref_name: None,
            ref_kind: None,
        };
        assert_eq!(b.ref_repr(), "snapshot 42");
    }

    #[test]
    fn display_matches_ref_repr() {
        let b = IcebergRefBinding {
            snapshot_id: 7,
            ref_name: Some("dev".into()),
            ref_kind: Some(IcebergRefKind::Branch),
        };
        assert_eq!(format!("{b}"), b.ref_repr());
    }
}
