#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(dead_code)]
pub(crate) enum BaseRowIdentity {
    IcebergRowId(i64),
    Position { file_path: String, pos: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BaseRowChangeKind {
    Insert,
    Delete,
    Update,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BaseRowChange {
    pub(crate) identity: BaseRowIdentity,
    pub(crate) kind: BaseRowChangeKind,
}

pub(crate) fn normalize_insert_delete_pairs(
    inserts: impl IntoIterator<Item = BaseRowIdentity>,
    deletes: impl IntoIterator<Item = BaseRowIdentity>,
) -> Vec<BaseRowChange> {
    use std::collections::BTreeSet;

    let insert_set: BTreeSet<_> = inserts.into_iter().collect();
    let delete_set: BTreeSet<_> = deletes.into_iter().collect();
    let mut out = Vec::new();

    for identity in delete_set.intersection(&insert_set) {
        out.push(BaseRowChange {
            identity: identity.clone(),
            kind: BaseRowChangeKind::Update,
        });
    }
    for identity in delete_set.difference(&insert_set) {
        out.push(BaseRowChange {
            identity: identity.clone(),
            kind: BaseRowChangeKind::Delete,
        });
    }
    for identity in insert_set.difference(&delete_set) {
        out.push(BaseRowChange {
            identity: identity.clone(),
            kind: BaseRowChangeKind::Insert,
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_row_id_insert_and_delete_becomes_update() {
        let changes = normalize_insert_delete_pairs(
            [BaseRowIdentity::IcebergRowId(7)],
            [BaseRowIdentity::IcebergRowId(7)],
        );
        assert_eq!(
            changes,
            vec![BaseRowChange {
                identity: BaseRowIdentity::IcebergRowId(7),
                kind: BaseRowChangeKind::Update,
            }]
        );
    }

    #[test]
    fn different_row_ids_remain_insert_and_delete() {
        let changes = normalize_insert_delete_pairs(
            [BaseRowIdentity::IcebergRowId(8)],
            [BaseRowIdentity::IcebergRowId(7)],
        );
        assert_eq!(
            changes,
            vec![
                BaseRowChange {
                    identity: BaseRowIdentity::IcebergRowId(7),
                    kind: BaseRowChangeKind::Delete,
                },
                BaseRowChange {
                    identity: BaseRowIdentity::IcebergRowId(8),
                    kind: BaseRowChangeKind::Insert,
                },
            ]
        );
    }
}
