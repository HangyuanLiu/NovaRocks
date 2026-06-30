pub(crate) const CHANGE_OP_DELETE: i32 = -1;
pub(crate) const CHANGE_OP_INSERT: i32 = 1;
pub(crate) const DATA_ROUTE_REUSE: i32 = 1;
pub(crate) const DATA_ROUTE_FRESH: i32 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ChangeStreamBranchKind {
    DeleteDv,
    ReuseData,
    FreshData,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ChangeStreamRouteKey {
    pub(crate) change_op: i32,
    pub(crate) data_route: Option<i32>,
}

impl ChangeStreamBranchKind {
    pub(crate) fn route_key(self) -> ChangeStreamRouteKey {
        match self {
            Self::DeleteDv => ChangeStreamRouteKey {
                change_op: CHANGE_OP_DELETE,
                data_route: None,
            },
            Self::ReuseData => ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT,
                data_route: Some(DATA_ROUTE_REUSE),
            },
            Self::FreshData => ChangeStreamRouteKey {
                change_op: CHANGE_OP_INSERT,
                data_route: Some(DATA_ROUTE_FRESH),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_kind_maps_to_canonical_route_key() {
        assert_eq!(
            ChangeStreamBranchKind::DeleteDv.route_key(),
            ChangeStreamRouteKey {
                change_op: -1,
                data_route: None,
            }
        );
        assert_eq!(
            ChangeStreamBranchKind::ReuseData.route_key(),
            ChangeStreamRouteKey {
                change_op: 1,
                data_route: Some(1),
            }
        );
        assert_eq!(
            ChangeStreamBranchKind::FreshData.route_key(),
            ChangeStreamRouteKey {
                change_op: 1,
                data_route: Some(2),
            }
        );
    }
}
