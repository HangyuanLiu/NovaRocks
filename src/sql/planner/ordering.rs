//! Planner-owned ordering vocabulary and satisfaction reasoning.
//!
//! Mirrors the optimizer's `property::{OrderingSpec, SortKey}` but lives in the
//! planner so that `distributed_plan_build` / codegen / `planner::mod` can reason
//! about ordering without importing `crate::sql::optimizer::*`. The optimizer
//! keeps its own copy; `optimizer_bridge` converts at the boundary.

use crate::sql::column_id::ColumnId;

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) struct SortKey {
    pub column: ColumnId,
    pub asc: bool,
    pub nulls_first: bool,
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(crate) enum OrderingSpec {
    Any,
    Required(Vec<SortKey>),
}

impl OrderingSpec {
    pub(crate) fn from_sort_keys<I>(items: I) -> Self
    where
        I: IntoIterator<Item = SortKey>,
    {
        let keys: Vec<SortKey> = items.into_iter().collect();
        if keys.is_empty() {
            OrderingSpec::Any
        } else {
            OrderingSpec::Required(keys)
        }
    }

    pub(crate) fn satisfies(&self, required: &OrderingSpec) -> bool {
        match required {
            OrderingSpec::Any => true,
            OrderingSpec::Required(req_keys) => {
                if let OrderingSpec::Required(my_keys) = self {
                    // Provided ordering must be a prefix-or-equal match.
                    my_keys.len() >= req_keys.len()
                        && my_keys.iter().zip(req_keys).all(|(m, r)| m == r)
                } else {
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(column: u32, asc: bool) -> SortKey {
        SortKey {
            column: ColumnId(column),
            asc,
            nulls_first: false,
        }
    }

    #[test]
    fn any_is_satisfied_by_anything() {
        assert!(OrderingSpec::Any.satisfies(&OrderingSpec::Any));
        assert!(OrderingSpec::Required(vec![key(1, true)]).satisfies(&OrderingSpec::Any));
    }

    #[test]
    fn required_needs_prefix_match() {
        let provided = OrderingSpec::Required(vec![key(1, true), key(2, false)]);
        assert!(provided.satisfies(&OrderingSpec::Required(vec![key(1, true)])));
        assert!(!OrderingSpec::Any.satisfies(&OrderingSpec::Required(vec![key(1, true)])));
        assert!(
            !OrderingSpec::Required(vec![key(2, true)])
                .satisfies(&OrderingSpec::Required(vec![key(1, true)]))
        );
    }

    #[test]
    fn from_sort_keys_empty_is_any() {
        assert_eq!(OrderingSpec::from_sort_keys(vec![]), OrderingSpec::Any);
    }
}
