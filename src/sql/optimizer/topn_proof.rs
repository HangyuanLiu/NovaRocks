use crate::sql::analysis::{ExprKind, ProjectItem, SortItem, TypedExpr};
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::property::{EquivalenceClasses, SortKey, typed_expr_to_column_id};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TopNWindow {
    pub(crate) limit: i64,
    pub(crate) offset: i64,
}

impl TopNWindow {
    pub(crate) fn from_limit_offset(limit: Option<i64>, offset: Option<i64>) -> Option<Self> {
        let limit = limit?;
        if limit < 0 {
            return None;
        }
        let offset = offset.unwrap_or(0);
        if offset < 0 {
            return None;
        }
        Some(Self { limit, offset })
    }

    pub(crate) fn end_exclusive(self) -> Option<i64> {
        self.offset.checked_add(self.limit)
    }

    pub(crate) fn covers(self, needed: Self) -> bool {
        let Some(self_end) = self.end_exclusive() else {
            return false;
        };
        let Some(needed_end) = needed.end_exclusive() else {
            return false;
        };
        self.offset <= needed.offset && self_end >= needed_end
    }
}

pub(crate) fn sort_items_to_keys(items: &[SortItem]) -> Option<Vec<SortKey>> {
    items
        .iter()
        .map(|item| {
            typed_expr_to_column_id(&item.expr).map(|column| SortKey {
                column,
                asc: item.asc,
                nulls_first: item.nulls_first,
            })
        })
        .collect()
}

pub(crate) fn sort_keys_equivalent(
    left: &[SortKey],
    right: &[SortKey],
    equivalences: Option<&EquivalenceClasses>,
) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter().zip(right).all(|(l, r)| {
        l.asc == r.asc
            && l.nulls_first == r.nulls_first
            && columns_equivalent(l.column, r.column, equivalences)
    })
}

pub(crate) fn ordering_covers(
    provided: &[SortKey],
    required: &[SortKey],
    equivalences: Option<&EquivalenceClasses>,
) -> bool {
    provided.len() >= required.len()
        && provided
            .iter()
            .take(required.len())
            .zip(required)
            .all(|(p, r)| {
                p.asc == r.asc
                    && p.nulls_first == r.nulls_first
                    && columns_equivalent(p.column, r.column, equivalences)
            })
}

pub(crate) fn columns_equivalent(
    left: ColumnId,
    right: ColumnId,
    equivalences: Option<&EquivalenceClasses>,
) -> bool {
    if left == right {
        return true;
    }
    equivalences
        .and_then(|classes| classes.class_containing(left))
        .map(|class| class.contains(right))
        .unwrap_or(false)
}

pub(crate) fn pure_project_column_remap(items: &[ProjectItem]) -> Vec<(ColumnId, ColumnId)> {
    items
        .iter()
        .filter_map(|item| {
            let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind else {
                return None;
            };
            if *column_id == ColumnId::UNSET || item.output_column_id == ColumnId::UNSET {
                return None;
            }
            Some((item.output_column_id, *column_id))
        })
        .collect()
}

pub(crate) fn remap_sort_items_through_project(
    items: &[SortItem],
    project_items: &[ProjectItem],
) -> Option<Vec<SortItem>> {
    let remap = pure_project_column_remap(project_items);
    items
        .iter()
        .map(|item| {
            let output_col = typed_expr_to_column_id(&item.expr)?;
            let input_col = remap
                .iter()
                .find_map(|(out, input)| (*out == output_col).then_some(*input))?;
            let mut remapped = item.clone();
            remapped.expr = TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: input_col,
                    qualifier: None,
                    column: format!("{}", input_col),
                },
                data_type: item.expr.data_type.clone(),
                nullable: item.expr.nullable,
            };
            Some(remapped)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::analysis::{LiteralValue, ProjectItem};
    use arrow::datatypes::DataType;

    fn col(id: u32, name: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: ColumnId(id),
                qualifier: None,
                column: name.to_string(),
            },
            data_type: DataType::Int64,
            nullable: true,
        }
    }

    fn sort_item(id: u32, asc: bool, nulls_first: bool) -> SortItem {
        SortItem {
            expr: col(id, &format!("c{id}")),
            asc,
            nulls_first,
        }
    }

    #[test]
    fn topn_window_requires_finite_non_negative_limit_and_offset() {
        assert_eq!(
            TopNWindow::from_limit_offset(Some(10), Some(2)),
            Some(TopNWindow {
                limit: 10,
                offset: 2
            })
        );
        assert_eq!(TopNWindow::from_limit_offset(None, Some(2)), None);
        assert_eq!(TopNWindow::from_limit_offset(Some(-1), Some(0)), None);
        assert_eq!(TopNWindow::from_limit_offset(Some(1), Some(-1)), None);
    }

    #[test]
    fn topn_window_covers_required_range() {
        let inner = TopNWindow {
            limit: 20,
            offset: 0,
        };
        let outer = TopNWindow {
            limit: 5,
            offset: 10,
        };
        assert!(inner.covers(outer));
        assert!(!outer.covers(inner));
    }

    #[test]
    fn sort_keys_use_equivalence_classes() {
        let mut eq = EquivalenceClasses::default();
        eq.merge_pair(ColumnId(1), ColumnId(2));
        let left = sort_items_to_keys(&[sort_item(1, true, false)]).unwrap();
        let right = sort_items_to_keys(&[sort_item(2, true, false)]).unwrap();
        assert!(sort_keys_equivalent(&left, &right, Some(&eq)));
    }

    #[test]
    fn sort_keys_reject_direction_or_null_order_mismatch() {
        let asc = sort_items_to_keys(&[sort_item(1, true, false)]).unwrap();
        let desc = sort_items_to_keys(&[sort_item(1, false, false)]).unwrap();
        let nulls_first = sort_items_to_keys(&[sort_item(1, true, true)]).unwrap();
        assert!(!sort_keys_equivalent(&asc, &desc, None));
        assert!(!sort_keys_equivalent(&asc, &nulls_first, None));
    }

    #[test]
    fn project_remap_accepts_column_refs_only() {
        let project_items = vec![
            ProjectItem {
                expr: col(1, "a"),
                output_name: "x".to_string(),
                output_column_id: ColumnId(10),
            },
            ProjectItem {
                expr: TypedExpr {
                    kind: ExprKind::Literal(LiteralValue::Int(7)),
                    data_type: DataType::Int64,
                    nullable: false,
                },
                output_name: "lit".to_string(),
                output_column_id: ColumnId(11),
            },
        ];

        assert_eq!(
            pure_project_column_remap(&project_items),
            vec![(ColumnId(10), ColumnId(1))]
        );
        assert!(
            remap_sort_items_through_project(&[sort_item(10, true, false)], &project_items)
                .is_some()
        );
        assert!(
            remap_sort_items_through_project(&[sort_item(11, true, false)], &project_items)
                .is_none()
        );
    }
}
