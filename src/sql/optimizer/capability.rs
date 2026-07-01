#![allow(dead_code)]
//! Operator capability contract for the low-cardinality dictionary
//! representation model.
//!
//! P1 populates a [`RepresentationProperty`] for `PhysicalScan` only. This
//! module extends that to interior operators via [`propagate_representation`],
//! a pure function that computes an operator's output representation from its
//! children's already-computed representation properties, per a conservative
//! capability matrix.
//!
//! The contract is deliberately conservative: the default is to DROP (return
//! an empty property), which the later planning pass reads as a materialization
//! (decode) boundary. A dictionary representation is only preserved when the
//! operator provably keeps the *same* dictionary domain applicable to the
//! *same* logical value — a pure column passthrough, a group-by on a dictionary
//! column, or `min`/`max` over an order-preserving dictionary domain. Anything
//! else drops. The child's key is never leaked upward: every preserved set is
//! re-keyed to the parent operator's own output `ColumnId`. Domains are never
//! merged or translated across columns, and no SQL type is changed here — this
//! is optimizer metadata only.

use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::representation::RepresentationProperty;
use crate::sql::optimizer::scalar::{ScalarArena, ScalarNode};

/// Compute an operator's output [`RepresentationProperty`] from its children's
/// already-computed representation properties.
///
/// `child_props` is index-aligned with the operator's children. Unknown /
/// unhandled operators conservatively return an empty property (a
/// materialization boundary).
pub(crate) fn propagate_representation(
    op: &Operator,
    child_props: &[&RepresentationProperty],
    arena: &ScalarArena,
) -> RepresentationProperty {
    match op {
        // Scan is the representation source: read straight from the scan plan.
        Operator::PhysicalScan(scan) => RepresentationProperty::from_scan(scan),

        // Project preserves a dictionary representation only for a pure column
        // passthrough (`output_col := ColumnRef(child_col)`), re-keyed to the
        // project's output column id. Any computed expression drops.
        Operator::PhysicalProject(project) => {
            let Some(child) = child_props.first() else {
                return RepresentationProperty::default();
            };
            let mut property = RepresentationProperty::default();
            for item in &project.items {
                let ScalarNode::ColumnRef(child_col) = arena.node(item.expr) else {
                    continue;
                };
                let Some(set) = child.get(*child_col) else {
                    continue;
                };
                if set.dictionary_representation().is_none() {
                    continue;
                }
                property.insert(set.remapped_to_output(
                    item.output_column_id,
                    &item.output_name,
                    arena.nullable(item.expr),
                ));
            }
            property
        }

        // Filter selects rows; its output columns are identical to the child's,
        // so the child's representation passes through unchanged (same keys).
        Operator::PhysicalFilter(_) => child_props
            .first()
            .map(|child| (*child).clone())
            .unwrap_or_default(),

        // Aggregate preserves a dictionary representation for:
        //   - a group-by key that is a pure `ColumnRef` on a dict column, and
        //   - `min`/`max` over an order-preserving dict domain (a single
        //     `ColumnRef` argument).
        // Everything else (count/sum/avg/computed group keys/...) drops. Each
        // preserved set is re-keyed to the aggregate's own output column id.
        Operator::PhysicalHashAggregate(agg) => {
            let Some(child) = child_props.first() else {
                return RepresentationProperty::default();
            };
            let mut property = RepresentationProperty::default();

            // Invariant: group_by[i] is index-aligned with
            // output_layout.group_key_columns[i] (the aggregate builders
            // construct them in lockstep); a length mismatch safely drops via
            // `.get(index)`.
            for (index, key) in agg.group_by.iter().enumerate() {
                let Some(out_col) = agg.output_layout.group_key_columns.get(index) else {
                    continue;
                };
                let ScalarNode::ColumnRef(child_col) = arena.node(*key) else {
                    continue;
                };
                let Some(set) = child.get(*child_col) else {
                    continue;
                };
                if set.dictionary_representation().is_none() {
                    continue;
                }
                property.insert(set.remapped_to_output(
                    out_col.column_id,
                    &out_col.name,
                    out_col.nullable,
                ));
            }

            // Aggregate calls, index-aligned with output_layout.aggregate_columns.
            for (index, spec) in agg.aggregates.iter().enumerate() {
                let Some(out_col) = agg.output_layout.aggregate_columns.get(index) else {
                    continue;
                };
                let is_min_or_max =
                    spec.name.eq_ignore_ascii_case("min") || spec.name.eq_ignore_ascii_case("max");
                if !is_min_or_max || spec.args.len() != 1 {
                    continue;
                }
                let ScalarNode::ColumnRef(child_col) = arena.node(spec.args[0]) else {
                    continue;
                };
                let Some(set) = child.get(*child_col) else {
                    continue;
                };
                let Some(dict) = set.dictionary_representation() else {
                    continue;
                };
                // min/max is only representation-preserving when the dictionary
                // ids order the same way as the logical values.
                if !dict.domain.order_preserving {
                    continue;
                }
                property.insert(set.remapped_to_output(
                    out_col.column_id,
                    &out_col.name,
                    out_col.nullable,
                ));
            }

            property
        }

        // Conservative default: drop (a future materialization boundary).
        _ => RepresentationProperty::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::OutputColumn;
    use crate::sql::optimizer::operator::{
        AggMode, AggregateOutputLayout, FilterOp, Operator, PhysicalHashAggregateOp, ProjectOp,
        ScalarAggregateSpec, ScalarProjectItem,
    };
    use crate::sql::optimizer::representation::{
        RepresentationProperty, test_dict_representation_set,
    };
    use crate::sql::optimizer::scalar::{ScalarArena, ScalarNode};
    use arrow::datatypes::DataType;

    /// Build a project with a single item over `child_expr` producing
    /// `output_column_id`.
    fn project_single(items: Vec<ScalarProjectItem>) -> Operator {
        Operator::PhysicalProject(ProjectOp {
            items,
            output_qualifier: None,
        })
    }

    fn out_col(id: ColumnId, name: &str, ty: DataType, nullable: bool) -> OutputColumn {
        OutputColumn {
            column_id: id,
            name: name.to_string(),
            data_type: ty,
            nullable,
            is_internal: false,
        }
    }

    /// Build a single-phase `PhysicalHashAggregate` from index-aligned
    /// group-by keys / group-key output columns and aggregate specs / aggregate
    /// output columns.
    fn agg_over(
        group_by: Vec<crate::sql::optimizer::scalar::ScalarId>,
        group_key_columns: Vec<OutputColumn>,
        aggregates: Vec<ScalarAggregateSpec>,
        aggregate_columns: Vec<OutputColumn>,
    ) -> Operator {
        let is_merge = vec![false; aggregates.len()];
        let output_columns: Vec<OutputColumn> = group_key_columns
            .iter()
            .chain(aggregate_columns.iter())
            .cloned()
            .collect();
        let output_layout = AggregateOutputLayout::new(group_key_columns, aggregate_columns);
        Operator::PhysicalHashAggregate(PhysicalHashAggregateOp {
            mode: AggMode::Single,
            group_by,
            aggregates,
            output_layout,
            output_columns,
            is_merge,
        })
    }

    #[test]
    fn project_passthrough_column_ref_preserves_dictionary_representation() {
        let child_logical = ColumnId::new_for_test(5);
        let child_slot = ColumnId::new_for_test(6);
        let output_col = ColumnId::new_for_test(7);

        let mut child = RepresentationProperty::default();
        child.insert(test_dict_representation_set(
            child_logical,
            child_slot,
            true,
        ));

        let mut arena = ScalarArena::new();
        let col_ref = arena.intern(ScalarNode::ColumnRef(child_logical), DataType::Utf8, true);

        let op = project_single(vec![ScalarProjectItem {
            expr: col_ref,
            output_name: "city_out".to_string(),
            output_column_id: output_col,
            expr_display: None,
        }]);

        let child_props: Vec<&RepresentationProperty> = vec![&child];
        let out = propagate_representation(&op, &child_props, &arena);

        // The child's key must NOT leak: the representation is re-keyed to the
        // project's output column id.
        assert!(out.get(child_logical).is_none());
        let set = out.get(output_col).expect("output representation exists");
        assert_eq!(set.logical_column.column_id, output_col);
        assert_eq!(set.logical_column.name, "city_out");
        assert_eq!(set.current_slot.column_id, output_col);
        assert!(set.dictionary_representation().is_some());
        assert!(out.has_dictionary_representation());
    }

    #[test]
    fn project_computed_expression_drops_dictionary_representation() {
        let child_logical = ColumnId::new_for_test(5);
        let child_slot = ColumnId::new_for_test(6);
        let output_col = ColumnId::new_for_test(7);

        let mut child = RepresentationProperty::default();
        child.insert(test_dict_representation_set(
            child_logical,
            child_slot,
            true,
        ));

        let mut arena = ScalarArena::new();
        let col_ref = arena.intern(ScalarNode::ColumnRef(child_logical), DataType::Utf8, true);
        // A computed expression over the dict column: upper(city).
        let computed = arena.intern(
            ScalarNode::FunctionCall {
                name: "upper".to_string(),
                args: vec![col_ref],
                distinct: false,
            },
            DataType::Utf8,
            true,
        );

        let op = project_single(vec![ScalarProjectItem {
            expr: computed,
            output_name: "city_upper".to_string(),
            output_column_id: output_col,
            expr_display: None,
        }]);

        let child_props: Vec<&RepresentationProperty> = vec![&child];
        let out = propagate_representation(&op, &child_props, &arena);

        assert!(out.is_empty());
        assert!(!out.has_dictionary_representation());
    }

    #[test]
    fn unknown_operator_defaults_to_empty() {
        // A physical limit is not handled by the capability matrix, so it must
        // drop conservatively.
        use crate::sql::optimizer::operator::LimitOp;
        let op = Operator::PhysicalLimit(LimitOp {
            limit: Some(1),
            offset: Some(0),
        });
        let arena = ScalarArena::new();
        let child_logical = ColumnId::new_for_test(5);
        let child_slot = ColumnId::new_for_test(6);
        let mut child = RepresentationProperty::default();
        child.insert(test_dict_representation_set(
            child_logical,
            child_slot,
            true,
        ));
        let child_props: Vec<&RepresentationProperty> = vec![&child];

        let out = propagate_representation(&op, &child_props, &arena);
        assert!(out.is_empty());
        assert!(!out.has_dictionary_representation());
    }

    #[test]
    fn filter_passes_child_representation_through_unchanged() {
        // Filter selects rows; its output columns are identical to the child's,
        // so the child's representation passes through unchanged (same keys).
        let child_logical = ColumnId::new_for_test(5);
        let child_slot = ColumnId::new_for_test(6);

        let mut child = RepresentationProperty::default();
        child.insert(test_dict_representation_set(
            child_logical,
            child_slot,
            true,
        ));

        let mut arena = ScalarArena::new();
        // A predicate referencing the dict column; its shape is irrelevant to
        // the filter arm, which does not remap columns.
        let predicate = arena.intern(
            ScalarNode::ColumnRef(child_logical),
            DataType::Boolean,
            true,
        );
        let op = Operator::PhysicalFilter(FilterOp { predicate });

        let child_props: Vec<&RepresentationProperty> = vec![&child];
        let out = propagate_representation(&op, &child_props, &arena);

        let set = out
            .get(child_logical)
            .expect("child representation passes through");
        assert_eq!(set.logical_column.column_id, child_logical);
        assert_eq!(set.current_slot.column_id, child_slot);
        assert!(set.dictionary_representation().is_some());
        assert!(out.has_dictionary_representation());
    }

    #[test]
    fn aggregate_group_by_dict_key_preserves_representation() {
        // GROUP BY on a dictionary column keeps the same dictionary domain
        // applicable to the group-key output column (re-keyed to the output id).
        let child_logical = ColumnId::new_for_test(5);
        let child_slot = ColumnId::new_for_test(6);
        let group_out = ColumnId::new_for_test(7);
        let cnt_out = ColumnId::new_for_test(8);

        let mut child = RepresentationProperty::default();
        child.insert(test_dict_representation_set(
            child_logical,
            child_slot,
            true,
        ));

        let mut arena = ScalarArena::new();
        let key = arena.intern(ScalarNode::ColumnRef(child_logical), DataType::Utf8, true);

        let op = agg_over(
            vec![key],
            vec![out_col(group_out, "city", DataType::Utf8, true)],
            vec![ScalarAggregateSpec {
                output_column_id: cnt_out,
                name: "count".to_string(),
                args: vec![],
                distinct: false,
                order_by: vec![],
            }],
            vec![out_col(cnt_out, "cnt", DataType::Int64, false)],
        );

        let child_props: Vec<&RepresentationProperty> = vec![&child];
        let out = propagate_representation(&op, &child_props, &arena);

        // Child key must not leak; representation is re-keyed to the group-key
        // output column id.
        assert!(out.get(child_logical).is_none());
        let set = out
            .get(group_out)
            .expect("group-by dict key representation exists");
        assert_eq!(set.logical_column.column_id, group_out);
        assert_eq!(set.current_slot.column_id, group_out);
        assert!(set.dictionary_representation().is_some());
        // The count aggregate output carries no dictionary representation.
        assert!(out.get(cnt_out).is_none());
    }

    #[test]
    fn aggregate_min_max_preserves_only_when_order_preserving() {
        // (order_preserving, expect_preserved)
        for (order_preserving, expect_preserved) in [(true, true), (false, false)] {
            for agg_name in ["min", "max"] {
                let child_logical = ColumnId::new_for_test(5);
                let child_slot = ColumnId::new_for_test(6);
                let agg_out = ColumnId::new_for_test(9);

                let mut child = RepresentationProperty::default();
                child.insert(test_dict_representation_set(
                    child_logical,
                    child_slot,
                    order_preserving,
                ));

                let mut arena = ScalarArena::new();
                let arg = arena.intern(ScalarNode::ColumnRef(child_logical), DataType::Utf8, true);

                let op = agg_over(
                    vec![],
                    vec![],
                    vec![ScalarAggregateSpec {
                        output_column_id: agg_out,
                        name: agg_name.to_string(),
                        args: vec![arg],
                        distinct: false,
                        order_by: vec![],
                    }],
                    vec![out_col(agg_out, agg_name, DataType::Utf8, true)],
                );

                let child_props: Vec<&RepresentationProperty> = vec![&child];
                let out = propagate_representation(&op, &child_props, &arena);

                if expect_preserved {
                    let set = out.get(agg_out).unwrap_or_else(|| {
                        panic!("{agg_name} over order-preserving dict must preserve")
                    });
                    assert_eq!(set.logical_column.column_id, agg_out);
                    assert_eq!(set.current_slot.column_id, agg_out);
                    assert!(set.dictionary_representation().is_some());
                    assert!(out.has_dictionary_representation());
                } else {
                    assert!(
                        out.is_empty(),
                        "{agg_name} over non-order-preserving dict must drop"
                    );
                }
                // The child key must never leak upward.
                assert!(out.get(child_logical).is_none());
            }
        }
    }

    #[test]
    fn aggregate_count_and_sum_drop_representation() {
        for agg_name in ["count", "sum"] {
            let child_logical = ColumnId::new_for_test(5);
            let child_slot = ColumnId::new_for_test(6);
            let agg_out = ColumnId::new_for_test(9);

            let mut child = RepresentationProperty::default();
            child.insert(test_dict_representation_set(
                child_logical,
                child_slot,
                true,
            ));

            let mut arena = ScalarArena::new();
            let arg = arena.intern(ScalarNode::ColumnRef(child_logical), DataType::Utf8, true);

            let op = agg_over(
                vec![],
                vec![],
                vec![ScalarAggregateSpec {
                    output_column_id: agg_out,
                    name: agg_name.to_string(),
                    args: vec![arg],
                    distinct: false,
                    order_by: vec![],
                }],
                vec![out_col(agg_out, agg_name, DataType::Int64, false)],
            );

            let child_props: Vec<&RepresentationProperty> = vec![&child];
            let out = propagate_representation(&op, &child_props, &arena);

            assert!(
                out.is_empty(),
                "{agg_name} must drop dictionary representation"
            );
            assert!(!out.has_dictionary_representation());
        }
    }
}
