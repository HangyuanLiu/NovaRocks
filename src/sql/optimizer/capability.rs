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
                    set.logical_column.nullable,
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
    use crate::sql::optimizer::operator::{Operator, ProjectOp, ScalarProjectItem};
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
}
