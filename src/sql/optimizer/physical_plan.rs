//! PhysicalPlan tree extracted from the Memo after optimization.

use crate::sql::analysis::OutputColumn;
use crate::sql::column_id::ColumnId;
use crate::sql::optimizer::derive::DeriveRequired;
use crate::sql::optimizer::operator::{Operator, PhysicalDistributionOp, PhysicalIcebergSinkOp};
use crate::sql::optimizer::property::PhysicalPropertySet;
use crate::sql::optimizer::statistics::Statistics;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinExecutionDistribution {
    Broadcast,
    Partitioned,
    Colocate,
}

#[derive(Clone, Debug)]
pub(crate) struct PlanExecutionProps {
    pub output_property: PhysicalPropertySet,
    pub child_output_properties: Vec<PhysicalPropertySet>,
    pub join_distribution: Option<JoinExecutionDistribution>,
}

impl Default for PlanExecutionProps {
    fn default() -> Self {
        Self {
            output_property: PhysicalPropertySet::any(),
            child_output_properties: Vec::new(),
            join_distribution: None,
        }
    }
}

/// A node in the physical plan tree produced by `extract_best`.
#[derive(Clone, Debug)]
pub(crate) struct PhysicalPlanNode {
    pub op: Operator,
    pub children: Vec<PhysicalPlanNode>,
    pub stats: Statistics,
    pub output_columns: Vec<OutputColumn>,
    pub execution_props: PlanExecutionProps,
    /// OQ-5: build-side runtime filters produced here (hash joins only).
    pub build_runtime_filters: Vec<crate::sql::optimizer::runtime_filter_pass::RuntimeFilterDesc>,
    /// OQ-5: probe-side runtime filters consumed here.
    pub probe_runtime_filters: Vec<crate::sql::optimizer::runtime_filter_pass::RuntimeFilterProbe>,
}

/// IW-7 (path B): wrap an optimized SELECT physical plan with a
/// `PhysicalIcebergSink` at the root. When the table is partitioned and the
/// SELECT output isn't already hash-partitioned by the partition key columns,
/// insert a `PhysicalDistribution` enforcer below the sink so each partition's
/// rows converge on a single writer. all-in-one collapses that enforcer
/// downstream (`collapse_distribution_enforcers_for_single_fragment`), so the
/// single-process path stays a single writer with no shuffle.
pub(crate) fn wrap_with_iceberg_sink(
    select_plan: PhysicalPlanNode,
    target_table_id: i64,
    partition_key_column_ids: Vec<ColumnId>,
) -> PhysicalPlanNode {
    let sink_op = PhysicalIcebergSinkOp {
        target_table_id,
        partition_key_column_ids,
    };
    let required = sink_op
        .derive_required(&PhysicalPropertySet::any(), 1)
        .into_iter()
        .next()
        .expect("iceberg sink derive_required yields one child requirement");

    let provided = select_plan.execution_props.output_property.clone();
    let child = if provided.distribution.satisfies(&required.distribution) {
        select_plan
    } else {
        let enforced_property = PhysicalPropertySet {
            distribution: required.distribution.clone(),
            ordering: provided.ordering.clone(),
        };
        PhysicalPlanNode {
            op: Operator::PhysicalDistribution(PhysicalDistributionOp {
                spec: required.distribution.clone(),
            }),
            stats: select_plan.stats.clone(),
            output_columns: select_plan.output_columns.clone(),
            execution_props: PlanExecutionProps {
                output_property: enforced_property,
                child_output_properties: vec![provided],
                join_distribution: None,
            },
            build_runtime_filters: Vec::new(),
            probe_runtime_filters: Vec::new(),
            children: vec![select_plan],
        }
    };

    let child_output_property = child.execution_props.output_property.clone();
    PhysicalPlanNode {
        op: Operator::PhysicalIcebergSink(sink_op),
        stats: child.stats.clone(),
        output_columns: child.output_columns.clone(),
        execution_props: PlanExecutionProps {
            output_property: child_output_property.clone(),
            child_output_properties: vec![child_output_property],
            join_distribution: None,
        },
        build_runtime_filters: Vec::new(),
        probe_runtime_filters: Vec::new(),
        children: vec![child],
    }
}

#[cfg(test)]
mod iceberg_sink_wrap_tests {
    use super::*;
    use crate::sql::optimizer::operator::PhysicalProjectOp;
    use crate::sql::optimizer::property::DistributionSpec;

    fn dummy_select() -> PhysicalPlanNode {
        // A minimal SELECT plan node whose output distribution is `Any`
        // (PlanExecutionProps::default()).
        PhysicalPlanNode {
            op: Operator::PhysicalProject(PhysicalProjectOp {
                items: vec![],
                output_qualifier: None,
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: 100.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![],
            execution_props: PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        }
    }

    #[test]
    fn partitioned_insert_inserts_distribution_enforcer_below_sink() {
        let plan = wrap_with_iceberg_sink(dummy_select(), 7, vec![ColumnId(2)]);
        assert!(matches!(plan.op, Operator::PhysicalIcebergSink(_)));
        assert_eq!(plan.children.len(), 1);
        match &plan.children[0].op {
            Operator::PhysicalDistribution(d) => match &d.spec {
                DistributionSpec::HashPartitioned { cols, .. } => {
                    assert_eq!(cols.as_slice(), &[ColumnId(2)]);
                }
                other => panic!("expected HashPartitioned, got {other:?}"),
            },
            other => panic!("expected PhysicalDistribution below sink, got {other:?}"),
        }
        // enforcer's child is the original SELECT.
        assert!(matches!(
            plan.children[0].children[0].op,
            Operator::PhysicalProject(_)
        ));
    }

    #[test]
    fn unpartitioned_insert_has_no_enforcer() {
        let plan = wrap_with_iceberg_sink(dummy_select(), 7, vec![]);
        assert!(matches!(plan.op, Operator::PhysicalIcebergSink(_)));
        assert_eq!(plan.children.len(), 1);
        // No enforcer: the sink's child is the SELECT directly.
        assert!(matches!(plan.children[0].op, Operator::PhysicalProject(_)));
    }
}

#[cfg(test)]
mod rf_field_tests {
    use super::*;
    use crate::sql::optimizer::runtime_filter_pass::{RuntimeFilterDesc, RuntimeFilterProbe};

    #[test]
    fn physical_node_carries_rf_annotations() {
        let mut node = PhysicalPlanNode {
            op: make_test_op(),
            children: vec![],
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![],
            execution_props: crate::sql::optimizer::physical_plan::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        assert!(node.build_runtime_filters.is_empty());
        node.build_runtime_filters
            .push(RuntimeFilterDesc::placeholder(0));
        node.probe_runtime_filters
            .push(RuntimeFilterProbe::placeholder(0));
        assert_eq!(node.build_runtime_filters.len(), 1);
        assert_eq!(node.probe_runtime_filters.len(), 1);
    }

    #[test]
    fn physical_node_carries_execution_properties() {
        let node = PhysicalPlanNode {
            op: make_test_op(),
            children: vec![],
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            output_columns: vec![],
            execution_props: PlanExecutionProps {
                output_property: crate::sql::optimizer::property::PhysicalPropertySet::broadcast(),
                child_output_properties: vec![
                    crate::sql::optimizer::property::PhysicalPropertySet::any(),
                ],
                join_distribution: Some(JoinExecutionDistribution::Broadcast),
            },
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };

        assert_eq!(
            node.execution_props.join_distribution,
            Some(JoinExecutionDistribution::Broadcast)
        );
        assert_eq!(
            node.execution_props.output_property.distribution,
            crate::sql::optimizer::property::DistributionSpec::Broadcast
        );
    }

    fn make_test_op() -> Operator {
        use crate::sql::optimizer::operator::PhysicalValuesOp;
        Operator::PhysicalValues(PhysicalValuesOp {
            rows: vec![],
            columns: vec![],
        })
    }
}
