//! PhysicalPlan tree extracted from the Memo after optimization.

use crate::sql::analysis::OutputColumn;
use crate::sql::optimizer::operator::Operator;
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
