//! Optimizer physical operator tree extracted from the Memo after optimization.

use std::sync::Arc;

use crate::sql::common::OutputColumn;
use crate::sql::optimizer::cost::BroadcastDecision;
use crate::sql::optimizer::operator::Operator;
use crate::sql::optimizer::property::PhysicalPropertySet;
use crate::sql::optimizer::representation::RepresentationProperty;
use crate::sql::optimizer::scalar::ScalarArena;
use crate::sql::optimizer::statistics::{CostEstimate, Statistics};

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
    pub representation_property: RepresentationProperty,
    /// Shared scalar arena that owns all `ScalarId` handles referenced by this
    /// optimizer physical tree. Attached after extraction so codegen can materialize the
    /// scalar handles at its TypedExpr boundary.
    pub scalar_arena: Option<Arc<ScalarArena>>,
}

impl Default for PlanExecutionProps {
    fn default() -> Self {
        Self {
            output_property: PhysicalPropertySet::any(),
            child_output_properties: Vec::new(),
            join_distribution: None,
            representation_property: RepresentationProperty::default(),
            scalar_arena: None,
        }
    }
}

impl PlanExecutionProps {
    pub(crate) fn with_empty_representation(mut self) -> Self {
        self.representation_property = RepresentationProperty::default();
        self
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct OptimizerExplainStats {
    pub cost_estimate: Option<CostEstimate>,
    pub broadcast_decision: Option<BroadcastDecision>,
}

/// A node in the optimizer physical operator tree produced by `extract_best`.
#[derive(Clone, Debug)]
pub(crate) struct OptimizerPhysicalNode {
    pub op: Operator,
    pub children: Vec<OptimizerPhysicalNode>,
    pub stats: Statistics,
    pub explain_stats: OptimizerExplainStats,
    pub output_columns: Vec<OutputColumn>,
    pub execution_props: PlanExecutionProps,
    /// OQ-5: build-side runtime filters produced here (hash joins only).
    pub build_runtime_filters: Vec<crate::sql::optimizer::runtime_filter_pass::RuntimeFilterDesc>,
    /// OQ-5: probe-side runtime filters consumed here.
    pub probe_runtime_filters: Vec<crate::sql::optimizer::runtime_filter_pass::RuntimeFilterProbe>,
}

pub(crate) fn attach_scalar_arena(root: &mut OptimizerPhysicalNode, arena: Arc<ScalarArena>) {
    root.execution_props.scalar_arena = Some(Arc::clone(&arena));
    for child in &mut root.children {
        attach_scalar_arena(child, Arc::clone(&arena));
    }
}

#[cfg(test)]
mod rf_field_tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::representation::{
        ColumnRepresentationSet, LogicalColumn, PhysicalRepresentation, PhysicalSlot,
        RepresentationProperty,
    };
    use crate::sql::optimizer::runtime_filter_pass::{RuntimeFilterDesc, RuntimeFilterProbe};
    use crate::sql::optimizer::scalar::ScalarArena;
    use arrow::datatypes::DataType;

    #[test]
    fn physical_node_carries_rf_annotations() {
        let mut scalars = ScalarArena::new();
        let mut node = OptimizerPhysicalNode {
            op: make_test_op(),
            children: vec![],
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![],
            execution_props: crate::sql::optimizer::physical_tree::PlanExecutionProps::default(),
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        };
        assert!(node.build_runtime_filters.is_empty());
        node.build_runtime_filters
            .push(RuntimeFilterDesc::placeholder(&mut scalars, 0));
        node.probe_runtime_filters
            .push(RuntimeFilterProbe::placeholder(&mut scalars, 0));
        assert_eq!(node.build_runtime_filters.len(), 1);
        assert_eq!(node.probe_runtime_filters.len(), 1);
    }

    #[test]
    fn physical_node_carries_execution_properties() {
        let node = OptimizerPhysicalNode {
            op: make_test_op(),
            children: vec![],
            stats: Statistics {
                output_row_count: 1.0,
                column_statistics: Default::default(),
                ..Default::default()
            },
            explain_stats: crate::sql::optimizer::physical_tree::OptimizerExplainStats::default(),
            output_columns: vec![],
            execution_props: PlanExecutionProps {
                output_property: crate::sql::optimizer::property::PhysicalPropertySet::broadcast(),
                child_output_properties: vec![
                    crate::sql::optimizer::property::PhysicalPropertySet::any(),
                ],
                join_distribution: Some(JoinExecutionDistribution::Broadcast),
                representation_property: RepresentationProperty::default(),
                scalar_arena: None,
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

    #[test]
    fn execution_props_can_clear_representation_property_only() {
        let scalar_arena = Arc::new(ScalarArena::new());
        let props = PlanExecutionProps {
            output_property: crate::sql::optimizer::property::PhysicalPropertySet::broadcast(),
            child_output_properties: vec![
                crate::sql::optimizer::property::PhysicalPropertySet::any(),
            ],
            join_distribution: Some(JoinExecutionDistribution::Broadcast),
            scalar_arena: Some(Arc::clone(&scalar_arena)),
            representation_property: test_representation_property(),
        };

        let cleared = props.with_empty_representation();

        assert!(cleared.representation_property.is_empty());
        assert_eq!(
            cleared.output_property.distribution,
            crate::sql::optimizer::property::DistributionSpec::Broadcast
        );
        assert_eq!(
            cleared.child_output_properties,
            vec![crate::sql::optimizer::property::PhysicalPropertySet::any()]
        );
        assert_eq!(
            cleared.join_distribution,
            Some(JoinExecutionDistribution::Broadcast)
        );
        assert!(Arc::ptr_eq(
            cleared.scalar_arena.as_ref().expect("scalar arena"),
            &scalar_arena
        ));
    }

    fn test_representation_property() -> RepresentationProperty {
        let mut property = RepresentationProperty::default();
        let column_id = ColumnId::new_for_test(5);
        property.insert(ColumnRepresentationSet {
            logical_column: LogicalColumn {
                column_id,
                name: "city".to_string(),
                logical_type: DataType::Utf8,
                nullable: true,
            },
            current_slot: PhysicalSlot {
                column_id,
                name: "__nr_dict_tbl_city".to_string(),
                data_type: DataType::Int32,
                nullable: true,
            },
            representations: vec![PhysicalRepresentation::Plain {
                logical_type: DataType::Utf8,
            }],
        });
        property
    }

    fn make_test_op() -> Operator {
        use crate::sql::optimizer::operator::ValuesOp;
        Operator::PhysicalValues(ValuesOp {
            rows: vec![],
            columns: vec![],
        })
    }
}
