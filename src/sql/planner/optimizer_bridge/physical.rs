use crate::sql::optimizer::OptimizerPhysicalNode;
use crate::sql::planner::PhysicalPlanNode;

pub(crate) fn optimizer_physical_to_plan(
    _root: &OptimizerPhysicalNode,
) -> Result<PhysicalPlanNode, String> {
    Err("Bridge 2a is not implemented".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::common::OutputColumn;
    use crate::sql::optimizer::operator::{Operator, ValuesOp};
    use crate::sql::optimizer::physical_tree::{OptimizerPhysicalNode, PlanExecutionProps};
    use crate::sql::optimizer::property::PhysicalPropertySet;
    use crate::sql::optimizer::scalar::ScalarArena;
    use crate::sql::optimizer::statistics::{Confidence, Statistics};
    use crate::sql::planner::PhysicalPlanKind;
    use std::sync::Arc;

    fn attach_arena(mut node: OptimizerPhysicalNode) -> OptimizerPhysicalNode {
        node.execution_props.scalar_arena = Some(Arc::new(ScalarArena::new()));
        node
    }

    fn values_node() -> OptimizerPhysicalNode {
        attach_arena(OptimizerPhysicalNode {
            op: Operator::PhysicalValues(ValuesOp {
                rows: vec![],
                columns: vec![OutputColumn {
                    column_id: ColumnId::new_for_test(1),
                    name: "v".to_string(),
                    data_type: arrow::datatypes::DataType::Int32,
                    nullable: false,
                    is_internal: false,
                }],
            }),
            children: vec![],
            stats: Statistics {
                output_row_count: 1.0,
                row_count_confidence: Confidence::Exact,
                ..Default::default()
            },
            explain_stats: Default::default(),
            output_columns: vec![],
            execution_props: PlanExecutionProps {
                output_property: PhysicalPropertySet::gather(),
                child_output_properties: vec![],
                join_distribution: None,
                scalar_arena: None,
            },
            build_runtime_filters: vec![],
            probe_runtime_filters: vec![],
        })
    }

    #[test]
    fn bridge_converts_values_without_optimizer_types() {
        let physical = optimizer_physical_to_plan(&values_node()).expect("bridge should convert");
        assert!(matches!(physical.kind, PhysicalPlanKind::Values(_)));
        assert!(physical.probe_runtime_filters.is_empty());
        assert_eq!(physical.stats.output_row_count, 1.0);
    }
}
