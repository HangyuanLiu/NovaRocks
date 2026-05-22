//! PhysicalDistribution: the distribution enforcer node.
//! Output: whatever its embedded spec says. Required: one Any child.

use crate::sql::optimizer::operator::PhysicalDistributionOp;
use crate::sql::optimizer::property::{OrderingSpec, PhysicalPropertySet};

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalDistributionOp {
    fn derive_output(&self, _children: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        PhysicalPropertySet {
            distribution: self.spec.clone(),
            ordering: OrderingSpec::Any,
        }
    }
}

impl DeriveRequired for PhysicalDistributionOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::any()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::operator::PhysicalDistributionOp;
    use crate::sql::optimizer::property::{DistributionSpec, HashSource};

    #[test]
    fn distribution_enforcer_outputs_required_source() {
        let op = PhysicalDistributionOp {
            spec: DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]),
        };
        let props = op.derive_output(&[]);
        match props.distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(source, HashSource::ShuffleJoin);
                assert_eq!(cols, vec![ColumnId(1), ColumnId(2)]);
            }
            other => panic!("expected ShuffleJoin enforcer output, got {other:?}"),
        }
    }
}
