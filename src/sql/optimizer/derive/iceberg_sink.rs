//! Iceberg table sink (IW-7, path B): terminal writer that requires its input
//! hash-partitioned by the table's partition key columns, so each writer owns
//! whole partitions. Unpartitioned table => `Any` (no shuffle).

use crate::sql::optimizer::operator::PhysicalIcebergSinkOp;
use crate::sql::optimizer::property::{DistributionSpec, OrderingSpec, PhysicalPropertySet};

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalIcebergSinkOp {
    fn derive_output(&self, children_outputs: &[&PhysicalPropertySet]) -> PhysicalPropertySet {
        // Terminal sink: delivers whatever its single input delivers.
        children_outputs
            .first()
            .map(|c| (*c).clone())
            .unwrap_or_else(PhysicalPropertySet::any)
    }
}

impl DeriveRequired for PhysicalIcebergSinkOp {
    fn derive_required(
        &self,
        _parent: &PhysicalPropertySet,
        _num_children: usize,
    ) -> Vec<PhysicalPropertySet> {
        // Require the input shuffled by the table's partition key columns so a
        // partition's rows all land on one writer. Empty partition keys
        // (unpartitioned table) => no distribution requirement.
        let distribution = if self.partition_key_column_ids.is_empty() {
            DistributionSpec::Any
        } else {
            DistributionSpec::shuffle_agg(self.partition_key_column_ids.iter().copied())
        };
        vec![PhysicalPropertySet {
            distribution,
            ordering: OrderingSpec::Any,
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::column_id::ColumnId;
    use crate::sql::optimizer::property::HashSource;

    #[test]
    fn partitioned_sink_requires_hash_partitioned_on_partition_keys() {
        let op = PhysicalIcebergSinkOp {
            target_table_id: 1,
            partition_key_column_ids: vec![ColumnId(3), ColumnId(5)],
        };
        let reqs = op.derive_required(&PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        match &reqs[0].distribution {
            DistributionSpec::HashPartitioned { cols, source } => {
                assert_eq!(*source, HashSource::ShuffleAgg);
                assert_eq!(cols.as_slice(), &[ColumnId(3), ColumnId(5)]);
            }
            other => panic!("expected HashPartitioned([c3,c5]), got {other:?}"),
        }
        assert!(matches!(reqs[0].ordering, OrderingSpec::Any));
    }

    #[test]
    fn unpartitioned_sink_requires_any() {
        let op = PhysicalIcebergSinkOp {
            target_table_id: 1,
            partition_key_column_ids: vec![],
        };
        let reqs = op.derive_required(&PhysicalPropertySet::any(), 1);
        assert_eq!(reqs.len(), 1);
        assert_eq!(reqs[0].distribution, DistributionSpec::Any);
    }
}
