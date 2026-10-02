// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Membership inherits only properties carried by its exact probe slots.

use super::{DeriveOutput, DeriveRequired};
use crate::optimizer::property::{DistributionSpec, OrderingSpec, PhysicalPropertySet};
use crate::optimizer::scalar::ScalarArena;
use crate::planner::membership::PlanMembershipNode;
use novarocks_physical_plan::MembershipDistribution;

impl DeriveOutput for PlanMembershipNode {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        children
            .first()
            .copied()
            .cloned()
            .unwrap_or_else(PhysicalPropertySet::any)
    }
}

impl DeriveRequired for PlanMembershipNode {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        parent: &PhysicalPropertySet,
        _num_children: usize,
    ) -> Vec<PhysicalPropertySet> {
        match self.distribution {
            MembershipDistribution::Singleton => {
                vec![PhysicalPropertySet::gather(), PhysicalPropertySet::gather()]
            }
            MembershipDistribution::BroadcastBuild => {
                let is_probe = |id| {
                    id != self.result.column_id
                        && self
                            .output_columns
                            .iter()
                            .any(|column| column.column_id == id)
                };
                let mut probe = parent.clone();
                if matches!(&probe.distribution, DistributionSpec::HashPartitioned { cols, .. } if !cols.iter().all(|id| is_probe(*id)))
                    || probe.distribution == DistributionSpec::Broadcast
                {
                    // A fresh result does not exist below membership; replication
                    // also belongs above it rather than duplicating probe evaluation.
                    probe.distribution = DistributionSpec::Any;
                }
                if matches!(&probe.ordering, OrderingSpec::Required(keys) if !keys.iter().all(|key| is_probe(key.column)))
                {
                    probe.ordering = OrderingSpec::Any;
                }
                vec![probe, PhysicalPropertySet::broadcast()]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column_id::ColumnId;
    use crate::common::OutputColumn;
    use crate::optimizer::property::SortKey;
    use arrow::datatypes::DataType;
    use novarocks_physical_plan::MembershipComparison;

    fn op(distribution: MembershipDistribution) -> PlanMembershipNode {
        let probe = OutputColumn {
            column_id: ColumnId(1),
            name: "probe".into(),
            data_type: DataType::Utf8,
            nullable: true,
            is_internal: true,
        };
        let result = OutputColumn {
            column_id: ColumnId(3),
            name: "result".into(),
            data_type: DataType::Boolean,
            nullable: true,
            is_internal: true,
        };
        PlanMembershipNode {
            probe: probe.column_id,
            build: ColumnId(2),
            result: result.clone(),
            output_columns: vec![probe, result],
            negated: false,
            comparison: MembershipComparison::JsonInListV1,
            distribution,
        }
    }

    #[test]
    fn membership_properties_preserve_probe_order_and_exact_two_child_requirements() {
        let op = op(MembershipDistribution::BroadcastBuild);
        let arena = ScalarArena::new();
        let probe = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_join([op.probe]),
            ordering: OrderingSpec::Required(vec![SortKey {
                column: op.probe,
                asc: true,
                nulls_first: false,
            }]),
        };
        assert_eq!(
            op.derive_output(&arena, &[&probe, &PhysicalPropertySet::broadcast()]),
            probe
        );
        assert_eq!(
            op.derive_required(&arena, &probe, 2),
            vec![probe, PhysicalPropertySet::broadcast()]
        );
        assert_eq!(
            self::op(MembershipDistribution::Singleton).derive_required(
                &arena,
                &PhysicalPropertySet::any(),
                2
            ),
            vec![PhysicalPropertySet::gather(), PhysicalPropertySet::gather()]
        );
    }

    #[test]
    fn membership_fresh_result_partition_and_sort_stay_above_probe() {
        let op = op(MembershipDistribution::BroadcastBuild);
        let required = PhysicalPropertySet {
            distribution: DistributionSpec::shuffle_agg([op.result.column_id]),
            ordering: OrderingSpec::Required(vec![SortKey {
                column: op.result.column_id,
                asc: true,
                nulls_first: true,
            }]),
        };
        assert_eq!(
            op.derive_required(&ScalarArena::new(), &required, 2),
            vec![PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()]
        );
        assert_eq!(
            op.derive_required(&ScalarArena::new(), &PhysicalPropertySet::broadcast(), 2),
            vec![PhysicalPropertySet::any(), PhysicalPropertySet::broadcast()]
        );
    }
}
