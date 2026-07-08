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

//! PhysicalDistribution: the distribution enforcer node.
//! Output: whatever its embedded spec says. Required: one Any child.

use crate::sql::optimizer::operator::PhysicalDistributionOp;
use crate::sql::optimizer::property::{OrderingSpec, PhysicalPropertySet};
use crate::sql::optimizer::scalar::ScalarArena;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for PhysicalDistributionOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet {
            distribution: self.spec.clone(),
            ordering: OrderingSpec::Any,
        }
    }
}

impl DeriveRequired for PhysicalDistributionOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
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
        for (spec, expected_source) in [
            (
                DistributionSpec::shuffle_agg([ColumnId(1), ColumnId(2)]),
                HashSource::ShuffleAgg,
            ),
            (
                DistributionSpec::shuffle_join([ColumnId(1), ColumnId(2)]),
                HashSource::ShuffleJoin,
            ),
        ] {
            let op = PhysicalDistributionOp { spec };
            let scalars = ScalarArena::new();
            let props = op.derive_output(&scalars, &[]);
            match props.distribution {
                DistributionSpec::HashPartitioned { cols, source } => {
                    assert_eq!(source, expected_source);
                    assert_eq!(cols, vec![ColumnId(1), ColumnId(2)]);
                }
                other => panic!("expected hash enforcer output, got {other:?}"),
            }
        }
    }
}
