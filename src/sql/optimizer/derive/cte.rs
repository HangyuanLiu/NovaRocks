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

//! CTEAnchor: structural wiring with two children (produce side, consume side).
//! Today's behaviour: output Any; both children required Any.
//! (CTEConsume lives in scan.rs because it's leaf-like at the property layer.
//!  CTEProduce lives in passthrough.rs because it forwards a single child.)

use crate::sql::optimizer::operator::CTEAnchorOp;
use crate::sql::optimizer::property::PhysicalPropertySet;
use crate::sql::optimizer::scalar::ScalarArena;

use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for CTEAnchorOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        _children: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        PhysicalPropertySet::any()
    }
}

impl DeriveRequired for CTEAnchorOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        _parent: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet::any(), PhysicalPropertySet::any()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cte_anchor_requires_any_for_both_children() {
        let op = CTEAnchorOp { cte_id: 7 };
        let parent_req = PhysicalPropertySet::gather();
        let scalars = ScalarArena::new();
        let child_reqs = op.derive_required(&scalars, &parent_req, 2);
        assert_eq!(child_reqs.len(), 2);
        assert_eq!(child_reqs[0], PhysicalPropertySet::any());
        assert_eq!(child_reqs[1], PhysicalPropertySet::any());
    }
}
