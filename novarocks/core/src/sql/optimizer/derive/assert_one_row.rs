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

//! AssertOneRow — runtime guard that its input yields at most one row.
//!
//! The row count must be observed globally, so the child is required to be
//! gathered to a single instance before the assert fires (same correctness
//! argument as a global LIMIT). Output mirrors the child's output; ordering
//! requirements pass through.

use crate::sql::optimizer::operator::AssertOneRowOp;
use crate::sql::optimizer::property::{DistributionSpec, PhysicalPropertySet};
use crate::sql::optimizer::scalar::ScalarArena;

use super::passthrough::passthrough_output;
use super::{DeriveOutput, DeriveRequired};

impl DeriveOutput for AssertOneRowOp {
    fn derive_output(
        &self,
        _scalars: &ScalarArena,
        children_outputs: &[&PhysicalPropertySet],
    ) -> PhysicalPropertySet {
        passthrough_output(children_outputs)
    }
}

impl DeriveRequired for AssertOneRowOp {
    fn derive_required(
        &self,
        _scalars: &ScalarArena,
        parent_required: &PhysicalPropertySet,
        _n: usize,
    ) -> Vec<PhysicalPropertySet> {
        vec![PhysicalPropertySet {
            distribution: DistributionSpec::Gather,
            ordering: parent_required.ordering.clone(),
        }]
    }
}
