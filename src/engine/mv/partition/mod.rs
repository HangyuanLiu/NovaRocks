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

pub(crate) mod derivation;
pub(crate) mod key;
pub(crate) mod mapping;
pub(crate) mod planner;

pub(crate) use derivation::{
    AffectedTargetPartitions, PartitionDerivationSpec, resolve_partition_derivation_spec,
};
// P2 partition-pruning asset: the chunk evaluator + its bound/error types were
// extracted in P1 (the dead pre-cutover apply path that used them is removed),
// and the live consumer lands in P2 (join PF / sink-side pruning, umbrella
// spec §5.1). Kept re-exported so P2 wires in without re-plumbing the module.
#[allow(unused_imports)]
pub(crate) use derivation::{
    AffectedPartitionError, BoundPartitionField, PartitionDerivationField,
    bind_spec_to_aggregate_layout, bind_spec_to_target_visible_columns, evaluate_partition_spec,
    evaluate_partition_spec_record_batch,
};
pub(crate) use key::{
    MvPartitionKey, MvPartitionKeyField, MvPartitionValue, TargetPartitionFilter,
};
