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

//! Deterministic final-physical-plan-to-protobuf encoding for the native FE/BE
//! boundary. The encoder consumes a completed physical plan and exact frozen
//! provider facts. It has no SQL compiler or Frontend runtime dependency.

pub mod native_type;
mod native_type_encode;
mod physical_encode;
mod physical_expr;
mod physical_type;
mod physical_v1;
mod write_targets;

pub use physical_encode::{
    NoPhysicalV1PrivateFacts, PhysicalV1CteConsumer, PhysicalV1PrivateFacts,
    PhysicalV1RuntimeFilterBinding, PhysicalV1RuntimeFilterBindingRole, PhysicalV1ScanColumn,
    PhysicalV1ScanFact, PhysicalV1WriteFact, ScanRuntimeFilterBindings, encode_physical_plan_v1,
    physical_v1_cte_consumers, physical_v1_runtime_filter_bindings,
    physical_v1_runtime_filter_comparator_digest, physical_v1_scan_runtime_filters,
    physical_v1_scan_source_seal_digest,
};
pub use physical_v1::{
    NATIVE_V1_MAX_TREE_DEPTH, PhysicalV1PreflightError, WireLayout, WireLayoutError, WireSlotId,
    preflight_physical_plan_v1,
};

pub use native_type_encode::encode_type as encode_native_type;
pub use write_targets::SealedWriteTargets;
