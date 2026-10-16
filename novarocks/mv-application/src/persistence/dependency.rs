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

use serde::{Deserialize, Serialize};

use crate::dependency::{MvDependencyObjectRef, iceberg_mv_dependency_ref};

pub(crate) const MV_ACCELERATOR_DEPENDENCY_SUBJECT: &str = "mv.accelerator_dependency";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredMvDependency {
    pub downstream_mv_id: i64,
    pub occurrence_id: u32,
    pub upstream_object_id: serde_bytes::ByteBuf,
    pub upstream: MvDependencyObjectRef,
    pub created_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateMvDependencyRequest {
    pub upstream: MvDependencyObjectRef,
    pub created_at_ms: i64,
}

pub fn stored_keyed_definition_dependency_ref(
    projection: &super::keyed_documents::StoredKeyedMvProjection,
) -> MvDependencyObjectRef {
    definition_dependency_ref(projection.facts.source_revision())
}

fn definition_dependency_ref(
    target: &super::definition::MvAcceleratorSourceRevision,
) -> MvDependencyObjectRef {
    iceberg_mv_dependency_ref(
        target.target.instance_id.as_str(),
        target.target.namespace.as_ref(),
        target.target.table.as_ref(),
    )
}
