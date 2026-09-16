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

use crate::dependency::{
    MvDependencyObjectRef, MvDependencyObjectType, MvDependencyStorageEngine,
    iceberg_mv_dependency_ref,
};
use crate::persistence::projection::StoredMvProjection;

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

pub fn stored_definition_dependency_ref(projection: &StoredMvProjection) -> MvDependencyObjectRef {
    let target = projection.facts.source_revision();
    iceberg_mv_dependency_ref(
        target.target.instance_id.as_str(),
        target.target.namespace.as_ref(),
        target.target.table.as_ref(),
    )
}

/// Index rows derive from D, never caller-supplied FQN watermarks.
pub(crate) fn projection_dependencies(
    downstream_mv_id: i64,
    facts: &crate::persistence::projection::MvDocumentProjection,
) -> Vec<StoredMvDependency> {
    facts
        .dependencies()
        .into_iter()
        .map(|dependency| StoredMvDependency {
            downstream_mv_id,
            occurrence_id: dependency.occurrence_id,
            upstream_object_id: serde_bytes::ByteBuf::from(
                dependency.object_id.as_bytes().to_vec(),
            ),
            upstream: MvDependencyObjectRef {
                catalog: Some(dependency.catalog),
                database_or_namespace: dependency.namespace,
                name: dependency.relation,
                object_type: MvDependencyObjectType::Unclassified,
                storage_engine: MvDependencyStorageEngine::Unclassified,
            },
            created_at_ms: facts.definition().created_at_ms,
        })
        .collect()
}

/// Classification is a derived view of one complete inventory, never a fact
/// guessed from the locator or persisted independently of its source root.
pub(crate) fn classify_dependencies(
    dependencies: &mut [StoredMvDependency],
    inventory: &[StoredMvProjection],
) {
    for dependency in dependencies {
        let is_mv = inventory.iter().any(|projection| {
            let source = projection.facts.source_revision();
            dependency.upstream.catalog.as_deref() == Some(source.target.instance_id.as_str())
                && dependency.upstream_object_id.as_slice()
                    == source.target_object_id.as_bytes().as_ref()
        });
        dependency.upstream.object_type = if is_mv {
            MvDependencyObjectType::MaterializedView
        } else {
            MvDependencyObjectType::Table
        };
        dependency.upstream.storage_engine = if is_mv {
            MvDependencyStorageEngine::Iceberg
        } else {
            MvDependencyStorageEngine::ExternalTable
        };
    }
}
