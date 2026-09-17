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
        // A dependency records its upstream inside the application's own fact
        // envelope; a projection records its target as the provider's bare
        // identity. Comparing the two as bytes never matches, so every
        // upstream read as a plain table -- a view over a view included.
        // Opening the envelope is not a provider decode: the value inside is
        // handed back unchanged and stays opaque.
        let upstream = crate::persistence::identity::ObjectIdentity::try_new(
            dependency.upstream_object_id.to_vec(),
        )
        .ok();
        let is_mv = upstream.is_some_and(|upstream| {
            inventory.iter().any(|projection| {
                let source = projection.facts.source_revision();
                dependency.upstream.catalog.as_deref() == Some(source.target.instance_id.as_str())
                    && crate::persistence::exact_revision::persisted_object_names(
                        &upstream,
                        &source.target_object_id,
                    )
                    .unwrap_or(false)
            })
        });
        // Classification is a derived view, so an upstream whose identity this
        // process cannot read stays unclassified rather than being called a
        // table it may not be. What must not be guessed is whether dropping it
        // is safe, and that answer is the dependency guard's, not this one's.
        let readable = crate::persistence::identity::ObjectIdentity::try_new(
            dependency.upstream_object_id.to_vec(),
        )
        .is_ok_and(|identity| {
            crate::persistence::exact_revision::restore_persisted_object(&identity).is_ok()
        });
        dependency.upstream.object_type = match (readable, is_mv) {
            (false, _) => MvDependencyObjectType::Unclassified,
            (true, true) => MvDependencyObjectType::MaterializedView,
            (true, false) => MvDependencyObjectType::Table,
        };
        dependency.upstream.storage_engine = match (readable, is_mv) {
            (false, _) => MvDependencyStorageEngine::Unclassified,
            (true, true) => MvDependencyStorageEngine::Iceberg,
            (true, false) => MvDependencyStorageEngine::ExternalTable,
        };
    }
}
