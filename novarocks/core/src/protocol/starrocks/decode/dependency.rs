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

use std::cell::RefCell;
use std::collections::BTreeMap;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use crate::runtime::endpoint::RuntimeEndpoint;
use crate::runtime::query_context::QueryId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StarRocksExternalDependency {
    QueryProfile {
        id: u64,
        query_id: String,
    },
    LakeMetaStorage {
        id: u64,
        request: LakeMetaStorageRequest,
    },
}

impl StarRocksExternalDependency {
    pub(crate) fn id(&self) -> u64 {
        match self {
            Self::QueryProfile { id, .. } => *id,
            Self::LakeMetaStorage { id, .. } => *id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum LakeMetaColumnKind {
    Dictionary,
    Value(DataType),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LakeMetaColumnRequest {
    pub(crate) column_id: String,
    pub(crate) kind: LakeMetaColumnKind,
}

impl LakeMetaColumnRequest {
    pub(crate) fn storage_key(&self) -> String {
        format!("{}:{:?}", self.column_id, self.kind)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LakeMetaTabletRequest {
    pub(crate) tablet_id: i64,
    pub(crate) version: i64,
    pub(crate) row_count_hint: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LakeMetaStorageRequest {
    id: u64,
    pub(crate) query_id: QueryId,
    pub(crate) catalog: String,
    pub(crate) db_name: String,
    pub(crate) table_name: String,
    pub(crate) db_id: i64,
    pub(crate) table_id: i64,
    pub(crate) schema_id: i64,
    pub(crate) tablets: Vec<LakeMetaTabletRequest>,
    pub(crate) columns: Vec<LakeMetaColumnRequest>,
}

impl LakeMetaStorageRequest {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        query_id: QueryId,
        catalog: String,
        db_name: String,
        table_name: String,
        db_id: i64,
        table_id: i64,
        schema_id: i64,
        tablets: Vec<LakeMetaTabletRequest>,
        columns: Vec<LakeMetaColumnRequest>,
    ) -> Self {
        let stable_key = format!(
            "{query_id}:{catalog}:{db_name}:{table_name}:{db_id}:{table_id}:{schema_id}:{tablets:?}:{columns:?}"
        );
        Self {
            id: stable_dependency_id("lake-meta-storage", &stable_key),
            query_id,
            catalog,
            db_name,
            table_name,
            db_id,
            table_id,
            schema_id,
            tablets,
            columns,
        }
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Clone, Debug)]
pub(crate) struct LakeMetaStorageFacts {
    pub(crate) total_rows: i64,
    pub(crate) column_arrays: BTreeMap<String, Vec<ArrayRef>>,
}

pub(crate) struct StarRocksExternalDependencyDraft {
    frontend_endpoint: Option<RuntimeEndpoint>,
    resolved_query_profiles: BTreeMap<String, String>,
    resolved_lake_meta_storage: BTreeMap<u64, LakeMetaStorageFacts>,
    requirements: RefCell<BTreeMap<u64, StarRocksExternalDependency>>,
}

impl StarRocksExternalDependencyDraft {
    pub(crate) fn new(
        frontend_endpoint: Option<RuntimeEndpoint>,
        resolved_query_profiles: BTreeMap<String, String>,
    ) -> Self {
        Self {
            frontend_endpoint,
            resolved_query_profiles,
            resolved_lake_meta_storage: BTreeMap::new(),
            requirements: RefCell::new(BTreeMap::new()),
        }
    }

    pub(crate) fn new_with_lake_meta_storage(
        frontend_endpoint: Option<RuntimeEndpoint>,
        resolved_query_profiles: BTreeMap<String, String>,
        resolved_lake_meta_storage: BTreeMap<u64, LakeMetaStorageFacts>,
    ) -> Self {
        Self {
            frontend_endpoint,
            resolved_query_profiles,
            resolved_lake_meta_storage,
            requirements: RefCell::new(BTreeMap::new()),
        }
    }

    pub(crate) fn frontend_endpoint(&self) -> Option<&RuntimeEndpoint> {
        self.frontend_endpoint.as_ref()
    }

    pub(crate) fn query_profile(&self, query_id: &str) -> Result<String, String> {
        if let Some(profile) = self.resolved_query_profiles.get(query_id) {
            return Ok(profile.clone());
        }
        let id = stable_dependency_id("query-profile", query_id);
        let requirement = StarRocksExternalDependency::QueryProfile {
            id,
            query_id: query_id.to_string(),
        };
        let mut requirements = self.requirements.borrow_mut();
        if let Some(existing) = requirements.get(&id)
            && existing != &requirement
        {
            return Err(format!("external dependency id collision for id={id}"));
        }
        requirements.insert(id, requirement);
        // Decode attempts are drafts until their requirement set is empty.  The
        // placeholder keeps discovery type-correct; the fragment decoder must
        // never publish or execute a draft that recorded this requirement.
        Ok(String::new())
    }

    pub(crate) fn lake_meta_storage(
        &self,
        request: &LakeMetaStorageRequest,
    ) -> Result<LakeMetaStorageFacts, String> {
        if let Some(facts) = self.resolved_lake_meta_storage.get(&request.id()) {
            return Ok(facts.clone());
        }
        let requirement = StarRocksExternalDependency::LakeMetaStorage {
            id: request.id(),
            request: request.clone(),
        };
        let mut requirements = self.requirements.borrow_mut();
        if let Some(existing) = requirements.get(&request.id())
            && existing != &requirement
        {
            return Err(format!(
                "external dependency id collision for id={}",
                request.id()
            ));
        }
        requirements.insert(request.id(), requirement);
        // Preserve the requested keys so the rest of LAKE_META_SCAN_NODE can
        // finish structural validation without touching storage during discovery.
        let column_arrays = request
            .columns
            .iter()
            .map(|column| (column.storage_key(), Vec::new()))
            .collect();
        Ok(LakeMetaStorageFacts {
            total_rows: 0,
            column_arrays,
        })
    }

    pub(crate) fn external_dependencies(&self) -> Vec<StarRocksExternalDependency> {
        self.requirements.borrow().values().cloned().collect()
    }
}

fn stable_dependency_id(kind: &str, key: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in kind.bytes().chain([0]).chain(key.bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
