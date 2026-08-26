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

use novarocks_frontend::mv::domain::dependency::model::MvDependencyObjectRef;
use novarocks_frontend::mv::domain::persistence::dependency::StoredMvDependency;
use novarocks_frontend::mv::domain::repository::{
    DeleteMvProjectionRequest, LoadedMvProjection, MvProjectionRequest, MvRepository,
    MvRepositoryError, MvRepositoryErrorKind, MvTarget, ReplaceMvProjectionRequest,
};
use uuid::Uuid;

pub struct DomainOnlyMvRepository;

fn unsupported<T>() -> Result<T, MvRepositoryError> {
    Err(MvRepositoryError::new(
        MvRepositoryErrorKind::InvalidRequest,
        "domain-only fake does not persist MV Accelerator records",
    ))
}

impl MvRepository for DomainOnlyMvRepository {
    fn create_projection(
        &self,
        _operation_id: Uuid,
        _projection: MvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError> {
        unsupported()
    }

    fn replace_projection(
        &self,
        _operation_id: Uuid,
        _request: ReplaceMvProjectionRequest,
    ) -> Result<LoadedMvProjection, MvRepositoryError> {
        unsupported()
    }

    fn load_by_id(&self, _mv_id: i64) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        Ok(None)
    }

    fn find_by_target(
        &self,
        _target: &MvTarget,
    ) -> Result<Option<LoadedMvProjection>, MvRepositoryError> {
        Ok(None)
    }

    fn list_projections(&self) -> Result<Vec<LoadedMvProjection>, MvRepositoryError> {
        Ok(Vec::new())
    }

    fn delete_projection(
        &self,
        _operation_id: Uuid,
        _request: DeleteMvProjectionRequest,
    ) -> Result<bool, MvRepositoryError> {
        Ok(false)
    }

    fn wipe_projection_by_target(
        &self,
        _operation_id: Uuid,
        _target: &MvTarget,
    ) -> Result<bool, MvRepositoryError> {
        Ok(false)
    }

    fn wipe_accelerator(&self, _operation_id: Uuid) -> Result<(), MvRepositoryError> {
        Ok(())
    }

    fn list_dependencies_by_downstream(
        &self,
        _mv_id: i64,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        Ok(Vec::new())
    }

    fn list_downstream_dependencies(
        &self,
        _upstream: &MvDependencyObjectRef,
    ) -> Result<Vec<StoredMvDependency>, MvRepositoryError> {
        Ok(Vec::new())
    }

    fn ensure_no_downstream_dependencies(
        &self,
        _upstream: &MvDependencyObjectRef,
    ) -> Result<(), MvRepositoryError> {
        Ok(())
    }
}
