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

//! Materialized-view application and engine ports.

use std::fmt;

use uuid::Uuid;

use crate::mv::repository::{
    CreateMvRepositoryRequest, MV_REPOSITORY_UNAVAILABLE_MESSAGE, MvRepository, MvTarget,
};
use crate::runtime::query_result::QueryResult;
use crate::sql::parser::ast::{CreateMaterializedViewStmt, Statement};

#[derive(Clone, Copy, Debug)]
pub struct MvRequestContext<'a> {
    pub current_catalog: Option<&'a str>,
    pub current_database: &'a str,
}

#[derive(Clone, Debug)]
pub enum MvStatementResult {
    Ok,
    Query(QueryResult),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvApplicationErrorKind {
    InvalidRequest,
    Engine,
    Repository,
    Unavailable,
    CommitUnknown,
    KnownCommittedFinalizeFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvApplicationError {
    kind: MvApplicationErrorKind,
    message: String,
}

impl MvApplicationError {
    pub fn new(kind: MvApplicationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> MvApplicationErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvApplicationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvEngineErrorKind {
    InvalidRequest,
    Analysis,
    TargetOperation,
    DescriptorSync,
    CatalogRegistration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvEngineError {
    kind: MvEngineErrorKind,
    message: String,
}

impl MvEngineError {
    pub fn new(kind: MvEngineErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn kind(&self) -> MvEngineErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvEngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvEngineError {}

#[derive(Clone, Copy, Debug)]
pub struct PrepareMvCreateRequest<'a> {
    pub statement: &'a CreateMaterializedViewStmt,
    pub context: MvRequestContext<'a>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedMvCreate {
    pub target: MvTarget,
    pub repository_request: CreateMvRepositoryRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatedMvTarget {
    pub target: MvTarget,
    pub table_uuid: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedMvDefinition {
    pub repository_request: CreateMvRepositoryRequest,
}

pub trait MvApplicationService: Send + Sync {
    fn try_handle_statement(
        &self,
        engine: &dyn MvEngine,
        statement: &Statement,
        context: MvRequestContext<'_>,
    ) -> Result<Option<MvStatementResult>, MvApplicationError>;
}

pub trait MvEngine: Send + Sync {
    fn prepare_create(
        &self,
        request: PrepareMvCreateRequest<'_>,
        repository: &dyn MvRepository,
    ) -> Result<PreparedMvCreate, MvEngineError>;

    fn create_target(
        &self,
        plan: &PreparedMvCreate,
        operation_id: Uuid,
    ) -> Result<CreatedMvTarget, MvEngineError>;

    fn inspect_created_target(
        &self,
        plan: &PreparedMvCreate,
        target: &CreatedMvTarget,
    ) -> Result<PreparedMvDefinition, MvEngineError>;

    fn sync_target_descriptor(
        &self,
        target: &CreatedMvTarget,
        definition: &crate::mv::persistence::definition::StoredMvDefinition,
    ) -> Result<(), MvEngineError>;

    fn register_target(&self, target: &CreatedMvTarget) -> Result<(), MvEngineError>;

    fn drop_created_target(&self, target: &CreatedMvTarget) -> Result<(), MvEngineError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UnavailableMvApplicationService;

impl MvApplicationService for UnavailableMvApplicationService {
    fn try_handle_statement(
        &self,
        _engine: &dyn MvEngine,
        statement: &Statement,
        _context: MvRequestContext<'_>,
    ) -> Result<Option<MvStatementResult>, MvApplicationError> {
        if matches!(statement, Statement::CreateMaterializedView(_)) {
            return Err(MvApplicationError::new(
                MvApplicationErrorKind::Unavailable,
                MV_REPOSITORY_UNAVAILABLE_MESSAGE,
            ));
        }
        Ok(None)
    }
}
