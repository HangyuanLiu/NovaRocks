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

use crate::mv::domain::repository::{
    CreateMvRepositoryRequest, MV_REPOSITORY_UNAVAILABLE_MESSAGE, MvRepository, MvTarget,
};
use crate::runtime::query_result::QueryResult;
use novarocks_parser::ast::{
    LiteralKind, MaterializedViewPartitionArgument, MaterializedViewPartitionField, Query,
};
use novarocks_sql::syntax::{
    CreateMaterializedViewStmt, IcebergPartitionFieldExpr, MaterializedViewRefreshPolicy,
};

/// Join refresh shape retained until query assembly admits exact connector
/// bindings for the corresponding write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvIncrementalJoinMode {
    AppendOnly,
    Coalesce,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvIncrementalWriteMode {
    FastAppend,
    RowDelta,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvIncrementalRewriteEvidence {
    None,
    Aggregate,
    JoinAggregate,
    BranchUnionAggregate,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvCreatePartitionField {
    Identity { column: String },
    Year { column: String },
    Month { column: String },
    Day { column: String },
    Hour { column: String },
    Bucket { column: String, num_buckets: u32 },
    Truncate { column: String, width: u32 },
    Void { column: String },
}

impl From<&IcebergPartitionFieldExpr> for MvCreatePartitionField {
    fn from(value: &IcebergPartitionFieldExpr) -> Self {
        match value {
            IcebergPartitionFieldExpr::Identity { column } => Self::Identity {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Year { column } => Self::Year {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Month { column } => Self::Month {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Day { column } => Self::Day {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Hour { column } => Self::Hour {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Bucket {
                column,
                num_buckets,
            } => Self::Bucket {
                column: column.clone(),
                num_buckets: *num_buckets,
            },
            IcebergPartitionFieldExpr::Truncate { column, width } => Self::Truncate {
                column: column.clone(),
                width: *width,
            },
            IcebergPartitionFieldExpr::Void { column } => Self::Void {
                column: column.clone(),
            },
        }
    }
}

impl From<&MaterializedViewPartitionField> for MvCreatePartitionField {
    fn from(value: &MaterializedViewPartitionField) -> Self {
        let (transform, arguments) = match value {
            MaterializedViewPartitionField::Identity(column) => {
                return Self::Identity {
                    column: normalized_partition_identifier(&column.value),
                };
            }
            MaterializedViewPartitionField::Transform {
                name, arguments, ..
            } => (name.value.to_ascii_lowercase(), arguments.as_slice()),
        };
        let column = partition_column_argument(arguments, &transform);
        match transform.as_str() {
            "identity" => Self::Identity { column },
            "year" => Self::Year { column },
            "month" => Self::Month { column },
            "day" => Self::Day { column },
            "hour" => Self::Hour { column },
            "void" => Self::Void { column },
            "bucket" => Self::Bucket {
                column,
                num_buckets: partition_u32_argument(arguments, "bucket"),
            },
            "truncate" => Self::Truncate {
                column,
                width: partition_u32_argument(arguments, "truncate"),
            },
            _ => unreachable!("MV partition transform was validated during SQL admission"),
        }
    }
}

fn normalized_partition_identifier(value: &str) -> String {
    novarocks_catalog::identifier::normalize_identifier(value)
        .expect("MV partition identifier was validated during SQL admission")
}

fn partition_column_argument(
    arguments: &[MaterializedViewPartitionArgument],
    transform: &str,
) -> String {
    let Some(MaterializedViewPartitionArgument::Ident(column)) = arguments.first() else {
        unreachable!("MV {transform} partition transform was validated during SQL admission");
    };
    normalized_partition_identifier(&column.value)
}

fn partition_u32_argument(arguments: &[MaterializedViewPartitionArgument], transform: &str) -> u32 {
    let Some(MaterializedViewPartitionArgument::Literal(value)) = arguments.get(1) else {
        unreachable!("MV {transform} partition transform was validated during SQL admission");
    };
    let LiteralKind::Number(value) = &value.kind else {
        unreachable!("MV {transform} partition transform was validated during SQL admission");
    };
    let value = value
        .parse::<u32>()
        .expect("MV partition numeric argument was validated during SQL admission");
    assert!(
        value > 0,
        "MV partition numeric argument was validated during SQL admission"
    );
    value
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvCreateDistribution {
    pub hash_columns: Vec<String>,
    pub bucket_count: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum MvCreateRefreshPolicy {
    #[default]
    Manual,
    AsyncOnChange,
    AsyncInterval {
        interval_ms: i64,
    },
}

impl From<&MaterializedViewRefreshPolicy> for MvCreateRefreshPolicy {
    fn from(value: &MaterializedViewRefreshPolicy) -> Self {
        match value {
            MaterializedViewRefreshPolicy::Manual => Self::Manual,
            MaterializedViewRefreshPolicy::AsyncOnChange => Self::AsyncOnChange,
            MaterializedViewRefreshPolicy::AsyncInterval { interval_ms } => Self::AsyncInterval {
                interval_ms: *interval_ms,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MvCreateStatement {
    pub name_parts: Vec<String>,
    pub if_not_exists: bool,
    pub partition_by: Option<Vec<MvCreatePartitionField>>,
    pub distribution: Option<MvCreateDistribution>,
    pub refresh_policy: MvCreateRefreshPolicy,
    pub select_sql: String,
    pub select_query: Query,
    pub properties: Vec<(String, String)>,
    pub primary_key: Option<Vec<String>>,
}

impl From<&CreateMaterializedViewStmt> for MvCreateStatement {
    fn from(value: &CreateMaterializedViewStmt) -> Self {
        Self {
            name_parts: value
                .name
                .parts
                .iter()
                .map(|part| part.value.clone())
                .collect(),
            if_not_exists: value.if_not_exists,
            partition_by: value
                .partition_by
                .as_ref()
                .map(|fields| fields.iter().map(MvCreatePartitionField::from).collect()),
            distribution: value
                .distribution
                .as_ref()
                .map(|distribution| MvCreateDistribution {
                    hash_columns: distribution.hash_columns.clone(),
                    bucket_count: distribution.bucket_count,
                }),
            refresh_policy: MvCreateRefreshPolicy::from(&value.refresh_policy),
            select_sql: value.select_sql.clone(),
            select_query: value.select_query.clone(),
            properties: value.properties.clone(),
            primary_key: value.primary_key.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "The SQL-facing statement carrier retains its direct create payload to preserve the existing application boundary."
)]
pub enum MvApplicationStatement {
    Create(MvCreateStatement),
    Unhandled,
}

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
    AlreadyActive,
    TargetGone,
    Corruption,
    RecoveryRequired,
    ShutdownCancelled,
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
    pub statement: &'a MvCreateStatement,
    pub context: MvRequestContext<'a>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreparedMvCreate {
    pub target: MvTarget,
    pub repository_request: CreateMvRepositoryRequest,
}

impl PreparedMvCreate {
    pub fn new(target: MvTarget, repository_request: CreateMvRepositoryRequest) -> Self {
        Self {
            target,
            repository_request,
        }
    }
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
        statement: &MvApplicationStatement,
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
        definition: &crate::mv::domain::persistence::definition::StoredMvDefinition,
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
        statement: &MvApplicationStatement,
        _context: MvRequestContext<'_>,
    ) -> Result<Option<MvStatementResult>, MvApplicationError> {
        if matches!(statement, MvApplicationStatement::Create(_)) {
            return Err(MvApplicationError::new(
                MvApplicationErrorKind::Unavailable,
                MV_REPOSITORY_UNAVAILABLE_MESSAGE,
            ));
        }
        Ok(None)
    }
}
