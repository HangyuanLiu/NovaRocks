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

//! Legacy materialized-view SQL carriers.
//!
//! These are execution-side DTOs for the SQLP-3 MV path. They intentionally
//! live outside `parser::ast`: SQLP-5's parser-owned AST must not carry
//! sqlparser nodes.

use crate::parser::ast::{IcebergPartitionFieldExpr, ObjectName};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializedViewDistribution {
    pub hash_columns: Vec<String>,
    pub bucket_count: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum MaterializedViewRefreshPolicy {
    #[default]
    Manual,
    AsyncOnChange,
    AsyncInterval {
        interval_ms: i64,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct CreateMaterializedViewStmt {
    pub name: ObjectName,
    pub if_not_exists: bool,
    pub partition_by: Option<Vec<IcebergPartitionFieldExpr>>,
    pub distribution: Option<MaterializedViewDistribution>,
    pub refresh_policy: MaterializedViewRefreshPolicy,
    pub select_sql: String,
    pub select_query: sqlparser::ast::Query,
    pub properties: Vec<(String, String)>,
    pub primary_key: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropMaterializedViewStmt {
    pub name: ObjectName,
    pub if_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AlterMaterializedViewAction {
    SetRefresh(MaterializedViewRefreshPolicy),
    SetProperties(Vec<(String, String)>),
    PauseRefresh,
    ResumeRefresh,
    Repartition(Vec<IcebergPartitionFieldExpr>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterMaterializedViewStmt {
    pub name: ObjectName,
    pub action: AlterMaterializedViewAction,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshMaterializedViewStmt {
    pub name: ObjectName,
    pub full: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowMaterializedViewsStmt {
    pub database: Option<String>,
}
