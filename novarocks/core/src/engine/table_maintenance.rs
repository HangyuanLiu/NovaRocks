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

//! Table-maintenance application ports shared with `novarocks-frontend`.
//!
//! This dependency-inversion boundary exposes only the typed engine
//! capabilities and application results needed by the frontend owner. It does
//! not expose standalone engine state or connector handles.

use std::collections::BTreeMap;
use std::sync::Arc;

use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;

use crate::runtime::query_result::QueryResult;
use crate::sql::parser::dialect::StarRocksDialect;

pub const TABLE_MAINTENANCE_SERVICE_UNAVAILABLE: &str = "table maintenance service is not injected";

#[derive(Clone, Copy, Debug)]
pub struct MaintenanceRequestContext<'a> {
    pub current_catalog: Option<&'a str>,
    pub current_database: &'a str,
}

#[derive(Clone, Debug)]
pub enum MaintenanceStatementResult {
    Ok,
    Query(QueryResult),
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct MaintenanceTarget {
    pub catalog: String,
    pub namespace: String,
    pub table: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceActionRequest {
    RewriteDataFiles {
        target: MaintenanceTarget,
        base_snapshot_id: i64,
        job_id: Option<i64>,
        options: BTreeMap<String, String>,
        branch: Option<String>,
        where_clause: Option<String>,
    },
    RewriteManifests {
        target: MaintenanceTarget,
        use_caching: Option<bool>,
        spec_id: Option<i32>,
    },
    ExpireSnapshots {
        target: MaintenanceTarget,
        older_than_ms: Option<i64>,
        retain_last: Option<u32>,
    },
    RemoveOrphanFiles {
        target: MaintenanceTarget,
        older_than_ms: i64,
    },
    RewritePositionDeleteFiles {
        target: MaintenanceTarget,
        options: BTreeMap<String, String>,
        where_clause: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MaintenanceActionOutcome {
    RewriteDataFiles {
        rewritten_data_files_count: i32,
        added_data_files_count: i32,
        rewritten_bytes_count: i64,
        failed_data_files_count: i32,
        removed_delete_files_count: i32,
    },
    RewriteManifests {
        rewritten_manifests_count: i32,
        added_manifests_count: i32,
    },
    ExpireSnapshots {
        deleted_data_files_count: Option<i64>,
        deleted_position_delete_files_count: Option<i64>,
        deleted_equality_delete_files_count: Option<i64>,
        deleted_manifest_files_count: Option<i64>,
        deleted_manifest_lists_count: Option<i64>,
        deleted_statistics_files_count: Option<i64>,
    },
    RemoveOrphanFiles {
        orphan_file_locations: Vec<String>,
    },
    RewritePositionDeleteFiles {
        rewritten_delete_files_count: i32,
        added_delete_files_count: i32,
        rewritten_bytes_count: i64,
        added_bytes_count: i64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum OptimizeJobState {
    Pending,
    Running,
    Finished,
    Failed,
}

impl OptimizeJobState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Finished => "FINISHED",
            Self::Failed => "FAILED",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OptimizeSubmission {
    Submitted { job_id: i64 },
    AlreadyActive,
}

// Design: ADR-0009 (docs/adr/ADR-0009-frontend-table-maintenance-owner.md)
pub trait TableMaintenanceEngine: Send + Sync {
    fn resolve_target(
        &self,
        name_parts: &[String],
        context: MaintenanceRequestContext<'_>,
    ) -> Result<MaintenanceTarget, String>;

    fn reject_user_action_on_mv(&self, target: &MaintenanceTarget) -> Result<(), String>;

    fn current_snapshot_id(&self, target: &MaintenanceTarget) -> Result<i64, String>;

    fn execute_action(
        &self,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String>;
}

pub trait TableMaintenanceService: Send + Sync {
    fn start(&self, engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String>;

    fn try_handle_statement(
        &self,
        engine: &dyn TableMaintenanceEngine,
        sql: &str,
        context: MaintenanceRequestContext<'_>,
    ) -> Result<Option<MaintenanceStatementResult>, String>;

    fn execute_automatic_action(
        &self,
        engine: &dyn TableMaintenanceEngine,
        request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String>;

    fn submit_automatic_optimize(
        &self,
        engine: &dyn TableMaintenanceEngine,
        target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String>;

    fn shutdown(&self) -> Result<(), String>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyTableMaintenanceService;

impl TableMaintenanceService for EmptyTableMaintenanceService {
    fn start(&self, _engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
        Ok(())
    }

    fn try_handle_statement(
        &self,
        _engine: &dyn TableMaintenanceEngine,
        sql: &str,
        _context: MaintenanceRequestContext<'_>,
    ) -> Result<Option<MaintenanceStatementResult>, String> {
        if looks_like_maintenance_statement(sql) {
            return Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_owned());
        }
        Ok(None)
    }

    fn execute_automatic_action(
        &self,
        _engine: &dyn TableMaintenanceEngine,
        _request: MaintenanceActionRequest,
    ) -> Result<MaintenanceActionOutcome, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_owned())
    }

    fn submit_automatic_optimize(
        &self,
        _engine: &dyn TableMaintenanceEngine,
        _target: MaintenanceTarget,
    ) -> Result<OptimizeSubmission, String> {
        Err(TABLE_MAINTENANCE_SERVICE_UNAVAILABLE.to_owned())
    }

    fn shutdown(&self) -> Result<(), String> {
        Ok(())
    }
}

fn looks_like_maintenance_statement(sql: &str) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(mut parser) = Parser::new(&StarRocksDialect).try_with_sql(&normalized) else {
        return false;
    };
    if parser.parse_keyword(Keyword::CALL) {
        let Ok(name) = parser.parse_object_name(false) else {
            return false;
        };
        let normalized_name = name
            .to_string()
            .replace(['`', '"', ' '], "")
            .to_ascii_lowercase();
        let parts = normalized_name.split('.').collect::<Vec<_>>();
        let [_, namespace, procedure] = parts.as_slice() else {
            return false;
        };
        return *namespace == "system"
            && [
                "rewrite_data_files",
                "rewrite_manifests",
                "expire_snapshots",
                "remove_orphan_files",
                "rewrite_position_delete_files",
            ]
            .iter()
            .any(|candidate| procedure == candidate);
    }
    if parser.parse_keyword(Keyword::SHOW) {
        return parser.parse_keyword(Keyword::ALTER)
            && parser.parse_keyword(Keyword::TABLE)
            && consume_word(&mut parser, "OPTIMIZE");
    }
    if !parser.parse_keyword(Keyword::ALTER) || !parser.parse_keyword(Keyword::TABLE) {
        return false;
    }
    if parser.parse_object_name(false).is_err() {
        return false;
    }
    consume_word(&mut parser, "OPTIMIZE")
        || (consume_word(&mut parser, "REWRITE") && consume_word(&mut parser, "MANIFESTS"))
        || (consume_word(&mut parser, "EXPIRE") && consume_word(&mut parser, "SNAPSHOTS"))
        || (consume_word(&mut parser, "REMOVE")
            && consume_word(&mut parser, "ORPHAN")
            && consume_word(&mut parser, "FILES"))
}

fn consume_word(parser: &mut Parser<'_>, expected: &str) -> bool {
    if parser
        .peek_token()
        .token
        .to_string()
        .eq_ignore_ascii_case(expected)
    {
        parser.next_token();
        true
    } else {
        false
    }
}
