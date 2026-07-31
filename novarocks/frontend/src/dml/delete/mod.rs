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

//! Frontend-owned DELETE statement recognition and application routing.

use novarocks::engine::delete_engine::{
    DeleteEngine, DeleteStatementKind, ExecuteDeleteRequest, parse_delete_statement,
    parse_equality_delete_statement,
};
use novarocks::query_execution::request_context::RequestContext;
use novarocks::runtime::query_options::QueryOptions;

use crate::dml::error::DmlError;
use crate::dml::service::DmlService;

impl DmlService {
    pub fn try_execute_delete(
        &self,
        engine: &dyn DeleteEngine,
        sql: &str,
        context: &RequestContext,
        query_options: Option<&QueryOptions>,
    ) -> Result<Option<()>, DmlError> {
        let kind = if parse_delete_statement(sql)
            .map_err(DmlError::executor)?
            .is_some()
        {
            DeleteStatementKind::Predicate
        } else if parse_equality_delete_statement(sql)
            .map_err(DmlError::executor)?
            .is_some()
        {
            DeleteStatementKind::Equality
        } else {
            return Ok(None);
        };

        self.require_journal()?;
        let session = context.session();
        engine
            .execute_delete(ExecuteDeleteRequest {
                sql,
                current_catalog: session.current_catalog().map(ToOwned::to_owned),
                current_database: session.current_database().to_string(),
                query_options: query_options.cloned(),
                execution: context.execution().clone(),
                kind,
            })
            .map_err(DmlError::executor)?;
        Ok(Some(()))
    }
}
