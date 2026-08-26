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

//! Closed Frontend backend-membership command capability.

use crate::query_execution::kernels::BackendManagementKernel;
use crate::runtime::statement_result::StatementResult;
use novarocks_parser::ast::{BackendStatement, LiteralKind};
use novarocks_types::ClusterRole;

#[derive(Clone)]
pub struct BackendCommandExecutor {
    kernel: BackendManagementKernel,
}

fn require_backend_management_role(statement: &str, role: ClusterRole) -> Result<(), String> {
    match role {
        ClusterRole::Fe => Ok(()),
        ClusterRole::Be => Err(format!(
            "{statement} is not available in role=be; backend management is owned by StarRocks FE"
        )),
    }
}

impl BackendCommandExecutor {
    pub fn new(kernel: BackendManagementKernel) -> Self {
        Self { kernel }
    }

    pub fn execute(
        &self,
        statement: &BackendStatement,
        role: ClusterRole,
    ) -> Result<StatementResult, String> {
        match statement {
            BackendStatement::AddBackend(statement) => {
                require_backend_management_role("ADD BACKEND", role)?;
                let address = string_literal(&statement.address)?;
                let endpoint = address
                    .parse()
                    .map_err(|error| format!("invalid backend address '{address}': {error}"))?;
                self.kernel.topology().add_backend(endpoint)?;
                Ok(StatementResult::Ok)
            }
            BackendStatement::DropBackend(statement) => {
                require_backend_management_role("DROP BACKEND", role)?;
                let address = string_literal(&statement.address)?;
                let endpoint = address
                    .parse()
                    .map_err(|error| format!("invalid backend address '{address}': {error}"))?;
                self.kernel
                    .topology()
                    .drop_backend(endpoint, statement.force)?;
                Ok(StatementResult::Ok)
            }
            BackendStatement::ShowBackends(_) => {
                if role == ClusterRole::Be {
                    return Err("SHOW BACKENDS is not available in role=be".to_string());
                }
                self.kernel
                    .topology()
                    .show_backends()
                    .map(StatementResult::Query)
            }
        }
    }
}

fn string_literal(literal: &novarocks_parser::ast::Literal) -> Result<&str, String> {
    match &literal.kind {
        LiteralKind::String(value) => Ok(value),
        _ => Err("backend address must be a string literal".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_parser::ast::Statement;

    use super::*;

    fn backend_statement(sql: &str) -> BackendStatement {
        let statements = novarocks_parser::parse(sql).expect("parse backend command");
        assert_eq!(statements.len(), 1);
        match statements.into_iter().next().expect("one statement") {
            Statement::Backend(statement) => statement,
            other => panic!("expected backend statement, got {other:?}"),
        }
    }

    #[test]
    fn typed_backend_statement_reaches_the_topology_owner() {
        let topology: crate::common::backend_topology::BackendTopologyService =
            Arc::new(crate::topology::ClusterBackendService::new_for_test(1));
        let executor =
            BackendCommandExecutor::new(BackendManagementKernel::new(Arc::clone(&topology)));

        assert!(matches!(
            executor.execute(
                &backend_statement("ADD BACKEND '127.0.0.1:19070'"),
                ClusterRole::Fe,
            ),
            Ok(StatementResult::Ok)
        ));
        topology
            .show_backends()
            .expect("typed ADD must update the topology owner");
        assert!(
            executor
                .execute(&backend_statement("SHOW BACKENDS"), ClusterRole::Be,)
                .expect_err("backend role must be rejected")
                .contains("role=be")
        );
    }
}
