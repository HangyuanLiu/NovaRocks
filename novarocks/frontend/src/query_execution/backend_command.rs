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

//! Closed Frontend backend-observability command capability.

use crate::query_execution::kernels::BackendManagementKernel;
use crate::runtime::statement_result::StatementResult;
use novarocks_parser::ast::ShowBackends;
use novarocks_types::ClusterRole;

#[derive(Clone)]
pub struct BackendCommandExecutor {
    kernel: BackendManagementKernel,
}

impl BackendCommandExecutor {
    pub fn new(kernel: BackendManagementKernel) -> Self {
        Self { kernel }
    }

    pub fn execute(
        &self,
        _statement: &ShowBackends,
        role: ClusterRole,
    ) -> Result<StatementResult, String> {
        if role == ClusterRole::Be {
            return Err("SHOW BACKENDS is not available in role=be".to_string());
        }
        self.kernel
            .topology()
            .show_backends()
            .map(StatementResult::Query)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_parser::ast::{ShowBackends, Statement};

    use super::*;

    fn show_backends_statement(sql: &str) -> ShowBackends {
        let statements = novarocks_parser::parse(sql).expect("parse backend command");
        assert_eq!(statements.len(), 1);
        match statements.into_iter().next().expect("one statement") {
            Statement::ShowBackends(statement) => statement,
            other => panic!("expected SHOW BACKENDS statement, got {other:?}"),
        }
    }

    #[test]
    fn show_backends_rejects_backend_role() {
        let topology: crate::common::backend_topology::BackendTopologyService =
            Arc::new(crate::topology::ClusterBackendService::new_transient_for_test(1));
        let executor =
            BackendCommandExecutor::new(BackendManagementKernel::new(Arc::clone(&topology)));

        assert!(
            executor
                .execute(&show_backends_statement("SHOW BACKENDS"), ClusterRole::Be,)
                .expect_err("backend role must be rejected")
                .contains("role=be")
        );
    }
}
