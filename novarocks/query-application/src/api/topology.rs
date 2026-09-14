// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use novarocks_types::ClusterRole;

use super::QueryResult;

/// Read-only backend topology capability consumed by the SQL command layer.
///
/// The command layer can observe a rendered topology but cannot inspect,
/// register, replace, or schedule backend processes.
pub trait BackendTopologyCommandPort: Send + Sync + 'static {
    fn show_backends(&self) -> Result<QueryResult, String>;
}

/// Query-application owner of the `SHOW BACKENDS` role gate and result path.
#[derive(Clone)]
pub struct BackendCommandExecutor {
    topology: Arc<dyn BackendTopologyCommandPort>,
}

impl BackendCommandExecutor {
    pub fn new(topology: Arc<dyn BackendTopologyCommandPort>) -> Self {
        Self { topology }
    }

    pub fn show_backends(&self, role: ClusterRole) -> Result<QueryResult, String> {
        if role == ClusterRole::Be {
            return Err("SHOW BACKENDS is not available in role=be".to_string());
        }
        self.topology.show_backends()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct UnreachableTopology;

    impl BackendTopologyCommandPort for UnreachableTopology {
        fn show_backends(&self) -> Result<QueryResult, String> {
            panic!("role gate must reject before observing topology")
        }
    }

    #[test]
    fn show_backends_rejects_backend_role_without_observing_topology() {
        let executor = BackendCommandExecutor::new(Arc::new(UnreachableTopology));

        assert!(
            executor
                .show_backends(ClusterRole::Be)
                .expect_err("backend role must be rejected")
                .contains("role=be")
        );
    }
}
