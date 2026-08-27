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

//! System (metadata) relations exposed by a connector.

use crate::connector::{ConnectorError, ConnectorErrorKind};

/// Where a system relation must run.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SystemTableDistribution {
    /// Distributed over the admitted workers through a typed split source.
    AllNodes,
    /// Executed exactly once. Native selects one backend rather than running
    /// the relation on the coordinator.
    SingleCoordinator,
}

/// One system relation's exact output column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemTableColumn {
    pub name: std::sync::Arc<str>,
    pub nullable: bool,
}

impl SystemTableColumn {
    pub fn try_new(name: impl AsRef<str>, nullable: bool) -> Result<Self, ConnectorError> {
        let name = name.as_ref();
        if name.is_empty() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "system table column name must not be empty",
            ));
        }
        Ok(Self {
            name: std::sync::Arc::from(name),
            nullable,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribution_is_a_closed_set() {
        assert_ne!(
            SystemTableDistribution::AllNodes,
            SystemTableDistribution::SingleCoordinator
        );
    }

    #[test]
    fn system_table_columns_require_a_name() {
        assert!(SystemTableColumn::try_new("", true).is_err());
        assert!(SystemTableColumn::try_new("content", false).is_ok());
    }
}
