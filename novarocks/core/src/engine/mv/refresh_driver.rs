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

use crate::engine::StatementResult;
use crate::mv::refresh::snapshot::RefreshDecision;

pub(crate) struct IcebergMvRefreshLifecycle;

impl IcebergMvRefreshLifecycle {
    pub(crate) fn run(
        decision: RefreshDecision,
        first_refresh: impl FnOnce() -> Result<StatementResult, String>,
        metadata_only: impl FnOnce() -> Result<StatementResult, String>,
        incremental: impl FnOnce() -> Result<StatementResult, String>,
    ) -> Result<StatementResult, String> {
        match decision {
            RefreshDecision::SkipEmpty => Ok(StatementResult::Ok),
            RefreshDecision::FirstRefresh => first_refresh(),
            RefreshDecision::MetadataOnly => metadata_only(),
            RefreshDecision::Incremental => incremental(),
            RefreshDecision::FailFast { reason } => Err(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn lifecycle_dispatches_first_refresh_closure() {
        let calls = RefCell::new(Vec::new());

        let result = IcebergMvRefreshLifecycle::run(
            RefreshDecision::FirstRefresh,
            || {
                calls.borrow_mut().push("first");
                Ok(StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("metadata");
                Ok(StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("incremental");
                Ok(StatementResult::Ok)
            },
        )
        .expect("first refresh closure should succeed");

        assert!(matches!(result, StatementResult::Ok));
        assert_eq!(*calls.borrow(), vec!["first"]);
    }

    #[test]
    fn lifecycle_dispatches_metadata_only_closure() {
        let calls = RefCell::new(Vec::new());

        let result = IcebergMvRefreshLifecycle::run(
            RefreshDecision::MetadataOnly,
            || {
                calls.borrow_mut().push("first");
                Ok(StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("metadata");
                Ok(StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("incremental");
                Ok(StatementResult::Ok)
            },
        )
        .expect("metadata-only closure should succeed");

        assert!(matches!(result, StatementResult::Ok));
        assert_eq!(*calls.borrow(), vec!["metadata"]);
    }

    #[test]
    fn lifecycle_dispatches_incremental_closure() {
        let calls = RefCell::new(Vec::new());

        let result = IcebergMvRefreshLifecycle::run(
            RefreshDecision::Incremental,
            || {
                calls.borrow_mut().push("first");
                Ok(StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("metadata");
                Ok(StatementResult::Ok)
            },
            || {
                calls.borrow_mut().push("incremental");
                Ok(StatementResult::Ok)
            },
        )
        .expect("incremental closure should succeed");

        assert!(matches!(result, StatementResult::Ok));
        assert_eq!(*calls.borrow(), vec!["incremental"]);
    }

    #[test]
    fn lifecycle_skip_empty_returns_ok_without_calling_closures() {
        let result = IcebergMvRefreshLifecycle::run(
            RefreshDecision::SkipEmpty,
            || panic!("first refresh closure must not run"),
            || panic!("metadata-only closure must not run"),
            || panic!("incremental closure must not run"),
        )
        .expect("skip-empty should succeed");

        assert!(matches!(result, StatementResult::Ok));
    }

    #[test]
    fn lifecycle_fail_fast_returns_reason_without_calling_closures() {
        let result = IcebergMvRefreshLifecycle::run(
            RefreshDecision::FailFast {
                reason: "missing snapshot".to_string(),
            },
            || panic!("first refresh closure must not run"),
            || panic!("metadata-only closure must not run"),
            || panic!("incremental closure must not run"),
        );

        assert_eq!(result.unwrap_err(), "missing snapshot");
    }
}
