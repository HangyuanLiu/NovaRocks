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

use std::cmp::Ordering;

use crate::query_execution::contract::QueryId;

use super::contract::QueryLifecycleError;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AttemptId(u64);

impl AttemptId {
    pub fn new(value: u64) -> Result<Self, QueryLifecycleError> {
        if value == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "attempt id must be nonzero",
            ));
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct QueryExecutionId {
    query_id: QueryId,
    attempt_id: AttemptId,
}

impl QueryExecutionId {
    pub fn new(query_id: QueryId, attempt_id: AttemptId) -> Result<Self, QueryLifecycleError> {
        if query_id.high() == 0 && query_id.low() == 0 {
            return Err(QueryLifecycleError::invalid_manifest(
                "query id must be nonzero",
            ));
        }
        Ok(Self {
            query_id,
            attempt_id,
        })
    }

    pub const fn query_id(self) -> QueryId {
        self.query_id
    }

    pub const fn attempt_id(self) -> AttemptId {
        self.attempt_id
    }
}

impl Ord for QueryExecutionId {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.query_id.high(), self.query_id.low(), self.attempt_id).cmp(&(
            other.query_id.high(),
            other.query_id.low(),
            other.attempt_id,
        ))
    }
}

impl PartialOrd for QueryExecutionId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::{AttemptId, QueryExecutionId};
    use crate::query_execution::contract::QueryId;
    use crate::query_execution::lifecycle::contract::QueryLifecycleErrorCode;

    #[test]
    fn query_lifecycle_identity_rejects_zero_attempt() {
        let error = AttemptId::new(0).expect_err("attempt zero must be rejected");
        assert_eq!(error.code(), QueryLifecycleErrorCode::InvalidManifest);
    }

    #[test]
    fn query_lifecycle_identity_rejects_missing_query_id() {
        let error = QueryExecutionId::new(
            QueryId::new(0, 0),
            AttemptId::new(1).expect("nonzero attempt"),
        )
        .expect_err("all-zero query id must be rejected");
        assert_eq!(error.code(), QueryLifecycleErrorCode::InvalidManifest);
    }
}
