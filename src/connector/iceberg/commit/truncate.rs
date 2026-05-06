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

//! `TruncateCommit` — write a single `operation=delete` snapshot that
//! marks every live data + delete file as DELETED while preserving
//! schema, partition spec, properties, and other refs.
//!
//! Skeleton only: the real `commit()` body lands in the next task.

use async_trait::async_trait;

use super::action::{CommitCtx, IcebergCommitAction};
use super::types::CommitOutcome;

pub struct TruncateCommit;

#[async_trait]
impl IcebergCommitAction for TruncateCommit {
    async fn commit(&self, _ctx: CommitCtx<'_>) -> Result<CommitOutcome, String> {
        Err("TruncateCommit::commit not implemented".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_commit_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>(_: &T) {}
        assert_send_sync(&TruncateCommit);
    }
}
