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

//! Statistics job business operations.

use std::sync::Arc;

use novarocks_workload_control::WorkOwner;

use crate::{
    StatisticsAttemptExecutor, StatisticsJob, StatisticsJobCreate, StatisticsJobId,
    StatisticsJobRepository, StatisticsRepositoryError, StatisticsWorker,
};

/// The process-local owner of statistics job submission and lifecycle lookup.
///
/// SQL, connector target capture, and Native attempt execution are supplied by
/// consumer applications. This service owns the business transition that makes
/// a captured target and a governed root responsibility into a statistics job.
#[derive(Clone)]
pub struct StatisticsJobService {
    repository: StatisticsJobRepository,
}

impl StatisticsJobService {
    pub fn new() -> Self {
        Self {
            repository: StatisticsJobRepository::new(),
        }
    }

    pub async fn submit(
        &self,
        request: StatisticsJobCreate,
        owner: WorkOwner,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository.create(request, owner).await
    }

    pub async fn list(&self) -> Result<Vec<StatisticsJob>, StatisticsRepositoryError> {
        self.repository.list().await
    }

    pub async fn request_cancel(
        &self,
        job_id: StatisticsJobId,
        at_ms: i64,
    ) -> Result<StatisticsJob, StatisticsRepositoryError> {
        self.repository.request_cancel(job_id, at_ms).await
    }

    /// Executes one claimed job with the role-supplied Native attempt adapter.
    /// The role owns that adapter; the product service retains the job state
    /// and worker orchestration.
    pub async fn run_one(
        &self,
        executor: Arc<dyn StatisticsAttemptExecutor>,
        at_ms: i64,
    ) -> Result<Option<StatisticsJob>, StatisticsRepositoryError> {
        StatisticsWorker::new(self.repository.clone(), executor)
            .run_one(at_ms)
            .await
    }
}

impl Default for StatisticsJobService {
    fn default() -> Self {
        Self::new()
    }
}
