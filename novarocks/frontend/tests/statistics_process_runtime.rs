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

//! Product-owner integration checks reached through frontend re-exports.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_frontend::statistics_jobs::model::{
    StatisticsColumns, StatisticsFailure, StatisticsJobConclusion, StatisticsJobCreate,
    StatisticsJobPhase, StatisticsJobState, StatisticsPublicationFact, StatisticsTarget,
};
use novarocks_frontend::statistics_jobs::repository::StatisticsJobRepository;
use novarocks_frontend::statistics_jobs::worker::{
    StatisticsAttemptError, StatisticsAttemptExecutor, StatisticsPublicationOutcome,
    StatisticsWorker,
};
use novarocks_workload_control::{
    ResourceConfig, WorkClass, WorkOwner, WorkRequest, WorkScope, WorkloadConfig, WorkloadControl,
};

fn create(at_ms: i64) -> StatisticsJobCreate {
    StatisticsJobCreate {
        target: StatisticsTarget {
            catalog: Arc::from("iceberg"),
            namespace: Arc::from("db"),
            table: Arc::from("t"),
            object_id: Arc::from(&b"table-object"[..]),
        },
        columns: StatisticsColumns::All,
        submitted_at_ms: at_ms,
    }
}

fn root() -> WorkOwner {
    let control = WorkloadControl::try_new(
        WorkloadConfig::default(),
        ResourceConfig {
            total_bytes: 16 * 1024 * 1024,
            control_bytes: 1024 * 1024,
            per_scope_bytes: 8 * 1024 * 1024,
        },
    )
    .expect("workload control");
    control.mark_ready().expect("ready");
    control
        .try_begin_root(WorkRequest::new(WorkClass::Statistics))
        .expect("root")
        .owner
}

struct PublishExecutor {
    publications: AtomicUsize,
    outcome: StatisticsPublicationFact,
    finalization_failure: bool,
}
impl StatisticsAttemptExecutor for PublishExecutor {
    fn prepare(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        scope: &WorkScope,
    ) -> Result<(), StatisticsAttemptError> {
        scope.check().map_err(failed)
    }
    fn collect(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        scope: &WorkScope,
    ) -> Result<(), StatisticsAttemptError> {
        scope.check().map_err(failed)
    }
    fn publish(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        scope: &WorkScope,
    ) -> Result<StatisticsPublicationOutcome, StatisticsAttemptError> {
        scope.check().map_err(failed)?;
        self.publications.fetch_add(1, Ordering::SeqCst);
        Ok(StatisticsPublicationOutcome {
            fact: self.outcome,
            finalization_failure: self.finalization_failure.then(|| StatisticsFailure {
                message: Arc::from("finalization projection failed"),
            }),
        })
    }
}
fn failed(error: novarocks_workload_control::WorkError) -> StatisticsAttemptError {
    StatisticsAttemptError::Failed(StatisticsFailure {
        message: Arc::from(error.to_string()),
    })
}

#[tokio::test]
async fn frontend_adapter_exposes_distinct_process_local_identities() {
    let repository = StatisticsJobRepository::new();
    let submitted = repository.create(create(1), root()).await.expect("create");
    let claimed = repository.claim_next(2).await.expect("claim").expect("job");
    assert_eq!(submitted.id.as_uuid().get_version_num(), 7);
    assert_eq!(submitted.publication_id.as_uuid().get_version_num(), 7);
    assert_ne!(
        submitted.id.as_uuid(),
        claimed.logical_execution_id.unwrap().as_uuid()
    );
    assert_ne!(
        claimed.logical_execution_id.unwrap().as_uuid(),
        claimed.query_attempt_id.unwrap().as_uuid()
    );
    assert_ne!(
        submitted.publication_id.as_uuid(),
        claimed.query_attempt_id.unwrap().as_uuid()
    );
    assert!(
        StatisticsJobRepository::new()
            .get(submitted.id)
            .await
            .expect("get")
            .is_none()
    );
}

#[tokio::test]
async fn commit_unknown_is_terminal_and_not_redispatched() {
    let repository = StatisticsJobRepository::new();
    let executor = Arc::new(PublishExecutor {
        publications: AtomicUsize::new(0),
        outcome: StatisticsPublicationFact::CommitUnknown,
        finalization_failure: false,
    });
    let worker = StatisticsWorker::new(repository.clone(), executor.clone());
    let job = repository.create(create(1), root()).await.expect("create");
    let terminal = worker.run_one(2).await.expect("run").expect("terminal");
    assert_eq!(
        terminal.state,
        StatisticsJobState::Terminal(StatisticsJobConclusion::CommitUnknown)
    );
    assert_eq!(
        terminal.publication,
        StatisticsPublicationFact::CommitUnknown
    );
    assert_eq!(executor.publications.load(Ordering::SeqCst), 1);
    assert!(worker.run_one(3).await.expect("idle").is_none());
    assert_eq!(executor.publications.load(Ordering::SeqCst), 1);
    assert_eq!(
        repository.get(job.id).await.expect("get").unwrap().state,
        terminal.state
    );
}

#[tokio::test]
async fn known_commit_finalization_failure_retains_the_provider_fact() {
    let repository = StatisticsJobRepository::new();
    let executor = Arc::new(PublishExecutor {
        publications: AtomicUsize::new(0),
        outcome: StatisticsPublicationFact::KnownCommitted,
        finalization_failure: true,
    });
    let worker = StatisticsWorker::new(repository.clone(), executor);
    repository.create(create(1), root()).await.expect("create");
    let terminal = worker.run_one(2).await.expect("run").expect("terminal");
    assert_eq!(
        terminal.state,
        StatisticsJobState::Terminal(StatisticsJobConclusion::Succeeded)
    );
    assert_eq!(
        terminal.publication,
        StatisticsPublicationFact::KnownCommitted
    );
    assert_eq!(
        terminal
            .publication_finalization_failure
            .unwrap()
            .message
            .as_ref(),
        "finalization projection failed"
    );
    assert!(terminal.convergence.is_complete());
}

#[tokio::test]
async fn submitted_is_a_phase_not_a_success_conclusion() {
    let repository = StatisticsJobRepository::new();
    let job = repository.create(create(1), root()).await.expect("create");
    assert_eq!(
        job.state,
        StatisticsJobState::Active(StatisticsJobPhase::Submitted)
    );
    assert!(!job.state.is_terminal());
}
