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
// software distributed under the Apache License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use novarocks_frontend::FrontendServingLifecycle;
use novarocks_frontend::statistics_jobs::application::StatisticsColumnIntent;
use novarocks_frontend::statistics_jobs::model::{
    StatisticsJob, StatisticsJobCreate, StatisticsJobErrorKind, StatisticsJobState,
    StatisticsJobTarget,
};
use novarocks_frontend::statistics_jobs::repository::{
    MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS, MAX_RECENT_TERMINAL_STATISTICS_JOBS,
    StatisticsJobRepository, StatisticsJobRepositoryErrorKind,
};
use novarocks_frontend::statistics_jobs::worker::{
    StatisticsAnalyzeWorker, StatisticsAttemptError, StatisticsAttemptExecutor,
    StatisticsCollectedAttempt,
};

fn create(at_ms: i64) -> StatisticsJobCreate {
    StatisticsJobCreate {
        target: StatisticsJobTarget {
            catalog: "iceberg".into(),
            namespace: "db".into(),
            table: "t".into(),
        },
        connector_instance_id: "iceberg".into(),
        object_id: b"table-object".to_vec(),
        columns: StatisticsColumnIntent::AllColumns,
        submitted_at_ms: at_ms,
    }
}

struct UnitCollected;

impl StatisticsCollectedAttempt for UnitCollected {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn basis_data_version(&self) -> &[u8] {
        b"test"
    }
}

struct CommitUnknownExecutor {
    publishes: AtomicUsize,
}

impl StatisticsAttemptExecutor for CommitUnknownExecutor {
    fn collect(
        &self,
        _job: &StatisticsJob,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError> {
        Ok(Box::new(UnitCollected))
    }

    fn prepare_publish(
        &self,
        _job: &StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<novarocks_spi::connector::ExternalMutationEvidence, StatisticsAttemptError> {
        novarocks_spi::connector::ExternalMutationEvidence::try_new(
            1,
            novarocks_spi::connector::ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .unwrap(),
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("iceberg")
                    .unwrap(),
            },
            novarocks_spi::connector::ConnectorInstanceIncarnation::from_bytes([1; 16]),
            novarocks_spi::connector::ConnectorMutationOperationId::from_bytes([2; 16]),
            "statistics",
            Bytes::from_static(b"test-evidence"),
        )
        .map_err(|error| {
            StatisticsAttemptError::permanent(StatisticsJobErrorKind::Internal, error.to_string())
        })
    }

    fn publish(
        &self,
        _job: &StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
        _evidence: &novarocks_spi::connector::ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        self.publishes.fetch_add(1, Ordering::SeqCst);
        Err(StatisticsAttemptError::publication(
            novarocks_frontend::statistics_jobs::application::StatisticsPublicationTerminal::CommitUnknown,
            "connector outcome is unknown",
        ))
    }
}

async fn wait_terminal(repository: &StatisticsJobRepository, job_id: uuid::Uuid) -> StatisticsJob {
    for _ in 0..200 {
        let job = repository.get(job_id).await.unwrap().unwrap();
        if job.state.is_terminal() {
            return job;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("statistics job did not reach a terminal state")
}

#[tokio::test]
async fn process_runtime_uses_v7_identities_and_never_recovers_another_incarnation() {
    let first = StatisticsJobRepository::new();
    let job = first.create(create(1)).await.unwrap();
    assert_eq!(job.job_id.get_version_num(), 7);
    assert_eq!(job.operation_id.as_uuid().get_version_num(), 7);
    assert!(first.get(job.job_id).await.unwrap().is_some());

    let restarted = StatisticsJobRepository::new();
    assert!(restarted.get(job.job_id).await.unwrap().is_none());
    assert!(restarted.list().await.unwrap().is_empty());
}

#[tokio::test]
async fn active_and_queued_jobs_are_bounded_without_evicting_live_work() {
    let repository = StatisticsJobRepository::new();
    for index in 0..MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS {
        repository.create(create(index as i64 + 1)).await.unwrap();
    }
    let error = repository.create(create(10_000)).await.unwrap_err();
    assert_eq!(error.kind(), StatisticsJobRepositoryErrorKind::Capacity);
    assert_eq!(
        repository.list().await.unwrap().len(),
        MAX_ACTIVE_OR_QUEUED_STATISTICS_JOBS
    );
}

#[tokio::test]
async fn terminal_history_is_count_bounded_while_active_jobs_remain_visible() {
    let repository = StatisticsJobRepository::new();
    let base = now_ms();
    for index in 0..=MAX_RECENT_TERMINAL_STATISTICS_JOBS {
        let job = repository
            .create(create(base + index as i64))
            .await
            .unwrap();
        let claimed = repository
            .claim_next(base + index as i64)
            .await
            .unwrap()
            .unwrap();
        repository
            .transition(
                claimed.job_id,
                StatisticsJobState::Preparing,
                StatisticsJobState::Running,
                base + index as i64,
                None,
            )
            .await
            .unwrap();
        repository
            .transition(
                claimed.job_id,
                StatisticsJobState::Running,
                StatisticsJobState::Failed,
                base + index as i64,
                None,
            )
            .await
            .unwrap();
        assert_eq!(job.job_id, claimed.job_id);
    }
    let jobs = repository.list().await.unwrap();
    assert_eq!(jobs.len(), MAX_RECENT_TERMINAL_STATISTICS_JOBS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_unknown_is_terminal_and_never_dispatches_another_mutation() {
    let repository = StatisticsJobRepository::new();
    let executor = Arc::new(CommitUnknownExecutor {
        publishes: AtomicUsize::new(0),
    });
    let lifecycle = FrontendServingLifecycle::new();
    lifecycle.mark_ready().expect("mark frontend ready");
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        repository.clone(),
        executor.clone(),
        lifecycle,
    )
    .await
    .unwrap();
    let job = repository.create(create(now_ms())).await.unwrap();
    let terminal = wait_terminal(&repository, job.job_id).await;
    assert_eq!(terminal.state, StatisticsJobState::CommitUnknown);
    assert_eq!(
        terminal.error.unwrap().kind,
        StatisticsJobErrorKind::CommitUnknown
    );
    assert_eq!(executor.publishes.load(Ordering::SeqCst), 1);
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(executor.publishes.load(Ordering::SeqCst), 1);
    worker.shutdown().unwrap();
}

#[tokio::test]
async fn explicit_cancel_of_a_queued_job_is_terminal_and_process_local() {
    let repository = StatisticsJobRepository::new();
    let job = repository.create(create(now_ms())).await.unwrap();
    let cancelled = repository
        .request_cancel(job.job_id, now_ms())
        .await
        .unwrap();
    assert_eq!(cancelled.state, StatisticsJobState::Cancelled);
    assert!(cancelled.cancel_requested);
    assert_eq!(
        cancelled.error.unwrap().kind,
        StatisticsJobErrorKind::Cancelled
    );
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
