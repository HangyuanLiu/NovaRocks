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

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_frontend::statistics_jobs::application::{
    StatisticsColumnIntent, StatisticsTargetCapture,
};
use novarocks_frontend::statistics_jobs::model::{
    StatisticsJobCreate, StatisticsJobError, StatisticsJobErrorKind, StatisticsJobState,
    StatisticsJobTarget,
};
use novarocks_frontend::statistics_jobs::repository::{
    FenceValidator, StatisticsJobRepository, StatisticsJobRepositoryErrorKind,
};
use novarocks_frontend::statistics_jobs::service::{
    AnalyzeTableStatement, CancelAnalyzeStatement, ShowAnalyzeJobsStatement,
    ShowTableStatsStatement, StatisticsApplicationErrorKind, StatisticsApplicationService,
    StatisticsJobTargetResolver, StatisticsStatement, StatisticsStatementResult,
    StatisticsTableStatRow, TableStatisticsReader,
};
use novarocks_frontend::statistics_jobs::worker::{
    STATISTICS_LEASE_RENEW_INTERVAL, StatisticsAnalyzeWorker, StatisticsAnalyzeWorkerCoordination,
    StatisticsAttemptError, StatisticsAttemptExecutor, StatisticsCollectedAttempt,
};
use novarocks_spi::connector::{
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorInstanceIncarnation,
    ConnectorMutationOperationId, ConnectorProviderId, ExternalMutationEvidence,
};
use novarocks_spi::state_store::{
    CommitOutcome, Direction, FeDeploymentView, Key, KeyRange, Precondition, RangeRequest,
    StateStore, TransactionId, Value,
};
use novarocks_state_store::coordination::IncarnationGate;
use novarocks_state_store::{
    OperationId, StateStoreAppConfig, StateStoreConfig, StateStoreHost, StateStoreHostConfig,
    StateStoreLimitOverrides, StateStoreProviderConfig, builtin_state_store_provider_registry,
};
use tempfile::TempDir;

const PREFIX: &str = "novarocks/frontend/statistics/v2/";

fn publication_evidence(
    job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
) -> ExternalMutationEvidence {
    ExternalMutationEvidence::try_new(
        1,
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("statistics-test").expect("provider ID"),
            instance_id: ConnectorInstanceId::parse("statistics-test").expect("instance ID"),
        },
        ConnectorInstanceIncarnation::default(),
        ConnectorMutationOperationId::from_bytes(*job.operation_id.as_bytes()),
        "statistics-publish",
        Bytes::from_static(b"operation-evidence"),
    )
    .expect("test evidence")
}

fn sqlite_config_with_value_limit(path: &Path, max_value_bytes: Option<usize>) -> StateStoreConfig {
    StateStoreConfig {
        cluster_id: "statistics-repository-test".to_string(),
        limits: StateStoreLimitOverrides {
            max_value_bytes,
            ..StateStoreLimitOverrides::default()
        },
        provider: StateStoreProviderConfig::Sqlite {
            path: path.to_path_buf(),
            deployment_owner: "statistics-repository-fe".to_string(),
        },
    }
}

async fn fixture() -> (TempDir, Arc<dyn StateStore>, StatisticsJobRepository) {
    fixture_with_value_limit(None).await
}

async fn fixture_with_value_limit(
    max_value_bytes: Option<usize>,
) -> (TempDir, Arc<dyn StateStore>, StatisticsJobRepository) {
    let temp = TempDir::new().expect("create temp directory");
    let registry = builtin_state_store_provider_registry().expect("built-in provider registry");
    let store = StateStoreHost::open(
        &registry,
        StateStoreHostConfig {
            state_store: StateStoreAppConfig {
                store: sqlite_config_with_value_limit(
                    &temp.path().join("state.sqlite"),
                    max_value_bytes,
                ),
                mysql_client: None,
            },
            foundationdb_client: None,
        },
        FeDeploymentView {
            active_fe_count: NonZeroUsize::new(1).unwrap(),
            topology_revision: Bytes::from_static(b"statistics-repository-topology"),
        },
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("open SQLite state store")
    .state_store()
    .expect("SQLite state store exposure");
    let repository = StatisticsJobRepository::open(Arc::clone(&store))
        .await
        .expect("open statistics repository");
    (temp, store, repository)
}

fn request(table: &str, submitted_at_ms: i64) -> StatisticsJobCreate {
    StatisticsJobCreate {
        target: StatisticsJobTarget {
            catalog: "ice".to_string(),
            namespace: "db".to_string(),
            table: table.to_string(),
        },
        connector_instance_id: "ice".to_string(),
        object_id: format!("object:{table}").into_bytes(),
        columns: StatisticsColumnIntent::Explicit(vec!["v".to_string()]),
        submitted_at_ms,
    }
}

fn always_valid_fence() -> FenceValidator {
    Arc::new(|_| Box::pin(async { Ok(()) }))
}

async fn stored_payloads(store: &dyn StateStore) -> Vec<String> {
    let prefix = Key::try_from(Bytes::from(PREFIX)).expect("valid statistics prefix");
    let range = KeyRange::for_prefix(prefix).expect("valid range");
    let mut transaction = store.begin_read().await.expect("begin raw read");
    let mut request = RangeRequest {
        range,
        direction: Direction::Forward,
        page_size: store.limits().max_page_size,
        continuation: None,
    };
    let mut payloads = Vec::new();
    loop {
        let page = transaction.range(&request).await.expect("read raw page");
        for record in page.records {
            payloads.push(String::from_utf8_lossy(record.value.as_bytes()).into_owned());
        }
        let Some(continuation) = page.continuation else {
            break;
        };
        request.continuation = Some(continuation);
    }
    transaction.abort().await.expect("finish raw read");
    payloads
}

async fn replace_job_schema_version(
    store: &dyn StateStore,
    job_id: uuid::Uuid,
    schema_version: u8,
) {
    let key = Key::try_from(Bytes::from(format!("{PREFIX}jobs/{job_id}"))).expect("valid job key");
    let mut read = store.begin_read().await.expect("begin raw read");
    let record = read
        .get(&key)
        .await
        .expect("read stored job")
        .expect("stored job");
    read.abort().await.expect("finish raw read");
    let mut payload: serde_json::Value =
        serde_json::from_slice(record.value.as_bytes()).expect("decode stored job JSON");
    payload["schema_version"] = serde_json::json!(schema_version);
    let mut write = store
        .begin_write(
            TransactionId::from(uuid::Uuid::now_v7()),
            "replace statistics schema",
        )
        .await
        .expect("begin raw write");
    write
        .put(
            key,
            Value::try_from(Bytes::from(
                serde_json::to_vec(&payload).expect("encode stored job JSON"),
            ))
            .expect("bounded raw job value"),
            Precondition::Any,
        )
        .await
        .expect("replace stored job");
    assert!(matches!(write.commit().await, CommitOutcome::Committed(_)));
}

#[tokio::test]
async fn records_are_versioned_durable_and_identical_analyze_requests_remain_distinct() {
    let (_temp, store, repository) = fixture().await;
    let first = repository
        .create(request("orders", 10))
        .await
        .expect("create first job");
    let second = repository
        .create(request("orders", 11))
        .await
        .expect("create second job");

    assert_ne!(first.job_id, second.job_id);
    assert_ne!(first.operation_id, second.operation_id);
    assert_eq!(first.connector_instance_id, "ice");
    assert_eq!(first.object_id, b"object:orders");
    assert_eq!(
        first.columns,
        StatisticsColumnIntent::Explicit(vec!["v".to_string()])
    );
    assert_eq!(first.job_id.get_version_num(), 7);
    assert_eq!(first.operation_id.get_version_num(), 7);
    assert_eq!(
        repository
            .list_by_state(StatisticsJobState::Submitted)
            .await
            .unwrap()
            .len(),
        2
    );

    let payloads = stored_payloads(store.as_ref()).await;
    assert!(
        payloads
            .iter()
            .any(|payload| payload.contains("\"schema_version\":3"))
    );
    for forbidden in [
        "artifact",
        "sketch",
        "runtime_handle",
        "record_batch",
        "table_handle",
        "\"data_version\"",
        "sql_columns",
    ] {
        assert!(payloads.iter().all(|payload| !payload.contains(forbidden)));
    }
}

#[tokio::test]
async fn durable_column_intent_preserves_all_columns_and_rejects_empty_explicit_list() {
    let (_temp, _store, repository) = fixture().await;
    let mut all_columns = request("all_columns", 10);
    all_columns.columns = StatisticsColumnIntent::AllColumns;
    let created = repository
        .create(all_columns)
        .await
        .expect("persist all-columns intent");
    assert_eq!(created.columns, StatisticsColumnIntent::AllColumns);

    let mut empty_explicit = request("empty_explicit", 11);
    empty_explicit.columns = StatisticsColumnIntent::Explicit(Vec::new());
    let error = repository
        .create(empty_explicit)
        .await
        .expect_err("empty explicit intent must not become all-columns");
    assert_eq!(error.kind(), StatisticsJobRepositoryErrorKind::Corruption);
}

#[tokio::test]
async fn record_budget_failure_is_typed_and_leaves_no_job_or_index_write() {
    let (_temp, store, repository) = fixture_with_value_limit(Some(1)).await;

    let error = repository
        .create(request("budget_limited", 10))
        .await
        .expect_err("one-byte StateStore budget must reject the complete record");

    assert_eq!(
        error.kind(),
        StatisticsJobRepositoryErrorKind::BudgetExceeded
    );
    assert_eq!(
        error.budget().map(|budget| budget.record_kind),
        Some("statistics-job")
    );
    let budget = error.budget().expect("typed record budget details");
    assert_eq!(budget.schema_version, 3);
    assert!(budget.actual_bytes > budget.limit_bytes);
    assert_eq!(budget.limit_bytes, 1);
    assert!(stored_payloads(store.as_ref()).await.is_empty());
}

#[tokio::test]
async fn unsupported_stored_schema_fails_closed() {
    let (_temp, store, repository) = fixture().await;
    let created = repository
        .create(request("schema_mismatch", 10))
        .await
        .expect("create durable job");
    replace_job_schema_version(store.as_ref(), created.job_id, 255).await;

    let error = repository
        .get(created.job_id)
        .await
        .expect_err("unknown durable schema must not be decoded");
    assert_eq!(
        error.kind(),
        StatisticsJobRepositoryErrorKind::UnsupportedSchemaVersion
    );
    assert!(error.to_string().contains("unsupported schema version"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_creates_retry_sqlite_snapshot_conflicts_with_stable_identities() {
    let (_temp, _store, repository) = fixture().await;
    let first_repository = repository.clone();
    let second_repository = repository.clone();
    let (first, second) = tokio::join!(
        first_repository.create(request("concurrent_orders", 10)),
        second_repository.create(request("concurrent_orders", 11)),
    );
    let first = first.expect("first concurrent create");
    let second = second.expect("second concurrent create");

    assert_ne!(first.job_id, second.job_id);
    assert_ne!(first.operation_id, second.operation_id);
    assert_eq!(
        repository
            .list_by_state(StatisticsJobState::Submitted)
            .await
            .expect("list submitted jobs")
            .len(),
        2
    );
}

struct SucceedingStatisticsExecutor {
    collected: AtomicUsize,
    published: AtomicUsize,
}

struct TransientlyFailingStatisticsExecutor {
    attempts: AtomicUsize,
}

struct ReplacedTargetStatisticsExecutor {
    attempts: AtomicUsize,
}

struct RenewingStatisticsExecutor {
    published: AtomicUsize,
}

impl StatisticsAttemptExecutor for RenewingStatisticsExecutor {
    fn collect(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError> {
        std::thread::sleep(STATISTICS_LEASE_RENEW_INTERVAL + Duration::from_millis(250));
        Ok(Box::new(()))
    }

    fn prepare_publish(
        &self,
        job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsAttemptError> {
        Ok(publication_evidence(job))
    }

    fn publish(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        self.published.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn reconcile(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        unreachable!("fresh statistics attempt must not reconcile")
    }
}

impl StatisticsAttemptExecutor for TransientlyFailingStatisticsExecutor {
    fn collect(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        Err(StatisticsAttemptError::transient(
            StatisticsJobErrorKind::Collection,
            "temporary collector outage",
        ))
    }

    fn prepare_publish(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsAttemptError> {
        panic!("collection failure must not enter publish")
    }

    fn publish(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        panic!("collection failure must not enter publish")
    }

    fn reconcile(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        panic!("collection failure must not enter publish")
    }
}

impl StatisticsAttemptExecutor for ReplacedTargetStatisticsExecutor {
    fn collect(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError> {
        self.attempts.fetch_add(1, Ordering::AcqRel);
        Err(StatisticsAttemptError::permanent(
            StatisticsJobErrorKind::TargetReplaced,
            "captured table object was replaced",
        ))
    }

    fn prepare_publish(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsAttemptError> {
        panic!("replaced target must not enter publish")
    }

    fn publish(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        panic!("replaced target must not enter publish")
    }

    fn reconcile(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        panic!("replaced target must not reconcile")
    }
}

impl StatisticsAttemptExecutor for SucceedingStatisticsExecutor {
    fn collect(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
    ) -> Result<Box<dyn StatisticsCollectedAttempt>, StatisticsAttemptError> {
        self.collected.fetch_add(1, Ordering::AcqRel);
        Ok(Box::new(()))
    }

    fn prepare_publish(
        &self,
        job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
    ) -> Result<ExternalMutationEvidence, StatisticsAttemptError> {
        Ok(publication_evidence(job))
    }

    fn publish(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _collected: &dyn StatisticsCollectedAttempt,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        self.published.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn reconcile(
        &self,
        _job: &novarocks_frontend::statistics_jobs::model::StatisticsJob,
        _evidence: &ExternalMutationEvidence,
    ) -> Result<(), StatisticsAttemptError> {
        self.published.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_claims_collects_and_publishes_under_the_fenced_lease() {
    let (_temp, _store, repository) = fixture().await;
    let job = repository
        .create(request("worker_orders", 10))
        .await
        .expect("create job");
    let concrete_executor = Arc::new(SucceedingStatisticsExecutor {
        collected: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        Arc::clone(&executor),
    )
    .await
    .expect("start worker");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = repository
                .get(job.job_id)
                .await
                .expect("read job")
                .expect("durable job");
            if current.state == StatisticsJobState::Succeeded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker must finish job");
    worker.shutdown().expect("shutdown worker");

    assert_eq!(concrete_executor.collected.load(Ordering::Acquire), 1);
    assert_eq!(concrete_executor.published.load(Ordering::Acquire), 1);
    let succeeded = repository
        .get(job.job_id)
        .await
        .expect("read succeeded job")
        .expect("durable succeeded job");
    assert_eq!(
        succeeded.basis_data_version,
        Some(b"unit-test-basis".to_vec())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_marks_typed_replaced_target_stale_without_retrying() {
    let (_temp, _store, repository) = fixture().await;
    let job = repository
        .create(request("replaced_worker_orders", 10))
        .await
        .expect("create job");
    let concrete_executor = Arc::new(ReplacedTargetStatisticsExecutor {
        attempts: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        executor,
    )
    .await
    .expect("start worker");

    let stale = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = repository
                .get(job.job_id)
                .await
                .expect("read job")
                .expect("durable job");
            if current.state == StatisticsJobState::Stale {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker must mark replaced target stale");
    worker.shutdown().expect("shutdown worker");

    assert_eq!(concrete_executor.attempts.load(Ordering::Acquire), 1);
    assert_eq!(
        stale.error.map(|error| error.kind),
        Some(StatisticsJobErrorKind::TargetReplaced)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_does_not_claim_new_submitted_jobs_while_restore_is_reconciling() {
    let (_temp, store, repository) = fixture().await;
    StatisticsAnalyzeWorkerCoordination::open(Arc::clone(&store))
        .await
        .expect("bootstrap statistics coordination");
    let gate = IncarnationGate::new(store);
    let open = gate.load().await.expect("load write-open control plane");
    gate.begin_restore(&open, OperationId::new_v7())
        .await
        .expect("begin restore");

    let job = repository
        .create(request("restore_blocked_worker_orders", 10))
        .await
        .expect("create submitted job");
    let concrete_executor = Arc::new(SucceedingStatisticsExecutor {
        collected: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        executor,
    )
    .await
    .expect("start worker in reconciling mode");

    tokio::time::sleep(Duration::from_millis(750)).await;
    worker.shutdown().expect("shutdown worker");

    let current = repository
        .get(job.job_id)
        .await
        .expect("read submitted job")
        .expect("durable job");
    assert_eq!(current.state, StatisticsJobState::Submitted);
    assert_eq!(concrete_executor.collected.load(Ordering::Acquire), 0);
    assert_eq!(concrete_executor.published.load(Ordering::Acquire), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_uses_the_latest_fence_after_lease_renewal() {
    let (_temp, _store, repository) = fixture().await;
    let job = repository
        .create(request("renewed_worker_orders", 10))
        .await
        .expect("create job");
    let concrete_executor = Arc::new(RenewingStatisticsExecutor {
        published: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        Arc::clone(&executor),
    )
    .await
    .expect("start worker");

    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let current = repository
                .get(job.job_id)
                .await
                .expect("read job")
                .expect("durable job");
            if current.state == StatisticsJobState::Succeeded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker must finish after renewing its lease");
    worker.shutdown().expect("shutdown worker");
    assert_eq!(concrete_executor.published.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_reconciles_publishing_without_recollecting() {
    let (_temp, _store, repository) = fixture().await;
    let fence = always_valid_fence();
    let created = repository
        .create(request("publishing_orders", 10))
        .await
        .expect("create job");
    repository
        .claim(created.job_id, 11, &fence)
        .await
        .expect("claim job");
    repository
        .transition(
            created.job_id,
            StatisticsJobState::Preparing,
            StatisticsJobState::Running,
            12,
            None,
            &fence,
        )
        .await
        .expect("run job");
    repository
        .begin_publishing(
            created.job_id,
            13,
            publication_evidence(&created)
                .try_to_wire_v1()
                .expect("encode test evidence"),
            Bytes::from_static(b"basis-v1"),
            &fence,
        )
        .await
        .expect("begin publish");
    let publishing_before_recovery = repository
        .get(created.job_id)
        .await
        .expect("read publishing job")
        .expect("durable publishing job");
    let stored_evidence = ExternalMutationEvidence::try_from_wire_v1(
        publishing_before_recovery
            .publication_evidence
            .as_deref()
            .expect("publishing job evidence"),
    )
    .expect("decode publishing job evidence");
    assert_eq!(
        stored_evidence.operation_id().to_bytes(),
        *created.operation_id.as_bytes()
    );

    let concrete_executor = Arc::new(SucceedingStatisticsExecutor {
        collected: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        Arc::clone(&executor),
    )
    .await
    .expect("start worker");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if repository
                .get(created.job_id)
                .await
                .expect("read job")
                .expect("durable job")
                .state
                == StatisticsJobState::Succeeded
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker must reconcile publishing job");
    worker.shutdown().expect("shutdown worker");
    assert_eq!(concrete_executor.collected.load(Ordering::Acquire), 0);
    assert_eq!(concrete_executor.published.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_retries_transient_collection_at_most_three_times_with_one_operation() {
    let (_temp, _store, repository) = fixture().await;
    let created = repository
        .create(request("retry_orders", 10))
        .await
        .expect("create job");
    let concrete_executor = Arc::new(TransientlyFailingStatisticsExecutor {
        attempts: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        Arc::clone(&executor),
    )
    .await
    .expect("start worker");

    let failed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = repository
                .get(created.job_id)
                .await
                .expect("read job")
                .expect("durable job");
            if current.state == StatisticsJobState::Failed {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker must exhaust retries");
    worker.shutdown().expect("shutdown worker");
    assert_eq!(failed.operation_id, created.operation_id);
    assert_eq!(failed.attempt, 3);
    assert_eq!(concrete_executor.attempts.load(Ordering::Acquire), 3);
}

#[tokio::test]
async fn claim_transitions_with_fence_and_cancel_observes_publish_boundary() {
    let (_temp, _store, repository) = fixture().await;
    let fence = always_valid_fence();
    let created = repository
        .create(request("orders", 10))
        .await
        .expect("create job");
    let preparing = repository
        .claim(created.job_id, 11, &fence)
        .await
        .expect("claim job")
        .expect("submitted job claimed");
    assert_eq!(preparing.state, StatisticsJobState::Preparing);
    assert_eq!(preparing.attempt, 1);
    let running = repository
        .transition(
            created.job_id,
            StatisticsJobState::Preparing,
            StatisticsJobState::Running,
            12,
            None,
            &fence,
        )
        .await
        .expect("start job");
    assert_eq!(running.state, StatisticsJobState::Running);
    let publishing = repository
        .begin_publishing(
            created.job_id,
            13,
            publication_evidence(&created)
                .try_to_wire_v1()
                .expect("encode test evidence"),
            Bytes::from_static(b"basis-v1"),
            &fence,
        )
        .await
        .expect("publish job");
    assert_eq!(publishing.state, StatisticsJobState::Publishing);
    let conflict = repository
        .cancel(created.job_id, 14, &fence)
        .await
        .unwrap_err();
    assert_eq!(conflict.kind(), StatisticsJobRepositoryErrorKind::Conflict);
    let succeeded = repository
        .transition(
            created.job_id,
            StatisticsJobState::Publishing,
            StatisticsJobState::Succeeded,
            15,
            None,
            &fence,
        )
        .await
        .expect("complete job");
    assert_eq!(succeeded.completed_at_ms, Some(15));

    let failed = repository
        .create(request("lineitem", 20))
        .await
        .expect("create failed job");
    let _ = repository
        .claim(failed.job_id, 21, &fence)
        .await
        .expect("claim failed job");
    let failed = repository
        .transition(
            failed.job_id,
            StatisticsJobState::Preparing,
            StatisticsJobState::Failed,
            22,
            Some(StatisticsJobError {
                kind: StatisticsJobErrorKind::Collection,
                message: "connector timed out".to_string(),
            }),
            &fence,
        )
        .await
        .expect("fail job");
    assert_eq!(failed.state, StatisticsJobState::Failed);
    assert_eq!(
        failed.error.unwrap().kind,
        StatisticsJobErrorKind::Collection
    );
}

struct StaticTableStatistics;

struct StaticStatisticsTargetResolver;

struct RecordingStatisticsTargetResolver {
    calls: Mutex<Vec<StatisticsJobTarget>>,
}

impl RecordingStatisticsTargetResolver {
    fn calls(&self) -> Vec<StatisticsJobTarget> {
        self.calls.lock().expect("statistics target calls").clone()
    }
}

impl StatisticsJobTargetResolver for StaticStatisticsTargetResolver {
    fn capture_table_object(
        &self,
        target: &StatisticsJobTarget,
    ) -> Result<StatisticsTargetCapture, String> {
        Ok(StatisticsTargetCapture {
            connector_instance_id: target.catalog.clone(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            object_id: format!("object:{}:{}", target.namespace, target.table).into_bytes(),
            sql_columns: vec!["v".to_string()],
        })
    }
}

impl StatisticsJobTargetResolver for RecordingStatisticsTargetResolver {
    fn capture_table_object(
        &self,
        target: &StatisticsJobTarget,
    ) -> Result<StatisticsTargetCapture, String> {
        self.calls
            .lock()
            .expect("statistics target calls")
            .push(target.clone());
        if target.table == "missing_external_table" {
            return Err(format!(
                "unknown external table {}.{}.{}",
                target.catalog, target.namespace, target.table
            ));
        }
        Ok(StatisticsTargetCapture {
            connector_instance_id: "statistics-test".to_string(),
            namespace: target.namespace.clone(),
            table: target.table.clone(),
            object_id: format!(
                "object:{}:{}:{}",
                target.catalog, target.namespace, target.table
            )
            .into_bytes(),
            sql_columns: vec!["value".to_string()],
        })
    }
}

impl TableStatisticsReader for StaticTableStatistics {
    fn show_table_stats(
        &self,
        target: &StatisticsJobTarget,
    ) -> Result<Vec<StatisticsTableStatRow>, String> {
        Ok(vec![StatisticsTableStatRow {
            metric_name: format!(
                "{}.{}.{}.row_count",
                target.catalog, target.namespace, target.table
            ),
            value: Some("1".to_string()),
            status: "AVAILABLE".to_string(),
            basis_version: "SAME".to_string(),
            source: "PROVIDER_ARTIFACT".to_string(),
            numeric_nature: "EXACT".to_string(),
            basis_relation: "IDENTICAL".to_string(),
        }])
    }
}

#[tokio::test]
async fn typed_application_never_reparses_sql_and_keeps_reads_available_without_state_store() {
    let table_statistics = StaticTableStatistics;
    let target = request("orders", 10).target;
    let unavailable = StatisticsApplicationService::unavailable();
    let read = unavailable
        .execute(
            StatisticsStatement::ShowTableStats(ShowTableStatsStatement {
                target: target.clone(),
            }),
            10,
            &table_statistics,
        )
        .await
        .expect("read-only table statistics remain available without StateStore");
    assert!(matches!(read, StatisticsStatementResult::TableStats(_)));
    let error = unavailable
        .execute(
            StatisticsStatement::AnalyzeTable(AnalyzeTableStatement {
                target: target.clone(),
                columns: StatisticsColumnIntent::AllColumns,
            }),
            10,
            &table_statistics,
        )
        .await
        .expect_err("ANALYZE requires a durable StateStore");
    assert_eq!(
        error.kind(),
        StatisticsApplicationErrorKind::StateStoreRequired
    );

    let (_temp, _store, repository) = fixture().await;
    let service = StatisticsApplicationService::with_repository_and_target_resolver(
        repository,
        Arc::new(StaticStatisticsTargetResolver),
    );
    let submitted = service
        .execute(
            StatisticsStatement::AnalyzeTable(AnalyzeTableStatement {
                target: target.clone(),
                columns: StatisticsColumnIntent::AllColumns,
            }),
            11,
            &table_statistics,
        )
        .await
        .expect("typed ANALYZE creates a job");
    assert!(matches!(
        submitted,
        StatisticsStatementResult::JobSubmitted(ref job)
            if job.columns == StatisticsColumnIntent::AllColumns
    ));
    let listed = service
        .execute(
            StatisticsStatement::ShowAnalyzeJobs(ShowAnalyzeJobsStatement {
                target: Some(target),
            }),
            12,
            &table_statistics,
        )
        .await
        .expect("typed SHOW ANALYZE JOBS reads durable jobs");
    assert!(matches!(listed, StatisticsStatementResult::AnalyzeJobs(jobs) if jobs.len() == 1));
}

#[tokio::test]
async fn sqlx2_application_analyze_target_resolution_preserves_admitted_external_identity() {
    let (_temp, _store, repository) = fixture().await;
    let resolver = Arc::new(RecordingStatisticsTargetResolver {
        calls: Mutex::new(Vec::new()),
    });
    let service = StatisticsApplicationService::with_repository_and_target_resolver(
        repository.clone(),
        resolver.clone(),
    );
    let table_statistics = StaticTableStatistics;
    let admitted_targets = [
        // The Core command route has already applied the session's current
        // catalog before the Frontend durable-job boundary is reached.
        StatisticsJobTarget {
            catalog: "current_catalog".to_string(),
            namespace: "current_db".to_string(),
            table: "current_table".to_string(),
        },
        // A two-part external name keeps the admitted catalog while changing
        // only the namespace.
        StatisticsJobTarget {
            catalog: "current_catalog".to_string(),
            namespace: "other_db".to_string(),
            table: "two_part_table".to_string(),
        },
        StatisticsJobTarget {
            catalog: "explicit_catalog".to_string(),
            namespace: "explicit_db".to_string(),
            table: "three_part_table".to_string(),
        },
    ];

    for (index, target) in admitted_targets.iter().cloned().enumerate() {
        let submitted = service
            .execute(
                StatisticsStatement::AnalyzeTable(AnalyzeTableStatement {
                    target: target.clone(),
                    columns: StatisticsColumnIntent::AllColumns,
                }),
                100 + index as i64,
                &table_statistics,
            )
            .await
            .expect("resolved external ANALYZE target must create a durable job");
        assert!(matches!(
            submitted,
            StatisticsStatementResult::JobSubmitted(job)
                if job.target == target
                    && job.object_id
                        == format!("object:{}:{}:{}", target.catalog, target.namespace, target.table)
                            .into_bytes()
                    && job.columns == StatisticsColumnIntent::AllColumns
        ));
    }

    assert_eq!(resolver.calls(), admitted_targets);
    let jobs = repository.list().await.expect("list durable jobs");
    assert_eq!(jobs.len(), 3);
}

#[tokio::test]
async fn sqlx2_application_analyze_unknown_target_fails_before_durable_job_creation() {
    let (_temp, _store, repository) = fixture().await;
    let resolver = Arc::new(RecordingStatisticsTargetResolver {
        calls: Mutex::new(Vec::new()),
    });
    let service = StatisticsApplicationService::with_repository_and_target_resolver(
        repository.clone(),
        resolver.clone(),
    );
    let table_statistics = StaticTableStatistics;
    let target = StatisticsJobTarget {
        catalog: "current_catalog".to_string(),
        namespace: "other_db".to_string(),
        table: "missing_external_table".to_string(),
    };

    let error = service
        .execute(
            StatisticsStatement::AnalyzeTable(AnalyzeTableStatement {
                target: target.clone(),
                columns: StatisticsColumnIntent::AllColumns,
            }),
            200,
            &table_statistics,
        )
        .await
        .expect_err("unknown external target must fail before durable job creation");

    assert_eq!(
        error.kind(),
        StatisticsApplicationErrorKind::TargetResolution
    );
    assert!(error.to_string().contains("unknown external table"));
    assert_eq!(resolver.calls(), vec![target]);
    assert!(
        repository
            .list()
            .await
            .expect("list durable jobs after failed resolution")
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_cancel_records_intent_and_the_fenced_worker_transitions_it() {
    let (_temp, _store, repository) = fixture().await;
    let service = StatisticsApplicationService::with_repository(repository.clone());
    let table_statistics = StaticTableStatistics;
    let created = repository
        .create(request("cancelled_orders", 10))
        .await
        .expect("create durable job");

    let requested = service
        .execute(
            StatisticsStatement::CancelAnalyze(CancelAnalyzeStatement {
                job_id: created.job_id,
            }),
            11,
            &table_statistics,
        )
        .await
        .expect("record cancellation request");
    assert!(matches!(
        requested,
        StatisticsStatementResult::JobCancellationRequested(job)
            if job.cancel_requested && job.state == StatisticsJobState::Submitted
    ));

    let concrete_executor = Arc::new(SucceedingStatisticsExecutor {
        collected: AtomicUsize::new(0),
        published: AtomicUsize::new(0),
    });
    let executor: Arc<dyn StatisticsAttemptExecutor> = concrete_executor.clone();
    let mut worker = StatisticsAnalyzeWorker::start(
        &tokio::runtime::Handle::current(),
        Arc::new(repository.clone()),
        Arc::clone(&executor),
    )
    .await
    .expect("start worker");
    let cancelled = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = repository
                .get(created.job_id)
                .await
                .expect("read job")
                .expect("durable job");
            if current.state == StatisticsJobState::Cancelled {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker must consume cancellation intent");
    worker.shutdown().expect("shutdown worker");
    assert!(!cancelled.cancel_requested);
    assert_eq!(concrete_executor.collected.load(Ordering::Acquire), 0);
    assert_eq!(concrete_executor.published.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn failover_requeues_preparing_and_running_before_publish() {
    let (_temp, _store, repository) = fixture().await;
    let fence = always_valid_fence();
    let job = repository
        .create(request("orders", 10))
        .await
        .expect("create job");
    let preparing = repository
        .claim(job.job_id, 11, &fence)
        .await
        .expect("claim job")
        .expect("job claimed");
    assert_eq!(preparing.attempt, 1);
    let requeued = repository
        .requeue_incomplete(job.job_id, 12, &fence)
        .await
        .expect("requeue PREPARING job")
        .expect("PREPARING is replayable");
    assert_eq!(requeued.state, StatisticsJobState::Submitted);
    let preparing = repository
        .claim(job.job_id, 13, &fence)
        .await
        .expect("claim replayed job")
        .expect("job reclaimed");
    assert_eq!(preparing.attempt, 2);
    let running = repository
        .transition(
            job.job_id,
            StatisticsJobState::Preparing,
            StatisticsJobState::Running,
            14,
            None,
            &fence,
        )
        .await
        .expect("begin collection");
    assert_eq!(running.state, StatisticsJobState::Running);
    let requeued = repository
        .requeue_incomplete(job.job_id, 15, &fence)
        .await
        .expect("requeue RUNNING job")
        .expect("RUNNING is replayable");
    assert_eq!(requeued.state, StatisticsJobState::Submitted);
}
