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

//! Focused statement-local TRUNCATE publication tests.

use std::any::Any;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_frontend::common::admitted_query_context::{RequestAdmission, RequestContext};
use novarocks_frontend::common::backend_topology::BackendTopologySnapshot;
use novarocks_frontend::common::query_cancellation::QueryCancellationSource;
use novarocks_frontend::dml::DmlService;
use novarocks_frontend::query_execution::dml::truncate::{
    PlanTruncateRequest, PreparedTruncate, TruncateCommand, TruncateEffect, TruncateEngine,
    TruncateEvidence, TruncateFailure, TruncateFailureKind, TruncateFinalization, TruncateOutcome,
    TruncatePlanError, TruncatePlanFacts, TruncatePlanSummary, TruncatePrepared, TruncateReceipt,
};
use novarocks_frontend::FrontendStatisticsService;
use novarocks_spi::connector::LakePublicationDisposition;
use novarocks_types::ClusterRole;

#[derive(Clone, Copy)]
enum Mode {
    Committed,
    CommittedFinalizationFailed,
    CommitUnknown,
    KnownUncommitted,
}

struct FakePrepared;

impl TruncatePrepared for FakePrepared {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

struct FakeTruncateEngine {
    execute: Mode,
    adjudicate: Mode,
    plan_calls: AtomicUsize,
    execute_calls: AtomicUsize,
    adjudicate_calls: AtomicUsize,
    cleanup_calls: AtomicUsize,
    delete_calls: AtomicUsize,
}

impl FakeTruncateEngine {
    fn new(execute: Mode, adjudicate: Mode) -> Self {
        Self {
            execute,
            adjudicate,
            plan_calls: AtomicUsize::new(0),
            execute_calls: AtomicUsize::new(0),
            adjudicate_calls: AtomicUsize::new(0),
            cleanup_calls: AtomicUsize::new(0),
            delete_calls: AtomicUsize::new(0),
        }
    }

    fn outcome(&self, mode: Mode, facts: &TruncatePlanFacts) -> TruncateOutcome {
        match mode {
            Mode::Committed | Mode::CommittedFinalizationFailed => {
                TruncateOutcome::KnownCommitted {
                    effect: TruncateEffect::Applied,
                    receipt: receipt(facts),
                    finalization: if matches!(mode, Mode::CommittedFinalizationFailed) {
                        TruncateFinalization::Failed(failure("cache invalidation failed"))
                    } else {
                        TruncateFinalization::Complete
                    },
                }
            }
            Mode::CommitUnknown => TruncateOutcome::CommitUnknown {
                failure: failure("catalog response lost"),
                evidence: TruncateEvidence {
                    schema_version: 1,
                    digest: [7; 32],
                    wire_bytes: vec![1, 2, 3],
                },
            },
            Mode::KnownUncommitted => TruncateOutcome::KnownUncommitted {
                failure: failure("catalog rejected mutation"),
            },
        }
    }
}

impl TruncateEngine for FakeTruncateEngine {
    fn plan_truncate(
        &self,
        request: PlanTruncateRequest,
    ) -> Result<PreparedTruncate, TruncatePlanError> {
        self.plan_calls.fetch_add(1, Ordering::SeqCst);
        Ok(PreparedTruncate {
            facts: TruncatePlanFacts {
                catalog: "ice".to_string(),
                namespace: "db".to_string(),
                table: "orders".to_string(),
                target_ref: request.command.target_ref,
                provider_id: "iceberg".to_string(),
                instance_id: "ice".to_string(),
                incarnation: [1; 16],
                mutation_operation_id: request.mutation_operation_id,
                request_digest: [2; 32],
                plan_digest: [3; 32],
                state_digest: [4; 32],
                summary: TruncatePlanSummary {
                    file_count: 3,
                    row_count: 5,
                    total_bytes: 8,
                },
            },
            handle: Arc::new(FakePrepared),
        })
    }

    fn execute_truncate(&self, _prepared: &dyn TruncatePrepared) -> TruncateOutcome {
        self.execute_calls.fetch_add(1, Ordering::SeqCst);
        self.outcome(self.execute, &facts())
    }

    fn adjudicate_truncate(
        &self,
        _prepared: &dyn TruncatePrepared,
        _evidence: &TruncateEvidence,
    ) -> TruncateOutcome {
        self.adjudicate_calls.fetch_add(1, Ordering::SeqCst);
        self.outcome(self.adjudicate, &facts())
    }
}

fn facts() -> TruncatePlanFacts {
    TruncatePlanFacts {
        catalog: "ice".to_string(),
        namespace: "db".to_string(),
        table: "orders".to_string(),
        target_ref: "main".to_string(),
        provider_id: "iceberg".to_string(),
        instance_id: "ice".to_string(),
        incarnation: [1; 16],
        mutation_operation_id: [0; 16],
        request_digest: [2; 32],
        plan_digest: [3; 32],
        state_digest: [4; 32],
        summary: TruncatePlanSummary {
            file_count: 3,
            row_count: 5,
            total_bytes: 8,
        },
    }
}

fn receipt(facts: &TruncatePlanFacts) -> TruncateReceipt {
    TruncateReceipt {
        provider_id: facts.provider_id.clone(),
        instance_id: facts.instance_id.clone(),
        incarnation: facts.incarnation,
        mutation_operation_id: facts.mutation_operation_id,
        operation_kind: "truncate".to_string(),
        request_digest: facts.request_digest,
        plan_digest: facts.plan_digest,
        state_digest: facts.state_digest,
        summary: facts.summary,
        opaque_payload: vec![9],
        opaque_payload_digest: [10; 32],
    }
}

fn failure(message: &str) -> TruncateFailure {
    TruncateFailure {
        kind: TruncateFailureKind::Unavailable,
        message: message.to_string(),
    }
}

fn context() -> RequestContext {
    let cancellation = QueryCancellationSource::new();
    RequestContext::admit(RequestAdmission::new(
        Some("ice".to_string()),
        "db".to_string(),
        ClusterRole::Fe,
        BackendTopologySnapshot::empty(1),
        Some(Instant::now() + Duration::from_secs(30)),
        cancellation.view(),
        Default::default(),
    ))
}

fn command() -> TruncateCommand {
    TruncateCommand {
        target_parts: vec!["ice".to_string(), "db".to_string(), "orders".to_string()],
        target_ref: "main".to_string(),
    }
}

fn service() -> DmlService {
    DmlService::compose(None, Arc::new(FrontendStatisticsService::new()))
}

#[test]
fn unknown_is_adjudicated_once_and_exact_positive_commits_without_cleanup() {
    let engine = FakeTruncateEngine::new(Mode::CommitUnknown, Mode::Committed);
    service()
        .execute_truncate(&engine, command(), &context(), None)
        .expect("exact positive adjudication commits");
    assert_eq!(engine.plan_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.cleanup_calls.load(Ordering::SeqCst), 0);
    assert_eq!(engine.delete_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn unknown_or_negative_adjudication_never_retries_or_mutates() {
    let engine = FakeTruncateEngine::new(Mode::CommitUnknown, Mode::KnownUncommitted);
    let error = service()
        .execute_truncate(&engine, command(), &context(), None)
        .expect_err("negative adjudication remains unknown");
    let terminal = error.publication_terminal().expect("explicit terminal");
    assert_eq!(
        terminal.disposition(),
        LakePublicationDisposition::CommitUnknown
    );
    assert!(terminal.do_not_retry());
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.cleanup_calls.load(Ordering::SeqCst), 0);
    assert_eq!(engine.delete_calls.load(Ordering::SeqCst), 0);
}

#[test]
fn finalization_failure_preserves_known_committed_terminal() {
    let engine = FakeTruncateEngine::new(Mode::CommittedFinalizationFailed, Mode::Committed);
    let error = service()
        .execute_truncate(&engine, command(), &context(), None)
        .expect_err("finalization failure is visible");
    let terminal = error.publication_terminal().expect("explicit terminal");
    assert_eq!(
        terminal.disposition(),
        LakePublicationDisposition::KnownCommitted
    );
    assert!(terminal.do_not_retry());
    assert_eq!(engine.execute_calls.load(Ordering::SeqCst), 1);
    assert_eq!(engine.adjudicate_calls.load(Ordering::SeqCst), 0);
}
