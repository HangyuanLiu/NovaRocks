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

//! Query-application ownership of final physical-plan completion.
//!
//! SQL publishes immutable typed needs and consumes exact fact batches. This
//! module owns the asynchronous application loop around that pure protocol,
//! statement cancellation, the admitted `WorkScope`, and publication of the
//! validated immutable plan candidate. Connector handles, native DTOs, task
//! placement, encoders, and runtime objects remain outside this boundary.

use std::{fmt, sync::Arc, time::Instant};

use async_trait::async_trait;
use novarocks_physical_plan::{PhysicalPlan, validate_plan};
use novarocks_sql::compiler::{
    SqlCompileProgress, SqlCompiler, SqlDisplayAnnotation, SqlDisplayIntent, SqlFactBatch,
    SqlFinalPlanCompileRequest, SqlNeedBatch,
};
use novarocks_workload_control::{CancellationView, Stage, StageRequest, WorkScope};

/// Immutable, fully validated final plan held by query application before any
/// runtime projection is built. There is deliberately no mutable-plan or
/// unchecked-plan constructor.
#[derive(Clone, Debug)]
pub struct CompletedPhysicalPlanCandidate {
    plan: Arc<PhysicalPlan>,
    display_intent: SqlDisplayIntent,
    display_annotations: Arc<[SqlDisplayAnnotation]>,
}

impl CompletedPhysicalPlanCandidate {
    fn from_completed(
        completed: novarocks_sql::compiler::SqlCompletedPlan,
    ) -> Result<Self, FinalPlanCompletionError> {
        let (plan, display_intent, display_annotations) = completed.into_parts();
        Self::try_new(plan, display_intent, display_annotations)
    }

    fn try_new(
        plan: PhysicalPlan,
        display_intent: SqlDisplayIntent,
        display_annotations: Box<[SqlDisplayAnnotation]>,
    ) -> Result<Self, FinalPlanCompletionError> {
        validate_plan(&plan).map_err(|error| FinalPlanCompletionError::InvalidPlan {
            message: Arc::from(error.to_string()),
        })?;
        Ok(Self {
            plan: Arc::new(plan),
            display_intent,
            display_annotations: display_annotations.into(),
        })
    }

    pub const fn plan(&self) -> &Arc<PhysicalPlan> {
        &self.plan
    }

    /// SQL's completed display semantics are part of the same immutable
    /// artifact as the plan. A renderer may inspect them but cannot ask SQL
    /// to analyze or optimize the statement again.
    pub const fn display_intent(&self) -> SqlDisplayIntent {
        self.display_intent
    }

    pub fn display_annotations(&self) -> &[SqlDisplayAnnotation] {
        &self.display_annotations
    }
}

/// Role composition supplies the application-owned adapter that resolves the
/// current exact SQL need batch. A fact source cannot retain compiler state,
/// mint a plan, or substitute a different need batch.
#[async_trait]
pub trait SqlCompletionFactSource: Send + Sync {
    async fn resolve(&self, needs: &SqlNeedBatch) -> Result<SqlFactBatch, String>;
}

/// Drives one query's pure SQL completion protocol under its admitted scope.
///
/// The driver carries no resource authority or runtime capability. It holds
/// the stage permit for the complete preparation lifetime and interrupts the
/// pending role adapter on statement cancellation or the frozen deadline.
pub struct FinalPlanCompletionDriver {
    facts: Arc<dyn SqlCompletionFactSource>,
}

impl FinalPlanCompletionDriver {
    pub fn new(facts: Arc<dyn SqlCompletionFactSource>) -> Self {
        Self { facts }
    }

    pub async fn complete(
        &self,
        request: SqlFinalPlanCompileRequest,
        scope: &WorkScope,
    ) -> Result<CompletedPhysicalPlanCandidate, FinalPlanCompletionError> {
        let control = request.control().clone();
        let permit = scope
            .acquire(StageRequest {
                stage: Stage::Preparation,
                retained_bytes: 0,
            })
            .map_err(governance_error)?
            .await
            .map_err(governance_error)?;
        permit
            .check(scope, Stage::Preparation)
            .map_err(governance_error)?;
        scope.check().map_err(governance_error)?;
        let cancellation = scope.cancellation().map_err(governance_error)?;
        let mut progress =
            SqlCompiler::start(request.try_into_completion().map_err(compiler_error)?)
                .map_err(compiler_progress_error)?;

        loop {
            scope.check().map_err(governance_error)?;
            progress = match progress {
                SqlCompileProgress::Complete(completed) => {
                    return CompletedPhysicalPlanCandidate::from_completed(completed);
                }
                SqlCompileProgress::Incomplete(compilation) => {
                    let facts = self
                        .resolve(&cancellation, control.deadline(), compilation.needs())
                        .await?;
                    SqlCompiler::finish(compilation, facts, &control)
                        .map_err(compiler_progress_error)?
                }
            };
        }
    }

    async fn resolve(
        &self,
        cancellation: &CancellationView,
        deadline: Option<Instant>,
        needs: &SqlNeedBatch,
    ) -> Result<SqlFactBatch, FinalPlanCompletionError> {
        let future = self.facts.resolve(needs);
        match deadline {
            Some(deadline) => {
                tokio::select! {
                    reason = cancellation.cancelled() => Err(FinalPlanCompletionError::Cancelled { reason: Arc::from(format!("{reason:?}")) }),
                    _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => Err(FinalPlanCompletionError::DeadlineExceeded),
                    result = future => result.map_err(|message| FinalPlanCompletionError::FactSource { message: Arc::from(message) }),
                }
            }
            None => {
                tokio::select! {
                    reason = cancellation.cancelled() => Err(FinalPlanCompletionError::Cancelled { reason: Arc::from(format!("{reason:?}")) }),
                    result = future => result.map_err(|message| FinalPlanCompletionError::FactSource { message: Arc::from(message) }),
                }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalPlanCompletionError {
    Governance { message: Arc<str> },
    Cancelled { reason: Arc<str> },
    DeadlineExceeded,
    Compiler { message: Arc<str> },
    FactSource { message: Arc<str> },
    InvalidPlan { message: Arc<str> },
}

impl fmt::Display for FinalPlanCompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Governance { message }
            | Self::Compiler { message }
            | Self::FactSource { message }
            | Self::InvalidPlan { message } => formatter.write_str(message),
            Self::Cancelled { reason } => {
                write!(formatter, "final plan completion cancelled: {reason}")
            }
            Self::DeadlineExceeded => {
                formatter.write_str("final plan completion deadline exceeded")
            }
        }
    }
}

impl std::error::Error for FinalPlanCompletionError {}

fn governance_error(error: novarocks_workload_control::WorkError) -> FinalPlanCompletionError {
    FinalPlanCompletionError::Governance {
        message: Arc::from(error.to_string()),
    }
}

fn compiler_error(error: novarocks_sql::compiler::SqlCompileError) -> FinalPlanCompletionError {
    match error {
        novarocks_sql::compiler::SqlCompileError::Cancelled => {
            FinalPlanCompletionError::Cancelled {
                reason: Arc::from("SQL compiler control"),
            }
        }
        novarocks_sql::compiler::SqlCompileError::DeadlineExceeded => {
            FinalPlanCompletionError::DeadlineExceeded
        }
        error => FinalPlanCompletionError::Compiler {
            message: Arc::from(error.to_string()),
        },
    }
}

fn compiler_progress_error(
    error: novarocks_sql::compiler::SqlCompileProgressError,
) -> FinalPlanCompletionError {
    match error {
        novarocks_sql::compiler::SqlCompileProgressError::Compile(error) => compiler_error(error),
        error => FinalPlanCompletionError::Compiler {
            message: Arc::from(error.to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks_physical_plan::{
        MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId, ScanReadBudget,
    };
    use novarocks_sql::compiler::{
        DEFAULT_COMPLETION_LIMITS, ExplainLevel, SessionOptimizerSettings, SqlCompileControl,
        SqlCompileIntent, SqlFinalPlanCompileRequest, SqlPlanningEnvironment, SqlSessionContext,
        SqlStatementInput, builtin_sql_function_catalog, noop_constant_evaluator,
    };
    use novarocks_workload_control::{
        ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;

    struct NoFactSource {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl SqlCompletionFactSource for NoFactSource {
        async fn resolve(&self, _needs: &SqlNeedBatch) -> Result<SqlFactBatch, String> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Err("VALUES completion must not request facts".to_string())
        }
    }

    fn scope() -> (novarocks_workload_control::RootWork, WorkScope) {
        let control = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload control");
        control.mark_ready().expect("workload control ready");
        let root = control
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .expect("query root");
        let scope = root.owner.scope();
        (root, scope)
    }

    fn request(
        sql: &str,
        control: SqlCompileControl,
        intent: SqlCompileIntent,
    ) -> SqlFinalPlanCompileRequest {
        SqlFinalPlanCompileRequest::new(
            PlanVersionId::try_new([9; 16]).expect("plan version"),
            SqlStatementInput::sql(sql),
            intent,
            SqlSessionContext {
                current_catalog: Some("iceberg".to_string()),
                current_database: "db".to_string(),
                optimizer_settings: SessionOptimizerSettings::default(),
            },
            SqlPlanningEnvironment::Distributed,
            builtin_sql_function_catalog().snapshot(),
            noop_constant_evaluator(),
            control,
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            ScanReadBudget {
                max_batch_rows: MAX_SCAN_BATCH_ROWS,
                max_batch_bytes: MAX_SCAN_BATCH_BYTES,
            },
            DEFAULT_COMPLETION_LIMITS,
        )
    }

    fn values_request() -> SqlFinalPlanCompileRequest {
        request(
            "SELECT 1",
            SqlCompileControl::unbounded(),
            SqlCompileIntent::Query,
        )
    }

    #[tokio::test]
    async fn values_completion_publishes_a_valid_candidate_without_fact_io() {
        let source = Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        });
        let driver = FinalPlanCompletionDriver::new(source.clone());
        let (_root, scope) = scope();

        let candidate = driver
            .complete(values_request(), &scope)
            .await
            .expect("VALUES final plan completion");

        assert_eq!(
            candidate.plan().version(),
            PlanVersionId::try_new([9; 16]).unwrap()
        );
        assert_eq!(candidate.display_intent(), SqlDisplayIntent::Execute);
        assert!(candidate.display_annotations().is_empty());
        assert_eq!(source.calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn candidate_retains_completed_explain_semantics() {
        let source = Arc::new(NoFactSource {
            calls: AtomicUsize::new(0),
        });
        let driver = FinalPlanCompletionDriver::new(source.clone());
        let (_root, scope) = scope();

        let candidate = driver
            .complete(
                request(
                    "SELECT 1",
                    SqlCompileControl::unbounded(),
                    SqlCompileIntent::Explain {
                        level: ExplainLevel::Verbose,
                        analyze: false,
                    },
                ),
                &scope,
            )
            .await
            .expect("VALUES explain final plan completion");

        assert_eq!(
            candidate.display_intent(),
            SqlDisplayIntent::Explain {
                level: ExplainLevel::Verbose,
                analyze: false,
            }
        );
        assert_eq!(source.calls.load(Ordering::Relaxed), 0);
    }

    struct PendingFactSource;

    #[async_trait]
    impl SqlCompletionFactSource for PendingFactSource {
        async fn resolve(&self, _needs: &SqlNeedBatch) -> Result<SqlFactBatch, String> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn deadline_interrupts_a_pending_fact_round() {
        let driver = FinalPlanCompletionDriver::new(Arc::new(PendingFactSource));
        let (_root, scope) = scope();
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(10);

        let error = driver
            .complete(
                request(
                    "SELECT * FROM iceberg.db.orders",
                    SqlCompileControl::new(deadline.into(), Arc::new(NeverCancelled)),
                    SqlCompileIntent::Query,
                ),
                &scope,
            )
            .await
            .expect_err("deadline must interrupt a pending fact round");

        assert_eq!(error, FinalPlanCompletionError::DeadlineExceeded);
    }

    struct NeverCancelled;

    impl novarocks_sql::compiler::SqlCancellationObservation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }
}
