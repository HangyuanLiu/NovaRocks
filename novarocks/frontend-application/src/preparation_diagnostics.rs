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

//! Authenticated, bounded statement observations for functional diagnostics.
//!
//! Authorization is immutable after first initialization and independent of
//! statement observer state. Observation never controls product execution.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::query_execution::lifecycle_diagnostics::QueryLifecycleConvergenceSnapshot;
use novarocks_query_application::session_control::StatementToken;
use novarocks_types::{QueryExecutionId, QueryId};

pub(crate) const CONTROL_SECRET_ENV: &str = "NOVAROCKS_PREPARATION_DIAGNOSTIC_SECRET";

#[derive(Debug, Deserialize)]
pub(crate) struct ControlRequest {
    pub run_token: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StatementObservationArmRequest {
    pub run_token: String,
    pub connection_id: u32,
    pub sql_sha256: String,
}

#[derive(Debug, Serialize)]
pub(crate) struct StatementObservationResponse {
    pub schema_version: u8,
    pub run_token: String,
    pub work_id: Option<String>,
    pub logical_execution_id: Option<String>,
    pub execution_id: Option<String>,
    pub query_process_namespace: Option<String>,
    pub query_local_sequence: Option<u64>,
    pub attempt_id: Option<u64>,
    pub snapshot: Option<serde_json::Value>,
}

const MAX_STATEMENT_OBSERVERS: usize = 64;
static ACTIVE_STATEMENT_OBSERVERS: AtomicU64 = AtomicU64::new(0);

/// An observation capability only: neither its lifetime nor its contents
/// control admission, execution, recovery, result visibility, or cleanup.
#[derive(Clone, Debug)]
pub(crate) struct StatementObservationHandle {
    run_token: String,
    generation: u64,
}

struct StatementObserver {
    generation: u64,
    connection_id: u32,
    sql_digest: [u8; 32],
    statement: Option<StatementToken>,
    logical_query: Option<QueryId>,
    execution: Option<QueryExecutionId>,
    snapshot: Option<Box<QueryLifecycleConvergenceSnapshot>>,
}

#[derive(Default)]
struct StatementObservers {
    next_generation: u64,
    runs: BTreeMap<String, StatementObserver>,
}

impl StatementObservers {
    fn arm(&mut self, request: StatementObservationArmRequest) -> Result<(), String> {
        if request.connection_id == 0 {
            return Err("statement observation connection id must be nonzero".to_owned());
        }
        if request.run_token.trim().is_empty() || request.run_token.len() > 256 {
            return Err("statement observation token must contain 1..256 bytes".to_owned());
        }
        if self.runs.contains_key(&request.run_token)
            || self
                .runs
                .values()
                .any(|run| run.connection_id == request.connection_id)
        {
            return Err("statement observation token or connection is already armed".to_owned());
        }
        if self.runs.len() >= MAX_STATEMENT_OBSERVERS {
            return Err("statement observation capacity exhausted".to_owned());
        }
        if request.sql_sha256.len() != 64
            || !request
                .sql_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("statement SQL digest must be lowercase SHA-256 hex".to_owned());
        }
        let mut sql_digest = [0u8; 32];
        for (index, byte) in sql_digest.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&request.sql_sha256[index * 2..index * 2 + 2], 16)
                .expect("validated SHA-256 hex");
        }
        let generation = self
            .next_generation
            .checked_add(1)
            .ok_or_else(|| "statement observation generation exhausted".to_owned())?;
        self.next_generation = generation;
        self.runs.insert(
            request.run_token,
            StatementObserver {
                generation,
                connection_id: request.connection_id,
                sql_digest,
                statement: None,
                logical_query: None,
                execution: None,
                snapshot: None,
            },
        );
        Ok(())
    }

    fn bind_statement(
        &mut self,
        statement: StatementToken,
        sql: &str,
    ) -> Option<StatementObservationHandle> {
        if statement.session().session_epoch() == 0 || statement.generation() == 0 {
            return None;
        }
        let digest: [u8; 32] = Sha256::digest(sql.as_bytes()).into();
        let (token, run) = self.runs.iter_mut().find(|(_, run)| {
            run.connection_id == statement.session().connection_id() && run.sql_digest == digest
        })?;
        if run.statement.is_some_and(|bound| bound != statement) {
            return None;
        }
        run.statement = Some(statement);
        Some(StatementObservationHandle {
            run_token: token.clone(),
            generation: run.generation,
        })
    }

    fn bound_statement(&self, statement: StatementToken) -> Option<StatementObservationHandle> {
        self.runs
            .iter()
            .find(|(_, run)| run.statement == Some(statement))
            .map(|(token, run)| StatementObservationHandle {
                run_token: token.clone(),
                generation: run.generation,
            })
    }

    fn run_mut(&mut self, handle: &StatementObservationHandle) -> Option<&mut StatementObserver> {
        self.runs
            .get_mut(&handle.run_token)
            .filter(|run| run.generation == handle.generation)
    }

    fn bind_attempt(&mut self, handle: &StatementObservationHandle, execution: QueryExecutionId) {
        let Some(run) = self.run_mut(handle) else {
            return;
        };
        if run.statement.is_none()
            || run
                .logical_query
                .is_some_and(|query| query != execution.query_id())
        {
            return;
        }
        if run
            .execution
            .is_some_and(|bound| bound.attempt_id().get() >= execution.attempt_id().get())
        {
            return;
        }
        run.logical_query = Some(execution.query_id());
        run.execution = Some(execution);
        run.snapshot = None;
    }

    fn publish(&mut self, snapshot: &QueryLifecycleConvergenceSnapshot) {
        for run in self.runs.values_mut() {
            if run.execution == Some(snapshot.execution_id) && run.snapshot.is_none() {
                run.snapshot = Some(Box::new(snapshot.clone()));
            }
        }
    }

    fn response(&self, token: &str) -> Result<StatementObservationResponse, String> {
        let run = self
            .runs
            .get(token)
            .ok_or_else(|| "statement observation token is not armed".to_owned())?;
        let attribution =
            run.execution
                .map(|execution| {
                    execution.query_id().process_attribution().ok_or_else(|| {
                        "observed execution has no exact process attribution".to_owned()
                    })
                })
                .transpose()?;
        let snapshot = run.snapshot.as_deref().map(|snapshot| {
            debug_assert_eq!(run.execution, Some(snapshot.execution_id));
            crate::native::report_server::lifecycle_convergence_debug_json(snapshot.clone())
        });
        Ok(StatementObservationResponse {
            schema_version: 1,
            run_token: token.to_owned(),
            work_id: run.statement.map(|statement| {
                format!(
                    "statement:{}:{}:{}",
                    statement.session().connection_id(),
                    statement.session().session_epoch(),
                    statement.generation()
                )
            }),
            logical_execution_id: run
                .logical_query
                .map(|query| format!("{}:{}", query.high(), query.low())),
            execution_id: run.execution.map(|execution| {
                format!(
                    "{}:{}:{}",
                    execution.query_id().high(),
                    execution.query_id().low(),
                    execution.attempt_id().get()
                )
            }),
            query_process_namespace: attribution
                .map(|attribution| attribution.namespace().to_string()),
            query_local_sequence: attribution.map(|attribution| attribution.sequence().get()),
            attempt_id: run.execution.map(|execution| execution.attempt_id().get()),
            snapshot,
        })
    }

    fn drain(&mut self, token: &str) -> Result<StatementObservationResponse, String> {
        let response = self.response(token)?;
        self.runs.remove(token);
        Ok(response)
    }
}

fn statement_observers() -> &'static Mutex<StatementObservers> {
    static OBSERVERS: OnceLock<Mutex<StatementObservers>> = OnceLock::new();
    OBSERVERS.get_or_init(|| Mutex::new(StatementObservers::default()))
}

pub(crate) fn arm_statement_observation(
    request: StatementObservationArmRequest,
) -> Result<(), String> {
    if !enabled() {
        return Err("preparation diagnostics are disabled".to_owned());
    }
    let mut state = statement_observers()
        .lock()
        .map_err(|_| "statement observation lock poisoned".to_owned())?;
    state.arm(request)?;
    ACTIVE_STATEMENT_OBSERVERS.store(state.runs.len() as u64, Ordering::Release);
    Ok(())
}

pub(crate) fn peek_statement_observation(
    token: &str,
) -> Result<StatementObservationResponse, String> {
    statement_observers()
        .lock()
        .map_err(|_| "statement observation lock poisoned".to_owned())?
        .response(token)
}

pub(crate) fn drain_statement_observation(
    token: &str,
) -> Result<StatementObservationResponse, String> {
    let mut state = statement_observers()
        .lock()
        .map_err(|_| "statement observation lock poisoned".to_owned())?;
    let response = state.drain(token)?;
    ACTIVE_STATEMENT_OBSERVERS.store(state.runs.len() as u64, Ordering::Release);
    Ok(response)
}

thread_local! {
    static STATEMENT_OBSERVATION: RefCell<Option<StatementObservationHandle>> = const { RefCell::new(None) };
}

pub(crate) struct StatementObservationScope(Option<StatementObservationHandle>);
impl Drop for StatementObservationScope {
    fn drop(&mut self) {
        STATEMENT_OBSERVATION.with(|slot| {
            slot.replace(self.0.take());
        });
    }
}

pub(crate) fn enter_statement_for_sql(
    statement: StatementToken,
    sql: &str,
) -> Option<StatementObservationScope> {
    if ACTIVE_STATEMENT_OBSERVERS.load(Ordering::Acquire) == 0 {
        return None;
    }
    let handle = statement_observers()
        .lock()
        .ok()?
        .bind_statement(statement, sql);
    Some(STATEMENT_OBSERVATION.with(|slot| StatementObservationScope(slot.replace(handle))))
}

pub(crate) fn enter_bound_statement(
    statement: StatementToken,
) -> Option<StatementObservationScope> {
    if ACTIVE_STATEMENT_OBSERVERS.load(Ordering::Acquire) == 0 {
        return None;
    }
    let handle = statement_observers()
        .lock()
        .ok()?
        .bound_statement(statement);
    Some(STATEMENT_OBSERVATION.with(|slot| StatementObservationScope(slot.replace(handle))))
}

pub(crate) fn capture_statement_observation() -> Option<StatementObservationHandle> {
    STATEMENT_OBSERVATION.with(|slot| slot.borrow().clone())
}

pub(crate) fn statement_observations_active() -> bool {
    ACTIVE_STATEMENT_OBSERVERS.load(Ordering::Acquire) != 0
}

pub(crate) fn bind_observed_attempt(
    handle: &StatementObservationHandle,
    execution: QueryExecutionId,
) {
    if let Ok(mut state) = statement_observers().lock() {
        state.bind_attempt(handle, execution);
    }
}

pub(crate) fn publish_observed_convergence(snapshot: &QueryLifecycleConvergenceSnapshot) {
    if ACTIVE_STATEMENT_OBSERVERS.load(Ordering::Acquire) == 0 {
        return;
    }
    if let Ok(mut state) = statement_observers().lock() {
        state.publish(snapshot);
    }
}

/// The credential owner has no collector, observer generation or mutation.
struct DiagnosticAuthorization {
    secret_digest: Option<[u8; 32]>,
}

impl DiagnosticAuthorization {
    fn from_secret(secret: Option<&str>) -> Self {
        Self {
            secret_digest: secret
                .filter(|value| value.len() >= 32)
                .map(|value| Sha256::digest(value.as_bytes()).into()),
        }
    }

    fn enabled(&self) -> bool {
        self.secret_digest.is_some()
    }

    fn authorized(&self, header: Option<&str>) -> bool {
        let Some(secret_digest) = self.secret_digest else {
            return false;
        };
        header
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(|candidate| <[u8; 32]>::from(Sha256::digest(candidate.as_bytes())))
            .is_some_and(|candidate_digest| candidate_digest == secret_digest)
    }
}

fn authorization() -> &'static DiagnosticAuthorization {
    static AUTHORIZATION: OnceLock<DiagnosticAuthorization> = OnceLock::new();
    AUTHORIZATION.get_or_init(|| {
        let secret = std::env::var(CONTROL_SECRET_ENV).ok();
        DiagnosticAuthorization::from_secret(secret.as_deref())
    })
}

pub(crate) fn enabled() -> bool {
    authorization().enabled()
}

pub(crate) fn authorize(header: Option<&str>) -> bool {
    authorization().authorized(header)
}

#[cfg(test)]
mod authorization_tests {
    use super::*;

    #[test]
    fn unconfigured_and_short_secrets_disable_statement_authorization() {
        for secret in [
            None,
            Some(""),
            Some("short"),
            Some("1234567890123456789012345678901"),
        ] {
            let auth = DiagnosticAuthorization::from_secret(secret);
            assert!(!auth.enabled());
            assert!(!auth.authorized(None));
            assert!(!auth.authorized(Some("Bearer anything")));
        }
        // Preserve the existing byte-length policy, not a Unicode character count.
        assert!(DiagnosticAuthorization::from_secret(Some(&"é".repeat(16))).enabled());
    }

    #[test]
    fn exact_bearer_digest_authorizes_the_unchanged_statement_observer() {
        let secret = "s".repeat(32);
        let auth = DiagnosticAuthorization::from_secret(Some(&secret));
        assert!(auth.enabled());
        assert_eq!(
            auth.secret_digest,
            Some(Sha256::digest(secret.as_bytes()).into())
        );
        for header in [
            None,
            Some("Bearer wrong"),
            Some(secret.as_str()),
            Some("bearer ssssssssssssssssssssssssssssssss"),
            Some("Bearer\tssssssssssssssssssssssssssssssss"),
            Some("Bearer ssssssssssssssssssssssssssssssss "),
        ] {
            assert!(!auth.authorized(header));
        }
        assert!(auth.authorized(Some(&format!("Bearer {secret}"))));
        let mut observers = StatementObservers::default();
        observers
            .arm(StatementObservationArmRequest {
                run_token: "authorized-statement".to_owned(),
                connection_id: 91,
                sql_sha256: format!("{:x}", Sha256::digest(b"SELECT 1")),
            })
            .expect("the authorized statement owner remains available");
        assert_eq!(
            observers
                .response("authorized-statement")
                .unwrap()
                .run_token,
            "authorized-statement"
        );
        observers.drain("authorized-statement").unwrap();
        assert!(observers.response("authorized-statement").is_err());
    }
}

#[cfg(test)]
mod statement_observation_tests {
    use super::*;
    use crate::query_execution::lifecycle_diagnostics::{
        RuntimeFilterTerminalRollupSnapshot, RuntimeFilterTerminalRollupUnavailable,
    };
    use novarocks_query_application::session_control::SessionToken;
    use novarocks_types::AttemptId;

    fn arm_request(token: &str, connection_id: u32, sql: &str) -> StatementObservationArmRequest {
        StatementObservationArmRequest {
            run_token: token.to_owned(),
            connection_id,
            sql_sha256: format!("{:x}", Sha256::digest(sql.as_bytes())),
        }
    }

    fn statement(connection: u32, epoch: u64, generation: u64) -> StatementToken {
        StatementToken::new(SessionToken::new(connection, epoch), generation)
    }

    fn execution(high: i64, low: i64, attempt: u64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(high, low), AttemptId::new(attempt).unwrap()).unwrap()
    }

    fn snapshot(execution_id: QueryExecutionId) -> QueryLifecycleConvergenceSnapshot {
        QueryLifecycleConvergenceSnapshot {
            execution_id,
            error_source: None,
            primary_error: None,
            runtime_filter: RuntimeFilterTerminalRollupSnapshot::Unavailable(
                RuntimeFilterTerminalRollupUnavailable::TerminalOutcomesIncomplete,
            ),
        }
    }

    #[test]
    fn statement_observation_matches_exact_connection_sql_and_statement_owner() {
        let mut state = StatementObservers::default();
        state
            .arm(arm_request("selected", 7, "SELECT k FROM t"))
            .unwrap();
        assert!(
            state
                .bind_statement(statement(8, 1, 1), "SELECT k FROM t")
                .is_none()
        );
        assert!(
            state
                .bind_statement(statement(7, 1, 1), "SELECT k FROM other")
                .is_none()
        );
        assert!(state.response("selected").unwrap().work_id.is_none());
        assert!(
            state
                .bind_statement(statement(7, 0, 12), "SELECT k FROM t")
                .is_none()
        );
        assert!(
            state
                .bind_statement(statement(7, 9, 0), "SELECT k FROM t")
                .is_none()
        );
        let token = statement(7, 9, 12);
        let handle = state.bind_statement(token, "SELECT k FROM t").unwrap();
        assert!(
            state
                .bind_statement(statement(7, 9, 13), "SELECT k FROM t")
                .is_none()
        );
        assert!(
            state
                .bind_statement(statement(7, 10, 12), "SELECT k FROM t")
                .is_none()
        );
        assert!(state.bound_statement(statement(7, 10, 12)).is_none());
        let exact = execution(-19, 41, 1);
        state.bind_attempt(&handle, exact);
        state.bind_attempt(&handle, execution(-19, 99, 2));
        let response = state.response("selected").unwrap();
        assert_eq!(response.work_id.as_deref(), Some("statement:7:9:12"));
        assert_eq!(response.logical_execution_id.as_deref(), Some("-19:41"));
        assert_eq!(response.execution_id.as_deref(), Some("-19:41:1"));
        assert_eq!(
            response.query_process_namespace.as_deref(),
            Some("0xffffffffffffffed")
        );
        assert_eq!(response.query_local_sequence, Some(41));
        assert_eq!(response.attempt_id, Some(1));
    }

    #[test]
    fn statement_observation_ignores_late_previous_query_and_old_attempt_snapshots() {
        let mut state = StatementObservers::default();
        state
            .arm(arm_request("selected", 7, "SELECT k FROM t"))
            .unwrap();
        let previous = execution(-19, 40, 1);
        state.publish(&snapshot(previous));
        let handle = state
            .bind_statement(statement(7, 9, 12), "SELECT k FROM t")
            .unwrap();
        let first = execution(-19, 41, 1);
        state.bind_attempt(&handle, first);
        state.publish(&snapshot(previous));
        assert!(state.response("selected").unwrap().snapshot.is_none());
        state.publish(&snapshot(first));
        state.bind_attempt(&handle, first);
        assert!(state.response("selected").unwrap().snapshot.is_some());
        let replacement = execution(-19, 41, 2);
        state.bind_attempt(&handle, replacement);
        assert!(state.response("selected").unwrap().snapshot.is_none());
        state.bind_attempt(&handle, first);
        state.publish(&snapshot(first));
        state.publish(&snapshot(execution(19, 41, 2)));
        assert!(state.response("selected").unwrap().snapshot.is_none());
        state.publish(&snapshot(replacement));
        let response = state.response("selected").unwrap();
        assert_eq!(response.execution_id.as_deref(), Some("-19:41:2"));
        let wire = response.snapshot.unwrap();
        assert_eq!(wire["execution_id"], "-19:41:2");
        assert_eq!(
            wire["query_process_namespace"],
            response.query_process_namespace.unwrap()
        );
        assert_eq!(
            wire["query_local_sequence"],
            response.query_local_sequence.unwrap()
        );
        assert_eq!(wire["query_attempt_id"], response.attempt_id.unwrap());
    }

    #[test]
    fn statement_observation_parallel_tokens_are_bounded_and_drain_invalidates_handles() {
        let mut state = StatementObservers::default();
        assert!(state.arm(arm_request("zero", 0, "SELECT 1")).is_err());
        for connection in 1..=MAX_STATEMENT_OBSERVERS as u32 {
            state
                .arm(arm_request(
                    &format!("run-{connection}"),
                    connection,
                    "SELECT 1",
                ))
                .unwrap();
        }
        assert!(state.arm(arm_request("run-1", 100, "SELECT 1")).is_err());
        assert!(state.arm(arm_request("another", 1, "SELECT 1")).is_err());
        assert!(state.arm(arm_request("overflow", 100, "SELECT 1")).is_err());
        let a = state
            .bind_statement(statement(1, 1, 1), "SELECT 1")
            .unwrap();
        let b = state
            .bind_statement(statement(2, 1, 1), "SELECT 1")
            .unwrap();
        state.bind_attempt(&a, execution(5, 41, 1));
        state.bind_attempt(&b, execution(5, 42, 1));
        state.publish(&snapshot(execution(5, 42, 1)));
        assert!(state.response("run-1").unwrap().snapshot.is_none());
        assert!(state.response("run-2").unwrap().snapshot.is_some());
        state.drain("run-1").unwrap();
        assert!(state.response("run-1").is_err());
        state.arm(arm_request("run-1", 1, "SELECT 1")).unwrap();
        state.bind_attempt(&a, execution(5, 43, 2));
        state.publish(&snapshot(execution(5, 41, 1)));
        assert!(state.response("run-1").unwrap().work_id.is_none());
        assert!(state.response("run-1").unwrap().execution_id.is_none());
        assert!(state.response("run-1").unwrap().snapshot.is_none());
        assert_eq!(state.runs.len(), MAX_STATEMENT_OBSERVERS);
        state.next_generation = u64::MAX;
        state.drain("run-1").unwrap();
        assert!(state.arm(arm_request("exhausted", 1, "SELECT 1")).is_err());
    }

    #[test]
    fn statement_observation_scope_restores_previous_context_without_cross_statement_leak() {
        assert!(capture_statement_observation().is_none());
        let outer = StatementObservationHandle {
            run_token: "outer".to_owned(),
            generation: 1,
        };
        let outer_scope =
            STATEMENT_OBSERVATION.with(|slot| StatementObservationScope(slot.replace(Some(outer))));
        assert_eq!(capture_statement_observation().unwrap().run_token, "outer");
        let unrelated_scope =
            STATEMENT_OBSERVATION.with(|slot| StatementObservationScope(slot.replace(None)));
        assert!(capture_statement_observation().is_none());
        drop(unrelated_scope);
        assert_eq!(capture_statement_observation().unwrap().run_token, "outer");
        drop(outer_scope);
        assert!(capture_statement_observation().is_none());
    }
}
