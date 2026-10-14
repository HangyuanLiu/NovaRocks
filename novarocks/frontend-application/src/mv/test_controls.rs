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

//! MV-owned finite test actions, isolated from ordinary product builds.

use novarocks_failpoint::scope::{ActionClaim, GrantHandle, ProcessBinding};
use novarocks_failpoint::target_binding::{StatementInputNomination, StatementWorkNomination};
use novarocks_mv_application::state_family::MV_ACCELERATOR_STATE_FAMILY;
use novarocks_parser::ast::{Query, Statement};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const DOMAIN: &str = "mv";

#[derive(Debug, Eq, PartialEq)]
struct Target {
    catalog: String,
    namespace: String,
    mv: String,
}

#[derive(Debug, Eq, PartialEq)]
enum Action {
    Wipe,
    Observe {
        target: Target,
        level: String,
    },
    Recovery {
        target: Target,
        phase: String,
        directory: PathBuf,
        token: String,
    },
    KnownCommitted {
        target: Target,
        directory: PathBuf,
    },
    Scheduler {
        target: Target,
        directory: PathBuf,
    },
    TransientPreparation {
        target: Target,
        directory: PathBuf,
    },
    Rewrite {
        catalog: String,
        namespace: String,
        query: Query,
        statement_run_token: String,
        directory: PathBuf,
    },
}

struct AuthorizedAction {
    grant: GrantHandle,
    action: Action,
    frozen_input: Mutex<Option<StatementInputNomination>>,
    frozen_nomination: Mutex<Option<StatementWorkNomination>>,
}

pub struct MvRewriteStatementTarget {
    actual_statement: novarocks_query_application::session_control::StatementToken,
    nominated_statement: novarocks_query_application::session_control::StatementToken,
    run_token: String,
}

impl MvRewriteStatementTarget {
    pub(crate) fn new(
        actual_statement: novarocks_query_application::session_control::StatementToken,
        nominated_statement: novarocks_query_application::session_control::StatementToken,
        run_token: String,
    ) -> Result<Self, String> {
        if actual_statement != nominated_statement || run_token.is_empty() {
            return Err(
                "MV rewrite nomination does not match the actual statement generation".into(),
            );
        }
        Ok(Self {
            actual_statement,
            nominated_statement,
            run_token,
        })
    }
}

pub struct MvTestControls {
    process: ProcessBinding,
    actions: Vec<AuthorizedAction>,
}

impl MvTestControls {
    pub fn from_grants(grants: Vec<GrantHandle>) -> Result<Self, String> {
        let process = grants
            .first()
            .ok_or("MV controls require at least one grant")?
            .cancellation()
            .process();
        if !matches!(process, ProcessBinding::Frontend(_)) {
            return Err("MV controls require the exact FE launch".into());
        }
        let session_id = grants[0].cancellation().session_id().to_string();
        let launch_id = grants[0].cancellation().launch_id().to_string();
        let mut actions: Vec<AuthorizedAction> = Vec::new();
        for grant in grants {
            if grant.domain() != DOMAIN
                || grant.cancellation().process() != process
                || grant.cancellation().session_id() != session_id
                || grant.cancellation().launch_id() != launch_id
            {
                return Err("MV controls cannot combine FE launches".into());
            }
            let action = parse_action(grant.parameters())?;
            if actions.iter().any(|old| {
                same_selection(&old.action, &action) && !matches!(action, Action::Observe { .. })
            }) {
                return Err("duplicate or conflicting MV control action".into());
            }
            if matches!(action, Action::Wipe)
                && actions.iter().any(|old| matches!(old.action, Action::Wipe))
            {
                return Err("MV family wipe permits only one claim per FE launch".into());
            }
            actions.push(AuthorizedAction {
                grant,
                action,
                frozen_input: Mutex::new(None),
                frozen_nomination: Mutex::new(None),
            });
        }
        Ok(Self { process, actions })
    }

    /// Pin only predeclared Rewrite inputs. Journal reuse cannot replace the
    /// original client or SQL; selection neither claims nor emits an action.
    pub fn pin_statement_input(&self, input: &StatementInputNomination) -> Result<(), String> {
        input.validate().map_err(|error| error.to_string())?;
        let mut pins = Vec::new();
        for action in &self.actions {
            let Action::Rewrite {
                statement_run_token,
                ..
            } = &action.action
            else {
                continue;
            };
            if statement_run_token != &input.statement_run_token {
                continue;
            }
            self.validate_rewrite_input(action, input)?;
            let frozen = action
                .frozen_input
                .lock()
                .map_err(|_| "MV input pin lock failed")?;
            if frozen.as_ref().is_some_and(|original| original != input) {
                return Err("MV rewrite statement input replacement refused".into());
            }
            pins.push(frozen);
        }
        // All matching, declaration-bounded slots are checked before any pin.
        for mut frozen in pins {
            if frozen.is_none() {
                *frozen = Some(input.clone());
            }
        }
        Ok(())
    }

    fn validate_rewrite_input(
        &self,
        action: &AuthorizedAction,
        input: &StatementInputNomination,
    ) -> Result<(), String> {
        let cancellation = action.grant.cancellation();
        if !cancellation.is_active()
            || input.target_session_id != cancellation.session_id()
            || input.target_launch_id != cancellation.launch_id()
            || ProcessBinding::Frontend(
                input
                    .frontend_process()
                    .map_err(|error| error.to_string())?,
            ) != self.process
        {
            return Err("MV statement input belongs to a foreign or closed FE launch".into());
        }
        Ok(())
    }

    /// Extend the immutable input with the real statement generation. No
    /// missing pin may be implicitly supplied by a later nominal call.
    pub fn nominate_statement(
        &self,
        input: &StatementInputNomination,
        nomination: &StatementWorkNomination,
    ) -> Result<(), String> {
        input
            .validate_statement(nomination)
            .map_err(|error| error.to_string())?;
        let mut selected = Vec::new();
        for action in &self.actions {
            let Action::Rewrite {
                statement_run_token,
                ..
            } = &action.action
            else {
                continue;
            };
            if statement_run_token != &input.statement_run_token {
                continue;
            }
            self.validate_rewrite_input(action, input)?;
            let pinned = action
                .frozen_input
                .lock()
                .map_err(|_| "MV input pin lock failed")?;
            match pinned.as_ref() {
                None => return Err("MV rewrite statement input was not pinned".into()),
                Some(original) if original != input => {
                    return Err("MV rewrite statement input replacement refused".into());
                }
                Some(_) => {}
            }
            let frozen = action
                .frozen_nomination
                .lock()
                .map_err(|_| "MV nomination lock failed")?;
            if frozen
                .as_ref()
                .is_some_and(|original| original != nomination)
            {
                return Err("MV rewrite statement generation replacement refused".into());
            }
            selected.push((pinned, frozen));
        }
        for (_pinned, mut frozen) in selected {
            if frozen.is_none() {
                *frozen = Some(nomination.clone());
            }
        }
        Ok(())
    }

    pub(crate) fn claim_call(
        &self,
        catalog: &str,
        namespace: &str,
        mv: &str,
        level: &str,
    ) -> Result<ActionClaim, String> {
        let selected = self.actions.iter().filter(|action| match &action.action {
            Action::Wipe => level == "wipe",
            Action::Observe {
                target,
                level: expected,
            } => level == expected && target.matches(catalog, namespace, mv),
            _ => false,
        });
        for action in selected {
            if let Ok(claim) = action.grant.claim_once(self.process) {
                return Ok(claim);
            }
        }
        Err(
            "MV test action has no active unclaimed authorization for this FE launch and target"
                .into(),
        )
    }

    pub(crate) fn recovery(
        &self,
        catalog: &str,
        namespace: &str,
        mv: &str,
        phase: &str,
    ) -> Result<(), String> {
        for action in &self.actions {
            let Action::Recovery {
                target,
                phase: expected,
                directory,
                token,
            } = &action.action
            else {
                continue;
            };
            if expected != phase || !target.matches(catalog, namespace, mv) {
                continue;
            }
            let trigger = directory.join(format!("mv-refresh-at-{phase}.trigger"));
            if !trigger.exists() {
                continue;
            }
            let contents = std::fs::read_to_string(&trigger)
                .map_err(|error| format!("read authorized MV recovery trigger: {error}"))?;
            if contents != format!("token={token}\n") {
                return Err("MV recovery trigger does not match its finite grant".into());
            }
            let mut claim = match action.grant.claim_once(self.process) {
                Ok(claim) => claim,
                Err(_) => continue,
            };
            claim.record_hit().map_err(|error| error.to_string())?;
            eprintln!("NOVAROCKS_MV_RECOVERY_PHASE phase={phase} token={token}");
            wait_release(&claim, &trigger, Duration::from_secs(30), || false)?;
        }
        Ok(())
    }

    pub(crate) fn known_committed(
        &self,
        catalog: &str,
        namespace: &str,
        mv: &str,
        publication_id: &novarocks_spi::connector::LakePublicationId,
    ) -> Result<(), String> {
        for action in &self.actions {
            let Action::KnownCommitted { target, directory } = &action.action else {
                continue;
            };
            if !target.matches(catalog, namespace, mv) {
                continue;
            }
            let trigger = novarocks_failpoint::mv_known_committed_before_projector_cas_trigger_path(
                directory,
            );
            if !trigger.exists() {
                continue;
            }
            let mut claim = match action.grant.claim_once(self.process) {
                Ok(claim) => claim,
                Err(_) => continue,
            };
            let marker =
                novarocks_failpoint::mv_known_committed_before_projector_cas_marker_path(directory);
            std::fs::write(
                marker,
                format!(
                    "publication_id={}\nphase=known-committed-before-projector-cas\n",
                    publication_id.as_uuid()
                ),
            )
            .map_err(|error| format!("write authorized MV projector marker: {error}"))?;
            claim.record_hit().map_err(|error| error.to_string())?;
            eprintln!(
                "NOVAROCKS_MV_PROJECTOR_PHASE publication_id={} phase=known-committed-before-projector-cas action=kill_fe",
                publication_id.as_uuid()
            );
            wait_release(&claim, &trigger, Duration::from_secs(30), || false)?;
        }
        Ok(())
    }

    pub(crate) fn scheduler(
        &self,
        target: &novarocks_sql::planning::mv::SqlMvTarget,
        cancellation: &novarocks_query_application::cancellation::QueryCancellationView,
    ) -> Result<bool, String> {
        for action in &self.actions {
            let Action::Scheduler {
                target: expected,
                directory,
            } = &action.action
            else {
                continue;
            };
            if !expected.matches(
                target.catalog.as_deref().unwrap_or(""),
                &target.database,
                &target.name,
            ) {
                continue;
            }
            let mut claim = match action.grant.claim_once(self.process) {
                Ok(claim) => claim,
                Err(_) => continue,
            };
            std::fs::write(
                directory.join(format!("mvx4-scheduler-admitted-{}.marker", target.name)),
                "admitted\n",
            )
            .map_err(|error| format!("write authorized MV scheduler marker: {error}"))?;
            claim.record_hit().map_err(|error| error.to_string())?;
            return wait_release(
                &claim,
                &directory.join("mvx4-scheduler-hold.trigger"),
                Duration::from_secs(120),
                || cancellation.is_cancelled(),
            )
            .map(|()| cancellation.is_cancelled());
        }
        Ok(cancellation.is_cancelled())
    }

    pub(crate) fn transient_preparation(
        &self,
        catalog: &str,
        namespace: &str,
        mv: &str,
    ) -> Result<bool, String> {
        for action in &self.actions {
            let Action::TransientPreparation { target, directory } = &action.action else {
                continue;
            };
            if !target.matches(catalog, namespace, mv) {
                continue;
            }
            let trigger =
                directory.join(format!("mvx4-scheduler-transient-preparation-{mv}.trigger"));
            if !trigger.exists() {
                continue;
            }
            let mut claim = match action.grant.claim_once(self.process) {
                Ok(claim) => claim,
                Err(_) => continue,
            };
            std::fs::rename(&trigger, trigger.with_extension("consumed"))
                .map_err(|error| format!("consume authorized MV transient fault: {error}"))?;
            claim.record_hit().map_err(|error| error.to_string())?;
            return Ok(true);
        }
        Ok(false)
    }

    pub(crate) fn rewrite(
        &self,
        catalog: Option<&str>,
        namespace: &str,
        query: &Query,
        selected: bool,
        statement_target: Option<&MvRewriteStatementTarget>,
        cancellation: &novarocks_query_application::cancellation::QueryCancellationView,
    ) -> Result<(), String> {
        if !selected {
            return Ok(());
        }
        let Some(statement_target) = statement_target else {
            return Ok(());
        };
        if statement_target.actual_statement != statement_target.nominated_statement {
            return Err("MV rewrite nomination is stale".into());
        }
        for action in &self.actions {
            let Action::Rewrite {
                catalog: expected_catalog,
                namespace: expected_namespace,
                query: expected_query,
                statement_run_token,
                directory,
            } = &action.action
            else {
                continue;
            };
            if catalog != Some(expected_catalog.as_str())
                || namespace != expected_namespace
                || query != expected_query
                || &statement_target.run_token != statement_run_token
            {
                continue;
            }
            let frozen = action
                .frozen_nomination
                .lock()
                .map_err(|_| "MV nomination lock failed")?;
            let original = frozen
                .as_ref()
                .ok_or("MV rewrite has no frozen statement nomination")?;
            let actual = statement_target.actual_statement;
            if original.statement_run_token != statement_target.run_token
                || original.connection_id != actual.session().connection_id()
                || original.session_epoch != actual.session().session_epoch()
                || original.statement_generation != actual.generation()
            {
                return Err("MV rewrite statement differs from its frozen original target".into());
            }
            drop(frozen);
            let hold = directory.join("mvx4-rewrite-hold.trigger");
            if !hold.exists() {
                continue;
            }
            let mut claim = match action.grant.claim_once(self.process) {
                Ok(claim) => claim,
                Err(_) => continue,
            };
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(directory.join("mvx4-completed-mv-target-frozen.marker"))
                .map_err(|error| format!("create authorized frozen MV marker: {error}"))?;
            claim.record_hit().map_err(|error| error.to_string())?;
            wait_release(&claim, &hold, Duration::from_secs(120), || {
                cancellation.is_cancelled()
            })?;
        }
        Ok(())
    }
}

fn same_selection(left: &Action, right: &Action) -> bool {
    match (left, right) {
        (Action::Wipe, Action::Wipe) => true,
        (
            Action::Observe {
                target: a,
                level: x,
            },
            Action::Observe {
                target: b,
                level: y,
            },
        ) => a == b && x == y,
        (
            Action::Recovery {
                target: a,
                phase: x,
                ..
            },
            Action::Recovery {
                target: b,
                phase: y,
                ..
            },
        ) => a == b && x == y,
        (Action::KnownCommitted { target: a, .. }, Action::KnownCommitted { target: b, .. })
        | (Action::Scheduler { target: a, .. }, Action::Scheduler { target: b, .. })
        | (
            Action::TransientPreparation { target: a, .. },
            Action::TransientPreparation { target: b, .. },
        ) => a == b,
        (
            Action::Rewrite {
                statement_run_token: a,
                ..
            },
            Action::Rewrite {
                statement_run_token: b,
                ..
            },
        ) => a == b,
        _ => false,
    }
}

impl Target {
    fn matches(&self, catalog: &str, namespace: &str, mv: &str) -> bool {
        self.catalog == catalog && self.namespace == namespace && self.mv == mv
    }
}

fn parse_action(value: &serde_json::Value) -> Result<Action, String> {
    let object = value
        .as_object()
        .ok_or("MV grant parameters must be an object")?;
    let text = |key: &str| {
        object
            .get(key)
            .and_then(serde_json::Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("MV grant requires {key}"))
    };
    let action = text("action")?;
    let allowed: &[&str] = match action {
        "accelerator-wipe" => &["action", "family"],
        "document-observation" => &["action", "catalog", "namespace", "mv", "level"],
        "recovery-hold" => &[
            "action",
            "catalog",
            "namespace",
            "mv",
            "phase",
            "directory",
            "token",
        ],
        "known-committed-hold" | "scheduler-hold" | "transient-preparation" => {
            &["action", "catalog", "namespace", "mv", "directory"]
        }
        "rewrite-hold" => &[
            "action",
            "catalog",
            "namespace",
            "sql",
            "statement_run_token",
            "directory",
        ],
        _ => return Err("unknown MV control action".into()),
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err("unknown MV control parameter".into());
    }
    if action == "accelerator-wipe" {
        if text("family")? != MV_ACCELERATOR_STATE_FAMILY.prefix() {
            return Err("MV wipe names another Accelerator family".into());
        }
        return Ok(Action::Wipe);
    }
    let directory = || -> Result<PathBuf, String> {
        let path = PathBuf::from(text("directory")?);
        if !path.is_absolute() {
            return Err("MV control directory must be absolute".into());
        }
        Ok(path)
    };
    if action == "rewrite-hold" {
        let statements = novarocks_parser::parse(text("sql")?)
            .map_err(|error| format!("parse exact MV rewrite target: {error}"))?;
        let [Statement::Query(query)] = statements.as_slice() else {
            return Err("MV rewrite target must be one query".into());
        };
        return Ok(Action::Rewrite {
            catalog: text("catalog")?.into(),
            namespace: text("namespace")?.into(),
            query: query.clone(),
            statement_run_token: text("statement_run_token")?.into(),
            directory: directory()?,
        });
    }
    let target = Target {
        catalog: text("catalog")?.into(),
        namespace: text("namespace")?.into(),
        mv: text("mv")?.into(),
    };
    Ok(match action {
        "document-observation" => {
            let level = text("level")?;
            if !matches!(level, "baseline" | "package" | "provenance") {
                return Err("MV observation grant has invalid level".into());
            }
            Action::Observe {
                target,
                level: level.into(),
            }
        }
        "recovery-hold" => {
            let phase = text("phase")?;
            if !matches!(
                phase,
                "data-prepared" | "write-committed" | "publication-committed"
            ) {
                return Err("MV recovery grant has invalid phase".into());
            }
            let token = text("token")?;
            if token.contains(['\n', '\r']) {
                return Err("MV recovery token contains a line break".into());
            }
            Action::Recovery {
                target,
                phase: phase.into(),
                directory: directory()?,
                token: token.into(),
            }
        }
        "known-committed-hold" => Action::KnownCommitted {
            target,
            directory: directory()?,
        },
        "scheduler-hold" => Action::Scheduler {
            target,
            directory: directory()?,
        },
        "transient-preparation" => Action::TransientPreparation {
            target,
            directory: directory()?,
        },
        _ => unreachable!(),
    })
}

fn wait_release(
    claim: &ActionClaim,
    trigger: &Path,
    phase_budget: Duration,
    cancelled: impl Fn() -> bool,
) -> Result<(), String> {
    let deadline = Instant::now()
        .checked_add(phase_budget)
        .ok_or("MV barrier deadline overflow")?
        .min(claim.cancellation().deadline());
    while trigger.exists() {
        if !claim.cancellation().is_active() {
            return Err("MV test session closed during authorized barrier".into());
        }
        if cancelled() {
            return Err("MV product request cancelled during authorized barrier".into());
        }
        if Instant::now() >= deadline {
            return Err("timed out waiting for authorized MV barrier release".into());
        }
        std::thread::park_timeout(Duration::from_millis(10));
    }
    Ok(())
}

#[cfg(all(test, unix))]
pub(crate) mod tests {
    use super::*;
    use novarocks_failpoint::liveness::{PreparedRoleSession, RoleSessionOwner, RunnerSession};
    use novarocks_failpoint::session::{DomainGrant, SessionDeclaration, SessionRole};
    use novarocks_query_application::session_control::{SessionToken, StatementToken};
    use serde_json::{Value, json};

    pub(crate) struct TestSession {
        root: PathBuf,
        role: RoleSessionOwner,
        runner: RunnerSession,
    }
    impl TestSession {
        pub(crate) fn new(parameters: Vec<Value>, role: SessionRole, lifetime: u64) -> Self {
            Self::new_in_domain(parameters, role, lifetime, DOMAIN)
        }
        fn new_in_domain(
            parameters: Vec<Value>,
            role: SessionRole,
            lifetime: u64,
            domain: &str,
        ) -> Self {
            use std::os::unix::fs::DirBuilderExt;
            // macOS TMPDIR may exceed the bounded Unix socket path length.
            let root = PathBuf::from("/tmp").join(format!("nrg2mv-{}", uuid::Uuid::now_v7()));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&root)
                .unwrap();
            let declaration = SessionDeclaration {
                schema_version: 1,
                session_id: "mv-test".into(),
                launch_id: "mv-launch".into(),
                role,
                lifetime_millis: lifetime,
                liveness_socket: root.join("live.sock"),
                handshake_secret: "m".repeat(32),
                domain_grants: parameters
                    .into_iter()
                    .enumerate()
                    .map(|(i, parameters)| DomainGrant {
                        domain: domain.into(),
                        grant_id: format!("mv-{i}"),
                        parameters,
                    })
                    .collect(),
            };
            let path = root.join("declaration.json");
            declaration.write_private(&path).unwrap();
            let prepared = PreparedRoleSession::read_and_claim(&path, role).unwrap();
            let runner = RunnerSession::start(declaration).unwrap();
            let process = match role {
                SessionRole::Fe => {
                    ProcessBinding::Frontend(novarocks_types::FrontendProcessId::new_v7())
                }
                SessionRole::Be => {
                    ProcessBinding::Backend(novarocks_types::BackendProcessId::new_v7())
                }
            };
            let role = prepared.bind(process).unwrap();
            Self { root, role, runner }
        }
        pub(crate) fn controls(&self) -> Result<MvTestControls, String> {
            MvTestControls::from_grants(self.role.scope().grants_for(DOMAIN))
        }
        pub(crate) fn close(&mut self) {
            self.role.close_and_join().unwrap();
            self.runner.end_and_join().unwrap();
        }
    }
    impl Drop for TestSession {
        fn drop(&mut self) {
            self.close();
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    pub(crate) fn wipe_grant() -> Value {
        json!({"action":"accelerator-wipe","family":MV_ACCELERATOR_STATE_FAMILY.prefix()})
    }
    fn observation() -> Value {
        json!({"action":"document-observation","catalog":"ice","namespace":"sales","mv":"orders","level":"package"})
    }

    #[test]
    fn mv_observation_has_no_family_wipe_authority_and_is_finite() {
        let session = TestSession::new(vec![observation(), observation()], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
        assert!(
            controls
                .claim_call("ice", "sales", "other", "package")
                .is_err()
        );
        drop(
            controls
                .claim_call("ice", "sales", "orders", "package")
                .unwrap(),
        );
        drop(
            controls
                .claim_call("ice", "sales", "orders", "package")
                .unwrap(),
        );
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "package")
                .is_err()
        );
    }

    #[test]
    fn mv_wipe_is_bound_to_fe_family_and_only_one_claim() {
        assert!(MvTestControls::from_grants(Vec::new()).is_err());
        assert!(
            parse_action(&json!({"action":"accelerator-wipe","family":"catalog/desired-state"}))
                .is_err()
        );
        let be = TestSession::new(vec![wipe_grant()], SessionRole::Be, 3000);
        assert!(be.controls().is_err());
        let fe = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        let controls = fe.controls().unwrap();
        drop(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .unwrap(),
        );
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
        let duplicate = TestSession::new(vec![wipe_grant(), wipe_grant()], SessionRole::Fe, 3000);
        assert!(duplicate.controls().is_err());
    }

    #[test]
    fn closing_fe_session_rejects_remaining_mv_actions() {
        let mut session = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        session.close();
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
    }

    #[test]
    fn mv_grant_parameters_are_closed_and_actions_are_not_interchangeable() {
        assert!(parse_action(&json!({"action":"document-observation","catalog":"ice","namespace":"sales","mv":"orders","level":"wipe"})).is_err());
        assert!(parse_action(&json!({"action":"accelerator-wipe","family":MV_ACCELERATOR_STATE_FAMILY.prefix(),"extra":true})).is_err());
        assert!(parse_action(&json!({"action":"start-empty"})).is_err());
    }

    #[test]
    fn rewrite_nomination_rejects_another_connection_or_generation() {
        let actual = StatementToken::new(SessionToken::new(7, 1), 2);
        let other_connection = StatementToken::new(SessionToken::new(8, 1), 2);
        let later = StatementToken::new(SessionToken::new(7, 1), 3);
        assert!(MvRewriteStatementTarget::new(actual, other_connection, "run".into()).is_err());
        assert!(MvRewriteStatementTarget::new(actual, later, "run".into()).is_err());
        assert!(MvRewriteStatementTarget::new(actual, actual, "run".into()).is_ok());
    }

    #[test]
    fn unrelated_rewrite_cannot_claim_or_create_a_marker() {
        let directory = std::env::temp_dir().join(format!("nrrewrite-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("mvx4-rewrite-hold.trigger"), "hold").unwrap();
        let session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"right","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let statement = StatementToken::new(SessionToken::new(7, 1), 2);
        let target = MvRewriteStatementTarget::new(statement, statement, "wrong".into()).unwrap();
        let statements = novarocks_parser::parse("SELECT 1").unwrap();
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("query")
        };
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new();
        controls
            .rewrite(
                Some("ice"),
                "sales",
                &query,
                true,
                Some(&target),
                &cancellation.view(),
            )
            .unwrap();
        assert!(session.role.scope().observations().is_empty());
        assert!(
            !directory
                .join("mvx4-completed-mv-target-frozen.marker")
                .exists()
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn transient_preparation_matches_exact_target_and_records_actual_single_action() {
        let directory = std::env::temp_dir().join(format!("nrtransient-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let trigger = directory.join("mvx4-scheduler-transient-preparation-orders.trigger");
        std::fs::write(&trigger, "inject").unwrap();
        let session = TestSession::new(
            vec![
                json!({"action":"transient-preparation","catalog":"ice","namespace":"sales","mv":"orders","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        assert!(
            !controls
                .transient_preparation("other", "sales", "orders")
                .unwrap()
        );
        assert!(trigger.exists());
        assert!(session.role.scope().observations().is_empty());
        assert!(
            controls
                .transient_preparation("ice", "sales", "orders")
                .unwrap()
        );
        assert!(!trigger.exists());
        assert!(trigger.with_extension("consumed").exists());
        std::fs::write(&trigger, "second injection").unwrap();
        assert!(
            !controls
                .transient_preparation("ice", "sales", "orders")
                .unwrap()
        );
        assert!(trigger.exists());
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            1
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn session_close_releases_scheduler_barrier_and_actual_waiter_is_joined() {
        let directory = std::env::temp_dir().join(format!("nrscheduler-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("mvx4-scheduler-hold.trigger"), "hold").unwrap();
        let mut session = TestSession::new(
            vec![
                json!({"action":"scheduler-hold","catalog":"ice","namespace":"sales","mv":"orders","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let waiter = std::thread::spawn(move || {
            controls.scheduler(
                &novarocks_sql::planning::mv::SqlMvTarget {
                    catalog: Some("ice".into()),
                    database: "sales".into(),
                    name: "orders".into(),
                },
                &novarocks_query_application::cancellation::QueryCancellationSource::new().view(),
            )
        });
        let until = Instant::now() + Duration::from_secs(1);
        while !directory
            .join("mvx4-scheduler-admitted-orders.marker")
            .exists()
        {
            assert!(Instant::now() < until);
            std::thread::yield_now();
        }
        session.close();
        assert!(
            waiter
                .join()
                .unwrap()
                .unwrap_err()
                .contains("session closed")
        );
        assert_eq!(
            session
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            1
        );
        std::fs::remove_dir_all(directory).unwrap();
    }
    fn nomination(
        session: &TestSession,
        statement: StatementToken,
        run_token: &str,
    ) -> StatementWorkNomination {
        StatementWorkNomination {
            target_session_id: session.role.scope().session_id().into(),
            statement_run_token: run_token.into(),
            frontend_process_id: session.role.scope().process().process_id(),
            connection_id: statement.session().connection_id(),
            session_epoch: statement.session().session_epoch(),
            statement_generation: statement.generation(),
        }
    }

    fn input(
        session: &TestSession,
        nomination: &StatementWorkNomination,
    ) -> StatementInputNomination {
        use sha2::Digest;
        StatementInputNomination {
            target_session_id: nomination.target_session_id.clone(),
            target_launch_id: session.role.scope().launch_id().into(),
            statement_run_token: nomination.statement_run_token.clone(),
            frontend_process_id: nomination.frontend_process_id.clone(),
            connection_id: nomination.connection_id,
            session_epoch: nomination.session_epoch,
            sql_sha256: sha2::Sha256::digest(b"SELECT 1").into(),
        }
    }

    #[test]
    fn rewrite_input_survives_journal_reuse_without_claiming_and_fresh_slot_stays_independent() {
        let mut session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"first","directory":"/tmp"}),
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"fresh","directory":"/tmp"}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let actual = StatementToken::new(SessionToken::new(7, 1), 1);
        let original = nomination(&session, actual, "first");
        let pinned = input(&session, &original);
        assert!(controls.nominate_statement(&pinned, &original).is_err());
        let mut foreign_session = pinned.clone();
        foreign_session.target_session_id = "foreign-session".into();
        let mut foreign_process = pinned.clone();
        foreign_process.frontend_process_id =
            novarocks_types::FrontendProcessId::new_v7().to_string();
        for foreign in [foreign_session, foreign_process] {
            assert!(controls.pin_statement_input(&foreign).is_err());
        }
        assert!(
            controls
                .actions
                .iter()
                .all(|action| action.frozen_input.lock().unwrap().is_none())
        );
        controls.pin_statement_input(&pinned).unwrap();
        controls.pin_statement_input(&pinned).unwrap();
        let mut different_client = pinned.clone();
        different_client.session_epoch += 1;
        let mut different_connection = pinned.clone();
        different_connection.connection_id += 1;
        let mut different_sql = pinned.clone();
        different_sql.sql_sha256[0] ^= 0xff;
        let mut different_launch = pinned.clone();
        different_launch.target_launch_id = "foreign-launch".into();
        for replacement in [
            different_client,
            different_connection,
            different_sql,
            different_launch,
        ] {
            assert!(controls.pin_statement_input(&replacement).is_err());
            assert!(
                controls
                    .nominate_statement(&replacement, &original)
                    .is_err()
            );
        }
        assert_eq!(
            controls.actions[0].frozen_input.lock().unwrap().as_ref(),
            Some(&pinned)
        );
        assert!(
            controls.actions.iter().all(|action| action
                .frozen_nomination
                .lock()
                .unwrap()
                .is_none())
        );
        let mut unknown = pinned.clone();
        unknown.statement_run_token = "not-declared".into();
        controls.pin_statement_input(&unknown).unwrap();
        assert_eq!(controls.actions.len(), 2);
        assert!(controls.actions[1].frozen_input.lock().unwrap().is_none());
        let fresh_nom = nomination(
            &session,
            StatementToken::new(SessionToken::new(8, 2), 1),
            "fresh",
        );
        let fresh_input = input(&session, &fresh_nom);
        controls.pin_statement_input(&fresh_input).unwrap();
        controls
            .nominate_statement(&fresh_input, &fresh_nom)
            .unwrap();
        controls.nominate_statement(&pinned, &original).unwrap();
        assert!(session.role.scope().observations().is_empty());
        session.close();
        assert!(controls.pin_statement_input(&pinned).is_err());
        assert!(controls.nominate_statement(&pinned, &original).is_err());
    }

    #[test]
    fn rewrite_first_nomination_cannot_redirect_before_any_action_claim() {
        let directory = std::env::temp_dir().join(format!("nrmvfreeze-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"frozen-run","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let a = StatementToken::new(SessionToken::new(7, 1), 1);
        let original = nomination(&session, a, "frozen-run");
        controls
            .pin_statement_input(&input(&session, &original))
            .unwrap();
        controls
            .nominate_statement(&input(&session, &original), &original)
            .unwrap();
        controls
            .nominate_statement(&input(&session, &original), &original)
            .unwrap();
        let statements = novarocks_parser::parse("SELECT 1").unwrap();
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("query")
        };
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new();
        let a_target = MvRewriteStatementTarget::new(a, a, "frozen-run".into()).unwrap();
        controls
            .rewrite(
                Some("ice"),
                "sales",
                query,
                false,
                Some(&a_target),
                &cancellation.view(),
            )
            .unwrap();
        assert!(session.role.scope().observations().is_empty());
        for b in [
            StatementToken::new(SessionToken::new(7, 1), 2),
            StatementToken::new(SessionToken::new(7, 2), 1),
            StatementToken::new(SessionToken::new(8, 1), 1),
        ] {
            assert!(
                controls
                    .nominate_statement(
                        &input(&session, &nomination(&session, b, "frozen-run")),
                        &nomination(&session, b, "frozen-run")
                    )
                    .unwrap_err()
                    .contains("replacement")
            );
            let b_target = MvRewriteStatementTarget::new(b, b, "frozen-run".into()).unwrap();
            assert!(
                controls
                    .rewrite(
                        Some("ice"),
                        "sales",
                        query,
                        true,
                        Some(&b_target),
                        &cancellation.view()
                    )
                    .unwrap_err()
                    .contains("frozen original")
            );
        }
        assert!(session.role.scope().observations().is_empty());
        assert!(
            !directory
                .join("mvx4-completed-mv-target-frozen.marker")
                .exists()
        );
        // No hold existed: the original target can replay without consuming an action.
        controls
            .rewrite(
                Some("ice"),
                "sales",
                query,
                true,
                Some(&a_target),
                &cancellation.view(),
            )
            .unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn unrelated_observer_nomination_does_not_steal_a_finite_rewrite_target() {
        let session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"listed","directory":"/tmp"}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let other = StatementToken::new(SessionToken::new(8, 1), 1);
        controls
            .nominate_statement(
                &input(&session, &nomination(&session, other, "unlisted")),
                &nomination(&session, other, "unlisted"),
            )
            .unwrap();
        assert!(
            controls.actions[0]
                .frozen_nomination
                .lock()
                .unwrap()
                .is_none()
        );
        let actual = StatementToken::new(SessionToken::new(7, 1), 1);
        controls
            .pin_statement_input(&input(&session, &nomination(&session, actual, "listed")))
            .unwrap();
        controls
            .nominate_statement(
                &input(&session, &nomination(&session, actual, "listed")),
                &nomination(&session, actual, "listed"),
            )
            .unwrap();
        assert!(session.role.scope().observations().is_empty());
    }

    #[test]
    fn mv_controls_reject_foreign_domain_and_mixed_actual_fe_processes() {
        let first = TestSession::new(vec![observation()], SessionRole::Fe, 3000);
        let second = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        assert_ne!(first.role.scope().process(), second.role.scope().process());
        let mut mixed = first.role.scope().grants_for(DOMAIN);
        mixed.extend(second.role.scope().grants_for(DOMAIN));
        assert!(MvTestControls::from_grants(mixed).is_err());
        assert!(first.role.scope().observations().is_empty());
        assert!(second.role.scope().observations().is_empty());

        let foreign =
            TestSession::new_in_domain(vec![wipe_grant()], SessionRole::Fe, 3000, "foreign-mv");
        assert!(
            MvTestControls::from_grants(foreign.role.scope().grants_for("foreign-mv")).is_err()
        );
        assert!(foreign.role.scope().observations().is_empty());
    }

    #[test]
    fn mv_document_target_mismatches_do_not_consume_independent_levels() {
        let mut provenance = observation();
        provenance["level"] = json!("provenance");
        let session = TestSession::new(vec![observation(), provenance], SessionRole::Fe, 3000);
        let controls = session.controls().unwrap();
        for (catalog, namespace, mv, level) in [
            ("other", "sales", "orders", "package"),
            ("ice", "other", "orders", "package"),
            ("ice", "sales", "other", "package"),
            ("ice", "sales", "orders", "baseline"),
            ("ice", "sales", "orders", "wipe"),
        ] {
            assert!(controls.claim_call(catalog, namespace, mv, level).is_err());
        }
        assert!(session.role.scope().observations().is_empty());
        let package = controls
            .claim_call("ice", "sales", "orders", "package")
            .unwrap();
        let provenance = controls
            .claim_call("ice", "sales", "orders", "provenance")
            .unwrap();
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "package")
                .is_err()
        );
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "provenance")
                .is_err()
        );
        let events = session.role.scope().observations();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event == "claimed")
                .count(),
            2
        );
        // Authorization alone is not evidence that lake documents were read.
        assert_eq!(
            events.iter().filter(|event| event.event == "hit").count(),
            0
        );
        drop((package, provenance));
    }

    #[test]
    fn mv_family_wipe_does_not_renew_by_changing_mv_or_rebuilding_owner() {
        let mut first = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        let controls = first.controls().unwrap();
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "package")
                .is_err()
        );
        let claim = controls
            .claim_call("ice", "sales", "orders", "wipe")
            .unwrap();
        let reconstructed = first.controls().unwrap();
        // Wipe is one whole-family authorization, not one authorization per MV.
        assert!(
            reconstructed
                .claim_call("other", "other", "other", "wipe")
                .is_err()
        );
        drop(claim);
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
        assert_eq!(
            first
                .role
                .scope()
                .observations()
                .iter()
                .filter(|event| event.event == "hit")
                .count(),
            0,
        );
        first.close();
        let replacement = TestSession::new(vec![wipe_grant()], SessionRole::Fe, 3000);
        assert_ne!(
            first.role.scope().process(),
            replacement.role.scope().process()
        );
        assert!(
            controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
        let new_controls = replacement.controls().unwrap();
        drop(
            new_controls
                .claim_call("ice", "sales", "orders", "wipe")
                .unwrap(),
        );
        assert!(
            new_controls
                .claim_call("ice", "sales", "orders", "wipe")
                .is_err()
        );
    }

    #[test]
    fn rewrite_foreign_session_or_fe_nomination_cannot_pin_the_target() {
        let session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"listed","directory":"/tmp"}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = session.controls().unwrap();
        let statement = StatementToken::new(SessionToken::new(7, 1), 1);
        let original = nomination(&session, statement, "listed");
        let mut wrong_session = original.clone();
        wrong_session.target_session_id = "other-fe-session".into();
        let mut wrong_process = original.clone();
        wrong_process.frontend_process_id =
            novarocks_types::FrontendProcessId::new_v7().to_string();
        for foreign in [wrong_session, wrong_process] {
            assert!(
                controls
                    .nominate_statement(&input(&session, &foreign), &foreign)
                    .is_err()
            );
            assert!(
                controls.actions[0]
                    .frozen_nomination
                    .lock()
                    .unwrap()
                    .is_none()
            );
        }
        controls
            .pin_statement_input(&input(&session, &original))
            .unwrap();
        controls
            .nominate_statement(&input(&session, &original), &original)
            .unwrap();
        controls
            .nominate_statement(&input(&session, &original), &original)
            .unwrap();
        assert_eq!(
            controls.actions[0]
                .frozen_nomination
                .lock()
                .unwrap()
                .as_ref(),
            Some(&original),
        );
        assert!(session.role.scope().observations().is_empty());
    }

    #[test]
    fn recovery_wrong_target_phase_or_arm_token_cannot_claim() {
        let root = std::env::temp_dir().join(format!("nrmvrecovery-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let controlled = TestSession::new(
            vec![
                json!({"action":"recovery-hold","catalog":"ice","namespace":"sales","mv":"orders","phase":"data-prepared","directory":root,"token":"right"}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = controlled.controls().unwrap();
        let trigger = root.join("mv-refresh-at-data-prepared.trigger");
        std::fs::write(&trigger, "token=wrong\n").unwrap();
        for (catalog, namespace, mv, phase) in [
            ("other", "sales", "orders", "data-prepared"),
            ("ice", "other", "orders", "data-prepared"),
            ("ice", "sales", "other", "data-prepared"),
            ("ice", "sales", "orders", "publication-committed"),
        ] {
            controls.recovery(catalog, namespace, mv, phase).unwrap();
        }
        assert!(controlled.role.scope().observations().is_empty());
        assert!(
            controls
                .recovery("ice", "sales", "orders", "data-prepared")
                .unwrap_err()
                .contains("finite grant")
        );
        assert!(controlled.role.scope().observations().is_empty());
        assert!(trigger.exists());
        // A wrong transport token did not consume the authorized action.
        drop(
            controls.actions[0]
                .grant
                .claim_once(controls.process)
                .unwrap(),
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rewrite_exact_nomination_and_query_claim_once_and_join_released_waiter() {
        let directory = std::env::temp_dir().join(format!("nrmvrewrite-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&directory).unwrap();
        let hold = directory.join("mvx4-rewrite-hold.trigger");
        let marker = directory.join("mvx4-completed-mv-target-frozen.marker");
        std::fs::write(&hold, "hold").unwrap();
        let mut session = TestSession::new(
            vec![
                json!({"action":"rewrite-hold","catalog":"ice","namespace":"sales","sql":"SELECT 1","statement_run_token":"exact","directory":directory}),
            ],
            SessionRole::Fe,
            3000,
        );
        let controls = std::sync::Arc::new(session.controls().unwrap());
        let actual = StatementToken::new(SessionToken::new(7, 1), 1);
        controls
            .pin_statement_input(&input(&session, &nomination(&session, actual, "exact")))
            .unwrap();
        controls
            .nominate_statement(
                &input(&session, &nomination(&session, actual, "exact")),
                &nomination(&session, actual, "exact"),
            )
            .unwrap();
        let target = MvRewriteStatementTarget::new(actual, actual, "exact".into()).unwrap();
        let statements = novarocks_parser::parse("SELECT 1").unwrap();
        let [Statement::Query(query)] = statements.as_slice() else {
            panic!("query");
        };
        let other_statements = novarocks_parser::parse("SELECT 2").unwrap();
        let [Statement::Query(other_query)] = other_statements.as_slice() else {
            panic!("query");
        };
        let cancellation =
            novarocks_query_application::cancellation::QueryCancellationSource::new();
        for (catalog, namespace, submitted, selected) in [
            (None, "sales", query, true),
            (Some("other"), "sales", query, true),
            (Some("ice"), "other", query, true),
            (Some("ice"), "sales", other_query, true),
            (Some("ice"), "sales", query, false),
        ] {
            controls
                .rewrite(
                    catalog,
                    namespace,
                    submitted,
                    selected,
                    Some(&target),
                    &cancellation.view(),
                )
                .unwrap();
        }
        assert!(session.role.scope().observations().is_empty());
        assert!(!marker.exists());
        let worker_controls = controls.clone();
        let worker_query = query.clone();
        let worker_target = MvRewriteStatementTarget::new(actual, actual, "exact".into()).unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let cancellation =
                novarocks_query_application::cancellation::QueryCancellationSource::new();
            let result = worker_controls.rewrite(
                Some("ice"),
                "sales",
                &worker_query,
                true,
                Some(&worker_target),
                &cancellation.view(),
            );
            let _ = send.send(result);
        });
        let assertion_deadline = Instant::now() + Duration::from_secs(1);
        while !marker.exists() && !worker.is_finished() && Instant::now() < assertion_deadline {
            std::thread::park_timeout(Duration::from_millis(1));
        }
        let saw_actual_marker = marker.exists();
        let release = std::fs::remove_file(&hold);
        let received = receive.recv_timeout(Duration::from_millis(300));
        if received.is_err() {
            session.close();
        }
        // Join even when marker matching or barrier release failed.
        let joined = worker.join();
        assert!(joined.is_ok(), "rewrite waiter panicked");
        release.unwrap();
        assert!(saw_actual_marker, "exact rewrite did not reach its marker");
        received
            .expect("released rewrite exceeded its watchdog")
            .unwrap();
        std::fs::write(&hold, "second hold").unwrap();
        controls
            .rewrite(
                Some("ice"),
                "sales",
                query,
                true,
                Some(&target),
                &cancellation.view(),
            )
            .unwrap();
        let events = session.role.scope().observations();
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event == "claimed")
                .count(),
            1
        );
        assert_eq!(
            events.iter().filter(|event| event.event == "hit").count(),
            1
        );
        assert!(hold.exists(), "replay consumed a second action");
        std::fs::remove_dir_all(directory).unwrap();
    }
}
