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

use std::sync::Arc;

use crate::exec::pipeline::operator::input_construction::ReceiverInputConstructionStopOwner;

#[cfg(test)]
use crate::runtime::exchange::ExchangeColumnBinding;
use crate::runtime::fragment::error::{
    FragmentLaunchError, FragmentLaunchErrorKind, FragmentLaunchStage,
};
use crate::runtime::fragment::io::{
    ExchangeReceiverKey, ExchangeReceiverPort, ExchangeReceiverRegistration, FragmentCommitLease,
    FragmentCommitPort,
};
use crate::runtime::fragment::io::{
    FragmentResultSession, FragmentResultWriter, ResultAbort, ResultWriteSpec,
};
use novarocks_types::UniqueId;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ResourceKind {
    SinkCommit,
    Result,
    Exchange,
}

impl ResourceKind {
    fn cleanup_failure_detail(self) -> &'static str {
        match self {
            Self::SinkCommit => "injected sink commit cleanup failure",
            Self::Result => "injected result registration cleanup failure",
            Self::Exchange => "injected exchange registration cleanup failure",
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct ResourceCleanupFaults {
    failed: std::collections::BTreeSet<ResourceKind>,
}

#[allow(
    dead_code,
    reason = "Cleanup fault injection is retained for fragment lifecycle regression coverage."
)]
impl ResourceCleanupFaults {
    #[cfg(test)]
    pub(crate) fn with_failure(mut self, resource: ResourceKind) -> Self {
        self.failed.insert(resource);
        self
    }

    fn should_fail(&self, resource: ResourceKind) -> bool {
        self.failed.contains(&resource)
    }
}

pub(crate) struct SinkCommitLease {
    lease: Option<Box<dyn FragmentCommitLease>>,
    cleanup_should_fail: bool,
}

impl SinkCommitLease {
    pub(crate) fn acquire(
        port: &Arc<dyn FragmentCommitPort>,
        finst_id: UniqueId,
        cleanup_should_fail: bool,
    ) -> Result<Self, FragmentLaunchError> {
        let lease = port.acquire(finst_id).map_err(registration_error)?;
        Ok(Self {
            lease: Some(lease),
            cleanup_should_fail,
        })
    }

    fn rollback(&mut self) -> Result<(), String> {
        if let Some(lease) = self.lease.take() {
            lease.rollback()?;
        }
        if self.cleanup_should_fail {
            self.cleanup_should_fail = false;
            return Err(ResourceKind::SinkCommit
                .cleanup_failure_detail()
                .to_string());
        }
        Ok(())
    }

    fn handoff(&mut self) -> Result<(), String> {
        if let Some(lease) = self.lease.take() {
            lease.handoff()?;
        }
        self.cleanup_should_fail = false;
        Ok(())
    }
}

impl Drop for SinkCommitLease {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

pub(crate) struct ResultRegistration {
    session: Arc<dyn FragmentResultSession>,
    active: bool,
    cleanup_should_fail: bool,
}

impl ResultRegistration {
    pub(crate) fn acquire(
        writer: &Arc<dyn FragmentResultWriter>,
        spec: ResultWriteSpec,
        cleanup_should_fail: bool,
    ) -> Result<Self, FragmentLaunchError> {
        let session = writer
            .open(spec)
            .map_err(|error| registration_error(error.to_string()))?;
        Ok(Self {
            session,
            active: true,
            cleanup_should_fail,
        })
    }

    fn rollback(&mut self) -> Result<(), String> {
        if self.active {
            self.session.abort(ResultAbort::PrepareRollback);
            self.active = false;
        }
        if self.cleanup_should_fail {
            self.cleanup_should_fail = false;
            return Err(ResourceKind::Result.cleanup_failure_detail().to_string());
        }
        Ok(())
    }

    fn finish_success(&mut self) {
        self.active = false;
        self.cleanup_should_fail = false;
    }

    fn finish_failure(&mut self, error: String) {
        if self.active {
            self.session.abort(ResultAbort::Failed(error));
            self.active = false;
        }
        self.cleanup_should_fail = false;
    }

    fn finish_cancelled(&mut self, reason: String) {
        if self.active {
            self.session.abort(ResultAbort::Cancelled(reason));
            self.active = false;
        }
        self.cleanup_should_fail = false;
    }

    /// Interrupt a result write as soon as cancellation wins, while retaining
    /// this registration until the fragment has actually stopped.
    fn abort_for_cancellation(&mut self, reason: String) {
        if self.active {
            self.session.abort(ResultAbort::Cancelled(reason));
            self.active = false;
        }
    }
}

impl Drop for ResultRegistration {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

struct RootResultRegistration {
    session: Arc<dyn crate::runtime::fragment::io::RootResultSession>,
    active: bool,
    cleanup_should_fail: bool,
}
impl RootResultRegistration {
    fn abort(&mut self, reason: ResultAbort) {
        if self.active {
            self.active = false;
            self.session.abort(reason);
        }
    }
    fn rollback(&mut self) -> Result<(), String> {
        self.abort(ResultAbort::PrepareRollback);
        if std::mem::take(&mut self.cleanup_should_fail) {
            return Err(ResourceKind::Result.cleanup_failure_detail().to_string());
        }
        Ok(())
    }
}
impl Drop for RootResultRegistration {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

pub(crate) struct ExchangeRegistration {
    port: Arc<dyn ExchangeReceiverPort>,
    keys: Vec<ExchangeReceiverKey>,
    active: bool,
    cleanup_should_fail: bool,
    construction_stop: Option<ReceiverInputConstructionStopOwner>,
}

impl ExchangeRegistration {
    /// Register the program's receivers, already projected for this instance,
    /// under their rollback and terminal-cleanup owner.
    pub(crate) fn acquire_registrations(
        port: Arc<dyn ExchangeReceiverPort>,
        registrations: Vec<ExchangeReceiverRegistration>,
        cleanup_should_fail: bool,
        construction_stop: Option<ReceiverInputConstructionStopOwner>,
    ) -> Result<Option<Self>, FragmentLaunchError> {
        if registrations.is_empty() {
            return Ok(None);
        }
        let mut registration = Self {
            port,
            keys: Vec::with_capacity(registrations.len()),
            active: true,
            cleanup_should_fail,
            construction_stop,
        };
        for entry in registrations {
            let key = entry.key;
            if let Err(error) = registration.port.register(entry) {
                let diagnostics = registration.rollback().err().into_iter().collect();
                return Err(registration_error(error).with_cleanup_diagnostics(diagnostics));
            }
            registration.keys.push(key);
        }
        Ok(Some(registration))
    }

    fn rollback(&mut self) -> Result<(), String> {
        self.stop_construction(novarocks_functions::KernelFailure::Cancelled.into());
        if self.active {
            for key in self.keys.drain(..).rev() {
                self.port.cancel(key);
            }
            self.active = false;
        }
        if self.cleanup_should_fail {
            self.cleanup_should_fail = false;
            return Err(ResourceKind::Exchange.cleanup_failure_detail().to_string());
        }
        Ok(())
    }

    fn finish_success(&mut self) {
        self.stop_construction(novarocks_functions::KernelFailure::Cancelled.into());
        if self.active {
            for key in self.keys.drain(..).rev() {
                self.port.remove(key);
            }
            self.active = false;
        }
        self.cleanup_should_fail = false;
    }

    fn finish_cancelled(&mut self) {
        self.stop_construction(novarocks_functions::KernelFailure::Cancelled.into());
        if self.active {
            for key in self.keys.drain(..).rev() {
                self.port.cancel(key);
            }
            self.active = false;
        }
        self.cleanup_should_fail = false;
    }

    fn stop_construction(&self, cause: super::ExecutionFailure) {
        if let Some(stop) = &self.construction_stop {
            stop.stop(cause);
        }
    }
}

impl Drop for ExchangeRegistration {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

/// The original receiver registration, moved to its application's retirement gate.
///
/// Dropping an uncompleted or rolled-back task cancels its original receivers.
/// A raw successful terminal fact selects the original success removal instead.
/// This capability is deliberately not cloneable.
pub struct ExchangeReceiverRetirement {
    registration: ExchangeRegistration,
    disposition: ExchangeRetirementDisposition,
}

#[derive(Clone, Copy)]
enum ExchangeRetirementDisposition {
    Cancel,
    Remove,
}

impl ExchangeReceiverRetirement {
    /// Record the kernel's fact before publishing task convergence. This does
    /// not remove or cancel a receiver; the application's gate still owns that.
    pub fn record_terminal(&mut self, fact: &super::FragmentTerminalFact) {
        self.disposition = match fact.outcome() {
            super::FragmentOutcome::Succeeded => ExchangeRetirementDisposition::Remove,
            super::FragmentOutcome::Failed(error) => {
                self.stop_construction(error.cause().clone());
                ExchangeRetirementDisposition::Cancel
            }
            super::FragmentOutcome::Cancelled { .. } => ExchangeRetirementDisposition::Cancel,
        };
    }

    /// Stop construction on a real failure or abort without retiring ingress.
    pub fn stop_construction(&self, cause: super::ExecutionFailure) {
        self.registration.stop_construction(cause);
    }

    /// Retire synchronously while the caller holds its original inbound gate.
    pub fn retire(self) {
        drop(self);
    }
}

impl Drop for ExchangeReceiverRetirement {
    fn drop(&mut self) {
        match self.disposition {
            ExchangeRetirementDisposition::Remove => self.registration.finish_success(),
            ExchangeRetirementDisposition::Cancel => self.registration.finish_cancelled(),
        }
    }
}

pub(crate) struct FragmentResources {
    commit_port: Arc<dyn FragmentCommitPort>,
    exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
    sink_commit: Option<SinkCommitLease>,
    result: Option<ResultRegistration>,
    root_result: Option<RootResultRegistration>,
    exchange: Option<ExchangeRegistration>,
    cleanup_faults: ResourceCleanupFaults,
}

impl FragmentResources {
    pub(crate) fn new(
        commit_port: Arc<dyn FragmentCommitPort>,
        exchange_receiver_port: Arc<dyn ExchangeReceiverPort>,
        cleanup_faults: ResourceCleanupFaults,
    ) -> Self {
        Self {
            commit_port,
            exchange_receiver_port,
            sink_commit: None,
            result: None,
            root_result: None,
            exchange: None,
            cleanup_faults,
        }
    }

    pub(crate) fn acquire_sink_commit(
        &mut self,
        finst_id: UniqueId,
    ) -> Result<(), FragmentLaunchError> {
        self.sink_commit = Some(SinkCommitLease::acquire(
            &self.commit_port,
            finst_id,
            self.cleanup_faults.should_fail(ResourceKind::SinkCommit),
        )?);
        Ok(())
    }

    pub(crate) fn acquire_result_for_static(
        &mut self,
        sink: Option<&novarocks_local_program::StaticSinkProgram>,
        writer: &Arc<dyn FragmentResultWriter>,
        spec: ResultWriteSpec,
    ) -> Result<(), FragmentLaunchError> {
        if !matches!(
            sink,
            Some(novarocks_local_program::StaticSinkProgram::Result)
        ) {
            return Ok(());
        }
        self.result = Some(ResultRegistration::acquire(
            writer,
            spec,
            self.cleanup_faults.should_fail(ResourceKind::Result),
        )?);
        Ok(())
    }

    pub(crate) fn acquire_root_result_for_static(
        &mut self,
        sink: Option<&novarocks_local_program::StaticSinkProgram>,
        session: Option<Arc<dyn crate::runtime::fragment::io::RootResultSession>>,
        identity: Option<novarocks_execution_contract::TaskIdentity>,
    ) -> Result<(), FragmentLaunchError> {
        let Some(novarocks_local_program::StaticSinkProgram::RootResult(contract)) = sink else {
            if let Some(session) = session {
                session.abort(ResultAbort::PrepareRollback);
                return Err(registration_error(
                    "bounded root session supplied for a different sink",
                ));
            }
            return Ok(());
        };
        let session = session.ok_or_else(|| {
            registration_error("frozen bounded root requires its host-owned session")
        })?;
        if Some(session.spec().task) != identity
            || session.spec().contract.as_ref() != contract.as_ref()
        {
            session.abort(ResultAbort::PrepareRollback);
            return Err(registration_error(
                "bounded root session disagrees with exact task or frozen contract",
            ));
        }
        self.root_result = Some(RootResultRegistration {
            session,
            active: true,
            cleanup_should_fail: self.cleanup_faults.should_fail(ResourceKind::Result),
        });
        Ok(())
    }
    pub(crate) fn root_result_session(
        &self,
    ) -> Option<Arc<dyn crate::runtime::fragment::io::RootResultSession>> {
        self.root_result
            .as_ref()
            .map(|root| Arc::clone(&root.session))
    }

    pub(crate) fn result_session(&self) -> Option<Arc<dyn FragmentResultSession>> {
        self.result
            .as_ref()
            .map(|registration| Arc::clone(&registration.session))
    }

    /// Wake a root result sink that is blocked on its client-facing session.
    ///
    /// This does not release the registration or establish a terminal fact;
    /// those remain owned by the actual driver-stop path.
    pub(crate) fn abort_result_for_cancellation(&mut self, reason: &str) {
        if let Some(root) = self.root_result.as_mut() {
            root.abort(ResultAbort::Cancelled(reason.to_string()));
        }
        if let Some(result) = self.result.as_mut() {
            result.abort_for_cancellation(reason.to_string());
        }
    }

    pub(crate) fn acquire_compiled_exchange(
        &mut self,
        registrations: Vec<ExchangeReceiverRegistration>,
        construction_stop: Option<ReceiverInputConstructionStopOwner>,
    ) -> Result<(), FragmentLaunchError> {
        self.exchange = ExchangeRegistration::acquire_registrations(
            Arc::clone(&self.exchange_receiver_port),
            registrations,
            self.cleanup_faults.should_fail(ResourceKind::Exchange),
            construction_stop,
        )?;
        Ok(())
    }

    pub(crate) fn take_exchange_retirement(&mut self) -> Option<ExchangeReceiverRetirement> {
        self.exchange
            .take()
            .map(|registration| ExchangeReceiverRetirement {
                registration,
                disposition: ExchangeRetirementDisposition::Cancel,
            })
    }

    pub(crate) fn rollback(&mut self) -> Vec<String> {
        let mut diagnostics = Vec::new();
        if let Some(mut root) = self.root_result.take()
            && let Err(error) = root.rollback()
        {
            diagnostics.push(error);
        }
        if let Some(mut exchange) = self.exchange.take()
            && let Err(error) = exchange.rollback()
        {
            diagnostics.push(error);
        }
        if let Some(mut result) = self.result.take()
            && let Err(error) = result.rollback()
        {
            diagnostics.push(error);
        }
        if let Some(mut sink_commit) = self.sink_commit.take()
            && let Err(error) = sink_commit.rollback()
        {
            diagnostics.push(error);
        }
        diagnostics
    }

    pub(crate) fn finish_success(&mut self) {
        if let Some(mut root) = self.root_result.take() {
            root.active = false;
            root.cleanup_should_fail = false;
        }
        if let Some(mut exchange) = self.exchange.take() {
            exchange.finish_success();
        }
        if let Some(mut result) = self.result.take() {
            result.finish_success();
        }
    }

    pub(crate) fn handoff_sink_commit(&mut self) {
        if let Some(mut sink_commit) = self.sink_commit.take() {
            let _ = sink_commit.handoff();
        }
    }

    pub(crate) fn finish_failure(&mut self, error: String) {
        if let Some(mut root) = self.root_result.take() {
            root.abort(ResultAbort::Failed(error.clone()));
            root.cleanup_should_fail = false;
        }
        if let Some(mut exchange) = self.exchange.take() {
            exchange.finish_cancelled();
        }
        if let Some(mut result) = self.result.take() {
            result.finish_failure(error);
        }
    }

    pub(crate) fn finish_cancelled(&mut self, reason: String) {
        if let Some(mut root) = self.root_result.take() {
            root.abort(ResultAbort::Cancelled(reason.clone()));
            root.cleanup_should_fail = false;
        }
        if let Some(mut exchange) = self.exchange.take() {
            exchange.finish_cancelled();
        }
        if let Some(mut result) = self.result.take() {
            result.finish_cancelled(reason);
        }
    }
}

impl Drop for FragmentResources {
    fn drop(&mut self) {
        let _ = self.rollback();
    }
}

fn registration_error(detail: impl Into<String>) -> FragmentLaunchError {
    FragmentLaunchError::new(
        FragmentLaunchStage::Register,
        FragmentLaunchErrorKind::DuplicateRegistration,
        detail,
    )
}

#[cfg(test)]
mod receiver_construction_tests {
    use super::*;
    use crate::exec::chunk::{ChunkSchemaRef, typed_fixture};
    use crate::exec::pipeline::operator::input_construction::{
        try_receiver_input_construction_factory, try_runtime_input_construction_factory,
    };
    use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionFactory};
    use crate::runtime::execution_runtime::test_execution_runtime;
    use crate::runtime::fragment::io::exchange_receiver::ExecutionRuntimeExchangeReceiverPort;
    use crate::runtime::fragment::{
        ExecutionFailure, FragmentCancelReason, FragmentExecutionError, FragmentExecutionErrorKind,
        FragmentOutcome, FragmentTerminalFact,
    };
    use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
    use novarocks_types::QueryId;

    fn typed_schema(factory: &dyn InputConstructionFactory) -> ChunkSchemaRef {
        let declaration =
            typed_fixture::layout(Arc::new(arrow::datatypes::Schema::empty()), &[], &[]);
        factory.create_compiled_chunk_schema(&declaration).unwrap()
    }

    fn entry(
        key: ExchangeReceiverKey,
        schema: &ChunkSchemaRef,
        factory: &Arc<dyn InputConstructionFactory>,
    ) -> ExchangeReceiverRegistration {
        ExchangeReceiverRegistration {
            key,
            expected_senders: 1,
            expected_chunk_schema: schema.clone(),
            column_binding: ExchangeColumnBinding::Positional,
            creation_factory: factory.clone(),
        }
    }

    // A duplicate is the real registry's supported refusal, not a fabricated
    // register failure. Protect prefix rollback and original primary/secondary
    // diagnostics using exact typed creators and independent existing keys.
    // This owner-local fixture needs no service, controller or timing gate.
    #[test]
    fn receiver_construction_registration_duplicate_rolls_back_only_its_successful_prefix() {
        for has_prefix in [false, true] {
            let runtime = test_execution_runtime();
            let port: Arc<dyn ExchangeReceiverPort> =
                Arc::new(ExecutionRuntimeExchangeReceiverPort::new(runtime));
            let existing_tracker = MemTracker::new_root("existing receiver declarations");
            existing_tracker.install_limit_once(1024 * 1024).unwrap();
            let existing_factory = try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                existing_tracker.clone(),
                Arc::new(RuntimeErrorState::default()),
            )
            .unwrap();
            let existing_schema = typed_schema(existing_factory.as_ref());
            let instance = UniqueId::new(85_100, i64::from(has_prefix));
            let prefix = ExchangeReceiverKey {
                fragment_instance_id: instance,
                node_id: 1,
            };
            let conflict = ExchangeReceiverKey {
                fragment_instance_id: instance,
                node_id: 2,
            };
            let sibling = ExchangeReceiverKey {
                fragment_instance_id: instance,
                node_id: 3,
            };
            port.register(entry(conflict, &existing_schema, &existing_factory))
                .unwrap();
            port.register(entry(sibling, &existing_schema, &existing_factory))
                .unwrap();
            let before_conflict = port.snapshot(conflict).unwrap();
            let before_sibling = port.snapshot(sibling).unwrap();

            let tracker = MemTracker::new_root("candidate receiver declarations");
            tracker.install_limit_once(1024 * 1024).unwrap();
            let (factory, stop) = try_receiver_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                tracker.clone(),
                Arc::new(RuntimeErrorState::default()),
            )
            .unwrap();
            let schema = typed_schema(factory.as_ref());
            let mut registrations = Vec::new();
            if has_prefix {
                registrations.push(entry(prefix, &schema, &factory));
            }
            registrations.push(entry(conflict, &schema, &factory));
            let result = ExchangeRegistration::acquire_registrations(
                port.clone(),
                registrations,
                has_prefix,
                Some(stop),
            );
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("the original registry must refuse its already registered key"),
            };
            assert_eq!(error.stage(), FragmentLaunchStage::Register);
            assert_eq!(error.kind(), FragmentLaunchErrorKind::DuplicateRegistration);
            assert_eq!(
                error.detail(),
                format!(
                    "exchange receiver already registered: finst={} node_id=2",
                    instance.to_uuid_string(),
                )
            );
            if has_prefix {
                assert_eq!(
                    error.cleanup_diagnostics(),
                    &["injected exchange registration cleanup failure"]
                );
                assert!(port.snapshot(prefix).is_none());
                let refused = match port.receiver_handle(prefix, 1) {
                    Err(error) => error,
                    Ok(_) => panic!("the cancelled prefix cannot acquire a receiver handle"),
                };
                assert_eq!(refused, "exchange canceled");
            } else {
                assert!(error.cleanup_diagnostics().is_empty());
            }
            for (key, before) in [(conflict, before_conflict), (sibling, before_sibling)] {
                let after = port
                    .snapshot(key)
                    .expect("a preexisting receiver must survive another owner's rollback");
                assert_eq!(after.expected_senders, before.expected_senders);
                assert_eq!(after.finished_senders, before.finished_senders);
                assert_eq!(after.queued_chunks, before.queued_chunks);
                assert_eq!(after.queued_rows, before.queued_rows);
                assert_eq!(after.canceled, before.canceled);
                assert!(!after.canceled);
                assert!(port.receiver_handle(key, 1).is_ok());
            }
            let cancelled: ExecutionFailure = novarocks_functions::KernelFailure::Cancelled.into();
            assert_eq!(
                factory.observe_input_work(0).unwrap_err(),
                ConstructionFailure::Caller(Box::new(cancelled))
            );
            drop(schema);
            drop(factory);
            assert_eq!(
                tracker.current(),
                0,
                "the refused owner's actual creator aliases have exited"
            );
            port.remove(conflict);
            port.remove(sibling);
            drop(existing_schema);
            drop(existing_factory);
            assert_eq!(existing_tracker.current(), 0);
        }
    }

    // Raw cancellation includes normal Quiesce and cannot end DATA creation;
    // an originating failure must carry its exact typed cause immediately.
    // The real registration remains installed until the original guard retires.
    #[test]
    fn receiver_construction_retirement_preserves_raw_cancel_and_original_failed_cause() {
        for failed in [false, true] {
            let runtime = test_execution_runtime();
            let port: Arc<dyn ExchangeReceiverPort> =
                Arc::new(ExecutionRuntimeExchangeReceiverPort::new(runtime));
            let tracker = MemTracker::new_root("receiver terminal control");
            tracker.install_limit_once(1024 * 1024).unwrap();
            let (factory, stop) = try_receiver_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                tracker.clone(),
                Arc::new(RuntimeErrorState::default()),
            )
            .unwrap();
            let schema = typed_schema(factory.as_ref());
            let instance = UniqueId::new(85_101, i64::from(failed));
            let key = ExchangeReceiverKey {
                fragment_instance_id: instance,
                node_id: 1,
            };
            let registration = ExchangeRegistration::acquire_registrations(
                port.clone(),
                vec![entry(key, &schema, &factory)],
                false,
                Some(stop),
            )
            .unwrap()
            .unwrap();
            let mut retirement = ExchangeReceiverRetirement {
                registration,
                disposition: ExchangeRetirementDisposition::Cancel,
            };
            let original: ExecutionFailure =
                novarocks_functions::KernelFailure::DeadlineExceeded.into();
            let outcome = if failed {
                FragmentOutcome::Failed(FragmentExecutionError::from_failure(
                    FragmentExecutionErrorKind::Pipeline,
                    original.clone(),
                ))
            } else {
                FragmentOutcome::Cancelled {
                    reason: FragmentCancelReason::new("normal quiesce"),
                }
            };
            retirement.record_terminal(&FragmentTerminalFact::new(
                QueryId::new(85_100, 1),
                instance,
                outcome,
                None,
            ));
            if failed {
                assert_eq!(
                    factory.observe_input_work(1).unwrap_err(),
                    ConstructionFailure::Caller(Box::new(original.clone()))
                );
            } else {
                factory.observe_input_work(1).unwrap();
            }
            assert!(
                port.snapshot(key).is_some(),
                "a terminal fact alone cannot delete ingress"
            );
            retirement.retire();
            assert!(port.snapshot(key).is_none());
            let stopped = if failed {
                original
            } else {
                novarocks_functions::KernelFailure::Cancelled.into()
            };
            assert_eq!(
                factory.observe_input_work(1).unwrap_err(),
                ConstructionFailure::Caller(Box::new(stopped))
            );
            drop(schema);
            drop(factory);
            assert_eq!(tracker.current(), 0);
        }
    }
}
