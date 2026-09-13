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

//! Worker-owned process-local runtime-filter participant state.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};

use novarocks_execution::runtime::mem_tracker::MemTracker;
use novarocks_execution::runtime_filter::{
    RuntimeFilterBindingId, RuntimeFilterChannelId, RuntimeFilterRowEffect,
    RuntimeFilterSessionRef, scan_domain::RuntimeFilterScanUnitOutcome,
};
use novarocks_types::UniqueId;

use super::domain::{
    BackendChannelIdentity, BackendConsumerSubscriptionIdentity, BackendFrontendFeedbackSink,
    BackendIngressDedupe, BackendIngressResult, BackendMaterializedDeliverySink,
    BackendParticipantInstall, BackendRuntimeFilterEvent, BackendRuntimeFilterEventObserver,
    BackendRuntimeFilterSession,
};
use super::execution_session::{
    RuntimeFilterParticipantOutbound, WorkerRuntimeFilterExecutionSession,
};
use super::observation::{RuntimeFilterObservationEmitter, RuntimeFilterObservationSnapshot};
use super::participant_ingress::{
    DeliveryIngressFrame, DeliveryIngressRoute, ProducerIngressCommand, ProducerIngressRoute,
    dispatch_delivery_frame, dispatch_producer_failure, dispatch_producer_frame,
};

/// One sealed runtime-filter participant's local domain state.
///
/// Native adapters retain wire decoding and their transport egress, but cannot
/// own or recreate installed routing, session, observation, dedupe, or
/// cancellation state.
pub struct WorkerRuntimeFilterParticipant {
    install: BackendParticipantInstall,
    observation: Arc<RuntimeFilterObservationEmitter>,
    producer_sessions: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    consumer_sessions: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
    delivery_dedupe: Arc<BackendIngressDedupe>,
    cancelled: Arc<AtomicBool>,
    _memory: Arc<MemTracker>,
}

impl WorkerRuntimeFilterParticipant {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        install: BackendParticipantInstall,
        observation: Arc<RuntimeFilterObservationEmitter>,
        producer_sessions: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
        consumer_sessions: BTreeMap<RuntimeFilterBindingId, Arc<BackendRuntimeFilterSession>>,
        delivery_dedupe: Arc<BackendIngressDedupe>,
        cancelled: Arc<AtomicBool>,
        memory: Arc<MemTracker>,
    ) -> Self {
        Self {
            install,
            observation,
            producer_sessions,
            consumer_sessions,
            delivery_dedupe,
            cancelled,
            _memory: memory,
        }
    }

    pub const fn participant(&self) -> super::domain::BackendParticipantIdentity {
        self.install.participant()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub fn session_for_fragment(
        &self,
        fragment_instance_id: UniqueId,
        outbound: Arc<dyn RuntimeFilterParticipantOutbound>,
    ) -> RuntimeFilterSessionRef {
        Arc::new(WorkerRuntimeFilterExecutionSession::new(
            fragment_instance_id,
            self.install.participant(),
            self.producer_sessions.clone(),
            self.consumer_sessions.clone(),
            outbound,
            Arc::clone(&self.observation),
            Arc::clone(&self.cancelled),
        )) as RuntimeFilterSessionRef
    }

    pub fn dispatch_delivery(
        &self,
        route: DeliveryIngressRoute,
        frame: DeliveryIngressFrame<'_>,
    ) -> BackendIngressResult {
        dispatch_delivery_frame(
            &self.install,
            &self.consumer_sessions,
            &self.delivery_dedupe,
            route,
            frame,
        )
    }

    pub fn dispatch_producer(
        &self,
        route: ProducerIngressRoute,
        command: ProducerIngressCommand,
    ) -> BackendIngressResult {
        dispatch_producer_frame(
            &self.install,
            &self.producer_sessions,
            &self.observation,
            route,
            command,
        )
    }

    pub fn dispatch_producer_failure(
        &self,
        channel_id: RuntimeFilterChannelId,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
    ) -> BackendIngressResult {
        dispatch_producer_failure(
            &self.install,
            &self.producer_sessions,
            channel_id,
            binding_id,
            fragment_instance_id,
        )
    }

    pub fn set_materialized_delivery_sink(&self, sink: Arc<dyn BackendMaterializedDeliverySink>) {
        for session in self.producer_sessions.values() {
            session.set_materialized_delivery_sink(Arc::clone(&sink));
        }
    }

    pub fn close(&self) {
        self.cancelled.store(true, Ordering::Release);
        for session in self.producer_sessions.values() {
            session.clear_frontend_feedback_sink();
        }
    }

    pub fn set_frontend_feedback_sink(&self, sink: Weak<dyn BackendFrontendFeedbackSink>) {
        for session in self.producer_sessions.values() {
            session.set_frontend_feedback_sink(sink.clone());
        }
    }

    pub fn capture_observation(&self) -> RuntimeFilterObservationSnapshot {
        self.observation.capture()
    }

    pub fn prepare_terminal_capture(
        &self,
        coordinator_finalize: bool,
    ) -> RuntimeFilterObservationSnapshot {
        if coordinator_finalize {
            self.observation.seal()
        } else {
            self.observation.cancel_open_channels_and_seal()
        }
    }

    pub fn record_row_effect(
        &self,
        fragment_instance_id: UniqueId,
        effect: RuntimeFilterRowEffect,
    ) {
        let Some(identity) = self.consumer_identity(effect.binding_id(), fragment_instance_id)
        else {
            return;
        };
        self.observation
            .record(BackendRuntimeFilterEvent::ConsumerRowsEvaluated {
                identity,
                logical_version: effect.logical_version(),
                input_rows: effect.input_rows(),
                output_rows: effect.output_rows(),
            });
    }

    pub fn record_scan_unit_outcome(
        &self,
        fragment_instance_id: UniqueId,
        outcome: RuntimeFilterScanUnitOutcome,
    ) {
        let Some(identity) = self.consumer_identity(outcome.binding_id(), fragment_instance_id)
        else {
            return;
        };
        match outcome.evaluation() {
            novarocks_execution::runtime_filter::scan_domain::RuntimeFilterScanUnitEvaluation::Evaluated {
                decision,
                logical_version,
            } => self.observation.record(
                BackendRuntimeFilterEvent::ConsumerScanUnitEvaluated {
                    identity,
                    logical_version,
                    decision,
                },
            ),
            novarocks_execution::runtime_filter::scan_domain::RuntimeFilterScanUnitEvaluation::NotEvaluated {
                reason,
                observed_version,
            } => self.observation.record(
                BackendRuntimeFilterEvent::ConsumerScanUnitNotEvaluated {
                    identity,
                    observed_version,
                    reason,
                },
            ),
        }
    }

    fn consumer_identity(
        &self,
        binding_id: RuntimeFilterBindingId,
        fragment_instance_id: UniqueId,
    ) -> Option<BackendConsumerSubscriptionIdentity> {
        let session = self.consumer_sessions.get(&binding_id)?;
        Some(BackendConsumerSubscriptionIdentity::new(
            BackendChannelIdentity::new(
                self.install.participant(),
                binding_id,
                session.channel().channel_id(),
            ),
            binding_id,
            fragment_instance_id,
        ))
    }
}
