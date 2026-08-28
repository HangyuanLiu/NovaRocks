//! FE attempt-local admission state for terminal runtime-filter feedback.

use std::collections::BTreeMap;
use std::sync::{Condvar, Mutex};

use novarocks_execution::runtime_filter::feedback_domain::RuntimeFilterFeedbackDomain;
use novarocks_proto_codec::lifecycle::{
    QueryControlEvent, QueryExecutionId, encode_query_execution_id,
};

use super::install_encoder::{
    FrontendRuntimeFilterFeedbackDeclaration, FrontendRuntimeFilterFeedbackPublisherSlot,
};

#[derive(Default)]
struct FeedbackState {
    closed: bool,
    channels: BTreeMap<u32, ChannelState>,
}

struct ChannelState {
    contract_digest: [u8; 32],
    max_encoded_domain_bytes: usize,
    data_type: arrow::datatypes::DataType,
    publishers: BTreeMap<u32, FrontendRuntimeFilterFeedbackPublisherSlot>,
    unavailable: BTreeMap<u32, i32>,
    winner: Option<Vec<u8>>,
}

/// A query-owned feedback admission gate. It intentionally retains no
/// StateStore reference or process-global registration.
pub(crate) struct RuntimeFilterFeedbackState {
    execution_id: QueryExecutionId,
    state: (Mutex<FeedbackState>, Condvar),
}

impl RuntimeFilterFeedbackState {
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        declaration: FrontendRuntimeFilterFeedbackDeclaration,
    ) -> Result<Self, String> {
        let mut channels = BTreeMap::new();
        for channel in declaration.channels() {
            let Some(binding) = channel.scan_bindings().first() else {
                return Err("runtime filter feedback declaration has no scan binding".into());
            };
            if channel
                .scan_bindings()
                .iter()
                .any(|candidate| candidate.data_type != binding.data_type)
            {
                return Err(
                    "runtime filter feedback channel binds incompatible scan value types".into(),
                );
            }
            channels.insert(
                channel.channel_id(),
                ChannelState {
                    contract_digest: channel.contract_digest(),
                    max_encoded_domain_bytes: usize::try_from(channel.max_encoded_domain_bytes())
                        .map_err(
                        |_| "runtime filter feedback domain budget exceeds usize",
                    )?,
                    data_type: binding.data_type.clone(),
                    publishers: channel
                        .publishers()
                        .iter()
                        .copied()
                        .map(|slot| (slot.participant_id, slot))
                        .collect(),
                    unavailable: BTreeMap::new(),
                    winner: None,
                },
            );
        }
        Ok(Self {
            execution_id,
            state: (
                Mutex::new(FeedbackState {
                    closed: false,
                    channels,
                }),
                Condvar::new(),
            ),
        })
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.0.lock().expect("runtime filter feedback state");
        state.closed = true;
        self.state.1.notify_all();
    }

    /// Validates and admits one active-stream event. A foreign/retired event
    /// is ignored only when its execution identity is different; every event
    /// claiming this active attempt must match the frozen slot and contract.
    pub(crate) fn admit(
        &self,
        event: &QueryControlEvent,
        backend_process: novarocks_types::BackendProcessId,
        init_digest: &[u8; 32],
    ) -> Result<(), String> {
        use novarocks_proto_models::novarocks::query_control_response::Event;
        use novarocks_proto_models::novarocks::runtime_filter_feedback_event::TerminalOutcome;

        let Some(Event::RuntimeFilterFeedback(feedback)) = event.as_proto().event.as_ref() else {
            return Err("runtime filter feedback admission received a non-feedback event".into());
        };
        if feedback.execution_id.as_ref() != Some(&encode_query_execution_id(self.execution_id)) {
            return Ok(());
        }
        if feedback.init_digest.as_slice() != init_digest {
            return Err("runtime filter feedback init digest differs from active session".into());
        }
        if feedback.deployment_epoch != self.execution_id.attempt_id().get() {
            return Err(
                "runtime filter feedback deployment epoch differs from active attempt".into(),
            );
        }
        let process_matches = feedback
            .backend
            .as_ref()
            .and_then(|backend| backend.process_id.as_ref())
            .is_some_and(|process| process.value.as_slice() == backend_process.to_bytes());
        if !process_matches {
            return Err(
                "runtime filter feedback backend process differs from active session".into(),
            );
        }
        let mut state = self.state.0.lock().expect("runtime filter feedback state");
        if state.closed {
            return Ok(());
        }
        let channel = state
            .channels
            .get_mut(&feedback.channel_id)
            .ok_or("runtime filter feedback channel is not declared for this attempt")?;
        if feedback.contract_digest.as_slice() != channel.contract_digest {
            return Err("runtime filter feedback contract digest differs from declaration".into());
        }
        let slot = channel
            .publishers
            .get(&feedback.participant_id)
            .ok_or("runtime filter feedback publisher is not authorized")?;
        if slot.backend_process_id != backend_process {
            return Err(
                "runtime filter feedback publisher process differs from declaration".into(),
            );
        }
        match feedback.terminal_outcome.as_ref() {
            Some(TerminalOutcome::CanonicalDomain(encoded)) => {
                RuntimeFilterFeedbackDomain::decode(
                    encoded,
                    &channel.data_type,
                    channel.max_encoded_domain_bytes,
                )
                .map_err(|error| error.to_string())?;
                match &channel.winner {
                    Some(existing) if existing == encoded => Ok(()),
                    Some(_) => Err(
                        "runtime filter feedback terminal domain conflicts with first winner"
                            .into(),
                    ),
                    None => {
                        channel.winner = Some(encoded.clone());
                        self.state.1.notify_all();
                        Ok(())
                    }
                }
            }
            Some(TerminalOutcome::UnavailableReason(reason)) => {
                if channel.winner.is_none() {
                    channel.unavailable.insert(feedback.participant_id, *reason);
                    self.state.1.notify_all();
                }
                Ok(())
            }
            None => Err("runtime filter feedback terminal outcome is absent".into()),
        }
    }
}
