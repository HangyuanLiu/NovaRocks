//! FE attempt-local admission state for terminal runtime-filter feedback.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Condvar, Mutex};

use novarocks_execution::runtime_filter::contribution::MembershipValues;
use novarocks_execution::runtime_filter::feedback_domain::RuntimeFilterFeedbackDomain;
use novarocks_proto_codec::lifecycle::{
    QueryControlEvent, QueryExecutionId, encode_query_execution_id,
};
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorReadColumnHandle, ConnectorReadDynamicFilterSnapshot, ConnectorValue, Domain,
    Range, TupleDomain, ValueSet,
};

use super::install_encoder::{
    FrontendRuntimeFilterFeedbackDeclaration, FrontendRuntimeFilterFeedbackPublisherSlot,
    FrontendRuntimeFilterFeedbackWaitEligibility,
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
    scan_bindings: BTreeSet<(i32, u32)>,
    wait_eligible: bool,
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
        Ok(Self {
            execution_id,
            state: (
                Mutex::new(FeedbackState {
                    closed: false,
                    channels: channel_states(declaration)?,
                }),
                Condvar::new(),
            ),
        })
    }

    /// The declaration is installed before the attempt's readers and split
    /// pump start.  Keeping the state object stable lets both owners share
    /// one attempt-local admission domain without any process-global lookup.
    pub(crate) fn configure(
        &self,
        declaration: FrontendRuntimeFilterFeedbackDeclaration,
    ) -> Result<(), String> {
        let mut state = self.state.0.lock().expect("runtime filter feedback state");
        if state.closed {
            return Err("cannot configure closed runtime filter feedback state".into());
        }
        state.channels = channel_states(declaration)?;
        self.state.1.notify_all();
        Ok(())
    }

    pub(crate) fn close(&self) {
        let mut state = self.state.0.lock().expect("runtime filter feedback state");
        state.closed = true;
        self.state.1.notify_all();
    }

    /// Snapshot the currently admitted domains for one connector scan.  A
    /// channel that is still pending (or that reported unavailable) widens to
    /// `all`; therefore this method is an optimization boundary and can never
    /// make a source omit a file merely because feedback was delayed.
    pub(crate) fn snapshot_for_scan(
        &self,
        plan_node_id: i32,
        bindings: &[(u32, ConnectorReadColumnHandle)],
    ) -> ConnectorReadDynamicFilterSnapshot {
        let state = self.state.0.lock().expect("runtime filter feedback state");
        if state.closed {
            return ConnectorReadDynamicFilterSnapshot::all_complete();
        }

        let mut domains: BTreeMap<ConnectorReadColumnHandle, Domain> = BTreeMap::new();
        let mut complete = true;
        for (binding_id, column) in bindings {
            let Some(channel) = state
                .channels
                .values()
                .find(|channel| channel.scan_bindings.contains(&(plan_node_id, *binding_id)))
            else {
                continue;
            };
            complete &= channel.is_terminal();
            let Some(encoded) = channel.winner.as_deref() else {
                continue;
            };
            let Ok(domain) = RuntimeFilterFeedbackDomain::decode(
                encoded,
                &channel.data_type,
                channel.max_encoded_domain_bytes,
            ) else {
                // Admission already rejects malformed feedback.  Retaining a
                // fail-open guard here keeps a future codec extension from
                // turning a split-planning optimization into a correctness
                // risk.
                continue;
            };
            let Some(domain) = connector_domain(&domain) else {
                continue;
            };
            match domains.remove(column) {
                Some(existing) => match existing.intersect(&domain) {
                    Ok(intersection) => {
                        domains.insert(column.clone(), intersection);
                    }
                    Err(_) => {
                        // A type mismatch cannot be a pruning authority.
                    }
                },
                None => {
                    domains.insert(column.clone(), domain);
                }
            }
        }
        let predicate = TupleDomain::with_column_domains(domains)
            .expect("feedback declarations bind at most the connector tuple-domain limit");
        ConnectorReadDynamicFilterSnapshot::new(predicate, complete)
    }

    pub(crate) fn is_initial_wait_eligible(
        &self,
        plan_node_id: i32,
        bindings: &[(u32, ConnectorReadColumnHandle)],
    ) -> bool {
        let state = self.state.0.lock().expect("runtime filter feedback state");
        bindings.iter().any(|(binding_id, _)| {
            state.channels.values().any(|channel| {
                channel.scan_bindings.contains(&(plan_node_id, *binding_id))
                    && channel.wait_eligible
                    && !channel.is_terminal()
            })
        })
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

fn channel_states(
    declaration: FrontendRuntimeFilterFeedbackDeclaration,
) -> Result<BTreeMap<u32, ChannelState>, String> {
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
                    .map_err(|_| "runtime filter feedback domain budget exceeds usize")?,
                data_type: binding.data_type.clone(),
                publishers: channel
                    .publishers()
                    .iter()
                    .copied()
                    .map(|slot| (slot.participant_id, slot))
                    .collect(),
                scan_bindings: channel
                    .scan_bindings()
                    .iter()
                    .map(|binding| (binding.plan_node_id, binding.binding_id))
                    .collect(),
                wait_eligible: matches!(
                    channel.wait_eligibility(),
                    FrontendRuntimeFilterFeedbackWaitEligibility::Eligible
                ),
                unavailable: BTreeMap::new(),
                winner: None,
            },
        );
    }
    Ok(channels)
}

impl ChannelState {
    fn is_terminal(&self) -> bool {
        self.winner.is_some() || self.unavailable.len() == self.publishers.len()
    }
}

/// Convert only exact, lossless representations into the Trino-style SPI
/// domain.  Any unsupported value, range, or resource shape becomes `None`,
/// which the caller treats as unconstrained.
fn connector_domain(feedback: &RuntimeFilterFeedbackDomain) -> Option<Domain> {
    match feedback {
        RuntimeFilterFeedbackDomain::All => None,
        RuntimeFilterFeedbackDomain::Exact(values) => {
            let values = connector_values(values)?;
            let value_type = values.first()?.value_type();
            let values = ValueSet::of_values(value_type, values).ok()?;
            Some(Domain::new(values, values_contains_null(feedback)))
        }
        RuntimeFilterFeedbackDomain::EnclosingRange {
            lower,
            upper,
            contains_null,
        } => {
            let mut lower = connector_values(lower)?;
            let mut upper = connector_values(upper)?;
            let [lower] = lower.as_mut_slice() else {
                return None;
            };
            let [upper] = upper.as_mut_slice() else {
                return None;
            };
            if lower.value_type() != upper.value_type() {
                return None;
            }
            let value_type = lower.value_type();
            let range = Range::try_new(
                value_type,
                Bound::Inclusive(lower.clone()),
                Bound::Inclusive(upper.clone()),
            )
            .ok()?;
            let values = ValueSet::of_ranges(value_type, vec![range]).ok()?;
            Some(Domain::new(values, *contains_null))
        }
    }
}

fn values_contains_null(feedback: &RuntimeFilterFeedbackDomain) -> bool {
    match feedback {
        RuntimeFilterFeedbackDomain::Exact(values) => values.contains_null(),
        RuntimeFilterFeedbackDomain::EnclosingRange { contains_null, .. } => *contains_null,
        RuntimeFilterFeedbackDomain::All => true,
    }
}

fn connector_values(
    values: &novarocks_execution::runtime_filter::contribution::ValueDomainDelta,
) -> Option<Vec<ConnectorValue>> {
    let values = match values.values() {
        MembershipValues::Boolean(values) => values
            .iter()
            .copied()
            .map(ConnectorValue::Boolean)
            .collect(),
        MembershipValues::Int8(values) => values
            .iter()
            .copied()
            .map(ConnectorValue::TinyInt)
            .collect(),
        MembershipValues::Int32(values) => values
            .iter()
            .copied()
            .map(ConnectorValue::Integer)
            .collect(),
        MembershipValues::Int64(values) => {
            values.iter().copied().map(ConnectorValue::BigInt).collect()
        }
        MembershipValues::Float32(values) => values
            .iter()
            .map(|value| f32::from_bits(value.bits()))
            .map(ConnectorValue::Real)
            .collect(),
        MembershipValues::Float64(values) => values
            .iter()
            .map(|value| f64::from_bits(value.bits()))
            .map(ConnectorValue::Double)
            .collect(),
        MembershipValues::Utf8(values) => values
            .iter()
            .cloned()
            .map(Into::into)
            .map(ConnectorValue::Varchar)
            .collect(),
        MembershipValues::Date32(values) => {
            values.iter().copied().map(ConnectorValue::Date).collect()
        }
        MembershipValues::Timestamp {
            unit,
            timezone,
            values,
        } => match (unit, timezone.as_deref()) {
            (arrow::datatypes::TimeUnit::Microsecond, None) => values
                .iter()
                .copied()
                .map(ConnectorValue::TimestampMicros)
                .collect(),
            (arrow::datatypes::TimeUnit::Nanosecond, None) => values
                .iter()
                .copied()
                .map(ConnectorValue::TimestampNanos)
                .collect(),
            (arrow::datatypes::TimeUnit::Microsecond, Some(zone))
                if zone.eq_ignore_ascii_case("UTC") =>
            {
                values
                    .iter()
                    .copied()
                    .map(ConnectorValue::TimestampTzMicros)
                    .collect()
            }
            (arrow::datatypes::TimeUnit::Nanosecond, Some(zone))
                if zone.eq_ignore_ascii_case("UTC") =>
            {
                values
                    .iter()
                    .copied()
                    .map(ConnectorValue::TimestampTzNanos)
                    .collect()
            }
            _ => return None,
        },
        MembershipValues::Decimal128 {
            precision,
            scale,
            values,
        } => values
            .iter()
            .map(|value| ConnectorValue::try_decimal(*value, *precision, *scale).ok())
            .collect::<Option<Vec<_>>>()?,
        MembershipValues::LargeInt(values) => values
            .iter()
            .map(|value| ConnectorValue::Fixed(value.to_be_bytes().into()))
            .collect(),
        // The declaration may support these FE values, but the connector SPI
        // has no equal-width predicate representation for them.
        MembershipValues::Int16(_) => return None,
    };
    if values.iter().any(|value| {
        matches!(value, ConnectorValue::Real(value) if value.is_nan())
            || matches!(value, ConnectorValue::Double(value) if value.is_nan())
    }) {
        return None;
    }
    Some(values)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use novarocks_execution::runtime_filter::contribution::ValueDomainDelta;
    use novarocks_proto_codec::lifecycle::{AttemptId, encode_query_execution_id};
    use novarocks_proto_models::novarocks;
    use novarocks_types::{BackendProcessId, QueryId};

    use super::super::install_encoder::{
        FrontendRuntimeFilterFeedbackChannel, FrontendRuntimeFilterFeedbackPublisherOwner,
        FrontendRuntimeFilterFeedbackScanBinding,
    };
    use super::*;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(11, 12),
            AttemptId::new(3).expect("valid attempt"),
        )
        .expect("valid execution id")
    }

    fn declaration(process: BackendProcessId) -> FrontendRuntimeFilterFeedbackDeclaration {
        FrontendRuntimeFilterFeedbackDeclaration::new([FrontendRuntimeFilterFeedbackChannel {
            channel_id: 7,
            contract_digest: [9; 32],
            max_encoded_domain_bytes: 64 * 1024,
            publishers: vec![FrontendRuntimeFilterFeedbackPublisherSlot {
                participant_id: 5,
                backend_process_id: process,
                owner: FrontendRuntimeFilterFeedbackPublisherOwner::Aggregator,
            }],
            scan_bindings: vec![FrontendRuntimeFilterFeedbackScanBinding {
                fragment_id: 2,
                plan_node_id: 17,
                binding_id: 7,
                data_type: DataType::Int64,
                nullable: false,
            }],
            wait_eligibility: FrontendRuntimeFilterFeedbackWaitEligibility::Eligible,
        }])
        .expect("valid declaration")
    }

    fn event(
        execution_id: QueryExecutionId,
        process: BackendProcessId,
        encoded: Vec<u8>,
    ) -> QueryControlEvent {
        QueryControlEvent::parse(novarocks::QueryControlResponse {
            event: Some(
                novarocks::query_control_response::Event::RuntimeFilterFeedback(
                    novarocks::RuntimeFilterFeedbackEvent {
                        execution_id: Some(encode_query_execution_id(execution_id)),
                        init_digest: vec![4; 32],
                        backend: Some(novarocks::ParticipantBackendIdentity {
                            endpoint: Some(novarocks::QueryControlEndpoint {
                                host: "127.0.0.1".into(),
                                port: 9030,
                            }),
                            process_id: Some(novarocks::BackendProcessId {
                                value: process.to_bytes().to_vec(),
                            }),
                        }),
                        participant_id: 5,
                        deployment_epoch: execution_id.attempt_id().get(),
                        channel_id: 7,
                        contract_digest: vec![9; 32],
                        terminal_outcome: Some(
                            novarocks::runtime_filter_feedback_event::TerminalOutcome::CanonicalDomain(
                                encoded,
                            ),
                        ),
                    },
                ),
            ),
        })
        .expect("valid event")
    }

    fn exact(value: i64) -> Vec<u8> {
        RuntimeFilterFeedbackDomain::Exact(ValueDomainDelta::new(
            MembershipValues::int64([value]),
            false,
        ))
        .encode(64 * 1024)
        .expect("canonical domain")
    }

    #[test]
    fn admits_only_the_first_authorized_terminal_domain_for_the_active_attempt() {
        let execution_id = execution_id();
        let process = BackendProcessId::new_v7();
        let state = RuntimeFilterFeedbackState::new(execution_id, declaration(process))
            .expect("feedback state");
        let first = exact(41);

        state
            .admit(
                &event(execution_id, process, first.clone()),
                process,
                &[4; 32],
            )
            .expect("first terminal domain is admitted");
        state
            .admit(
                &event(execution_id, process, first.clone()),
                process,
                &[4; 32],
            )
            .expect("identical duplicate is idempotent");
        let conflict = state
            .admit(&event(execution_id, process, exact(42)), process, &[4; 32])
            .expect_err("a distinct terminal domain cannot replace the winner");
        assert!(conflict.contains("conflicts with first winner"));

        let state = state.state.0.lock().expect("feedback state");
        assert_eq!(state.channels[&7].winner.as_deref(), Some(first.as_slice()));
        assert!(state.channels[&7].is_terminal());
    }

    #[test]
    fn ignores_a_retired_attempt_before_authorizing_any_slot() {
        let execution_id = execution_id();
        let process = BackendProcessId::new_v7();
        let state = RuntimeFilterFeedbackState::new(execution_id, declaration(process))
            .expect("feedback state");
        let retired = QueryExecutionId::new(
            QueryId::new(11, 12),
            AttemptId::new(2).expect("valid attempt"),
        )
        .expect("valid execution id");

        state
            .admit(&event(retired, process, exact(41)), process, &[4; 32])
            .expect("retired event is ignored");
        let state = state.state.0.lock().expect("feedback state");
        assert!(state.channels[&7].winner.is_none());
    }
}
