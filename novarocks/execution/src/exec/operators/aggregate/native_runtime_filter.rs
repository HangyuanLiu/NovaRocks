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
use std::sync::atomic::{AtomicBool, Ordering};

use crate::runtime_filter as execution;
use crate::runtime_filter::contribution::OrderedTuple;

use crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding;
use crate::exec::node::runtime_filter::RuntimeFilterExecutionContract;
use crate::exec::operators::aggregate::topn_boundary::AggregateTopNBoundaryBinding;

#[derive(Default)]
struct AggregateTopNProducerInstanceCoordinator {
    failed: AtomicBool,
}

#[derive(Clone)]
struct AggregateTopNProducerBinding {
    binding_id: u32,
    execution_contract: execution::RuntimeFilterProducerContract,
    session: execution::RuntimeFilterSessionRef,
    coordinator: Arc<AggregateTopNProducerInstanceCoordinator>,
}

impl AggregateTopNProducerBinding {
    fn from_plan(
        spec: &AggregateTopNRuntimeFilterProducerBinding,
        session: execution::RuntimeFilterSessionRef,
    ) -> Result<Self, String> {
        if !matches!(
            spec.contract().contract(),
            RuntimeFilterExecutionContract::Ordered(_)
        ) {
            return Err(format!(
                "native aggregate TopN producer binding_id={} requires an ordered contract",
                spec.binding_id()
            ));
        }
        if spec.contract().kind() != execution::RuntimeFilterProducerKind::OrderedBound {
            return Err(format!(
                "native aggregate TopN producer binding_id={} requires an ordered-bound producer contract",
                spec.binding_id()
            ));
        }
        Ok(Self {
            binding_id: spec.binding_id(),
            execution_contract: spec.contract().clone(),
            session,
            coordinator: Arc::new(AggregateTopNProducerInstanceCoordinator::default()),
        })
    }
}

pub(crate) struct AggregateTopNProducerSessionFactory {
    bindings: Vec<AggregateTopNProducerBinding>,
    local_partition_count: u32,
}

impl std::fmt::Debug for AggregateTopNProducerSessionFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AggregateTopNProducerSessionFactory")
            .field("binding_count", &self.bindings.len())
            .field("local_partition_count", &self.local_partition_count)
            .finish()
    }
}

impl AggregateTopNProducerSessionFactory {
    pub(crate) fn from_plan(
        specs: &[AggregateTopNRuntimeFilterProducerBinding],
        session: execution::RuntimeFilterSessionRef,
        local_partition_count: i32,
    ) -> Result<Self, String> {
        let local_partition_count = validate_partition_count(local_partition_count)?;
        let bindings = specs
            .iter()
            .map(|spec| AggregateTopNProducerBinding::from_plan(spec, Arc::clone(&session)))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            bindings,
            local_partition_count,
        })
    }

    pub(crate) const fn local_partition_count(&self) -> u32 {
        self.local_partition_count
    }

    pub(crate) fn create_for_driver(
        &self,
        actual_dop: i32,
        local_index: i32,
    ) -> Result<AggregateTopNProducerSession, String> {
        let actual_dop = validate_partition_count(actual_dop)?;
        if actual_dop != self.local_partition_count {
            return Err(format!(
                "native aggregate TopN producer DOP drifted between factory build and operator creation: expected={} actual={actual_dop}",
                self.local_partition_count
            ));
        }
        let local_index = u32::try_from(local_index).map_err(|_| format!(
            "native aggregate TopN producer local index {local_index} cannot be represented as a partition id"
        ))?;
        if local_index >= self.local_partition_count {
            return Err(format!(
                "native aggregate TopN producer local index {local_index} is outside DOP {}",
                self.local_partition_count
            ));
        }
        Ok(AggregateTopNProducerSession {
            streams: self
                .bindings
                .iter()
                .cloned()
                .map(|binding| {
                    AggregateTopNProducerStream::new(
                        binding,
                        execution::PartitionId::new(local_index),
                        self.local_partition_count,
                    )
                })
                .collect(),
            completed: false,
        })
    }
}

fn validate_partition_count(local_partition_count: i32) -> Result<u32, String> {
    let local_partition_count = u32::try_from(local_partition_count).map_err(|_| {
        format!(
            "native aggregate TopN producer DOP {local_partition_count} cannot be represented as a partition count"
        )
    })?;
    if local_partition_count == 0 {
        return Err("native aggregate TopN producer DOP must be positive".to_string());
    }
    Ok(local_partition_count)
}

pub(crate) struct AggregateTopNProducerSession {
    streams: Vec<AggregateTopNProducerStream>,
    completed: bool,
}

impl AggregateTopNProducerSession {
    pub(crate) fn bind(&mut self) -> Result<(), String> {
        for index in 0..self.streams.len() {
            if let Err(error) = self.streams[index].bind() {
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn submit_pending(
        &mut self,
        boundaries: &mut [AggregateTopNBoundaryBinding],
    ) -> Result<(), String> {
        if self.completed {
            return Ok(());
        }
        self.require_matching_boundaries(boundaries)?;
        for (index, boundary) in boundaries.iter_mut().enumerate() {
            let pending = boundary
                .state_mut()
                .take_pending_tightening()
                .map_err(|error| error.to_string())?;
            if let Some(bound) = pending
                && let Err(error) = self.streams[index].submit(bound)
            {
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                self.completed = true;
                return Err(error);
            }
        }
        Ok(())
    }

    pub(crate) fn finish(
        &mut self,
        boundaries: &mut [AggregateTopNBoundaryBinding],
    ) -> Result<(), String> {
        if self.completed {
            return Ok(());
        }
        self.require_matching_boundaries(boundaries)?;
        for (index, boundary) in boundaries.iter_mut().enumerate() {
            let pending = boundary
                .state_mut()
                .finish()
                .map_err(|error| error.to_string())?;
            if let Some(bound) = pending
                && let Err(error) = self.streams[index].submit(bound)
            {
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                self.completed = true;
                return Err(error);
            }
        }
        for index in 0..self.streams.len() {
            if let Err(error) = self.streams[index].close() {
                let _ =
                    self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
                self.completed = true;
                return Err(error);
            }
        }
        self.completed = true;
        Ok(())
    }

    pub(crate) fn fail(
        &mut self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<(), String> {
        if self.completed {
            return Ok(());
        }
        let result = self.fail_incomplete(reason);
        self.completed = true;
        result
    }

    fn require_matching_boundaries(
        &mut self,
        boundaries: &[AggregateTopNBoundaryBinding],
    ) -> Result<(), String> {
        if self.streams.len() == boundaries.len() {
            return Ok(());
        }
        let error = format!(
            "native aggregate TopN producer session/boundary count mismatch: sessions={} boundaries={}",
            self.streams.len(),
            boundaries.len()
        );
        let _ = self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
        self.completed = true;
        Err(error)
    }

    fn fail_incomplete(
        &mut self,
        reason: execution::RuntimeFilterProducerFailure,
    ) -> Result<(), String> {
        let mut first_error = None;
        for stream in &mut self.streams {
            if let Err(error) = stream.fail(reason)
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for AggregateTopNProducerSession {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.fail_incomplete(execution::RuntimeFilterProducerFailure::ExecutionFailed);
        }
    }
}

struct AggregateTopNProducerStream {
    binding: AggregateTopNProducerBinding,
    partition_id: execution::PartitionId,
    local_partition_count: u32,
    next_sequence: u64,
    terminal: bool,
    producer: Option<execution::RuntimeFilterProducerHandle>,
}

impl AggregateTopNProducerStream {
    fn new(
        binding: AggregateTopNProducerBinding,
        partition_id: execution::PartitionId,
        local_partition_count: u32,
    ) -> Self {
        Self {
            binding,
            partition_id,
            local_partition_count,
            next_sequence: 0,
            terminal: false,
            producer: None,
        }
    }

    fn bind(&mut self) -> Result<(), String> {
        if self.producer.is_some() || self.terminal {
            return Ok(());
        }
        let request = execution::RuntimeFilterProducerOpenRequest::new(
            self.binding.execution_contract.clone(),
            self.local_partition_count,
        );
        match self.binding.session.open_producer(request) {
            Ok(execution::RuntimeFilterBindOutcome::Bound(producer)) => {
                self.producer = Some(producer);
                Ok(())
            }
            Ok(execution::RuntimeFilterBindOutcome::Unavailable(_)) => {
                self.mark_service_unavailable();
                Ok(())
            }
            Err(error)
                if error.kind() == execution::RuntimeFilterContractViolationKind::SessionClosed =>
            {
                self.mark_service_unavailable();
                Ok(())
            }
            Err(error) => Err(format!(
                "native aggregate TopN producer binding_id={} open failed during operator bind: {error}",
                self.binding.binding_id
            )),
        }
    }

    fn submit(&mut self, bound: OrderedTuple) -> Result<(), String> {
        if self.terminal || self.binding.coordinator.failed.load(Ordering::Acquire) {
            self.terminal = true;
            return Ok(());
        }
        let producer = self.producer.as_ref().ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} was not bound before input",
                self.binding.binding_id
            )
        })?;
        let contribution = encode_execution_ordered_bound(
            &self.binding.execution_contract,
            &bound,
            producer.max_contribution_bytes(),
        ).map_err(|error| format!(
            "native aggregate TopN producer binding_id={} contribution encoding failed: {error}",
            self.binding.binding_id
        ))?;
        match producer.submit(
            self.partition_id,
            execution::ProducerSequence::new(self.next_sequence),
            contribution,
        ) {
            Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop) => {
                self.binding
                    .coordinator
                    .failed
                    .store(true, Ordering::Release);
                self.terminal = true;
                return Ok(());
            }
            Ok(_) => {}
            Err(error)
                if error.kind() == execution::RuntimeFilterContractViolationKind::SessionClosed =>
            {
                self.mark_service_unavailable();
                return Ok(());
            }
            Err(error) => {
                return Err(format!(
                    "native aggregate TopN producer binding_id={} contribution failed: {error}",
                    self.binding.binding_id
                ));
            }
        }
        self.next_sequence = self.next_sequence.checked_add(1).ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} producer sequence overflow",
                self.binding.binding_id
            )
        })?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        if self.terminal || self.binding.coordinator.failed.load(Ordering::Acquire) {
            self.terminal = true;
            return Ok(());
        }
        let producer = self.producer.as_ref().ok_or_else(|| {
            format!(
                "native aggregate TopN producer binding_id={} was not bound before finish",
                self.binding.binding_id
            )
        })?;
        match producer.close_partition(
            self.partition_id,
            execution::ProducerSequence::new(self.next_sequence),
        ) {
            Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop) => {
                self.binding
                    .coordinator
                    .failed
                    .store(true, Ordering::Release);
            }
            Ok(_) => {}
            Err(error)
                if error.kind() == execution::RuntimeFilterContractViolationKind::SessionClosed =>
            {
                self.mark_service_unavailable();
                return Ok(());
            }
            Err(error) => {
                return Err(format!(
                    "native aggregate TopN producer binding_id={} close failed: {error}",
                    self.binding.binding_id
                ));
            }
        }
        self.terminal = true;
        Ok(())
    }

    fn mark_service_unavailable(&mut self) {
        self.binding
            .coordinator
            .failed
            .store(true, Ordering::Release);
        self.terminal = true;
    }

    fn fail(&mut self, reason: execution::RuntimeFilterProducerFailure) -> Result<(), String> {
        self.terminal = true;
        if self
            .binding
            .coordinator
            .failed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }
        let Some(producer) = self.producer.as_ref() else {
            return Ok(());
        };
        if let Err(error) = producer.fail(reason)
            && error.kind() != execution::RuntimeFilterContractViolationKind::SessionClosed
        {
            return Err(format!(
                "native aggregate TopN producer binding_id={} fail-open failed: {error}",
                self.binding.binding_id
            ));
        }
        Ok(())
    }
}

fn encode_execution_ordered_bound(
    producer: &execution::RuntimeFilterProducerContract,
    bound: &OrderedTuple,
    max_contribution_bytes: usize,
) -> Result<execution::RuntimeFilterContribution, execution::contribution::ContributionCodecError> {
    let execution::RuntimeFilterExecutionContract::Ordered(contract) = producer.contract() else {
        return Err(execution::contribution::ContributionCodecError::SchemaMismatch);
    };
    let update = execution::contribution::OrderedBoundUpdate::try_new(contract, bound.clone())
        .map_err(|_| execution::contribution::ContributionCodecError::SchemaMismatch)?;
    let typed = execution::contribution::RuntimeFilterContribution::ordered_bound(update);
    let encoded = execution::contribution::encode_contribution(
        &typed,
        execution::contribution::ContributionCodecExpectation::OrderedBound(contract),
        max_contribution_bytes,
    )?;
    let (contract_digest, canonical_bytes) = encoded.into_parts();
    Ok(execution::RuntimeFilterContribution::new(
        execution::RuntimeFilterContributionKind::OrderedBound,
        contract_digest,
        canonical_bytes,
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::num::NonZeroU32;
    use std::sync::{Arc, Mutex};

    use arrow::datatypes::DataType;

    use super::{AggregateTopNProducerSessionFactory, execution};
    use crate::exec::expr::ExprId;
    use crate::exec::node::aggregate::AggregateTopNRuntimeFilterProducerBinding;
    use crate::exec::node::runtime_filter::RuntimeFilterExecutionContract;
    use crate::exec::operators::aggregate::topn_boundary::AggregateTopNBoundaryBinding;
    use crate::runtime_filter::contribution::{
        OrderedScalar, OrderedTuple, RuntimeOrderContract, RuntimeOrderKey, RuntimeOrderNullOrder,
        RuntimeOrderSortDirection,
    };

    #[derive(Clone, Debug, Eq, PartialEq)]
    enum Event {
        Open {
            binding_id: u32,
            local_partition_count: u32,
        },
        Submit {
            partition: u32,
            sequence: u64,
        },
        Close {
            partition: u32,
            sequence: u64,
        },
        Fail(execution::RuntimeFilterProducerFailure),
    }

    #[derive(Clone, Copy)]
    enum OpenAction {
        Bound,
        Unavailable,
        SessionClosed,
        Rejected,
    }

    #[derive(Clone, Copy)]
    enum SubmitAction {
        Applied,
        TerminalNoop,
    }

    struct RecordingState {
        events: Mutex<Vec<Event>>,
        open_actions: Mutex<VecDeque<OpenAction>>,
        submit_actions: Mutex<VecDeque<SubmitAction>>,
    }

    impl RecordingState {
        fn with_open_actions(open_actions: impl IntoIterator<Item = OpenAction>) -> Self {
            Self {
                events: Mutex::new(Vec::new()),
                open_actions: Mutex::new(open_actions.into_iter().collect()),
                submit_actions: Mutex::new(VecDeque::new()),
            }
        }

        fn events(&self) -> Vec<Event> {
            self.events.lock().expect("events lock").clone()
        }

        fn record(&self, event: Event) {
            self.events.lock().expect("events lock").push(event);
        }

        fn next_open_action(&self) -> OpenAction {
            self.open_actions
                .lock()
                .expect("open actions lock")
                .pop_front()
                .unwrap_or(OpenAction::Bound)
        }

        fn next_submit_action(&self) -> SubmitAction {
            self.submit_actions
                .lock()
                .expect("submit actions lock")
                .pop_front()
                .unwrap_or(SubmitAction::Applied)
        }
    }

    struct RecordingProducer {
        state: Arc<RecordingState>,
    }

    impl execution::RuntimeFilterProducer for RecordingProducer {
        fn max_contribution_bytes(&self) -> usize {
            1024
        }

        fn submit(
            &self,
            partition: execution::PartitionId,
            sequence: execution::ProducerSequence,
            _: execution::RuntimeFilterContribution,
        ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
        {
            self.state.record(Event::Submit {
                partition: partition.get(),
                sequence: sequence.get(),
            });
            Ok(match self.state.next_submit_action() {
                SubmitAction::Applied => execution::RuntimeFilterSubmitOutcome::Applied,
                SubmitAction::TerminalNoop => execution::RuntimeFilterSubmitOutcome::TerminalNoop,
            })
        }

        fn close_partition(
            &self,
            partition: execution::PartitionId,
            sequence: execution::ProducerSequence,
        ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
        {
            self.state.record(Event::Close {
                partition: partition.get(),
                sequence: sequence.get(),
            });
            Ok(execution::RuntimeFilterSubmitOutcome::Completed)
        }

        fn fail(
            &self,
            reason: execution::RuntimeFilterProducerFailure,
        ) -> Result<execution::RuntimeFilterSubmitOutcome, execution::RuntimeFilterContractViolation>
        {
            self.state.record(Event::Fail(reason));
            Ok(execution::RuntimeFilterSubmitOutcome::TerminalNoop)
        }
    }

    struct RecordingSession {
        state: Arc<RecordingState>,
    }

    impl execution::RuntimeFilterSession for RecordingSession {
        fn open_producer(
            &self,
            request: execution::RuntimeFilterProducerOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterProducerHandle>,
            execution::RuntimeFilterContractViolation,
        > {
            self.state.record(Event::Open {
                binding_id: request.contract().binding_id().get(),
                local_partition_count: request.local_partition_count(),
            });
            match self.state.next_open_action() {
                OpenAction::Bound => Ok(execution::RuntimeFilterBindOutcome::Bound(Arc::new(
                    RecordingProducer {
                        state: Arc::clone(&self.state),
                    },
                ))),
                OpenAction::Unavailable => Ok(execution::RuntimeFilterBindOutcome::Unavailable(
                    execution::UnavailableReason::ResourceLimit,
                )),
                OpenAction::SessionClosed => Err(execution::RuntimeFilterContractViolation::new(
                    execution::RuntimeFilterContractViolationKind::SessionClosed,
                    "recording session is closed",
                )),
                OpenAction::Rejected => Err(execution::RuntimeFilterContractViolation::new(
                    execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                    "recording session rejects this binding",
                )),
            }
        }

        fn subscribe(
            &self,
            _: execution::RuntimeFilterSubscriptionRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<execution::RuntimeFilterSubscriptionHandle>,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "recording producer session has no subscriptions",
            ))
        }

        fn open_final_domain_completion(
            &self,
            _: execution::RuntimeFilterFinalDomainOpenRequest,
        ) -> Result<
            execution::RuntimeFilterBindOutcome<
                execution::RuntimeFilterFinalDomainCompletionHandle,
            >,
            execution::RuntimeFilterContractViolation,
        > {
            Err(execution::RuntimeFilterContractViolation::new(
                execution::RuntimeFilterContractViolationKind::UnauthorizedBinding,
                "recording producer session has no final-domain completion",
            ))
        }
    }

    fn recording_session(state: Arc<RecordingState>) -> execution::RuntimeFilterSessionRef {
        Arc::new(RecordingSession { state })
    }

    fn ordered_contract() -> Arc<RuntimeOrderContract> {
        Arc::new(RuntimeOrderContract::from_frozen(
            [RuntimeOrderKey::with_order(
                DataType::Int64,
                RuntimeOrderSortDirection::Ascending,
                RuntimeOrderNullOrder::Last,
            )],
            [0; 32],
            [0; 32],
        ))
    }

    fn binding(binding_id: u32, limit: u32) -> AggregateTopNRuntimeFilterProducerBinding {
        let contract = ordered_contract();
        AggregateTopNRuntimeFilterProducerBinding::new(
            ExprId(binding_id as usize),
            0,
            NonZeroU32::new(limit).expect("non-zero TopN limit"),
            execution::RuntimeFilterProducerContract::ordered_bound(
                execution::RuntimeFilterBindingId::new(binding_id),
                execution::RuntimeFilterChannelId::new(binding_id + 100),
                RuntimeFilterExecutionContract::Ordered(contract),
            )
            .expect("ordered producer contract"),
        )
    }

    fn boundary(
        binding: &AggregateTopNRuntimeFilterProducerBinding,
        values: &[i64],
    ) -> AggregateTopNBoundaryBinding {
        let mut boundary = AggregateTopNBoundaryBinding::try_from_spec(binding)
            .expect("aggregate TopN boundary binding");
        let contract = Arc::clone(boundary.state().contract());
        for (group_id, value) in values.iter().copied().enumerate() {
            boundary
                .state_mut()
                .observe_new_group(
                    group_id,
                    OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(value))])
                        .expect("ordered candidate tuple"),
                )
                .expect("monotonic aggregate group id");
        }
        boundary
    }

    #[test]
    fn aggregate_topn_runtime_filter_forwards_dop_partitions_and_sequences() {
        let state = Arc::new(RecordingState::with_open_actions([]));
        let spec = binding(7, 2);
        let factory = AggregateTopNProducerSessionFactory::from_plan(
            &[spec.clone()],
            recording_session(Arc::clone(&state)),
            2,
        )
        .expect("factory");

        let mut first = factory.create_for_driver(2, 0).expect("first driver");
        let mut second = factory.create_for_driver(2, 1).expect("second driver");
        first.bind().expect("bind first driver");
        second.bind().expect("bind second driver");
        let mut first_boundaries = [boundary(&spec, &[9, 4])];
        let mut second_boundaries = [boundary(&spec, &[8, 3])];
        first
            .submit_pending(&mut first_boundaries)
            .expect("submit first boundary");
        second
            .submit_pending(&mut second_boundaries)
            .expect("submit second boundary");
        first
            .finish(&mut first_boundaries)
            .expect("finish first driver");
        second
            .finish(&mut second_boundaries)
            .expect("finish second driver");

        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 7,
                    local_partition_count: 2,
                },
                Event::Open {
                    binding_id: 7,
                    local_partition_count: 2,
                },
                Event::Submit {
                    partition: 0,
                    sequence: 0,
                },
                Event::Submit {
                    partition: 1,
                    sequence: 0,
                },
                Event::Close {
                    partition: 0,
                    sequence: 1,
                },
                Event::Close {
                    partition: 1,
                    sequence: 1,
                },
            ]
        );
    }

    #[test]
    fn aggregate_topn_runtime_filter_withholds_not_ready_and_flushes_final_tightening() {
        let state = Arc::new(RecordingState::with_open_actions([]));
        let spec = binding(8, 2);
        let factory = AggregateTopNProducerSessionFactory::from_plan(
            &[spec.clone()],
            recording_session(Arc::clone(&state)),
            1,
        )
        .expect("factory");
        let mut session = factory.create_for_driver(1, 0).expect("driver");
        session.bind().expect("bind");

        let mut boundary = boundary(&spec, &[5]);
        session
            .submit_pending(std::slice::from_mut(&mut boundary))
            .expect("not-ready boundary does not submit");
        let contract = Arc::clone(boundary.state().contract());
        boundary
            .state_mut()
            .observe_new_group(
                1,
                OrderedTuple::try_new(&contract, [Some(OrderedScalar::Int64(3))])
                    .expect("ordered candidate tuple"),
            )
            .expect("second group");
        session
            .finish(std::slice::from_mut(&mut boundary))
            .expect("finish flushes the first real tightening");

        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 8,
                    local_partition_count: 1,
                },
                Event::Submit {
                    partition: 0,
                    sequence: 0,
                },
                Event::Close {
                    partition: 0,
                    sequence: 1,
                },
            ]
        );
    }

    #[test]
    fn aggregate_topn_runtime_filter_rejects_dop_drift_and_invalid_partition() {
        let state = Arc::new(RecordingState::with_open_actions([]));
        let factory = AggregateTopNProducerSessionFactory::from_plan(
            &[binding(9, 1)],
            recording_session(state),
            2,
        )
        .expect("factory");

        assert_eq!(factory.local_partition_count(), 2);
        assert!(
            factory.create_for_driver(1, 0).is_err(),
            "DOP drift is rejected"
        );
        assert!(
            factory.create_for_driver(2, -1).is_err(),
            "negative local index is rejected"
        );
        assert!(
            factory.create_for_driver(2, 2).is_err(),
            "out-of-range index is rejected"
        );
    }

    #[test]
    fn aggregate_topn_runtime_filter_fails_open_for_unavailable_and_closed_session() {
        for action in [OpenAction::Unavailable, OpenAction::SessionClosed] {
            let state = Arc::new(RecordingState::with_open_actions([action]));
            let spec = binding(10, 1);
            let factory = AggregateTopNProducerSessionFactory::from_plan(
                &[spec.clone()],
                recording_session(Arc::clone(&state)),
                1,
            )
            .expect("factory");
            let mut session = factory.create_for_driver(1, 0).expect("driver");
            session.bind().expect("unavailable capability is fail-open");
            session
                .finish(&mut [boundary(&spec, &[1])])
                .expect("unavailable capability remains fail-open");
            assert_eq!(
                state.events(),
                vec![Event::Open {
                    binding_id: 10,
                    local_partition_count: 1,
                }]
            );
        }
    }

    #[test]
    fn aggregate_topn_runtime_filter_partial_bind_failure_fails_bound_stream_once() {
        let state = Arc::new(RecordingState::with_open_actions([
            OpenAction::Bound,
            OpenAction::Rejected,
        ]));
        let factory = AggregateTopNProducerSessionFactory::from_plan(
            &[binding(11, 1), binding(12, 1)],
            recording_session(Arc::clone(&state)),
            1,
        )
        .expect("factory");
        {
            let mut session = factory.create_for_driver(1, 0).expect("driver");
            assert!(
                session.bind().is_err(),
                "non-fail-open bind error reaches the operator"
            );
        }

        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 11,
                    local_partition_count: 1,
                },
                Event::Open {
                    binding_id: 12,
                    local_partition_count: 1,
                },
                Event::Fail(execution::RuntimeFilterProducerFailure::ExecutionFailed),
            ]
        );
    }

    #[test]
    fn aggregate_topn_runtime_filter_explicit_fail_and_drop_are_single_fail_open() {
        let state = Arc::new(RecordingState::with_open_actions([]));
        let factory = AggregateTopNProducerSessionFactory::from_plan(
            &[binding(13, 1)],
            recording_session(Arc::clone(&state)),
            1,
        )
        .expect("factory");
        {
            let mut first = factory.create_for_driver(1, 0).expect("first driver");
            let mut second = factory.create_for_driver(1, 0).expect("second driver");
            first.bind().expect("bind first driver");
            second.bind().expect("bind second driver");
            first
                .fail(execution::RuntimeFilterProducerFailure::Cancelled)
                .expect("explicit fail");
            second
                .fail(execution::RuntimeFilterProducerFailure::Cancelled)
                .expect("shared coordinator suppresses duplicate fail");
        }
        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 13,
                    local_partition_count: 1,
                },
                Event::Open {
                    binding_id: 13,
                    local_partition_count: 1,
                },
                Event::Fail(execution::RuntimeFilterProducerFailure::Cancelled),
            ]
        );

        let drop_state = Arc::new(RecordingState::with_open_actions([]));
        let drop_factory = AggregateTopNProducerSessionFactory::from_plan(
            &[binding(14, 1)],
            recording_session(Arc::clone(&drop_state)),
            1,
        )
        .expect("factory");
        {
            let mut session = drop_factory.create_for_driver(1, 0).expect("driver");
            session.bind().expect("bind");
        }
        assert_eq!(
            drop_state.events(),
            vec![
                Event::Open {
                    binding_id: 14,
                    local_partition_count: 1,
                },
                Event::Fail(execution::RuntimeFilterProducerFailure::ExecutionFailed),
            ]
        );
    }

    #[test]
    fn aggregate_topn_runtime_filter_terminal_noop_stops_submission_and_close() {
        let state = Arc::new(RecordingState::with_open_actions([]));
        state
            .submit_actions
            .lock()
            .expect("submit actions lock")
            .push_back(SubmitAction::TerminalNoop);
        let spec = binding(15, 1);
        let factory = AggregateTopNProducerSessionFactory::from_plan(
            &[spec.clone()],
            recording_session(Arc::clone(&state)),
            1,
        )
        .expect("factory");
        let mut session = factory.create_for_driver(1, 0).expect("driver");
        session.bind().expect("bind");
        session
            .submit_pending(&mut [boundary(&spec, &[1])])
            .expect("terminal noop is fail-open");
        session
            .finish(&mut [boundary(&spec, &[1])])
            .expect("terminal noop suppresses close");

        assert_eq!(
            state.events(),
            vec![
                Event::Open {
                    binding_id: 15,
                    local_partition_count: 1,
                },
                Event::Submit {
                    partition: 0,
                    sequence: 0,
                },
            ]
        );
    }
}
