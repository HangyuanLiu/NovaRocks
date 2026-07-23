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

use std::collections::BTreeSet;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::runtime_filter::{
    NativeRuntimeFilterConsumerSpec, NativeRuntimeFilterContract, NativeRuntimeFilterReduction,
};
use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::profile::{
    OperatorProfiles, ProfileUnit, RUNTIME_FILTER_INPUT_ROWS, RUNTIME_FILTER_OUTPUT_ROWS,
};
use crate::runtime::runtime_state::RuntimeState;
use crate::runtime_filter::exec::membership_predicate::{
    MembershipPredicateContract, NativeRuntimeFilterPredicate, PredicateEvaluationError,
};
use crate::runtime_filter::model::contract::{
    ArtifactCapability, BindingId, ChannelId, ConsumerActivation, RuntimeFilterLifecycle,
};
use crate::runtime_filter::port::artifact::{
    ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
};
use crate::runtime_filter::port::identity::LogicalVersion;
use crate::runtime_filter::port::producer::RuntimeContractViolationKind;
use crate::runtime_filter::port::subscription::{
    ArtifactAcquireOutcome, BlockingSnapshotSubscription, SubscriptionKind,
};
use arrow::compute::filter_record_batch;

#[derive(Clone)]
pub(crate) struct NativeRuntimeFilterConsumerSet {
    inner: Arc<NativeConsumerInner>,
}

struct NativeConsumerInner {
    arena: Arc<ExprArena>,
    bindings: Mutex<Vec<NativeConsumerBinding>>,
    acquire_phase: Mutex<NativeConsumerAcquirePhase>,
    acquire_ready: Condvar,
    wait_timeout: Mutex<Duration>,
}

enum NativeConsumerAcquirePhase {
    Pending,
    Acquiring,
    Complete,
    Failed(String),
}

struct NativeConsumerBinding {
    spec: NativeRuntimeFilterConsumerSpec,
    state: NativeConsumerBindingState,
}

enum NativeConsumerBindingState {
    Unbound,
    Bound(Arc<dyn BlockingSnapshotSubscription>),
    Acquiring,
    Active(NativeRuntimeFilterPredicate),
    PassThrough,
}

impl NativeRuntimeFilterConsumerSet {
    pub(crate) fn from_plan(
        specs: &[NativeRuntimeFilterConsumerSpec],
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        validate_plan_specs(specs, &arena)?;
        Ok(Self {
            inner: Arc::new(NativeConsumerInner {
                arena,
                bindings: Mutex::new(
                    specs
                        .iter()
                        .cloned()
                        .map(|spec| NativeConsumerBinding {
                            spec,
                            state: NativeConsumerBindingState::Unbound,
                        })
                        .collect(),
                ),
                acquire_phase: Mutex::new(NativeConsumerAcquirePhase::Pending),
                acquire_ready: Condvar::new(),
                wait_timeout: Mutex::new(Duration::from_secs(1)),
            }),
        })
    }

    #[cfg(test)]
    fn from_bound_for_test(
        specs: Vec<NativeRuntimeFilterConsumerSpec>,
        arena: Arc<ExprArena>,
        subscriptions: Vec<Arc<dyn BlockingSnapshotSubscription>>,
    ) -> Self {
        validate_plan_specs(&specs, &arena).unwrap();
        assert_eq!(specs.len(), subscriptions.len());
        let bindings = specs
            .into_iter()
            .zip(subscriptions)
            .map(|(spec, subscription)| NativeConsumerBinding {
                spec,
                state: NativeConsumerBindingState::Bound(subscription),
            })
            .collect();
        Self {
            inner: Arc::new(NativeConsumerInner {
                arena,
                bindings: Mutex::new(bindings),
                acquire_phase: Mutex::new(NativeConsumerAcquirePhase::Pending),
                acquire_ready: Condvar::new(),
                wait_timeout: Mutex::new(Duration::from_secs(1)),
            }),
        }
    }

    pub(crate) fn bind(&self, state: &RuntimeState) -> Result<(), String> {
        *self
            .inner
            .wait_timeout
            .lock()
            .expect("native RF timeout lock") = state
            .runtime_filter_wait_timeout()
            .unwrap_or(Duration::from_secs(1));
        let mut bindings = self.inner.bindings.lock().expect("native RF consumer lock");
        if bindings
            .iter()
            .all(|binding| !matches!(binding.state, NativeConsumerBindingState::Unbound))
        {
            return Ok(());
        }
        let Some(context) = state.native_runtime_filter_context() else {
            if bindings.is_empty() {
                return Ok(());
            }
            return Err(
                "native runtime-filter consumers require an installed execution context".into(),
            );
        };
        for binding in bindings.iter_mut() {
            if !matches!(binding.state, NativeConsumerBindingState::Unbound) {
                continue;
            }
            let resolved = match context.resolve_consumer(
                BindingId::new(binding.spec.binding_id),
                ChannelId::new(binding.spec.channel_id),
                SubscriptionKind::BlockingSnapshot,
            ) {
                Ok(resolved) => resolved,
                Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                    binding.state = NativeConsumerBindingState::PassThrough;
                    continue;
                }
                Err(error) => return Err(error.to_string()),
            };
            validate_resolved_consumer(&binding.spec, &resolved)?;
            match resolved.subscribe() {
                Ok(handle) => {
                    binding.state = NativeConsumerBindingState::Bound(
                        handle.into_blocking().map_err(|error| error.to_string())?,
                    );
                }
                Err(error) if error.kind() == RuntimeContractViolationKind::ServiceUnavailable => {
                    binding.state = NativeConsumerBindingState::PassThrough;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
        Ok(())
    }

    pub(crate) fn acquire_blocking(&self, timeout: Duration) -> Result<(), String> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        let mut phase = self
            .inner
            .acquire_phase
            .lock()
            .expect("native RF acquire phase lock");
        loop {
            match &*phase {
                NativeConsumerAcquirePhase::Pending => {
                    *phase = NativeConsumerAcquirePhase::Acquiring;
                    break;
                }
                NativeConsumerAcquirePhase::Acquiring => {
                    phase = self
                        .inner
                        .acquire_ready
                        .wait(phase)
                        .expect("native RF acquire phase lock");
                }
                NativeConsumerAcquirePhase::Complete => return Ok(()),
                NativeConsumerAcquirePhase::Failed(error) => return Err(error.clone()),
            }
        }
        drop(phase);

        let result = self.acquire_once(deadline);
        let mut phase = self
            .inner
            .acquire_phase
            .lock()
            .expect("native RF acquire phase lock");
        *phase = match &result {
            Ok(()) => NativeConsumerAcquirePhase::Complete,
            Err(error) => NativeConsumerAcquirePhase::Failed(error.clone()),
        };
        self.inner.acquire_ready.notify_all();
        result
    }

    fn acquire_once(&self, deadline: Instant) -> Result<(), String> {
        let pending = {
            let mut bindings = self.inner.bindings.lock().expect("native RF consumer lock");
            bindings
                .iter_mut()
                .enumerate()
                .filter_map(|(index, binding)| {
                    let state = std::mem::replace(
                        &mut binding.state,
                        NativeConsumerBindingState::Acquiring,
                    );
                    match state {
                        NativeConsumerBindingState::Bound(subscription) => {
                            Some((index, binding.spec.clone(), subscription))
                        }
                        state => {
                            binding.state = state;
                            None
                        }
                    }
                })
                .collect::<Vec<_>>()
        };

        for (index, spec, subscription) in pending {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let outcome = subscription.acquire(remaining);
            let state = match outcome {
                ArtifactAcquireOutcome::Published(bundle) => {
                    let contract = membership_predicate_contract(&spec)?;
                    NativeConsumerBindingState::Active(
                        NativeRuntimeFilterPredicate::compile(&bundle, &contract)
                            .map_err(|error| error.to_string())?,
                    )
                }
                ArtifactAcquireOutcome::Unsupported(_)
                | ArtifactAcquireOutcome::Unavailable(_)
                | ArtifactAcquireOutcome::Cancelled
                | ArtifactAcquireOutcome::TimedOut => NativeConsumerBindingState::PassThrough,
            };
            self.inner.bindings.lock().expect("native RF consumer lock")[index].state = state;
        }
        Ok(())
    }

    pub(crate) fn acquire_configured(&self) -> Result<(), String> {
        let timeout = *self
            .inner
            .wait_timeout
            .lock()
            .expect("native RF timeout lock");
        self.acquire_blocking(timeout)
    }

    pub(crate) fn set_wait_timeout(&self, timeout: Duration) {
        *self
            .inner
            .wait_timeout
            .lock()
            .expect("native RF timeout lock") = timeout;
    }

    pub(crate) fn apply_chunk(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        self.apply_chunk_profiled(chunk, None)
    }

    pub(crate) fn apply_chunk_profiled(
        &self,
        chunk: Chunk,
        profiles: Option<&OperatorProfiles>,
    ) -> Result<Option<Chunk>, String> {
        let configured = !self
            .inner
            .bindings
            .lock()
            .expect("native RF consumer lock")
            .is_empty();
        let input_rows = i64::try_from(chunk.len()).unwrap_or(i64::MAX);
        let output = self.apply_chunk_inner(chunk)?;
        if configured && let Some(profiles) = profiles {
            profiles
                .common
                .counter_add(RUNTIME_FILTER_INPUT_ROWS, ProfileUnit::Unit, input_rows);
            profiles.common.counter_add(
                RUNTIME_FILTER_OUTPUT_ROWS,
                ProfileUnit::Unit,
                output
                    .as_ref()
                    .map_or(0, |chunk| i64::try_from(chunk.len()).unwrap_or(i64::MAX)),
            );
        }
        Ok(output)
    }

    fn apply_chunk_inner(&self, chunk: Chunk) -> Result<Option<Chunk>, String> {
        let active = {
            let bindings = self.inner.bindings.lock().expect("native RF consumer lock");
            if bindings.iter().any(|binding| {
                matches!(
                    binding.state,
                    NativeConsumerBindingState::Unbound
                        | NativeConsumerBindingState::Bound(_)
                        | NativeConsumerBindingState::Acquiring
                )
            }) {
                return Err("native runtime-filter consumers must acquire before apply".into());
            }
            bindings
                .iter()
                .enumerate()
                .filter_map(|(index, binding)| match &binding.state {
                    NativeConsumerBindingState::Active(predicate) => {
                        Some((index, binding.spec.expr_id, predicate.clone()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        if active.is_empty() {
            return Ok(Some(chunk));
        }
        let chunk = crate::exec::chunk::hydrate_dictionary_columns_except(&chunk, |_, _| false)?;
        let mut current = Some(chunk);
        for (index, expr_id, predicate) in active {
            let Some(input) = current else {
                return Ok(None);
            };
            let array = self.inner.arena.eval(expr_id, &input)?;
            let mask = match predicate.evaluate(array.as_ref()) {
                Ok(mask) => mask,
                Err(PredicateEvaluationError::ResourceUnavailable) => {
                    self.inner.bindings.lock().expect("native RF consumer lock")[index].state =
                        NativeConsumerBindingState::PassThrough;
                    current = Some(input);
                    continue;
                }
                Err(error) => return Err(error.to_string()),
            };
            if mask.iter().all(|value| value == Some(true)) {
                current = Some(input);
            } else if mask.iter().all(|value| value != Some(true)) {
                current = None;
            } else {
                let filtered =
                    filter_record_batch(&input.batch, &mask).map_err(|e| e.to_string())?;
                current = Some(Chunk::try_new_like(filtered, &input)?);
            }
        }
        Ok(current)
    }
}

fn join_profile() -> Result<ConsumerArtifactProfile, String> {
    ConsumerArtifactProfile::new(
        BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
        None,
    )
    .map_err(|error| format!("invalid native Join runtime-filter profile: {error:?}"))
}

fn validate_plan_specs(
    specs: &[NativeRuntimeFilterConsumerSpec],
    arena: &ExprArena,
) -> Result<(), String> {
    let mut bindings = BTreeSet::new();
    for spec in specs {
        if !bindings.insert(spec.binding_id) {
            return Err(format!(
                "duplicate native runtime-filter consumer binding_id={}",
                spec.binding_id
            ));
        }
        if spec.activation != ConsumerActivation::BlockingSnapshot {
            return Err(format!(
                "native Join runtime-filter binding_id={} requires BlockingSnapshot",
                spec.binding_id
            ));
        }
        if spec.capabilities
            != BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ])
        {
            return Err(format!(
                "native Join runtime-filter binding_id={} has an unsupported artifact capability profile",
                spec.binding_id
            ));
        }
        if spec.reduction != NativeRuntimeFilterReduction::SetUnion {
            return Err(format!(
                "native Join runtime-filter binding_id={} requires SetUnion",
                spec.binding_id
            ));
        }
        membership_predicate_contract_with_arena(spec, arena)?;
    }
    Ok(())
}

fn membership_predicate_contract_with_arena(
    spec: &NativeRuntimeFilterConsumerSpec,
    arena: &ExprArena,
) -> Result<MembershipPredicateContract, String> {
    let NativeRuntimeFilterContract::Membership {
        canonical_schema,
        schema_digest,
    } = &spec.contract
    else {
        return Err(format!(
            "native Join runtime-filter binding_id={} requires a Membership contract",
            spec.binding_id
        ));
    };
    let view = ArtifactMembershipSchema::view(canonical_schema)
        .map_err(|error| format!("invalid native membership schema: {error:?}"))?;
    let data_type = arena
        .data_type(spec.expr_id)
        .ok_or_else(|| {
            format!(
                "native runtime-filter expression {:?} is missing",
                spec.expr_id
            )
        })?
        .clone();
    let rebuilt = ArtifactMembershipSchema::new(&data_type, view.null_semantics())
        .map_err(|error| format!("invalid native membership expression type: {error:?}"))?;
    if rebuilt.canonical_bytes() != canonical_schema.as_ref()
        || rebuilt.digest().bytes() != *schema_digest
    {
        return Err(format!(
            "native runtime-filter binding_id={} expression type/null contract does not match its canonical schema",
            spec.binding_id
        ));
    }
    MembershipPredicateContract::join(
        ChannelId::new(spec.channel_id),
        data_type,
        view.null_semantics(),
        LogicalVersion::FIRST,
    )
    .map_err(|error| format!("invalid native Join predicate contract: {error:?}"))
}

fn membership_predicate_contract(
    spec: &NativeRuntimeFilterConsumerSpec,
) -> Result<MembershipPredicateContract, String> {
    let NativeRuntimeFilterContract::Membership {
        canonical_schema, ..
    } = &spec.contract
    else {
        unreachable!("plan validation accepts only Membership")
    };
    let view = ArtifactMembershipSchema::view(canonical_schema)
        .map_err(|error| format!("invalid native membership schema: {error:?}"))?;
    let data_type = data_type_from_schema_view(view)?;
    MembershipPredicateContract::join(
        ChannelId::new(spec.channel_id),
        data_type,
        view.null_semantics(),
        LogicalVersion::FIRST,
    )
    .map_err(|error| format!("invalid native Join predicate contract: {error:?}"))
}

fn data_type_from_schema_view(
    view: crate::runtime_filter::port::artifact::ArtifactMembershipSchemaView<'_>,
) -> Result<arrow::datatypes::DataType, String> {
    use arrow::datatypes::{DataType, TimeUnit};
    Ok(match view.payload_tag() {
        1 => DataType::Boolean,
        2 => DataType::Int8,
        3 => DataType::Int16,
        4 => DataType::Int32,
        5 => DataType::Int64,
        6 => DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
        7 => DataType::Float32,
        8 => DataType::Float64,
        9 => DataType::Utf8,
        10 => DataType::Date32,
        11 => {
            let (unit, timezone) = view
                .timestamp_contract()
                .ok_or_else(|| "missing timestamp membership contract".to_string())?;
            let unit = match unit {
                1 => TimeUnit::Second,
                2 => TimeUnit::Millisecond,
                3 => TimeUnit::Microsecond,
                4 => TimeUnit::Nanosecond,
                _ => return Err("invalid timestamp membership unit".into()),
            };
            DataType::Timestamp(unit, timezone.map(Arc::<str>::from))
        }
        12 => {
            let (precision, scale) = view
                .decimal_contract()
                .ok_or_else(|| "missing decimal membership contract".to_string())?;
            DataType::Decimal128(precision, scale)
        }
        _ => return Err("unsupported membership schema tag".into()),
    })
}

fn validate_resolved_consumer(
    spec: &NativeRuntimeFilterConsumerSpec,
    resolved: &crate::runtime_filter::service::ResolvedNativeConsumer,
) -> Result<(), String> {
    if resolved.activation() != ConsumerActivation::BlockingSnapshot
        || resolved.lifecycle() != RuntimeFilterLifecycle::CompleteOnce
        || resolved.capabilities() != &spec.capabilities
        || resolved.artifact_profile() != &join_profile()?
        || resolved.reduction_requirement()
            != crate::runtime_filter::model::contract::ReductionRequirement::SetUnion
    {
        return Err(format!(
            "native runtime-filter binding_id={} installed lifecycle/profile contract mismatch",
            spec.binding_id
        ));
    }
    match (&spec.contract, resolved.contract()) {
        (
            NativeRuntimeFilterContract::Membership {
                canonical_schema,
                schema_digest,
            },
            crate::runtime_filter::service::InstalledNativeRuntimeFilterContract::Membership {
                canonical_schema: installed_schema,
                schema_digest: installed_digest,
            },
        ) if canonical_schema == installed_schema && schema_digest == installed_digest => Ok(()),
        _ => Err(format!(
            "native runtime-filter binding_id={} installed membership contract mismatch",
            spec.binding_id
        )),
    }
}

pub(crate) struct NativeRuntimeFilterProcessorFactory {
    name: String,
    consumers: NativeRuntimeFilterConsumerSet,
}

impl NativeRuntimeFilterProcessorFactory {
    pub(crate) fn new(
        owner_node_id: i32,
        specs: &[NativeRuntimeFilterConsumerSpec],
        arena: Arc<ExprArena>,
    ) -> Result<Self, String> {
        Ok(Self {
            name: format!("NativeRuntimeFilter (id={owner_node_id})"),
            consumers: NativeRuntimeFilterConsumerSet::from_plan(specs, arena)?,
        })
    }
}

impl OperatorFactory for NativeRuntimeFilterProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(NativeRuntimeFilterProcessor {
            name: self.name.clone(),
            consumers: self.consumers.clone(),
            output: None,
            finishing: false,
            profiles: None,
        })
    }
}

struct NativeRuntimeFilterProcessor {
    name: String,
    consumers: NativeRuntimeFilterConsumerSet,
    output: Option<Chunk>,
    finishing: bool,
    profiles: Option<OperatorProfiles>,
}

impl Operator for NativeRuntimeFilterProcessor {
    fn name(&self) -> &str {
        &self.name
    }

    fn set_profiles(&mut self, profiles: OperatorProfiles) {
        self.profiles = Some(profiles);
    }

    fn bind_runtime_state(&mut self, state: &RuntimeState) -> Result<(), String> {
        self.consumers.bind(state)
    }

    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }

    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }

    fn is_finished(&self) -> bool {
        self.finishing && self.output.is_none()
    }
}

impl ProcessorOperator for NativeRuntimeFilterProcessor {
    fn need_input(&self) -> bool {
        !self.finishing && self.output.is_none()
    }

    fn has_output(&self) -> bool {
        self.output.is_some()
    }

    fn push_chunk(&mut self, state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        if !self.need_input() {
            return Err("native runtime-filter processor cannot accept input".into());
        }
        self.consumers.acquire_blocking(
            state
                .runtime_filter_wait_timeout()
                .unwrap_or(Duration::from_secs(1)),
        )?;
        self.output = self
            .consumers
            .apply_chunk_profiled(chunk, self.profiles.as_ref())?;
        Ok(())
    }

    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(self.output.take())
    }

    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finishing = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Condvar, Mutex, mpsc};
    use std::time::Duration;

    use arrow::array::Int32Array;
    use arrow::datatypes::DataType;

    use super::NativeRuntimeFilterConsumerSet;
    use crate::common::ids::SlotId;
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::runtime_filter::{
        NativeRuntimeFilterConsumerSpec, NativeRuntimeFilterContract, NativeRuntimeFilterReduction,
    };
    use crate::exec::operators::runtime_filter::tests_support::{chunk, membership_bundle};
    use crate::exec::pipeline::operator::{Operator, ProcessorOperator};
    use crate::runtime::profile::{RUNTIME_FILTER_INPUT_ROWS, RUNTIME_FILTER_OUTPUT_ROWS};
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, ConsumerActivation, NullSemantics,
    };
    use crate::runtime_filter::port::artifact::ArtifactBundle;
    use crate::runtime_filter::port::subscription::{
        ArtifactAcquireOutcome, BlockingSnapshotSubscription, UnavailableReason,
    };

    struct TestSubscription {
        outcomes: Mutex<Vec<ArtifactAcquireOutcome>>,
        snapshot: Option<Arc<ArtifactBundle>>,
    }

    impl TestSubscription {
        fn new(outcomes: Vec<ArtifactAcquireOutcome>) -> Self {
            let snapshot = outcomes.iter().find_map(|outcome| match outcome {
                ArtifactAcquireOutcome::Published(bundle) => Some(Arc::clone(bundle)),
                _ => None,
            });
            Self {
                outcomes: Mutex::new(outcomes),
                snapshot,
            }
        }
    }

    impl BlockingSnapshotSubscription for TestSubscription {
        fn acquire(&self, _timeout: Duration) -> ArtifactAcquireOutcome {
            self.outcomes.lock().unwrap().remove(0)
        }

        fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
            self.snapshot.clone()
        }
    }

    fn fixture(
        outcomes: Vec<ArtifactAcquireOutcome>,
    ) -> (NativeRuntimeFilterConsumerSet, crate::exec::chunk::Chunk) {
        let mut arena = ExprArena::default();
        let spec = consumer_spec(&mut arena);
        let subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(TestSubscription::new(outcomes));
        (
            NativeRuntimeFilterConsumerSet::from_bound_for_test(
                vec![spec],
                Arc::new(arena),
                vec![subscription],
            ),
            chunk(&[1, 2, 3, 4]),
        )
    }

    fn consumer_spec(arena: &mut ExprArena) -> NativeRuntimeFilterConsumerSpec {
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let schema = crate::runtime_filter::port::artifact::ArtifactMembershipSchema::new(
            &DataType::Int32,
            NullSemantics::NeverMatches,
        )
        .unwrap();
        NativeRuntimeFilterConsumerSpec {
            binding_id: 11,
            channel_id: 7,
            expr_id,
            activation: ConsumerActivation::BlockingSnapshot,
            capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            contract: NativeRuntimeFilterContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            },
            reduction: NativeRuntimeFilterReduction::SetUnion,
        }
    }

    #[test]
    fn native_join_loopback_build_probe_applies_membership_artifact() {
        use crate::exec::chunk::{Chunk, ChunkSchema};
        use crate::exec::node::join::{
            JoinDistributionMode, JoinType, NativeJoinRuntimeFilterProducerSpec,
        };
        use crate::exec::operators::hashjoin::HashJoinBuildSinkFactory;
        use crate::exec::operators::hashjoin::build_state::JoinBuildSinkState;
        use crate::exec::operators::hashjoin::native_runtime_filter::NativeRuntimeFilterProducerFactory;
        use crate::exec::operators::hashjoin::partitioned_join_shared::PartitionedJoinSharedState;
        use crate::exec::pipeline::dependency::DependencyManager;
        use crate::exec::pipeline::operator_factory::OperatorFactory;
        use crate::runtime_filter::model::contract::{CompletionRequirement, ContributionKind};
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{Field, Schema};

        let (_service, producer_context, consumer_context) =
            crate::runtime_filter::service::tests::installed_join_loopback_service_for_exec_test();
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int64);
        let schema = crate::runtime_filter::port::artifact::ArtifactMembershipSchema::new(
            &DataType::Int64,
            NullSemantics::NeverMatches,
        )
        .unwrap();
        let contract = NativeRuntimeFilterContract::Membership {
            canonical_schema: Arc::from(schema.canonical_bytes()),
            schema_digest: schema.digest().bytes(),
        };
        let consumer_spec = NativeRuntimeFilterConsumerSpec {
            binding_id: 30,
            channel_id: 1,
            expr_id,
            activation: ConsumerActivation::BlockingSnapshot,
            capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            contract: contract.clone(),
            reduction: NativeRuntimeFilterReduction::SetUnion,
        };
        let arena = Arc::new(arena);
        let consumer_factory = super::NativeRuntimeFilterProcessorFactory::new(
            1,
            &[consumer_spec],
            Arc::clone(&arena),
        )
        .unwrap();
        let consumer_state = crate::runtime::runtime_state::RuntimeState::default()
            .with_native_runtime_filter_context(Some(consumer_context));

        let producer_spec = NativeJoinRuntimeFilterProducerSpec {
            binding_id: 10,
            channel_id: 1,
            build_expr_id: expr_id,
            build_key_index: 0,
            contribution_kinds: BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ]),
            completion_requirement: CompletionRequirement::ProducerClosed,
            contract,
            reduction: NativeRuntimeFilterReduction::SetUnion,
        };
        let producer_factory = Arc::new(
            NativeRuntimeFilterProducerFactory::from_plan(
                &[producer_spec],
                &[expr_id],
                &[false],
                arena.as_ref(),
                producer_context,
                1,
            )
            .unwrap(),
        );
        let join_state = Arc::new(PartitionedJoinSharedState::new(
            1,
            1,
            DependencyManager::new(),
            false,
        ));
        let build_state: Arc<dyn JoinBuildSinkState> = join_state;
        let build_factory = HashJoinBuildSinkFactory::new_native_with_runtime_filters(
            Arc::clone(&arena),
            JoinType::Inner,
            false,
            true,
            true,
            vec![expr_id],
            vec![false],
            JoinDistributionMode::Partitioned,
            build_state,
            Some(producer_factory),
        );
        let arrow_schema = Schema::new(vec![Field::new("v", DataType::Int64, false)]);
        let chunk_schema =
            ChunkSchema::try_ref_from_schema_and_slot_ids(&arrow_schema, &[SlotId::new(1)])
                .unwrap();
        let mut build = build_factory.create(1, 0);
        let build_state = crate::runtime::runtime_state::RuntimeState::default();
        build.bind_runtime_state(&build_state).unwrap();
        build
            .as_processor_mut()
            .unwrap()
            .push_chunk(
                &build_state,
                Chunk::try_new_with_columns(
                    Arc::clone(&chunk_schema),
                    vec![Arc::new(Int64Array::from(vec![2, 4])) as ArrayRef],
                )
                .unwrap(),
            )
            .unwrap();
        build
            .as_processor_mut()
            .unwrap()
            .set_finishing(&build_state)
            .unwrap();

        let input = Chunk::try_new_with_columns(
            chunk_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef],
        )
        .unwrap();
        let mut consumer = consumer_factory.create(1, 0);
        consumer.bind_runtime_state(&consumer_state).unwrap();
        consumer
            .as_processor_mut()
            .unwrap()
            .push_chunk(&consumer_state, input)
            .unwrap();
        let output = consumer
            .as_processor_mut()
            .unwrap()
            .pull_chunk(&consumer_state)
            .unwrap()
            .unwrap();
        let values = output.columns()[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values();
        assert_eq!(values.as_ref(), &[2, 4]);
    }

    #[test]
    fn native_join_blocking_timeout_fails_open_without_late_reapply() {
        let artifact = membership_bundle(&[2, 4]);
        let (consumers, input) = fixture(vec![
            ArtifactAcquireOutcome::TimedOut,
            ArtifactAcquireOutcome::Published(artifact),
        ]);
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        assert_eq!(consumers.apply_chunk(input).unwrap().unwrap().len(), 4);
    }

    #[test]
    fn native_join_unavailable_fails_open_without_result_drift() {
        let (consumers, input) = fixture(vec![ArtifactAcquireOutcome::Unavailable(
            UnavailableReason::ProducerFailed,
        )]);
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        assert_eq!(consumers.apply_chunk(input).unwrap().unwrap().len(), 4);
    }

    #[test]
    fn empty_domain_filters_all_rows() {
        let artifact = membership_bundle(&[]);
        let (consumers, input) = fixture(vec![ArtifactAcquireOutcome::Published(artifact)]);
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        assert!(consumers.apply_chunk(input).unwrap().is_none());
    }

    #[test]
    fn native_direct_wrapper_applies_the_shared_membership_mask() {
        let (consumers, _) = super::tests_support::published_consumer_set(
            super::tests_support::membership_bundle(&[2, 4]),
        );
        let state = crate::runtime::runtime_state::RuntimeState::default();
        let mut processor = super::NativeRuntimeFilterProcessor {
            name: "native-rf".into(),
            consumers,
            output: None,
            finishing: false,
            profiles: None,
        };
        let profiler = crate::runtime::profile::Profiler::new("native-rf-test");
        let profiles = crate::runtime::profile::OperatorProfiles::new(
            profiler.child("NativeRuntimeFilter (id=1)"),
        );
        processor.set_profiles(profiles);
        processor.bind_runtime_state(&state).unwrap();
        processor.push_chunk(&state, chunk(&[1, 2, 3, 4])).unwrap();
        let output = processor.pull_chunk(&state).unwrap().unwrap();
        let values = output.columns()[0]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[2, 4]);
        let tree = profiler.to_native_tree();
        let common = &tree.root.children[0].children[0];
        let counter = |name: &str| {
            common
                .counters
                .iter()
                .find(|counter| counter.name == name)
                .map(|counter| counter.value)
        };
        assert_eq!(counter("RuntimeFilterInputRows"), Some(4));
        assert_eq!(counter("RuntimeFilterOutputRows"), Some(2));
    }

    #[test]
    fn empty_consumer_set_does_not_record_apply_counters() {
        let consumers =
            NativeRuntimeFilterConsumerSet::from_plan(&[], Arc::new(ExprArena::default())).unwrap();
        let profiler = crate::runtime::profile::Profiler::new("empty-native-rf-test");
        let profiles = crate::runtime::profile::OperatorProfiles::new(
            profiler.child("NativeRuntimeFilter (id=1)"),
        );

        let output = consumers
            .apply_chunk_profiled(chunk(&[1, 2, 3, 4]), Some(&profiles))
            .unwrap()
            .unwrap();

        assert_eq!(output.len(), 4);
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_INPUT_ROWS),
            None
        );
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_OUTPUT_ROWS),
            None
        );
    }

    #[test]
    fn configured_pass_through_records_equal_apply_counters() {
        let (consumers, input) = fixture(vec![ArtifactAcquireOutcome::Unavailable(
            UnavailableReason::ProducerFailed,
        )]);
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        let profiler = crate::runtime::profile::Profiler::new("pass-through-native-rf-test");
        let profiles = crate::runtime::profile::OperatorProfiles::new(
            profiler.child("NativeRuntimeFilter (id=1)"),
        );

        let output = consumers
            .apply_chunk_profiled(input, Some(&profiles))
            .unwrap()
            .unwrap();

        assert_eq!(output.len(), 4);
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_INPUT_ROWS),
            Some(4)
        );
        assert_eq!(
            profiles.common.counter_value(RUNTIME_FILTER_OUTPUT_ROWS),
            Some(4)
        );
    }

    #[test]
    fn native_consumer_rejects_duplicate_binding_before_subscribe() {
        let mut arena = ExprArena::default();
        let spec = consumer_spec(&mut arena);
        let error =
            NativeRuntimeFilterConsumerSet::from_plan(&[spec.clone(), spec], Arc::new(arena))
                .err()
                .expect("duplicate binding must fail");
        assert!(error.contains("duplicate"));
    }

    #[test]
    fn native_consumer_rejects_artifact_outside_installed_join_profile() {
        use crate::runtime_filter::port::artifact::{
            ArtifactBundle, ArtifactKind, ConsumerArtifactProfile,
        };
        let valid = membership_bundle(&[2, 4]);
        let (kind, artifact) = &valid.artifacts()[0];
        assert_eq!(*kind, ArtifactKind::ValueSet);
        let profile =
            ConsumerArtifactProfile::new(BTreeSet::from([ArtifactKind::ValueSet]), None).unwrap();
        let wrong = Arc::new(
            ArtifactBundle::new(
                valid.channel_id(),
                valid.version(),
                &profile,
                vec![(*kind, Arc::clone(artifact))],
                usize::MAX,
            )
            .unwrap(),
        );
        let (consumers, _) = fixture(vec![ArtifactAcquireOutcome::Published(wrong)]);
        let error = consumers
            .acquire_blocking(Duration::ZERO)
            .expect_err("profile drift must fail synchronously");
        assert!(error.contains("ProfileMismatch"));
    }

    #[test]
    fn native_consumers_share_one_total_blocking_deadline() {
        struct RecordingSubscription {
            delay: Duration,
            outcome: ArtifactAcquireOutcome,
            observed: Arc<Mutex<Vec<Duration>>>,
        }
        impl BlockingSnapshotSubscription for RecordingSubscription {
            fn acquire(&self, timeout: Duration) -> ArtifactAcquireOutcome {
                self.observed.lock().unwrap().push(timeout);
                std::thread::sleep(self.delay);
                self.outcome.clone()
            }
            fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
                None
            }
        }

        let mut arena = ExprArena::default();
        let first = consumer_spec(&mut arena);
        let mut second = first.clone();
        second.binding_id = 12;
        let first_seen = Arc::new(Mutex::new(Vec::new()));
        let second_seen = Arc::new(Mutex::new(Vec::new()));
        let first_subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(RecordingSubscription {
                delay: Duration::from_millis(20),
                outcome: ArtifactAcquireOutcome::TimedOut,
                observed: Arc::clone(&first_seen),
            });
        let second_subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(RecordingSubscription {
                delay: Duration::ZERO,
                outcome: ArtifactAcquireOutcome::Unavailable(UnavailableReason::ProducerFailed),
                observed: Arc::clone(&second_seen),
            });
        let consumers = NativeRuntimeFilterConsumerSet::from_bound_for_test(
            vec![first, second],
            Arc::new(arena),
            vec![first_subscription, second_subscription],
        );
        consumers
            .acquire_blocking(Duration::from_millis(5))
            .unwrap();
        assert_eq!(first_seen.lock().unwrap().len(), 1);
        assert_eq!(second_seen.lock().unwrap().as_slice(), &[Duration::ZERO]);
    }

    #[test]
    fn native_concurrent_acquire_is_single_flight_without_holding_bindings_lock() {
        struct GatedSubscription {
            calls: Arc<AtomicUsize>,
            entered: Mutex<Option<mpsc::Sender<()>>>,
            release: Arc<(Mutex<bool>, Condvar)>,
            bundle: Arc<ArtifactBundle>,
        }
        impl BlockingSnapshotSubscription for GatedSubscription {
            fn acquire(&self, _timeout: Duration) -> ArtifactAcquireOutcome {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if let Some(entered) = self.entered.lock().unwrap().take() {
                    entered.send(()).unwrap();
                }
                let (lock, ready) = &*self.release;
                let mut released = lock.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
                ArtifactAcquireOutcome::Published(Arc::clone(&self.bundle))
            }
            fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
                None
            }
        }

        let mut arena = ExprArena::default();
        let spec = consumer_spec(&mut arena);
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let (entered_tx, entered_rx) = mpsc::channel();
        let subscription: Arc<dyn BlockingSnapshotSubscription> = Arc::new(GatedSubscription {
            calls: Arc::clone(&calls),
            entered: Mutex::new(Some(entered_tx)),
            release: Arc::clone(&release),
            bundle: membership_bundle(&[2, 4]),
        });
        let consumers = NativeRuntimeFilterConsumerSet::from_bound_for_test(
            vec![spec],
            Arc::new(arena),
            vec![subscription],
        );

        let first_consumers = consumers.clone();
        let first =
            std::thread::spawn(move || first_consumers.acquire_blocking(Duration::from_secs(1)));
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let second_consumers = consumers.clone();
        let second =
            std::thread::spawn(move || second_consumers.acquire_blocking(Duration::from_secs(1)));
        let apply_consumers = consumers.clone();
        let (apply_tx, apply_rx) = mpsc::channel();
        std::thread::spawn(move || {
            apply_tx
                .send(apply_consumers.apply_chunk(chunk(&[1, 2, 3, 4])))
                .unwrap();
        });
        let apply_error = apply_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("bindings lock must remain available during external acquire")
            .expect_err("apply must wait for acquisition to complete");
        assert!(apply_error.contains("must acquire before apply"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let (lock, ready) = &*release;
        *lock.lock().unwrap() = true;
        ready.notify_all();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn native_consumer_acquires_once_and_reuses_one_immutable_version_across_chunks() {
        struct CountingSubscription {
            calls: Arc<AtomicUsize>,
            bundle: Arc<ArtifactBundle>,
        }
        impl BlockingSnapshotSubscription for CountingSubscription {
            fn acquire(&self, _timeout: Duration) -> ArtifactAcquireOutcome {
                self.calls.fetch_add(1, Ordering::SeqCst);
                ArtifactAcquireOutcome::Published(Arc::clone(&self.bundle))
            }
            fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
                Some(Arc::clone(&self.bundle))
            }
        }

        let mut arena = ExprArena::default();
        let spec = consumer_spec(&mut arena);
        let calls = Arc::new(AtomicUsize::new(0));
        let subscription: Arc<dyn BlockingSnapshotSubscription> = Arc::new(CountingSubscription {
            calls: Arc::clone(&calls),
            bundle: membership_bundle(&[2, 4]),
        });
        let consumers = NativeRuntimeFilterConsumerSet::from_bound_for_test(
            vec![spec],
            Arc::new(arena),
            vec![subscription],
        );

        consumers.acquire_blocking(Duration::ZERO).unwrap();
        let first = consumers
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .unwrap()
            .unwrap();
        consumers.acquire_blocking(Duration::ZERO).unwrap();
        let second = consumers
            .apply_chunk(chunk(&[2, 3, 4, 5]))
            .unwrap()
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            first.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[2, 4]
        );
        assert_eq!(
            second.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[2, 4]
        );
    }

    #[test]
    fn native_consumer_keeps_active_binding_when_another_binding_is_unavailable() {
        let mut arena = ExprArena::default();
        let first = consumer_spec(&mut arena);
        let mut second = first.clone();
        second.binding_id = 12;
        let active: Arc<dyn BlockingSnapshotSubscription> = Arc::new(TestSubscription::new(vec![
            ArtifactAcquireOutcome::Published(membership_bundle(&[2, 4])),
        ]));
        let unavailable: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(TestSubscription::new(vec![
                ArtifactAcquireOutcome::Unavailable(UnavailableReason::ProducerFailed),
            ]));
        let consumers = NativeRuntimeFilterConsumerSet::from_bound_for_test(
            vec![first, second],
            Arc::new(arena),
            vec![active, unavailable],
        );

        consumers.acquire_blocking(Duration::ZERO).unwrap();
        let output = consumers
            .apply_chunk(chunk(&[1, 2, 3, 4]))
            .unwrap()
            .unwrap();
        assert_eq!(
            output.columns()[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[2, 4]
        );
    }

    #[test]
    fn native_consumer_unsupported_and_cancelled_are_sticky_pass_through() {
        use crate::runtime_filter::port::subscription::ArtifactUnsupportedReason;

        for outcome in [
            ArtifactAcquireOutcome::Unsupported(
                ArtifactUnsupportedReason::NoAcceptedRepresentation,
            ),
            ArtifactAcquireOutcome::Cancelled,
        ] {
            let (consumers, input) = fixture(vec![outcome]);
            consumers.acquire_blocking(Duration::ZERO).unwrap();
            consumers.acquire_blocking(Duration::ZERO).unwrap();
            assert_eq!(consumers.apply_chunk(input).unwrap().unwrap().len(), 4);
        }
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use crate::common::ids::SlotId;
    use crate::exec::chunk::{Chunk, ChunkSchema};
    use crate::exec::expr::{ExprArena, ExprNode};
    use crate::exec::node::runtime_filter::{
        NativeRuntimeFilterConsumerSpec, NativeRuntimeFilterContract, NativeRuntimeFilterReduction,
    };
    use crate::exec::operators::runtime_filter::NativeRuntimeFilterConsumerSet;
    use crate::runtime_filter::materializer::codec::{
        build_membership_index, encode_membership_leaf, inspect_membership_index,
    };
    use crate::runtime_filter::model::contract::{
        ArtifactCapability, ChannelId, ConsumerActivation, NullSemantics,
    };
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile,
        PhysicalArtifact,
    };
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::subscription::{
        ArtifactAcquireOutcome, BlockingSnapshotSubscription,
    };
    use crate::runtime_filter::port::value_domain::{MembershipValues, ReducedMembershipDomain};

    pub(crate) fn chunk(values: &[i32]) -> Chunk {
        let schema = Schema::new(vec![Field::new("v", DataType::Int32, true)]);
        Chunk::try_new_with_columns(
            ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[SlotId::new(1)]).unwrap(),
            vec![Arc::new(Int32Array::from(values.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    pub(crate) fn membership_bundle(values: &[i32]) -> Arc<ArtifactBundle> {
        let null_semantics = NullSemantics::NeverMatches;
        let version = LogicalVersion::FIRST;
        let domain =
            ReducedMembershipDomain::new(MembershipValues::int32(values.iter().copied()), false);
        let encoded = encode_membership_leaf(&domain, null_semantics, version).unwrap();
        let kind = ArtifactKind::from_tag(encoded[6]).unwrap();
        let schema = ArtifactMembershipSchema::new(&DataType::Int32, null_semantics).unwrap();
        let plan = inspect_membership_index(&encoded).unwrap();
        let index = build_membership_index(&encoded, &plan).unwrap();
        let artifact = Arc::new(
            PhysicalArtifact::new_indexed_test(
                kind,
                schema.digest(),
                version,
                false,
                encoded.into(),
                index,
            )
            .unwrap(),
        );
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        Arc::new(
            ArtifactBundle::new(
                ChannelId::new(7),
                version,
                &profile,
                vec![(kind, artifact)],
                usize::MAX,
            )
            .unwrap(),
        )
    }

    struct PublishedSubscription(Arc<ArtifactBundle>);

    impl BlockingSnapshotSubscription for PublishedSubscription {
        fn acquire(&self, _timeout: Duration) -> ArtifactAcquireOutcome {
            ArtifactAcquireOutcome::Published(Arc::clone(&self.0))
        }

        fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
            Some(Arc::clone(&self.0))
        }
    }

    pub(crate) struct AcquireObserver {
        calls: AtomicUsize,
        thread: Mutex<Option<std::thread::ThreadId>>,
    }

    impl AcquireObserver {
        pub(crate) fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        pub(crate) fn thread(&self) -> Option<std::thread::ThreadId> {
            *self.thread.lock().expect("acquire observer lock")
        }
    }

    struct ObservedPublishedSubscription {
        bundle: Arc<ArtifactBundle>,
        observer: Arc<AcquireObserver>,
    }

    impl BlockingSnapshotSubscription for ObservedPublishedSubscription {
        fn acquire(&self, _timeout: Duration) -> ArtifactAcquireOutcome {
            self.observer.calls.fetch_add(1, Ordering::SeqCst);
            *self.observer.thread.lock().expect("acquire observer lock") =
                Some(std::thread::current().id());
            ArtifactAcquireOutcome::Published(Arc::clone(&self.bundle))
        }

        fn snapshot(&self) -> Option<Arc<ArtifactBundle>> {
            Some(Arc::clone(&self.bundle))
        }
    }

    pub(crate) fn published_consumer_set(
        bundle: Arc<ArtifactBundle>,
    ) -> (NativeRuntimeFilterConsumerSet, Arc<ExprArena>) {
        published_consumer_set_for(bundle, DataType::Int32)
    }

    pub(crate) fn observed_published_consumer_set(
        bundle: Arc<ArtifactBundle>,
    ) -> (
        NativeRuntimeFilterConsumerSet,
        Arc<ExprArena>,
        Arc<AcquireObserver>,
    ) {
        let observer = Arc::new(AcquireObserver {
            calls: AtomicUsize::new(0),
            thread: Mutex::new(None),
        });
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Int32);
        let arena = Arc::new(arena);
        let schema =
            ArtifactMembershipSchema::new(&DataType::Int32, NullSemantics::NeverMatches).unwrap();
        let spec = NativeRuntimeFilterConsumerSpec {
            binding_id: 11,
            channel_id: 7,
            expr_id,
            activation: ConsumerActivation::BlockingSnapshot,
            capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            contract: NativeRuntimeFilterContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            },
            reduction: NativeRuntimeFilterReduction::SetUnion,
        };
        let subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(ObservedPublishedSubscription {
                bundle,
                observer: Arc::clone(&observer),
            });
        (
            NativeRuntimeFilterConsumerSet::from_bound_for_test(
                vec![spec],
                Arc::clone(&arena),
                vec![subscription],
            ),
            arena,
            observer,
        )
    }

    pub(crate) fn utf8_membership_bundle(values: &[&str]) -> Arc<ArtifactBundle> {
        membership_bundle_for(MembershipValues::utf8_set(
            values.iter().map(|value| (*value).to_string()).collect(),
        ))
    }

    pub(crate) fn published_utf8_consumer_set(
        bundle: Arc<ArtifactBundle>,
    ) -> (NativeRuntimeFilterConsumerSet, Arc<ExprArena>) {
        published_consumer_set_for(bundle, DataType::Utf8)
    }

    fn published_consumer_set_for(
        bundle: Arc<ArtifactBundle>,
        data_type: DataType,
    ) -> (NativeRuntimeFilterConsumerSet, Arc<ExprArena>) {
        let mut arena = ExprArena::default();
        let expr_id = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), data_type.clone());
        let arena = Arc::new(arena);
        let schema =
            ArtifactMembershipSchema::new(&data_type, NullSemantics::NeverMatches).unwrap();
        let spec = NativeRuntimeFilterConsumerSpec {
            binding_id: 11,
            channel_id: 7,
            expr_id,
            activation: ConsumerActivation::BlockingSnapshot,
            capabilities: BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ]),
            contract: NativeRuntimeFilterContract::Membership {
                canonical_schema: Arc::from(schema.canonical_bytes()),
                schema_digest: schema.digest().bytes(),
            },
            reduction: NativeRuntimeFilterReduction::SetUnion,
        };
        let subscription: Arc<dyn BlockingSnapshotSubscription> =
            Arc::new(PublishedSubscription(bundle));
        (
            NativeRuntimeFilterConsumerSet::from_bound_for_test(
                vec![spec],
                Arc::clone(&arena),
                vec![subscription],
            ),
            arena,
        )
    }

    fn membership_bundle_for(values: MembershipValues) -> Arc<ArtifactBundle> {
        let null_semantics = NullSemantics::NeverMatches;
        let version = LogicalVersion::FIRST;
        let data_type = values.data_type();
        let domain = ReducedMembershipDomain::new(values, false);
        let encoded = encode_membership_leaf(&domain, null_semantics, version).unwrap();
        let kind = ArtifactKind::from_tag(encoded[6]).unwrap();
        let schema = ArtifactMembershipSchema::new(&data_type, null_semantics).unwrap();
        let plan = inspect_membership_index(&encoded).unwrap();
        let index = build_membership_index(&encoded, &plan).unwrap();
        let artifact = Arc::new(
            PhysicalArtifact::new_indexed_test(
                kind,
                schema.digest(),
                version,
                false,
                encoded.into(),
                index,
            )
            .unwrap(),
        );
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        Arc::new(
            ArtifactBundle::new(
                ChannelId::new(7),
                version,
                &profile,
                vec![(kind, artifact)],
                usize::MAX,
            )
            .unwrap(),
        )
    }
}
