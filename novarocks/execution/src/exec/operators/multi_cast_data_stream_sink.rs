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
//! Multi-cast stream sink for replicated exchange output.
//!
//! Responsibilities:
//! - Sends the same serialized chunk stream to multiple remote destinations.
//! - Coordinates per-destination transport state and completion signaling.
//!
//! Key exported interfaces:
//! - Types: `MultiCastDataStreamSinkFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::exec::pipeline::{
    input::PreparedInput,
    operator::{
        ConstructionFailure, EncodedValueOccurrence, InputConstructionFactory, InputEncodingPolicy,
    },
    shape::fanout::{Fanout, FanoutRouter, RouteStep},
    step_context::StepContext,
};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};

use std::sync::Arc;

use crate::runtime::fragment::io::exchange_edge::ExchangeEdgeGates;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::ExchangeFrameTransmitter;
use novarocks_types::UniqueId;

use super::DataStreamSinkFactory;
use crate::exec::fragment::sink::DataStreamSinkFactoryInput;

struct InnerSinkSpec {
    demand: Option<Arc<LimitDemand>>,
    factory: DataStreamSinkFactory,
}

/// Factory for multi-cast stream sinks that replicate output to multiple remote channels.
pub struct MultiCastDataStreamSinkFactory {
    name: String,
    init_error: Option<ExecutionFailure>,
    sinks: Vec<InnerSinkSpec>,
}

impl MultiCastDataStreamSinkFactory {
    pub fn new(
        sinks: Vec<(DataStreamSinkFactoryInput, Option<i64>)>,
        fragment_instance_id: UniqueId,
        sender_id: Option<i32>,
        partition_arena: ExprArena,
        plan_node_id: i32,
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
    ) -> Self {
        let name = if plan_node_id >= 0 {
            format!("MULTI_CAST_DATA_STREAM_SINK (id={plan_node_id})")
        } else {
            "MULTI_CAST_DATA_STREAM_SINK".to_string()
        };
        let mut init_error = None;
        let mut out = Vec::new();

        if sinks.is_empty() {
            init_error = Some(ExecutionFailure::from_domain(
                MultiCastFailure::EmptyBranches,
            ));
        } else {
            for (sink, limit) in sinks {
                let demand = match limit {
                    Some(-1) | None => None,
                    Some(value) if value >= 0 => Some(Arc::new(LimitDemand {
                        remaining: AtomicI64::new(value),
                        changed: Arc::new(Observable::new()),
                    })),
                    Some(value) => {
                        init_error.get_or_insert_with(|| {
                            ExecutionFailure::from_domain(MultiCastFailure::InvalidLimit(value))
                        });
                        None
                    }
                };

                out.push(InnerSinkSpec {
                    demand,
                    factory: DataStreamSinkFactory::new(
                        sink,
                        fragment_instance_id,
                        sender_id,
                        plan_node_id,
                        partition_arena.clone(),
                        Arc::clone(&transmitter),
                    ),
                });
            }
        }

        Self {
            name,
            init_error,
            sinks: out,
        }
    }

    /// Grants the inner sinks the edge gates their destinations are bound by.
    ///
    /// A composite sink's branches all belong to the same task, so one gate
    /// set governs all of them; forwarding here keeps the send path's
    /// permission check in exactly one place.
    /// Whether every branch is bound by a gate set.
    #[cfg(test)]
    pub(crate) fn is_edge_gated(&self) -> bool {
        !self.sinks.is_empty() && self.sinks.iter().all(|spec| spec.factory.is_edge_gated())
    }

    pub fn with_edge_gates(mut self, gates: Arc<ExchangeEdgeGates>) -> Self {
        self.sinks = self
            .sinks
            .into_iter()
            .map(|mut spec| {
                spec.factory = spec.factory.with_edge_gates(Arc::clone(&gates));
                spec
            })
            .collect();
        self
    }
}

impl OperatorFactory for MultiCastDataStreamSinkFactory {
    #[cfg(test)]
    fn is_edge_gated(&self) -> bool {
        Self::is_edge_gated(self)
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, dop: i32, driver_id: i32) -> Box<dyn Operator> {
        let branches = self
            .sinks
            .iter()
            .map(|spec| spec.factory.create(dop, driver_id))
            .collect();
        let router = MultiCastRouter {
            demands: self.sinks.iter().map(|spec| spec.demand.clone()).collect(),
        };
        Box::new(Fanout::new(
            self.name.clone(),
            branches,
            router,
            self.init_error.clone(),
        ))
    }

    fn is_sink(&self) -> bool {
        true
    }
}

struct LimitDemand {
    remaining: AtomicI64,
    changed: Arc<Observable>,
}
impl LimitDemand {
    fn claim(&self, rows: usize) -> ExecutionResult<usize> {
        let rows = i64::try_from(rows)
            .map_err(|_| ExecutionFailure::from_domain(MultiCastFailure::RowExtent))?;
        loop {
            let available = self.remaining.load(Ordering::Acquire);
            if available <= 0 {
                return Ok(0);
            }
            let take = rows.min(available);
            let next = available - take;
            if self
                .remaining
                .compare_exchange(available, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if next == 0 {
                    self.changed.notify_observers();
                }
                return Ok(take as usize);
            }
        }
    }
}
struct MultiCastRouter {
    demands: Vec<Option<Arc<LimitDemand>>>,
}
impl InputEncodingPolicy for MultiCastRouter {
    fn retains_encoded_path(&self, _: EncodedValueOccurrence<'_>) -> bool {
        true
    }
}
impl FanoutRouter for MultiCastRouter {
    type Cursor = (Chunk, usize);
    fn begin(
        &mut self,
        input: PreparedInput,
        _: &dyn InputConstructionFactory,
        ctx: &mut StepContext<'_>,
    ) -> ExecutionResult<Self::Cursor> {
        if !input.chunk().chunk_schema().is_typed() {
            let failure = ExecutionFailure::from_domain(MultiCastFailure::MissingBindings);
            ctx.report_failure(failure.clone());
            return Err(failure);
        }
        Ok((input.into_uncovered_chunk()?, 0))
    }
    fn advance(
        &mut self,
        cursor: &mut Self::Cursor,
        factory: &dyn InputConstructionFactory,
        ctx: &mut StepContext<'_>,
    ) -> ExecutionResult<RouteStep> {
        if cursor.1 == self.demands.len() {
            return Ok(RouteStep::Finished);
        }
        let branch = cursor.1;
        cursor.1 += 1;
        let rows = match &self.demands[branch] {
            Some(demand) => demand.claim(cursor.0.len())?,
            None => cursor.0.len(),
        };
        if rows == 0 {
            return Ok(RouteStep::Progress);
        }
        let chunk = if rows == cursor.0.len() {
            cursor.0.clone()
        } else {
            let owner = factory.begin_input_construction(&cursor.0)?;
            cursor
                .0
                .slice_typed_observed::<ConstructionFailure, _>(
                    0,
                    rows,
                    owner.storage_lifetime(),
                    |step| owner.observe(0, &[], step),
                )
                .map_err(|failure| {
                    let failure = ExecutionFailure::from(failure);
                    ctx.report_failure(failure.clone());
                    failure
                })?
        };
        Ok(RouteStep::Input { branch, chunk })
    }
    fn accepts_branch(&self, branch: usize) -> bool {
        self.demands[branch]
            .as_ref()
            .is_none_or(|demand| demand.remaining.load(Ordering::Acquire) > 0)
    }
    fn visit_demand_events(&self, visit: &mut dyn FnMut(Arc<Observable>)) {
        for demand in self.demands.iter().flatten() {
            visit(demand.changed.clone());
        }
    }
}
#[derive(Debug)]
enum MultiCastFailure {
    EmptyBranches,
    MissingBindings,
    RowExtent,
    InvalidLimit(i64),
}
impl std::fmt::Display for MultiCastFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyBranches => f.write_str("multicast requires at least one sink branch"),
            Self::MissingBindings => {
                f.write_str("multicast input requires complete frozen value bindings")
            }
            Self::RowExtent => {
                f.write_str("multicast row extent exceeds its limit counter representation")
            }
            Self::InvalidLimit(value) => write!(f, "multicast has invalid frozen limit {value}"),
        }
    }
}
impl std::error::Error for MultiCastFailure {}
impl novarocks_failure_contract::FailureProjection for MultiCastFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self {
            Self::EmptyBranches => (FailureClass::Invariant, 100),
            Self::MissingBindings => (FailureClass::Invariant, 101),
            Self::RowExtent => (FailureClass::Representation, 102),
            Self::InvalidLimit(_) => (FailureClass::Invariant, 103),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::EXCHANGE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::{
        driver::{DriverState, PipelineDriver},
        input::RawInput,
        shape::{ControlStep, StepOperator, fanout::test_support as support},
        step::{InputResult, NextStep, StepResult, WaitFact, WaitReason},
    };
    use crate::runtime::runtime_state::RuntimeState;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    #[test]
    fn multicast_preserves_typed_rows_and_stops_accepting_while_a_required_branch_is_full() {
        let first = support::Probe::new(false);
        let second = support::Probe::new(true);
        let mut fanout = Fanout::new(
            "multicast routes".into(),
            vec![support::sink(first.clone()), support::sink(second.clone())],
            MultiCastRouter {
                demands: vec![None, None],
            },
            None,
        );
        support::prepare(&mut fanout);
        assert!(matches!(
            support::until_wait_or_input(&mut fanout),
            NextStep::NeedInput
        ));
        support::push(&mut fanout, support::typed_i32(vec![10, 20]));
        assert!(matches!(
            support::until_wait_or_input(&mut fanout),
            NextStep::Wait(_)
        ));
        assert_eq!(second.rows.lock().unwrap().as_slice(), [10, 20]);
        assert_eq!(first.drives.load(Ordering::Acquire), 0);
        let next = RawInput::for_test(support::typed_i32(vec![30]));
        let identity = next.original_for_test().columns()[0].clone();
        match support::context(|ctx| fanout.try_push(ctx, next)) {
            InputResult::Deferred {
                input,
                next: crate::exec::pipeline::step::AdmissionNext::Wait(_),
            } => assert!(Arc::ptr_eq(
                &identity,
                &input.original_for_test().columns()[0]
            )),
            _ => panic!("the full required branch must retain coupled parent backpressure"),
        }
        first.ready.store(true, Ordering::Release);
        first.event.notify_observers();
        assert!(matches!(
            support::until_wait_or_input(&mut fanout),
            NextStep::NeedInput
        ));
        assert_eq!(first.rows.lock().unwrap().as_slice(), [10, 20]);
        assert_eq!(second.rows.lock().unwrap().as_slice(), [10, 20]);
    }
    struct EmptySource {
        event: Arc<Observable>,
        polls: Arc<AtomicUsize>,
    }
    impl OperatorLifecycle for EmptySource {
        fn name(&self) -> &str {
            "LIMIT_EMPTY_SOURCE"
        }
    }
    impl StepOperator for EmptySource {
        fn try_push(&mut self, _: &mut StepContext<'_>, _: RawInput) -> InputResult {
            panic!("source has no input")
        }
        fn drive(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
            self.polls.fetch_add(1, Ordering::AcqRel);
            Ok(StepResult::new(
                None,
                NextStep::Wait(WaitFact {
                    reason: WaitReason::Input,
                    observable: self.event.clone(),
                    observed_generation: self.event.generation(),
                    deadline: None,
                }),
                0,
            ))
        }
        fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<()> {
            panic!("source remains open")
        }
    }
    #[test]
    fn shared_limit_zero_wakes_an_empty_driver_without_overtaking_the_claimed_prefix() {
        let demand = Arc::new(LimitDemand {
            remaining: AtomicI64::new(3),
            changed: Arc::new(Observable::new()),
        });
        let owner = support::Probe::new(false);
        let idle = support::Probe::new(true);
        let mut first = Fanout::new(
            "claimed prefix".into(),
            vec![support::sink(owner.clone())],
            MultiCastRouter {
                demands: vec![Some(demand.clone())],
            },
            None,
        );
        support::prepare(&mut first);
        assert!(matches!(
            support::until_wait_or_input(&mut first),
            NextStep::NeedInput
        ));
        let runtime = Arc::new(RuntimeState::default());
        let mut second = Fanout::new(
            "idle limit driver".into(),
            vec![support::sink(idle.clone())],
            MultiCastRouter {
                demands: vec![Some(demand.clone())],
            },
            None,
        );
        support::prepare_on(&mut second, &runtime);
        let polls = Arc::new(AtomicUsize::new(0));
        let mut driver = PipelineDriver::new(
            1,
            vec![
                Box::new(EmptySource {
                    event: Arc::new(Observable::new()),
                    polls: polls.clone(),
                }),
                Box::new(second),
            ],
            None,
            vec![],
            runtime,
            None,
        );
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Blocked(crate::exec::pipeline::operator::BlockedReason::InputEmpty)
        );
        support::push(&mut first, support::typed_i32(vec![10, 20, 30, 40]));
        assert!(matches!(
            support::until_wait_or_input(&mut first),
            NextStep::Wait(_)
        ));
        assert_eq!(demand.remaining.load(Ordering::Acquire), 0);
        assert_eq!(
            owner.finishes.load(Ordering::Acquire),
            0,
            "the claimed prefix still owns its exact branch input"
        );
        driver.set_ready();
        assert_eq!(
            driver.process(Duration::from_secs(1)),
            DriverState::Finished
        );
        assert_eq!(polls.load(Ordering::Acquire), 1);
        assert_eq!(idle.tries.load(Ordering::Acquire), 0);
        assert_eq!(idle.finishes.load(Ordering::Acquire), 1);
        assert_eq!(idle.seals.load(Ordering::Acquire), 1);
        assert_eq!(
            support::context(|ctx| first.poll_control(ctx)).unwrap(),
            ControlStep::Active
        );
        owner.ready.store(true, Ordering::Release);
        owner.event.notify_observers();
        assert!(matches!(
            support::until_wait_or_input(&mut first),
            NextStep::Finished
        ));
        assert_eq!(owner.rows.lock().unwrap().as_slice(), [10, 20, 30]);
        assert_eq!(owner.finishes.load(Ordering::Acquire), 1);
        assert_eq!(owner.seals.load(Ordering::Acquire), 1);
    }
}
