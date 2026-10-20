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
//! Split stream sink for partitioned exchange output.
//!
//! Responsibilities:
//! - Splits incoming chunks into disjoint per-destination subsets based on split expressions.
//! - Sends split chunks to dedicated downstream data stream sinks.
//! - Preserves sink completion signaling.

use crate::exec::pipeline::{
    input::PreparedInput,
    operator::{ConstructionFailure, InputConstructionFactory, InputEncodingPolicy},
    shape::fanout::{Fanout, FanoutRouter, RouteStep},
    step_context::StepContext,
};
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};

use std::sync::Arc;

use crate::runtime::fragment::io::exchange_edge::ExchangeEdgeGates;

use arrow::array::{Array, BooleanArray};

use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprId};
use crate::exec::pipeline::operator::Operator;
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::schedule::observer::Observable;
use crate::runtime::fragment::io::ExchangeFrameTransmitter;
use novarocks_types::UniqueId;

use super::DataStreamSinkFactory;
use crate::exec::fragment::sink::DataStreamSinkFactoryInput;

struct InnerSinkSpec {
    factory: DataStreamSinkFactory,
}

/// Factory for split stream sinks that partition rows to multiple remote channels.
pub struct SplitDataStreamSinkFactory {
    name: String,
    init_error: Option<ExecutionFailure>,
    split_arena: Arc<ExprArena>,
    split_exprs: Vec<ExprId>,
    fanout: bool,
    sinks: Vec<InnerSinkSpec>,
}

impl SplitDataStreamSinkFactory {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        sinks: Vec<DataStreamSinkFactoryInput>,
        fragment_instance_id: UniqueId,
        sender_id: Option<i32>,
        partition_arena: ExprArena,
        plan_node_id: i32,
        split_arena: Arc<ExprArena>,
        split_exprs: Vec<ExprId>,
        fanout: bool,
        transmitter: Arc<dyn ExchangeFrameTransmitter>,
    ) -> Self {
        let name = if plan_node_id >= 0 {
            format!("SPLIT_DATA_STREAM_SINK (id={plan_node_id})")
        } else {
            "SPLIT_DATA_STREAM_SINK".to_string()
        };

        let mut init_error = None;
        let mut sinks_out = Vec::new();

        if sinks.is_empty() {
            init_error = Some(split_failure(SplitFailure::EmptyBranches));
        } else if split_exprs.len() != sinks.len() {
            init_error = Some(split_failure(SplitFailure::WrongArity));
        } else {
            for sink in sinks {
                sinks_out.push(InnerSinkSpec {
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
            split_arena,
            split_exprs,
            fanout,
            sinks: sinks_out,
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

impl OperatorFactory for SplitDataStreamSinkFactory {
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
        let router = SplitRouter {
            arena: self.split_arena.clone(),
            expressions: self.split_exprs.clone(),
            fanout: self.fanout,
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

struct SplitRouter {
    arena: Arc<ExprArena>,
    expressions: Vec<ExprId>,
    fanout: bool,
}
impl InputEncodingPolicy for SplitRouter {}
struct SplitSelection {
    rows: Vec<u32>,
    _storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
struct SplitCursor {
    input: Chunk,
    remaining: Option<SplitSelection>,
    next: usize,
}
impl FanoutRouter for SplitRouter {
    type Cursor = SplitCursor;
    fn begin(
        &mut self,
        input: PreparedInput,
        factory: &dyn InputConstructionFactory,
        ctx: &mut StepContext<'_>,
    ) -> ExecutionResult<Self::Cursor> {
        let result: ExecutionResult<Option<SplitSelection>> = (|| {
            if !input.chunk().chunk_schema().is_typed() {
                return Err(split_failure(SplitFailure::MissingBindings));
            }
            u32::try_from(input.chunk().len())
                .map_err(|_| split_failure(SplitFailure::RowExtent))?;
            let remaining = if self.fanout {
                None
            } else {
                let owner = factory.begin_input_construction(input.chunk())?;
                owner.request(std::alloc::Layout::array::<u32>(input.chunk().len()).map_err(|_|ConstructionFailure::from(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
                let mut rows = Vec::with_capacity(input.chunk().len());
                rows.extend((0..input.chunk().len()).map(|row| row as u32));
                Some(SplitSelection {
                    rows,
                    _storage: owner.storage_lifetime(),
                })
            };
            Ok(remaining)
        })();
        match result {
            Ok(remaining) => Ok(SplitCursor {
                input: input.into_uncovered_chunk()?,
                remaining,
                next: if self.fanout {
                    0
                } else {
                    self.expressions.len()
                },
            }),
            Err(failure) => {
                ctx.report_failure(failure.clone());
                Err(failure)
            }
        }
    }
    fn advance(
        &mut self,
        cursor: &mut Self::Cursor,
        factory: &dyn InputConstructionFactory,
        ctx: &mut StepContext<'_>,
    ) -> ExecutionResult<RouteStep> {
        if self.fanout {
            if cursor.next == self.expressions.len() {
                return Ok(RouteStep::Finished);
            }
        } else if cursor.next == 0
            || cursor
                .remaining
                .as_ref()
                .is_none_or(|remaining| remaining.rows.is_empty())
        {
            return Ok(RouteStep::Finished);
        }
        let branch = if self.fanout {
            let branch = cursor.next;
            cursor.next += 1;
            branch
        } else {
            cursor.next -= 1;
            cursor.next
        };
        let input = if let Some(remaining) = &cursor.remaining {
            gather_split_input(&cursor.input, &remaining.rows, factory, ctx)?
        } else {
            cursor.input.clone()
        };
        let predicate = {
            let owner = factory.begin_input_construction(&input)?;
            let evaluation =
                crate::exec::expr::ExprEvaluationContext::new(&self.arena, owner.as_ref(), ctx);
            evaluation
                .eval(self.expressions[branch], &input)
                .map_err(|failure| {
                    ctx.report_failure(failure.clone());
                    failure
                })?
        };
        let mask = predicate
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                let failure = split_failure(SplitFailure::PredicateKind);
                ctx.report_failure(failure.clone());
                failure
            })?;
        if mask.len() != input.len() {
            let failure = split_failure(SplitFailure::MaskExtent);
            ctx.report_failure(failure.clone());
            return Err(failure);
        }
        let owner = factory.begin_input_construction(&cursor.input)?;
        owner.request(std::alloc::Layout::array::<u32>(input.len()).map_err(|_| {
            ConstructionFailure::from(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
            )
        })?)?;
        let mut selected = Vec::with_capacity(input.len());
        if let Some(remaining) = &mut cursor.remaining {
            let mut keep = 0;
            for row in 0..mask.len() {
                let physical = remaining.rows[row];
                if mask.is_valid(row) && mask.value(row) {
                    selected.push(physical);
                } else {
                    remaining.rows[keep] = physical;
                    keep += 1;
                }
            }
            remaining.rows.truncate(keep);
        } else {
            selected.extend(
                (0..mask.len())
                    .filter(|row| mask.is_valid(*row) && mask.value(*row))
                    .map(|row| row as u32),
            );
        }
        if selected.is_empty() {
            return Ok(RouteStep::Progress);
        }
        let chunk = gather_split_input(&cursor.input, &selected, factory, ctx)?;
        Ok(RouteStep::Input { branch, chunk })
    }
    fn accepts_branch(&self, _: usize) -> bool {
        true
    }
}
fn gather_split_input(
    source: &Chunk,
    rows: &[u32],
    factory: &dyn InputConstructionFactory,
    ctx: &mut StepContext<'_>,
) -> ExecutionResult<Chunk> {
    if rows.len() == source.len() {
        return Ok(source.clone());
    }
    let owner = factory.begin_input_construction(source)?;
    let result: ExecutionResult<Chunk> = (|| {
        use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
        owner.request(
            std::alloc::Layout::array::<u32>(rows.len())
                .map_err(|_| ConstructionFailure::from(LayoutResourceError::Arithmetic))?,
        )?;
        let indices = arrow::array::UInt32Array::from(rows.to_vec());
        owner.request(
            std::alloc::Layout::array::<arrow::array::ArrayRef>(source.columns().len())
                .map_err(|_| ConstructionFailure::from(LayoutResourceError::Arithmetic))?,
        )?;
        let mut columns = Vec::with_capacity(source.columns().len());
        for array in source.columns() {
            columns.push(
                arrow::compute::take(array.as_ref(), &indices, None)
                    .map_err(|error| split_failure(SplitFailure::Arrow(error)))?,
            );
        }
        // Every gather is rooted directly in the one original source. No
        // previous remaining batch or recursively accumulated wrapper is kept.
        let storage = source.source_storage_lifetime_observed::<ConstructionFailure>(
            owner.storage_lifetime(),
            |layout| owner.request(layout),
        )?;
        Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            ConstructionFailure,
            _,
        >(
            source.chunk_schema_ref(),
            columns,
            rows.len(),
            storage,
            |step| owner.observe(0, &[], step),
        )?)
    })();
    result.map_err(|failure| {
        ctx.report_failure(failure.clone());
        failure
    })
}
#[derive(Debug)]
enum SplitFailure {
    EmptyBranches,
    WrongArity,
    MissingBindings,
    RowExtent,
    PredicateKind,
    MaskExtent,
    Arrow(arrow::error::ArrowError),
}
fn split_failure(reason: SplitFailure) -> ExecutionFailure {
    ExecutionFailure::from_domain(reason)
}
impl std::fmt::Display for SplitFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyBranches => f.write_str("split requires at least one sink branch"),
            Self::WrongArity => {
                f.write_str("split expression count differs from its frozen branch count")
            }
            Self::MissingBindings => {
                f.write_str("split input requires complete frozen value bindings")
            }
            Self::RowExtent => f.write_str("split row extent exceeds its selection representation"),
            Self::PredicateKind => f.write_str("split predicate did not produce BOOLEAN"),
            Self::MaskExtent => {
                f.write_str("split predicate row count differs from its evaluation input")
            }
            Self::Arrow(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for SplitFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Arrow(error) => Some(error),
            _ => None,
        }
    }
}
impl novarocks_failure_contract::FailureProjection for SplitFailure {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{FailureClass, FailureFact, FailureReason, domain};
        let (class, code) = match self {
            Self::EmptyBranches => (FailureClass::Invariant, 110),
            Self::WrongArity => (FailureClass::Invariant, 111),
            Self::MissingBindings => (FailureClass::Invariant, 112),
            Self::RowExtent => (FailureClass::Representation, 113),
            Self::PredicateKind => (FailureClass::Invariant, 114),
            Self::MaskExtent => (FailureClass::Invariant, 115),
            Self::Arrow(_) => (FailureClass::Unexpected, 116),
        };
        FailureFact::new(
            class,
            FailureReason::new(domain::EXCHANGE, std::num::NonZeroU16::new(code).unwrap()),
        )
    }
}

#[cfg(test)]
fn split_chunk_by_exprs_with(
    split_exprs: &[ExprId],
    chunk: Chunk,
    fanout: bool,
    evaluate: &mut dyn FnMut(ExprId, &Chunk) -> ExecutionResult<arrow::array::ArrayRef>,
    filter_input: &mut dyn FnMut(&Chunk, &BooleanArray) -> ExecutionResult<Chunk>,
) -> ExecutionResult<Vec<Option<Chunk>>> {
    if split_exprs.is_empty() {
        return Ok(vec![]);
    }

    if fanout {
        // A row-mutation route is a filter, not an exclusive partition.  In
        // particular, a logical Replace must reach both the delete and
        // replacement-data routes.
        return split_exprs
            .iter()
            .map(|expr| {
                eval_split_mask(evaluate, *expr, &chunk)
                    .and_then(|mask| filter_chunk_by_mask(&chunk, &mask, filter_input))
            })
            .collect();
    }

    let mut out = vec![None; split_exprs.len()];
    let mut remaining = chunk;

    for idx in (1..split_exprs.len()).rev() {
        if remaining.is_empty() {
            break;
        }
        let mask = eval_split_mask(evaluate, split_exprs[idx], &remaining)?;
        let matched = mask.iter().filter(|flag| **flag).count();
        if matched == 0 {
            continue;
        }
        if matched == remaining.len() {
            out[idx] = Some(remaining);
            return Ok(out);
        }

        out[idx] = filter_chunk_by_mask(&remaining, &mask, filter_input)?;
        let remaining_mask = mask.into_iter().map(|flag| !flag).collect::<Vec<_>>();
        remaining = filter_chunk_by_mask(&remaining, &remaining_mask, filter_input)?
            .ok_or_else(|| split_failure(SplitFailure::MaskExtent))?;
    }

    if !remaining.is_empty() {
        let mask = eval_split_mask(evaluate, split_exprs[0], &remaining)?;
        out[0] = filter_chunk_by_mask(&remaining, &mask, filter_input)?;
    }
    Ok(out)
}
#[cfg(test)]
fn eval_split_mask(
    evaluate: &mut dyn FnMut(ExprId, &Chunk) -> ExecutionResult<arrow::array::ArrayRef>,
    expr_id: ExprId,
    chunk: &Chunk,
) -> ExecutionResult<Vec<bool>> {
    let predicate_array = evaluate(expr_id, chunk)?;
    let mask = predicate_array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| split_failure(SplitFailure::PredicateKind))?;
    if mask.len() != chunk.len() {
        return Err(split_failure(SplitFailure::MaskExtent));
    }

    let mut out = Vec::with_capacity(mask.len());
    for i in 0..mask.len() {
        out.push(mask.is_valid(i) && mask.value(i));
    }
    Ok(out)
}
#[cfg(test)]
fn filter_chunk_by_mask(
    chunk: &Chunk,
    mask: &[bool],
    filter_input: &mut dyn FnMut(&Chunk, &BooleanArray) -> ExecutionResult<Chunk>,
) -> ExecutionResult<Option<Chunk>> {
    if mask.len() != chunk.len() {
        return Err(split_failure(SplitFailure::MaskExtent));
    }

    let selected = mask.iter().filter(|flag| **flag).count();
    if selected == 0 {
        return Ok(None);
    }
    if selected == chunk.len() {
        return Ok(Some(chunk.clone()));
    }

    let predicate = BooleanArray::from(mask.to_vec());
    Ok(Some(filter_input(chunk, &predicate)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::chunk::ChunkSchema;
    use crate::exec::{
        expr::ExprNode,
        pipeline::{shape::StepOperator, shape::fanout::test_support as support, step::NextStep},
    };
    use crate::runtime::runtime_state::RuntimeState;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use novarocks_types::SlotId;
    fn fixture() -> Chunk {
        support::typed_chunk(vec![
            (
                SlotId::new(1),
                "first",
                DataType::Boolean,
                true,
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    None,
                    Some(true),
                    Some(false),
                ])),
            ),
            (
                SlotId::new(2),
                "second",
                DataType::Boolean,
                true,
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                ])),
            ),
            (
                SlotId::new(3),
                "value",
                DataType::Int32,
                false,
                Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])),
            ),
        ])
    }
    fn route(fanout: bool) -> [Vec<i32>; 2] {
        let mut arena = ExprArena::default();
        let first_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(1)), DataType::Boolean);
        let second_expr = arena.push_typed(ExprNode::SlotId(SlotId::new(2)), DataType::Boolean);
        let first = support::Probe::with_slot(true, SlotId::new(3));
        let second = support::Probe::with_slot(true, SlotId::new(3));
        let mut op = Fanout::new(
            "split routes".into(),
            vec![support::sink(first.clone()), support::sink(second.clone())],
            SplitRouter {
                arena: Arc::new(arena),
                expressions: vec![first_expr, second_expr],
                fanout,
            },
            None,
        );
        support::prepare(&mut op);
        assert!(matches!(
            support::until_wait_or_input(&mut op),
            NextStep::NeedInput
        ));
        support::push(&mut op, fixture());
        assert!(matches!(
            support::until_wait_or_input(&mut op),
            NextStep::NeedInput
        ));
        support::context(|ctx| op.finish(ctx)).unwrap();
        assert!(matches!(
            support::until_wait_or_input(&mut op),
            NextStep::Finished
        ));
        let rows = [
            first.rows.lock().unwrap().clone(),
            second.rows.lock().unwrap().clone(),
        ];
        rows
    }
    #[test]
    fn exclusive_split_preserves_reverse_precedence_first_predicate_and_channel_row_order() {
        assert_eq!(route(false), [vec![40], vec![10, 20, 30]]);
    }
    #[test]
    fn fanout_split_keeps_rows_required_by_both_routes_and_discards_unknown() {
        assert_eq!(route(true), [vec![10, 40], vec![10, 20, 30]]);
    }
    fn typed_split_fixture() -> (ExprArena, Vec<crate::exec::expr::ExprId>, Chunk) {
        use crate::exec::chunk::ChunkSlotSchema;
        use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
        let slot = SlotId::new(91);
        let ty = FunctionValueType::new(DataType::Boolean, true);
        let field = Arc::new(Field::new("flag", DataType::Boolean, true));
        let schema = Arc::new(
            ChunkSchema::try_new_typed(
                vec![
                    ChunkSlotSchema::try_new_typed(
                        slot,
                        field,
                        ty.typed_binding().clone(),
                        None,
                        ValueTypeStorageLifetime::pure(),
                    )
                    .unwrap(),
                ],
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        );
        let input = Chunk::try_new_with_chunk_schema(
            RecordBatch::try_new(
                schema.arrow_schema_ref(),
                vec![Arc::new(BooleanArray::from(vec![
                    Some(true),
                    None,
                    Some(false),
                ]))],
            )
            .unwrap(),
            schema,
        )
        .unwrap();
        let mut arena = ExprArena::default();
        let flag = arena.push_typed(ExprNode::SlotId(slot), DataType::Boolean);
        let inverse = arena.push_typed(ExprNode::Not(flag), DataType::Boolean);
        (arena, vec![inverse, flag], input)
    }

    #[test]
    fn actual_split_remaining_has_its_own_creator_and_original_null_routing() {
        let (arena, exprs, input) = typed_split_fixture();
        let state = RuntimeState::default();
        let factory =
            crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                crate::runtime::mem_tracker::MemTracker::new_root("split actual creator fixture"),
                state.error_state(),
            )
            .unwrap();
        let control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(
            state.error_state(),
        );
        let mut evaluations = Vec::new();
        let mut filters = Vec::new();
        let out = super::split_chunk_by_exprs_with(
            &exprs,
            input,
            false,
            &mut |expr, input| {
                evaluations.push((
                    expr,
                    input.len(),
                    input.construction_storage_lifetime().is_pure(),
                ));
                let owner = factory.begin_input_construction(input)?;
                crate::exec::expr::ExprEvaluationContext::new(&arena, owner.as_ref(), &control)
                    .eval(expr, input)
            },
            &mut |input, mask| {
                filters.push(input.len());
                let owner = factory.begin_input_construction(input)?;
                input.filter_for_evaluation(mask, owner.as_ref())
            },
        )
        .unwrap();
        assert_eq!(evaluations, vec![(exprs[1], 3, true), (exprs[0], 2, false)]);
        assert_eq!(filters, vec![3, 3, 2]);
        for (index, expected) in [(0, false), (1, true)] {
            let output = out[index].as_ref().unwrap();
            assert_eq!(output.len(), 1);
            assert!(!output.construction_storage_lifetime().is_pure());
            assert_eq!(
                output.columns()[0]
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(expected)]
            );
        }
    }

    #[test]
    fn split_filter_and_next_evaluation_preserve_the_original_runtime_failure() {
        use crate::runtime::fragment::{ExecutionFailure, PipelineOperation};
        use novarocks_functions::{KernelDiagnostic, KernelFailure};
        let mut failures: Vec<ExecutionFailure> = vec![
            KernelFailure::Cancelled,
            KernelFailure::DeadlineExceeded,
            KernelFailure::ResourceExhausted,
            KernelFailure::InstanceFailed,
            KernelFailure::InvalidProgram(KernelDiagnostic::new("exact invalid")),
            KernelFailure::Internal(KernelDiagnostic::new("exact internal")),
            KernelFailure::Operational(KernelDiagnostic::new("exact operation")),
        ]
        .into_iter()
        .map(Into::into)
        .collect();
        failures.push(
            ExecutionFailure::from(KernelFailure::Operational(KernelDiagnostic::new(
                "cancelled",
            )))
            .at_operator(47, PipelineOperation::Push),
        );
        for original in failures {
            let (arena, exprs, input) = typed_split_fixture();
            let state = RuntimeState::default();
            let factory =
                crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
                    crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
                    crate::runtime::mem_tracker::MemTracker::new_root("split failure fixture"),
                    state.error_state(),
                )
                .unwrap();
            let control = crate::exec::operators::compiled_expression::RuntimeKernelControl::new(
                state.error_state(),
            );
            let mut calls = 0;
            let failure = super::split_chunk_by_exprs_with(
                &exprs,
                input,
                false,
                &mut |expr, input| {
                    calls += 1;
                    assert_eq!(calls, 1, "no evaluation after originating failure");
                    let owner = factory.begin_input_construction(input)?;
                    let result = crate::exec::expr::ExprEvaluationContext::new(
                        &arena,
                        owner.as_ref(),
                        &control,
                    )
                    .eval(expr, input)?;
                    state.error_state().set_error(original.clone());
                    Ok(result)
                },
                &mut |input, mask| {
                    let owner = factory.begin_input_construction(input)?;
                    input.filter_for_evaluation(mask, owner.as_ref())
                },
            )
            .unwrap_err();
            assert_eq!(failure, original);
            assert_eq!(calls, 1);
        }
    }
}
