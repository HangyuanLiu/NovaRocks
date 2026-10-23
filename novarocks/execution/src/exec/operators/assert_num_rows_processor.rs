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
//! Assertion processor for row-count contract checks.
//!
//! Responsibilities:
//! - Validates row-count constraints required by ASSERT NUM ROWS semantics at runtime.
//! - Fails fast with explicit errors when produced row counts violate configured predicates.
//!
//! Key exported interfaces:
//! - Types: `AssertNumRowsProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, ResourceDimension, ResourceFacts,
    ResourceScope,
};
#[derive(Debug)]
enum AssertRowsFailure {
    Contract(BoundedDetail),
    Mismatch {
        actual: u64,
        expected: u64,
        predicate: Assertion,
        early: bool,
        detail: BoundedDetail,
    },
    DuplicateKey(BoundedDetail),
    CounterOverflow {
        current: u64,
        incoming: u64,
    },
}
impl std::fmt::Display for AssertRowsFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) | Self::DuplicateKey(detail) | Self::Mismatch { detail, .. } => {
                detail.fmt(f)
            }
            Self::CounterOverflow { current, incoming } => {
                write!(f, "assert row counter cannot add {incoming} to {current}")
            }
        }
    }
}
impl std::error::Error for AssertRowsFailure {}
impl FailureProjection for AssertRowsFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, reason) = match self {
            Self::Contract(_) => (FailureClass::Invariant, ExecutionReason::AssertRowsContract),
            Self::Mismatch { .. } => (FailureClass::Data, ExecutionReason::AssertRowsMismatch),
            Self::DuplicateKey(_) => (FailureClass::Data, ExecutionReason::AssertRowsDuplicateKey),
            Self::CounterOverflow { current, incoming } => {
                return FailureFact::new(
                    FailureClass::Resource,
                    ExecutionReason::AssertRowsCounter.reason(),
                )
                .with_resource(ResourceFacts {
                    request_scope: Some(ResourceScope::Operator),
                    constraint_scope: Some(ResourceScope::Operator),
                    dimension: ResourceDimension::Items,
                    requested: Some(*incoming),
                    required_growth: None,
                    limit: Some(u64::MAX),
                    live: Some(*current),
                    committed: None,
                });
            }
        };
        FailureFact::new(class, reason.reason())
    }
}
fn assert_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(AssertRowsFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::hash_table::allocation::{
    AggregateHashSet, AggregateVec, ExecutionStateAllocator, aggregate_hash_set,
};
use crate::exec::hash_table::key_builder::encode_canonical_tuple_row_tracked;
use crate::runtime::mem_tracker::MemTracker;
use std::sync::{Arc, Mutex};

use arrow::array::Array;
use arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::exec::chunk::Chunk;
use crate::exec::node::assert::{AssertNumRowsMode, Assertion};
use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
    step_context::StepContext,
};
use crate::runtime::runtime_state::RuntimeState;
use novarocks_types::SlotId;

/// Factory for processors that enforce ASSERT NUM ROWS runtime constraints.
pub struct AssertNumRowsProcessorFactory {
    name: String,
    mode: AssertNumRowsMode,
    applies: Option<SlotId>,
    budget: SharedAssertionBudget,
}

impl AssertNumRowsProcessorFactory {
    pub fn new(node_id: i32, mode: AssertNumRowsMode) -> ExecutionResult<Self> {
        RuntimeAssertNumRowsMode::validate_plan_mode(&mode)?;

        let name = if node_id >= 0 {
            format!("AssertNumRows (id={node_id})")
        } else {
            "AssertNumRows".to_string()
        };
        Ok(Self {
            name,
            mode,
            applies: None,
            budget: Arc::new(Mutex::new(None)),
        })
    }
}

/// A factory is one Task/assertion owner. Drivers share its budget, while
/// correctly colocated keys remain driver-local.
struct AssertionBudget {
    parent: Arc<MemTracker>,
    tracker: Arc<MemTracker>,
    limit: usize,
}
type SharedAssertionBudget = Arc<Mutex<Option<AssertionBudget>>>;

fn bind_assertion_budget(
    shared: &SharedAssertionBudget,
    parent: Arc<MemTracker>,
    limit: usize,
) -> ExecutionResult<()> {
    let signed = i64::try_from(limit)
        .map_err(|_| assert_contract("keyed assertion state limit exceeds signed byte extent"))?;
    if signed <= 0 {
        return Err(assert_contract(
            "keyed assertion state limit must be positive",
        ));
    }
    let mut binding = shared.lock().expect("assertion budget lock");
    if let Some(existing) = binding.as_ref() {
        if !Arc::ptr_eq(&existing.parent, &parent) || existing.limit != limit {
            return Err(assert_contract(
                "keyed assertion budget was rebound to a different Task or limit",
            ));
        }
        return Ok(());
    }
    let tracker = MemTracker::new_child("keyed_assertion_state", &parent);
    tracker
        .install_limit_once(signed)
        .map_err(ExecutionFailure::from_domain)?;
    *binding = Some(AssertionBudget {
        parent,
        tracker,
        limit,
    });
    Ok(())
}

impl AssertNumRowsProcessorFactory {
    /// Bind the original Task tracker and exact runtime limit before creating drivers.
    pub fn new_bound(
        node_id: i32,
        mode: AssertNumRowsMode,
        applies: Option<SlotId>,
        parent: Arc<MemTracker>,
        limit: usize,
    ) -> ExecutionResult<Self> {
        let mut factory = Self::new(node_id, mode)?;
        if applies.is_some() && !matches!(factory.mode, AssertNumRowsMode::PerKeyAtMostOne { .. }) {
            return Err(assert_contract(
                "conditional applicability requires a keyed assertion",
            ));
        }
        factory.applies = applies;
        bind_assertion_budget(&factory.budget, parent, limit)?;
        Ok(factory)
    }
}

impl OperatorFactory for AssertNumRowsProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        let mut mode = RuntimeAssertNumRowsMode::from_plan(&self.mode)
            .expect("validated assert_num_rows processor mode");
        if let RuntimeAssertNumRowsMode::PerKeyAtMostOne {
            budget, applies, ..
        } = &mut mode
        {
            *budget = self.budget.clone();
            *applies = self.applies;
        }
        Box::new(Transform::hydrating(AssertNumRowsProcessorKernel {
            name: self.name.clone(),
            mode,
        }))
    }
}

struct AssertNumRowsProcessorKernel {
    name: String,
    mode: RuntimeAssertNumRowsMode,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum KeyValue {
    Null,
    NonNull { data_type: String, display: String },
}

#[cfg(test)]
impl KeyValue {
    fn message_value(&self) -> String {
        match self {
            KeyValue::Null => "<NULL>".to_string(),
            KeyValue::NonNull { data_type, display } if is_string_key_type(data_type) => {
                format!("{display:?}")
            }
            KeyValue::NonNull { display, .. } => display.clone(),
        }
    }
}

enum RuntimeAssertNumRowsMode {
    Global {
        desired_num_rows: Option<u64>,
        assertion: Assertion,
        subquery_string: Option<String>,
        rows_seen: u64,
        done: bool,
    },
    PerKeyAtMostOne {
        key_slots: Vec<SlotId>,
        key_labels: Vec<String>,
        message_prefix: String,
        seen_keys: Option<AggregateHashSet<AggregateVec<u8>>>,
        key_schema: Option<ChunkSchemaRef>,
        budget: SharedAssertionBudget,
        applies: Option<SlotId>,
    },
}

impl RuntimeAssertNumRowsMode {
    fn from_plan(mode: &AssertNumRowsMode) -> ExecutionResult<Self> {
        Self::validate_plan_mode(mode)?;

        Ok(match mode {
            AssertNumRowsMode::Global {
                desired_num_rows,
                assertion,
                subquery_string,
            } => Self::Global {
                desired_num_rows: desired_num_rows.map(|v| v as u64),
                assertion: assertion.clone(),
                subquery_string: subquery_string.clone(),
                rows_seen: 0,
                done: false,
            },
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots,
                key_labels,
                message_prefix,
            } => Self::PerKeyAtMostOne {
                key_slots: key_slots.clone(),
                key_labels: key_labels.clone(),
                message_prefix: message_prefix.clone(),
                seen_keys: None,
                key_schema: None,
                budget: Arc::new(Mutex::new(None)),
                applies: None,
            },
        })
    }

    fn validate_plan_mode(mode: &AssertNumRowsMode) -> ExecutionResult<()> {
        if let AssertNumRowsMode::PerKeyAtMostOne {
            key_slots,
            key_labels,
            ..
        } = mode
        {
            validate_keyed_invariants(key_slots, key_labels)?;
        }
        Ok(())
    }

    fn check_final(&mut self) -> ExecutionResult<()> {
        let Self::Global {
            desired_num_rows,
            assertion,
            subquery_string,
            rows_seen,
            done,
        } = self
        else {
            return Ok(());
        };

        if *done {
            return Ok(());
        }

        let Some(desired) = *desired_num_rows else {
            // No assertion configured, treat as no-op.
            *done = true;
            return Ok(());
        };
        let actual = *rows_seen;
        let ok = match *assertion {
            Assertion::Eq => actual == desired,
            Assertion::Ne => actual != desired,
            Assertion::Lt => actual < desired,
            Assertion::Le => actual <= desired,
            Assertion::Gt => actual > desired,
            Assertion::Ge => actual >= desired,
        };
        if ok {
            *done = true;
            return Ok(());
        }

        let op_str = match *assertion {
            Assertion::Eq => "=",
            Assertion::Ne => "!=",
            Assertion::Lt => "<",
            Assertion::Le => "<=",
            Assertion::Gt => ">",
            Assertion::Ge => ">=",
        };
        let base = format!(
            "assert_num_rows failed: actual={} row(s), expected {} {} row(s)",
            actual, op_str, desired
        );
        let msg = if let Some(sql) = subquery_string.as_ref() {
            format!("subquery '{}' {}", sql, base)
        } else {
            base
        };
        Err(ExecutionFailure::from_domain(AssertRowsFailure::Mismatch {
            actual,
            expected: desired,
            predicate: assertion.clone(),
            early: false,
            detail: BoundedDetail::truncating(&msg),
        }))
    }

    fn observe_chunk(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
        match self {
            Self::Global {
                desired_num_rows,
                assertion,
                subquery_string,
                rows_seen,
                done,
            } => {
                if *done {
                    return Ok(());
                }

                let rows = chunk.len() as u64;
                if rows == 0 {
                    return Ok(());
                }

                *rows_seen = rows_seen.checked_add(rows).ok_or_else(|| {
                    ExecutionFailure::from_domain(AssertRowsFailure::CounterOverflow {
                        current: *rows_seen,
                        incoming: rows,
                    })
                })?;
                Self::maybe_early_fail(
                    *desired_num_rows,
                    assertion,
                    subquery_string.as_ref(),
                    *rows_seen,
                )
            }
            Self::PerKeyAtMostOne {
                key_slots,
                key_labels,
                message_prefix,
                seen_keys,
                key_schema,
                budget,
                applies,
            } => {
                let mask = applies
                    .map(|slot| {
                        chunk
                            .column_by_slot_id(slot)
                            .map_err(ExecutionFailure::from)
                    })
                    .transpose()?;
                let mask = mask
                    .as_ref()
                    .map(|column| {
                        column
                            .as_any()
                            .downcast_ref::<arrow::array::BooleanArray>()
                            .ok_or_else(|| {
                                assert_contract("keyed assertion applicability is not Boolean")
                            })
                    })
                    .transpose()?;
                for row in 0..chunk.len() {
                    if mask.is_some_and(|values| values.is_null(row) || !values.value(row)) {
                        continue;
                    }
                    let tracker = budget
                        .lock()
                        .expect("assertion budget lock")
                        .as_ref()
                        .map(|binding| binding.tracker.clone())
                        .ok_or_else(|| {
                            assert_contract("keyed assertion has no bound Task state budget")
                        })?;
                    let allocator = ExecutionStateAllocator::new_nominal(tracker);
                    if let Some(original) = key_schema.as_ref() {
                        for slot in key_slots.iter() {
                            let expected = original.slot(*slot).ok_or_else(|| {
                                assert_contract(
                                    "keyed assertion key is absent from its retained schema",
                                )
                            })?;
                            let actual = chunk.chunk_schema().slot(*slot).ok_or_else(|| {
                                assert_contract("keyed assertion key is absent from input")
                            })?;
                            if expected.field_schema() != actual.field_schema() {
                                return Err(assert_contract(
                                    "keyed assertion input key type changed",
                                ));
                            }
                        }
                    } else {
                        *key_schema = Some(chunk.chunk_schema_ref());
                    }
                    if applies.is_some() {
                        for slot in key_slots.iter() {
                            let column = chunk.column_by_slot_id(*slot)?;
                            if column.is_null(row) {
                                return Err(assert_contract(
                                    "selected matched action has no target key",
                                ));
                            }
                        }
                    }
                    let columns = key_slots.iter().map(|slot| {
                        let index = chunk.chunk_schema().index_of(*slot).ok_or_else(|| {
                            assert_contract("keyed assertion key is absent from input")
                        })?;
                        Ok(&chunk.columns()[index])
                    });
                    let key = encode_canonical_tuple_row_tracked(columns, row, allocator.clone())?;
                    let keys =
                        seen_keys.get_or_insert_with(|| aggregate_hash_set(allocator.clone()));
                    if keys.contains(&key) {
                        return Err(ExecutionFailure::from_domain(
                            AssertRowsFailure::DuplicateKey(BoundedDetail::truncating_display(
                                DuplicateKeyDisplay {
                                    prefix: message_prefix,
                                    labels: key_labels,
                                    slots: key_slots,
                                    chunk,
                                    row,
                                },
                            )),
                        ));
                    }
                    keys.try_reserve(1).map_err(|source| {
                        allocator.allocation_error("reserve keyed assertion table", source)
                    })?;
                    keys.insert(key);
                }
                Ok(())
            }
        }
    }

    fn maybe_early_fail(
        desired_num_rows: Option<u64>,
        assertion: &Assertion,
        subquery_string: Option<&String>,
        rows_seen: u64,
    ) -> ExecutionResult<()> {
        let Some(desired) = desired_num_rows else {
            return Ok(());
        };
        let actual = rows_seen;

        let must_fail = match assertion {
            Assertion::Eq | Assertion::Le => actual > desired,
            Assertion::Lt => actual >= desired,
            // For these, we can't be sure until EOS.
            Assertion::Ne | Assertion::Gt | Assertion::Ge => false,
        };

        if !must_fail {
            return Ok(());
        }

        let op_str = match assertion {
            Assertion::Eq => "=",
            Assertion::Ne => "!=",
            Assertion::Lt => "<",
            Assertion::Le => "<=",
            Assertion::Gt => ">",
            Assertion::Ge => ">=",
        };
        let base = format!(
            "assert_num_rows failed (early): actual={} row(s), expected {} {} row(s)",
            actual, op_str, desired
        );
        let msg = if let Some(sql) = subquery_string {
            format!("subquery '{}' {}", sql, base)
        } else {
            base
        };
        Err(ExecutionFailure::from_domain(AssertRowsFailure::Mismatch {
            actual,
            expected: desired,
            predicate: assertion.clone(),
            early: true,
            detail: BoundedDetail::truncating(&msg),
        }))
    }
}

struct DuplicateKeyDisplay<'a> {
    prefix: &'a str,
    labels: &'a [String],
    slots: &'a [SlotId],
    chunk: &'a Chunk,
    row: usize,
}
impl std::fmt::Display for DuplicateKeyDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: duplicate ", self.prefix)?;
        for (index, slot) in self.slots.iter().enumerate() {
            if index != 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}=", self.labels[index])?;
            let Ok(column) = self.chunk.column_by_slot_id(*slot) else {
                return f.write_str("<unavailable>");
            };
            if column.is_null(self.row) {
                f.write_str("<NULL>")?;
                continue;
            }
            match column.data_type() {
                arrow::datatypes::DataType::Utf8 => write!(
                    f,
                    "{:?}",
                    column
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or(std::fmt::Error)?
                        .value(self.row)
                )?,
                arrow::datatypes::DataType::LargeUtf8 => write!(
                    f,
                    "{:?}",
                    column
                        .as_any()
                        .downcast_ref::<arrow::array::LargeStringArray>()
                        .ok_or(std::fmt::Error)?
                        .value(self.row)
                )?,
                _ => {
                    let options = FormatOptions::default();
                    match ArrayFormatter::try_new(column.as_ref(), &options) {
                        Ok(formatter) => write!(f, "{}", formatter.value(self.row))?,
                        Err(_) => f.write_str("<unavailable>")?,
                    }
                }
            }
        }
        Ok(())
    }
}

fn validate_keyed_invariants(key_slots: &[SlotId], key_labels: &[String]) -> ExecutionResult<()> {
    if key_slots.is_empty() {
        return Err(assert_contract(
            "keyed assert_num_rows requires at least one key slot",
        ));
    }
    if key_labels.len() != key_slots.len() {
        return Err(assert_contract(format_args!(
            "keyed assert_num_rows key_labels length mismatch: key_slots={} labels={}",
            key_slots.len(),
            key_labels.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
fn format_key_message(labels: &[String], key: &[KeyValue]) -> ExecutionResult<String> {
    if labels.len() != key.len() {
        return Err(assert_contract(format_args!(
            "keyed assert_num_rows key_labels length mismatch: key_values={} labels={}",
            key.len(),
            labels.len()
        )));
    }

    Ok(key
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let label = labels[idx].as_str();
            format!("{}={}", label, value.message_value())
        })
        .collect::<Vec<_>>()
        .join(", "))
}

#[cfg(test)]
fn is_string_key_type(data_type: &str) -> bool {
    matches!(data_type, "Utf8" | "LargeUtf8" | "Utf8View")
}

impl OperatorLifecycle for AssertNumRowsProcessorKernel {
    fn name(&self) -> &str {
        &self.name
    }
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        if let RuntimeAssertNumRowsMode::PerKeyAtMostOne { budget, .. } = &mut self.mode {
            let parent = state.mem_tracker().ok_or_else(|| {
                assert_contract("keyed assertion requires the original Task tracker")
            })?;
            let runtime = state.execution_runtime().ok_or_else(|| {
                assert_contract("keyed assertion requires the bound execution runtime")
            })?;
            bind_assertion_budget(
                budget,
                parent,
                runtime.config().keyed_assertion_state_max_bytes,
            )?;
        }
        Ok(())
    }
    fn cancel(&mut self) {
        if let RuntimeAssertNumRowsMode::PerKeyAtMostOne {
            seen_keys,
            key_schema,
            ..
        } = &mut self.mode
        {
            *seen_keys = None;
            *key_schema = None;
        }
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.cancel();
        Ok(())
    }
}
impl InputEncodingPolicy for AssertNumRowsProcessorKernel {}
impl TransformKernel for AssertNumRowsProcessorKernel {
    fn apply(
        &mut self,
        _: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        self.mode.observe_chunk(&chunk)?;
        Ok(StepResult::new(Some(chunk), NextStep::NeedInput, rows))
    }
    fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        self.mode.check_final()?;
        Ok(StepResult::new(None, NextStep::Finished, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::test_driver::TestDriver;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    fn make_key_chunk(values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_row_id",
            DataType::Int32,
            true,
        )]));
        let array = Arc::new(Int32Array::from(values)) as _;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
            batch.schema().as_ref(),
            &[SlotId::new(7)],
        )
        .expect("chunk schema");
        Chunk::new_with_chunk_schema(batch, chunk_schema)
    }

    fn make_chunk(rows: usize) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));
        let data: Vec<i32> = (0..rows as i32).collect();
        let array = Arc::new(Int32Array::from(data)) as _;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn run_ok(
        desired: Option<usize>,
        assertion: Assertion,
        chunks: &[usize],
    ) -> ExecutionResult<()> {
        let rt = RuntimeState::default();
        let mut op = Transform::hydrating(AssertNumRowsProcessorKernel {
            name: "test".to_string(),
            mode: RuntimeAssertNumRowsMode::from_plan(&AssertNumRowsMode::Global {
                desired_num_rows: desired,
                assertion,
                subquery_string: Some("select c1 from test".to_string()),
            })
            .expect("runtime mode"),
        });
        let mut calls = TestDriver::new(&mut op, &rt);
        for &n in chunks {
            let _ = calls.input_and_collect(make_chunk(n))?;
        }
        calls.signal_eos()?;
        let _ = calls.collect_until_finished()?;
        Ok(())
    }

    fn run_err(
        desired: Option<usize>,
        assertion: Assertion,
        chunks: &[usize],
    ) -> crate::runtime::fragment::ExecutionFailure {
        let rt = RuntimeState::default();
        let mut op = Transform::hydrating(AssertNumRowsProcessorKernel {
            name: "test".to_string(),
            mode: RuntimeAssertNumRowsMode::from_plan(&AssertNumRowsMode::Global {
                desired_num_rows: desired,
                assertion,
                subquery_string: Some("select c1 from test".to_string()),
            })
            .expect("runtime mode"),
        });
        let mut calls = TestDriver::new(&mut op, &rt);
        for &n in chunks {
            if let Err(error) = calls.input_and_collect(make_chunk(n)) {
                return error;
            }
        }
        if let Err(error) = calls.signal_eos() {
            return error;
        }
        calls
            .collect_until_finished()
            .expect_err("the asserted row constraint must fail")
    }

    #[test]
    fn assert_eq_pass_and_fail() {
        // desired = 1, actual = 1 -> ok
        run_ok(Some(1), Assertion::Eq, &[1]).expect("eq pass");

        // desired = 1, actual = 2 -> early fail
        let msg = run_err(Some(1), Assertion::Eq, &[1, 1]);
        assert!(msg.detail().contains("assert_num_rows failed"));
        assert!(msg.detail().contains("expected = 1 row(s)"));
    }

    #[test]
    fn assert_eq_early_error_matches_existing_text() {
        let msg = run_err(Some(1), Assertion::Eq, &[2]);
        assert_eq!(
            msg.detail(),
            "subquery 'select c1 from test' assert_num_rows failed (early): actual=2 row(s), expected = 1 row(s)"
        );
    }

    #[test]
    fn assert_le_early_fail() {
        // desired <= 2, actual = 2 -> ok
        run_ok(Some(2), Assertion::Le, &[1, 1]).expect("le pass");

        // desired <= 2, actual = 3 -> early fail
        let msg = run_err(Some(2), Assertion::Le, &[2, 1]);
        assert!(msg.detail().contains("failed (early)"));
    }

    #[test]
    fn assert_lt_and_ge_finalize() {
        // LT: desired = 2, actual = 1 -> ok
        run_ok(Some(2), Assertion::Lt, &[1]).expect("lt pass");

        // LT: desired = 2, actual = 2 -> early fail
        let msg = run_err(Some(2), Assertion::Lt, &[1, 1]);
        assert!(msg.detail().contains("failed (early)"));

        // GE: desired >= 2, actual = 2 -> ok, checked at finish()
        run_ok(Some(2), Assertion::Ge, &[1, 1]).expect("ge pass");
    }

    #[test]
    fn assert_ge_final_error_matches_existing_text() {
        let msg = run_err(Some(2), Assertion::Ge, &[1]);
        assert_eq!(
            msg.detail(),
            "subquery 'select c1 from test' assert_num_rows failed: actual=1 row(s), expected >= 2 row(s)"
        );
    }

    #[test]
    fn keyed_assert_num_rows_factory_rejects_empty_key_slots() {
        let err = match AssertNumRowsProcessorFactory::new(
            11,
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots: vec![],
                key_labels: vec![],
                message_prefix: "assert_num_rows failed".to_string(),
            },
        ) {
            Ok(_) => panic!("empty keyed slots should fail"),
            Err(err) => err,
        };
        assert_eq!(
            err.detail(),
            "keyed assert_num_rows requires at least one key slot"
        );
    }

    #[test]
    fn keyed_assert_num_rows_factory_rejects_key_label_mismatch() {
        let err = match AssertNumRowsProcessorFactory::new(
            11,
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots: vec![SlotId::new(7)],
                key_labels: vec![],
                message_prefix: "assert_num_rows failed".to_string(),
            },
        ) {
            Ok(_) => panic!("key label mismatch should fail"),
            Err(err) => err,
        };
        assert_eq!(
            err.detail(),
            "keyed assert_num_rows key_labels length mismatch: key_slots=1 labels=0"
        );
    }

    #[test]
    fn keyed_assert_num_rows_fails_on_second_key() {
        let rt = RuntimeState::default();
        let op = AssertNumRowsProcessorFactory::new(
            11,
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots: vec![SlotId::new(7)],
                key_labels: vec!["_row_id".to_string()],
                message_prefix: "assert_num_rows failed".to_string(),
            },
        )
        .expect("factory");
        bind_assertion_budget(
            &op.budget,
            MemTracker::new_root("assertion fixture"),
            64 * 1024 * 1024,
        )
        .unwrap();
        let mut op = op.create(1, 0);
        let mut op = TestDriver::new(op.as_mut(), &rt);

        let err = op
            .input_and_collect(make_key_chunk(vec![7, 7]))
            .expect_err("duplicate key should fail");
        assert_eq!(err.detail(), "assert_num_rows failed: duplicate _row_id=7");
    }

    #[test]
    fn keyed_assert_num_rows_fails_on_duplicate_key_across_chunks() {
        let rt = RuntimeState::default();
        let op = AssertNumRowsProcessorFactory::new(
            11,
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots: vec![SlotId::new(7)],
                key_labels: vec!["_row_id".to_string()],
                message_prefix: "assert_num_rows failed".to_string(),
            },
        )
        .expect("factory");
        bind_assertion_budget(
            &op.budget,
            MemTracker::new_root("assertion fixture"),
            64 * 1024 * 1024,
        )
        .unwrap();
        let mut op = op.create(1, 0);
        let mut op = TestDriver::new(op.as_mut(), &rt);

        let output = op
            .input_and_collect(make_key_chunk(vec![7]))
            .expect("first key");
        assert_eq!(output.len(), 1);

        let err = op
            .input_and_collect(make_key_chunk(vec![7]))
            .expect_err("duplicate key across chunks should fail");
        assert_eq!(err.detail(), "assert_num_rows failed: duplicate _row_id=7");
    }

    #[test]
    fn keyed_assert_num_rows_allows_distinct_keys_across_chunks() {
        let rt = RuntimeState::default();
        let op = AssertNumRowsProcessorFactory::new(
            11,
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots: vec![SlotId::new(7)],
                key_labels: vec!["_row_id".to_string()],
                message_prefix: "assert_num_rows failed".to_string(),
            },
        )
        .expect("factory");
        bind_assertion_budget(
            &op.budget,
            MemTracker::new_root("assertion fixture"),
            64 * 1024 * 1024,
        )
        .unwrap();
        let mut op = op.create(1, 0);
        let mut op = TestDriver::new(op.as_mut(), &rt);

        let output = op
            .input_and_collect(make_key_chunk(vec![7]))
            .expect("first key");
        assert_eq!(output.len(), 1);
        let output = op
            .input_and_collect(make_key_chunk(vec![8]))
            .expect("second key");
        assert_eq!(output.len(), 1);
        op.signal_eos().expect("signal EOS");
        assert!(
            op.collect_until_finished()
                .expect("final constraint")
                .is_empty()
        );
    }

    #[test]
    fn keyed_assert_num_rows_formats_sql_null_and_string_null_distinctly() {
        let labels = vec!["_row_id".to_string()];
        let sql_null = vec![KeyValue::Null];
        let string_null = vec![KeyValue::NonNull {
            data_type: "Utf8".to_string(),
            display: "NULL".to_string(),
        }];

        assert_eq!(
            format_key_message(&labels, &sql_null).expect("format sql null"),
            "_row_id=<NULL>"
        );
        assert_eq!(
            format_key_message(&labels, &string_null).expect("format string null"),
            "_row_id=\"NULL\""
        );
    }

    #[test]
    fn keyed_assert_num_rows_format_key_message_rejects_label_mismatch() {
        let err = format_key_message(&[], &[KeyValue::Null]).expect_err("label mismatch");
        assert_eq!(
            err.detail(),
            "keyed assert_num_rows key_labels length mismatch: key_values=1 labels=0"
        );
    }
}

#[cfg(test)]
mod failure_fact_tests {
    use super::*;
    use arrow::{array::RecordBatch, datatypes::Schema};
    use std::sync::Arc;

    fn one_row() -> Chunk {
        let schema = Arc::new(Schema::empty());
        let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(1));
        let batch = RecordBatch::try_new_with_options(schema.clone(), vec![], &options).unwrap();
        let shape = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[])
            .unwrap();
        Chunk::try_new_with_chunk_schema(batch, shape).unwrap()
    }

    #[test]
    fn actual_row_constraint_and_counter_refusal_have_different_domains_and_atomic_state() {
        let mut mode = RuntimeAssertNumRowsMode::from_plan(&AssertNumRowsMode::Global {
            desired_num_rows: Some(0),
            assertion: Assertion::Eq,
            subquery_string: Some("ResourceExhausted".into()),
        })
        .unwrap();
        let error = mode.observe_chunk(&one_row()).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Data);
        assert_eq!(fact.reason(), ExecutionReason::AssertRowsMismatch.reason());
        assert_eq!(fact.resource(), None);
        let mut mode = RuntimeAssertNumRowsMode::Global {
            desired_num_rows: Some(u64::MAX),
            assertion: Assertion::Ge,
            subquery_string: None,
            rows_seen: u64::MAX,
            done: false,
        };
        let error = mode.observe_chunk(&one_row()).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason(), ExecutionReason::AssertRowsCounter.reason());
        let resource = fact.resource().unwrap();
        assert_eq!(resource.requested, Some(1));
        assert_eq!(resource.live, Some(u64::MAX));
        assert_eq!(resource.limit, Some(u64::MAX));
        assert!(matches!(
            mode,
            RuntimeAssertNumRowsMode::Global {
                rows_seen: u64::MAX,
                done: false,
                ..
            }
        ));
    }
}

#[cfg(test)]
#[path = "assert_num_rows_processor/typed_state_tests.rs"]
mod typed_state_tests;
