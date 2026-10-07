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
//! Private resumable JSON IN-list pair policy. Membership never calls ExprArena
//! once per candidate. Parser implementation follows this frozen interface.
//!
//! P28 supplies JsonPairCursor with these concrete methods:
//! new(&JsonPairTask) -> Self; start(JsonPairInput) -> Result<(), JsonPairError>;
//! poll(JsonPairInput, JsonPairContext, &mut JsonPairWork) -> Result<JsonPairPoll, JsonPairError>;
//! clear(&mut self). The cursor owns only tracked flat state and input offsets;
//! each poll borrows immutable input anew and checks its owner-minted identity.
mod compare;
mod interface;
mod number;
#[cfg(test)]
pub(crate) mod test_support;
mod text;
mod tree;
pub(crate) use interface::*;

use crate::exec::expr::agg::AggregateVec;
use compare::{CompareCursor, CompareStep};
use novarocks_types::value::variant_json_cursor::{
    VariantJsonCursor, VariantJsonFrame, VariantJsonStep,
};
use text::{ParseStep, Parser};

/// The two memory backends share this closed pair policy. Conversion failure
/// selects raw text; execution failure is never an input to this decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JsonPairDecision {
    Unknown,
    Parsed,
    Raw,
}
pub(crate) fn json_pair_decision(
    lhs_null: bool,
    rhs_null: bool,
    lhs_valid: bool,
    rhs_valid: bool,
) -> JsonPairDecision {
    if lhs_null || rhs_null {
        JsonPairDecision::Unknown
    } else if lhs_valid && rhs_valid {
        JsonPairDecision::Parsed
    } else {
        JsonPairDecision::Raw
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Source {
    Text,
    Variant,
    Valid,
    Invalid,
}
struct Side {
    parser: Parser,
    variant: VariantJsonCursor,
    frames: AggregateVec<VariantJsonFrame>,
    source: Source,
    at: usize,
    pending: Option<Option<u8>>,
}
impl Side {
    fn new(task: &JsonPairTask) -> Self {
        Self {
            parser: Parser::new(task),
            variant: VariantJsonCursor::new(),
            frames: AggregateVec::new_in(task.allocator().clone()),
            source: Source::Text,
            at: 0,
            pending: None,
        }
    }
    fn done(&self) -> bool {
        matches!(self.source, Source::Valid | Source::Invalid)
    }
    fn step(&mut self, task: &JsonPairTask, input: &str) -> Result<(), JsonPairError> {
        if self.done() {
            return Ok(());
        }
        if let Some(byte) = self.pending {
            match self.parser.step(task, byte)? {
                ParseStep::Consumed => self.pending = None,
                ParseStep::Progress => {}
                ParseStep::Valid => {
                    self.source = Source::Valid;
                    self.pending = None;
                }
                ParseStep::Invalid => {
                    self.pending = None;
                    self.parser = Parser::new(task);
                    // The offset is read from the process snapshot only when a
                    // serialized Variant conversion actually starts; an
                    // expired snapshot fails the pair instead of selecting
                    // the raw-text comparison.
                    self.source = if self.source == Source::Text
                        && self
                            .variant
                            .start_resolving_offset(input.as_bytes(), || task.conversion_offset())?
                    {
                        Source::Variant
                    } else {
                        Source::Invalid
                    };
                }
            }
        } else if self.source == Source::Text {
            let byte = input.as_bytes().get(self.at).copied();
            if byte.is_some() {
                self.at += 1;
            }
            self.pending = Some(byte);
        } else {
            match self.variant.step(input.as_bytes(), self.frames.last_mut()) {
                VariantJsonStep::Byte(byte) => self.pending = Some(Some(byte)),
                VariantJsonStep::Progress => {}
                VariantJsonStep::Push(frame) => {
                    self.frames
                        .try_reserve(1)
                        .map_err(|_| task.allocation_error())?;
                    self.frames.push(frame);
                }
                VariantJsonStep::Pop => {
                    self.frames.pop();
                }
                VariantJsonStep::End => self.pending = Some(None),
                VariantJsonStep::Invalid => {
                    self.source = Source::Invalid;
                    self.parser = Parser::new(task);
                }
            }
        }
        Ok(())
    }
}
/// Task-owned scratch for exactly one immutable pair. clear/drop release the
/// actual final owners of every flat-tree, byte-pool and comparison allocation.
pub(crate) struct JsonPairCursor {
    task: JsonPairTask,
    id: Option<JsonPairInputId>,
    shape: [Option<usize>; 2],
    lhs: Side,
    rhs: Side,
    compare: CompareCursor,
    comparing: bool,
    raw_at: usize,
    ready: Option<JsonPairTruth>,
}
impl JsonPairCursor {
    pub(crate) fn new(task: &JsonPairTask) -> Self {
        Self {
            task: task.clone(),
            id: None,
            shape: [None, None],
            lhs: Side::new(task),
            rhs: Side::new(task),
            compare: CompareCursor::new(task),
            comparing: false,
            raw_at: 0,
            ready: None,
        }
    }
    pub(crate) fn start(&mut self, input: JsonPairInput<'_>) -> Result<(), JsonPairError> {
        self.clear();
        self.id = Some(input.id);
        self.shape = [input.lhs.map(str::len), input.rhs.map(str::len)];
        if input.lhs.is_none() || input.rhs.is_none() {
            self.ready = Some(JsonPairTruth::Unknown);
        }
        Ok(())
    }
    pub(crate) fn clear(&mut self) {
        self.lhs = Side::new(&self.task);
        self.rhs = Side::new(&self.task);
        self.compare = CompareCursor::new(&self.task);
        self.id = None;
        self.comparing = false;
        self.raw_at = 0;
        self.ready = None;
        self.shape = [None, None];
    }
    pub(crate) fn poll(
        &mut self,
        input: JsonPairInput<'_>,
        context: JsonPairContext<'_>,
        work: &mut JsonPairWork,
    ) -> Result<JsonPairPoll, JsonPairError> {
        if !self.task.same_owner(context.task) {
            return Err(JsonPairError::Contract(
                "JSON pair cursor crossed task memory owners",
            ));
        }
        if self.id != Some(input.id)
            || self.shape != [input.lhs.map(str::len), input.rhs.map(str::len)]
        {
            return Err(JsonPairError::Contract(
                "JSON pair cursor input identity changed",
            ));
        }
        context.check_running()?;
        loop {
            if let Some(ready) = self.ready {
                return Ok(JsonPairPoll::Ready(ready));
            }
            if !work.spend_one() {
                return Ok(JsonPairPoll::Yield);
            }
            context.check_running()?;
            let lhs = input.lhs.expect("non-null active pair");
            let rhs = input.rhs.expect("non-null active pair");
            if !self.lhs.done() {
                self.lhs.step(&self.task, lhs)?;
                continue;
            }
            // Even an invalid left side must not hide right-side refusal/cancel.
            if !self.rhs.done() {
                self.rhs.step(&self.task, rhs)?;
                continue;
            }
            match json_pair_decision(
                false,
                false,
                self.lhs.source == Source::Valid,
                self.rhs.source == Source::Valid,
            ) {
                JsonPairDecision::Unknown => unreachable!("SQL NULL resolves before parsing"),
                JsonPairDecision::Raw => {
                    if lhs.len() != rhs.len() {
                        self.ready = Some(JsonPairTruth::False);
                    } else if self.raw_at == lhs.len() {
                        self.ready = Some(JsonPairTruth::True);
                    } else if lhs.as_bytes()[self.raw_at] != rhs.as_bytes()[self.raw_at] {
                        self.ready = Some(JsonPairTruth::False);
                    } else {
                        self.raw_at += 1;
                    }
                }
                JsonPairDecision::Parsed => {
                    if !self.comparing {
                        self.compare.start(&self.task)?;
                        self.comparing = true;
                        continue;
                    }
                    match self.compare.step(
                        &self.task,
                        &self.lhs.parser.tree,
                        &self.rhs.parser.tree,
                    )? {
                        CompareStep::Progress => {}
                        CompareStep::Equal => self.ready = Some(JsonPairTruth::True),
                        CompareStep::Different => self.ready = Some(JsonPairTruth::False),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{
        EMPTY_METADATA, ascii_timestamp_variant, ascii_variant, runtime_with_offset_owner,
        shared_runtime, task_state, variant_array, variant_primitive, with_admission_witness,
    };
    use super::*;
    use crate::runtime::execution_runtime::ExecutionRuntime;
    use crate::runtime::local_offset::{
        LocalOffsetOwner, LocalOffsetStepRules, LocalRulesSource, unix_seconds_floor,
    };
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeState;
    use chrono::FixedOffset;
    use novarocks_execution_contract::{TaskFailureCategory, TaskIdentity};
    use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId};
    use novarocks_types::value::variant::VariantValue;
    use novarocks_types::{BackendProcessId, StageId, TaskId};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::{Duration, Instant, SystemTime};

    fn identity() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    /// Binds a task on the shared runtime with the production local offset
    /// owner, the same offset source the old IN-list path reads.
    pub(super) fn task(tracker: Arc<MemTracker>) -> JsonPairTask {
        bound_task(tracker, shared_runtime()).1
    }
    fn bound_task(
        tracker: Arc<MemTracker>,
        runtime: Arc<ExecutionRuntime>,
    ) -> (RuntimeState, JsonPairTask) {
        let state = task_state(identity(), tracker, Some(runtime));
        let task = JsonPairTask::try_bind(&state).unwrap();
        (state, task)
    }
    pub(super) fn limited_task() -> (Arc<MemTracker>, JsonPairTask) {
        let tracker = MemTracker::new_root("allocation-witness");
        tracker.install_limit_once(1_000_000).unwrap();
        let task = task(tracker.clone());
        (tracker, task)
    }
    pub(super) fn occupy_remaining(tracker: &MemTracker) -> (i64, i64) {
        let retained = tracker.current();
        let occupied = tracker.limit() - retained;
        tracker.consume_and_check_limit(occupied).unwrap();
        (retained, occupied)
    }
    pub(super) fn assert_refusal<T>(result: Result<T, JsonPairError>) {
        assert!(
            matches!(result,Err(JsonPairError::Failed(ref failure)) if matches!(failure.category(),TaskFailureCategory::CapacityRefused{..}))
        );
    }
    pub(super) fn release_occupation(tracker: &MemTracker, retained: i64, occupied: i64) {
        tracker.release(occupied);
        assert_eq!(
            tracker.current(),
            retained,
            "failed allocation changed retained owner charge"
        );
    }
    fn input<'a>(lhs: Option<&'a str>, rhs: Option<&'a str>) -> JsonPairInput<'a> {
        JsonPairInput {
            id: JsonPairInputId {
                probe_generation: 1,
                probe_row: 0,
                build_batch: 0,
                build_row: 0,
            },
            lhs,
            rhs,
        }
    }
    /// The old IN-list pair rule over the old conversion `convert`.
    fn decide(
        lhs: Option<&str>,
        rhs: Option<&str>,
        convert: impl Fn(&str) -> Option<serde_json::Value>,
    ) -> JsonPairTruth {
        let (Some(lhs), Some(rhs)) = (lhs, rhs) else {
            return JsonPairTruth::Unknown;
        };
        let a = convert(lhs);
        let b = convert(rhs);
        let equal = if a.is_some() && b.is_some() {
            a == b
        } else {
            lhs == rhs
        };
        if equal {
            JsonPairTruth::True
        } else {
            JsonPairTruth::False
        }
    }
    /// The unchanged old IN-list entry, which reads chrono `Local` itself.
    fn oracle(lhs: Option<&str>, rhs: Option<&str>) -> JsonPairTruth {
        decide(
            lhs,
            rhs,
            super::super::in_pred::json_value_from_text_or_variant,
        )
    }
    /// The old IN-list conversion given the conversion-start offset.
    fn oracle_at(lhs: Option<&str>, rhs: Option<&str>, offset: FixedOffset) -> JsonPairTruth {
        decide(lhs, rhs, |text| {
            super::super::in_pred::json_value_from_text_or_variant_at(text, offset)
        })
    }
    fn finish(
        cursor: &mut JsonPairCursor,
        task: &JsonPairTask,
        lhs: Option<&str>,
        rhs: Option<&str>,
        credit: usize,
    ) -> Result<(JsonPairTruth, usize), JsonPairError> {
        let stopped = AtomicBool::new(false);
        let mut yields = 0;
        cursor.start(input(lhs, rhs))?;
        for _ in 0..1_000_000 {
            let mut work = JsonPairWork::new(credit);
            match cursor.poll(
                input(lhs, rhs),
                JsonPairContext {
                    task,
                    stopped: &stopped,
                },
                &mut work,
            )? {
                JsonPairPoll::Yield => {
                    assert!(work.exhausted());
                    yields += 1;
                }
                JsonPairPoll::Ready(value) => return Ok((value, yields)),
            }
        }
        panic!("JSON pair cursor did not finish");
    }
    fn parity(lhs: &str, rhs: &str) {
        let tracker = MemTracker::new_root("pair-task");
        let task = task(tracker.clone());
        let mut cursor = JsonPairCursor::new(&task);
        for credit in [1, 7, 64, 65536] {
            let actual = finish(&mut cursor, &task, Some(lhs), Some(rhs), credit)
                .unwrap()
                .0;
            assert_eq!(
                actual,
                oracle(Some(lhs), Some(rhs)),
                "lhs={lhs:?} rhs={rhs:?} credit={credit}"
            );
        }
        cursor.clear();
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn json_in_pair_serde_boundary_and_duplicate_differential() {
        let cases = [
            "null",
            "true",
            "false",
            "0",
            "-0",
            "0.0",
            "-0.0",
            "1",
            "1.0",
            "1e0",
            "-1",
            "18446744073709551615",
            "18446744073709551616",
            "-9223372036854775808",
            "-9223372036854775809",
            "1.2345678901234567890123456789",
            "0.00000000000000000000000001",
            "1e308",
            "1e309",
            "1e-999999999999999999999999",
            "0e999999999999999999999999",
            "-0e999999999999999999999999",
            r#""é😀""#,
            r#""\u00e9\ud83d\ude00""#,
            r#""\ud800""#,
            r#""\udc00""#,
            r#""\ud800\u0041""#,
            "[]",
            "{}",
            r#"{"a":1,"a":2}"#,
            r#"{"a":2}"#,
            r#"{"a":1,"b":2}"#,
            r#"{"b":2,"a":1}"#,
            r#"{"a":0,"\u0061":2}"#,
            r#"{"a":[1,{"x":null}]}"#,
            r#"[1,null,2]"#,
            r#"[2,null,1]"#,
            "",
            "plain",
            " plain",
            "01",
            "-",
            "1.",
            "1e",
            "[1,]",
            "{\"a\":1,}",
            "null true",
            "\"raw\nnewline\"",
            "\"\\q\"",
        ];
        for a in cases {
            for b in cases {
                parity(a, b);
            }
        }
        assert_ne!(
            serde_json::from_str::<serde_json::Value>("1").unwrap(),
            serde_json::from_str::<serde_json::Value>("1.0").unwrap(),
            "serde number features drifted"
        );
    }
    #[test]
    fn json_in_pair_generated_number_text_differential() {
        for i in 0..100u64 {
            let digits = format!("{}{}{}", u64::MAX, i, i.wrapping_mul(7919));
            for number in [
                digits.clone(),
                format!("-{digits}"),
                format!("{digits}.12345678901234567890e-90"),
                format!("0.{digits}e300"),
                format!("{i}e-999999999999999999999"),
            ] {
                // The independent serde result is serialized as the comparison target.
                let rhs = serde_json::from_str::<serde_json::Value>(&number)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|_| number.clone());
                parity(&number, &rhs);
                parity(&number, &format!("{number} "));
                parity(&number, &format!("{number}x"));
            }
        }
    }
    #[test]
    fn json_in_pair_default_recursion_and_long_token_yield() {
        for depth in [126, 127, 128, 129] {
            let value = format!("{}null{}", "[".repeat(depth), "]".repeat(depth));
            parity(&value, &format!(" {value}"));
        }
        let lhs = format!(
            "{{\"{}\":\"{}\",\"a\":2}}",
            "é".repeat(1000),
            "x".repeat(3000)
        );
        let rhs = format!(
            "{{\"a\":2,\"{}\":\"{}\"}}",
            "é".repeat(1000),
            "x".repeat(3000)
        );
        parity(&lhs, &rhs);
        let tracker = MemTracker::new_root("pair-task");
        let task = task(tracker.clone());
        let mut cursor = JsonPairCursor::new(&task);
        let (_, yields) = finish(&mut cursor, &task, Some(&lhs), Some(&rhs), 1).unwrap();
        assert!(yields > lhs.len() + rhs.len());
        assert!(tracker.current() > 0);
        drop(cursor);
        assert_eq!(tracker.current(), 0);
        let raw = "not-json".repeat(1000);
        parity(&raw, &raw);
    }
    #[test]
    fn json_in_pair_variant_null_and_sql_null_are_distinct() {
        let null = std::str::from_utf8(&[4, 0, 0, 0, 1, 0, 0, 0]).unwrap();
        parity(null, "null");
        parity(null, "0");
        // The original UTF8 carrier also admits low-byte nested Variant values.
        for json in ["[null,true,false]", r#"{"x":[null,true]}"#, r#""é""#] {
            let bytes =
                novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes(json)
                    .unwrap();
            if let Ok(text) = std::str::from_utf8(&bytes) {
                parity(text, json);
                parity(text, text);
            }
        }

        let tracker = MemTracker::new_root("pair-task");
        let task = task(tracker.clone());
        let mut cursor = JsonPairCursor::new(&task);
        assert_eq!(
            finish(&mut cursor, &task, None, Some("null"), 1).unwrap().0,
            JsonPairTruth::Unknown
        );
        assert_eq!(tracker.current(), 0);
        parity("{invalid", "{invalid");
    }
    #[test]
    fn json_in_pair_capacity_grow_ancestor_and_invalid_side_are_not_fallback() {
        let value = format!("[\"{}\",{}]", "x".repeat(4000), "1,".repeat(200) + "2");
        for limit in [1, 64, 256, 1024, 4096, 8192, 16384] {
            let ancestor = MemTracker::new_root("query-budget");
            ancestor.install_limit_once(limit).unwrap();
            let tracker = MemTracker::new_child("pair-task", &ancestor);
            let task = task(tracker.clone());
            let mut cursor = JsonPairCursor::new(&task);
            let result = finish(&mut cursor, &task, Some("invalid"), Some(&value), 7);
            assert!(
                matches!(result,Err(JsonPairError::Failed(ref failure)) if matches!(failure.category(),TaskFailureCategory::CapacityRefused{..})),
                "limit={limit} result={result:?}"
            );
            assert!(ancestor.current() <= limit);
            cursor.clear();
            assert_eq!(tracker.current(), 0);
            assert_eq!(ancestor.current(), 0);
        }
    }
    #[test]
    fn json_in_pair_cancel_owner_identity_and_zero_credit() {
        let tracker = MemTracker::new_root("pair-task");
        let task = task(tracker.clone());
        let mut cursor = JsonPairCursor::new(&task);
        let stopped = AtomicBool::new(false);
        let text = format!("\"{}\"", "a".repeat(10000));
        cursor.start(input(Some(&text), Some(&text))).unwrap();
        assert_eq!(
            cursor
                .poll(
                    input(Some(&text), Some(&text)),
                    JsonPairContext {
                        task: &task,
                        stopped: &stopped
                    },
                    &mut JsonPairWork::new(0)
                )
                .unwrap(),
            JsonPairPoll::Yield
        );
        assert_eq!(tracker.current(), 0);
        assert_eq!(
            cursor
                .poll(
                    input(Some(&text), Some(&text)),
                    JsonPairContext {
                        task: &task,
                        stopped: &stopped
                    },
                    &mut JsonPairWork::new(100)
                )
                .unwrap(),
            JsonPairPoll::Yield
        );
        assert!(tracker.current() > 0);
        stopped.store(true, Ordering::Release);
        assert!(matches!(
            cursor.poll(
                input(Some(&text), Some(&text)),
                JsonPairContext {
                    task: &task,
                    stopped: &stopped
                },
                &mut JsonPairWork::new(1)
            ),
            Err(JsonPairError::Stopped)
        ));
        stopped.store(false, Ordering::Release);
        let foreign = super::tests::task(MemTracker::new_root("foreign"));
        assert!(matches!(
            cursor.poll(
                input(Some(&text), Some(&text)),
                JsonPairContext {
                    task: &foreign,
                    stopped: &stopped
                },
                &mut JsonPairWork::new(1)
            ),
            Err(JsonPairError::Contract(_))
        ));
        let mut changed = input(Some(&text), Some(&text));
        changed.id.build_row = 1;
        assert!(matches!(
            cursor.poll(
                changed,
                JsonPairContext {
                    task: &task,
                    stopped: &stopped
                },
                &mut JsonPairWork::new(1)
            ),
            Err(JsonPairError::Contract(_))
        ));
        cursor.clear();
        assert_eq!(tracker.current(), 0);
    }
    #[test]
    fn json_in_pair_every_allocation_turn_replays_typed_refusal() {
        let lhs = r#"{"a":["long decoded string with \u00e9",{"deep":[1,2,3]}],"a":[true,false],"b":{"x":null}}"#;
        let rhs = r#"{"b":{"x":null},"a":[true,false]}"#;
        every_allocation_turn_replays_typed_refusal(lhs, rhs);
    }
    /// Refuses each allocating turn of one full pair in turn and requires the
    /// typed capacity cause, never a parse result.
    fn every_allocation_turn_replays_typed_refusal(lhs: &str, rhs: &str) {
        let (tracker, task) = limited_task();
        let mut cursor = JsonPairCursor::new(&task);
        cursor.start(input(Some(lhs), Some(rhs))).unwrap();
        let stopped = AtomicBool::new(false);
        let mut events = Vec::new();
        for turn in 0..100_000 {
            let before = tracker.allocated();
            let result = cursor
                .poll(
                    input(Some(lhs), Some(rhs)),
                    JsonPairContext {
                        task: &task,
                        stopped: &stopped,
                    },
                    &mut JsonPairWork::new(1),
                )
                .unwrap();
            if tracker.allocated() != before {
                events.push(turn);
            }
            if matches!(result, JsonPairPoll::Ready(_)) {
                break;
            }
        }
        assert!(events.len() >= 10);
        cursor.clear();
        assert_eq!(tracker.current(), 0);
        for event in events {
            let (tracker, task) = limited_task();
            let mut cursor = JsonPairCursor::new(&task);
            cursor.start(input(Some(lhs), Some(rhs))).unwrap();
            for _ in 0..event {
                assert_eq!(
                    cursor
                        .poll(
                            input(Some(lhs), Some(rhs)),
                            JsonPairContext {
                                task: &task,
                                stopped: &stopped
                            },
                            &mut JsonPairWork::new(1)
                        )
                        .unwrap(),
                    JsonPairPoll::Yield
                );
            }
            let (retained, occupied) = occupy_remaining(&tracker);
            assert_refusal(cursor.poll(
                input(Some(lhs), Some(rhs)),
                JsonPairContext {
                    task: &task,
                    stopped: &stopped,
                },
                &mut JsonPairWork::new(1),
            ));
            release_occupation(&tracker, retained, occupied);
            cursor.clear();
            assert_eq!(tracker.current(), 0);
            drop(cursor);
            assert_eq!(tracker.current(), 0);
        }
    }
    #[test]
    fn json_in_pair_variant_frame_first_and_growth_refusal() {
        let json = format!("{}null{}", "[".repeat(12), "]".repeat(12));
        let raw = novarocks_types::value::variant_encode::encode_json_text_to_variant_bytes(&json)
            .unwrap();
        let text = std::str::from_utf8(&raw).expect("low-byte Variant is a real UTF8 input");
        let (tracker, task) = limited_task();
        let mut side = Side::new(&task);
        assert!(
            side.variant
                .start_with_offset(raw.as_slice(), task.conversion_offset().unwrap())
        );
        side.source = Source::Variant;
        let mut events = Vec::new();
        for step in 0..100_000 {
            let capacity = side.frames.capacity();
            side.step(&task, text).unwrap();
            if side.frames.capacity() != capacity {
                events.push(step);
            }
            if side.done() {
                break;
            }
        }
        assert!(side.source == Source::Valid);
        assert!(events.len() >= 3);
        drop(side);
        assert_eq!(tracker.current(), 0);
        for event in events {
            let (tracker, task) = limited_task();
            let mut side = Side::new(&task);
            assert!(
                side.variant
                    .start_with_offset(&raw, task.conversion_offset().unwrap())
            );
            side.source = Source::Variant;
            for _ in 0..event {
                side.step(&task, text).unwrap();
            }
            let capacity = side.frames.capacity();
            let len = side.frames.len();
            let (retained, occupied) = occupy_remaining(&tracker);
            assert_refusal(side.step(&task, text));
            assert_eq!(side.frames.capacity(), capacity);
            assert_eq!(side.frames.len(), len);
            release_occupation(&tracker, retained, occupied);
            drop(side);
            assert_eq!(tracker.current(), 0);
        }
    }

    /// TimestampTz micros whose little-endian bytes are all ASCII, with and
    /// without a fractional second.
    const STAMPS: [i64; 3] = [0x0011_2233_4455_6677, 0x0123_4567_0000, 0];

    fn constant_owner(offset: FixedOffset) -> Arc<LocalOffsetOwner> {
        LocalOffsetOwner::start(LocalOffsetStepRules {
            transition_at_utc: i64::MAX,
            before: offset,
            after: offset,
        })
        .unwrap()
    }
    /// The old renderer's JSON text for a serialized Variant at `offset`.
    fn rendered_at(variant: &str, offset: FixedOffset) -> String {
        VariantValue::from_serialized(variant.as_bytes())
            .unwrap()
            .to_json(Some(offset))
            .unwrap()
    }

    #[test]
    fn json_in_pair_variant_local_timestamps_admit_every_allocation() {
        let offset = FixedOffset::east_opt(5 * 3600 + 45 * 60).unwrap();
        let variant = ascii_timestamp_variant(&STAMPS);
        let reordered = ascii_timestamp_variant(&[STAMPS[1], STAMPS[0], STAMPS[2]]);
        let rendered = rendered_at(&variant, offset);
        let tracker = MemTracker::new_root("witness-pair-task");
        let (_state, task) = bound_task(
            Arc::clone(&tracker),
            runtime_with_offset_owner(constant_owner(offset)),
        );
        let mut cursor = JsonPairCursor::new(&task);
        // The task's error state is a std Mutex. On macOS std boxes a pthread
        // mutex lazily at its first lock, once per task error state and
        // independent of any pair; Linux futex mutexes never allocate. Lock
        // it once here so the witness measures the conversion path alone.
        JsonPairContext {
            task: &task,
            stopped: &AtomicBool::new(false),
        }
        .check_running()
        .unwrap();
        for (lhs, rhs) in [
            (&variant, &rendered),
            (&variant, &variant),
            (&rendered, &variant),
            (&variant, &reordered),
        ] {
            for credit in [1, 64] {
                let (result, report) = with_admission_witness(&tracker, || {
                    finish(&mut cursor, &task, Some(lhs), Some(rhs), credit)
                });
                assert_eq!(
                    result.unwrap().0,
                    oracle_at(Some(lhs), Some(rhs), offset),
                    "lhs={lhs:?} rhs={rhs:?} credit={credit}"
                );
                assert_eq!(
                    report.non_admitted_allocations, 0,
                    "lhs={lhs:?} rhs={rhs:?} credit={credit} report={report:?}"
                );
                assert!(
                    report.admitted_bytes > 0,
                    "the pair parses into tracked trees"
                );
            }
        }
        assert_eq!(
            finish(&mut cursor, &task, Some(&variant), Some(&rendered), 64)
                .unwrap()
                .0,
            JsonPairTruth::True
        );
        cursor.clear();
        assert_eq!(tracker.current(), 0);

        // The witness does see the ungoverned path: the old conversion
        // allocates outside task admission.
        let (_, report) = with_admission_witness(&tracker, || {
            oracle_at(Some(&variant), Some(&rendered), offset)
        });
        assert!(report.non_admitted_allocations > 0, "report={report:?}");
        // And on a fresh driver-like thread, the first chrono `Local` read
        // loads the zone outside admission; the snapshot read above did not.
        let fresh = Arc::clone(&tracker);
        let report =
            std::thread::spawn(move || with_admission_witness(&fresh, chrono::Local::now).1)
                .join()
                .unwrap();
        assert!(report.non_admitted_allocations > 0, "report={report:?}");
    }

    #[test]
    fn json_in_pair_injected_local_offset_matches_old_in_list_oracle() {
        let variant = ascii_timestamp_variant(&STAMPS);
        let serialized_null = std::str::from_utf8(&[4, 0, 0, 0, 1, 0, 0, 0])
            .unwrap()
            .to_owned();
        let offsets = [
            -12 * 3600,
            -(3 * 3600 + 1800),
            0,
            5 * 3600 + 45 * 60,
            14 * 3600,
        ]
        .map(|seconds| FixedOffset::east_opt(seconds).unwrap());
        let mut inputs = vec![
            variant.clone(),
            serialized_null,
            "null".to_owned(),
            "plain".to_owned(),
        ];
        inputs.extend(offsets.iter().map(|offset| rendered_at(&variant, *offset)));
        for offset in offsets {
            let tracker = MemTracker::new_root("injected-offset-task");
            let (_state, task) = bound_task(
                Arc::clone(&tracker),
                runtime_with_offset_owner(constant_owner(offset)),
            );
            let mut cursor = JsonPairCursor::new(&task);
            for lhs in &inputs {
                for rhs in &inputs {
                    for credit in [1, 64] {
                        assert_eq!(
                            finish(&mut cursor, &task, Some(lhs), Some(rhs), credit)
                                .unwrap()
                                .0,
                            oracle_at(Some(lhs), Some(rhs), offset),
                            "offset={offset} lhs={lhs:?} rhs={rhs:?} credit={credit}"
                        );
                    }
                }
            }
            // The governed conversion follows the injected owner.
            assert_eq!(
                finish(
                    &mut cursor,
                    &task,
                    Some(&variant),
                    Some(&rendered_at(&variant, offset)),
                    64
                )
                .unwrap()
                .0,
                JsonPairTruth::True
            );
            cursor.clear();
            assert_eq!(tracker.current(), 0);
        }
    }

    #[test]
    fn json_in_pair_production_local_offset_matches_unchanged_old_entry() {
        let variant = ascii_timestamp_variant(&STAMPS);
        let local = super::super::in_pred::json_value_from_text_or_variant(&variant)
            .expect("the old entry converts the Variant")
            .to_string();
        let utc = rendered_at(&variant, FixedOffset::east_opt(0).unwrap());
        for lhs in [&variant, &local, &utc] {
            for rhs in [&variant, &local, &utc] {
                parity(lhs, rhs);
            }
        }
    }

    /// Rules that hold one offset until `stable_until` and then change every
    /// second, so no later window establishes a unique transition.
    struct FlappingRules {
        stable_until: i64,
    }
    impl LocalRulesSource for FlappingRules {
        fn offset_at(&self, utc_secs: i64) -> Option<FixedOffset> {
            let shifted = utc_secs > self.stable_until && (utc_secs - self.stable_until) % 2 == 1;
            FixedOffset::east_opt(if shifted { 7200 } else { 3600 })
        }
    }
    /// An owner whose newest snapshot no longer covers the wall clock.
    fn expired_owner() -> Arc<LocalOffsetOwner> {
        for _ in 0..5 {
            let now = unix_seconds_floor(SystemTime::now());
            // The first window ends at the second transition, `now + 2`.
            let Ok(owner) = LocalOffsetOwner::start(FlappingRules { stable_until: now }) else {
                continue;
            };
            let deadline = Instant::now() + Duration::from_secs(15);
            while owner.offset_now().is_ok() {
                assert!(
                    Instant::now() < deadline,
                    "the snapshot did not expire: {:?}",
                    owner.snapshot()
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            return owner;
        }
        panic!("the flapping owner could not publish its first snapshot");
    }

    #[test]
    fn json_in_pair_expired_local_offset_is_typed_failure_not_fallback() {
        let owner = expired_owner();
        let tracker = MemTracker::new_root("expired-offset-task");
        let (_state, task) = bound_task(Arc::clone(&tracker), runtime_with_offset_owner(owner));
        let variant = ascii_timestamp_variant(&STAMPS);
        let mut cursor = JsonPairCursor::new(&task);
        for credit in [1, 64] {
            // Raw equality would answer TRUE for the first pair; an expired
            // snapshot must fail instead of selecting any comparison.
            for (lhs, rhs) in [
                (variant.as_str(), variant.as_str()),
                (variant.as_str(), "plain"),
                ("[1]", variant.as_str()),
            ] {
                let result = finish(&mut cursor, &task, Some(lhs), Some(rhs), credit);
                assert!(
                    matches!(
                        result,
                        Err(JsonPairError::Failed(ref failure))
                            if failure.category() == TaskFailureCategory::ResourceExhausted
                                && failure
                                    .detail()
                                    .as_str()
                                    .contains("local time zone offset snapshot")
                    ),
                    "lhs={lhs:?} rhs={rhs:?} credit={credit} result={result:?}"
                );
            }
            // A pair that never starts a Variant conversion never reads it.
            for (lhs, rhs) in [
                ("plain", "plain"),
                (r#"{"a":1}"#, r#"{"a": 1}"#),
                ("plain", "[1]"),
            ] {
                assert_eq!(
                    finish(&mut cursor, &task, Some(lhs), Some(rhs), credit)
                        .unwrap()
                        .0,
                    oracle(Some(lhs), Some(rhs))
                );
            }
            assert_eq!(
                finish(&mut cursor, &task, None, Some(&variant), credit)
                    .unwrap()
                    .0,
                JsonPairTruth::Unknown
            );
        }
        cursor.clear();
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn json_in_pair_variant_every_allocation_turn_replays_typed_refusal() {
        let variant = ascii_timestamp_variant(&STAMPS);
        let rendered = super::super::in_pred::json_value_from_text_or_variant(&variant)
            .expect("the old entry converts the Variant")
            .to_string();
        every_allocation_turn_replays_typed_refusal(&variant, &rendered);
        every_allocation_turn_replays_typed_refusal(&rendered, &variant);
    }

    #[test]
    fn json_in_pair_variant_primitive_rejections_keep_raw_fallback() {
        // TimeNtz, the nanosecond timestamps and unassigned primitive ids are
        // rejected by the old renderer, top-level or nested.
        for kind in [17, 18, 19, 21, 25, 31] {
            let primitive = variant_primitive(kind, &[0; 16]);
            let top = ascii_variant(&EMPTY_METADATA, &primitive);
            let nested = ascii_variant(
                &EMPTY_METADATA,
                &variant_array(&[variant_primitive(0, &[]), primitive]),
            );
            for value in [&top, &nested] {
                parity(value, value);
                parity(value, "null");
                parity(value, "[null,null]");
                parity("[null]", value);
            }
        }
        // Every convertible primitive id whose encoding fits the UTF8 carrier.
        for (kind, payload) in [
            (0u8, vec![]),
            (1, vec![]),
            (2, vec![]),
            (3, vec![0x7f]),
            (4, 0x1234i16.to_le_bytes().to_vec()),
            (5, 0x0102_0304i32.to_le_bytes().to_vec()),
            (6, 0x0102_0304_0506_0708i64.to_le_bytes().to_vec()),
            (7, 0x3f40_0000_0000_0000u64.to_le_bytes().to_vec()),
            (8, [2, 0x10, 0x27, 0, 0].to_vec()),
            (9, [3, 0x10, 0x27, 0, 0, 0, 0, 0, 0].to_vec()),
            (
                10,
                [4, 0x10, 0x27, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0].to_vec(),
            ),
            (11, 0x4e20i32.to_le_bytes().to_vec()),
            (12, STAMPS[0].to_le_bytes().to_vec()),
            (13, STAMPS[0].to_le_bytes().to_vec()),
            (14, 0x3f40_0000u32.to_le_bytes().to_vec()),
            (15, [3, 0, 0, 0, b'a', b'b', b'c'].to_vec()),
            (16, [3, 0, 0, 0, b'x', b'"', b'\\'].to_vec()),
            (20, (0u8..16).collect()),
        ] {
            let value = ascii_variant(&EMPTY_METADATA, &variant_primitive(kind, &payload));
            let rendered = super::super::in_pred::json_value_from_text_or_variant(&value)
                .unwrap_or_else(|| panic!("kind {kind} converts"))
                .to_string();
            parity(&value, &rendered);
            parity(&value, &value);
            parity(&value, "0");
        }
        // A short string is its own basic type: length in the header byte.
        let short = ascii_variant(&EMPTY_METADATA, &[(3 << 2) | 1, b'a', b'"', b'c']);
        parity(&short, r#""a\"c""#);
        parity(&short, &short);
    }

    #[test]
    fn json_in_pair_task_failure_stops_pair_without_parse_result() {
        let tracker = MemTracker::new_root("pair-task");
        let (state, task) = bound_task(Arc::clone(&tracker), shared_runtime());
        let mut cursor = JsonPairCursor::new(&task);
        let text = format!("\"{}\"", "a".repeat(1000));
        let stopped = AtomicBool::new(false);
        cursor.start(input(Some(&text), Some(&text))).unwrap();
        let poll = |cursor: &mut JsonPairCursor| {
            cursor.poll(
                input(Some(&text), Some(&text)),
                JsonPairContext {
                    task: &task,
                    stopped: &stopped,
                },
                &mut JsonPairWork::new(16),
            )
        };
        assert_eq!(poll(&mut cursor).unwrap(), JsonPairPoll::Yield);
        state.error_state().set_error("peer task failed".to_owned());
        assert!(matches!(poll(&mut cursor), Err(JsonPairError::Stopped)));
        cursor.clear();
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn json_in_pair_escape_control_and_exponent_boundaries() {
        let cases = [
            r#""\/""#,
            r#""/""#,
            r#""\b\f\n\r\t""#,
            "\"\u{8}\u{c}\n\r\t\"",
            r#""é""#,
            r#""é""#,
            r#""😀""#,
            r#""\ud83d""#,
            r#""\ud83d\""#,
            r#""\ud83dx""#,
            r#""\u12""#,
            r#""\u12G4""#,
            r#""\u0000""#,
            "\"\u{1}\"",
            "\"\u{7f}\"",
            " 1 ",
            "\t[\n1\r]",
            "1\u{a0}",
            "\u{feff}1",
            "1e2147483647",
            "1e2147483648",
            "0e2147483648",
            "1e-2147483648",
            "1e-2147483649",
            "-1e-400",
            "1E+2",
            "1e+0",
            "1e-0",
            "123.456e-2",
            "0.1e1",
            "100000000000000000000e-20",
            "9007199254740993",
            "1.7976931348623157e308",
            "1.7976931348623158e308",
            "1.8e308",
            "4.9e-324",
            "2e-324",
            r#"{"a":{"b":1},"a":{"c":2}}"#,
            r#"{"a":{"c":2}}"#,
            r#"{"":1}"#,
            r#"{"a":1 , "b" : 2 }"#,
            r#"{"a"}"#,
            r#"{"a":}"#,
            r#"{,}"#,
            r#"{"a":1"b":2}"#,
        ];
        for a in cases {
            for b in cases {
                parity(a, b);
            }
        }
        for depth in [126, 127, 128] {
            let object = format!("{}1{}", r#"{"a":"#.repeat(depth), "}".repeat(depth));
            parity(&object, &object.replace(':', " : "));
        }
    }

    /// Deterministic xorshift for generated documents.
    struct Generator(u64);
    impl Generator {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, bound: usize) -> usize {
            (self.next() % bound as u64) as usize
        }
        fn pick<'a>(&mut self, values: &[&'a str]) -> &'a str {
            values[self.below(values.len())]
        }
        fn space(&mut self, out: &mut String) {
            out.push_str(self.pick(&["", "", " ", "\n", " \t"]));
        }
        fn document(&mut self, depth: usize, out: &mut String) {
            self.space(out);
            match self.below(if depth >= 4 { 4 } else { 6 }) {
                0 => out.push_str(self.pick(&["null", "true", "false"])),
                1 => out.push_str(self.pick(&[
                    "0",
                    "-0",
                    "1",
                    "-1",
                    "1.5",
                    "1e3",
                    "1E-3",
                    "-0.0",
                    "0.1",
                    "1.25e+2",
                    "9007199254740993",
                    "18446744073709551615",
                    "18446744073709551616",
                    "-9223372036854775808",
                    "123456789012345678901234567890",
                ])),
                2 | 3 => {
                    out.push('"');
                    for _ in 0..self.below(4) {
                        out.push_str(self.pick(&[
                            "a",
                            "é",
                            "😀",
                            "\\n",
                            "\\\"",
                            "\\\\",
                            "\\/",
                            "\\u00e9",
                            "\\u00E9",
                            "\\ud83d\\ude00",
                            "\\b",
                            "\\t",
                            " ",
                        ]));
                    }
                    out.push('"');
                }
                4 => {
                    out.push('[');
                    for index in 0..self.below(4) {
                        if index > 0 {
                            out.push(',');
                        }
                        self.document(depth + 1, out);
                    }
                    self.space(out);
                    out.push(']');
                }
                _ => {
                    out.push('{');
                    for index in 0..self.below(4) {
                        if index > 0 {
                            out.push(',');
                        }
                        self.space(out);
                        out.push_str(self.pick(&[r#""a""#, r#""b""#, r#""a""#, r#""é""#, r#""""#]));
                        self.space(out);
                        out.push(':');
                        self.document(depth + 1, out);
                    }
                    self.space(out);
                    out.push('}');
                }
            }
            self.space(out);
        }
        fn mutate(&mut self, document: &str) -> String {
            let ascii: Vec<usize> = document
                .char_indices()
                .filter(|(_, c)| c.is_ascii())
                .map(|(index, _)| index)
                .collect();
            match self.below(4) {
                0 => {
                    let mut end = self.below(document.len() + 1);
                    while !document.is_char_boundary(end) {
                        end -= 1;
                    }
                    document[..end].to_owned()
                }
                1 if !ascii.is_empty() => {
                    let at = ascii[self.below(ascii.len())];
                    let replacement = self.pick(&["x", ",", "}", "]", "\"", "\\", "\u{1}", " "]);
                    format!("{}{replacement}{}", &document[..at], &document[at + 1..])
                }
                2 => format!("{document}{}", self.pick(&["x", " ", ",", "]", "1"])),
                _ => format!("{document}{document}"),
            }
        }
    }

    #[test]
    fn json_in_pair_generated_document_mutation_differential() {
        let mut generator = Generator(0x9e37_79b9_7f4a_7c15);
        let mut previous = String::from("null");
        let (mut valid, mut invalid, mut nested) = (0, 0, 0);
        for _ in 0..150 {
            let mut document = String::new();
            generator.document(0, &mut document);
            // An independent serde rendering of the same value, when valid.
            let canonical = serde_json::from_str::<serde_json::Value>(&document)
                .map(|value| value.to_string())
                .unwrap_or_else(|_| document.clone());
            let mutated = generator.mutate(&document);
            let mutated_again = generator.mutate(&mutated);
            for text in [&document, &mutated, &mutated_again] {
                if serde_json::from_str::<serde_json::Value>(text).is_ok() {
                    valid += 1;
                } else {
                    invalid += 1;
                }
            }
            fn depth(value: &serde_json::Value) -> usize {
                match value {
                    serde_json::Value::Array(items) => {
                        1 + items.iter().map(depth).max().unwrap_or(0)
                    }
                    serde_json::Value::Object(fields) => {
                        1 + fields.values().map(depth).max().unwrap_or(0)
                    }
                    _ => 0,
                }
            }
            if serde_json::from_str::<serde_json::Value>(&document).is_ok_and(|v| depth(&v) >= 2) {
                nested += 1;
            }
            for (lhs, rhs) in [
                (&document, &document),
                (&document, &canonical),
                (&canonical, &document),
                (&document, &mutated),
                (&mutated, &mutated_again),
                (&mutated, &mutated),
                (&document, &previous),
            ] {
                parity(lhs, rhs);
            }
            previous = document;
        }
        // The generator must keep producing both outcomes and real nesting.
        assert!(
            valid >= 100 && invalid >= 50 && nested >= 10,
            "valid={valid} invalid={invalid} nested={nested}"
        );
    }
}
