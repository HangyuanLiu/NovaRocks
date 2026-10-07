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
                    if self.source == Source::Text && self.variant.start(input.as_bytes()) {
                        self.source = Source::Variant;
                    } else {
                        self.source = Source::Invalid;
                    }
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
    use super::*;
    use crate::runtime::mem_tracker::MemTracker;
    use crate::runtime::runtime_state::RuntimeState;
    use crate::runtime::verification::TaskVerificationHolder;
    use novarocks_execution_contract::{TaskFailureCategory, TaskIdentity};
    use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId};
    use novarocks_types::{BackendProcessId, StageId, TaskId};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    pub(super) fn task(tracker: Arc<MemTracker>) -> JsonPairTask {
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        );
        let state = RuntimeState::new(None, None, None, None, None, Some(tracker), None)
            .with_verification(Arc::new(TaskVerificationHolder::new(identity)));
        JsonPairTask::try_bind(&state).unwrap()
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
    fn oracle(lhs: Option<&str>, rhs: Option<&str>) -> JsonPairTruth {
        let (Some(lhs), Some(rhs)) = (lhs, rhs) else {
            return JsonPairTruth::Unknown;
        };
        let a = super::super::in_pred::json_value_from_text_or_variant(lhs);
        let b = super::super::in_pred::json_value_from_text_or_variant(rhs);
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
        assert!(side.variant.start(raw.as_slice()));
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
            assert!(side.variant.start(&raw));
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
}
