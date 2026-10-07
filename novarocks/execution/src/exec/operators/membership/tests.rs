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

//! Real-driver evidence for the membership RHS and probe lifecycle. Every
//! expected result comes from the unchanged old IN-list pair rule.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_execution_contract::{TaskFailureCategory, TaskIdentity};
use novarocks_local_program::{MembershipComparison, MembershipDistribution, MembershipSpec};
use novarocks_types::identity::{AttemptId, QueryExecutionId, QueryId};
use novarocks_types::{BackendProcessId, SlotId, StageId, TaskId};

use super::{MembershipBuildSinkFactory, MembershipProbeFactory, MembershipShared};
use crate::exec::chunk::{Chunk, ChunkSchema, ChunkSchemaRef};
use crate::exec::expr::json_in_pair::JsonPairTruth;
use crate::exec::expr::json_in_pair::test_support::{
    ascii_timestamp_variant, old_in_list_pair, shared_runtime, task_state, with_admission_witness,
};
use crate::exec::pipeline::dependency::DependencyManager;
use crate::exec::pipeline::driver::{DriverState, PipelineDriver};
use crate::exec::pipeline::operator::{BlockedReason, Operator, ProcessorOperator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::observable::Observable;
use crate::runtime::runtime_state::RuntimeState;

const PROBE: u32 = 1;
const EXTRA: u32 = 2;
const BUILD: u32 = 3;
const RESULT: u32 = 4;

fn identity() -> TaskIdentity {
    TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(1).unwrap()).unwrap(),
        StageId::new(1).unwrap(),
        TaskId::new(1).unwrap(),
        BackendProcessId::new_v7(),
    )
}

struct TaskFixture {
    state: Arc<RuntimeState>,
    tracker: Arc<MemTracker>,
}

fn task(limit: Option<i64>) -> TaskFixture {
    let tracker = MemTracker::new_root("membership-task");
    if let Some(limit) = limit {
        tracker.install_limit_once(limit).unwrap();
    }
    let state = Arc::new(task_state(
        identity(),
        Arc::clone(&tracker),
        Some(shared_runtime()),
    ));
    TaskFixture { state, tracker }
}

fn schema(slots: &[(u32, DataType)]) -> ChunkSchemaRef {
    let fields = slots
        .iter()
        .map(|(id, data_type)| Field::new(format!("c{id}"), data_type.clone(), true))
        .collect::<Vec<_>>();
    let ids = slots
        .iter()
        .map(|(id, _)| SlotId::new(*id))
        .collect::<Vec<_>>();
    ChunkSchema::try_ref_from_schema_and_slot_ids(&Schema::new(fields), &ids).unwrap()
}

fn probe_schema() -> ChunkSchemaRef {
    schema(&[(PROBE, DataType::Utf8), (EXTRA, DataType::Int64)])
}

fn build_schema() -> ChunkSchemaRef {
    schema(&[(BUILD, DataType::Utf8)])
}

fn output_schema() -> ChunkSchemaRef {
    schema(&[
        (PROBE, DataType::Utf8),
        (EXTRA, DataType::Int64),
        (RESULT, DataType::Boolean),
    ])
}

fn probe_chunk(values: &[Option<&str>], first: i64) -> Chunk {
    Chunk::try_new_with_columns(
        probe_schema(),
        vec![
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from_iter_values(
                first..first + values.len() as i64,
            )),
        ],
    )
    .unwrap()
}

fn build_chunk(values: &[Option<&str>]) -> Chunk {
    Chunk::try_new_with_columns(
        build_schema(),
        vec![Arc::new(StringArray::from(values.to_vec())) as ArrayRef],
    )
    .unwrap()
}

fn spec(negated: bool) -> MembershipSpec {
    MembershipSpec {
        probe: SlotId::new(PROBE),
        build: SlotId::new(BUILD),
        result: SlotId::new(RESULT),
        negated,
        comparison: MembershipComparison::JsonInListV1,
        distribution: MembershipDistribution::BroadcastBuild,
    }
}

struct NodeFixture {
    shared: Arc<MembershipShared>,
    build: MembershipBuildSinkFactory,
    probe: MembershipProbeFactory,
}

fn new_node(negated: bool, probe_drivers: usize, work_units: Option<usize>) -> NodeFixture {
    let manager = DependencyManager::new();
    let shared = MembershipShared::try_new(
        7,
        SlotId::new(BUILD),
        &build_schema(),
        probe_drivers,
        &manager,
    )
    .unwrap();
    let mut probe = MembershipProbeFactory::try_new(
        Arc::clone(&shared),
        &spec(negated),
        &probe_schema(),
        output_schema(),
    )
    .unwrap();
    if let Some(units) = work_units {
        probe = probe.with_work_units(units);
    }
    NodeFixture {
        build: MembershipBuildSinkFactory::new(Arc::clone(&shared)),
        shared,
        probe,
    }
}

/// A source that emits its chunks and finishes only once its senders are
/// open; it can also fail after a number of pulls.
struct Source {
    chunks: VecDeque<Chunk>,
    pulls: Arc<AtomicUsize>,
    open: Arc<AtomicBool>,
    observable: Arc<Observable>,
    fail_at: Option<usize>,
}

impl Source {
    fn new(chunks: Vec<Chunk>) -> Self {
        Self {
            chunks: chunks.into(),
            pulls: Arc::new(AtomicUsize::new(0)),
            open: Arc::new(AtomicBool::new(true)),
            observable: Arc::new(Observable::new()),
            fail_at: None,
        }
    }
}

impl Operator for Source {
    fn name(&self) -> &str {
        "MembershipTestSource"
    }
    fn is_finished(&self) -> bool {
        self.chunks.is_empty() && self.open.load(Ordering::Acquire) && self.fail_at.is_none()
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for Source {
    fn need_input(&self) -> bool {
        false
    }
    fn has_output(&self) -> bool {
        !self.chunks.is_empty() || self.fail_at.is_some()
    }
    fn push_chunk(&mut self, _state: &RuntimeState, _chunk: Chunk) -> Result<(), String> {
        Err("the test source accepts no input".to_string())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        let pulls = self.pulls.fetch_add(1, Ordering::AcqRel);
        if self.fail_at == Some(pulls) {
            return Err("injected sender failure".to_string());
        }
        Ok(self.chunks.pop_front())
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        Ok(())
    }
    fn source_observable(&self) -> Option<Arc<Observable>> {
        Some(Arc::clone(&self.observable))
    }
}

/// A terminal sink that can refuse input to apply backpressure.
struct Sink {
    chunks: Arc<Mutex<Vec<Chunk>>>,
    accepting: Arc<AtomicBool>,
    observable: Arc<Observable>,
    finished: bool,
}

impl Sink {
    fn new() -> Self {
        Self {
            chunks: Arc::new(Mutex::new(Vec::new())),
            accepting: Arc::new(AtomicBool::new(true)),
            observable: Arc::new(Observable::new()),
            finished: false,
        }
    }
}

impl Operator for Sink {
    fn name(&self) -> &str {
        "MembershipTestSink"
    }
    fn is_finished(&self) -> bool {
        self.finished
    }
    fn as_processor_mut(&mut self) -> Option<&mut dyn ProcessorOperator> {
        Some(self)
    }
    fn as_processor_ref(&self) -> Option<&dyn ProcessorOperator> {
        Some(self)
    }
}

impl ProcessorOperator for Sink {
    fn need_input(&self) -> bool {
        !self.finished && self.accepting.load(Ordering::Acquire)
    }
    fn has_output(&self) -> bool {
        false
    }
    fn push_chunk(&mut self, _state: &RuntimeState, chunk: Chunk) -> Result<(), String> {
        self.chunks.lock().unwrap().push(chunk);
        Ok(())
    }
    fn pull_chunk(&mut self, _state: &RuntimeState) -> Result<Option<Chunk>, String> {
        Ok(None)
    }
    fn set_finishing(&mut self, _state: &RuntimeState) -> Result<(), String> {
        self.finished = true;
        Ok(())
    }
    fn sink_observable(&self) -> Option<Arc<Observable>> {
        Some(Arc::clone(&self.observable))
    }
}

fn driver(
    index: i32,
    mut operators: Vec<Box<dyn Operator>>,
    state: &Arc<RuntimeState>,
) -> PipelineDriver {
    for operator in &mut operators {
        operator.prepare().unwrap();
        operator.bind_runtime_state(state).unwrap();
    }
    PipelineDriver::new(index, operators, None, Vec::new(), Arc::clone(state), None)
}

fn build_driver(
    fixture: &NodeFixture,
    source: Source,
    state: &Arc<RuntimeState>,
) -> PipelineDriver {
    driver(
        100,
        vec![Box::new(source), fixture.build.create(1, 0)],
        state,
    )
}

struct ProbeDriver {
    driver: PipelineDriver,
    pulls: Arc<AtomicUsize>,
    output: Arc<Mutex<Vec<Chunk>>>,
    accepting: Arc<AtomicBool>,
    sink_observable: Arc<Observable>,
}

fn probe_driver(
    fixture: &NodeFixture,
    chunks: Vec<Chunk>,
    dop: i32,
    index: i32,
    state: &Arc<RuntimeState>,
) -> ProbeDriver {
    let source = Source::new(chunks);
    let pulls = Arc::clone(&source.pulls);
    let sink = Sink::new();
    let output = Arc::clone(&sink.chunks);
    let accepting = Arc::clone(&sink.accepting);
    let sink_observable = Arc::clone(&sink.observable);
    ProbeDriver {
        driver: driver(
            index,
            vec![
                Box::new(source),
                fixture.probe.create(dop, index),
                Box::new(sink),
            ],
            state,
        ),
        pulls,
        output,
        accepting,
        sink_observable,
    }
}

/// Runs scheduling turns until the driver leaves Ready; returns that state
/// and the number of Ready turns before it.
fn run(driver: &mut PipelineDriver) -> (DriverState, usize) {
    for turns in 0..1_000_000 {
        let state = driver.process(Duration::from_secs(30));
        if state != DriverState::Ready {
            return (state, turns);
        }
    }
    panic!("membership driver never left Ready")
}

fn results(chunks: &[Chunk]) -> Vec<Option<bool>> {
    let mut out = Vec::new();
    for chunk in chunks {
        assert_eq!(chunk.columns().len(), 3);
        let result = chunk.columns()[2]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        out.extend(result.iter());
    }
    out
}

/// The probe columns pass through unchanged and in order.
fn extra_ids(chunks: &[Chunk]) -> Vec<i64> {
    chunks
        .iter()
        .flat_map(|chunk| {
            chunk.columns()[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn expected(probe: &[Option<&str>], rhs: &[Option<&str>], negated: bool) -> Vec<Option<bool>> {
    probe
        .iter()
        .map(|lhs| {
            if rhs.is_empty() {
                return Some(negated);
            }
            let mut unknown = false;
            for candidate in rhs {
                match old_in_list_pair(*lhs, *candidate) {
                    JsonPairTruth::True => return Some(!negated),
                    JsonPairTruth::Unknown => unknown = true,
                    JsonPairTruth::False => {}
                }
            }
            if unknown { None } else { Some(negated) }
        })
        .collect()
}

/// Builds the RHS on one real build driver, then runs every probe driver.
/// Returns each probe driver's output.
fn execute(
    fixture: &TaskFixture,
    negated: bool,
    rhs: &[Vec<Option<&str>>],
    probes: &[Vec<Vec<Option<&str>>>],
    work_units: Option<usize>,
) -> Vec<Vec<Chunk>> {
    let node = new_node(negated, probes.len(), work_units);
    let mut build = build_driver(
        &node,
        Source::new(rhs.iter().map(|values| build_chunk(values)).collect()),
        &fixture.state,
    );
    assert_eq!(run(&mut build).0, DriverState::Finished);
    assert!(node.shared.dependency().is_ready());
    let mut outputs = Vec::new();
    for (index, chunks) in probes.iter().enumerate() {
        let mut first = 0;
        let chunks = chunks
            .iter()
            .map(|values| {
                let chunk = probe_chunk(values, first);
                first += values.len() as i64;
                chunk
            })
            .collect();
        let mut probe = probe_driver(
            &node,
            chunks,
            probes.len() as i32,
            index as i32,
            &fixture.state,
        );
        assert_eq!(run(&mut probe.driver).0, DriverState::Finished);
        outputs.push(std::mem::take(&mut *probe.output.lock().unwrap()));
    }
    outputs
}

#[test]
fn membership_multi_batch_rhs_and_multiple_local_probes_match_old_in_list() {
    let rhs = vec![
        vec![Some(r#"{"a":1}"#), None, Some("[1,2]")],
        vec![],
        vec![Some(r#""x""#), Some(r#"{"b": 2, "a": 1}"#)],
        vec![Some("7"), Some("null"), Some(r#""x""#)],
    ];
    let probes = vec![
        vec![
            vec![Some(r#"{"a":1}"#), Some(r#"{"a":2}"#)],
            vec![None, Some("null")],
        ],
        vec![vec![Some("[1,2]"), Some("[2,1]"), Some("x"), Some("7.0")]],
        vec![vec![
            Some(r#""x""#),
            Some(r#""x""#),
            Some(r#"{ "a" : 1 , "b" : 2 }"#),
            Some("plain"),
        ]],
    ];
    let rhs_flat = rhs.iter().flatten().copied().collect::<Vec<_>>();
    for negated in [false, true] {
        let fixture = task(None);
        let outputs = execute(&fixture, negated, &rhs, &probes, None);
        for (output, chunks) in outputs.iter().zip(&probes) {
            let probe = chunks.iter().flatten().copied().collect::<Vec<_>>();
            assert_eq!(results(output), expected(&probe, &rhs_flat, negated));
            assert_eq!(
                extra_ids(output),
                (0..probe.len() as i64).collect::<Vec<_>>()
            );
        }
        drop(outputs);
        assert_eq!(fixture.tracker.current(), 0, "negated={negated}");
    }
}

#[test]
fn membership_true_wins_unknown_and_null_probe_follow_the_three_valued_table() {
    for rhs in [vec![None, Some("1")], vec![Some("1"), None]] {
        for negated in [false, true] {
            let fixture = task(None);
            let probe = vec![Some("1"), Some("2"), None, Some("1.0")];
            let outputs = execute(
                &fixture,
                negated,
                std::slice::from_ref(&rhs),
                &[vec![probe.clone()]],
                None,
            );
            let actual = results(&outputs[0]);
            assert_eq!(actual, expected(&probe, &rhs, negated));
            let (hit, miss) = if negated {
                (Some(false), None)
            } else {
                (Some(true), None)
            };
            assert_eq!(actual[0], hit, "TRUE wins over an UNKNOWN candidate");
            assert_eq!(actual[1], miss, "no TRUE but an UNKNOWN is SQL NULL");
            assert_eq!(actual[2], None, "a NULL probe against candidates is NULL");
        }
    }
}

#[test]
fn membership_empty_rhs_decides_every_row_including_null_probe() {
    for rhs in [vec![], vec![vec![]], vec![vec![], vec![]]] {
        for negated in [false, true] {
            let fixture = task(None);
            let probe = vec![None, Some("1"), Some("null")];
            let outputs = execute(&fixture, negated, &rhs, &[vec![probe.clone()]], None);
            assert_eq!(results(&outputs[0]), vec![Some(negated); 3]);
            assert_eq!(results(&outputs[0]), expected(&probe, &[], negated));
        }
    }
}

#[test]
fn membership_rhs_completes_only_after_every_sender_and_local_eos() {
    let fixture = task(None);
    let node = new_node(false, 1, None);
    let mut source = Source::new(vec![build_chunk(&[Some("1")]), build_chunk(&[Some("2")])]);
    let open = Arc::clone(&source.open);
    let senders = Arc::clone(&source.observable);
    open.store(false, Ordering::Release);
    source.fail_at = None;
    let mut build = build_driver(&node, source, &fixture.state);
    let mut probe = probe_driver(
        &node,
        vec![probe_chunk(&[Some("2"), Some("3")], 0)],
        1,
        0,
        &fixture.state,
    );
    // Every chunk arrived but a frozen sender has not reached EOS.
    assert!(matches!(
        run(&mut build).0,
        DriverState::Blocked(BlockedReason::InputEmpty)
    ));
    assert!(!node.shared.dependency().is_ready());
    assert!(matches!(
        run(&mut probe.driver).0,
        DriverState::Blocked(BlockedReason::Dependency(_))
    ));
    assert!(probe.output.lock().unwrap().is_empty());
    open.store(true, Ordering::Release);
    senders.notify_observers();
    assert_eq!(run(&mut build).0, DriverState::Finished);
    assert!(node.shared.dependency().is_ready());
    assert_eq!(run(&mut probe.driver).0, DriverState::Finished);
    assert_eq!(
        results(&probe.output.lock().unwrap()),
        vec![Some(true), Some(false)]
    );
}

#[test]
fn membership_huge_candidate_resumes_across_many_turns_without_recomputing_probe() {
    let huge = format!(r#"{{"k":"{}","n":[1,2,3]}}"#, "x".repeat(200_000));
    let reordered = format!(r#"{{ "n" : [1,2,3] , "k" : "{}" }}"#, "x".repeat(200_000));
    let fixture = task(None);
    let node = new_node(false, 1, Some(512));
    let mut build = build_driver(
        &node,
        Source::new(vec![build_chunk(&[Some(huge.as_str())])]),
        &fixture.state,
    );
    assert_eq!(run(&mut build).0, DriverState::Finished);
    let mut probe = probe_driver(
        &node,
        vec![probe_chunk(&[Some(reordered.as_str()), Some("1")], 0)],
        1,
        0,
        &fixture.state,
    );
    let (state, turns) = run(&mut probe.driver);
    assert_eq!(state, DriverState::Finished);
    assert!(turns >= 100, "only {turns} turns");
    assert_eq!(
        probe.pulls.load(Ordering::Acquire),
        1,
        "the probe input is pulled once"
    );
    assert_eq!(
        results(&probe.output.lock().unwrap()),
        vec![Some(true), Some(false)]
    );
}

#[test]
fn membership_long_keys_raw_text_and_variant_expansion_match_old_in_list() {
    let long_key = "é".repeat(2000);
    let lhs_object = format!(
        r#"{{"{long_key}":"{}","b":[1,{{"c":null}}]}}"#,
        "v".repeat(5000)
    );
    let rhs_object = format!(
        r#"{{"b":[1,{{"c":null}}],"{long_key}":"{}"}}"#,
        "v".repeat(5000)
    );
    let raw = "raw-not-json ".repeat(3000);
    let variant = ascii_timestamp_variant(&[0x0011_2233_4455_6677, 0x0123_4567_0000, 0]);
    let variant_text = crate::exec::expr::json_in_pair::test_support::old_in_list_text(&variant);
    let rhs = vec![vec![
        Some(rhs_object.as_str()),
        Some(raw.as_str()),
        Some(variant_text.as_str()),
    ]];
    let probe = vec![
        Some(lhs_object.as_str()),
        Some(raw.as_str()),
        Some(variant.as_str()),
        Some(r#"{"b":[1]}"#),
    ];
    for negated in [false, true] {
        let fixture = task(None);
        let outputs = execute(&fixture, negated, &rhs, &[vec![probe.clone()]], Some(97));
        let expected = expected(&probe, &rhs[0], negated);
        assert_eq!(results(&outputs[0]), expected);
        assert_eq!(expected[..3], [Some(!negated); 3]);
    }
}

#[test]
fn membership_downstream_backpressure_keeps_the_cursor() {
    let fixture = task(None);
    let node = new_node(false, 1, Some(64));
    let candidate = format!(r#"["{}"]"#, "q".repeat(4000));
    let mut build = build_driver(
        &node,
        Source::new(vec![build_chunk(&[Some(candidate.as_str())])]),
        &fixture.state,
    );
    assert_eq!(run(&mut build).0, DriverState::Finished);
    let mut probe = probe_driver(
        &node,
        vec![probe_chunk(&[Some(candidate.as_str())], 0)],
        1,
        0,
        &fixture.state,
    );
    // Part of the pair is decided, then the downstream stops accepting.
    assert_eq!(
        probe.driver.process(Duration::from_secs(30)),
        DriverState::Ready
    );
    probe.accepting.store(false, Ordering::Release);
    assert!(matches!(
        probe.driver.process(Duration::from_secs(30)),
        DriverState::Blocked(BlockedReason::OutputFull)
    ));
    probe.accepting.store(true, Ordering::Release);
    probe.sink_observable.notify_observers();
    probe.driver.set_ready();
    assert_eq!(run(&mut probe.driver).0, DriverState::Finished);
    assert_eq!(probe.pulls.load(Ordering::Acquire), 1);
    assert_eq!(results(&probe.output.lock().unwrap()), vec![Some(true)]);
}

#[test]
fn membership_cancel_while_waiting_and_mid_pair_releases_every_owner() {
    // Cancelled while blocked on the unfinished RHS.
    let fixture = task(None);
    let node = new_node(false, 1, Some(32));
    let source = Source::new(vec![build_chunk(&[Some(r#"["a"]"#)])]);
    source.open.store(false, Ordering::Release);
    let open = Arc::clone(&source.open);
    let mut build = build_driver(&node, source, &fixture.state);
    let mut probe = probe_driver(
        &node,
        vec![probe_chunk(&[Some(r#"["a"]"#)], 0)],
        1,
        0,
        &fixture.state,
    );
    assert!(matches!(run(&mut build).0, DriverState::Blocked(_)));
    assert!(matches!(run(&mut probe.driver).0, DriverState::Blocked(_)));
    assert_eq!(
        probe.driver.cancel_for_fragment_abort(),
        DriverState::Canceled
    );
    // Every probe left: the build finishes early instead of waiting.
    assert!(node.shared.consumers_gone());
    open.store(true, Ordering::Release);
    assert_eq!(run(&mut build).0, DriverState::Finished);
    drop(probe);
    drop(build);
    assert_eq!(fixture.tracker.current(), 0);

    // Cancelled in the middle of one pair.
    let fixture = task(None);
    let node = new_node(false, 1, Some(32));
    let value = format!(r#"["{}"]"#, "a".repeat(1000));
    let mut build = build_driver(
        &node,
        Source::new(vec![build_chunk(&[Some(value.as_str())])]),
        &fixture.state,
    );
    assert_eq!(run(&mut build).0, DriverState::Finished);
    let mut probe = probe_driver(
        &node,
        vec![probe_chunk(&[Some(value.as_str())], 0)],
        1,
        0,
        &fixture.state,
    );
    assert_eq!(
        probe.driver.process(Duration::from_secs(30)),
        DriverState::Ready
    );
    assert!(fixture.tracker.current() > 0);
    assert_eq!(
        probe.driver.cancel_for_fragment_abort(),
        DriverState::Canceled
    );
    assert_eq!(
        probe.driver.cancel_for_fragment_abort(),
        DriverState::Canceled
    );
    assert!(probe.output.lock().unwrap().is_empty());
    drop(probe);
    drop(build);
    assert_eq!(fixture.tracker.current(), 0);
}

#[test]
fn membership_build_failure_wakes_probes_with_the_original_typed_cause() {
    // A capacity refusal while the RHS is being retained.
    let fixture = task(Some(64));
    let node = new_node(false, 2, None);
    let mut build = build_driver(
        &node,
        Source::new(vec![build_chunk(&[Some(&"z".repeat(4096))])]),
        &fixture.state,
    );
    let mut probes = (0..2)
        .map(|index| {
            probe_driver(
                &node,
                vec![probe_chunk(&[Some("1")], 0)],
                2,
                index,
                &fixture.state,
            )
        })
        .collect::<Vec<_>>();
    assert!(matches!(
        run(&mut probes[0].driver).0,
        DriverState::Blocked(BlockedReason::Dependency(_))
    ));
    assert!(matches!(run(&mut build).0, DriverState::Failed(_)));
    let failure = fixture.state.error_state().task_failure().unwrap();
    assert!(matches!(
        failure.category(),
        TaskFailureCategory::CapacityRefused { .. }
    ));
    assert!(
        node.shared.dependency().is_ready(),
        "a failure wakes waiters"
    );
    for probe in &mut probes {
        match run(&mut probe.driver).0 {
            DriverState::Failed(error) => assert_eq!(error, failure.to_string()),
            other => panic!("probe after RHS failure: {other:?}"),
        }
        assert!(probe.output.lock().unwrap().is_empty());
    }
    // The shared phase carries the same cause to a probe operator directly.
    let operator_state = Arc::new(RuntimeState::clone(&fixture.state));
    let mut direct = node.probe.create(2, 0);
    direct.bind_runtime_state(&operator_state).unwrap();
    let processor = direct.as_processor_mut().unwrap();
    processor
        .push_chunk(&operator_state, probe_chunk(&[Some("1")], 0))
        .unwrap();
    assert_eq!(
        processor.pull_chunk(&operator_state).unwrap_err(),
        failure.to_string()
    );
    drop(direct);
    drop(probes);
    drop(build);
    assert_eq!(fixture.tracker.current(), 0);

    // A sender failure is the build's failure, never Complete. Its cause is
    // not typed, so waiting probes stay parked until the fragment publishes
    // the original error, which no derived probe message can precede.
    let fixture = task(None);
    let node = new_node(false, 1, None);
    let mut source = Source::new(vec![build_chunk(&[Some("1")])]);
    source.fail_at = Some(1);
    let mut build = build_driver(&node, source, &fixture.state);
    let mut probe = probe_driver(
        &node,
        vec![probe_chunk(&[Some("1")], 0)],
        1,
        0,
        &fixture.state,
    );
    let DriverState::Failed(error) = run(&mut build).0 else {
        panic!("the injected sender failure must fail the build driver");
    };
    assert!(error.contains("injected sender failure"));
    assert!(!node.shared.dependency().is_ready());
    assert!(matches!(
        run(&mut probe.driver).0,
        DriverState::Blocked(BlockedReason::Dependency(_))
    ));
    // What the fragment does with the build driver's error.
    fixture.state.error_state().set_error(error.clone());
    probe.driver.set_ready();
    assert_eq!(run(&mut probe.driver).0, DriverState::Failed(error));
    assert!(probe.output.lock().unwrap().is_empty());
    drop(probe);
    drop(build);
    assert_eq!(fixture.tracker.current(), 0);
}

/// Occupies all remaining task capacity so the next admission is refused.
fn occupy(tracker: &MemTracker) -> i64 {
    let occupied = tracker.limit() - tracker.current();
    tracker.consume(occupied);
    occupied
}

#[test]
fn membership_probe_capacity_refusals_are_typed_at_every_phase() {
    // 0: the result bitmaps at input; 1: a pair's parser tree; 2: the
    // admitted management bound of the output.
    for phase in 0..3 {
        let fixture = task(Some(1 << 20));
        let node = new_node(false, 1, Some(1 << 20));
        let mut build = build_driver(
            &node,
            Source::new(vec![build_chunk(&[Some(r#"{"k":[1,2,3]}"#)])]),
            &fixture.state,
        );
        assert_eq!(run(&mut build).0, DriverState::Finished);
        let mut operator = node.probe.create(1, 0);
        operator.bind_runtime_state(&fixture.state).unwrap();
        let processor = operator.as_processor_mut().unwrap();
        processor.begin_turn();
        // Phase 2 decides every row without a pair, so only the output
        // assembly can allocate after the capacity is gone.
        let values: &[Option<&str>] = if phase == 2 {
            &[None, None]
        } else {
            &[Some(r#"{"k":[1,2,3]}"#), Some("2")]
        };
        let chunk = probe_chunk(values, 0);
        if phase > 0 {
            processor.push_chunk(&fixture.state, chunk.clone()).unwrap();
        }
        let occupied = occupy(&fixture.tracker);
        let refused = if phase == 0 {
            processor.push_chunk(&fixture.state, chunk).err()
        } else {
            processor.pull_chunk(&fixture.state).err()
        };
        fixture.tracker.release(occupied);
        let failure = fixture.state.error_state().task_failure().unwrap();
        assert!(
            matches!(
                failure.category(),
                TaskFailureCategory::CapacityRefused { .. }
            ),
            "phase {phase}: {failure}"
        );
        assert_eq!(refused, Some(failure.to_string()), "phase {phase}");
        drop(operator);
        drop(build);
        assert_eq!(fixture.tracker.current(), 0, "phase {phase}");
    }
}

#[test]
fn membership_early_limit_and_last_probe_release_the_rhs() {
    let fixture = task(None);
    let node = new_node(false, 2, None);
    let mut build = build_driver(
        &node,
        Source::new(vec![build_chunk(&[Some("1"), Some(&"w".repeat(10_000))])]),
        &fixture.state,
    );
    assert_eq!(run(&mut build).0, DriverState::Finished);
    let retained = fixture.tracker.current();
    assert!(retained > 10_000, "the RHS is retained once: {retained}");
    let mut first = probe_driver(
        &node,
        vec![probe_chunk(&[Some("1")], 0)],
        2,
        0,
        &fixture.state,
    );
    assert_eq!(run(&mut first.driver).0, DriverState::Finished);
    let output = std::mem::take(&mut *first.output.lock().unwrap());
    // A finished probe leaves; the second probe still needs the RHS.
    drop(first);
    drop(output);
    assert_eq!(fixture.tracker.current(), retained);
    // An early LIMIT ends the second probe pipeline before any input.
    let mut second = probe_driver(
        &node,
        vec![probe_chunk(&[Some("2")], 0)],
        2,
        1,
        &fixture.state,
    );
    second.driver.cancel_for_fragment_abort();
    drop(second);
    assert_eq!(
        fixture.tracker.current(),
        0,
        "the last probe released the RHS"
    );
    // A later driver never reads the released RHS.
    let mut late = node.probe.create(2, 1);
    late.bind_runtime_state(&fixture.state).unwrap();
    let processor = late.as_processor_mut().unwrap();
    processor.begin_turn();
    processor
        .push_chunk(&fixture.state, probe_chunk(&[Some("1")], 0))
        .unwrap();
    assert!(
        processor
            .pull_chunk(&fixture.state)
            .unwrap_err()
            .contains("after every probe left")
    );
    drop(late);
    drop(build);
}

#[test]
fn membership_output_arrays_keep_their_charge_until_the_last_owner() {
    let fixture = task(None);
    let node = new_node(false, 1, None);
    let mut build = build_driver(
        &node,
        Source::new(vec![build_chunk(&[Some("1")])]),
        &fixture.state,
    );
    assert_eq!(run(&mut build).0, DriverState::Finished);
    drop(build);
    let rhs = fixture.tracker.current();
    // The probe chunk arrives charged to the task, as a CheckedTask Project
    // publishes it.
    let mut chunk = probe_chunk(&[Some("1"), None, Some(&"p".repeat(10_000))], 0);
    chunk.try_transfer_to(&fixture.tracker).unwrap();
    let probe_bytes = fixture.tracker.current() - rhs;
    let mut operator = node.probe.create(1, 0);
    operator.bind_runtime_state(&fixture.state).unwrap();
    let processor = operator.as_processor_mut().unwrap();
    processor.begin_turn();
    processor.push_chunk(&fixture.state, chunk).unwrap();
    assert!(!processor.need_input(), "at most one pending probe chunk");
    let output = processor.pull_chunk(&fixture.state).unwrap().unwrap();
    assert_eq!(
        results(std::slice::from_ref(&output)),
        vec![Some(true), None, Some(false)]
    );
    let probe_column = Arc::clone(&output.columns()[0]);
    let result_slice = output.columns()[2].slice(1, 1);
    drop(output);
    drop(operator);
    assert!(
        fixture.tracker.current() >= probe_bytes,
        "the probe chunk stays charged while its column lives"
    );
    drop(probe_column);
    let after_probe = fixture.tracker.current();
    assert!(after_probe > 0, "the result slice keeps its bitmap charged");
    assert!(after_probe < probe_bytes);
    drop(result_slice);
    assert_eq!(fixture.tracker.current(), 0);
}

#[test]
fn membership_probe_hot_path_admits_every_allocation() {
    use arrow::array::{
        BinaryArray, Date32Array, Decimal128Array, DictionaryArray, FixedSizeBinaryArray,
        LargeStringArray, ListArray, MapBuilder, StringBuilder, StringViewArray, StructArray,
        TimestampMicrosecondArray,
    };
    use arrow::datatypes::Int32Type;

    let rows = 6;
    let list = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
        (0..rows as i32).map(|i| (i % 3 != 0).then(|| vec![Some(i), None])),
    )) as ArrayRef;
    let structs = Arc::new(StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int64, true)),
            Arc::new(Int64Array::from_iter(
                (0..rows as i64 + 2).map(|i| (i % 2 == 0).then_some(i)),
            )) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Utf8, true)),
            Arc::new(StringArray::from_iter(
                (0..rows + 2).map(|i| (i % 3 != 0).then(|| format!("s{i}"))),
            )) as ArrayRef,
        ),
    ]))
    .slice(2, rows);
    let structs = Arc::new(structs) as ArrayRef;
    let mut maps = MapBuilder::new(
        None,
        StringBuilder::new(),
        arrow::array::Int32Builder::new(),
    );
    for row in 0..rows {
        maps.keys().append_value(format!("k{row}"));
        maps.values().append_value(row as i32);
        maps.append(row % 2 == 0).unwrap();
    }
    let maps = Arc::new(maps.finish()) as ArrayRef;
    let dictionary = Arc::new(
        (0..rows)
            .map(|i| if i % 2 == 0 { "even" } else { "odd" })
            .collect::<DictionaryArray<Int32Type>>(),
    ) as ArrayRef;
    let fixed =
        Arc::new(FixedSizeBinaryArray::try_from_iter((0..rows).map(|i| [i as u8; 4])).unwrap())
            as ArrayRef;
    let extras: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(0..rows as i64)),
        Arc::new(LargeStringArray::from_iter_values(
            (0..rows).map(|i| format!("l{i}")),
        )),
        Arc::new(BinaryArray::from_iter_values(
            (0..rows).map(|i| vec![i as u8; 3]),
        )),
        list,
        structs,
        maps,
        dictionary,
        fixed,
        Arc::new(
            Decimal128Array::from_iter_values((0..rows as i128).map(|i| i * 100))
                .with_precision_and_scale(10, 2)
                .unwrap(),
        ),
        Arc::new(StringViewArray::from_iter_values(
            (0..rows).map(|i| format!("view-value-longer-than-twelve-{i}")),
        )),
        Arc::new(Date32Array::from_iter_values(0..rows as i32)),
        Arc::new(TimestampMicrosecondArray::from_iter_values(0..rows as i64)),
        Arc::new(BooleanArray::from_iter(
            (0..rows).map(|i| (i % 4 != 0).then_some(i % 2 == 0)),
        )),
        Arc::new(arrow::array::NullArray::new(rows)),
    ];
    let probe_values = vec![
        Some(r#"{"a":1}"#),
        None,
        Some("[1,2]"),
        Some("2"),
        Some(r#""x""#),
        Some(r#"{"b":[null]}"#),
    ];
    let rhs_values = vec![Some(r#"{"a": 1}"#), None, Some("[2,1]"), Some("2.0")];
    let mut probe_columns = vec![Arc::new(StringArray::from(probe_values.clone())) as ArrayRef];
    probe_columns.extend(extras.iter().cloned());
    let mut probe_slots = vec![(PROBE, DataType::Utf8)];
    for (index, column) in extras.iter().enumerate() {
        probe_slots.push((10 + index as u32, column.data_type().clone()));
    }
    let probe_schema = schema(&probe_slots);
    let mut output_slots = probe_slots.clone();
    output_slots.push((RESULT, DataType::Boolean));
    let probe_chunk =
        Chunk::try_new_with_columns(Arc::clone(&probe_schema), probe_columns).unwrap();

    for units in [1 << 20, 7] {
        let fixture = task(None);
        let manager = DependencyManager::new();
        let shared =
            MembershipShared::try_new(7, SlotId::new(BUILD), &build_schema(), 1, &manager).unwrap();
        let factory = MembershipProbeFactory::try_new(
            Arc::clone(&shared),
            &spec(false),
            &probe_schema,
            schema(&output_slots),
        )
        .unwrap()
        .with_work_units(units);
        let build = MembershipBuildSinkFactory::new(Arc::clone(&shared));
        let mut build = driver(
            100,
            vec![
                Box::new(Source::new(vec![build_chunk(&rhs_values)])),
                build.create(1, 0),
            ],
            &fixture.state,
        );
        assert_eq!(run(&mut build).0, DriverState::Finished);
        let mut operator = factory.create(1, 0);
        operator.bind_runtime_state(&fixture.state).unwrap();
        let processor = operator.as_processor_mut().unwrap();
        let input = probe_chunk.clone();
        let (output, report) = with_admission_witness(&fixture.tracker, || {
            processor.push_chunk(&fixture.state, input).unwrap();
            let mut turns = 0;
            loop {
                processor.begin_turn();
                if let Some(output) = processor.pull_chunk(&fixture.state).unwrap() {
                    break (output, turns);
                }
                assert!(processor.take_yield_request());
                turns += 1;
            }
        });
        let (output, turns) = output;
        assert_eq!(
            report.non_admitted_allocations, 0,
            "units={units} turns={turns} report={report:?}"
        );
        assert!(report.admitted_bytes > 0);
        assert_eq!(
            output.columns()[2..output.columns().len() - 1].len(),
            extras.len() - 1
        );
        for (index, column) in extras.iter().enumerate() {
            assert_eq!(output.columns()[index + 1].as_ref(), column.as_ref());
        }
        let result = output.columns().last().unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(
            result.iter().collect::<Vec<_>>(),
            expected(&probe_values, &rhs_values, false)
        );
        drop(output);
        drop(operator);
        drop(build);
        assert_eq!(fixture.tracker.current(), 0);
    }
}

#[test]
fn membership_rejects_unsupported_layouts_and_a_second_build_driver() {
    let manager = DependencyManager::new();
    assert!(
        MembershipShared::try_new(1, SlotId::new(BUILD), &probe_schema(), 1, &manager).is_err()
    );
    let shared =
        MembershipShared::try_new(1, SlotId::new(BUILD), &build_schema(), 1, &manager).unwrap();
    let wrong_output = schema(&[(PROBE, DataType::Utf8), (RESULT, DataType::Boolean)]);
    assert!(
        MembershipProbeFactory::try_new(
            Arc::clone(&shared),
            &spec(false),
            &probe_schema(),
            wrong_output
        )
        .is_err()
    );
    let mut not_json = spec(false);
    not_json.probe = SlotId::new(EXTRA);
    assert!(
        MembershipProbeFactory::try_new(
            Arc::clone(&shared),
            &not_json,
            &probe_schema(),
            output_schema()
        )
        .is_err()
    );
    let fixture = task(None);
    let mut second = MembershipBuildSinkFactory::new(shared).create(2, 0);
    assert!(second.bind_runtime_state(&fixture.state).is_err());
}

#[test]
fn membership_output_batch_layout_is_probe_columns_then_result() {
    let fixture = task(None);
    let outputs = execute(
        &fixture,
        true,
        &[vec![Some("1")]],
        &[vec![vec![Some("1"), Some("2")]]],
        None,
    );
    let batch: &RecordBatch = &outputs[0][0].batch;
    assert_eq!(batch.schema(), output_schema().arrow_schema_ref());
    assert_eq!(results(&outputs[0]), vec![Some(false), Some(true)]);
}
