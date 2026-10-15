// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file distributed
// with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with it.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Functional tests of the default sort factory and its installed BE creator.
//! Declarations are pure compiler facts; every runtime scaffold uses the real
//! tracker/error signal. Original row payload accounting is a separate policy.

use super::sort_processor::SortProcessorFactory;
use crate::exec::chunk::Chunk;
use crate::exec::expr::{ExprArena, ExprNode};
use crate::exec::node::sort::{SortExpression, SortTopNType};
use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
use crate::exec::pipeline::operator::{ConstructionFailure, InputConstructionFactory, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::runtime::fragment::{ExecutionFailure, ExecutionFailureCause, PipelineOperation};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;
use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use novarocks_local_program::StaticLayout;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionValueType, PureCompileControl, TypedValueBinding,
    ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};
use novarocks_types::SlotId;
use std::{alloc::Layout, sync::Arc};

struct DeclarationControl;
impl PureCompileControl for DeclarationControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
fn declared(kind: ValueKind, carrier: DataType, nullable: bool) -> TypedValueBinding {
    let storage = ValueTypeStorageLifetime::pure();
    TypedValueBinding::try_new(
        ValueTypeRef::try_new(kind, nullable, storage.clone()).unwrap(),
        ValueLayout::try_new(&carrier, storage).unwrap(),
    )
    .unwrap()
}
fn input_layout(payload: ValueKind) -> StaticLayout {
    let bindings = vec![
        declared(ValueKind::Scalar(DataType::Int32), DataType::Int32, true),
        declared(payload, DataType::Utf8, false),
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, true),
        Field::new("document", DataType::Utf8, false),
    ]));
    StaticLayout::try_new_typed_for_compile(
        schema,
        Arc::from([SlotId::new(1), SlotId::new(2)]),
        bindings
            .into_iter()
            .map(|binding| (binding, None))
            .collect(),
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &DeclarationControl,
    )
    .unwrap()
}
struct Source {
    layout: StaticLayout,
    state: RuntimeState,
    tracker: Arc<MemTracker>,
    creator: Arc<dyn InputConstructionFactory>,
}
fn source_fixture() -> Source {
    let state = RuntimeState::new(None, None, None, None, None, None, None);
    let tracker = MemTracker::new_root("default-sort-actual-owner");
    let creator = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        state.error_state(),
    )
    .unwrap();
    Source {
        layout: input_layout(ValueKind::Nominal(ValueLogicalType::Json)),
        state,
        tracker,
        creator,
    }
}
fn sort_factory(
    source: &Source,
    topn: bool,
    limit: Option<usize>,
    offset: usize,
    kind: SortTopNType,
    prune_rows: Option<usize>,
) -> (SortProcessorFactory, std::sync::Weak<ExprArena>) {
    let mut arena = ExprArena::default();
    let key = arena.push_value_typed(
        ExprNode::SlotId(SlotId::new(1)),
        FunctionValueType::try_from_typed_binding(
            source.layout.value_binding_at(0).unwrap().clone(),
        )
        .unwrap(),
    );
    let arena = Arc::new(arena);
    let weak = Arc::downgrade(&arena);
    let order = vec![SortExpression {
        expr: key,
        asc: true,
        nulls_first: true,
    }];
    let factory = if topn {
        SortProcessorFactory::new_topn(
            71,
            arena,
            order,
            limit,
            offset,
            kind,
            prune_rows,
            None,
            vec![],
            None,
        )
    } else {
        SortProcessorFactory::new(
            71,
            arena,
            order,
            limit,
            offset,
            kind,
            prune_rows,
            None,
            vec![],
            None,
        )
    };
    (factory.with_input_layout(source.layout.clone()), weak)
}
fn make_operator(source: &Source, factory: &SortProcessorFactory) -> Box<dyn Operator> {
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(source.creator.clone());
    operator.prepare().unwrap();
    operator.bind_runtime_state(&source.state).unwrap();
    operator
}
fn chunk_for_layout(
    source: &Source,
    layout: &StaticLayout,
    keys: Vec<Option<i32>>,
    documents: Vec<String>,
) -> Chunk {
    assert_eq!(keys.len(), documents.len());
    let rows = keys.len();
    let arrays = [
        Arc::new(Int32Array::from(keys)) as ArrayRef,
        Arc::new(StringArray::from(documents)) as ArrayRef,
    ];
    let schema = source.creator.create_compiled_chunk_schema(layout).unwrap();
    let owner = source.creator.begin_typed_construction(&schema).unwrap();
    owner
        .request(Layout::array::<ArrayRef>(2).unwrap())
        .unwrap();
    Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        arrays.into_iter().collect(),
        rows,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap()
}
fn chunk(source: &Source, keys: Vec<Option<i32>>) -> Chunk {
    let documents = keys
        .iter()
        .enumerate()
        .map(|(ordinal, key)| {
            format!(
                "{{\"key\":{},\"ordinal\":{ordinal}}}",
                key.map_or_else(|| "null".to_owned(), |key| key.to_string())
            )
        })
        .collect();
    chunk_for_layout(source, &source.layout, keys, documents)
}
fn keys(chunk: &Chunk) -> Vec<Option<i32>> {
    chunk.columns()[0]
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .iter()
        .collect()
}
fn finish(operator: &mut dyn Operator, state: &RuntimeState) -> Chunk {
    let processor = operator.as_processor_mut().unwrap();
    processor.set_finishing(state).unwrap();
    let output = processor.pull_chunk(state).unwrap().unwrap();
    assert!(processor.is_finished());
    assert!(processor.pull_chunk(state).unwrap().is_none());
    output
}
fn assert_retired(source: Source, operator: Box<dyn Operator>, output: Chunk) {
    let projection = output.into_batch_projection();
    let tracker = source.tracker.clone();
    let factory = Arc::downgrade(&source.creator);
    drop(operator);
    drop(source);
    assert!(
        factory.upgrade().is_none(),
        "output does not retain the installed factory"
    );
    assert!(
        tracker.current() > 0,
        "independent output holds its finite creators"
    );
    assert!(projection.record_batch().schema().metadata().capacity() == 0);
    drop(projection);
    assert_eq!(
        tracker.current(),
        0,
        "last independent output retires all scaffold charges"
    );
}

#[test]
fn default_sort_global_nulls_ties_offset_and_last_projection_keep_exact_declaration() {
    let source = source_fixture();
    let (factory, arena) = sort_factory(&source, false, Some(4), 2, SortTopNType::RowNumber, None);
    let mut operator = make_operator(&source, &factory);
    drop(factory);
    let first = chunk_for_layout(
        &source,
        &source.layout,
        vec![None, Some(2), Some(1), Some(1)],
        ["null", "two", "first", "second"]
            .map(|s| format!("\"{s}\""))
            .into(),
    );
    let second = chunk_for_layout(
        &source,
        &source.layout,
        vec![Some(0), Some(1), Some(3)],
        ["zero", "third", "three"]
            .map(|s| format!("\"{s}\""))
            .into(),
    );
    operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&source.state, first)
        .unwrap();
    operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&source.state, second)
        .unwrap();
    let output = finish(operator.as_mut(), &source.state);
    assert_eq!(keys(&output), vec![Some(1), Some(1), Some(1), Some(2)]);
    let documents = output.columns()[1]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        documents.iter().collect::<Vec<_>>(),
        vec![
            Some("\"first\""),
            Some("\"second\""),
            Some("\"third\""),
            Some("\"two\"")
        ]
    );
    assert_eq!(
        output.chunk_schema().slots()[1].typed_binding(),
        source.layout.value_binding_at(1).unwrap()
    );
    let projection = output.into_batch_projection();
    let tracker = source.tracker.clone();
    let creator = Arc::downgrade(&source.creator);
    drop(operator);
    drop(source);
    assert!(
        arena.upgrade().is_none(),
        "output retains no expression arena/program"
    );
    assert!(creator.upgrade().is_none());
    assert!(tracker.current() > 0);
    assert_eq!(projection.record_batch().num_rows(), 4);
    drop(projection);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn default_sort_rank_and_dense_rank_keep_cross_batch_boundary_peers_after_pruning() {
    for (kind, expected) in [
        (SortTopNType::Rank, vec![Some(1), Some(1)]),
        (
            SortTopNType::DenseRank,
            vec![Some(1), Some(1), Some(2), Some(2), Some(2)],
        ),
    ] {
        let source = source_fixture();
        let (factory, _) = sort_factory(&source, true, Some(2), 0, kind, Some(3));
        let mut operator = make_operator(&source, &factory);
        drop(factory);
        operator
            .as_processor_mut()
            .unwrap()
            .push_chunk(
                &source.state,
                chunk(&source, vec![Some(4), Some(2), Some(2), Some(1)]),
            )
            .unwrap();
        operator
            .as_processor_mut()
            .unwrap()
            .push_chunk(
                &source.state,
                chunk(&source, vec![Some(2), Some(1), Some(3)]),
            )
            .unwrap();
        let output = finish(operator.as_mut(), &source.state);
        assert_eq!(keys(&output), expected);
        assert_retired(source, operator, output);
    }
}

#[test]
fn default_sort_heap_threshold_sides_preserve_offset_and_late_best_rows() {
    // offset + limit is the existing 1024/1025 strategy boundary, not a new cap.
    for rows_to_keep in [1024usize, 1025] {
        let source = source_fixture();
        let offset = 3;
        let limit = rows_to_keep - offset;
        let (factory, _) = sort_factory(
            &source,
            true,
            Some(limit),
            offset,
            SortTopNType::RowNumber,
            Some(rows_to_keep + 1),
        );
        let mut operator = make_operator(&source, &factory);
        drop(factory);
        let mut original = (0..(rows_to_keep * 2) as i32)
            .rev()
            .map(Some)
            .collect::<Vec<_>>();
        operator
            .as_processor_mut()
            .unwrap()
            .push_chunk(&source.state, chunk(&source, original.clone()))
            .unwrap();
        let late = (-6..0).map(Some).collect::<Vec<_>>();
        operator
            .as_processor_mut()
            .unwrap()
            .push_chunk(&source.state, chunk(&source, late.clone()))
            .unwrap();
        original.extend(late);
        original.sort_unstable();
        let output = finish(operator.as_mut(), &source.state);
        assert_eq!(
            keys(&output),
            original
                .into_iter()
                .skip(offset)
                .take(limit)
                .collect::<Vec<_>>()
        );
        assert_retired(source, operator, output);
    }
}

#[test]
fn default_sort_rejects_same_carrier_wrong_complete_domain_before_buffering() {
    let source = source_fixture();
    let (factory, _) = sort_factory(&source, false, None, 0, SortTopNType::RowNumber, None);
    let mut operator = make_operator(&source, &factory);
    drop(factory);
    let foreign = input_layout(ValueKind::String);
    let input = chunk_for_layout(
        &source,
        &foreign,
        vec![Some(2), Some(1)],
        vec!["text".into(), "text".into()],
    );
    let original = input.into_batch_projection();
    let before = source.tracker.allocated();
    let failure = operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&source.state, original.clone().into_chunk())
        .unwrap_err();
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::InvalidSource(_))
    ));
    assert_eq!(
        source.tracker.allocated(),
        before,
        "no construction after semantic refusal"
    );
    assert_eq!(original.record_batch().num_rows(), 2);
    let tracker = source.tracker.clone();
    drop(original);
    drop(operator);
    drop(source);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn default_sort_requires_installed_prepare_owner_and_preserves_first_memory_refusal() {
    let source = source_fixture();
    let (factory, _) = sort_factory(&source, false, None, 0, SortTopNType::RowNumber, None);
    let mut operator = factory.create(1, 0);
    assert!(operator.creates_typed_storage());
    assert!(matches!(
        operator.prepare().unwrap_err().cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
    ));
    operator.set_construction_factory(source.creator.clone());
    let before = source.tracker.current();
    source.tracker.install_limit_once(before).unwrap();
    let failure = operator.prepare().unwrap_err();
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(
        source.tracker.current(),
        before,
        "refused prepare scope is rolled back"
    );
    let tracker = source.tracker.clone();
    drop(operator);
    drop(factory);
    drop(source);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn default_sort_materialization_keeps_first_memory_caller_and_original_input() {
    let source = source_fixture();
    let (factory, _) = sort_factory(&source, false, None, 0, SortTopNType::RowNumber, None);
    let mut operator = make_operator(&source, &factory);
    drop(factory);
    let original_input = chunk(&source, vec![Some(2), Some(1)]).into_batch_projection();
    operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&source.state, original_input.clone().into_chunk())
        .unwrap();
    let baseline = source.tracker.current();
    source.tracker.install_limit_once(baseline).unwrap();
    let failure = operator
        .as_processor_mut()
        .unwrap()
        .set_finishing(&source.state)
        .unwrap_err();
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(
        source.tracker.current(),
        baseline,
        "first failed construction has no live charged tail"
    );
    assert_eq!(original_input.record_batch().num_rows(), 2);
    let tracker = source.tracker.clone();
    drop(operator);
    drop(source);
    assert!(tracker.current() > 0);
    drop(original_input);
    assert_eq!(tracker.current(), 0);
    let source = source_fixture();
    let (factory, _) = sort_factory(&source, false, None, 0, SortTopNType::RowNumber, None);
    let mut operator = make_operator(&source, &factory);
    drop(factory);
    let input = chunk(&source, vec![Some(2), Some(1)]).into_batch_projection();
    operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&source.state, input.clone().into_chunk())
        .unwrap();
    let original =
        ExecutionFailure::from("original default sort materialization caller".to_owned())
            .at_operator(71, PipelineOperation::Admission);
    source.state.error_state().set_error(original.clone());
    let before = source.tracker.allocated();
    let failure = operator
        .as_processor_mut()
        .unwrap()
        .set_finishing(&source.state)
        .unwrap_err();
    assert_eq!(failure.cause(), original.cause());
    assert_eq!(failure.context(), original.context());
    assert_eq!(
        source.tracker.allocated(),
        before,
        "no constructor after first original Caller refusal"
    );
    assert_eq!(
        input
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[2, 1]
    );
    let tracker = source.tracker.clone();
    drop(operator);
    drop(source);
    assert!(
        tracker.current() > 0,
        "independent original input remains valid after refusal"
    );
    drop(input);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn default_sort_fixed16_largeint_and_uuid_keep_their_declared_order_through_real_owner() {
    use arrow::array::FixedSizeBinaryArray;
    for (logical, expected) in [
        (ValueLogicalType::LargeInt, vec![-1_i128, 0, 1]),
        (ValueLogicalType::Uuid, vec![0_i128, 1, -1]),
    ] {
        let binding = declared(
            ValueKind::Nominal(logical),
            DataType::FixedSizeBinary(16),
            false,
        );
        let layout = StaticLayout::try_new_typed_for_compile(
            Arc::new(Schema::new(vec![Field::new(
                "key",
                DataType::FixedSizeBinary(16),
                false,
            )])),
            Arc::from([SlotId::new(1)]),
            vec![(binding, None)],
            ValueTypeStorageLifetime::pure(),
            |_| Ok(()),
            &DeclarationControl,
        )
        .unwrap();
        let mut source = source_fixture();
        source.layout = layout;
        let (factory, arena) = sort_factory(&source, false, None, 0, SortTopNType::RowNumber, None);
        let mut operator = make_operator(&source, &factory);
        drop(factory);
        let schema = source
            .creator
            .create_compiled_chunk_schema(&source.layout)
            .unwrap();
        let owner = source.creator.begin_typed_construction(&schema).unwrap();
        owner
            .request(Layout::array::<ArrayRef>(1).unwrap())
            .unwrap();
        let bytes = [
            1_i128.to_be_bytes(),
            (-1_i128).to_be_bytes(),
            0_i128.to_be_bytes(),
        ];
        let values =
            FixedSizeBinaryArray::try_from_iter(bytes.iter().map(|b| b.as_slice())).unwrap();
        let input = Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
            schema,
            vec![Arc::new(values) as ArrayRef],
            3,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )
        .unwrap();
        drop(owner);
        operator
            .as_processor_mut()
            .unwrap()
            .push_chunk(&source.state, input)
            .unwrap();
        let output = finish(operator.as_mut(), &source.state);
        let values = output.columns()[0]
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .unwrap();
        let actual = (0..3)
            .map(|row| i128::from_be_bytes(values.value(row).try_into().unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(
            output.chunk_schema().slots()[0]
                .typed_binding()
                .value_type()
                .kind(),
            &ValueKind::Nominal(logical)
        );
        let projection = output.into_batch_projection();
        let tracker = source.tracker.clone();
        drop(operator);
        drop(source);
        assert!(arena.upgrade().is_none());
        assert!(tracker.current() > 0);
        drop(projection);
        assert_eq!(tracker.current(), 0);
    }
}
