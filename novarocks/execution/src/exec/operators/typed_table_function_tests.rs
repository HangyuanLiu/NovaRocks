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
use super::*;
use crate::exec::chunk::{ChunkSchema, ChunkSlotSchema};
use crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory;
use crate::runtime::fragment::{ExecutionFailureCause, PipelineOperation};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeErrorState;
use arrow::array::{ListBuilder, StringArray, StringBuilder};
use arrow::datatypes::{Field, Schema};
use novarocks_type_contract::{
    FunctionValueType, TypedValueBinding, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
    ValueTypeStorageLifetime,
};
fn declared(kind: ValueKind, carrier: DataType, nullable: bool) -> TypedValueBinding {
    let storage = ValueTypeStorageLifetime::pure();
    TypedValueBinding::try_new(
        ValueTypeRef::try_new(kind, nullable, storage.clone()).unwrap(),
        ValueLayout::try_new(&carrier, storage).unwrap(),
    )
    .unwrap()
}
fn layout(fields: Vec<(u32, &str, TypedValueBinding)>) -> StaticLayout {
    let slots = fields
        .iter()
        .map(|(slot, _, _)| SlotId::new(*slot))
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(
        fields
            .iter()
            .map(|(_, name, binding)| {
                Field::new(*name, binding.data_type().clone(), binding.nullable())
            })
            .collect::<Vec<_>>(),
    ));
    StaticLayout::try_new_typed_for_compile(
        schema,
        Arc::from(slots),
        fields
            .into_iter()
            .map(|(_, _, binding)| (binding, None))
            .collect(),
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &Control,
    )
    .unwrap()
}
struct Control;
impl novarocks_type_contract::PureCompileControl for Control {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}
struct Source {
    state: RuntimeState,
    tracker: Arc<MemTracker>,
    factory: Arc<dyn InputConstructionFactory>,
    input: StaticLayout,
    output: StaticLayout,
}
fn source_fixture(json_result: bool) -> Source {
    let item = declared(
        ValueKind::Nominal(ValueLogicalType::Json),
        DataType::Utf8,
        false,
    );
    let input = layout(vec![(
        1,
        "json_list",
        declared(
            ValueKind::List(item.value_type().clone()),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
        ),
    )]);
    let output = layout(vec![(
        2,
        "item",
        declared(
            if json_result {
                ValueKind::Nominal(ValueLogicalType::Json)
            } else {
                ValueKind::String
            },
            DataType::Utf8,
            true,
        ),
    )]);
    let state = RuntimeState::new(None, None, None, None, None, None, None);
    let tracker = MemTracker::new_root("native-table-function-original-owner");
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker.clone(),
        state.error_state(),
    )
    .unwrap();
    Source {
        state,
        tracker,
        factory,
        input,
        output,
    }
}
fn make_operator(source: &Source) -> Box<dyn Operator> {
    let factory = TableFunctionProcessorFactory::new_typed(
        3,
        "unnest".into(),
        vec![SlotId::new(1)],
        vec![],
        vec![SlotId::new(2)],
        true,
        true,
        vec![source.input.schema().field(0).data_type().clone()],
        vec![DataType::Utf8],
        source.input.clone(),
        source.output.clone(),
        vec![TableFunctionOutputSlot::Result { index: 0 }],
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    operator.set_construction_factory(source.factory.clone());
    operator
}
fn input(source: &Source) -> Chunk {
    let mut builder = ListBuilder::new(StringBuilder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Utf8,
        false,
    )));
    builder.values().append_value("{\"v\":1}");
    builder.append(true);
    builder.append(false);
    builder.append(true);
    let array = Arc::new(builder.finish()) as ArrayRef;
    let schema = source
        .factory
        .create_compiled_chunk_schema(&source.input)
        .unwrap();
    let owner = source.factory.begin_typed_construction(&schema).unwrap();
    owner
        .request(Layout::array::<ArrayRef>(1).unwrap())
        .unwrap();
    Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        vec![array],
        3,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .unwrap()
}
#[test]
fn native_table_function_complete_json_child_and_last_output_projection_keep_creation() {
    let source = source_fixture(true);
    let mut operator = make_operator(&source);
    operator.prepare().unwrap();
    operator.bind_runtime_state(&source.state).unwrap();
    operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&source.state, input(&source))
        .unwrap();
    let output = operator
        .as_processor_mut()
        .unwrap()
        .pull_chunk(&source.state)
        .unwrap()
        .unwrap();
    assert_eq!(output.len(), 3, "original LEFT UNNEST padding algorithm");
    let values = output.columns()[0]
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(0), "{\"v\":1}");
    assert!(values.is_null(1) && values.is_null(2));
    assert_eq!(
        output.chunk_schema().slots()[0].typed_binding(),
        source.output.value_binding_at(0).unwrap()
    );
    let projection = output.into_batch_projection();
    let tracker = source.tracker.clone();
    drop(operator);
    drop(source);
    assert!(tracker.current() > 0);
    drop(projection);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn native_table_function_rejects_same_carrier_wrong_domain_and_missing_owner() {
    let source = source_fixture(false);
    let mut operator = make_operator(&source);
    let failure = operator
        .prepare()
        .expect_err("Text cannot replace original JSON child meaning");
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::InvalidSource(_))
    ));
    drop(operator);
    drop(source);
    let source = source_fixture(true);
    let factory = TableFunctionProcessorFactory::new_typed(
        3,
        "unnest".into(),
        vec![SlotId::new(1)],
        vec![],
        vec![SlotId::new(2)],
        true,
        true,
        vec![source.input.schema().field(0).data_type().clone()],
        vec![DataType::Utf8],
        source.input.clone(),
        source.output.clone(),
        vec![TableFunctionOutputSlot::Result { index: 0 }],
    )
    .unwrap();
    let mut operator = factory.create(1, 0);
    assert!(matches!(
        operator.prepare().unwrap_err().cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::MissingOwner)
    ));
    drop(operator);
    let tracker = source.tracker.clone();
    drop(source);
    assert_eq!(tracker.current(), 0);
}
#[test]
fn native_table_function_preserves_first_memory_and_caller_refusal_without_tail() {
    let source = source_fixture(true);
    let mut operator = make_operator(&source);
    let baseline = source.tracker.current();
    source.tracker.install_limit_once(baseline).unwrap();
    let failure = operator
        .prepare()
        .expect_err("next real construction is refused");
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(source.tracker.current(), baseline);
    let tracker = source.tracker.clone();
    drop(operator);
    drop(source);
    assert_eq!(tracker.current(), 0);
    let source = caller_source();
    let mut operator = make_operator(&source);
    let original = ExecutionFailure::from("original native table function caller".to_owned())
        .at_operator(3, PipelineOperation::Admission);
    source.state.error_state().set_error(original.clone());
    let before = source.tracker.allocated();
    let failure = operator.prepare().unwrap_err();
    assert_eq!(failure.cause(), original.cause());
    assert_eq!(failure.context(), original.context());
    assert_eq!(
        source.tracker.allocated(),
        before,
        "no actual allocation after first Caller refusal"
    );
    let tracker = source.tracker.clone();
    drop(operator);
    drop(source);
    assert_eq!(tracker.current(), 0);
}
fn caller_source() -> Source {
    source_fixture(true)
}

#[test]
fn native_table_function_output_headers_outlive_temporary_operation_scratch() {
    let source = source_fixture(true);
    let mut operator = make_operator(&source);
    operator.prepare().unwrap();
    let input = input(&source);
    let before_allocated = source.tracker.allocated();
    let before_live = source.tracker.current();
    operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&source.state, input.clone())
        .unwrap();
    let output = operator
        .as_processor_mut()
        .unwrap()
        .pull_chunk(&source.state)
        .unwrap()
        .unwrap();
    assert_eq!(output.len(), 3);
    let allocated = source.tracker.allocated() - before_allocated;
    let retained = source.tracker.current() - before_live;
    assert!(retained > 0, "the actual returned wrappers remain live");
    assert!(
        allocated > retained,
        "temporary source-reference/model scopes exited after the opaque operation"
    );
    let projection = output.into_batch_projection();
    let tracker = source.tracker.clone();
    drop(input);
    drop(operator);
    drop(source);
    assert!(tracker.current() > 0);
    drop(projection);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn native_table_function_materialization_first_memory_refusal_keeps_original_input() {
    let source = source_fixture(true);
    let mut operator = make_operator(&source);
    operator.prepare().unwrap();
    let input = input(&source);
    let original = input.into_batch_projection();
    let input = original.clone().into_chunk();
    let baseline = source.tracker.current();
    source.tracker.install_limit_once(baseline).unwrap();
    let failure = operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&source.state, input)
        .unwrap_err();
    assert!(matches!(
        failure.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert_eq!(
        source.tracker.current(),
        baseline,
        "refused operation has no charged tail and original input remains pinned"
    );
    assert_eq!(original.record_batch().num_rows(), 3);
    let tracker = source.tracker.clone();
    drop(original);
    drop(operator);
    drop(source);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn native_table_function_materialization_preserves_original_caller_before_any_constructor() {
    let source = source_fixture(true);
    let mut operator = make_operator(&source);
    operator.prepare().unwrap();
    let input = input(&source);
    let original_input = input.clone();
    let failure = ExecutionFailure::from("original materialization caller".to_owned())
        .at_operator(3, PipelineOperation::Admission);
    source.state.error_state().set_error(failure.clone());
    let before = source.tracker.allocated();
    let actual = operator
        .as_processor_mut()
        .unwrap()
        .push_chunk(&source.state, input)
        .unwrap_err();
    assert_eq!(actual.cause(), failure.cause());
    assert_eq!(actual.context(), failure.context());
    assert_eq!(
        source.tracker.allocated(),
        before,
        "no source packet, scratch or output wrapper was constructed after refusal"
    );
    assert_eq!(original_input.len(), 3);
    let tracker = source.tracker.clone();
    drop(original_input);
    drop(operator);
    drop(source);
    assert_eq!(tracker.current(), 0);
}
