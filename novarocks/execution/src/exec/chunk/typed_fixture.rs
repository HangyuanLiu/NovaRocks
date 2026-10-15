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

//! Explicit fixture declarations and the actual BE construction host.
use super::Chunk;
use crate::exec::pipeline::operator::input_construction::{
    ConstructionFailure, try_runtime_input_construction_factory,
};
use crate::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
use arrow::record_batch::RecordBatch;
use novarocks_local_program::StaticLayout;
use novarocks_type_contract::{FunctionValueType, ValueTypeStorageLifetime};
use novarocks_types::SlotId;
use std::sync::Arc;

struct DeclarationControl;
impl novarocks_type_contract::PureCompileControl for DeclarationControl {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}

/// Freeze a declared original logical call from the fixture's actual catalog.
/// Merge input state arrays are not logical arguments or type-domain authors.
pub(crate) fn frozen_aggregate(
    catalog: &novarocks_functions::EngineFunctionCatalog,
    name: &str,
    logical_types: &[FunctionValueType],
) -> Arc<novarocks_local_program::FrozenAggregateSelection> {
    use novarocks_functions::{FunctionArgument, FunctionBindingRequest, FunctionKind};
    let arguments = logical_types
        .iter()
        .cloned()
        .map(|value_type| FunctionArgument::Value {
            value_type,
            constant: None,
        })
        .collect::<Vec<_>>();
    let request = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    };
    let bound = catalog
        .resolve_bound_trusted(name, FunctionKind::Aggregate, request, &DeclarationControl)
        .expect("original complete aggregate declaration");
    catalog
        .validate_bound(&bound, request, &DeclarationControl)
        .expect("installed original aggregate selection");
    Arc::new(
        novarocks_local_program::FrozenAggregateSelection::try_new(
            bound.function_id,
            Arc::new(bound.selected),
            arguments.len(),
            ValueTypeStorageLifetime::pure(),
        )
        .expect("complete original aggregate fixture"),
    )
}

pub(crate) fn layout(
    schema: arrow::datatypes::SchemaRef,
    slots: &[SlotId],
    types: &[FunctionValueType],
) -> StaticLayout {
    // Pure source declarations are supplied by each fixture, never recovered
    // from row values, Field annotations or an observed first batch.
    StaticLayout::try_new_typed_for_compile(
        schema,
        Arc::from(slots),
        types
            .iter()
            .map(|ty| (ty.typed_binding().clone(), None))
            .collect(),
        ValueTypeStorageLifetime::pure(),
        |_| Ok(()),
        &DeclarationControl,
    )
    .expect("complete fixture layout")
}

pub(crate) fn chunk(batch: RecordBatch, slots: &[SlotId], types: &[FunctionValueType]) -> Chunk {
    let declared = layout(batch.schema(), slots, types);
    let tracker = MemTracker::new_root("explicit native fixture construction");
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        tracker,
        Arc::new(RuntimeErrorState::default()),
    )
    .expect("real fixture construction host");
    let schema = factory
        .create_compiled_chunk_schema(&declared)
        .expect("created fixture schema");
    let owner = factory
        .begin_typed_construction(&schema)
        .expect("fixture wrapper owner");
    // The original Arrow payload is fixture data. Only newly created schema
    // and wrapper responsibility is charged here; existing payload policy stays.
    Chunk::try_new_typed_record_batch_owned_observed::<ConstructionFailure, _, _>(
        batch,
        schema,
        ValueTypeStorageLifetime::pure(),
        owner.storage_lifetime(),
        |request| owner.request(request),
        |step| owner.observe(0, &[], step),
    )
    .expect("created complete fixture chunk")
}

/// Freeze the genuine construction tree on the same actual finite host.
/// Unlike the expansion adapter, this preserves supplied complete layouts.
pub(crate) fn lower(
    plan: crate::exec::node::ExecPlan,
    profile: novarocks_local_program::CompileProfile,
    sink_requirements: Vec<crate::exec::node::ExternalSinkRequirement>,
    sink: novarocks_local_program::StaticSinkProgram,
) -> (
    novarocks_local_program::LocalProgramGraph,
    crate::exec::node::LocalRuntimeBindings,
) {
    try_lower_with_scans(
        plan,
        profile,
        std::collections::BTreeMap::new(),
        sink_requirements,
        sink,
    )
    .expect("complete created local fixture")
}

/// Observe the original root profile on a genuine finite construction owner.
/// This temporary layout exits before the separately created local graph.
pub(crate) fn profile(
    plan: &crate::exec::node::ExecPlan,
    pipeline_dop: std::num::NonZeroUsize,
    root_sink_dop: Option<std::num::NonZeroUsize>,
) -> novarocks_local_program::CompileProfile {
    let declared = lowering_source_schema(&plan.root);
    let (owner, temporary) = lowering_owners(&declared);
    let mut request = |layout| owner.request(layout);
    let mut temporary_request = |layout| temporary.request(layout);
    let mut creation = crate::exec::node::LocalLayoutCreation::new(
        owner.storage_lifetime(),
        &mut request,
        &mut temporary_request,
        &DeclarationControl,
    );
    plan.local_compile_profile_with_creator(pipeline_dop, root_sink_dop, &mut creation)
        .expect("actual created fixture compile profile")
}

/// Supply the original static scan recipes and preserve typed lowering causes.
pub(crate) fn try_lower_with_scans(
    plan: crate::exec::node::ExecPlan,
    profile: novarocks_local_program::CompileProfile,
    scans: std::collections::BTreeMap<i32, novarocks_local_program::StaticConnectorScan>,
    sink_requirements: Vec<crate::exec::node::ExternalSinkRequirement>,
    sink: novarocks_local_program::StaticSinkProgram,
) -> Result<
    (
        novarocks_local_program::LocalProgramGraph,
        crate::exec::node::LocalRuntimeBindings,
    ),
    crate::exec::node::LocalProgramCreationFailure<ConstructionFailure>,
> {
    let declared = lowering_source_schema(&plan.root);
    let (owner, temporary) = lowering_owners(&declared);
    let mut request = |layout| owner.request(layout);
    let mut temporary_request = |layout| temporary.request(layout);
    plan.into_local_program_and_bindings_with_creator(
        profile,
        scans,
        sink_requirements,
        sink,
        crate::exec::node::LocalLayoutCreation::new(
            owner.storage_lifetime(),
            &mut request,
            &mut temporary_request,
            &DeclarationControl,
        ),
    )
}

fn lowering_owners(
    declared: &super::ChunkSchema,
) -> (
    Box<dyn crate::exec::pipeline::operator::InputConstructionOwner>,
    Box<dyn crate::exec::pipeline::operator::InputConstructionOwner>,
) {
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("explicit native fixture lowering"),
        Arc::new(RuntimeErrorState::default()),
    )
    .expect("real fixture lowering host");
    let retained = factory
        .begin_typed_construction(declared)
        .expect("fixture lowering owner");
    let temporary = factory
        .begin_typed_construction(declared)
        .expect("fixture temporary lowering owner");
    (retained, temporary)
}

// Select an existing actual source schema only to begin the construction scope;
// the governed dispatcher remains the sole author of derived output fields.
fn lowering_source_schema(mut node: &crate::exec::node::ExecNode) -> super::ChunkSchemaRef {
    use crate::exec::node::ExecNodeKind;
    loop {
        node = match &node.kind {
            ExecNodeKind::Repeat(node) => &node.input,
            ExecNodeKind::Filter(node) => &node.input,
            ExecNodeKind::Limit(node) => &node.input,
            ExecNodeKind::Sort(node) => &node.input,
            ExecNodeKind::AssertNumRows(node) => &node.input,
            ExecNodeKind::RuntimeFilterConsumer(node) => &node.input,
            ExecNodeKind::UnionAll(node) => node.inputs.first().expect("fixture UNION source"),
            _ => {
                return crate::exec::pipeline::builder::output_chunk_schema_for_node(node)
                    .expect("actual fixture source declaration");
            }
        };
    }
}

/// Publish caller-supplied source arrays through the actual typed ingress.
pub(crate) fn columns(
    names: &[&str],
    slots: &[SlotId],
    types: &[FunctionValueType],
    columns: Vec<arrow::array::ArrayRef>,
    rows: usize,
) -> Chunk {
    assert_eq!(names.len(), types.len());
    let schema = Arc::new(arrow::datatypes::Schema::new(
        names
            .iter()
            .zip(types)
            .map(|(name, ty)| {
                arrow::datatypes::Field::new(*name, ty.data_type().clone(), ty.nullable())
            })
            .collect::<Vec<_>>(),
    ));
    let declared = layout(schema, slots, types);
    let factory = try_runtime_input_construction_factory(
        crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),
        MemTracker::new_root("explicit fixture typed publication"),
        Arc::new(RuntimeErrorState::default()),
    )
    .expect("real publication host");
    let schema = factory
        .create_compiled_chunk_schema(&declared)
        .expect("created publication schema");
    let owner = factory
        .begin_typed_construction(&schema)
        .expect("publication owner");
    Chunk::try_new_typed_with_columns_owned_observed::<ConstructionFailure, _>(
        schema,
        columns,
        rows,
        owner.storage_lifetime(),
        |step| owner.observe(0, &[], step),
    )
    .expect("actual typed publication")
}
