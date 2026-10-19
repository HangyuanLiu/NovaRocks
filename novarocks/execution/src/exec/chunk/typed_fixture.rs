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
