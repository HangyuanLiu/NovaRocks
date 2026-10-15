// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//! Actual TableFunction sites delegate the sole structural models.
use super::*;
use crate::exec::pipeline::operator::InputConstructionOwner;
use novarocks_functions::selected_copy::{self, ExtendSegment};
use novarocks_type_contract::{ValueLayout, ValueSchemaCreation};

pub(super) struct ConstructionScopes<'a> {
    factory: &'a dyn InputConstructionFactory,
    output: &'a crate::exec::chunk::ChunkSchema,
    input: &'a StaticLayout,
    results: &'a [SlotId],
    retained: &'a dyn InputConstructionOwner,
    scratch: Box<dyn InputConstructionOwner>,
    operation: Option<Box<dyn InputConstructionOwner>>,
}
fn work(owner: &dyn InputConstructionOwner, units: usize) -> Result<(), ConstructionFailure> {
    owner.observe(
        0,
        &[],
        crate::exec::chunk::ArrayNormalizationStep::InspectConstructionWork { units },
    )
}
fn copy_failure(
    cause: selected_copy::CopyConstructionError<ConstructionFailure>,
) -> ExecutionFailure {
    super::super::unpivot_processor::unpivot_copy_failure(cause)
}
impl<'a> ConstructionScopes<'a> {
    pub(super) fn try_new(
        factory: &'a dyn InputConstructionFactory,
        output: &'a crate::exec::chunk::ChunkSchema,
        retained: &'a dyn InputConstructionOwner,
        input: &'a StaticLayout,
        results: &'a [SlotId],
    ) -> ExecutionResult<Self> {
        Ok(Self {
            factory,
            output,
            input,
            results,
            retained,
            scratch: factory.begin_typed_construction(output)?,
            operation: None,
        })
    }
    fn result_layout(&self, result: usize) -> ExecutionResult<&ValueLayout> {
        work(self.scratch.as_ref(), 1)?;
        let slot = self.results.get(result).ok_or_else(|| {
            ConstructionFailure::InvalidSource(
                "table function constructor result occurrence is absent".into(),
            )
        })?;
        self.output
            .slot(*slot)
            .map(|slot| slot.typed_binding().layout())
            .ok_or_else(|| {
                ConstructionFailure::InvalidSource(
                    "table function constructor has no complete result binding".into(),
                )
                .into()
            })
    }
    fn null(&self, layout: &ValueLayout, rows: usize) -> ExecutionResult<()> {
        let mut request = |layout| self.retained.request(layout);
        let mut opaque = |bytes| self.retained.observe_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(self.retained.storage_lifetime(), &mut request, &mut opaque);
        selected_copy::preflight_null_structure_construction_observed(
            layout,
            rows,
            &mut creation,
            &mut |boundary| work(self.scratch.as_ref(), usize::from(!boundary)),
        )
        .map_err(copy_failure)?;
        Ok(())
    }
    pub(super) fn observe(
        &mut self,
        step: TableFunctionConstructionStep<'_>,
    ) -> ExecutionResult<()> {
        use TableFunctionConstructionStep::*;
        work(self.scratch.as_ref(), 0)?;
        match step {
            Scratch(layout) => {
                self.scratch.request(layout)?;
            }
            Work(units) => work(self.scratch.as_ref(), units)?,
            OuterMap(count) => {
                use novarocks_type_contract::owned_resources::{
                    hashmap, layout::LayoutResourceError,
                };
                let shape =
                    hashmap::fresh_table_layout::<SlotId, ArrayRef>(count).map_err(|cause| {
                        ConstructionFailure::Resource(match cause {
                            hashmap::HashMapResourceError::Arithmetic(_) => {
                                LayoutResourceError::Arithmetic
                            }
                            hashmap::HashMapResourceError::SourceModel(_) => {
                                LayoutResourceError::SourceModel
                            }
                        })
                    })?;
                if shape.request_bytes_upper_bound != 0 {
                    self.scratch
                        .observe_opaque_upper_bound(shape.request_bytes_upper_bound)?;
                }
            }
            EndOperation => {
                let operation = self.operation.take().ok_or_else(|| {
                    ConstructionFailure::InvalidSource(
                        "table function operation ended without its actual temporary creator"
                            .into(),
                    )
                })?;
                work(operation.as_ref(), 0)?;
                drop(operation);
            }
            PrimitiveIndex => {
                let mut request = |layout| self.scratch.request(layout);
                let mut opaque = |bytes| self.scratch.observe_opaque_upper_bound(bytes);
                let mut creation = ValueSchemaCreation::new(
                    self.scratch.storage_lifetime(),
                    &mut request,
                    &mut opaque,
                );
                selected_copy::preflight_primitive_vec_adoption_construction_observed(
                    &mut creation,
                    &mut |boundary| work(self.scratch.as_ref(), usize::from(!boundary)),
                )
                .map_err(copy_failure)?;
            }
            Flat { result, has_nulls } => {
                let layout = self.result_layout(result)?;
                let mut request = |layout| self.retained.request(layout);
                let mut opaque = |bytes| self.retained.observe_opaque_upper_bound(bytes);
                let mut creation = ValueSchemaCreation::new(
                    self.retained.storage_lifetime(),
                    &mut request,
                    &mut opaque,
                );
                selected_copy::preflight_flat_array_structure_construction(
                    layout,
                    has_nulls,
                    &mut creation,
                    &mut |boundary| work(self.scratch.as_ref(), usize::from(!boundary)),
                )
                .map_err(copy_failure)?;
            }
            Null { result, rows } => self.null(self.result_layout(result)?, rows)?,
            Empty(ordinal) => {
                work(self.scratch.as_ref(), 1)?;
                let slot = self.output.slots().get(ordinal).ok_or_else(|| {
                    ConstructionFailure::InvalidSource(
                        "table function empty output occurrence is absent".into(),
                    )
                })?;
                self.null(slot.typed_binding().layout(), 0)?;
            }
            EmptyOuter(slot) => {
                work(self.scratch.as_ref(), 1)?;
                let binding = self.input.value_binding(slot).ok_or_else(|| {
                    ConstructionFailure::InvalidSource(
                        "table function empty outer occurrence is absent".into(),
                    )
                })?;
                self.null(binding.layout(), 0)?;
            }
            Take { source, indices } => {
                let operation = self.factory.begin_typed_construction(self.output)?;
                let mut temporary_request = |layout| operation.request(layout);
                let mut temporary_opaque = |bytes| operation.observe_opaque_upper_bound(bytes);
                let mut temporary = ValueSchemaCreation::new(
                    operation.storage_lifetime(),
                    &mut temporary_request,
                    &mut temporary_opaque,
                );
                let mut retained_request = |layout| self.retained.request(layout);
                let mut retained_opaque = |bytes| self.retained.observe_opaque_upper_bound(bytes);
                let mut retained = ValueSchemaCreation::new(
                    self.retained.storage_lifetime(),
                    &mut retained_request,
                    &mut retained_opaque,
                );
                selected_copy::preflight_take_structure_construction(
                    source,
                    selected_copy::CopyIndexView::UInt32(indices),
                    &mut temporary,
                    &mut retained,
                    &mut |boundary| work(operation.as_ref(), usize::from(!boundary)),
                )
                .map_err(copy_failure)?;
                self.operation = Some(operation);
            }
            Mutable { list, counts, rows } => {
                let operation = self.factory.begin_typed_construction(self.output)?;
                operation.request(Layout::array::<ExtendSegment>(counts.len()).map_err(|_| ConstructionFailure::Resource(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
                let mut segments = Vec::new();
                segments.try_reserve_exact(counts.len()).map_err(|_| {
                    ConstructionFailure::Control(
                        novarocks_type_contract::CompileControlError::ResourceExhausted,
                    )
                })?;
                let offsets = list.value_offsets();
                for (row, count) in counts.iter().copied().enumerate() {
                    work(operation.as_ref(), 1)?;
                    if count == 0 {
                        continue;
                    }
                    let (start, len) = if list.is_null(row) {
                        (0, 0)
                    } else {
                        let start = usize::try_from(offsets[row]).map_err(|_| {
                            ConstructionFailure::InvalidSource(
                                "table function source offset is negative".into(),
                            )
                        })?;
                        let end = usize::try_from(offsets[row + 1]).map_err(|_| {
                            ConstructionFailure::InvalidSource(
                                "table function source offset is negative".into(),
                            )
                        })?;
                        (
                            start,
                            end.checked_sub(start).ok_or_else(|| {
                                ConstructionFailure::InvalidSource(
                                    "table function source offsets decrease".into(),
                                )
                            })?,
                        )
                    };
                    // Exactly the stock range extension followed by its NULL
                    // padding; inactive parent payload never becomes a range.
                    segments.push(ExtendSegment {
                        source: 0,
                        start,
                        len,
                        repeats: 1,
                        nulls: count.checked_sub(len).ok_or_else(|| {
                            ConstructionFailure::InvalidSource(
                                "table function padding differs from its original row count".into(),
                            )
                        })?,
                    });
                }
                let mut temporary_request = |layout| operation.request(layout);
                let mut temporary_opaque = |bytes| operation.observe_opaque_upper_bound(bytes);
                let mut temporary = ValueSchemaCreation::new(
                    operation.storage_lifetime(),
                    &mut temporary_request,
                    &mut temporary_opaque,
                );
                let mut retained_request = |layout| self.retained.request(layout);
                let mut retained_opaque = |bytes| self.retained.observe_opaque_upper_bound(bytes);
                let mut retained = ValueSchemaCreation::new(
                    self.retained.storage_lifetime(),
                    &mut retained_request,
                    &mut retained_opaque,
                );
                selected_copy::preflight_extend_multi_structure_construction(
                    &[list.values().as_ref()],
                    &segments,
                    rows,
                    &mut temporary,
                    &mut retained,
                    &mut |boundary| work(operation.as_ref(), usize::from(!boundary)),
                )
                .map_err(copy_failure)?;
                self.operation = Some(operation);
            }
        }
        Ok(())
    }
}
