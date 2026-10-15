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
//! Creator/complete-declaration ports over the original table function body.
use super::*;
use crate::exec::chunk::{ArrayNormalizationStep, normalize_execution_array_observed};
use crate::exec::pipeline::operator::InputConstructionOwner;
use novarocks_type_contract::{FunctionValueType, ValueKind, ValueLogicalType};

fn source_failure(message: &str) -> ExecutionFailure {
    ConstructionFailure::InvalidSource(message.into()).into()
}
fn reserve_columns(
    count: usize,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<Vec<ArrayRef>> {
    owner.request(Layout::array::<ArrayRef>(count).map_err(|_| {
        ConstructionFailure::Resource(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        )
    })?)?;
    let mut columns = Vec::new();
    columns.try_reserve_exact(count).map_err(|_| {
        ConstructionFailure::Control(
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        )
    })?;
    Ok(columns)
}
fn integer_domain(value: &novarocks_type_contract::ValueTypeRef) -> bool {
    matches!(
        value.kind(),
        ValueKind::Scalar(
            DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
        )
    ) || value.domain() == Some(ValueLogicalType::LargeInt)
}
fn binding_fits(
    actual: &FunctionValueType,
    expected: &FunctionValueType,
    owner: &dyn InputConstructionOwner,
) -> ExecutionResult<bool> {
    enum Failure {
        Source(novarocks_type_contract::ValueTypeError),
        Creator(ConstructionFailure),
    }
    impl From<novarocks_type_contract::ValueTypeError> for Failure {
        fn from(cause: novarocks_type_contract::ValueTypeError) -> Self {
            Self::Source(cause)
        }
    }
    actual
        .fits_physical_binding_observed(expected, || {
            owner
                .observe(
                    0,
                    &[],
                    ArrayNormalizationStep::InspectConstructionWork { units: 1 },
                )
                .map_err(Failure::Creator)
        })
        .map_err(|cause| match cause {
            Failure::Creator(cause) => cause.into(),
            Failure::Source(cause) => ConstructionFailure::InvalidSource(cause.to_string()).into(),
        })
}
impl TableFunctionProcessorOperator {
    pub(super) fn validate_typed_declarations(
        &self,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<()> {
        let TableFunctionSchemaSource::Typed { input, output } = &self.schema_source else {
            return Ok(());
        };
        let work = |units| {
            owner.observe(
                0,
                &[],
                ArrayNormalizationStep::InspectConstructionWork { units },
            )
        };
        for (ordinal, slot) in self.param_slots.iter().enumerate() {
            work(1)?;
            let binding = input
                .value_binding(*slot)
                .ok_or_else(|| source_failure("table function parameter occurrence is absent"))?;
            if self.param_types.get(ordinal) != Some(binding.data_type()) {
                return Err(source_failure(
                    "table function parameter carrier differs from its complete declaration",
                ));
            }
        }
        for (ordinal, slot) in self.fn_result_slots.iter().enumerate() {
            work(1)?;
            let binding = output
                .value_binding(*slot)
                .ok_or_else(|| source_failure("table function result occurrence is absent"))?;
            if self.ret_types.get(ordinal) != Some(binding.data_type()) {
                return Err(source_failure(
                    "table function result carrier differs from its complete declaration",
                ));
            }
        }
        for (ordinal, source) in self.output_slot_sources.iter().enumerate() {
            work(1)?;
            let expected = output
                .value_binding_at(ordinal)
                .ok_or_else(|| source_failure("table function output occurrence is absent"))?;
            match source {
                TableFunctionOutputSlot::Outer { slot } => {
                    let actual = input.value_binding(*slot).ok_or_else(|| {
                        source_failure("table function outer declaration is absent")
                    })?;
                    if !actual
                        .borrowed()
                        .exactly_equals_observed(expected.borrowed(), || work(1))?
                    {
                        return Err(source_failure(
                            "table function outer output differs from its complete original declaration",
                        ));
                    }
                }
                TableFunctionOutputSlot::Result { index } => {
                    let slot = self
                        .fn_result_slots
                        .get(*index)
                        .ok_or_else(|| source_failure("table function result index is absent"))?;
                    if !output
                        .value_binding(*slot)
                        .ok_or_else(|| {
                            source_failure("table function result declaration is absent")
                        })?
                        .borrowed()
                        .exactly_equals_observed(expected.borrowed(), || work(1))?
                    {
                        return Err(source_failure(
                            "table function result mapping changes its complete declaration",
                        ));
                    }
                }
            }
        }
        if self.function_name.eq_ignore_ascii_case("unnest") {
            if self.fn_result_required && self.param_slots.len() != self.fn_result_slots.len() {
                return Err(source_failure(
                    "unnest result occurrence arity differs from its parameters",
                ));
            }
            for (parameter, result) in self.param_slots.iter().zip(&self.fn_result_slots) {
                work(1)?;
                let parameter = input.value_binding(*parameter).expect("checked parameter");
                let ValueKind::List(item) = parameter.value_type().kind() else {
                    return Err(source_failure(
                        "unnest parameter is not its declared List domain",
                    ));
                };
                let DataType::List(_) = parameter.data_type() else {
                    return Err(source_failure("unnest parameter layout is not List"));
                };
                let result = output.value_binding(*result).expect("checked result");
                if (item.nullable() || self.is_left_join || self.param_slots.len() > 1)
                    && !result.nullable()
                {
                    return Err(source_failure(
                        "unnest result NULL promise excludes its declared padding or child NULLs",
                    ));
                }
                // This child is borrowed from the original complete binding;
                // comparison never constructs a temporary semantic/layout tree.
                let child = parameter
                    .borrowed()
                    .child_at(0)
                    .expect("checked List child");
                if !child.same_physical_binding_ignoring_root_nullability_observed(
                    result.borrowed(),
                    || work(1),
                )? {
                    return Err(source_failure(
                        "unnest result differs from the complete original child declaration",
                    ));
                }
            }
        } else if self.function_name.eq_ignore_ascii_case("unnest_bitmap")
            || self.function_name.eq_ignore_ascii_case("subdivide_bitmap")
        {
            let bitmap =
                input
                    .value_binding(*self.param_slots.first().ok_or_else(|| {
                        source_failure("bitmap table function argument is absent")
                    })?)
                    .expect("checked parameter");
            if bitmap.value_type().domain() != Some(ValueLogicalType::Bitmap) {
                return Err(source_failure(
                    "bitmap table function requires the explicit Bitmap domain",
                ));
            }
            if self.function_name.eq_ignore_ascii_case("subdivide_bitmap") {
                let size = input
                    .value_binding(
                        *self
                            .param_slots
                            .get(1)
                            .ok_or_else(|| source_failure("subdivide_bitmap size is absent"))?,
                    )
                    .expect("checked parameter");
                if !integer_domain(size.value_type()) {
                    return Err(source_failure(
                        "subdivide_bitmap size is not a declared integer domain",
                    ));
                }
            }
            for slot in &self.fn_result_slots {
                let result = output.value_binding(*slot).expect("checked result");
                let valid = if self.function_name.eq_ignore_ascii_case("subdivide_bitmap") {
                    result.value_type().domain() == Some(ValueLogicalType::Bitmap)
                } else {
                    result.value_type().kind() == &ValueKind::Scalar(DataType::Int64)
                };
                if !valid || (self.is_left_join && !result.nullable()) {
                    return Err(source_failure(
                        "bitmap table function result differs from its declared domain/NULL law",
                    ));
                }
            }
        } else if self.function_name.eq_ignore_ascii_case("generate_series") {
            if self.param_slots.iter().any(|slot| {
                !integer_domain(
                    input
                        .value_binding(*slot)
                        .expect("checked parameter")
                        .value_type(),
                )
            }) {
                return Err(source_failure(
                    "generate_series arguments are not declared integer domains",
                ));
            }
            for slot in &self.fn_result_slots {
                let result = output.value_binding(*slot).expect("checked result");
                if !integer_domain(result.value_type()) || (self.is_left_join && !result.nullable())
                {
                    return Err(source_failure(
                        "generate_series result differs from its declared integer/NULL law",
                    ));
                }
            }
        }
        Ok(())
    }
    pub(super) fn validate_typed_input(
        &self,
        chunk: &Chunk,
        owner: &dyn InputConstructionOwner,
    ) -> ExecutionResult<()> {
        let TableFunctionSchemaSource::Typed { input, .. } = &self.schema_source else {
            return Err(ConstructionFailure::MissingOwner.into());
        };
        if !chunk.chunk_schema().is_typed() || chunk.chunk_schema().slot_ids() != input.slots() {
            return Err(source_failure(
                "table function input differs from its complete frozen occurrences",
            ));
        }
        for (ordinal, slot) in chunk.chunk_schema().slots().iter().enumerate() {
            let actual = FunctionValueType::try_from_typed_binding(slot.typed_binding().clone())
                .map_err(|cause| {
                    ConstructionFailure::InvalidSource(format!(
                        "table function input declaration: {cause}"
                    ))
                })?;
            let expected = FunctionValueType::try_from_typed_binding(
                input
                    .value_binding_at(ordinal)
                    .ok_or_else(|| source_failure("table function input declaration is absent"))?
                    .clone(),
            )
            .map_err(|cause| {
                ConstructionFailure::InvalidSource(format!(
                    "table function expected declaration: {cause}"
                ))
            })?;
            if !binding_fits(&actual, &expected, owner)? {
                return Err(source_failure(
                    "table function input changes a complete domain, child NULL promise or layout",
                ));
            }
        }

        Ok(())
    }
    pub(super) fn build_output_chunk_typed(&self, chunk: &Chunk) -> ExecutionResult<Chunk> {
        if !matches!(self.schema_source, TableFunctionSchemaSource::Typed { .. }) {
            return Err(ConstructionFailure::MissingOwner.into());
        }
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let output = self
            .output_chunk_schema
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_typed_construction(output)?;
        self.validate_typed_input(chunk, owner.as_ref())?;

        let storage = owner
            .storage_lifetime()
            .retaining_observed::<ConstructionFailure>(
                &chunk.array_type_storage_lifetime(),
                |layout| owner.request(layout),
            )?;
        // The original scalar/bitmap/series/MutableArrayData payload algorithms
        // and SchemeS accounting stay unchanged. No complete CopyInvoice is
        // billed again on this schema/reference/wrapper construction scope.
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork { units: 0 },
        )?;
        let mut scopes = super::typed_table_function_construction::ConstructionScopes::try_new(
            factory.as_ref(),
            output.as_ref(),
            owner.as_ref(),
            match &self.schema_source {
                TableFunctionSchemaSource::Typed { input, .. } => input,
                _ => return Err(ConstructionFailure::MissingOwner.into()),
            },
            &self.fn_result_slots,
        )?;
        let mut events = |event: TableFunctionConstructionStep<'_>| scopes.observe(event);
        let mut generated = self.build_output_columns::<ExecutionFailure>(
            chunk,
            &mut |count| reserve_columns(count, owner.as_ref()),
            &mut TableFunctionConstruction {
                observe: Some(&mut events),
            },
        )?;
        drop(events);
        drop(scopes);

        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork { units: 0 },
        )?;
        for array in &mut generated.columns {
            *array = normalize_execution_array_observed::<ConstructionFailure, _>(array, |step| {
                owner.observe(0, &[], step)
            })?;
        }
        Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            ConstructionFailure,
            _,
        >(
            output.clone(),
            generated.columns,
            generated.rows,
            storage,
            |step| owner.observe(0, &[], step),
        )?)
    }
    pub(super) fn empty_output_chunk_typed(&self) -> ExecutionResult<Chunk> {
        let output = self
            .output_chunk_schema
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let factory = self
            .construction
            .as_ref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let owner = factory.begin_typed_construction(output)?;
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork { units: 0 },
        )?;
        let mut scopes = super::typed_table_function_construction::ConstructionScopes::try_new(
            factory.as_ref(),
            output.as_ref(),
            owner.as_ref(),
            match &self.schema_source {
                TableFunctionSchemaSource::Typed { input, .. } => input,
                _ => return Err(ConstructionFailure::MissingOwner.into()),
            },
            &self.fn_result_slots,
        )?;
        let mut events = |event: TableFunctionConstructionStep<'_>| scopes.observe(event);
        let generated = self.empty_output_columns::<ExecutionFailure>(
            &mut |count| reserve_columns(count, owner.as_ref()),
            &mut TableFunctionConstruction {
                observe: Some(&mut events),
            },
        )?;
        drop(events);
        drop(scopes);
        owner.observe(
            0,
            &[],
            ArrayNormalizationStep::InspectConstructionWork { units: 0 },
        )?;
        Ok(Chunk::try_new_typed_with_columns_owned_observed::<
            ConstructionFailure,
            _,
        >(
            output.clone(),
            generated.columns,
            0,
            owner.storage_lifetime(),
            |step| owner.observe(0, &[], step),
        )?)
    }
}
