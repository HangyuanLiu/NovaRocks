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

//! Borrowed construction facts for the already installed Values constructors.
//! Arrow 58.4 interleave.rs selects primitive/bytes or the original Mutable
//! fallback. No concat or substitute interleave executes in this resource body.
use super::{
    CopyConstructionError, CopyError, CopyMode, CopyObservation, CopyOperationFacts,
    ScratchCoverage, add, mul, mutable_constructor_geometry, take_host, validate_interleave_plan,
};
use crate::kernel_input::EvaluationCheckpoints;
use crate::{KernelControlObservation, KernelEvaluationControl};
use arrow_array::{Array, ArrayRef};
use arrow_data::ArrayData;
use arrow_schema::DataType;
use novarocks_type_contract::ValueSchemaCreation;

pub fn preflight_interleave_construction<H>(
    ty: &DataType,
    sources: &[ArrayRef],
    choices: &[(usize, usize)],
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<CopyOperationFacts, CopyConstructionError<H>> {
    let observed = KernelControlObservation::new(control);
    let mut checkpoints = EvaluationCheckpoints::new(&observed);
    let mut invoice = take_host::CopyInvoiceTotals::default();
    let mut source_metadata = 0;
    // Borrowed metadata geometry covers original to_data/freeze/make_array,
    // source table/header coexistence and private Buffer owners, not source
    // payload. Existing source custody remains the caller's independent hold.
    for source in sources {
        checkpoints.step().map_err(CopyError::Control)?;
        source_metadata = add(
            source_metadata,
            crate::array_backing_geometry::copy_metadata_bytes(source.as_ref(), &mut checkpoints)
                .map_err(CopyError::Control)?,
        )?;
    }
    // The specialized original Interleave::new collects a concrete reference
    // table. Its type is thin, so an exact pointer-width table bounds it.
    let reference_table = mul(sources.len(), size_of::<&dyn Array>())?;
    source_metadata = add(source_metadata, reference_table)?;
    checkpoints.flush().map_err(CopyError::Control)?;
    creation
        .observe_opaque_upper_bound(source_metadata)
        .map_err(CopyConstructionError::Creator)?;
    creation.with_request_ports(|exact, _| {
        let mut request = |layout| exact(layout).map_err(CopyConstructionError::Creator);
        let mut observe = |boundary| {
            if boundary {
                checkpoints.flush()
            } else {
                checkpoints.step()
            }
        };
        let mut work = CopyObservation(
            super::WorkObservation::Legacy(&mut observe),
            None,
            ScratchCoverage::RecursiveSelections,
            Some(&mut invoice),
            Some(&mut request),
        );
        super::guarded_interleave_extent(ty, choices.len())?;
        validate_interleave_plan(ty, sources, choices, &mut work)?;
        if choices.is_empty() {
            // The caller uses the separate empty-constructor invoice. This
            // entry describes only nonempty original interleave selections.
            return Err(
                CopyError::Invalid("empty interleave needs its empty constructor facts").into(),
            );
        }
        if ty.primitive_width().is_some() {
            let width = ty.primitive_width().expect("checked primitive");
            work.3
                .as_deref_mut()
                .expect("same invoice")
                .interleave_exact_payload(mul(choices.len(), width)?)?;
            interleave_validity(sources, choices.len(), &mut work)?;
        } else if matches!(
            ty,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
        ) {
            let width = if matches!(ty, DataType::Utf8 | DataType::Binary) {
                4
            } else {
                8
            };
            work.3
                .as_deref_mut()
                .expect("same invoice")
                .interleave_exact_payload(mul(add(choices.len(), 1)?, width)?)?;
            // SAME selected offset span author also validates NULL row spans.
            match ty {
                DataType::Utf8 => {
                    super::interleave_byte_payload::<arrow_array::types::Utf8Type, _>(
                        sources, choices, false, &mut work,
                    )?
                }
                DataType::LargeUtf8 => super::interleave_byte_payload::<
                    arrow_array::types::LargeUtf8Type,
                    _,
                >(sources, choices, true, &mut work)?,
                DataType::Binary => super::interleave_byte_payload::<
                    arrow_array::types::BinaryType,
                    _,
                >(sources, choices, false, &mut work)?,
                DataType::LargeBinary => super::interleave_byte_payload::<
                    arrow_array::types::LargeBinaryType,
                    _,
                >(sources, choices, true, &mut work)?,
                _ => unreachable!("checked byte carrier"),
            }
            interleave_validity(sources, choices.len(), &mut work)?;
        } else {
            // Only Boolean/FixedSizeBinary remain in the existing Values gate.
            // The pinned original uses MutableArrayData with all real sources.
            let mut data = work.scratch::<ArrayData>(sources.len(), None)?;
            for source in sources {
                work.boundary()?;
                work.push(&mut data, source.to_data())?;
                work.boundary()?;
            }
            let mut refs = work.scratch::<&ArrayData>(data.len(), None)?;
            for source in &data {
                work.push(&mut refs, source)?;
                work.step()?;
            }
            mutable_constructor_geometry(&refs, choices.len(), CopyMode::Extend, &mut work)?;
            work.3.as_deref_mut().expect("same invoice").selected_node(
                sources[0].as_ref(),
                choices.len(),
                CopyMode::Extend,
            )?;
        }
        Ok::<_, CopyConstructionError<H>>(())
    })?;
    let facts = invoice.finish(source_metadata)?;
    checkpoints.flush().map_err(CopyError::Control)?;
    creation
        .observe_opaque_upper_bound(facts.operation_peak_bytes())
        .map_err(CopyConstructionError::Creator)?;
    Ok(facts)
}

fn interleave_validity<E: From<CopyError>>(
    sources: &[ArrayRef],
    rows: usize,
    work: &mut CopyObservation<'_, E>,
) -> Result<(), E> {
    // Interleave::new examines all source null_count values even if some
    // sources are not selected; its output bitmap is installed on that fact.
    let mut has_nulls = false;
    for source in sources {
        has_nulls |= source.null_count() != 0;
        work.step()?;
    }
    if has_nulls {
        work.3
            .as_deref_mut()
            .expect("same invoice")
            .interleave_validity(rows)?;
    }
    Ok(())
}

/// Borrow a validated normalized layout before Arrow's sole empty author.
/// No ArrayData or temporary empty array is created by this projection.
pub fn preflight_empty_construction<H>(
    layout: &novarocks_type_contract::ValueLayout,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<CopyOperationFacts, CopyConstructionError<H>> {
    preflight_null_construction(layout, 0, creation, control)
}

/// The SAME locked Arrow ArrayData::new_null/make_array construction author.
/// This is a construction envelope, not a payload funding or custody receipt.
/// The caller must checkpoint before/after the actual opaque Arrow operation.
pub fn preflight_null_construction<H>(
    layout: &novarocks_type_contract::ValueLayout,
    rows: usize,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<CopyOperationFacts, CopyConstructionError<H>> {
    let observed = KernelControlObservation::new(control);
    let mut work = EvaluationCheckpoints::new(&observed);
    preflight_null_construction_with_observation(layout, rows, creation, &mut |boundary| {
        if boundary {
            work.flush().map_err(CopyError::Control)
        } else {
            work.step().map_err(CopyError::Control)
        }
        .map_err(CopyConstructionError::Copy)
    })
}

/// The same complete empty-constructor invoice with the caller's original
/// construction/control cause. Unlike structure-only preflight, this includes
/// the zero-row offset buffers of bytes and nested arrays.
pub fn preflight_empty_construction_observed<H>(
    layout: &novarocks_type_contract::ValueLayout,
    creation: &mut ValueSchemaCreation<'_, H>,
    observe: &mut impl FnMut(bool) -> Result<(), H>,
) -> Result<CopyOperationFacts, CopyConstructionError<H>> {
    preflight_null_construction_with_observation(layout, 0, creation, &mut |boundary| {
        observe(boundary).map_err(CopyConstructionError::Creator)
    })
}

fn preflight_null_construction_with_observation<H>(
    layout: &novarocks_type_contract::ValueLayout,
    rows: usize,
    creation: &mut ValueSchemaCreation<'_, H>,
    observe: &mut impl FnMut(bool) -> Result<(), CopyConstructionError<H>>,
) -> Result<CopyOperationFacts, CopyConstructionError<H>> {
    let mut invoice = take_host::CopyInvoiceTotals::default();
    let metadata = null_node(layout.data_type(), rows, &mut invoice, false, observe)?;
    let facts = invoice.finish(metadata)?;
    observe(true)?;
    creation
        .observe_opaque_upper_bound(facts.operation_peak_bytes())
        .map_err(CopyConstructionError::Creator)?;
    Ok(facts)
}

/// Only the Arrow constructor's wrapper/header geometry is newly observed.
/// The original caller's null payload allocation/accounting policy is unchanged.
pub fn preflight_null_structure_construction<H>(
    layout: &novarocks_type_contract::ValueLayout,
    rows: usize,
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<CopyOperationFacts, CopyConstructionError<H>> {
    let observed = KernelControlObservation::new(control);
    let mut work = EvaluationCheckpoints::new(&observed);
    let mut invoice = take_host::CopyInvoiceTotals::default();
    let metadata = null_node(
        layout.data_type(),
        rows,
        &mut invoice,
        false,
        &mut |boundary| {
            if boundary {
                work.flush().map_err(CopyError::Control)
            } else {
                work.step().map_err(CopyError::Control)
            }
        },
    )?;
    let facts = invoice.finish(metadata)?;
    work.flush().map_err(CopyError::Control)?;
    creation
        .observe_opaque_upper_bound(facts.metadata_upper_bytes())
        .map_err(CopyConstructionError::Creator)?;
    Ok(facts)
}

fn null_node<E: From<CopyError>>(
    ty: &DataType,
    rows: usize,
    invoice: &mut take_host::CopyInvoiceTotals,
    raw_arrow: bool,
    work: &mut impl FnMut(bool) -> Result<(), E>,
) -> Result<usize, E> {
    // Arrow 58.4 data.rs637: new_empty is exactly new_null(type, 0).
    // One zeroed Buffer constructor rounds its actual requested extent once;
    // no append/growth or guessed average value width enters this model.
    work(false)?;
    let bits = add(rows / 8, usize::from(!rows.is_multiple_of(8)))?;
    let (buffers, children, nulls) = if let Some(width) = ty.primitive_width() {
        invoice.empty_offsets(mul(width, rows)?)?;
        (1, 0, true)
    } else {
        match ty {
            DataType::Null => (0, 0, false),
            DataType::Boolean => {
                invoice.empty_offsets(bits)?;
                (1, 0, true)
            }
            DataType::FixedSizeBinary(width) => {
                let width = usize::try_from(*width).map_err(|_| CopyError::Extent)?;
                invoice.empty_offsets(mul(width, rows)?)?;
                (1, 0, true)
            }
            DataType::Utf8View | DataType::BinaryView => {
                invoice.empty_offsets(mul(rows, 16)?)?;
                (1, 0, true)
            }
            DataType::Utf8 | DataType::Binary => {
                invoice.empty_offsets(mul(add(rows, 1)?, 4)?)?;
                (2, 0, true)
            }
            DataType::LargeUtf8 | DataType::LargeBinary => {
                invoice.empty_offsets(mul(add(rows, 1)?, 8)?)?;
                (2, 0, true)
            }
            DataType::List(_) | DataType::Map(_, _) => {
                invoice.empty_offsets(mul(add(rows, 1)?, 4)?)?;
                (1, 1, true)
            }
            DataType::LargeList(_) => {
                invoice.empty_offsets(mul(add(rows, 1)?, 8)?)?;
                (1, 1, true)
            }
            DataType::ListView(_) | DataType::LargeListView(_) => {
                let width = if matches!(ty, DataType::ListView(_)) {
                    4
                } else {
                    8
                };
                invoice.empty_offsets(mul(rows, width)?)?;
                invoice.empty_offsets(mul(rows, width)?)?;
                (2, 1, true)
            }
            DataType::FixedSizeList(_, _) => (0, 1, true),
            DataType::Struct(fields) => (0, fields.len(), true),
            DataType::Union(fields, mode) => {
                if fields.is_empty() {
                    return Err(
                        CopyError::Invalid("empty Union lacks Arrow constructor child").into(),
                    );
                }
                invoice.empty_offsets(rows)?;
                if *mode == arrow_schema::UnionMode::Dense {
                    i32::try_from(rows).map_err(|_| CopyError::Extent)?;
                    invoice.empty_offsets(mul(rows, 4)?)?;
                }
                (
                    if *mode == arrow_schema::UnionMode::Dense {
                        2
                    } else {
                        1
                    },
                    fields.len(),
                    false,
                )
            }
            DataType::Dictionary(key, _) if raw_arrow => {
                let width = key.primitive_width().ok_or(CopyError::Extent)?;
                invoice.empty_offsets(mul(width, rows)?)?;
                (1, 1, true)
            }
            DataType::RunEndEncoded(ends, _) if raw_arrow => {
                let maximum = match ends.data_type() {
                    DataType::Int16 => i16::MAX as usize,
                    DataType::Int32 => i32::MAX as usize,
                    DataType::Int64 => super::offset_max(true),
                    _ => {
                        return Err(CopyError::Invalid(
                            "null run-end has no original index carrier",
                        )
                        .into());
                    }
                };
                super::limit(rows, maximum)?;
                (0, 2, false)
            }
            DataType::Dictionary(_, _) | DataType::RunEndEncoded(_, _) => {
                return Err(CopyError::Invalid(
                    "null construction requires normalized value layout",
                )
                .into());
            }
            other => return Err(CopyError::Unsupported(other.clone()).into()),
        }
    };
    if nulls {
        invoice.empty_offsets(bits)?;
    }
    let mut metadata = crate::arrow_result_custody::custody_typed_node_metadata_upper_bound(
        ty, buffers, nulls, children,
    )
    .map_err(CopyError::Control)?;
    match ty {
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => {
            metadata = add(
                metadata,
                null_node(field.data_type(), 0, invoice, raw_arrow, work)?,
            )?;
        }
        DataType::FixedSizeList(field, width) => {
            let width = usize::try_from(*width).map_err(|_| CopyError::Extent)?;
            metadata = add(
                metadata,
                null_node(
                    field.data_type(),
                    mul(rows, width)?,
                    invoice,
                    raw_arrow,
                    work,
                )?,
            )?;
        }
        DataType::Struct(fields) => {
            for field in fields {
                metadata = add(
                    metadata,
                    null_node(field.data_type(), rows, invoice, raw_arrow, work)?,
                )?;
            }
        }
        DataType::Union(fields, mode) => {
            for (index, (_, field)) in fields.iter().enumerate() {
                let child_rows = if index == 0 || *mode == arrow_schema::UnionMode::Sparse {
                    rows
                } else {
                    0
                };
                metadata = add(
                    metadata,
                    null_node(field.data_type(), child_rows, invoice, raw_arrow, work)?,
                )?;
            }
        }
        DataType::Dictionary(_, value) if raw_arrow => {
            metadata = add(metadata, null_node(value, 0, invoice, raw_arrow, work)?)?;
        }
        DataType::RunEndEncoded(ends, values) if raw_arrow => {
            if rows == 0 {
                metadata = add(
                    metadata,
                    null_node(ends.data_type(), 0, invoice, raw_arrow, work)?,
                )?;
                metadata = add(
                    metadata,
                    null_node(values.data_type(), 0, invoice, raw_arrow, work)?,
                )?;
            } else {
                work(false)?;
                invoice.empty_offsets(
                    ends.data_type()
                        .primitive_width()
                        .ok_or(CopyError::Extent)?,
                )?;
                metadata = add(
                    metadata,
                    take_host::result_node_metadata_upper(ends.data_type(), 1, false, 0)?,
                )?;
                metadata = add(
                    metadata,
                    null_node(values.data_type(), 1, invoice, raw_arrow, work)?,
                )?;
            }
        }
        _ => {}
    }
    Ok(metadata)
}

/// The caller has already requested the actual primitive Vec backing. Its
/// move into ScalarBuffer constructs only the locked private Buffer ownership
/// metadata. An independently allocated array wrapper is a separate request.
pub fn preflight_primitive_vec_adoption_construction<H>(
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), CopyConstructionError<H>> {
    control.checkpoint(0).map_err(CopyError::Control)?;
    let metadata = crate::arrow_result_custody::custody_node_metadata_upper_bound(1, false, 0)
        .map_err(CopyError::Control)?;
    creation
        .observe_opaque_upper_bound(metadata)
        .map_err(CopyConstructionError::Creator)
}

/// Borrowed Arrow representation for the original constructor. This entry
/// supplies structure geometry only; it never declares a semantic value type.
pub fn preflight_null_arrow_structure_observed<H>(
    ty: &DataType,
    rows: usize,
    creation: &mut ValueSchemaCreation<'_, H>,
    observe: &mut impl FnMut(bool) -> Result<(), H>,
) -> Result<CopyOperationFacts, CopyConstructionError<H>> {
    let mut invoice = take_host::CopyInvoiceTotals::default();
    let metadata = null_node(ty, rows, &mut invoice, true, &mut |boundary| {
        observe(boundary).map_err(CopyConstructionError::Creator)
    })?;
    let facts = invoice.finish(metadata)?;
    observe(true).map_err(CopyConstructionError::Creator)?;
    creation
        .observe_opaque_upper_bound(metadata)
        .map_err(CopyConstructionError::Creator)?;
    Ok(facts)
}
pub fn preflight_null_structure_construction_observed<H>(
    layout: &novarocks_type_contract::ValueLayout,
    rows: usize,
    creation: &mut ValueSchemaCreation<'_, H>,
    observe: &mut impl FnMut(bool) -> Result<(), H>,
) -> Result<CopyOperationFacts, CopyConstructionError<H>> {
    preflight_null_arrow_structure_observed(layout.data_type(), rows, creation, observe)
}
pub fn preflight_primitive_vec_adoption_construction_observed<H>(
    creation: &mut ValueSchemaCreation<'_, H>,
    observe: &mut impl FnMut(bool) -> Result<(), H>,
) -> Result<(), CopyConstructionError<H>> {
    crate::arrow_result_custody::preflight_buffer_adoption_structure(creation, observe)
}
/// SAME result-custody node author, without payload allocation or semantic
/// inference. The caller performs the original flat constructor afterwards.
pub fn preflight_flat_arrow_structure_construction<H>(
    ty: &DataType,
    has_nulls: bool,
    creation: &mut ValueSchemaCreation<'_, H>,
    observe: &mut impl FnMut(bool) -> Result<(), H>,
) -> Result<(), CopyConstructionError<H>> {
    observe(false).map_err(CopyConstructionError::Creator)?;
    let buffers = if ty.primitive_width().is_some()
        || matches!(ty, DataType::Boolean | DataType::FixedSizeBinary(_))
    {
        1
    } else if matches!(
        ty,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
    ) {
        2
    } else {
        return Err(CopyError::Unsupported(ty.clone()).into());
    };
    let metadata = take_host::result_node_metadata_upper(ty, buffers, has_nulls, 0)?;
    observe(true).map_err(CopyConstructionError::Creator)?;
    creation
        .observe_opaque_upper_bound(metadata)
        .map_err(CopyConstructionError::Creator)
}
pub fn preflight_flat_array_structure_construction<H>(
    layout: &novarocks_type_contract::ValueLayout,
    has_nulls: bool,
    creation: &mut ValueSchemaCreation<'_, H>,
    observe: &mut impl FnMut(bool) -> Result<(), H>,
) -> Result<(), CopyConstructionError<H>> {
    preflight_flat_arrow_structure_construction(layout.data_type(), has_nulls, creation, observe)
}

/// The original VALUES gather-index entry uses the same primitive adoption
/// body. Its caller separately observes the actual UInt64 Vec payload.
pub fn preflight_uint64_indices_construction<H>(
    creation: &mut ValueSchemaCreation<'_, H>,
    control: &dyn KernelEvaluationControl,
) -> Result<(), CopyConstructionError<H>> {
    preflight_primitive_vec_adoption_construction(creation, control)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use novarocks_type_contract::{ValueLayout, ValueTypeStorageLifetime};
    use std::{
        alloc::Layout,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    struct Control(AtomicBool);
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), crate::KernelFailure> {
            if self.0.load(Ordering::SeqCst) {
                Err(crate::KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: Duration) -> Result<(), crate::KernelFailure> {
            panic!("copy resource author never waits")
        }
    }
    fn facts(
        ty: &DataType,
        sources: &[ArrayRef],
        choices: &[(usize, usize)],
    ) -> CopyOperationFacts {
        let mut exact = |_: Layout| Ok::<_, std::convert::Infallible>(());
        let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        preflight_interleave_construction(
            ty,
            sources,
            choices,
            &mut creation,
            &Control(AtomicBool::new(false)),
        )
        .unwrap()
    }
    #[test]
    fn original_interleave_invoice_uses_selected_byte_spans_including_null_offsets() {
        // The real Arrow byte constructor copies an offset span for NULL rows.
        // Equal row counts with different selected spans must differ in cost.
        let data = arrow_data::ArrayData::builder(DataType::Utf8)
            .len(2)
            .add_buffer(arrow_buffer::Buffer::from_slice_ref([0i32, 1, 102]))
            .add_buffer(arrow_buffer::Buffer::from_vec(vec![b'x'; 102]))
            .null_bit_buffer(Some(arrow_buffer::Buffer::from_vec(vec![1u8])))
            .build()
            .unwrap();
        let sources: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(data)),
            Arc::new(StringArray::from(vec!["z"])),
        ];
        let short = facts(&DataType::Utf8, &sources, &[(0, 0), (1, 0)]);
        let null_span = facts(&DataType::Utf8, &sources, &[(0, 1), (1, 0)]);
        assert_eq!(
            null_span.retained_new_backing_upper() - short.retained_new_backing_upper(),
            100
        );
        let borrowed: Vec<&dyn Array> = sources.iter().map(AsRef::as_ref).collect();
        let actual = arrow_select::interleave::interleave(&borrowed, &[(0, 1), (1, 0)]).unwrap();
        let actual = actual.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(actual.is_null(0));
        assert_eq!(actual.value_offsets(), &[0, 101, 102]);
        assert_eq!(actual.value(1), "z");
    }
    #[test]
    fn empty_nested_layout_observes_child_offset_allocation_without_source_array() {
        let control = Control(AtomicBool::new(false));
        let mut exact = |_: Layout| Ok::<_, std::convert::Infallible>(());
        let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let scalar =
            ValueLayout::try_new(&DataType::Int64, ValueTypeStorageLifetime::pure()).unwrap();
        let nested = ValueLayout::try_new(
            &DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Utf8,
                true,
            ))),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let scalar_facts = preflight_empty_construction(&scalar, &mut creation, &control).unwrap();
        let nested_facts = preflight_empty_construction(&nested, &mut creation, &control).unwrap();
        assert_eq!(scalar_facts.retained_new_backing_upper(), 0);
        assert_eq!(nested_facts.retained_new_backing_upper(), 128);
        let array = arrow_array::new_empty_array(nested.data_type());
        let array = array
            .as_any()
            .downcast_ref::<arrow_array::ListArray>()
            .unwrap();
        assert_eq!(array.value_offsets(), &[0]);
        let child = array
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(child.value_offsets(), &[0]);
    }
    // Protect the complete zero-row author used by typed aggregate output.
    // The independent Arrow constructor still has one offset for each bytes
    // node; the observed port must retain the caller's exact refusal/control.
    // This owner-local unit test has no service or performance prerequisites.
    #[test]
    fn empty_observed_full_invoice_keeps_offsets_and_original_creator_cause() {
        #[derive(Debug, PartialEq)]
        enum Cause {
            Stopped,
            Refused,
        }
        for (ty, backing) in [
            (DataType::Utf8, 64),
            (
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Utf8,
                    true,
                ))),
                128,
            ),
        ] {
            let layout = ValueLayout::try_new(&ty, ValueTypeStorageLifetime::pure()).unwrap();
            let mut charged = 0;
            let mut request = |_: Layout| Ok::<_, Cause>(());
            let mut opaque = |bytes| {
                charged += bytes;
                Ok::<_, Cause>(())
            };
            let mut creation = ValueSchemaCreation::new(
                ValueTypeStorageLifetime::pure(),
                &mut request,
                &mut opaque,
            );
            let facts =
                preflight_empty_construction_observed(&layout, &mut creation, &mut |_| Ok(()))
                    .unwrap();
            assert_eq!(facts.retained_new_backing_upper(), backing);
            assert_eq!(charged, facts.operation_peak_bytes());
            assert!(charged > facts.metadata_upper_bytes());
            let array = arrow_array::new_empty_array(&ty);
            assert_eq!(array.len(), 0);
            let bytes = if let Some(list) = array.as_any().downcast_ref::<arrow_array::ListArray>()
            {
                assert_eq!(list.value_offsets(), &[0]);
                list.values().as_ref()
            } else {
                array.as_ref()
            };
            assert_eq!(
                bytes
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value_offsets(),
                &[0]
            );

            let mut opaque = |_: usize| Err(Cause::Refused);
            let mut creation = ValueSchemaCreation::new(
                ValueTypeStorageLifetime::pure(),
                &mut request,
                &mut opaque,
            );
            assert!(matches!(
                preflight_empty_construction_observed(&layout, &mut creation, &mut |_| Ok(()),),
                Err(CopyConstructionError::Creator(Cause::Refused))
            ));
            // A stop in the borrowed walk occurs before an opaque admission.
            assert!(matches!(
                preflight_empty_construction_observed(&layout, &mut creation, &mut |_| Err(
                    Cause::Stopped
                ),),
                Err(CopyConstructionError::Creator(Cause::Stopped))
            ));
        }
    }

    #[test]
    fn copy_creator_refusal_preserves_original_cause_before_cancelled_tail() {
        #[derive(Debug, PartialEq)]
        struct Refusal(u32);
        let control = Control(AtomicBool::new(false));
        let mut exact = |_: Layout| Ok::<_, Refusal>(());
        let mut opaque = |_: usize| {
            control.0.store(true, Ordering::SeqCst);
            Err(Refusal(71))
        };
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let sources: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];
        assert!(matches!(
            preflight_interleave_construction(
                &DataType::Int64,
                &sources,
                &[(0, 0)],
                &mut creation,
                &control
            ),
            Err(CopyConstructionError::Creator(Refusal(71)))
        ));
    }
    #[test]
    fn paired_take_and_extend_keep_original_facts_without_billing_payload_to_structure() {
        fn check(array: &dyn Array) -> (usize, usize, CopyOperationFacts) {
            let indices = arrow_array::UInt32Array::from(vec![0, 0]);
            let mut temporary_bytes = 0usize;
            let mut retained_bytes = 0usize;
            let mut exact = |layout: Layout| {
                temporary_bytes += layout.size();
                Ok::<_, std::convert::Infallible>(())
            };
            // Separate accumulators reflect the actual paired request channels.
            let mut opaque_bytes = 0usize;
            let mut opaque = |bytes| {
                opaque_bytes += bytes;
                Ok::<_, std::convert::Infallible>(())
            };
            let mut retained_exact =
                |_: Layout| panic!("result custody uses its sole opaque author");
            let mut retained_opaque = |bytes| {
                retained_bytes += bytes;
                Ok::<_, std::convert::Infallible>(())
            };
            let mut temporary =
                ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
            let mut retained = ValueSchemaCreation::new(
                ValueTypeStorageLifetime::pure(),
                &mut retained_exact,
                &mut retained_opaque,
            );
            let mut work = |_| Ok::<_, std::convert::Infallible>(());
            let paired = super::super::preflight_take_structure_construction(
                array,
                super::super::CopyIndexView::UInt32(&indices),
                &mut temporary,
                &mut retained,
                &mut work,
            )
            .unwrap();
            drop(temporary);
            drop(retained);
            drop(exact);
            drop(opaque);
            drop(retained_opaque);
            let mut original_exact = |_: Layout| Ok::<_, std::convert::Infallible>(());
            let mut original_opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
            let mut original = ValueSchemaCreation::new(
                ValueTypeStorageLifetime::pure(),
                &mut original_exact,
                &mut original_opaque,
            );
            let full = super::super::preflight_take_construction(
                array,
                super::super::CopyIndexView::UInt32(&indices),
                &mut original,
                &Control(AtomicBool::new(false)),
            )
            .unwrap();
            assert_eq!(paired.operation_peak_bytes(), full.operation_peak_bytes());
            assert_eq!(
                paired.retained_new_backing_upper(),
                full.retained_new_backing_upper()
            );
            assert_eq!(paired.metadata_upper_bytes(), full.metadata_upper_bytes());
            let actual = arrow_select::take::take(array, &indices, None).unwrap();
            assert_eq!(actual.len(), 2);
            let segment = super::super::ExtendSegment {
                source: 0,
                start: 0,
                len: 1,
                repeats: 2,
                nulls: 0,
            };
            let mut exact = |_: Layout| Ok::<_, std::convert::Infallible>(());
            let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
            let mut retained_exact = |_: Layout| Ok::<_, std::convert::Infallible>(());
            let mut retained_opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
            let mut temporary =
                ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
            let mut retained = ValueSchemaCreation::new(
                ValueTypeStorageLifetime::pure(),
                &mut retained_exact,
                &mut retained_opaque,
            );
            let paired_extend = super::super::preflight_extend_multi_structure_construction(
                &[array],
                &[segment],
                2,
                &mut temporary,
                &mut retained,
                &mut work,
            )
            .unwrap();
            let full_extend = super::super::preflight_extend_multi_construction(
                &[array],
                &[segment],
                2,
                &mut original,
                &Control(AtomicBool::new(false)),
            )
            .unwrap();
            assert_eq!(
                paired_extend.operation_peak_bytes(),
                full_extend.operation_peak_bytes()
            );
            assert_eq!(
                paired_extend.retained_new_backing_upper(),
                full_extend.retained_new_backing_upper()
            );
            assert_eq!(
                paired_extend.metadata_upper_bytes(),
                full_extend.metadata_upper_bytes()
            );
            (temporary_bytes + opaque_bytes, retained_bytes, paired)
        }
        let short = StringArray::from(vec!["x"]);
        let long_text = "x".repeat(1024 * 1024);
        let long = StringArray::from(vec![long_text.as_str()]);
        let a = check(&short);
        let b = check(&long);
        assert_eq!((a.0, a.1), (b.0, b.1));
        assert!(b.2.retained_new_backing_upper() > a.2.retained_new_backing_upper());
        assert!(a.0 > 0 && a.1 > 0);
    }

    #[test]
    fn paired_structural_work_and_each_request_preserve_first_caller_without_tail() {
        #[derive(Clone, Debug, PartialEq)]
        enum Event {
            Work(bool),
            TemporaryExact(usize),
            TemporaryOpaque(usize),
            Retained(usize),
        }
        fn run(
            extend: bool,
            refuse: Option<usize>,
            cause: Arc<u32>,
        ) -> (
            Result<CopyOperationFacts, CopyConstructionError<Arc<u32>>>,
            Vec<Event>,
        ) {
            let trace = std::cell::RefCell::new(Vec::new());
            let observe = |event| {
                let mut trace = trace.borrow_mut();
                trace.push(event);
                if refuse == Some(trace.len() - 1) {
                    Err(cause.clone())
                } else {
                    Ok(())
                }
            };
            let mut exact = |layout: Layout| observe(Event::TemporaryExact(layout.size()));
            let mut opaque = |bytes| observe(Event::TemporaryOpaque(bytes));
            let mut retained_exact =
                |_: Layout| panic!("result custody has no additional wrapper request");
            let mut retained_opaque = |bytes| observe(Event::Retained(bytes));
            let mut temporary =
                ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
            let mut retained = ValueSchemaCreation::new(
                ValueTypeStorageLifetime::pure(),
                &mut retained_exact,
                &mut retained_opaque,
            );
            let mut work = |boundary| observe(Event::Work(boundary));
            let array = Int64Array::from(vec![Some(10), None]);
            let indices = arrow_array::UInt32Array::from(vec![Some(1), None, Some(0)]);
            let result = if extend {
                let segment = super::super::ExtendSegment {
                    source: 0,
                    start: 0,
                    len: 2,
                    repeats: 1,
                    nulls: 1,
                };
                super::super::preflight_extend_multi_structure_construction(
                    &[&array],
                    &[segment],
                    3,
                    &mut temporary,
                    &mut retained,
                    &mut work,
                )
            } else {
                super::super::preflight_take_structure_construction(
                    &array,
                    super::super::CopyIndexView::UInt32(&indices),
                    &mut temporary,
                    &mut retained,
                    &mut work,
                )
            };
            (result, trace.into_inner())
        }
        let cause = Arc::new(71);
        for extend in [false, true] {
            let (result, trace) = run(extend, None, cause.clone());
            result.unwrap();
            assert!(
                trace
                    .iter()
                    .any(|event| matches!(event, Event::TemporaryExact(_)))
            );
            assert!(
                trace
                    .iter()
                    .any(|event| matches!(event, Event::TemporaryOpaque(_)))
            );
            assert!(matches!(trace.last(), Some(Event::Retained(bytes)) if *bytes > 0));
            for boundary in 0..trace.len() {
                let (result, actual) = run(extend, Some(boundary), cause.clone());
                match result {
                    Err(CopyConstructionError::Creator(error)) => {
                        assert!(Arc::ptr_eq(&error, &cause))
                    }
                    other => panic!("wrong original cause: {other:?}"),
                }
                assert_eq!(actual, trace[..=boundary]);
            }
        }
    }

    #[test]
    fn borrowed_null_arrow_structure_uses_original_dictionary_constructor() {
        let ty = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let mut exact = |_: Layout| panic!("null structure is an opaque library constructor");
        let mut bytes = 0;
        let mut opaque = |value| {
            bytes += value;
            Ok::<_, std::convert::Infallible>(())
        };
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let mut work = |_| Ok::<_, std::convert::Infallible>(());
        let facts =
            preflight_null_arrow_structure_observed(&ty, 3, &mut creation, &mut work).unwrap();
        let actual = arrow_array::new_null_array(&ty, 3);
        assert_eq!(actual.data_type(), &ty);
        assert_eq!(actual.null_count(), 3);
        drop(creation);
        drop(opaque);
        assert_eq!(bytes, facts.metadata_upper_bytes());
        assert!(facts.retained_new_backing_upper() > 0);
    }
}
