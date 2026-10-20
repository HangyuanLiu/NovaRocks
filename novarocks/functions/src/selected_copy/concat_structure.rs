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

//! Structural ports for the ONE pinned Arrow concat kernel. The caller keeps
//! its existing payload/accounting and independently verified complete types.
use super::{
    CopyConstructionError, CopyError, CopyObservation, CopyOperationFacts, ScratchCoverage,
    WorkObservation, add, preflight_original_concat_with_observation, take_host,
};
use arrow_array::{Array, ArrayRef};
use novarocks_type_contract::ValueSchemaCreation;
use std::alloc::Layout;

#[derive(Debug)]
pub enum ConcatStructureFailure<H> {
    Construction(CopyConstructionError<H>),
    /// Original constructor-model source error, never a caller/host cause.
    Source(crate::array_projection::ArrayProjectionFailure),
    ValueType(novarocks_type_contract::ValueTypeError),
}
impl<H> From<novarocks_type_contract::ValueTypeError> for ConcatStructureFailure<H> {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        Self::ValueType(error)
    }
}
impl<H> From<crate::array_projection::ArrayProjectionFailure> for ConcatStructureFailure<H> {
    fn from(source: crate::array_projection::ArrayProjectionFailure) -> Self {
        Self::Source(source)
    }
}
impl<H> From<CopyConstructionError<H>> for ConcatStructureFailure<H> {
    fn from(error: CopyConstructionError<H>) -> Self {
        Self::Construction(error)
    }
}
impl<H> From<CopyError> for ConcatStructureFailure<H> {
    fn from(error: CopyError) -> Self {
        Self::Construction(error.into())
    }
}

/// Inputs must already have the same fully checked execution declaration and
/// exact Arrow carrier. This borrowed check rejects a mismatch before quoting;
/// it does not infer semantics, normalize arrays or grant a writer capability.
/// The caller keeps temporary alive through its ONE original concat and loans
/// retained's finite marker to the result. Neither port includes payload fees.
pub fn preflight_concat_structure_construction<H>(
    sources: &[&ArrayRef],
    temporary: &mut ValueSchemaCreation<'_, H>,
    retained: &mut ValueSchemaCreation<'_, H>,
    observe: &mut impl FnMut(bool) -> Result<(), H>,
) -> Result<CopyOperationFacts, ConcatStructureFailure<H>> {
    let mut work = |boundary| observe(boundary).map_err(CopyConstructionError::Creator);
    work(true)?;
    let first = sources.first().ok_or(CopyError::Invalid(
        "typed concat requires a nonempty source table",
    ))?;
    for source in sources.iter().skip(1) {
        work(true)?;
        let same = novarocks_type_contract::arrow_data_types_exact_observed::<
            ConcatStructureFailure<H>,
        >(source.data_type(), first.data_type(), || {
            work(false).map_err(ConcatStructureFailure::from)
        })?;
        if !same {
            return Err(CopyError::Invalid(
                "typed concat sources differ in their exact Arrow carrier",
            )
            .into());
        }
    }
    if sources.len() == 1 {
        // Original concat(one) calls its exact source's slice. Reuse the sole
        // source-based slice constructor model rather than pretending no new
        // wrappers exist. This is its conservative operation upper, not an
        // exact retained stock measurement; the finite result marker owns it.
        let mut bytes = 0;
        retained.with_request_ports(|request, opaque| {
            let mut request = |layout| {
                request(layout).map_err(|h| {
                    ConcatStructureFailure::Construction(CopyConstructionError::Creator(h))
                })
            };
            let mut opaque = |bound: crate::array_projection::ArrayProjectionOpaqueBound| {
                bytes = add(bytes, bound.bytes_upper)?;
                opaque(bound.bytes_upper).map_err(|h| {
                    ConcatStructureFailure::Construction(CopyConstructionError::Creator(h))
                })
            };
            let mut observe_units = |units| {
                work(true)?;
                for _ in 0..units {
                    work(false)?;
                }
                Ok::<_, ConcatStructureFailure<H>>(())
            };
            crate::array_projection::observe_array_normalization_construction(
                crate::array_projection::ArrayNormalizationStep::SliceArray {
                    source: first,
                    offset: 0,
                    length: first.len(),
                },
                &mut request,
                &mut opaque,
                &mut observe_units,
            )
        })?;
        return Ok(CopyOperationFacts::borrowed_slice(bytes)?);
    }
    let mut source_metadata = 0;
    for source in sources {
        source_metadata = add(
            source_metadata,
            crate::array_backing_geometry::copy_metadata_bytes_observed(
                source.as_ref(),
                &mut work,
            )?,
        )?;
    }
    work(true)?;
    temporary
        .observe_opaque_upper_bound(source_metadata)
        .map_err(CopyConstructionError::Creator)?;
    // This real model/preflight reference table is separate from the caller's
    // Vec<&ArrayRef> and Arrow concat_batches' own per-column reference table.
    temporary
        .request_layout(
            Layout::array::<&dyn Array>(sources.len())
                .map_err(|source| CopyError::layout::<&dyn Array>(sources.len(), source))?,
        )
        .map_err(CopyConstructionError::Creator)?;
    let mut refs = Vec::new();
    refs.try_reserve_exact(sources.len()).map_err(|source| {
        CopyError::Control(crate::KernelFailure::from_domain(
            crate::allocation_failure::AllocationRefusal::new(
                "copy concat reference table",
                source,
            ),
        ))
    })?;
    for source in sources {
        work(false)?;
        refs.push(source.as_ref());
    }
    let mut invoice = take_host::CopyInvoiceTotals::structural();
    temporary.with_request_ports(|request, _| {
        let mut request = |layout| request(layout).map_err(CopyConstructionError::Creator);
        let mut observation = CopyObservation(
            WorkObservation::Creator(&mut work),
            None,
            ScratchCoverage::RecursiveSelections,
            Some(&mut invoice),
            Some(&mut request),
        );
        preflight_original_concat_with_observation(&refs, &mut observation)
    })?;
    let scratch = invoice.temporary_metadata_upper()?;
    let result = invoice.result_metadata_upper();
    let facts = invoice.finish(source_metadata)?;
    work(true)?;
    temporary
        .observe_opaque_upper_bound(scratch)
        .map_err(CopyConstructionError::Creator)?;
    retained
        .observe_opaque_upper_bound(result)
        .map_err(CopyConstructionError::Creator)?;
    Ok(facts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use novarocks_type_contract::ValueTypeStorageLifetime;
    use std::cell::Cell;

    #[test]
    fn same_original_concat_values_and_source_refusal_preserve_paired_header_ports() {
        let a: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        let b: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![Some(7)]));
        let temporary_bytes = Cell::new(0usize);
        let result_bytes = Cell::new(0usize);
        let mut exact = |_: Layout| Ok::<_, &'static str>(());
        let mut temporary_opaque = |bytes| {
            temporary_bytes.set(temporary_bytes.get() + bytes);
            Ok::<_, &'static str>(())
        };
        let mut result_exact = |_: Layout| Ok::<_, &'static str>(());
        let mut result_opaque = |bytes| {
            result_bytes.set(result_bytes.get() + bytes);
            Ok::<_, &'static str>(())
        };
        let mut temporary = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut exact,
            &mut temporary_opaque,
        );
        let mut retained = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut result_exact,
            &mut result_opaque,
        );
        let facts = preflight_concat_structure_construction(
            &[&a, &b],
            &mut temporary,
            &mut retained,
            &mut |_| Ok(()),
        )
        .unwrap();
        // Execute the ONE original Arrow kernel, preserving actual selected
        // values/nulls. This model never constructs or normalizes the result.
        let actual = arrow_select::concat::concat(&[a.as_ref(), b.as_ref()]).unwrap();
        let actual = actual.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            actual,
            &Int64Array::from(vec![Some(1), None, Some(3), Some(7)])
        );
        assert!(temporary_bytes.get() > 0);
        assert!(result_bytes.get() > 0);
        assert!(facts.operation_peak_bytes() > facts.metadata_upper_bytes());
        // A new result port observes headers; none of the payload operation
        // peak is promoted to a result construction fee.
        assert!(result_bytes.get() < facts.operation_peak_bytes());

        let mut exact = |_: Layout| -> Result<(), &'static str> {
            panic!("scratch after first creator refusal")
        };
        let opaque_calls = Cell::new(0);
        let events = Cell::new(0);
        let refused_work = Cell::new(None);
        let mut opaque = |_: usize| {
            opaque_calls.set(opaque_calls.get() + 1);
            refused_work.set(Some(events.get()));
            Err("original temporary Host")
        };
        let mut temporary =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let result_before = result_bytes.get();
        assert!(matches!(
            preflight_concat_structure_construction(
                &[&a, &b],
                &mut temporary,
                &mut retained,
                &mut |_| {
                    events.set(events.get() + 1);
                    Ok(())
                }
            ),
            Err(ConcatStructureFailure::Construction(
                CopyConstructionError::Creator("original temporary Host")
            ))
        ));
        assert_eq!(opaque_calls.get(), 1);
        assert_eq!(result_bytes.get(), result_before);
        assert_eq!(refused_work.get(), Some(events.get()));
        assert_eq!(
            a.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from(vec![Some(1), None, Some(3)])
        );
    }

    #[test]
    fn original_single_concat_slice_and_full_carrier_negative_keep_source_values() {
        let source: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![Some(5), None, Some(9)]));
        let mut exact = |_: Layout| Ok::<_, &'static str>(());
        let mut opaque = |_: usize| Ok::<_, &'static str>(());
        let mut result_exact = |_: Layout| Ok::<_, &'static str>(());
        let mut result_bytes = 0;
        let mut result_opaque = |bytes| {
            result_bytes += bytes;
            Ok::<_, &'static str>(())
        };
        let mut temporary =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut exact, &mut opaque);
        let mut retained = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut result_exact,
            &mut result_opaque,
        );
        preflight_concat_structure_construction(
            &[&source],
            &mut temporary,
            &mut retained,
            &mut |_| Ok(()),
        )
        .unwrap();
        let actual = arrow_select::concat::concat(&[source.as_ref()]).unwrap();
        assert_eq!(
            actual.as_any().downcast_ref::<Int64Array>().unwrap(),
            source.as_any().downcast_ref::<Int64Array>().unwrap()
        );
        // Different exact carrier is a Source failure before either creator.
        let wrong: ArrayRef = std::sync::Arc::new(arrow_array::Int32Array::from(vec![5]));
        let mut no_exact = |_: Layout| -> Result<(), &'static str> {
            panic!("constructor before source equality")
        };
        let mut no_opaque =
            |_: usize| -> Result<(), &'static str> { panic!("constructor before source equality") };
        let mut temporary = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::pure(),
            &mut no_exact,
            &mut no_opaque,
        );
        assert!(matches!(
            preflight_concat_structure_construction(
                &[&source, &wrong],
                &mut temporary,
                &mut retained,
                &mut |_| Ok(())
            ),
            Err(ConcatStructureFailure::Construction(
                CopyConstructionError::Copy(CopyError::Invalid(
                    "typed concat sources differ in their exact Arrow carrier"
                ))
            ))
        ));
        assert!(result_bytes > 0);
    }
}
