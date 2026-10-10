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

//! ONE selected-copy extent author feeds this opaque take admission. Geometry
//! is not authority: only the supplied host grants its operation reservation.
use super::{CopyError, CopyMode, ScratchCoverage, add, mul, buffer_extent, zip};
use super::copy_buffer_peak::CopyBufferPeak;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{AggregateStateAllocator, KernelControlObservation, KernelEvaluationControl, KernelFailure};
use arrow_array::{Array, ArrayRef, UInt32Array, UInt64Array};
use arrow_buffer::ArrowNativeType;
use arrow_data::{ArrayData, transform::MutableArrayData};
use arrow_schema::{DataType, UnionMode};
use std::sync::Arc;

/// These are the two actual gather-index carriers used by the existing hosts.
/// The caller retains their original backing owner separately from a new grant.
#[derive(Clone)]
pub enum CopyIndices {
    UInt32(Arc<UInt32Array>),
    UInt64(Arc<UInt64Array>),
}
impl CopyIndices {
    pub(super) fn len(&self) -> usize {
        match self {
            Self::UInt32(a) => a.len(),
            Self::UInt64(a) => a.len(),
        }
    }
    pub(super) fn as_array(&self) -> &dyn Array {
        match self {
            Self::UInt32(a) => a.as_ref(),
            Self::UInt64(a) => a.as_ref(),
        }
    }
    pub(super) fn raw_value(&self, row: usize) -> usize {
        match self {
            Self::UInt32(a) => a.value(row).as_usize(),
            Self::UInt64(a) => a.value(row).as_usize(),
        }
    }
    pub(super) fn index_maximum(&self) -> usize {
        match self {
            Self::UInt32(_) => u32::MAX as usize,
            Self::UInt64(_) => usize::MAX,
        }
    }
}
#[derive(Debug)]
pub enum CopyOperationError {
    Preflight(CopyError),
    OriginalData(super::copy_diagnostic::OriginalCopyData),
    Control(KernelFailure),
}
impl From<CopyError> for CopyOperationError {
    fn from(e: CopyError) -> Self {
        match e {
            CopyError::Control(c) => Self::Control(c),
            other => Self::Preflight(other),
        }
    }
}
impl From<KernelFailure> for CopyOperationError {
    fn from(e: KernelFailure) -> Self {
        Self::Control(e)
    }
}

/// Unforgeable successful output of the SAME constructor/selection traversal.
/// No caller budget, input stock or function name can construct these facts.
#[derive(Debug)]
pub struct CopyOperationFacts {
    new_retained: usize,
    operation_peak: usize,
    metadata: usize,
    copied_root: Option<usize>,
}
impl CopyOperationFacts {
    pub fn retained_new_backing_upper(&self) -> usize {
        self.new_retained
    }
    pub fn operation_peak_bytes(&self) -> usize {
        self.operation_peak
    }
    pub fn metadata_upper_bytes(&self) -> usize {
        self.metadata
    }
    pub(super) fn borrowed_slice(metadata: usize) -> Result<Self, CopyError> {
        buffer_extent(metadata, 1)?;
        Ok(Self {
            new_retained: 0,
            operation_peak: metadata,
            metadata,
            copied_root: None,
        })
    }
    pub(super) fn admit_original_concat_diagnostic(
        &mut self,
        diagnostic: &super::concat_diagnostic::OriginalConcatDiagnosticFacts,
    ) -> Result<(), CopyError> {
        self.operation_peak = add(self.operation_peak, diagnostic.operation_peak_bytes())?;
        Ok(())
    }
    pub(super) fn bind_original_copy(&mut self, original: &ArrayRef) {
        assert!(self.copied_root.is_none(), "copy facts bound only once");
        self.copied_root = Some(Arc::as_ptr(original) as *const () as usize);
    }
    pub(crate) fn matches_original_copy(&self, original: &ArrayRef) -> bool {
        self.copied_root == Some(Arc::as_ptr(original) as *const () as usize)
    }
}
#[derive(Default)]
pub(super) struct CopyInvoiceTotals {
    initial_buffers: usize,
    selected_buffers: usize,
    exact_buffers: usize,
    metadata: usize,
}
impl CopyInvoiceTotals {
    fn initial(&mut self, bytes: usize) -> Result<(), CopyError> {
        self.initial_buffers = add(self.initial_buffers, zip::rounded_capacity(bytes)?)?;
        Ok(())
    }
    fn required(&mut self, bytes: usize) -> Result<(), CopyError> {
        self.selected_buffers = add(self.selected_buffers, zip::rounded_capacity(bytes)?)?;
        Ok(())
    }
    fn exact(&mut self, bytes: usize) -> Result<(), CopyError> {
        self.exact_buffers = add(
            self.exact_buffers,
            CopyBufferPeak::exact(bytes)?.retained_upper(),
        )?;
        Ok(())
    }
    pub(super) fn selected_payload(
        &mut self,
        bytes: usize,
        mode: CopyMode,
    ) -> Result<(), CopyError> {
        if mode.is_take() {
            self.exact(bytes)
        } else {
            self.required(bytes)
        }
    }
    /// This is buffer geometry, not another value traversal. Exact payload and
    /// child extents are supplied by the existing bytes/map_ranges/run authors.
    pub(super) fn selected_node(
        &mut self,
        array: &dyn Array,
        rows: usize,
        mode: CopyMode,
    ) -> Result<(), CopyError> {
        let ty = array.data_type();
        let bitmap = add(rows / 8, usize::from(!rows.is_multiple_of(8)))?;
        // The null bitmap may borrow indices in take, or be fresh. A fresh
        // rounded envelope is conservative; existing input bytes are NOT stock.
        self.required(bitmap)?;
        if let Some(width) = ty.primitive_width() {
            return self.required(mul(rows, width)?);
        }
        match ty {
            DataType::Null => Ok(()),
            DataType::Boolean => self.required(bitmap),
            DataType::FixedSizeBinary(w) => self.required(mul(
                rows,
                usize::try_from(*w).map_err(|_| CopyError::Extent)?,
            )?),
            DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
                let width = if matches!(ty, DataType::LargeUtf8 | DataType::LargeBinary) {
                    8
                } else {
                    4
                };
                // extend_offsets reserves an input slice with one additional
                // offset before appending. It can transiently request rows+2.
                self.required(mul(add(rows, 2)?, width)?)
            }
            DataType::Utf8View | DataType::BinaryView => self.required(mul(rows, 16)?),
            DataType::Dictionary(key, _) => {
                self.required(mul(rows, key.primitive_width().ok_or(CopyError::Extent)?)?)
            }
            DataType::List(_) | DataType::Map(..) => self.required(mul(add(rows, 2)?, 4)?),
            DataType::LargeList(_) => self.required(mul(add(rows, 2)?, 8)?),
            DataType::ListView(_) => self.required(mul(rows, 8)?),
            DataType::LargeListView(_) => self.required(mul(rows, 16)?),
            DataType::Struct(_) => Ok(()),
            DataType::FixedSizeList(_, w) => {
                if mode.is_take() {
                    // Original take_value_indices_from_fixed_size_list builds
                    // a UInt32 child index and validity, including NULL parents.
                    let child = mul(rows, usize::try_from(*w).map_err(|_| CopyError::Extent)?)?;
                    self.required(mul(child, 4)?)?;
                    self.required(add(child / 8, usize::from(!child.is_multiple_of(8)))?)?;
                }
                Ok(())
            }
            DataType::Union(fields, union_mode) => {
                self.required(rows)?;
                if *union_mode == UnionMode::Dense {
                    // Original offsets and final offsets coexist. Each field's
                    // filter has a mask and filtered offsets. Its Int32 route
                    // does not enable FilterBuilder's heap strategy tables.
                    self.required(mul(rows, 8)?)?;
                    for _ in fields.iter() {
                        self.required(bitmap)?;
                        self.required(mul(rows, 4)?)?;
                        let headers = add(
                            size_of::<arrow_array::BooleanArray>(),
                            mul(2, size_of::<arrow_array::Int32Array>())?,
                        )?;
                        self.metadata = add(
                            self.metadata,
                            add(headers, mul(3, 2 * size_of::<usize>())?)?,
                        )?;
                    }
                }
                Ok(())
            }
            DataType::RunEndEncoded(ends, _) => {
                let width = ends
                    .data_type()
                    .primitive_width()
                    .ok_or(CopyError::Extent)?;
                if mode.is_take() {
                    // RunEndBuffer owns ordered_indices AND physical_indices
                    // simultaneously. Unstable index sort allocates no heap.
                    self.exact(mul(2, mul(rows, size_of::<usize>())?)?)?;
                    // Both original builders start at one and double; final
                    // emitted runs cannot exceed actual selected logical rows.
                    self.initial(width)?;
                    self.initial(8)?;
                    self.required(mul(rows, width)?)?;
                    self.required(mul(rows, 8)?)
                } else {
                    // Original MutableArrayData run::build_extend_arrays owns
                    // a growing Vec<u8> beside the destination run-end buffer.
                    // Its selected physical run count is bounded by THIS
                    // invocation's logical rows; no source stock is copied here.
                    self.required(mul(rows, width)?)?;
                    self.required(mul(rows, width)?)
                }
            }
            other => Err(CopyError::Unsupported(other.clone())),
        }
    }
    pub(super) fn original_concat_node(
        &mut self,
        array: &dyn Array,
        source_count: usize,
        rows: usize,
    ) -> Result<(), CopyError> {
        // Each specialized original concat owns a per-source reference table.
        // List/Struct/Run paths additionally own Arc slice and output tables.
        let borrowed = mul(source_count, size_of::<&dyn Array>())?;
        let slices = mul(source_count, size_of::<ArrayRef>())?;
        self.metadata = add(self.metadata, add(borrowed, slices)?)?;
        let fields = match array.data_type() {
            DataType::Struct(fields) => fields.len(),
            DataType::Union(fields, _) => fields.len(),
            _ => 0,
        };
        self.metadata = add(self.metadata, mul(fields, size_of::<ArrayRef>())?)?;
        if matches!(
            array.data_type(),
            DataType::Null | DataType::RunEndEncoded(..)
        ) {
            // Null concat creates no row-sized payload. Run is invoiced by the
            // SAME run child-range author using actual physical ends below.
            return Ok(());
        }
        self.selected_node(array, rows, CopyMode::Extend)
    }

    pub(super) fn original_run_concat_scratch(
        &mut self,
        physical_rows: usize,
        source_count: usize,
        end_width: usize,
    ) -> Result<(), CopyError> {
        // Pinned concat_run_arrays stores a borrowed Run vector, an adjustment
        // vector (N+1), values_slice Arc vector and borrowed child-concat vector.
        self.metadata = add(self.metadata, mul(source_count, size_of::<&dyn Array>())?)?;
        self.exact(mul(add(source_count, 1)?, end_width)?)?;
        // A recursively reached run child of original Mutable fallback can
        // retain its growing extension Vec beside the destination end buffer.
        self.required(mul(physical_rows, end_width)?)?;
        self.required(mul(physical_rows, end_width)?)
    }

    pub(super) fn original_dictionary_merge_scratch(
        &mut self,
        values: &[&dyn Array],
        key: &DataType,
        source_count: usize,
    ) -> Result<(), CopyError> {
        let value_type = values
            .first()
            .ok_or(CopyError::Invalid(
                "original dictionary concat has no value source",
            ))?
            .data_type();
        if !value_type.is_primitive()
            && !matches!(
                value_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary
            )
        {
            return Ok(());
        }
        let mut domain = 0;
        for array in values {
            domain = add(domain, array.len())?;
            self.required(add(
                array.len() / 8,
                usize::from(!array.len().is_multiple_of(8)),
            )?)?;
        }
        let key_width = key.primitive_width().ok_or(CopyError::Extent)?;
        // Actual masked-values Vec tuples (index, optional borrowed bytes),
        // one native mapping per domain element and interleave source choices.
        self.exact(mul(domain, size_of::<(usize, Option<&[u8]>)>())?)?;
        self.exact(mul(domain, key_width)?)?;
        self.exact(mul(domain, size_of::<(usize, usize)>())?)?;
        self.metadata = add(
            self.metadata,
            mul(source_count, size_of::<Vec<(usize, Option<&[u8]>)>>())?,
        )?;
        self.metadata = add(self.metadata, mul(source_count, size_of::<Vec<u64>>())?)?;
        // ONE original fixed interner formula, with capacity bounded by the
        // actual full domain; no hash/equality computation or key-limit gate.
        // u64 has the largest installed dictionary native key width/alignment.
        let capacity = u64::try_from(domain).map_err(|_| CopyError::Extent)?;
        let shift = capacity
            .checked_add(128)
            .ok_or(CopyError::Extent)?
            .leading_zeros();
        let buckets = (u64::MAX >> shift).saturating_add(1);
        let buckets = usize::try_from(buckets).map_err(|_| CopyError::Extent)?;
        self.exact(mul(buckets, size_of::<Option<(Option<&[u8]>, u64)>>())?)
    }

    pub(super) fn constructor(
        &mut self,
        data: &ArrayData,
        sources: usize,
        capacity: usize,
    ) -> Result<(), CopyError> {
        // Actual Arrow MutableArrayData body and vectors, with two per-source
        // Box<dyn Fn> tables. Across pinned transform/*.rs, the largest capture
        // is two borrowed slices plus two usize scalars; no cloned value heap.
        let refs = mul(sources, size_of::<&ArrayData>())?;
        let tables = mul(sources, mul(2, size_of::<Box<dyn Fn()>>())?)?;
        let captures = mul(sources, mul(2, size_of::<(&[u8], &[u8], usize, usize)>())?)?;
        let children = mul(
            data.child_data().len(),
            size_of::<MutableArrayData<'static>>(),
        )?;
        let buffers = mul(data.buffers().len(), size_of::<arrow_buffer::Buffer>())?;
        let own = add(
            size_of::<MutableArrayData<'static>>(),
            add(add(refs, tables)?, add(captures, add(children, buffers)?)?)?,
        )?;
        // Vec geometric growth can retain two capacities and its predecessor.
        self.metadata = add(self.metadata, mul(own, 3)?)?;
        self.initial(add(capacity / 8, usize::from(!capacity.is_multiple_of(8)))?)?;
        let ty = data.data_type();
        if let Some(width) = ty.primitive_width() {
            return self.initial(mul(capacity, width)?);
        }
        match ty {
            DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => {
                self.initial(mul(
                    add(capacity, 1)?,
                    if matches!(ty, DataType::LargeUtf8 | DataType::LargeBinary) {
                        8
                    } else {
                        4
                    },
                )?)?;
                self.initial(capacity)
            }
            DataType::Boolean => {
                self.initial(add(capacity / 8, usize::from(!capacity.is_multiple_of(8)))?)
            }
            DataType::FixedSizeBinary(w) => self.initial(mul(
                capacity,
                usize::try_from(*w).map_err(|_| CopyError::Extent)?,
            )?),
            DataType::List(_) | DataType::Map(..) => self.initial(mul(add(capacity, 1)?, 4)?),
            DataType::LargeList(_) => self.initial(mul(add(capacity, 1)?, 8)?),
            DataType::ListView(_) => self.initial(mul(capacity, 8)?),
            DataType::LargeListView(_) => self.initial(mul(capacity, 16)?),
            DataType::Utf8View | DataType::BinaryView => self.initial(mul(capacity, 16)?),
            DataType::Dictionary(key, _) => self.initial(mul(
                capacity,
                key.primitive_width().ok_or(CopyError::Extent)?,
            )?),
            DataType::Union(_, mode) => {
                self.initial(capacity)?;
                if *mode == UnionMode::Dense {
                    self.initial(mul(capacity, 4)?)?;
                }
                Ok(())
            }
            DataType::Null
            | DataType::Struct(_)
            | DataType::FixedSizeList(..)
            | DataType::RunEndEncoded(..) => Ok(()),
            other => Err(CopyError::Unsupported(other.clone())),
        }
    }
    pub(super) fn finish(self, source_metadata: usize) -> Result<CopyOperationFacts, CopyError> {
        // Summing constructor and selected envelopes avoids pairing distinct
        // nodes by guessed identity. Each actual buffer's initial/required is
        // <= their sum. Pinned reserve max(round64(required),2*previous) gives
        // retained <=2*sum and old+new <=3*sum, independently of in-place realloc.
        let high = add(self.initial_buffers, self.selected_buffers)?;
        let mutable_peak = CopyBufferPeak::mutable(0, high)?;
        let retained = add(self.exact_buffers, mutable_peak.retained_upper())?;
        let metadata = add(self.metadata, source_metadata)?;
        let peak = add(
            add(self.exact_buffers, mutable_peak.transient_upper())?,
            metadata,
        )?;
        buffer_extent(peak, 1)?;
        Ok(CopyOperationFacts {
            new_retained: retained,
            operation_peak: peak,
            metadata,
            copied_root: None,
        })
    }
}

struct CopySources<C> {
    source: ArrayRef,
    indices: CopyIndices,
    original_owner: C,
}
impl<C> crate::arrow_result_custody::CopyInputBacking for CopySources<C> {
    fn source_count(&self) -> usize {
        1
    }
    fn source_array_at(&self, ordinal: usize) -> Option<&dyn Array> {
        (ordinal == 0).then_some(self.source.as_ref())
    }
    fn index_array(&self) -> Option<&dyn Array> {
        Some(self.indices.as_array())
    }
}

pub struct RetainedTakeResult {
    values: ArrayRef,
    facts: CopyOperationFacts,
    new_backing_envelope: usize,
    new_buffer_stock: usize,
}
impl RetainedTakeResult {
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }
    pub fn facts(&self) -> &CopyOperationFacts {
        &self.facts
    }
    pub fn new_backing_envelope(&self) -> usize {
        self.new_backing_envelope
    }
    pub fn new_buffer_stock(&self) -> usize {
        self.new_buffer_stock
    }
    pub fn into_values(self) -> ArrayRef {
        self.values
    }
}

/// Complete standalone take: actual host scratch + pre-copy opaque admission +
/// ONE original Arrow take + ONE Buffer custody handoff. Existing entry points
/// and all compiled/public callers remain unchanged until separately activated.
/// `original_owner` is the caller's actual source/index lease, not a new budget.
/// Original Arrow Data strings and panics propagate without reinterpretation.
pub fn take_copy_in<C: Send + Sync + 'static>(
    source: ArrayRef,
    indices: CopyIndices,
    original_owner: C,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<RetainedTakeResult, CopyOperationError> {
    // Pin input/index backing before its original owner on EVERY exit,
    // including constructor refusal before any actual output exists.
    let sources = CopySources {
        source,
        indices,
        original_owner,
    };
    let observed = KernelControlObservation::new(control);
    observed.checkpoint(0)?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
    observed.checkpoint(0)?;
    let allocator = HostAggregateAllocator::try_new(host)?;
    let mut work = EvaluationCheckpoints::new(&observed);
    // SAME borrowed structural author; no allocating to_data precedes grant.
    let source_metadata = add(
        crate::array_backing_geometry::copy_metadata_bytes(sources.source.as_ref(), &mut work)?,
        crate::array_backing_geometry::copy_metadata_bytes(sources.indices.as_array(), &mut work)?,
    )?;
    let metadata_scope = charge.reserve_operation(source_metadata)?;
    work.flush()?;
    let mut invoice = CopyInvoiceTotals::default();
    super::preflight_original_take_with_invoice(
        sources.source.as_ref(),
        &sources.indices,
        |boundary| if boundary { work.flush() } else { work.step() },
        &allocator,
        &mut invoice,
    )?;
    // Both original input carriers are still borrowed by their source owner.
    // Every raw-index block/range was backed by the actual host scratch vector.
    drop(metadata_scope);
    let mut facts = invoice.finish(source_metadata)?;
    work.flush()?;
    let diagnostic = super::copy_diagnostic::OriginalTakeDiagnosticFacts::try_new()?;
    facts.operation_peak = add(facts.operation_peak, diagnostic.operation_peak_bytes())?;
    let mut reservation = charge.reserve_operation(facts.operation_peak_bytes())?;
    work.flush()?;
    let copied =
        match arrow_select::take::take(sources.source.as_ref(), sources.indices.as_array(), None) {
            Ok(copied) => copied,
            Err(error) => {
                // Error construction and the ONE original Display-to-String author
                // are under the earlier actual diagnostic peak grant. Keep the
                // returned full text charged until its real last Drop.
                return Err(CopyOperationError::OriginalData(diagnostic.retain(
                    error,
                    charge,
                    &mut reservation,
                )?));
            }
        };
    facts.bind_original_copy(&copied);
    work.flush()?;
    let result = crate::arrow_result_custody::retain_copied_result_backing(
        copied,
        sources,
        charge,
        &mut reservation,
        allocator,
        &facts,
        &mut work,
    )?;
    work.finish()?;
    Ok(RetainedTakeResult {
        values: result.values,
        facts,
        new_backing_envelope: result.new_backing_envelope,
        new_buffer_stock: result.new_buffer_stock,
    })
}

#[cfg(test)]
#[path = "take_host_tests.rs"]
mod tests;
