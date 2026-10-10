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

//! ONE original concat under actual host scratch/opaque admission. This entry
//! neither merges dictionaries itself nor substitutes MutableArrayData for the
//! specialized original ListView/Run/Dictionary author. Caller-owned sources
//! and their actual lease owner are transferred to returned Buffer custody.
use super::{CopyOperationError, CopyOperationFacts, add};
use super::child_scratch::ChildScratchVec;
use super::concat_diagnostic::OriginalConcatDiagnosticFacts;
use super::take_host::CopyInvoiceTotals;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::opaque_memory::OpaqueRetainedCharge;
use crate::{AggregateStateAllocator, KernelControlObservation, KernelEvaluationControl};
use arrow_array::{Array, ArrayRef};
use std::sync::Arc;

struct ConcatSources<C> {
    // Destroy original array aliases before their same-site source lease.
    sources: Vec<ArrayRef>,
    original_owner: C,
}
impl<C> crate::arrow_result_custody::CopyInputBacking for ConcatSources<C> {
    fn source_count(&self) -> usize {
        self.sources.len()
    }
    fn source_array_at(&self, ordinal: usize) -> Option<&dyn Array> {
        self.sources.get(ordinal).map(|source| source.as_ref())
    }
    fn index_array(&self) -> Option<&dyn Array> {
        None
    }
}
pub struct RetainedConcatResult {
    values: ArrayRef,
    facts: CopyOperationFacts,
    new_backing_envelope: usize,
    new_buffer_stock: usize,
}
impl RetainedConcatResult {
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

/// Sources are the caller's already-owned original occurrence table. The
/// actual application caller must construct this table and its lease together;
/// generic C identity alone is not a source/owner pairing proof or memory grant.
/// Original Arrow Data, panics, full types, fields and metadata remain intact.
pub fn concat_copy_in<C: Send + Sync + 'static>(
    arrays: Vec<ArrayRef>,
    original_owner: C,
    host: Arc<dyn AggregateStateAllocator>,
    control: &dyn KernelEvaluationControl,
) -> Result<RetainedConcatResult, CopyOperationError> {
    let sources = ConcatSources {
        sources: arrays,
        original_owner,
    };
    let observed = KernelControlObservation::new(control);
    observed.checkpoint(0)?;
    let charge = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
    observed.checkpoint(0)?;
    let allocator = HostAggregateAllocator::try_new(host)?;
    let mut work = EvaluationCheckpoints::new(&observed);
    // The borrowed reference table itself requests its actual Layout. It is
    // not charged from an unrelated source stock or fabricated row budget.
    let mut refs = ChildScratchVec::try_with_capacity(sources.sources.len(), Some(&allocator))?;
    let mut source_metadata = 0;
    for source in &sources.sources {
        work.step()?;
        source_metadata = add(
            source_metadata,
            crate::array_backing_geometry::copy_metadata_bytes(source.as_ref(), &mut work)?,
        )?;
        refs.try_push(source.as_ref())?;
    }
    // Observe exact source type equality only to select resource work. The
    // original concat still authors the unique type-set order and full error.
    let mut compatible = true;
    if let Some(first) = refs.first() {
        for source in refs.iter().skip(1) {
            work.step()?;
            compatible &= source.data_type() == first.data_type();
        }
    }
    let diagnostic = OriginalConcatDiagnosticFacts::try_new(&refs, &mut work)?;
    let metadata_scope = charge.reserve_operation(source_metadata)?;
    work.flush()?;
    let mut invoice = CopyInvoiceTotals::default();
    if compatible {
        super::preflight_original_concat_with_invoice(
            &refs,
            |boundary| if boundary { work.flush() } else { work.step() },
            &allocator,
            &mut invoice,
        )?;
    }
    drop(metadata_scope);
    let mut facts = invoice.finish(source_metadata)?;
    facts.admit_original_concat_diagnostic(&diagnostic)?;
    work.flush()?;
    let mut reservation = charge.reserve_operation(facts.operation_peak_bytes())?;
    work.flush()?;
    let copied = match arrow_select::concat::concat(&refs) {
        Ok(copied) => copied,
        Err(error) => {
            // The original dynamic DataType renderer and ONE Display-to-String
            // conversion are covered BEFORE concat constructs its error.
            return Err(CopyOperationError::OriginalData(diagnostic.retain(
                error,
                charge,
                &mut reservation,
            )?));
        }
    };
    facts.bind_original_copy(&copied);
    drop(refs);
    work.flush()?;
    let retained = crate::arrow_result_custody::retain_copied_result_backing(
        copied,
        sources,
        charge,
        &mut reservation,
        allocator,
        &facts,
        &mut work,
    )?;
    work.finish()?;
    Ok(RetainedConcatResult {
        values: retained.values,
        facts,
        new_backing_envelope: retained.new_backing_envelope,
        new_buffer_stock: retained.new_buffer_stock,
    })
}
