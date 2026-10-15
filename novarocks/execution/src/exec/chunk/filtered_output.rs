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

//! The direct ordered-scan filter's one unpublished output transaction.
//! Buffer identity measures new stock only. The original input and its actual
//! authority remain in the same paid backing through every returned alias.

use super::{
    AdditionalBufferLedger, Chunk, OutputColumnMetadata, record_batch_additional_bytes_observed,
    retain_column_capacity,
};
use crate::exec::arrow_work::{ArrowWorkBudget, ArrowWorkShape};
use crate::exec::pipeline::operator::InputConstructionOwner;
use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, RecordBatch, RecordBatchOptions,
};
use arrow::compute::filter_record_batch;
use novarocks_functions::opaque_memory::OpaqueRetainedCharge;
use novarocks_functions::{
    AggregateStateAllocator, EvaluationCheckpoints, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, PreparedSourceCustody, SourceBackingOwner, source_custody_prefix_facts,
};
use std::sync::Arc;

fn invalid(detail: &'static str) -> KernelFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(detail))
}

struct NormalizedMask {
    mask: BooleanArray,
    _stock: Option<OpaqueRetainedCharge>,
}

// Field order is the transaction's cleanup contract, including all refusal
// paths: destroy actual Arrow objects before their fees and public work.
struct FilterTransaction {
    raw: Option<RecordBatch>,
    raw_columns: Option<OpaqueRetainedCharge>,
    mask: Option<NormalizedMask>,
    input: Option<Chunk>,
    ledger: Option<AdditionalBufferLedger>,
    _work: ArrowWorkBudget,
}

struct FilteredBatchBacking {
    raw: RecordBatch,
    _raw_columns: OpaqueRetainedCharge,
    _input: Chunk,
    _ledger: AdditionalBufferLedger,
}

fn shape(input: &Chunk) -> Result<ArrowWorkShape, KernelFailure> {
    let rows = input.len();
    let fields = input.columns().len();
    let cells = rows
        .checked_mul(fields)
        .ok_or(KernelFailure::ResourceExhausted)?;
    let payload_bytes = input.columns().iter().try_fold(0usize, |bytes, column| {
        bytes
            .checked_add(column.get_array_memory_size())
            .ok_or(KernelFailure::ResourceExhausted)
    })?;
    // Every admitted flat Arrow carrier has at most three buffers, including
    // its validity buffer. This is a public operation policy, not private stock.
    let buffers = fields
        .checked_mul(3)
        .and_then(|n| n.checked_add(1))
        .ok_or(KernelFailure::ResourceExhausted)?;
    Ok(ArrowWorkShape {
        rows,
        cells,
        payload_bytes,
        fields,
        buffers,
    })
}

/// The existing TruthOnly normalization shared by the processor and source.
/// The controlled source supplies work observations; the original processor
/// supplies its original no-op policy. NULL is always not TRUE.
pub(crate) fn normalize_filter_mask_observed(
    truth: &BooleanArray,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<BooleanArray, KernelFailure> {
    if truth.null_count() == 0 {
        return Ok(truth.clone());
    }
    let mut builder = BooleanBuilder::with_capacity(truth.len());
    for value in truth {
        observe()?;
        builder.append_value(value == Some(true));
    }
    Ok(builder.finish())
}

impl Chunk {
    /// Only the closed direct-scan route calls this creator. The supplied
    /// construction owner is the same actual task's explicit typed author.
    /// NULL is not TRUE, exactly as in the existing synchronous filter body.
    pub(crate) fn filter_ordered_scan_controlled(
        self,
        truth: &BooleanArray,
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
        construction: &dyn InputConstructionOwner,
    ) -> ExecutionResult<Option<Self>> {
        if !self.chunk_schema().is_typed() || !self.source_authority().is_attached() {
            return Err(invalid(
                "ordered scan filtering requires its actual typed source authority",
            )
            .into());
        }
        if truth.len() != self.len() {
            return Err(invalid(
                "ordered scan filter mask differs from its actual input row count",
            )
            .into());
        }
        for slot in self.chunk_schema().slots() {
            let ty = novarocks_type_contract::FunctionValueType::try_from_typed_binding(
                slot.typed_binding().clone(),
            )
            .map_err(|_| invalid("ordered scan filter has an invalid complete payload type"))?;
            if !novarocks_local_program::is_ordered_filter_flat_payload_type(&ty) {
                return Err(invalid(
                    "ordered scan filter does not support this complete payload type",
                )
                .into());
            }
        }
        let authority = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
        let _prefix =
            authority.reserve_operation(source_custody_prefix_facts().requests().bytes())?;
        let mut work = EvaluationCheckpoints::new(control);
        work.flush()?;
        let mut selected = 0usize;
        for value in truth {
            work.step()?;
            selected += usize::from(value == Some(true));
        }
        if selected == 0 {
            work.finish()?;
            return Ok(None);
        }
        if selected == truth.len() {
            work.finish()?;
            return Ok(Some(self));
        }
        let public_work = ArrowWorkBudget::admit(Arc::clone(&host), shape(&self)?)?;
        // Only mixed output needs these cold holders. Admit them before Arrow
        // creates any raw output, while preserving the direct shortcut above.
        let ledger = AdditionalBufferLedger::prepare(&self, Arc::clone(&host), control)?;
        let mut transaction = FilterTransaction {
            raw: None,
            raw_columns: None,
            mask: None,
            input: Some(self),
            ledger: Some(ledger),
            _work: public_work,
        };
        transaction.mask = Some(NormalizedMask {
            mask: normalize_filter_mask_observed(truth, || work.step())?,
            _stock: None,
        });
        if truth.null_count() != 0 {
            // Keep the new bitmap in the transaction before its stock can be
            // refused. Its actual free then precedes the public work release.
            let mask = transaction.mask.as_mut().expect("created normalized mask");
            let mut stock = OpaqueRetainedCharge::try_new(Arc::clone(&host))?;
            let bytes = mask.mask.values().inner().capacity();
            let mut reservation = stock.reserve_operation(bytes)?;
            stock.reconcile_under_reservation(bytes, &mut reservation)?;
            drop(reservation);
            mask._stock = Some(stock);
        }
        work.flush()?;
        let input = transaction.input.as_ref().expect("unpublished input");
        transaction.raw = Some(
            filter_record_batch(
                input.record_batch(),
                &transaction.mask.as_ref().expect("normalized mask").mask,
            )
            .map_err(|error| ExecutionFailure::from(format!("compiled Filter: {error}")))?,
        );
        work.flush()?;
        let delta = record_batch_additional_bytes_observed(
            transaction.raw.as_ref().expect("filtered raw output"),
            input.record_batch(),
            || work.step(),
        )?;
        transaction
            .ledger
            .as_ref()
            .expect("unpublished ledger")
            .initialize(delta)?;
        // Arrow owns this actual Vec. Preserve its moved capacity independently
        // of the final wrapped column container's original constructor.
        let (schema, columns, rows) = transaction
            .raw
            .take()
            .expect("filtered raw output")
            .into_parts();
        transaction.raw_columns = Some(retain_column_capacity(
            columns.capacity(),
            Arc::clone(&host),
        )?);
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        transaction.raw = Some(
            RecordBatch::try_new_with_options(schema, columns, &options)
                .map_err(|error| ExecutionFailure::from(format!("compiled Filter: {error}")))?,
        );
        let schema = transaction
            .input
            .as_ref()
            .expect("unpublished input")
            .chunk_schema_ref();
        let ledger = transaction
            .ledger
            .as_ref()
            .expect("unpublished ledger")
            .clone();
        let backing = SourceBackingOwner::try_from_owned_direct(
            FilteredBatchBacking {
                raw: transaction.raw.take().expect("filtered raw output"),
                _raw_columns: transaction.raw_columns.take().expect("raw column capacity"),
                _input: transaction.input.take().expect("unpublished input"),
                _ledger: transaction.ledger.take().expect("unpublished ledger"),
            },
            Arc::clone(&host),
            &mut work,
        )?;
        let metadata = OutputColumnMetadata::try_new(
            backing.borrowed().raw.num_columns(),
            Arc::clone(&host),
            control,
        )?;
        // This local guard destroys partially wrapped columns before their
        // already admitted container keeper on every return path.
        struct WrappedColumns {
            columns: Vec<ArrayRef>,
            metadata: OutputColumnMetadata,
        }
        let mut wrapped = WrappedColumns {
            columns: Vec::new(),
            metadata,
        };
        wrapped
            .columns
            .try_reserve_exact(backing.borrowed().raw.num_columns())
            .map_err(|_| KernelFailure::ResourceExhausted)?;
        if wrapped.columns.capacity() != backing.borrowed().raw.num_columns() {
            return Err(invalid(
                "ordered scan wrapped column capacity differs from its admitted request",
            )
            .into());
        }
        for original in backing.borrowed().raw.columns() {
            let prepared = PreparedSourceCustody::prepare(original, &mut work)?;
            let mut reservation =
                authority.reserve_operation(prepared.request_facts().global_peak_upper())?;
            wrapped
                .columns
                .push(prepared.retain(&backing, &mut reservation, &mut work)?);
        }
        let batch =
            RecordBatch::try_new_with_options(schema.arrow_schema_ref(), wrapped.columns, &options)
                .map_err(|error| ExecutionFailure::from(format!("compiled Filter: {error}")))?;
        let mut output = Self::try_new_with_output_column_metadata(
            batch,
            schema,
            wrapped.metadata,
            construction,
        )?;
        output.attach_additional_buffers(ledger)?;
        work.finish()?;
        Ok(Some(output))
    }
}

#[cfg(test)]
#[path = "filtered_output_tests.rs"]
mod tests;
