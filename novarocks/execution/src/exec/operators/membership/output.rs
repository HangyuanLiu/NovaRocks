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

//! Result bits and output assembly of one probe chunk.
//!
//! The output carries every original probe column zero-copy, re-exposed
//! through buffers whose shared owner retains the probe chunk and its
//! accounting owner, and one new nullable Boolean whose bitmaps are task
//! allocations owned by their last Arrow buffer. Every Arrow management
//! allocation of the assembly is admitted, by a bound, before it is made.
//!
//! Charging boundary: these owners keep the exact charges above for as long
//! as any derived buffer lives. A later pipeline edge that moves this output
//! under Scheme S charges its buffers once more for that edge's holding
//! period; that conservative duplicate across chunks is the existing chunk
//! accounting, not skipped or reconciled here.

use std::ptr::NonNull;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, RecordBatch};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};

use super::shared::{admit, pair_error_text};
use crate::exec::chunk::{BYTES_ALLOCATION_BOUND, Chunk, ChunkSchemaRef, arc_allocation_bytes};
use crate::exec::expr::agg::{AggregateRetainedCharge, AggregateVec};
use crate::exec::expr::json_in_pair::{JsonPairTask, JsonPairTruth};

/// The nullable Boolean of one probe chunk, decided row by row. Both
/// bitmaps are reserved on the exact Task tracker before the first row.
pub(super) struct ResultBits {
    values: AggregateVec<u8>,
    validity: AggregateVec<u8>,
    rows: usize,
    nulls: usize,
}

impl ResultBits {
    pub(super) fn try_new(task: &JsonPairTask, rows: usize) -> Result<Self, String> {
        let bytes = rows.div_ceil(8);
        let bitmap = || -> Result<AggregateVec<u8>, String> {
            let mut bits = AggregateVec::new_in(task.allocator().clone());
            bits.try_reserve_exact(bytes)
                .map_err(|_| pair_error_text(&task.allocation_error()))?;
            bits.resize(bytes, 0);
            Ok(bits)
        };
        Ok(Self {
            values: bitmap()?,
            validity: bitmap()?,
            rows,
            nulls: 0,
        })
    }

    /// Records the decided membership of `row` with NOT applied once.
    pub(super) fn set(&mut self, row: usize, truth: JsonPairTruth, negated: bool) {
        let value = match truth {
            JsonPairTruth::True => !negated,
            JsonPairTruth::False => negated,
            JsonPairTruth::Unknown => {
                self.nulls += 1;
                return;
            }
        };
        let (byte, bit) = (row / 8, 1u8 << (row % 8));
        self.validity[byte] |= bit;
        if value {
            self.values[byte] |= bit;
        }
    }
}

/// One result bitmap allocation, owned by its last Arrow buffer, with the
/// admitted charge of the Arrow structures that reference it.
struct ResultBitmap {
    _bits: AggregateVec<u8>,
    _management: AggregateRetainedCharge,
}

fn bitmap_buffer(bits: AggregateVec<u8>, management: AggregateRetainedCharge) -> Buffer {
    let length = bits.len();
    let pointer = NonNull::new(bits.as_ptr() as *mut u8).expect("vector pointer is nonnull");
    let owner = Arc::new(ResultBitmap {
        _bits: bits,
        _management: management,
    });
    // SAFETY: the owner keeps this immutable, byte-aligned allocation alive
    // and unmodified for the buffer's whole lifetime.
    unsafe { Buffer::from_custom_allocation(pointer, length, owner) }
}

/// Builds the output of one fully decided probe chunk.
pub(super) fn assemble(
    task: &JsonPairTask,
    schema: &ChunkSchemaRef,
    probe: Chunk,
    bits: ResultBits,
) -> Result<Chunk, String> {
    let columns = probe.columns().len() + 1;
    // The pinned probe columns and the output column vector.
    let probe_management = probe
        .scoped_columns_bound::<AggregateRetainedCharge>()?
        .checked_add(columns * std::mem::size_of::<ArrayRef>())
        .ok_or("membership output management bound overflow")?;
    let probe_management = admit(task, probe_management, "membership output probe columns")?;
    // Each result bitmap: its owner and its Arc<Bytes>; the values bitmap
    // also carries the BooleanArray that references both.
    let bitmap_management = arc_allocation_bytes::<ResultBitmap>() + BYTES_ALLOCATION_BOUND;
    let values_management = admit(
        task,
        bitmap_management + arc_allocation_bytes::<BooleanArray>(),
        "membership output result",
    )?;
    let validity_management = if bits.nulls > 0 {
        Some(admit(
            task,
            bitmap_management,
            "membership output result validity",
        )?)
    } else {
        None
    };

    let mut output = Vec::with_capacity(columns);
    probe.into_scoped_columns(probe_management, &mut output);
    let values = BooleanBuffer::new(bitmap_buffer(bits.values, values_management), 0, bits.rows);
    let nulls = validity_management.map(|management| {
        let validity = BooleanBuffer::new(bitmap_buffer(bits.validity, management), 0, bits.rows);
        // SAFETY: `nulls` counts exactly the rows whose validity bit is unset.
        unsafe { NullBuffer::new_unchecked(validity, bits.nulls) }
    });
    output.push(Arc::new(BooleanArray::new(values, nulls)) as ArrayRef);
    let batch = RecordBatch::try_new(schema.arrow_schema_ref(), output)
        .map_err(|error| format!("membership output does not match its frozen layout: {error}"))?;
    Chunk::try_new_with_chunk_schema(batch, Arc::clone(schema))
}
