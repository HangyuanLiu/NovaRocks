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
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayData, RecordBatch};
use arrow::buffer::Buffer;
use novarocks_spi::connector::ConnectorOutputMemoryToken;

use crate::runtime::mem_tracker::MemTracker;

/// Estimate RecordBatch size by summing unique buffers inside the batch.
///
/// NOTE: Scheme S (per-batch accounting).
/// We de-duplicate buffers only within a single RecordBatch.
/// Shared buffers across batches (e.g., slices/dictionaries) will be double-counted.
/// TODO: Upgrade to Scheme R with global buffer refcount to avoid cross-batch double counting.
pub fn record_batch_bytes(batch: &RecordBatch) -> usize {
    let mut seen = HashSet::new();
    let mut total = 0usize;
    for column in batch.columns() {
        total = total.saturating_add(array_data_bytes(&column.to_data(), &mut seen));
    }
    total
}

/// Returns bytes retained by `batch` whose Arrow buffers are not already
/// retained by `owner`. This is used when a zero-copy projection transfers the
/// owner's accounting lease together with the projected batch.
pub(crate) fn record_batch_additional_bytes(batch: &RecordBatch, owner: &RecordBatch) -> usize {
    let mut seen = HashSet::new();
    for column in owner.columns() {
        collect_array_buffers(&column.to_data(), &mut seen);
    }
    let mut total = 0usize;
    for column in batch.columns() {
        total = total.saturating_add(array_data_bytes(&column.to_data(), &mut seen));
    }
    total
}

/// Returns the unique Arrow buffer capacity in `owner` that remains reachable
/// from `batch`.
///
/// Provider pages use a conservative whole-array estimate while they own the
/// source. At the writer boundary, ownership changes to the queue's Scheme S
/// buffer accounting. Intersect allocations rather than columns because a
/// cast can retain only a null or child buffer while replacing its values.
pub(crate) fn record_batch_shared_owner_bytes(batch: &RecordBatch, owner: &RecordBatch) -> usize {
    let mut retained_buffers = HashSet::new();
    for column in batch.columns() {
        collect_array_buffers(&column.to_data(), &mut retained_buffers);
    }
    let mut counted = HashSet::new();
    owner.columns().iter().fold(0usize, |total, column| {
        total.saturating_add(array_data_shared_bytes(
            &column.to_data(),
            &retained_buffers,
            &mut counted,
        ))
    })
}

#[derive(Clone, Debug)]
pub(crate) struct ChunkMemoryLease {
    owner: ChunkMemoryLeaseOwner,
}

#[derive(Clone, Debug)]
enum ChunkMemoryLeaseOwner {
    Native(Arc<ChunkAccounting>),
    Connector(Arc<Mutex<ConnectorOutputMemoryToken>>),
}

impl ChunkMemoryLease {
    pub(super) fn native(accounting: Arc<ChunkAccounting>) -> Self {
        Self {
            owner: ChunkMemoryLeaseOwner::Native(accounting),
        }
    }

    pub(super) fn connector(output_memory: Arc<Mutex<ConnectorOutputMemoryToken>>) -> Self {
        Self {
            owner: ChunkMemoryLeaseOwner::Connector(output_memory),
        }
    }

    /// Splits an exact byte charge out of an exclusively owned source chunk.
    /// The returned guard becomes the queue's accounting owner; dropping this
    /// lease releases only the unprojected remainder.
    pub(crate) fn try_split_to(
        &self,
        tracker: Option<&Arc<MemTracker>>,
        bytes: usize,
        connector_retained_bytes: usize,
    ) -> Result<Option<TransferredChunkBytes>, String> {
        match &self.owner {
            ChunkMemoryLeaseOwner::Native(accounting) => {
                let Some(tracker) = tracker else {
                    return Ok(None);
                };
                if Arc::strong_count(accounting) != 1 {
                    return Ok(None);
                }
                let bytes = i64::try_from(bytes)
                    .map_err(|_| "chunk accounting split exceeds i64 range".to_string())?;
                if bytes == 0 {
                    return Ok(Some(TransferredChunkBytes::empty(Arc::clone(tracker))));
                }
                let mut state = accounting
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if bytes > state.bytes {
                    return Err(format!(
                        "chunk accounting split of {bytes} bytes exceeds source charge of {} bytes",
                        state.bytes
                    ));
                }
                MemTracker::try_transfer_charge(&state.tracker, tracker, bytes)?;
                state.bytes -= bytes;
                Ok(Some(TransferredChunkBytes::native(
                    bytes,
                    Arc::clone(tracker),
                )))
            }
            ChunkMemoryLeaseOwner::Connector(output_memory) => {
                // A connector reservation may move only when this chunk is its
                // sole owner. Shared clones keep the reservation where it is
                // and let the writer use the existing per-batch fallback.
                if Arc::strong_count(output_memory) != 1 {
                    return Ok(None);
                }
                let shared_bytes = u64::try_from(connector_retained_bytes).map_err(|_| {
                    "connector output accounting split exceeds u64 range".to_string()
                })?;
                let reserved_bytes = output_memory
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .bytes();
                if shared_bytes > reserved_bytes {
                    return Err(format!(
                        "connector output accounting transfer of {shared_bytes} bytes exceeds source reservation of {reserved_bytes} bytes"
                    ));
                }
                // Keep the complete reservation: Arrow projections can share
                // buffers whose allocation exceeds the projected logical
                // prefix. Moving the one owner is exact and avoids both a
                // release/recharge gap and a second query-hierarchy charge.
                Ok(Some(TransferredChunkBytes::connector(
                    Arc::clone(output_memory),
                    shared_bytes,
                )))
            }
        }
    }

    pub(crate) fn tracker(&self) -> Option<Arc<MemTracker>> {
        match &self.owner {
            ChunkMemoryLeaseOwner::Native(accounting) => Some(accounting.tracker()),
            ChunkMemoryLeaseOwner::Connector(_) => None,
        }
    }
}

#[derive(Debug)]
pub(crate) struct TransferredChunkBytes {
    owner: TransferredChunkBytesOwner,
}

#[derive(Debug)]
enum TransferredChunkBytesOwner {
    Native {
        bytes: i64,
        tracker: Arc<MemTracker>,
    },
    Connector {
        _output_memory: Arc<Mutex<ConnectorOutputMemoryToken>>,
        retained_bytes: u64,
    },
}

impl TransferredChunkBytes {
    fn native(bytes: i64, tracker: Arc<MemTracker>) -> Self {
        Self {
            owner: TransferredChunkBytesOwner::Native { bytes, tracker },
        }
    }

    fn connector(
        output_memory: Arc<Mutex<ConnectorOutputMemoryToken>>,
        retained_bytes: u64,
    ) -> Self {
        Self {
            owner: TransferredChunkBytesOwner::Connector {
                _output_memory: output_memory,
                retained_bytes,
            },
        }
    }

    fn empty(tracker: Arc<MemTracker>) -> Self {
        Self::native(0, tracker)
    }

    /// Finalize a connector projection after the source `Chunk` has been
    /// dropped and any newly allocated buffers have been charged.
    pub(crate) fn release_unshared_connector_bytes(&mut self) -> Result<(), String> {
        let TransferredChunkBytesOwner::Connector {
            _output_memory,
            retained_bytes,
        } = &self.owner
        else {
            return Ok(());
        };
        _output_memory
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .shrink_to(*retained_bytes)
            .map_err(|error| {
                format!("shrink connector output accounting after projection: {error}")
            })
    }
}

impl Drop for TransferredChunkBytes {
    fn drop(&mut self) {
        if let TransferredChunkBytesOwner::Native { bytes, tracker } = &self.owner {
            tracker.release(*bytes);
        }
    }
}

#[derive(Debug)]
pub(super) struct ChunkAccounting {
    state: Mutex<ChunkAccountingState>,
}

#[derive(Debug)]
struct ChunkAccountingState {
    bytes: i64,
    tracker: Arc<MemTracker>,
}

impl ChunkAccounting {
    pub(super) fn new(bytes: i64, tracker: &Arc<MemTracker>) -> Self {
        tracker.consume(bytes);
        Self::from_charged(bytes, tracker)
    }

    pub(super) fn from_charged(bytes: i64, tracker: &Arc<MemTracker>) -> Self {
        Self {
            state: Mutex::new(ChunkAccountingState {
                bytes,
                tracker: Arc::clone(tracker),
            }),
        }
    }

    pub(super) fn transfer_to(&self, tracker: &Arc<MemTracker>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::ptr_eq(&state.tracker, tracker) {
            return;
        }
        state.tracker.release(state.bytes);
        tracker.consume(state.bytes);
        state.tracker = Arc::clone(tracker);
    }

    pub(super) fn try_transfer_to(&self, tracker: &Arc<MemTracker>) -> Result<(), String> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if Arc::ptr_eq(&state.tracker, tracker) {
            return Ok(());
        }
        MemTracker::try_transfer_charge(&state.tracker, tracker, state.bytes)?;
        state.tracker = Arc::clone(tracker);
        Ok(())
    }

    pub(super) fn tracker(&self) -> Arc<MemTracker> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(&state.tracker)
    }
}

impl Drop for ChunkAccounting {
    fn drop(&mut self) {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.tracker.release(state.bytes);
    }
}

pub(super) fn chunk_bytes_i64(batch: &RecordBatch) -> i64 {
    i64::try_from(record_batch_bytes(batch)).unwrap_or(i64::MAX)
}

fn array_data_bytes(data: &ArrayData, seen: &mut HashSet<usize>) -> usize {
    let mut total = 0usize;
    for buffer in data.buffers() {
        total = total.saturating_add(buffer_bytes(buffer, seen));
    }
    if let Some(nulls) = data.nulls() {
        total = total.saturating_add(buffer_bytes(nulls.buffer(), seen));
    }
    for child in data.child_data() {
        total = total.saturating_add(array_data_bytes(child, seen));
    }
    total
}

fn collect_array_buffers(data: &ArrayData, seen: &mut HashSet<usize>) {
    for buffer in data.buffers() {
        seen.insert(buffer.data_ptr().as_ptr() as usize);
    }
    if let Some(nulls) = data.nulls() {
        seen.insert(nulls.buffer().data_ptr().as_ptr() as usize);
    }
    for child in data.child_data() {
        collect_array_buffers(child, seen);
    }
}

fn array_data_shared_bytes(
    data: &ArrayData,
    retained: &HashSet<usize>,
    counted: &mut HashSet<usize>,
) -> usize {
    let mut total = 0usize;
    for buffer in data.buffers() {
        if retained.contains(&(buffer.data_ptr().as_ptr() as usize)) {
            total = total.saturating_add(buffer_bytes(buffer, counted));
        }
    }
    if let Some(nulls) = data.nulls()
        && retained.contains(&(nulls.buffer().data_ptr().as_ptr() as usize))
    {
        total = total.saturating_add(buffer_bytes(nulls.buffer(), counted));
    }
    for child in data.child_data() {
        total = total.saturating_add(array_data_shared_bytes(child, retained, counted));
    }
    total
}

fn buffer_bytes(buffer: &Buffer, seen: &mut HashSet<usize>) -> usize {
    let ptr = buffer.data_ptr().as_ptr() as usize;
    if !seen.insert(ptr) {
        return 0;
    }
    buffer.capacity().max(buffer.len())
}

// ---------------------------------------------------------------------------
// Scoped buffer ownership for one derived output path.
//
// A derived output may expose a source chunk's Arrow buffers zero-copy. Each
// such buffer is re-exposed through a custom allocation whose shared owner
// retains the source chunk, and with it the chunk's accounting owner, until
// the last derived buffer drops: an ArrayRef or slice kept after the output
// chunk is gone keeps the charge. Nothing here skips or changes the charge of
// any other chunk owner.
// ---------------------------------------------------------------------------

/// Exact allocation of a std `Arc<T>`: `ArcInner` is `#[repr(C)]` with the
/// strong and weak counters ahead of the value.
pub(crate) fn arc_allocation_bytes<T>() -> usize {
    std::alloc::Layout::new::<[usize; 2]>()
        .extend(std::alloc::Layout::new::<T>())
        .map(|(layout, _)| layout.pad_to_align().size())
        .unwrap_or(usize::MAX)
}

/// Upper bound of one `Arc<Bytes>` made by `Buffer::from_custom_allocation`.
///
/// In arrow-buffer 58.2 `Bytes` holds a pointer, a length, a `Deallocation`
/// (at most an `Arc<dyn Allocation>` plus a length and its tag) and, with the
/// `pool` feature, a `Mutex<Option<Box<dyn MemoryReservation>>>`: at most
/// 8 + 8 + 32 + 32 bytes behind the 16-byte Arc header.
pub(crate) const BYTES_ALLOCATION_BOUND: usize = 128;

const fn max_of(sizes: &[usize]) -> usize {
    let mut max = 0;
    let mut index = 0;
    while index < sizes.len() {
        if sizes[index] > max {
            max = sizes[index];
        }
        index += 1;
    }
    max
}

/// The largest concrete Arrow array struct `make_array` can allocate for a
/// supported data type; `Arc<dyn Array>` adds the 16-byte header.
const ARRAY_STRUCT_BOUND: usize = {
    use arrow::array::*;
    use arrow::datatypes::*;
    use std::mem::size_of;
    max_of(&[
        size_of::<Int64Array>(),
        size_of::<Decimal128Array>(),
        size_of::<Decimal256Array>(),
        size_of::<IntervalMonthDayNanoArray>(),
        size_of::<TimestampMicrosecondArray>(),
        size_of::<BooleanArray>(),
        size_of::<NullArray>(),
        size_of::<StringArray>(),
        size_of::<LargeStringArray>(),
        size_of::<BinaryArray>(),
        size_of::<LargeBinaryArray>(),
        size_of::<FixedSizeBinaryArray>(),
        size_of::<StringViewArray>(),
        size_of::<BinaryViewArray>(),
        size_of::<ListArray>(),
        size_of::<LargeListArray>(),
        size_of::<FixedSizeListArray>(),
        size_of::<StructArray>(),
        size_of::<MapArray>(),
        size_of::<DictionaryArray<Int8Type>>(),
        size_of::<DictionaryArray<Int64Type>>(),
        size_of::<DictionaryArray<UInt64Type>>(),
    ])
};

const ARRAY_ALLOCATION_BOUND: usize = 16 + ((ARRAY_STRUCT_BOUND + 15) & !15);

/// Bytes `DataType::clone` allocates: only dictionary types box their key
/// and value types; every other nested type shares an `Arc`.
fn data_type_clone_bytes(data_type: &arrow::datatypes::DataType) -> usize {
    match data_type {
        arrow::datatypes::DataType::Dictionary(key, value) => {
            2 * std::mem::size_of::<arrow::datatypes::DataType>()
                + data_type_clone_bytes(key)
                + data_type_clone_bytes(value)
        }
        _ => 0,
    }
}

/// Upper bound of the Arrow management allocations made while re-exposing
/// `array` through scoped buffers: `to_data`, the rebuilt `ArrayData`, one
/// `Arc<Bytes>` per buffer and validity bitmap, and `make_array`, including
/// the child copies a sliced struct ancestor makes. The buffer counts follow
/// the Arrow physical layout of each type; types whose conversion is not
/// bounded here are refused rather than guessed.
fn scoped_array_bound(array: &dyn Array, struct_depth: usize) -> Result<usize, String> {
    use arrow::array::{
        BinaryViewArray, DictionaryArray, FixedSizeListArray, LargeListArray, ListArray, MapArray,
        StringViewArray, StructArray,
    };
    use arrow::datatypes::{DataType, Int8Type, Int16Type, Int32Type, Int64Type};
    use arrow::datatypes::{UInt8Type, UInt16Type, UInt32Type, UInt64Type};
    use std::mem::size_of;

    fn downcast<'a, T: 'static>(array: &'a dyn Array, what: &str) -> Result<&'a T, String> {
        array
            .as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| format!("scoped output expected a {what} array"))
    }

    // Computing the bound must not allocate: it runs before the bound is
    // admitted. Children are visited in place.
    let data_type = array.data_type();
    let child_depth = struct_depth + usize::from(matches!(data_type, DataType::Struct(_)));
    let mut children = 0usize;
    let mut children_bound = 0usize;
    let mut child = |value: &dyn Array| -> Result<(), String> {
        children += 1;
        children_bound = children_bound
            .checked_add(scoped_array_bound(value, child_depth)?)
            .ok_or_else(|| "scoped output management bound overflow".to_string())?;
        Ok(())
    };
    let mut view_buffers = 0usize;
    let buffers = match data_type {
        DataType::Null => 0,
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::FixedSizeBinary(_) => 1,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => 2,
        DataType::Utf8View => {
            view_buffers = downcast::<StringViewArray>(array, "string view")?
                .data_buffers()
                .len();
            1 + view_buffers
        }
        DataType::BinaryView => {
            view_buffers = downcast::<BinaryViewArray>(array, "binary view")?
                .data_buffers()
                .len();
            1 + view_buffers
        }
        DataType::List(_) => {
            child(downcast::<ListArray>(array, "list")?.values().as_ref())?;
            1
        }
        DataType::LargeList(_) => {
            child(
                downcast::<LargeListArray>(array, "large list")?
                    .values()
                    .as_ref(),
            )?;
            1
        }
        DataType::FixedSizeList(_, _) => {
            child(
                downcast::<FixedSizeListArray>(array, "fixed-size list")?
                    .values()
                    .as_ref(),
            )?;
            0
        }
        DataType::Struct(_) => {
            for column in downcast::<StructArray>(array, "struct")?.columns() {
                child(column.as_ref())?;
            }
            0
        }
        DataType::Map(_, _) => {
            child(downcast::<MapArray>(array, "map")?.entries() as &dyn Array)?;
            1
        }
        DataType::Dictionary(key, _) => {
            macro_rules! values {
                ($key:ty) => {
                    downcast::<DictionaryArray<$key>>(array, "dictionary")?
                        .values()
                        .as_ref()
                };
            }
            child(match key.as_ref() {
                DataType::Int8 => values!(Int8Type),
                DataType::Int16 => values!(Int16Type),
                DataType::Int32 => values!(Int32Type),
                DataType::Int64 => values!(Int64Type),
                DataType::UInt8 => values!(UInt8Type),
                DataType::UInt16 => values!(UInt16Type),
                DataType::UInt32 => values!(UInt32Type),
                DataType::UInt64 => values!(UInt64Type),
                other => return Err(format!("scoped output cannot bound dictionary key {other}")),
            })?;
            1
        }
        other => {
            return Err(format!(
                "scoped output cannot bound the Arrow management graph of {other}"
            ));
        }
    };
    let nulls = usize::from(array.nulls().is_some());
    let vectors = buffers * size_of::<Buffer>()
        + children * size_of::<ArrayData>()
        + data_type_clone_bytes(data_type);
    let node = ARRAY_ALLOCATION_BOUND
        + (buffers + nulls) * BYTES_ALLOCATION_BOUND
        // to_data, the rebuilt tree, and one copy per slicing struct ancestor.
        + vectors * (2 + struct_depth)
        + children * size_of::<arrow::array::ArrayRef>()
        + if view_buffers > 0 {
            16 + view_buffers * size_of::<Buffer>()
        } else {
            0
        };
    node.checked_add(children_bound)
        .ok_or_else(|| "scoped output management bound overflow".to_string())
}

/// Upper bound of the management bytes `scoped_columns` allocates for
/// `batch` with an owner of type `O`.
pub(super) fn scoped_columns_bound<O>(batch: &RecordBatch) -> Result<usize, String> {
    let mut total = arc_allocation_bytes::<O>();
    for column in batch.columns() {
        total = total
            .checked_add(scoped_array_bound(column.as_ref(), 0)?)
            .ok_or("scoped output management bound overflow")?;
    }
    Ok(total)
}

fn scoped_buffer(buffer: &Buffer, owner: &Arc<dyn arrow::alloc::Allocation>) -> Buffer {
    let pointer = std::ptr::NonNull::new(buffer.as_ptr() as *mut u8)
        .expect("Arrow buffer pointers are never null");
    // SAFETY: `owner` retains the source chunk, whose arrays retain the
    // allocation behind `buffer`; Arrow buffers are immutable, and the new
    // buffer covers exactly the visible bytes of the original.
    unsafe { Buffer::from_custom_allocation(pointer, buffer.len(), Arc::clone(owner)) }
}

fn scoped_data(data: &ArrayData, owner: &Arc<dyn arrow::alloc::Allocation>) -> ArrayData {
    let mut buffers = Vec::with_capacity(data.buffers().len());
    for buffer in data.buffers() {
        buffers.push(scoped_buffer(buffer, owner));
    }
    let nulls = data.nulls().map(|nulls| {
        let bits = arrow::buffer::BooleanBuffer::new(
            scoped_buffer(nulls.buffer(), owner),
            nulls.offset(),
            nulls.len(),
        );
        // SAFETY: the same bits with the same validity count.
        unsafe { arrow::buffer::NullBuffer::new_unchecked(bits, nulls.null_count()) }
    });
    let mut children = Vec::with_capacity(data.child_data().len());
    for child in data.child_data() {
        children.push(scoped_data(child, owner));
    }
    let builder = arrow::array::ArrayDataBuilder::new(data.data_type().clone())
        .len(data.len())
        .offset(data.offset())
        .buffers(buffers)
        .child_data(children)
        .nulls(nulls);
    // SAFETY: an exact copy of already valid array data with every buffer
    // replaced by one exposing the same bytes.
    unsafe { builder.build_unchecked() }
}

/// Re-exposes every column of `batch`, recursively, through buffers owned by
/// `owner`, appending them to `columns`.
pub(super) fn scoped_columns(
    batch: &RecordBatch,
    owner: &Arc<dyn arrow::alloc::Allocation>,
    columns: &mut Vec<arrow::array::ArrayRef>,
) {
    for column in batch.columns() {
        columns.push(arrow::array::make_array(scoped_data(
            &column.to_data(),
            owner,
        )));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, Int64Array};

    #[test]
    fn additional_bytes_excludes_zero_copy_projection_buffers() {
        let left = Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let right = Arc::new(Int64Array::from(vec![4, 5, 6])) as Arc<dyn Array>;
        let owner =
            RecordBatch::try_from_iter(vec![("left", left.clone()), ("right", right)]).unwrap();
        let projection = RecordBatch::try_from_iter(vec![("left", left)]).unwrap();

        assert_eq!(record_batch_additional_bytes(&projection, &owner), 0);
    }

    #[test]
    fn additional_bytes_charges_new_projection_buffers() {
        let owner_values = Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let owner = RecordBatch::try_from_iter(vec![("value", owner_values)]).unwrap();
        let projected_values = Arc::new(Int64Array::from(vec![2, 4, 6])) as Arc<dyn Array>;
        let projection = RecordBatch::try_from_iter(vec![("value", projected_values)]).unwrap();

        assert_eq!(
            record_batch_additional_bytes(&projection, &owner),
            record_batch_bytes(&projection)
        );
    }

    #[test]
    fn shared_owner_bytes_keep_only_source_columns_reachable_from_projection() {
        let left = Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>;
        let right = Arc::new(Int64Array::from(vec![4, 5, 6])) as Arc<dyn Array>;
        let owner =
            RecordBatch::try_from_iter(vec![("left", left.clone()), ("right", right)]).unwrap();
        let projection = RecordBatch::try_from_iter(vec![("left", left.clone())]).unwrap();
        let materialized = RecordBatch::try_from_iter(vec![(
            "left",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as Arc<dyn Array>,
        )])
        .unwrap();

        assert_eq!(
            record_batch_shared_owner_bytes(&projection, &owner),
            record_batch_bytes(&projection)
        );
        assert_eq!(record_batch_shared_owner_bytes(&materialized, &owner), 0);
    }

    #[test]
    fn shared_owner_bytes_keep_only_a_reused_null_bitmap_after_cast() {
        let source_values = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let owner =
            RecordBatch::try_from_iter(vec![("value", source_values.clone() as Arc<dyn Array>)])
                .unwrap();
        let cast_values = Arc::new(Int64Array::new(
            vec![1_i64, 0, 3].into(),
            source_values.nulls().cloned(),
        )) as Arc<dyn Array>;
        let projection = RecordBatch::try_from_iter(vec![("value", cast_values)]).unwrap();
        let additional = record_batch_additional_bytes(&projection, &owner);
        let shared = record_batch_shared_owner_bytes(&projection, &owner);

        assert!(shared > 0, "the null bitmap must remain shared");
        assert_eq!(shared + additional, record_batch_bytes(&projection));
        assert!(shared < record_batch_bytes(&owner));
    }

    #[test]
    fn offset_slice_uses_the_same_underlying_buffer_allocation_identity() {
        let source_values = Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as Arc<dyn Array>;
        let owner =
            RecordBatch::try_from_iter(vec![("value", Arc::clone(&source_values))]).unwrap();
        let sliced = source_values.slice(1, 2);
        let projection = RecordBatch::try_from_iter(vec![("value", sliced)]).unwrap();

        assert_eq!(record_batch_additional_bytes(&projection, &owner), 0);
        assert_eq!(
            record_batch_shared_owner_bytes(&projection, &owner),
            record_batch_bytes(&projection)
        );
    }
}
