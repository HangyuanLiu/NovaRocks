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

//! Structural requests of the locked public flat RecordBatch reader followed
//! by the constant owner. Summing cumulative requests also bounds coexistence;
//! it is not an allocator measurement or an Account/grant admission. Payload,
//! error formatting and opaque reader work have separate owners.

use super::{FlatConstantStream, FlatPoolResourceError, FlatPoolResourceProjection};
#[cfg(test)]
use crate::resource_source_model::locked_family;
use crate::resource_source_model::{LOCKED_FAMILY, LOCKED_TOOLCHAIN};
use crate::{
    ipc_flat_batch_v2::{Layout as FlatLayout, layout},
    physical_type_v2::TypeCodecError,
};
use arrow::{
    array::*,
    datatypes::{DataType, FieldRef, IntervalUnit, Schema, TimeUnit},
};
use arrow_buffer::{Buffer, MutableBuffer};
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::CompileCheckpoints;
use std::{alloc::Layout, mem, ptr::NonNull, sync::atomic::AtomicUsize};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReaderAllocationRequests {
    pub structural_request_bytes_upper_bound: usize,
    pub allocation_requests_upper_bound: usize,
}

fn invalid(message: &'static str) -> FlatPoolResourceError {
    TypeCodecError::InvalidShape(message).into()
}
fn add(left: usize, right: usize) -> Result<usize, FlatPoolResourceError> {
    left.checked_add(right)
        .ok_or_else(|| invalid("flat reader structural request overflow"))
}
fn mul(left: usize, right: usize) -> Result<usize, FlatPoolResourceError> {
    left.checked_mul(right)
        .ok_or_else(|| invalid("flat reader structural request overflow"))
}
fn array_layout<T>(count: usize) -> Result<Layout, FlatPoolResourceError> {
    Layout::array::<T>(count)
        .map_err(|_| invalid("flat reader container layout is not representable"))
}
pub(crate) fn arc_layout(payload: Layout) -> Result<Layout, FlatPoolResourceError> {
    // Rust 1.92 alloc/sync.rs: ArcInner is repr(C, align(2)), with exactly
    // strong/weak AtomicUsize counters followed by the actual payload.
    let counters = Layout::new::<[AtomicUsize; 2]>();
    let header = counters
        .align_to(counters.align().max(2))
        .map_err(|_| invalid("flat reader Arc header layout is not representable"))?
        .pad_to_align();
    header
        .extend(payload)
        .map(|(layout, _)| layout.pad_to_align())
        .map_err(|_| invalid("flat reader Arc backing layout is not representable"))
}

// These fields are the locked no-pool public MutableBuffer source layout, not
// a Bytes payload estimate. A unified pool feature adds a reservation Mutex;
// its hidden backend allocation is deliberately not admitted by this model.
#[allow(dead_code)]
struct NoPoolMutableBuffer {
    data: NonNull<u8>,
    len: usize,
    allocation: Layout,
}
#[allow(dead_code)]
struct NoPoolBytes {
    data: NonNull<u8>,
    len: usize,
    // Locked Deallocation's Standard(Layout)/Custom(Arc<dyn Allocation>,usize)
    // occupies three usize words. No external/custom allocation is constructed.
    deallocation: [usize; 3],
}
fn argument_layout<T>(_: fn(T) -> Buffer) -> Layout {
    Layout::new::<T>()
}
#[allow(deprecated)] // Infers the public opaque argument type without calling it.
fn bytes_layout() -> Layout {
    argument_layout(Buffer::from_bytes)
}

pub(crate) fn environment(
    work: &mut CompileCheckpoints<'_>,
) -> Result<Layout, FlatPoolResourceError> {
    let bytes = bytes_layout();
    let supported = LOCKED_FAMILY
        && LOCKED_TOOLCHAIN
        && arrow::ARROW_VERSION == "58.2.0"
        && cfg!(target_endian = "little")
        && Layout::new::<MutableBuffer>() == Layout::new::<NoPoolMutableBuffer>()
        && bytes == Layout::new::<NoPoolBytes>();
    work.step()?;
    if !supported {
        return Err(invalid(
            "flat reader allocation source or feature model changed",
        ));
    }
    // The checked-in toolchain contract is not runtime introspection of a
    // rustc +toolchain override. Build composition must enforce that contract.
    Ok(bytes)
}

#[derive(Default)]
pub(crate) struct Requests {
    pub(crate) bytes: usize,
    pub(crate) count: usize,
}
impl Requests {
    pub(crate) fn record(
        &mut self,
        layout: Layout,
        copies: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FlatPoolResourceError> {
        let result = (|| {
            self.bytes = add(self.bytes, mul(layout.size(), copies)?)?;
            self.count = add(self.count, copies)?;
            Ok(())
        })();
        work.step()?;
        result
    }
    pub(crate) fn exact_vec<T>(
        &mut self,
        count: usize,
        copies: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FlatPoolResourceError> {
        if count == 0 || mem::size_of::<T>() == 0 {
            work.step()?;
            return Ok(());
        }
        let layout = array_layout::<T>(count)?;
        self.record(layout, copies, work)
    }
    pub(crate) fn arc(
        &mut self,
        payload: Layout,
        copies: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FlatPoolResourceError> {
        self.record(arc_layout(payload)?, copies, work)
    }
    pub(crate) fn growing_vec<T>(
        &mut self,
        count: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FlatPoolResourceError> {
        if count == 0 || mem::size_of::<T>() == 0 {
            work.step()?;
            return Ok(());
        }
        // Locked RawVec minimum is 8 for bytes, 4 through 1024-byte elements,
        // otherwise 1. Generic collect can start above the minimum from a size
        // hint; subsequent requests double. Final capacity <= max(min,2*n),
        // and the sum of the geometric chain is <= twice that upper capacity.
        let minimum: usize = if mem::size_of::<T>() == 1 {
            8
        } else if mem::size_of::<T>() <= 1024 {
            4
        } else {
            1
        };
        let maximum = minimum.max(mul(count, 2)?);
        let mut capacity = minimum;
        let mut requests = 1;
        while capacity < count {
            capacity = mul(capacity, 2)?;
            requests = add(requests, 1)?;
            work.step()?;
        }
        let per_request = array_layout::<T>(maximum)?;
        let result = (|| {
            self.bytes = add(self.bytes, mul(per_request.size(), 2)?)?;
            self.count = add(self.count, requests)?;
            Ok(())
        })();
        work.step()?;
        result
    }
    pub(crate) fn view_to_data(
        &mut self,
        variadic: usize,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), FlatPoolResourceError> {
        // GenericByteViewArray::into ArrayData first to_vec(V), then insert(0).
        // The existing V-header Vec and its grow request can coexist.
        self.exact_vec::<Buffer>(variadic, 1, work)?;
        let grown = 4usize.max(mul(variadic, 2)?).max(add(variadic, 1)?);
        self.exact_vec::<Buffer>(grown, 1, work)
    }
}

pub(crate) fn concrete_array_layout(ty: &DataType) -> Result<Layout, FlatPoolResourceError> {
    // Exactly the locked make_array concrete object dispatch. Closedness is
    // decided by the shared raw geometry layout, not this allocation mapping.
    macro_rules! object {
        ($array:ty) => {
            Layout::new::<$array>()
        };
    }
    Ok(match ty {
        DataType::Struct(_) => object!(StructArray),
        DataType::List(_) => object!(ListArray),
        DataType::LargeList(_) => object!(LargeListArray),
        DataType::Map(_, _) => object!(MapArray),
        DataType::Null => object!(NullArray),
        DataType::Boolean => object!(BooleanArray),
        DataType::Int8 => object!(Int8Array),
        DataType::Int16 => object!(Int16Array),
        DataType::Int32 => object!(Int32Array),
        DataType::Int64 => object!(Int64Array),
        DataType::UInt8 => object!(UInt8Array),
        DataType::UInt16 => object!(UInt16Array),
        DataType::UInt32 => object!(UInt32Array),
        DataType::UInt64 => object!(UInt64Array),
        DataType::Float16 => object!(Float16Array),
        DataType::Float32 => object!(Float32Array),
        DataType::Float64 => object!(Float64Array),
        DataType::Decimal32(_, _) => object!(Decimal32Array),
        DataType::Decimal64(_, _) => object!(Decimal64Array),
        DataType::Decimal128(_, _) => object!(Decimal128Array),
        DataType::Decimal256(_, _) => object!(Decimal256Array),
        DataType::Date32 => object!(Date32Array),
        DataType::Date64 => object!(Date64Array),
        DataType::Time32(TimeUnit::Second) => object!(Time32SecondArray),
        DataType::Time32(TimeUnit::Millisecond) => object!(Time32MillisecondArray),
        DataType::Time64(TimeUnit::Microsecond) => object!(Time64MicrosecondArray),
        DataType::Time64(TimeUnit::Nanosecond) => object!(Time64NanosecondArray),
        DataType::Timestamp(TimeUnit::Second, _) => object!(TimestampSecondArray),
        DataType::Timestamp(TimeUnit::Millisecond, _) => object!(TimestampMillisecondArray),
        DataType::Timestamp(TimeUnit::Microsecond, _) => object!(TimestampMicrosecondArray),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => object!(TimestampNanosecondArray),
        DataType::Duration(TimeUnit::Second) => object!(DurationSecondArray),
        DataType::Duration(TimeUnit::Millisecond) => object!(DurationMillisecondArray),
        DataType::Duration(TimeUnit::Microsecond) => object!(DurationMicrosecondArray),
        DataType::Duration(TimeUnit::Nanosecond) => object!(DurationNanosecondArray),
        DataType::Interval(IntervalUnit::YearMonth) => object!(IntervalYearMonthArray),
        DataType::Interval(IntervalUnit::DayTime) => object!(IntervalDayTimeArray),
        DataType::Interval(IntervalUnit::MonthDayNano) => object!(IntervalMonthDayNanoArray),
        DataType::FixedSizeBinary(_) => object!(FixedSizeBinaryArray),
        DataType::Utf8 => object!(StringArray),
        DataType::LargeUtf8 => object!(LargeStringArray),
        DataType::Binary => object!(BinaryArray),
        DataType::LargeBinary => object!(LargeBinaryArray),
        DataType::Utf8View => object!(StringViewArray),
        DataType::BinaryView => object!(BinaryViewArray),
        _ => {
            return Err(invalid(
                "flat reader concrete array allocation is not covered",
            ));
        }
    })
}

/// Requires the pool projection computed for this same checked stream. This
/// private helper borrows the parent's original checkpoints; parent owns its
/// success/ordinary tail and primary refusal handling. It makes no allocations.
pub(super) fn preflight(
    stream: &FlatConstantStream<'_, '_>,
    pool: &FlatPoolResourceProjection,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ReaderAllocationRequests, FlatPoolResourceError> {
    let bytes = environment(work)?;
    let flat = layout(stream.field().data_type())?;
    work.step()?;
    let geometry = stream.geometry();
    let variadic = geometry.variadic_buffers;
    let mut requests = Requests::default();
    requests.arc(array_layout::<FieldRef>(1)?, 1, work)?;
    requests.arc(Layout::new::<Schema>(), 1, work)?;
    // Buffer::from_slice_ref always constructs a Bytes Arc, even at capacity 0.
    let owners = add(
        add(1, usize::from(pool.alignment_repair_possible))?,
        usize::from(pool.empty_offset_capacity_bytes != 0),
    )?;
    requests.arc(bytes, owners, work)?;
    requests.arc(concrete_array_layout(stream.field().data_type())?, 2, work)?;
    // read_record_batch's one-column Vec push reserves RawVec's minimum four.
    requests.exact_vec::<ArrayRef>(4, 1, work)?;
    match flat {
        FlatLayout::Null => {}
        FlatLayout::Views => {
            // Generic VecDeque<i64> collection of the one variadic count.
            requests.exact_vec::<i64>(4, 1, work)?;
            // Result collection of D read Buffer headers, then builder to_vec.
            requests.growing_vec::<Buffer>(geometry.buffer_descriptors, work)?;
            requests.exact_vec::<Buffer>(add(variadic, 1)?, 1, work)?;
            requests.arc(array_layout::<Buffer>(variadic)?, 2, work)?;
            requests.view_to_data(variadic, work)?;
            requests.view_to_data(variadic, work)?;
        }
        FlatLayout::Offsets(_) => {
            // Reader builder, input array.to_data and canonical array.to_data.
            requests.exact_vec::<Buffer>(2, 3, work)?;
        }
        FlatLayout::Bits | FlatLayout::Fixed(_) => {
            // Reader .add_buffer push, then two exact vec![Buffer] projections.
            requests.exact_vec::<Buffer>(4, 1, work)?;
            requests.exact_vec::<Buffer>(1, 2, work)?;
        }
    }
    // ArrayData::align_buffers plus four possible validate_data calls: reader
    // build, decoded to_data, pool validate_full and canonical to_data. The
    // latter two to_data validations can be enabled by force_validate. They
    // allocate the same scratch, without introducing another alignment copy.
    let specs = match flat {
        FlatLayout::Null => 0,
        FlatLayout::Offsets(_) => 2,
        _ => 1,
    };
    requests.exact_vec::<BufferSpec>(specs, 5, work)?;
    // Include the completed shape prefix cumulatively: two schema walks, one
    // batch walk, one pool bridge and one post-array validate_type. Each runs
    // the same flat walker vec![(&DataType,1)] with no children. Prefix
    // admission and its actual lifetime remain separately owned by the host.
    requests.exact_vec::<(&DataType, usize)>(1, 5, work)?;
    if !matches!(flat, FlatLayout::Null) {
        // Sole semantic validator's exact private frame layout, no enum mirror.
        // Flat Range processing pop/push retains capacity one, even for N=0.
        requests.record(ConstantPool::flat_value_validation_stack_layout(), 1, work)?;
    }
    requests.record(ConstantPool::backing_allocation_layout(), 1, work)?;
    Ok(ReaderAllocationRequests {
        structural_request_bytes_upper_bound: requests.bytes,
        allocation_requests_upper_bound: requests.count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
    use std::sync::Mutex;

    struct Control(Mutex<Vec<u32>>);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.0.lock().unwrap().push(units);
            Ok(())
        }
    }
    #[test]
    fn exact_opaque_bytes_and_pinned_arc_requests_have_no_pool_feature() {
        let control = Control(Mutex::new(Vec::new()));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let bytes = environment(&mut work).unwrap();
        assert_eq!(bytes, bytes_layout());
        assert_eq!(
            Layout::new::<MutableBuffer>(),
            Layout::new::<NoPoolMutableBuffer>()
        );
        for payload in [Layout::new::<u8>(), Layout::new::<Schema>(), bytes] {
            let request = arc_layout(payload).unwrap();
            assert_eq!(request, request.pad_to_align());
            assert!(request.size() >= payload.size() + 2 * mem::size_of::<AtomicUsize>());
            assert!(request.align() >= payload.align());
        }
        assert!(array_layout::<Buffer>(usize::MAX).is_err());
        let huge = Layout::from_size_align(isize::MAX as usize, 1).unwrap();
        assert!(arc_layout(huge).is_err());
        work.finish().unwrap();
    }
    #[test]
    fn view_header_growth_counts_both_old_and_new_requested_layouts() {
        let control = Control(Mutex::new(Vec::new()));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        for count in [0usize, 1, 2, 4, 257] {
            let mut requests = Requests::default();
            requests.view_to_data(count, &mut work).unwrap();
            let capacity = 4usize.max(count * 2).max(count + 1);
            assert_eq!(
                requests.bytes,
                (count + capacity) * mem::size_of::<Buffer>()
            );
            assert_eq!(requests.count, if count == 0 { 1 } else { 2 });
        }
        work.finish().unwrap();
    }
    #[test]
    fn concrete_flat_object_requests_follow_the_actual_make_array_classes() {
        for (ty, expected) in [
            (DataType::Null, Layout::new::<NullArray>()),
            (DataType::Boolean, Layout::new::<BooleanArray>()),
            (DataType::Int64, Layout::new::<Int64Array>()),
            (
                DataType::Decimal256(76, 0),
                Layout::new::<Decimal256Array>(),
            ),
            (DataType::Utf8, Layout::new::<StringArray>()),
            (DataType::BinaryView, Layout::new::<BinaryViewArray>()),
        ] {
            assert_eq!(concrete_array_layout(&ty).unwrap(), expected);
        }
        let lock = include_str!("../../../../Cargo.lock");
        assert!(locked_family(lock.as_bytes()));
        let changed = lock.replacen(
            "name = \"arrow-array\"\nversion = \"58.2.0\"",
            "name = \"arrow-array\"\nversion = \"58.3.0\"",
            1,
        );
        assert!(!locked_family(changed.as_bytes()));
        let duplicated = format!("{lock}\nname = \"arrow-array\"\nversion = \"58.2.0\"\n");
        assert!(!locked_family(duplicated.as_bytes()));
        let bigint_changed = lock.replacen(
            "name = \"num-bigint\"\nversion = \"0.4.6\"",
            "name = \"num-bigint\"\nversion = \"0.4.7\"",
            1,
        );
        assert!(!locked_family(bigint_changed.as_bytes()));
        let bigint_duplicated = format!("{lock}\nname = \"num-bigint\"\nversion = \"0.4.6\"\n");
        assert!(!locked_family(bigint_duplicated.as_bytes()));
    }
    #[test]
    fn actual_flat_stream_families_supply_checked_structural_requests() {
        use crate::{
            ipc_flat_batch_v2::FlatBatchProjectionLimits,
            ipc_flat_stream_v2::{FlatStreamProjectionLimits, preflight_flat_constant_stream},
            ipc_schema_v2::IpcSchemaProjectionLimits,
        };
        use arrow::{datatypes::Field, ipc::writer::StreamWriter, record_batch::RecordBatch};
        use novarocks_arrow_ipc_frame::VerifierOptions;
        use novarocks_constant_contract::ConstantPolicy;
        use novarocks_type_contract::FunctionValueType;
        use std::sync::Arc;

        let bounds = FlatStreamProjectionLimits {
            max_input_bytes: 65536,
            schema: IpcSchemaProjectionLimits {
                max_field_occurrences: 1,
                max_type_occurrences: 1,
                max_string_bytes: 1024,
                max_flatbuffer_bytes: 16384,
            },
            batch: FlatBatchProjectionLimits {
                max_metadata_bytes: 16384,
                max_body_bytes: 16384,
                max_rows: 16,
                max_buffer_descriptors: 16,
                max_view_validation_bytes: 1024,
            },
        };
        let verifier = VerifierOptions {
            max_depth: 67,
            max_tables: 64,
            max_apparent_size: 65536,
            ignore_missing_null_terminator: false,
        };
        let policy = ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 1,
            max_logical_elements: 16,
            max_retained_buffer_bytes: 65536,
            max_type_depth: 64,
            max_type_nodes: 1,
            max_dictionary_depth: 0,
            max_metadata_bytes: 1024,
            max_library_validation_work: 65536,
            max_library_validation_bytes: 65536,
        };
        let mut by_family = Vec::new();
        for array in [
            Arc::new(NullArray::new(3)) as ArrayRef,
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(Int64Array::from(vec![Some(71), None, Some(-71)])),
            Arc::new(StringArray::from(vec![Some("first"), None, Some("last")])),
            Arc::new(StringViewArray::from(vec![
                Some("a long view payload"),
                None,
                Some("a long view payload"),
            ])),
        ] {
            let field = Arc::new(Field::new("source", array.data_type().clone(), true));
            let ty = FunctionValueType::new(array.data_type().clone(), true);
            let schema = Arc::new(Schema::new([Arc::clone(&field)]));
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
            let mut input = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut input, &schema).unwrap();
                writer.write(&batch).unwrap();
                writer.finish().unwrap();
            }
            let control = Control(Mutex::new(Vec::new()));
            let stream =
                preflight_flat_constant_stream(&input, &field, bounds, &verifier, &control)
                    .unwrap();
            let pool = stream
                .preflight_pool_resources(&ty, policy, &control)
                .unwrap();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let requests = preflight(&stream, &pool, &mut work).unwrap();
            work.finish().unwrap();
            assert!(std::ptr::eq(stream.field(), field.as_ref()));
            assert!(
                requests.structural_request_bytes_upper_bound
                    >= arc_layout(Layout::new::<Schema>()).unwrap().size()
                        + ConstantPool::backing_allocation_layout().size()
            );
            assert!(requests.allocation_requests_upper_bound >= 11);
            assert!(control.0.lock().unwrap().iter().all(|&units| units <= 256));
            by_family.push(requests);
        }
        assert!(
            by_family[1].structural_request_bytes_upper_bound
                > by_family[0].structural_request_bytes_upper_bound
        );
        assert!(
            by_family[4].allocation_requests_upper_bound
                > by_family[3].allocation_requests_upper_bound
        );
    }
}
