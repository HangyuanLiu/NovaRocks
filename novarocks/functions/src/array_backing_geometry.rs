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

//! Borrow actual immutable Arrow structure before its allocating to_data route.
//! These are table facts, not a value decoder, retained payload grant, SQL type
//! coercion, or a row-dependent string estimate.
use crate::KernelFailure;
use crate::kernel_control::invalid;
use crate::kernel_input::EvaluationCheckpoints;
use crate::source_custody_resources::{
    RequestSourceNode, RequestSummary, SourceCustodyRequestFacts, SourceNodeRole,
    SourceRequestPricing, summarize_source_node,
};
use arrow_array::{Array, MapArray};
use arrow_schema::DataType;
use std::alloc::Layout;

fn concrete<T: Array + 'static>(array: &dyn Array) -> Result<&T, KernelFailure> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        invalid("source backing geometry requires its actual built-in Arrow carrier")
    })
}
fn plus(a: usize, b: usize) -> Result<usize, KernelFailure> {
    a.checked_add(b).ok_or(KernelFailure::ResourceExhausted)
}

/// A borrowed node from the SAME source traversal. No ArrayData, payload,
/// metadata clone, or authorization is created by this loan.
pub(crate) struct BorrowedSourceNode<'a> {
    pub(crate) array: &'a dyn Array,
    pub(crate) data_buffers: usize,
    #[cfg(test)]
    pub(crate) child_count: usize,
    #[cfg(test)]
    pub(crate) copy_nulls: bool,
}
pub(crate) trait BorrowedSourceObservation<'source> {
    /// The observer owns any work or allocation it elects to perform. The
    /// source author adds neither a scope nor an extra success checkpoint.
    fn node(
        &mut self,
        node: BorrowedSourceNode<'source>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure>;
}

pub(crate) trait MetadataWork {
    type Error: From<KernelFailure>;
    fn step(&mut self) -> Result<(), Self::Error>;
}
impl MetadataWork for EvaluationCheckpoints<'_> {
    type Error = KernelFailure;
    fn step(&mut self) -> Result<(), Self::Error> {
        EvaluationCheckpoints::step(self)
    }
}
struct CreatorMetadataWork<'a, E>(&'a mut dyn FnMut(bool) -> Result<(), E>);
impl<E: From<KernelFailure>> MetadataWork for CreatorMetadataWork<'_, E> {
    type Error = E;
    fn step(&mut self) -> Result<(), E> {
        (self.0)(false)
    }
}

struct SourceGraphResult {
    legacy: Option<usize>,
    requests: Option<RequestSummary>,
    bufferless_root: Option<bool>,
}

fn actual_rebox_layout(array: &dyn Array) -> Result<Layout, KernelFailure> {
    use arrow_array::types::*;
    use arrow_array::*;
    use arrow_schema::{IntervalUnit, TimeUnit};
    let actual = array.as_any();
    macro_rules! primitive {
        ($ty:ty) => {
            actual.is::<PrimitiveArray<$ty>>()
        };
    }
    let valid = match array.data_type() {
        DataType::Null => actual.is::<NullArray>(),
        DataType::Boolean => actual.is::<BooleanArray>(),
        DataType::Int8 => primitive!(Int8Type),
        DataType::Int16 => primitive!(Int16Type),
        DataType::Int32 => primitive!(Int32Type),
        DataType::Int64 => primitive!(Int64Type),
        DataType::UInt8 => primitive!(UInt8Type),
        DataType::UInt16 => primitive!(UInt16Type),
        DataType::UInt32 => primitive!(UInt32Type),
        DataType::UInt64 => primitive!(UInt64Type),
        DataType::Float16 => primitive!(Float16Type),
        DataType::Float32 => primitive!(Float32Type),
        DataType::Float64 => primitive!(Float64Type),
        DataType::Date32 => primitive!(Date32Type),
        DataType::Date64 => primitive!(Date64Type),
        DataType::Time32(TimeUnit::Second) => primitive!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => primitive!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => primitive!(Time64MicrosecondType),
        DataType::Time64(TimeUnit::Nanosecond) => primitive!(Time64NanosecondType),
        DataType::Time32(_) | DataType::Time64(_) => false,
        DataType::Timestamp(TimeUnit::Second, _) => primitive!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => primitive!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => primitive!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => primitive!(TimestampNanosecondType),
        DataType::Duration(TimeUnit::Second) => primitive!(DurationSecondType),
        DataType::Duration(TimeUnit::Millisecond) => primitive!(DurationMillisecondType),
        DataType::Duration(TimeUnit::Microsecond) => primitive!(DurationMicrosecondType),
        DataType::Duration(TimeUnit::Nanosecond) => primitive!(DurationNanosecondType),
        DataType::Interval(IntervalUnit::YearMonth) => primitive!(IntervalYearMonthType),
        DataType::Interval(IntervalUnit::DayTime) => primitive!(IntervalDayTimeType),
        DataType::Interval(IntervalUnit::MonthDayNano) => primitive!(IntervalMonthDayNanoType),
        DataType::Decimal32(..) => primitive!(Decimal32Type),
        DataType::Decimal64(..) => primitive!(Decimal64Type),
        DataType::Decimal128(..) => primitive!(Decimal128Type),
        DataType::Decimal256(..) => primitive!(Decimal256Type),
        DataType::Utf8 => actual.is::<StringArray>(),
        DataType::LargeUtf8 => actual.is::<LargeStringArray>(),
        DataType::Binary => actual.is::<BinaryArray>(),
        DataType::LargeBinary => actual.is::<LargeBinaryArray>(),
        DataType::Utf8View => actual.is::<StringViewArray>(),
        DataType::BinaryView => actual.is::<BinaryViewArray>(),
        DataType::FixedSizeBinary(_) => actual.is::<FixedSizeBinaryArray>(),
        DataType::List(_) => actual.is::<ListArray>(),
        DataType::LargeList(_) => actual.is::<LargeListArray>(),
        DataType::ListView(_) => actual.is::<ListViewArray>(),
        DataType::LargeListView(_) => actual.is::<LargeListViewArray>(),
        DataType::FixedSizeList(..) => actual.is::<FixedSizeListArray>(),
        DataType::Struct(_) => actual.is::<StructArray>(),
        DataType::Map(..) => actual.is::<MapArray>(),
        // These branches already verify the precise key/index carrier in the
        // SAME child traversal below; as_any exposes its delegated header.
        DataType::Dictionary(..) | DataType::RunEndEncoded(..) => true,
        DataType::Union(..) => actual.is::<UnionArray>(),
    };
    if !valid {
        return Err(invalid(
            "source custody requires its actual built-in Arrow carrier",
        ));
    }
    Ok(Layout::for_value(actual))
}

/// Use ONE custody metadata arithmetic with borrowed actual table counts. The
/// returned bytes cover metadata construction only; existing input lease owners
/// retain the original payload independently and are never replaced by this sum.
fn source_metadata_graph<'source, W: MetadataWork>(
    array: &'source dyn Array,
    work: &mut W,
    copy_nulls: bool,
    observer: &mut impl FnMut(BorrowedSourceNode<'source>, &mut W) -> Result<(), W::Error>,
    pricing: Option<&SourceRequestPricing>,
    role: SourceNodeRole,
) -> Result<SourceGraphResult, W::Error> {
    work.step()?;
    // Do not run unused legacy envelope arithmetic on the request route.
    let mut child_bytes = pricing.is_none().then_some(0usize);
    let mut child_requests = pricing.map(|_| RequestSummary::empty());
    let mut descendant_has_custody_buffer = false;
    let body = if pricing.is_some() {
        Some(actual_rebox_layout(array)?)
    } else {
        None
    };
    let node = novarocks_value_arrays::storage::borrowed_node(array)
        .map_err(|source| W::Error::from(KernelFailure::from_domain(source)))?;
    let buffers = node.data_buffers();
    let children = node.child_count();
    macro_rules! accumulate {
        ($result:expr) => {{
            let result = $result;
            if let Some(bufferless) = result.bufferless_root {
                descendant_has_custody_buffer |= !bufferless;
            }
            if let Some(bytes) = &mut child_bytes {
                *bytes = plus(
                    *bytes,
                    result.legacy.expect("legacy source traversal amount"),
                )?;
            }
            if let Some(requests) = &mut child_requests {
                *requests =
                    requests.plus(result.requests.expect("source request traversal summary"))?;
            }
        }};
    }
    for index in 0..children {
        let child = novarocks_value_arrays::storage::borrowed_child(array, index)
            .map_err(|source| W::Error::from(KernelFailure::from_domain(source)))?;
        match child {
            novarocks_value_arrays::storage::BorrowedNode::Array { array: child, .. } => {
                accumulate!(source_metadata_graph(
                    child,
                    work,
                    copy_nulls,
                    observer,
                    pricing,
                    if matches!(array.data_type(), DataType::Map(..)) {
                        SourceNodeRole::MapEntries
                    } else {
                        SourceNodeRole::Rebox
                    }
                )?);
            }
            novarocks_value_arrays::storage::BorrowedNode::RunEnds { data_type, .. } => {
                // Borrow the real implicit primitive Buffer. The existing
                // numerical author still prices its single construction; no
                // temporary RunEndArray or second metadata authority is added.
                descendant_has_custody_buffer = true;
                if let Some(bytes) = &mut child_bytes {
                    *bytes = plus(
                        *bytes,
                        crate::arrow_result_custody::custody_node_metadata_upper_bound(
                            1, false, 0,
                        )?,
                    )?;
                }
                if let (Some(pricing), Some(requests)) = (pricing, child_requests.as_mut()) {
                    let ends = summarize_source_node(
                        RequestSourceNode {
                            data_type,
                            data_buffers: 1,
                            child_count: 0,
                            nulls: None,
                            header_fields: 0,
                            body: None,
                            role: SourceNodeRole::RunEnds,
                        },
                        RequestSummary::empty(),
                        pricing,
                        work,
                    )?;
                    *requests = requests.plus(ends)?;
                }
            }
        }
    }
    {
        observer(
            BorrowedSourceNode {
                array,
                data_buffers: buffers,
                #[cfg(test)]
                child_count: children,
                #[cfg(test)]
                copy_nulls,
            },
            work,
        )?;
    }
    let legacy = match child_bytes {
        Some(child_bytes) => {
            let own = crate::arrow_result_custody::custody_typed_node_metadata_upper_bound(
                array.data_type(),
                buffers,
                copy_nulls || array.nulls().is_some(),
                children,
            )?;
            Some(plus(own, child_bytes)?)
        }
        None => None,
    };
    let requests = match (pricing, child_requests) {
        (Some(pricing), Some(child_requests)) => {
            let header_fields = match array.data_type() {
                DataType::Map(..) => concrete::<MapArray>(array)?.entries().columns().len(),
                _ => 0,
            };
            Some(summarize_source_node(
                RequestSourceNode {
                    data_type: array.data_type(),
                    data_buffers: buffers,
                    child_count: children,
                    nulls: array.nulls(),
                    header_fields,
                    body,
                    role,
                },
                child_requests,
                pricing,
                work,
            )?)
        }
        (None, None) => None,
        _ => {
            return Err(
                invalid("source custody traversal has inconsistent request premises").into(),
            );
        }
    };
    let bufferless_root = pricing.map(|_| {
        let effective_nulls = array.nulls().is_some_and(|nulls| nulls.null_count() != 0);
        // Match the real recursive wrapping route: child metadata without
        // any physical Buffer needs the SAME one root custody wrapper.
        buffers == 0 && !descendant_has_custody_buffer && !effective_nulls
    });
    Ok(SourceGraphResult {
        legacy,
        requests,
        bufferless_root,
    })
}

pub(crate) fn source_metadata_bytes(
    array: &dyn Array,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    // Four envelopes cover pinned Arrow to_data transient table growth,
    // clone/into_builder/make_array coexistence, and custom-buffer metadata.
    // Apply it ONCE to the complete actual graph, not exponentially by depth.
    source_metadata_graph(
        array,
        work,
        false,
        &mut |_, _| Ok(()),
        None,
        SourceNodeRole::Rebox,
    )?
    .legacy
    .expect("legacy source traversal amount")
    .checked_mul(4)
    .ok_or(KernelFailure::ResourceExhausted)
}

/// SAME source graph for opaque to_data/validation/canonical wrapper calls.
/// Original payload is borrowed and excluded from this metadata-only bound.
pub fn source_metadata_bytes_observed<E: From<KernelFailure>>(
    array: &dyn Array,
    observe: &mut impl FnMut(bool) -> Result<(), E>,
) -> Result<usize, E> {
    let mut work = CreatorMetadataWork(observe);
    let bytes = source_metadata_graph(
        array,
        &mut work,
        false,
        &mut |_, _| Ok(()),
        None,
        SourceNodeRole::Rebox,
    )?
    .legacy
    .expect("legacy source traversal amount");
    Ok(bytes
        .checked_mul(4)
        .ok_or(KernelFailure::ResourceExhausted)?)
}

/// The SAME actual source graph bounds copy metadata. Any copied node may gain
/// validity from actual nullable indices even when its source has no bitmap.
/// Dictionary/View children remain source-owned; counting their metadata does
/// not admit or charge their already-owned payload.
pub(crate) fn copy_metadata_bytes(
    array: &dyn Array,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<usize, KernelFailure> {
    source_metadata_graph(
        array,
        work,
        true,
        &mut |_, _| Ok(()),
        None,
        SourceNodeRole::Rebox,
    )?
    .legacy
    .expect("legacy copy traversal amount")
    .checked_mul(4)
    .ok_or(KernelFailure::ResourceExhausted)
}

/// Direct creator work observes the SAME source graph; original H is never
/// converted into a kernel-control error or captured in a side channel.
pub(crate) fn copy_metadata_bytes_observed<E: From<KernelFailure>>(
    array: &dyn Array,
    observe: &mut impl FnMut(bool) -> Result<(), E>,
) -> Result<usize, E> {
    let mut work = CreatorMetadataWork(observe);
    let bytes = source_metadata_graph(
        array,
        &mut work,
        true,
        &mut |_, _| Ok(()),
        None,
        SourceNodeRole::Rebox,
    )?
    .legacy
    .expect("legacy copy traversal amount");
    Ok(bytes
        .checked_mul(4)
        .ok_or(KernelFailure::ResourceExhausted)?)
}

/// Explicit resource observation borrows the existing parent work. Legacy
/// entrypoints keep their original None path and original checkpoint trace.
pub(crate) fn observe_source_metadata<'source>(
    array: &'source dyn Array,
    copy_nulls: bool,
    work: &mut EvaluationCheckpoints<'_>,
    observer: &mut dyn BorrowedSourceObservation<'source>,
) -> Result<usize, KernelFailure> {
    source_metadata_graph(
        array,
        work,
        copy_nulls,
        &mut |node, work| observer.node(node, work),
        None,
        SourceNodeRole::Rebox,
    )?
    .legacy
    .expect("legacy observed traversal amount")
    .checked_mul(4)
    .ok_or(KernelFailure::ResourceExhausted)
}

#[cfg(test)]
#[path = "array_backing_geometry_observation_tests.rs"]
mod observation_tests;

pub(crate) fn source_request_facts(
    array: &dyn Array,
    pricing: &SourceRequestPricing,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<SourceCustodyRequestFacts, KernelFailure> {
    let result = source_metadata_graph(
        array,
        work,
        false,
        &mut |_, _| Ok(()),
        Some(pricing),
        SourceNodeRole::Rebox,
    )?;
    result
        .requests
        .expect("source request traversal summary")
        .finish(
            pricing,
            result.bufferless_root.expect("source request root facts"),
        )
}
