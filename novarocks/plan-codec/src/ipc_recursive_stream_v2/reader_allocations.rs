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

//! Structural requests of the locked recursive reader and constant owner.
//! Each list/map ancestor causes another child to_data/make_array projection;
//! Struct uses its direct constructor. No payload or allocation grant is added.

use super::reader_resources::{PayloadRequests, ReaderInput, add, invalid, mul};
use crate::ipc_flat_batch_v2::{Layout as FlatLayout, layout};
use crate::ipc_flat_stream_v2::{
    FlatPoolResourceError,
    reader_allocations::{Requests, concrete_array_layout, environment},
};
use arrow::{
    array::{ArrayData, ArrayRef, BufferSpec},
    datatypes::{DataType, FieldRef, Schema},
};
use arrow_buffer::Buffer;
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::CompileCheckpoints;
use std::alloc::Layout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReaderAllocationRequests {
    pub structural_request_bytes_upper_bound: usize,
    pub allocation_requests_upper_bound: usize,
}
fn array<T>(count: usize) -> Result<Layout, FlatPoolResourceError> {
    Layout::array::<T>(count).map_err(|_| invalid("recursive reader structural layout"))
}

/// Cumulative RawVec requests for an actual private element Layout. The pinned
/// minimum and doubling are the same source as shared Requests::growing_vec.
fn growing_layout(
    requests: &mut Requests,
    element: Layout,
    count: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FlatPoolResourceError> {
    if count == 0 || element.size() == 0 {
        work.step()?;
        return Ok(());
    }
    let minimum = if element.size() == 1 {
        8
    } else if element.size() <= 1024 {
        4
    } else {
        1
    };
    let maximum = minimum.max(mul(count, 2)?);
    let layout = Layout::from_size_align(mul(element.size(), maximum)?, element.align())
        .map_err(|_| invalid("recursive reader growing layout"))?;
    let mut capacity = minimum;
    let mut number = 2usize; // also covers an initial vec![one] request
    while capacity < count {
        capacity = mul(capacity, 2)?;
        number = add(number, 1)?;
        work.step()?;
    }
    requests.bytes = add(requests.bytes, mul(layout.size(), 2)?)?;
    requests.count = add(requests.count, number)?;
    work.step()?;
    Ok(())
}

fn allocation_set(
    requests: &mut Requests,
    owners: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FlatPoolResourceError> {
    if owners <= 3 {
        work.step()?;
        return Ok(());
    }
    let layout = crate::btree_resources_v2::node_layout::<(usize, usize), ()>().map_err(invalid)?;
    // Only insertions occur. Every retained node has at least one key, and no
    // node is deleted/reallocated. Cumulative node allocations <= distinct
    // backing identities, including the first three copied from inline slots.
    requests.record(layout, owners, work)
}

pub(super) fn preflight(
    input: &ReaderInput<'_, '_>,
    payload: &PayloadRequests,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ReaderAllocationRequests, FlatPoolResourceError> {
    let bytes = environment(work)?;
    let mut requests = Requests::default();
    requests.arc(array::<FieldRef>(1)?, 1, work)?;
    requests.arc(Layout::new::<Schema>(), 1, work)?;
    let owners = add(add(1, payload.repair_count)?, payload.empty_offsets_count)?;
    requests.arc(bytes, owners, work)?;
    requests.exact_vec::<ArrayRef>(4, 1, work)?; // one RecordBatch column push
    if input.geometry.view_fields != 0 {
        requests.growing_vec::<i64>(input.geometry.view_fields, work)?; // variadic VecDeque
    }
    for node in input.nodes {
        let projections = add(2, node.list_map_ancestors)?;
        requests.arc(
            concrete_array_layout(node.field.data_type())?,
            projections,
            work,
        )?;
        let specs;
        match node.field.data_type() {
            DataType::Struct(_) => {
                // Direct reader child pushes; later make_array exact child
                // collection. to_data produces an exact ArrayData child Vec.
                requests.growing_vec::<ArrayRef>(node.children, work)?;
                requests.exact_vec::<ArrayRef>(node.children, projections - 1, work)?;
                requests.exact_vec::<ArrayData>(node.children, projections, work)?;
                specs = 0;
            }
            DataType::List(_) | DataType::LargeList(_) | DataType::Map(_, _) => {
                requests.exact_vec::<Buffer>(4, 1, work)?;
                requests.exact_vec::<ArrayData>(4, 1, work)?;
                requests.exact_vec::<Buffer>(1, projections, work)?;
                requests.exact_vec::<ArrayData>(1, projections, work)?;
                specs = 1;
            }
            ty => match layout(ty)? {
                FlatLayout::Null => {
                    specs = 0;
                }
                FlatLayout::Views => {
                    requests.growing_vec::<Buffer>(node.buffer_count, work)?;
                    requests.exact_vec::<Buffer>(add(node.variadic_buffers, 1)?, 1, work)?;
                    requests.arc(array::<Buffer>(node.variadic_buffers)?, projections, work)?;
                    for _ in 0..projections {
                        requests.view_to_data(node.variadic_buffers, work)?;
                    }
                    specs = 1;
                }
                FlatLayout::Offsets(_) => {
                    requests.exact_vec::<Buffer>(2, add(projections, 1)?, work)?;
                    specs = 2;
                }
                FlatLayout::Bits | FlatLayout::Fixed(_) => {
                    requests.exact_vec::<Buffer>(4, 1, work)?;
                    requests.exact_vec::<Buffer>(1, projections, work)?;
                    specs = 1;
                }
            },
        }
        // A list/map ancestor re-aligns the already-aligned subtree (layout
        // scratch only) and creates one force_validate to_data projection.
        // Own read build, two final projections and pool.validate_full give
        // the existing five flat layout requests at ancestor count zero.
        requests.exact_vec::<BufferSpec>(specs, add(5, mul(2, node.list_map_ancestors)?)?, work)?;
        // scan_data accumulates actual child metrics once at this occurrence.
        growing_layout(
            &mut requests,
            ConstantPool::scan_child_metric_layout(),
            node.children,
            work,
        )?;
        work.step()?;
    }
    // Prefix shape/type stacks and both pool source validations use the same
    // bounded DFS pending vector. Conservatively include every source walk.
    for _ in 0..5 {
        requests.growing_vec::<(&DataType, usize)>(input.nodes.len(), work)?;
    }
    if !matches!(input.field.data_type(), DataType::Null) {
        // RequiredFrame is the owner's actual Layout, not a mirrored enum.
        // Active DFS keeps at most all pending Struct siblings plus one Range
        // per ancestor and the current row: <= T + D + 1 frames, independent
        // of list storage row count (ranges resume one row at a time).
        let frames = add(add(input.nodes.len(), input.geometry.maximum_depth)?, 1)?;
        growing_layout(
            &mut requests,
            ConstantPool::flat_value_validation_stack_layout(),
            frames,
            work,
        )?;
    }
    allocation_set(&mut requests, owners, work)?;
    requests.record(ConstantPool::backing_allocation_layout(), 1, work)?;
    Ok(ReaderAllocationRequests {
        structural_request_bytes_upper_bound: requests.bytes,
        allocation_requests_upper_bound: requests.count,
    })
}
