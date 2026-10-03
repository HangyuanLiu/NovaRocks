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

//! Cumulative visits of the actual recursive safe-reader/canonical pool graph.
//! These bounds describe opaque source work; they are not synthetic callback
//! charges, CPU cycles, formal host grants or internal cooperation claims.

use super::{
    reader_allocations::ReaderAllocationRequests,
    reader_resources::{PayloadRequests, ReaderInput, add, invalid, mul},
};
use crate::ipc_flat_stream_v2::{
    FlatPoolResourceError, reader_diagnostics::ReaderDiagnosticRequests,
};
use crate::ipc_recursive_batch_v2::RecursiveNodeGeometry;
use arrow::datatypes::DataType;
use novarocks_constant_contract::ConstantResourceFacts;
use novarocks_type_contract::{CompileCheckpoints, MAX_VALUE_TYPE_NODES, NR_LOGICAL_TYPE_KEY};
use std::cmp::Ordering;

#[derive(Clone, Copy, Debug)]
pub(super) struct SourceMetadataWork {
    pub source_retained_bytes: usize,
    pub total: usize,
    #[cfg(test)]
    pub one_comparison: usize,
    pub native_comparisons: usize,
}

fn metadata_parameters(entries: usize) -> Result<(usize, usize), FlatPoolResourceError> {
    if entries == 0 {
        return Ok((0, 0));
    }
    Ok((
        add(mul(3, entries)?, 3)?,
        add(add(mul(2, mul(entries, entries)?)?, mul(2, entries)?)?, 2)?,
    ))
}

#[cfg(test)]
pub(super) fn metadata_comparison_work(
    entries: usize,
    source_retained_bytes: usize,
) -> Result<usize, FlatPoolResourceError> {
    let (weight, candidates) = metadata_parameters(entries)?;
    add(mul(source_retained_bytes, weight)?, candidates)
}

fn compare_names(
    left: &[u8],
    right: &[u8],
    model_work: &mut usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Ordering, FlatPoolResourceError> {
    for (left, right) in left.chunks(1024).zip(right.chunks(1024)) {
        let ordering = left.cmp(right);
        *model_work = add(*model_work, add(add(left.len(), right.len())?, 1)?)?;
        work.step()?;
        if ordering != Ordering::Equal {
            return Ok(ordering);
        }
    }
    let ordering = left.len().cmp(&right.len());
    *model_work = add(*model_work, 1)?;
    work.step()?;
    Ok(ordering)
}

fn sift_down(
    indices: &mut [usize],
    mut root: usize,
    nodes: &[RecursiveNodeGeometry<'_>],
    model_work: &mut usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FlatPoolResourceError> {
    loop {
        let left = add(mul(2, root)?, 1)?;
        *model_work = add(*model_work, 1)?;
        work.step()?;
        if left >= indices.len() {
            return Ok(());
        }
        let right = add(left, 1)?;
        let mut child = left;
        if right < indices.len()
            && compare_names(
                nodes[indices[left]].field.name().as_bytes(),
                nodes[indices[right]].field.name().as_bytes(),
                model_work,
                work,
            )? == Ordering::Less
        {
            child = right;
        }
        if compare_names(
            nodes[indices[root]].field.name().as_bytes(),
            nodes[indices[child]].field.name().as_bytes(),
            model_work,
            work,
        )? != Ordering::Less
        {
            return Ok(());
        }
        indices.swap(root, child);
        root = child;
        *model_work = add(*model_work, 1)?;
        work.step()?;
    }
}

fn sort_indices(
    indices: &mut [usize],
    nodes: &[RecursiveNodeGeometry<'_>],
    model_work: &mut usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FlatPoolResourceError> {
    // In-place heapsort is O(N log N) and its comparator remains fallible.
    // A refusing comparison immediately returns the original error; there is
    // no standard-sort comparator fallback or retry with a fabricated order.
    for root in (0..indices.len() / 2).rev() {
        sift_down(indices, root, nodes, model_work, work)?;
    }
    for end in (1..indices.len()).rev() {
        indices.swap(0, end);
        *model_work = add(*model_work, 1)?;
        work.step()?;
        sift_down(&mut indices[..end], 0, nodes, model_work, work)?;
    }
    Ok(())
}

pub(super) fn native_metadata_frequency(
    node: &RecursiveNodeGeometry<'_>,
) -> Result<usize, FlatPoolResourceError> {
    // get_valid_child_data compares the child's DataType, excluding that
    // child's Field metadata. Only proper ancestors at depths 1..d-2 can
    // therefore inspect this Field. validate_full invokes validate recursively
    // at every ancestor: four complete applications contribute 4*sum(depth).
    // Extra list/map ancestor projections contribute at most A*sum(depth),
    // and constructor child-type comparisons contribute (2+A)*(d-2).
    let ancestors = node.depth.saturating_sub(2);
    let depth_sum = mul(ancestors, add(ancestors, 1)?)? / 2;
    add(
        mul(add(4, node.list_map_ancestors)?, depth_sum)?,
        mul(add(2, node.list_map_ancestors)?, ancestors)?,
    )
}

pub(super) fn source_metadata_work(
    nodes: &[RecursiveNodeGeometry<'_>],
    source_retained_bytes: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<SourceMetadataWork, FlatPoolResourceError> {
    // The invoice includes both original Field and supplied FVT backing,
    // including strings and every HashMap bucket/control byte (also deleted
    // buckets). Source walks and nominal-key probes keep their original term.
    let original = add(
        mul(source_retained_bytes, add(mul(6, nodes.len())?, 2)?)?,
        mul(mul(4, nodes.len())?, NR_LOGICAL_TYPE_KEY.len())?,
    )?;
    work.step()?;
    // The actual geometry author calls the sole type grammar validator before
    // its DFS, enforcing MAX_VALUE_TYPE_NODES, and checks exact node coverage.
    // This fixed scratch reuses that admitted bound; it is not a new profile
    // limit. Its stack storage remains a host obligation, not a heap request
    // or a grant. Initializing the fixed array is an opaque bounded operation
    // bracketed on the same meter, with all initialized bytes in model work.
    if nodes.len() > MAX_VALUE_TYPE_NODES {
        return Err(invalid(
            "recursive reader geometry exceeds admitted type nodes",
        ));
    }
    let mut model_work = 1;
    let mut candidates = 0;
    let mut native_candidates = 0;
    for node in nodes.iter().skip(1) {
        let (_, candidate_count) = metadata_parameters(node.field.metadata().len())?;
        candidates = add(candidates, candidate_count)?;
        native_candidates = add(
            native_candidates,
            mul(candidate_count, native_metadata_frequency(node)?)?,
        )?;
        model_work = add(model_work, 1)?;
        work.step()?;
    }
    if candidates == 0 {
        // No nested metadata means no name grouping or scratch initialization.
        // The actual O(1)-per-node length scan is still counted and observed.
        return Ok(SourceMetadataWork {
            source_retained_bytes,
            total: add(original, model_work)?,
            #[cfg(test)]
            one_comparison: 0,
            native_comparisons: 0,
        });
    }
    work.flush()?;
    let mut indices = [0usize; MAX_VALUE_TYPE_NODES];
    work.flush()?;
    model_work = add(model_work, std::mem::size_of_val(&indices))?;
    let mut count = 0;
    // DataType comparison excludes the root Field's metadata. For each nested
    // Field K entries need <=1 left scan + K right scans, <=K squared key
    // candidates and K values. Byte inspection/table scans weigh 3K+3 copies
    // of the original backing; candidate/length operations weigh 2K²+2K+2.
    // No entry-count or key-size cap is assumed.
    //
    // Field owns its metadata map; aliases of that map are aliases of the
    // SAME immutable Field/name. Both exact Walk and Arrow Field::eq check
    // names BEFORE metadata. For a different supplied FVT, any wrong-name
    // alias stops before inspecting its map. Each retained map's repeated
    // visits are bounded by its source-name group's summed weight. The maximum
    // group weight times the invoice covers all distinct backings together,
    // including shared Field occurrences and tombstones. Sorting only stack
    // indices keeps that proof without quadratic numerical grouping work.
    for (index, node) in nodes.iter().enumerate().skip(1) {
        if !node.field.metadata().is_empty() {
            indices[count] = index;
            count = add(count, 1)?;
        }
        model_work = add(model_work, 1)?;
        work.step()?;
    }
    let indices = &mut indices[..count];
    sort_indices(indices, nodes, &mut model_work, work)?;
    let mut maximum_weight = 0;
    let mut maximum_native_weight = 0;
    let mut previous_name: Option<&[u8]> = None;
    let mut group_weight = 0;
    let mut native_weight = 0;
    for &index in indices.iter() {
        let node = &nodes[index];
        let name = node.field.name().as_bytes();
        let same_group = match previous_name {
            Some(previous) => {
                compare_names(previous, name, &mut model_work, work)? == Ordering::Equal
            }
            None => false,
        };
        if !same_group {
            maximum_weight = maximum_weight.max(group_weight);
            maximum_native_weight = maximum_native_weight.max(native_weight);
            group_weight = 0;
            native_weight = 0;
        }
        let (weight, _) = metadata_parameters(node.field.metadata().len())?;
        group_weight = add(group_weight, weight)?;
        native_weight = add(
            native_weight,
            mul(weight, native_metadata_frequency(node)?)?,
        )?;
        previous_name = Some(name);
        model_work = add(model_work, 1)?;
        work.step()?;
    }
    maximum_weight = maximum_weight.max(group_weight);
    maximum_native_weight = maximum_native_weight.max(native_weight);
    let one_comparison = add(mul(source_retained_bytes, maximum_weight)?, candidates)?;
    let native_comparisons = add(
        mul(source_retained_bytes, maximum_native_weight)?,
        native_candidates,
    )?;
    // Three Constant type passes: one before arrays, two in try_new. This
    // already-executed numerical grouping work is counted exactly once, and
    // its facts are reused by the final envelope without another grouping walk.
    Ok(SourceMetadataWork {
        source_retained_bytes,
        total: add(add(original, mul(3, one_comparison)?)?, model_work)?,
        #[cfg(test)]
        one_comparison,
        native_comparisons,
    })
}
fn extent(n: u64) -> Result<usize, FlatPoolResourceError> {
    usize::try_from(n).map_err(|_| invalid("recursive reader work extent"))
}

pub(super) fn preflight(
    input: &ReaderInput<'_, '_>,
    payload: &PayloadRequests,
    pool: &ConstantResourceFacts,
    structures: &ReaderAllocationRequests,
    diagnostics: &ReaderDiagnosticRequests,
    source: &SourceMetadataWork,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, FlatPoolResourceError> {
    let source_retained_bytes = source.source_retained_bytes;
    // The shared recursive envelope covers full descendant type/NULL/value
    // validation, including unused storage, whole UTF8 fallback and repeated
    // view references. Reader build, final two force_validate projections and
    // pool.validate_full are four applications of that same sole author.
    let mut total = mul(4, extent(pool.library_validation_work_upper_bound)?)?;
    total = add(
        total,
        add(
            add(input.body.len(), payload.repair_capacity)?,
            payload.empty_offsets_capacity,
        )?,
    )?;
    let mut source_projections = 0;
    let mut headers = 0;
    let mut metadata_pairs = 0;
    for node in input.nodes {
        let projections = add(2, node.list_map_ancestors)?;
        // datatype_display::format_metadata collects and sorts existing keys.
        // K² comparisons bounds the locked sort even for adversarial order;
        // every comparison can inspect <= the longest authored key.
        let entries = node.field.metadata().len();
        let mut longest_key = 0;
        work.flush()?;
        for key in node.field.metadata().keys() {
            longest_key = longest_key.max(key.len());
            work.step()?;
        }
        work.flush()?;
        metadata_pairs = add(
            metadata_pairs,
            mul(mul(entries, entries)?, add(longest_key, 1)?)?,
        )?;
        // Extra list/map ancestor to_data validation: actual subtree type
        // correspondence and this node's own row/buffer-value inspection.
        let local = add(
            add(
                add(mul(node.subtree_nodes, node.subtree_nodes)?, node.rows)?,
                node.buffer_count,
            )?,
            add(node.described_buffer_bytes, node.view_validation_bytes)?,
        )?;
        let local = if matches!(node.field.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
            add(local, node.described_buffer_bytes)? // full values fallback <= descriptors
        } else {
            local
        };
        total = add(total, mul(node.list_map_ancestors, local)?)?;
        source_projections = add(source_projections, mul(projections, node.subtree_nodes)?)?;
        // Each projection/canonical reconstruction clones and eventually drops
        // only header owners, not payload bytes. Initial reader slicing and
        // alignment also visit these exact local descriptor/header counts.
        let local_headers = add(
            mul(add(mul(2, projections)?, 3)?, node.buffer_count)?,
            add(
                mul(projections, node.variadic_buffers)?,
                mul(mul(2, projections)?, node.children)?,
            )?,
        )?;
        headers = add(headers, local_headers)?;
        // The post-array constant scan revisits raw headers/buffers plus every
        // view row. RequiredFrame semantic visits are bounded by the original
        // logical-elements author, not the root's row count alone.
        total = add(
            total,
            add(
                add(node.buffer_count, node.described_buffer_bytes)?,
                add(
                    1,
                    if matches!(
                        node.field.data_type(),
                        DataType::Utf8View | DataType::BinaryView
                    ) {
                        node.rows
                    } else {
                        0
                    },
                )?,
            )?,
        )?;
        if matches!(
            node.field.data_type(),
            DataType::Utf8View | DataType::BinaryView
        ) {
            total = add(
                total,
                mul(mul(add(4, node.list_map_ancestors)?, 4)?, node.rows)?,
            )?;
        }
        work.step()?;
    }
    // Native recursive comparisons inspect only actual nested child-type
    // paths, not every Field in every type visit. In particular, a wide Struct
    // of primitive children has no native metadata visits. RecordBatch's root
    // type comparison retains the cloned source DataType's nested Arc owners:
    // Arc equality short-circuits those identical Field/Fields owners. Arrays
    // in this reader are built solely from that source, without reauthoring
    // Fields; supplied FVT is used only by the three Constant comparisons.
    total = add(total, source.native_comparisons)?;
    // Formatting visits
    // complete descendant types once per candidate and ancestor wrapper;
    // metadata sorting has its own key-pair term, independent of type width.
    total = add(total, source.total)?;
    let diagnostic_type_visits = mul(
        mul(input.nodes.len(), input.nodes.len())?,
        add(input.geometry.maximum_depth, 4)?,
    )?;
    total = add(
        total,
        mul(
            source_retained_bytes,
            add(source_projections, diagnostic_type_visits)?,
        )?,
    )?;
    let render_passes = mul(
        mul(4, input.nodes.len())?,
        add(input.geometry.maximum_depth, 4)?,
    )?;
    // Four format arguments per failing candidate plus each ancestor wrapper.
    // The factor six also covers Debug escaping of the visited source bytes.
    let sorted_metadata = mul(mul(6, render_passes)?, metadata_pairs)?;
    total = add(total, sorted_metadata)?;
    total = add(total, mul(2, headers)?)?;
    total = add(
        total,
        mul(
            4,
            add(
                extent(pool.logical_elements_upper_bound)?,
                input.nodes.len(),
            )?,
        )?,
    )?;
    let requests = add(
        add(
            structures.structural_request_bytes_upper_bound,
            diagnostics.request_bytes_upper_bound,
        )?,
        input.geometry_scratch_request_bytes,
    )?;
    let payload_count = add(
        add(
            usize::from(payload.body_capacity != 0),
            payload.repair_count,
        )?,
        payload.empty_offsets_count,
    )?;
    let request_count = add(
        add(
            add(structures.allocation_requests_upper_bound, payload_count)?,
            diagnostics.allocation_requests_upper_bound,
        )?,
        input.geometry_scratch_request_count,
    )?;
    // Allocation initialization, text/growing-container moves, and teardown.
    total = add(total, add(mul(4, requests)?, request_count)?)?;
    let owners = add(add(1, payload.repair_count)?, payload.empty_offsets_count)?;
    if owners > 3 {
        total = add(total, mul(owners, owners)?)?;
    }
    work.step()?;
    Ok(total)
}
