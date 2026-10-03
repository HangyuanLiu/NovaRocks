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
use arrow::datatypes::DataType;
use novarocks_constant_contract::ConstantResourceFacts;
use novarocks_type_contract::{CompileCheckpoints, NR_LOGICAL_TYPE_KEY};

pub(super) fn source_metadata_work(
    nodes: usize,
    source_retained_bytes: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, FlatPoolResourceError> {
    // Original invoice includes every HashMap bucket/control backing (also
    // deleted buckets), source strings and original raw owner. At each actual
    // Field occurrence both source walks and four nominal-key probes can visit
    // at most this entire backing, plus the static probe key's bytes.
    let result = add(
        mul(source_retained_bytes, add(mul(6, nodes)?, 2)?)?,
        mul(mul(4, nodes)?, NR_LOGICAL_TYPE_KEY.len())?,
    );
    work.step()?;
    result
}
fn extent(n: u64) -> Result<usize, FlatPoolResourceError> {
    usize::try_from(n).map_err(|_| invalid("recursive reader work extent"))
}

pub(super) fn preflight(
    input: &ReaderInput<'_, '_>,
    source_retained_bytes: usize,
    payload: &PayloadRequests,
    pool: &ConstantResourceFacts,
    structures: &ReaderAllocationRequests,
    diagnostics: &ReaderDiagnosticRequests,
    work: &mut CompileCheckpoints<'_>,
) -> Result<usize, FlatPoolResourceError> {
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
    // Each source projection/type comparison reads at most the original
    // trusted backing, including removed HashMap buckets. Formatting visits
    // complete descendant types once per candidate and ancestor wrapper;
    // metadata sorting has its own key-pair term, independent of type width.
    let source = source_metadata_work(input.nodes.len(), source_retained_bytes, work)?;
    total = add(total, source)?;
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
