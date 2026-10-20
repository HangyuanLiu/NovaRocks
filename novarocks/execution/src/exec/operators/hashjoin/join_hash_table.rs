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
//! Hash-table primitives for join key indexing.
//!
//! Responsibilities:
//! - Builds hash buckets and row-reference chains from build-side key arrays.
//! - Supports null-sensitive key handling and efficient group-id lookup for probing.
//!
//! Key exported interfaces:
//! - Types: `JoinHashTable`.
//! - Functions: `row_has_forbidden_null`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use std::mem;
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::DataType;

use crate::exec::hash_table::allocation::AggregateVec;
use crate::exec::hash_table::key_builder::{GroupKeyArrayView, build_group_key_views};
use crate::exec::hash_table::key_strategy::GroupKeyStrategy;
use crate::exec::hash_table::key_table::{KeyLookup, KeyTable};
use crate::runtime::mem_tracker::MemTracker;

use crate::runtime::fragment::{ExecutionFailure, ExecutionResult};

#[derive(Debug)]
struct JoinHashTableContract(novarocks_failure_contract::BoundedDetail);
impl std::fmt::Display for JoinHashTableContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_str())
    }
}
impl std::error::Error for JoinHashTableContract {}
impl novarocks_failure_contract::FailureProjection for JoinHashTableContract {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureFact::new(
            novarocks_failure_contract::FailureClass::Invariant,
            crate::runtime::fragment::ExecutionReason::JoinHashTableContract.reason(),
        )
    }
}
fn table_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(JoinHashTableContract(
        novarocks_failure_contract::BoundedDetail::truncating_display(detail),
    ))
}
#[derive(Debug)]
struct JoinRowIndexCapacity {
    current: usize,
    incoming: usize,
}
impl std::fmt::Display for JoinRowIndexCapacity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "join row index capacity exceeded: live={} incoming={} maximum={}",
            self.current,
            self.incoming,
            u32::MAX
        )
    }
}
impl std::error::Error for JoinRowIndexCapacity {}
impl novarocks_failure_contract::FailureProjection for JoinRowIndexCapacity {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        use novarocks_failure_contract::{
            FailureClass, FailureFact, ResourceDimension, ResourceFacts, ResourceScope,
        };
        FailureFact::new(
            FailureClass::Resource,
            crate::runtime::fragment::ExecutionReason::JoinRowIndexCapacity.reason(),
        )
        .with_resource(ResourceFacts {
            request_scope: Some(ResourceScope::Operator),
            constraint_scope: Some(ResourceScope::Operator),
            dimension: ResourceDimension::Positions,
            requested: u64::try_from(self.incoming).ok(),
            required_growth: u64::try_from(self.incoming).ok(),
            limit: Some(u32::MAX as u64),
            live: u64::try_from(self.current).ok(),
            committed: None,
        })
    }
}
/// ROW_NONE consumes the all-ones reference, so valid indices end one below
/// it. This is an index-position bound, not a memory grant or SQL arithmetic.
pub(super) fn checked_row_count(current: usize, incoming: usize) -> ExecutionResult<usize> {
    let next = current as u128 + incoming as u128;
    if next > u32::MAX as u128 {
        Err(ExecutionFailure::from_domain(JoinRowIndexCapacity {
            current,
            incoming,
        }))
    } else {
        Ok(next as usize)
    }
}

const ENABLE_GROUP_KEY_OPTIMIZATIONS: bool = true;
const ROW_NONE: u32 = u32::MAX;

pub(crate) enum SerializedRows {
    Fallback(AggregateVec<AggregateVec<u8>>),
}

impl SerializedRows {
    fn row_bytes(&self, row: usize) -> Result<&[u8], ExecutionFailure> {
        match self {
            Self::Fallback(rows) => rows.get(row).map(|v| v.as_slice()).ok_or_else(|| {
                table_contract(format_args!(
                    "fallback join row missing at row={} (rows={})",
                    row,
                    rows.len()
                ))
            }),
        }
    }
}

/// Hash-table container for join key buckets and build-row reference chains.
pub(crate) struct JoinHashTable {
    key_table: KeyTable,
    null_safe_eq: Vec<bool>,
    group_head: Vec<u32>,
    row_next: Vec<u32>,
    row_count: usize,
    group_offsets: Option<Vec<u32>>,
    group_rows: Option<Vec<u32>>,
    mem_tracker: Option<Arc<MemTracker>>,
    accounted_bytes: i64,
}

/// Check whether the row has null on any key that is not null-safe (`=` semantics).
pub(crate) fn row_has_forbidden_null(
    views: &[GroupKeyArrayView<'_>],
    row: usize,
    null_safe_eq: &[bool],
) -> bool {
    for (idx, view) in views.iter().enumerate() {
        if *null_safe_eq.get(idx).unwrap_or(&false) {
            continue;
        }
        let is_null = match view {
            GroupKeyArrayView::Int(view) => view.value_at(row).is_none(),
            GroupKeyArrayView::Float(view) => view.value_at(row).is_none(),
            GroupKeyArrayView::Boolean(arr) => arr.is_null(row),
            GroupKeyArrayView::Utf8(arr) => arr.is_null(row),
            GroupKeyArrayView::Dictionary(dict) => dict.is_null(row),
            GroupKeyArrayView::Date32(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampSecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampMillisecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampMicrosecond(arr) => arr.is_null(row),
            GroupKeyArrayView::TimestampNanosecond(arr) => arr.is_null(row),
            GroupKeyArrayView::Decimal128(arr) => arr.is_null(row),
            GroupKeyArrayView::Decimal256(arr) => arr.is_null(row),
            GroupKeyArrayView::LargeIntBinary(arr) => arr.is_null(row),
            GroupKeyArrayView::ListUtf8 { list, .. } => list.is_null(row),
            GroupKeyArrayView::ListInt32 { list, .. } => list.is_null(row),
            GroupKeyArrayView::Complex(arr) => arr.is_null(row),
        };
        if is_null {
            return true;
        }
    }
    false
}

impl JoinHashTable {
    pub(crate) fn new_with_tracker(
        key_types: Vec<DataType>,
        null_safe_eq: Vec<bool>,
        tracker: Arc<MemTracker>,
    ) -> Result<Self, ExecutionFailure> {
        if key_types.is_empty() {
            return Err(table_contract("join hash table requires join keys"));
        }
        if key_types.len() != null_safe_eq.len() {
            return Err(table_contract(format_args!(
                "join hash table null-safe key count mismatch: key_types={} flags={}",
                key_types.len(),
                null_safe_eq.len()
            )));
        }
        let key_table = KeyTable::new_with_tracker(
            key_types,
            ENABLE_GROUP_KEY_OPTIMIZATIONS,
            MemTracker::new_child("KeyTable", &tracker),
        )?;
        if key_table.key_strategy() == GroupKeyStrategy::Scalar {
            return Err(table_contract("join hash table requires join keys"));
        }
        Ok(Self {
            key_table,
            null_safe_eq,
            group_head: Vec::new(),
            row_next: Vec::new(),
            row_count: 0,
            group_offsets: None,
            group_rows: None,
            mem_tracker: Some(tracker),
            accounted_bytes: 0,
        })
    }

    pub(crate) fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        if let Some(current) = self.mem_tracker.as_ref() {
            if Arc::ptr_eq(current, &tracker) {
                return;
            }
            current.release(self.accounted_bytes);
        }
        let bytes = self.tracked_bytes();
        tracker.consume(bytes);
        self.mem_tracker = Some(Arc::clone(&tracker));
        self.accounted_bytes = bytes;

        // KeyTable owns allocation-time checked containers whose allocator is
        // immutable. Re-parent only this join wrapper's legacy support vectors;
        // the KeyTable stays on the query-scoped tracker supplied at construction.
    }

    pub(crate) fn key_strategy(&self) -> GroupKeyStrategy {
        self.key_table.key_strategy()
    }

    pub(crate) fn null_safe_eq(&self) -> &[bool] {
        &self.null_safe_eq
    }

    pub(crate) fn hash_seed(&self) -> u64 {
        self.key_table.hash_seed()
    }

    pub(crate) fn compressed_ctx(
        &self,
    ) -> Option<&crate::exec::hash_table::key_layout::CompressedKeyContext> {
        self.key_table.compressed_ctx()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.group_head.is_empty()
    }

    pub(crate) fn build_rows_or_fallback(
        &self,
        arrays: &[arrow::array::ArrayRef],
    ) -> Result<SerializedRows, ExecutionFailure> {
        self.key_table
            .build_rows_fallback(arrays)
            .map(SerializedRows::Fallback)
    }

    pub(crate) fn group_rows_slice(&self, group_id: usize) -> Result<&[u32], ExecutionFailure> {
        let offsets = self
            .group_offsets
            .as_ref()
            .ok_or_else(|| table_contract("join group offsets missing"))?;
        let rows = self
            .group_rows
            .as_ref()
            .ok_or_else(|| table_contract("join group rows missing"))?;
        if group_id >= offsets.len().saturating_sub(1) {
            return Err(table_contract("join group id out of bounds"));
        }
        let start = offsets[group_id] as usize;
        let end = offsets[group_id + 1] as usize;
        Ok(&rows[start..end])
    }

    pub(crate) fn group_build_rows(&self, group_id: usize) -> Result<&[u32], ExecutionFailure> {
        self.group_rows_slice(group_id)
    }

    pub(crate) fn add_build_rows(
        &mut self,
        key_arrays: &[arrow::array::ArrayRef],
        num_rows: usize,
    ) -> Result<(), ExecutionFailure> {
        if self.group_offsets.is_some() || self.group_rows.is_some() {
            return Err(table_contract("join hash table already finalized"));
        }
        if key_arrays.len() != self.key_table.key_types().len() {
            return Err(table_contract("join key length mismatch"));
        }
        for (array, expected) in key_arrays.iter().zip(self.key_table.key_types()) {
            if array.len() != num_rows {
                return Err(table_contract(format_args!(
                    "join build key length mismatch: array={} rows={}",
                    array.len(),
                    num_rows
                )));
            }
            if array.data_type() != expected {
                return Err(table_contract("join key type mismatch"));
            }
        }
        if num_rows == 0 {
            return Ok(());
        }
        let next_row_count = checked_row_count(self.row_count, num_rows)?;
        let base_row_id = self.row_count as u32;
        self.row_next.resize(next_row_count, ROW_NONE);
        self.row_count = next_row_count;

        let key_views = build_group_key_views(key_arrays)?;
        self.key_table.ensure_compressed_ctx(&key_views)?;

        match self.key_table.key_strategy() {
            GroupKeyStrategy::OneNumber => {
                let view = key_views
                    .first()
                    .ok_or_else(|| table_contract("join one number key view missing"))?;
                let hashes = self.key_table.build_one_number_hashes(view, num_rows)?;
                for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
                    if row_has_forbidden_null(&key_views, row, &self.null_safe_eq) {
                        continue;
                    }
                    let lookup = self.key_table.find_or_insert_one_number(view, row, hash)?;
                    self.handle_lookup(lookup, base_row_id + row as u32)?;
                }
            }
            GroupKeyStrategy::OneString => {
                let view = key_views
                    .first()
                    .ok_or_else(|| table_contract("join one string key view missing"))?;
                let GroupKeyArrayView::Utf8(arr) = view else {
                    return Err(table_contract("join one string key expects Utf8 view"));
                };
                let hashes = self.key_table.build_group_hashes(&key_views, num_rows)?;
                for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
                    if row_has_forbidden_null(&key_views, row, &self.null_safe_eq) {
                        continue;
                    }
                    let key = if arr.is_null(row) {
                        None
                    } else {
                        Some(arr.value(row))
                    };
                    let lookup = self
                        .key_table
                        .find_or_insert_one_string(view, row, key, hash)?;
                    self.handle_lookup(lookup, base_row_id + row as u32)?;
                }
            }
            GroupKeyStrategy::FixedSize => {
                let hashes = self.key_table.build_group_hashes(&key_views, num_rows)?;
                for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
                    if row_has_forbidden_null(&key_views, row, &self.null_safe_eq) {
                        continue;
                    }
                    let lookup = self
                        .key_table
                        .find_or_insert_fixed_size(&key_views, row, hash)?;
                    self.handle_lookup(lookup, base_row_id + row as u32)?;
                }
            }
            GroupKeyStrategy::CompressedFixed => {
                let keys = self
                    .key_table
                    .build_compressed_flags(&key_views, num_rows)?;
                let hashes = self.key_table.build_group_hashes(&key_views, num_rows)?;
                let mut rows_opt = None;
                for (row, (key, hash)) in keys
                    .iter()
                    .copied()
                    .zip(hashes.iter().copied())
                    .enumerate()
                    .take(num_rows)
                {
                    if row_has_forbidden_null(&key_views, row, &self.null_safe_eq) {
                        continue;
                    }
                    let lookup = if key {
                        self.key_table
                            .find_or_insert_compressed(&key_views, row, hash)?
                    } else {
                        if rows_opt.is_none() {
                            rows_opt = Some(self.build_rows_or_fallback(key_arrays)?);
                        }
                        let rows = rows_opt.as_ref().expect("join rows");
                        let row_bytes = rows.row_bytes(row)?;
                        self.key_table
                            .find_or_insert_from_row(&key_views, row, row_bytes, hash)?
                    };
                    self.handle_lookup(lookup, base_row_id + row as u32)?;
                }
            }
            GroupKeyStrategy::Serialized => {
                let rows = self.build_rows_or_fallback(key_arrays)?;
                let hashes = self.key_table.build_group_hashes(&key_views, num_rows)?;
                for (row, hash) in hashes.iter().copied().enumerate().take(num_rows) {
                    if row_has_forbidden_null(&key_views, row, &self.null_safe_eq) {
                        continue;
                    }
                    let row_bytes = rows.row_bytes(row)?;
                    let lookup = self
                        .key_table
                        .find_or_insert_from_row(&key_views, row, row_bytes, hash)?;
                    self.handle_lookup(lookup, base_row_id + row as u32)?;
                }
            }
            GroupKeyStrategy::Scalar => {
                return Err(table_contract(
                    "join hash table does not support empty keys",
                ));
            }
        }
        self.refresh_accounting();
        Ok(())
    }

    pub(crate) fn finalize_groups(&mut self) -> Result<(), ExecutionFailure> {
        if self.group_offsets.is_some() || self.group_rows.is_some() {
            return Ok(());
        }
        let group_count = self.group_head.len();
        let mut counts = vec![0u32; group_count];
        for (group_id, count) in counts.iter_mut().enumerate().take(group_count) {
            let mut row = *self
                .group_head
                .get(group_id)
                .ok_or_else(|| table_contract("join group id out of bounds"))?;
            while row != ROW_NONE {
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| table_contract("join group row count overflow"))?;
                row = self.next_row(row)?;
            }
        }

        let mut offsets = Vec::with_capacity(group_count + 1);
        offsets.push(0);
        let mut total = 0u32;
        for count in &counts {
            total = total
                .checked_add(*count)
                .ok_or_else(|| table_contract("join group rows overflow"))?;
            offsets.push(total);
        }

        let mut rows = vec![0u32; total as usize];
        let mut write_pos = offsets[..group_count].to_vec();
        for group_id in 0..group_count {
            let mut row = *self
                .group_head
                .get(group_id)
                .ok_or_else(|| table_contract("join group id out of bounds"))?;
            while row != ROW_NONE {
                let slot = *write_pos
                    .get(group_id)
                    .ok_or_else(|| table_contract("join group id out of bounds"))?;
                let idx = slot as usize;
                if idx >= rows.len() {
                    return Err(table_contract("join group row index out of bounds"));
                }
                rows[idx] = row;
                write_pos[group_id] = slot
                    .checked_add(1)
                    .ok_or_else(|| table_contract("join group row count overflow"))?;
                row = self.next_row(row)?;
            }
        }

        self.group_offsets = Some(offsets);
        self.group_rows = Some(rows);
        self.refresh_accounting();
        Ok(())
    }

    pub(crate) fn lookup_serialized(
        &self,
        row_bytes: &[u8],
        hash: u64,
    ) -> Result<Option<usize>, ExecutionFailure> {
        self.key_table.lookup_serialized(row_bytes, hash)
    }

    pub(crate) fn lookup_one_string(
        &self,
        key: &str,
        hash: u64,
    ) -> Result<Option<usize>, ExecutionFailure> {
        self.key_table.lookup_one_string(key, hash)
    }

    pub(crate) fn lookup_one_string_null(&self) -> Option<usize> {
        self.key_table.lookup_one_string_null()
    }

    pub(crate) fn lookup_one_string_batch(
        &self,
        view: &GroupKeyArrayView<'_>,
        hashes: &[u64],
        nulls: &[bool],
    ) -> Result<Vec<Option<usize>>, ExecutionFailure> {
        if hashes.len() != nulls.len() {
            return Err(table_contract("join lookup batch length mismatch"));
        }
        let GroupKeyArrayView::Utf8(arr) = view else {
            return Err(table_contract("join one string key expects Utf8 view"));
        };
        let mut group_ids = vec![None; hashes.len()];
        for row in 0..hashes.len() {
            if nulls[row] {
                continue;
            }
            if arr.is_null(row) {
                group_ids[row] = self.lookup_one_string_null();
                continue;
            }
            let key = arr.value(row);
            group_ids[row] = self.lookup_one_string(key, hashes[row])?;
        }
        Ok(group_ids)
    }

    pub(crate) fn lookup_one_number(
        &self,
        view: &GroupKeyArrayView<'_>,
        row: usize,
        hash: u64,
    ) -> Result<Option<usize>, ExecutionFailure> {
        self.key_table.lookup_one_number(view, row, hash)
    }

    pub(crate) fn lookup_one_number_batch(
        &self,
        view: &GroupKeyArrayView<'_>,
        hashes: &[u64],
        nulls: &[bool],
    ) -> Result<Vec<Option<usize>>, ExecutionFailure> {
        if hashes.len() != nulls.len() {
            return Err(table_contract("join lookup batch length mismatch"));
        }
        let mut group_ids = vec![None; hashes.len()];
        for row in 0..hashes.len() {
            if nulls[row] {
                continue;
            }
            group_ids[row] = self.lookup_one_number(view, row, hashes[row])?;
        }
        Ok(group_ids)
    }

    pub(crate) fn lookup_fixed_size(
        &self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        hash: u64,
    ) -> Result<Option<usize>, ExecutionFailure> {
        self.key_table.lookup_fixed_size(views, row, hash)
    }

    pub(crate) fn lookup_fixed_size_batch(
        &self,
        views: &[GroupKeyArrayView<'_>],
        hashes: &[u64],
        nulls: &[bool],
    ) -> Result<Vec<Option<usize>>, ExecutionFailure> {
        if hashes.len() != nulls.len() {
            return Err(table_contract("join lookup batch length mismatch"));
        }
        let mut group_ids = vec![None; hashes.len()];
        for row in 0..hashes.len() {
            if nulls[row] {
                continue;
            }
            group_ids[row] = self.lookup_fixed_size(views, row, hashes[row])?;
        }
        Ok(group_ids)
    }

    pub(crate) fn lookup_compressed(
        &self,
        views: &[GroupKeyArrayView<'_>],
        row: usize,
        hash: u64,
    ) -> Result<Option<usize>, ExecutionFailure> {
        self.key_table.lookup_compressed(views, row, hash)
    }

    pub(crate) fn lookup_compressed_batch(
        &self,
        views: &[GroupKeyArrayView<'_>],
        keys: &[bool],
        rows: Option<&SerializedRows>,
        hashes: &[u64],
        nulls: &[bool],
    ) -> Result<Vec<Option<usize>>, ExecutionFailure> {
        if hashes.len() != nulls.len() || hashes.len() != keys.len() {
            return Err(table_contract("join lookup batch length mismatch"));
        }
        let mut group_ids = vec![None; hashes.len()];
        for row in 0..hashes.len() {
            if nulls[row] {
                continue;
            }
            let group_id = if keys[row] {
                self.lookup_compressed(views, row, hashes[row])?
            } else {
                let rows = rows.ok_or_else(|| table_contract("join row data missing"))?;
                let row_bytes = rows.row_bytes(row)?;
                self.lookup_serialized(row_bytes, hashes[row])?
            };
            group_ids[row] = group_id;
        }
        Ok(group_ids)
    }

    pub(crate) fn lookup_serialized_batch(
        &self,
        rows: &SerializedRows,
        hashes: &[u64],
        nulls: &[bool],
    ) -> Result<Vec<Option<usize>>, ExecutionFailure> {
        if hashes.len() != nulls.len() {
            return Err(table_contract("join lookup batch length mismatch"));
        }
        let mut group_ids = vec![None; hashes.len()];
        for row in 0..hashes.len() {
            if nulls[row] {
                continue;
            }
            let row_bytes = rows.row_bytes(row)?;
            group_ids[row] = self.lookup_serialized(row_bytes, hashes[row])?;
        }
        Ok(group_ids)
    }

    fn handle_lookup(&mut self, lookup: KeyLookup, row_id: u32) -> Result<(), ExecutionFailure> {
        if lookup.is_new {
            if lookup.group_id != self.group_head.len() {
                return Err(table_contract("join group id out of bounds"));
            }
            self.group_head.push(ROW_NONE);
        }
        self.link_row(lookup.group_id, row_id)
    }

    fn link_row(&mut self, group_id: usize, row_id: u32) -> Result<(), ExecutionFailure> {
        let head = self
            .group_head
            .get(group_id)
            .copied()
            .ok_or_else(|| table_contract("join group id out of bounds"))?;
        let slot = row_id as usize;
        if slot >= self.row_next.len() {
            return Err(table_contract("join row id out of bounds"));
        }
        self.row_next[slot] = head;
        self.group_head[group_id] = row_id;
        Ok(())
    }

    fn next_row(&self, row_id: u32) -> Result<u32, ExecutionFailure> {
        self.row_next
            .get(row_id as usize)
            .copied()
            .ok_or_else(|| table_contract("join row id out of bounds"))
    }

    fn refresh_accounting(&mut self) {
        let Some(tracker) = self.mem_tracker.as_ref() else {
            return;
        };
        let bytes = self.tracked_bytes();
        let delta = bytes - self.accounted_bytes;
        if delta > 0 {
            tracker.consume(delta);
        } else if delta < 0 {
            tracker.release(-delta);
        }
        self.accounted_bytes = bytes;
    }

    fn tracked_bytes(&self) -> i64 {
        fn vec_bytes<T>(v: &Vec<T>) -> i64 {
            let bytes = v.capacity().saturating_mul(mem::size_of::<T>());
            i64::try_from(bytes).unwrap_or(i64::MAX)
        }
        fn opt_vec_bytes<T>(v: &Option<Vec<T>>) -> i64 {
            v.as_ref().map(vec_bytes).unwrap_or(0)
        }

        vec_bytes(&self.group_head)
            .saturating_add(vec_bytes(&self.row_next))
            .saturating_add(opt_vec_bytes(&self.group_offsets))
            .saturating_add(opt_vec_bytes(&self.group_rows))
    }
}

impl Drop for JoinHashTable {
    fn drop(&mut self) {
        if let Some(tracker) = self.mem_tracker.as_ref() {
            tracker.release(self.accounted_bytes);
        }
    }
}

#[cfg(test)]
mod failure_tests {
    use super::*;
    use novarocks_failure_contract::{FailureClass, FailureProjection, ResourceDimension};

    #[test]
    fn row_index_limit_is_resource_and_does_not_reserve_the_sentinel() {
        assert_eq!(
            checked_row_count(u32::MAX as usize - 1, 1).unwrap(),
            u32::MAX as usize
        );
        assert_eq!(
            checked_row_count(u32::MAX as usize, 0).unwrap(),
            u32::MAX as usize
        );
        let error = checked_row_count(u32::MAX as usize, 1).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        let resource = fact.resource().unwrap();
        assert_eq!(resource.dimension, ResourceDimension::Positions);
        assert_eq!(resource.live, Some(u32::MAX as u64));
        assert_eq!(resource.requested, Some(1));
        assert_eq!(resource.limit, Some(u32::MAX as u64));
        assert_eq!(resource.committed, None);
        assert!(checked_row_count(usize::MAX, usize::MAX).is_err());
    }

    #[test]
    fn invalid_build_shape_is_rejected_before_row_storage_changes() {
        let tracker = MemTracker::new_root("join-shape-refusal");
        let weak = Arc::downgrade(&tracker);
        let mut table = JoinHashTable::new_with_tracker(
            vec![DataType::Int32],
            vec![false],
            Arc::clone(&tracker),
        )
        .unwrap();
        let arrays: Vec<arrow::array::ArrayRef> =
            vec![Arc::new(arrow::array::Int32Array::from(vec![1]))];
        let array_weak = Arc::downgrade(&arrays[0]);
        let before = tracker.current();
        let error = table.add_build_rows(&arrays, 2).unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        assert_eq!(table.row_count, 0);
        assert!(table.row_next.is_empty());
        assert_eq!(tracker.current(), before);
        drop(arrays);
        assert!(array_weak.upgrade().is_none());
        drop(table);
        assert_eq!(tracker.current(), 0);
        drop(tracker);
        assert!(weak.upgrade().is_none());
        assert!(!error.detail().is_empty());
    }
}
