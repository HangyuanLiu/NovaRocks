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

//! Core-only Parquet scan configuration and variant column adaptation.
//!
//! Byte access, metadata/page caching, projection, pruning, ranges and
//! physical decoding are owned by `novarocks-fs`.

#[path = "parquet/variant_pruning.rs"]
mod variant_pruning;
#[path = "parquet/variant_read.rs"]
mod variant_read;

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field};

use novarocks_execution::exec::chunk::ChunkSchemaRef;
pub(crate) use novarocks_execution::exec::min_max_predicate::MinMaxPredicate;
use novarocks_fs::DataCacheContext;
use novarocks_types::SlotId;
pub use variant_pruning::VariantPathPruningPredicate;
pub use variant_read::{
    collapse_variant_struct_to_largebinary, convert_variant_columns, is_variant_struct_data_type,
    materialize_variant_path_columns,
};

#[derive(Clone, Debug)]
pub struct VariantPathSpec {
    pub source_slot_id: SlotId,
    pub source_read_slot_id: SlotId,
    pub output_slot_id: SlotId,
    pub source_field_id: Option<i32>,
    pub source_name: String,
    pub output_name: String,
    pub source_field: Field,
    pub output_field: Field,
    pub canonical_path: String,
    pub requested_type: DataType,
    pub strict: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ParquetSlotKind {
    Regular,
    Variant,
}

impl ParquetSlotKind {
    pub(crate) fn is_variant(self) -> bool {
        self == Self::Variant
    }
}

#[derive(Clone, Debug)]
pub struct ParquetScanConfig {
    pub columns: Vec<String>,
    pub chunk_schema: ChunkSchemaRef,
    pub slot_kinds: Vec<ParquetSlotKind>,
    pub case_sensitive: bool,
    pub enable_page_index: bool,
    pub min_max_predicates: Vec<MinMaxPredicate>,
    pub runtime_min_max_filter_columns: HashMap<i32, String>,
    pub variant_path_predicates: Vec<VariantPathPruningPredicate>,
    pub batch_size: Option<usize>,
    pub datacache: DataCacheContext,
    pub cache_policy: ParquetReadCachePolicy,
    pub profile_label: Option<String>,
    pub variant_path_columns: Vec<VariantPathSpec>,
    pub query_global_dicts: novarocks_execution::exec::dict_encode::QueryGlobalDictEncodeMap,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParquetReadCachePolicy {
    pub enable_metacache: bool,
    pub enable_pagecache: bool,
    pub page_cache_min_read_bytes: usize,
    pub page_cache_max_read_bytes: usize,
    pub page_cache_evict_probability: Option<u32>,
}

impl ParquetReadCachePolicy {
    pub const DEFAULT_PAGE_CACHE_MIN_READ_BYTES: usize = 1024;
    pub const DEFAULT_PAGE_CACHE_MAX_READ_BYTES: usize = 2 * 1024 * 1024;

    pub fn with_flags(
        enable_metacache: bool,
        enable_pagecache: bool,
        page_cache_evict_probability: Option<u32>,
    ) -> Self {
        Self {
            enable_metacache,
            enable_pagecache,
            page_cache_min_read_bytes: Self::DEFAULT_PAGE_CACHE_MIN_READ_BYTES,
            page_cache_max_read_bytes: Self::DEFAULT_PAGE_CACHE_MAX_READ_BYTES,
            page_cache_evict_probability,
        }
    }

    pub fn should_cache_page_read(&self, length: usize) -> bool {
        self.enable_pagecache
            && (self.page_cache_min_read_bytes..=self.page_cache_max_read_bytes).contains(&length)
    }
}
