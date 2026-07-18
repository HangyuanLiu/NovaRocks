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
//! Native StarRocks page decoding pipeline.
//!
//! Module split:
//! - `footer`: protobuf-lite page footer structs.
//! - `envelope`: checksum/footer validation and decompression.
//! - `data_page`: page-level decode control flow.
//! - `index_page`: index-page entry decode for ordinal/page indexes.
//!
//! Current limitations:
//! - Nullable page decoding supports format-v2 nullmaps only.
//! - Page decompression supports `NO_COMPRESSION` and `LZ4_FRAME` only.

mod data_page;
mod envelope;
mod footer;
mod index_page;

pub(super) use data_page::{
    DecodedDataPageValues, DecodedPageValuePayload, decode_data_page_values, fixed_value_size_bytes,
};
pub(super) use envelope::{decode_page_envelope, slice_page_bytes};
pub(super) use index_page::{DecodedIndexPageEntry, IndexPageNodeType, decode_index_page};
