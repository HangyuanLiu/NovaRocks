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

//! Frozen layout format, shared by the global wrapper and explicit owners.
pub use crate::lane::ATTRIBUTION_THRESHOLD_BYTES;
use std::alloc::Layout;
pub const ATTRIBUTION_TOKEN_BYTES: usize = 8;
pub const fn is_tagged(size: usize) -> bool {
    size >= ATTRIBUTION_THRESHOLD_BYTES
}
/// Preserves user alignment. The token occupies requested bytes, never usable-size slack.
pub fn tagged_layout(layout: Layout) -> Option<Layout> {
    let size = layout.size().checked_add(ATTRIBUTION_TOKEN_BYTES)?;
    Layout::from_size_align(size, layout.align()).ok()
}
