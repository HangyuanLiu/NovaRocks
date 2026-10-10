// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The original fixed scratch geometry of the borrowed type grammar.
//! This stack layout is not a heap request, allocator/CPU bound or MEM grant.
//! Callers admit initialization work before creating the actual scratch.

use arrow_schema::DataType;
use std::alloc::Layout;

/// The original borrowed traversal stack, including every slot's occupancy.
pub type TypeValidationScratch<'a> = [Option<(&'a DataType, usize)>; crate::MAX_VALUE_TYPE_NODES];

/// Actual backing layout; this function does not initialize or allocate it.
pub fn scratch_layout() -> Layout {
    Layout::new::<TypeValidationScratch<'_>>()
}

/// Byte work for one opaque initialization of the complete fixed backing.
/// Slot count is not the byte cost, and no internal cooperation is claimed.
pub fn scratch_work_upper_bound() -> usize {
    scratch_layout().size()
}
