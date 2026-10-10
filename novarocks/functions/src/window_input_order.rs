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

//! The original analytic partition-only regroup decision. The source authors
//! supply explicit frame presence and typed window-family eligibility; an
//! effective full frame or a name is never used as original source evidence.
pub fn should_regroup_partition_only(
    has_order_keys: bool,
    has_explicit_frame: bool,
    eligible_calls: impl IntoIterator<Item = bool>,
) -> bool {
    !has_order_keys && !has_explicit_frame && eligible_calls.into_iter().all(|eligible| eligible)
}
