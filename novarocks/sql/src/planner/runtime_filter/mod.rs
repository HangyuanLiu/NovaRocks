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

//! Runtime-filter vocabulary retained by SQL's physical planner.
//!
//! Production uses the null-semantics and ordering declarations in `contract`.
//! The richer graph, activation, and progress models are available only to
//! unit tests.

#[cfg(test)]
pub(crate) mod activation;
#[cfg(test)]
pub(crate) mod comparator;
pub(crate) mod contract;
#[cfg(test)]
pub(crate) mod coverage;
#[cfg(test)]
pub(crate) mod graph;
#[cfg(test)]
pub(crate) mod policy;
#[cfg(test)]
pub(crate) mod progress;
#[cfg(test)]
pub(crate) mod sealed;
#[cfg(test)]
pub(crate) mod validation;
#[cfg(test)]
pub(crate) mod wait_graph;
