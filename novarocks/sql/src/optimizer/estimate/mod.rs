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

//! Pure-function statistics kernel: a single source of truth for saturating
//! arithmetic, join cardinality, predicate selectivity and NDV propagation.
//! Both the Cascades `stats` derivation and the join-reorder `cardinality`
//! walker delegate here so they never drift numerically.

pub(crate) mod arith;
pub(crate) mod cardinality;
pub(crate) mod join_condition;
pub(crate) mod ndv;
pub(crate) mod selectivity;
