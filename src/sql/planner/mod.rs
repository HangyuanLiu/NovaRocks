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

//! Planner — converts analyzed SQL into logical plans and distributed Bridge 2 IR.
//!
//! This is a structural transformation that builds a relational algebra tree
//! from the analyzed query IR. It also owns Bridge 2, which materializes
//! physical optimizer plans into planner-side distributed plan fragments before
//! codegen encodes them into NovaRocks native fragments.

pub(crate) mod distributed;
pub(crate) mod imv_rewrite;
pub(crate) mod logical;
pub(crate) mod optimizer_bridge;
pub(crate) mod ordering;
pub(crate) mod payload;
pub(crate) mod physical;
pub(crate) use logical::build::{plan_output_columns, plan_query};
