#![allow(dead_code)]
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

//! Distributed-stage runtime-filter wiring.

use crate::sql::analysis::TypedExpr;
use crate::sql::planner::distributed::FragmentId;
use crate::sql::planner::physical::JoinExecutionMode;

#[derive(Clone, Debug)]
pub(crate) struct WiredRuntimeFilterBuild {
    pub filter_id: i32,
    pub build_expr: TypedExpr,
    pub probe_expr: TypedExpr,
    pub expr_order: usize,
    pub execution_mode: JoinExecutionMode,
    pub source_fragment_id: FragmentId,
    pub target_fragment_ids: Vec<FragmentId>,
}

#[derive(Clone, Debug)]
pub(crate) struct WiredRuntimeFilterProbe {
    pub filter_id: i32,
    pub probe_expr: TypedExpr,
    pub source_fragment_id: FragmentId,
}
