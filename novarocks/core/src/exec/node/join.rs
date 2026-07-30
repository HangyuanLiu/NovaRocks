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
use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprId;
use crate::exec::node::ExecNode;
use crate::exec::node::runtime_filter::{
    NativeRuntimeFilterContract, NativeRuntimeFilterReduction,
};
use crate::runtime_filter::model::contract::{CompletionRequirement, ContributionKind};
use std::collections::BTreeSet;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    RightOuter,
    FullOuter,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    NullAwareLeftAnti,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum JoinDistributionMode {
    Broadcast,
    Partitioned,
}

#[derive(Clone, Debug)]
pub(crate) struct NativeJoinRuntimeFilterProducerSpec {
    pub(crate) binding_id: u32,
    pub(crate) channel_id: u32,
    pub(crate) build_expr_id: ExprId,
    pub(crate) build_key_index: usize,
    pub(crate) contribution_kinds: BTreeSet<ContributionKind>,
    pub(crate) completion_requirement: CompletionRequirement,
    pub(crate) contract: NativeRuntimeFilterContract,
    pub(crate) reduction: NativeRuntimeFilterReduction,
}

#[derive(Clone, Debug)]
pub struct JoinRuntimeFilterExecution {
    pub(crate) producers: Vec<NativeJoinRuntimeFilterProducerSpec>,
}

impl JoinRuntimeFilterExecution {
    /// Compat fragments do not carry native runtime-filter producers.
    pub const fn empty() -> Self {
        Self {
            producers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct JoinNode {
    pub left: Box<ExecNode>,
    pub right: Box<ExecNode>,
    pub node_id: i32,
    pub join_type: JoinType,
    pub distribution_mode: JoinDistributionMode,
    pub left_chunk_schema: ChunkSchemaRef,
    pub right_chunk_schema: ChunkSchemaRef,
    pub join_scope_chunk_schema: ChunkSchemaRef,
    pub probe_keys: Vec<ExprId>,
    pub build_keys: Vec<ExprId>,
    /// Null-safe flags aligned with join key pairs from FE eq_join_conjuncts.
    /// `true` means this key uses null-safe equality (`<=>` / EQ_FOR_NULL).
    pub eq_null_safe: Vec<bool>,
    pub residual_predicate: Option<ExprId>,
    pub runtime_filter_execution: JoinRuntimeFilterExecution,
}

impl JoinNode {
    pub fn left_schema(&self) -> arrow::datatypes::SchemaRef {
        self.left_chunk_schema.arrow_schema_ref()
    }

    pub fn right_schema(&self) -> arrow::datatypes::SchemaRef {
        self.right_chunk_schema.arrow_schema_ref()
    }

    pub fn join_scope_schema(&self) -> arrow::datatypes::SchemaRef {
        self.join_scope_chunk_schema.arrow_schema_ref()
    }
}
