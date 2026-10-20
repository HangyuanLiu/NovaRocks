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
use crate::runtime_filter::RuntimeFilterProducerContract;

use crate::exec::chunk::ChunkSchemaRef;
use crate::exec::expr::ExprId;

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
pub struct JoinRuntimeFilterProducerBinding {
    pub build_expr_id: ExprId,
    pub build_key_index: usize,
    pub contract: RuntimeFilterProducerContract,
}

#[derive(Clone, Debug)]
pub struct JoinRuntimeFilterExecution {
    pub producers: Vec<JoinRuntimeFilterProducerBinding>,
}

impl JoinRuntimeFilterExecution {
    /// Compat fragments do not carry native runtime-filter producers.
    pub const fn empty() -> Self {
        Self {
            producers: Vec::new(),
        }
    }

    pub const fn new(producers: Vec<JoinRuntimeFilterProducerBinding>) -> Self {
        Self { producers }
    }
}

impl JoinRuntimeFilterProducerBinding {
    pub const fn new(
        build_expr_id: ExprId,
        build_key_index: usize,
        contract: RuntimeFilterProducerContract,
    ) -> Self {
        Self {
            build_expr_id,
            build_key_index,
            contract,
        }
    }

    pub const fn contract(&self) -> &RuntimeFilterProducerContract {
        &self.contract
    }

    pub const fn binding_id(&self) -> u32 {
        self.contract.binding_id().get()
    }

    pub const fn channel_id(&self) -> u32 {
        self.contract.channel_id().get()
    }
}
