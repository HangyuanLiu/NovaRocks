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

//! Actual owned input chunks for a function-major window invocation. Moving
//! the Chunk preserves its original backing and source lease. Only the new
//! vector backing is charged here; its size observation grants no payload copy.
use crate::exec::chunk::Chunk;
use novarocks_functions::{
    AggregateStateAllocator, KernelEvaluationControl, KernelFailure, WindowInvocationScratch,
};
use std::sync::Arc;

pub(super) struct InvocationInputBuffer {
    // Moving chunks preserves source backing/lease; only this exact structural
    // allocation and its metadata are delegated to the real host here.
    chunks: WindowInvocationScratch<Chunk>,
}
impl InvocationInputBuffer {
    pub(super) fn try_new(
        host: Arc<dyn AggregateStateAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        Ok(Self {
            chunks: WindowInvocationScratch::try_new(host, control)?,
        })
    }
    pub(super) fn push(
        &mut self,
        chunk: Chunk,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        self.chunks.try_push(chunk, control)
    }
    pub(super) fn chunks(&self) -> &[Chunk] {
        self.chunks.as_slice()
    }
}
