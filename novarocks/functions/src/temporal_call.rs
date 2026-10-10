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
//! Typed operational source channels retained by one verified call contract.
use crate::{CallArgumentUses, CallEffectInput, FunctionValueType, KernelFailure};
use novarocks_type_contract::{
    CompileCheckpoints, ExpressionEffectContext, TemporalSourceFacts, TemporalSourceRole,
};
#[derive(Clone, Copy, Debug)]
pub struct TemporalSourceChannel<'a> {
    pub role: TemporalSourceRole,
    pub context: ExpressionEffectContext,
    pub value_type: &'a FunctionValueType,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedTemporalSource {
    pub role: TemporalSourceRole,
    pub context: ExpressionEffectContext,
    pub value_type: FunctionValueType,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalCallContract {
    pub facts: TemporalSourceFacts,
    pub sources: Box<[PreparedTemporalSource]>,
}
pub(crate) fn own_temporal_call(
    input: CallEffectInput<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<TemporalCallContract>, KernelFailure> {
    let CallArgumentUses::TemporalSources { facts, channels } = input.argument_uses else {
        return Ok(None);
    };
    facts
        .validate()
        .map_err(|_| crate::kernel_control::invalid("invalid temporal source fact"))?;
    if channels.len() != facts.shape().source_count() {
        return Err(crate::kernel_control::invalid(
            "invalid temporal source count",
        ));
    }
    for _ in facts.cast_chain() {
        work.step()
            .map_err(crate::kernel_control::compile_failure)?;
    }
    work.flush()
        .map_err(crate::kernel_control::compile_failure)?;
    let facts = facts.clone();
    work.flush()
        .map_err(crate::kernel_control::compile_failure)?;
    let mut sources = Vec::new();
    sources
        .try_reserve_exact(channels.len())
        .map_err(|_| KernelFailure::ResourceExhausted)?;
    work.flush()
        .map_err(crate::kernel_control::compile_failure)?;
    for channel in channels {
        crate::kernel_input::validate_type_observed(channel.value_type, work)?;
        work.flush()
            .map_err(crate::kernel_control::compile_failure)?;
        sources.push(PreparedTemporalSource {
            role: channel.role,
            context: channel.context,
            value_type: channel.value_type.clone(),
        });
        work.flush()
            .map_err(crate::kernel_control::compile_failure)?;
    }
    work.flush()
        .map_err(crate::kernel_control::compile_failure)?;
    let sources = sources.into_boxed_slice();
    work.flush()
        .map_err(crate::kernel_control::compile_failure)?;
    Ok(Some(TemporalCallContract { facts, sources }))
}
