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

//! Fresh occurrences for actual Union normalization Project slot reads.
//!
//! The caller supplies actual emitted definitions and exact input channels.
//! The original program owners validate definition, root, type and lexical
//! correspondence afterward. No physical use or effect proof is reused here.

use crate::FragmentCompileError;
use novarocks_local_program::{
    ProgramChannelSite, ProgramExprId, ProgramExpressionArena, ProgramExpressionRootSite,
    ProgramExpressionUse, ProgramLexicalSource, ProgramNodeExpressionRole, ProgramNodeId,
    ProgramRootUseBinding, ProgramSlotBinding, ProgramUseRef,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ControlShape, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionEvaluationDomain, ExpressionUseId,
    MAX_CONTROL_DEFINITIONS, MAX_CONTROL_USE_REFERENCES, PureCompileControl,
};
use std::alloc::Layout;

pub(crate) struct UnionRoot {
    pub node: ProgramNodeId,
    pub ordinal: u32,
    pub definition: ProgramExprId,
    pub source: ProgramChannelSite,
}

/// Append only roots for actual normalization projects. On failure the caller
/// must discard the unpublished vectors, including any appended prefix.
/// Count/Layout checks and fallible requests are not a memory grant: caller
/// admission owns existing capacities, scratch and retained coexistence.
pub(crate) fn append_union_roots(
    roots: &[UnionRoot],
    domains: &mut Vec<ExpressionEvaluationDomain>,
    uses: &mut Vec<ProgramExpressionUse>,
    bindings: &mut Vec<ProgramRootUseBinding>,
    slots: &mut Vec<ProgramSlotBinding>,
    control: &dyn PureCompileControl,
) -> Result<(), FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let outcome = append(roots, domains, uses, bindings, slots, &mut work);
    if matches!(&outcome, Err(FragmentCompileError::Control(_))) {
        return outcome;
    }
    work.finish()?;
    outcome
}

fn append(
    roots: &[UnionRoot],
    domains: &mut Vec<ExpressionEvaluationDomain>,
    uses: &mut Vec<ProgramExpressionUse>,
    bindings: &mut Vec<ProgramRootUseBinding>,
    slots: &mut Vec<ProgramSlotBinding>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    let domain_count = count(domains.len(), roots.len(), MAX_CONTROL_DEFINITIONS)?;
    let use_count = count(uses.len(), roots.len(), MAX_CONTROL_USE_REFERENCES)?;
    let binding_count = count(bindings.len(), roots.len(), MAX_CONTROL_USE_REFERENCES)?;
    let slot_count = count(slots.len(), roots.len(), MAX_CONTROL_USE_REFERENCES)?;
    let mut references = use_count;
    for invocation in uses.iter() {
        references = count(
            references,
            invocation.arguments.len(),
            MAX_CONTROL_USE_REFERENCES,
        )?;
        work.step()?;
    }
    // Gate every output and scratch request before the first reserve. The
    // count-bounded candidate ranges contain enough free IDs by pigeonhole,
    // regardless of sparse existing IDs, including u32::MAX.
    layout::<ExpressionEvaluationDomain>(domain_count)?;
    layout::<ProgramExpressionUse>(use_count)?;
    layout::<ProgramRootUseBinding>(binding_count)?;
    layout::<ProgramSlotBinding>(slot_count)?;
    layout::<bool>(domain_count)?;
    layout::<bool>(use_count)?;
    if roots.is_empty() {
        return Ok(());
    }

    let mut occupied_domains = occupancy(domain_count, work)?;
    let mut occupied_uses = occupancy(use_count, work)?;
    for domain in domains.iter() {
        if let Ok(index) = usize::try_from(domain.id.get())
            && let Some(occupied) = occupied_domains.get_mut(index)
        {
            *occupied = true;
        }
        work.step()?;
    }
    for invocation in uses.iter() {
        if let Ok(index) = usize::try_from(invocation.context.use_id.get())
            && let Some(occupied) = occupied_uses.get_mut(index)
        {
            *occupied = true;
        }
        work.step()?;
    }
    reserve(domains, roots.len(), work)?;
    reserve(uses, roots.len(), work)?;
    reserve(bindings, roots.len(), work)?;
    reserve(slots, roots.len(), work)?;
    let mut next_domain = 0;
    let mut next_use = 0;
    for root in roots {
        let domain = EvaluationDomainId::new(fresh(&mut occupied_domains, &mut next_domain, work)?);
        let use_id = ExpressionUseId::new(fresh(&mut occupied_uses, &mut next_use, work)?);
        domains.push(ExpressionEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        });
        work.step()?;
        uses.push(ProgramExpressionUse {
            context: ExpressionEffectContext {
                use_id,
                domain,
                demand: EvaluationDemand::Value,
            },
            definition: root.definition,
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        work.step()?;
        bindings.push(ProgramRootUseBinding {
            site: ProgramExpressionRootSite::Node {
                node: root.node,
                role: ProgramNodeExpressionRole::ProjectOutput {
                    expression: root.ordinal,
                },
            },
            use_id,
        });
        work.step()?;
        slots.push(ProgramSlotBinding {
            occurrence: ProgramUseRef {
                arena: ProgramExpressionArena::Main,
                use_id,
            },
            source: ProgramLexicalSource::Input(root.source),
        });
        work.step()?;
    }
    Ok(())
}

fn count(left: usize, right: usize, maximum: usize) -> Result<usize, FragmentCompileError> {
    left.checked_add(right)
        .filter(|value| *value <= maximum)
        .ok_or_else(|| CompileControlError::ResourceExhausted.into())
}

fn layout<T>(count: usize) -> Result<(), FragmentCompileError> {
    Layout::array::<T>(count)
        .map(|_| ())
        .map_err(|_| CompileControlError::ResourceExhausted.into())
}

fn reserve<T>(
    output: &mut Vec<T>,
    additional: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    work.flush()?;
    output
        .try_reserve_exact(additional)
        .map_err(|_| CompileControlError::ResourceExhausted)?;
    work.step()?;
    work.flush()?;
    Ok(())
}

fn occupancy(
    count: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<bool>, FragmentCompileError> {
    let mut occupied = Vec::new();
    reserve(&mut occupied, count, work)?;
    for _ in 0..count {
        occupied.push(false);
        work.step()?;
    }
    Ok(occupied)
}

fn fresh(
    occupied: &mut [bool],
    next: &mut usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<u32, FragmentCompileError> {
    while let Some(slot) = occupied.get_mut(*next) {
        let taken = *slot;
        *slot = true;
        let candidate = *next;
        *next += 1;
        work.step()?;
        if !taken {
            return u32::try_from(candidate)
                .map_err(|_| CompileControlError::ResourceExhausted.into());
        }
    }
    Err(FragmentCompileError::Invalid(
        "Union normalization has no free count-bounded identity",
    ))
}
