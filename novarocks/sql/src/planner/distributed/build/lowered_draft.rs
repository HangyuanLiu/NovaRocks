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

//! SQL-owned lowering sources retained beside the immutable physical plan.

use std::{collections::BTreeMap, sync::Arc};

use novarocks_physical_plan::{
    AggregateCall, AggregateCallId, AggregatePhase, ExprId, Fragment, FragmentId, NodeKind,
    PhysicalCallSite, PhysicalNode, PhysicalPlan, PlanAnnotation, PlanBuilder,
    PlanConstructionError, ValueId,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use crate::binding::{CapturedAggregateLogicalRequest, CapturedLogicalCallArguments};

/// A completed SQL source owner. Its physical view is read-only; consuming a
/// statement or DML result transfers this entire owner, including its original
/// logical-source journal. This is not certification of fresh kernel coverage,
/// a neutral wire source, a FragmentPackage, or a runtime allocation grant.
#[derive(Clone)]
pub struct SqlAuthoredPhysicalPlan {
    plan: Arc<PhysicalPlan>,
    call_sources: Arc<SqlLogicalSourceJournal>,
    functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
}
impl std::fmt::Debug for SqlAuthoredPhysicalPlan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SqlAuthoredPhysicalPlan")
            .field("plan", &self.plan)
            .field("call_sources", &self.call_sources)
            .field("functions", &"retained immutable catalogue")
            .finish()
    }
}
impl SqlAuthoredPhysicalPlan {
    /// The original completion snapshot moves with this source owner. Fresh
    /// preparation borrows it directly; it must never capture a second one.
    pub(crate) const fn function_catalog(&self) -> &Arc<dyn crate::compiler::SqlFunctionCatalog> {
        &self.functions
    }

    pub fn plan(&self) -> &PhysicalPlan {
        &self.plan
    }
    pub const fn plan_arc(&self) -> &Arc<PhysicalPlan> {
        &self.plan
    }

    /// Loan an ordinary SQL call's original request from its exact emission.
    /// A physical expression's constant shape does not reconstruct presence.
    /// This does not provide late typed projection, use/domain scope or effects.
    pub(crate) fn checked_scalar_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        source: &'a novarocks_physical_plan::ExprNode,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedScalarLogicalSourceEntry<'a>, AggregateSourceJournalError> {
        work.flush()?;
        let same_fragment = self
            .plan
            .fragments()
            .get(&fragment.id())
            .is_some_and(|original| std::ptr::eq(original, fragment));
        work.step()?;
        let same_expression = fragment
            .expressions()
            .get(source.id)
            .is_some_and(|original| std::ptr::eq(original, source));
        work.step()?;
        if !same_fragment || !same_expression {
            return Err(AggregateSourceJournalError::InvalidSource(
                "scalar journal loans a foreign plan or expression",
            ));
        }
        let entry = self
            .call_sources
            .scalar_entries
            .get(&(fragment.id(), source.id));
        work.step()?;
        let entry = entry.ok_or(AggregateSourceJournalError::MissingEntry)?;
        validate_scalar_source_entry_observed(entry, source, work)?;
        work.flush()?;
        Ok(CheckedScalarLogicalSourceEntry {
            entry,
            fragment,
            source,
        })
    }

    /// Only an entry sealed by the actual lowering producer can loan an
    /// authenticated request. Same-signature foreign plans cannot supply it.
    pub(crate) fn checked_aggregate_source_observed<'a>(
        &'a self,
        fragment: &'a Fragment,
        node: &'a PhysicalNode,
        site: PhysicalCallSite,
        source: &'a AggregateCall,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<CheckedAggregateLogicalSourceEntry<'a>, AggregateSourceJournalError> {
        work.flush()?;
        let original_fragment = self.plan.fragments().get(&fragment.id());
        let same_fragment = original_fragment.is_some_and(|value| std::ptr::eq(value, fragment));
        work.step()?;
        let same_node = fragment
            .nodes()
            .get(&node.id)
            .is_some_and(|value| std::ptr::eq(value, node));
        work.step()?;
        if !same_fragment || !same_node {
            return Err(AggregateSourceJournalError::InvalidSource(
                "aggregate journal loans a foreign plan or node",
            ));
        }
        let actual = match (site, &node.kind) {
            (PhysicalCallSite::Aggregate { node: id, call }, NodeKind::Aggregate { calls, .. })
                if id == node.id =>
            {
                calls.get(call as usize)
            }
            (
                PhysicalCallSite::TopNState { node: id, call },
                NodeKind::TopN {
                    reduction: novarocks_physical_plan::TopNReduction::GroupedStates { calls, .. },
                    ..
                },
            ) if id == node.id => calls.get(call as usize),
            _ => None,
        };
        let same_call = actual.is_some_and(|value| std::ptr::eq(value, source));
        work.step()?;
        if !same_call {
            return Err(AggregateSourceJournalError::InvalidSource(
                "aggregate journal call differs from its original site",
            ));
        }
        let entry = self.call_sources.entries.get(&(fragment.id(), site));
        work.step()?;
        let entry = entry.ok_or(AggregateSourceJournalError::MissingEntry)?;
        let same_phase = entry.phase == source.binding.phase;
        let same_call_id = entry.target == AggregateSourceTarget::Aggregate(source.id);
        work.step()?;
        if !same_phase || !same_call_id {
            return Err(AggregateSourceJournalError::InvalidSource(
                "aggregate journal phase or producer association differs",
            ));
        }
        let expected_state = match entry.phase {
            AggregatePhase::Single | AggregatePhase::Partial { .. } => {
                AggregateRuntimeDemand::Update
            }
            AggregatePhase::Intermediate { .. } | AggregatePhase::Final { .. } => {
                if source.arguments.len() != 1 || !source.order_by.is_empty() || source.distinct {
                    return Err(AggregateSourceJournalError::InvalidSource(
                        "aggregate merge has invalid physical state channels",
                    ));
                }
                AggregateRuntimeDemand::ExpressionState(source.arguments[0])
            }
        };
        work.step()?;
        if entry.runtime != expected_state {
            return Err(AggregateSourceJournalError::InvalidSource(
                "aggregate journal runtime state differs from its producer",
            ));
        }
        let captured = match &entry.logical {
            LoweredAggregateLogicalSource::Captured(captured) => captured,
            LoweredAggregateLogicalSource::Uncertified => {
                return Err(AggregateSourceJournalError::MissingLogicalSource);
            }
        };
        work.step()?;
        work.flush()?;
        Ok(CheckedAggregateLogicalSourceEntry {
            captured,
            phase: entry.phase,
            runtime: entry.runtime,
            fragment,
            node,
            site,
            source,
        })
    }
}

/// Only the source owner can create this original emission loan. There is no
/// conversion from a binding, a selected signature or captured data alone.
pub(crate) struct CheckedScalarLogicalSourceEntry<'a> {
    entry: &'a LoweredScalarSourceEntry,
    fragment: &'a Fragment,
    source: &'a novarocks_physical_plan::ExprNode,
}
impl<'a> CheckedScalarLogicalSourceEntry<'a> {
    pub(crate) const fn captured(&self) -> &'a CapturedLogicalCallArguments {
        &self.entry.captured
    }
    pub(crate) const fn fragment(&self) -> &'a Fragment {
        self.fragment
    }
    pub(crate) const fn source(&self) -> &'a novarocks_physical_plan::ExprNode {
        self.source
    }
    pub(crate) fn arguments(&self) -> &'a [ExprId] {
        &self.entry.arguments
    }
}

pub(super) fn validate_scalar_source_entry_observed(
    entry: &LoweredScalarSourceEntry,
    source: &novarocks_physical_plan::ExprNode,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), AggregateSourceJournalError> {
    let same_scope = source.owner == entry.owner && source.lambda_scope == entry.lambda_scope;
    work.step()?;
    let novarocks_physical_plan::ExprKind::FunctionCall { args, .. } = &source.kind else {
        return Err(AggregateSourceJournalError::InvalidSource(
            "scalar journal site is not its original function emission",
        ));
    };
    let same_count = args.len() == entry.arguments.len();
    work.step()?;
    if !same_scope || !same_count {
        return Err(AggregateSourceJournalError::InvalidSource(
            "scalar journal scope or channel count differs from its emission",
        ));
    }
    for (actual, original) in args.iter().zip(entry.arguments.iter()) {
        let same_argument = actual == original;
        work.step()?;
        if !same_argument {
            return Err(AggregateSourceJournalError::InvalidSource(
                "scalar journal channel differs from its original expression identity",
            ));
        }
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) enum AggregateSourceJournalError {
    Control(CompileControlError),
    MissingEntry,
    MissingLogicalSource,
    InvalidSource(&'static str),
}
impl From<CompileControlError> for AggregateSourceJournalError {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}

/// A borrowed original producer association. Construction is private to this
/// journal; type/signature equality never creates one. Selected correspondence
/// and actual runtime state-domain validation remain their sole owners.
pub(crate) struct CheckedAggregateLogicalSourceEntry<'a> {
    captured: &'a CapturedAggregateLogicalRequest,
    phase: AggregatePhase,
    runtime: AggregateRuntimeDemand,
    fragment: &'a Fragment,
    node: &'a PhysicalNode,
    site: PhysicalCallSite,
    source: &'a AggregateCall,
}
impl<'a> CheckedAggregateLogicalSourceEntry<'a> {
    pub(crate) const fn captured(&self) -> &'a CapturedAggregateLogicalRequest {
        self.captured
    }
    pub(crate) const fn phase(&self) -> AggregatePhase {
        self.phase
    }
    pub(crate) const fn runtime(&self) -> AggregateRuntimeDemand {
        self.runtime
    }
    pub(crate) const fn fragment(&self) -> &'a Fragment {
        self.fragment
    }
    pub(crate) const fn node(&self) -> &'a PhysicalNode {
        self.node
    }
    pub(crate) const fn site(&self) -> PhysicalCallSite {
        self.site
    }
    pub(crate) const fn source(&self) -> &'a AggregateCall {
        self.source
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AggregateSourceTarget {
    Aggregate(AggregateCallId),
    Writer(ValueId),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AggregateRuntimeDemand {
    Update,
    ExpressionState(ExprId),
    WriterState(ValueId),
}
#[derive(Debug)]
pub(super) enum LoweredAggregateLogicalSource {
    Captured(CapturedAggregateLogicalRequest),
    /// Existing manual/structural IR explicitly lacks an authenticated update.
    /// Retention cannot upgrade it; a fresh-source loan returns MissingLogicalSource.
    Uncertified,
}
impl LoweredAggregateLogicalSource {
    pub(super) fn captured(&self) -> Option<&CapturedAggregateLogicalRequest> {
        match self {
            Self::Captured(source) => Some(source),
            Self::Uncertified => None,
        }
    }
}
#[derive(Debug)]
pub(super) struct LoweredAggregateSourceEntry {
    pub(super) logical: LoweredAggregateLogicalSource,
    pub(super) phase: AggregatePhase,
    pub(super) runtime: AggregateRuntimeDemand,
    pub(super) target: AggregateSourceTarget,
}
#[derive(Debug)]
pub(super) struct LoweredScalarSourceEntry {
    pub(super) captured: CapturedLogicalCallArguments,
    pub(super) owner: novarocks_physical_plan::NodeId,
    pub(super) lambda_scope: Option<ExprId>,
    pub(super) arguments: Box<[ExprId]>,
}
#[derive(Debug)]
pub(super) struct SqlLogicalSourceJournal {
    // Only actual ordinary TypedExpr::FunctionCall emissions are recorded.
    // Synthetic conversion/VARIANT calls remain a distinct open source gate.
    pub(super) scalar_entries: BTreeMap<(FragmentId, ExprId), LoweredScalarSourceEntry>,
    pub(super) entries: BTreeMap<(FragmentId, PhysicalCallSite), LoweredAggregateSourceEntry>,
}

/// Sole actual lowering result. Only the original visitor constructs this;
/// there is no raw-builder conversion or consuming builder accessor.
pub(crate) struct LoweredSqlPhysicalDraft {
    builder: PlanBuilder,
    call_sources: SqlLogicalSourceJournal,
    functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
}
impl LoweredSqlPhysicalDraft {
    pub(super) fn from_lowering(
        builder: PlanBuilder,
        call_sources: SqlLogicalSourceJournal,
        functions: Arc<dyn crate::compiler::SqlFunctionCatalog>,
    ) -> Self {
        Self {
            builder,
            call_sources,
            functions,
        }
    }
    pub(crate) fn add_annotation(&mut self, annotation: PlanAnnotation) {
        self.builder.add_annotation(annotation);
    }
    pub(crate) fn finish_observed(
        self,
        control: &dyn PureCompileControl,
    ) -> Result<SqlAuthoredPhysicalPlan, PlanConstructionError> {
        let control_error = |cause| {
            PlanConstructionError::Constants(
                novarocks_physical_plan::ConstantReferenceError::Control(cause),
            )
        };
        let mut work =
            CompileCheckpoints::try_new(control, CompilePhase::Validate).map_err(control_error)?;
        let result = (|| {
            work.flush().map_err(control_error)?;
            let plan = self.builder.finish_observed(control)?;
            work.flush().map_err(control_error)?;
            let plan = Arc::new(plan);
            work.step().map_err(control_error)?;
            work.flush().map_err(control_error)?;
            let call_sources = Arc::new(self.call_sources);
            work.step().map_err(control_error)?;
            // The source journal moves with this exact immutable plan. The
            // two opaque Arc allocations retain caller admission obligations.
            Ok(SqlAuthoredPhysicalPlan {
                plan,
                call_sources,
                functions: self.functions,
            })
        })();
        if matches!(
            &result,
            Err(PlanConstructionError::Constants(
                novarocks_physical_plan::ConstantReferenceError::Control(_)
            ))
        ) {
            return result;
        }
        work.finish().map_err(control_error)?;
        result
    }
}

#[cfg(test)]
#[path = "lowered_draft_publication_tests.rs"]
mod publication_tests;
