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

//! Checked diagnostic provenance for the actual local execution entities.
//! Diagnostic sources never replace an exact runtime binding identity.

use crate::{MAX_PROGRAM_NODES, ProgramNodeId};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};

/// Sparse identity within one compiled program/profile. Several local
/// operators may originate from the same physical node and remain distinct.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LocalOperatorId(u32);
impl LocalOperatorId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}
/// Fragment-local physical node used for diagnostics only. The compiler
/// supplies the exact allowed physical node set from its checked package.
/// It cannot address a scan/writer/exchange binding through this identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DiagnosticSourceNodeId(u32);
impl DiagnosticSourceNodeId {
    pub const fn new(value: u32) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyntheticOperatorReason {
    HashOwnership,
    Gather,
    ExpressionControl,
    StateMerge,
    WriterCohort,
    ResultSink,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalOperatorOrigin {
    /// One directly represented entity for this physical source. Additional
    /// appearances require an explicit Fused, Split or Synthetic origin;
    /// this does not assert a global bijection for the diagnostic source.
    Direct,
    Fused,
    /// A stable piece within one physical source, never an arrival ordinal.
    Split {
        piece: u32,
    },
    Synthetic {
        reason: SyntheticOperatorReason,
    },
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricAggregation {
    Sum,
    Maximum,
}
/// Each metric's rule is explicit; a counter's unit does not choose it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperatorMetricAggregation {
    pub cpu_time: MetricAggregation,
    pub wall_time: MetricAggregation,
    pub peak_retained_bytes: MetricAggregation,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalOperatorProvenance {
    pub id: LocalOperatorId,
    pub lowered_nodes: Box<[ProgramNodeId]>,
    pub sources: Box<[DiagnosticSourceNodeId]>,
    pub origin: LocalOperatorOrigin,
    /// One actual entity records a shared cost exactly once. Other entities
    /// associate themselves with that owner instead of copying its counters.
    pub cost_owner: LocalOperatorId,
    pub metrics: OperatorMetricAggregation,
}

pub const MAX_PROFILE_OPERATORS: usize = MAX_PROGRAM_NODES;
pub const MAX_PROFILE_REFERENCES: usize = 262_144;

#[derive(Clone, Debug)]
pub struct ProgramProvenance {
    operators: Arc<BTreeMap<LocalOperatorId, LocalOperatorProvenance>>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProvenanceError {
    Control(CompileControlError),
    Empty,
    DuplicateOperator,
    InvalidLoweredNode,
    DuplicateLoweredNode,
    InvalidSource,
    DuplicateSource,
    InvalidOrigin,
    DuplicateSplitPiece,
    DuplicateDirectSource,
    UncoveredLoweredNode,
    MissingCostOwner,
    IndirectCostOwner,
    CostMetricMismatch,
}
impl fmt::Display for ProvenanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid program provenance: {self:?}")
    }
}
impl std::error::Error for ProvenanceError {}
impl From<CompileControlError> for ProvenanceError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl ProgramProvenance {
    pub fn try_new(
        operators: Vec<LocalOperatorProvenance>,
        lowered_node_count: usize,
        allowed_sources: &BTreeSet<DiagnosticSourceNodeId>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ProvenanceError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        if operators.len() > MAX_PROFILE_OPERATORS
            || lowered_node_count > MAX_PROGRAM_NODES
            || allowed_sources.len() > MAX_PROGRAM_NODES
        {
            return Err(CompileControlError::ResourceExhausted.into());
        }
        if operators.is_empty() {
            return Err(ProvenanceError::Empty);
        }
        let mut reference_count = 0usize;
        // Bound the complete graph before indexes/normalization allocations.
        for operator in &operators {
            reference_count = reference_count
                .checked_add(operator.lowered_nodes.len())
                .and_then(|n| n.checked_add(operator.sources.len()))
                .ok_or(CompileControlError::ResourceExhausted)?;
            if reference_count > MAX_PROFILE_REFERENCES {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            work.step()?;
        }
        let mut covered = vec![false; lowered_node_count];
        let mut indexed = BTreeMap::new();
        let mut split_pieces = BTreeSet::new();
        let mut direct_sources = BTreeSet::new();
        for mut operator in operators {
            if indexed.contains_key(&operator.id) {
                return Err(ProvenanceError::DuplicateOperator);
            }
            match operator.origin {
                LocalOperatorOrigin::Direct | LocalOperatorOrigin::Split { .. }
                    if operator.lowered_nodes.len() != 1 || operator.sources.len() != 1 =>
                {
                    return Err(ProvenanceError::InvalidOrigin);
                }
                LocalOperatorOrigin::Fused
                    if operator.sources.is_empty()
                        || operator.lowered_nodes.is_empty()
                        || (operator.sources.len() < 2 && operator.lowered_nodes.len() < 2) =>
                {
                    return Err(ProvenanceError::InvalidOrigin);
                }
                _ => {}
            }
            let mut nodes = BTreeSet::new();
            for node in &operator.lowered_nodes {
                let Some(covered) = covered.get_mut(node.index()) else {
                    return Err(ProvenanceError::InvalidLoweredNode);
                };
                if !nodes.insert(*node) {
                    return Err(ProvenanceError::DuplicateLoweredNode);
                }
                *covered = true;
                work.step()?;
            }
            let mut sources = BTreeSet::new();
            for source in &operator.sources {
                if !allowed_sources.contains(source) {
                    return Err(ProvenanceError::InvalidSource);
                }
                if !sources.insert(*source) {
                    return Err(ProvenanceError::DuplicateSource);
                }
                work.step()?;
            }
            if let LocalOperatorOrigin::Split { piece } = operator.origin
                && !split_pieces.insert((operator.sources[0], piece))
            {
                return Err(ProvenanceError::DuplicateSplitPiece);
            }
            if operator.origin == LocalOperatorOrigin::Direct
                && !direct_sources.insert(operator.sources[0])
            {
                return Err(ProvenanceError::DuplicateDirectSource);
            }
            // These are semantic sets. Canonical ordering changes no source
            // occurrence or runtime identity and never indexes by max ID.
            let mut normalized_nodes = Vec::with_capacity(nodes.len());
            for node in nodes {
                normalized_nodes.push(node);
                work.step()?;
            }
            operator.lowered_nodes = normalized_nodes.into_boxed_slice();
            let mut normalized_sources = Vec::with_capacity(sources.len());
            for source in sources {
                normalized_sources.push(source);
                work.step()?;
            }
            operator.sources = normalized_sources.into_boxed_slice();
            if indexed.insert(operator.id, operator).is_some() {
                return Err(ProvenanceError::DuplicateOperator);
            }
            work.step()?;
        }
        for operator in indexed.values() {
            let Some(owner) = indexed.get(&operator.cost_owner) else {
                return Err(ProvenanceError::MissingCostOwner);
            };
            if owner.cost_owner != owner.id {
                return Err(ProvenanceError::IndirectCostOwner);
            }
            if owner.metrics != operator.metrics {
                return Err(ProvenanceError::CostMetricMismatch);
            }
            work.step()?;
        }
        for covered in covered {
            if !covered {
                return Err(ProvenanceError::UncoveredLoweredNode);
            }
            work.step()?;
        }
        work.finish()?;
        Ok(Self {
            operators: Arc::new(indexed),
        })
    }
    pub fn operators(&self) -> &BTreeMap<LocalOperatorId, LocalOperatorProvenance> {
        &self.operators
    }
    pub fn get(&self, operator: LocalOperatorId) -> Option<&LocalOperatorProvenance> {
        self.operators.get(&operator)
    }
    /// Iterate the actual owners once. A consumer associates sources with
    /// these entities; it does not multiply metrics by source count.
    pub fn cost_owners(&self) -> impl Iterator<Item = &LocalOperatorProvenance> {
        self.operators
            .values()
            .filter(|definition| definition.id == definition.cost_owner)
    }
    pub fn records_cost(&self, operator: LocalOperatorId) -> Option<bool> {
        self.get(operator)
            .map(|definition| definition.cost_owner == operator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Control(bool);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
            if self.0 && work > 0 {
                Err(CompileControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    fn direct(id: u32, node: usize, source: u32) -> LocalOperatorProvenance {
        LocalOperatorProvenance {
            id: LocalOperatorId::new(id),
            lowered_nodes: Box::from([ProgramNodeId::new(node)]),
            sources: Box::from([DiagnosticSourceNodeId::new(source)]),
            origin: LocalOperatorOrigin::Direct,
            cost_owner: LocalOperatorId::new(id),
            metrics: OperatorMetricAggregation {
                cpu_time: MetricAggregation::Sum,
                wall_time: MetricAggregation::Maximum,
                peak_retained_bytes: MetricAggregation::Maximum,
            },
        }
    }
    #[test]
    fn split_and_fused_sources_preserve_actual_entities_and_record_shared_cost_once() {
        let sources = BTreeSet::from([
            DiagnosticSourceNodeId::new(0),
            DiagnosticSourceNodeId::new(u32::MAX),
        ]);
        let mut first = direct(0, 0, 0);
        first.origin = LocalOperatorOrigin::Split { piece: 0 };
        let mut second = direct(u32::MAX, 0, 0);
        second.origin = LocalOperatorOrigin::Split { piece: 1 };
        second.cost_owner = first.id;
        let profile = ProgramProvenance::try_new(
            vec![first.clone(), second.clone()],
            1,
            &sources,
            &Control(false),
        )
        .unwrap();
        assert_eq!(profile.operators().len(), 2);
        assert_eq!(profile.cost_owners().count(), 1);
        assert_eq!(profile.records_cost(first.id), Some(true));
        assert_eq!(profile.records_cost(second.id), Some(false));
        let mut fused = direct(7, 0, 0);
        fused.origin = LocalOperatorOrigin::Fused;
        fused.sources = Box::from([
            DiagnosticSourceNodeId::new(u32::MAX),
            DiagnosticSourceNodeId::new(0),
        ]);
        let profile =
            ProgramProvenance::try_new(vec![fused], 1, &sources, &Control(false)).unwrap();
        assert_eq!(
            profile
                .get(LocalOperatorId::new(7))
                .unwrap()
                .sources
                .as_ref(),
            &[
                DiagnosticSourceNodeId::new(0),
                DiagnosticSourceNodeId::new(u32::MAX)
            ]
        );
        assert_eq!(profile.records_cost(LocalOperatorId::new(7)), Some(true));
        assert_eq!(profile.records_cost(LocalOperatorId::new(999)), None);
    }
    #[test]
    fn synthetic_source_free_entities_require_an_explicit_origin_and_preserve_node_zero() {
        let mut synthetic = direct(0, 0, 0);
        synthetic.sources = Box::default();
        assert!(matches!(
            ProgramProvenance::try_new(
                vec![synthetic.clone()],
                1,
                &BTreeSet::new(),
                &Control(false)
            ),
            Err(ProvenanceError::InvalidOrigin)
        ));
        synthetic.origin = LocalOperatorOrigin::Synthetic {
            reason: SyntheticOperatorReason::StateMerge,
        };
        assert!(
            ProgramProvenance::try_new(vec![synthetic], 1, &BTreeSet::new(), &Control(false))
                .is_ok()
        );
        assert!(matches!(
            ProgramProvenance::try_new(vec![direct(0, 0, 0)], 1, &BTreeSet::new(), &Control(false)),
            Err(ProvenanceError::InvalidSource)
        ));
    }
    #[test]
    fn identity_coverage_cost_ownership_and_duplicate_piece_fail_closed() {
        let sources = BTreeSet::from([DiagnosticSourceNodeId::new(0)]);
        let mut first = direct(0, 0, 0);
        first.origin = LocalOperatorOrigin::Split { piece: 0 };
        let mut missing = direct(1, 0, 0);
        missing.cost_owner = LocalOperatorId::new(9);
        missing.origin = LocalOperatorOrigin::Split { piece: 1 };
        assert!(matches!(
            ProgramProvenance::try_new(vec![first.clone(), missing], 1, &sources, &Control(false)),
            Err(ProvenanceError::MissingCostOwner)
        ));
        let mut chain = direct(1, 0, 0);
        chain.origin = LocalOperatorOrigin::Split { piece: 1 };
        let mut owner = first.clone();
        owner.cost_owner = chain.id;
        chain.cost_owner = owner.id;
        assert!(matches!(
            ProgramProvenance::try_new(vec![owner, chain], 1, &sources, &Control(false)),
            Err(ProvenanceError::IndirectCostOwner)
        ));
        let mut split = first.clone();
        split.origin = LocalOperatorOrigin::Split { piece: 1 };
        let mut duplicate = split.clone();
        duplicate.id = LocalOperatorId::new(7);
        duplicate.cost_owner = duplicate.id;
        assert!(matches!(
            ProgramProvenance::try_new(vec![split, duplicate], 1, &sources, &Control(false)),
            Err(ProvenanceError::DuplicateSplitPiece)
        ));
        assert!(matches!(
            ProgramProvenance::try_new(vec![first.clone()], 2, &sources, &Control(false)),
            Err(ProvenanceError::UncoveredLoweredNode)
        ));
        assert!(matches!(
            ProgramProvenance::try_new(vec![first.clone(), first], 1, &sources, &Control(false)),
            Err(ProvenanceError::DuplicateOperator)
        ));
    }
    #[test]
    fn counts_bound_sparse_identity_instead_of_allocating_by_max_id_and_observe_control() {
        let sources = (0..MAX_PROFILE_OPERATORS)
            .map(|id| DiagnosticSourceNodeId::new(id as u32))
            .collect();
        let make = |count: usize| {
            (0..count)
                .map(|id| direct(id as u32, id, id as u32))
                .collect::<Vec<_>>()
        };
        assert!(
            ProgramProvenance::try_new(
                make(MAX_PROFILE_OPERATORS),
                MAX_PROFILE_OPERATORS,
                &sources,
                &Control(false)
            )
            .is_ok()
        );
        assert!(matches!(
            ProgramProvenance::try_new(
                make(MAX_PROFILE_OPERATORS + 1),
                MAX_PROFILE_OPERATORS,
                &sources,
                &Control(false)
            ),
            Err(ProvenanceError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert!(matches!(
            ProgramProvenance::try_new(make(1024), 1024, &sources, &Control(true)),
            Err(ProvenanceError::Control(CompileControlError::Cancelled))
        ));
    }

    #[test]
    fn total_reference_near_over_and_shared_metric_mismatch_have_explicit_errors() {
        let sources = BTreeSet::from([DiagnosticSourceNodeId::new(0)]);
        let mut definitions = Vec::new();
        for id in 0..4 {
            let mut entry = direct(id, 0, 0);
            entry.origin = LocalOperatorOrigin::Fused;
            let count = if id == 3 {
                MAX_PROGRAM_NODES - 4
            } else {
                MAX_PROGRAM_NODES
            };
            entry.lowered_nodes = (0..count).map(ProgramNodeId::new).collect();
            definitions.push(entry);
        }
        assert_eq!(
            definitions
                .iter()
                .map(|entry| entry.lowered_nodes.len() + entry.sources.len())
                .sum::<usize>(),
            MAX_PROFILE_REFERENCES
        );
        assert!(
            ProgramProvenance::try_new(
                definitions.clone(),
                MAX_PROGRAM_NODES,
                &sources,
                &Control(false)
            )
            .is_ok()
        );
        definitions[3].lowered_nodes = (0..MAX_PROGRAM_NODES - 3).map(ProgramNodeId::new).collect();
        assert!(matches!(
            ProgramProvenance::try_new(definitions, MAX_PROGRAM_NODES, &sources, &Control(false)),
            Err(ProvenanceError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        let mut owner = direct(0, 0, 0);
        owner.origin = LocalOperatorOrigin::Split { piece: 0 };
        let mut member = direct(1, 0, 0);
        member.origin = LocalOperatorOrigin::Split { piece: 1 };
        member.cost_owner = owner.id;
        member.metrics.wall_time = MetricAggregation::Sum;
        assert!(matches!(
            ProgramProvenance::try_new(vec![owner, member], 1, &sources, &Control(false)),
            Err(ProvenanceError::CostMetricMismatch)
        ));
    }

    #[test]
    fn malformed_references_and_origin_labels_are_not_accepted_as_profile_sources() {
        let sources = BTreeSet::from([DiagnosticSourceNodeId::new(0)]);
        let check = |entry: LocalOperatorProvenance| {
            ProgramProvenance::try_new(vec![entry], 1, &sources, &Control(false))
        };
        let mut entry = direct(0, 1, 0);
        assert!(matches!(
            check(entry.clone()),
            Err(ProvenanceError::InvalidLoweredNode)
        ));
        entry.lowered_nodes = Box::from([ProgramNodeId::new(0), ProgramNodeId::new(0)]);
        entry.origin = LocalOperatorOrigin::Fused;
        assert!(matches!(
            check(entry.clone()),
            Err(ProvenanceError::DuplicateLoweredNode)
        ));
        entry.lowered_nodes = Box::from([ProgramNodeId::new(0)]);
        entry.sources = Box::from([
            DiagnosticSourceNodeId::new(0),
            DiagnosticSourceNodeId::new(0),
        ]);
        assert!(matches!(
            check(entry.clone()),
            Err(ProvenanceError::DuplicateSource)
        ));
        entry.sources = Box::from([DiagnosticSourceNodeId::new(0)]);
        assert!(matches!(
            check(entry.clone()),
            Err(ProvenanceError::InvalidOrigin)
        ));
        entry.lowered_nodes = Box::default();
        assert!(matches!(check(entry), Err(ProvenanceError::InvalidOrigin)));
        assert!(matches!(
            ProgramProvenance::try_new(
                vec![direct(0, 0, 0), direct(1, 1, 0)],
                2,
                &sources,
                &Control(false)
            ),
            Err(ProvenanceError::DuplicateDirectSource)
        ));
        let mut first = direct(0, 0, 0);
        first.origin = LocalOperatorOrigin::Split { piece: 7 };
        let mut second = direct(1, 1, 0);
        second.origin = LocalOperatorOrigin::Split { piece: 7 };
        assert!(matches!(
            ProgramProvenance::try_new(vec![first, second], 2, &sources, &Control(false)),
            Err(ProvenanceError::DuplicateSplitPiece)
        ));
    }

    #[test]
    fn normalization_observes_each_reference_and_propagates_all_control_categories() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct After {
            completed: AtomicUsize,
            fail: CompileControlError,
        }
        impl PureCompileControl for After {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                let completed =
                    self.completed.fetch_add(units as usize, Ordering::Relaxed) + units as usize;
                // Preflight=1, nodes=300, source=1: threshold=512 therefore
                // fails while moving the canonical node set into owned data.
                if completed >= 512 {
                    Err(self.fail)
                } else {
                    Ok(())
                }
            }
        }
        let sources = BTreeSet::from([DiagnosticSourceNodeId::new(0)]);
        for fail in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let mut entry = direct(0, 0, 0);
            entry.origin = LocalOperatorOrigin::Fused;
            entry.lowered_nodes = (0..300).map(ProgramNodeId::new).collect();
            let control = After {
                completed: AtomicUsize::new(0),
                fail,
            };
            assert!(
                matches!(ProgramProvenance::try_new(vec![entry], 300, &sources, &control), Err(ProvenanceError::Control(error)) if error == fail)
            );
            assert_eq!(control.completed.load(Ordering::Relaxed), 512);
        }
    }
}
