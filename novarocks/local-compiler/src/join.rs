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

//! Lower one admitted physical HashJoin or NestLoopJoin into the local join
//! owner.
//!
//! Orientation: the local `left` input is always the probe child and the
//! local `right` input the build child. A physical join whose build side is
//! Left swaps its children and mirrors its kind, so a physical RightSemi or
//! RightAnti becomes the probe-preserving local LeftSemi or LeftAnti. Key
//! ordinals are kept, so key `k` is `probe_keys[k]` and `build_keys[k]`.
//!
//! Sources: a join-owned value read resolves per actual use. A key root reads
//! its own side's layout; a residual or nested-loop predicate reads the join
//! scope, which is the probe slots followed by the build slots with the
//! children's own slot identities.
//!
//! Output: the join node publishes a canonical output with frozen nullability
//! (probe followed by build for Inner and LeftOuter, the probe alone for the
//! semi and anti kinds) under fresh slots. When the physical output is not
//! exactly that canonical sequence, a compiler-authored selection Project
//! reads it; a physical NULL extension maps to the canonical ordinal of its
//! source, whose canonical type is already nullable.
//!
//! This milestone admits local Inner, LeftOuter, LeftSemi and LeftAnti, the
//! hash NullAwareLeftAnti and the nested-loop Cross. Build-preserving local
//! kinds (RightOuter, FullOuter, RightSemi, RightAnti), a partitioned or
//! colocated null-aware anti join and a nested-loop null-aware anti join with
//! a predicate stay explicit refusals.
//!
//! Runtime filters: a hash join owns the build-key membership producers the
//! runtime-filter plan admitted for it. Producer `k` observes `build_keys[k]`,
//! whose `JoinBuildKey` root is already the producer's key root.

use crate::{
    assert_rows::reserve_vec,
    channels::{ResolvedInput, UnionChannelBranch, UnionChannelSource},
    lowering::FragmentCompileError,
    union_flow::UnionRoot,
};
use arrow_schema::{FieldRef, Schema};
use novarocks_local_program::{
    DiagnosticSourceNodeId, FilterConsumerAtJoinKey, FilterProducerAtExpr, JoinDistributionMode,
    JoinType, LocalOperatorId, LocalOperatorOrigin, LocalOperatorProvenance, MetricAggregation,
    NestedLoopJoinType, OperatorMetricAggregation, ProgramChannelLayoutRole, ProgramChannelSite,
    ProgramExprId, ProgramNode, ProgramNodeExpressionRole, ProgramNodeId, ProgramNodeKind,
    StaticFilterProducer, StaticLayout,
};
use novarocks_physical_plan::{
    Distribution, ExprId, ExpressionRootRole, Fragment, FragmentPackage, JoinDistribution,
    JoinKind, JoinSide, NestLoopJoinDistribution, NodeId, NodeKind, PhysicalNode, RowMultiplicity,
    ValueId, ValueOrigin,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, ExpressionUseId, FunctionValueType,
    PureCompileControl, arrow_data_types_exact_observed,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

/// The local join family and kind after orientation normalization.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LocalJoinKind {
    Hash(JoinType),
    NestLoop(NestedLoopJoinType),
}

/// Which physical input is the local probe (`left`) child and the local kind
/// in that orientation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JoinOrientation {
    /// Physical input ordinal of the probe child; the build is the other one.
    pub probe_input: usize,
    pub kind: LocalJoinKind,
}

impl JoinOrientation {
    pub(crate) const fn probe_side(self) -> JoinSide {
        if self.probe_input == 0 {
            JoinSide::Left
        } else {
            JoinSide::Right
        }
    }
    /// The canonical output is the probe side alone.
    const fn probe_only(self) -> bool {
        matches!(
            self.kind,
            LocalJoinKind::Hash(
                JoinType::LeftSemi | JoinType::LeftAnti | JoinType::NullAwareLeftAnti
            ) | LocalJoinKind::NestLoop(
                NestedLoopJoinType::LeftSemi
                    | NestedLoopJoinType::LeftAnti
                    | NestedLoopJoinType::NullAwareLeftAnti
            )
        )
    }
    /// The build side is NULL-extended for unmatched probe rows.
    const fn null_extends_build(self) -> bool {
        matches!(
            self.kind,
            LocalJoinKind::Hash(JoinType::LeftOuter)
                | LocalJoinKind::NestLoop(NestedLoopJoinType::LeftOuter)
        )
    }
}

fn unsupported(node: &PhysicalNode, feature: &'static str) -> FragmentCompileError {
    FragmentCompileError::Unsupported {
        node: Some(node.id),
        feature,
    }
}

/// Admit one physical join and normalize its orientation. Every refusal is
/// explicit; nothing is approximated by another kind.
pub(crate) fn orient_join(node: &PhysicalNode) -> Result<JoinOrientation, FragmentCompileError> {
    if node.inputs.len() != 2 {
        return Err(FragmentCompileError::Invalid(
            "join requires exactly two inputs",
        ));
    }
    match &node.kind {
        NodeKind::HashJoin {
            kind,
            build_side,
            distribution,
            ..
        } => {
            let (probe_input, local) = match build_side {
                JoinSide::Right => (
                    0,
                    match kind {
                        JoinKind::Inner => JoinType::Inner,
                        JoinKind::LeftOuter => JoinType::LeftOuter,
                        JoinKind::LeftSemi => JoinType::LeftSemi,
                        JoinKind::LeftAnti => JoinType::LeftAnti,
                        JoinKind::NullAwareLeftAnti => JoinType::NullAwareLeftAnti,
                        JoinKind::RightOuter
                        | JoinKind::FullOuter
                        | JoinKind::RightSemi
                        | JoinKind::RightAnti => {
                            return Err(unsupported(
                                node,
                                "build-preserving hash join kind (merged build match flags)",
                            ));
                        }
                        JoinKind::Cross => {
                            return Err(FragmentCompileError::Invalid(
                                "cross join cannot use the hash-join node",
                            ));
                        }
                    },
                ),
                // The mirror of a build-left join keeps the probe preserved.
                JoinSide::Left => (
                    1,
                    match kind {
                        JoinKind::Inner => JoinType::Inner,
                        JoinKind::RightOuter => JoinType::LeftOuter,
                        JoinKind::RightSemi => JoinType::LeftSemi,
                        JoinKind::RightAnti => JoinType::LeftAnti,
                        JoinKind::NullAwareLeftAnti => {
                            return Err(unsupported(
                                node,
                                "null-aware anti join with a left build side has no mirror",
                            ));
                        }
                        JoinKind::LeftOuter
                        | JoinKind::FullOuter
                        | JoinKind::LeftSemi
                        | JoinKind::LeftAnti => {
                            return Err(unsupported(
                                node,
                                "build-preserving hash join kind (merged build match flags)",
                            ));
                        }
                        JoinKind::Cross => {
                            return Err(FragmentCompileError::Invalid(
                                "cross join cannot use the hash-join node",
                            ));
                        }
                    },
                ),
            };
            // Cross-instance null-key knowledge is not representable when
            // each instance holds only its partition of the build.
            if local == JoinType::NullAwareLeftAnti
                && matches!(
                    distribution,
                    JoinDistribution::Partitioned | JoinDistribution::Colocated
                )
            {
                return Err(unsupported(
                    node,
                    "partitioned or colocated null-aware anti join",
                ));
            }
            Ok(JoinOrientation {
                probe_input,
                kind: LocalJoinKind::Hash(local),
            })
        }
        NodeKind::NestLoopJoin {
            kind,
            distribution,
            predicate,
            ..
        } => {
            let (probe_input, local) = match kind {
                JoinKind::Cross => (0, NestedLoopJoinType::Cross),
                JoinKind::Inner => (0, NestedLoopJoinType::Inner),
                JoinKind::LeftOuter => (0, NestedLoopJoinType::LeftOuter),
                JoinKind::LeftSemi => (0, NestedLoopJoinType::LeftSemi),
                JoinKind::LeftAnti => (0, NestedLoopJoinType::LeftAnti),
                // Its key conjuncts are not separated from its residual in the
                // physical contract; recovering them from expression shape
                // would be a guess.
                JoinKind::NullAwareLeftAnti if predicate.is_some() => {
                    return Err(unsupported(
                        node,
                        "nested-loop null-aware anti join with a predicate",
                    ));
                }
                JoinKind::NullAwareLeftAnti => (0, NestedLoopJoinType::NullAwareLeftAnti),
                // Only a singleton placement may swap: a broadcast right side
                // would become the preserved side and duplicate its rows.
                JoinKind::RightSemi | JoinKind::RightAnti
                    if *distribution == NestLoopJoinDistribution::Singleton =>
                {
                    (
                        1,
                        if *kind == JoinKind::RightSemi {
                            NestedLoopJoinType::LeftSemi
                        } else {
                            NestedLoopJoinType::LeftAnti
                        },
                    )
                }
                JoinKind::RightSemi | JoinKind::RightAnti => {
                    return Err(unsupported(
                        node,
                        "nested-loop right semi or anti join over a broadcast right side",
                    ));
                }
                JoinKind::RightOuter | JoinKind::FullOuter => {
                    return Err(unsupported(
                        node,
                        "build-preserving nested-loop join kind (merged build match flags)",
                    ));
                }
            };
            Ok(JoinOrientation {
                probe_input,
                kind: LocalJoinKind::NestLoop(local),
            })
        }
        _ => Err(FragmentCompileError::Invalid("node is not a join")),
    }
}

/// The physical inputs that may arrive replicated: the build ExchangeSource
/// of a broadcast-build hash join and the right ExchangeSource of a
/// broadcast-right nested-loop join. Any other replicated placement is
/// refused by the caller.
pub(crate) fn replicated_build_inputs(
    fragment: &Fragment,
    work: &mut CompileCheckpoints<'_>,
) -> Result<BTreeSet<NodeId>, FragmentCompileError> {
    let mut inputs = BTreeSet::new();
    for node in fragment.nodes().values() {
        work.step()?;
        let build = match &node.kind {
            NodeKind::HashJoin {
                distribution: JoinDistribution::BroadcastBuild,
                build_side,
                ..
            } => node.inputs.get(build_side.input_ordinal() as usize),
            NodeKind::NestLoopJoin {
                distribution: NestLoopJoinDistribution::BroadcastRight,
                ..
            } => node.inputs.get(1),
            _ => None,
        };
        let Some(&build) = build else {
            continue;
        };
        let receiver = fragment.nodes().get(&build).is_some_and(|input| {
            matches!(input.kind, NodeKind::ExchangeSource { .. })
                && input.output_properties.distribution == Distribution::Broadcast
                && input.output_properties.row_multiplicity == RowMultiplicity::Replicated
        });
        if receiver {
            inputs.insert(build);
        }
    }
    Ok(inputs)
}

/// The upper bound of what lowering one join adds beyond one node and one
/// output layout: the selection Project node, the JoinLeft, JoinRight and
/// JoinScope channels plus the canonical output, and one derived slot read
/// per physical output occurrence.
pub(crate) fn resource_bound(
    fragment: &Fragment,
    node: &PhysicalNode,
) -> Result<Option<(usize, usize, usize)>, FragmentCompileError> {
    if !matches!(
        node.kind,
        NodeKind::HashJoin { .. } | NodeKind::NestLoopJoin { .. }
    ) {
        return Ok(None);
    }
    let mut sides = 0usize;
    for input in node.inputs.iter() {
        let width = fragment
            .nodes()
            .get(input)
            .ok_or(FragmentCompileError::Invalid("missing join input"))?
            .output
            .columns
            .len();
        sides = sides
            .checked_add(width)
            .ok_or(CompileControlError::ResourceExhausted)?;
    }
    let channels = sides
        .checked_mul(3)
        .ok_or(CompileControlError::ResourceExhausted)?;
    Ok(Some((1, channels, node.output.columns.len())))
}

/// One join's planned channels. `join` is the local join node; when a
/// selection Project is emitted it is the physical node's published local.
pub(crate) struct PlannedJoin {
    pub orientation: JoinOrientation,
    pub join: ProgramNodeId,
    pub probe: NodeId,
    pub build: NodeId,
    pub canonical: Arc<[SlotId]>,
    pub canonical_types: Vec<FunctionValueType>,
    /// The canonical ordinal of each physical output occurrence, present only
    /// when a selection Project publishes the physical output.
    pub selection: Option<Vec<u32>>,
    probe_slots: Arc<[SlotId]>,
    build_slots: Arc<[SlotId]>,
}

/// The canonical ordinal each physical output occurrence reads and whether
/// that differs from the canonical sequence itself.
pub(crate) struct JoinShape {
    orientation: JoinOrientation,
    probe: NodeId,
    build: NodeId,
    canonical_types: Vec<FunctionValueType>,
    sources: Vec<u32>,
    selection: bool,
}

impl JoinShape {
    pub(crate) const fn selection(&self) -> bool {
        self.selection
    }
    /// The physical probe and build children.
    pub(crate) const fn children(&self) -> (NodeId, NodeId) {
        (self.probe, self.build)
    }
}

/// Map every physical output occurrence to its canonical ordinal. `ports`
/// gives each child's value-to-ordinal representatives.
pub(crate) fn shape_join(
    fragment: &Fragment,
    node: &PhysicalNode,
    ports: &BTreeMap<NodeId, BTreeMap<ValueId, usize>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<JoinShape, FragmentCompileError> {
    let orientation = orient_join(node)?;
    let probe = node.inputs[orientation.probe_input];
    let build = node.inputs[1 - orientation.probe_input];
    let probe_port = ports
        .get(&probe)
        .ok_or(FragmentCompileError::Invalid("join probe port is missing"))?;
    let build_port = ports
        .get(&build)
        .ok_or(FragmentCompileError::Invalid("join build port is missing"))?;
    let values = fragment.values();
    let child_types = |child: NodeId,
                       nullable: bool,
                       types: &mut Vec<FunctionValueType>,
                       work: &mut CompileCheckpoints<'_>|
     -> Result<usize, FragmentCompileError> {
        let columns = &fragment
            .nodes()
            .get(&child)
            .ok_or(FragmentCompileError::Invalid("missing join input"))?
            .output
            .columns;
        for value in columns.iter() {
            let mut ty = values
                .get(value)
                .ok_or(FragmentCompileError::Invalid("missing join input value"))?
                .ty
                .clone();
            ty.nullable |= nullable;
            types.push(ty);
            work.step()?;
        }
        Ok(columns.len())
    };
    let mut canonical_types = Vec::new();
    let probe_width = child_types(probe, false, &mut canonical_types, work)?;
    if !orientation.probe_only() {
        child_types(
            build,
            orientation.null_extends_build(),
            &mut canonical_types,
            work,
        )?;
    }
    let mut sources = Vec::new();
    reserve_vec(&mut sources, node.output.columns.len(), work)?;
    for value in node.output.columns.iter() {
        let definition = values
            .get(value)
            .ok_or(FragmentCompileError::Invalid("missing join output value"))?;
        let canonical = match definition.origin {
            ValueOrigin::NullExtended { node: owner, of } if owner == node.id => {
                if !orientation.null_extends_build() {
                    return Err(FragmentCompileError::Invalid(
                        "join kind publishes no NULL extension",
                    ));
                }
                let ordinal = *build_port.get(&of).ok_or(FragmentCompileError::Invalid(
                    "NULL extension source is not on the build side",
                ))?;
                probe_width
                    .checked_add(ordinal)
                    .ok_or(CompileControlError::ResourceExhausted)?
            }
            _ => match (probe_port.get(value), build_port.get(value)) {
                (Some(&ordinal), None) => ordinal,
                // Only Inner publishes original build values: an outer join
                // publishes its build side through NULL extensions.
                (None, Some(&ordinal))
                    if !orientation.probe_only() && !orientation.null_extends_build() =>
                {
                    probe_width
                        .checked_add(ordinal)
                        .ok_or(CompileControlError::ResourceExhausted)?
                }
                _ => {
                    return Err(FragmentCompileError::Invalid(
                        "join output value is not valid for its local join kind",
                    ));
                }
            },
        };
        let expected = canonical_types
            .get(canonical)
            .ok_or(FragmentCompileError::Invalid(
                "join output maps outside its canonical output",
            ))?;
        work.flush()?;
        let same = definition
            .ty
            .exactly_equals_observed::<FragmentCompileError>(expected, || {
                work.step().map_err(Into::into)
            })?;
        if !same {
            return Err(unsupported(
                node,
                "join output type differs from its canonical source type",
            ));
        }
        sources.push(u32::try_from(canonical).map_err(|_| CompileControlError::ResourceExhausted)?);
        work.step()?;
    }
    let mut identity = sources.len() == canonical_types.len();
    for (ordinal, &source) in sources.iter().enumerate() {
        identity &= source as usize == ordinal;
        work.step()?;
    }
    Ok(JoinShape {
        orientation,
        probe,
        build,
        canonical_types,
        sources,
        selection: !identity,
    })
}

/// The planned channels of one join node, plus the selection branch whose
/// slot reads the expression author materializes like a union normalizer.
pub(crate) struct JoinChannels {
    pub planned: PlannedJoin,
    pub slots: Arc<[SlotId]>,
    pub port: BTreeMap<ValueId, usize>,
    pub selection: Option<UnionChannelBranch>,
}

/// Assign fresh canonical (and selection) slots. `local` is the physical
/// node's published local node; the join itself is `local - 1` when a
/// selection Project follows it.
pub(crate) fn plan_join_channels(
    node: &PhysicalNode,
    shape: JoinShape,
    local: ProgramNodeId,
    probe_slots: Arc<[SlotId]>,
    build_slots: Arc<[SlotId]>,
    next_slot: &mut u64,
    work: &mut CompileCheckpoints<'_>,
) -> Result<JoinChannels, FragmentCompileError> {
    let join = if shape.selection {
        ProgramNodeId::new(
            local
                .index()
                .checked_sub(1)
                .ok_or(FragmentCompileError::Invalid("join selection has no join"))?,
        )
    } else {
        local
    };
    let mut fresh = |count: usize,
                     work: &mut CompileCheckpoints<'_>|
     -> Result<Arc<[SlotId]>, FragmentCompileError> {
        let mut slots = Vec::new();
        reserve_vec(&mut slots, count, work)?;
        for _ in 0..count {
            let slot = u32::try_from(*next_slot)
                .map_err(|_| FragmentCompileError::Invalid("slot identity exhausted"))?;
            *next_slot = next_slot
                .checked_add(1)
                .ok_or(FragmentCompileError::Invalid("slot identity exhausted"))?;
            slots.push(SlotId::new(slot));
            work.step()?;
        }
        work.flush()?;
        Ok(Arc::from(slots))
    };
    let canonical = fresh(shape.canonical_types.len(), work)?;
    let mut port = BTreeMap::new();
    for (ordinal, value) in node.output.columns.iter().enumerate() {
        port.entry(*value).or_insert(ordinal);
        work.step()?;
    }
    let (slots, selection, branch) = if shape.selection {
        let slots = fresh(node.output.columns.len(), work)?;
        let mut sources = Vec::new();
        reserve_vec(&mut sources, shape.sources.len(), work)?;
        for &canonical_ordinal in &shape.sources {
            let index = canonical_ordinal as usize;
            sources.push(UnionChannelSource {
                input: ResolvedInput {
                    slot: canonical[index],
                    source: ProgramChannelSite::Layout {
                        node: join,
                        role: ProgramChannelLayoutRole::NodeOutput,
                        ordinal: canonical_ordinal,
                    },
                },
                ty: shape.canonical_types[index].clone(),
            });
            work.step()?;
        }
        let branch = UnionChannelBranch {
            normalizer: local,
            input: join,
            sources,
        };
        (slots, Some(shape.sources), Some(branch))
    } else {
        (Arc::clone(&canonical), None, None)
    };
    Ok(JoinChannels {
        planned: PlannedJoin {
            orientation: shape.orientation,
            join,
            probe: shape.probe,
            build: shape.build,
            canonical,
            canonical_types: shape.canonical_types,
            selection,
            probe_slots,
            build_slots,
        },
        slots,
        port,
        selection: branch,
    })
}

/// Where a join-owned value read lives: its side and ordinal there, and its
/// ordinal in the join scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JoinValueSource {
    pub join: ProgramNodeId,
    side: ProgramChannelLayoutRole,
    side_ordinal: u32,
    scope_ordinal: u32,
}

impl JoinValueSource {
    /// The source of one use whose root reads `role` of `join`.
    pub(crate) fn for_root(
        self,
        join: ProgramNodeId,
        role: ProgramChannelLayoutRole,
    ) -> Result<ProgramChannelSite, FragmentCompileError> {
        if join != self.join {
            return Err(FragmentCompileError::Invalid(
                "join value is read by another node's root",
            ));
        }
        let ordinal = if role == ProgramChannelLayoutRole::JoinScope {
            self.scope_ordinal
        } else if role == self.side {
            self.side_ordinal
        } else {
            return Err(FragmentCompileError::Invalid(
                "join key root reads the other side's value",
            ));
        };
        Ok(ProgramChannelSite::Layout {
            node: join,
            role,
            ordinal,
        })
    }
}

/// Resolve one join-owned value read. The value lies in exactly one side.
pub(crate) fn resolve_join_value(
    planned: &PlannedJoin,
    value: ValueId,
    ports: &BTreeMap<NodeId, BTreeMap<ValueId, usize>>,
) -> Result<(ResolvedInput, JoinValueSource), FragmentCompileError> {
    let probe = ports.get(&planned.probe).and_then(|port| port.get(&value));
    let build = ports.get(&planned.build).and_then(|port| port.get(&value));
    let (side, ordinal, slots, offset) = match (probe, build) {
        (Some(&ordinal), None) => (
            ProgramChannelLayoutRole::JoinLeft,
            ordinal,
            &planned.probe_slots,
            0,
        ),
        (None, Some(&ordinal)) => (
            ProgramChannelLayoutRole::JoinRight,
            ordinal,
            &planned.build_slots,
            planned.probe_slots.len(),
        ),
        _ => {
            return Err(FragmentCompileError::Invalid(
                "join value is not on exactly one input side",
            ));
        }
    };
    let slot = *slots.get(ordinal).ok_or(FragmentCompileError::Invalid(
        "join input occurrence has no slot",
    ))?;
    let side_ordinal =
        u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?;
    let scope_ordinal = u32::try_from(
        offset
            .checked_add(ordinal)
            .ok_or(CompileControlError::ResourceExhausted)?,
    )
    .map_err(|_| CompileControlError::ResourceExhausted)?;
    Ok((
        ResolvedInput {
            slot,
            source: ProgramChannelSite::Layout {
                node: planned.join,
                role: side,
                ordinal: side_ordinal,
            },
        },
        JoinValueSource {
            join: planned.join,
            side,
            side_ordinal,
            scope_ordinal,
        },
    ))
}

/// The local root role of one physical join root, and the layout its uses
/// read.
pub(crate) fn root_role(
    planned: &PlannedJoin,
    role: ExpressionRootRole,
) -> Result<(ProgramNodeExpressionRole, ProgramChannelLayoutRole), FragmentCompileError> {
    match (role, planned.orientation.kind) {
        (ExpressionRootRole::JoinKey { key, side }, LocalJoinKind::Hash(_)) => {
            Ok(if side == planned.orientation.probe_side() {
                (
                    ProgramNodeExpressionRole::JoinProbeKey { key },
                    ProgramChannelLayoutRole::JoinLeft,
                )
            } else {
                (
                    ProgramNodeExpressionRole::JoinBuildKey { key },
                    ProgramChannelLayoutRole::JoinRight,
                )
            })
        }
        (ExpressionRootRole::HashJoinResidual, LocalJoinKind::Hash(_)) => Ok((
            ProgramNodeExpressionRole::JoinResidual,
            ProgramChannelLayoutRole::JoinScope,
        )),
        (ExpressionRootRole::NestLoopPredicate, LocalJoinKind::NestLoop(_)) => Ok((
            ProgramNodeExpressionRole::NestedLoopPredicate,
            ProgramChannelLayoutRole::JoinScope,
        )),
        _ => Err(FragmentCompileError::Invalid(
            "join root role differs from its join family",
        )),
    }
}

/// The join layout every use under a join root reads. Uses are per
/// occurrence, so each use belongs to exactly one root.
pub(crate) fn join_use_roles(
    package: &FragmentPackage,
    joins: &BTreeMap<NodeId, PlannedJoin>,
    control: &dyn PureCompileControl,
) -> Result<
    BTreeMap<ExpressionUseId, (ProgramNodeId, ProgramChannelLayoutRole)>,
    FragmentCompileError,
> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = (|| {
        let uses = package.expression_uses().flow().uses();
        let mut roles = BTreeMap::new();
        if joins.is_empty() {
            return Ok(roles);
        }
        let mut stack = Vec::new();
        for (site, &root) in package.expression_uses().bindings() {
            work.step()?;
            let Some(planned) = joins.get(&site.node) else {
                continue;
            };
            let (_, layout) = root_role(planned, site.role)?;
            stack.push(root);
            while let Some(use_id) = stack.pop() {
                work.step()?;
                if roles.insert(use_id, (planned.join, layout)).is_some() {
                    return Err(FragmentCompileError::Invalid(
                        "join expression occurrence is shared by two roots",
                    ));
                }
                let invocation = uses.get(&use_id).ok_or(FragmentCompileError::Invalid(
                    "join root occurrence has no invocation",
                ))?;
                for &argument in invocation.arguments.iter() {
                    stack.push(argument);
                    work.step()?;
                }
            }
        }
        Ok(roles)
    })();
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

/// The nodes, channels, provenance and selection roots of one lowered join.
pub(crate) struct LoweredJoin {
    pub nodes: Vec<ProgramNode>,
    pub channels: Vec<(ProgramChannelSite, FunctionValueType)>,
    pub operators: Vec<LocalOperatorProvenance>,
    pub selection_roots: Vec<UnionRoot>,
}

/// One lowered join input: its local node and published layout.
pub(crate) struct JoinInput<'a> {
    pub node: ProgramNodeId,
    pub layout: &'a StaticLayout,
}

/// `runtime_filters` are the join's admitted membership producers in binding
/// order, each with the build-key ordinal it observes.
#[expect(
    clippy::too_many_arguments,
    reason = "The planned join, its two lowered inputs, the derived selection reads and its runtime-filter producers are independent inputs"
)]
pub(crate) fn lower_join(
    package: &FragmentPackage,
    node: &PhysicalNode,
    planned: &PlannedJoin,
    published: &[SlotId],
    probe: JoinInput<'_>,
    build: JoinInput<'_>,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    selection_definitions: Option<&[ProgramExprId]>,
    runtime_filters: Vec<(usize, StaticFilterProducer)>,
    runtime_filter_consumers: Vec<(
        usize,
        ProgramExprId,
        novarocks_local_program::StaticFilterConsumer,
    )>,
    control: &dyn PureCompileControl,
) -> Result<LoweredJoin, FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(
        package,
        node,
        planned,
        published,
        probe,
        build,
        expressions,
        selection_definitions,
        runtime_filters,
        runtime_filter_consumers,
        &mut work,
    );
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn layout_role_channels(
    node: ProgramNodeId,
    role: ProgramChannelLayoutRole,
    types: impl IntoIterator<Item = FunctionValueType>,
    channels: &mut Vec<(ProgramChannelSite, FunctionValueType)>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCompileError> {
    for (ordinal, ty) in types.into_iter().enumerate() {
        channels.push((
            ProgramChannelSite::Layout {
                node,
                role,
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
            },
            ty,
        ));
        work.step()?;
    }
    Ok(())
}

fn named_layout(
    package: &FragmentPackage,
    node: &PhysicalNode,
    local: ProgramNodeId,
    types: &[FunctionValueType],
    slots: &[SlotId],
    labels: bool,
    work: &mut CompileCheckpoints<'_>,
) -> Result<StaticLayout, FragmentCompileError> {
    // Full result labels are authoritative only when the entire ordered
    // physical output is the result port's, whichever node publishes it.
    let result = package
        .result()
        .filter(|result| labels && result.output.columns == node.output.columns);
    let mut fields = Vec::new();
    reserve_vec(&mut fields, types.len(), work)?;
    for (ordinal, ty) in types.iter().enumerate() {
        let name = match result {
            Some(result) => {
                let field = result
                    .fields
                    .get(ordinal)
                    .ok_or(FragmentCompileError::Invalid("missing join result label"))?;
                field.alias.as_deref().unwrap_or(&field.name).to_owned()
            }
            None => format!("local_{}_{}", local.index(), ordinal),
        };
        work.flush()?;
        let field = ty.try_to_field(name);
        work.flush()?;
        fields.push(field?);
        work.step()?;
    }
    work.flush()?;
    let layout = StaticLayout::try_new_for_compile(
        Arc::new(Schema::new(fields)),
        Arc::from(slots),
        work.control(),
    )?;
    work.flush()?;
    Ok(layout)
}

fn channel_types(
    package: &FragmentPackage,
    child: NodeId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<FunctionValueType>, FragmentCompileError> {
    let columns = &package
        .fragment()
        .nodes()
        .get(&child)
        .ok_or(FragmentCompileError::Invalid("missing join input"))?
        .output
        .columns;
    let mut types = Vec::new();
    reserve_vec(&mut types, columns.len(), work)?;
    for value in columns.iter() {
        types.push(
            package
                .fragment()
                .values()
                .get(value)
                .ok_or(FragmentCompileError::Invalid("missing join input value"))?
                .ty
                .clone(),
        );
        work.step()?;
    }
    Ok(types)
}

fn lowered(
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    expression: ExprId,
) -> Result<ProgramExprId, FragmentCompileError> {
    expressions
        .get(&expression)
        .copied()
        .ok_or(FragmentCompileError::Invalid(
            "missing lowered join expression",
        ))
}

#[expect(
    clippy::too_many_arguments,
    reason = "Mirrors lower_join with the caller's checkpoint scope"
)]
fn lower_core(
    package: &FragmentPackage,
    node: &PhysicalNode,
    planned: &PlannedJoin,
    published: &[SlotId],
    probe: JoinInput<'_>,
    build: JoinInput<'_>,
    expressions: &BTreeMap<ExprId, ProgramExprId>,
    selection_definitions: Option<&[ProgramExprId]>,
    runtime_filters: Vec<(usize, StaticFilterProducer)>,
    runtime_filter_consumers: Vec<(
        usize,
        ProgramExprId,
        novarocks_local_program::StaticFilterConsumer,
    )>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<LoweredJoin, FragmentCompileError> {
    let fragment = package.fragment();
    let probe_types = channel_types(package, planned.probe, work)?;
    let build_types = channel_types(package, planned.build, work)?;
    if probe.layout.slots() != planned.probe_slots.as_ref()
        || build.layout.slots() != planned.build_slots.as_ref()
        || probe_types.len() != probe.layout.slots().len()
        || build_types.len() != build.layout.slots().len()
    {
        return Err(FragmentCompileError::Invalid(
            "join input layouts differ from their planned channels",
        ));
    }
    // The scope is the probe occurrences followed by the build occurrences,
    // with the children's own fields and slot identities.
    let width = probe
        .layout
        .slots()
        .len()
        .checked_add(build.layout.slots().len())
        .ok_or(CompileControlError::ResourceExhausted)?;
    let mut scope_fields: Vec<FieldRef> = Vec::new();
    reserve_vec(&mut scope_fields, width, work)?;
    let mut scope_slots = Vec::new();
    reserve_vec(&mut scope_slots, width, work)?;
    for layout in [probe.layout, build.layout] {
        for (field, slot) in layout.schema().fields().iter().zip(layout.slots()) {
            scope_fields.push(Arc::clone(field));
            scope_slots.push(*slot);
            work.step()?;
        }
    }
    work.flush()?;
    let scope = StaticLayout::try_new_for_compile(
        Arc::new(Schema::new(scope_fields)),
        Arc::from(scope_slots),
        work.control(),
    )?;
    work.flush()?;
    let join = planned.join;
    let canonical = named_layout(
        package,
        node,
        join,
        &planned.canonical_types,
        &planned.canonical,
        planned.selection.is_none(),
        work,
    )?;
    let kind = match (&node.kind, planned.orientation.kind) {
        (NodeKind::HashJoin { keys, residual, .. }, LocalJoinKind::Hash(join_type)) => {
            // Every physical distribution delivers this instance its whole
            // share of both sides, so the local strategy shares one build per
            // instance; the local mode records exactly that.
            let mut probe_keys = Vec::new();
            reserve_vec(&mut probe_keys, keys.len(), work)?;
            let mut build_keys = Vec::new();
            reserve_vec(&mut build_keys, keys.len(), work)?;
            let mut eq_null_safe = Vec::new();
            reserve_vec(&mut eq_null_safe, keys.len(), work)?;
            for key in keys.iter() {
                let (probe_key, build_key) = if planned.orientation.probe_input == 0 {
                    (key.left, key.right)
                } else {
                    (key.right, key.left)
                };
                let probe_type = &fragment
                    .expressions()
                    .get(probe_key)
                    .ok_or(FragmentCompileError::Invalid("missing join key definition"))?
                    .ty;
                let build_type = &fragment
                    .expressions()
                    .get(build_key)
                    .ok_or(FragmentCompileError::Invalid("missing join key definition"))?
                    .ty;
                // The hash table compares exact carriers; a key pair whose
                // carriers differ would need a BE cast, which is refused.
                work.flush()?;
                let same = probe_type.logical_type == build_type.logical_type
                    && arrow_data_types_exact_observed::<FragmentCompileError>(
                        &probe_type.data_type,
                        &build_type.data_type,
                        || work.step().map_err(Into::into),
                    )?;
                if !same {
                    return Err(unsupported(node, "join key carriers differ"));
                }
                probe_keys.push(lowered(expressions, probe_key)?);
                build_keys.push(lowered(expressions, build_key)?);
                eq_null_safe.push(key.null_safe);
                work.step()?;
            }
            // Each producer observes the build key its witness names.
            let mut producers = Vec::new();
            reserve_vec(&mut producers, runtime_filters.len(), work)?;
            for (key_ordinal, producer) in runtime_filters {
                let expr_id = *build_keys
                    .get(key_ordinal)
                    .ok_or(FragmentCompileError::Invalid(
                        "runtime-filter producer names an absent build key",
                    ))?;
                producers.push(FilterProducerAtExpr {
                    expr_id,
                    key_ordinal,
                    producer,
                });
                work.step()?;
            }
            let mut consumers = Vec::new();
            reserve_vec(&mut consumers, runtime_filter_consumers.len(), work)?;
            for (key_ordinal, expr_id, consumer) in runtime_filter_consumers {
                if probe_keys.get(key_ordinal) != Some(&expr_id) {
                    return Err(FragmentCompileError::Invalid(
                        "probe consumer differs from its exact key ordinal",
                    ));
                }
                consumers.push(FilterConsumerAtJoinKey {
                    key_ordinal,
                    expr_id,
                    consumer,
                });
                work.step()?;
            }
            ProgramNodeKind::Join {
                left: probe.node,
                right: build.node,
                join_type,
                distribution_mode: JoinDistributionMode::Broadcast,
                left_layout: probe.layout.clone(),
                right_layout: build.layout.clone(),
                join_scope_layout: scope,
                probe_keys,
                build_keys,
                eq_null_safe,
                residual_predicate: residual.map(|id| lowered(expressions, id)).transpose()?,
                runtime_filters: producers,
                runtime_filter_consumers: consumers,
            }
        }
        (NodeKind::NestLoopJoin { .. }, LocalJoinKind::NestLoop(_))
            if !runtime_filters.is_empty() || !runtime_filter_consumers.is_empty() =>
        {
            return Err(FragmentCompileError::Invalid(
                "nested-loop join has runtime-filter producers",
            ));
        }
        (NodeKind::NestLoopJoin { predicate, .. }, LocalJoinKind::NestLoop(join_type)) => {
            ProgramNodeKind::NestedLoopJoin {
                left: probe.node,
                right: build.node,
                join_type,
                join_conjunct: predicate.map(|id| lowered(expressions, id)).transpose()?,
                left_layout: probe.layout.clone(),
                right_layout: build.layout.clone(),
                join_scope_layout: scope,
            }
        }
        _ => {
            return Err(FragmentCompileError::Invalid(
                "join family differs from its orientation",
            ));
        }
    };
    let pieces = if planned.selection.is_some() { 2 } else { 1 };
    let mut channels = Vec::new();
    let channel_count = planned
        .canonical_types
        .len()
        .checked_add(
            width
                .checked_mul(2)
                .ok_or(CompileControlError::ResourceExhausted)?,
        )
        .and_then(|count| count.checked_add(published.len()))
        .ok_or(CompileControlError::ResourceExhausted)?;
    reserve_vec(&mut channels, channel_count, work)?;
    layout_role_channels(
        join,
        ProgramChannelLayoutRole::NodeOutput,
        planned.canonical_types.iter().cloned(),
        &mut channels,
        work,
    )?;
    layout_role_channels(
        join,
        ProgramChannelLayoutRole::JoinLeft,
        probe_types.iter().cloned(),
        &mut channels,
        work,
    )?;
    layout_role_channels(
        join,
        ProgramChannelLayoutRole::JoinRight,
        build_types.iter().cloned(),
        &mut channels,
        work,
    )?;
    layout_role_channels(
        join,
        ProgramChannelLayoutRole::JoinScope,
        probe_types.into_iter().chain(build_types),
        &mut channels,
        work,
    )?;
    let source = DiagnosticSourceNodeId::new(node.id.get());
    let owner = LocalOperatorId::new(
        u32::try_from(join.index()).map_err(|_| CompileControlError::ResourceExhausted)?,
    );
    let metrics = OperatorMetricAggregation {
        cpu_time: MetricAggregation::Sum,
        wall_time: MetricAggregation::Maximum,
        peak_retained_bytes: MetricAggregation::Maximum,
    };
    let mut nodes = Vec::new();
    reserve_vec(&mut nodes, pieces, work)?;
    let mut operators = Vec::new();
    reserve_vec(&mut operators, pieces, work)?;
    let mut selection_roots = Vec::new();
    nodes.push(ProgramNode::new_local(join, vec![source], kind, canonical));
    operators.push(LocalOperatorProvenance {
        id: owner,
        lowered_nodes: Box::from([join]),
        sources: Box::from([source]),
        origin: if pieces == 1 {
            LocalOperatorOrigin::Direct
        } else {
            LocalOperatorOrigin::Split { piece: 0 }
        },
        cost_owner: owner,
        metrics,
    });
    if let Some(sources) = &planned.selection {
        let definitions = selection_definitions.ok_or(FragmentCompileError::Invalid(
            "missing join selection definitions",
        ))?;
        if definitions.len() != sources.len()
            || published.len() != sources.len()
            || node.output.columns.len() != sources.len()
        {
            return Err(FragmentCompileError::Invalid(
                "join selection width differs from its physical output",
            ));
        }
        let local = ProgramNodeId::new(
            join.index()
                .checked_add(1)
                .ok_or(CompileControlError::ResourceExhausted)?,
        );
        let mut types = Vec::new();
        reserve_vec(&mut types, sources.len(), work)?;
        reserve_vec(&mut selection_roots, sources.len(), work)?;
        for (ordinal, (&canonical_ordinal, &definition)) in
            sources.iter().zip(definitions).enumerate()
        {
            types.push(planned.canonical_types[canonical_ordinal as usize].clone());
            selection_roots.push(UnionRoot {
                node: local,
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| CompileControlError::ResourceExhausted)?,
                definition,
                source: ProgramChannelSite::Layout {
                    node: join,
                    role: ProgramChannelLayoutRole::NodeOutput,
                    ordinal: canonical_ordinal,
                },
            });
            work.step()?;
        }
        let layout = named_layout(package, node, local, &types, published, true, work)?;
        layout_role_channels(
            local,
            ProgramChannelLayoutRole::NodeOutput,
            types,
            &mut channels,
            work,
        )?;
        nodes.push(ProgramNode::new_local(
            local,
            vec![source],
            ProgramNodeKind::Project {
                input: join,
                is_subordinate: true,
                exprs: definitions.to_vec(),
                expr_slot_ids: published.to_vec(),
                expr_slot_schemas: None,
                output_indices: None,
            },
            layout,
        ));
        operators.push(LocalOperatorProvenance {
            id: LocalOperatorId::new(
                u32::try_from(local.index()).map_err(|_| CompileControlError::ResourceExhausted)?,
            ),
            lowered_nodes: Box::from([local]),
            sources: Box::from([source]),
            origin: LocalOperatorOrigin::Split { piece: 1 },
            cost_owner: owner,
            metrics,
        });
    } else if published != planned.canonical.as_ref() {
        return Err(FragmentCompileError::Invalid(
            "join publishes slots other than its canonical output",
        ));
    }
    Ok(LoweredJoin {
        nodes,
        channels,
        operators,
        selection_roots,
    })
}
