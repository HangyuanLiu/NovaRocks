// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Lower one admitted outbound stream cut into the existing static sink owner.
//!
//! A physical partition key is a root output column, not an expression, so
//! the compiler authors each key as a fresh SlotId definition in a separate
//! Sink arena with its own eager Value occurrence. Gather and Broadcast still
//! own that arena, empty, because every collected arena needs a flow and types.

use crate::{assert_rows::reserve_vec, lowering::FragmentCompileError};
use novarocks_local_program::{
    BindingRequirement, DataStreamPartitionType, ExpressionsCompileError, ImmutableExpressions,
    ProgramChannelLayoutRole, ProgramChannelSite, ProgramControlFlow, ProgramEvaluationDomain,
    ProgramExprId, ProgramExpressionArena, ProgramExpressionRootSite, ProgramExpressionUse,
    ProgramLexicalSource, ProgramNodeId, ProgramRootUseBinding, ProgramSlotBinding, ProgramUseRef,
    StaticExprKind, StaticExprNode, StaticLayout, StaticSinkProgram, StaticStreamBranch,
};
use novarocks_physical_plan::{Distribution, FragmentPackage, OutboundFragmentCut, ValueId};
use novarocks_type_contract::{
    BucketLayoutAlgorithm, CompileCheckpoints, CompileControlError, CompilePhase, ControlShape,
    EvaluationDemand, EvaluationDomainId, ExpressionEffectContext, ExpressionUseId,
    FunctionArgumentType, PartitionHashAlgorithm, PureCompileControl,
};
use novarocks_types::SlotId;
use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

/// Complete Sink-arena scope: one flow and one type list for its arena, the
/// root binding of each partition key and each key's exact input occurrence.
pub(crate) struct SinkFlow {
    pub flow: ProgramControlFlow,
    pub types: Vec<FunctionArgumentType>,
    pub roots: Vec<ProgramRootUseBinding>,
    pub slots: Vec<ProgramSlotBinding>,
}

const NO_KEYS: &[ValueId] = &[];

pub(crate) struct LoweredStreamSink {
    pub sink: StaticSinkProgram,
    pub requirement: BindingRequirement,
    pub flow: SinkFlow,
}

pub(crate) fn lower_stream_sink(
    package: &FragmentPackage,
    cut: &OutboundFragmentCut,
    root: ProgramNodeId,
    root_layout: &StaticLayout,
    control: &dyn PureCompileControl,
) -> Result<LoweredStreamSink, FragmentCompileError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
    let result = lower_core(package, cut, root, root_layout, &mut work);
    if matches!(&result, Err(FragmentCompileError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}

fn lower_core(
    package: &FragmentPackage,
    cut: &OutboundFragmentCut,
    root: ProgramNodeId,
    root_layout: &StaticLayout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<LoweredStreamSink, FragmentCompileError> {
    let fragment = package.fragment();
    let root_values = &fragment
        .nodes()
        .get(&fragment.root())
        .ok_or(FragmentCompileError::Invalid("missing stream root node"))?
        .output
        .columns;
    if root_values.len() != root_layout.slots().len() {
        return Err(FragmentCompileError::Invalid(
            "stream root layout width differs from its output",
        ));
    }
    // The sender keeps today's routing key: the destination physical node.
    let dest_node_id = i32::try_from(cut.destination_node.get()).map_err(|_| {
        FragmentCompileError::Unsupported {
            node: Some(cut.destination_node),
            feature: "destination exchange node exceeds the native routing range",
        }
    })?;
    let (partition_type, keys): (DataStreamPartitionType, &[ValueId]) =
        match &cut.partitioning.source {
            Distribution::Singleton | Distribution::Broadcast => {
                (DataStreamPartitionType::Unpartitioned, NO_KEYS)
            }
            Distribution::Hash { keys, scheme } => {
                // The compiled branch names only the runtime hash family.
                if scheme.definition.algorithm != PartitionHashAlgorithm::NativeExchangeV1 {
                    return Err(FragmentCompileError::Unsupported {
                        node: None,
                        feature: "hash stream partitioning algorithm",
                    });
                }
                (DataStreamPartitionType::HashPartitioned, &keys[..])
            }
            Distribution::BucketShuffle { keys, scheme } => {
                if scheme.hash != PartitionHashAlgorithm::NativeBucketCrc32V1
                    || scheme.layout != BucketLayoutAlgorithm::DenseZeroBasedV1
                {
                    return Err(FragmentCompileError::Unsupported {
                        node: None,
                        feature: "bucket-shuffle stream partitioning algorithm",
                    });
                }
                (
                    DataStreamPartitionType::BucketShuffleHashPartitioned,
                    &keys[..],
                )
            }
            Distribution::Unconstrained | Distribution::RoundRobin => {
                return Err(FragmentCompileError::Unsupported {
                    node: None,
                    feature: "unconstrained or round-robin stream partitioning",
                });
            }
        };
    if partition_type.requires_exprs() && keys.is_empty() {
        return Err(FragmentCompileError::Invalid(
            "hash stream partitioning has no keys",
        ));
    }
    let output_columns = projected_slots(cut, root_values, root_layout, work)?;
    // Each key is the first root occurrence of its value; the cut law already
    // proved membership, which is checked again rather than assumed.
    let mut nodes = Vec::new();
    let mut types = Vec::new();
    let mut ordinals = Vec::new();
    reserve_vec(&mut nodes, keys.len(), work)?;
    reserve_vec(&mut types, keys.len(), work)?;
    reserve_vec(&mut ordinals, keys.len(), work)?;
    for key in keys {
        let ordinal = first_ordinal(root_values, *key, work)?.ok_or(
            FragmentCompileError::Invalid("stream partition key is outside the root output"),
        )?;
        let ty = &fragment
            .values()
            .get(key)
            .ok_or(FragmentCompileError::Invalid(
                "missing stream partition key type",
            ))?
            .ty;
        work.flush()?;
        nodes.push(StaticExprNode::new(
            StaticExprKind::SlotId(root_layout.slots()[ordinal]),
            ty.data_type.clone(),
            None,
        ));
        types.push(FunctionArgumentType::Value(ty.clone()));
        work.flush()?;
        ordinals.push(u32::try_from(ordinal).map_err(|_| CompileControlError::ResourceExhausted)?);
        work.step()?;
    }
    work.flush()?;
    // No legacy exception, dictionary or session-timezone capability exists
    // for a slot read; the arena is distinct from the Main arena by design.
    let arena = ImmutableExpressions::try_new_for_compile(
        nodes,
        false,
        HashMap::new(),
        None,
        work.control(),
    )
    .map_err(|error| match error {
        ExpressionsCompileError::Control(cause) => FragmentCompileError::Control(cause),
        error => FragmentCompileError::Owner {
            phase: "sink expressions",
            error: Box::new(error),
        },
    })?;
    let arena = Arc::new(arena);
    work.flush()?;
    let (flow, roots, slots) = partition_flow(root, &ordinals, arena.nodes().len(), work)?;
    let partition_exprs = (0..ordinals.len()).map(ProgramExprId::new).collect();
    work.flush()?;
    let layout = root_layout.project_by_slots_for_compile(&output_columns, work.control())?;
    work.flush()?;
    let branch = StaticStreamBranch::try_new(
        dest_node_id,
        partition_type,
        partition_exprs,
        output_columns,
        None,
    )
    .map_err(sink_error)?;
    let sink = StaticSinkProgram::try_data_stream(branch, arena).map_err(sink_error)?;
    work.flush()?;
    Ok(LoweredStreamSink {
        sink,
        requirement: BindingRequirement::ExchangeOutput { branch: 0, layout },
        flow: SinkFlow {
            flow,
            types,
            roots,
            slots,
        },
    })
}

/// Root slots in cut projection order. The positional projection of the
/// whole root output keeps repeated occurrences; any other projection names
/// each value by its first root occurrence and must not need a slot twice.
fn projected_slots(
    cut: &OutboundFragmentCut,
    root_values: &[ValueId],
    root_layout: &StaticLayout,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Vec<SlotId>, FragmentCompileError> {
    let mut positional = cut.projection.len() == root_values.len();
    for (projected, value) in cut.projection.iter().zip(root_values) {
        positional &= projected.value == *value;
        work.step()?;
    }
    let mut slots = Vec::new();
    reserve_vec(&mut slots, cut.projection.len(), work)?;
    if positional {
        for slot in root_layout.slots() {
            slots.push(*slot);
            work.step()?;
        }
    } else {
        let mut seen = BTreeSet::new();
        for projected in cut.projection.iter() {
            let ordinal = first_ordinal(root_values, projected.value, work)?.ok_or(
                FragmentCompileError::Invalid("stream projection value is outside the root output"),
            )?;
            let slot = root_layout.slots()[ordinal];
            let unique = seen.insert(slot);
            work.step()?;
            if !unique {
                return Err(FragmentCompileError::Unsupported {
                    node: None,
                    feature: "stream projection repeats a value outside the root occurrence order",
                });
            }
            slots.push(slot);
        }
    }
    // An empty branch projection means the complete input layout to the sink
    // owner; it can only express an empty cut projection of an empty root.
    if slots.is_empty() && !root_values.is_empty() {
        return Err(FragmentCompileError::Unsupported {
            node: None,
            feature: "empty stream projection of a non-empty root",
        });
    }
    Ok(slots)
}

fn first_ordinal(
    root_values: &[ValueId],
    value: ValueId,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<usize>, FragmentCompileError> {
    for (ordinal, candidate) in root_values.iter().enumerate() {
        let found = *candidate == value;
        work.step()?;
        if found {
            return Ok(Some(ordinal));
        }
    }
    Ok(None)
}

type PartitionFlow = (
    ProgramControlFlow,
    Vec<ProgramRootUseBinding>,
    Vec<ProgramSlotBinding>,
);

/// One root domain and one eager Value occurrence per key definition `i`,
/// bound to `SinkPartition { branch: 0, key: i }` and read from the root's
/// actual output occurrence. Identities are dense in this fresh Sink scope.
fn partition_flow(
    root: ProgramNodeId,
    ordinals: &[u32],
    definitions: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<PartitionFlow, FragmentCompileError> {
    let mut domains = Vec::new();
    let mut uses = Vec::new();
    let mut roots = Vec::new();
    let mut slots = Vec::new();
    reserve_vec(&mut domains, ordinals.len(), work)?;
    reserve_vec(&mut uses, ordinals.len(), work)?;
    reserve_vec(&mut roots, ordinals.len(), work)?;
    reserve_vec(&mut slots, ordinals.len(), work)?;
    for (key, &ordinal) in ordinals.iter().enumerate() {
        let identity = u32::try_from(key).map_err(|_| CompileControlError::ResourceExhausted)?;
        let domain = EvaluationDomainId::new(identity);
        let use_id = ExpressionUseId::new(identity);
        domains.push(ProgramEvaluationDomain {
            id: domain,
            parent: None,
            guard: None,
        });
        uses.push(ProgramExpressionUse {
            context: ExpressionEffectContext {
                use_id,
                domain,
                demand: EvaluationDemand::Value,
            },
            definition: ProgramExprId::new(key),
            control: ControlShape::Eager,
            arguments: Box::default(),
        });
        roots.push(ProgramRootUseBinding {
            site: ProgramExpressionRootSite::SinkPartition {
                branch: 0,
                key: identity,
            },
            use_id,
        });
        slots.push(ProgramSlotBinding {
            occurrence: ProgramUseRef {
                arena: ProgramExpressionArena::Sink,
                use_id,
            },
            source: ProgramLexicalSource::Input(ProgramChannelSite::Layout {
                node: root,
                role: ProgramChannelLayoutRole::NodeOutput,
                ordinal,
            }),
        });
        work.step()?;
    }
    work.flush()?;
    let flow = ProgramControlFlow::try_new(domains, uses, definitions, work.control())?;
    work.flush()?;
    Ok((flow, roots, slots))
}

fn sink_error(error: novarocks_local_program::StaticSinkError) -> FragmentCompileError {
    FragmentCompileError::Owner {
        phase: "stream sink",
        error: Box::new(error),
    }
}
