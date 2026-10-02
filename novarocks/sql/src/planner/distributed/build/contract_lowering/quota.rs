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

//! Closed visible-bag quota and one-evaluation predicate fanout lowering.

use super::*;
use crate::planner::quota::{
    PlanFanoutAnchorNode, PlanFanoutConsumeNode, PlanFanoutDistribution, PlanQuotaNeed,
    PlanQuotaPreclaimNode, PlanQuotaTrimNode,
};
use novarocks_physical_plan::{
    PhysicalNode, PredicateFanoutBranch, QuotaNeed, QuotaPreclaimSpec, QuotaTrimSpec,
    RuntimeFilterApplyPoint,
};
use novarocks_type_contract::ResultContentEquivalence;

pub(super) struct FanoutProducer {
    fragment: FragmentId,
    root: NodeId,
    outputs: BTreeMap<ColumnId, (ValueId, ValueType)>,
    predicates: Vec<ExprId>,
    distributions: Vec<PlanFanoutDistribution>,
    edges: Vec<Option<EdgeId>>,
}

fn quota_value(input: &LoweredNode, column: ColumnId) -> Result<ValueId, ContractLoweringError> {
    input
        .columns
        .get(&column)
        .copied()
        .ok_or(ContractLoweringError::UnknownColumnReference(column))
}
fn quota_need(
    input: &LoweredNode,
    need: PlanQuotaNeed,
) -> Result<QuotaNeed, ContractLoweringError> {
    let value = quota_value(input, need.column())?;
    Ok(match need {
        PlanQuotaNeed::Count(_) => QuotaNeed::Count { value },
        PlanQuotaNeed::NegativeWeight(_) => QuotaNeed::NegativeWeight { value },
    })
}
fn single_properties() -> PhysicalProperties {
    PhysicalProperties {
        distribution: Distribution::Unconstrained,
        row_multiplicity: RowMultiplicity::SingleCopy,
        ordering: Box::default(),
    }
}

impl ContractLoweringVisitor {
    pub(super) fn lower_quota_preclaim(
        &mut self,
        plan: &PhysicalPlanNode,
        spec: &PlanQuotaPreclaimNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 2)?;
        require_output_shape("QuotaPreclaim", &plan.output_columns, &spec.output_columns)?;
        let demand = self.lower_node(&plan.children[0])?;
        let target = self.lower_node(&plan.children[1])?;
        if demand.fragment != self.current_fragment || target.fragment != self.current_fragment {
            return Err(invalid_write(
                "quota inputs do not belong to their owning fragment".into(),
            ));
        }
        let node = self.fragment_mut().reserve_node_id()?;
        if self.quota_domains.insert(spec.domain, node).is_some() {
            return Err(invalid_write(
                "quota domain label repeats a preclaim definition".into(),
            ));
        }
        let kind = NodeKind::QuotaPreclaim {
            spec: QuotaPreclaimSpec {
                demand_entry_id: quota_value(&demand, spec.demand_entry_id)?,
                demand_key: quota_value(&demand, spec.demand_key)?,
                demand_need: quota_need(&demand, spec.demand_need)?,
                demand_values: spec
                    .demand_values
                    .iter()
                    .map(|id| quota_value(&demand, *id))
                    .collect::<Result<Box<[_]>, _>>()?,
                target_values: spec
                    .target_values
                    .iter()
                    .map(|id| quota_value(&target, *id))
                    .collect::<Result<Box<[_]>, _>>()?,
                target_file: quota_value(&target, spec.target_file)?,
                target_position: quota_value(&target, spec.target_position)?,
                content_equivalence: ResultContentEquivalence::NativeResultContentV1,
                preselection_domain: node,
                max_state_bytes: spec.max_state_bytes,
            },
        };
        let required = vec![
            PhysicalProperties {
                distribution: Distribution::Broadcast,
                row_multiplicity: RowMultiplicity::Replicated,
                ordering: Box::default(),
            },
            single_properties(),
        ];
        if matches!(plan.children[1].kind, PhysicalPlanKind::Scan(_)) {
            for (ordinal, (demand_id, target_id)) in spec
                .demand_values
                .iter()
                .zip(&spec.target_values)
                .enumerate()
            {
                let demand_value = quota_value(&demand, *demand_id)?;
                let target_value = quota_value(&target, *target_id)?;
                let ty = self.value_declared_type_in(self.current_fragment, demand_value)?;
                if novarocks_physical_plan::quota_content_runtime_filter_type_supported(
                    &ty.data_type,
                ) {
                    self.quota_filters.push(PendingQuotaFilter {
                        fragment: self.current_fragment,
                        preclaim: node,
                        field_ordinal: checked_ordinal("quota content field", ordinal)?,
                        demand: demand_value,
                        scan: target.node,
                        target: target_value,
                        ty,
                    });
                }
            }
        }
        self.insert_quota_node(
            plan,
            node,
            [demand.node, target.node],
            required.into_boxed_slice(),
            kind,
        )
    }

    pub(super) fn lower_quota_trim(
        &mut self,
        plan: &PhysicalPlanNode,
        spec: &PlanQuotaTrimNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 2)?;
        require_output_shape("QuotaTrim", &plan.output_columns, &spec.output_columns)?;
        // Candidate lowering defines the exact Preclaim domain before the
        // seed references it. Both receivers use this destination's one scheme.
        let scheme = self.allocate_hash_scheme()?;
        let candidate = self.lower_quota_hash_input(&plan.children[1], &scheme)?;
        let seed = self.lower_quota_hash_input(&plan.children[0], &scheme)?;
        let domain = *self.quota_domains.get(&spec.domain).ok_or_else(|| {
            invalid_write("quota trim references an undefined preclaim domain".into())
        })?;
        let node = self.fragment_mut().reserve_node_id()?;
        let seed_entry_id = quota_value(&seed, spec.seed_entry_id)?;
        let candidate_entry_id = quota_value(&candidate, spec.candidate_entry_id)?;
        let kind = NodeKind::QuotaTrim {
            spec: QuotaTrimSpec {
                seed_entry_id,
                seed_need: quota_need(&seed, spec.seed_need)?,
                candidate_entry_id,
                candidate_file: quota_value(&candidate, spec.candidate_file)?,
                candidate_position: quota_value(&candidate, spec.candidate_position)?,
                content_equivalence: ResultContentEquivalence::NativeResultContentV1,
                preselection_domain: domain,
                max_state_bytes: spec.max_state_bytes,
            },
        };
        let required = [seed_entry_id, candidate_entry_id]
            .into_iter()
            .map(|key| PhysicalProperties {
                distribution: Distribution::Hash {
                    keys: Box::from([key]),
                    scheme: scheme.clone(),
                },
                row_multiplicity: RowMultiplicity::SingleCopy,
                ordering: Box::default(),
            })
            .collect();
        self.insert_quota_node(plan, node, [seed.node, candidate.node], required, kind)
    }

    fn lower_quota_hash_input(
        &mut self,
        plan: &PhysicalPlanNode,
        scheme: &HashPartitionScheme,
    ) -> Result<LoweredNode, ContractLoweringError> {
        if let PhysicalPlanKind::Redistribute(redistribute) = &plan.kind
            && matches!(redistribute.mode, RedistributeMode::Hash { .. })
        {
            self.lower_redistribute(plan, redistribute, Some(scheme.clone()))
        } else {
            self.lower_node(plan)
        }
    }

    fn insert_quota_node(
        &mut self,
        plan: &PhysicalPlanNode,
        node: NodeId,
        inputs: [NodeId; 2],
        required_inputs: Box<[PhysicalProperties]>,
        kind: NodeKind,
    ) -> Result<LoweredNode, ContractLoweringError> {
        let mut columns = BTreeMap::new();
        let mut output = Vec::new();
        for (ordinal, column) in plan.output_columns.iter().enumerate() {
            let value = self.fragment_mut().add_value(
                value_type(column),
                ValueOrigin::NodeOutput {
                    node,
                    output_ordinal: checked_ordinal("quota output", ordinal)?,
                },
            )?;
            if columns.insert(column.column_id, value).is_some() {
                return Err(invalid_write(
                    "quota output repeats a column identity".into(),
                ));
            }
            output.push(value);
        }
        let properties = single_properties();
        self.fragment_mut().insert_node_unchecked(PhysicalNode {
            id: node,
            inputs: Box::from(inputs),
            required_inputs,
            output_properties: properties.clone(),
            output: OutputPort {
                node,
                columns: output.clone().into_boxed_slice(),
            },
            kind,
        })?;
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: plan
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
        })
    }

    pub(super) fn lower_fanout_anchor(
        &mut self,
        plan: &PhysicalPlanNode,
        anchor: &PlanFanoutAnchorNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 2)?;
        if anchor.branches.is_empty() || self.fanout_producers.contains_key(&anchor.id) {
            return Err(invalid_write(
                "fanout requires one unique nonempty definition".into(),
            ));
        }
        let destination = self.current_fragment;
        let producer_fragment = self.allocate_fragment()?;
        self.current_fragment = producer_fragment;
        let producer = self.lower_node(&plan.children[0])?;
        if producer.fragment != producer_fragment {
            return Err(invalid_write(
                "fanout producer changed its owning fragment".into(),
            ));
        }
        require_single_copy_input("PredicateFanout", &producer.properties)?;
        let mut outputs = BTreeMap::new();
        for (column, value) in plan.children[0].output_columns.iter().zip(&producer.output) {
            if outputs
                .insert(column.column_id, (*value, value_type(column)))
                .is_some()
            {
                return Err(invalid_write(
                    "fanout producer repeats a column identity".into(),
                ));
            }
        }
        let predicates = anchor
            .branches
            .iter()
            .map(|branch| {
                self.lower_expression(producer.node, &branch.predicate, &producer.columns)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.fanout_producers.insert(
            anchor.id,
            FanoutProducer {
                fragment: producer_fragment,
                root: producer.node,
                outputs,
                predicates,
                distributions: anchor
                    .branches
                    .iter()
                    .map(|branch| branch.distribution.clone())
                    .collect(),
                edges: vec![None; anchor.branches.len()],
            },
        );
        self.current_fragment = destination;
        let body = self.lower_node(&plan.children[1])?;
        require_output_shape(
            "PredicateFanout",
            &plan.output_columns,
            &plan.children[1].output_columns,
        )?;
        let producer = self
            .fanout_producers
            .remove(&anchor.id)
            .expect("fanout stays active through body lowering");
        let branches = producer
            .edges
            .into_iter()
            .zip(producer.predicates)
            .map(|(edge, predicate)| {
                Ok(PredicateFanoutBranch {
                    edge: edge.ok_or_else(|| {
                        invalid_write("fanout branch has no exact consumer".into())
                    })?,
                    predicate,
                })
            })
            .collect::<Result<Box<[_]>, ContractLoweringError>>()?;
        self.complete_fragment(
            producer.fragment,
            producer.root,
            FragmentSink::PredicateFanout { branches },
        )?;
        Ok(body)
    }

    pub(super) fn lower_fanout_consume(
        &mut self,
        plan: &PhysicalPlanNode,
        consume: &PlanFanoutConsumeNode,
    ) -> Result<LoweredNode, ContractLoweringError> {
        expect_children(plan, 0)?;
        require_output_shape(
            "FanoutConsume",
            &plan.output_columns,
            &consume.output_columns,
        )?;
        if consume.output_columns.len() != consume.producer_column_ids.len() {
            return Err(invalid_write(
                "fanout consumer mapping arity differs".into(),
            ));
        }
        let producer = self
            .fanout_producers
            .get(&consume.anchor)
            .ok_or_else(|| invalid_write("fanout consumer has no in-scope producer".into()))?;
        if producer.distributions.get(consume.branch) != Some(&consume.distribution)
            || producer.edges[consume.branch].is_some()
        {
            return Err(invalid_write(
                "fanout branch is unknown, repeated or has a conflicting distribution".into(),
            ));
        }
        let source_fragment = producer.fragment;
        let selected = consume
            .output_columns
            .iter()
            .zip(&consume.producer_column_ids)
            .map(|(column, producer_id)| {
                let (value, ty) = producer
                    .outputs
                    .get(producer_id)
                    .ok_or(ContractLoweringError::UnknownColumnReference(*producer_id))?;
                if *ty != value_type(column) {
                    return Err(invalid_write(
                        "fanout consumer changed the frozen field type".into(),
                    ));
                }
                Ok((*producer_id, *value, ty.clone()))
            })
            .collect::<Result<Vec<_>, ContractLoweringError>>()?;
        if source_fragment == self.current_fragment {
            return Err(invalid_write(
                "fanout consumer cannot receive in its producer fragment".into(),
            ));
        }
        let edge = self.plan_builder.reserve_edge_id()?;
        let receiver = self.fragment_mut().reserve_node_id()?;
        let mut projection = Vec::new();
        let mut mapping = Vec::new();
        let mut output = Vec::new();
        let mut columns = BTreeMap::new();
        let mut producer_to_import = BTreeMap::new();
        for (column, (producer_id, value, ty)) in consume.output_columns.iter().zip(&selected) {
            let imported = self.add_inherited_value(
                ty.clone(),
                ValueOrigin::ExchangeImport {
                    edge,
                    source_value: *value,
                },
                source_fragment,
                *value,
            )?;
            projection.push(*value);
            mapping.push((*value, imported));
            output.push(imported);
            if columns.insert(column.column_id, imported).is_some()
                || producer_to_import.insert(*producer_id, imported).is_some()
            {
                return Err(invalid_write(
                    "fanout consumer repeats a field mapping".into(),
                ));
            }
        }
        let (source_distribution, destination_distribution, multiplicity) =
            match &consume.distribution {
                PlanFanoutDistribution::RoundRobin => (
                    Distribution::Unconstrained,
                    Distribution::Unconstrained,
                    RowMultiplicity::SingleCopy,
                ),
                PlanFanoutDistribution::Broadcast => (
                    Distribution::Broadcast,
                    Distribution::Broadcast,
                    RowMultiplicity::Replicated,
                ),
                PlanFanoutDistribution::Hash(keys) => {
                    if keys.is_empty() {
                        return Err(invalid_write(
                            "fanout hash branch requires entry keys".into(),
                        ));
                    }
                    let scheme = self.allocate_hash_scheme()?;
                    let source_keys = keys
                        .iter()
                        .map(|key| {
                            selected
                                .iter()
                                .find(|(id, _, _)| id == key)
                                .map(|(_, value, _)| *value)
                                .ok_or(ContractLoweringError::UnknownColumnReference(*key))
                        })
                        .collect::<Result<Box<[_]>, _>>()?;
                    let destination_keys = keys
                        .iter()
                        .map(|key| {
                            producer_to_import
                                .get(key)
                                .copied()
                                .ok_or(ContractLoweringError::UnknownColumnReference(*key))
                        })
                        .collect::<Result<Box<[_]>, _>>()?;
                    (
                        Distribution::Hash {
                            keys: source_keys,
                            scheme: scheme.clone(),
                        },
                        Distribution::Hash {
                            keys: destination_keys,
                            scheme,
                        },
                        RowMultiplicity::SingleCopy,
                    )
                }
            };
        self.fragment_mut().add_exchange_source(
            receiver,
            edge,
            mapping.clone().into_boxed_slice(),
            output.clone().into_boxed_slice(),
            destination_distribution.clone(),
            multiplicity,
        )?;
        let properties = self
            .fragment_mut()
            .node_output_properties(receiver)
            .expect("fanout receiver was inserted")
            .clone();
        let contract = Edge {
            id: edge,
            kind: EdgeKind::PredicateFanout,
            source: EdgeSource {
                fragment: source_fragment,
                projection: projection.into_boxed_slice(),
            },
            destination: EdgeDestination {
                fragment: self.current_fragment,
                node: receiver,
                receive_mapping: mapping.into_boxed_slice(),
            },
            partitioning: EdgePartitioning {
                source: source_distribution,
                source_multiplicity: RowMultiplicity::SingleCopy,
                destination: destination_distribution,
                destination_multiplicity: multiplicity,
            },
        };
        self.plan_builder.add_edge(contract.clone())?;
        self.edges.insert(edge, contract);
        self.fanout_producers
            .get_mut(&consume.anchor)
            .expect("fanout is active")
            .edges[consume.branch] = Some(edge);
        Ok(LoweredNode {
            fragment: self.current_fragment,
            node: receiver,
            output: output.into_boxed_slice(),
            columns,
            properties,
            display_names: consume
                .output_columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
        })
    }
}

pub(super) struct PendingQuotaFilter {
    fragment: FragmentId,
    preclaim: NodeId,
    field_ordinal: u32,
    demand: ValueId,
    scan: NodeId,
    target: ValueId,
    ty: ValueType,
}
impl ContractLoweringVisitor {
    pub(super) fn allocate_quota_filter_ids(
        &mut self,
    ) -> Result<Vec<RuntimeFilterId>, ContractLoweringError> {
        let mut next = self
            .runtime_filter_builds
            .keys()
            .chain(self.runtime_filter_probes.keys())
            .map(|id| runtime_filter_id(*id).map(|id| id.get()))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max()
            .unwrap_or(0);
        let mut ids = Vec::with_capacity(self.quota_filters.len());
        for index in 0..self.quota_filters.len() {
            next = next
                .checked_add(1)
                .ok_or(ContractLoweringError::IdentitySpaceExhausted(
                    "quota runtime filter",
                ))?;
            let id = RuntimeFilterId::new(next);
            self.attach_runtime_filter(self.quota_filters[index].fragment, id)?;
            ids.push(id);
        }
        Ok(ids)
    }
    pub(super) fn materialize_quota_filters(
        &self,
        ids: &[RuntimeFilterId],
        fragments: &BTreeMap<FragmentId, Fragment>,
    ) -> Result<Vec<RuntimeFilter>, ContractLoweringError> {
        self.quota_filters
            .iter()
            .zip(ids)
            .map(|(pending, id)| {
                let fragment = fragments.get(&pending.fragment).ok_or(
                    ContractLoweringError::IncompleteFragment {
                        fragment: pending.fragment,
                    },
                )?;
                let preclaim = fragment.nodes().get(&pending.preclaim).ok_or(
                    ContractLoweringError::IdentitySpaceExhausted("quota filter producer"),
                )?;
                let build = subtree_inbound_edges(fragment, preclaim.inputs[0]);
                let non_build = fragment_inbound_edges(fragment)
                    .difference(&build)
                    .copied()
                    .collect::<Vec<_>>();
                let witness = RuntimeFilterWitnessId::new(id.get());
                Ok(RuntimeFilter {
                    id: *id,
                    domain: RuntimeFilterDomain::Membership {
                        ty: pending.ty.clone(),
                        null_semantics: RuntimeFilterNullSemantics::NullSafeEqual,
                    },
                    kind: RuntimeFilterKind::InList,
                    lifecycle: RuntimeFilterLifecycle::CompleteOnce,
                    reduction: RuntimeFilterReduction::SetUnion,
                    availability_coverage: leaf_runtime_filter_witness(witness),
                    terminal_coverage: leaf_runtime_filter_witness(witness),
                    equality_witnesses: Box::default(),
                    producers: Box::from([RuntimeFilterProducer {
                        witness,
                        endpoint: RuntimeFilterEndpoint {
                            fragment: pending.fragment,
                            node: pending.preclaim,
                            values: Box::from([pending.demand]),
                        },
                        apply_point: RuntimeFilterApplyPoint::NodeInput { input_ordinal: 0 },
                        contribution_kinds: Box::from([
                            RuntimeFilterContributionKind::ValueDomainDelta,
                            RuntimeFilterContributionKind::ProducerClosed,
                        ]),
                        completion: RuntimeFilterCompletion::ProducerClosed,
                        progress: RuntimeFilterProducerProgress {
                            build_edges: build.into_iter().collect(),
                            non_build_edges: non_build.into_boxed_slice(),
                        },
                        target: RuntimeFilterProducerTarget::QuotaContentField {
                            field_ordinal: pending.field_ordinal,
                            content_equivalence: ResultContentEquivalence::NativeResultContentV1,
                        },
                    }]),
                    consumers: Box::from([RuntimeFilterConsumer {
                        endpoint: RuntimeFilterEndpoint {
                            fragment: pending.fragment,
                            node: pending.scan,
                            values: Box::from([pending.target]),
                        },
                        apply_point: RuntimeFilterApplyPoint::ScanSource,
                        capabilities: Box::from([
                            RuntimeFilterArtifactCapability::Membership,
                            RuntimeFilterArtifactCapability::EmptyDomain,
                        ]),
                        activation:
                            RuntimeFilterConsumerActivation::StartUnfilteredThenApplyComplete {
                                late_apply: novarocks_physical_plan::LateApplyGranularity::Batch,
                            },
                        target: RuntimeFilterConsumerTarget::QuotaContentScanField {
                            producer: witness,
                            lineage: Box::default(),
                        },
                    }]),
                    policy: RuntimeFilterPolicy {
                        max_contribution_bytes: 1024,
                        max_artifact_bytes: 4096,
                        deadline_ms: 30000,
                        max_retries: 3,
                    },
                })
            })
            .collect()
    }
}
