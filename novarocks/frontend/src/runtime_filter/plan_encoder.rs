//! Frontend-owned semantic mapping from sealed RF facts to native plan DTOs.

use novarocks::query_execution::artifact::{
    RuntimeFilterBindingAttachment, RuntimeFilterBindingEncodingView,
};
use novarocks::query_execution::contract::DistributedQueryError;
use novarocks::query_execution::{
    RuntimeFilterApplyPoint, RuntimeFilterArtifactCapability, RuntimeFilterBindingFacts,
    RuntimeFilterBindingFragmentFactsView, RuntimeFilterBindingRoleFacts,
    RuntimeFilterCompletionRequirement, RuntimeFilterConsumerActivation,
    RuntimeFilterConsumerTarget, RuntimeFilterContractFacts, RuntimeFilterContributionKind,
    RuntimeFilterLateApplyGranularity, RuntimeFilterNullOrder, RuntimeFilterProducerTarget,
    RuntimeFilterReductionFacts, RuntimeFilterSortDirection,
};
use novarocks_protocol::plan;

fn encoding_error(message: impl Into<String>) -> DistributedQueryError {
    novarocks::query_execution::contract::DistributedQueryError::new(
        novarocks::query_execution::contract::DistributedQueryErrorKind::ContractViolation,
        message,
    )
}

/// Encode the complete, stable-ordered binding table for every sealed native
/// fragment and seal it into a consuming Core attachment.
pub fn encode_binding_attachment(
    view: RuntimeFilterBindingEncodingView<'_>,
) -> Result<RuntimeFilterBindingAttachment, DistributedQueryError> {
    let tables = view
        .facts()
        .fragments()
        .map(encode_binding_table)
        .collect::<Result<Vec<_>, _>>()?;
    view.seal(tables)
}

fn encode_binding_table(
    fragment: RuntimeFilterBindingFragmentFactsView<'_>,
) -> Result<plan::RuntimeFilterBindingTable, DistributedQueryError> {
    encode_binding_table_from_facts(fragment.fragment_id(), fragment.bindings())
}

fn encode_binding_table_from_facts<'a>(
    fragment_id: u32,
    bindings: impl IntoIterator<Item = RuntimeFilterBindingFacts<'a>>,
) -> Result<plan::RuntimeFilterBindingTable, DistributedQueryError> {
    let mut previous = None;
    let bindings = bindings
        .into_iter()
        .map(|binding| {
            validate_binding_order(&mut previous, binding.binding_id())?;
            encode_binding(binding)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(plan::RuntimeFilterBindingTable {
        fragment_id,
        bindings,
    })
}

fn validate_binding_order(
    previous: &mut Option<u32>,
    binding_id: u32,
) -> Result<(), DistributedQueryError> {
    if previous.is_some_and(|prior| prior >= binding_id) {
        return Err(encoding_error(format!(
            "runtime filter binding facts are not strictly ordered: previous={previous:?} current={binding_id}"
        )));
    }
    *previous = Some(binding_id);
    Ok(())
}

fn encode_binding(
    binding: novarocks::query_execution::RuntimeFilterBindingFacts<'_>,
) -> Result<plan::RuntimeFilterBinding, DistributedQueryError> {
    Ok(plan::RuntimeFilterBinding {
        binding_id: binding.binding_id(),
        channel_id: binding.channel_id(),
        node_id: binding.node_id(),
        apply_point: encode_apply_point(binding.apply_point()),
        expression: Some(binding.expression().map_err(encoding_error)?),
        contract: Some(encode_contract(binding.contract())?),
        reduction: Some(encode_reduction(binding.reduction())),
        role: Some(encode_role(binding.role())?),
    })
}

fn encode_apply_point(apply_point: RuntimeFilterApplyPoint) -> i32 {
    match apply_point {
        RuntimeFilterApplyPoint::NodeInput => i32::from(plan::RuntimeFilterApplyPoint::NodeInput),
        RuntimeFilterApplyPoint::NodeOutput => i32::from(plan::RuntimeFilterApplyPoint::NodeOutput),
    }
}

fn encode_contract(
    contract: RuntimeFilterContractFacts<'_>,
) -> Result<plan::RuntimeFilterContract, DistributedQueryError> {
    use plan::runtime_filter_contract::Kind;
    let kind = match contract {
        RuntimeFilterContractFacts::Membership {
            canonical_schema,
            schema_digest,
        } => Kind::Membership(plan::RuntimeFilterMembershipContract {
            canonical_schema: canonical_schema.to_vec(),
            schema_digest: schema_digest.to_vec(),
        }),
        RuntimeFilterContractFacts::Ordered {
            comparator_digest,
            order_contract_digest,
            ..
        } => Kind::Ordered(plan::RuntimeFilterOrderedContract {
            keys: contract
                .ordered_keys()
                .into_iter()
                .map(|key| plan::RuntimeFilterOrderKey {
                    r#type: Some(key.r#type),
                    direction: match key.direction {
                        RuntimeFilterSortDirection::Ascending => {
                            i32::from(plan::RuntimeFilterSortDirection::Ascending)
                        }
                        RuntimeFilterSortDirection::Descending => {
                            i32::from(plan::RuntimeFilterSortDirection::Descending)
                        }
                    },
                    null_order: match key.null_order {
                        RuntimeFilterNullOrder::First => {
                            i32::from(plan::RuntimeFilterNullOrder::First)
                        }
                        RuntimeFilterNullOrder::Last => {
                            i32::from(plan::RuntimeFilterNullOrder::Last)
                        }
                    },
                })
                .collect(),
            comparator_digest: comparator_digest.to_vec(),
            order_contract_digest: order_contract_digest.to_vec(),
        }),
    };
    Ok(plan::RuntimeFilterContract { kind: Some(kind) })
}

fn encode_reduction(
    reduction: RuntimeFilterReductionFacts,
) -> plan::RuntimeFilterReductionContract {
    use plan::runtime_filter_reduction_contract::Kind;
    let kind = match reduction {
        RuntimeFilterReductionFacts::SetUnion => Kind::SetUnion(true),
        RuntimeFilterReductionFacts::TightenOrderedBound => Kind::TightenOrderedBound(true),
        RuntimeFilterReductionFacts::MergeTopKSummary { k, contract_digest } => {
            Kind::MergeTopkSummary(plan::RuntimeFilterTopKReduction {
                k,
                contract_digest: contract_digest.to_vec(),
            })
        }
    };
    plan::RuntimeFilterReductionContract { kind: Some(kind) }
}

fn encode_role(
    role: RuntimeFilterBindingRoleFacts,
) -> Result<plan::runtime_filter_binding::Role, DistributedQueryError> {
    Ok(match role {
        RuntimeFilterBindingRoleFacts::Producer {
            contribution_kinds,
            completion_requirement,
            target,
        } => plan::runtime_filter_binding::Role::Producer(plan::RuntimeFilterProducerRole {
            contribution_kinds: contribution_kinds
                .into_iter()
                .map(|kind| match kind {
                    RuntimeFilterContributionKind::ValueDomainDelta => {
                        i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta)
                    }
                    RuntimeFilterContributionKind::FinalDomainShard => {
                        i32::from(plan::RuntimeFilterContributionKind::FinalDomainShard)
                    }
                    RuntimeFilterContributionKind::OrderedBoundUpdate => {
                        i32::from(plan::RuntimeFilterContributionKind::OrderedBoundUpdate)
                    }
                    RuntimeFilterContributionKind::TopKSummary => {
                        i32::from(plan::RuntimeFilterContributionKind::TopkSummary)
                    }
                    RuntimeFilterContributionKind::ProducerClosed => {
                        i32::from(plan::RuntimeFilterContributionKind::ProducerClosed)
                    }
                })
                .collect(),
            completion_requirement: match completion_requirement {
                RuntimeFilterCompletionRequirement::ProducerClosed => {
                    i32::from(plan::RuntimeFilterCompletionRequirement::ProducerClosed)
                }
                RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen => {
                    i32::from(plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen)
                }
            },
            target: Some(match target {
                RuntimeFilterProducerTarget::JoinBuildKey { ordinal } => {
                    plan::runtime_filter_producer_role::Target::JoinBuildKey(
                        plan::RuntimeFilterJoinBuildKey { ordinal },
                    )
                }
                RuntimeFilterProducerTarget::AggregateTopNKey {
                    group_key_ordinal,
                    limit,
                } => plan::runtime_filter_producer_role::Target::AggregateTopnKey(
                    plan::RuntimeFilterAggregateTopNKey {
                        group_key_ordinal,
                        limit,
                    },
                ),
            }),
        }),
        RuntimeFilterBindingRoleFacts::Consumer {
            capabilities,
            activation,
            target,
        } => plan::runtime_filter_binding::Role::Consumer(plan::RuntimeFilterConsumerRole {
            capabilities: capabilities
                .into_iter()
                .map(|capability| match capability {
                    RuntimeFilterArtifactCapability::Membership => {
                        i32::from(plan::RuntimeFilterArtifactCapability::Membership)
                    }
                    RuntimeFilterArtifactCapability::OrderedRange => {
                        i32::from(plan::RuntimeFilterArtifactCapability::OrderedRange)
                    }
                    RuntimeFilterArtifactCapability::EmptyDomain => {
                        i32::from(plan::RuntimeFilterArtifactCapability::EmptyDomain)
                    }
                })
                .collect(),
            activation: Some(plan::RuntimeFilterConsumerActivation {
                kind: Some(match activation {
                    RuntimeFilterConsumerActivation::BlockingSnapshot => {
                        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true)
                    }
                    RuntimeFilterConsumerActivation::NonBlockingLive(granularity) => {
                        plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(
                            match granularity {
                                RuntimeFilterLateApplyGranularity::Row => {
                                    i32::from(plan::RuntimeFilterLateApplyGranularity::Row)
                                }
                                RuntimeFilterLateApplyGranularity::Batch => {
                                    i32::from(plan::RuntimeFilterLateApplyGranularity::Batch)
                                }
                                RuntimeFilterLateApplyGranularity::RowGroup => {
                                    i32::from(plan::RuntimeFilterLateApplyGranularity::RowGroup)
                                }
                                RuntimeFilterLateApplyGranularity::Split => {
                                    i32::from(plan::RuntimeFilterLateApplyGranularity::Split)
                                }
                                RuntimeFilterLateApplyGranularity::File => {
                                    i32::from(plan::RuntimeFilterLateApplyGranularity::File)
                                }
                            },
                        )
                    }
                }),
            }),
            target: Some(match target {
                RuntimeFilterConsumerTarget::DirectInputOrdinal(ordinal) => {
                    plan::runtime_filter_consumer_role::Target::DirectInputOrdinal(ordinal)
                }
                RuntimeFilterConsumerTarget::SourceBoundary => {
                    plan::runtime_filter_consumer_role::Target::SourceBoundary(true)
                }
            }),
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use novarocks::runtime_filter_transition::model::contract::{NullOrder, SortDirection};
    use novarocks::runtime_filter_transition::port::ordered_bound::RuntimeOrderKey;
    use plan::runtime_filter_binding::Role;
    use plan::runtime_filter_consumer_activation::Kind as ActivationKind;
    use plan::runtime_filter_reduction_contract::Kind as ReductionKind;

    #[test]
    fn empty_fragment_encodes_an_explicit_empty_binding_table() {
        let table = encode_binding_table_from_facts(
            17,
            std::iter::empty::<RuntimeFilterBindingFacts<'static>>(),
        )
        .expect("empty sealed fragment must encode");

        assert_eq!(table.fragment_id, 17);
        assert!(table.bindings.is_empty());
    }

    #[test]
    fn binding_order_rejects_duplicate_and_reversed_ids() {
        let mut previous = None;
        validate_binding_order(&mut previous, 7).expect("first binding");
        let duplicate =
            validate_binding_order(&mut previous, 7).expect_err("duplicate binding id must fail");
        assert!(duplicate.to_string().contains("previous=Some(7) current=7"));

        let mut previous = Some(9);
        let reversed =
            validate_binding_order(&mut previous, 8).expect_err("reversed binding id must fail");
        assert!(reversed.to_string().contains("previous=Some(9) current=8"));
    }

    #[test]
    fn apply_points_map_without_defaults() {
        assert_eq!(
            encode_apply_point(RuntimeFilterApplyPoint::NodeInput),
            i32::from(plan::RuntimeFilterApplyPoint::NodeInput)
        );
        assert_eq!(
            encode_apply_point(RuntimeFilterApplyPoint::NodeOutput),
            i32::from(plan::RuntimeFilterApplyPoint::NodeOutput)
        );
    }

    #[test]
    fn membership_and_ordered_contracts_preserve_semantic_fields() {
        let membership = encode_contract(RuntimeFilterContractFacts::Membership {
            canonical_schema: &[1, 2, 3],
            schema_digest: [4; 32],
        })
        .expect("membership contract");
        let Some(plan::runtime_filter_contract::Kind::Membership(membership)) = membership.kind
        else {
            panic!("membership kind");
        };
        assert_eq!(membership.canonical_schema, vec![1, 2, 3]);
        assert_eq!(membership.schema_digest, vec![4; 32]);

        let keys = [
            RuntimeOrderKey::new(DataType::Int64, SortDirection::Ascending, NullOrder::First),
            RuntimeOrderKey::new(DataType::Utf8, SortDirection::Descending, NullOrder::Last),
        ];
        let ordered = encode_contract(RuntimeFilterContractFacts::Ordered {
            keys: &keys,
            comparator_digest: [5; 32],
            order_contract_digest: [6; 32],
        })
        .expect("ordered contract");
        let Some(plan::runtime_filter_contract::Kind::Ordered(ordered)) = ordered.kind else {
            panic!("ordered kind");
        };
        assert_eq!(ordered.keys.len(), 2);
        assert_eq!(
            ordered.keys[0].direction,
            i32::from(plan::RuntimeFilterSortDirection::Ascending)
        );
        assert_eq!(
            ordered.keys[0].null_order,
            i32::from(plan::RuntimeFilterNullOrder::First)
        );
        assert_eq!(
            ordered.keys[1].direction,
            i32::from(plan::RuntimeFilterSortDirection::Descending)
        );
        assert_eq!(
            ordered.keys[1].null_order,
            i32::from(plan::RuntimeFilterNullOrder::Last)
        );
        assert!(ordered.keys.iter().all(|key| key.r#type.is_some()));
        assert_eq!(ordered.comparator_digest, vec![5; 32]);
        assert_eq!(ordered.order_contract_digest, vec![6; 32]);
    }

    #[test]
    fn reductions_map_every_sealed_variant() {
        assert_eq!(
            encode_reduction(RuntimeFilterReductionFacts::SetUnion).kind,
            Some(ReductionKind::SetUnion(true))
        );
        assert_eq!(
            encode_reduction(RuntimeFilterReductionFacts::TightenOrderedBound).kind,
            Some(ReductionKind::TightenOrderedBound(true))
        );
        assert_eq!(
            encode_reduction(RuntimeFilterReductionFacts::MergeTopKSummary {
                k: 11,
                contract_digest: [7; 32],
            })
            .kind,
            Some(ReductionKind::MergeTopkSummary(
                plan::RuntimeFilterTopKReduction {
                    k: 11,
                    contract_digest: vec![7; 32],
                }
            ))
        );
    }

    #[test]
    fn producer_role_preserves_contributions_completion_and_targets() {
        let role = encode_role(RuntimeFilterBindingRoleFacts::Producer {
            contribution_kinds: vec![
                RuntimeFilterContributionKind::ValueDomainDelta,
                RuntimeFilterContributionKind::FinalDomainShard,
                RuntimeFilterContributionKind::OrderedBoundUpdate,
                RuntimeFilterContributionKind::TopKSummary,
                RuntimeFilterContributionKind::ProducerClosed,
            ],
            completion_requirement: RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen,
            target: RuntimeFilterProducerTarget::JoinBuildKey { ordinal: 3 },
        })
        .expect("producer role");
        let Role::Producer(producer) = role else {
            panic!("producer role");
        };
        assert_eq!(
            producer.contribution_kinds,
            vec![
                i32::from(plan::RuntimeFilterContributionKind::ValueDomainDelta),
                i32::from(plan::RuntimeFilterContributionKind::FinalDomainShard),
                i32::from(plan::RuntimeFilterContributionKind::OrderedBoundUpdate),
                i32::from(plan::RuntimeFilterContributionKind::TopkSummary),
                i32::from(plan::RuntimeFilterContributionKind::ProducerClosed),
            ]
        );
        assert_eq!(
            producer.completion_requirement,
            i32::from(plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen)
        );
        assert_eq!(
            producer.target,
            Some(plan::runtime_filter_producer_role::Target::JoinBuildKey(
                plan::RuntimeFilterJoinBuildKey { ordinal: 3 }
            ))
        );

        let Role::Producer(aggregate) = encode_role(RuntimeFilterBindingRoleFacts::Producer {
            contribution_kinds: vec![RuntimeFilterContributionKind::ProducerClosed],
            completion_requirement: RuntimeFilterCompletionRequirement::ProducerClosed,
            target: RuntimeFilterProducerTarget::AggregateTopNKey {
                group_key_ordinal: 4,
                limit: 9,
            },
        })
        .expect("aggregate producer role") else {
            panic!("aggregate producer role");
        };
        assert_eq!(
            aggregate.target,
            Some(
                plan::runtime_filter_producer_role::Target::AggregateTopnKey(
                    plan::RuntimeFilterAggregateTopNKey {
                        group_key_ordinal: 4,
                        limit: 9,
                    }
                )
            )
        );
    }

    #[test]
    fn consumer_role_preserves_capabilities_activation_and_targets() {
        let Role::Consumer(blocking) = encode_role(RuntimeFilterBindingRoleFacts::Consumer {
            capabilities: vec![
                RuntimeFilterArtifactCapability::Membership,
                RuntimeFilterArtifactCapability::OrderedRange,
                RuntimeFilterArtifactCapability::EmptyDomain,
            ],
            activation: RuntimeFilterConsumerActivation::BlockingSnapshot,
            target: RuntimeFilterConsumerTarget::DirectInputOrdinal(5),
        })
        .expect("blocking consumer") else {
            panic!("consumer role");
        };
        assert_eq!(
            blocking.capabilities,
            vec![
                i32::from(plan::RuntimeFilterArtifactCapability::Membership),
                i32::from(plan::RuntimeFilterArtifactCapability::OrderedRange),
                i32::from(plan::RuntimeFilterArtifactCapability::EmptyDomain),
            ]
        );
        assert_eq!(
            blocking.activation.and_then(|activation| activation.kind),
            Some(ActivationKind::BlockingSnapshot(true))
        );
        assert_eq!(
            blocking.target,
            Some(plan::runtime_filter_consumer_role::Target::DirectInputOrdinal(5))
        );

        for (granularity, expected) in [
            (
                RuntimeFilterLateApplyGranularity::Row,
                plan::RuntimeFilterLateApplyGranularity::Row,
            ),
            (
                RuntimeFilterLateApplyGranularity::Batch,
                plan::RuntimeFilterLateApplyGranularity::Batch,
            ),
            (
                RuntimeFilterLateApplyGranularity::RowGroup,
                plan::RuntimeFilterLateApplyGranularity::RowGroup,
            ),
            (
                RuntimeFilterLateApplyGranularity::Split,
                plan::RuntimeFilterLateApplyGranularity::Split,
            ),
            (
                RuntimeFilterLateApplyGranularity::File,
                plan::RuntimeFilterLateApplyGranularity::File,
            ),
        ] {
            let Role::Consumer(live) = encode_role(RuntimeFilterBindingRoleFacts::Consumer {
                capabilities: vec![RuntimeFilterArtifactCapability::Membership],
                activation: RuntimeFilterConsumerActivation::NonBlockingLive(granularity),
                target: RuntimeFilterConsumerTarget::SourceBoundary,
            })
            .expect("live consumer") else {
                panic!("consumer role");
            };
            assert_eq!(
                live.activation.and_then(|activation| activation.kind),
                Some(ActivationKind::NonBlockingLive(i32::from(expected)))
            );
            assert_eq!(
                live.target,
                Some(plan::runtime_filter_consumer_role::Target::SourceBoundary(
                    true
                ))
            );
        }
    }
}
