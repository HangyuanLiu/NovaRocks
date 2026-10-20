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

//! Runtime contracts projected from compiled static filter facts.

use crate::runtime_filter as execution;
use novarocks_local_program as lp;
use std::sync::Arc;

fn runtime_filter_contract(
    contract: &lp::StaticFilterContract,
) -> Result<execution::RuntimeFilterExecutionContract, String> {
    match contract {
        lp::StaticFilterContract::Membership {
            canonical,
            value_type,
        } => {
            let schema = execution::RuntimeFilterMembershipSchema::from_canonical_value_type(
                canonical.clone(),
                value_type.clone(),
            )
            .map_err(|error| error.to_string())?;
            Ok(execution::RuntimeFilterExecutionContract::Membership(
                schema,
            ))
        }
        lp::StaticFilterContract::Ordered {
            keys,
            comparator_digest,
            contract_digest,
        } => {
            let keys = keys
                .iter()
                .map(|key| {
                    execution::contribution::RuntimeOrderKey::with_value_type(
                        key.value_type.clone(),
                        match key.direction {
                            lp::FilterSortDirection::Ascending => {
                                execution::contribution::RuntimeOrderSortDirection::Ascending
                            }
                            lp::FilterSortDirection::Descending => {
                                execution::contribution::RuntimeOrderSortDirection::Descending
                            }
                        },
                        match key.null_order {
                            lp::FilterNullOrder::First => {
                                execution::contribution::RuntimeOrderNullOrder::First
                            }
                            lp::FilterNullOrder::Last => {
                                execution::contribution::RuntimeOrderNullOrder::Last
                            }
                        },
                    )
                })
                .collect::<Vec<_>>();
            let order = execution::contribution::RuntimeOrderContract::from_fragment_contract(
                keys,
                *comparator_digest,
                *contract_digest,
            )
            .map_err(|error| error.to_string())?;
            Ok(execution::RuntimeFilterExecutionContract::Ordered(
                Arc::new(order).into(),
            ))
        }
    }
}

/// The runtime contract of one static consumer. Its membership digest is
/// checked against the canonical schema of its key type.
pub(super) fn runtime_filter_consumer_contract(
    static_consumer: &lp::StaticFilterConsumer,
) -> Result<execution::RuntimeFilterConsumerContract, String> {
    let id = execution::RuntimeFilterBindingId::new(static_consumer.binding_id());
    let channel = execution::RuntimeFilterChannelId::new(static_consumer.channel_id());
    let contract = runtime_filter_contract(static_consumer.contract())?;
    let runtime_contract = match (static_consumer.activation(), static_consumer.reduction()) {
        (lp::FilterConsumerActivation::BlockingSnapshot, lp::FilterReduction::SetUnion) => {
            execution::RuntimeFilterConsumerContract::membership_blocking(id, channel, contract)
        }
        (lp::FilterConsumerActivation::NonBlockingLive { late_apply }, reduction) => {
            let late_apply = match late_apply {
                lp::FilterLateApplyGranularity::Row => {
                    execution::RuntimeFilterLateApplyGranularity::Row
                }
                lp::FilterLateApplyGranularity::Batch => {
                    execution::RuntimeFilterLateApplyGranularity::Batch
                }
                lp::FilterLateApplyGranularity::RowGroup => {
                    execution::RuntimeFilterLateApplyGranularity::RowGroup
                }
                lp::FilterLateApplyGranularity::Split => {
                    execution::RuntimeFilterLateApplyGranularity::Split
                }
                lp::FilterLateApplyGranularity::File => {
                    execution::RuntimeFilterLateApplyGranularity::File
                }
            };
            match reduction {
                lp::FilterReduction::SetUnion => {
                    execution::RuntimeFilterConsumerContract::membership_live(
                        id, channel, late_apply, contract,
                    )
                }
                lp::FilterReduction::TightenOrderedBound => {
                    execution::RuntimeFilterConsumerContract::ordered_live(
                        id, channel, late_apply, contract,
                    )
                }
                lp::FilterReduction::MergeTopKSummary { k } => {
                    execution::RuntimeFilterConsumerContract::top_k_live(
                        id,
                        channel,
                        late_apply,
                        k.get(),
                        contract,
                    )
                }
            }
        }
        _ => {
            return Err(
                "local program runtime filter consumer activation/reduction mismatch".to_string(),
            );
        }
    }
    .map_err(|error| error.to_string())?;
    Ok(runtime_contract)
}

/// The runtime contract of one static producer. Its membership digest is
/// checked against the canonical schema of its key type.
pub(super) fn runtime_filter_producer(
    producer: &lp::StaticFilterProducer,
) -> Result<execution::RuntimeFilterProducerContract, String> {
    let id = execution::RuntimeFilterBindingId::new(producer.binding_id());
    let channel = execution::RuntimeFilterChannelId::new(producer.channel_id());
    let contract = runtime_filter_contract(producer.contract())?;
    let result = match (producer.kind(), producer.reduction()) {
        (lp::FilterProducerKind::Membership, lp::FilterReduction::SetUnion) => {
            execution::RuntimeFilterProducerContract::membership(id, channel, contract)
        }
        (lp::FilterProducerKind::FinalDomain, lp::FilterReduction::SetUnion) => {
            execution::RuntimeFilterProducerContract::final_domain(id, channel, contract)
        }
        (lp::FilterProducerKind::OrderedBound, lp::FilterReduction::TightenOrderedBound) => {
            execution::RuntimeFilterProducerContract::ordered_bound(id, channel, contract)
        }
        (lp::FilterProducerKind::TopKSummary, lp::FilterReduction::MergeTopKSummary { k }) => {
            execution::RuntimeFilterProducerContract::top_k_summary(id, channel, k.get(), contract)
        }
        _ => {
            return Err(
                "local program runtime filter producer kind/reduction mismatch".to_string(),
            );
        }
    };
    result.map_err(|error| error.to_string())
}
