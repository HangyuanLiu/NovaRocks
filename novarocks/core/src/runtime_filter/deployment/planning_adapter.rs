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

//! One-way SQL planning to runtime-install value projection.
// Design: ADR-0027 (docs/adr/ADR-0027-sql-runtime-filter-planning-ownership.md)
//!
//! Deliberately no `RuntimeFilterGraph` conversion exists here. Deployment
//! consumes a sealed SQL graph and materializes only the role/install facts it
//! needs for the current query attempt.

use std::collections::{BTreeMap, BTreeSet};

use crate::runtime_filter::model::{contract as runtime, coverage as runtime_coverage};
use crate::sql::planner::runtime_filter::{contract as sql, coverage as sql_coverage};

pub(crate) fn binding_id(value: sql::BindingId) -> runtime::BindingId {
    runtime::BindingId::new(value.get())
}

pub(crate) fn channel_id(value: sql::ChannelId) -> runtime::ChannelId {
    runtime::ChannelId::new(value.get())
}

pub(crate) fn witness_id(value: sql::CoverageWitnessId) -> runtime::CoverageWitnessId {
    runtime::CoverageWitnessId::new(value.get())
}

pub(crate) fn fragment_id(value: sql::PlanFragmentId) -> runtime::PlanFragmentId {
    runtime::PlanFragmentId::new(value.get())
}

pub(crate) fn coverage(value: &sql_coverage::Coverage) -> runtime_coverage::Coverage {
    match value {
        sql_coverage::Coverage::Leaf(witness) => {
            runtime_coverage::Coverage::Leaf(witness_id(*witness))
        }
        sql_coverage::Coverage::AllOf(children) => {
            runtime_coverage::Coverage::AllOf(children.iter().map(coverage).collect())
        }
        sql_coverage::Coverage::AnyOf(children) => {
            runtime_coverage::Coverage::AnyOf(children.iter().map(coverage).collect())
        }
    }
}

pub(crate) fn logical_domain(
    value: &sql::RuntimeFilterLogicalDomain,
) -> runtime::RuntimeFilterLogicalDomain {
    match value {
        sql::RuntimeFilterLogicalDomain::Membership {
            value_type,
            null_semantics,
        } => runtime::RuntimeFilterLogicalDomain::Membership {
            value_type: value_type.clone(),
            null_semantics: match null_semantics {
                sql::NullSemantics::NeverMatches => runtime::NullSemantics::NeverMatches,
                sql::NullSemantics::NullSafeEqual => runtime::NullSemantics::NullSafeEqual,
            },
        },
        sql::RuntimeFilterLogicalDomain::OrderedBound(contract) => {
            runtime::RuntimeFilterLogicalDomain::OrderedBound(runtime::OrderContract {
                keys: contract
                    .keys
                    .iter()
                    .map(|key| runtime::OrderKeyContract {
                        data_type: key.data_type.clone(),
                        direction: match key.direction {
                            sql::SortDirection::Ascending => runtime::SortDirection::Ascending,
                            sql::SortDirection::Descending => runtime::SortDirection::Descending,
                        },
                        null_order: match key.null_order {
                            sql::NullOrder::First => runtime::NullOrder::First,
                            sql::NullOrder::Last => runtime::NullOrder::Last,
                        },
                    })
                    .collect(),
                inclusive: contract.inclusive,
                comparator_digest: runtime::ComparatorDigest::new(contract.comparator_digest.get()),
            })
        }
    }
}

pub(crate) fn lifecycle(value: sql::RuntimeFilterLifecycle) -> runtime::RuntimeFilterLifecycle {
    match value {
        sql::RuntimeFilterLifecycle::CompleteOnce => runtime::RuntimeFilterLifecycle::CompleteOnce,
        sql::RuntimeFilterLifecycle::MonotonicUpdates => {
            runtime::RuntimeFilterLifecycle::MonotonicUpdates
        }
    }
}

pub(crate) fn reduction(value: sql::ReductionRequirement) -> runtime::ReductionRequirement {
    match value {
        sql::ReductionRequirement::SetUnion => runtime::ReductionRequirement::SetUnion,
        sql::ReductionRequirement::TightenOrderedBound => {
            runtime::ReductionRequirement::TightenOrderedBound
        }
        sql::ReductionRequirement::MergeTopKSummary(requirement) => {
            runtime::ReductionRequirement::MergeTopKSummary(
                runtime::TopKSummaryRequirement::try_new(requirement.k().get())
                    .expect("SQL TopK requirement is nonzero"),
            )
        }
    }
}

pub(crate) fn contribution_kinds(
    values: &BTreeSet<sql::ContributionKind>,
) -> BTreeSet<runtime::ContributionKind> {
    values
        .iter()
        .map(|value| match value {
            sql::ContributionKind::ValueDomainDelta => runtime::ContributionKind::ValueDomainDelta,
            sql::ContributionKind::FinalDomainShard => runtime::ContributionKind::FinalDomainShard,
            sql::ContributionKind::OrderedBoundUpdate => {
                runtime::ContributionKind::OrderedBoundUpdate
            }
            sql::ContributionKind::TopKSummary => runtime::ContributionKind::TopKSummary,
            sql::ContributionKind::ProducerClosed => runtime::ContributionKind::ProducerClosed,
        })
        .collect()
}

pub(crate) fn completion(value: sql::CompletionRequirement) -> runtime::CompletionRequirement {
    match value {
        sql::CompletionRequirement::ProducerClosed => {
            runtime::CompletionRequirement::ProducerClosed
        }
        sql::CompletionRequirement::FencedFinalDomain(kind) => {
            runtime::CompletionRequirement::FencedFinalDomain(match kind {
                sql::CompletionFenceKind::CommittedDomainFrozen => {
                    runtime::CompletionFenceKind::CommittedDomainFrozen
                }
            })
        }
    }
}

pub(crate) fn capabilities(
    values: &BTreeSet<sql::ArtifactCapability>,
) -> BTreeSet<runtime::ArtifactCapability> {
    values
        .iter()
        .map(|value| match value {
            sql::ArtifactCapability::Membership => runtime::ArtifactCapability::Membership,
            sql::ArtifactCapability::OrderedRange => runtime::ArtifactCapability::OrderedRange,
            sql::ArtifactCapability::EmptyDomain => runtime::ArtifactCapability::EmptyDomain,
        })
        .collect()
}

pub(crate) fn activation(value: sql::ConsumerActivation) -> runtime::ConsumerActivation {
    match value {
        sql::ConsumerActivation::BlockingSnapshot => runtime::ConsumerActivation::BlockingSnapshot,
        sql::ConsumerActivation::NonBlockingLive { late_apply } => {
            runtime::ConsumerActivation::NonBlockingLive {
                late_apply: match late_apply {
                    sql::LateApplyGranularity::Row => runtime::LateApplyGranularity::Row,
                    sql::LateApplyGranularity::Batch => runtime::LateApplyGranularity::Batch,
                    sql::LateApplyGranularity::RowGroup => runtime::LateApplyGranularity::RowGroup,
                    sql::LateApplyGranularity::Split => runtime::LateApplyGranularity::Split,
                    sql::LateApplyGranularity::File => runtime::LateApplyGranularity::File,
                },
            }
        }
    }
}

pub(crate) fn policy(
    value: sql::RuntimeFilterPolicyRequirement,
) -> runtime::RuntimeFilterPolicyRequirement {
    runtime::RuntimeFilterPolicyRequirement {
        max_contribution_bytes: value.max_contribution_bytes,
        max_artifact_bytes: value.max_artifact_bytes,
        deadline_ms: value.deadline_ms,
        max_retries: value.max_retries,
    }
}

pub(crate) fn producer_witness(
    values: &BTreeMap<sql::BindingId, sql::CoverageWitnessId>,
) -> BTreeMap<runtime::BindingId, runtime::CoverageWitnessId> {
    values
        .iter()
        .map(|(binding, witness)| (binding_id(*binding), witness_id(*witness)))
        .collect()
}
