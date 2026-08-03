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

//! Backend-owned native runtime-filter install decoder.
//!
//! Core carries an opaque contribution DTO. This module is the only native
//! boundary that interprets its lifecycle/install/routing semantics and builds
//! the participant-local install consumed by the Backend service.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::time::Duration;

use arrow::datatypes::DataType;
use novarocks::protocol::{FieldPath, ProtocolError, ProtocolErrorKind, ProtocolFamily};
use novarocks::query_execution::lifecycle::{
    QueryExecutionId, QueryLifecycleError, QueryLifecycleErrorCode, RuntimeFilterContribution,
};
use novarocks::runtime::endpoint::RuntimeEndpoint;
use novarocks::runtime_filter_transition::materializer::bloom::BloomHashContract;
use novarocks::runtime_filter_transition::model::contract::{
    ArtifactCapability, BindingId, ChannelId, ComparatorDigest, CompletionFenceKind,
    CompletionRequirement, ConsumerActivation, ContributionKind, CoverageWitnessId,
    LateApplyGranularity, NullOrder, NullSemantics, OrderContract, OrderKeyContract,
    ReductionRequirement, RuntimeFilterLifecycle, RuntimeFilterLogicalDomain,
    RuntimeFilterPolicyRequirement, SortDirection, TopKSummaryRequirement,
};
use novarocks::runtime_filter_transition::model::coverage::Coverage;
use novarocks::runtime_filter_transition::port::artifact::{
    ArtifactKind, ArtifactMembershipSchema, ConsumerArtifactProfile, ConsumerProfileId,
    HashContractDigest,
};
use novarocks::runtime_filter_transition::port::identity::{
    DeploymentEpoch, RouteEdgeId, RuntimeFilterParticipantId,
};
use novarocks::runtime_filter_transition::port::install::{
    ConsumerDeployment, MaterializationPolicy, OutboundMaterializationGroup,
    OutboundMaterializationOwner, ProducerDeployment, RuntimeFilterChannelDeployment,
    RuntimeFilterCoreBudget, RuntimeFilterInstallView, RuntimeFilterParticipantInstall,
};
use novarocks::runtime_filter_transition::port::ordered_bound::{
    OrderContractDigest, RuntimeOrderContract, RuntimeOrderKey,
};
use novarocks::runtime_filter_transition::port::producer::{
    InstallContractError, InstallContractErrorKind,
};
use novarocks::runtime_filter_transition::port::routing::{
    RuntimeFilterChannelRoutingView, RuntimeFilterRouteEndpointView, RuntimeFilterRoutePeer,
    RuntimeFilterRouteRole, RuntimeFilterRoutingEdgeView, RuntimeFilterRoutingShard,
    canonical_route_allowed_kinds,
};
use novarocks::runtime_filter_transition::port::topk_summary::RuntimeTopKSummaryContract;
use novarocks::runtime_filter_transition::port::transport::RuntimeFilterEnvelopeKind;
use novarocks::runtime_filter_transition::port::value_domain::MembershipValues;
use novarocks_protocol::{common, filter, plan};
use novarocks_types::UniqueId;
use prost::Message;
use sha2::Digest;

use crate::native::type_decode::decode_type;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeFilterQueryLifecycleOptions {
    pub(crate) delivery_expire: Duration,
    pub(crate) query_expire: Duration,
    pub(crate) transport_retry_interval: Duration,
    pub(crate) transport_max_attempts: u32,
    pub(crate) transport_deadline: Duration,
    pub(crate) transport_max_pending_entries: usize,
    pub(crate) transport_max_pending_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DecodedRuntimeFilterParticipantInstall {
    pub(crate) query_id: UniqueId,
    pub(crate) lifecycle: RuntimeFilterQueryLifecycleOptions,
    pub(crate) install: RuntimeFilterParticipantInstall,
}

#[derive(Clone, Debug)]
pub(crate) struct DecodedRuntimeFilterContribution {
    pub(crate) lifecycle: RuntimeFilterQueryLifecycleOptions,
    pub(crate) install: RuntimeFilterParticipantInstall,
}

type CodecResult<T> = Result<T, ProtocolError>;

const CONTRIBUTION_DIGEST_DOMAIN: &[u8] =
    b"novarocks.query-lifecycle.runtime-filter-contribution.v1\0";

pub(crate) fn decode_runtime_filter_contribution(
    execution_id: QueryExecutionId,
    contribution: &RuntimeFilterContribution,
) -> Result<DecodedRuntimeFilterContribution, QueryLifecycleError> {
    let wire = contribution.wire();
    let request = filter::InstallRuntimeFilterDeploymentRequest {
        query_id: Some(common::UniqueId {
            hi: execution_id.query_id().high(),
            lo: execution_id.query_id().low(),
        }),
        deployment_epoch: execution_id.attempt_id().get(),
        participant_id: wire.participant_id,
        lifecycle: wire.lifecycle.clone(),
        install: wire.install.clone(),
    };
    let mut hasher = sha2::Sha256::new();
    hasher.update(CONTRIBUTION_DIGEST_DOMAIN);
    hasher.update(request.encode_to_vec());
    if hasher.finalize().as_slice() != contribution.digest() {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::InvalidManifest,
            "runtime filter contribution digest does not match install DTO",
        ));
    }
    let decoded = decode_participant_install(&request).map_err(|error| {
        QueryLifecycleError::new(QueryLifecycleErrorCode::InvalidManifest, error.to_string())
    })?;
    if decoded.query_id
        != UniqueId::new(
            execution_id.query_id().high(),
            execution_id.query_id().low(),
        )
    {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::InvalidManifest,
            "runtime filter install query id does not match execution attempt",
        ));
    }
    if decoded.install.epoch().get() != execution_id.attempt_id().get() {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::InvalidManifest,
            "runtime filter install epoch does not match query execution attempt",
        ));
    }
    if decoded.install.local_participant_id().get() != contribution.participant_id() {
        return Err(QueryLifecycleError::new(
            QueryLifecycleErrorCode::InvalidManifest,
            "runtime filter install participant does not match manifest contribution",
        ));
    }
    Ok(DecodedRuntimeFilterContribution {
        lifecycle: decoded.lifecycle,
        install: decoded.install,
    })
}

fn error(path: FieldPath, kind: ProtocolErrorKind, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(ProtocolFamily::Native, path, kind, detail)
}

fn contract_missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    error(path, ProtocolErrorKind::MissingField, detail)
}

fn contract_invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    error(path, ProtocolErrorKind::InvalidValue, detail)
}

fn contract_inconsistent(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    error(path, ProtocolErrorKind::InconsistentFields, detail)
}

fn contract_digest32(binding_id: u32, field: &str, bytes: &[u8]) -> Result<[u8; 32], String> {
    bytes.try_into().map_err(|_| format!(
        "native runtime-filter binding_id={binding_id} {field} must be exactly 32 bytes, got {}",
        bytes.len()
    ))
}

enum DecodedContract {
    Membership {
        canonical_schema: Vec<u8>,
    },
    Ordered {
        keys: Vec<RuntimeOrderKey>,
        comparator_digest: [u8; 32],
    },
}

enum DecodedReduction {
    SetUnion,
    TightenOrderedBound,
    MergeTopKSummary { k: NonZeroU32 },
}

fn decode_runtime_filter_logical_domain_and_reduction(
    wire_type: Option<&common::TypeDesc>,
    wire_contract: Option<&plan::RuntimeFilterContract>,
    wire_reduction: Option<&plan::RuntimeFilterReductionContract>,
    path: FieldPath,
) -> CodecResult<(RuntimeFilterLogicalDomain, ReductionRequirement)> {
    let type_path = path.clone().field("value_type");
    let wire_type = wire_type.ok_or_else(|| {
        contract_missing(
            type_path.clone(),
            "runtime filter deployment logical domain is contract_missing value type",
        )
    })?;
    let value_type =
        decode_type(wire_type).map_err(|detail| contract_invalid(type_path, detail))?;
    let contract = decode_contract(
        0,
        &value_type,
        wire_contract,
        path.clone().field("contract"),
    )?;
    let reduction = decode_reduction(0, &contract, wire_reduction, path.field("reduction"))?;
    let domain = match &contract {
        DecodedContract::Membership { canonical_schema } => {
            let schema = ArtifactMembershipSchema::view(canonical_schema).map_err(|reason| {
                contract_invalid(
                    FieldPath::root("runtime_filter_install")
                        .field("logical_domain")
                        .field("contract"),
                    format!("contract_invalid membership schema: {reason:?}"),
                )
            })?;
            RuntimeFilterLogicalDomain::Membership {
                value_type,
                null_semantics: schema.null_semantics(),
            }
        }
        DecodedContract::Ordered {
            keys,
            comparator_digest,
        } => RuntimeFilterLogicalDomain::OrderedBound(OrderContract {
            keys: keys
                .iter()
                .map(|key| OrderKeyContract {
                    data_type: key.data_type().clone(),
                    direction: key.direction(),
                    null_order: key.null_order(),
                })
                .collect(),
            inclusive: true,
            comparator_digest: ComparatorDigest::new(*comparator_digest),
        }),
    };
    let reduction = match reduction {
        DecodedReduction::SetUnion => ReductionRequirement::SetUnion,
        DecodedReduction::TightenOrderedBound => ReductionRequirement::TightenOrderedBound,
        DecodedReduction::MergeTopKSummary { k } => ReductionRequirement::MergeTopKSummary(
            TopKSummaryRequirement::try_new(k.get()).expect("decoded TopK K is nonzero"),
        ),
    };
    Ok((domain, reduction))
}

fn decode_runtime_filter_contribution_kind(
    raw: i32,
    path: FieldPath,
) -> CodecResult<ContributionKind> {
    match plan::RuntimeFilterContributionKind::try_from(raw) {
        Ok(plan::RuntimeFilterContributionKind::ValueDomainDelta) => {
            Ok(ContributionKind::ValueDomainDelta)
        }
        Ok(plan::RuntimeFilterContributionKind::FinalDomainShard) => {
            Ok(ContributionKind::FinalDomainShard)
        }
        Ok(plan::RuntimeFilterContributionKind::OrderedBoundUpdate) => {
            Ok(ContributionKind::OrderedBoundUpdate)
        }
        Ok(plan::RuntimeFilterContributionKind::TopkSummary) => Ok(ContributionKind::TopKSummary),
        Ok(plan::RuntimeFilterContributionKind::ProducerClosed) => {
            Ok(ContributionKind::ProducerClosed)
        }
        Ok(plan::RuntimeFilterContributionKind::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("contract_invalid runtime filter contribution kind={raw}"),
        )),
    }
}

fn decode_runtime_filter_completion(
    raw: i32,
    path: FieldPath,
) -> CodecResult<CompletionRequirement> {
    match plan::RuntimeFilterCompletionRequirement::try_from(raw) {
        Ok(plan::RuntimeFilterCompletionRequirement::ProducerClosed) => {
            Ok(CompletionRequirement::ProducerClosed)
        }
        Ok(plan::RuntimeFilterCompletionRequirement::FencedCommittedDomainFrozen) => Ok(
            CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen),
        ),
        Ok(plan::RuntimeFilterCompletionRequirement::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("contract_invalid runtime filter completion requirement={raw}"),
        )),
    }
}

fn decode_runtime_filter_capability(raw: i32, path: FieldPath) -> CodecResult<ArtifactCapability> {
    match plan::RuntimeFilterArtifactCapability::try_from(raw) {
        Ok(plan::RuntimeFilterArtifactCapability::Membership) => Ok(ArtifactCapability::Membership),
        Ok(plan::RuntimeFilterArtifactCapability::OrderedRange) => {
            Ok(ArtifactCapability::OrderedRange)
        }
        Ok(plan::RuntimeFilterArtifactCapability::EmptyDomain) => {
            Ok(ArtifactCapability::EmptyDomain)
        }
        Ok(plan::RuntimeFilterArtifactCapability::Unspecified) | Err(_) => Err(error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("contract_invalid runtime filter artifact capability={raw}"),
        )),
    }
}

fn decode_runtime_filter_activation(
    wire: Option<&plan::RuntimeFilterConsumerActivation>,
    path: FieldPath,
) -> CodecResult<ConsumerActivation> {
    let wire = wire.ok_or_else(|| {
        contract_missing(
            path.clone(),
            "contract_missing runtime filter consumer activation",
        )
    })?;
    match wire.kind.as_ref().ok_or_else(|| {
        contract_missing(
            path.clone().field("kind"),
            "contract_missing runtime filter consumer activation kind",
        )
    })? {
        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(true) => {
            Ok(ConsumerActivation::BlockingSnapshot)
        }
        plan::runtime_filter_consumer_activation::Kind::BlockingSnapshot(false) => {
            Err(contract_invalid(
                path.field("kind").field("blocking_snapshot"),
                "runtime filter blocking activation marker must be true",
            ))
        }
        plan::runtime_filter_consumer_activation::Kind::NonBlockingLive(raw) => {
            let late_apply = match plan::RuntimeFilterLateApplyGranularity::try_from(*raw) {
                Ok(plan::RuntimeFilterLateApplyGranularity::Row) => LateApplyGranularity::Row,
                Ok(plan::RuntimeFilterLateApplyGranularity::Batch) => LateApplyGranularity::Batch,
                Ok(plan::RuntimeFilterLateApplyGranularity::RowGroup) => {
                    LateApplyGranularity::RowGroup
                }
                Ok(plan::RuntimeFilterLateApplyGranularity::Split) => LateApplyGranularity::Split,
                Ok(plan::RuntimeFilterLateApplyGranularity::File) => LateApplyGranularity::File,
                Ok(plan::RuntimeFilterLateApplyGranularity::Unspecified) | Err(_) => {
                    return Err(error(
                        path.field("kind").field("non_blocking_live"),
                        ProtocolErrorKind::InvalidEnum,
                        format!("contract_invalid runtime filter late-apply granularity={raw}"),
                    ));
                }
            };
            Ok(ConsumerActivation::NonBlockingLive { late_apply })
        }
    }
}

fn decode_contract(
    binding_id: u32,
    expression_type: &arrow::datatypes::DataType,
    wire: Option<&plan::RuntimeFilterContract>,
    path: FieldPath,
) -> CodecResult<DecodedContract> {
    let wire = wire.ok_or_else(|| {
        contract_missing(
            path.clone(),
            format!("native runtime-filter binding_id={binding_id} contract_missing contract"),
        )
    })?;
    let kind = wire.kind.as_ref().ok_or_else(|| {
        contract_missing(
            path.clone().field("kind"),
            format!("native runtime-filter binding_id={binding_id} contract_missing contract kind"),
        )
    })?;
    match kind {
        plan::runtime_filter_contract::Kind::Membership(membership) => {
            let path = path.field("membership");
            if membership.canonical_schema.is_empty() {
                return Err(contract_invalid(
                    path.clone().field("canonical_schema"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema is empty"
                    ),
                ));
            }
            let view = ArtifactMembershipSchema::view(&membership.canonical_schema).map_err(|reason| contract_invalid(
                path.clone().field("canonical_schema"),
                format!("native runtime-filter binding_id={binding_id} membership schema is noncanonical: {reason:?}"),
            ))?;
            let digest = contract_digest32(
                binding_id,
                "membership schema_digest",
                &membership.schema_digest,
            )
            .map_err(|detail| contract_invalid(path.clone().field("schema_digest"), detail))?;
            if view.digest().bytes() != digest {
                return Err(contract_inconsistent(
                    path.clone().field("schema_digest"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema digest mismatch"
                    ),
                ));
            }
            let expected = ArtifactMembershipSchema::new(expression_type, view.null_semantics()).map_err(|reason| contract_invalid(
                path.clone().field("canonical_schema"),
                format!("native runtime-filter binding_id={binding_id} expression type cannot form membership schema: {reason:?}"),
            ))?;
            if expected.canonical_bytes() != membership.canonical_schema {
                return Err(contract_inconsistent(
                    path.field("canonical_schema"),
                    format!(
                        "native runtime-filter binding_id={binding_id} membership schema does not match expression type"
                    ),
                ));
            }
            Ok(DecodedContract::Membership {
                canonical_schema: membership.canonical_schema.clone(),
            })
        }
        plan::runtime_filter_contract::Kind::Ordered(ordered) => {
            let path = path.field("ordered");
            if ordered.keys.len() != 1 {
                return Err(contract_invalid(
                    path.clone().field("keys"),
                    format!(
                        "native runtime-filter binding_id={binding_id} ordered contract must contain exactly one key, got {}",
                        ordered.keys.len()
                    ),
                ));
            }
            let mut keys = Vec::with_capacity(ordered.keys.len());
            for (index, key) in ordered.keys.iter().enumerate() {
                let key_path = path.clone().field("keys").index(index);
                let wire_type = key.r#type.as_ref().ok_or_else(|| {
                    contract_missing(
                        key_path.clone().field("type"),
                        format!(
                            "native runtime-filter binding_id={binding_id} ordered key type contract_missing"
                        ),
                    )
                })?;
                let data_type = decode_type(wire_type)
                    .map_err(|detail| contract_invalid(key_path.clone().field("type"), detail))?;
                let direction = match plan::RuntimeFilterSortDirection::try_from(key.direction) {
                    Ok(plan::RuntimeFilterSortDirection::Ascending) => SortDirection::Ascending,
                    Ok(plan::RuntimeFilterSortDirection::Descending) => SortDirection::Descending,
                    Ok(plan::RuntimeFilterSortDirection::Unspecified) | Err(_) => {
                        return Err(error(
                            key_path.clone().field("direction"),
                            ProtocolErrorKind::InvalidEnum,
                            format!(
                                "native runtime-filter binding_id={binding_id} contract_invalid sort direction={}",
                                key.direction
                            ),
                        ));
                    }
                };
                let null_order = match plan::RuntimeFilterNullOrder::try_from(key.null_order) {
                    Ok(plan::RuntimeFilterNullOrder::First) => NullOrder::First,
                    Ok(plan::RuntimeFilterNullOrder::Last) => NullOrder::Last,
                    Ok(plan::RuntimeFilterNullOrder::Unspecified) | Err(_) => {
                        return Err(error(
                            key_path.field("null_order"),
                            ProtocolErrorKind::InvalidEnum,
                            format!(
                                "native runtime-filter binding_id={binding_id} contract_invalid null order={}",
                                key.null_order
                            ),
                        ));
                    }
                };
                keys.push(RuntimeOrderKey::new(data_type, direction, null_order));
            }
            if keys[0].data_type() != expression_type {
                return Err(contract_inconsistent(
                    path.clone().field("keys").index(0).field("type"),
                    format!(
                        "native runtime-filter binding_id={binding_id} ordered key type {:?} does not match expression type {:?}",
                        keys[0].data_type(),
                        expression_type
                    ),
                ));
            }
            let comparator =
                contract_digest32(binding_id, "comparator_digest", &ordered.comparator_digest)
                    .map_err(|detail| {
                        contract_invalid(path.clone().field("comparator_digest"), detail)
                    })?;
            let order_digest = contract_digest32(
                binding_id,
                "order_contract_digest",
                &ordered.order_contract_digest,
            )
            .map_err(|detail| {
                contract_invalid(path.clone().field("order_contract_digest"), detail)
            })?;
            let order = OrderContract {
                keys: keys
                    .iter()
                    .map(|key| OrderKeyContract {
                        data_type: key.data_type().clone(),
                        direction: key.direction(),
                        null_order: key.null_order(),
                    })
                    .collect(),
                inclusive: true,
                comparator_digest: ComparatorDigest::new(comparator),
            };
            let canonical = RuntimeOrderContract::try_from_plan(&order).map_err(|reason| contract_invalid(path.clone(), format!("native runtime-filter binding_id={binding_id} ordered contract is noncanonical: {reason:?}")))?;
            if canonical.digest().bytes() != order_digest {
                return Err(contract_inconsistent(
                    path.field("order_contract_digest"),
                    format!(
                        "native runtime-filter binding_id={binding_id} order contract digest mismatch"
                    ),
                ));
            }
            Ok(DecodedContract::Ordered {
                keys,
                comparator_digest: comparator,
            })
        }
    }
}

fn decode_reduction(
    binding_id: u32,
    contract: &DecodedContract,
    wire: Option<&plan::RuntimeFilterReductionContract>,
    path: FieldPath,
) -> CodecResult<DecodedReduction> {
    let wire = wire.ok_or_else(|| {
        contract_missing(
            path.clone(),
            format!(
                "native runtime-filter binding_id={binding_id} contract_missing reduction contract"
            ),
        )
    })?;
    let kind = wire.kind.as_ref().ok_or_else(|| {
        contract_missing(
            path.clone().field("kind"),
            format!(
                "native runtime-filter binding_id={binding_id} contract_missing reduction kind"
            ),
        )
    })?;
    match kind {
        plan::runtime_filter_reduction_contract::Kind::SetUnion(true) => {
            Ok(DecodedReduction::SetUnion)
        }
        plan::runtime_filter_reduction_contract::Kind::TightenOrderedBound(true) => {
            Ok(DecodedReduction::TightenOrderedBound)
        }
        plan::runtime_filter_reduction_contract::Kind::SetUnion(false)
        | plan::runtime_filter_reduction_contract::Kind::TightenOrderedBound(false) => {
            Err(contract_invalid(
                path.field("kind"),
                format!(
                    "native runtime-filter binding_id={binding_id} reduction marker must be true"
                ),
            ))
        }
        plan::runtime_filter_reduction_contract::Kind::MergeTopkSummary(topk) => {
            let topk_path = path.field("kind").field("merge_topk_summary");
            let k = NonZeroU32::new(topk.k).ok_or_else(|| {
                contract_invalid(
                    topk_path.clone().field("k"),
                    format!("native runtime-filter binding_id={binding_id} TopK K must be nonzero"),
                )
            })?;
            let digest =
                contract_digest32(binding_id, "TopK contract_digest", &topk.contract_digest)
                    .map_err(|detail| {
                        contract_invalid(topk_path.clone().field("contract_digest"), detail)
                    })?;
            let DecodedContract::Ordered {
                keys,
                comparator_digest,
            } = contract
            else {
                return Err(contract_inconsistent(
                    topk_path.clone(),
                    format!(
                        "native runtime-filter binding_id={binding_id} TopK reduction requires ordered contract"
                    ),
                ));
            };
            let order = OrderContract {
                keys: keys
                    .iter()
                    .map(|key| OrderKeyContract {
                        data_type: key.data_type().clone(),
                        direction: key.direction(),
                        null_order: key.null_order(),
                    })
                    .collect(),
                inclusive: true,
                comparator_digest: ComparatorDigest::new(*comparator_digest),
            };
            let expected = RuntimeTopKSummaryContract::try_from_plan(&order, TopKSummaryRequirement::try_new(k.get()).expect("nonzero"))
                .map_err(|reason| contract_invalid(topk_path.clone(), format!("native runtime-filter binding_id={binding_id} TopK contract is noncanonical: {reason:?}")))?;
            if expected.digest().bytes() != digest {
                return Err(contract_inconsistent(
                    topk_path.field("contract_digest"),
                    format!(
                        "native runtime-filter binding_id={binding_id} TopK contract digest mismatch"
                    ),
                ));
            }
            Ok(DecodedReduction::MergeTopKSummary { k })
        }
    }
}

fn codec_error(
    path: FieldPath,
    kind: ProtocolErrorKind,
    detail: impl Into<String>,
) -> ProtocolError {
    ProtocolError::new(ProtocolFamily::Native, path, kind, detail)
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    codec_error(path, ProtocolErrorKind::InvalidValue, detail)
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    codec_error(path, ProtocolErrorKind::MissingField, detail)
}

fn duplicate(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    codec_error(path, ProtocolErrorKind::DuplicateField, detail)
}

fn inconsistent(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    codec_error(path, ProtocolErrorKind::InconsistentFields, detail)
}

fn reject_zero(raw: u64, path: FieldPath, identity: &'static str) -> CodecResult<()> {
    if raw == 0 {
        Err(invalid(path, format!("{identity} must be nonzero")))
    } else {
        Ok(())
    }
}

fn decode_unique_id(value: Option<&common::UniqueId>, path: FieldPath) -> CodecResult<UniqueId> {
    let value = value.ok_or_else(|| missing(path.clone(), "unique id is required"))?;
    let decoded = UniqueId::new(value.hi, value.lo);
    if decoded.high() == 0 && decoded.low() == 0 {
        return Err(invalid(path, "unique id must be nonzero"));
    }
    Ok(decoded)
}

fn allocatable_usize(raw: u64, path: FieldPath, label: &'static str) -> CodecResult<usize> {
    let value = usize::try_from(raw).map_err(|_| {
        codec_error(
            path.clone(),
            ProtocolErrorKind::OutOfRange,
            format!("{label} does not fit usize"),
        )
    })?;
    if value > isize::MAX as usize {
        return Err(codec_error(
            path,
            ProtocolErrorKind::OutOfRange,
            format!("{label} exceeds the maximum allocatable size"),
        ));
    }
    Ok(value)
}

fn decode_lifecycle_options(
    options: Option<&filter::RuntimeFilterQueryLifecycleOptions>,
) -> CodecResult<RuntimeFilterQueryLifecycleOptions> {
    let root = FieldPath::root("install_runtime_filter_deployment_request").field("lifecycle");
    let options = options.ok_or_else(|| missing(root.clone(), "lifecycle options are required"))?;
    for (raw, field, label) in [
        (
            options.delivery_expire_ms,
            "delivery_expire_ms",
            "delivery expiry",
        ),
        (options.query_expire_ms, "query_expire_ms", "query expiry"),
        (
            options.transport_retry_interval_ms,
            "transport_retry_interval_ms",
            "transport retry interval",
        ),
        (
            options.transport_max_attempts,
            "transport_max_attempts",
            "transport max attempts",
        ),
        (
            options.transport_deadline_ms,
            "transport_deadline_ms",
            "transport deadline",
        ),
        (
            options.transport_max_pending_entries,
            "transport_max_pending_entries",
            "transport max pending entries",
        ),
        (
            options.transport_max_pending_bytes,
            "transport_max_pending_bytes",
            "transport max pending bytes",
        ),
    ] {
        reject_zero(raw, root.clone().field(field), label)?;
    }
    Ok(RuntimeFilterQueryLifecycleOptions {
        delivery_expire: Duration::from_millis(options.delivery_expire_ms),
        query_expire: Duration::from_millis(options.query_expire_ms),
        transport_retry_interval: Duration::from_millis(options.transport_retry_interval_ms),
        transport_max_attempts: u32::try_from(options.transport_max_attempts).map_err(|_| {
            codec_error(
                root.clone().field("transport_max_attempts"),
                ProtocolErrorKind::OutOfRange,
                "transport max attempts does not fit u32",
            )
        })?,
        transport_deadline: Duration::from_millis(options.transport_deadline_ms),
        transport_max_pending_entries: allocatable_usize(
            options.transport_max_pending_entries,
            root.clone().field("transport_max_pending_entries"),
            "transport max pending entries",
        )?,
        transport_max_pending_bytes: allocatable_usize(
            options.transport_max_pending_bytes,
            root.field("transport_max_pending_bytes"),
            "transport max pending bytes",
        )?,
    })
}

pub(crate) fn decode_participant_install(
    request: &filter::InstallRuntimeFilterDeploymentRequest,
) -> CodecResult<DecodedRuntimeFilterParticipantInstall> {
    let root = FieldPath::root("install_runtime_filter_deployment_request");
    let query_id = decode_unique_id(request.query_id.as_ref(), root.clone().field("query_id"))?;
    reject_zero(
        request.deployment_epoch,
        root.clone().field("deployment_epoch"),
        "deployment epoch",
    )?;
    let epoch = DeploymentEpoch::new(request.deployment_epoch);
    reject_zero(
        u64::from(request.participant_id),
        root.clone().field("participant_id"),
        "participant id",
    )?;
    let participant = RuntimeFilterParticipantId::new(request.participant_id);
    let lifecycle = decode_lifecycle_options(request.lifecycle.as_ref())?;
    let wire = request.install.as_ref().ok_or_else(|| {
        missing(
            root.clone().field("install"),
            "participant install is required",
        )
    })?;
    let install = decode_install(wire, epoch, participant, root.clone().field("install"))?;
    validate_participant_install(&install)
        .map_err(|error| invalid(root.field("install"), error.to_string()))?;
    Ok(DecodedRuntimeFilterParticipantInstall {
        query_id,
        lifecycle,
        install,
    })
}

fn decode_install(
    wire: &filter::RuntimeFilterParticipantInstall,
    epoch: DeploymentEpoch,
    participant: RuntimeFilterParticipantId,
    path: FieldPath,
) -> CodecResult<RuntimeFilterParticipantInstall> {
    let mut core_channels = BTreeMap::new();
    let mut binding_ids = BTreeSet::new();
    let mut consumer_route_ids = BTreeSet::new();
    for (index, channel) in wire.core_channels.iter().enumerate() {
        let item_path = path.clone().field("core_channels").index(index);
        let decoded = decode_core_channel(channel, item_path.clone())?;
        if core_channels
            .insert(decoded.channel_id(), decoded)
            .is_some()
        {
            return Err(duplicate(
                item_path.field("channel_id"),
                "duplicate core channel id",
            ));
        }
        let decoded = core_channels
            .get(&ChannelId::new(channel.channel_id))
            .expect("inserted core channel");
        for binding in decoded.producers().keys().chain(decoded.consumers().keys()) {
            if !binding_ids.insert(*binding) {
                return Err(duplicate(
                    item_path.clone(),
                    "duplicate producer or consumer binding id across core install",
                ));
            }
        }
        for route in decoded
            .consumers()
            .values()
            .flat_map(|consumer| consumer.route_edge_ids())
        {
            if !consumer_route_ids.insert(*route) {
                return Err(duplicate(
                    item_path.clone(),
                    "duplicate consumer route edge id across core install",
                ));
            }
        }
    }
    let mut routing_channels = BTreeMap::new();
    for (index, channel) in wire.routing_channels.iter().enumerate() {
        let item_path = path.clone().field("routing_channels").index(index);
        let decoded = decode_routing_channel(channel, participant, item_path.clone())?;
        if routing_channels
            .insert(decoded.channel_id(), decoded)
            .is_some()
        {
            return Err(duplicate(
                item_path.field("channel_id"),
                "duplicate routing channel id",
            ));
        }
    }
    let core = RuntimeFilterInstallView::new(epoch, participant, core_channels);
    let routing = RuntimeFilterRoutingShard::new(epoch, participant, routing_channels)
        .map_err(|error| invalid(path, error.to_string()))?;
    Ok(RuntimeFilterParticipantInstall::new(core, routing))
}

fn decode_core_channel(
    wire: &filter::RuntimeFilterChannelDeployment,
    path: FieldPath,
) -> CodecResult<RuntimeFilterChannelDeployment> {
    reject_zero(
        u64::from(wire.channel_id),
        path.clone().field("channel_id"),
        "channel id",
    )?;
    let logical = wire.logical_domain.as_ref().ok_or_else(|| {
        missing(
            path.clone().field("logical_domain"),
            "logical domain is required",
        )
    })?;
    let (domain, reduction) = decode_runtime_filter_logical_domain_and_reduction(
        logical.value_type.as_ref(),
        logical.contract.as_ref(),
        wire.reduction.as_ref(),
        path.clone(),
    )?;
    let lifecycle = match filter::RuntimeFilterLifecycle::try_from(wire.lifecycle) {
        Ok(filter::RuntimeFilterLifecycle::CompleteOnce) => RuntimeFilterLifecycle::CompleteOnce,
        Ok(filter::RuntimeFilterLifecycle::MonotonicUpdates) => {
            RuntimeFilterLifecycle::MonotonicUpdates
        }
        Ok(filter::RuntimeFilterLifecycle::Unspecified) | Err(_) => {
            return Err(codec_error(
                path.clone().field("lifecycle"),
                ProtocolErrorKind::InvalidEnum,
                format!("invalid runtime filter lifecycle={}", wire.lifecycle),
            ));
        }
    };
    let availability = decode_coverage(
        wire.availability_coverage.as_ref(),
        path.clone().field("availability_coverage"),
    )?;
    let terminal = decode_coverage(
        wire.terminal_coverage.as_ref(),
        path.clone().field("terminal_coverage"),
    )?;
    let mut contributions = BTreeSet::new();
    for (index, raw) in wire.allowed_contribution_kinds.iter().copied().enumerate() {
        let item_path = path
            .clone()
            .field("allowed_contribution_kinds")
            .index(index);
        let contribution = decode_runtime_filter_contribution_kind(raw, item_path.clone())?;
        if !contributions.insert(contribution) {
            return Err(duplicate(item_path, "duplicate contribution kind"));
        }
    }
    if contributions.is_empty() {
        return Err(invalid(
            path.clone().field("allowed_contribution_kinds"),
            "allowed contribution kinds must be nonempty",
        ));
    }
    let completion = decode_runtime_filter_completion(
        wire.completion_requirement,
        path.clone().field("completion_requirement"),
    )?;
    let policy_wire = wire
        .policy
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("policy"), "policy is required"))?;
    let policy = RuntimeFilterPolicyRequirement {
        max_contribution_bytes: policy_wire.max_contribution_bytes,
        max_artifact_bytes: policy_wire.max_artifact_bytes,
        deadline_ms: policy_wire.deadline_ms,
        max_retries: policy_wire.max_retries,
    };
    let budget = wire
        .core_budget
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("core_budget"), "core budget is required"))?;
    reject_zero(
        budget.max_reducer_bytes,
        path.clone().field("core_budget").field("max_reducer_bytes"),
        "core reducer budget",
    )?;
    let materialization = decode_materialization_policy(
        wire.materialization_policy.as_ref(),
        path.clone().field("materialization_policy"),
    )?;
    let mut producers = BTreeMap::new();
    for (index, producer) in wire.producers.iter().enumerate() {
        let item_path = path.clone().field("producers").index(index);
        let (binding, deployment) = decode_producer(producer, item_path.clone())?;
        if producers.insert(binding, deployment).is_some() {
            return Err(duplicate(
                item_path.field("binding_id"),
                "duplicate producer binding",
            ));
        }
    }
    let mut consumers = BTreeMap::new();
    for (index, consumer) in wire.consumers.iter().enumerate() {
        let item_path = path.clone().field("consumers").index(index);
        let (binding, deployment) = decode_consumer(consumer, item_path.clone())?;
        if consumers.insert(binding, deployment).is_some() {
            return Err(duplicate(
                item_path.field("binding_id"),
                "duplicate consumer binding",
            ));
        }
    }
    let mut groups = BTreeMap::new();
    for (index, group) in wire.outbound_materialization_groups.iter().enumerate() {
        let item_path = path
            .clone()
            .field("outbound_materialization_groups")
            .index(index);
        let group = decode_outbound_materialization_group(group, item_path.clone())?;
        if groups.insert(group.profile().id(), group).is_some() {
            return Err(duplicate(
                item_path,
                "duplicate outbound materialization profile",
            ));
        }
    }
    Ok(RuntimeFilterChannelDeployment::new(
        ChannelId::new(wire.channel_id),
        domain,
        lifecycle,
        availability,
        terminal,
        reduction,
        contributions,
        completion,
        policy,
        RuntimeFilterCoreBudget::new(budget.max_reducer_bytes),
        materialization,
        producers,
        consumers,
    )
    .with_outbound_materialization_groups(groups))
}

fn decode_outbound_materialization_group(
    wire: &filter::RuntimeFilterOutboundMaterializationGroup,
    path: FieldPath,
) -> CodecResult<OutboundMaterializationGroup> {
    let owner = match filter::RuntimeFilterOutboundMaterializationOwner::try_from(wire.owner) {
        Ok(filter::RuntimeFilterOutboundMaterializationOwner::DirectSource) => {
            OutboundMaterializationOwner::DirectSource
        }
        Ok(filter::RuntimeFilterOutboundMaterializationOwner::Aggregator) => {
            OutboundMaterializationOwner::Aggregator
        }
        Ok(filter::RuntimeFilterOutboundMaterializationOwner::Unspecified) | Err(_) => {
            return Err(codec_error(
                path.clone().field("owner"),
                ProtocolErrorKind::InvalidEnum,
                format!("invalid outbound materialization owner={}", wire.owner),
            ));
        }
    };
    let profile = decode_artifact_profile(
        wire.artifact_profile.as_ref(),
        path.clone().field("artifact_profile"),
    )?;
    let mut routes = BTreeSet::new();
    for (index, raw) in wire.route_edge_ids.iter().copied().enumerate() {
        let item_path = path.clone().field("route_edge_ids").index(index);
        reject_zero(u64::from(raw), item_path.clone(), "route edge id")?;
        if !routes.insert(RouteEdgeId::new(raw)) {
            return Err(duplicate(
                item_path,
                "duplicate outbound materialization route edge id",
            ));
        }
    }
    if routes.is_empty() {
        return Err(invalid(
            path.field("route_edge_ids"),
            "outbound materialization route edge ids must be nonempty",
        ));
    }
    Ok(OutboundMaterializationGroup::new(owner, profile, routes))
}

fn decode_coverage(
    wire: Option<&filter::RuntimeFilterCoverage>,
    path: FieldPath,
) -> CodecResult<Coverage> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "coverage is required"))?;
    let coverage = match wire
        .kind
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("kind"), "coverage kind is required"))?
    {
        filter::runtime_filter_coverage::Kind::LeafWitnessId(raw) => {
            reject_zero(
                u64::from(*raw),
                path.clone().field("leaf_witness_id"),
                "coverage witness id",
            )?;
            Coverage::Leaf(CoverageWitnessId::new(*raw))
        }
        filter::runtime_filter_coverage::Kind::AllOf(all) => Coverage::AllOf(
            all.children
                .iter()
                .enumerate()
                .map(|(index, child)| {
                    decode_coverage(
                        Some(child),
                        path.clone().field("all_of").field("children").index(index),
                    )
                })
                .collect::<CodecResult<Vec<_>>>()?,
        ),
        filter::runtime_filter_coverage::Kind::AnyOf(any) => Coverage::AnyOf(
            any.children
                .iter()
                .enumerate()
                .map(|(index, child)| {
                    decode_coverage(
                        Some(child),
                        path.clone().field("any_of").field("children").index(index),
                    )
                })
                .collect::<CodecResult<Vec<_>>>()?,
        ),
    };
    coverage
        .validate_shape()
        .map_err(|error| invalid(path, format!("invalid coverage: {error:?}")))?;
    Ok(coverage)
}

fn decode_materialization_policy(
    wire: Option<&filter::RuntimeFilterMaterializationPolicy>,
    path: FieldPath,
) -> CodecResult<MaterializationPolicy> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "materialization policy is required"))?;
    let version = u16::try_from(wire.bloom_algorithm_version).map_err(|_| {
        codec_error(
            path.clone().field("bloom_algorithm_version"),
            ProtocolErrorKind::OutOfRange,
            "bloom algorithm version does not fit u16",
        )
    })?;
    let jobs = allocatable_usize(
        wire.max_concurrent_jobs,
        path.clone().field("max_concurrent_jobs"),
        "max concurrent jobs",
    )?;
    MaterializationPolicy::new(
        wire.bloom_bits_per_key,
        wire.bloom_hash_count,
        wire.bloom_seed,
        version,
        wire.max_total_retained_bytes,
        wire.max_scratch_bytes_per_job,
        jobs,
    )
    .map_err(|error| invalid(path, format!("invalid materialization policy: {error:?}")))
}

fn decode_producer(
    wire: &filter::RuntimeFilterProducerDeployment,
    path: FieldPath,
) -> CodecResult<(BindingId, ProducerDeployment)> {
    reject_zero(
        u64::from(wire.binding_id),
        path.clone().field("binding_id"),
        "producer binding id",
    )?;
    reject_zero(
        u64::from(wire.coverage_witness_id),
        path.clone().field("coverage_witness_id"),
        "coverage witness id",
    )?;
    let instances = decode_unique_id_set(
        &wire.expected_fragment_instances,
        path.field("expected_fragment_instances"),
    )?;
    Ok((
        BindingId::new(wire.binding_id),
        ProducerDeployment::new(CoverageWitnessId::new(wire.coverage_witness_id), instances),
    ))
}

fn decode_consumer(
    wire: &filter::RuntimeFilterConsumerDeployment,
    path: FieldPath,
) -> CodecResult<(BindingId, ConsumerDeployment)> {
    reject_zero(
        u64::from(wire.binding_id),
        path.clone().field("binding_id"),
        "consumer binding id",
    )?;
    let activation = decode_runtime_filter_activation(
        wire.activation.as_ref(),
        path.clone().field("activation"),
    )?;
    let mut capabilities = BTreeSet::new();
    for (index, raw) in wire.capabilities.iter().copied().enumerate() {
        let item_path = path.clone().field("capabilities").index(index);
        let capability = decode_runtime_filter_capability(raw, item_path.clone())?;
        if !capabilities.insert(capability) {
            return Err(duplicate(item_path, "duplicate consumer capability"));
        }
    }
    if capabilities.is_empty() {
        return Err(invalid(
            path.clone().field("capabilities"),
            "consumer capabilities must be nonempty",
        ));
    }
    let profile = decode_artifact_profile(
        wire.artifact_profile.as_ref(),
        path.clone().field("artifact_profile"),
    )?;
    let mut routes = BTreeSet::new();
    for (index, raw) in wire.route_edge_ids.iter().copied().enumerate() {
        let item_path = path.clone().field("route_edge_ids").index(index);
        reject_zero(u64::from(raw), item_path.clone(), "route edge id")?;
        if !routes.insert(RouteEdgeId::new(raw)) {
            return Err(duplicate(item_path, "duplicate consumer route edge id"));
        }
    }
    if routes.is_empty() {
        return Err(invalid(
            path.clone().field("route_edge_ids"),
            "consumer route edge ids must be nonempty",
        ));
    }
    let instances = decode_unique_id_set(
        &wire.expected_fragment_instances,
        path.clone().field("expected_fragment_instances"),
    )?;
    Ok((
        BindingId::new(wire.binding_id),
        ConsumerDeployment::with_profile(activation, capabilities, profile, routes, instances),
    ))
}

fn decode_unique_id_set(
    wire: &[common::UniqueId],
    path: FieldPath,
) -> CodecResult<BTreeSet<UniqueId>> {
    let mut values = BTreeSet::new();
    for (index, item) in wire.iter().enumerate() {
        let item_path = path.clone().index(index);
        let value = decode_unique_id(Some(item), item_path.clone())?;
        if !values.insert(value) {
            return Err(duplicate(item_path, "duplicate unique id"));
        }
    }
    if values.is_empty() {
        return Err(invalid(path, "unique id set must be nonempty"));
    }
    Ok(values)
}

fn decode_artifact_profile(
    wire: Option<&filter::RuntimeFilterConsumerArtifactProfile>,
    path: FieldPath,
) -> CodecResult<ConsumerArtifactProfile> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "artifact profile is required"))?;
    let mut kinds = BTreeSet::new();
    for (index, raw) in wire.accepted_kinds.iter().copied().enumerate() {
        let item_path = path.clone().field("accepted_kinds").index(index);
        let kind = decode_artifact_kind(raw, item_path.clone())?;
        if !kinds.insert(kind) {
            return Err(duplicate(item_path, "duplicate artifact kind"));
        }
    }
    let bloom = wire
        .bloom_hash_contract
        .as_deref()
        .map(|bytes| digest32(bytes, path.clone().field("bloom_hash_contract")))
        .transpose()?
        .map(HashContractDigest::new);
    let order = wire
        .order_contract_digest
        .as_deref()
        .map(|bytes| digest32(bytes, path.clone().field("order_contract_digest")))
        .transpose()?
        .map(OrderContractDigest::from_bytes_for_codec);
    let profile = match order {
        Some(order) => {
            if kinds != BTreeSet::from([ArtifactKind::Range]) || bloom.is_some() {
                return Err(invalid(
                    path.clone(),
                    "ordered artifact profile must contain only Range and no bloom digest",
                ));
            }
            ConsumerArtifactProfile::new_ordered_range(order)
        }
        None => ConsumerArtifactProfile::new(kinds, bloom),
    }
    .map_err(|error| invalid(path.clone(), format!("invalid artifact profile: {error:?}")))?;
    let profile_id = digest32(&wire.profile_id, path.clone().field("profile_id"))?;
    if profile.id().bytes() != profile_id {
        return Err(inconsistent(
            path.field("profile_id"),
            "artifact profile id does not match typed profile",
        ));
    }
    Ok(profile)
}

fn digest32(bytes: &[u8], path: FieldPath) -> CodecResult<[u8; 32]> {
    bytes.try_into().map_err(|_| {
        invalid(
            path,
            format!("digest must be exactly 32 bytes, got {}", bytes.len()),
        )
    })
}

fn decode_artifact_kind(raw: i32, path: FieldPath) -> CodecResult<ArtifactKind> {
    match filter::RuntimeFilterArtifactKind::try_from(raw) {
        Ok(filter::RuntimeFilterArtifactKind::ValueSet) => Ok(ArtifactKind::ValueSet),
        Ok(filter::RuntimeFilterArtifactKind::Bloom) => Ok(ArtifactKind::Bloom),
        Ok(filter::RuntimeFilterArtifactKind::Bitset) => Ok(ArtifactKind::Bitset),
        Ok(filter::RuntimeFilterArtifactKind::Range) => Ok(ArtifactKind::Range),
        Ok(filter::RuntimeFilterArtifactKind::EmptyDomain) => Ok(ArtifactKind::EmptyDomain),
        Ok(filter::RuntimeFilterArtifactKind::Unspecified) | Err(_) => Err(codec_error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("invalid runtime filter artifact kind={raw}"),
        )),
    }
}

fn decode_routing_channel(
    wire: &filter::RuntimeFilterChannelRoutingView,
    local_participant: RuntimeFilterParticipantId,
    path: FieldPath,
) -> CodecResult<RuntimeFilterChannelRoutingView> {
    reject_zero(
        u64::from(wire.channel_id),
        path.clone().field("channel_id"),
        "channel id",
    )?;
    let mut roles = BTreeSet::new();
    for (index, role) in wire.local_roles.iter().enumerate() {
        let item_path = path.clone().field("local_roles").index(index);
        let role = decode_route_role(role, item_path.clone())?;
        if !roles.insert(role) {
            return Err(duplicate(item_path, "duplicate local route role"));
        }
    }
    if roles.is_empty() {
        return Err(invalid(
            path.clone().field("local_roles"),
            "local route roles must be nonempty",
        ));
    }
    let mut producer_instances = BTreeMap::new();
    for (index, route) in wire.producer_instances.iter().enumerate() {
        let item_path = path.clone().field("producer_instances").index(index);
        reject_zero(
            u64::from(route.binding_id),
            item_path.clone().field("binding_id"),
            "producer binding id",
        )?;
        let instance = decode_unique_id(
            route.fragment_instance_id.as_ref(),
            item_path.clone().field("fragment_instance_id"),
        )?;
        reject_zero(
            u64::from(route.participant_id),
            item_path.clone().field("participant_id"),
            "producer participant id",
        )?;
        if producer_instances
            .insert(
                (BindingId::new(route.binding_id), instance),
                RuntimeFilterParticipantId::new(route.participant_id),
            )
            .is_some()
        {
            return Err(duplicate(item_path, "duplicate producer instance route"));
        }
    }
    let inbound = wire
        .inbound_edges
        .iter()
        .enumerate()
        .map(|(index, edge)| {
            decode_routing_edge(
                edge,
                ChannelId::new(wire.channel_id),
                path.clone().field("inbound_edges").index(index),
            )
        })
        .collect::<CodecResult<Vec<_>>>()?;
    let outbound = wire
        .outbound_edges
        .iter()
        .enumerate()
        .map(|(index, edge)| {
            decode_routing_edge(
                edge,
                ChannelId::new(wire.channel_id),
                path.clone().field("outbound_edges").index(index),
            )
        })
        .collect::<CodecResult<Vec<_>>>()?;
    let channel = RuntimeFilterChannelRoutingView::new(
        ChannelId::new(wire.channel_id),
        roles,
        producer_instances,
        inbound,
        outbound,
    )
    .map_err(|error| invalid(path.clone(), error.to_string()))?;
    for edge in channel.inbound_edges() {
        if edge.target().participant_id() != local_participant {
            return Err(inconsistent(
                path.clone().field("inbound_edges"),
                "inbound edge target does not match request participant",
            ));
        }
    }
    for edge in channel.outbound_edges() {
        if edge.source().participant_id() != local_participant {
            return Err(inconsistent(
                path.clone().field("outbound_edges"),
                "outbound edge source does not match request participant",
            ));
        }
    }
    Ok(channel)
}

fn decode_route_role(
    wire: &filter::RuntimeFilterRouteRole,
    path: FieldPath,
) -> CodecResult<RuntimeFilterRouteRole> {
    match wire
        .role
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("role"), "route role is required"))?
    {
        filter::runtime_filter_route_role::Role::ProducerBindingId(raw) => {
            reject_zero(
                u64::from(*raw),
                path.field("producer_binding_id"),
                "producer binding id",
            )?;
            Ok(RuntimeFilterRouteRole::Producer(BindingId::new(*raw)))
        }
        filter::runtime_filter_route_role::Role::Aggregator(true) => {
            Ok(RuntimeFilterRouteRole::Aggregator)
        }
        filter::runtime_filter_route_role::Role::Relay(true) => Ok(RuntimeFilterRouteRole::Relay),
        filter::runtime_filter_route_role::Role::ConsumerBindingId(raw) => {
            reject_zero(
                u64::from(*raw),
                path.field("consumer_binding_id"),
                "consumer binding id",
            )?;
            Ok(RuntimeFilterRouteRole::Consumer(BindingId::new(*raw)))
        }
        filter::runtime_filter_route_role::Role::Aggregator(false) => Err(invalid(
            path.field("aggregator"),
            "aggregator marker must be true",
        )),
        filter::runtime_filter_route_role::Role::Relay(false) => {
            Err(invalid(path.field("relay"), "relay marker must be true"))
        }
    }
}

fn decode_routing_edge(
    wire: &filter::RuntimeFilterRoutingEdgeView,
    channel_id: ChannelId,
    path: FieldPath,
) -> CodecResult<RuntimeFilterRoutingEdgeView> {
    reject_zero(
        u64::from(wire.route_edge_id),
        path.clone().field("route_edge_id"),
        "route edge id",
    )?;
    let source = decode_route_endpoint(wire.source.as_ref(), path.clone().field("source"))?;
    let target = decode_route_endpoint(wire.target.as_ref(), path.clone().field("target"))?;
    let peer = decode_route_peer(wire.peer.as_ref(), path.clone().field("peer"))?;
    let mut allowed = BTreeSet::new();
    for (index, raw) in wire.allowed_kinds.iter().copied().enumerate() {
        let item_path = path.clone().field("allowed_kinds").index(index);
        let kind = decode_envelope_kind(raw, item_path.clone())?;
        if !allowed.insert(kind) {
            return Err(duplicate(item_path, "duplicate allowed envelope kind"));
        }
    }
    RuntimeFilterRoutingEdgeView::new(
        channel_id,
        RouteEdgeId::new(wire.route_edge_id),
        source,
        target,
        peer,
        allowed,
    )
    .map_err(|error| invalid(path, error.to_string()))
}

fn decode_route_endpoint(
    wire: Option<&filter::RuntimeFilterRouteEndpointView>,
    path: FieldPath,
) -> CodecResult<RuntimeFilterRouteEndpointView> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "route endpoint is required"))?;
    reject_zero(
        u64::from(wire.participant_id),
        path.clone().field("participant_id"),
        "route participant id",
    )?;
    Ok(RuntimeFilterRouteEndpointView::new(
        RuntimeFilterParticipantId::new(wire.participant_id),
        decode_route_role(
            wire.role.as_ref().ok_or_else(|| {
                missing(
                    path.clone().field("role"),
                    "route endpoint role is required",
                )
            })?,
            path.field("role"),
        )?,
    ))
}

fn decode_route_peer(
    wire: Option<&filter::RuntimeFilterRoutePeer>,
    path: FieldPath,
) -> CodecResult<RuntimeFilterRoutePeer> {
    let wire = wire.ok_or_else(|| missing(path.clone(), "route peer is required"))?;
    match wire
        .peer
        .as_ref()
        .ok_or_else(|| missing(path.clone().field("peer"), "route peer kind is required"))?
    {
        filter::runtime_filter_route_peer::Peer::Loopback(true) => {
            Ok(RuntimeFilterRoutePeer::Loopback)
        }
        filter::runtime_filter_route_peer::Peer::Loopback(false) => Err(invalid(
            path.field("loopback"),
            "loopback marker must be true",
        )),
        filter::runtime_filter_route_peer::Peer::Remote(remote) => {
            reject_zero(
                u64::from(remote.participant_id),
                path.clone().field("remote").field("participant_id"),
                "remote participant id",
            )?;
            Ok(RuntimeFilterRoutePeer::Remote {
                participant_id: RuntimeFilterParticipantId::new(remote.participant_id),
                endpoint: RuntimeEndpoint::parse(&remote.endpoint)
                    .map_err(|error| invalid(path.field("remote").field("endpoint"), error))?,
            })
        }
    }
}

fn decode_envelope_kind(raw: i32, path: FieldPath) -> CodecResult<RuntimeFilterEnvelopeKind> {
    match filter::RuntimeFilterEnvelopeKind::try_from(raw) {
        Ok(filter::RuntimeFilterEnvelopeKind::Contribution) => {
            Ok(RuntimeFilterEnvelopeKind::Contribution)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Artifact) => Ok(RuntimeFilterEnvelopeKind::Artifact),
        Ok(filter::RuntimeFilterEnvelopeKind::ProducerClosed) => {
            Ok(RuntimeFilterEnvelopeKind::ProducerClosed)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::ProducerUnavailable) => {
            Ok(RuntimeFilterEnvelopeKind::ProducerUnavailable)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Unavailable) => {
            Ok(RuntimeFilterEnvelopeKind::Unavailable)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Ack) => Ok(RuntimeFilterEnvelopeKind::Ack),
        Ok(filter::RuntimeFilterEnvelopeKind::CompletedWithoutArtifact) => {
            Ok(RuntimeFilterEnvelopeKind::CompletedWithoutArtifact)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::DegradedLogical) => {
            Ok(RuntimeFilterEnvelopeKind::DegradedLogical)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::FinalArtifact) => {
            Ok(RuntimeFilterEnvelopeKind::FinalArtifact)
        }
        Ok(filter::RuntimeFilterEnvelopeKind::Unspecified) | Err(_) => Err(codec_error(
            path,
            ProtocolErrorKind::InvalidEnum,
            format!("invalid runtime filter envelope kind={raw}"),
        )),
    }
}

const MAX_ARTIFACT_BYTES: u64 = 1 << 30;
const MAX_DEADLINE_MS: u64 = 86_400_000;
const MAX_RETRIES: u32 = 100;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeFilterPolicyValidationError {
    ZeroMaxContributionBytes,
    ZeroMaxArtifactBytes,
    ZeroDeadlineMs,
    ZeroMaxRetries,
    ContributionBytesExceedArtifactBytes,
    ArtifactBytesExceedLimit,
    DeadlineExceedsLimit,
    RetriesExceedLimit,
}

pub fn validate_runtime_filter_policy(
    policy: RuntimeFilterPolicyRequirement,
) -> Result<(), RuntimeFilterPolicyValidationError> {
    if policy.max_contribution_bytes == 0 {
        return Err(RuntimeFilterPolicyValidationError::ZeroMaxContributionBytes);
    }
    if policy.max_artifact_bytes == 0 {
        return Err(RuntimeFilterPolicyValidationError::ZeroMaxArtifactBytes);
    }
    if policy.deadline_ms == 0 {
        return Err(RuntimeFilterPolicyValidationError::ZeroDeadlineMs);
    }
    if policy.max_retries == 0 {
        return Err(RuntimeFilterPolicyValidationError::ZeroMaxRetries);
    }
    if policy.max_contribution_bytes > policy.max_artifact_bytes {
        return Err(RuntimeFilterPolicyValidationError::ContributionBytesExceedArtifactBytes);
    }
    if policy.max_artifact_bytes > MAX_ARTIFACT_BYTES {
        return Err(RuntimeFilterPolicyValidationError::ArtifactBytesExceedLimit);
    }
    if policy.deadline_ms > MAX_DEADLINE_MS {
        return Err(RuntimeFilterPolicyValidationError::DeadlineExceedsLimit);
    }
    if policy.max_retries > MAX_RETRIES {
        return Err(RuntimeFilterPolicyValidationError::RetriesExceedLimit);
    }
    Ok(())
}

pub fn validate_participant_install(
    install: &RuntimeFilterParticipantInstall,
) -> Result<(), InstallContractError> {
    validate_install_identity(install)?;
    if install.core_view().is_empty() && install.routing_shard().channels().is_empty() {
        return Ok(());
    }
    validate_view_with_routing(install.core_view(), Some(install.routing_shard()))
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
pub fn validate_install_view_contract_for_test(
    view: &RuntimeFilterInstallView,
) -> Result<(), InstallContractError> {
    validate_view_with_routing(view, None)
}

#[cfg(any(test, feature = "runtime-filter-test-support"))]
pub fn validate_channel_contract_for_test(
    channel: &RuntimeFilterChannelDeployment,
) -> Result<(), InstallContractError> {
    validate_channel(channel, &mut BTreeMap::new(), (true, true))
}

fn validate_install_identity(
    install: &RuntimeFilterParticipantInstall,
) -> Result<(), InstallContractError> {
    if install.core_view().epoch() != install.routing_shard().deployment_epoch() {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "participant install core and routing epochs differ",
        ));
    }
    if install.core_view().local_participant_id() != install.routing_shard().local_participant_id()
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "participant install core and routing participants differ",
        ));
    }
    Ok(())
}

fn validate_view_with_routing<'a>(
    view: &'a RuntimeFilterInstallView,
    routing_shard: Option<&RuntimeFilterRoutingShard>,
) -> Result<(), InstallContractError> {
    if view.epoch().get() == 0 {
        return Err(install_error(
            InstallContractErrorKind::InvalidEpoch,
            "deployment epoch must be non-zero",
        ));
    }

    if let Some(shard) = routing_shard {
        for (channel_id, routing) in shard.channels() {
            validate_route_family_contract(routing)?;
            let requires_core = routing.local_roles().iter().any(|role| {
                matches!(
                    role,
                    RuntimeFilterRouteRole::Producer(_)
                        | RuntimeFilterRouteRole::Aggregator
                        | RuntimeFilterRouteRole::Consumer(_)
                )
            });
            let relay_only =
                routing.local_roles() == &BTreeSet::from([RuntimeFilterRouteRole::Relay]);
            match (requires_core, view.channels().contains_key(channel_id)) {
                (true, false) => {
                    return Err(install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "routing channel {} requires Core authority for its local roles",
                            channel_id.get()
                        ),
                    ));
                }
                (false, true) => {
                    return Err(install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "routing-only channel {} must not carry fake Core authority",
                            channel_id.get()
                        ),
                    ));
                }
                (false, false) if !relay_only => {
                    return Err(install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "routing channel {} has no genuine local role",
                            channel_id.get()
                        ),
                    ));
                }
                _ => {}
            }
        }
    }

    validate_install_identities(view)?;
    let mut profile_encodings = BTreeMap::<ConsumerProfileId, &'a [u8]>::new();
    for channel in view.channels().values() {
        if channel
            .producers()
            .values()
            .any(|producer| producer.expected_fragment_instances().is_empty())
            || channel
                .consumers()
                .values()
                .any(|consumer| consumer.expected_fragment_instances().is_empty())
        {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "producer and consumer expected fragment instances must be non-empty",
            ));
        }
        let role_requirements = match routing_shard {
            Some(shard) => {
                let routing = shard.channel(channel.channel_id()).ok_or_else(|| {
                    install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "core channel {} is missing from routing shard",
                            channel.channel_id().get()
                        ),
                    )
                })?;
                validate_channel_routing_contract(view.local_participant_id(), channel, routing)?
            }
            None => (true, true),
        };
        validate_channel(channel, &mut profile_encodings, role_requirements)?;
    }
    Ok(())
}

fn validate_route_family_contract(
    routing: &RuntimeFilterChannelRoutingView,
) -> Result<(), InstallContractError> {
    for edge in routing
        .inbound_edges()
        .iter()
        .chain(routing.outbound_edges())
    {
        let Some(expected) =
            canonical_route_allowed_kinds(edge.source().role(), edge.target().role())
        else {
            return Err(invalid_route_family(
                edge,
                "endpoint role pair is not canonical",
            ));
        };
        if edge.allowed_kinds() != &expected {
            return Err(invalid_route_family(
                edge,
                "allowed kinds do not exactly match the endpoint route family",
            ));
        }
    }
    Ok(())
}

fn invalid_route_family(edge: &RuntimeFilterRoutingEdgeView, detail: &str) -> InstallContractError {
    install_error(
        InstallContractErrorKind::UnsupportedChannelContract,
        format!(
            "routing edge {} {detail}: source {:?}, target {:?}, allowed {:?}",
            edge.route_edge_id().get(),
            edge.source().role(),
            edge.target().role(),
            edge.allowed_kinds(),
        ),
    )
}

fn validate_install_identities(
    view: &RuntimeFilterInstallView,
) -> Result<(), InstallContractError> {
    let mut channel_ids = BTreeSet::new();
    let mut binding_ids = BTreeSet::new();
    let mut route_ids = BTreeSet::new();
    for (map_channel_id, channel) in view.channels() {
        if *map_channel_id != channel.channel_id() || !channel_ids.insert(channel.channel_id()) {
            return Err(install_error(
                InstallContractErrorKind::DuplicateIdentity,
                "channel map key and channel identity must match and be unique",
            ));
        }
        for binding_id in channel.producers().keys() {
            if !binding_ids.insert(*binding_id) {
                return Err(install_error(
                    InstallContractErrorKind::DuplicateIdentity,
                    "producer binding identities must be unique across the install view",
                ));
            }
        }
        for (binding_id, consumer) in channel.consumers() {
            if !binding_ids.insert(*binding_id) || consumer.route_edge_ids().is_empty() {
                return Err(install_error(
                    InstallContractErrorKind::DuplicateIdentity,
                    "consumer binding identities must be unique and route sets must be nonempty",
                ));
            }
            for route_edge_id in consumer.route_edge_ids() {
                if !route_ids.insert(*route_edge_id) {
                    return Err(install_error(
                        InstallContractErrorKind::DuplicateIdentity,
                        "consumer route identities must be unique across the install view",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_channel_routing_contract(
    local_participant_id: RuntimeFilterParticipantId,
    channel: &RuntimeFilterChannelDeployment,
    routing: &RuntimeFilterChannelRoutingView,
) -> Result<(bool, bool), InstallContractError> {
    let is_local_aggregator = routing
        .local_roles()
        .contains(&RuntimeFilterRouteRole::Aggregator);
    let local_producer_bindings = routing
        .local_roles()
        .iter()
        .filter_map(|role| match role {
            RuntimeFilterRouteRole::Producer(binding_id) => Some(*binding_id),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let local_consumer_bindings = routing
        .local_roles()
        .iter()
        .filter_map(|role| match role {
            RuntimeFilterRouteRole::Consumer(binding_id) => Some(*binding_id),
            _ => None,
        })
        .collect::<BTreeSet<_>>();

    for (binding_id, producer) in channel.producers() {
        for fragment_instance_id in producer.expected_fragment_instances() {
            let participant_id = routing
                .producer_participant(*binding_id, *fragment_instance_id)
                .ok_or_else(|| {
                    install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        format!(
                            "producer binding {} instance {:?} is missing from routing producer index",
                            binding_id.get(), fragment_instance_id
                        ),
                    )
                })?;
            if !is_local_aggregator && participant_id != local_participant_id {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "non-aggregator producer binding {} instance {:?} maps to remote participant {:?}",
                        binding_id.get(),
                        fragment_instance_id,
                        participant_id
                    ),
                ));
            }
            if participant_id == local_participant_id
                && !routing
                    .local_roles()
                    .contains(&RuntimeFilterRouteRole::Producer(*binding_id))
            {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "local producer binding {} has no matching local Producer role",
                        binding_id.get()
                    ),
                ));
            }
        }
    }

    if is_local_aggregator {
        for ((binding_id, fragment_instance_id), _) in routing.producer_instances() {
            let installed = channel.producers().get(binding_id).is_some_and(|producer| {
                producer
                    .expected_fragment_instances()
                    .contains(fragment_instance_id)
            });
            if !installed {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!(
                        "aggregator core is missing routing-authorized producer binding {} instance {:?}",
                        binding_id.get(),
                        fragment_instance_id
                    ),
                ));
            }
        }
    }

    let expected_producer_instances = routing
        .producer_instances()
        .iter()
        .filter(|(_, participant_id)| {
            is_local_aggregator || **participant_id == local_participant_id
        })
        .fold(
            BTreeMap::<BindingId, BTreeSet<UniqueId>>::new(),
            |mut expected, ((binding_id, fragment_instance_id), _)| {
                expected
                    .entry(*binding_id)
                    .or_default()
                    .insert(*fragment_instance_id);
                expected
            },
        );
    let installed_producer_instances = channel
        .producers()
        .iter()
        .map(|(binding_id, producer)| (*binding_id, producer.expected_fragment_instances().clone()))
        .collect::<BTreeMap<_, _>>();
    if installed_producer_instances != expected_producer_instances {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "Core producer authority does not exactly match local routing producer roles",
        ));
    }
    if !is_local_aggregator
        && channel.producers().keys().copied().collect::<BTreeSet<_>>() != local_producer_bindings
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "Core producer bindings do not exactly match local Producer roles",
        ));
    }

    if is_local_aggregator {
        validate_aggregator_edges(local_participant_id, routing)?;
    }
    if channel.consumers().keys().copied().collect::<BTreeSet<_>>() != local_consumer_bindings {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "Core consumer bindings do not exactly match local Consumer roles",
        ));
    }
    for (binding_id, consumer) in channel.consumers() {
        let expected_routes = routing
            .inbound_edges()
            .iter()
            .filter(|edge| {
                edge.target().participant_id() == local_participant_id
                    && edge.target().role() == RuntimeFilterRouteRole::Consumer(*binding_id)
            })
            .map(|edge| edge.route_edge_id())
            .collect::<BTreeSet<_>>();
        if consumer.route_edge_ids() != &expected_routes || expected_routes.is_empty() {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                format!(
                    "consumer binding {} Core route authority does not exactly match inbound routing edges",
                    binding_id.get()
                ),
            ));
        }
    }
    validate_outbound_materialization_contract(local_participant_id, channel, routing)?;
    Ok((
        is_local_aggregator || !local_producer_bindings.is_empty(),
        !local_consumer_bindings.is_empty(),
    ))
}

fn validate_outbound_materialization_contract(
    local_participant_id: RuntimeFilterParticipantId,
    channel: &RuntimeFilterChannelDeployment,
    routing: &RuntimeFilterChannelRoutingView,
) -> Result<(), InstallContractError> {
    let mut expected = BTreeMap::new();
    for edge in routing
        .outbound_edges()
        .iter()
        .filter(|edge| matches!(edge.target().role(), RuntimeFilterRouteRole::Consumer(_)))
    {
        if edge.source().participant_id() != local_participant_id {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "outbound materialization edge must originate locally",
            ));
        }
        let owner = match edge.source().role() {
            RuntimeFilterRouteRole::Producer(_) => OutboundMaterializationOwner::DirectSource,
            RuntimeFilterRouteRole::Aggregator => OutboundMaterializationOwner::Aggregator,
            _ => {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "outbound materialization edge must originate from a Producer or Aggregator role",
                ));
            }
        };
        if expected.insert(edge.route_edge_id(), owner).is_some() {
            return Err(install_error(
                InstallContractErrorKind::DuplicateIdentity,
                "outbound materialization route identity is duplicated",
            ));
        }
    }

    let mut actual = BTreeMap::new();
    for (profile_id, group) in channel.outbound_materialization_groups() {
        if *profile_id != group.profile().id() || group.route_edge_ids().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::DuplicateIdentity,
                "outbound materialization profile key must match and own a nonempty route set",
            ));
        }
        let owner_role_present = match group.owner() {
            OutboundMaterializationOwner::DirectSource => routing
                .local_roles()
                .iter()
                .any(|role| matches!(role, RuntimeFilterRouteRole::Producer(_))),
            OutboundMaterializationOwner::Aggregator => routing
                .local_roles()
                .contains(&RuntimeFilterRouteRole::Aggregator),
        };
        if !owner_role_present {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "outbound materialization owner has no matching local routing role",
            ));
        }
        for route in group.route_edge_ids() {
            if actual.insert(*route, group.owner()).is_some() {
                return Err(install_error(
                    InstallContractErrorKind::DuplicateIdentity,
                    "outbound materialization route belongs to more than one profile group",
                ));
            }
        }
    }
    if actual != expected {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "outbound materialization groups do not exactly cover local Artifact/Unavailable edges",
        ));
    }
    for (binding_id, consumer) in channel.consumers() {
        for route in consumer.route_edge_ids() {
            let is_loopback_outbound = routing.outbound_edges().iter().any(|edge| {
                edge.route_edge_id() == *route
                    && edge.target().role() == RuntimeFilterRouteRole::Consumer(*binding_id)
            });
            if is_loopback_outbound {
                let Some(group) = channel
                    .outbound_materialization_groups()
                    .values()
                    .find(|group| group.route_edge_ids().contains(route))
                else {
                    return Err(install_error(
                        InstallContractErrorKind::UnsupportedChannelContract,
                        "loopback consumer route is missing materialization authority",
                    ));
                };
                if group.profile().canonical_bytes()
                    != consumer.artifact_profile().canonical_bytes()
                {
                    return Err(install_error(
                        InstallContractErrorKind::ConflictingDeployment,
                        "loopback consumer and materializer profiles differ",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_aggregator_edges(
    local_participant_id: RuntimeFilterParticipantId,
    routing: &RuntimeFilterChannelRoutingView,
) -> Result<(), InstallContractError> {
    let authorized_sources = routing
        .producer_instances()
        .iter()
        .map(|((binding_id, _), participant_id)| (*binding_id, *participant_id))
        .collect::<BTreeSet<_>>();
    for edge in routing.inbound_edges().iter().filter(|edge| {
        matches!(edge.source().role(), RuntimeFilterRouteRole::Producer(_))
            && edge.target().participant_id() == local_participant_id
            && edge.target().role() == RuntimeFilterRouteRole::Aggregator
    }) {
        let RuntimeFilterRouteRole::Producer(binding_id) = edge.source().role() else {
            unreachable!("inbound producer edges were filtered by source role")
        };
        if !authorized_sources.contains(&(binding_id, edge.source().participant_id())) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "aggregator inbound producer edge source has no authorized producer instance",
            ));
        }
    }
    for (binding_id, source_participant_id) in authorized_sources {
        let matching_edges = routing
            .inbound_edges()
            .iter()
            .filter(|edge| {
                edge.source().participant_id() == source_participant_id
                    && edge.source().role() == RuntimeFilterRouteRole::Producer(binding_id)
                    && edge.target().participant_id() == local_participant_id
                    && edge.target().role() == RuntimeFilterRouteRole::Aggregator
            })
            .collect::<Vec<_>>();
        if matching_edges.len() != 1 {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                format!(
                    "aggregator producer binding {} source participant {:?} requires exactly one inbound Producer-to-Aggregator edge",
                    binding_id.get(),
                    source_participant_id
                ),
            ));
        }
    }
    Ok(())
}

fn validate_channel<'a>(
    channel: &'a RuntimeFilterChannelDeployment,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
    role_requirements: (bool, bool),
) -> Result<(), InstallContractError> {
    if matches!(
        channel.logical_domain(),
        RuntimeFilterLogicalDomain::OrderedBound(_)
    ) {
        return validate_ordered_channel(channel, profile_encodings, role_requirements);
    }
    validate_membership_channel(channel, profile_encodings, role_requirements)
}

fn validate_membership_channel<'a>(
    channel: &'a RuntimeFilterChannelDeployment,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
    role_requirements: (bool, bool),
) -> Result<(), InstallContractError> {
    let RuntimeFilterLogicalDomain::Membership {
        value_type,
        null_semantics,
    } = channel.logical_domain()
    else {
        unreachable!("membership validator is called only for membership channels")
    };
    let ordinary = channel.lifecycle() == RuntimeFilterLifecycle::CompleteOnce
        && channel.reduction_requirement() == ReductionRequirement::SetUnion
        && channel.allowed_contribution_kinds()
            == &BTreeSet::from([
                ContributionKind::ValueDomainDelta,
                ContributionKind::ProducerClosed,
            ])
        && channel.completion_requirement() == CompletionRequirement::ProducerClosed;
    let fenced_final = channel.lifecycle() == RuntimeFilterLifecycle::CompleteOnce
        && channel.reduction_requirement() == ReductionRequirement::SetUnion
        && channel.allowed_contribution_kinds()
            == &BTreeSet::from([
                ContributionKind::FinalDomainShard,
                ContributionKind::ProducerClosed,
            ])
        && channel.completion_requirement()
            == CompletionRequirement::FencedFinalDomain(CompletionFenceKind::CommittedDomainFrozen)
        && *null_semantics == NullSemantics::NullSafeEqual
        && channel.availability_coverage().is_all_of_only()
        && channel.terminal_coverage().is_all_of_only();
    if !ordinary && !fenced_final {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "channel does not match the CompleteOnce Membership SetUnion matrix",
        ));
    }
    if MembershipValues::empty_for_data_type(value_type).is_none() {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedMembershipType,
            "membership data type is not supported by the runtime filter port",
        ));
    }
    validate_common_channel(channel, role_requirements)?;

    let schema = ArtifactMembershipSchema::new(value_type, *null_semantics).map_err(|_| {
        install_error(
            InstallContractErrorKind::UnsupportedMembershipType,
            "membership schema has no canonical artifact encoding",
        )
    })?;
    validate_producer_coverage(channel)?;
    if !channel
        .availability_coverage()
        .is_canonically_equivalent_to(channel.terminal_coverage())
    {
        return Err(install_error(
            InstallContractErrorKind::InvalidCoverage,
            "CompleteOnce availability and terminal coverage must be canonically equivalent",
        ));
    }

    let mut unique_profiles = BTreeSet::new();
    for consumer in channel.consumers().values() {
        if ordinary && !consumer.activation().is_blocking_or_batch_live() {
            return Err(install_error(
                InstallContractErrorKind::InvalidConsumerActivation,
                "M1 consumers must use BlockingSnapshot or Batch NonBlockingLive activation",
            ));
        }
        if fenced_final
            && !matches!(
                consumer.activation(),
                ConsumerActivation::NonBlockingLive { .. }
            )
        {
            return Err(install_error(
                InstallContractErrorKind::InvalidConsumerActivation,
                "fenced-final consumers must use NonBlockingLive activation",
            ));
        }
        validate_membership_consumer(
            channel,
            consumer,
            &schema,
            fenced_final,
            &mut unique_profiles,
            profile_encodings,
        )?;
    }
    let mut materialization_profiles = BTreeSet::new();
    for group in channel.outbound_materialization_groups().values() {
        let profile = group.profile();
        if !profile.accepts(ArtifactKind::EmptyDomain) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "Membership materialization profile must accept EmptyDomain",
            ));
        }
        let value_set = profile.accepts(ArtifactKind::ValueSet);
        let bitset = profile.accepts(ArtifactKind::Bitset) && bitset_schema_is_feasible(value_type);
        let bloom = profile.accepts(ArtifactKind::Bloom);
        if !value_set && !bitset && !bloom || profile.accepts(ArtifactKind::Range) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "Membership materialization profile has no feasible membership representation",
            ));
        }
        if matches!(
            channel.logical_domain(),
            RuntimeFilterLogicalDomain::Membership {
                null_semantics: NullSemantics::NullSafeEqual,
                ..
            }
        ) && !value_set
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "NullSafeEqual Membership materialization profile must accept ValueSet",
            ));
        }
        if bloom {
            let expected = BloomHashContract::new(&schema, channel.materialization_policy())
                .map_err(|_| {
                    install_error(
                        InstallContractErrorKind::InvalidPolicy,
                        "materialization Bloom policy is not supported",
                    )
                })?
                .digest();
            if profile.bloom_hash_contract() != Some(expected) {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "Bloom materialization profile does not match channel schema and policy",
                ));
            }
        }
        validate_profile_identity(profile, profile_encodings)?;
        materialization_profiles.insert((profile.id(), profile.canonical_bytes()));
    }
    validate_materialization_concurrency(
        channel,
        !channel.outbound_materialization_groups().is_empty(),
        materialization_profiles.len(),
    )
}

fn validate_membership_consumer<'a>(
    channel: &'a RuntimeFilterChannelDeployment,
    consumer: &'a ConsumerDeployment,
    schema: &ArtifactMembershipSchema,
    fenced_final: bool,
    unique_profiles: &mut BTreeSet<(ConsumerProfileId, &'a [u8])>,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
) -> Result<(), InstallContractError> {
    if consumer.expected_fragment_instances().is_empty() {
        return Err(install_error(
            InstallContractErrorKind::EmptyExpectedInstances,
            "consumer expected fragment instance set must be non-empty",
        ));
    }
    let capabilities = consumer.capabilities();
    let profile = consumer.artifact_profile();
    unique_profiles.insert((profile.id(), profile.canonical_bytes()));
    if !capabilities.contains(&ArtifactCapability::Membership)
        || !capabilities.contains(&ArtifactCapability::EmptyDomain)
    {
        return Err(install_error(
            InstallContractErrorKind::MissingMembershipCapability,
            "M2 Membership consumers must declare Membership and EmptyDomain semantics",
        ));
    }
    if fenced_final
        && (capabilities
            != &BTreeSet::from([
                ArtifactCapability::Membership,
                ArtifactCapability::EmptyDomain,
            ])
            || profile.accepted_kinds()
                != &BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]))
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "fenced-final consumers require exact Membership and EmptyDomain semantics",
        ));
    }
    if !profile.accepts(ArtifactKind::EmptyDomain) {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "M2 Membership profile must accept EmptyDomain",
        ));
    }
    let value_type = match channel.logical_domain() {
        RuntimeFilterLogicalDomain::Membership { value_type, .. } => value_type,
        RuntimeFilterLogicalDomain::OrderedBound(_) => unreachable!(),
    };
    let value_set = profile.accepts(ArtifactKind::ValueSet);
    let bitset = profile.accepts(ArtifactKind::Bitset) && bitset_schema_is_feasible(value_type);
    let bloom = profile.accepts(ArtifactKind::Bloom);
    if !value_set && !bitset && !bloom {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "M2 Membership profile has no statically feasible membership representation",
        ));
    }
    if matches!(
        channel.logical_domain(),
        RuntimeFilterLogicalDomain::Membership {
            null_semantics: NullSemantics::NullSafeEqual,
            ..
        }
    ) && !value_set
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "NullSafeEqual Membership profile must accept ValueSet",
        ));
    }
    if profile.accepts(ArtifactKind::Range)
        && !capabilities.contains(&ArtifactCapability::OrderedRange)
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "Range physical kind requires OrderedRange semantic capability",
        ));
    }
    if profile.accepted_kinds().iter().any(|kind| {
        matches!(
            kind,
            ArtifactKind::ValueSet | ArtifactKind::Bloom | ArtifactKind::Bitset
        )
    }) && !capabilities.contains(&ArtifactCapability::Membership)
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "membership physical kinds require Membership semantic capability",
        ));
    }
    validate_profile_identity(profile, profile_encodings)?;
    if profile.accepts(ArtifactKind::Bloom) {
        let expected = BloomHashContract::new(schema, channel.materialization_policy())
            .map_err(|_| {
                install_error(
                    InstallContractErrorKind::InvalidPolicy,
                    "materialization Bloom policy is not supported",
                )
            })?
            .digest();
        if profile.bloom_hash_contract() != Some(expected) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "Bloom profile hash contract does not match channel schema and policy",
            ));
        }
    }
    Ok(())
}

fn validate_ordered_channel<'a>(
    channel: &'a RuntimeFilterChannelDeployment,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
    role_requirements: (bool, bool),
) -> Result<(), InstallContractError> {
    let RuntimeFilterLogicalDomain::OrderedBound(plan) = channel.logical_domain() else {
        unreachable!("ordered validator is called only for ordered channels")
    };
    let contract = RuntimeOrderContract::try_from_plan(plan).map_err(|error| {
        install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            format!("ordered channel has an invalid order contract: {error:?}"),
        )
    })?;
    match channel.reduction_requirement() {
        ReductionRequirement::TightenOrderedBound => {
            if channel.lifecycle() != RuntimeFilterLifecycle::MonotonicUpdates
                || channel.allowed_contribution_kinds()
                    != &BTreeSet::from([
                        ContributionKind::OrderedBoundUpdate,
                        ContributionKind::ProducerClosed,
                    ])
                || channel.completion_requirement() != CompletionRequirement::ProducerClosed
            {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "channel does not match the MonotonicUpdates OrderedBound M3A matrix",
                ));
            }
        }
        ReductionRequirement::MergeTopKSummary(requirement) => {
            RuntimeTopKSummaryContract::try_from_plan(plan, requirement).map_err(|error| {
                install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    format!("ordered channel has an invalid top-k summary contract: {error:?}"),
                )
            })?;
            if !channel
                .availability_coverage()
                .is_canonically_equivalent_to(channel.terminal_coverage())
            {
                return Err(install_error(
                    InstallContractErrorKind::InvalidCoverage,
                    "top-k summary availability and terminal coverage must be canonically equivalent",
                ));
            }
            if channel.lifecycle() != RuntimeFilterLifecycle::MonotonicUpdates
                || channel.allowed_contribution_kinds()
                    != &BTreeSet::from([
                        ContributionKind::TopKSummary,
                        ContributionKind::ProducerClosed,
                    ])
                || channel.completion_requirement() != CompletionRequirement::ProducerClosed
                || !channel.availability_coverage().is_all_of_only()
                || !channel.terminal_coverage().is_all_of_only()
            {
                return Err(install_error(
                    InstallContractErrorKind::UnsupportedChannelContract,
                    "channel does not match the MonotonicUpdates OrderedBound TopKSummary M3B matrix",
                ));
            }
        }
        ReductionRequirement::SetUnion => {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered channel cannot use SetUnion reduction",
            ));
        }
    }
    validate_common_channel(channel, role_requirements)?;
    validate_producer_coverage(channel)?;

    let mut unique_profiles = BTreeSet::new();
    for consumer in channel.consumers().values() {
        if consumer.expected_fragment_instances().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "consumer expected fragment instance set must be non-empty",
            ));
        }
        if !matches!(
            consumer.activation(),
            ConsumerActivation::NonBlockingLive { .. }
        ) {
            return Err(install_error(
                InstallContractErrorKind::InvalidConsumerActivation,
                "ordered consumers must use NonBlockingLive activation",
            ));
        }
        if consumer.capabilities() != &BTreeSet::from([ArtifactCapability::OrderedRange]) {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered consumers must declare exactly OrderedRange capability",
            ));
        }
        let profile = consumer.artifact_profile();
        if profile.accepted_kinds() != &BTreeSet::from([ArtifactKind::Range])
            || profile.order_contract_digest() != Some(contract.digest())
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered consumer profile must accept only Range with the channel order digest",
            ));
        }
        unique_profiles.insert((profile.id(), profile.canonical_bytes()));
        validate_profile_identity(profile, profile_encodings)?;
    }
    let mut materialization_profiles = BTreeSet::new();
    for group in channel.outbound_materialization_groups().values() {
        let profile = group.profile();
        if profile.accepted_kinds() != &BTreeSet::from([ArtifactKind::Range])
            || profile.order_contract_digest() != Some(contract.digest())
        {
            return Err(install_error(
                InstallContractErrorKind::UnsupportedChannelContract,
                "ordered materialization profile must accept only Range with the channel order digest",
            ));
        }
        validate_profile_identity(profile, profile_encodings)?;
        materialization_profiles.insert((profile.id(), profile.canonical_bytes()));
    }
    validate_materialization_concurrency(
        channel,
        !channel.outbound_materialization_groups().is_empty(),
        materialization_profiles.len(),
    )
}

fn validate_common_channel(
    channel: &RuntimeFilterChannelDeployment,
    role_requirements: (bool, bool),
) -> Result<(), InstallContractError> {
    let (requires_producer, requires_consumer) = role_requirements;
    if channel.producers().is_empty() != !requires_producer
        || channel.consumers().is_empty() != !requires_consumer
    {
        return Err(install_error(
            InstallContractErrorKind::UnsupportedChannelContract,
            "Core roles do not match the routing requirements",
        ));
    }
    validate_runtime_filter_policy(channel.policy()).map_err(|error| {
        install_error(
            InstallContractErrorKind::InvalidPolicy,
            format!("invalid runtime filter policy: {error:?}"),
        )
    })?;
    if channel.core_budget().max_reducer_bytes() == 0 {
        return Err(install_error(
            InstallContractErrorKind::InvalidBudget,
            "max reducer bytes must be non-zero",
        ));
    }
    let policy = channel.materialization_policy();
    usize::try_from(policy.max_total_retained_bytes()).map_err(|_| {
        install_error(
            InstallContractErrorKind::InvalidBudget,
            "materialization retained budget does not fit this platform",
        )
    })?;
    usize::try_from(policy.max_scratch_bytes_per_job()).map_err(|_| {
        install_error(
            InstallContractErrorKind::InvalidBudget,
            "materialization scratch budget does not fit this platform",
        )
    })?;
    policy.aggregate_scratch_bytes().map_err(|_| {
        install_error(
            InstallContractErrorKind::InvalidBudget,
            "materialization aggregate scratch budget overflows",
        )
    })?;
    Ok(())
}

fn validate_producer_coverage(
    channel: &RuntimeFilterChannelDeployment,
) -> Result<(), InstallContractError> {
    let mut witnesses = BTreeSet::new();
    for producer in channel.producers().values() {
        if !witnesses.insert(producer.coverage_witness_id()) {
            return Err(install_error(
                InstallContractErrorKind::DuplicateCoverageWitness,
                "producer witness identities must be unique within a channel",
            ));
        }
        if producer.expected_fragment_instances().is_empty() {
            return Err(install_error(
                InstallContractErrorKind::EmptyExpectedInstances,
                "producer expected fragment instance set must be non-empty",
            ));
        }
    }
    if !channel.producers().is_empty() {
        validate_coverage(channel.availability_coverage(), channel)?;
        validate_coverage(channel.terminal_coverage(), channel)?;
    } else {
        for coverage in [channel.availability_coverage(), channel.terminal_coverage()] {
            coverage.validate_shape().map_err(|error| {
                install_error(
                    InstallContractErrorKind::InvalidCoverage,
                    format!("invalid coverage shape: {error:?}"),
                )
            })?;
        }
    }
    Ok(())
}

fn validate_profile_identity<'a>(
    profile: &'a ConsumerArtifactProfile,
    profile_encodings: &mut BTreeMap<ConsumerProfileId, &'a [u8]>,
) -> Result<(), InstallContractError> {
    if let Some(existing) = profile_encodings.insert(profile.id(), profile.canonical_bytes())
        && existing != profile.canonical_bytes()
    {
        return Err(install_error(
            InstallContractErrorKind::ConflictingDeployment,
            "consumer profile digest collision carried different canonical bytes",
        ));
    }
    Ok(())
}

fn validate_materialization_concurrency(
    channel: &RuntimeFilterChannelDeployment,
    owns_materialization: bool,
    unique_profiles: usize,
) -> Result<(), InstallContractError> {
    if owns_materialization
        && channel.materialization_policy().max_concurrent_jobs() > unique_profiles
    {
        return Err(install_error(
            InstallContractErrorKind::InvalidPolicy,
            "max concurrent materialization jobs exceeds normalized unique profile count",
        ));
    }
    Ok(())
}

fn bitset_schema_is_feasible(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Decimal128(1..=18, _)
    )
}

fn validate_coverage(
    coverage: &Coverage,
    channel: &RuntimeFilterChannelDeployment,
) -> Result<(), InstallContractError> {
    coverage.validate_shape().map_err(|error| {
        install_error(
            InstallContractErrorKind::InvalidCoverage,
            format!("invalid coverage shape: {error:?}"),
        )
    })?;
    let expected = channel
        .producers()
        .values()
        .map(|producer| producer.coverage_witness_id())
        .collect::<BTreeSet<_>>();
    let mut counts = BTreeMap::new();
    count_witnesses(coverage, &mut counts);
    if counts.keys().any(|witness| !expected.contains(witness)) {
        return Err(install_error(
            InstallContractErrorKind::UnknownCoverageWitness,
            "coverage references a witness without an installed producer",
        ));
    }
    if counts.values().any(|count| *count != 1) {
        return Err(install_error(
            InstallContractErrorKind::DuplicateCoverageWitness,
            "coverage must reference each producer witness exactly once",
        ));
    }
    if counts.keys().copied().collect::<BTreeSet<_>>() != expected {
        return Err(install_error(
            InstallContractErrorKind::UnknownCoverageWitness,
            "coverage must reference every installed producer witness",
        ));
    }
    Ok(())
}

fn count_witnesses(coverage: &Coverage, counts: &mut BTreeMap<CoverageWitnessId, usize>) {
    match coverage {
        Coverage::Leaf(witness) => *counts.entry(*witness).or_default() += 1,
        Coverage::AllOf(children) | Coverage::AnyOf(children) => {
            for child in children {
                count_witnesses(child, counts);
            }
        }
    }
}

fn install_error(
    kind: InstallContractErrorKind,
    detail: impl Into<String>,
) -> InstallContractError {
    InstallContractError::new(kind, detail)
}

#[cfg(test)]
mod tests {
    use super::{CONTRIBUTION_DIGEST_DOMAIN, decode_runtime_filter_contribution};
    use novarocks::query_execution::lifecycle::{
        AttemptId, QueryExecutionId, RuntimeFilterContribution,
    };
    use novarocks_protocol::{common, filter, novarocks as proto_novarocks};
    use novarocks_types::QueryId;
    use prost::Message;
    use sha2::Digest;

    fn execution_id() -> QueryExecutionId {
        QueryExecutionId::new(
            QueryId::new(0x5246_4f34, 7),
            AttemptId::new(3).expect("nonzero attempt"),
        )
        .expect("nonzero execution id")
    }

    fn valid_empty_contribution(execution_id: QueryExecutionId) -> RuntimeFilterContribution {
        let lifecycle = filter::RuntimeFilterQueryLifecycleOptions {
            delivery_expire_ms: 1,
            query_expire_ms: 1,
            transport_retry_interval_ms: 1,
            transport_max_attempts: 1,
            transport_deadline_ms: 1,
            transport_max_pending_entries: 1,
            transport_max_pending_bytes: 1,
        };
        let install = filter::RuntimeFilterParticipantInstall::default();
        let envelope = filter::InstallRuntimeFilterDeploymentRequest {
            query_id: Some(common::UniqueId {
                hi: execution_id.query_id().high(),
                lo: execution_id.query_id().low(),
            }),
            deployment_epoch: execution_id.attempt_id().get(),
            participant_id: 3,
            lifecycle: Some(lifecycle.clone()),
            install: Some(install.clone()),
        };
        let mut digest = sha2::Sha256::new();
        digest.update(CONTRIBUTION_DIGEST_DOMAIN);
        digest.update(envelope.encode_to_vec());
        RuntimeFilterContribution::from_wire(proto_novarocks::RuntimeFilterContribution {
            participant_id: 3,
            lifecycle: Some(lifecycle),
            install: Some(install),
            contribution_digest: digest.finalize().to_vec(),
        })
        .expect("valid opaque contribution fixture")
    }

    #[test]
    fn decodes_backend_participant_domain_only_after_digest_validation() {
        let execution_id = execution_id();
        let contribution = valid_empty_contribution(execution_id);

        let decoded = decode_runtime_filter_contribution(execution_id, &contribution)
            .expect("backend decodes the valid participant install");

        assert_eq!(decoded.install.epoch().get(), 3);
        assert_eq!(decoded.install.local_participant_id().get(), 3);
        assert!(decoded.install.core_view().is_empty());
    }

    #[test]
    fn rejects_bad_digest_before_constructing_participant_domain() {
        let execution_id = execution_id();
        let contribution = valid_empty_contribution(execution_id);
        let mut wire = contribution.wire().clone();
        wire.contribution_digest[0] ^= 0x01;
        let malformed = RuntimeFilterContribution::from_wire(wire)
            .expect("length remains a generic carrier invariant");

        let error = decode_runtime_filter_contribution(execution_id, &malformed)
            .expect_err("bad digest is rejected before service installation");
        assert_eq!(
            error.to_string(),
            "InvalidManifest: runtime filter contribution digest does not match install DTO"
        );
    }
}
