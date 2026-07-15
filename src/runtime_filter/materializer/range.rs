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

use std::sync::Arc;

use crate::runtime_filter::materializer::codec::{encode_range_leaf, encoded_range_leaf_len};
use crate::runtime_filter::port::artifact::{
    ArtifactBundle, ArtifactKind, ConsumerArtifactProfile, PhysicalArtifact, RangeArtifactData,
};
use crate::runtime_filter::port::support::{
    ArtifactRetainedBudget, ArtifactRetention, ArtifactScratchBudget, ArtifactScratchReservation,
    RuntimeFilterMemoryAccount,
};
use crate::runtime_filter::port::value_domain::LogicalSnapshot;

#[derive(Debug)]
pub(crate) enum RangeMaterializationOutcome {
    Published(Arc<ArtifactBundle>),
    ContractViolation,
    ResourceUnavailable,
    MaterializationFailed,
}

pub(crate) struct RangeMaterializationPlan<'a> {
    snapshot: Arc<LogicalSnapshot>,
    profile: &'a ConsumerArtifactProfile,
    max_artifact_bytes: usize,
    leaf_encoded_bytes: usize,
    tuple_resident_bytes: usize,
}

pub(crate) struct AdmittedRangeMaterialization<'a> {
    plan: RangeMaterializationPlan<'a>,
    _scratch: ArtifactScratchReservation,
    artifact_footprint: usize,
    total_footprint: usize,
    retained: Arc<ArtifactRetention>,
}

pub(crate) struct RangeMaterializer;

impl RangeMaterializer {
    pub(crate) fn plan<'a>(
        snapshot: Arc<LogicalSnapshot>,
        profile: &'a ConsumerArtifactProfile,
        max_artifact_bytes: usize,
    ) -> Result<RangeMaterializationPlan<'a>, RangeMaterializationOutcome> {
        let domain = snapshot
            .ordered_bound()
            .ok_or(RangeMaterializationOutcome::ContractViolation)?;
        if profile.accepted_kinds() != &std::collections::BTreeSet::from([ArtifactKind::Range])
            || profile.order_contract_digest() != Some(domain.contract().digest())
        {
            return Err(RangeMaterializationOutcome::ContractViolation);
        }
        domain
            .contract()
            .compare(domain.bound(), domain.bound())
            .map_err(|_| RangeMaterializationOutcome::ContractViolation)?;
        let leaf_encoded_bytes = encoded_range_leaf_len(domain.contract(), domain.bound())
            .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        let bundle_bytes =
            ArtifactBundle::canonical_encoded_len_for_single_artifact(leaf_encoded_bytes)
                .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        if bundle_bytes > max_artifact_bytes {
            return Err(RangeMaterializationOutcome::ResourceUnavailable);
        }
        let tuple_resident_bytes = domain
            .bound()
            .estimated_retained_bytes()
            .ok_or(RangeMaterializationOutcome::ResourceUnavailable)?;
        Ok(RangeMaterializationPlan {
            snapshot,
            profile,
            max_artifact_bytes,
            leaf_encoded_bytes,
            tuple_resident_bytes,
        })
    }

    pub(crate) fn admit<'a>(
        plan: RangeMaterializationPlan<'a>,
        retained_budget: Arc<ArtifactRetainedBudget>,
        scratch_budget: Arc<ArtifactScratchBudget>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> Result<AdmittedRangeMaterialization<'a>, RangeMaterializationOutcome> {
        let scratch_bytes = plan
            .leaf_encoded_bytes
            .checked_mul(3)
            .ok_or(RangeMaterializationOutcome::ResourceUnavailable)?;
        let scratch = ArtifactScratchReservation::try_new(
            scratch_bytes,
            scratch_budget,
            memory_account.clone(),
        )
        .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        let data_bytes =
            RangeArtifactData::accounted_resident_bytes_for_tuple(plan.tuple_resident_bytes)
                .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        let artifact_footprint =
            PhysicalArtifact::accounted_resident_component_bytes(plan.leaf_encoded_bytes)
                .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?
                .checked_add(data_bytes)
                .ok_or(RangeMaterializationOutcome::ResourceUnavailable)?;
        let bundle_footprint = ArtifactBundle::accounted_resident_overhead(plan.profile, 1)
            .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?;
        let total_footprint = artifact_footprint
            .checked_add(bundle_footprint)
            .ok_or(RangeMaterializationOutcome::ResourceUnavailable)?;
        let retained = Arc::new(
            ArtifactRetention::try_new(total_footprint, retained_budget, memory_account)
                .map_err(|_| RangeMaterializationOutcome::ResourceUnavailable)?,
        );
        Ok(AdmittedRangeMaterialization {
            plan,
            _scratch: scratch,
            artifact_footprint,
            total_footprint,
            retained,
        })
    }

    pub(crate) fn encode(
        admitted: AdmittedRangeMaterialization<'_>,
    ) -> Result<Arc<ArtifactBundle>, RangeMaterializationOutcome> {
        let domain = admitted
            .plan
            .snapshot
            .ordered_bound()
            .ok_or(RangeMaterializationOutcome::ContractViolation)?;
        let encoded = encode_range_leaf(
            domain.contract(),
            domain.bound(),
            admitted.plan.snapshot.version(),
        )
        .map_err(|_| RangeMaterializationOutcome::MaterializationFailed)?;
        if encoded.len() != admitted.plan.leaf_encoded_bytes {
            return Err(RangeMaterializationOutcome::MaterializationFailed);
        }
        let data = RangeArtifactData::new(domain.contract().clone(), domain.bound().clone())
            .map_err(|_| RangeMaterializationOutcome::ContractViolation)?;
        let artifact = Arc::new(
            PhysicalArtifact::from_range_shared_retained(
                admitted.plan.snapshot.version(),
                data,
                encoded.into(),
                admitted.artifact_footprint,
                admitted.total_footprint,
                admitted.retained.clone(),
            )
            .map_err(|_| RangeMaterializationOutcome::MaterializationFailed)?,
        );
        let bundle = ArtifactBundle::new_retained(
            admitted.plan.snapshot.channel_id(),
            admitted.plan.snapshot.version(),
            admitted.plan.profile,
            vec![(ArtifactKind::Range, artifact)],
            admitted.plan.max_artifact_bytes,
            admitted.retained,
        )
        .map_err(|_| RangeMaterializationOutcome::MaterializationFailed)?;
        Ok(Arc::new(bundle))
    }

    pub(crate) fn materialize(
        snapshot: Arc<LogicalSnapshot>,
        profile: &ConsumerArtifactProfile,
        max_artifact_bytes: usize,
        retained_budget: Arc<ArtifactRetainedBudget>,
        scratch_budget: Arc<ArtifactScratchBudget>,
        memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    ) -> RangeMaterializationOutcome {
        let plan = match Self::plan(snapshot, profile, max_artifact_bytes) {
            Ok(plan) => plan,
            Err(outcome) => return outcome,
        };
        let admitted = match Self::admit(plan, retained_budget, scratch_budget, memory_account) {
            Ok(admitted) => admitted,
            Err(outcome) => return outcome,
        };
        match Self::encode(admitted) {
            Ok(bundle) => RangeMaterializationOutcome::Published(bundle),
            Err(outcome) => outcome,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::DataType;

    use crate::runtime_filter::core::ordered_reducer::OrderedBoundDomain;
    use crate::runtime_filter::model::contract::{
        ChannelId, NullOrder, OrderContract, OrderKeyContract, SortDirection,
    };
    use crate::runtime_filter::port::artifact::{ArtifactKind, ConsumerArtifactProfile};
    use crate::runtime_filter::port::identity::LogicalVersion;
    use crate::runtime_filter::port::ordered_bound::{
        COMPARATOR_ALGORITHM_VERSION, ComparatorDigestV1, OrderedScalar, OrderedTuple,
        RuntimeOrderContract,
    };
    use crate::runtime_filter::port::support::{
        ArtifactRetainedBudget, ArtifactScratchBudget, MemoryAccountError,
        RetainedMemoryReservation, RuntimeFilterMemoryAccount,
    };
    use crate::runtime_filter::port::value_domain::LogicalSnapshot;

    use super::{RangeMaterializationOutcome, RangeMaterializer};

    #[derive(Default)]
    struct CountingMemory(AtomicUsize);

    impl RuntimeFilterMemoryAccount for CountingMemory {
        fn try_consume(&self, bytes: usize) -> Result<(), MemoryAccountError> {
            self.0.fetch_add(bytes, Ordering::SeqCst);
            Ok(())
        }

        fn release(&self, bytes: usize) {
            self.0.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    struct Fixture {
        snapshot: Arc<LogicalSnapshot>,
        profile: ConsumerArtifactProfile,
        retained: Arc<ArtifactRetainedBudget>,
        scratch: Arc<ArtifactScratchBudget>,
        memory: Arc<CountingMemory>,
    }

    fn fixture() -> Fixture {
        let keys = vec![
            OrderKeyContract {
                data_type: DataType::Int64,
                direction: SortDirection::Ascending,
                null_order: NullOrder::Last,
            },
            OrderKeyContract {
                data_type: DataType::Utf8,
                direction: SortDirection::Descending,
                null_order: NullOrder::First,
            },
        ];
        let comparator =
            ComparatorDigestV1::for_contract(&keys, COMPARATOR_ALGORITHM_VERSION).unwrap();
        let contract = Arc::new(
            RuntimeOrderContract::try_from_plan(&OrderContract {
                keys,
                inclusive: true,
                comparator_digest: comparator,
            })
            .unwrap(),
        );
        let bound = OrderedTuple::try_new(
            &contract,
            [
                Some(OrderedScalar::Int64(42)),
                Some(OrderedScalar::Utf8("deterministic".into())),
            ],
        )
        .unwrap();
        let snapshot = Arc::new(LogicalSnapshot::ordered(
            ChannelId::new(7),
            LogicalVersion::new(3),
            Arc::new(OrderedBoundDomain::new(contract.clone(), bound)),
            RetainedMemoryReservation::empty(),
        ));
        Fixture {
            profile: ConsumerArtifactProfile::new_ordered_range(contract.digest()).unwrap(),
            snapshot,
            retained: Arc::new(ArtifactRetainedBudget::new(1 << 20)),
            scratch: Arc::new(ArtifactScratchBudget::new(1 << 20, 1 << 20).unwrap()),
            memory: Arc::new(CountingMemory::default()),
        }
    }

    #[test]
    fn range_materialization_is_deterministic_and_preserves_the_typed_payload() {
        let first = fixture();
        let second = fixture();
        let first_bundle = RangeMaterializer::materialize(
            first.snapshot,
            &first.profile,
            usize::MAX,
            first.retained,
            first.scratch,
            first.memory,
        );
        let second_bundle = RangeMaterializer::materialize(
            second.snapshot,
            &second.profile,
            usize::MAX,
            second.retained,
            second.scratch,
            second.memory,
        );
        let (
            RangeMaterializationOutcome::Published(first_bundle),
            RangeMaterializationOutcome::Published(second_bundle),
        ) = (first_bundle, second_bundle)
        else {
            panic!("valid Range fixtures must materialize")
        };
        assert_eq!(
            first_bundle.canonical_digest(),
            second_bundle.canonical_digest()
        );
        assert_eq!(first_bundle.artifacts()[0].0, ArtifactKind::Range);
        assert_eq!(
            first_bundle.artifacts()[0].1.range().unwrap().bound(),
            second_bundle.artifacts()[0].1.range().unwrap().bound()
        );
        assert_eq!(
            first_bundle.artifacts()[0]
                .1
                .range()
                .unwrap()
                .semantic_digest(),
            second_bundle.artifacts()[0]
                .1
                .range()
                .unwrap()
                .semantic_digest()
        );
    }

    #[test]
    fn range_materialization_budget_failures_leave_zero_retained_bytes() {
        let fixture = fixture();
        let retained = Arc::new(ArtifactRetainedBudget::new(1));
        let outcome = RangeMaterializer::materialize(
            fixture.snapshot,
            &fixture.profile,
            usize::MAX,
            retained.clone(),
            fixture.scratch.clone(),
            fixture.memory.clone(),
        );
        assert!(matches!(
            outcome,
            RangeMaterializationOutcome::ResourceUnavailable
        ));
        assert_eq!(retained.retained_bytes(), 0);
        assert_eq!(fixture.scratch.retained_bytes(), 0);
        assert_eq!(fixture.memory.0.load(Ordering::SeqCst), 0);
    }
}
