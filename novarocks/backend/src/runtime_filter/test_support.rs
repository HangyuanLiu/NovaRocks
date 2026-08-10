//! Backend-local runtime-filter fixtures.
//!
//! These fixtures intentionally build only Execution contracts and canonical
//! contributions. They neither call a Frontend compiler nor borrow Core's
//! runtime-filter test support.

use arrow::datatypes::DataType;
use novarocks_execution::runtime_filter::{
    RuntimeFilterBindingId, RuntimeFilterChannelId, RuntimeFilterContribution,
    RuntimeFilterContributionKind, RuntimeFilterExecutionContract, RuntimeFilterMembershipSchema,
    RuntimeFilterNullSemantics, RuntimeFilterProducerContract, contribution,
};
use novarocks_types::UniqueId;

use super::domain::{BackendCoverage, BackendCoverageWitnessId, BackendParticipantIdentity};

pub(crate) struct BackendRuntimeFilterFixture {
    identity: BackendParticipantIdentity,
    producer_contract: RuntimeFilterProducerContract,
    coverage: BackendCoverage,
    membership_contribution: RuntimeFilterContribution,
}

impl BackendRuntimeFilterFixture {
    pub(crate) fn membership() -> Self {
        let schema = RuntimeFilterMembershipSchema::new(
            &DataType::Int64,
            RuntimeFilterNullSemantics::NeverMatches,
        )
        .expect("Int64 membership schema is supported");
        let contract = RuntimeFilterExecutionContract::Membership(schema.clone());
        let producer_contract = RuntimeFilterProducerContract::membership(
            RuntimeFilterBindingId::new(7),
            RuntimeFilterChannelId::new(11),
            contract,
        )
        .expect("membership producer contract is valid");
        let domain = contribution::ValueDomainDelta::new(
            contribution::MembershipValues::int64([3, 9]),
            false,
        );
        let encoded = contribution::encode_contribution(
            &contribution::RuntimeFilterContribution::membership(domain),
            contribution::ContributionCodecExpectation::membership(
                schema.data_type(),
                schema.digest(),
            ),
            1024,
        )
        .expect("fixture contribution fits its budget");
        Self {
            identity: BackendParticipantIdentity::new(UniqueId::new(17, 19), 23),
            producer_contract,
            coverage: BackendCoverage::witness(BackendCoverageWitnessId::new(29)),
            membership_contribution: RuntimeFilterContribution::new(
                RuntimeFilterContributionKind::Membership,
                *encoded.schema_digest(),
                encoded.into_parts().1,
            ),
        }
    }

    pub(crate) const fn identity(&self) -> BackendParticipantIdentity {
        self.identity
    }

    pub(crate) fn producer_contract(&self) -> RuntimeFilterProducerContract {
        self.producer_contract.clone()
    }

    pub(crate) fn coverage(&self) -> BackendCoverage {
        self.coverage.clone()
    }

    pub(crate) fn membership_contribution(&self) -> RuntimeFilterContribution {
        self.membership_contribution.clone()
    }

    pub(crate) fn membership_contribution_with_values(
        &self,
        values: impl IntoIterator<Item = i64>,
    ) -> RuntimeFilterContribution {
        let RuntimeFilterExecutionContract::Membership(schema) = self.producer_contract.contract()
        else {
            panic!("membership fixture must use a membership contract")
        };
        let encoded = contribution::encode_contribution(
            &contribution::RuntimeFilterContribution::membership(
                contribution::ValueDomainDelta::new(
                    contribution::MembershipValues::int64(values),
                    false,
                ),
            ),
            contribution::ContributionCodecExpectation::membership(
                schema.data_type(),
                schema.digest(),
            ),
            1024,
        )
        .expect("fixture contribution fits its budget");
        RuntimeFilterContribution::new(
            RuntimeFilterContributionKind::Membership,
            *encoded.schema_digest(),
            encoded.into_parts().1,
        )
    }

    pub(crate) fn contribution_with_digest(&self, digest: [u8; 32]) -> RuntimeFilterContribution {
        RuntimeFilterContribution::new(
            RuntimeFilterContributionKind::Membership,
            digest,
            self.membership_contribution.canonical_bytes().clone(),
        )
    }
}
