use novarocks_execution::runtime_filter::{
    LogicalVersion, PartitionId, ProducerSequence, RuntimeFilterChannelId,
    RuntimeFilterContribution, RuntimeFilterContributionKind,
    contribution::{
        ContributionCodecExpectation, MembershipValues,
        RuntimeFilterContribution as TypedContribution, ValueDomainDelta, encode_contribution,
    },
};

use super::{
    BackendCoverage, BackendCoverageProgress, BackendCoverageState, BackendCoverageWitnessId,
    BackendInstallPolicy, BackendLogicalSnapshot, BackendProducerStreamIdentity,
    BackendReducedLogicalDomain, BackendReducedLogicalSnapshot, BackendReductionApply,
    BackendReductionState,
};
use crate::runtime_filter::test_support::BackendRuntimeFilterFixture;

#[test]
fn membership_install_accepts_only_its_exact_execution_contribution() {
    let fixture = BackendRuntimeFilterFixture::membership();
    let policy = BackendInstallPolicy::new(
        fixture.identity(),
        fixture.producer_contract(),
        fixture.coverage(),
        1024,
    )
    .expect("fixture is a valid Backend install policy");

    let snapshot = BackendLogicalSnapshot::new(
        &policy,
        LogicalVersion::FIRST,
        fixture.membership_contribution(),
    )
    .expect("matching canonical contribution is accepted");
    assert_eq!(snapshot.logical_version(), LogicalVersion::FIRST);
    assert!(
        BackendLogicalSnapshot::new(
            &policy,
            LogicalVersion::FIRST,
            fixture.contribution_with_digest([99; 32]),
        )
        .is_err()
    );
}

#[test]
fn reduction_state_accepts_execution_nrfc_and_publishes_backend_snapshot() {
    let fixture = BackendRuntimeFilterFixture::membership();
    let policy = BackendInstallPolicy::new(
        fixture.identity(),
        fixture.producer_contract(),
        fixture.coverage(),
        1024,
    )
    .unwrap();
    let stream = BackendProducerStreamIdentity::new(
        policy.channel(),
        novarocks_types::UniqueId::new(31, 37),
        PartitionId::new(0),
    );
    let mut state = BackendReductionState::new(policy).unwrap();
    let (outcome, snapshot) = state
        .submit(
            stream,
            ProducerSequence::new(0),
            fixture.membership_contribution(),
        )
        .unwrap();
    assert_eq!(
        outcome,
        BackendReductionApply::Applied {
            version: LogicalVersion::FIRST
        }
    );
    assert!(matches!(
        snapshot.expect("accepted membership publishes a snapshot").domain(),
        BackendReducedLogicalDomain::Membership(domain)
            if domain.values() == &MembershipValues::int64([3, 9])
    ));
}

#[test]
fn reduction_state_retains_latest_aggregate_snapshot_for_terminal_publication() {
    let fixture = BackendRuntimeFilterFixture::membership();
    let policy = BackendInstallPolicy::new(
        fixture.identity(),
        fixture.producer_contract(),
        fixture.coverage(),
        1024,
    )
    .unwrap();
    let first_stream = BackendProducerStreamIdentity::new(
        policy.channel(),
        novarocks_types::UniqueId::new(31, 37),
        PartitionId::new(0),
    );
    let second_stream = BackendProducerStreamIdentity::new(
        policy.channel(),
        novarocks_types::UniqueId::new(41, 43),
        PartitionId::new(0),
    );
    let schema = fixture.producer_contract().contract().clone();
    let novarocks_execution::runtime_filter::RuntimeFilterExecutionContract::Membership(schema) =
        schema
    else {
        panic!("membership fixture must use a membership contract")
    };
    let contribution = |values| {
        let encoded = encode_contribution(
            &TypedContribution::membership(ValueDomainDelta::new(
                MembershipValues::int64(values),
                false,
            )),
            ContributionCodecExpectation::membership(schema.data_type(), schema.digest()),
            1024,
        )
        .unwrap();
        RuntimeFilterContribution::new(
            RuntimeFilterContributionKind::Membership,
            *encoded.schema_digest(),
            encoded.into_parts().1,
        )
    };
    let mut state = BackendReductionState::new(policy).unwrap();

    state
        .submit(first_stream, ProducerSequence::new(0), contribution([3, 9]))
        .unwrap();
    state
        .submit(
            second_stream,
            ProducerSequence::new(0),
            contribution([12, 18]),
        )
        .unwrap();

    assert!(matches!(
        state.latest_snapshot().expect("aggregate snapshot exists").domain(),
        BackendReducedLogicalDomain::Membership(domain)
            if domain.values() == &MembershipValues::int64([3, 9, 12, 18])
    ));
}

#[test]
fn reduction_state_applies_newer_sequence_payload_on_the_same_stream() {
    let fixture = BackendRuntimeFilterFixture::membership();
    let policy = BackendInstallPolicy::new(
        fixture.identity(),
        fixture.producer_contract(),
        fixture.coverage(),
        1024,
    )
    .unwrap();
    let stream = BackendProducerStreamIdentity::new(
        policy.channel(),
        novarocks_types::UniqueId::new(31, 37),
        PartitionId::new(0),
    );
    let mut state = BackendReductionState::new(policy).unwrap();

    state
        .submit(
            stream,
            ProducerSequence::new(0),
            fixture.membership_contribution_with_values([3, 9]),
        )
        .unwrap();
    let (outcome, snapshot) = state
        .submit(
            stream,
            ProducerSequence::new(1),
            fixture.membership_contribution_with_values([12, 18]),
        )
        .unwrap();

    assert_eq!(
        outcome,
        BackendReductionApply::Applied {
            version: LogicalVersion::new(2)
        }
    );
    assert!(matches!(
        snapshot.expect("new payload advances the aggregate").domain(),
        BackendReducedLogicalDomain::Membership(domain)
            if domain.values() == &MembershipValues::int64([3, 9, 12, 18])
    ));
}

#[test]
fn reduced_membership_snapshot_keeps_execution_domain_and_backend_publication_identity() {
    let snapshot = BackendReducedLogicalSnapshot::membership(
        RuntimeFilterChannelId::new(9),
        LogicalVersion::FIRST,
        ValueDomainDelta::new(MembershipValues::int64([3, 9]), false),
    );
    assert_eq!(snapshot.channel_id(), RuntimeFilterChannelId::new(9));
    assert_eq!(snapshot.logical_version(), LogicalVersion::FIRST);
    assert!(matches!(
        snapshot.domain(),
        BackendReducedLogicalDomain::Membership(domain)
            if domain.values() == &MembershipValues::int64([3, 9])
    ));
}

#[test]
fn coverage_is_three_valued_and_terminal_witnesses_do_not_regress() {
    let left = BackendCoverageWitnessId::new(1);
    let right = BackendCoverageWitnessId::new(2);
    let coverage = BackendCoverage::all_of([
        BackendCoverage::witness(left),
        BackendCoverage::any_of([BackendCoverage::witness(right)]).expect("valid nested coverage"),
    ])
    .expect("non-empty coverage is valid");
    let mut state = BackendCoverageState::new(&coverage).expect("coverage has all witnesses");

    assert_eq!(state.progress(&coverage), BackendCoverageProgress::Pending);
    assert!(state.mark_satisfied(left));
    assert_eq!(state.progress(&coverage), BackendCoverageProgress::Pending);
    assert!(state.mark_impossible(right));
    assert_eq!(
        state.progress(&coverage),
        BackendCoverageProgress::Impossible
    );
    assert!(!state.mark_satisfied(right));
}
