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

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, Weak};

use crate::runtime_filter::model::contract::ChannelId;
use crate::runtime_filter::port::artifact::{ArtifactBundle, ConsumerProfileId};
use crate::runtime_filter::port::identity::{DeploymentEpoch, LogicalVersion};
use crate::runtime_filter::port::producer::{
    RuntimeContractViolation, RuntimeContractViolationKind,
};
use crate::runtime_filter::port::subscription::ArtifactDeliveryOutcome;

use crate::runtime_filter::materializer::{
    AdmittedMaterialization, MaterializationAdmission, MaterializationOutcome, Materializer,
    UnavailableReason as MaterializerUnavailableReason,
    UnsupportedReason as MaterializerUnsupportedReason,
};
use crate::runtime_filter::port::events::{ArtifactMaterializationIdentity, RuntimeFilterEvent};
use crate::runtime_filter::port::subscription::{ArtifactUnsupportedReason, UnavailableReason};
use crate::runtime_filter::port::support::RuntimeFilterMemoryAccount;
use crate::runtime_filter::port::value_domain::LogicalSnapshot;

use super::registry::{CapabilityGroup, ChannelArtifactPlan};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ArtifactPublishKey {
    channel_id: ChannelId,
    epoch: DeploymentEpoch,
    profile_id: ConsumerProfileId,
}

impl ArtifactPublishKey {
    pub(super) const fn new(
        channel_id: ChannelId,
        epoch: DeploymentEpoch,
        profile_id: ConsumerProfileId,
    ) -> Self {
        Self {
            channel_id,
            epoch,
            profile_id,
        }
    }

    pub(super) const fn channel_id(self) -> ChannelId {
        self.channel_id
    }

    pub(super) const fn profile_id(self) -> ConsumerProfileId {
        self.profile_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PublishCommitOutcome {
    Published,
    Stale,
    Idempotent,
    Cancelled,
}

#[derive(Default)]
struct KeyState {
    generation: u64,
    cancelled: bool,
    latest: Option<(LogicalVersion, ArtifactDeliveryOutcome)>,
    in_flight: BTreeMap<(LogicalVersion, u64), Arc<JobFlight>>,
}

#[derive(Default)]
struct GateState {
    keys: BTreeMap<ArtifactPublishKey, KeyState>,
}

#[derive(Default)]
struct JobFlight {
    outcome: Mutex<Option<ArtifactDeliveryOutcome>>,
    changed: Condvar,
}

impl JobFlight {
    fn completed(outcome: ArtifactDeliveryOutcome) -> Arc<Self> {
        Arc::new(Self {
            outcome: Mutex::new(Some(outcome)),
            changed: Condvar::new(),
        })
    }

    fn finish(&self, outcome: ArtifactDeliveryOutcome) {
        let mut state = self
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.is_none() {
            *state = Some(outcome);
            self.changed.notify_all();
        }
    }

    fn wait(&self) -> ArtifactDeliveryOutcome {
        let mut state = self
            .outcome
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while state.is_none() {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|error| error.into_inner());
        }
        state.as_ref().expect("completed artifact flight").clone()
    }
}

#[derive(Clone, Default)]
pub(super) struct ArtifactPublishGate {
    state: Arc<Mutex<GateState>>,
}

pub(super) enum ArtifactJobClaim {
    Owner(ArtifactJobOwner),
    Follower(ArtifactJobFollower),
    Stale,
}

pub(super) struct ArtifactJobFollower {
    flight: Arc<JobFlight>,
}

impl ArtifactJobFollower {
    pub(super) fn wait(self) -> ArtifactDeliveryOutcome {
        self.flight.wait()
    }
}

pub(super) struct ArtifactJobOwner {
    gate: Weak<Mutex<GateState>>,
    key: ArtifactPublishKey,
    version: LogicalVersion,
    generation: u64,
    flight: Arc<JobFlight>,
    finished: bool,
}

impl ArtifactJobOwner {
    pub(super) fn finish(
        mut self,
        outcome: ArtifactDeliveryOutcome,
    ) -> Result<PublishCommitOutcome, RuntimeContractViolation> {
        self.finished = true;
        let Some(state) = self.gate.upgrade() else {
            self.flight.finish(ArtifactDeliveryOutcome::Cancelled);
            return Ok(PublishCommitOutcome::Cancelled);
        };
        finish_job(
            &state,
            self.key,
            self.version,
            self.generation,
            &self.flight,
            outcome,
        )
    }
}

impl Drop for ArtifactJobOwner {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let Some(state) = self.gate.upgrade() else {
            self.flight.finish(ArtifactDeliveryOutcome::Cancelled);
            return;
        };
        let _ = finish_job(
            &state,
            self.key,
            self.version,
            self.generation,
            &self.flight,
            ArtifactDeliveryOutcome::Cancelled,
        );
    }
}

impl ArtifactPublishGate {
    pub(super) fn generation(&self, key: ArtifactPublishKey) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .keys
            .entry(key)
            .or_default()
            .generation
    }

    pub(super) fn claim(
        &self,
        key: ArtifactPublishKey,
        version: LogicalVersion,
    ) -> ArtifactJobClaim {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let key_state = state.keys.entry(key).or_default();
        if key_state.cancelled {
            return ArtifactJobClaim::Follower(ArtifactJobFollower {
                flight: JobFlight::completed(ArtifactDeliveryOutcome::Cancelled),
            });
        }
        if let Some((latest_version, outcome)) = &key_state.latest {
            if version < *latest_version {
                return ArtifactJobClaim::Stale;
            }
            if version == *latest_version {
                return ArtifactJobClaim::Follower(ArtifactJobFollower {
                    flight: JobFlight::completed(outcome.clone()),
                });
            }
        }
        let generation = key_state.generation;
        if let Some(flight) = key_state.in_flight.get(&(version, generation)) {
            return ArtifactJobClaim::Follower(ArtifactJobFollower {
                flight: flight.clone(),
            });
        }
        let flight = Arc::new(JobFlight::default());
        key_state
            .in_flight
            .insert((version, generation), flight.clone());
        ArtifactJobClaim::Owner(ArtifactJobOwner {
            gate: Arc::downgrade(&self.state),
            key,
            version,
            generation,
            flight,
            finished: false,
        })
    }

    pub(super) fn commit_published(
        &self,
        key: ArtifactPublishKey,
        generation: u64,
        bundle: Arc<ArtifactBundle>,
    ) -> Result<PublishCommitOutcome, RuntimeContractViolation> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        commit_locked(
            state.keys.entry(key).or_default(),
            generation,
            bundle.version(),
            ArtifactDeliveryOutcome::Published(bundle),
        )
    }

    pub(super) fn cancel(&self, key: ArtifactPublishKey) -> bool {
        self.cancel_all([key]).contains(&key)
    }

    pub(super) fn cancel_all(
        &self,
        keys: impl IntoIterator<Item = ArtifactPublishKey>,
    ) -> Vec<ArtifactPublishKey> {
        let (newly_terminalized, flights) = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let mut newly_terminalized = Vec::new();
            let mut flights = Vec::new();
            for key in keys {
                let key_state = state.keys.entry(key).or_default();
                if key_state.cancelled {
                    continue;
                }
                if key_state.latest.is_none() {
                    newly_terminalized.push(key);
                }
                key_state.generation = key_state.generation.checked_add(1).unwrap_or(u64::MAX);
                key_state.cancelled = true;
                flights.extend(std::mem::take(&mut key_state.in_flight).into_values());
            }
            (newly_terminalized, flights)
        };
        for flight in flights {
            flight.finish(ArtifactDeliveryOutcome::Cancelled);
        }
        newly_terminalized
    }

    pub(super) fn cancel_channel(&self, channel_id: ChannelId, epoch: DeploymentEpoch) {
        let keys = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state
                .keys
                .keys()
                .copied()
                .filter(|key| key.channel_id == channel_id && key.epoch == epoch)
                .collect::<Vec<_>>()
        };
        self.cancel_all(keys);
    }
}

fn finish_job(
    state: &Arc<Mutex<GateState>>,
    key: ArtifactPublishKey,
    version: LogicalVersion,
    generation: u64,
    flight: &Arc<JobFlight>,
    outcome: ArtifactDeliveryOutcome,
) -> Result<PublishCommitOutcome, RuntimeContractViolation> {
    let result = {
        let mut state = state.lock().unwrap_or_else(|error| error.into_inner());
        let key_state = state.keys.entry(key).or_default();
        let active = key_state
            .in_flight
            .get(&(version, generation))
            .is_some_and(|active| Arc::ptr_eq(active, flight));
        if !active || key_state.cancelled || key_state.generation != generation {
            Ok((
                PublishCommitOutcome::Cancelled,
                ArtifactDeliveryOutcome::Cancelled,
            ))
        } else {
            key_state.in_flight.remove(&(version, generation));
            commit_locked(key_state, generation, version, outcome.clone()).map(|decision| {
                let follower = if decision == PublishCommitOutcome::Cancelled {
                    ArtifactDeliveryOutcome::Cancelled
                } else {
                    outcome
                };
                (decision, follower)
            })
        }
    };
    match result {
        Ok((decision, follower_outcome)) => {
            flight.finish(follower_outcome);
            Ok(decision)
        }
        Err(error) => {
            flight.finish(ArtifactDeliveryOutcome::Cancelled);
            Err(error)
        }
    }
}

fn commit_locked(
    state: &mut KeyState,
    generation: u64,
    version: LogicalVersion,
    outcome: ArtifactDeliveryOutcome,
) -> Result<PublishCommitOutcome, RuntimeContractViolation> {
    if state.cancelled || state.generation != generation {
        return Ok(PublishCommitOutcome::Cancelled);
    }
    if let Some((latest_version, latest_outcome)) = &state.latest {
        if version < *latest_version {
            return Ok(PublishCommitOutcome::Stale);
        }
        if version == *latest_version {
            if outcomes_identical(latest_outcome, &outcome) {
                return Ok(PublishCommitOutcome::Idempotent);
            }
            return Err(RuntimeContractViolation::new(
                RuntimeContractViolationKind::ConflictingArtifactPublish,
                "same artifact profile version carried a different terminal outcome",
            ));
        }
    }
    state.latest = Some((version, outcome));
    Ok(PublishCommitOutcome::Published)
}

fn outcomes_identical(left: &ArtifactDeliveryOutcome, right: &ArtifactDeliveryOutcome) -> bool {
    match (left, right) {
        (ArtifactDeliveryOutcome::Published(left), ArtifactDeliveryOutcome::Published(right)) => {
            left.canonical_digest() == right.canonical_digest()
        }
        (
            ArtifactDeliveryOutcome::Unsupported(left),
            ArtifactDeliveryOutcome::Unsupported(right),
        ) => left == right,
        (
            ArtifactDeliveryOutcome::Unavailable(left),
            ArtifactDeliveryOutcome::Unavailable(right),
        ) => left == right,
        (ArtifactDeliveryOutcome::Cancelled, ArtifactDeliveryOutcome::Cancelled) => true,
        _ => false,
    }
}

pub(super) enum MaterializationWorkClaim {
    Owner(ArtifactJobOwner),
    Follower,
    Stale,
}

pub(super) struct MaterializationWorkResult {
    pub(super) group: CapabilityGroup,
    pub(super) claim: MaterializationWorkClaim,
    pub(super) outcome: Option<ArtifactDeliveryOutcome>,
    pub(super) events: Vec<RuntimeFilterEvent>,
}

pub(super) fn run_materialization_jobs(
    plan: &ChannelArtifactPlan,
    gate: &ArtifactPublishGate,
    snapshot: &Arc<LogicalSnapshot>,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
    before_encode: Option<Arc<dyn Fn(ConsumerProfileId) + Send + Sync>>,
) -> Vec<MaterializationWorkResult> {
    let groups = plan.groups();
    if groups.is_empty() {
        return Vec::new();
    }
    let batch_size = plan.max_concurrent_jobs().min(groups.len()).max(1);
    let mut results = Vec::with_capacity(groups.len());
    for batch in groups.chunks(batch_size) {
        // Claim, plan, and reserve in canonical profile order. This makes scarce-budget
        // winners deterministic while keeping the expensive codec phase concurrent.
        let admitted = batch
            .iter()
            .map(|group| admit_group(plan, gate, snapshot, group, memory_account.clone()))
            .collect::<Vec<_>>();
        let mut batch_results = std::thread::scope(|scope| {
            let mut jobs = Vec::with_capacity(admitted.len());
            for work in admitted {
                match work {
                    AdmittedGroup::Complete(result) => jobs.push(ScopedJob::Complete(result)),
                    AdmittedGroup::Follower { group, follower } => {
                        jobs.push(ScopedJob::Running(scope.spawn(move || {
                            MaterializationWorkResult {
                                group,
                                claim: MaterializationWorkClaim::Follower,
                                outcome: Some(follower.wait()),
                                events: Vec::new(),
                            }
                        })));
                    }
                    AdmittedGroup::Ready {
                        group,
                        owner,
                        identity,
                        admitted,
                        events,
                    } => {
                        let before_encode = before_encode.clone();
                        jobs.push(ScopedJob::Running(scope.spawn(move || {
                            let outcome =
                                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                    if let Some(hook) = before_encode {
                                        hook(group.profile().id());
                                    }
                                    Materializer::encode(admitted)
                                }))
                                .ok()
                                .and_then(Result::ok)
                                .map(MaterializationOutcome::Published)
                                .unwrap_or(
                                    MaterializationOutcome::Unavailable(
                                        MaterializerUnavailableReason::MaterializationFailed,
                                    ),
                                );
                            complete_group(group, owner, identity, events, outcome)
                        })));
                    }
                }
            }
            jobs.into_iter()
                .map(|job| match job {
                    ScopedJob::Complete(result) => result,
                    ScopedJob::Running(handle) => handle
                        .join()
                        .expect("materialization encode worker catches every panic"),
                })
                .collect::<Vec<_>>()
        });
        results.append(&mut batch_results);
    }
    results
}

enum AdmittedGroup<'a> {
    Ready {
        group: CapabilityGroup,
        owner: ArtifactJobOwner,
        identity: ArtifactMaterializationIdentity,
        admitted: AdmittedMaterialization<'a>,
        events: Vec<RuntimeFilterEvent>,
    },
    Follower {
        group: CapabilityGroup,
        follower: ArtifactJobFollower,
    },
    Complete(MaterializationWorkResult),
}

enum ScopedJob<'scope> {
    Complete(MaterializationWorkResult),
    Running(std::thread::ScopedJoinHandle<'scope, MaterializationWorkResult>),
}

fn admit_group<'a>(
    plan: &'a ChannelArtifactPlan,
    gate: &ArtifactPublishGate,
    snapshot: &Arc<LogicalSnapshot>,
    group: &'a CapabilityGroup,
    memory_account: Arc<dyn RuntimeFilterMemoryAccount>,
) -> AdmittedGroup<'a> {
    match gate.claim(group.key(), snapshot.version()) {
        ArtifactJobClaim::Stale => AdmittedGroup::Complete(MaterializationWorkResult {
            group: group.clone(),
            claim: MaterializationWorkClaim::Stale,
            outcome: None,
            events: Vec::new(),
        }),
        ArtifactJobClaim::Follower(follower) => AdmittedGroup::Follower {
            group: group.clone(),
            follower,
        },
        ArtifactJobClaim::Owner(owner) => {
            let identity = ArtifactMaterializationIdentity::new(
                group.common(),
                group.profile().id(),
                snapshot.version(),
            );
            let events = vec![RuntimeFilterEvent::MaterializationStarted { identity }];
            let admitted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let materialization_plan = Materializer::plan(
                    snapshot.clone(),
                    plan.schema(),
                    group.profile(),
                    plan.policy(),
                    plan.max_artifact_bytes(),
                )?;
                Ok::<_, crate::runtime_filter::materializer::MaterializationError>(
                    Materializer::admit(
                        materialization_plan,
                        plan.retained_budget(),
                        plan.scratch_budget(),
                        memory_account,
                    ),
                )
            }));
            match admitted {
                Ok(Ok(MaterializationAdmission::Ready(admitted))) => AdmittedGroup::Ready {
                    group: group.clone(),
                    owner,
                    identity,
                    admitted,
                    events,
                },
                Ok(Ok(MaterializationAdmission::Complete(outcome))) => AdmittedGroup::Complete(
                    complete_group(group.clone(), owner, identity, events, outcome),
                ),
                Ok(Err(_)) | Err(_) => AdmittedGroup::Complete(complete_group(
                    group.clone(),
                    owner,
                    identity,
                    events,
                    MaterializationOutcome::Unavailable(
                        MaterializerUnavailableReason::MaterializationFailed,
                    ),
                )),
            }
        }
    }
}

fn complete_group(
    group: CapabilityGroup,
    owner: ArtifactJobOwner,
    identity: ArtifactMaterializationIdentity,
    mut events: Vec<RuntimeFilterEvent>,
    outcome: MaterializationOutcome,
) -> MaterializationWorkResult {
    let outcome = match outcome {
        MaterializationOutcome::Published(bundle) => {
            let (kind, _) = bundle
                .artifacts()
                .first()
                .expect("materialized bundle is non-empty");
            events.push(RuntimeFilterEvent::ArtifactMaterialized {
                identity,
                kind: *kind,
                bytes: bundle.encoded_bytes(),
                digest: bundle.canonical_digest(),
            });
            ArtifactDeliveryOutcome::Published(bundle)
        }
        MaterializationOutcome::Unsupported(reason) => {
            let reason = match reason {
                MaterializerUnsupportedReason::RangeDeferred => {
                    ArtifactUnsupportedReason::RangeDeferred
                }
                MaterializerUnsupportedReason::NoAcceptedRepresentation => {
                    ArtifactUnsupportedReason::NoAcceptedRepresentation
                }
            };
            events.push(RuntimeFilterEvent::ArtifactUnsupported { identity, reason });
            ArtifactDeliveryOutcome::Unsupported(reason)
        }
        MaterializationOutcome::Unavailable(MaterializerUnavailableReason::ResourceLimit) => {
            events.push(RuntimeFilterEvent::ArtifactUnavailable {
                identity,
                reason: UnavailableReason::ResourceLimit,
            });
            ArtifactDeliveryOutcome::Unavailable(UnavailableReason::ResourceLimit)
        }
        MaterializationOutcome::Unavailable(
            MaterializerUnavailableReason::MaterializationFailed,
        ) => {
            events.push(RuntimeFilterEvent::ArtifactUnavailable {
                identity,
                reason: UnavailableReason::MaterializationFailed,
            });
            ArtifactDeliveryOutcome::Unavailable(UnavailableReason::MaterializationFailed)
        }
    };
    MaterializationWorkResult {
        group,
        claim: MaterializationWorkClaim::Owner(owner),
        outcome: Some(outcome),
        events,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use crate::runtime_filter::model::contract::{ChannelId, NullSemantics};
    use crate::runtime_filter::port::artifact::{
        ArtifactBundle, ArtifactKind, ArtifactSchemaDigest, ConsumerArtifactProfile,
        ConsumerProfileId, PhysicalArtifact,
    };
    use crate::runtime_filter::port::identity::{DeploymentEpoch, LogicalVersion};
    use crate::runtime_filter::port::producer::RuntimeContractViolationKind;
    use crate::runtime_filter::port::subscription::{
        ArtifactDeliveryOutcome, ArtifactUnsupportedReason, UnavailableReason,
    };

    use super::{ArtifactJobClaim, ArtifactPublishGate, ArtifactPublishKey, PublishCommitOutcome};

    fn bundle(version: LogicalVersion, byte: u8) -> Arc<ArtifactBundle> {
        let profile = ConsumerArtifactProfile::new(
            BTreeSet::from([ArtifactKind::ValueSet, ArtifactKind::EmptyDomain]),
            None,
        )
        .unwrap();
        let schema =
            ArtifactSchemaDigest::for_membership(&DataType::Int64, NullSemantics::NeverMatches)
                .unwrap();
        let artifact = Arc::new(PhysicalArtifact::new_test(
            ArtifactKind::ValueSet,
            schema,
            version,
            false,
            Arc::from([byte]),
        ));
        Arc::new(
            ArtifactBundle::new(
                ChannelId::new(1),
                version,
                &profile,
                vec![(ArtifactKind::ValueSet, artifact)],
                usize::MAX,
            )
            .unwrap(),
        )
    }

    fn key(profile: u8) -> ArtifactPublishKey {
        ArtifactPublishKey::new(
            ChannelId::new(1),
            DeploymentEpoch::new(2),
            ConsumerProfileId::for_test([profile; 32]),
        )
    }

    #[test]
    fn publish_gate_handles_first_stale_idempotent_conflict_and_higher() {
        let gate = ArtifactPublishGate::default();
        let key = key(3);
        let generation = gate.generation(key);
        let first = bundle(LogicalVersion::FIRST, 1);
        assert_eq!(
            gate.commit_published(key, generation, first.clone())
                .unwrap(),
            PublishCommitOutcome::Published
        );
        assert_eq!(
            gate.commit_published(key, generation, bundle(LogicalVersion::new(0), 2))
                .unwrap(),
            PublishCommitOutcome::Stale
        );
        assert_eq!(
            gate.commit_published(key, generation, first).unwrap(),
            PublishCommitOutcome::Idempotent
        );
        assert_eq!(
            gate.commit_published(key, generation, bundle(LogicalVersion::FIRST, 9))
                .unwrap_err()
                .kind(),
            RuntimeContractViolationKind::ConflictingArtifactPublish
        );
        assert_eq!(
            gate.commit_published(key, generation, bundle(LogicalVersion::new(2), 7))
                .unwrap(),
            PublishCommitOutcome::Published
        );
    }

    #[test]
    fn same_version_single_flight_reuses_success_unsupported_and_unavailable() {
        let gate = ArtifactPublishGate::default();
        let success_key = key(4);
        let ArtifactJobClaim::Owner(success_owner) = gate.claim(success_key, LogicalVersion::FIRST)
        else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(success_follower) =
            gate.claim(success_key, LogicalVersion::FIRST)
        else {
            panic!("duplicate claimant must follow");
        };
        let published = bundle(LogicalVersion::FIRST, 3);
        success_owner
            .finish(ArtifactDeliveryOutcome::Published(published.clone()))
            .unwrap();
        let ArtifactDeliveryOutcome::Published(reused) = success_follower.wait() else {
            panic!("follower must reuse published outcome");
        };
        assert!(Arc::ptr_eq(&published, &reused));

        let unavailable_key = key(5);
        let ArtifactJobClaim::Owner(owner) = gate.claim(unavailable_key, LogicalVersion::FIRST)
        else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(follower) =
            gate.claim(unavailable_key, LogicalVersion::FIRST)
        else {
            panic!("duplicate claimant must follow");
        };
        owner
            .finish(ArtifactDeliveryOutcome::Unavailable(
                UnavailableReason::ResourceLimit,
            ))
            .unwrap();
        assert!(matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Unavailable(UnavailableReason::ResourceLimit)
        ));

        let unsupported_key = key(8);
        let ArtifactJobClaim::Owner(owner) = gate.claim(unsupported_key, LogicalVersion::FIRST)
        else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(follower) =
            gate.claim(unsupported_key, LogicalVersion::FIRST)
        else {
            panic!("duplicate claimant must follow");
        };
        owner
            .finish(ArtifactDeliveryOutcome::Unsupported(
                ArtifactUnsupportedReason::NoAcceptedRepresentation,
            ))
            .unwrap();
        assert!(matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Unsupported(
                ArtifactUnsupportedReason::NoAcceptedRepresentation
            )
        ));
    }

    #[test]
    fn cancel_invalidates_generation_wakes_follower_and_rejects_late_finish() {
        let gate = ArtifactPublishGate::default();
        let key = key(6);
        let ArtifactJobClaim::Owner(owner) = gate.claim(key, LogicalVersion::FIRST) else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(follower) = gate.claim(key, LogicalVersion::FIRST) else {
            panic!("duplicate claimant must follow");
        };
        gate.cancel(key);
        assert!(matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Cancelled
        ));
        assert_eq!(
            owner
                .finish(ArtifactDeliveryOutcome::Published(bundle(
                    LogicalVersion::FIRST,
                    1
                )))
                .unwrap(),
            PublishCommitOutcome::Cancelled,
        );
    }

    #[test]
    fn conflicting_owner_finish_always_wakes_same_version_follower() {
        let gate = ArtifactPublishGate::default();
        let key = key(7);
        let ArtifactJobClaim::Owner(owner) = gate.claim(key, LogicalVersion::new(2)) else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(follower) = gate.claim(key, LogicalVersion::new(2)) else {
            panic!("duplicate claimant must follow");
        };
        let generation = gate.generation(key);
        gate.commit_published(key, generation, bundle(LogicalVersion::new(2), 1))
            .unwrap();
        let (sent, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || sent.send(follower.wait()).unwrap());

        let _ = owner.finish(ArtifactDeliveryOutcome::Published(bundle(
            LogicalVersion::new(2),
            9,
        )));
        assert!(matches!(
            received.recv_timeout(std::time::Duration::from_secs(1)),
            Ok(ArtifactDeliveryOutcome::Cancelled)
        ));
    }

    #[test]
    fn dropping_owner_and_generation_exhaustion_never_leave_followers_pending() {
        let gate = ArtifactPublishGate::default();
        let dropped_key = key(9);
        let ArtifactJobClaim::Owner(owner) = gate.claim(dropped_key, LogicalVersion::FIRST) else {
            panic!("first claimant must own the job");
        };
        let ArtifactJobClaim::Follower(follower) = gate.claim(dropped_key, LogicalVersion::FIRST)
        else {
            panic!("duplicate claimant must follow");
        };
        drop(owner);
        assert!(matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Cancelled
        ));

        let exhausted_key = key(10);
        gate.state
            .lock()
            .unwrap()
            .keys
            .entry(exhausted_key)
            .or_default()
            .generation = u64::MAX;
        gate.cancel(exhausted_key);
        gate.cancel(exhausted_key);
        assert_eq!(gate.generation(exhausted_key), u64::MAX);
        let ArtifactJobClaim::Follower(follower) = gate.claim(exhausted_key, LogicalVersion::FIRST)
        else {
            panic!("exhausted cancelled generation cannot resurrect an owner");
        };
        assert!(matches!(
            follower.wait(),
            ArtifactDeliveryOutcome::Cancelled
        ));
    }

    #[test]
    fn cancel_only_newly_terminalizes_a_pending_publish_key() {
        let gate = ArtifactPublishGate::default();
        let pending = key(11);
        assert!(gate.cancel(pending));
        assert!(!gate.cancel(pending));

        let published = key(12);
        let generation = gate.generation(published);
        assert_eq!(
            gate.commit_published(published, generation, bundle(LogicalVersion::FIRST, 1),)
                .unwrap(),
            PublishCommitOutcome::Published
        );
        assert!(!gate.cancel(published));
    }
}
