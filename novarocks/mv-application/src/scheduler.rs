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

//! Product policy and scheduling over validated lake document projections.

use std::collections::BTreeMap;

use crate::persistence::codec::{
    ConfigurationDocument, PublicationDocument, PublicationInput, RefreshPolicy, RelationOccurrence,
};
use crate::persistence::definition::MvAcceleratorSourceRevision;
use crate::persistence::projection::{
    MvDocumentProjection, MvPublicationState, StoredMvProjection,
};
use crate::persistence::validation::validate_configuration;
use crate::product::MvTarget;
use crate::scheduler_runtime::{
    MvRefreshDisposition, MvRefreshProductRuntime, MvRefreshRuntimeDecision,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvSchedulerSemanticDecision {
    Paused,
    Manual,
    IntervalNotDue { eligible_at_ms: u64 },
    IntervalDue,
    OnChangeNotDue,
    OnChangeDue,
    Invalid { reason: String },
}

/// Interpret C and the exact publication facts retained in a validated root.
/// Current inputs are addressed by D occurrence, not FQN or a numeric snapshot.
pub fn mv_scheduler_semantic_decision(
    facts: &MvDocumentProjection,
    now_ms: i64,
    current_inputs: Option<&[PublicationInput]>,
) -> MvSchedulerSemanticDecision {
    if let Some(inputs) = current_inputs {
        if let Err(reason) =
            validate_current_inputs(&facts.definition().relation_occurrences, inputs)
        {
            return MvSchedulerSemanticDecision::Invalid { reason };
        }
    }
    let publication = match facts.publication() {
        MvPublicationState::NeverPublished => None,
        MvPublicationState::Published(published) => Some(published.document()),
    };
    semantic_decision(facts.configuration(), publication, now_ms, current_inputs)
}

fn semantic_decision(
    configuration: &ConfigurationDocument,
    publication: Option<&PublicationDocument>,
    now_ms: i64,
    current_inputs: Option<&[PublicationInput]>,
) -> MvSchedulerSemanticDecision {
    if let Err(error) = validate_configuration(configuration) {
        return MvSchedulerSemanticDecision::Invalid {
            reason: error.to_string(),
        };
    }
    if configuration.paused {
        return MvSchedulerSemanticDecision::Paused;
    }
    let Ok(now_ms) = u64::try_from(now_ms) else {
        return MvSchedulerSemanticDecision::Invalid {
            reason: "MV scheduler time must not be negative".to_string(),
        };
    };
    match configuration.refresh_policy {
        RefreshPolicy::Manual => MvSchedulerSemanticDecision::Manual,
        RefreshPolicy::AsyncInterval => {
            let Some(publication) = publication else {
                return MvSchedulerSemanticDecision::IntervalDue;
            };
            // This is P's frozen publication-fact time, not a claimed exact
            // provider commit completion time or an Accelerator insertion time.
            let eligible_at_ms = publication.publication_prepared_at_ms.saturating_add(
                configuration
                    .refresh_interval_ms
                    .expect("validated interval"),
            );
            if now_ms >= eligible_at_ms {
                MvSchedulerSemanticDecision::IntervalDue
            } else {
                MvSchedulerSemanticDecision::IntervalNotDue { eligible_at_ms }
            }
        }
        RefreshPolicy::AsyncOnChange => {
            let Some(current_inputs) = current_inputs else {
                return MvSchedulerSemanticDecision::Invalid {
                    reason: "ASYNC_ON_CHANGE requires exact current source occurrences".to_string(),
                };
            };
            if publication
                .is_some_and(|published| exact_inputs_match(&published.inputs, current_inputs))
            {
                MvSchedulerSemanticDecision::OnChangeNotDue
            } else {
                MvSchedulerSemanticDecision::OnChangeDue
            }
        }
    }
}

fn validate_current_inputs(
    occurrences: &[RelationOccurrence],
    inputs: &[PublicationInput],
) -> Result<(), String> {
    let expected = occurrences
        .iter()
        .map(|item| (item.occurrence_id, item))
        .collect::<BTreeMap<_, _>>();
    if expected.len() != occurrences.len() || inputs.len() != occurrences.len() {
        return Err("MV current inputs do not cover every definition occurrence".to_string());
    }
    let mut seen = std::collections::BTreeSet::new();
    for input in inputs {
        let occurrence = expected
            .get(&input.relation_occurrence_id)
            .ok_or_else(|| "MV current input names an unknown definition occurrence".to_string())?;
        if !seen.insert(input.relation_occurrence_id) {
            return Err("MV current inputs repeat a definition occurrence".to_string());
        }
        if input.object_id != occurrence.object_id {
            return Err("MV current input belongs to a replaced source object".to_string());
        }
    }
    Ok(())
}

fn exact_inputs_match(published: &[PublicationInput], current: &[PublicationInput]) -> bool {
    let by_occurrence = current
        .iter()
        .map(|input| (input.relation_occurrence_id, input))
        .collect::<BTreeMap<_, _>>();
    let published_ids = published
        .iter()
        .map(|input| input.relation_occurrence_id)
        .collect::<std::collections::BTreeSet<_>>();
    published.len() == current.len()
        && by_occurrence.len() == current.len()
        && published_ids.len() == published.len()
        && published
            .iter()
            .all(|input| by_occurrence.get(&input.relation_occurrence_id).copied() == Some(input))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MvScheduledRefreshReason {
    Interval,
    SourceChange,
}

#[derive(Clone, Debug)]
pub struct MvScheduledRefreshRequest {
    projection: StoredMvProjection,
    target: MvTarget,
    reason: MvScheduledRefreshReason,
}

impl MvScheduledRefreshRequest {
    pub fn projection(&self) -> &StoredMvProjection {
        &self.projection
    }
    pub fn target(&self) -> &MvTarget {
        &self.target
    }
    pub const fn reason(&self) -> MvScheduledRefreshReason {
        self.reason
    }
}

/// Frozen D/C/P facts whose Current source observation is still outstanding.
pub struct MvCurrentInputObservation {
    projection: StoredMvProjection,
    target: MvTarget,
}

impl MvCurrentInputObservation {
    pub fn target(&self) -> &MvTarget {
        &self.target
    }
    pub fn projection(&self) -> &StoredMvProjection {
        &self.projection
    }
    pub fn occurrences(&self) -> &[RelationOccurrence] {
        &self.projection.facts.definition().relation_occurrences
    }
}

#[derive(Debug)]
pub struct MvRefreshScheduler {
    runtime: MvRefreshProductRuntime<i64, MvAcceleratorSourceRevision, MvScheduledRefreshRequest>,
}

impl MvRefreshScheduler {
    pub fn new(config: MvSchedulerConfig) -> Self {
        Self {
            runtime: MvRefreshProductRuntime::new(config),
        }
    }
    pub const fn enabled(&self) -> bool {
        self.runtime.enabled()
    }

    pub fn observe_projection(
        &mut self,
        projection: StoredMvProjection,
        now_ms: i64,
    ) -> Option<MvCurrentInputObservation> {
        if !self.runtime.enabled()
            || !self.runtime.begin_observation(
                projection.mv_id,
                projection.facts.source_revision().clone(),
                now_ms,
            )
            || projection.facts.configuration().paused
        {
            return None;
        }
        let target = &projection.facts.source_revision().target;
        let target = MvTarget::from_parts(
            Some(target.instance_id.as_str()),
            &target.namespace,
            &target.table,
        );
        if matches!(
            projection.facts.configuration().refresh_policy,
            RefreshPolicy::AsyncOnChange
        ) {
            return Some(MvCurrentInputObservation { projection, target });
        }
        self.apply_semantic_decision(projection, target, None, now_ms);
        None
    }

    pub fn resolve_current_inputs(
        &mut self,
        observation: MvCurrentInputObservation,
        current_inputs: Vec<PublicationInput>,
        now_ms: i64,
    ) {
        self.apply_semantic_decision(
            observation.projection,
            observation.target,
            Some(&current_inputs),
            now_ms,
        );
    }

    pub fn record_observation_failure(
        &mut self,
        observation: &MvCurrentInputObservation,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) {
        if self.runtime.is_current_source(
            &observation.projection.mv_id,
            observation.projection.facts.source_revision(),
        ) {
            let _ = self
                .runtime
                .record(&observation.projection.mv_id, disposition, now_ms);
        }
    }

    pub fn take_ready(&mut self) -> Vec<MvScheduledRefreshRequest> {
        self.runtime.take_ready()
    }
    pub fn mark_started(&mut self, mv_id: i64) -> bool {
        self.runtime.mark_started(&mv_id)
    }
    pub fn requeue(&mut self, request: MvScheduledRefreshRequest) {
        self.runtime.requeue(request.projection.mv_id, request);
    }
    pub fn complete(
        &mut self,
        request: &MvScheduledRefreshRequest,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        self.runtime
            .complete(&request.projection.mv_id, disposition, now_ms)
    }
    pub fn record(
        &mut self,
        mv_id: i64,
        disposition: MvRefreshDisposition,
        now_ms: i64,
    ) -> MvRefreshRuntimeDecision {
        self.runtime.record(&mv_id, disposition, now_ms)
    }

    fn apply_semantic_decision(
        &mut self,
        projection: StoredMvProjection,
        target: MvTarget,
        current_inputs: Option<&[PublicationInput]>,
        now_ms: i64,
    ) {
        if !self
            .runtime
            .is_current_source(&projection.mv_id, projection.facts.source_revision())
        {
            return;
        }
        let reason = match mv_scheduler_semantic_decision(&projection.facts, now_ms, current_inputs)
        {
            MvSchedulerSemanticDecision::IntervalDue => MvScheduledRefreshReason::Interval,
            MvSchedulerSemanticDecision::OnChangeDue => MvScheduledRefreshReason::SourceChange,
            MvSchedulerSemanticDecision::Paused
            | MvSchedulerSemanticDecision::Manual
            | MvSchedulerSemanticDecision::IntervalNotDue { .. }
            | MvSchedulerSemanticDecision::OnChangeNotDue => return,
            MvSchedulerSemanticDecision::Invalid { reason } => {
                self.runtime.record(
                    &projection.mv_id,
                    MvRefreshDisposition::InvalidDefinition(reason),
                    now_ms,
                );
                return;
            }
        };
        self.runtime.enqueue(
            projection.mv_id,
            MvScheduledRefreshRequest {
                projection,
                target,
                reason,
            },
        );
    }
}

/// Frozen process-local policy for asynchronous materialized-view refresh.
///
/// It does not own a queue, thread, provider handle, or persisted record. The
/// role-local scheduler owns those resources and consumes these product bounds
/// after configuration has been resolved once at startup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvSchedulerConfig {
    enabled: bool,
    tick_interval_ms: u64,
    max_concurrent_refreshes: usize,
    failure_backoff_ms: i64,
    max_failure_backoff_ms: i64,
}

impl MvSchedulerConfig {
    pub const fn new(
        enabled: bool,
        tick_interval_ms: u64,
        max_concurrent_refreshes: usize,
        failure_backoff_ms: i64,
        max_failure_backoff_ms: i64,
    ) -> Self {
        Self {
            enabled,
            tick_interval_ms,
            max_concurrent_refreshes,
            failure_backoff_ms,
            max_failure_backoff_ms,
        }
    }

    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    pub const fn tick_interval_ms(&self) -> u64 {
        self.tick_interval_ms
    }

    pub const fn max_concurrent_refreshes(&self) -> usize {
        self.max_concurrent_refreshes
    }

    pub const fn failure_backoff_ms(&self) -> i64 {
        self.failure_backoff_ms
    }

    pub const fn max_failure_backoff_ms(&self) -> i64 {
        self.max_failure_backoff_ms
    }
}

impl Default for MvSchedulerConfig {
    fn default() -> Self {
        Self::new(false, 30_000, 1, 60_000, 1_800_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::codec::{PublicationKind, PublicationOutput, PublicationStatistics};
    use crate::persistence::identity::{
        DocumentRevision, NativeDataVersion, ObjectIdentity, PublicationIdentity, SchemaVersion,
    };
    use crate::persistence::test_support::ProjectionFixture;

    fn stored(mv_id: i64, fixture: ProjectionFixture) -> StoredMvProjection {
        StoredMvProjection {
            mv_id,
            facts: fixture.build().expect("valid scheduler projection"),
        }
    }

    fn target() -> MvTarget {
        MvTarget::from_parts(Some("ice"), "sales", "mv")
    }

    fn published_inputs(projection: &StoredMvProjection) -> Vec<PublicationInput> {
        match projection.facts.publication() {
            MvPublicationState::Published(published) => published.document().inputs.clone(),
            MvPublicationState::NeverPublished => panic!("test projection must be published"),
        }
    }

    fn configuration(policy: RefreshPolicy) -> ConfigurationDocument {
        ConfigurationDocument {
            refresh_policy: policy,
            paused: false,
            refresh_interval_ms: matches!(policy, RefreshPolicy::AsyncInterval).then_some(100),
            max_staleness_ms: None,
        }
    }
    fn input(id: u32, object: &[u8], version: &[u8]) -> PublicationInput {
        PublicationInput {
            relation_occurrence_id: id,
            object_id: ObjectIdentity::try_new(object.to_vec()).unwrap(),
            native_data_version: NativeDataVersion::try_new(version.to_vec()).unwrap(),
        }
    }
    fn publication(inputs: Vec<PublicationInput>) -> PublicationDocument {
        PublicationDocument {
            publication_id: PublicationIdentity::try_new(vec![1]).unwrap(),
            definition_revision: DocumentRevision::from_canonical_bytes(b"D"),
            interpretation_revision: DocumentRevision::from_canonical_bytes(b"L"),
            publication_prepared_at_ms: 1_000,
            inputs,
            output: PublicationOutput {
                object_id: ObjectIdentity::try_new(b"target".to_vec()).unwrap(),
                empty_result: false,
            },
            kind: PublicationKind::FullRefresh,
            statistics: PublicationStatistics::default(),
        }
    }
    fn occurrence(id: u32) -> RelationOccurrence {
        RelationOccurrence {
            occurrence_id: id,
            catalog_at_binding: "iceberg".to_string(),
            namespace_at_binding: "db".to_string(),
            relation_at_binding: "base".to_string(),
            qualifier_at_binding: "base".to_string(),
            object_id: ObjectIdentity::try_new(b"source".to_vec()).unwrap(),
            schema_version: SchemaVersion::try_new(vec![1]).unwrap(),
            fields: Vec::new(),
        }
    }

    #[test]
    fn default_policy_preserves_the_deployed_scheduler_bounds() {
        assert_eq!(
            MvSchedulerConfig::default(),
            MvSchedulerConfig::new(false, 30_000, 1, 60_000, 1_800_000)
        );
    }
    #[test]
    fn interval_uses_published_fact_time_not_accelerator_insertion_time() {
        let c = configuration(RefreshPolicy::AsyncInterval);
        let p = publication(vec![input(7, b"source", b"v1")]);
        assert_eq!(
            semantic_decision(&c, Some(&p), 1_099, None),
            MvSchedulerSemanticDecision::IntervalNotDue {
                eligible_at_ms: 1_100
            }
        );
        assert_eq!(
            semantic_decision(&c, Some(&p), 1_100, None),
            MvSchedulerSemanticDecision::IntervalDue
        );
        assert_eq!(
            semantic_decision(&c, None, 1_099, None),
            MvSchedulerSemanticDecision::IntervalDue
        );
    }
    #[test]
    fn on_change_compares_each_occurrence_and_complete_opaque_revision() {
        let c = configuration(RefreshPolicy::AsyncOnChange);
        let inputs = vec![input(7, b"source", b"v1"), input(42, b"source", b"v2")];
        let p = publication(inputs.clone());
        let mut reordered = inputs.clone();
        reordered.reverse();
        assert_eq!(
            semantic_decision(&c, Some(&p), 2_000, Some(&reordered)),
            MvSchedulerSemanticDecision::OnChangeNotDue
        );
        for changed in [
            vec![input(7, b"source", b"v2"), input(42, b"source", b"v1")],
            vec![input(7, b"replacement", b"v1"), inputs[1].clone()],
            vec![inputs[0].clone()],
        ] {
            assert_eq!(
                semantic_decision(&c, Some(&p), 2_000, Some(&changed)),
                MvSchedulerSemanticDecision::OnChangeDue
            );
        }
    }
    #[test]
    fn observations_require_every_occurrence_without_fqn_deduplication() {
        let definition = vec![occurrence(7), occurrence(42)];
        let current = vec![input(42, b"source", b"v2"), input(7, b"source", b"v1")];
        assert!(validate_current_inputs(&definition, &current).is_ok());
        for invalid in [
            vec![current[0].clone()],
            vec![current[0].clone(), current[0].clone()],
            vec![current[0].clone(), input(99, b"source", b"v1")],
            vec![current[0].clone(), input(7, b"replacement", b"v1")],
        ] {
            assert!(validate_current_inputs(&definition, &invalid).is_err());
        }
    }
    #[test]
    fn paused_manual_and_missing_observation_are_explicit() {
        let mut paused = configuration(RefreshPolicy::AsyncInterval);
        paused.paused = true;
        assert_eq!(
            semantic_decision(&paused, None, 100, None),
            MvSchedulerSemanticDecision::Paused
        );
        assert_eq!(
            semantic_decision(&configuration(RefreshPolicy::Manual), None, 100, None),
            MvSchedulerSemanticDecision::Manual
        );
        assert!(matches!(
            semantic_decision(
                &configuration(RefreshPolicy::AsyncOnChange),
                None,
                100,
                None
            ),
            MvSchedulerSemanticDecision::Invalid { .. }
        ));
        assert_eq!(
            semantic_decision(
                &configuration(RefreshPolicy::AsyncOnChange),
                None,
                100,
                Some(&[input(7, b"source", b"empty-version")])
            ),
            MvSchedulerSemanticDecision::OnChangeDue
        );
    }
    #[test]
    fn duplicate_inputs_never_equal_a_publication() {
        let expected = vec![input(7, b"source", b"v1"), input(42, b"source", b"v2")];
        assert!(!exact_inputs_match(
            &expected,
            &[expected[0].clone(), expected[0].clone()]
        ));
    }

    #[test]
    fn concrete_interval_scheduler_consumes_the_validated_projection() {
        let projection = stored(7, ProjectionFixture::new(target(), Some(11)));
        let mut scheduler =
            MvRefreshScheduler::new(MvSchedulerConfig::new(true, 30_000, 1, 10, 40));

        assert!(
            scheduler
                .observe_projection(projection, 1_700_000_061_000)
                .is_none()
        );
        let ready = scheduler.take_ready();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].projection().mv_id, 7);
        assert_eq!(ready[0].target(), &target());
    }

    #[test]
    fn a_stale_observation_failure_cannot_block_a_newer_configuration() {
        let mut first = ProjectionFixture::new(target(), Some(11));
        first.configuration = configuration(RefreshPolicy::AsyncOnChange);
        let first = stored(7, first);
        let mut second = ProjectionFixture::new(target(), Some(11));
        second.configuration = configuration(RefreshPolicy::AsyncOnChange);
        second.configuration.max_staleness_ms = Some(1_000);
        let second = stored(7, second);
        let current = published_inputs(&second);
        let mut scheduler =
            MvRefreshScheduler::new(MvSchedulerConfig::new(true, 30_000, 1, 10, 40));

        let stale = scheduler
            .observe_projection(first, 1_700_000_002_000)
            .expect("first observation");
        let current_observation = scheduler
            .observe_projection(second, 1_700_000_002_000)
            .expect("new configuration observation");
        scheduler.record_observation_failure(
            &stale,
            MvRefreshDisposition::TerminalFailure("late failure".to_string()),
            1_700_000_002_000,
        );
        let mut changed = current;
        changed[0].native_data_version = NativeDataVersion::try_new(b"changed".to_vec()).unwrap();
        scheduler.resolve_current_inputs(current_observation, changed, 1_700_000_002_000);

        assert_eq!(scheduler.take_ready().len(), 1);
    }
    #[test]
    fn concrete_scheduler_has_explicit_terminal_decisions() {
        let mut scheduler =
            MvRefreshScheduler::new(MvSchedulerConfig::new(true, 30_000, 1, 10, 40));
        assert_eq!(
            scheduler.record(7, MvRefreshDisposition::Completed, 100),
            MvRefreshRuntimeDecision::Success
        );
        assert_eq!(
            scheduler.record(
                7,
                MvRefreshDisposition::TransientUnavailable("offline".to_string()),
                100
            ),
            MvRefreshRuntimeDecision::TransientBackoff {
                error: "offline".to_string(),
                retry_at_ms: 110
            }
        );
        for disposition in [
            MvRefreshDisposition::InvalidDefinition("bad".to_string()),
            MvRefreshDisposition::TerminalFailure("terminal".to_string()),
            MvRefreshDisposition::Corruption("corrupt".to_string()),
            MvRefreshDisposition::InvariantViolation("invariant".to_string()),
        ] {
            assert!(matches!(
                scheduler.record(7, disposition, 100),
                MvRefreshRuntimeDecision::Blocked { .. }
            ));
        }
    }
}
