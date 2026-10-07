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

//! MV-owned eligibility policy on exact, opaque Connector document CAS.

use novarocks_mv_application::persistence::codec::{DefinitionDocument, PublicationDocument};
use novarocks_mv_application::persistence::definition::MvAcceleratorSourceRevision;
use novarocks_mv_application::persistence::documents::MvObservedCurrentDocuments;
use novarocks_mv_application::persistence::eligibility::{
    EligibilityBinding, EligibilityDocument, EligibilityEvidence, EligibilityState,
    native_result_content_v1_identity,
};
use novarocks_mv_application::persistence::identity::NativeDataVersion;
use novarocks_mv_application::persistence::projection::{MvDocumentProjection, MvPublicationState};
use novarocks_spi::connector::document_storage::{
    ConnectorDocumentManagementObservation, ConnectorDocumentObservationRequest,
    ConnectorDocumentStorageBudget, ConnectorDocumentStorageLimits,
};
use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorRequestContext, ConnectorTableObjectCaptureRequest,
    ConnectorTableObjectSelector, ConnectorTableResolution,
};
use novarocks_types::QueryExecutionId;

#[derive(Debug)]
pub(crate) enum ValidationUpdateError {
    ValidationConflict(String),
    KnownUncommitted(String),
    CommitUnknown(String),
    CommittedProjectionFailed(String),
}

impl From<String> for ValidationUpdateError {
    fn from(message: String) -> Self {
        Self::ValidationConflict(message)
    }
}
impl From<&str> for ValidationUpdateError {
    fn from(message: &str) -> Self {
        Self::ValidationConflict(message.into())
    }
}
impl std::fmt::Display for ValidationUpdateError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::ValidationConflict(message)
            | Self::KnownUncommitted(message)
            | Self::CommitUnknown(message)
            | Self::CommittedProjectionFailed(message) => message,
        };
        formatter.write_str(message)
    }
}
impl std::error::Error for ValidationUpdateError {}

pub(crate) enum ValidationCompletion<'a> {
    Invalid {
        execution_id: QueryExecutionId,
        evidence: EligibilityEvidence,
    },
    VerifiedUncommitted {
        verification: &'a crate::task_execution::execution::ReleasedVerificationFacts,
    },
}

pub(crate) fn observe_current_eligibility_for_projection(
    planning: &ConnectorControlPlanningLease,
    projection: &MvDocumentProjection,
    context: &ConnectorRequestContext,
) -> Result<Option<EligibilityDocument>, String> {
    let source = projection.source_revision();
    let (_, documents) = observe_current(planning, source, context)?;
    if documents.eligibility_revision() != source.eligibility_revision {
        return Err("MV eligibility changed since its Current projection".into());
    }
    Ok(documents.eligibility().cloned())
}

/// A single-use handoff of the same Current that admitted a metadata transition.
/// It performs no provider read and acquires no management turn.
pub(crate) struct FrozenEligibilityProjectionSource {
    observation: std::sync::Mutex<
        Option<novarocks_mv_application::readiness::MvCurrentProjectionObservation>,
    >,
}

impl FrozenEligibilityProjectionSource {
    pub(crate) fn new(
        observation: novarocks_mv_application::readiness::MvCurrentProjectionObservation,
    ) -> Self {
        Self {
            observation: std::sync::Mutex::new(Some(observation)),
        }
    }
}

#[async_trait::async_trait]
impl novarocks_mv_application::readiness::MvCurrentProjectionSource
    for FrozenEligibilityProjectionSource
{
    async fn observe(
        &self,
        _request: &novarocks_mv_application::readiness::MvCurrentProjectionRequest,
    ) -> Result<
        novarocks_mv_application::readiness::MvCurrentProjectionObservation,
        novarocks_mv_application::readiness::MvProjectionError,
    > {
        self.observation
            .lock()
            .map_err(|_| {
                novarocks_mv_application::readiness::MvProjectionError::new(
                    novarocks_mv_application::readiness::MvProjectionErrorKind::SourceConflict,
                    "MV eligibility projection handoff is poisoned",
                )
            })?
            .take()
            .ok_or_else(|| {
                novarocks_mv_application::readiness::MvProjectionError::new(
                    novarocks_mv_application::readiness::MvProjectionErrorKind::SourceConflict,
                    "MV eligibility projection handoff was already consumed",
                )
            })
    }
}

pub(crate) fn observe_current(
    planning: &ConnectorControlPlanningLease,
    source: &MvAcceleratorSourceRevision,
    context: &ConnectorRequestContext,
) -> Result<
    (
        ConnectorDocumentManagementObservation,
        MvObservedCurrentDocuments,
    ),
    String,
> {
    let context = context.clone().after_external_effect();
    let binding = planning
        .binding()
        .metadata()
        .capture_table_object_binding(ConnectorTableObjectCaptureRequest {
            table: source.target.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            selector: ConnectorTableObjectSelector::Current,
            context: context.clone(),
        })
        .map_err(|error| format!("observe MV eligibility target: {error}"))?;
    if binding.metadata.identity != source.target || binding.object_id != source.target_object_id {
        return Err("MV eligibility target object changed".into());
    }
    let lease = planning
        .derive_document_storage_lease()
        .map_err(|error| format!("derive MV eligibility document lease: {error}"))?;
    let request = ConnectorDocumentObservationRequest::try_new(
        lease.owner().clone(),
        lease.catalog_handle().clone(),
        source.target.clone(),
        binding.object_id,
        ConnectorDocumentStorageBudget::new(ConnectorDocumentStorageLimits::spec_default()),
        context,
    )
    .map_err(|error| format!("prepare MV eligibility observation: {error}"))?;
    let (observation, documents) =
        novarocks_mv_application::persistence::documents::observe_current_management_document_set(
            &lease,
            request,
            novarocks_mv_application::persistence::validation::PersistenceDecodeBudget::default(),
        )
        .map_err(|error| format!("decode Current MV eligibility: {error}"))?
        .into_parts();
    if documents.definition_revision() != source.definition_revision
        || documents.interpretation_revision() != source.interpretation_revision
        || documents.publication_revision() != source.publication_revision
        || observation.marker().owner() != source.deployment_owner.as_str()
        || observation.marker().incarnation() != source.process_incarnation.as_str()
    {
        return Err("MV eligibility dependencies or ownership changed".into());
    }
    Ok((observation, documents))
}

pub(crate) fn published_binding(
    definition: &DefinitionDocument,
    publication: &PublicationDocument,
    generation: u64,
) -> Result<EligibilityBinding, String> {
    let revision = novarocks_mv_application::persistence::codec::encode_publication(publication)
        .map_err(|error| format!("encode MV eligibility publication: {error}"))?
        .revision();
    Ok(EligibilityBinding {
        object_id: publication.output.object_id.clone(),
        publication_id: publication.publication_id.clone(),
        publication_revision: revision,
        computation_identity: definition.computation_identity,
        content_contract_identity: native_result_content_v1_identity(),
        generation,
    })
}

pub(crate) fn require_eligible(
    projection: &MvDocumentProjection,
    eligibility: Option<&EligibilityDocument>,
) -> Result<(), String> {
    if !projection.interpretation().aggregates.is_empty() {
        return Ok(());
    }
    let MvPublicationState::Published(publication) = projection.publication() else {
        return Err("MV has no published incremental baseline; a full refresh is required".into());
    };
    require_publication_eligible(projection.definition(), publication.document(), eligibility)
}

fn require_publication_eligible(
    definition: &DefinitionDocument,
    publication: &PublicationDocument,
    eligibility: Option<&EligibilityDocument>,
) -> Result<(), String> {
    let eligibility = eligibility.ok_or_else(|| {
        "MV published baseline has no eligibility fact; REFRESH FULL is required".to_string()
    })?;
    let binding = published_binding(definition, publication, eligibility.binding.generation)?;
    if !eligibility.is_eligible_for(&binding) {
        return Err(
            "MV baseline is pending validation or invalid; REFRESH FULL is required".into(),
        );
    }
    Ok(())
}

pub(crate) fn for_publication(
    existing: Option<&EligibilityDocument>,
    definition: &DefinitionDocument,
    publication: &PublicationDocument,
    kind: novarocks_mv_application::persistence::codec::PublicationKind,
    repartitioned: bool,
    validation_attempt: Option<QueryExecutionId>,
) -> Result<EligibilityDocument, String> {
    use novarocks_mv_application::persistence::codec::PublicationKind;
    if !repartitioned && kind != PublicationKind::FullRefresh {
        match existing.map(|eligibility| &eligibility.state) {
            Some(EligibilityState::Eligible) => {}
            Some(EligibilityState::ValidationPending { attempt, .. })
                if kind == PublicationKind::IncrementalRefresh
                    && validation_attempt == Some(*attempt) => {}
            _ => {
                return Err(
                    "MV publication cannot discharge an unowned pending or invalid baseline".into(),
                );
            }
        }
    }
    if let Some(existing) = existing {
        if repartitioned && existing.state != EligibilityState::Eligible {
            return Ok(existing.clone());
        }
    }
    Ok(EligibilityDocument {
        binding: published_binding(
            definition,
            publication,
            existing.map_or(Ok(1), |existing| {
                existing
                    .binding
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| "MV eligibility generation overflow".to_string())
            })?,
        )?,
        state: EligibilityState::Eligible,
    })
}

pub(crate) fn with_eligibility(
    set: novarocks_spi::connector::document_storage::ConnectorDocumentSet,
    eligibility: &EligibilityDocument,
) -> Result<novarocks_spi::connector::document_storage::ConnectorDocumentSet, String> {
    let mut documents = set.documents().to_vec();
    documents.extend(
        novarocks_mv_application::persistence::documents::eligibility_document_set(eligibility)
            .map_err(|error| format!("encode MV publication eligibility: {error}"))?
            .documents()
            .iter()
            .cloned(),
    );
    novarocks_spi::connector::document_storage::ConnectorDocumentSet::try_new(documents)
        .map_err(|error| format!("bind atomic MV publication eligibility: {error}"))
}

pub(crate) fn pending(
    eligibility: &EligibilityDocument,
    execution_id: QueryExecutionId,
    target_snapshot: i64,
) -> Result<EligibilityDocument, String> {
    if target_snapshot <= 0 {
        return Err("MV deletion validation requires an exact positive target snapshot".into());
    }
    if eligibility.state != EligibilityState::Eligible {
        return Err("MV deletion validation requires an Eligible baseline".into());
    }
    let generation = eligibility
        .binding
        .generation
        .checked_add(1)
        .ok_or_else(|| "MV eligibility generation overflow".to_string())?;
    let mut next = eligibility.clone();
    next.binding.generation = generation;
    next.state = EligibilityState::ValidationPending {
        attempt: execution_id,
        target_snapshot: NativeDataVersion::try_new(target_snapshot.to_be_bytes().to_vec())
            .map_err(|error| format!("bind MV validation target snapshot: {error}"))?,
    };
    next.validate().map_err(|error| error.to_string())?;
    Ok(next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_mv_application::persistence::identity::{
        ComputationIdentity, DocumentRevision, ObjectIdentity, PublicationIdentity,
    };
    use novarocks_types::{AttemptId, QueryId};

    fn eligible() -> EligibilityDocument {
        EligibilityDocument {
            binding: EligibilityBinding {
                object_id: ObjectIdentity::try_new(vec![1]).unwrap(),
                publication_id: PublicationIdentity::try_new(vec![2]).unwrap(),
                publication_revision: DocumentRevision::from_canonical_bytes(b"P"),
                computation_identity: ComputationIdentity::from_canonical_bytes(b"D"),
                content_contract_identity: native_result_content_v1_identity(),
                generation: 1,
            },
            state: EligibilityState::Eligible,
        }
    }
    fn attempt(sequence: i64) -> QueryExecutionId {
        QueryExecutionId::new(QueryId::new(1, sequence), AttemptId::new(1).unwrap()).unwrap()
    }

    #[test]
    fn validation_fence_retains_exact_publication_and_attempt_snapshot() {
        let previous = eligible();
        let next = pending(&previous, attempt(2), 97).unwrap();
        let mut expected = previous.binding.clone();
        expected.generation = 2;
        assert_eq!(next.binding, expected);
        assert_eq!(
            next.state,
            EligibilityState::ValidationPending {
                attempt: attempt(2),
                target_snapshot: NativeDataVersion::try_new(97_i64.to_be_bytes().to_vec()).unwrap(),
            }
        );
        assert!(pending(&next, attempt(3), 97).is_err());
        assert!(pending(&previous, attempt(2), 0).is_err());
    }

    #[test]
    fn invalid_completion_requires_exact_attempt_and_positive_bounded_shortage() {
        let fence = pending(&eligible(), attempt(2), 97).unwrap();
        let evidence = EligibilityEvidence {
            requested: 4,
            matched: 3,
            samples: vec![vec![7]],
        };
        assert!(
            completed(
                &fence,
                ValidationCompletion::Invalid {
                    execution_id: attempt(3),
                    evidence: evidence.clone()
                }
            )
            .is_err()
        );
        let invalid = completed(
            &fence,
            ValidationCompletion::Invalid {
                execution_id: attempt(2),
                evidence,
            },
        )
        .unwrap();
        assert_eq!(invalid.binding.generation, 3);
        assert!(matches!(invalid.state, EligibilityState::Invalid { .. }));
        assert!(
            completed(
                &fence,
                ValidationCompletion::Invalid {
                    execution_id: attempt(2),
                    evidence: EligibilityEvidence {
                        requested: 4,
                        matched: 4,
                        samples: vec![]
                    }
                }
            )
            .is_err()
        );
        assert!(
            completed(
                &fence,
                ValidationCompletion::Invalid {
                    execution_id: attempt(2),
                    evidence: EligibilityEvidence {
                        requested: 4,
                        matched: 3,
                        samples: vec![vec![0; 257]]
                    }
                }
            )
            .is_err()
        );
    }

    #[test]
    fn validation_generation_is_checked() {
        let mut baseline = eligible();
        baseline.binding.generation = u64::MAX;
        assert!(pending(&baseline, attempt(2), 97).is_err());
        baseline.binding.generation -= 1;
        let fence = pending(&baseline, attempt(2), 97).unwrap();
        assert!(
            completed(
                &fence,
                ValidationCompletion::Invalid {
                    execution_id: attempt(2),
                    evidence: EligibilityEvidence {
                        requested: 2,
                        matched: 1,
                        samples: vec![]
                    }
                }
            )
            .is_err()
        );
    }
    #[test]
    fn publication_requires_exact_eligible_binding_and_preserves_management_blocks() {
        use novarocks_mv_application::persistence::{
            codec::{ExpressionKind, PhysicalFieldLogicalIdentity, PublicationKind},
            test_support::ProjectionFixture,
        };
        let mut fixture = ProjectionFixture::new(
            novarocks_mv_application::product::MvTarget::from_parts(Some("iceberg"), "db", "mv"),
            Some(97),
        );
        fixture.definition.query.effective_sql = "SELECT o.amount FROM ice.sales.orders o UNION ALL SELECT o2.amount FROM ice.sales.orders o2".into();
        fixture.definition.outputs[0].name = "amount".into();
        fixture.definition.outputs[0].expression.kind = ExpressionKind::Field;
        fixture.definition.outputs[0].expression.function_identity = None;
        fixture.interpretation.aggregates.clear();
        fixture.interpretation.state_slots.clear();
        fixture.interpretation.apply_key = None;
        fixture.interpretation.branches.clear();
        fixture.interpretation.target.fields.retain(|field| {
            matches!(
                field.logical_identity,
                PhysicalFieldLogicalIdentity::Output(_)
            )
        });
        let projection = fixture.build().unwrap();
        let MvPublicationState::Published(published) = projection.publication() else {
            panic!("fixture publication")
        };
        let publication = published.document();
        let baseline = EligibilityDocument {
            binding: published_binding(projection.definition(), publication, 1).unwrap(),
            state: EligibilityState::Eligible,
        };
        assert!(
            require_publication_eligible(projection.definition(), publication, Some(&baseline))
                .is_ok()
        );
        let mut new_publication = publication.clone();
        new_publication.publication_id = PublicationIdentity::try_new(vec![99]).unwrap();
        assert!(
            require_publication_eligible(
                projection.definition(),
                &new_publication,
                Some(&baseline)
            )
            .is_err()
        );
        let fenced = pending(&baseline, attempt(2), 97).unwrap();
        let invalid = completed(
            &fenced,
            ValidationCompletion::Invalid {
                execution_id: attempt(2),
                evidence: EligibilityEvidence {
                    requested: 4,
                    matched: 3,
                    samples: vec![],
                },
            },
        )
        .unwrap();
        for blocked in [&fenced, &invalid] {
            let preserved = for_publication(
                Some(blocked),
                projection.definition(),
                &new_publication,
                PublicationKind::FullRefresh,
                true,
                None,
            )
            .unwrap();
            assert_eq!(&preserved, blocked);
            assert!(
                require_publication_eligible(
                    projection.definition(),
                    &new_publication,
                    Some(&preserved)
                )
                .is_err()
            );
            let publication_set =
                novarocks_mv_application::persistence::documents::publication_document_set(
                    projection.definition(),
                    projection.interpretation(),
                    &new_publication,
                )
                .unwrap();
            let set = with_eligibility(publication_set, &preserved).unwrap();
            assert_eq!(set.documents().len(), 2);
        }
        assert!(
            for_publication(
                Some(&fenced),
                projection.definition(),
                &new_publication,
                PublicationKind::IncrementalRefresh,
                false,
                Some(attempt(3))
            )
            .is_err()
        );
        let committed = for_publication(
            Some(&fenced),
            projection.definition(),
            &new_publication,
            PublicationKind::IncrementalRefresh,
            false,
            Some(attempt(2)),
        )
        .unwrap();
        assert!(
            require_publication_eligible(
                projection.definition(),
                &new_publication,
                Some(&committed)
            )
            .is_ok()
        );
        assert_eq!(committed.binding.generation, 3);
    }

    #[test]
    fn rollback_requires_complete_exact_attempt_release_receipts() {
        use crate::task_execution::execution::ReleasedVerificationFacts;
        use novarocks_execution_contract::{
            ContextVerificationFacts, QueryContextRef, TaskIdentity, TaskVerificationFacts,
            TaskVerificationObservation, VerificationInstance, VerificationRecord,
            VerificationState,
        };
        use novarocks_types::{BackendProcessId, FrontendProcessId, StageId, TaskId};
        use std::collections::BTreeMap;
        let execution = attempt(2);
        let backend = BackendProcessId::new_v7();
        let context = QueryContextRef::new(execution, FrontendProcessId::new_v7(), backend);
        let identity = TaskIdentity::new(
            execution,
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            backend,
        );
        let instance = VerificationInstance {
            plan_node_id: 17,
            local_instance_id: 0,
        };
        let expected = vec![(identity, instance)];
        let fence = pending(&eligible(), execution, 97).unwrap();
        for complete in [false, true] {
            let receipts = ReleasedVerificationFacts::for_test(
                execution,
                BTreeMap::new(),
                expected.clone(),
                complete,
            );
            assert!(
                completed(
                    &fence,
                    ValidationCompletion::VerifiedUncommitted {
                        verification: &receipts
                    }
                )
                .is_err()
            );
        }
        let contexts = BTreeMap::from([(
            context,
            ContextVerificationFacts {
                context,
                truncated: false,
                tasks: vec![TaskVerificationFacts {
                    identity,
                    observation: TaskVerificationObservation::Available(vec![VerificationRecord {
                        instance,
                        state: VerificationState::Completed {
                            requested: 4,
                            matched: 4,
                        },
                    }]),
                }],
            },
        )]);
        let incomplete = ReleasedVerificationFacts::for_test(
            execution,
            contexts.clone(),
            expected.clone(),
            false,
        );
        assert!(
            completed(
                &fence,
                ValidationCompletion::VerifiedUncommitted {
                    verification: &incomplete
                }
            )
            .is_err()
        );
        let receipts = ReleasedVerificationFacts::for_test(
            execution,
            contexts.clone(),
            expected.clone(),
            true,
        );
        let rolled_back = completed(
            &fence,
            ValidationCompletion::VerifiedUncommitted {
                verification: &receipts,
            },
        )
        .unwrap();
        assert_eq!(rolled_back.state, EligibilityState::Eligible);
        assert_eq!(
            rolled_back.binding.publication_revision,
            fence.binding.publication_revision
        );
        let wrong_attempt =
            ReleasedVerificationFacts::for_test(attempt(3), contexts, expected, true);
        assert!(
            completed(
                &fence,
                ValidationCompletion::VerifiedUncommitted {
                    verification: &wrong_attempt
                }
            )
            .is_err()
        );
    }
}

pub(crate) fn completed(
    pending: &EligibilityDocument,
    completion: ValidationCompletion<'_>,
) -> Result<EligibilityDocument, String> {
    let EligibilityState::ValidationPending { attempt, .. } = pending.state else {
        return Err("MV validation completion requires the exact pending attempt".into());
    };
    let mut next = pending.clone();
    next.binding.generation = next
        .binding
        .generation
        .checked_add(1)
        .ok_or_else(|| "MV eligibility generation overflow".to_string())?;
    next.state = match completion {
        ValidationCompletion::Invalid {
            execution_id,
            evidence,
        } if execution_id == attempt => EligibilityState::Invalid { evidence },
        ValidationCompletion::VerifiedUncommitted { verification }
            if verification.execution_id() == attempt && verification.permits_rollback() =>
        {
            EligibilityState::Eligible
        }
        _ => {
            return Err(
                "MV validation result lacks exact complete evidence for the pending attempt".into(),
            );
        }
    };
    next.validate().map_err(|error| error.to_string())?;
    Ok(next)
}
