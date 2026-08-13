// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.

//! Catalog-authoritative historical recovery for fenced Iceberg CTAS.
//!
//! This facet belongs only to the current Connector generation. It sends the
//! durable operation identity and current fence directly to the REST catalog
//! extension; it never acquires an ordinary staged-create lease, reconstructs
//! an old writer handle, consults the MV recovery facet, or lists objects to
//! infer whether an action happened.

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION, ConnectorCtasActionId,
    ConnectorCtasAdvanceFenceRequest, ConnectorCtasConflictKind, ConnectorCtasFailure,
    ConnectorCtasProofPurpose, ConnectorCtasPublicationFence, ConnectorCtasPublicationFenceReceipt,
    ConnectorCtasPublicationProof, ConnectorCtasStagedLocator,
    ConnectorCtasStagedPublicationCapability, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionBindingKey, ConnectorHistoricalCtasAction,
    ConnectorHistoricalCtasCleanupReceipt, ConnectorHistoricalCtasCleanupRequest,
    ConnectorHistoricalCtasDescriptor, ConnectorHistoricalCtasDispatchState,
    ConnectorHistoricalCtasDisposition, ConnectorHistoricalCtasObservation,
    ConnectorHistoricalCtasStagedPublicationRecovery, ConnectorInstanceDescriptor,
    ConnectorMutationFailure, ConnectorMutationFailureKind, ConnectorRequestContext,
};
use serde::{Deserialize, Serialize};

use crate::control_provider::IcebergControlProvider;
use crate::control_runtime::IcebergControlRuntime;
use crate::iceberg_catalog_rest::{
    AbortCtasTargetRequest, AbortCtasTargetResponse, AdvanceCtasFenceRequest,
    AdvanceCtasFenceResponse, CtasFencedAction, CtasFencedGeneration, CtasFencedOperation,
    CtasFencedPublicationConflictKind, CtasFencedPublicationError, InspectCtasTargetRequest,
    InspectCtasTargetResponse,
};

const HISTORICAL_PROOF_VERSION: u16 = 1;

/// Provider-private payload sealed by the neutral historical proof.
///
/// `staged_proof` is deliberately retained separately from the terminal proof:
/// a no-op is terminal catalog evidence, but only the original staged proof is
/// accepted by the catalog as guarded cleanup authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct IcebergHistoricalCtasProofV1 {
    version: u16,
    state: String,
    catalog_proof: String,
    staged_proof: Option<String>,
}

trait HistoricalCtasCatalog: Send + Sync {
    fn advance_fence(
        &self,
        request: AdvanceCtasFenceRequest,
    ) -> Result<AdvanceCtasFenceResponse, ConnectorCtasFailure>;

    fn inspect(
        &self,
        request: InspectCtasTargetRequest,
    ) -> Result<InspectCtasTargetResponse, ConnectorCtasFailure>;

    fn abort(
        &self,
        request: AbortCtasTargetRequest,
    ) -> Result<AbortCtasTargetResponse, ConnectorCtasFailure>;
}

struct RestHistoricalCtasCatalog {
    runtime: Arc<IcebergControlRuntime>,
    catalog: Arc<crate::iceberg_catalog_rest::RestCatalog>,
}

impl HistoricalCtasCatalog for RestHistoricalCtasCatalog {
    fn advance_fence(
        &self,
        request: AdvanceCtasFenceRequest,
    ) -> Result<AdvanceCtasFenceResponse, ConnectorCtasFailure> {
        let catalog = Arc::clone(&self.catalog);
        self.runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                catalog
                    .require_ctas_fenced_publication()
                    .await?
                    .advance_fence(&request)
                    .await
            })
            .map_err(local_possibly_dispatched)?
            .map_err(rest_failure)
    }

    fn inspect(
        &self,
        request: InspectCtasTargetRequest,
    ) -> Result<InspectCtasTargetResponse, ConnectorCtasFailure> {
        let catalog = Arc::clone(&self.catalog);
        self.runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                catalog
                    .require_ctas_fenced_publication()
                    .await?
                    .inspect(&request)
                    .await
            })
            .map_err(local_ambiguous)?
            .map_err(rest_failure)
    }

    fn abort(
        &self,
        request: AbortCtasTargetRequest,
    ) -> Result<AbortCtasTargetResponse, ConnectorCtasFailure> {
        let catalog = Arc::clone(&self.catalog);
        self.runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                catalog
                    .require_ctas_fenced_publication()
                    .await?
                    .abort(&request)
                    .await
            })
            .map_err(local_possibly_dispatched)?
            .map_err(rest_failure)
    }
}

/// Current-generation Iceberg historical CTAS capability.
pub struct IcebergHistoricalCtasRecovery {
    descriptor: ConnectorInstanceDescriptor,
    binding_key: ConnectorExecutionBindingKey,
    catalog: Arc<dyn HistoricalCtasCatalog>,
}

impl IcebergHistoricalCtasRecovery {
    pub(crate) fn try_new(provider: Arc<IcebergControlProvider>) -> Result<Self, ConnectorError> {
        if !provider.runtime().has_ctas_fenced_publication() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg catalog does not advertise fenced CTAS historical recovery v1",
            ));
        }
        let catalog = provider.runtime().rest_catalog().cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg historical CTAS recovery requires the exact REST catalog generation",
            )
        })?;
        let binding_key = ConnectorExecutionBindingKey {
            instance_id: provider.descriptor().instance_id.clone(),
            incarnation: provider.incarnation(),
        };
        Ok(Self {
            descriptor: provider.descriptor().clone(),
            binding_key,
            catalog: Arc::new(RestHistoricalCtasCatalog {
                runtime: Arc::clone(provider.runtime()),
                catalog,
            }),
        })
    }

    fn validate_context(
        &self,
        context: &ConnectorRequestContext,
    ) -> Result<(), ConnectorCtasFailure> {
        if context.cancellation().is_cancelled() {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::Cancelled,
                "Iceberg historical CTAS request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::DeadlineExceeded,
                "Iceberg historical CTAS request deadline elapsed",
            ));
        }
        Ok(())
    }

    fn validate_descriptor(
        &self,
        descriptor: &ConnectorHistoricalCtasDescriptor,
    ) -> Result<(), ConnectorCtasFailure> {
        descriptor.validate().map_err(local_known_not_dispatched)?;
        if descriptor.historical_binding.instance_id != self.descriptor.instance_id
            || descriptor.fence.target().instance_id != self.descriptor.instance_id
        {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::InvalidRequest,
                "Iceberg historical CTAS descriptor names another connector instance",
            ));
        }
        let advance =
            unique_checkpoint_record(descriptor, ConnectorHistoricalCtasAction::AdvanceFence)
                .map_err(|message| {
                    known_not_dispatched(ConnectorMutationFailureKind::InvalidRequest, message)
                })?;
        if advance.dispatch
            != novarocks_spi::connector::ConnectorHistoricalCtasDispatchState::Completed
            || advance.evidence_digest != Some(descriptor.fence_receipt_digest)
        {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::InvalidRequest,
                "Iceberg historical CTAS inspection requires the exact completed current-fence receipt",
            ));
        }
        let stage =
            optional_unique_checkpoint_record(descriptor, ConnectorHistoricalCtasAction::Stage)
                .map_err(|message| {
                    known_not_dispatched(ConnectorMutationFailureKind::InvalidRequest, message)
                })?;
        if (descriptor.locator.is_some() || descriptor.evidence.is_some()) && stage.is_none() {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::InvalidRequest,
                "Iceberg historical CTAS staged authority requires the exact stage checkpoint",
            ));
        }
        if descriptor.locator.as_ref().is_some_and(|locator| {
            stage.is_none_or(|stage| locator.stage_action_id() != stage.action_id)
        }) {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::InvalidRequest,
                "Iceberg historical CTAS staged locator does not match its durable stage checkpoint",
            ));
        }
        Ok(())
    }

    fn map_inspection(
        &self,
        descriptor: &ConnectorHistoricalCtasDescriptor,
        response: InspectCtasTargetResponse,
    ) -> Result<ConnectorHistoricalCtasObservation, ConnectorCtasFailure> {
        match response {
            InspectCtasTargetResponse::NotCreated { proof } => self.conclusive_observation(
                descriptor,
                ConnectorHistoricalCtasDisposition::NotCreated,
                None,
                "not-created",
                proof,
                None,
            ),
            InspectCtasTargetResponse::Staged {
                staged_locator,
                proof,
            } => {
                let locator = match self.bind_locator(descriptor, &staged_locator) {
                    Ok(locator) => locator,
                    Err(message) => return self.local_ambiguous_observation(descriptor, message),
                };
                self.conclusive_observation(
                    descriptor,
                    ConnectorHistoricalCtasDisposition::Staged,
                    Some(locator),
                    "staged",
                    proof.clone(),
                    Some(proof),
                )
            }
            InspectCtasTargetResponse::Published { provenance, proof } => {
                let payload = join_terminal_proof("published", provenance, proof)?;
                self.conclusive_observation(
                    descriptor,
                    ConnectorHistoricalCtasDisposition::Published,
                    None,
                    "published",
                    payload,
                    None,
                )
            }
            InspectCtasTargetResponse::NoOp {
                provenance,
                proof,
                staged_locator,
                staged_proof,
            } => {
                if descriptor.create_policy != novarocks_spi::connector::CreatePolicy::NoOpIfExists
                {
                    return self.local_ambiguous_observation(
                        descriptor,
                        "Iceberg CTAS no-op inspection contradicts the durable create policy",
                    );
                }
                if staged_locator.is_some() != staged_proof.is_some() {
                    return self.local_ambiguous_observation(
                        descriptor,
                        "Iceberg CTAS no-op inspection returned incomplete staged cleanup authority",
                    );
                }
                let locator = match staged_locator {
                    Some(locator) => match self.bind_locator(descriptor, &locator) {
                        Ok(locator) => Some(locator),
                        Err(message) => {
                            return self.local_ambiguous_observation(descriptor, message);
                        }
                    },
                    None => None,
                };
                let terminal = join_terminal_proof("no-op", provenance, proof)?;
                self.conclusive_observation(
                    descriptor,
                    ConnectorHistoricalCtasDisposition::NoOp,
                    locator,
                    "no-op",
                    terminal,
                    staged_proof,
                )
            }
            InspectCtasTargetResponse::Aborted { provenance, proof } => {
                let payload = join_terminal_proof("aborted", provenance, proof)?;
                self.conclusive_observation(
                    descriptor,
                    ConnectorHistoricalCtasDisposition::Aborted,
                    None,
                    "aborted",
                    payload,
                    None,
                )
            }
            InspectCtasTargetResponse::Conflict {
                kind,
                message,
                proof,
            } => {
                let kind = conflict_kind(kind);
                let failure = mutation_failure(ConnectorMutationFailureKind::Conflict, message);
                let proof = self.historical_proof(
                    descriptor,
                    ConnectorHistoricalCtasDisposition::Conflict,
                    None,
                    "conflict",
                    proof,
                    None,
                )?;
                ConnectorHistoricalCtasObservation::try_new(
                    self.binding_key.clone(),
                    descriptor,
                    ConnectorHistoricalCtasDisposition::Conflict,
                    None,
                    Some(proof),
                    Some(kind),
                    Some(failure),
                )
                .map_err(local_committed_response)
            }
            InspectCtasTargetResponse::Ambiguous { message, proof } => {
                let failure = mutation_failure(ConnectorMutationFailureKind::Unavailable, message);
                let proof = proof
                    .map(|proof| {
                        self.historical_proof(
                            descriptor,
                            ConnectorHistoricalCtasDisposition::Ambiguous,
                            None,
                            "ambiguous",
                            proof,
                            None,
                        )
                    })
                    .transpose()?;
                ConnectorHistoricalCtasObservation::try_new(
                    self.binding_key.clone(),
                    descriptor,
                    ConnectorHistoricalCtasDisposition::Ambiguous,
                    None,
                    proof,
                    None,
                    Some(failure),
                )
                .map_err(local_committed_response)
            }
            InspectCtasTargetResponse::Unsupported { message } => {
                ConnectorHistoricalCtasObservation::try_new(
                    self.binding_key.clone(),
                    descriptor,
                    ConnectorHistoricalCtasDisposition::Unsupported,
                    None,
                    None,
                    None,
                    Some(mutation_failure(
                        ConnectorMutationFailureKind::Unsupported,
                        message,
                    )),
                )
                .map_err(local_committed_response)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn conclusive_observation(
        &self,
        descriptor: &ConnectorHistoricalCtasDescriptor,
        disposition: ConnectorHistoricalCtasDisposition,
        locator: Option<ConnectorCtasStagedLocator>,
        state: &str,
        catalog_proof: String,
        staged_proof: Option<String>,
    ) -> Result<ConnectorHistoricalCtasObservation, ConnectorCtasFailure> {
        let proof = self.historical_proof(
            descriptor,
            disposition,
            locator.as_ref(),
            state,
            catalog_proof,
            staged_proof,
        )?;
        ConnectorHistoricalCtasObservation::try_new(
            self.binding_key.clone(),
            descriptor,
            disposition,
            locator,
            Some(proof),
            None,
            None,
        )
        .map_err(local_committed_response)
    }

    fn historical_proof(
        &self,
        descriptor: &ConnectorHistoricalCtasDescriptor,
        disposition: ConnectorHistoricalCtasDisposition,
        locator: Option<&ConnectorCtasStagedLocator>,
        state: &str,
        catalog_proof: String,
        staged_proof: Option<String>,
    ) -> Result<ConnectorCtasPublicationProof, ConnectorCtasFailure> {
        if catalog_proof.is_empty() || staged_proof.as_deref() == Some("") {
            return Err(local_ambiguous(
                "Iceberg historical CTAS inspection returned an empty proof",
            ));
        }
        let payload = serde_json::to_vec(&IcebergHistoricalCtasProofV1 {
            version: HISTORICAL_PROOF_VERSION,
            state: state.to_owned(),
            catalog_proof,
            staged_proof,
        })
        .map_err(|error| {
            local_ambiguous(format!("encode Iceberg historical CTAS proof: {error}"))
        })?;
        ConnectorCtasPublicationProof::try_new(
            self.binding_key.clone(),
            &descriptor.fence,
            historical_purpose(disposition),
            None,
            descriptor.digest(),
            locator,
            Bytes::from(payload),
        )
        .map_err(local_ambiguous_connector)
    }

    fn bind_locator(
        &self,
        descriptor: &ConnectorHistoricalCtasDescriptor,
        catalog_locator: &str,
    ) -> Result<ConnectorCtasStagedLocator, String> {
        if catalog_locator.is_empty() {
            return Err("Iceberg historical CTAS inspection returned an empty locator".into());
        }
        let stage_action = unique_checkpoint(descriptor, ConnectorHistoricalCtasAction::Stage)
            .map_err(str::to_owned)?;
        let stage_checkpoint = descriptor
            .checkpoints
            .iter()
            .find(|checkpoint| {
                checkpoint.action == ConnectorHistoricalCtasAction::Stage
                    && checkpoint.action_id == stage_action
            })
            .ok_or_else(|| "Iceberg historical CTAS stage checkpoint is missing".to_string())?;
        if stage_checkpoint.dispatch == ConnectorHistoricalCtasDispatchState::NotDispatched {
            return Err(
                "Iceberg catalog reported staged state for a definitely undispatched stage action"
                    .into(),
            );
        }
        if let Some(locator) = &descriptor.locator {
            let durable = std::str::from_utf8(locator.payload())
                .map_err(|error| format!("durable Iceberg CTAS locator is not UTF-8: {error}"))?;
            if durable != catalog_locator {
                return Err(
                    "Iceberg historical CTAS locator drifted from the durable descriptor".into(),
                );
            }
            return Ok(locator.clone());
        }
        ConnectorCtasStagedLocator::try_new(
            self.binding_key.clone(),
            &descriptor.fence,
            stage_action,
            descriptor.target_digest,
            Bytes::copy_from_slice(catalog_locator.as_bytes()),
        )
        .map_err(|error| format!("bind catalog-staged Iceberg CTAS locator: {error}"))
    }

    fn local_ambiguous_observation(
        &self,
        descriptor: &ConnectorHistoricalCtasDescriptor,
        message: impl Into<Arc<str>>,
    ) -> Result<ConnectorHistoricalCtasObservation, ConnectorCtasFailure> {
        ConnectorHistoricalCtasObservation::try_new(
            self.binding_key.clone(),
            descriptor,
            ConnectorHistoricalCtasDisposition::Ambiguous,
            None,
            None,
            None,
            Some(mutation_failure(
                ConnectorMutationFailureKind::CorruptData,
                message,
            )),
        )
        .map_err(local_committed_response)
    }
}

impl ConnectorHistoricalCtasStagedPublicationRecovery for IcebergHistoricalCtasRecovery {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.binding_key
    }

    fn capability(&self) -> ConnectorCtasStagedPublicationCapability {
        ConnectorCtasStagedPublicationCapability::try_new(
            CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION,
        )
        .expect("static CTAS protocol version is valid")
    }

    fn advance_fence(
        &self,
        request: ConnectorCtasAdvanceFenceRequest,
    ) -> Result<ConnectorCtasPublicationFenceReceipt, ConnectorCtasFailure> {
        request.validate().map_err(local_known_not_dispatched)?;
        if request.fence.target().instance_id != self.descriptor.instance_id {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::InvalidRequest,
                "Iceberg historical CTAS advance fence names another connector instance",
            ));
        }
        self.validate_context(&request.context)?;
        let wire = AdvanceCtasFenceRequest {
            action: wire_action(&request.fence, request.action_id, request.input_digest)?,
        };
        let response = self.catalog.advance_fence(wire.clone())?;
        if response.generation != wire.action.generation
            || response.input_digest != encode_hex(request.input_digest)
        {
            return Err(committed_response_invalid(
                "Iceberg historical CTAS advance-fence response drifted from the request",
            ));
        }
        ConnectorCtasPublicationFenceReceipt::try_new(&request, Bytes::from(response.receipt))
            .map_err(local_committed_response)
    }

    fn inspect(
        &self,
        descriptor: ConnectorHistoricalCtasDescriptor,
        context: ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalCtasObservation, ConnectorCtasFailure> {
        self.validate_context(&context)?;
        self.validate_descriptor(&descriptor)?;
        let response = self.catalog.inspect(InspectCtasTargetRequest {
            operation: wire_operation(&descriptor.fence)?,
            generation: wire_generation(descriptor.fence.generation()),
            input_digest: encode_hex(
                unique_checkpoint_record(&descriptor, ConnectorHistoricalCtasAction::AdvanceFence)
                    .map_err(|message| {
                        known_not_dispatched(ConnectorMutationFailureKind::InvalidRequest, message)
                    })?
                    .input_digest,
            ),
        })?;
        self.map_inspection(&descriptor, response)
    }

    fn cleanup(
        &self,
        request: ConnectorHistoricalCtasCleanupRequest,
    ) -> Result<ConnectorHistoricalCtasCleanupReceipt, ConnectorCtasFailure> {
        request.validate().map_err(local_known_not_dispatched)?;
        self.validate_context(&request.context)?;
        self.validate_descriptor(&request.descriptor)?;
        if request.observation.inspection_binding != self.binding_key {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::InvalidRequest,
                "Iceberg historical CTAS cleanup observation belongs to another generation",
            ));
        }
        let locator = request
            .observation
            .locator
            .as_ref()
            .expect("validated historical cleanup locator");
        let proof = request
            .observation
            .proof
            .as_ref()
            .expect("validated historical cleanup proof");
        let provider_proof: IcebergHistoricalCtasProofV1 = serde_json::from_slice(proof.payload())
            .map_err(|error| {
                known_not_dispatched(
                    ConnectorMutationFailureKind::CorruptData,
                    format!("decode Iceberg historical CTAS cleanup proof: {error}"),
                )
            })?;
        if provider_proof.version != HISTORICAL_PROOF_VERSION
            || !matches!(provider_proof.state.as_str(), "staged" | "no-op")
            || (request.observation.disposition == ConnectorHistoricalCtasDisposition::Staged
                && provider_proof.state != "staged")
            || (request.observation.disposition == ConnectorHistoricalCtasDisposition::NoOp
                && provider_proof.state != "no-op")
        {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::CorruptData,
                "Iceberg historical CTAS cleanup proof has an unsupported state",
            ));
        }
        let staged_proof = provider_proof.staged_proof.ok_or_else(|| {
            known_not_dispatched(
                ConnectorMutationFailureKind::CorruptData,
                "Iceberg historical CTAS cleanup proof does not retain staged authority",
            )
        })?;
        let checkpoint =
            unique_checkpoint_record(&request.descriptor, ConnectorHistoricalCtasAction::Abort)
                .map_err(|message| {
                    known_not_dispatched(ConnectorMutationFailureKind::InvalidRequest, message)
                })?;
        let wire = AbortCtasTargetRequest {
            action: wire_action(
                &request.descriptor.fence,
                checkpoint.action_id,
                checkpoint.input_digest,
            )?,
            staged_locator: opaque_text("staged locator", locator.payload())?,
            staged_proof,
            // The catalog retained the bounded provider cleanup action at
            // stage/publish time. Historical cleanup names that authority and
            // never reconstructs it by listing object storage.
            provider_payload: String::new(),
        };
        let response = self.catalog.abort(wire)?;
        let cleanup_payload = serde_json::to_vec(&IcebergHistoricalCtasProofV1 {
            version: HISTORICAL_PROOF_VERSION,
            state: "cleanup".into(),
            catalog_proof: join_terminal_proof("cleanup", response.provenance, response.proof)?,
            staged_proof: None,
        })
        .map_err(|error| committed_response_invalid(format!("encode cleanup proof: {error}")))?;
        let proof = ConnectorCtasPublicationProof::try_new(
            self.binding_key.clone(),
            &request.descriptor.fence,
            ConnectorCtasProofPurpose::HistoricalCleanup,
            None,
            request.observation.digest(),
            Some(locator),
            Bytes::from(cleanup_payload),
        )
        .map_err(local_committed_response)?;
        ConnectorHistoricalCtasCleanupReceipt::try_new(&request, proof)
            .map_err(local_committed_response)
    }
}

fn unique_checkpoint(
    descriptor: &ConnectorHistoricalCtasDescriptor,
    action: ConnectorHistoricalCtasAction,
) -> Result<ConnectorCtasActionId, &'static str> {
    Ok(unique_checkpoint_record(descriptor, action)?.action_id)
}

fn unique_checkpoint_record(
    descriptor: &ConnectorHistoricalCtasDescriptor,
    action: ConnectorHistoricalCtasAction,
) -> Result<&novarocks_spi::connector::ConnectorHistoricalCtasCheckpoint, &'static str> {
    optional_unique_checkpoint_record(descriptor, action)?
        .ok_or("historical CTAS descriptor is missing the required action checkpoint")
}

fn optional_unique_checkpoint_record(
    descriptor: &ConnectorHistoricalCtasDescriptor,
    action: ConnectorHistoricalCtasAction,
) -> Result<Option<&novarocks_spi::connector::ConnectorHistoricalCtasCheckpoint>, &'static str> {
    let mut matches = descriptor
        .checkpoints
        .iter()
        .filter(|checkpoint| checkpoint.action == action);
    let checkpoint = matches.next();
    if matches.next().is_some() {
        return Err("historical CTAS descriptor has multiple records for one required action");
    }
    Ok(checkpoint)
}

fn join_terminal_proof(
    state: &str,
    provenance: String,
    proof: String,
) -> Result<String, ConnectorCtasFailure> {
    if provenance.is_empty() || proof.is_empty() {
        return Err(local_ambiguous(
            "Iceberg historical CTAS terminal evidence is empty",
        ));
    }
    serde_json::to_string(&(state, provenance, proof)).map_err(|error| {
        local_ambiguous(format!(
            "encode Iceberg historical CTAS terminal evidence: {error}"
        ))
    })
}

fn historical_purpose(
    disposition: ConnectorHistoricalCtasDisposition,
) -> ConnectorCtasProofPurpose {
    match disposition {
        ConnectorHistoricalCtasDisposition::NotCreated => {
            ConnectorCtasProofPurpose::HistoricalNotCreated
        }
        ConnectorHistoricalCtasDisposition::Staged => ConnectorCtasProofPurpose::HistoricalStaged,
        ConnectorHistoricalCtasDisposition::Published => {
            ConnectorCtasProofPurpose::HistoricalPublished
        }
        ConnectorHistoricalCtasDisposition::NoOp => ConnectorCtasProofPurpose::HistoricalNoOp,
        ConnectorHistoricalCtasDisposition::Aborted => ConnectorCtasProofPurpose::HistoricalAborted,
        ConnectorHistoricalCtasDisposition::Conflict => {
            ConnectorCtasProofPurpose::HistoricalConflict
        }
        ConnectorHistoricalCtasDisposition::Ambiguous => {
            ConnectorCtasProofPurpose::HistoricalAmbiguous
        }
        ConnectorHistoricalCtasDisposition::Unsupported => {
            ConnectorCtasProofPurpose::HistoricalUnsupported
        }
    }
}

fn wire_operation(
    fence: &ConnectorCtasPublicationFence,
) -> Result<CtasFencedOperation, ConnectorCtasFailure> {
    Ok(CtasFencedOperation {
        cluster_id: encode_hex(fence.cluster().digest()),
        operation_id: uuid::Uuid::from_bytes(fence.operation_id().to_bytes()).to_string(),
        target: crate::iceberg::TableIdent::from_strs([
            &fence.target().namespace,
            &fence.target().table,
        ])
        .map_err(|error| local_invalid(format!("build Iceberg CTAS target: {error}")))?,
    })
}

fn wire_action(
    fence: &ConnectorCtasPublicationFence,
    action_id: ConnectorCtasActionId,
    input_digest: [u8; 32],
) -> Result<CtasFencedAction, ConnectorCtasFailure> {
    Ok(CtasFencedAction {
        operation: wire_operation(fence)?,
        generation: wire_generation(fence.generation()),
        action_id: uuid::Uuid::from_bytes(action_id.to_bytes()).to_string(),
        input_digest: encode_hex(input_digest),
    })
}

fn wire_generation(
    generation: novarocks_spi::connector::ConnectorExternalFenceGeneration,
) -> CtasFencedGeneration {
    CtasFencedGeneration {
        control_plane_incarnation: generation.control_plane_incarnation(),
        resource_epoch: generation.resource_epoch(),
        fence_generation: generation.coordination_attempt(),
    }
}

fn conflict_kind(kind: CtasFencedPublicationConflictKind) -> ConnectorCtasConflictKind {
    match kind {
        CtasFencedPublicationConflictKind::StaleFence => ConnectorCtasConflictKind::StaleFence,
        CtasFencedPublicationConflictKind::IdentityConflict => {
            ConnectorCtasConflictKind::IdentityConflict
        }
        CtasFencedPublicationConflictKind::DigestConflict => {
            ConnectorCtasConflictKind::DigestConflict
        }
        CtasFencedPublicationConflictKind::AlreadyPublished => {
            ConnectorCtasConflictKind::AlreadyPublished
        }
        CtasFencedPublicationConflictKind::AlreadyAborted => {
            ConnectorCtasConflictKind::AlreadyAborted
        }
        CtasFencedPublicationConflictKind::CreatePolicyConflict => {
            ConnectorCtasConflictKind::CreatePolicyConflict
        }
    }
}

fn rest_failure(error: CtasFencedPublicationError) -> ConnectorCtasFailure {
    match error {
        CtasFencedPublicationError::Unsupported(error) => local_ambiguous(format!(
            "advertised Iceberg CTAS historical extension became unsupported: {error}"
        )),
        CtasFencedPublicationError::Conflict { kind, error } => ConnectorCtasFailure::Conflict {
            kind: conflict_kind(kind),
            failure: mutation_failure(ConnectorMutationFailureKind::Conflict, error.to_string()),
        },
        CtasFencedPublicationError::KnownNotDispatched(error) => {
            known_not_dispatched(ConnectorMutationFailureKind::Unavailable, error.to_string())
        }
        CtasFencedPublicationError::PossiblyDispatched(error) => {
            ConnectorCtasFailure::PossiblyDispatched(mutation_failure(
                ConnectorMutationFailureKind::Unavailable,
                error.to_string(),
            ))
        }
        CtasFencedPublicationError::CommittedResponseInvalid(error) => {
            ConnectorCtasFailure::CommittedResponseInvalid(mutation_failure(
                ConnectorMutationFailureKind::CorruptData,
                error.to_string(),
            ))
        }
        CtasFencedPublicationError::Ambiguous(error) => local_ambiguous(error.to_string()),
    }
}

fn mutation_failure(
    kind: ConnectorMutationFailureKind,
    message: impl Into<Arc<str>>,
) -> ConnectorMutationFailure {
    ConnectorMutationFailure::new(kind, message)
}

fn known_not_dispatched(
    kind: ConnectorMutationFailureKind,
    message: impl Into<Arc<str>>,
) -> ConnectorCtasFailure {
    ConnectorCtasFailure::KnownNotDispatched(mutation_failure(kind, message))
}

fn local_invalid(message: impl Into<Arc<str>>) -> ConnectorCtasFailure {
    known_not_dispatched(ConnectorMutationFailureKind::InvalidRequest, message)
}

fn local_ambiguous(message: impl Into<Arc<str>>) -> ConnectorCtasFailure {
    ConnectorCtasFailure::Ambiguous(mutation_failure(
        ConnectorMutationFailureKind::Unavailable,
        message,
    ))
}

fn local_ambiguous_connector(error: ConnectorError) -> ConnectorCtasFailure {
    ConnectorCtasFailure::Ambiguous(mutation_failure(
        connector_failure_kind(error.kind()),
        error.to_string(),
    ))
}

fn local_possibly_dispatched(message: impl Into<Arc<str>>) -> ConnectorCtasFailure {
    ConnectorCtasFailure::PossiblyDispatched(mutation_failure(
        ConnectorMutationFailureKind::Internal,
        message,
    ))
}

fn committed_response_invalid(message: impl Into<Arc<str>>) -> ConnectorCtasFailure {
    ConnectorCtasFailure::CommittedResponseInvalid(mutation_failure(
        ConnectorMutationFailureKind::CorruptData,
        message,
    ))
}

fn local_known_not_dispatched(error: ConnectorError) -> ConnectorCtasFailure {
    known_not_dispatched(connector_failure_kind(error.kind()), error.to_string())
}

fn local_committed_response(error: ConnectorError) -> ConnectorCtasFailure {
    ConnectorCtasFailure::CommittedResponseInvalid(mutation_failure(
        connector_failure_kind(error.kind()),
        error.to_string(),
    ))
}

fn connector_failure_kind(kind: ConnectorErrorKind) -> ConnectorMutationFailureKind {
    match kind {
        ConnectorErrorKind::InvalidRequest => ConnectorMutationFailureKind::InvalidRequest,
        ConnectorErrorKind::NotFound => ConnectorMutationFailureKind::NotFound,
        ConnectorErrorKind::PermissionDenied => ConnectorMutationFailureKind::PermissionDenied,
        ConnectorErrorKind::Unsupported => ConnectorMutationFailureKind::Unsupported,
        ConnectorErrorKind::Cancelled => ConnectorMutationFailureKind::Cancelled,
        ConnectorErrorKind::DeadlineExceeded => ConnectorMutationFailureKind::DeadlineExceeded,
        ConnectorErrorKind::ResourceExhausted => ConnectorMutationFailureKind::ResourceExhausted,
        ConnectorErrorKind::Unavailable => ConnectorMutationFailureKind::Unavailable,
        ConnectorErrorKind::CorruptData => ConnectorMutationFailureKind::CorruptData,
        ConnectorErrorKind::Internal => ConnectorMutationFailureKind::Internal,
    }
}

fn opaque_text(name: &str, payload: &Bytes) -> Result<String, ConnectorCtasFailure> {
    std::str::from_utf8(payload)
        .map(str::to_owned)
        .map_err(|error| {
            known_not_dispatched(
                ConnectorMutationFailureKind::CorruptData,
                format!("Iceberg historical CTAS {name} is not UTF-8: {error}"),
            )
        })
}

fn encode_hex(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use novarocks_spi::connector::{
        ConnectorCancellation, ConnectorClusterIdentity, ConnectorCtasOperationId,
        ConnectorExternalFenceGeneration, ConnectorHistoricalCtasCheckpoint,
        ConnectorHistoricalCtasDispatchState, ConnectorInstanceId, ConnectorInstanceIncarnation,
        ConnectorProviderId, ConnectorTableIdentity, CreatePolicy,
    };

    use super::*;

    struct NeverCancelled;

    impl ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    #[derive(Default)]
    struct FakeCatalog {
        inspect_response: Mutex<Option<InspectCtasTargetResponse>>,
        abort_response: Mutex<Option<Result<AbortCtasTargetResponse, ConnectorCtasFailure>>>,
        inspect_calls: AtomicUsize,
        abort_calls: AtomicUsize,
        last_inspect: Mutex<Option<InspectCtasTargetRequest>>,
        last_abort: Mutex<Option<AbortCtasTargetRequest>>,
    }

    impl FakeCatalog {
        fn with_inspect(response: InspectCtasTargetResponse) -> Arc<Self> {
            Arc::new(Self {
                inspect_response: Mutex::new(Some(response)),
                ..Self::default()
            })
        }

        fn set_abort(&self, response: Result<AbortCtasTargetResponse, ConnectorCtasFailure>) {
            *self.abort_response.lock().expect("abort response lock") = Some(response);
        }
    }

    impl HistoricalCtasCatalog for FakeCatalog {
        fn advance_fence(
            &self,
            request: AdvanceCtasFenceRequest,
        ) -> Result<AdvanceCtasFenceResponse, ConnectorCtasFailure> {
            Ok(AdvanceCtasFenceResponse {
                generation: request.action.generation,
                input_digest: request.action.input_digest,
                receipt: "fence-receipt".into(),
            })
        }

        fn inspect(
            &self,
            request: InspectCtasTargetRequest,
        ) -> Result<InspectCtasTargetResponse, ConnectorCtasFailure> {
            self.inspect_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_inspect.lock().expect("inspect request lock") = Some(request);
            self.inspect_response
                .lock()
                .expect("inspect response lock")
                .take()
                .ok_or_else(|| local_ambiguous("missing fake inspect response"))
        }

        fn abort(
            &self,
            request: AbortCtasTargetRequest,
        ) -> Result<AbortCtasTargetResponse, ConnectorCtasFailure> {
            self.abort_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_abort.lock().expect("abort request lock") = Some(request);
            self.abort_response
                .lock()
                .expect("abort response lock")
                .take()
                .unwrap_or_else(|| {
                    Ok(AbortCtasTargetResponse {
                        provenance: "aborted".into(),
                        proof: "abort-proof".into(),
                    })
                })
        }
    }

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            Arc::new(NeverCancelled),
            1024 * 1024,
            4 * 1024 * 1024,
        )
        .expect("request context")
    }

    fn instance() -> ConnectorInstanceId {
        ConnectorInstanceId::parse("iceberg-rest").expect("instance id")
    }

    fn binding(value: u8) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: instance(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([value; 16]),
        }
    }

    fn descriptor_identity() -> ConnectorInstanceDescriptor {
        ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider id"),
            instance_id: instance(),
        }
    }

    fn uuid_v7_bytes(value: u8) -> [u8; 16] {
        let mut bytes = [value; 16];
        bytes[6] = 0x70 | (value & 0x0f);
        bytes[8] = 0x80 | (value & 0x3f);
        bytes
    }

    fn action(value: u8) -> ConnectorCtasActionId {
        ConnectorCtasActionId::try_from_bytes(uuid_v7_bytes(value)).expect("action id")
    }

    fn fence(attempt: u64) -> ConnectorCtasPublicationFence {
        ConnectorCtasPublicationFence::try_new(
            ConnectorClusterIdentity::derive("cluster-a").expect("cluster identity"),
            ConnectorExternalFenceGeneration::try_new(7, 3, attempt).expect("generation"),
            ConnectorCtasOperationId::try_from_bytes(uuid_v7_bytes(9)).expect("operation id"),
            ConnectorTableIdentity {
                instance_id: instance(),
                namespace: "analytics".into(),
                table: "orders".into(),
            },
        )
        .expect("CTAS fence")
    }

    fn checkpoints() -> Vec<ConnectorHistoricalCtasCheckpoint> {
        vec![
            ConnectorHistoricalCtasCheckpoint {
                action_id: action(1),
                action: ConnectorHistoricalCtasAction::AdvanceFence,
                dispatch: ConnectorHistoricalCtasDispatchState::Completed,
                input_digest: [11; 32],
                evidence_digest: Some([4; 32]),
            },
            ConnectorHistoricalCtasCheckpoint {
                action_id: action(2),
                action: ConnectorHistoricalCtasAction::Stage,
                dispatch: ConnectorHistoricalCtasDispatchState::Unknown,
                input_digest: [12; 32],
                evidence_digest: None,
            },
            ConnectorHistoricalCtasCheckpoint {
                action_id: action(3),
                action: ConnectorHistoricalCtasAction::Abort,
                dispatch: ConnectorHistoricalCtasDispatchState::Dispatched,
                input_digest: [13; 32],
                evidence_digest: None,
            },
        ]
    }

    fn descriptor_with_policy(
        with_locator: bool,
        create_policy: CreatePolicy,
    ) -> ConnectorHistoricalCtasDescriptor {
        let locator = with_locator.then(|| {
            ConnectorCtasStagedLocator::try_new(
                binding(1),
                &fence(1),
                action(2),
                [3; 32],
                Bytes::from_static(b"staged-locator"),
            )
            .expect("historical locator")
        });
        ConnectorHistoricalCtasDescriptor::try_new(
            binding(1),
            fence(2),
            [4; 32],
            [3; 32],
            create_policy,
            locator,
            checkpoints(),
            None,
        )
        .expect("historical descriptor")
    }

    fn descriptor(with_locator: bool) -> ConnectorHistoricalCtasDescriptor {
        descriptor_with_policy(with_locator, CreatePolicy::NoOpIfExists)
    }

    fn pre_stage_descriptor() -> ConnectorHistoricalCtasDescriptor {
        let advance = checkpoints()
            .into_iter()
            .find(|checkpoint| checkpoint.action == ConnectorHistoricalCtasAction::AdvanceFence)
            .expect("advance-fence checkpoint");
        ConnectorHistoricalCtasDescriptor::try_new(
            binding(1),
            fence(2),
            [4; 32],
            [3; 32],
            CreatePolicy::NoOpIfExists,
            None,
            vec![advance],
            None,
        )
        .expect("pre-stage historical descriptor")
    }

    fn recovery(fake: Arc<FakeCatalog>) -> IcebergHistoricalCtasRecovery {
        IcebergHistoricalCtasRecovery {
            descriptor: descriptor_identity(),
            binding_key: binding(2),
            catalog: fake,
        }
    }

    fn inspect(
        response: InspectCtasTargetResponse,
        with_locator: bool,
    ) -> (ConnectorHistoricalCtasObservation, Arc<FakeCatalog>) {
        let fake = FakeCatalog::with_inspect(response);
        let observation = recovery(Arc::clone(&fake))
            .inspect(descriptor(with_locator), context())
            .expect("historical inspection");
        (observation, fake)
    }

    #[test]
    fn maps_every_catalog_disposition_without_an_ordinary_handle() {
        let cases = [
            (
                InspectCtasTargetResponse::NotCreated {
                    proof: "not-created-proof".into(),
                },
                ConnectorHistoricalCtasDisposition::NotCreated,
            ),
            (
                InspectCtasTargetResponse::Staged {
                    staged_locator: "staged-locator".into(),
                    proof: "stage-proof".into(),
                },
                ConnectorHistoricalCtasDisposition::Staged,
            ),
            (
                InspectCtasTargetResponse::Published {
                    provenance: "published".into(),
                    proof: "published-proof".into(),
                },
                ConnectorHistoricalCtasDisposition::Published,
            ),
            (
                InspectCtasTargetResponse::NoOp {
                    provenance: "no-op".into(),
                    proof: "no-op-proof".into(),
                    staged_locator: Some("staged-locator".into()),
                    staged_proof: Some("stage-proof".into()),
                },
                ConnectorHistoricalCtasDisposition::NoOp,
            ),
            (
                InspectCtasTargetResponse::Aborted {
                    provenance: "aborted".into(),
                    proof: "abort-proof".into(),
                },
                ConnectorHistoricalCtasDisposition::Aborted,
            ),
            (
                InspectCtasTargetResponse::Conflict {
                    kind: CtasFencedPublicationConflictKind::CreatePolicyConflict,
                    message: "target exists".into(),
                    proof: "conflict-proof".into(),
                },
                ConnectorHistoricalCtasDisposition::Conflict,
            ),
            (
                InspectCtasTargetResponse::Ambiguous {
                    message: "record missing".into(),
                    proof: Some("missing-proof".into()),
                },
                ConnectorHistoricalCtasDisposition::Ambiguous,
            ),
            (
                InspectCtasTargetResponse::Unsupported {
                    message: "future version".into(),
                },
                ConnectorHistoricalCtasDisposition::Unsupported,
            ),
        ];

        for (response, expected) in cases {
            let (observation, fake) = inspect(response, true);
            assert_eq!(observation.disposition, expected);
            assert_eq!(fake.inspect_calls.load(Ordering::SeqCst), 1);
            assert_eq!(fake.abort_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn inspect_uses_current_generation_and_exact_advance_lineage_digest() {
        let (observation, fake) = inspect(
            InspectCtasTargetResponse::Published {
                provenance: "published".into(),
                proof: "published-proof".into(),
            },
            false,
        );
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalCtasDisposition::Published
        );
        let request = fake
            .last_inspect
            .lock()
            .expect("inspect request lock")
            .clone()
            .expect("inspect request");
        assert_eq!(request.generation, wire_generation(fence(2).generation()));
        assert_eq!(request.input_digest, encode_hex([11; 32]));
        assert_eq!(request.operation.target.name(), "orders");
    }

    #[test]
    fn fence_only_descriptor_can_prove_not_created_without_cleanup() {
        let fake = FakeCatalog::with_inspect(InspectCtasTargetResponse::NotCreated {
            proof: "not-created-proof".into(),
        });
        let recovery = recovery(Arc::clone(&fake));
        let descriptor = pre_stage_descriptor();

        let observation = recovery
            .inspect(descriptor, context())
            .expect("fence-only inspection");

        assert_eq!(
            observation.disposition,
            ConnectorHistoricalCtasDisposition::NotCreated
        );
        assert!(observation.locator.is_none());
        assert_eq!(fake.inspect_calls.load(Ordering::SeqCst), 1);
        assert_eq!(fake.abort_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn retained_staging_without_stage_checkpoint_is_ambiguous_and_cannot_cleanup() {
        for response in [
            InspectCtasTargetResponse::Staged {
                staged_locator: "recovered-locator".into(),
                proof: "recovered-stage-proof".into(),
            },
            InspectCtasTargetResponse::NoOp {
                provenance: "no-op".into(),
                proof: "no-op-proof".into(),
                staged_locator: Some("recovered-locator".into()),
                staged_proof: Some("recovered-stage-proof".into()),
            },
        ] {
            let fake = FakeCatalog::with_inspect(response);
            let recovery = recovery(Arc::clone(&fake));
            let descriptor = pre_stage_descriptor();
            let observation = recovery
                .inspect(descriptor.clone(), context())
                .expect("retained staging degrades to a typed observation");

            assert_eq!(
                observation.disposition,
                ConnectorHistoricalCtasDisposition::Ambiguous
            );
            assert!(observation.locator.is_none());
            assert!(observation.proof.is_none());
            let cleanup = recovery.cleanup(ConnectorHistoricalCtasCleanupRequest {
                descriptor,
                observation,
                context: context(),
            });
            assert!(matches!(
                cleanup,
                Err(ConnectorCtasFailure::KnownNotDispatched(_))
            ));
            assert_eq!(fake.inspect_calls.load(Ordering::SeqCst), 1);
            assert_eq!(fake.abort_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn staged_result_cannot_contradict_a_definitely_undispatched_stage() {
        let fake = FakeCatalog::with_inspect(InspectCtasTargetResponse::Staged {
            staged_locator: "recovered-locator".into(),
            proof: "recovered-stage-proof".into(),
        });
        let recovery = recovery(Arc::clone(&fake));
        let mut stage_not_dispatched = checkpoints();
        stage_not_dispatched
            .retain(|checkpoint| checkpoint.action != ConnectorHistoricalCtasAction::Abort);
        stage_not_dispatched
            .iter_mut()
            .find(|checkpoint| checkpoint.action == ConnectorHistoricalCtasAction::Stage)
            .expect("stage checkpoint")
            .dispatch = ConnectorHistoricalCtasDispatchState::NotDispatched;
        for locator in [
            None,
            Some(
                ConnectorCtasStagedLocator::try_new(
                    binding(1),
                    &fence(1),
                    action(2),
                    [3; 32],
                    Bytes::from_static(b"recovered-locator"),
                )
                .expect("durable locator"),
            ),
        ] {
            let descriptor = ConnectorHistoricalCtasDescriptor::try_new(
                binding(1),
                fence(2),
                [4; 32],
                [3; 32],
                CreatePolicy::NoOpIfExists,
                locator,
                stage_not_dispatched.clone(),
                None,
            )
            .expect("descriptor with definitely undispatched stage");

            *fake.inspect_response.lock().expect("inspect response lock") =
                Some(InspectCtasTargetResponse::Staged {
                    staged_locator: "recovered-locator".into(),
                    proof: "recovered-stage-proof".into(),
                });
            let observation = recovery
                .inspect(descriptor, context())
                .expect("contradictory staging degrades to typed ambiguity");

            assert_eq!(
                observation.disposition,
                ConnectorHistoricalCtasDisposition::Ambiguous
            );
            assert!(observation.locator.is_none());
            assert!(observation.proof.is_none());
        }
        assert_eq!(fake.inspect_calls.load(Ordering::SeqCst), 2);
        assert_eq!(fake.abort_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn staged_response_can_bind_a_current_historical_locator_after_reply_loss() {
        let (observation, _) = inspect(
            InspectCtasTargetResponse::Staged {
                staged_locator: "recovered-locator".into(),
                proof: "recovered-stage-proof".into(),
            },
            false,
        );
        let locator = observation.locator.expect("recovered locator");
        assert_eq!(locator.issuance_owner(), &binding(2));
        assert_eq!(locator.issuance_fence(), &fence(2));
        assert_eq!(locator.stage_action_id(), action(2));
        assert_eq!(locator.payload(), &Bytes::from_static(b"recovered-locator"));
    }

    #[test]
    fn stale_drop_recreate_and_missing_or_corrupt_records_remain_typed() {
        let (stale, _) = inspect(
            InspectCtasTargetResponse::Conflict {
                kind: CtasFencedPublicationConflictKind::StaleFence,
                message: "stale generation".into(),
                proof: "stale-proof".into(),
            },
            true,
        );
        assert_eq!(
            stale.conflict_kind,
            Some(ConnectorCtasConflictKind::StaleFence)
        );

        let (recreated, _) = inspect(
            InspectCtasTargetResponse::Conflict {
                kind: CtasFencedPublicationConflictKind::IdentityConflict,
                message: "target identity changed".into(),
                proof: "identity-proof".into(),
            },
            true,
        );
        assert_eq!(
            recreated.conflict_kind,
            Some(ConnectorCtasConflictKind::IdentityConflict)
        );

        for message in ["catalog record missing", "catalog record corrupt"] {
            let (ambiguous, _) = inspect(
                InspectCtasTargetResponse::Ambiguous {
                    message: message.into(),
                    proof: Some(format!("{message}-proof")),
                },
                true,
            );
            assert_eq!(
                ambiguous.disposition,
                ConnectorHistoricalCtasDisposition::Ambiguous
            );
            assert!(ambiguous.failure.is_some());
        }
    }

    #[test]
    fn locator_drift_and_incomplete_no_op_authority_degrade_to_ambiguous() {
        let (drift, _) = inspect(
            InspectCtasTargetResponse::Staged {
                staged_locator: "foreign-locator".into(),
                proof: "stage-proof".into(),
            },
            true,
        );
        assert_eq!(
            drift.disposition,
            ConnectorHistoricalCtasDisposition::Ambiguous
        );
        assert!(drift.locator.is_none());

        let (incomplete, _) = inspect(
            InspectCtasTargetResponse::NoOp {
                provenance: "no-op".into(),
                proof: "no-op-proof".into(),
                staged_locator: Some("staged-locator".into()),
                staged_proof: None,
            },
            true,
        );
        assert_eq!(
            incomplete.disposition,
            ConnectorHistoricalCtasDisposition::Ambiguous
        );

        let fake = FakeCatalog::with_inspect(InspectCtasTargetResponse::NoOp {
            provenance: "no-op".into(),
            proof: "no-op-proof".into(),
            staged_locator: Some("staged-locator".into()),
            staged_proof: Some("stage-proof".into()),
        });
        let recovery = recovery(Arc::clone(&fake));
        let descriptor = descriptor_with_policy(true, CreatePolicy::FailIfExists);
        let observation = recovery
            .inspect(descriptor.clone(), context())
            .expect("policy-conflicting inspection remains typed");
        assert_eq!(
            observation.disposition,
            ConnectorHistoricalCtasDisposition::Ambiguous
        );
        assert!(observation.locator.is_none());
        assert!(observation.proof.is_none());
        let cleanup = recovery.cleanup(ConnectorHistoricalCtasCleanupRequest {
            descriptor,
            observation,
            context: context(),
        });
        assert!(cleanup.is_err());
        assert_eq!(fake.abort_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn proof_bound_staged_and_no_op_cleanup_use_catalog_abort_only() {
        for response in [
            InspectCtasTargetResponse::Staged {
                staged_locator: "staged-locator".into(),
                proof: "stage-proof".into(),
            },
            InspectCtasTargetResponse::NoOp {
                provenance: "no-op".into(),
                proof: "no-op-proof".into(),
                staged_locator: Some("staged-locator".into()),
                staged_proof: Some("stage-proof".into()),
            },
        ] {
            let fake = FakeCatalog::with_inspect(response);
            let recovery = recovery(Arc::clone(&fake));
            let descriptor = descriptor(true);
            let observation = recovery
                .inspect(descriptor.clone(), context())
                .expect("inspection");
            let request = ConnectorHistoricalCtasCleanupRequest {
                descriptor,
                observation,
                context: context(),
            };
            let receipt = recovery.cleanup(request).expect("guarded cleanup");
            assert_ne!(receipt.digest(), [0; 32]);
            assert_eq!(fake.abort_calls.load(Ordering::SeqCst), 1);
            let abort = fake
                .last_abort
                .lock()
                .expect("abort request lock")
                .clone()
                .expect("abort request");
            assert_eq!(
                abort.action.action_id,
                uuid::Uuid::from_bytes(action(3).to_bytes()).to_string()
            );
            assert_eq!(abort.action.input_digest, encode_hex([13; 32]));
            assert_eq!(abort.staged_locator, "staged-locator");
            assert_eq!(abort.staged_proof, "stage-proof");
            assert!(abort.provider_payload.is_empty());
        }
    }

    #[test]
    fn published_or_ambiguous_observation_never_calls_cleanup() {
        for response in [
            InspectCtasTargetResponse::Published {
                provenance: "published".into(),
                proof: "published-proof".into(),
            },
            InspectCtasTargetResponse::Ambiguous {
                message: "publish race unresolved".into(),
                proof: Some("race-proof".into()),
            },
        ] {
            let fake = FakeCatalog::with_inspect(response);
            let recovery = recovery(Arc::clone(&fake));
            let descriptor = descriptor(true);
            let observation = recovery
                .inspect(descriptor.clone(), context())
                .expect("inspection");
            let result = recovery.cleanup(ConnectorHistoricalCtasCleanupRequest {
                descriptor,
                observation,
                context: context(),
            });
            assert!(matches!(
                result,
                Err(ConnectorCtasFailure::KnownNotDispatched(_))
            ));
            assert_eq!(fake.abort_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[test]
    fn publish_abort_race_preserves_already_published_without_retry_or_drop() {
        let fake = FakeCatalog::with_inspect(InspectCtasTargetResponse::Staged {
            staged_locator: "staged-locator".into(),
            proof: "stage-proof".into(),
        });
        fake.set_abort(Err(ConnectorCtasFailure::Conflict {
            kind: ConnectorCtasConflictKind::AlreadyPublished,
            failure: mutation_failure(
                ConnectorMutationFailureKind::Conflict,
                "publish won the catalog race",
            ),
        }));
        let recovery = recovery(Arc::clone(&fake));
        let descriptor = descriptor(true);
        let observation = recovery
            .inspect(descriptor.clone(), context())
            .expect("staged observation");
        let result = recovery.cleanup(ConnectorHistoricalCtasCleanupRequest {
            descriptor,
            observation,
            context: context(),
        });
        assert!(matches!(
            result,
            Err(ConnectorCtasFailure::Conflict {
                kind: ConnectorCtasConflictKind::AlreadyPublished,
                ..
            })
        ));
        assert_eq!(fake.abort_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn corrupt_provider_proof_fails_before_catalog_call() {
        let fake = FakeCatalog::with_inspect(InspectCtasTargetResponse::Staged {
            staged_locator: "staged-locator".into(),
            proof: "stage-proof".into(),
        });
        let recovery = recovery(Arc::clone(&fake));
        let descriptor = descriptor(true);
        let inspected = recovery
            .inspect(descriptor.clone(), context())
            .expect("staged observation");
        let locator = inspected.locator.clone().expect("staged locator");
        let corrupt = ConnectorCtasPublicationProof::try_new(
            binding(2),
            &descriptor.fence,
            ConnectorCtasProofPurpose::HistoricalStaged,
            None,
            descriptor.digest(),
            Some(&locator),
            Bytes::from_static(b"{"),
        )
        .expect("opaque but provider-corrupt proof");
        let observation = ConnectorHistoricalCtasObservation::try_new(
            binding(2),
            &descriptor,
            ConnectorHistoricalCtasDisposition::Staged,
            Some(locator),
            Some(corrupt),
            None,
            None,
        )
        .expect("provider-neutral observation accepts opaque proof");
        let result = recovery.cleanup(ConnectorHistoricalCtasCleanupRequest {
            descriptor,
            observation,
            context: context(),
        });
        assert!(matches!(
            result,
            Err(ConnectorCtasFailure::KnownNotDispatched(failure))
                if failure.kind() == ConnectorMutationFailureKind::CorruptData
        ));
        assert_eq!(fake.abort_calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn duplicate_abort_checkpoint_fails_before_catalog_call() {
        for duplicate in [
            ConnectorHistoricalCtasCheckpoint {
                action_id: action(4),
                action: ConnectorHistoricalCtasAction::Abort,
                dispatch: ConnectorHistoricalCtasDispatchState::Unknown,
                input_digest: [14; 32],
                evidence_digest: None,
            },
            ConnectorHistoricalCtasCheckpoint {
                action_id: action(3),
                action: ConnectorHistoricalCtasAction::Abort,
                dispatch: ConnectorHistoricalCtasDispatchState::Completed,
                input_digest: [14; 32],
                evidence_digest: Some([15; 32]),
            },
        ] {
            let fake = FakeCatalog::with_inspect(InspectCtasTargetResponse::Staged {
                staged_locator: "staged-locator".into(),
                proof: "stage-proof".into(),
            });
            let recovery = recovery(Arc::clone(&fake));
            let mut descriptor = descriptor(true);
            descriptor.checkpoints.push(duplicate);
            // Re-seal the deliberately conflicting durable descriptor.
            descriptor = ConnectorHistoricalCtasDescriptor::try_new(
                descriptor.historical_binding.clone(),
                descriptor.fence.clone(),
                descriptor.fence_receipt_digest,
                descriptor.target_digest,
                descriptor.create_policy,
                descriptor.locator.clone(),
                descriptor.checkpoints.clone(),
                descriptor.evidence.clone(),
            )
            .expect("descriptor with duplicate abort records");
            let observation = recovery
                .inspect(descriptor.clone(), context())
                .expect("staged observation");
            let result = recovery.cleanup(ConnectorHistoricalCtasCleanupRequest {
                descriptor,
                observation,
                context: context(),
            });
            assert!(matches!(
                result,
                Err(ConnectorCtasFailure::KnownNotDispatched(_))
            ));
            assert_eq!(fake.abort_calls.load(Ordering::SeqCst), 0);
        }
    }
}
