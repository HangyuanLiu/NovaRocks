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

//! Iceberg REST catalog-native fenced CTAS publication.
//!
//! The external extension is the durable stage/publish/abort authority. This
//! adapter keeps only exact-generation writer objects and never treats its
//! process-local cache as historical truth.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bytes::Bytes;
use novarocks_spi::connector::{
    CONNECTOR_CTAS_STAGED_PUBLICATION_CONTRACT_VERSION, ConnectorCtasAbortDisposition,
    ConnectorCtasAbortRequest, ConnectorCtasAbortResult, ConnectorCtasActionId,
    ConnectorCtasAdvanceFenceRequest, ConnectorCtasConflictKind, ConnectorCtasFailure,
    ConnectorCtasProofPurpose, ConnectorCtasPublicationFence, ConnectorCtasPublicationFenceReceipt,
    ConnectorCtasPublicationProof, ConnectorCtasPublicationReceipt,
    ConnectorCtasPublishDisposition, ConnectorCtasPublishRequest, ConnectorCtasPublishResult,
    ConnectorCtasStageRequest, ConnectorCtasStageResult, ConnectorCtasStagedLocator,
    ConnectorCtasStagedPublication, ConnectorCtasStagedPublicationCapability, ConnectorError,
    ConnectorErrorKind, ConnectorExecutionBindingKey, ConnectorInstanceDescriptor,
    ConnectorInstanceIncarnation, ConnectorMutationFailure, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorStagedTableHandle, ConnectorStagedWritePlanningBinding,
    ConnectorStagedWritePlanningRequest, ConnectorWriteOperationCompletion, CreatePolicy,
};
use novarocks_types::naming::normalize_identifier;
use sha2::{Digest, Sha256};

use super::staged_create::{IcebergStagedCreateAdapter, RestStagedTableCreate};
use crate::commit::IcebergWriteControl;
use crate::control_provider::IcebergControlProvider;
use crate::control_runtime::IcebergControlRuntime;
use crate::iceberg::{NamespaceIdent, TableCreation, TableUpdate};
use crate::iceberg_catalog_rest::{
    AbortCtasTargetRequest, AdvanceCtasFenceRequest, CtasCreatePolicy, CtasFencedAction,
    CtasFencedGeneration, CtasFencedOperation, CtasFencedPublicationConflictKind,
    CtasFencedPublicationError, PublishCtasTargetRequest, PublishCtasTargetResponse,
    StageCtasTargetRequest, StageCtasTargetResponse,
};

pub struct IcebergCtasFencedPublication {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
    runtime: Arc<IcebergControlRuntime>,
    foreground: IcebergStagedCreateAdapter,
    stage_proofs: Mutex<HashMap<ConnectorCtasActionId, String>>,
    action_payloads: Mutex<HashMap<ConnectorCtasActionId, CachedActionPayload>>,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum CachedActionKind {
    Publish,
    Abort,
}

struct CachedActionPayload {
    kind: CachedActionKind,
    input_digest: [u8; 32],
    payload: String,
}

#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
enum IcebergCtasDownstreamAction {
    IcebergPublishV1 {
        action: serde_json::Value,
        #[serde(rename = "data-prefixes")]
        data_prefixes: Vec<String>,
        objects: Vec<String>,
    },
    IcebergCleanupV1 {
        #[serde(rename = "data-prefixes")]
        data_prefixes: Vec<String>,
        objects: Vec<String>,
    },
}

impl IcebergCtasFencedPublication {
    pub fn try_new(
        provider: Arc<IcebergControlProvider>,
        write_control: Arc<IcebergWriteControl>,
    ) -> Result<Self, ConnectorError> {
        if !provider.runtime().has_ctas_fenced_publication() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "Iceberg catalog does not advertise fenced CTAS staged publication v1",
            ));
        }
        let foreground = IcebergStagedCreateAdapter::try_new(Arc::clone(&provider), write_control)?;
        Ok(Self {
            descriptor: provider.descriptor().clone(),
            incarnation: provider.incarnation(),
            runtime: Arc::clone(provider.runtime()),
            foreground,
            stage_proofs: Mutex::new(HashMap::new()),
            action_payloads: Mutex::new(HashMap::new()),
        })
    }

    fn owner(&self) -> ConnectorExecutionBindingKey {
        ConnectorExecutionBindingKey {
            instance_id: self.descriptor.instance_id.clone(),
            incarnation: self.incarnation,
        }
    }

    fn rest_catalog(
        &self,
    ) -> Result<Arc<crate::iceberg_catalog_rest::RestCatalog>, ConnectorCtasFailure> {
        self.runtime.rest_catalog().cloned().ok_or_else(|| {
            known_not_dispatched(
                ConnectorMutationFailureKind::Unsupported,
                "Iceberg CTAS fenced publication requires the exact REST catalog generation",
            )
        })
    }

    fn validate_context(
        context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<(), ConnectorCtasFailure> {
        if context.cancellation().is_cancelled() {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::Cancelled,
                "Iceberg CTAS fenced publication request was cancelled",
            ));
        }
        if Instant::now() >= context.deadline() {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::DeadlineExceeded,
                "Iceberg CTAS fenced publication request deadline elapsed",
            ));
        }
        Ok(())
    }

    fn build_staged_creation(
        &self,
        request: &ConnectorCtasStageRequest,
    ) -> Result<(NamespaceIdent, TableCreation, HashMap<String, String>), ConnectorCtasFailure>
    {
        if !request.provider_payload.is_empty() {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::InvalidRequest,
                "Iceberg CTAS stage does not accept an application-authored provider payload",
            ));
        }
        let namespace_name = normalize_identifier(&request.definition.table.namespace)
            .map_err(|error| local_invalid(error.to_string()))?;
        let table_name = normalize_identifier(&request.definition.table.table)
            .map_err(|error| local_invalid(error.to_string()))?;
        let mut properties = request.definition.properties.clone();
        properties.retain(|key, _| !key.eq_ignore_ascii_case("novarocks.ctas.operation-id"));
        properties.insert(
            Arc::from("novarocks.ctas.operation-id"),
            Arc::from(uuid_string(request.fence.operation_id().to_bytes())),
        );
        let property_pairs = properties.into_iter().collect::<Vec<_>>();
        let (format_version, mut properties) = super::catalog_mutation::table_properties(
            &request.definition.columns,
            None,
            &property_pairs,
        )
        .map_err(local_connector)?;
        if format_version != crate::iceberg::spec::FormatVersion::V3
            && request.definition.columns.iter().any(|column| {
                column.default.as_ref().is_some_and(|value| {
                    !matches!(value, novarocks_spi::connector::ConnectorDefaultValue::Null)
                })
            })
        {
            return Err(known_not_dispatched(
                ConnectorMutationFailureKind::InvalidRequest,
                "Iceberg column defaults require format-version 3",
            ));
        }
        let schema = crate::iceberg::spec::Schema::builder()
            .with_fields(
                super::type_mapping::schema_fields(&request.definition.columns)
                    .map_err(local_invalid)?,
            )
            .build()
            .map_err(|error| local_invalid(format!("build staged Iceberg schema: {error}")))?;
        let partition_spec = super::catalog_mutation::initial_partition_spec(
            &schema,
            &request.definition.partitioning,
        )
        .map_err(local_invalid)?;
        properties.insert(
            "format-version".to_string(),
            (format_version as u8).to_string(),
        );
        let publication_properties = properties
            .iter()
            .filter(|(key, _)| !key.eq_ignore_ascii_case("format-version"))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<HashMap<_, _>>();
        let creation = TableCreation::builder()
            .name(table_name)
            .schema(schema)
            .properties(properties)
            .format_version(format_version);
        let creation = if let Some(spec) = partition_spec {
            creation.partition_spec(spec).build()
        } else {
            creation.build()
        };
        Ok((
            NamespaceIdent::new(namespace_name),
            creation,
            publication_properties,
        ))
    }

    fn stage_proof(
        &self,
        action_id: ConnectorCtasActionId,
    ) -> Result<String, ConnectorCtasFailure> {
        self.stage_proofs
            .lock()
            .map_err(|error| {
                local_internal(format!(
                    "Iceberg CTAS stage proof lock is poisoned: {error}"
                ))
            })?
            .get(&action_id)
            .cloned()
            .ok_or_else(|| {
                known_not_dispatched(
                    ConnectorMutationFailureKind::InvalidRequest,
                    "Iceberg CTAS foreground stage proof is unavailable in this generation",
                )
            })
    }

    fn cache_stage_proof(
        &self,
        action_id: ConnectorCtasActionId,
        proof: String,
    ) -> Result<(), ConnectorCtasFailure> {
        let mut proofs = self.stage_proofs.lock().map_err(|error| {
            committed_response_invalid(format!(
                "Iceberg CTAS stage proof lock is poisoned: {error}"
            ))
        })?;
        if let Some(existing) = proofs.get(&action_id) {
            if existing != &proof {
                return Err(committed_response_invalid(
                    "Iceberg CTAS exact stage replay returned a different proof",
                ));
            }
            return Ok(());
        }
        proofs.insert(action_id, proof);
        Ok(())
    }

    fn cached_or_build_action_payload(
        &self,
        action_id: ConnectorCtasActionId,
        input_digest: [u8; 32],
        kind: CachedActionKind,
        build: impl FnOnce() -> Result<String, ConnectorCtasFailure>,
    ) -> Result<String, ConnectorCtasFailure> {
        cached_or_build_action_payload(&self.action_payloads, action_id, input_digest, kind, build)
    }
}

fn cached_or_build_action_payload(
    action_payloads: &Mutex<HashMap<ConnectorCtasActionId, CachedActionPayload>>,
    action_id: ConnectorCtasActionId,
    input_digest: [u8; 32],
    kind: CachedActionKind,
    build: impl FnOnce() -> Result<String, ConnectorCtasFailure>,
) -> Result<String, ConnectorCtasFailure> {
    let mut payloads = action_payloads.lock().map_err(|error| {
        local_internal(format!(
            "Iceberg CTAS action payload lock is poisoned: {error}"
        ))
    })?;
    if let Some(cached) = payloads.get(&action_id) {
        if cached.kind != kind || cached.input_digest != input_digest {
            return Err(local_invalid(
                "Iceberg CTAS action id was reused with a different purpose or input digest",
            ));
        }
        return Ok(cached.payload.clone());
    }
    let payload = build()?;
    payloads.insert(
        action_id,
        CachedActionPayload {
            kind,
            input_digest,
            payload: payload.clone(),
        },
    );
    Ok(payload)
}

impl ConnectorCtasStagedPublication for IcebergCtasFencedPublication {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
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
                "Iceberg CTAS advance-fence request names another connector instance",
            ));
        }
        Self::validate_context(&request.context)?;
        let rest = self.rest_catalog()?;
        let wire = AdvanceCtasFenceRequest {
            action: wire_action(&request.fence, request.action_id, request.input_digest)?,
        };
        let response = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                rest.require_ctas_fenced_publication()
                    .await?
                    .advance_fence(&wire)
                    .await
            })
            .map_err(local_possibly_dispatched)?
            .map_err(rest_failure)?;
        if response.generation != wire_generation(request.fence.generation())
            || response.input_digest != encode_hex(request.input_digest)
        {
            return Err(committed_response_invalid(
                "Iceberg CTAS advance-fence response drifted from the requested generation",
            ));
        }
        ConnectorCtasPublicationFenceReceipt::try_new(&request, Bytes::from(response.receipt))
            .map_err(local_committed_response)
    }

    fn stage(
        &self,
        request: ConnectorCtasStageRequest,
    ) -> Result<ConnectorCtasStageResult, ConnectorCtasFailure> {
        request
            .validate_for(&self.owner())
            .map_err(local_known_not_dispatched)?;
        Self::validate_context(&request.context)?;
        let rest = self.rest_catalog()?;
        let (namespace, creation, publication_properties) = self.build_staged_creation(&request)?;
        let rest_for_payload = Arc::clone(&rest);
        let provider_payload = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                rest_for_payload
                    .encode_ctas_stage_provider_payload(&namespace, creation)
                    .await
            })
            .map_err(local_internal)?
            .map_err(|error| local_invalid(error.to_string()))?;
        let table_ident = wire_table_ident(&request.fence)?;
        let wire = StageCtasTargetRequest {
            action: wire_action(&request.fence, request.action_id, request.input_digest)?,
            staged_identity: encode_hex(request.initialization_digest),
            initialization_digest: encode_hex(request.initialization_digest),
            create_policy_digest: create_policy_digest(request.create_policy),
            create_policy: wire_create_policy(request.create_policy),
            provider_payload,
        };
        let rest_for_stage = Arc::clone(&rest);
        let response = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                rest_for_stage
                    .require_ctas_fenced_publication()
                    .await?
                    .stage(&wire)
                    .await
            })
            .map_err(local_possibly_dispatched)?
            .map_err(rest_failure)?;
        let handle_payload = stage_handle_payload(&response)?;
        let staged_table = response.staged_table.clone();
        let rest_for_materialize = Arc::clone(&rest);
        let staged = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                rest_for_materialize
                    .materialize_ctas_staged_table(table_ident, staged_table)
                    .await
            })
            .map_err(committed_response_invalid)?
            .map_err(|error| {
                committed_response_invalid(format!(
                    "materialize Iceberg fenced CTAS staged table: {error}"
                ))
            })?;
        let (table, mut initialization_updates) = staged.into_parts();
        if !publication_properties.is_empty() {
            initialization_updates.push(TableUpdate::SetProperties {
                updates: publication_properties,
            });
        }
        let operation_id = ConnectorMutationOperationId::from_bytes(request.action_id.to_bytes());
        let handle = self
            .foreground
            .register_fenced_stage(
                operation_id,
                RestStagedTableCreate {
                    catalog: rest,
                    table,
                    initialization_updates,
                },
                request.create_policy,
                handle_payload,
            )
            .map_err(local_committed_response)?;
        self.cache_stage_proof(request.action_id, response.staged_proof.clone())?;
        let locator = ConnectorCtasStagedLocator::try_new(
            self.owner(),
            &request.fence,
            request.action_id,
            request.target_digest,
            Bytes::from(response.staged_locator.clone()),
        )
        .map_err(local_committed_response)?;
        let receipt = ConnectorCtasPublicationReceipt::try_new(
            &request.fence,
            request.action_id,
            request.input_digest,
            Bytes::from(response.staged_locator),
        )
        .map_err(local_committed_response)?;
        let proof = ConnectorCtasPublicationProof::try_new(
            self.owner(),
            &request.fence,
            ConnectorCtasProofPurpose::Stage,
            Some(request.action_id),
            request.input_digest,
            Some(&locator),
            Bytes::from(response.staged_proof),
        )
        .map_err(local_committed_response)?;
        ConnectorCtasStageResult::try_new(&request, locator, handle, receipt, proof)
            .map_err(local_committed_response)
    }

    fn plan_write(
        &self,
        request: ConnectorStagedWritePlanningRequest,
    ) -> Result<ConnectorStagedWritePlanningBinding, ConnectorCtasFailure> {
        self.foreground
            .plan_fenced_write(request)
            .map_err(local_known_not_dispatched)
    }

    fn bind_write(
        &self,
        handle: ConnectorStagedTableHandle,
        completion: ConnectorWriteOperationCompletion,
    ) -> Result<(), ConnectorCtasFailure> {
        self.foreground
            .bind_fenced_write(handle, completion)
            .map_err(local_known_not_dispatched)
    }

    fn publish(
        &self,
        request: ConnectorCtasPublishRequest,
    ) -> Result<ConnectorCtasPublishResult, ConnectorCtasFailure> {
        request
            .validate_for(&self.owner())
            .map_err(local_known_not_dispatched)?;
        Self::validate_context(&request.context)?;
        let stage_action = request.locator.stage_action_id();
        let operation_id = ConnectorMutationOperationId::from_bytes(stage_action.to_bytes());
        let rest = self.rest_catalog()?;
        let provider_payload = self.cached_or_build_action_payload(
            request.action_id,
            request.input_digest,
            CachedActionKind::Publish,
            || {
                let commit = self
                    .foreground
                    .fenced_publish_commit(operation_id, request.write_completion_digest)
                    .map_err(local_known_not_dispatched)?;
                let rest_for_payload = Arc::clone(&rest);
                let payload = self
                    .runtime
                    .resources()
                    .catalog_runtime()
                    .block_on(async move {
                        rest_for_payload
                            .encode_ctas_publish_provider_payload(commit)
                            .await
                    })
                    .map_err(local_internal)?
                    .map_err(|error| local_invalid(error.to_string()))?;
                let action = serde_json::from_str(&payload).map_err(|error| {
                    local_internal(format!(
                        "decode Iceberg fenced CTAS publish action for durable cleanup binding: {error}"
                    ))
                })?;
                let cleanup = self
                    .foreground
                    .fenced_cleanup_action(operation_id)
                    .map_err(local_known_not_dispatched)?;
                let payload = serde_json::to_string(
                    &IcebergCtasDownstreamAction::IcebergPublishV1 {
                        action,
                        data_prefixes: cleanup.data_prefixes,
                        objects: cleanup.objects,
                    },
                )
                .map_err(|error| {
                    local_internal(format!(
                        "encode Iceberg fenced CTAS publish and cleanup action: {error}"
                    ))
                })?;
                validate_provider_action_size(&payload)?;
                Ok(payload)
            },
        )?;
        let wire = PublishCtasTargetRequest {
            action: wire_action(&request.fence, request.action_id, request.input_digest)?,
            staged_locator: opaque_text("staged locator", request.locator.payload())?,
            staged_proof: self.stage_proof(stage_action)?,
            write_completion_digest: encode_hex(request.write_completion_digest),
            create_policy_digest: create_policy_digest(request.create_policy),
            create_policy: wire_create_policy(request.create_policy),
            provider_payload,
        };
        let response = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                rest.require_ctas_fenced_publication()
                    .await?
                    .publish(&wire)
                    .await
            })
            .map_err(local_possibly_dispatched)?
            .map_err(rest_failure)?;
        let (disposition, purpose, provenance, proof_payload) = match response {
            PublishCtasTargetResponse::Published { provenance, proof } => {
                if let Err(error) = self.foreground.finish_fenced_published(operation_id) {
                    return Err(local_committed_response(error));
                }
                (
                    ConnectorCtasPublishDisposition::Published,
                    ConnectorCtasProofPurpose::PublishPublished,
                    provenance,
                    proof,
                )
            }
            PublishCtasTargetResponse::NoOp { provenance, proof } => (
                ConnectorCtasPublishDisposition::NoOp,
                ConnectorCtasProofPurpose::PublishNoOp,
                provenance,
                proof,
            ),
        };
        let receipt = ConnectorCtasPublicationReceipt::try_new(
            &request.fence,
            request.action_id,
            request.input_digest,
            Bytes::from(provenance),
        )
        .map_err(local_committed_response)?;
        let proof = ConnectorCtasPublicationProof::try_new(
            self.owner(),
            &request.fence,
            purpose,
            Some(request.action_id),
            request.input_digest,
            Some(&request.locator),
            Bytes::from(proof_payload),
        )
        .map_err(local_committed_response)?;
        ConnectorCtasPublishResult::try_new(&request, disposition, receipt, proof)
            .map_err(local_committed_response)
    }

    fn abort(
        &self,
        request: ConnectorCtasAbortRequest,
    ) -> Result<ConnectorCtasAbortResult, ConnectorCtasFailure> {
        request
            .validate_for(&self.owner())
            .map_err(local_known_not_dispatched)?;
        Self::validate_context(&request.context)?;
        let operation_id =
            ConnectorMutationOperationId::from_bytes(request.locator.stage_action_id().to_bytes());
        let rest = self.rest_catalog()?;
        let provider_payload = self.cached_or_build_action_payload(
            request.action_id,
            request.input_digest,
            CachedActionKind::Abort,
            || {
                let cleanup = self
                    .foreground
                    .fenced_cleanup_action(operation_id)
                    .map_err(local_known_not_dispatched)?;
                let payload =
                    serde_json::to_string(&IcebergCtasDownstreamAction::IcebergCleanupV1 {
                        data_prefixes: cleanup.data_prefixes,
                        objects: cleanup.objects,
                    })
                    .map_err(|error| {
                        local_internal(format!(
                            "encode Iceberg fenced CTAS cleanup action: {error}"
                        ))
                    })?;
                validate_provider_action_size(&payload)?;
                Ok(payload)
            },
        )?;
        let wire = AbortCtasTargetRequest {
            action: wire_action(&request.fence, request.action_id, request.input_digest)?,
            staged_locator: opaque_text("staged locator", request.locator.payload())?,
            staged_proof: opaque_text("staged proof", request.proof.payload())?,
            provider_payload,
        };
        let response = self
            .runtime
            .resources()
            .catalog_runtime()
            .block_on(async move {
                rest.require_ctas_fenced_publication()
                    .await?
                    .abort(&wire)
                    .await
            })
            .map_err(local_possibly_dispatched)?
            .map_err(rest_failure)?;
        self.foreground
            .finish_fenced_aborted(operation_id)
            .map_err(local_committed_response)?;
        let receipt = ConnectorCtasPublicationReceipt::try_new(
            &request.fence,
            request.action_id,
            request.input_digest,
            Bytes::from(response.provenance),
        )
        .map_err(local_committed_response)?;
        let proof = ConnectorCtasPublicationProof::try_new(
            self.owner(),
            &request.fence,
            ConnectorCtasProofPurpose::AbortAborted,
            Some(request.action_id),
            request.input_digest,
            Some(&request.locator),
            Bytes::from(response.proof),
        )
        .map_err(local_committed_response)?;
        ConnectorCtasAbortResult::try_new(
            &request,
            ConnectorCtasAbortDisposition::Aborted,
            receipt,
            proof,
        )
        .map_err(local_committed_response)
    }
}

fn wire_operation(
    fence: &ConnectorCtasPublicationFence,
) -> Result<CtasFencedOperation, ConnectorCtasFailure> {
    Ok(CtasFencedOperation {
        cluster_id: encode_hex(fence.cluster().digest()),
        operation_id: uuid_string(fence.operation_id().to_bytes()),
        target: wire_table_ident(fence)?,
    })
}

fn wire_table_ident(
    fence: &ConnectorCtasPublicationFence,
) -> Result<crate::iceberg::TableIdent, ConnectorCtasFailure> {
    crate::iceberg::TableIdent::from_strs([&fence.target().namespace, &fence.target().table])
        .map_err(|error| local_invalid(format!("build Iceberg CTAS target: {error}")))
}

fn wire_action(
    fence: &ConnectorCtasPublicationFence,
    action_id: ConnectorCtasActionId,
    input_digest: [u8; 32],
) -> Result<CtasFencedAction, ConnectorCtasFailure> {
    Ok(CtasFencedAction {
        operation: wire_operation(fence)?,
        generation: wire_generation(fence.generation()),
        action_id: uuid_string(action_id.to_bytes()),
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

fn wire_create_policy(policy: CreatePolicy) -> CtasCreatePolicy {
    match policy {
        CreatePolicy::FailIfExists => CtasCreatePolicy::FailIfExists,
        CreatePolicy::NoOpIfExists => CtasCreatePolicy::NoOpIfExists,
    }
}

fn create_policy_digest(policy: CreatePolicy) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg-ctas-create-policy.v1\0");
    hasher.update([match policy {
        CreatePolicy::FailIfExists => 1,
        CreatePolicy::NoOpIfExists => 2,
    }]);
    encode_hex(hasher.finalize())
}

fn stage_handle_payload(response: &StageCtasTargetResponse) -> Result<Bytes, ConnectorCtasFailure> {
    let staged_table = serde_json::to_vec(&response.staged_table).map_err(|error| {
        committed_response_invalid(format!(
            "encode Iceberg CTAS staged table handle identity: {error}"
        ))
    })?;
    let mut hasher = Sha256::new();
    hasher.update(b"novarocks.iceberg-ctas-stage-handle.v1\0");
    for field in [
        response.staged_locator.as_bytes(),
        response.staged_proof.as_bytes(),
        staged_table.as_slice(),
    ] {
        hasher.update((field.len() as u64).to_be_bytes());
        hasher.update(field);
    }
    Ok(Bytes::copy_from_slice(&hasher.finalize()))
}

fn validate_provider_action_size(payload: &str) -> Result<(), ConnectorCtasFailure> {
    if payload.len() > 48 * 1024 {
        return Err(local_invalid(
            "Iceberg fenced CTAS provider action exceeds the 48 KiB provider limit",
        ));
    }
    Ok(())
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

fn uuid_string(bytes: [u8; 16]) -> String {
    uuid::Uuid::from_bytes(bytes).to_string()
}

fn opaque_text(name: &str, payload: &Bytes) -> Result<String, ConnectorCtasFailure> {
    std::str::from_utf8(payload)
        .map(str::to_owned)
        .map_err(|error| {
            known_not_dispatched(
                ConnectorMutationFailureKind::CorruptData,
                format!("Iceberg CTAS {name} is not valid UTF-8: {error}"),
            )
        })
}

fn rest_failure(error: CtasFencedPublicationError) -> ConnectorCtasFailure {
    match error {
        CtasFencedPublicationError::Unsupported(error) => {
            ConnectorCtasFailure::Ambiguous(mutation_failure(
                ConnectorMutationFailureKind::Unsupported,
                format!("advertised Iceberg CTAS extension became unsupported: {error}"),
            ))
        }
        CtasFencedPublicationError::Conflict { kind, error } => ConnectorCtasFailure::Conflict {
            kind: match kind {
                CtasFencedPublicationConflictKind::StaleFence => {
                    ConnectorCtasConflictKind::StaleFence
                }
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
            },
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
        CtasFencedPublicationError::Ambiguous(error) => ConnectorCtasFailure::Ambiguous(
            mutation_failure(ConnectorMutationFailureKind::Unavailable, error.to_string()),
        ),
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

fn committed_response_invalid(message: impl Into<Arc<str>>) -> ConnectorCtasFailure {
    ConnectorCtasFailure::CommittedResponseInvalid(mutation_failure(
        ConnectorMutationFailureKind::CorruptData,
        message,
    ))
}

fn local_invalid(message: impl Into<Arc<str>>) -> ConnectorCtasFailure {
    known_not_dispatched(ConnectorMutationFailureKind::InvalidRequest, message)
}

fn local_internal(message: impl Into<Arc<str>>) -> ConnectorCtasFailure {
    known_not_dispatched(ConnectorMutationFailureKind::Internal, message)
}

fn local_possibly_dispatched(message: impl Into<Arc<str>>) -> ConnectorCtasFailure {
    ConnectorCtasFailure::PossiblyDispatched(mutation_failure(
        ConnectorMutationFailureKind::Internal,
        message,
    ))
}

fn local_connector(error: ConnectorError) -> ConnectorCtasFailure {
    local_known_not_dispatched(error)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn rest_error(message: &str) -> crate::iceberg::Error {
        crate::iceberg::Error::new(crate::iceberg::ErrorKind::Unexpected, message)
    }

    #[test]
    fn rest_failures_preserve_dispatch_and_conflict_classification() {
        let conflict = rest_failure(CtasFencedPublicationError::Conflict {
            kind: CtasFencedPublicationConflictKind::DigestConflict,
            error: rest_error("digest drift"),
        });
        assert!(matches!(
            conflict,
            ConnectorCtasFailure::Conflict {
                kind: ConnectorCtasConflictKind::DigestConflict,
                ..
            }
        ));
        assert!(matches!(
            rest_failure(CtasFencedPublicationError::KnownNotDispatched(rest_error(
                "not sent"
            ))),
            ConnectorCtasFailure::KnownNotDispatched(_)
        ));
        assert!(matches!(
            rest_failure(CtasFencedPublicationError::PossiblyDispatched(rest_error(
                "reply lost"
            ))),
            ConnectorCtasFailure::PossiblyDispatched(_)
        ));
        assert!(matches!(
            rest_failure(CtasFencedPublicationError::CommittedResponseInvalid(
                rest_error("bad success")
            )),
            ConnectorCtasFailure::CommittedResponseInvalid(_)
        ));
    }

    #[test]
    fn advertised_endpoint_becoming_unsupported_is_ambiguous() {
        assert!(matches!(
            rest_failure(CtasFencedPublicationError::Unsupported(rest_error(
                "extension disappeared"
            ))),
            ConnectorCtasFailure::Ambiguous(_)
        ));
    }

    #[test]
    fn exact_action_payload_replay_reuses_the_first_bounded_payload() {
        let payloads = Mutex::new(HashMap::new());
        let action_id = ConnectorCtasActionId::try_from_bytes(*uuid::Uuid::now_v7().as_bytes())
            .expect("action id");
        let first = cached_or_build_action_payload(
            &payloads,
            action_id,
            [7; 32],
            CachedActionKind::Publish,
            || Ok("first".to_string()),
        )
        .expect("first payload");
        let replay = cached_or_build_action_payload(
            &payloads,
            action_id,
            [7; 32],
            CachedActionKind::Publish,
            || panic!("exact replay must not rebuild its provider payload"),
        )
        .expect("replay payload");
        assert_eq!(first, replay);
        assert!(
            cached_or_build_action_payload(
                &payloads,
                action_id,
                [8; 32],
                CachedActionKind::Publish,
                || Ok("drift".to_string()),
            )
            .is_err()
        );
        assert!(
            cached_or_build_action_payload(
                &payloads,
                action_id,
                [7; 32],
                CachedActionKind::Abort,
                || Ok("drift".to_string()),
            )
            .is_err()
        );
    }

    #[test]
    fn stage_handle_and_cleanup_actions_are_stable_and_typed() {
        let response = StageCtasTargetResponse {
            staged_locator: "locator".to_string(),
            staged_proof: "proof".to_string(),
            staged_table: serde_json::json!({"metadata": {"location": "s3://bucket/table"}}),
        };
        assert_eq!(
            stage_handle_payload(&response).expect("first handle"),
            stage_handle_payload(&response).expect("replay handle")
        );
        let publish = serde_json::to_value(IcebergCtasDownstreamAction::IcebergPublishV1 {
            action: serde_json::json!({"method":"POST","path":"/v1/tables"}),
            data_prefixes: vec!["s3://bucket/table/data/_staging/action/".to_string()],
            objects: vec!["s3://bucket/table/metadata/manifest.avro".to_string()],
        })
        .expect("publish action");
        assert_eq!(publish["kind"], "iceberg-publish-v1");
        assert!(publish["data-prefixes"].is_array());
        let cleanup = serde_json::to_value(IcebergCtasDownstreamAction::IcebergCleanupV1 {
            data_prefixes: vec!["s3://bucket/table/data/_staging/action/".to_string()],
            objects: Vec::new(),
        })
        .expect("cleanup action");
        assert_eq!(cleanup["kind"], "iceberg-cleanup-v1");
    }
}
