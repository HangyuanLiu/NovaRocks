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

//! Versioned catalog-native CTAS fenced staged-publication extension.

use std::fmt;

use iceberg::{Error, ErrorKind, Result, TableIdent};
use reqwest::{Method, StatusCode};
use serde::{Serialize, de::DeserializeOwned};
use serde_derive::Deserialize;

use super::RestCatalog;

/// `/v1/config` property advertising catalog-native CTAS fencing.
pub const CTAS_FENCED_PUBLICATION_CAPABILITY: &str = "fenced-staged-publication";
/// Wire version understood by this client.
pub const CTAS_FENCED_PUBLICATION_VERSION: &str = "1";

const EXTENSION_PATH: &str = "extensions/fenced-staged-publication";
const MAX_REQUEST_BYTES: usize = 64 * 1024;
const MAX_RESPONSE_BYTES: usize = 64 * 1024;

/// An advertised catalog-native CTAS publication capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CtasFencedPublicationCapability {
    /// Exact protocol version advertised by the server.
    pub protocol_version: &'static str,
}

/// Stable identity of one CTAS operation and destination.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct CtasFencedOperation {
    /// Stable identity of the NovaRocks cluster that owns the operation.
    pub cluster_id: String,
    /// Stable top-level CTAS saga identity.
    pub operation_id: String,
    /// Destination table bound to the operation.
    pub target: TableIdent,
}

/// Common identity and fencing fields carried by every mutating request.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct CtasFencedAction {
    /// Stable CTAS operation and destination.
    pub operation: CtasFencedOperation,
    /// Monotonically ordered external ownership generation.
    pub generation: u64,
    /// Stable child action identity within the operation.
    pub action_id: String,
    /// Digest sealing every semantic input to this action.
    pub input_digest: String,
}

/// Establish or replay the current catalog fence.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct AdvanceCtasFenceRequest {
    /// Fenced action identity.
    pub action: CtasFencedAction,
}

/// Durable receipt proving the established catalog generation.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct AdvanceCtasFenceResponse {
    /// Established generation.
    pub generation: u64,
    /// Digest bound to the established generation.
    pub input_digest: String,
    /// Bounded opaque catalog receipt.
    pub receipt: String,
}

impl fmt::Debug for AdvanceCtasFenceResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AdvanceCtasFenceResponse")
            .field("generation", &self.generation)
            .field("input_digest", &self.input_digest)
            .field("receipt", &redacted_len(&self.receipt))
            .finish()
    }
}

/// Register an invisible staged target under the current fence.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct StageCtasTargetRequest {
    /// Fenced action identity.
    pub action: CtasFencedAction,
    /// Provider-defined stable staged identity.
    pub staged_identity: String,
    /// Digest of the authoritative staged initialization.
    pub initialization_digest: String,
    /// Digest of the destination create policy.
    pub create_policy_digest: String,
    /// Bounded provider payload interpreted only by the catalog extension.
    pub provider_payload: String,
}

impl fmt::Debug for StageCtasTargetRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StageCtasTargetRequest")
            .field("action", &self.action)
            .field("staged_identity", &self.staged_identity)
            .field("initialization_digest", &self.initialization_digest)
            .field("create_policy_digest", &self.create_policy_digest)
            .field("provider_payload", &redacted_len(&self.provider_payload))
            .finish()
    }
}

/// Durable proof and locator for an invisible staged target.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct StageCtasTargetResponse {
    /// Provider-opaque locator usable after client restart.
    pub staged_locator: String,
    /// Provider-opaque proof binding the locator to this operation.
    pub staged_proof: String,
}

impl fmt::Debug for StageCtasTargetResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StageCtasTargetResponse")
            .field("staged_locator", &redacted_len(&self.staged_locator))
            .field("staged_proof", &redacted_len(&self.staged_proof))
            .finish()
    }
}

/// Inspect the current catalog-authoritative state without replaying a write.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct InspectCtasTargetRequest {
    /// Stable CTAS operation and destination.
    pub operation: CtasFencedOperation,
    /// Generation whose ownership must be inspected.
    pub generation: u64,
    /// Digest of the expected operation lineage.
    pub input_digest: String,
}

/// Catalog-authoritative CTAS visibility and cleanup state.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "kebab-case")]
pub enum InspectCtasTargetResponse {
    /// The catalog proves that the fenced stage action was never accepted.
    NotCreated {
        /// Bounded catalog proof for the negative observation.
        proof: String,
    },
    /// An unpublished staged target exists.
    Staged {
        /// Durable provider-opaque staged locator.
        #[serde(rename = "staged-locator")]
        staged_locator: String,
        /// Proof binding the staged target to this saga and generation.
        proof: String,
    },
    /// Publication completed and is visible.
    Published {
        /// Bounded terminal publication provenance.
        provenance: String,
        /// Proof binding the visible target to this saga.
        proof: String,
    },
    /// Guarded abort completed.
    Aborted {
        /// Bounded terminal abort provenance.
        provenance: String,
        /// Proof binding the abort to this saga.
        proof: String,
    },
    /// The requested identity conflicts with catalog truth.
    Conflict {
        /// Typed conflict category.
        kind: CtasFencedPublicationConflictKind,
        /// Bounded diagnostic message.
        message: String,
    },
    /// Catalog truth cannot be determined safely.
    Ambiguous {
        /// Bounded opaque evidence for operator-assisted recovery.
        proof: String,
    },
    /// The catalog record uses an unsupported protocol or operation.
    Unsupported {
        /// Bounded diagnostic message.
        message: String,
    },
}

impl fmt::Debug for InspectCtasTargetResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotCreated { proof } => f
                .debug_struct("NotCreated")
                .field("proof", &redacted_len(proof))
                .finish(),
            Self::Staged {
                staged_locator,
                proof,
            } => f
                .debug_struct("Staged")
                .field("staged_locator", &redacted_len(staged_locator))
                .field("proof", &redacted_len(proof))
                .finish(),
            Self::Published { provenance, proof } => f
                .debug_struct("Published")
                .field("provenance", &redacted_len(provenance))
                .field("proof", &redacted_len(proof))
                .finish(),
            Self::Aborted { provenance, proof } => f
                .debug_struct("Aborted")
                .field("provenance", &redacted_len(provenance))
                .field("proof", &redacted_len(proof))
                .finish(),
            Self::Conflict { kind, message } => f
                .debug_struct("Conflict")
                .field("kind", kind)
                .field("message", message)
                .finish(),
            Self::Ambiguous { proof } => f
                .debug_struct("Ambiguous")
                .field("proof", &redacted_len(proof))
                .finish(),
            Self::Unsupported { message } => f
                .debug_struct("Unsupported")
                .field("message", message)
                .finish(),
        }
    }
}

/// Atomically publish a completed staged target under the current fence.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct PublishCtasTargetRequest {
    /// Fenced action identity.
    pub action: CtasFencedAction,
    /// Durable staged locator returned by `stage`.
    pub staged_locator: String,
    /// Proof binding the locator to the operation.
    pub staged_proof: String,
    /// Digest proving that all staged writes completed.
    pub write_completion_digest: String,
    /// Digest of the destination create policy.
    pub create_policy_digest: String,
    /// Bounded provider payload interpreted only by the catalog extension.
    pub provider_payload: String,
}

impl fmt::Debug for PublishCtasTargetRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishCtasTargetRequest")
            .field("action", &self.action)
            .field("staged_locator", &redacted_len(&self.staged_locator))
            .field("staged_proof", &redacted_len(&self.staged_proof))
            .field("write_completion_digest", &self.write_completion_digest)
            .field("create_policy_digest", &self.create_policy_digest)
            .field("provider_payload", &redacted_len(&self.provider_payload))
            .finish()
    }
}

/// Typed terminal result of a catalog publication attempt.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "disposition", rename_all = "kebab-case")]
pub enum PublishCtasTargetResponse {
    /// This operation published the staged target.
    Published {
        /// Bounded opaque publication provenance.
        provenance: String,
        /// Proof binding the visible target to the operation.
        proof: String,
    },
    /// The sealed create policy produced a successful no-op.
    NoOp {
        /// Bounded opaque no-op provenance.
        provenance: String,
        /// Proof binding the no-op observation to the destination.
        proof: String,
    },
}

impl fmt::Debug for PublishCtasTargetResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (name, provenance, proof) = match self {
            Self::Published { provenance, proof } => ("Published", provenance, proof),
            Self::NoOp { provenance, proof } => ("NoOp", provenance, proof),
        };
        f.debug_struct(name)
            .field("provenance", &redacted_len(provenance))
            .field("proof", &redacted_len(proof))
            .finish()
    }
}

/// Abort an unpublished staged target under the current fence.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct AbortCtasTargetRequest {
    /// Fenced action identity.
    pub action: CtasFencedAction,
    /// Durable staged locator returned by `stage`.
    pub staged_locator: String,
    /// Proof binding the locator to the operation.
    pub staged_proof: String,
    /// Bounded provider payload interpreted only by the catalog extension.
    pub provider_payload: String,
}

impl fmt::Debug for AbortCtasTargetRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AbortCtasTargetRequest")
            .field("action", &self.action)
            .field("staged_locator", &redacted_len(&self.staged_locator))
            .field("staged_proof", &redacted_len(&self.staged_proof))
            .field("provider_payload", &redacted_len(&self.provider_payload))
            .finish()
    }
}

/// Terminal catalog abort provenance.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub struct AbortCtasTargetResponse {
    /// Bounded opaque abort provenance.
    pub provenance: String,
    /// Proof binding the abort to the operation.
    pub proof: String,
}

impl fmt::Debug for AbortCtasTargetResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AbortCtasTargetResponse")
            .field("provenance", &redacted_len(&self.provenance))
            .field("proof", &redacted_len(&self.proof))
            .finish()
    }
}

/// A typed catalog conflict returned by mutating extension endpoints.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CtasFencedPublicationConflictKind {
    /// The request generation is older than catalog truth.
    StaleFence,
    /// Cluster, operation, target, or child identity does not match.
    IdentityConflict,
    /// The same identity was previously sealed with a different digest.
    DigestConflict,
    /// A different publication already made the target visible.
    AlreadyPublished,
    /// The staged target was already aborted.
    AlreadyAborted,
    /// The destination no longer satisfies the sealed create policy.
    CreatePolicyConflict,
}

/// Failure classification preserving dispatch and catalog conflict semantics.
pub enum CtasFencedPublicationError {
    /// The server did not advertise this exact protocol version.
    Unsupported(Error),
    /// The server rejected the request with a typed conflict.
    Conflict {
        /// Exact catalog conflict category.
        kind: CtasFencedPublicationConflictKind,
        /// Catalog error with bounded context.
        error: Error,
    },
    /// The request is known not to have reached the extension action.
    KnownNotDispatched(Error),
    /// The request may have reached or changed catalog state.
    PossiblyDispatched(Error),
    /// A success status was received but its committed response was invalid.
    CommittedResponseInvalid(Error),
    /// The response cannot be mapped to safe catalog truth.
    Ambiguous(Error),
}

impl fmt::Debug for CtasFencedPublicationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(error) => f.debug_tuple("Unsupported").field(error).finish(),
            Self::Conflict { kind, error } => f
                .debug_struct("Conflict")
                .field("kind", kind)
                .field("error", error)
                .finish(),
            Self::KnownNotDispatched(error) => {
                f.debug_tuple("KnownNotDispatched").field(error).finish()
            }
            Self::PossiblyDispatched(error) => {
                f.debug_tuple("PossiblyDispatched").field(error).finish()
            }
            Self::CommittedResponseInvalid(error) => f
                .debug_tuple("CommittedResponseInvalid")
                .field(error)
                .finish(),
            Self::Ambiguous(error) => f.debug_tuple("Ambiguous").field(error).finish(),
        }
    }
}

/// Client view over the `RestCatalog` runtime context and authenticated client.
#[derive(Debug, Clone, Copy)]
pub struct CtasFencedPublication<'a> {
    catalog: &'a RestCatalog,
}

#[derive(Debug, Deserialize)]
struct ExtensionErrorResponse {
    error: ExtensionErrorModel,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct ExtensionErrorModel {
    kind: ExtensionErrorKind,
    message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum ExtensionErrorKind {
    Unsupported,
    StaleFence,
    IdentityConflict,
    DigestConflict,
    AlreadyPublished,
    AlreadyAborted,
    CreatePolicyConflict,
    Ambiguous,
}

#[derive(Clone, Copy)]
enum SuccessSemantics {
    Observation,
    Mutation,
}

impl RestCatalog {
    /// Return the CTAS extension only when the server advertises exact version
    /// `1` in `/v1/config`. User properties never participate in this gate.
    pub async fn ctas_fenced_publication(&self) -> Result<Option<CtasFencedPublication<'_>>> {
        let context = self.context().await?;
        Ok((context
            .server_properties
            .get(CTAS_FENCED_PUBLICATION_CAPABILITY)
            .map(String::as_str)
            == Some(CTAS_FENCED_PUBLICATION_VERSION))
        .then_some(CtasFencedPublication { catalog: self }))
    }

    /// Require the exact CTAS extension version or return a local typed
    /// `Unsupported` failure before any extension endpoint is called.
    pub async fn require_ctas_fenced_publication(
        &self,
    ) -> std::result::Result<CtasFencedPublication<'_>, CtasFencedPublicationError> {
        let context = self
            .context()
            .await
            .map_err(CtasFencedPublicationError::KnownNotDispatched)?;
        let advertised = context
            .server_properties
            .get(CTAS_FENCED_PUBLICATION_CAPABILITY)
            .map(String::as_str);
        if advertised == Some(CTAS_FENCED_PUBLICATION_VERSION) {
            Ok(CtasFencedPublication { catalog: self })
        } else {
            Err(CtasFencedPublicationError::Unsupported(
                Error::new(
                    ErrorKind::FeatureUnsupported,
                    "Catalog does not advertise the required CTAS fenced-publication protocol",
                )
                .with_context("required-version", CTAS_FENCED_PUBLICATION_VERSION)
                .with_context("advertised-version", advertised.unwrap_or("absent")),
            ))
        }
    }
}

impl CtasFencedPublication<'_> {
    /// Describe the exact supported protocol version.
    pub fn capability(&self) -> CtasFencedPublicationCapability {
        CtasFencedPublicationCapability {
            protocol_version: CTAS_FENCED_PUBLICATION_VERSION,
        }
    }

    /// Establish or replay a catalog fence.
    pub async fn advance_fence(
        &self,
        request: &AdvanceCtasFenceRequest,
    ) -> std::result::Result<AdvanceCtasFenceResponse, CtasFencedPublicationError> {
        self.call("advance-fence", request, SuccessSemantics::Mutation)
            .await
    }

    /// Register an invisible staged target.
    pub async fn stage(
        &self,
        request: &StageCtasTargetRequest,
    ) -> std::result::Result<StageCtasTargetResponse, CtasFencedPublicationError> {
        self.call("stage", request, SuccessSemantics::Mutation)
            .await
    }

    /// Inspect catalog-authoritative operation state.
    pub async fn inspect(
        &self,
        request: &InspectCtasTargetRequest,
    ) -> std::result::Result<InspectCtasTargetResponse, CtasFencedPublicationError> {
        self.call("inspect", request, SuccessSemantics::Observation)
            .await
    }

    /// Atomically publish a completed staged target.
    pub async fn publish(
        &self,
        request: &PublishCtasTargetRequest,
    ) -> std::result::Result<PublishCtasTargetResponse, CtasFencedPublicationError> {
        self.call("publish", request, SuccessSemantics::Mutation)
            .await
    }

    /// Abort an unpublished staged target.
    pub async fn abort(
        &self,
        request: &AbortCtasTargetRequest,
    ) -> std::result::Result<AbortCtasTargetResponse, CtasFencedPublicationError> {
        self.call("abort", request, SuccessSemantics::Mutation)
            .await
    }

    async fn call<Request, Response>(
        &self,
        operation: &str,
        request: &Request,
        success_semantics: SuccessSemantics,
    ) -> std::result::Result<Response, CtasFencedPublicationError>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        let body = serde_json::to_vec(request).map_err(|error| {
            CtasFencedPublicationError::KnownNotDispatched(
                Error::new(
                    ErrorKind::DataInvalid,
                    "Failed to encode CTAS extension request",
                )
                .with_source(error),
            )
        })?;
        if body.len() > MAX_REQUEST_BYTES {
            return Err(CtasFencedPublicationError::KnownNotDispatched(
                Error::new(
                    ErrorKind::DataInvalid,
                    "CTAS extension request exceeds the bounded wire limit",
                )
                .with_context("limit", MAX_REQUEST_BYTES.to_string()),
            ));
        }

        let context = self
            .catalog
            .context()
            .await
            .map_err(|error| CtasFencedPublicationError::KnownNotDispatched(error))?;
        let endpoint = context.config.url_prefixed(&[EXTENSION_PATH, operation]);
        let http_request = context
            .client
            .request(Method::POST, endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .build()
            .map_err(|error| CtasFencedPublicationError::KnownNotDispatched(error.into()))?;
        let response = context
            .client
            .query_catalog(http_request)
            .await
            .map_err(CtasFencedPublicationError::PossiblyDispatched)?;
        let status = response.status();

        let bytes = read_bounded_response(response)
            .await
            .map_err(|error| classify_response_read_error(status, success_semantics, error))?;

        if status.is_success() {
            return serde_json::from_slice(&bytes).map_err(|error| {
                let error = Error::new(
                    ErrorKind::DataInvalid,
                    "Failed to decode CTAS extension success response",
                )
                .with_context("status", status.to_string())
                .with_source(error);
                match success_semantics {
                    SuccessSemantics::Observation => CtasFencedPublicationError::Ambiguous(error),
                    SuccessSemantics::Mutation => {
                        CtasFencedPublicationError::CommittedResponseInvalid(error)
                    }
                }
            });
        }

        let wire_error: ExtensionErrorResponse =
            serde_json::from_slice(&bytes).map_err(|error| {
                if status.is_server_error() {
                    return classify_untyped_server_error(status, success_semantics);
                }
                CtasFencedPublicationError::Ambiguous(
                    Error::new(
                        ErrorKind::Unexpected,
                        "CTAS extension returned an unclassifiable error response",
                    )
                    .with_context("status", status.to_string())
                    .with_source(error),
                )
            })?;
        Err(classify_wire_error(
            status,
            success_semantics,
            wire_error.error,
        ))
    }
}

fn classify_wire_error(
    status: StatusCode,
    success_semantics: SuccessSemantics,
    wire: ExtensionErrorModel,
) -> CtasFencedPublicationError {
    let error = Error::new(ErrorKind::PreconditionFailed, wire.message)
        .with_context("status", status.to_string());
    let conflict = match (status, wire.kind) {
        (StatusCode::NOT_IMPLEMENTED, ExtensionErrorKind::Unsupported) => {
            return CtasFencedPublicationError::Unsupported(error);
        }
        (StatusCode::PRECONDITION_FAILED, ExtensionErrorKind::StaleFence) => {
            CtasFencedPublicationConflictKind::StaleFence
        }
        (StatusCode::CONFLICT, ExtensionErrorKind::IdentityConflict) => {
            CtasFencedPublicationConflictKind::IdentityConflict
        }
        (StatusCode::CONFLICT, ExtensionErrorKind::DigestConflict) => {
            CtasFencedPublicationConflictKind::DigestConflict
        }
        (StatusCode::CONFLICT, ExtensionErrorKind::AlreadyPublished) => {
            CtasFencedPublicationConflictKind::AlreadyPublished
        }
        (StatusCode::CONFLICT, ExtensionErrorKind::AlreadyAborted) => {
            CtasFencedPublicationConflictKind::AlreadyAborted
        }
        (StatusCode::CONFLICT, ExtensionErrorKind::CreatePolicyConflict) => {
            CtasFencedPublicationConflictKind::CreatePolicyConflict
        }
        (StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED, ExtensionErrorKind::Ambiguous) => {
            return CtasFencedPublicationError::Ambiguous(error);
        }
        (status, _) if status.is_server_error() => {
            return classify_untyped_server_error(status, success_semantics);
        }
        _ => return CtasFencedPublicationError::Ambiguous(error),
    };
    CtasFencedPublicationError::Conflict {
        kind: conflict,
        error,
    }
}

enum BoundedResponseReadError {
    TooLarge,
    Transport(Error),
}

async fn read_bounded_response(
    mut response: reqwest::Response,
) -> std::result::Result<Vec<u8>, BoundedResponseReadError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RESPONSE_BYTES as u64)
    {
        return Err(BoundedResponseReadError::TooLarge);
    }

    let mut bytes = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(MAX_RESPONSE_BYTES as u64) as usize,
    );
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| BoundedResponseReadError::Transport(error.into()))?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err(BoundedResponseReadError::TooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn classify_response_read_error(
    status: StatusCode,
    success_semantics: SuccessSemantics,
    read_error: BoundedResponseReadError,
) -> CtasFencedPublicationError {
    let error = match read_error {
        BoundedResponseReadError::TooLarge => bounded_response_error(status),
        BoundedResponseReadError::Transport(error) => error,
    };
    if status.is_success() {
        return match success_semantics {
            SuccessSemantics::Observation => CtasFencedPublicationError::Ambiguous(error),
            SuccessSemantics::Mutation => {
                CtasFencedPublicationError::CommittedResponseInvalid(error)
            }
        };
    }
    if status.is_server_error() && matches!(success_semantics, SuccessSemantics::Mutation) {
        CtasFencedPublicationError::PossiblyDispatched(error)
    } else {
        CtasFencedPublicationError::Ambiguous(error)
    }
}

fn classify_untyped_server_error(
    status: StatusCode,
    success_semantics: SuccessSemantics,
) -> CtasFencedPublicationError {
    let error = Error::new(
        ErrorKind::Unexpected,
        "CTAS extension returned an unclassified server failure",
    )
    .with_context("status", status.to_string());
    match success_semantics {
        SuccessSemantics::Mutation => CtasFencedPublicationError::PossiblyDispatched(error),
        SuccessSemantics::Observation => CtasFencedPublicationError::Ambiguous(error),
    }
}

fn bounded_response_error(status: StatusCode) -> Error {
    Error::new(
        ErrorKind::DataInvalid,
        "CTAS extension response exceeds the bounded wire limit",
    )
    .with_context("status", status.to_string())
    .with_context("limit", MAX_RESPONSE_BYTES.to_string())
}

struct RedactedLen(usize);

impl fmt::Debug for RedactedLen {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[REDACTED len={}]", self.0)
    }
}

fn redacted_len(value: &str) -> RedactedLen {
    RedactedLen(value.len())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use iceberg::{CatalogBuilder, NamespaceIdent};
    use mockito::{Mock, Server, ServerGuard};
    use reqwest::Client;

    use super::*;
    use crate::{REST_CATALOG_PROP_URI, RestCatalogBuilder};

    async fn config_mock(
        server: &mut ServerGuard,
        capability: Option<&str>,
        prefix: Option<&str>,
    ) -> Mock {
        let capability = capability
            .map(|version| format!(r#", "{CTAS_FENCED_PUBLICATION_CAPABILITY}": "{version}""#))
            .unwrap_or_default();
        let prefix = prefix
            .map(|value| format!(r#", "prefix": "{value}""#))
            .unwrap_or_default();
        server
            .mock("GET", "/v1/config")
            .match_header("authorization", "Bearer extension-token")
            .with_status(200)
            .with_body(format!(
                r#"{{"defaults": {{"warehouse": "s3://warehouse"{capability}{prefix}}}, "overrides": {{}}}}"#
            ))
            .create_async()
            .await
    }

    async fn catalog(server: &ServerGuard, extra: &[(&str, &str)]) -> RestCatalog {
        let mut props = HashMap::from([
            (REST_CATALOG_PROP_URI.to_string(), server.url()),
            ("token".to_string(), "extension-token".to_string()),
        ]);
        props.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
        );
        RestCatalogBuilder::default()
            .load("ctas-extension-test", props)
            .await
            .unwrap()
    }

    fn operation() -> CtasFencedOperation {
        CtasFencedOperation {
            cluster_id: "cluster-a".to_string(),
            operation_id: "saga-a".to_string(),
            target: TableIdent::new(NamespaceIdent::new("db".to_string()), "target".to_string()),
        }
    }

    fn action(action_id: &str) -> CtasFencedAction {
        CtasFencedAction {
            operation: operation(),
            generation: 7,
            action_id: action_id.to_string(),
            input_digest: format!("digest-{action_id}"),
        }
    }

    #[tokio::test]
    async fn server_advertisement_installs_exact_version_and_reuses_auth_and_prefix() {
        let mut server = Server::new_async().await;
        let config = config_mock(&mut server, Some("1"), Some("catalog-prefix")).await;
        let advance = server
            .mock(
                "POST",
                "/v1/catalog-prefix/extensions/fenced-staged-publication/advance-fence",
            )
            .match_header("authorization", "Bearer extension-token")
            .match_header("content-type", "application/json")
            .with_status(200)
            .with_body(r#"{"generation":7,"input-digest":"digest-advance","receipt":"receipt-a"}"#)
            .create_async()
            .await;
        let catalog = catalog(&server, &[]).await;
        let extension = catalog.ctas_fenced_publication().await.unwrap().unwrap();

        assert_eq!(
            extension.capability(),
            CtasFencedPublicationCapability {
                protocol_version: "1"
            }
        );
        assert_eq!(
            extension
                .advance_fence(&AdvanceCtasFenceRequest {
                    action: action("advance")
                })
                .await
                .unwrap(),
            AdvanceCtasFenceResponse {
                generation: 7,
                input_digest: "digest-advance".to_string(),
                receipt: "receipt-a".to_string(),
            }
        );
        config.assert_async().await;
        advance.assert_async().await;
    }

    #[tokio::test]
    async fn user_property_cannot_forge_server_capability() {
        let mut server = Server::new_async().await;
        let config = config_mock(&mut server, None, None).await;
        let catalog = catalog(&server, &[(CTAS_FENCED_PUBLICATION_CAPABILITY, "1")]).await;

        assert!(catalog.ctas_fenced_publication().await.unwrap().is_none());
        assert!(matches!(
            catalog.require_ctas_fenced_publication().await,
            Err(CtasFencedPublicationError::Unsupported(_))
        ));
        config.assert_async().await;
    }

    #[tokio::test]
    async fn mismatched_server_version_does_not_install_extension() {
        let mut server = Server::new_async().await;
        let config = config_mock(&mut server, Some("2"), None).await;
        let catalog = catalog(&server, &[]).await;

        assert!(catalog.ctas_fenced_publication().await.unwrap().is_none());
        config.assert_async().await;
    }

    #[tokio::test]
    async fn all_five_typed_operations_use_the_versioned_extension() {
        let mut server = Server::new_async().await;
        let config = config_mock(&mut server, Some("1"), None).await;
        let stage = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/stage")
            .with_status(200)
            .with_body(r#"{"staged-locator":"locator","staged-proof":"stage-proof"}"#)
            .create_async()
            .await;
        let inspect = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/inspect")
            .with_status(200)
            .with_body(r#"{"state":"staged","staged-locator":"locator","proof":"stage-proof"}"#)
            .create_async()
            .await;
        let publish = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/publish")
            .with_status(200)
            .with_body(
                r#"{"disposition":"published","provenance":"published","proof":"publish-proof"}"#,
            )
            .create_async()
            .await;
        let abort = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/abort")
            .with_status(200)
            .with_body(r#"{"provenance":"aborted","proof":"abort-proof"}"#)
            .create_async()
            .await;
        let catalog = catalog(&server, &[]).await;
        let extension = catalog.ctas_fenced_publication().await.unwrap().unwrap();

        let staged = extension
            .stage(&StageCtasTargetRequest {
                action: action("stage"),
                staged_identity: "staged-a".to_string(),
                initialization_digest: "initialization".to_string(),
                create_policy_digest: "create-policy".to_string(),
                provider_payload: "payload".to_string(),
            })
            .await
            .unwrap();
        assert_eq!(staged.staged_locator, "locator");
        assert_eq!(
            extension
                .inspect(&InspectCtasTargetRequest {
                    operation: operation(),
                    generation: 7,
                    input_digest: "inspect-digest".to_string(),
                })
                .await
                .unwrap(),
            InspectCtasTargetResponse::Staged {
                staged_locator: "locator".to_string(),
                proof: "stage-proof".to_string(),
            }
        );
        assert_eq!(
            extension
                .publish(&PublishCtasTargetRequest {
                    action: action("publish"),
                    staged_locator: "locator".to_string(),
                    staged_proof: "stage-proof".to_string(),
                    write_completion_digest: "write-complete".to_string(),
                    create_policy_digest: "create-policy".to_string(),
                    provider_payload: "payload".to_string(),
                })
                .await
                .unwrap(),
            PublishCtasTargetResponse::Published {
                provenance: "published".to_string(),
                proof: "publish-proof".to_string(),
            }
        );
        assert_eq!(
            extension
                .abort(&AbortCtasTargetRequest {
                    action: action("abort"),
                    staged_locator: "locator".to_string(),
                    staged_proof: "stage-proof".to_string(),
                    provider_payload: "payload".to_string(),
                })
                .await
                .unwrap()
                .provenance,
            "aborted"
        );

        config.assert_async().await;
        stage.assert_async().await;
        inspect.assert_async().await;
        publish.assert_async().await;
        abort.assert_async().await;
    }

    #[tokio::test]
    async fn response_body_timeout_is_committed_response_invalid() {
        let mut server = Server::new_async().await;
        let _config = config_mock(&mut server, Some("1"), None).await;
        let publish = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/publish")
            .with_status(200)
            .with_chunked_body(|writer| {
                std::thread::sleep(Duration::from_millis(150));
                writer.write_all(
                    br#"{"disposition":"published","provenance":"published","proof":"proof"}"#,
                )
            })
            .create_async()
            .await;
        let client = Client::builder()
            .timeout(Duration::from_millis(25))
            .build()
            .unwrap();
        let props = HashMap::from([
            (REST_CATALOG_PROP_URI.to_string(), server.url()),
            ("token".to_string(), "extension-token".to_string()),
        ]);
        let catalog = RestCatalogBuilder::default()
            .with_client(client)
            .load("ctas-timeout-test", props)
            .await
            .unwrap();
        let extension = catalog.ctas_fenced_publication().await.unwrap().unwrap();

        assert!(matches!(
            extension
                .publish(&PublishCtasTargetRequest {
                    action: action("publish"),
                    staged_locator: "locator".to_string(),
                    staged_proof: "proof".to_string(),
                    write_completion_digest: "write".to_string(),
                    create_policy_digest: "policy".to_string(),
                    provider_payload: String::new(),
                })
                .await,
            Err(CtasFencedPublicationError::CommittedResponseInvalid(_))
        ));
        publish.assert_async().await;
    }

    #[tokio::test]
    async fn conflict_body_distinguishes_stale_fence_and_identity_conflict() {
        let mut server = Server::new_async().await;
        let _config = config_mock(&mut server, Some("1"), None).await;
        let stale = server
            .mock(
                "POST",
                "/v1/extensions/fenced-staged-publication/advance-fence",
            )
            .with_status(412)
            .with_body(r#"{"error":{"kind":"stale-fence","message":"generation is stale"}}"#)
            .create_async()
            .await;
        let identity = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/publish")
            .with_status(409)
            .with_body(r#"{"error":{"kind":"identity-conflict","message":"foreign saga"}}"#)
            .create_async()
            .await;
        let catalog = catalog(&server, &[]).await;
        let extension = catalog.ctas_fenced_publication().await.unwrap().unwrap();

        assert!(matches!(
            extension
                .advance_fence(&AdvanceCtasFenceRequest {
                    action: action("advance")
                })
                .await,
            Err(CtasFencedPublicationError::Conflict {
                kind: CtasFencedPublicationConflictKind::StaleFence,
                ..
            })
        ));
        assert!(matches!(
            extension
                .publish(&PublishCtasTargetRequest {
                    action: action("publish"),
                    staged_locator: "locator".to_string(),
                    staged_proof: "proof".to_string(),
                    write_completion_digest: "write".to_string(),
                    create_policy_digest: "policy".to_string(),
                    provider_payload: String::new(),
                })
                .await,
            Err(CtasFencedPublicationError::Conflict {
                kind: CtasFencedPublicationConflictKind::IdentityConflict,
                ..
            })
        ));
        stale.assert_async().await;
        identity.assert_async().await;
    }

    #[tokio::test]
    async fn advertised_endpoint_not_found_is_ambiguous_not_unsupported() {
        let mut server = Server::new_async().await;
        let _config = config_mock(&mut server, Some("1"), None).await;
        let inspect = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/inspect")
            .with_status(404)
            .with_body(r#"{"error":{"kind":"unsupported","message":"record missing"}}"#)
            .create_async()
            .await;
        let catalog = catalog(&server, &[]).await;
        let extension = catalog.ctas_fenced_publication().await.unwrap().unwrap();

        assert!(matches!(
            extension
                .inspect(&InspectCtasTargetRequest {
                    operation: operation(),
                    generation: 7,
                    input_digest: "inspect".to_string(),
                })
                .await,
            Err(CtasFencedPublicationError::Ambiguous(_))
        ));
        inspect.assert_async().await;
    }

    #[test]
    fn only_protocol_status_and_kind_pairs_are_typed() {
        let unsupported = ExtensionErrorModel {
            kind: ExtensionErrorKind::Unsupported,
            message: "unsupported".to_string(),
        };
        assert!(matches!(
            classify_wire_error(
                StatusCode::NOT_IMPLEMENTED,
                SuccessSemantics::Mutation,
                unsupported,
            ),
            CtasFencedPublicationError::Unsupported(_)
        ));

        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::FOUND,
            StatusCode::NOT_FOUND,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            assert!(matches!(
                classify_wire_error(
                    status,
                    SuccessSemantics::Observation,
                    ExtensionErrorModel {
                        kind: ExtensionErrorKind::Unsupported,
                        message: "not a protocol pair".to_string(),
                    },
                ),
                CtasFencedPublicationError::Ambiguous(_)
            ));
        }

        assert!(matches!(
            classify_wire_error(
                StatusCode::CONFLICT,
                SuccessSemantics::Mutation,
                ExtensionErrorModel {
                    kind: ExtensionErrorKind::StaleFence,
                    message: "wrong status for stale".to_string(),
                },
            ),
            CtasFencedPublicationError::Ambiguous(_)
        ));
        assert!(matches!(
            classify_wire_error(
                StatusCode::PRECONDITION_FAILED,
                SuccessSemantics::Mutation,
                ExtensionErrorModel {
                    kind: ExtensionErrorKind::IdentityConflict,
                    message: "wrong status for identity".to_string(),
                },
            ),
            CtasFencedPublicationError::Ambiguous(_)
        ));
    }

    #[test]
    fn publish_dispositions_round_trip_without_collapsing_no_op() {
        for response in [
            PublishCtasTargetResponse::Published {
                provenance: "published-provenance".to_string(),
                proof: "published-proof".to_string(),
            },
            PublishCtasTargetResponse::NoOp {
                provenance: "no-op-provenance".to_string(),
                proof: "no-op-proof".to_string(),
            },
        ] {
            let encoded = serde_json::to_vec(&response).unwrap();
            assert_eq!(
                serde_json::from_slice::<PublishCtasTargetResponse>(&encoded).unwrap(),
                response
            );
        }
    }

    #[tokio::test]
    async fn invalid_success_is_committed_response_invalid_for_mutation() {
        let mut server = Server::new_async().await;
        let _config = config_mock(&mut server, Some("1"), None).await;
        let publish = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/publish")
            .with_status(200)
            .with_body("not-json")
            .create_async()
            .await;
        let catalog = catalog(&server, &[]).await;
        let extension = catalog.ctas_fenced_publication().await.unwrap().unwrap();

        assert!(matches!(
            extension
                .publish(&PublishCtasTargetRequest {
                    action: action("publish"),
                    staged_locator: "locator".to_string(),
                    staged_proof: "proof".to_string(),
                    write_completion_digest: "write".to_string(),
                    create_policy_digest: "policy".to_string(),
                    provider_payload: String::new(),
                })
                .await,
            Err(CtasFencedPublicationError::CommittedResponseInvalid(_))
        ));
        publish.assert_async().await;
    }

    #[tokio::test]
    async fn server_failure_is_possibly_dispatched() {
        let mut server = Server::new_async().await;
        let _config = config_mock(&mut server, Some("1"), None).await;
        let abort = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/abort")
            .with_status(503)
            .with_body(r#"{"error":{"message":"down"}}"#)
            .create_async()
            .await;
        let catalog = catalog(&server, &[]).await;
        let extension = catalog.ctas_fenced_publication().await.unwrap().unwrap();

        assert!(matches!(
            extension
                .abort(&AbortCtasTargetRequest {
                    action: action("abort"),
                    staged_locator: "locator".to_string(),
                    staged_proof: "proof".to_string(),
                    provider_payload: String::new(),
                })
                .await,
            Err(CtasFencedPublicationError::PossiblyDispatched(_))
        ));
        abort.assert_async().await;
    }

    #[tokio::test]
    async fn oversized_success_client_error_and_server_error_are_bounded() {
        let mut server = Server::new_async().await;
        let _config = config_mock(&mut server, Some("1"), None).await;
        let oversized = "x".repeat(MAX_RESPONSE_BYTES + 1);
        let publish = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/publish")
            .with_status(200)
            .with_body(oversized.clone())
            .create_async()
            .await;
        let inspect = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/inspect")
            .with_status(429)
            .with_body(oversized.clone())
            .create_async()
            .await;
        let abort = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/abort")
            .with_status(503)
            .with_body(oversized)
            .create_async()
            .await;
        let catalog = catalog(&server, &[]).await;
        let extension = catalog.ctas_fenced_publication().await.unwrap().unwrap();

        assert!(matches!(
            extension
                .publish(&PublishCtasTargetRequest {
                    action: action("publish"),
                    staged_locator: "locator".to_string(),
                    staged_proof: "proof".to_string(),
                    write_completion_digest: "write".to_string(),
                    create_policy_digest: "policy".to_string(),
                    provider_payload: String::new(),
                })
                .await,
            Err(CtasFencedPublicationError::CommittedResponseInvalid(_))
        ));
        assert!(matches!(
            extension
                .inspect(&InspectCtasTargetRequest {
                    operation: operation(),
                    generation: 7,
                    input_digest: "inspect".to_string(),
                })
                .await,
            Err(CtasFencedPublicationError::Ambiguous(_))
        ));
        assert!(matches!(
            extension
                .abort(&AbortCtasTargetRequest {
                    action: action("abort"),
                    staged_locator: "locator".to_string(),
                    staged_proof: "proof".to_string(),
                    provider_payload: String::new(),
                })
                .await,
            Err(CtasFencedPublicationError::PossiblyDispatched(_))
        ));
        publish.assert_async().await;
        inspect.assert_async().await;
        abort.assert_async().await;
    }

    #[tokio::test]
    async fn oversized_chunked_response_without_content_length_is_bounded() {
        let mut server = Server::new_async().await;
        let _config = config_mock(&mut server, Some("1"), None).await;
        let publish = server
            .mock("POST", "/v1/extensions/fenced-staged-publication/publish")
            .with_status(200)
            .with_chunked_body(|writer| writer.write_all(&vec![b'x'; MAX_RESPONSE_BYTES + 1]))
            .create_async()
            .await;
        let catalog = catalog(&server, &[]).await;
        let extension = catalog.ctas_fenced_publication().await.unwrap().unwrap();

        assert!(matches!(
            extension
                .publish(&PublishCtasTargetRequest {
                    action: action("publish"),
                    staged_locator: "locator".to_string(),
                    staged_proof: "proof".to_string(),
                    write_completion_digest: "write".to_string(),
                    create_policy_digest: "policy".to_string(),
                    provider_payload: String::new(),
                })
                .await,
            Err(CtasFencedPublicationError::CommittedResponseInvalid(_))
        ));
        publish.assert_async().await;
    }

    #[test]
    fn opaque_wire_fields_are_redacted_from_debug() {
        const SENTINEL: &str = "super-secret-opaque-sentinel";
        let values = [
            format!(
                "{:?}",
                AdvanceCtasFenceResponse {
                    generation: 7,
                    input_digest: "digest".to_string(),
                    receipt: SENTINEL.to_string(),
                }
            ),
            format!(
                "{:?}",
                StageCtasTargetRequest {
                    action: action("stage"),
                    staged_identity: "identity".to_string(),
                    initialization_digest: "initialization".to_string(),
                    create_policy_digest: "policy".to_string(),
                    provider_payload: SENTINEL.to_string(),
                }
            ),
            format!(
                "{:?}",
                StageCtasTargetResponse {
                    staged_locator: SENTINEL.to_string(),
                    staged_proof: SENTINEL.to_string(),
                }
            ),
            format!(
                "{:?}",
                InspectCtasTargetResponse::Published {
                    provenance: SENTINEL.to_string(),
                    proof: SENTINEL.to_string(),
                }
            ),
            format!(
                "{:?}",
                PublishCtasTargetRequest {
                    action: action("publish"),
                    staged_locator: SENTINEL.to_string(),
                    staged_proof: SENTINEL.to_string(),
                    write_completion_digest: "write".to_string(),
                    create_policy_digest: "policy".to_string(),
                    provider_payload: SENTINEL.to_string(),
                }
            ),
            format!(
                "{:?}",
                PublishCtasTargetResponse::NoOp {
                    provenance: SENTINEL.to_string(),
                    proof: SENTINEL.to_string(),
                }
            ),
            format!(
                "{:?}",
                AbortCtasTargetRequest {
                    action: action("abort"),
                    staged_locator: SENTINEL.to_string(),
                    staged_proof: SENTINEL.to_string(),
                    provider_payload: SENTINEL.to_string(),
                }
            ),
            format!(
                "{:?}",
                AbortCtasTargetResponse {
                    provenance: SENTINEL.to_string(),
                    proof: SENTINEL.to_string(),
                }
            ),
        ];

        for value in values {
            assert!(!value.contains(SENTINEL), "opaque value leaked: {value}");
            assert!(value.contains("REDACTED"));
        }
    }

    #[tokio::test]
    async fn oversized_request_is_known_not_dispatched() {
        let mut server = Server::new_async().await;
        let _config = config_mock(&mut server, Some("1"), None).await;
        let catalog = catalog(&server, &[]).await;
        let extension = catalog.ctas_fenced_publication().await.unwrap().unwrap();

        assert!(matches!(
            extension
                .abort(&AbortCtasTargetRequest {
                    action: action("abort"),
                    staged_locator: "locator".to_string(),
                    staged_proof: "proof".to_string(),
                    provider_payload: "x".repeat(MAX_REQUEST_BYTES),
                })
                .await,
            Err(CtasFencedPublicationError::KnownNotDispatched(_))
        ));
    }
}
