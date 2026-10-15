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

//! A REST request prepared entirely before entering its evidence interval.

use super::*;
use crate::catalog::publication::EvidenceContext;
use crate::catalog::publication_dispatch::{PreparationFailure, PreparationResult};
use crate::commit::attempt::observation::{DispatchEvidence, ProtocolObservation};
use crate::commit::publication_descriptor::{
    PublicationDescriptor, RecoveryBinding, RecoveryPrincipalScope,
};
use novarocks_spi::connector::{
    ConnectorOperationControl, ConnectorRequestContext, ProviderBindingEpoch,
};
use std::sync::Arc;

/// Only the catalog owner constructs this route from its admitted operation.
#[derive(Clone, Debug)]
pub(crate) enum MutationRoute {
    CommitTable(TableIdent),
    CreateTable(NamespaceIdent),
    CreateNamespace,
    DropNamespace(NamespaceIdent),
    DropTable(TableIdent),
    CreateView(NamespaceIdent),
    ReplaceView(TableIdent),
    DropView(TableIdent),
}
impl MutationRoute {
    fn request(&self, binding: &SessionBinding) -> (Method, String, bool) {
        match self {
            Self::CommitTable(table) => (Method::POST, binding.table_url(table), true),
            Self::CreateTable(namespace) => (Method::POST, binding.tables_url(namespace), true),
            Self::CreateNamespace => (Method::POST, binding.namespaces_url(), true),
            Self::DropNamespace(namespace) => {
                (Method::DELETE, binding.namespace_url(namespace), false)
            }
            Self::DropTable(table) => (Method::DELETE, binding.table_url(table), false),
            Self::CreateView(namespace) => (Method::POST, binding.views_url(namespace), true),
            Self::ReplaceView(view) => (Method::POST, binding.view_url(view), true),
            Self::DropView(view) => (Method::DELETE, binding.view_url(view), false),
        }
    }
}

pub(crate) struct PreparedRestMutation {
    request: RestRequest,
    binding: RecoveryBinding,
    expectation: AttributionExpectation,
    owner: Arc<crate::rest_protocol::auth::RestAuthOwner>,
    keep_document: bool,
}
impl std::fmt::Debug for PreparedRestMutation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedRestMutation")
            .field("method", self.request.method())
            .field("keep_document", &self.keep_document)
            .finish_non_exhaustive()
    }
}

impl PreparedRestMutation {
    pub(crate) async fn prepare(
        endpoints: Arc<RestEndpoints>,
        route: MutationRoute,
        descriptor: &PublicationDescriptor,
        generation: ProviderBindingEpoch,
        preparation: &ConnectorRequestContext,
        expectation: AttributionExpectation,
        keep_document: bool,
        delegation: Option<HeaderValue>,
    ) -> PreparationResult<Self> {
        use crate::iceberg::{Error, ErrorKind};
        let stopped = PreparationFailure::from;
        let binding_failure = |step| {
            PreparationFailure(novarocks_spi::connector::ConnectorMutationFailure::new(
                crate::rest_protocol::evidence::not_written_kind(NotWrittenCause::Binding(step)),
                "REST publication session or authentication preparation failed",
            ))
        };
        preparation.check_active().map_err(stopped)?;
        descriptor.validate()?;
        // These awaits are session/authentication reads. Their shared attempt
        // remains driven by other waiters if this caller stops waiting.
        let session = tokio::select! {
            result = endpoints.session() => result.map_err(|failure| binding_failure(failure.step))?,
            _ = preparation.stop().stopped() => return Err(PreparationFailure(novarocks_spi::connector::ConnectorMutationFailure::new(novarocks_spi::connector::ConnectorMutationFailureKind::Cancelled, "REST publication preparation cancelled"))),
            _ = tokio::time::sleep_until(preparation.deadline().into()) => return Err(PreparationFailure(novarocks_spi::connector::ConnectorMutationFailure::new(novarocks_spi::connector::ConnectorMutationFailureKind::DeadlineExceeded, "REST publication preparation deadline elapsed"))),
        };
        let mut headers = session.request_headers(delegation);
        tokio::select! {
            result = endpoints.owner().authorize(&mut headers) => result.map_err(|failure| binding_failure(failure.step))?,
            _ = preparation.stop().stopped() => return Err(PreparationFailure(novarocks_spi::connector::ConnectorMutationFailure::new(novarocks_spi::connector::ConnectorMutationFailureKind::Cancelled, "REST publication preparation cancelled"))),
            _ = tokio::time::sleep_until(preparation.deadline().into()) => return Err(PreparationFailure(novarocks_spi::connector::ConnectorMutationFailure::new(novarocks_spi::connector::ConnectorMutationFailureKind::DeadlineExceeded, "REST publication preparation deadline elapsed"))),
        }
        preparation.check_active().map_err(stopped)?;
        let (method, endpoint, has_body) = route.request(&session);
        let parsed = reqwest::Url::parse(&endpoint).map_err(|_| {
            Error::new(ErrorKind::DataInvalid, "Invalid prepared REST mutation URL")
        })?;
        let lifetime = match session.idempotency_capability() {
            crate::rest_protocol::configuration::IdempotencyCapability::Supported { lifetime } => {
                headers.insert(
                    "Idempotency-Key",
                    HeaderValue::from_str(&descriptor.key.to_string())
                        .expect("UUIDv7 is a valid header value"),
                );
                Some(*lifetime)
            }
            crate::rest_protocol::configuration::IdempotencyCapability::Absent
            | crate::rest_protocol::configuration::IdempotencyCapability::Invalid { .. } => None,
        };
        let request = RestRequest::new(method.clone(), endpoint.clone(), headers);
        let request = if has_body {
            if descriptor.request.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Prepared REST mutation requires its frozen JSON body",
                )
                .into());
            }
            request.with_encoded_json_body(descriptor.request.clone())
        } else {
            if !descriptor.request.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Bodyless REST mutation has unexpected frozen bytes",
                )
                .into());
            }
            request
        };
        let principal_scope = if endpoints.owner().is_anonymous() {
            RecoveryPrincipalScope::Anonymous
        } else {
            // This records an exact constraint, without claiming knowledge of
            // the subject hidden behind an opaque token. Recovery must retain
            // this generation's immutable identity, never just the auth mode.
            RecoveryPrincipalScope::CatalogGeneration {
                generation: generation.to_bytes(),
            }
        };
        let warehouse = session.warehouse().map(str::to_owned);
        let domain = serde_json::to_string(&(
            parsed.origin().ascii_serialization(),
            &warehouse,
            &principal_scope,
        ))
        .map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "Cannot encode REST deduplication domain",
            )
            .with_source(error)
        })?;
        let binding = RecoveryBinding {
            generation: generation.to_bytes(),
            method: method.to_string(),
            endpoint,
            warehouse,
            query: parsed
                .query_pairs()
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect(),
            principal_scope,
            deduplication_domain: domain,
            lifetime,
        };
        Ok(Self {
            request,
            binding,
            expectation,
            owner: Arc::clone(endpoints.owner()),
            keep_document,
        })
    }

    pub(crate) fn binding(&self) -> &RecoveryBinding {
        &self.binding
    }

    /// Starts no new session/auth request and serializes no request data.
    pub(crate) async fn dispatch(self, context: EvidenceContext) -> PreparedRestOutcome {
        let (stage, body) = self
            .owner
            .http()
            .send_once(
                Ok(self.request),
                self.expectation.endpoint().success_statuses(),
                &RequestContext::with_deadline(context.deadline().into()),
            )
            .await;
        let (read, document) = match (self.expectation.endpoint().response_shape(), body) {
            (Some(shape), Some(body)) => {
                read_view(shape, &self.expectation, body, self.keep_document).await
            }
            (None, Some(body)) => (read_empty(body).await, None),
            _ => (BodyRead::NotRead, None),
        };
        let evidence = classify(&stage, read, &self.expectation);
        let document = document.filter(|_| matches!(evidence, RestEvidence::Applied { .. }));
        PreparedRestOutcome {
            evidence,
            stage,
            document,
        }
    }
}

pub(crate) struct PreparedRestOutcome {
    pub evidence: RestEvidence,
    pub stage: DispatchStage,
    pub document: Option<Vec<u8>>,
}
impl PreparedRestOutcome {
    /// Mapping is a mechanism observation, not a retryable-error inference.
    pub(crate) fn dispatch_evidence(&self) -> DispatchEvidence {
        use novarocks_spi::connector::{ConnectorMutationFailure, ConnectorMutationFailureKind};
        let observation = match &self.stage {
            DispatchStage::NotWritten(_) => ProtocolObservation {
                adapter: "rest",
                stage: "not-written",
                ..Default::default()
            },
            DispatchStage::WrittenNoStatus(_) => ProtocolObservation {
                adapter: "rest",
                stage: "written-no-status",
                ..Default::default()
            },
            DispatchStage::Status { code, header_names } => ProtocolObservation {
                adapter: "rest",
                stage: "status",
                status_code: Some(*code),
                header_names: header_names
                    .iter()
                    .map(|name| name.as_str().to_owned())
                    .collect(),
            },
        };
        match &self.evidence {
            RestEvidence::NotDispatched { kind, .. } => DispatchEvidence::NotDispatched {
                failure: ConnectorMutationFailure::new(*kind, self.evidence.message()),
            },
            RestEvidence::Rejected { kind, .. } => DispatchEvidence::Rejected {
                failure: ConnectorMutationFailure::new(*kind, self.evidence.message()),
                observation,
            },
            RestEvidence::Unknown { .. } => DispatchEvidence::Unknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    self.evidence.message(),
                ),
                observation,
            },
            RestEvidence::Applied { receipt } => {
                let mut proof =
                    crate::catalog::transaction::CommitProof::applied(receipt.snapshot_id);
                if let Some(uuid) = receipt.uuid {
                    proof = proof.with_table_uuid(uuid.to_string());
                }
                proof.metadata_location = receipt.metadata_location.as_deref().map(Arc::from);
                proof.metadata_predecessor = receipt.metadata_predecessor.clone();
                DispatchEvidence::Applied { proof }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::publication::CatalogEvidencePolicy;
    use crate::commit::publication_descriptor::AttributionPlan;
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};
    use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
    use std::time::{Duration, Instant};

    async fn endpoints(server: &TestServer) -> Arc<RestEndpoints> {
        crate::catalog_runtime::build_vended_rest_generation_for_test(
            std::collections::HashMap::from([("uri".into(), server.url())]),
            None,
            crate::iceberg::Runtime::try_current().unwrap(),
        )
        .await
        .unwrap()
        .1
    }
    fn context(stop: &ConnectorStopOwner) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            stop.view(),
            64,
            128,
        )
        .unwrap()
    }
    fn descriptor() -> PublicationDescriptor {
        PublicationDescriptor::catalog(
            [1; 16],
            vec!["db.t".into()],
            b"\n{\"requirements\":[],\"updates\":[]}\n".to_vec(),
            AttributionPlan::ObserveOnly,
        )
        .unwrap()
    }
    fn expectation() -> AttributionExpectation {
        AttributionExpectation::table_commit(Uuid::nil(), None).unwrap()
    }
    fn route() -> MutationRoute {
        MutationRoute::CommitTable(TableIdent::new(
            NamespaceIdent::new("db".into()),
            "t".into(),
        ))
    }

    #[tokio::test]
    async fn exact_wire_body_and_supported_key_survive_statement_stop_until_evidence() {
        let reply = serde_json::json!({"metadata-location":"s3://warehouse/t/metadata/v2.json", "metadata":{"table-uuid":Uuid::nil()}});
        let server = TestServer::start(Script::new()
            .on("GET", "/v1/config", Reply::json(200, &serde_json::json!({"defaults":{},"overrides":{"header.Idempotency-Key":"server-forgery"},"idempotency-key-lifetime":"PT0.000000001S"})))
            .on("POST", "/v1/namespaces/db/tables/t", Reply::json(200, &reply).with_delay(Duration::from_millis(50))));
        let endpoints = endpoints(&server).await;
        let stop = ConnectorStopOwner::new();
        let preparation = context(&stop);
        let descriptor = descriptor();
        let generation = ProviderBindingEpoch::new();
        let prepared = PreparedRestMutation::prepare(
            endpoints,
            route(),
            &descriptor,
            generation,
            &preparation,
            expectation(),
            false,
            None,
        )
        .await
        .unwrap();
        assert_eq!(prepared.binding().generation, generation.to_bytes());
        assert_eq!(prepared.binding().lifetime, Some(Duration::from_nanos(1)));
        prepared
            .binding()
            .validate_generation(generation.to_bytes())
            .unwrap();
        let policy = CatalogEvidencePolicy {
            evidence: Duration::from_secs(1),
            ..Default::default()
        };
        let granted = policy.begin_evidence(&preparation).unwrap();
        let task = tokio::spawn(prepared.dispatch(granted));
        tokio::time::timeout(Duration::from_secs(1), async {
            while server.count("POST", "/v1/namespaces/db/tables/t") == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        stop.request_stop();
        let outcome = task.await.unwrap();
        assert!(matches!(
            outcome.dispatch_evidence(),
            DispatchEvidence::Applied { .. }
        ));
        let requests = server.requests_to("POST", "/v1/namespaces/db/tables/t");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body, descriptor.request);
        assert_eq!(
            requests[0].header("idempotency-key"),
            Some(descriptor.key.to_string().as_str())
        );
        assert_eq!(server.count("GET", "/v1/config"), 1);
    }

    #[tokio::test]
    async fn missing_or_invalid_capability_sends_once_without_key_and_stall_exits_unknown() {
        for lifetime in [None, Some(serde_json::json!("invalid"))] {
            let mut config = serde_json::json!({"defaults":{},"overrides":{}});
            if let Some(value) = lifetime {
                config["idempotency-key-lifetime"] = value;
            }
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::json(200, &config))
                    .on("POST", "/v1/namespaces/db/tables/t", Reply::NeverAnswer),
            );
            let stop = ConnectorStopOwner::new();
            let preparation = context(&stop);
            let prepared = PreparedRestMutation::prepare(
                endpoints(&server).await,
                route(),
                &descriptor(),
                ProviderBindingEpoch::new(),
                &preparation,
                expectation(),
                false,
                None,
            )
            .await
            .unwrap();
            assert!(prepared.binding().lifetime.is_none());
            let policy = CatalogEvidencePolicy {
                evidence: Duration::from_millis(30),
                ..Default::default()
            };
            let outcome = prepared
                .dispatch(policy.begin_evidence(&preparation).unwrap())
                .await;
            assert!(matches!(
                outcome.dispatch_evidence(),
                DispatchEvidence::Unknown { .. }
            ));
            let requests = server.requests_to("POST", "/v1/namespaces/db/tables/t");
            assert_eq!(requests.len(), 1);
            assert!(requests[0].header("idempotency-key").is_none());
        }
    }
}
