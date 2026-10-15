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

//! The transitional `iceberg::Catalog` the direct paths reach through
//! `vendored_client()` until IRU-6a moves them onto the owner (L13).
//!
//! All reads and mutations use the generation's provider-owned endpoints and
//! single establishment gate. Every mutation is a
//! NovaRocks request with D13 evidence, and its failure has exactly two error
//! shapes: a received 409 keeps its dedicated kind, and anything else is an
//! explicitly built `Unexpected`. Both carry the typed evidence as their
//! source, so the shared outcome judgment reads the evidence, while the direct
//! paths' own classifiers can never derive a definite rejection from a REST
//! failure other than 409.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::ConnectorMutationFailureKind;

use crate::iceberg::spec::TableMetadata;
use crate::iceberg::table::Table;
use crate::iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent, TableRequirement, TableUpdate,
};
use crate::rest_protocol::wire::{CommitTableResponse, CreateTableRequest, LoadTableResult};

use crate::rest_protocol::deferred::FrozenLoadTable;
use crate::rest_protocol::endpoints::RestEndpoints;
use crate::rest_protocol::evidence::{
    AttributionExpectation, ExpectedSnapshot, NotDispatchedCause, RestEvidence,
};
use crate::rest_protocol::mutation::MutationOutcome;

/// Typed evidence attached as the source of an adapter mutation error.
///
/// Its `Display` is the evidence's fixed text: `iceberg::Error` prints its
/// source, and nothing server-controlled may reach that text (L29).
pub(crate) struct RestEvidenceCarrier(RestEvidence);

impl RestEvidenceCarrier {
    pub(crate) fn new(evidence: RestEvidence) -> Self {
        Self(evidence)
    }

    pub(crate) fn evidence(&self) -> &RestEvidence {
        &self.0
    }
}

impl fmt::Display for RestEvidenceCarrier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0.message())
    }
}

impl fmt::Debug for RestEvidenceCarrier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, formatter)
    }
}

impl std::error::Error for RestEvidenceCarrier {}

/// The typed evidence a REST mutation error carries, if it is one.
pub(crate) fn rest_evidence(error: &Error) -> Option<&RestEvidence> {
    std::error::Error::source(error)?
        .downcast_ref::<RestEvidenceCarrier>()
        .map(RestEvidenceCarrier::evidence)
}

/// Which dedicated kind a received 409 keeps (L13).
#[derive(Clone, Copy)]
enum Conflict409 {
    Commit,
    TableExists,
    NamespaceExists,
    /// No dedicated kind: the 409 stays `Unexpected` with its evidence.
    None,
}

/// The generation's one transitional adapter.
pub(crate) struct RestTransitionalAdapter {
    endpoints: Arc<RestEndpoints>,
}

impl fmt::Debug for RestTransitionalAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestTransitionalAdapter")
            .field("endpoints", &self.endpoints)
            .finish()
    }
}

impl RestTransitionalAdapter {
    pub(crate) fn new(endpoints: Arc<RestEndpoints>) -> Self {
        Self { endpoints }
    }

    /// Materialize a successful table response the way upstream did: with
    /// the static generation's storage factory. A vended generation has none,
    /// so its success becomes an `Unexpected` carrying `Applied` evidence.
    fn materialize(
        &self,
        ident: TableIdent,
        metadata_location: Option<String>,
        metadata: TableMetadata,
        evidence: RestEvidence,
    ) -> Result<Table> {
        let Some(file_io) = self.endpoints.static_file_io() else {
            return Err(evidence_error(
                evidence,
                Conflict409::None,
                "REST catalog applied the request, but this vended generation cannot build its \
                 table object",
            ));
        };
        let response = LoadTableResult {
            metadata_location,
            metadata,
            config: HashMap::new(),
            storage_credentials: None,
        };
        let (frozen, _) =
            FrozenLoadTable::from_response(ident, response, self.endpoints.runtime().clone());
        frozen.materialize(file_io).map_err(|_| {
            evidence_error(
                evidence,
                Conflict409::None,
                "REST catalog applied the request, but its table object could not be built",
            )
        })
    }
}

/// Build an adapter mutation error. Never derived through `?` from serde,
/// url or other conversions (L13).
fn evidence_error(evidence: RestEvidence, conflict: Conflict409, text: &str) -> Error {
    let kind = match (&evidence, conflict) {
        (RestEvidence::Rejected { status: 409, .. }, Conflict409::Commit) => {
            ErrorKind::CatalogCommitConflicts
        }
        (RestEvidence::Rejected { status: 409, .. }, Conflict409::TableExists) => {
            ErrorKind::TableAlreadyExists
        }
        (RestEvidence::Rejected { status: 409, .. }, Conflict409::NamespaceExists) => {
            ErrorKind::NamespaceAlreadyExists
        }
        _ => ErrorKind::Unexpected,
    };
    let retryable = kind == ErrorKind::CatalogCommitConflicts;
    let error = Error::new(kind, text.to_string()).with_source(RestEvidenceCarrier(evidence));
    if retryable {
        // A rejected commit conflict is retried by its caller, as before.
        error.with_retryable(true)
    } else {
        error
    }
}

/// Turn a mutation outcome that is not `Applied` into its error.
fn failed(outcome: MutationOutcome, conflict: Conflict409) -> Error {
    let text = outcome.evidence.message();
    evidence_error(outcome.evidence, conflict, &text)
}

/// The decoded success document of an applied mutation.
fn decode<T: serde::de::DeserializeOwned>(
    outcome: MutationOutcome,
) -> std::result::Result<(T, RestEvidence), Error> {
    let MutationOutcome { evidence, document } = outcome;
    match document.and_then(|document| serde_json::from_slice::<T>(&document).ok()) {
        Some(value) => Ok((value, evidence)),
        None => Err(evidence_error(
            evidence,
            Conflict409::None,
            "REST catalog applied the request, but its response could not be decoded",
        )),
    }
}

/// The table UUID an existing-table commit froze (L34).
fn frozen_table_uuid(requirements: &[TableRequirement]) -> Option<uuid::Uuid> {
    let mut frozen = None;
    for requirement in requirements {
        if let TableRequirement::UuidMatch { uuid } = requirement {
            match frozen {
                None => frozen = Some(*uuid),
                Some(existing) if existing == *uuid => {}
                Some(_) => return None,
            }
        }
    }
    frozen
}

/// The snapshot this commit adds, with the ref that is set to it.
fn expected_snapshot(updates: &[TableUpdate]) -> Option<ExpectedSnapshot> {
    let snapshot_id = updates.iter().rev().find_map(|update| match update {
        TableUpdate::AddSnapshot { snapshot } => Some(snapshot.snapshot_id()),
        _ => None,
    })?;
    let target_ref = updates.iter().find_map(|update| match update {
        TableUpdate::SetSnapshotRef {
            ref_name,
            reference,
        } if reference.snapshot_id == snapshot_id => Some(ref_name.clone()),
        _ => None,
    });
    Some(ExpectedSnapshot::new(snapshot_id, target_ref))
}

fn not_dispatched(kind: ConnectorMutationFailureKind, cause: NotDispatchedCause) -> Error {
    let evidence = RestEvidence::NotDispatched { kind, cause };
    let text = evidence.message();
    evidence_error(evidence, Conflict409::None, &text)
}

fn read_error(error: novarocks_spi::connector::ConnectorError) -> Error {
    let kind = match error.kind() {
        novarocks_spi::connector::ConnectorErrorKind::NotFound => ErrorKind::TableNotFound,
        _ => ErrorKind::Unexpected,
    };
    Error::new(kind, error.message().to_string())
}

fn unsupported(operation: &str) -> Error {
    Error::new(
        ErrorKind::FeatureUnsupported,
        format!("NovaRocks does not issue REST {operation} requests"),
    )
}

#[async_trait]
impl Catalog for RestTransitionalAdapter {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        if parent.is_some() {
            return Err(unsupported("nested namespace listing"));
        }
        self.endpoints
            .list_namespaces_page(None, 1000)
            .await
            .map(|page| page.namespaces)
            .map_err(read_error)
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        #[cfg(not(test))]
        {
            Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::FeatureUnsupported,
                "Catalog mutation must use its prepared publication journal",
            ))
        }
        #[cfg(test)]
        {
            let outcome = self
                .endpoints
                .create_namespace(namespace, properties.clone())
                .await;
            if !matches!(outcome.evidence, RestEvidence::Applied { .. }) {
                return Err(failed(outcome, Conflict409::NamespaceExists));
            }
            Ok(Namespace::with_properties(namespace.clone(), properties))
        }
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.endpoints
            .load_namespace(namespace)
            .await
            .map_err(read_error)
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.endpoints
            .namespace_exists(namespace)
            .await
            .map_err(read_error)
    }

    async fn update_namespace(
        &self,
        _namespace: &NamespaceIdent,
        _properties: HashMap<String, String>,
    ) -> Result<()> {
        Err(unsupported("namespace property update"))
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        #[cfg(not(test))]
        {
            Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::FeatureUnsupported,
                "Catalog mutation must use its prepared publication journal",
            ))
        }
        #[cfg(test)]
        {
            let outcome = self.endpoints.drop_namespace(namespace).await;
            match outcome.evidence {
                RestEvidence::Applied { .. } => Ok(()),
                _ => Err(failed(outcome, Conflict409::None)),
            }
        }
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.endpoints
            .list_tables_page(namespace, None, 1000)
            .await
            .map(|page| page.identifiers)
            .map_err(read_error)
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        #[cfg(not(test))]
        {
            Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::FeatureUnsupported,
                "Catalog mutation must use its prepared publication journal",
            ))
        }
        #[cfg(test)]
        {
            let ident = TableIdent::new(namespace.clone(), creation.name.clone());
            let request = CreateTableRequest {
                name: creation.name,
                location: creation.location,
                schema: creation.schema,
                partition_spec: creation.partition_spec,
                write_order: creation.sort_order,
                stage_create: None,
                properties: creation.properties,
            };
            let outcome = self.endpoints.create_table(namespace, request).await;
            if !matches!(outcome.evidence, RestEvidence::Applied { .. }) {
                return Err(failed(outcome, Conflict409::TableExists));
            }
            let (response, evidence) = decode::<LoadTableResult>(outcome)?;
            self.materialize(
                ident,
                response.metadata_location,
                response.metadata,
                evidence,
            )
        }
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        let (frozen, _) = self
            .endpoints
            .load_table(table, false)
            .await
            .map_err(read_error)?;
        let file_io = self.endpoints.static_file_io().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "vended REST table requires delegated materialization",
            )
        })?;
        frozen.materialize(file_io)
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        #[cfg(not(test))]
        {
            Err(crate::iceberg::Error::new(
                crate::iceberg::ErrorKind::FeatureUnsupported,
                "Catalog mutation must use its prepared publication journal",
            ))
        }
        #[cfg(test)]
        {
            let outcome = self.endpoints.drop_table(table).await;
            match outcome.evidence {
                RestEvidence::Applied { .. } => Ok(()),
                _ => Err(failed(outcome, Conflict409::None)),
            }
        }
    }

    async fn purge_table(&self, _table: &TableIdent) -> Result<()> {
        Err(unsupported("purge"))
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.endpoints
            .table_exists(table)
            .await
            .map_err(|error| match error.kind() {
                novarocks_spi::connector::ConnectorErrorKind::NotFound => {
                    Error::new(ErrorKind::TableNotFound, error.message().to_string())
                }
                _ => Error::new(ErrorKind::Unexpected, error.message().to_string()),
            })
    }

    async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> Result<()> {
        Err(unsupported("rename"))
    }

    async fn register_table(
        &self,
        _table: &TableIdent,
        _metadata_location: String,
    ) -> Result<Table> {
        Err(unsupported("register"))
    }

    async fn update_table(&self, mut commit: TableCommit) -> Result<Table> {
        let ident = commit.identifier().clone();
        let requirements = commit.take_requirements();
        let updates = commit.take_updates();
        // An existing-table commit must name the incarnation it was produced
        // from; the adapter never reloads to fill it in (L34).
        let Some(table_uuid) = frozen_table_uuid(&requirements) else {
            return Err(not_dispatched(
                ConnectorMutationFailureKind::Internal,
                NotDispatchedCause::UnfrozenTableUuid,
            ));
        };
        let expect =
            match AttributionExpectation::table_commit(table_uuid, expected_snapshot(&updates)) {
                Ok(expect) => expect,
                Err(_) => {
                    return Err(not_dispatched(
                        ConnectorMutationFailureKind::Internal,
                        NotDispatchedCause::UnfrozenTableUuid,
                    ));
                }
            };
        let commit = TableCommit::builder()
            .ident(ident.clone())
            .requirements(requirements)
            .updates(updates)
            .build();
        let outcome = self.endpoints.commit_table(commit, &expect, true).await;
        if !matches!(outcome.evidence, RestEvidence::Applied { .. }) {
            return Err(failed(outcome, Conflict409::Commit));
        }
        let (response, evidence) = decode::<CommitTableResponse>(outcome)?;
        self.materialize(
            ident,
            Some(response.metadata_location),
            response.metadata,
            evidence,
        )
    }
}

#[cfg(test)]
mod tests {
    //! The transitional adapter's two error shapes (L13) and its refusal of
    //! an existing-table commit without a frozen UUID (L34), against a
    //! scripted server through the real session.
    use std::collections::HashMap;

    use novarocks_spi::connector::ConnectorMutationFailureKind as Kind;

    use super::{RestTransitionalAdapter, rest_evidence};
    use crate::iceberg::{
        Catalog, ErrorKind, NamespaceIdent, TableCommit, TableIdent, TableRequirement,
    };
    use crate::rest_protocol::evidence::RestEvidence;
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};

    const UUID: &str = "1a2b3c4d-0000-4000-8000-000000000001";

    async fn adapter(server: &TestServer) -> RestTransitionalAdapter {
        let (_upstream, endpoints) = crate::catalog_runtime::build_vended_rest_generation_for_test(
            HashMap::from([("uri".to_string(), server.url())]),
            None,
            crate::iceberg::Runtime::try_current().expect("test runtime"),
        )
        .await
        .expect("REST generation");
        RestTransitionalAdapter::new(endpoints)
    }

    fn scripted(method: &str, path: &str, reply: Reply) -> TestServer {
        TestServer::start(
            Script::new()
                .on("GET", "/v1/config", Reply::config(&[], &[], None))
                .on(method, path, reply),
        )
    }

    fn commit(requirements: Vec<TableRequirement>) -> TableCommit {
        TableCommit::builder()
            .ident(TableIdent::new(
                NamespaceIdent::new("db".into()),
                "t".into(),
            ))
            .requirements(requirements)
            .updates(vec![])
            .build()
    }

    fn frozen() -> Vec<TableRequirement> {
        vec![TableRequirement::UuidMatch {
            uuid: uuid::Uuid::parse_str(UUID).unwrap(),
        }]
    }

    #[tokio::test]
    async fn mutation_errors_are_a_409_kind_or_unexpected_with_evidence() {
        let db = NamespaceIdent::new("db".into());
        for status in [409, 401, 403, 404, 400, 500, 419] {
            let server = scripted("POST", "/v1/namespaces/db/tables/t", Reply::status(status));
            let error = adapter(&server)
                .await
                .update_table(commit(frozen()))
                .await
                .expect_err("not applied");
            let expected = if status == 409 {
                ErrorKind::CatalogCommitConflicts
            } else {
                ErrorKind::Unexpected
            };
            assert_eq!(error.kind(), expected, "commit HTTP {status}");
            assert!(rest_evidence(&error).is_some(), "commit HTTP {status}");

            let server = scripted("POST", "/v1/namespaces", Reply::status(status));
            let error = adapter(&server)
                .await
                .create_namespace(&db, HashMap::new())
                .await
                .expect_err("not applied");
            let expected = if status == 409 {
                ErrorKind::NamespaceAlreadyExists
            } else {
                ErrorKind::Unexpected
            };
            assert_eq!(error.kind(), expected, "namespace HTTP {status}");

            let server = scripted(
                "DELETE",
                "/v1/namespaces/db/tables/t",
                Reply::status(status),
            );
            let error = adapter(&server)
                .await
                .drop_table(&TableIdent::new(db.clone(), "t".into()))
                .await
                .expect_err("not applied");
            // A drop has no dedicated 409 kind; the evidence still says
            // rejected, so the shared judgment proves "not applied".
            assert_eq!(error.kind(), ErrorKind::Unexpected, "drop HTTP {status}");
        }
    }

    #[tokio::test]
    async fn a_404_never_becomes_a_definite_rejection_for_the_direct_paths() {
        let server = scripted("POST", "/v1/namespaces/db/tables/t", Reply::status(404));
        let error = adapter(&server)
            .await
            .update_table(commit(frozen()))
            .await
            .expect_err("unknown");
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(matches!(
            rest_evidence(&error),
            Some(RestEvidence::Unknown { .. })
        ));
        assert!(!crate::catalog::error::proves_uncommitted(&error));
    }

    #[tokio::test]
    async fn an_existing_table_commit_without_a_frozen_uuid_is_never_sent() {
        let server = scripted("POST", "/v1/namespaces/db/tables/t", Reply::status(200));
        let error = adapter(&server)
            .await
            .update_table(commit(vec![]))
            .await
            .expect_err("refused");
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        let Some(RestEvidence::NotDispatched { kind, .. }) = rest_evidence(&error) else {
            panic!("expected not-dispatched evidence: {error:?}");
        };
        assert_eq!(*kind, Kind::Internal);
        assert_eq!(server.count("POST", "/v1/namespaces/db/tables/t"), 0);
    }

    #[tokio::test]
    async fn a_response_for_another_table_incarnation_is_a_protocol_anomaly() {
        let response = serde_json::json!({
            "metadata-location": "s3://warehouse/db/t/metadata/2.json",
            "metadata": {"table-uuid": "ffffffff-0000-4000-8000-000000000000"},
        });
        let server = scripted(
            "POST",
            "/v1/namespaces/db/tables/t",
            Reply::json(200, &response),
        );
        let error = adapter(&server)
            .await
            .update_table(commit(frozen()))
            .await
            .expect_err("unknown");
        assert!(matches!(
            rest_evidence(&error),
            Some(RestEvidence::Unknown { .. })
        ));
    }

    #[tokio::test]
    async fn a_vended_success_without_a_table_object_stays_applied_evidence() {
        let metadata = serde_json::json!({
            "metadata-location": "s3://warehouse/db/t/metadata/2.json",
            "metadata": {"table-uuid": UUID},
        });
        let server = scripted(
            "POST",
            "/v1/namespaces/db/tables/t",
            Reply::json(200, &metadata),
        );
        let error = adapter(&server)
            .await
            .update_table(commit(frozen()))
            .await
            .expect_err("no table object in a vended generation");
        assert_eq!(error.kind(), ErrorKind::Unexpected);
        assert!(matches!(
            rest_evidence(&error),
            Some(RestEvidence::Applied { .. })
        ));
        // Applied is not a proof of "not applied": the direct path reports
        // an unknown outcome, as before.
        assert!(!crate::catalog::error::proves_uncommitted(&error));
    }
}
