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

//! The provider-private Iceberg catalog owner.
//!
//! Design: ADR-0118 (docs/adr/ADR-0118-iceberg-provider-private-catalog-owner.md)
//!
//! [`NovaRocksCatalog`] is the only place Iceberg catalog semantics are
//! decided. Everything above it — metadata, DDL, CTAS, write, maintenance,
//! cleanup — asks this trait and never inspects a catalog kind, an optional
//! concrete client, or a capability slot.
//!
//! # Why the methods take owned requests
//!
//! The neutral SPI is synchronous, so every catalog call is polled through
//! `IcebergCatalogRuntime::block_on`, which spawns a bridge thread and
//! therefore requires `Future + Send + 'static`. Call sites satisfy that by
//! moving an `Arc<dyn NovaRocksCatalog>` and an owned request into an
//! `async move` block:
//!
//! ```ignore
//! let catalog = Arc::clone(context.novarocks_catalog());
//! runtime.block_on(async move { catalog.load_table(request).await })
//! ```
//!
//! The block owns everything it touches, so it is `'static` even though the
//! trait methods borrow `&self`. Requests must not borrow for the same reason.
//!
//! # What is deliberately absent
//!
//! There is no `rename_table` or `rename_view`: the neutral
//! `ConnectorCatalogMutationOperation` has no rename operation, so such methods
//! would be surface no caller reaches and no test can exercise. The trait
//! covers the operations the provider actually performs, and grows only when a
//! real caller appears.
//!
//! There is no `register_table`: every supported write protocol already
//! publishes its own metadata pointer. The retained `anchor_written_metadata`
//! call performs pure admission and returns NoOp; it cannot create namespaces
//! or register another pointer after a write has committed.

pub(crate) mod admission;
#[cfg(test)]
pub(crate) mod admission_test_support;
pub(crate) mod catalog_request;
pub(crate) mod delegate;
pub(crate) mod dispatch;
pub(crate) mod error;
pub(crate) mod factory;
pub(crate) mod hadoop;
pub(crate) mod hive;
#[cfg(feature = "mem-1-m07-hms-listing-observe")]
pub(crate) mod hms_listing_observer;
pub(crate) mod listing_admission;
pub(crate) mod metadata_read;
#[cfg(test)]
mod metadata_read_tests;
pub(crate) mod publication;
pub(crate) mod publication_dispatch;
pub(crate) mod rest;
pub(crate) mod table_coordination;
pub(crate) mod view_request;

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::{ConnectorError, ConnectorListingBound, ConnectorMutationFailure};

use self::error::{CatalogOutcome, CatalogUnsupported};
pub(crate) use self::metadata_read::{CatalogMetadataLoad, CatalogMetadataValidator};

/// A namespace this catalog can be asked about.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CatalogNamespaceName {
    pub(crate) namespace: Arc<str>,
}

impl CatalogNamespaceName {
    pub(crate) fn new(namespace: impl Into<Arc<str>>) -> Self {
        Self {
            namespace: namespace.into(),
        }
    }
}

impl std::fmt::Display for CatalogNamespaceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.namespace)
    }
}

/// A table or view this catalog can be asked about.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CatalogTableName {
    pub(crate) namespace: Arc<str>,
    pub(crate) name: Arc<str>,
}

/// One provider-owned catalog page. The continuation remains opaque outside
/// the REST implementation and is never synthesized from table names.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogTablePage {
    pub(crate) tables: Vec<CatalogTableName>,
    pub(crate) next_page_token: Option<Arc<str>>,
}

impl CatalogTableName {
    pub(crate) fn new(namespace: impl Into<Arc<str>>, name: impl Into<Arc<str>>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }

    pub(crate) fn from_identifier(ident: &crate::iceberg::TableIdent) -> Self {
        Self::new(ident.namespace.as_ref().join("."), ident.name.clone())
    }

    pub(crate) fn canonical(&self) -> Arc<str> {
        Arc::from(format!("{}.{}", self.namespace, self.name))
    }
}

impl std::fmt::Display for CatalogTableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.namespace, self.name)
    }
}

/// Only authoritative metadata facts cross a commit reload. REST response
/// credentials never replace the original operation's storage authority.
#[derive(Debug)]
pub(crate) struct CatalogCommitBase {
    pub metadata: crate::iceberg::spec::TableMetadata,
    pub metadata_location: String,
}

impl CatalogCommitBase {
    pub(crate) fn from_table(table: crate::iceberg::table::Table) -> crate::iceberg::Result<Self> {
        let metadata_location = table
            .metadata_location()
            .ok_or_else(|| {
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::DataInvalid,
                    "Commit reload returned no metadata location",
                )
            })?
            .to_owned();
        Ok(Self {
            metadata: table.metadata().clone(),
            metadata_location,
        })
    }
}

/// What a table creation is ultimately for.
///
/// This is the reason admission is a per-request question rather than a
/// per-catalog flag. A Hadoop catalog can publish an empty table atomically
/// (ADR-0077) but cannot satisfy a standard staged CTAS; a REST catalog can
/// create a table yet still be unable to run CTAS safely when it has no
/// enumerable staging root. One boolean cannot say either thing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CatalogCreateIntent {
    /// `CREATE TABLE` with no rows. The catalog owns the whole frontier.
    EmptyTable,
    /// `CREATE TABLE AS SELECT`. A source runs and a writer dispatches between
    /// admission and publication, so the catalog must prove the whole path
    /// before the first side effect.
    CreateTableAsSelect,
}

impl CatalogCreateIntent {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::EmptyTable => "empty-table",
            Self::CreateTableAsSelect => "ctas",
        }
    }
}

/// A drop that committed, with the exact identity captured before the drop.
///
/// The identity is captured ahead of the catalog request precisely because it
/// is unreadable afterwards, and post-commit cleanup may only ever act on
/// exact identity. See ADR-0118 on the catalog/filesystem split.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CatalogDropTableReceipt {
    pub(crate) table_uuid: Option<Arc<str>>,
    pub(crate) table_location: Option<Arc<str>>,
    pub(crate) metadata_location: Option<Arc<str>>,
    /// The table's own last-updated timestamp, in lake time, read before the
    /// drop. Age evidence for later collection; this owner never compares it.
    pub(crate) last_updated_ms: i64,
}

/// Inputs for a conditional create.
#[derive(Debug)]
#[cfg(test)]
pub(crate) struct ConditionalCreateRequest {
    pub(crate) namespace: CatalogNamespaceName,
    pub(crate) creation: crate::iceberg::TableCreation,
    /// Caller-owned operation identity, stamped into the attempt so a later
    /// adjudication can recognise this exact attempt rather than any create.
    pub(crate) operation_id: Arc<str>,
}

/// A prepared-but-unpublished conditional create.
///
/// Opaque above this module: the concrete attempt state belongs to whichever
/// implementation prepared it.
#[derive(Debug)]
#[cfg(test)]
pub(crate) struct ConditionalCreateAttempt {
    pub(crate) target: CatalogTableName,
    pub(crate) facts: ConditionalCreateFacts,
    inner: ConditionalCreateAttemptState,
}

#[derive(Debug)]
#[cfg(test)]
enum ConditionalCreateAttemptState {
    Hadoop(Box<crate::hadoop_catalog::HadoopCreateAttempt>),
}

#[cfg(test)]

impl ConditionalCreateAttempt {
    pub(crate) fn hadoop(
        attempt: crate::hadoop_catalog::HadoopCreateAttempt,
        facts: ConditionalCreateFacts,
        target: CatalogTableName,
    ) -> Self {
        Self {
            target,
            facts,
            inner: ConditionalCreateAttemptState::Hadoop(Box::new(attempt)),
        }
    }

    pub(crate) fn into_hadoop(self) -> Option<crate::hadoop_catalog::HadoopCreateAttempt> {
        match self.inner {
            ConditionalCreateAttemptState::Hadoop(attempt) => Some(*attempt),
        }
    }
}

/// Facts a prepared attempt exposes so its caller can build publication
/// evidence before the attempt is dispatched.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ConditionalCreateFacts {
    pub(crate) operation_id: Arc<str>,
    pub(crate) table_uuid: Arc<str>,
    pub(crate) metadata_location: Arc<str>,
    pub(crate) metadata_digest: Arc<str>,
}

/// What a published conditional create produced.
// Produced only by `publish_conditional_create`; see the note there.
#[allow(dead_code)]
#[derive(Clone, Debug)]
#[cfg(test)]
pub(crate) struct ConditionalCreateReceipt {
    pub(crate) facts: ConditionalCreateFacts,
    pub(crate) already_existed: bool,
    pub(crate) authoritative_table_uuid: Arc<str>,
    pub(crate) authoritative_metadata_digest: Arc<str>,
    /// Metadata location the published table now resolves to.
    pub(crate) published_metadata_location: Option<Arc<str>>,
    /// A failure that happened after the create committed. It never downgrades
    /// the commit; it is finalization state.
    pub(crate) finalization_failure: Option<Arc<str>>,
}

/// Identity a later adjudication compares against.
#[derive(Clone, Debug)]
pub(crate) struct ConditionalCreateEvidence {
    pub(crate) namespace: Arc<str>,
    pub(crate) table: Arc<str>,
    pub(crate) expected_table_uuid: Arc<str>,
    pub(crate) metadata_location: Arc<str>,
    pub(crate) metadata_digest: Arc<str>,
}

/// Read-only verdict on a conditional create.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ConditionalCreateVerdict {
    /// Exact positive: this attempt's metadata is present.
    Committed {
        finalization_failure: Option<Arc<str>>,
    },
    /// Nothing is there. Not proof the request never landed elsewhere.
    Absent,
    /// Something is there, but it is not this attempt's.
    Foreign,
}

/// What a staged create produced, keeping every dispatch distinction the
/// underlying protocol makes.
///
/// The arms mirror the REST client's typed staged-create result rather than
/// collapsing it: `Conflict` is a definite server rejection, `KnownUncommitted`
/// proves the request never went out, and `CommitUnknown` means it may have.
/// Flattening these is how a lost response becomes a duplicate publication.
#[cfg(test)]
pub(crate) enum StagedCreateStart {
    Staged {
        /// The table response must not select a FileIO before a vended
        /// delegation, when present, has entered the query-attempt collector.
        table: crate::loaded_table::IcebergLoadedTableMaterialization,
        initialization_updates: Vec<crate::iceberg::TableUpdate>,
        /// Response-local vended material.  It is intentionally not part of
        /// the table or staged-write payload; the caller must hand it to the
        /// query-attempt lease owner or refuse the operation.
        access_delegation: crate::loaded_table::IcebergAccessDelegation,
    },
    Conflict(String),
    /// Proven not applied: never dispatched, or rejected before publication
    /// (D13). The failure keeps its kind.
    KnownUncommitted(ConnectorMutationFailure),
    CommitUnknown(String),
    /// This catalog has no staged-create protocol. Proven zero side effect.
    Unsupported(CatalogUnsupported),
}

pub(crate) struct StagedCreateMaterial {
    pub table: crate::loaded_table::IcebergLoadedTableMaterialization,
    pub initialization_updates: Vec<crate::iceberg::TableUpdate>,
    pub access_delegation: crate::loaded_table::IcebergAccessDelegation,
}

/// What the final publication of a staged create must show to be attributed
/// to this operation (spec 5.7 items 4-6).
#[derive(Clone, Debug)]
pub(crate) struct StagedPublicationExpectation {
    /// The UUID the stage froze.
    pub(crate) staged_table_uuid: uuid::Uuid,
    /// The snapshot this publication adds to `main`, if it writes data.
    pub(crate) snapshot_id: Option<i64>,
    /// Properties NovaRocks itself names: the operation marker and any
    /// document properties.
    pub(crate) properties: Vec<(String, String)>,
}

/// What committing a staged create produced.
#[derive(Debug)]
#[cfg(test)]
pub(crate) enum StagedCommitResult {
    /// The receipt: this publication's snapshot and the table UUID.
    Committed(transaction::CommitProof),
    /// A received 409: another table holds the name.
    Conflict(ConnectorMutationFailure),
    /// Proven not applied: never dispatched, or rejected before publication.
    KnownUncommitted(ConnectorMutationFailure),
    CommitUnknown(String),
    Unsupported(CatalogUnsupported),
}

/// The single semantic owner of Iceberg catalog behavior.
///
/// Implementations are chosen once per control generation by
/// [`factory::NovaRocksCatalogFactory`] and are never downcast, kind-checked,
/// or unwrapped by callers.
///
/// # Read methods
///
/// Reads have no publication frontier. They must keep `NotFound`,
/// `Unsupported`, and `Unavailable` distinguishable: an implementation that
/// cannot answer returns `Unsupported` and never fabricates `false` or an empty
/// collection. That fiction is what this owner exists to remove.
///
/// # Mutation methods
///
/// Direct mutations own exactly one external frontier with no caller-owned gap
/// in front of it, so they answer with [`CatalogOutcome`] directly. Operations
/// that do have such a gap — a source to run, a writer to dispatch, staged
/// objects to publish — go through a transaction constructor instead, so that
/// admission completes before the first side effect.
#[async_trait]
pub(crate) trait NovaRocksCatalog: Debug + Send + Sync + 'static {
    /// Human-readable implementation name, for diagnostics and log lines only.
    ///
    /// This must never be consulted to decide whether an operation is
    /// supported. Ask the operation.
    // Identity for assertions; nothing in production needs to ask.
    #[allow(dead_code)]
    fn implementation_name(&self) -> &'static str;

    #[cfg(not(test))]
    fn bind_publication_generation(
        &self,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
    ) -> Result<(), ConnectorError>;
    #[cfg(test)]
    fn bind_publication_generation(
        &self,
        _generation: novarocks_spi::connector::ProviderBindingEpoch,
    ) -> Result<(), ConnectorError> {
        Ok(())
    }

    /// A read-only positive attribution proves publication, but cannot claim
    /// that a lost owner's independent local finalization actually ran.
    #[cfg(not(test))]
    fn positive_attribution_finalization(
        &self,
    ) -> novarocks_spi::connector::ExternalMutationFinalization;
    #[cfg(test)]
    fn positive_attribution_finalization(
        &self,
    ) -> novarocks_spi::connector::ExternalMutationFinalization {
        novarocks_spi::connector::ExternalMutationFinalization::Complete
    }

    #[cfg(not(test))]
    fn evidence_policy(&self) -> publication::CatalogEvidencePolicy;
    #[cfg(test)]
    fn evidence_policy(&self) -> publication::CatalogEvidencePolicy {
        publication::CatalogEvidencePolicy::default()
    }

    #[cfg(not(test))]
    async fn prepare_frozen_publication(
        &self,
        request: crate::commit::model::FrozenRequest,
        operation: admission::CatalogOperation,
        staged_expectation: Option<StagedPublicationExpectation>,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
    ) -> publication_dispatch::PreparationResult<publication_dispatch::PreparedPublication>;

    /// Freeze one ordinary create using the admitted owner's exact mechanism.
    /// This synchronous boundary builds immutable facts only.
    #[cfg(not(test))]
    fn freeze_create_publication(
        &self,
        request: catalog_request::CatalogCreatePublicationRequest,
    ) -> publication_dispatch::PreparationResult<catalog_request::FrozenCatalogPublication>;
    #[cfg(test)]
    fn freeze_create_publication(
        &self,
        _request: catalog_request::CatalogCreatePublicationRequest,
    ) -> publication_dispatch::PreparationResult<catalog_request::FrozenCatalogPublication> {
        Err(publication_dispatch::PreparationFailure(
            novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                "Catalog has no local create freezer",
            ),
        ))
    }

    /// Artifactless requests use the same journal executor as frozen commits.
    /// Catalogs without this mechanism refuse before external side effects.
    async fn prepare_catalog_publication(
        &self,
        _request: catalog_request::FrozenCatalogPublication,
        _descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        _generation: novarocks_spi::connector::ProviderBindingEpoch,
        _preparation: novarocks_spi::connector::ConnectorRequestContext,
        _document: catalog_request::CatalogResponseDocument,
        _read_binding: crate::access_binding::IcebergReadBinding,
    ) -> publication_dispatch::PreparationResult<publication_dispatch::PreparedPublication> {
        Err(publication_dispatch::PreparationFailure(
            ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                "Catalog has no prepared artifactless publication mechanism",
            ),
        ))
    }

    /// Post-proof materialization has no authority to decide publication.
    fn materialize_staged_create_response(
        &self,
        _table: CatalogTableName,
        _document: Vec<u8>,
    ) -> Result<StagedCreateMaterial, ConnectorError> {
        Err(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "Catalog has no staged-create response materialization",
        ))
    }
    #[cfg(test)]
    async fn prepare_frozen_publication(
        &self,
        _request: crate::commit::model::FrozenRequest,
        _operation: admission::CatalogOperation,
        _staged_expectation: Option<StagedPublicationExpectation>,
        _descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        _generation: novarocks_spi::connector::ProviderBindingEpoch,
        _preparation: novarocks_spi::connector::ConnectorRequestContext,
    ) -> publication_dispatch::PreparationResult<publication_dispatch::PreparedPublication> {
        Err(publication_dispatch::PreparationFailure(
            novarocks_spi::connector::ConnectorMutationFailure::new(
                novarocks_spi::connector::ConnectorMutationFailureKind::Unsupported,
                "Test catalog has not installed a prepared publication mechanism",
            ),
        ))
    }

    /// Explicit test admission capability; production has no policy bypass.
    #[cfg(test)]
    async fn prepare_frozen_for_admission_test(
        &self,
        request: crate::commit::model::FrozenRequest,
        operation: admission::CatalogOperation,
        staged_expectation: Option<StagedPublicationExpectation>,
        descriptor: crate::commit::publication_descriptor::PublicationDescriptor,
        generation: novarocks_spi::connector::ProviderBindingEpoch,
        preparation: novarocks_spi::connector::ConnectorRequestContext,
    ) -> publication_dispatch::PreparationResult<publication_dispatch::PreparedPublication> {
        self.prepare_frozen_publication(
            request,
            operation,
            staged_expectation,
            descriptor,
            generation,
            preparation,
        )
        .await
    }

    /// The vendored client this owner wraps.
    ///
    /// Migration seam, removed with the last unmigrated caller. It exists so a
    /// generation holds exactly *one* catalog while the operation families move
    /// across one at a time. Building a second client for the legacy handle
    /// instead would give two clients with separate in-memory state that
    /// disagree about the same lake — a dropped table still visible through the
    /// other handle, for instance.
    #[cfg(test)]
    fn vendored_client(&self) -> Arc<dyn crate::iceberg::Catalog>;

    /// The provider-private REST client that may refresh an already-admitted
    /// vended credential scope. Only the REST implementation owns such a
    /// capability; callers never downcast or construct a second catalog.
    fn vended_credential_refresh_catalog(
        &self,
    ) -> Option<Arc<crate::rest_protocol::endpoints::RestEndpoints>> {
        None
    }

    /// The one admission domain shared by this catalog generation's listings,
    /// including maintenance object listings.
    fn listing_admission(&self) -> Arc<listing_admission::ListingAdmission>;

    // ---- A. Reads -------------------------------------------------------
    //
    // Every enumeration is bounded at its source by the caller's
    // `ConnectorListingBound`: a listing that would exceed it is refused with
    // `ResourceExhausted`, never truncated. A source that can page does so
    // with pages no larger than `bound.page_entries`; a source that cannot is
    // checked as one complete listing.

    async fn list_namespaces(
        &self,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError>;

    async fn list_namespaces_for_read(
        &self,
        _binding: crate::access_binding::IcebergReadBinding,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.list_namespaces(bound).await
    }

    async fn namespace_exists(
        &self,
        namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError>;

    async fn namespace_exists_for_read(
        &self,
        namespace: CatalogNamespaceName,
        _binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<bool, ConnectorError> {
        self.namespace_exists(namespace).await
    }

    async fn list_tables(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError>;

    async fn list_tables_for_read(
        &self,
        namespace: CatalogNamespaceName,
        _binding: crate::access_binding::IcebergReadBinding,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        self.list_tables(namespace, bound).await
    }

    async fn list_tables_page(
        &self,
        _namespace: CatalogNamespaceName,
        _page_token: Option<Arc<str>>,
        _page_size: usize,
    ) -> Result<CatalogTablePage, ConnectorError> {
        Err(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "this Iceberg catalog has no bounded table pagination capability",
        ))
    }

    async fn table_exists(&self, table: CatalogTableName) -> Result<bool, ConnectorError>;

    async fn table_exists_for_read(
        &self,
        table: CatalogTableName,
        _binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<bool, ConnectorError> {
        self.table_exists(table).await
    }

    async fn load_table(
        &self,
        table: CatalogTableName,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError>;

    /// Load one table for an admitted read without retaining that read's
    /// control in the catalog generation. A filesystem catalog overrides this
    /// to bind its metadata and version-hint reads to the caller's FileIO.
    async fn load_table_for_read(
        &self,
        table: CatalogTableName,
        _binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        self.load_table(table).await
    }

    /// Read a complete authoritative metadata representation for reclamation.
    /// The caller retains its original storage authority and full baseline.
    /// Non-REST owners return a fresh representation without a validator.
    async fn load_complete_metadata_for_read(
        &self,
        table: CatalogTableName,
        binding: crate::access_binding::IcebergReadBinding,
        validator: Option<&CatalogMetadataValidator>,
    ) -> Result<CatalogMetadataLoad, ConnectorError> {
        use novarocks_spi::connector::{ConnectorErrorKind, ConnectorOperationControl};
        let context = binding.request_context().cloned().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Complete metadata load requires the original admitted request context",
            )
        })?;
        context.check_active()?;
        if validator.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "This catalog cannot validate a REST metadata representation",
            ));
        }
        let loaded = self.load_table_for_read(table, binding).await?;
        context.check_active()?;
        metadata_read::complete_metadata(
            loaded.metadata().clone(),
            loaded.metadata_location(),
            None,
        )
    }

    /// Reload commit metadata using the operation's authorized object-store
    /// I/O. Catalog HTTP calls remain catalog-owned; filesystem pointer and
    /// metadata reads must use the supplied governor, never a cached FileIO.
    async fn load_commit_base(
        &self,
        _table: CatalogTableName,
        _file_io: crate::iceberg::io::FileIO,
    ) -> crate::iceberg::Result<CatalogCommitBase> {
        Err(crate::iceberg::Error::new(
            crate::iceberg::ErrorKind::FeatureUnsupported,
            "Catalog has no admitted commit-base loading mechanism",
        ))
    }

    /// Whether the view exists.
    ///
    /// An implementation whose catalog format cannot hold views returns
    /// `Unsupported`, not `Ok(false)`. Callers that need "is this name a view?"
    /// as a hint may map `Unsupported` to "not a view" themselves; callers that
    /// need an authoritative enumeration must surface the refusal.
    async fn view_exists(&self, view: CatalogTableName) -> Result<bool, ConnectorError>;

    #[cfg(not(test))]
    async fn view_exists_for_request(
        &self,
        view: CatalogTableName,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<bool, ConnectorError>;
    #[cfg(test)]
    async fn view_exists_for_request(
        &self,
        view: CatalogTableName,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<bool, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        context.check_active()?;
        let result = self.view_exists(view).await?;
        context.check_active()?;
        Ok(result)
    }
    #[cfg(not(test))]
    async fn load_view_for_request(
        &self,
        view: CatalogTableName,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError>;
    #[cfg(test)]
    async fn load_view_for_request(
        &self,
        view: CatalogTableName,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        context.check_active()?;
        let result = self.load_view(view).await?;
        context.check_active()?;
        Ok(result)
    }

    /// Enumerate views. See [`NovaRocksCatalog::view_exists`] on why this is
    /// not allowed to answer with an empty vector when it cannot answer.
    async fn list_views_for_request(
        &self,
        namespace: CatalogNamespaceName,
        context: novarocks_spi::connector::ConnectorRequestContext,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        context.check_active()?;
        self.list_views(namespace, bound).await
    }

    async fn list_tables_page_for_request(
        &self,
        namespace: CatalogNamespaceName,
        page_token: Option<Arc<str>>,
        page_size: usize,
        context: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<CatalogTablePage, ConnectorError> {
        use novarocks_spi::connector::ConnectorOperationControl;
        context.check_active()?;
        self.list_tables_page(namespace, page_token, page_size)
            .await
    }

    async fn list_views(
        &self,
        namespace: CatalogNamespaceName,
        bound: ConnectorListingBound,
    ) -> Result<Vec<String>, ConnectorError>;

    async fn load_view(
        &self,
        view: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError>;

    // ---- B. Direct mutations -------------------------------------------

    /// Create a view. A catalog without views answers `Unsupported`.
    #[cfg(test)]
    async fn create_view(
        &self,
        namespace: CatalogNamespaceName,
        _creation: view_request::ViewCreate,
    ) -> CatalogOutcome<CatalogTableName> {
        CatalogOutcome::unsupported(format!(
            "{} Iceberg catalog does not support views (namespace {})",
            self.implementation_name(),
            namespace.namespace
        ))
    }

    /// Replace a view's definition; the replacement freezes the view UUID.
    #[cfg(test)]
    async fn replace_view(
        &self,
        replace: view_request::ValidatedViewReplace,
    ) -> CatalogOutcome<CatalogTableName> {
        CatalogOutcome::unsupported(format!(
            "{} Iceberg catalog does not support views ({})",
            self.implementation_name(),
            replace.ident()
        ))
    }

    /// Drop a view.
    #[cfg(test)]
    async fn drop_view(&self, view: CatalogTableName) -> CatalogOutcome<CatalogTableName> {
        CatalogOutcome::unsupported(format!(
            "{} Iceberg catalog does not support views ({view})",
            self.implementation_name()
        ))
    }

    #[cfg(test)]

    async fn create_namespace(
        &self,
        namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName>;

    #[cfg(test)]

    async fn drop_namespace(
        &self,
        namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName>;

    /// Drop a table from the catalog.
    ///
    /// This removes catalog visibility only. Object deletion is the filesystem
    /// owner's job and may only proceed on `KnownCommitted`; see ADR-0118.
    #[cfg(test)]
    async fn drop_table(&self, table: CatalogTableName) -> CatalogOutcome<CatalogDropTableReceipt>;

    /// Admit an already committed write without another external mutation.
    /// Supported write protocols publish their own pointer. Unsupported
    /// catalogs still refuse; this boundary grants no registration authority.
    async fn anchor_written_metadata(
        &self,
        table: CatalogTableName,
        metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName>;

    /// Create an invisible target for a staged publication.
    ///
    /// The staged table is not reachable through the catalog until it is
    /// committed, which is what makes a CTAS absent-or-complete. A catalog with
    /// no such protocol answers `Unsupported`, and never by creating a visible
    /// empty table to fill in afterwards.
    #[cfg(test)]
    async fn stage_create_table(
        &self,
        namespace: CatalogNamespaceName,
        creation: crate::iceberg::TableCreation,
    ) -> StagedCreateStart;

    /// Publish a staged create with exactly one assert-create request,
    /// attributed against `expectation` before any evidence is reported.
    #[cfg(test)]
    async fn commit_staged_table(
        &self,
        commit: crate::iceberg::TableCommit,
        expectation: StagedPublicationExpectation,
    ) -> StagedCommitResult;

    /// Stage a conditional create against storage, without publishing it.
    ///
    /// A filesystem catalog's create is not a catalog call: its linearization
    /// point is a conditional write of the canonical metadata (ADR-0077). That
    /// primitive has no equivalent on the generic catalog trait, so it lives
    /// here rather than being reached for through a concrete client -- which is
    /// how a catalog kind used to escape the factory.
    ///
    /// Preparing is local: it builds metadata and sends nothing.
    #[cfg(test)]
    async fn prepare_conditional_create(
        &self,
        request: ConditionalCreateRequest,
    ) -> CatalogOutcome<ConditionalCreateAttempt>;

    /// Publish a prepared conditional create with exactly one storage request.
    // Reached only by tests, which is the only way the lost-response path can
    // be exercised at all: production publishes through `ConditionalCreateDispatch`.
    #[allow(dead_code)]
    #[cfg(test)]
    async fn publish_conditional_create(
        &self,
        attempt: ConditionalCreateAttempt,
    ) -> CatalogOutcome<ConditionalCreateReceipt>;

    /// Read-only adjudication of a conditional create whose outcome is unknown.
    ///
    /// Exact-positive only: an absent target proves nothing, and this must never
    /// write or delete.
    async fn adjudicate_conditional_create(
        &self,
        evidence: ConditionalCreateEvidence,
    ) -> Result<ConditionalCreateVerdict, ConnectorError>;

    async fn adjudicate_conditional_create_for_read(
        &self,
        _evidence: ConditionalCreateEvidence,
        _binding: crate::access_binding::IcebergReadBinding,
    ) -> Result<ConditionalCreateVerdict, ConnectorError> {
        Err(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "This catalog has no request-bound conditional-create observation",
        ))
    }

    /// One local operation rule, shared by admission and the mutation owner.
    fn admit_operation(
        &self,
        operation: &admission::CatalogOperation,
        target: &admission::CatalogAdmissionTarget,
    ) -> Result<(), CatalogUnsupported>;

    fn admit_initiation(
        &self,
        _request: &admission::CatalogAdmissionRequest,
    ) -> Result<admission::CatalogAdmission, CatalogUnsupported> {
        Ok(admission::CatalogAdmission::Admitted)
    }

    // Design: ADR-0169 (docs/adr/ADR-0169-read-only-hms-and-single-writer-admission.md)
    fn admit(
        &self,
        request: &admission::CatalogAdmissionRequest,
    ) -> Result<admission::CatalogAdmission, CatalogUnsupported> {
        request.operation.validate_target(&request.target)?;
        self.admit_operation(&request.operation, &request.target)?;
        self.admit_initiation(request)
    }
}

/// The answer of a catalog that cannot hold views. It is given by the owner
/// itself: no listing admission, no SDK call.
pub(super) fn views_unsupported(operation: &str) -> ConnectorError {
    ConnectorError::new(
        novarocks_spi::connector::ConnectorErrorKind::Unsupported,
        format!("{operation} is not supported by this catalog"),
    )
}

pub(crate) mod transaction;

#[cfg(all(test, feature = "mem-1-m07-hms-listing-observe"))]
#[path = "../hms_listing_probe_tests.rs"]
mod hms_listing_probe_tests;
