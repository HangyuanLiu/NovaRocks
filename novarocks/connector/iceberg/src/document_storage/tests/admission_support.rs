// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0.

//! Side-effect tripwires for document-management admission tests.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use novarocks_spi::connector::ConnectorError;

use crate::catalog::error::{CatalogOutcome, CatalogUnsupported};
use crate::catalog::transaction::{CreateTableTransactionRequest, TransactionRequest};
use crate::catalog::{
    CatalogCreateIntent, CatalogDropTableReceipt, CatalogNamespaceName, CatalogTableName,
    CatalogTablePage, CatalogTransactionStart, ConditionalCreateAttempt, ConditionalCreateEvidence,
    ConditionalCreateReceipt, ConditionalCreateRequest, ConditionalCreateVerdict, NovaRocksCatalog,
    StagedCommitResult, StagedCreateStart,
};

#[derive(Debug)]
pub(super) struct AdmissionCatalogSpy {
    inner: Arc<dyn NovaRocksCatalog>,
    reads: AtomicUsize,
    mutations: AtomicUsize,
    client_accesses: AtomicUsize,
}

impl AdmissionCatalogSpy {
    pub(super) fn new(inner: Arc<dyn NovaRocksCatalog>) -> Self {
        Self {
            inner,
            reads: AtomicUsize::new(0),
            mutations: AtomicUsize::new(0),
            client_accesses: AtomicUsize::new(0),
        }
    }

    pub(super) fn assert_no_io(&self) {
        assert_eq!(self.reads.load(Ordering::SeqCst), 0, "catalog reads");
        assert_eq!(
            self.mutations.load(Ordering::SeqCst),
            0,
            "catalog mutations"
        );
        assert_eq!(
            self.client_accesses.load(Ordering::SeqCst),
            0,
            "catalog client access"
        );
    }

    fn read<T>(&self) -> T {
        self.reads.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported document admission attempted catalog I/O");
    }

    fn mutation<T>(&self) -> T {
        self.mutations.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported document admission attempted a catalog mutation");
    }
}

#[async_trait]
impl NovaRocksCatalog for AdmissionCatalogSpy {
    fn implementation_name(&self) -> &'static str {
        self.inner.implementation_name()
    }

    fn admit_create(&self, intent: CatalogCreateIntent) -> Result<(), CatalogUnsupported> {
        self.inner.admit_create(intent)
    }

    fn vendored_client(&self) -> Arc<dyn crate::iceberg::Catalog> {
        self.client_accesses.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported document admission requested the catalog client");
    }

    async fn list_namespaces(&self) -> Result<Vec<String>, ConnectorError> {
        self.read()
    }

    async fn namespace_exists(
        &self,
        _namespace: CatalogNamespaceName,
    ) -> Result<bool, ConnectorError> {
        self.read()
    }

    async fn list_tables(
        &self,
        _namespace: CatalogNamespaceName,
    ) -> Result<Vec<String>, ConnectorError> {
        self.read()
    }

    async fn list_tables_page(
        &self,
        _namespace: CatalogNamespaceName,
        _page_token: Option<Arc<str>>,
        _page_size: usize,
    ) -> Result<CatalogTablePage, ConnectorError> {
        self.read()
    }

    async fn table_exists(&self, _table: CatalogTableName) -> Result<bool, ConnectorError> {
        self.read()
    }

    async fn load_table(
        &self,
        _table: CatalogTableName,
    ) -> Result<crate::loaded_table::IcebergLoadedTable, ConnectorError> {
        self.read()
    }

    async fn view_exists(&self, _view: CatalogTableName) -> Result<bool, ConnectorError> {
        self.read()
    }

    async fn list_views(
        &self,
        _namespace: CatalogNamespaceName,
    ) -> Result<Vec<String>, ConnectorError> {
        self.read()
    }

    async fn load_view(
        &self,
        _view: CatalogTableName,
    ) -> Result<crate::iceberg::spec::ViewMetadata, ConnectorError> {
        self.read()
    }

    async fn create_namespace(
        &self,
        _namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        self.mutation()
    }

    async fn drop_namespace(
        &self,
        _namespace: CatalogNamespaceName,
    ) -> CatalogOutcome<CatalogNamespaceName> {
        self.mutation()
    }

    async fn drop_table(
        &self,
        _table: CatalogTableName,
    ) -> CatalogOutcome<CatalogDropTableReceipt> {
        self.mutation()
    }

    async fn anchor_written_metadata(
        &self,
        _table: CatalogTableName,
        _metadata_location: Arc<str>,
    ) -> CatalogOutcome<CatalogTableName> {
        self.mutation()
    }

    async fn new_transaction(&self, _request: TransactionRequest) -> CatalogTransactionStart {
        self.mutation()
    }

    async fn stage_create_table(
        &self,
        _namespace: CatalogNamespaceName,
        _creation: crate::iceberg::TableCreation,
    ) -> StagedCreateStart {
        self.mutation()
    }

    async fn commit_staged_table(
        &self,
        _commit: crate::iceberg::TableCommit,
        _request_file_io: crate::iceberg::io::FileIO,
    ) -> StagedCommitResult {
        self.mutation()
    }

    async fn prepare_conditional_create(
        &self,
        _request: ConditionalCreateRequest,
    ) -> CatalogOutcome<ConditionalCreateAttempt> {
        self.mutation()
    }

    async fn publish_conditional_create(
        &self,
        _attempt: ConditionalCreateAttempt,
    ) -> CatalogOutcome<ConditionalCreateReceipt> {
        self.mutation()
    }

    async fn adjudicate_conditional_create(
        &self,
        _evidence: ConditionalCreateEvidence,
    ) -> Result<ConditionalCreateVerdict, ConnectorError> {
        self.read()
    }

    async fn new_create_table_transaction(
        &self,
        _request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        self.mutation()
    }

    async fn new_create_or_replace_table_transaction(
        &self,
        _request: CreateTableTransactionRequest,
    ) -> CatalogTransactionStart {
        self.mutation()
    }
}

/// Records every filesystem runtime entry without driving a future. The
/// warehouse and reserved metastore endpoint are checked separately, so a
/// passing test does not confuse an unpolled request with an external effect.
#[derive(Default)]
pub(super) struct AdmissionFileIoSpy {
    dispatches: AtomicUsize,
}

impl AdmissionFileIoSpy {
    pub(super) fn assert_no_dispatch(&self) {
        assert_eq!(
            self.dispatches.load(Ordering::SeqCst),
            0,
            "filesystem I/O dispatches"
        );
    }

    fn unexpected<T>(&self) -> T {
        self.dispatches.fetch_add(1, Ordering::SeqCst);
        panic!("unsupported document admission dispatched filesystem I/O");
    }
}

impl novarocks_fs::FileIoRuntime for AdmissionFileIoSpy {
    fn block_on_bytes(
        &self,
        _future: novarocks_fs::FileBytesFuture,
    ) -> novarocks_fs::FileResult<bytes::Bytes> {
        self.unexpected()
    }

    fn block_on_u64(&self, _future: novarocks_fs::FileU64Future) -> novarocks_fs::FileResult<u64> {
        self.unexpected()
    }
}

impl novarocks_fs::FileTaskSpawner for AdmissionFileIoSpy {
    fn spawn(
        &self,
        _task: novarocks_fs::FileTaskFuture,
    ) -> novarocks_fs::FileResult<novarocks_fs::FileTask> {
        self.unexpected()
    }
}
