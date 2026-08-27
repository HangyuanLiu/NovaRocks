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

//! The worker-side entry point of the typed Iceberg read stack.
//!
//! One provider serves one BE fragment instance and scan node. It owns the two
//! things every split of that scan should share -- the Parquet footer cache and
//! the delete manager -- and nothing else: each split still gets its own page
//! source with its own cursor, reader, and close latch.
//!
//! The provider is also the only place a protocol-validated carrier becomes a
//! concrete Iceberg type. Every closed `oneof` is matched exhaustively, so a
//! non-Iceberg or wrong-category variant is a typed rejection rather than a
//! downcast that could not have been checked.

use std::sync::Arc;

use novarocks_fs::{FileReadBudget, FileReadContext, FileReaderOptions};
use novarocks_proto_codec::connector_read::{
    CatalogTableHandle, ConnectorRelation, ScanAssignment, SplitCategory,
    TypedConnectorPageSourceProvider, ValidatedConnectorSplit, WireDynamicFilter,
};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{ConnectorPageSource, ConnectorSession};

use crate::access_binding::IcebergReadBinding;

use super::change_window::{IcebergChangeSplit, IcebergChangeWindowHandle};
use super::change_window_page_source::{
    IcebergChangeWindowPageSourceRequest, create_iceberg_change_window_page_source,
};
use super::column_handle::{IcebergColumnHandle, invalid};
use super::delete_manager::{DeleteEvaluationMode, DeleteManager};
use super::page_source::{
    IcebergPageSourceRequest, IcebergReadRelation, ParquetFooterCache, create_iceberg_page_source,
};
use super::rewrite_position_page_source::{
    IcebergRewritePositionDeleteFilesPageSourceRequest,
    create_iceberg_rewrite_position_delete_files_page_source,
};
use super::split::IcebergSplit;
use super::table_execute::{
    IcebergRewritePositionDeleteFilesSplit, IcebergTableExecuteHandle,
    IcebergTableExecuteProcedureHandle,
};
use super::table_handle::IcebergTableHandle;

/// Reader policy the fragment instance chose, not something a split carries.
#[derive(Clone, Copy, Debug)]
pub struct IcebergPageSourceProviderOptions {
    /// The row and byte budget of one produced page.
    pub budget: FileReadBudget,
    pub reader_options: FileReaderOptions,
}

impl IcebergPageSourceProviderOptions {
    /// The budget a fragment instance uses when it states no preference. The
    /// values match the other native reader budgets rather than introducing a
    /// third number for the same idea.
    pub fn with_default_budget() -> Self {
        Self {
            budget: FileReadBudget {
                max_rows: std::num::NonZeroUsize::new(4096).expect("nonzero"),
                max_bytes: std::num::NonZeroUsize::new(8 * 1024 * 1024).expect("nonzero"),
            },
            reader_options: FileReaderOptions::default(),
        }
    }
}

/// One BE fragment instance and scan node's Iceberg reader factory.
pub struct IcebergPageSourceProvider {
    access_binding: IcebergReadBinding,
    context: FileReadContext,
    options: IcebergPageSourceProviderOptions,
    footers: Arc<ParquetFooterCache>,
    delete_manager: Arc<DeleteManager>,
    /// The `$files` relation is distributed, so its reader shares this
    /// provider's lifetime with the data reader.
    system_tables: Arc<super::system_page_source::IcebergSystemTableProvider>,
}

impl IcebergPageSourceProvider {
    pub fn new(
        access_binding: IcebergReadBinding,
        context: FileReadContext,
        options: IcebergPageSourceProviderOptions,
    ) -> Self {
        let delete_manager = Arc::new(DeleteManager::new(access_binding.clone(), context.clone()));
        let system_tables = Arc::new(super::system_page_source::IcebergSystemTableProvider::new(
            access_binding.clone(),
            context.clone(),
            options.budget.max_rows,
        ));
        Self {
            access_binding,
            context,
            options,
            footers: Arc::new(ParquetFooterCache::new()),
            delete_manager,
            system_tables,
        }
    }

    /// The footer cache shared by the splits of this scan.
    pub fn footers(&self) -> &Arc<ParquetFooterCache> {
        &self.footers
    }

    /// The delete manager shared by the splits of this scan.
    pub fn delete_manager(&self) -> &Arc<DeleteManager> {
        &self.delete_manager
    }
}

impl TypedConnectorPageSourceProvider for IcebergPageSourceProvider {
    fn create_page_source(
        &self,
        _session: &ConnectorSession,
        table: &CatalogTableHandle,
        split: &ValidatedConnectorSplit,
        scheduled_split_sequence_id: u64,
        columns: &[ScanAssignment],
        dynamic_filter: &Arc<WireDynamicFilter>,
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
        let columns = iceberg_scan_columns(columns)?;

        // `$files` is the one system relation that is distributed, so its
        // split arrives on the same data path. Everything else about it is
        // different: it reads manifests, never a data file, and needs no
        // delete manager or dynamic filter.
        if split.category() == SplitCategory::SystemFiles {
            let files_split = iceberg_files_table_split(split)?;
            return self
                .system_tables
                .create_files_page_source(&files_split, &columns);
        }

        // A change-window split reads data files, but not with the data
        // relation's semantics: its sign comes from the split variant and its
        // reverse side selects the rows a delete removed instead of hiding
        // them. Both carriers are decoded here so a mismatched pairing fails
        // as a mismatch rather than as a wrong read.
        if split.category() == SplitCategory::ChangeWindow {
            let handle = iceberg_change_window_handle(table)?;
            let change_split = iceberg_change_window_split(split)?;
            return create_iceberg_change_window_page_source(
                IcebergChangeWindowPageSourceRequest {
                    handle: &handle,
                    split: &change_split,
                    columns: &columns,
                    delete_manager: Arc::clone(&self.delete_manager),
                    footers: Arc::clone(&self.footers),
                    access_binding: self.access_binding.clone(),
                    context: self.context.clone(),
                    budget: self.options.budget,
                    reader_options: self.options.reader_options,
                    scheduled_split_sequence_id,
                    dynamic_filter: Arc::clone(dynamic_filter),
                },
            );
        }

        // A rewrite-position split opens no data file at all: it reads the
        // delete artifacts its frozen group named and re-encodes the positions
        // they hold. The data relation's reader has nothing to contribute to
        // that, so both carriers are decoded here as their own pair.
        if split.category() == SplitCategory::RewritePositionDeleteFiles {
            let handle = iceberg_table_execute_handle(table)?;
            let rewrite_split = iceberg_rewrite_position_delete_files_split(split)?;
            expect_rewrite_position_delete_files(&handle)?;
            return create_iceberg_rewrite_position_delete_files_page_source(
                IcebergRewritePositionDeleteFilesPageSourceRequest {
                    split: &rewrite_split,
                    columns: &columns,
                    access_binding: self.access_binding.clone(),
                    context: self.context.clone(),
                    budget: self.options.budget,
                },
            );
        }

        let table_handle = iceberg_table_handle(table)?;
        let split = iceberg_data_split(split)?;
        let relation = IcebergReadRelation::of_table(&table_handle, split.partition_spec_id())?;

        create_iceberg_page_source(IcebergPageSourceRequest {
            relation: &relation,
            split: &split,
            columns: &columns,
            delete_manager: Arc::clone(&self.delete_manager),
            delete_mode: DeleteEvaluationMode::ExcludeDeleted,
            footers: Arc::clone(&self.footers),
            access_binding: self.access_binding.clone(),
            context: self.context.clone(),
            budget: self.options.budget,
            reader_options: self.options.reader_options,
            scheduled_split_sequence_id,
            dynamic_filter: Arc::clone(dynamic_filter),
        })
    }
}

/// Turn a protocol-validated relation into the Iceberg DATA table handle.
///
/// System relations, table functions, change windows, table execute, and merge
/// all have their own entry points. Accepting one here would let a worker read
/// a relation whose semantics this page source does not implement.
pub fn iceberg_table_handle(
    table: &CatalogTableHandle,
) -> Result<IcebergTableHandle, ConnectorError> {
    match table.relation() {
        ConnectorRelation::Table(handle) => IcebergTableHandle::from_table_handle_proto(handle),
        ConnectorRelation::TableFunction(_) => Err(invalid(
            "an iceberg page source reads a table, not a table function",
        )),
        ConnectorRelation::ChangeWindow(_) => Err(invalid(
            "an iceberg page source reads a table, not a change window",
        )),
        ConnectorRelation::SystemTable(_) => Err(invalid(
            "an iceberg page source reads a table, not a system relation",
        )),
        ConnectorRelation::TableExecute(_) => Err(invalid(
            "an iceberg page source reads a table, not a table execute target",
        )),
        ConnectorRelation::MergeTable(_) => Err(invalid(
            "an iceberg page source reads a table, not a merge target",
        )),
    }
}

/// Turn a protocol-validated split into the Iceberg DATA split.
pub fn iceberg_data_split(split: &ValidatedConnectorSplit) -> Result<IcebergSplit, ConnectorError> {
    match split.category() {
        SplitCategory::Data => IcebergSplit::from_connector_split_proto(split.as_proto()),
        SplitCategory::TableChanges => Err(invalid(
            "an iceberg page source reads a data split, not a table-changes split",
        )),
        SplitCategory::ChangeWindow => Err(invalid(
            "an iceberg page source reads a data split, not a change-window split",
        )),
        SplitCategory::SystemFiles => Err(invalid(
            "a system-files split is routed to the system relation reader, not here",
        )),
        SplitCategory::RewritePositionDeleteFiles => Err(invalid(
            "an iceberg page source reads a data split, not a rewrite-position-delete split",
        )),
    }
}

/// Turn a protocol-validated relation into the Iceberg CHANGE WINDOW handle.
pub fn iceberg_change_window_handle(
    table: &CatalogTableHandle,
) -> Result<IcebergChangeWindowHandle, ConnectorError> {
    match table.relation() {
        ConnectorRelation::ChangeWindow(handle) => {
            IcebergChangeWindowHandle::from_change_window_handle_proto(handle)
        }
        ConnectorRelation::Table(_) => Err(invalid(
            "a change-window split names a change window, not a table",
        )),
        ConnectorRelation::TableFunction(_) => Err(invalid(
            "a change-window split names a change window, not a table function",
        )),
        ConnectorRelation::SystemTable(_) => Err(invalid(
            "a change-window split names a change window, not a system relation",
        )),
        ConnectorRelation::TableExecute(_) => Err(invalid(
            "a change-window split names a change window, not a table execute target",
        )),
        ConnectorRelation::MergeTable(_) => Err(invalid(
            "a change-window split names a change window, not a merge target",
        )),
    }
}

/// Turn a protocol-validated split into the Iceberg CHANGE WINDOW split.
pub fn iceberg_change_window_split(
    split: &ValidatedConnectorSplit,
) -> Result<IcebergChangeSplit, ConnectorError> {
    match split.category() {
        SplitCategory::ChangeWindow => {
            IcebergChangeSplit::from_connector_split_proto(split.as_proto())
        }
        SplitCategory::Data => Err(invalid(
            "the iceberg change-window reader reads a change-window split, not a data split",
        )),
        SplitCategory::TableChanges => Err(invalid(
            "the iceberg change-window reader reads a change-window split, not a table-changes split",
        )),
        SplitCategory::SystemFiles => Err(invalid(
            "the iceberg change-window reader reads a change-window split, not a system-files split",
        )),
        SplitCategory::RewritePositionDeleteFiles => Err(invalid(
            "the iceberg change-window reader reads a change-window split, not a rewrite-position-delete split",
        )),
    }
}

/// Turn a protocol-validated relation into the Iceberg TABLE EXECUTE handle.
pub fn iceberg_table_execute_handle(
    table: &CatalogTableHandle,
) -> Result<IcebergTableExecuteHandle, ConnectorError> {
    match table.relation() {
        ConnectorRelation::TableExecute(handle) => {
            IcebergTableExecuteHandle::from_table_execute_handle_proto(handle)
        }
        ConnectorRelation::Table(_) => Err(invalid(
            "a rewrite-position split names a table execute target, not a table",
        )),
        ConnectorRelation::TableFunction(_) => Err(invalid(
            "a rewrite-position split names a table execute target, not a table function",
        )),
        ConnectorRelation::ChangeWindow(_) => Err(invalid(
            "a rewrite-position split names a table execute target, not a change window",
        )),
        ConnectorRelation::SystemTable(_) => Err(invalid(
            "a rewrite-position split names a table execute target, not a system relation",
        )),
        ConnectorRelation::MergeTable(_) => Err(invalid(
            "a rewrite-position split names a table execute target, not a merge target",
        )),
    }
}

/// Prove the table-execute relation a rewrite-position split arrived with is
/// the procedure that reads delete artifacts.
///
/// `OPTIMIZE` is the other procedure with a worker-visible handle, and it reads
/// ordinary data splits; pairing it with this split would run a delete-artifact
/// reader for a procedure whose commit replaces data files.
fn expect_rewrite_position_delete_files(
    handle: &IcebergTableExecuteHandle,
) -> Result<(), ConnectorError> {
    match handle.procedure_handle() {
        Some(IcebergTableExecuteProcedureHandle::RewritePositionDeleteFiles(_)) => Ok(()),
        Some(IcebergTableExecuteProcedureHandle::Optimize(_)) | None => Err(invalid(
            "a rewrite-position split names a table execute target that does not rewrite position deletes",
        )),
    }
}

/// Turn a protocol-validated split into the Iceberg REWRITE POSITION DELETE
/// split.
pub fn iceberg_rewrite_position_delete_files_split(
    split: &ValidatedConnectorSplit,
) -> Result<IcebergRewritePositionDeleteFilesSplit, ConnectorError> {
    match split.category() {
        SplitCategory::RewritePositionDeleteFiles => {
            IcebergRewritePositionDeleteFilesSplit::from_connector_split_proto(split.as_proto())
        }
        SplitCategory::Data => Err(invalid(
            "the iceberg rewrite-position reader reads a rewrite-position-delete split, not a data split",
        )),
        SplitCategory::TableChanges => Err(invalid(
            "the iceberg rewrite-position reader reads a rewrite-position-delete split, not a table-changes split",
        )),
        SplitCategory::ChangeWindow => Err(invalid(
            "the iceberg rewrite-position reader reads a rewrite-position-delete split, not a change-window split",
        )),
        SplitCategory::SystemFiles => Err(invalid(
            "the iceberg rewrite-position reader reads a rewrite-position-delete split, not a system-files split",
        )),
    }
}

/// Turn a protocol-validated split into the `$files` manifest split.
pub fn iceberg_files_table_split(
    split: &ValidatedConnectorSplit,
) -> Result<super::system_table::FilesTableSplit, ConnectorError> {
    match split.category() {
        SplitCategory::SystemFiles => {
            let raw = match split.as_proto().category.as_ref() {
                Some(novarocks_proto_models::connector_read::connector_split::Category::SystemFiles(
                    category,
                )) => match category.provider.as_ref() {
                    Some(
                        novarocks_proto_models::connector_read::system_files_split_category::Provider::Iceberg(
                            files,
                        ),
                    ) => files,
                    None => {
                        return Err(invalid("a system-files split carries no provider variant"));
                    }
                },
                _ => return Err(invalid("split category disagrees with its validated category")),
            };
            super::system_table::FilesTableSplit::from_proto(raw)
        }
        SplitCategory::Data
        | SplitCategory::TableChanges
        | SplitCategory::ChangeWindow
        | SplitCategory::RewritePositionDeleteFiles => Err(invalid(
            "the system relation reader reads a system-files split, nothing else",
        )),
    }
}

/// The scan's ordered output columns, in assignment order.
///
/// Order is the contract: the engine binds page channel `i` to assignment `i`,
/// so this conversion must never sort, dedupe, or reorder.
pub fn iceberg_scan_columns(
    columns: &[ScanAssignment],
) -> Result<Vec<IcebergColumnHandle>, ConnectorError> {
    columns
        .iter()
        .map(|assignment| {
            IcebergColumnHandle::from_column_handle_proto(assignment.column().as_proto())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use std::collections::BTreeSet;

    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::connector_read as dto;
    use novarocks_spi::connector::ConnectorErrorKind;
    use novarocks_spi::connector::read_stack::{
        DynamicFilter, SchemaTableName, SplitWeight, TupleDomain,
    };

    use super::super::change_window::{IcebergAddedRows, IcebergChangeWindowHandleParams};
    use super::super::split::{IcebergFileFormat, IcebergSplitParams};
    use super::super::table_handle::tests::partitioned_schema;
    use super::*;

    /// The unconstrained filter every scan starts from. It answers nothing and
    /// is never waited on, so it cannot affect what a dispatch decides.
    struct UnconstrainedDynamicFilter {
        covered: BTreeSet<novarocks_proto_codec::connector_read::ValidatedColumnHandle>,
    }

    impl DynamicFilter<novarocks_proto_codec::connector_read::ValidatedColumnHandle>
        for UnconstrainedDynamicFilter
    {
        fn columns_covered(
            &self,
        ) -> &BTreeSet<novarocks_proto_codec::connector_read::ValidatedColumnHandle> {
            &self.covered
        }

        fn current_predicate(
            &self,
        ) -> TupleDomain<novarocks_proto_codec::connector_read::ValidatedColumnHandle> {
            TupleDomain::all()
        }

        fn is_complete(&self) -> bool {
            true
        }

        fn is_awaitable(&self) -> bool {
            false
        }
    }

    fn unconstrained_filter() -> Arc<WireDynamicFilter> {
        Arc::new(UnconstrainedDynamicFilter {
            covered: BTreeSet::new(),
        })
    }

    fn change_window_relation() -> CatalogTableHandle {
        let schema = partitioned_schema();
        let handle = IcebergChangeWindowHandle::try_new(IcebergChangeWindowHandleParams {
            schema_table_name: SchemaTableName::try_new("sales", "orders").expect("name"),
            table_schema_json: serde_json::to_string(&schema).expect("schema json"),
            columns: vec![
                IcebergColumnHandle::base_column_of(&schema, 1).expect("id"),
                IcebergColumnHandle::base_column_of(&schema, 3).expect("amount"),
            ],
            name_mapping_json: None,
            from_snapshot_id_exclusive: 11,
            to_snapshot_id_inclusive: 12,
            partition_spec_jsons: std::collections::BTreeMap::new(),
        })
        .expect("change window handle");
        let raw = dto::CatalogTableHandle {
            catalog_name: "lake".to_owned(),
            instance_incarnation: vec![7_u8; 16],
            transaction: Some(dto::ConnectorTransactionHandle {
                handle: Some(dto::connector_transaction_handle::Handle::Iceberg(
                    dto::HiveTransactionHandle {
                        auto_commit: true,
                        uuid: vec![1_u8; 16],
                    },
                )),
            }),
            relation: Some(dto::catalog_table_handle::Relation::ChangeWindow(
                handle.to_change_window_handle_proto(),
            )),
        };
        CatalogTableHandle::parse(raw, FieldPath::root("catalog_table_handle"))
            .expect("valid change window relation")
    }

    fn change_window_split() -> ValidatedConnectorSplit {
        let data = IcebergSplit::try_new(IcebergSplitParams {
            path: "s3://lake/orders/added.parquet".to_owned(),
            start: 0,
            length: 100,
            file_size: 100,
            file_record_count: 10,
            file_format: IcebergFileFormat::Parquet,
            partition_spec_id: 7,
            partition_data_json: "{}".to_owned(),
            deletes: Vec::new(),
            file_statistics_domain: TupleDomain::all(),
            data_sequence_number: Some(4),
            file_first_row_id: None,
            decryption_data: None,
            split_weight: SplitWeight::STANDARD,
            affinity_key: None,
        })
        .expect("data split");
        let split = IcebergChangeSplit::AddedRows(
            IcebergAddedRows::try_new(data, Vec::new()).expect("added rows"),
        );
        ValidatedConnectorSplit::parse(
            split.to_connector_split_proto(),
            FieldPath::root("connector_split"),
        )
        .expect("valid change window split")
    }

    fn session() -> ConnectorSession {
        ConnectorSession::try_new("q1", "u", "UTC", "en_US", std::time::SystemTime::UNIX_EPOCH)
            .expect("session")
    }

    fn provider_options() -> IcebergPageSourceProviderOptions {
        IcebergPageSourceProviderOptions {
            budget: FileReadBudget {
                max_rows: NonZeroUsize::new(1024).expect("nonzero"),
                max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero"),
            },
            reader_options: FileReaderOptions::default(),
        }
    }

    fn provider() -> (tokio::runtime::Runtime, IcebergPageSourceProvider) {
        let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        let binding = IcebergReadBinding::new(
            None,
            FsAccessResolver::new(),
            Arc::clone(&file_runtime),
            Arc::clone(&task_spawner),
        );
        let context = FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(60)),
            runtime: file_runtime,
            task_spawner,
        };
        let provider = IcebergPageSourceProvider::new(binding, context, provider_options());
        (runtime, provider)
    }

    fn system_table_relation() -> CatalogTableHandle {
        let raw = dto::CatalogTableHandle {
            catalog_name: "lake".to_owned(),
            instance_incarnation: vec![7_u8; 16],
            transaction: Some(dto::ConnectorTransactionHandle {
                handle: Some(dto::connector_transaction_handle::Handle::Iceberg(
                    dto::HiveTransactionHandle {
                        auto_commit: true,
                        uuid: vec![1_u8; 16],
                    },
                )),
            }),
            relation: Some(dto::catalog_table_handle::Relation::SystemTable(
                dto::ConnectorSystemTableReference {
                    reference: Some(dto::connector_system_table_reference::Reference::Iceberg(
                        dto::IcebergSystemTableReference {
                            schema_table_name: Some(dto::SchemaTableName {
                                schema_name: "sales".to_owned(),
                                table_name: "orders".to_owned(),
                            }),
                            system_table_type: dto::IcebergSystemTableType::Files as i32,
                            metadata_file_location: "/tmp/orders/metadata/v1.json".to_owned(),
                            table_uuid: "1b4e28ba-2fa1-11d2-883f-0016d3cca427".to_owned(),
                            snapshot_id: Some(3),
                        },
                    )),
                },
            )),
        };
        CatalogTableHandle::parse(raw, FieldPath::root("catalog_table_handle"))
            .expect("valid system relation")
    }

    #[test]
    fn a_non_data_relation_is_a_typed_error_rather_than_a_downcast() {
        let (_runtime, _provider) = provider();
        let error = iceberg_table_handle(&system_table_relation())
            .expect_err("a system relation has its own entry point");
        assert_eq!(
            error.kind(),
            novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn a_change_window_split_whose_partition_spec_the_handle_lacks_is_rejected_not_guessed() {
        let (_runtime, provider) = provider();
        let outcome = provider.create_page_source(
            &session(),
            &change_window_relation(),
            &change_window_split(),
            0,
            &[],
            &unconstrained_filter(),
        );
        let Err(error) = outcome else {
            panic!("a split whose partition spec the window does not carry cannot be read");
        };

        // A window spans two snapshots, so files written under different specs
        // appear on both sides of the difference and every spec travels on the
        // handle. Inventing the missing one would be a guess about how the
        // relation is partitioned.
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        assert!(error.to_string().contains("partition spec id 7"));
        assert!(error.to_string().contains("change window"));
        // It reached the change-window branch rather than the data reader's
        // generic split rejection.
        assert!(!error.to_string().contains("not a change-window split"));
    }

    #[test]
    fn a_change_window_carrier_never_answers_a_data_relations_decoder_or_the_reverse() {
        // The two lanes are separate questions, so neither carrier decodes as
        // the other one. Accepting either pairing would read a relation whose
        // semantics the chosen reader does not implement.
        assert!(iceberg_table_handle(&change_window_relation()).is_err());
        assert!(iceberg_data_split(&change_window_split()).is_err());
        assert!(iceberg_change_window_handle(&system_table_relation()).is_err());

        let window = iceberg_change_window_handle(&change_window_relation()).expect("window");
        assert_eq!(window.from_snapshot_id_exclusive(), 11);
        assert_eq!(window.to_snapshot_id_inclusive(), 12);
        let split = iceberg_change_window_split(&change_window_split()).expect("split");
        // The sign comes from the variant, never from a field on the wire.
        assert_eq!(split.change_op(), 1);
    }

    #[test]
    fn the_provider_shares_one_footer_cache_and_one_delete_manager() {
        let (_runtime, provider) = provider();
        assert!(provider.footers().is_empty().expect("footer cache"));
        assert_eq!(
            provider.delete_manager().loaded_artifacts().expect("state"),
            0
        );
        // Two reads of the shared handles observe the same instance, which is
        // what lets sibling splits of one scan reuse a footer.
        assert!(Arc::ptr_eq(provider.footers(), provider.footers()));
        assert!(Arc::ptr_eq(
            provider.delete_manager(),
            provider.delete_manager()
        ));
    }
}
