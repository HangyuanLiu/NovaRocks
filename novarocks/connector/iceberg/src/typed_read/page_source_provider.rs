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
use novarocks_proto::connector_read::{
    CatalogTableHandle, ConnectorRelation, ScanAssignment, SplitCategory,
    TypedConnectorPageSourceProvider, ValidatedConnectorSplit, WireDynamicFilter,
};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{ConnectorPageSource, ConnectorSession};

use crate::access_binding::IcebergReadBinding;

use super::column_handle::{IcebergColumnHandle, invalid};
use super::delete_manager::DeleteManager;
use super::page_source::{
    IcebergPageSourceRequest, ParquetFooterCache, create_iceberg_page_source,
};
use super::split::IcebergSplit;
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

        let table_handle = iceberg_table_handle(table)?;
        let split = iceberg_data_split(split)?;

        create_iceberg_page_source(IcebergPageSourceRequest {
            table_handle: &table_handle,
            split: &split,
            columns: &columns,
            delete_manager: Arc::clone(&self.delete_manager),
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

    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use novarocks_proto::FieldPath;
    use novarocks_proto_models::connector_read as dto;

    use super::*;

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
