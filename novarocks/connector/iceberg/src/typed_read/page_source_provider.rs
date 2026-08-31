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

use novarocks_fs::{
    CacheOptions, DataCacheContext, FileReadBudget, FileReadContext, FileReaderOptions,
};
use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{ConnectorPageSource, ConnectorSession, DynamicFilter};

use crate::access_binding::IcebergReadBinding;

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
use super::table_execute::{IcebergTableExecuteHandle, IcebergTableExecuteProcedureHandle};
use super::{IcebergReadSplit, IcebergRuntimeRelation};

/// Reader policy the fragment instance chose, not something a split carries.
#[derive(Clone, Debug)]
pub struct IcebergPageSourceProviderOptions {
    /// The row and byte budget of one produced page.
    pub budget: FileReadBudget,
    pub reader_options: FileReaderOptions,
    /// Query-scoped external cache policy, converted from the neutral SPI
    /// policy at the connector boundary.
    pub cache_options: CacheOptions,
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
            cache_options: disabled_external_cache_options(),
        }
    }
}

pub(crate) fn disabled_external_cache_options() -> CacheOptions {
    CacheOptions {
        enable_scan_datacache: false,
        enable_populate_datacache: false,
        enable_datacache_async_populate_mode: false,
        enable_datacache_io_adaptor: false,
        enable_cache_select: false,
        datacache_evict_probability: 100,
        datacache_priority: 0,
        datacache_ttl_seconds: 0,
        datacache_sharing_work_period: None,
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

impl<P> novarocks_spi::connector::read_stack::adapter::ProviderReadPageSourceProvider<P>
    for IcebergPageSourceProvider
where
    P: novarocks_spi::connector::read_stack::adapter::ProviderReadRuntime<
            Table = IcebergRuntimeRelation,
            Column = IcebergColumnHandle,
            Transaction = super::HiveTransactionHandle,
            Split = IcebergReadSplit,
        >,
{
    fn create_page_source(
        &self,
        _session: &ConnectorSession,
        table: &IcebergRuntimeRelation,
        split: &IcebergReadSplit,
        scheduled_split_sequence_id: u64,
        columns: &[novarocks_spi::connector::read_stack::Assignment<IcebergColumnHandle>],
        dynamic_filter: &Arc<dyn DynamicFilter<IcebergColumnHandle>>,
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
        let columns = columns
            .iter()
            .map(|assignment| assignment.column().clone())
            .collect::<Vec<_>>();
        if let IcebergReadSplit::SystemFiles(files_split) = split {
            return self
                .system_tables
                .create_files_page_source(files_split, &columns);
        }
        if let (
            IcebergRuntimeRelation::ChangeWindow(handle),
            IcebergReadSplit::ChangeWindow(split),
        ) = (table, split)
        {
            return create_iceberg_change_window_page_source(
                IcebergChangeWindowPageSourceRequest {
                    handle,
                    split,
                    columns: &columns,
                    delete_manager: Arc::clone(&self.delete_manager),
                    footers: Arc::clone(&self.footers),
                    access_binding: self.access_binding.clone(),
                    context: self.context.clone(),
                    cache: Some(DataCacheContext::external(
                        self.options.cache_options.clone(),
                    )),
                    budget: self.options.budget,
                    reader_options: self.options.reader_options,
                    scheduled_split_sequence_id,
                    dynamic_filter: Arc::clone(dynamic_filter),
                },
            );
        }
        if let (
            IcebergRuntimeRelation::TableExecute(handle),
            IcebergReadSplit::RewritePositionDeleteFiles(split),
        ) = (table, split)
        {
            expect_rewrite_position_delete_files(handle)?;
            return create_iceberg_rewrite_position_delete_files_page_source(
                IcebergRewritePositionDeleteFilesPageSourceRequest {
                    split,
                    columns: &columns,
                    access_binding: self.access_binding.clone(),
                    context: self.context.clone(),
                    budget: self.options.budget,
                },
            );
        }
        let (IcebergRuntimeRelation::Table(table), IcebergReadSplit::Data(split)) = (table, split)
        else {
            return Err(invalid(
                "iceberg relation and split categories are incompatible",
            ));
        };
        let relation = IcebergReadRelation::of_table(table, split.partition_spec_id())?;
        create_iceberg_page_source(IcebergPageSourceRequest {
            relation: &relation,
            split,
            columns: &columns,
            delete_manager: Arc::clone(&self.delete_manager),
            delete_mode: DeleteEvaluationMode::ExcludeDeleted,
            footers: Arc::clone(&self.footers),
            access_binding: self.access_binding.clone(),
            context: self.context.clone(),
            cache: Some(DataCacheContext::external(
                self.options.cache_options.clone(),
            )),
            budget: self.options.budget,
            reader_options: self.options.reader_options,
            scheduled_split_sequence_id,
            dynamic_filter: Arc::clone(dynamic_filter),
        })
    }
}

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

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };

    use super::*;

    #[test]
    fn the_provider_shares_one_footer_cache_and_one_delete_manager() {
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
        let provider = IcebergPageSourceProvider::new(
            binding,
            context,
            IcebergPageSourceProviderOptions {
                budget: FileReadBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero"),
                    max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero"),
                },
                reader_options: FileReaderOptions::default(),
                cache_options: disabled_external_cache_options(),
            },
        );

        assert!(provider.footers().is_empty().expect("footer cache"));
        assert_eq!(
            provider.delete_manager().loaded_artifacts().expect("state"),
            0
        );
        assert!(Arc::ptr_eq(provider.footers(), provider.footers()));
        assert!(Arc::ptr_eq(
            provider.delete_manager(),
            provider.delete_manager()
        ));
    }
}
