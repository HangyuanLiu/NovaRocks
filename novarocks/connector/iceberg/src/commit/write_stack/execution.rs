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

//! Backend-local Iceberg writers for the provider-neutral write stack.
//!
//! One pipeline driver owns one writer for its whole lifetime. A writer stages
//! artifacts and describes each one as an independent commit fragment; it has
//! no catalog client and no commit capability at all.
//!
//! The deletion-vector and position-delete writers are where the NCP-6 D10
//! inversion lands: they read the frozen old-delete references through this
//! request's storage resolver, validate each artifact against the data file it
//! claims, merge, and write. A read that fails for any reason fails the writer
//! and therefore the query — it never degrades into an empty old delete set.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use novarocks_spi::connector::write_stack::{
    ConnectorBatchWriter, ConnectorCommitFragment, ConnectorOpenWriterRequest,
    ConnectorWriteExecution, ConnectorWriteExecutionFactory, ConnectorWriterPhysicalContext,
};
use novarocks_spi::connector::{
    CatalogHandle, CatalogProperties, ConnectorError, ConnectorErrorKind,
    ConnectorExecutionResources, ConnectorInstanceDescriptor, ConnectorRequestContext,
    ConnectorResourceClass, ConnectorResourceReservation,
};

use crate::access_binding::IcebergReadBinding;
use crate::commit::data_writer::{
    StagedDataFile, StagedWriteContext, cleanup_staged_files, staged_data_file_to_writer_report,
    write_record_batches,
};
use crate::commit::frozen_write::{FrozenDataWriteFacts, staged_write_context_from_frozen_facts};
use crate::commit::report::{IcebergPartitionReport, partition_path_from_struct};
use crate::commit::write_io::build_staged_file_io;
use crate::commit::write_stack::domain::{
    IcebergArtifactMetrics, IcebergArtifactPartition, IcebergCommitFragment, IcebergContentRange,
    IcebergDataFileArtifact, IcebergDeletionVectorArtifact, IcebergEqualityDeleteFileArtifact,
    IcebergPositionDeleteFileArtifact, IcebergWriteBranch, IcebergWriterHandle,
};
use crate::commit::write_stack::old_delete::read_and_merge_old_deletes;
use crate::commit::write_stack::runtime::IcebergWriteAdapter;
use crate::commit::{DeletionVector, write_single_deletion_vector_puffin};
use crate::commit::{PositionDeleteGroup, write_position_delete_files};
use crate::delete_file::IcebergFileFormat;
use crate::write_descriptor::encode_partition_descriptor;

fn error(kind: ConnectorErrorKind, message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(kind, message.into())
}

/// The backend write capability of one exact catalog generation.
pub struct IcebergWriteStackExecution {
    catalog_handle: CatalogHandle,
    adapter: IcebergWriteAdapter,
    binding: IcebergReadBinding,
}

impl IcebergWriteStackExecution {
    pub(crate) fn new(
        catalog_handle: CatalogHandle,
        adapter: IcebergWriteAdapter,
        binding: IcebergReadBinding,
    ) -> Self {
        Self {
            catalog_handle,
            adapter,
            binding,
        }
    }
}

#[async_trait::async_trait]
impl ConnectorWriteExecution for IcebergWriteStackExecution {
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    async fn open_writer(
        &self,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Box<dyn ConnectorBatchWriter>, ConnectorError> {
        // The adapter is the only door back to the provider domain, and it is
        // bound to this exact generation, so a handle minted by any other
        // generation cannot open a writer here.
        let handle = self.adapter.writer_handle(&request.handle)?.clone();
        if handle.table().default_partition_spec_id() < 0 {
            return Err(error(
                ConnectorErrorKind::CorruptData,
                "Iceberg writer handle carries a negative partition spec id",
            ));
        }
        match handle.branch() {
            IcebergWriteBranch::Data => Ok(Box::new(IcebergDataStackWriter::open(
                self, handle, request,
            )?)),
            IcebergWriteBranch::PositionDelete | IcebergWriteBranch::DeletionVector => Ok(
                Box::new(IcebergDeleteStackWriter::open(self, handle, request)?),
            ),
            IcebergWriteBranch::EqualityDelete => Ok(Box::new(
                IcebergEqualityDeleteStackWriter::open(self, handle, request)?,
            )),
        }
    }
}

/// Startup-sealed factory for the catalog-keyed backend write capability.
///
/// The adapter is minted here, from the exact catalog generation the properties
/// name, so a backend can never open a writer for a handle produced by another
/// generation.
#[derive(Clone)]
pub struct IcebergWriteStackExecutionFactory {
    descriptor: ConnectorInstanceDescriptor,
    binding: IcebergReadBinding,
}

impl IcebergWriteStackExecutionFactory {
    pub fn new(descriptor: ConnectorInstanceDescriptor, binding: IcebergReadBinding) -> Self {
        Self {
            descriptor,
            binding,
        }
    }
}

impl ConnectorWriteExecutionFactory for IcebergWriteStackExecutionFactory {
    fn build(
        &self,
        properties: &CatalogProperties,
    ) -> Result<Arc<dyn ConnectorWriteExecution>, ConnectorError> {
        let catalog_handle = properties.handle().clone();
        if self.descriptor.instance_id != *catalog_handle.catalog_name() {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg write execution factory does not own this catalog",
            ));
        }
        let binding = self.binding.bind_catalog(properties)?;
        let adapter = crate::commit::write_stack::runtime::build_write_adapter(
            self.descriptor.clone(),
            catalog_handle.clone(),
        );
        Ok(Arc::new(IcebergWriteStackExecution::new(
            catalog_handle,
            adapter,
            binding,
        )))
    }
}

/// Attempt-local output naming.
///
/// The physical context is used for naming, logging, and failure localization
/// only. It never enters a commit fragment's identity, so replaying an attempt
/// under a new physical context cannot change what was or was not committed.
fn staged_path(
    handle: &IcebergWriterHandle,
    physical: &ConnectorWriterPhysicalContext,
    partition_path: &str,
    sequence: u64,
    extension: &str,
) -> String {
    let base = format!(
        "{}/_staging/{}",
        handle.table().data_location().trim_end_matches('/'),
        uuid::Uuid::from_bytes(physical.execution_query_id())
    );
    let name = format!(
        "{}-{}-{:08x}-{:08x}-{:016x}.{extension}",
        handle.branch().as_str(),
        uuid::Uuid::from_bytes(physical.fragment_instance_id()),
        physical.driver_id(),
        physical.writer_ordinal(),
        sequence
    );
    let partition = partition_path.trim_matches('/');
    if partition.is_empty() {
        format!("{base}/{name}")
    } else {
        format!("{base}/{partition}/{name}")
    }
}

fn ensure_live(
    terminal: bool,
    context: &ConnectorRequestContext,
    what: &str,
) -> Result<(), ConnectorError> {
    if terminal {
        return Err(error(
            ConnectorErrorKind::InvalidRequest,
            format!("Iceberg {what} writer is already terminal"),
        ));
    }
    if context.is_cancelled() {
        return Err(error(
            ConnectorErrorKind::Cancelled,
            format!("Iceberg {what} writer was cancelled"),
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(error(
            ConnectorErrorKind::DeadlineExceeded,
            format!("Iceberg {what} writer deadline elapsed"),
        ));
    }
    Ok(())
}

/// The per-driver data-file writer.
struct IcebergDataStackWriter {
    adapter: IcebergWriteAdapter,
    context: StagedWriteContext,
    request_context: ConnectorRequestContext,
    fragments: Vec<ConnectorCommitFragment>,
    staged_paths: Vec<String>,
    terminal: bool,
}

impl IcebergDataStackWriter {
    fn open(
        execution: &IcebergWriteStackExecution,
        handle: IcebergWriterHandle,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Self, ConnectorError> {
        let recipe = handle.data().ok_or_else(|| {
            error(
                ConnectorErrorKind::CorruptData,
                "Iceberg data writer handle carries no data branch recipe",
            )
        })?;
        let input_schema = recipe.input_schema().cloned().ok_or_else(|| {
            error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg data writer handle carries no frozen input schema",
            )
        })?;
        let binding = execution.binding.for_request(request.context.clone());
        let facts = FrozenDataWriteFacts {
            table_location: handle.table().table_location().to_string(),
            data_location: handle.table().data_location().to_string(),
            target_partition_spec_id: handle.table().default_partition_spec_id(),
            partition_source_column_names: recipe.partition_source_column_names().to_vec(),
            partition_column_names: recipe.partition_column_names().to_vec(),
            transform_exprs: recipe.transform_exprs().to_vec(),
            data_input_schema: input_schema,
            parquet_row_group_size_bytes: handle.output().parquet_row_group_size_bytes(),
        };
        let context =
            staged_write_context_from_frozen_facts(&binding, &request.expected_schema, facts)
                .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        Ok(Self {
            adapter: execution.adapter.clone(),
            context,
            request_context: request.context,
            fragments: Vec::new(),
            staged_paths: Vec::new(),
            terminal: false,
        })
    }

    fn record(&mut self, staged: Vec<StagedDataFile>) -> Result<(), ConnectorError> {
        for file in staged {
            let metadata = file.metadata.clone();
            let partition_spec = metadata
                .partition_spec_by_id(file.partition_spec_id)
                .ok_or_else(|| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        format!(
                            "Iceberg staged data file references unknown partition spec {}",
                            file.partition_spec_id
                        ),
                    )
                })?;
            let (partition_path, null_fingerprint) =
                partition_path_from_struct(file.data_file.partition(), partition_spec)
                    .map_err(|message| error(ConnectorErrorKind::CorruptData, message))?;
            let descriptor = encode_partition_descriptor(
                file.data_file.partition(),
                file.partition_spec_id,
                metadata.as_ref(),
            )
            .map_err(|descriptor_error| {
                error(
                    ConnectorErrorKind::CorruptData,
                    descriptor_error.detail_message(),
                )
            })?;
            let partition = IcebergArtifactPartition::try_new(
                partition_path.clone(),
                null_fingerprint.clone(),
                file.partition_spec_id,
                descriptor,
            )?;
            let report = staged_data_file_to_writer_report(
                &file,
                IcebergPartitionReport {
                    partition_path,
                    null_fingerprint,
                    partition_spec_id: file.partition_spec_id,
                    partition_values: file.data_file.partition().clone(),
                },
                "parquet".to_string(),
                crate::delete_file::IcebergFileContent::Data,
            )
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
            let metrics = IcebergArtifactMetrics::try_new(
                u64::try_from(report.file.record_count).map_err(|_| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg staged data file record count is negative",
                    )
                })?,
                u64::try_from(report.file.file_size_in_bytes).map_err(|_| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg staged data file size is negative",
                    )
                })?,
                report.file.split_offsets.clone().unwrap_or_default(),
                report.file.column_stats.clone(),
            )?;
            self.staged_paths.push(report.file.path.clone());
            let artifact = IcebergDataFileArtifact::try_new(
                report.file.path.clone(),
                IcebergFileFormat::Parquet,
                partition,
                metrics,
                report.file.first_row_id,
            )?;
            self.fragments.push(
                self.adapter
                    .wrap_commit_fragment(IcebergCommitFragment::data_file(artifact)),
            );
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for IcebergDataStackWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        ensure_live(self.terminal, &self.request_context, "data")?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let staged = write_record_batches(&self.context, [batch])
            .await
            .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
        self.record(staged)
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        ensure_live(self.terminal, &self.request_context, "data")?;
        self.terminal = true;
        // A writer that staged nothing returns an empty vector; that is a
        // legal outcome and is not a failure.
        Ok(std::mem::take(&mut self.fragments))
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        if !self.staged_paths.is_empty() {
            let paths = std::mem::take(&mut self.staged_paths);
            cleanup_staged_files(&self.context, &paths)
                .await
                .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
        }
        self.fragments.clear();
        self.terminal = true;
        Ok(())
    }
}

/// The per-driver delete writer, shared by the position-delete and
/// deletion-vector branches.
///
/// Both branches accept the same `(_file, _pos)` input, own the same frozen
/// old-delete references, and differ only in the artifact they stage.
struct IcebergDeleteStackWriter {
    adapter: IcebergWriteAdapter,
    binding: IcebergReadBinding,
    handle: IcebergWriterHandle,
    physical: ConnectorWriterPhysicalContext,
    request_context: ConnectorRequestContext,
    file_io: crate::iceberg::io::FileIO,
    pending: BTreeMap<String, roaring::RoaringTreemap>,
    resources: ConnectorExecutionResources,
    pending_reservation: ConnectorResourceReservation,
    staged_paths: Vec<String>,
    next_sequence: u64,
    terminal: bool,
}

impl IcebergDeleteStackWriter {
    fn open(
        execution: &IcebergWriteStackExecution,
        handle: IcebergWriterHandle,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Self, ConnectorError> {
        let binding = execution.binding.for_request(request.context.clone());
        let file_io = build_staged_file_io(&binding, handle.table().data_location())
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        let pending_reservation = request
            .resources
            .try_reserve(ConnectorResourceClass::WriterState, 256)?;
        Ok(Self {
            adapter: execution.adapter.clone(),
            binding: execution.binding.clone(),
            handle,
            physical: request.physical,
            request_context: request.context,
            file_io,
            pending: BTreeMap::new(),
            resources: request.resources,
            pending_reservation,
            staged_paths: Vec::new(),
            next_sequence: 0,
            terminal: false,
        })
    }

    fn branch_name(&self) -> &'static str {
        self.handle.branch().as_str()
    }

    async fn stage_position_delete(
        &mut self,
        data_file: &str,
        positions: &roaring::RoaringTreemap,
        merged: Vec<String>,
    ) -> Result<IcebergCommitFragment, ConnectorError> {
        let target = self.handle.old_deletes().get(data_file).ok_or_else(|| {
            error(
                ConnectorErrorKind::CorruptData,
                "frozen Iceberg delete merge target disappeared",
            )
        })?;
        let partition = target.partition().clone();
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        let staging_dir = staged_path(
            &self.handle,
            &self.physical,
            partition.partition_path(),
            sequence,
            "d",
        );
        let staging_dir = staging_dir
            .rsplit_once('/')
            .map_or(staging_dir.clone(), |(parent, _)| parent.to_string());
        let ordered = positions
            .iter()
            .map(|position| {
                i64::try_from(position).map_err(|_| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        "Iceberg position-delete position overflows i64",
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let written = write_position_delete_files(
            &self.file_io,
            &staging_dir,
            vec![PositionDeleteGroup {
                referenced_data_file: data_file.to_string(),
                partition_spec_id: partition.partition_spec_id(),
                // The backend never reconstructs partition values; the
                // frontend decodes them from the frozen descriptor against
                // the real table metadata at commit.
                partition_values: crate::iceberg::spec::Struct::empty(),
                positions: ordered,
            }],
        )
        .await
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
        let [file] = written.as_slice() else {
            return Err(error(
                ConnectorErrorKind::Internal,
                "Iceberg position-delete writer staged an unexpected file count",
            ));
        };
        self.staged_paths.push(file.path.clone());
        let metrics = IcebergArtifactMetrics::try_new(
            file.record_count,
            file.file_size_in_bytes,
            Vec::new(),
            None,
        )?;
        let artifact = IcebergPositionDeleteFileArtifact::try_new(
            file.path.clone(),
            partition,
            metrics,
            data_file.to_string(),
            merged,
        )?;
        Ok(IcebergCommitFragment::position_delete_file(artifact))
    }

    async fn stage_deletion_vector(
        &mut self,
        data_file: &str,
        positions: &roaring::RoaringTreemap,
        merged: Vec<String>,
    ) -> Result<IcebergCommitFragment, ConnectorError> {
        let target = self.handle.old_deletes().get(data_file).ok_or_else(|| {
            error(
                ConnectorErrorKind::CorruptData,
                "frozen Iceberg delete merge target disappeared",
            )
        })?;
        let partition = target.partition().clone();
        let mut vector = DeletionVector::new();
        for position in positions.iter() {
            vector.insert(position).map_err(|vector_error| {
                error(
                    ConnectorErrorKind::CorruptData,
                    format!("encode Iceberg deletion vector failed: {vector_error}"),
                )
            })?;
        }
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        let path = staged_path(
            &self.handle,
            &self.physical,
            partition.partition_path(),
            sequence,
            "puffin",
        );
        let written = write_single_deletion_vector_puffin(&self.file_io, &path, data_file, &vector)
            .await
            .map_err(|write_error| {
                error(
                    ConnectorErrorKind::Internal,
                    format!("stage Iceberg deletion vector failed: {write_error}"),
                )
            })?;
        self.staged_paths.push(written.path.clone());
        let metrics = IcebergArtifactMetrics::try_new(
            written.cardinality,
            written.file_size_in_bytes,
            Vec::new(),
            None,
        )?;
        let artifact = IcebergDeletionVectorArtifact::try_new(
            written.path,
            partition,
            metrics,
            written.referenced_data_file,
            IcebergContentRange::try_new(written.content_offset, written.content_size_in_bytes)?,
            written.cardinality,
            merged,
        )?;
        Ok(IcebergCommitFragment::deletion_vector(artifact))
    }

    async fn cleanup(&mut self) -> Result<(), ConnectorError> {
        if self.staged_paths.is_empty() {
            return Ok(());
        }
        let paths = std::mem::take(&mut self.staged_paths);
        let file_io = self.file_io.clone();
        for path in &paths {
            file_io.delete(path).await.map_err(|delete_error| {
                error(
                    ConnectorErrorKind::Internal,
                    format!("cleanup staged Iceberg delete artifact {path}: {delete_error}"),
                )
            })?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for IcebergDeleteStackWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        ensure_live(self.terminal, &self.request_context, self.branch_name())?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if batch.num_columns() < 2 {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg delete writer input requires _file and _pos columns",
            ));
        }
        let file_paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                error(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg delete writer _file column must be Utf8",
                )
            })?;
        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                error(
                    ConnectorErrorKind::CorruptData,
                    "Iceberg delete writer _pos column must be Int64",
                )
            })?;
        if file_paths.null_count() != 0 || positions.null_count() != 0 {
            return Err(error(
                ConnectorErrorKind::InvalidRequest,
                "Iceberg delete writer rows must not contain NULL _file or _pos",
            ));
        }
        for row in 0..batch.num_rows() {
            let referenced = file_paths.value(row);
            // A row for a data file this logical target does not own would
            // make two writers rewrite the same data file's deletes. The
            // sealed route says which target owns it, so a foreign row is a
            // routing error, not something to absorb.
            let target = self.handle.old_deletes().get(referenced).ok_or_else(|| {
                error(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "Iceberg {} writer does not own data file `{referenced}` (owned files={})",
                        self.handle.branch().as_str(),
                        self.handle.old_deletes().len()
                    ),
                )
            })?;
            let position = u64::try_from(positions.value(row)).map_err(|_| {
                error(
                    ConnectorErrorKind::InvalidRequest,
                    "Iceberg delete writer position must be non-negative",
                )
            })?;
            if position >= target.data_file_record_count() {
                return Err(error(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "Iceberg delete writer position {position} is outside `{referenced}`, which has {} rows",
                        target.data_file_record_count()
                    ),
                ));
            }
            insert_pending_delete_position(
                &mut self.pending,
                &mut self.pending_reservation,
                referenced,
                position,
            )?;
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        ensure_live(self.terminal, &self.request_context, self.branch_name())?;
        self.terminal = true;
        let pending = std::mem::take(&mut self.pending);
        let mut fragments = Vec::with_capacity(pending.len());
        for (data_file, new_positions) in pending {
            let target = self
                .handle
                .old_deletes()
                .get(&data_file)
                .cloned()
                .ok_or_else(|| {
                    error(
                        ConnectorErrorKind::CorruptData,
                        "frozen Iceberg delete merge target disappeared",
                    )
                })?;
            // D10: the old artifacts are read here, on the backend, through
            // this request's storage lease. Any failure — missing, unreadable,
            // corrupt, mismatched, or stale — propagates and fails the query.
            // The retained merge result and its temporary decoded bitmap,
            // staging bitmap, ordered positions and serialized DV coexist.
            // Bound them by the exact file row-position domain, not observed
            // cardinality or a sample. This can refuse a very large domain.
            let merge_bytes = delete_merge_reservation_bytes(&target, new_positions.len())?;
            let _merge_reservation = self
                .resources
                .try_reserve(ConnectorResourceClass::WriterState, merge_bytes)?;
            let merged = read_and_merge_old_deletes(&target, &self.binding, &self.request_context)?;
            let merged_references = merged.merged_references().to_vec();
            let mut positions = merged.into_positions();
            positions |= new_positions;
            if positions.is_empty() {
                continue;
            }
            let fragment = match self.handle.branch() {
                IcebergWriteBranch::DeletionVector => {
                    self.stage_deletion_vector(&data_file, &positions, merged_references)
                        .await?
                }
                IcebergWriteBranch::PositionDelete => {
                    self.stage_position_delete(&data_file, &positions, merged_references)
                        .await?
                }
                IcebergWriteBranch::Data | IcebergWriteBranch::EqualityDelete => {
                    return Err(error(
                        ConnectorErrorKind::CorruptData,
                        format!(
                            "Iceberg position-delete writer opened on the {} branch",
                            self.handle.branch().as_str()
                        ),
                    ));
                }
            };
            fragments.push(self.adapter.wrap_commit_fragment(fragment));
        }
        self.pending_reservation.shrink_to(0)?;
        Ok(fragments)
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        self.pending.clear();
        self.pending_reservation.shrink_to(0)?;
        self.cleanup().await?;
        self.terminal = true;
        Ok(())
    }
}

fn insert_pending_delete_position(
    pending: &mut BTreeMap<String, roaring::RoaringTreemap>,
    reservation: &mut ConnectorResourceReservation,
    referenced: &str,
    position: u64,
) -> Result<(), ConnectorError> {
    // A sparse RoaringTreemap can allocate a high-key node and a
    // bitmap container for one position. Charge the upper bound before
    // mutating it; duplicates expose a broken unique-locator contract.
    if pending
        .get(referenced)
        .is_some_and(|positions| positions.contains(position))
    {
        return Err(error(
            ConnectorErrorKind::CorruptData,
            format!("Iceberg delete writer repeats position {position} in {referenced}"),
        ));
    }
    let key_bytes = if pending.contains_key(referenced) {
        0
    } else {
        (referenced.len() as u64).checked_add(1024).ok_or_else(|| {
            error(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg pending delete path reservation overflow",
            )
        })?
    };
    reservation.try_grow(1024_u64.checked_add(key_bytes).ok_or_else(|| {
        error(
            ConnectorErrorKind::ResourceExhausted,
            "Iceberg pending delete reservation overflow",
        )
    })?)?;
    let inserted = pending
        .entry(referenced.to_string())
        .or_default()
        .insert(position);
    debug_assert!(inserted);
    Ok(())
}

/// A conservative upper bound for the resident valid merge/staging state.
/// Each possible position is charged for four sparse container allocations;
/// encoded old artifacts and retained path/reference bookkeeping are added.
fn delete_merge_reservation_bytes(
    target: &crate::commit::write_stack::old_delete::IcebergOldDeleteMergeTarget,
    new_count: u64,
) -> Result<u64, ConnectorError> {
    let rows = if target.references().is_empty() {
        new_count
    } else {
        target.data_file_record_count()
    };
    let mut bytes = rows
        .checked_mul(1024)
        .and_then(|value| value.checked_add(512))
        .ok_or_else(|| {
            error(
                ConnectorErrorKind::ResourceExhausted,
                "Iceberg delete merge row-domain reservation overflow",
            )
        })?;
    for reference in target.references() {
        bytes = reference
            .file_size_in_bytes()
            .checked_mul(4)
            .and_then(|value| value.checked_add(reference.path().len() as u64 + 512))
            .and_then(|value| bytes.checked_add(value))
            .ok_or_else(|| {
                error(
                    ConnectorErrorKind::ResourceExhausted,
                    "Iceberg old delete merge reservation overflow",
                )
            })?;
    }
    Ok(bytes)
}

/// The per-driver equality-delete writer.
///
/// It is a separate writer from the position-delete one because the two share
/// nothing but the word "delete": this one reads no old artifact, owns no data
/// file, and stages a Parquet file whose rows *are* the match key. Its Arrow
/// types come from the fragment's own frozen input schema; the handle carries
/// only the field ids and the type rendering the frontend signed, and a
/// disagreement between them is a refusal rather than a coercion.
struct IcebergEqualityDeleteStackWriter {
    adapter: IcebergWriteAdapter,
    handle: IcebergWriterHandle,
    request_context: ConnectorRequestContext,
    file_io: crate::iceberg::io::FileIO,
    staging_dir: String,
    columns: Vec<crate::commit::EqualityDeleteColumn>,
    equality_field_ids: Vec<i32>,
    partition: IcebergArtifactPartition,
    fragments: Vec<ConnectorCommitFragment>,
    staged_paths: Vec<String>,
    terminal: bool,
}

impl IcebergEqualityDeleteStackWriter {
    fn open(
        execution: &IcebergWriteStackExecution,
        handle: IcebergWriterHandle,
        request: ConnectorOpenWriterRequest,
    ) -> Result<Self, ConnectorError> {
        let recipe = handle.equality().ok_or_else(|| {
            error(
                ConnectorErrorKind::CorruptData,
                "Iceberg equality-delete writer handle carries no equality recipe",
            )
        })?;
        let columns = resolve_equality_columns(recipe, &request.expected_schema)?;
        let equality_field_ids = recipe.field_ids();
        let binding = execution.binding.for_request(request.context.clone());
        let file_io = build_staged_file_io(&binding, handle.table().data_location())
            .map_err(|message| error(ConnectorErrorKind::InvalidRequest, message))?;
        // One directory per query attempt, the same shape every other writer
        // stages into, so an abort has one prefix to clean.
        let staging_dir = format!(
            "{}/_staging/{}",
            handle.table().data_location().trim_end_matches('/'),
            uuid::Uuid::from_bytes(request.physical.execution_query_id())
        );
        // An equality delete is admitted only on an unpartitioned table, so its
        // artifact carries the empty partition of the table's default spec. The
        // frontend still decodes that descriptor against the real metadata at
        // commit, which is what makes the emptiness checked rather than assumed.
        let partition = IcebergArtifactPartition::try_new(
            String::new(),
            String::new(),
            handle.table().default_partition_spec_id(),
            crate::write_descriptor::IcebergPartitionDescriptor { values: Vec::new() },
        )?;
        Ok(Self {
            adapter: execution.adapter.clone(),
            handle,
            request_context: request.context,
            file_io,
            staging_dir,
            columns,
            equality_field_ids,
            partition,
            fragments: Vec::new(),
            staged_paths: Vec::new(),
            terminal: false,
        })
    }

    async fn cleanup(&mut self) -> Result<(), ConnectorError> {
        if self.staged_paths.is_empty() {
            return Ok(());
        }
        let paths = std::mem::take(&mut self.staged_paths);
        let file_io = self.file_io.clone();
        for path in &paths {
            file_io.delete(path).await.map_err(|delete_error| {
                error(
                    ConnectorErrorKind::Internal,
                    format!("cleanup staged Iceberg equality-delete file {path}: {delete_error}"),
                )
            })?;
        }
        Ok(())
    }
}

/// Bind the frozen match key to the Arrow types the fragment actually carries.
///
/// The handle names the columns and their field ids; the fragment's expected
/// schema is the only place a real Arrow type exists on this side. Requiring
/// them to agree exactly is what keeps an Arrow value off the FE/BE boundary
/// without letting a writer invent a type.
fn resolve_equality_columns(
    recipe: &crate::commit::write_stack::domain::IcebergEqualityDeleteRecipe,
    expected_schema: &arrow::datatypes::SchemaRef,
) -> Result<Vec<crate::commit::EqualityDeleteColumn>, ConnectorError> {
    if recipe.columns().len() != expected_schema.fields().len() {
        return Err(error(
            ConnectorErrorKind::InvalidRequest,
            format!(
                "Iceberg equality-delete handle names {} columns but its fragment input carries {}",
                recipe.columns().len(),
                expected_schema.fields().len()
            ),
        ));
    }
    recipe
        .columns()
        .iter()
        .zip(expected_schema.fields())
        .map(|(frozen, actual)| {
            if frozen.name() != actual.name().as_str()
                || frozen.data_type() != format!("{:?}", actual.data_type())
                || frozen.nullable() != actual.is_nullable()
            {
                return Err(error(
                    ConnectorErrorKind::InvalidRequest,
                    format!(
                        "Iceberg equality-delete column `{}` does not match fragment input `{}`",
                        frozen.name(),
                        actual.name()
                    ),
                ));
            }
            Ok(crate::commit::EqualityDeleteColumn {
                name: frozen.name().to_string(),
                field_id: frozen.field_id(),
                data_type: actual.data_type().clone(),
                nullable: actual.is_nullable(),
            })
        })
        .collect()
}

#[async_trait::async_trait]
impl ConnectorBatchWriter for IcebergEqualityDeleteStackWriter {
    async fn append(&mut self, batch: RecordBatch) -> Result<(), ConnectorError> {
        ensure_live(self.terminal, &self.request_context, "equality-delete")?;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let written = crate::commit::write_equality_delete_file(
            &self.file_io,
            &self.staging_dir,
            self.handle.table().default_partition_spec_id(),
            self.columns.clone(),
            batch,
        )
        .await
        .map_err(|message| error(ConnectorErrorKind::Internal, message))?;
        let Some(written) = written else {
            return Ok(());
        };
        self.staged_paths.push(written.path.clone());
        let metrics = IcebergArtifactMetrics::try_new(
            written.record_count,
            written.file_size_in_bytes,
            Vec::new(),
            None,
        )?;
        let artifact = IcebergEqualityDeleteFileArtifact::try_new(
            written.path,
            self.partition.clone(),
            metrics,
            self.equality_field_ids.clone(),
        )?;
        self.fragments.push(
            self.adapter
                .wrap_commit_fragment(IcebergCommitFragment::equality_delete_file(artifact)),
        );
        Ok(())
    }

    async fn finish(&mut self) -> Result<Vec<ConnectorCommitFragment>, ConnectorError> {
        ensure_live(self.terminal, &self.request_context, "equality-delete")?;
        self.terminal = true;
        Ok(std::mem::take(&mut self.fragments))
    }

    async fn abort(&mut self) -> Result<(), ConnectorError> {
        self.cleanup().await?;
        self.fragments.clear();
        self.terminal = true;
        Ok(())
    }
}

#[cfg(test)]
mod bounded_delete_tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorResourceCheckpoint, ConnectorResourceLease, ConnectorResourceLedger,
    };
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Ledger {
        limit: u64,
        used: Arc<AtomicU64>,
    }
    struct Lease {
        limit: u64,
        used: Arc<AtomicU64>,
        held: u64,
    }
    impl ConnectorResourceLedger for Ledger {
        fn checkpoint(&self) -> Result<ConnectorResourceCheckpoint, ConnectorError> {
            Ok(ConnectorResourceCheckpoint::new(1))
        }
        fn try_reserve(
            &self,
            class: ConnectorResourceClass,
            bytes: u64,
        ) -> Result<Box<dyn ConnectorResourceLease>, ConnectorError> {
            assert_eq!(class, ConnectorResourceClass::WriterState);
            let mut lease = Lease {
                limit: self.limit,
                used: self.used.clone(),
                held: 0,
            };
            lease.try_grow(bytes)?;
            Ok(Box::new(lease))
        }
    }
    impl ConnectorResourceLease for Lease {
        fn bytes(&self) -> u64 {
            self.held
        }
        fn try_grow(&mut self, additional: u64) -> Result<(), ConnectorError> {
            if self
                .used
                .load(Ordering::SeqCst)
                .checked_add(additional)
                .is_none_or(|value| value > self.limit)
            {
                return Err(error(
                    ConnectorErrorKind::ResourceExhausted,
                    "test writer capacity refused",
                ));
            }
            self.used.fetch_add(additional, Ordering::SeqCst);
            self.held += additional;
            Ok(())
        }
        fn shrink_to(&mut self, bytes: u64) -> Result<(), ConnectorError> {
            self.used.fetch_sub(self.held - bytes, Ordering::SeqCst);
            self.held = bytes;
            Ok(())
        }
    }
    impl Drop for Lease {
        fn drop(&mut self) {
            self.used.fetch_sub(self.held, Ordering::SeqCst);
        }
    }

    #[test]
    fn delete_writer_duplicate_locator_fails_without_mutating_or_growing_state() {
        let used = Arc::new(AtomicU64::new(0));
        let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(Ledger {
            limit: 10000,
            used: used.clone(),
        }));
        let mut reservation = resources
            .try_reserve(ConnectorResourceClass::WriterState, 256)
            .unwrap();
        let mut pending = BTreeMap::new();
        insert_pending_delete_position(&mut pending, &mut reservation, "file", 7).unwrap();
        let before = used.load(Ordering::SeqCst);
        let error =
            insert_pending_delete_position(&mut pending, &mut reservation, "file", 7).unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
        assert_eq!(pending["file"].len(), 1);
        assert_eq!(used.load(Ordering::SeqCst), before);
        drop(pending);
        drop(reservation);
        assert_eq!(used.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn delete_writer_capacity_refusal_happens_before_pending_allocation() {
        let used = Arc::new(AtomicU64::new(0));
        let resources = ConnectorExecutionResources::from_admitted_ledger(Arc::new(Ledger {
            limit: 256,
            used: used.clone(),
        }));
        let mut reservation = resources
            .try_reserve(ConnectorResourceClass::WriterState, 256)
            .unwrap();
        let mut pending = BTreeMap::new();
        assert_eq!(
            insert_pending_delete_position(&mut pending, &mut reservation, "file", 7)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::ResourceExhausted
        );
        assert!(pending.is_empty());
        assert_eq!(used.load(Ordering::SeqCst), 256);
    }
}
