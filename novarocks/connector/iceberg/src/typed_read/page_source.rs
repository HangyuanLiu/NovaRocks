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

//! One split, one page source.
//!
//! A page source owns its cursor, its reader, its buffers, and one close
//! latch. Nothing here is process-global and nothing survives an attempt: a
//! replacement attempt builds a new page source over the same frozen split
//! rather than resuming this one.
//!
//! Three invariants shape the reader:
//!
//! * a row position is file-level, absolute, and zero-based -- byte-range
//!   selection narrows which row groups are read, never how rows are numbered;
//! * the scan's ordered columns are the output prefix, and whatever the delete
//!   filter needs is appended as a hidden suffix that is dropped again once the
//!   deletes and the remaining predicate have run over the complete page;
//! * `next_source_page() == None` means "nothing right now". Only
//!   [`ConnectorPageSource::is_finished`] is terminal.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, FixedSizeBinaryArray,
    Float32Array, Float64Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
    RecordBatch, StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
    TimestampNanosecondArray, UInt64Array,
};
use arrow::datatypes::{Field, FieldRef, Schema as ArrowSchema};
use novarocks_fs::{
    FileBatchReader, FileIdentity, FileProjection, FileReadBudget, FileReadContext, FileReadRange,
    FileReadRequest, FileReaderOptions, FsAccessHandle, ParquetMetadataInspection, PhysicalPruning,
    inspect_parquet_metadata, open_file_reader,
};
use novarocks_proto::connector_read::WireDynamicFilter;
use novarocks_spi::connector::read_stack::{
    ConnectorPageSource, ConnectorSplit, ConnectorValue, ConnectorValueType, Domain,
    PageSourceMetrics, SourcePage, TupleDomain,
};
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::access_binding::IcebergReadBinding;
use crate::file_reader::map_file_error;
use crate::iceberg::spec::{Literal, NameMapping, PartitionSpec, Schema, Struct};

use super::column_handle::{IcebergColumnHandle, corrupt, invalid, unsupported};
use super::delete_manager::{DeleteEvaluationMode, DeleteManager, SplitDeleteFilter};
use super::schema_binding::{
    FileFieldIdCoverage, IcebergColumnSource, IcebergSchemaBinding, IcebergSchemaBindingRequest,
    IcebergSplitFacts, bind_scan_columns,
};
use super::split::{IcebergFileFormat, IcebergSplit, ParquetFileDecryptionData};
use super::table_handle::IcebergTableHandle;

/// Where the live dynamic filter is consulted.
///
/// The three checkpoints are the only moments at which pruning could still
/// save work: once the footer is known, once before the split's first row
/// group is decoded, and once before every row group that has not been read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DynamicFilterCheckpoint {
    Footer,
    FirstRowGroup,
    NextRowGroup,
}

/// What the dynamic-filter seam decided.
///
/// It has exactly one variant today because this stack does not prune on a
/// dynamic filter yet. Adding a pruning verdict later is an additive change,
/// and every match over it stays exhaustive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DynamicFilterVerdict {
    ReadEverything,
}

/// What one live dynamic filter looked like when the page source was built.
///
/// `createPageSource` hands out a borrowed filter while a page source is a
/// `'static` boxed trait object, so the live handle cannot be retained yet.
/// The observation is therefore taken once and is truthful about that: it
/// records what the filter said, and the seam below never claims to have acted
/// on it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DynamicFilterObservation {
    covered_columns: usize,
    constrains_anything: bool,
    complete: bool,
}

impl DynamicFilterObservation {
    pub fn observe(filter: &WireDynamicFilter) -> Self {
        let snapshot = filter.snapshot();
        Self {
            covered_columns: filter.columns_covered().len(),
            constrains_anything: !snapshot.current_predicate().is_all(),
            complete: snapshot.is_complete(),
        }
    }

    /// The unconstrained observation, matching `CompleteAllDynamicFilter`.
    pub const fn complete_all() -> Self {
        Self {
            covered_columns: 0,
            constrains_anything: false,
            complete: true,
        }
    }

    pub const fn covered_columns(&self) -> usize {
        self.covered_columns
    }

    /// Whether the filter had narrowed anything at all when it was observed.
    pub const fn constrains_anything(&self) -> bool {
        self.constrains_anything
    }

    pub const fn is_complete(&self) -> bool {
        self.complete
    }
}

/// The single seam where a live backend dynamic filter will prune this split.
///
/// It is deliberately the only place any dynamic-filter decision is made, so a
/// later task adds runtime-filter pruning here and nowhere else. Today it
/// prunes nothing: reporting a row group as skipped that was in fact read --
/// or the reverse -- would silently change what the scan returns, and the
/// engine keeps its own filter regardless.
const fn consult_dynamic_filter(
    _observation: &DynamicFilterObservation,
    _checkpoint: DynamicFilterCheckpoint,
) -> DynamicFilterVerdict {
    DynamicFilterVerdict::ReadEverything
}

/// The half-open absolute row-position window a split actually read.
///
/// It adapts the reader's per-batch positions into the `[start, end)` form the
/// Iceberg reader reasons in. It is private on purpose: neither the SPI nor the
/// wire carries a row-position window, and publishing one would invite a
/// scheduler to treat it as split identity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ReaderPageSourceWithRowPositions {
    start_row_position: Option<u64>,
    end_row_position: Option<u64>,
}

impl ReaderPageSourceWithRowPositions {
    const fn end_row_position(&self) -> Option<u64> {
        self.end_row_position
    }

    /// Fold one batch's absolute positions into the window.
    fn observe(&mut self, positions: &UInt64Array, path: &str) -> Result<(), ConnectorError> {
        if positions.is_empty() {
            return Ok(());
        }
        if positions.null_count() != 0 {
            return Err(corrupt(format!(
                "iceberg data file {path} produced a null absolute row position"
            )));
        }
        let first = positions.value(0);
        let last = positions.value(positions.len() - 1);
        if last < first {
            return Err(corrupt(format!(
                "iceberg data file {path} produced absolute row positions out of order"
            )));
        }
        if let Some(previous_end) = self.end_row_position
            && first < previous_end
        {
            return Err(corrupt(format!(
                "iceberg data file {path} revisited absolute row position {first}"
            )));
        }
        if self.start_row_position.is_none() {
            self.start_row_position = Some(first);
        }
        self.end_row_position = Some(last.saturating_add(1));
        Ok(())
    }
}

/// One immutable Parquet footer per data file, shared by the splits of a scan.
///
/// The cache lives on the provider, which lives for one fragment instance and
/// scan node. Splits of the same file therefore read the footer once, and
/// nothing survives the provider.
#[derive(Debug, Default)]
pub struct ParquetFooterCache {
    entries: Mutex<HashMap<Arc<str>, ParquetMetadataInspection>>,
}

impl ParquetFooterCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read one data file's footer, or hand back the copy this scan already has.
    pub fn footer(
        &self,
        access: &FsAccessHandle,
        context: &FileReadContext,
        path: &str,
        file_size: u64,
    ) -> Result<ParquetMetadataInspection, ConnectorError> {
        if let Some(cached) = self.lock()?.get(path) {
            return Ok(cached.clone());
        }
        let file = access
            .bind_location(path, FileIdentity::new(path, file_size, None))
            .map_err(map_file_error)?;
        let inspection =
            inspect_parquet_metadata(file, None, context.clone()).map_err(map_file_error)?;
        self.lock()?.insert(Arc::from(path), inspection.clone());
        Ok(inspection)
    }

    /// How many distinct footers this scan has read.
    pub fn len(&self) -> Result<usize, ConnectorError> {
        Ok(self.lock()?.len())
    }

    pub fn is_empty(&self) -> Result<bool, ConnectorError> {
        Ok(self.lock()?.is_empty())
    }

    fn lock(
        &self,
    ) -> Result<
        std::sync::MutexGuard<'_, HashMap<Arc<str>, ParquetMetadataInspection>>,
        ConnectorError,
    > {
        self.entries.lock().map_err(|error| {
            ConnectorError::new(
                ConnectorErrorKind::Internal,
                format!("iceberg parquet footer cache lock: {error}"),
            )
        })
    }
}

/// Everything one page source needs, all of it already frozen or process-local.
pub struct IcebergPageSourceRequest<'a> {
    pub table_handle: &'a IcebergTableHandle,
    pub split: &'a IcebergSplit,
    /// The scan's ordered output columns; they become the page's prefix.
    pub columns: &'a [IcebergColumnHandle],
    pub delete_manager: Arc<DeleteManager>,
    pub footers: Arc<ParquetFooterCache>,
    pub access_binding: IcebergReadBinding,
    pub context: FileReadContext,
    pub budget: FileReadBudget,
    pub reader_options: FileReaderOptions,
    pub dynamic_filter: DynamicFilterObservation,
}

/// Build the page source for one Iceberg data split.
pub fn create_iceberg_page_source(
    request: IcebergPageSourceRequest<'_>,
) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
    let split = request.split;
    admit_file_format(split.file_format())?;
    reject_encryption_material(
        split.decryption_data(),
        &format!("iceberg data file {}", split.path()),
    )?;
    for delete in split.deletes() {
        reject_encryption_material(
            delete.decryption_data(),
            &format!("iceberg delete file {}", delete.path()),
        )?;
    }

    let table_schema = Arc::new(request.table_handle.parse_table_schema()?);
    let partition_spec = request
        .table_handle
        .parse_partition_spec(split.partition_spec_id())?;
    let partition_values = parse_partition_values(split, &partition_spec, &table_schema)?;
    let effective_predicate = request.table_handle.effective_predicate()?;

    if let Some(fast_path) = try_partition_only_page_source(
        split,
        request.columns,
        &partition_spec,
        &partition_values,
        &table_schema,
        &effective_predicate,
        request.budget,
    )? {
        return Ok(Box::new(fast_path));
    }

    let name_mapping = parse_name_mapping(request.table_handle)?;
    let delete_filter = request.delete_manager.open_split(
        split,
        &table_schema,
        DeleteEvaluationMode::ExcludeDeleted,
    )?;
    let hidden_columns = delete_filter.required_hidden_columns().to_vec();

    Ok(Box::new(IcebergParquetPageSource {
        split: split.clone(),
        table_schema,
        name_mapping,
        partition_spec,
        partition_values,
        prefix_len: request.columns.len(),
        bound_handles: request
            .columns
            .iter()
            .cloned()
            .chain(hidden_columns)
            .collect(),
        delete_filter,
        effective_predicate,
        access_binding: request.access_binding,
        context: request.context,
        footers: request.footers,
        budget: request.budget,
        reader_options: request.reader_options,
        dynamic_filter: request.dynamic_filter,
        state: ReaderState::NotOpened,
        row_window: ReaderPageSourceWithRowPositions::default(),
        completed_bytes: 0,
        completed_positions: 0,
        read_time_nanos: 0,
        retained_bytes: split.retained_size_in_bytes(),
        finished: false,
        closed: false,
    }))
}

/// Parquet is implemented; the other formats keep their contract slot and a
/// stable rejection rather than a partially working reader.
fn admit_file_format(format: IcebergFileFormat) -> Result<(), ConnectorError> {
    match format {
        IcebergFileFormat::Parquet => Ok(()),
        IcebergFileFormat::Orc => Err(unsupported(
            "iceberg ORC data files are not readable by this page source",
        )),
        IcebergFileFormat::Avro => Err(unsupported(
            "iceberg AVRO data files are not readable by this page source",
        )),
        IcebergFileFormat::Puffin => Err(invalid(
            "an iceberg data split is never in the puffin delete-artifact format",
        )),
    }
}

/// Modular encryption is contracted but not implemented.
///
/// The rejection names the file and nothing else: key metadata and AAD
/// prefixes never reach a message, a log, or a `Debug` rendering.
fn reject_encryption_material(
    material: Option<&ParquetFileDecryptionData>,
    what: &str,
) -> Result<(), ConnectorError> {
    let Some(material) = material else {
        return Ok(());
    };
    if material.key_metadata().is_empty() && material.aad_prefix().is_empty() {
        return Ok(());
    }
    Err(unsupported(format!(
        "{what} carries parquet decryption material, which this read stack does not implement"
    )))
}

fn parse_name_mapping(
    table_handle: &IcebergTableHandle,
) -> Result<Option<Arc<NameMapping>>, ConnectorError> {
    let Some(json) = table_handle.name_mapping_json() else {
        return Ok(None);
    };
    let mapping: NameMapping = serde_json::from_str(json)
        .map_err(|error| invalid(format!("iceberg name mapping json is invalid: {error}")))?;
    Ok(Some(Arc::new(mapping)))
}

fn parse_partition_values(
    split: &IcebergSplit,
    partition_spec: &PartitionSpec,
    table_schema: &Schema,
) -> Result<Struct, ConnectorError> {
    let partition_type = partition_spec
        .partition_type(table_schema)
        .map_err(|error| {
            invalid(format!(
                "iceberg partition spec {} does not bind to the frozen table schema: {error}",
                partition_spec.spec_id()
            ))
        })?;
    let json: serde_json::Value =
        serde_json::from_str(split.partition_data_json()).map_err(|error| {
            corrupt(format!(
                "iceberg split partition data json is invalid: {error}"
            ))
        })?;
    let literal = Literal::try_from_json(json, &crate::iceberg::spec::Type::Struct(partition_type))
        .map_err(|error| corrupt(format!("iceberg split partition data: {error}")))?;
    match literal {
        Some(Literal::Struct(values)) => Ok(values),
        // A partition struct is never absent: an unpartitioned file encodes an
        // empty struct, which is a value, not a missing fact.
        Some(_) | None => Err(corrupt(
            "iceberg split partition data json is not a partition struct",
        )),
    }
}

// ---------------------------------------------------------------------------
// Partition-only fast path
// ---------------------------------------------------------------------------

/// Build the partition-only page source when every precondition holds.
///
/// The path is legal only when the split covers the whole file, has no
/// deletes, has an unconstrained effective predicate, and every requested
/// column is an identity partition column. All four facts are frozen, so the
/// data file is never opened and the record count comes straight from the
/// manifest.
fn try_partition_only_page_source(
    split: &IcebergSplit,
    columns: &[IcebergColumnHandle],
    partition_spec: &PartitionSpec,
    partition_values: &Struct,
    table_schema: &Schema,
    effective_predicate: &TupleDomain<IcebergColumnHandle>,
    budget: FileReadBudget,
) -> Result<Option<IcebergPartitionOnlyPageSource>, ConnectorError> {
    if !split.is_whole_file() || !split.deletes().is_empty() || !effective_predicate.is_all() {
        return Ok(None);
    }
    let binding = bind_scan_columns(IcebergSchemaBindingRequest {
        table_schema,
        // An empty file schema forces every column onto a non-physical source,
        // so a column that would need the file cannot slip into this path.
        file_schema: &Arc::new(ArrowSchema::empty()),
        name_mapping: None,
        partition_spec: Some(partition_spec),
        partition_values: Some(partition_values),
        columns,
    });
    // A binding failure here only proves the fast path does not apply. The
    // ordinary reader re-binds against the real footer and raises the same
    // error properly if it is a genuine one.
    let Ok(binding) = binding else {
        return Ok(None);
    };
    let mut constants = Vec::with_capacity(binding.columns().len());
    for column in binding.columns() {
        match column.source() {
            IcebergColumnSource::IdentityPartitionConstant(value) => {
                constants.push((Arc::clone(column.target()), value.clone()));
            }
            IcebergColumnSource::Physical { .. }
            | IcebergColumnSource::InitialDefault
            | IcebergColumnSource::TypedNull
            | IcebergColumnSource::Metadata(_) => return Ok(None),
        }
    }
    let total_rows = u64::try_from(split.file_record_count()).map_err(|_| {
        corrupt(format!(
            "iceberg data file {} declares a negative record count",
            split.path()
        ))
    })?;
    Ok(Some(IcebergPartitionOnlyPageSource {
        constants,
        total_rows,
        emitted_rows: 0,
        max_batch_rows: budget.max_rows.get(),
        retained_bytes: split.retained_size_in_bytes(),
        closed: false,
    }))
}

/// A scan that needs no byte of the data file.
pub struct IcebergPartitionOnlyPageSource {
    constants: Vec<(FieldRef, Option<Literal>)>,
    total_rows: u64,
    emitted_rows: u64,
    max_batch_rows: usize,
    retained_bytes: u64,
    closed: bool,
}

impl ConnectorPageSource for IcebergPartitionOnlyPageSource {
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if self.closed || self.emitted_rows >= self.total_rows {
            return Ok(None);
        }
        let remaining = self.total_rows - self.emitted_rows;
        let rows = usize::try_from(remaining.min(self.max_batch_rows as u64)).map_err(|_| {
            ConnectorError::new(ConnectorErrorKind::Internal, "row budget overflow")
        })?;
        let page = if self.constants.is_empty() {
            // A count-only or partition-only scan legitimately produces
            // positions without producing a single column.
            SourcePage::zero_channel(rows)
        } else {
            let mut columns = Vec::with_capacity(self.constants.len());
            for (field, value) in &self.constants {
                columns.push(constant_column(value.as_ref(), field.as_ref(), rows)?);
            }
            SourcePage::try_new(rows, columns)?
        };
        self.emitted_rows += rows as u64;
        Ok(Some(page))
    }

    fn is_finished(&self) -> bool {
        self.closed || self.emitted_rows >= self.total_rows
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: 0,
            completed_positions: self.emitted_rows,
            read_time_nanos: 0,
        }
    }

    fn memory_usage_bytes(&self) -> u64 {
        self.retained_bytes
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        self.closed = true;
        Ok(())
    }
}

fn constant_column(
    value: Option<&Literal>,
    field: &Field,
    rows: usize,
) -> Result<ArrayRef, ConnectorError> {
    match value {
        None => Ok(arrow::array::new_null_array(field.data_type(), rows)),
        Some(literal) => {
            crate::default_value::literal_to_constant_array(literal, field.data_type(), rows)
                .map_err(|error| {
                    corrupt(format!(
                        "iceberg identity partition constant for {}: {error}",
                        field.name()
                    ))
                })
        }
    }
}

// ---------------------------------------------------------------------------
// The Parquet page source
// ---------------------------------------------------------------------------

enum ReaderState {
    NotOpened,
    Open {
        reader: Box<dyn FileBatchReader>,
        binding: IcebergSchemaBinding,
        page_schema: Arc<ArrowSchema>,
        checks: Vec<PredicateCheck>,
        positions_required: bool,
        first_row_group_pending: bool,
    },
    Drained,
}

/// One column domain the page source still has to prove per row.
struct PredicateCheck {
    channel: usize,
    domain: Domain,
}

/// The per-split Parquet reader.
pub struct IcebergParquetPageSource {
    split: IcebergSplit,
    table_schema: Arc<Schema>,
    name_mapping: Option<Arc<NameMapping>>,
    partition_spec: PartitionSpec,
    partition_values: Struct,
    /// How many leading channels the scan actually asked for.
    prefix_len: usize,
    /// The ordered output columns followed by the delete filter's hidden suffix.
    bound_handles: Vec<IcebergColumnHandle>,
    delete_filter: SplitDeleteFilter,
    effective_predicate: TupleDomain<IcebergColumnHandle>,
    access_binding: IcebergReadBinding,
    context: FileReadContext,
    footers: Arc<ParquetFooterCache>,
    budget: FileReadBudget,
    reader_options: FileReaderOptions,
    dynamic_filter: DynamicFilterObservation,
    state: ReaderState,
    row_window: ReaderPageSourceWithRowPositions,
    completed_bytes: u64,
    completed_positions: u64,
    read_time_nanos: u64,
    retained_bytes: u64,
    finished: bool,
    closed: bool,
}

impl IcebergParquetPageSource {
    fn open(&mut self) -> Result<(), ConnectorError> {
        let access = self.access_binding.resolve_access(self.split.path())?;
        let file_size = u64::try_from(self.split.file_size()).map_err(|_| {
            corrupt(format!(
                "iceberg data file {} declares a negative size",
                self.split.path()
            ))
        })?;
        let footer = self
            .footers
            .footer(&access, &self.context, self.split.path(), file_size)?;
        match consult_dynamic_filter(&self.dynamic_filter, DynamicFilterCheckpoint::Footer) {
            DynamicFilterVerdict::ReadEverything => {}
        }

        let binding = bind_scan_columns(IcebergSchemaBindingRequest {
            table_schema: &self.table_schema,
            file_schema: footer.schema(),
            name_mapping: self.name_mapping.clone(),
            partition_spec: Some(&self.partition_spec),
            partition_values: Some(&self.partition_values),
            columns: &self.bound_handles,
        })?;

        // A bounded split is defined by a row range, so its rows can only be
        // named by their file-level absolute positions. Deletes and row
        // lineage need them for the same reason.
        let positions_required = !self.split.is_whole_file()
            || !self.delete_filter.is_empty()
            || binding.requires_row_positions();

        let projection = match binding.coverage() {
            // A legacy file has no field ids to project by, so the whole file
            // is opened and the name mapping resolves the columns afterwards.
            FileFieldIdCoverage::None => FileProjection::All,
            FileFieldIdCoverage::Complete => {
                FileProjection::FieldIds(binding.physical_base_field_ids().to_vec())
            }
        };
        let range = if self.split.is_whole_file() {
            FileReadRange::WholeFile
        } else {
            let start = u64::try_from(self.split.start())
                .map_err(|_| corrupt("iceberg split start offset is negative".to_owned()))?;
            let length = u64::try_from(self.split.length())
                .map_err(|_| corrupt("iceberg split length is negative".to_owned()))?;
            FileReadRange::bounded(start, length).map_err(map_file_error)?
        };

        let file = access
            .bind_location(
                self.split.path(),
                FileIdentity::new(self.split.path(), file_size, None),
            )
            .map_err(map_file_error)?;
        let reader = open_file_reader(FileReadRequest {
            file,
            format: novarocks_fs::FileFormat::Parquet,
            range,
            projection,
            budget: self.budget,
            predicates: Vec::new(),
            pruning: PhysicalPruning::default(),
            options: self.reader_options,
            cache: None,
            context: self.context.clone(),
        })
        .map_err(map_file_error)?;

        let page_schema = Arc::new(ArrowSchema::new(
            binding
                .columns()
                .iter()
                .map(|column| Arc::clone(column.target()))
                .collect::<Vec<_>>(),
        ));
        let checks = self.build_predicate_checks()?;

        self.state = ReaderState::Open {
            reader,
            binding,
            page_schema,
            checks,
            positions_required,
            first_row_group_pending: true,
        };
        Ok(())
    }

    /// Bind the effective predicate onto the page's channels.
    ///
    /// A constrained column the page does not produce cannot be proved here,
    /// and silently dropping it would let the scan return rows the predicate
    /// excludes.
    fn build_predicate_checks(&self) -> Result<Vec<PredicateCheck>, ConnectorError> {
        let Some(domains) = self.effective_predicate.domains() else {
            // An unsatisfiable predicate is handled before any read; reaching
            // it here would mean the split should never have been opened.
            return Err(invalid(
                "iceberg scan opened a split whose effective predicate is unsatisfiable",
            ));
        };
        let mut checks = Vec::with_capacity(domains.len());
        for (column, domain) in domains {
            let channel = self
                .bound_handles
                .iter()
                .position(|handle| handle == column)
                .ok_or_else(|| {
                    unsupported(format!(
                        "iceberg scan constrains field id {} that its page does not produce",
                        column.base_field_id()
                    ))
                })?;
            checks.push(PredicateCheck {
                channel,
                domain: domain.clone(),
            });
        }
        Ok(checks)
    }
}

impl ConnectorPageSource for IcebergParquetPageSource {
    fn next_source_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if self.closed || self.finished {
            return Ok(None);
        }
        let began = Instant::now();
        let result = self.produce_page();
        self.read_time_nanos = self
            .read_time_nanos
            .saturating_add(began.elapsed().as_nanos() as u64);
        result
    }

    fn is_finished(&self) -> bool {
        self.finished || self.closed
    }

    fn metrics(&self) -> PageSourceMetrics {
        PageSourceMetrics {
            completed_bytes: self.completed_bytes,
            completed_positions: self.completed_positions,
            read_time_nanos: self.read_time_nanos,
        }
    }

    fn memory_usage_bytes(&self) -> u64 {
        let binding = match &self.state {
            ReaderState::Open { binding, .. } => binding.retained_size_in_bytes(),
            ReaderState::NotOpened | ReaderState::Drained => 0,
        };
        self.retained_bytes.saturating_add(binding)
    }

    fn close(&mut self) -> Result<(), ConnectorError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        // The reader is dropped whatever its own close says: a page source
        // that has been closed must not keep an open file handle alive because
        // the underlying close reported a late I/O error.
        let state = std::mem::replace(&mut self.state, ReaderState::Drained);
        if let ReaderState::Open { mut reader, .. } = state {
            self.completed_bytes = reader.metrics_snapshot().bytes_read;
            reader.close().map_err(map_file_error)?;
        }
        Ok(())
    }
}

impl IcebergParquetPageSource {
    fn produce_page(&mut self) -> Result<Option<SourcePage>, ConnectorError> {
        if matches!(self.state, ReaderState::NotOpened) {
            self.open()?;
        }
        loop {
            let ReaderState::Open {
                reader,
                binding,
                page_schema,
                checks,
                positions_required,
                first_row_group_pending,
            } = &mut self.state
            else {
                self.finished = true;
                return Ok(None);
            };

            // Every batch may cross into a row group this split has not read
            // yet, which is exactly when a tighter dynamic filter could still
            // save the decode.
            let checkpoint = if *first_row_group_pending {
                *first_row_group_pending = false;
                DynamicFilterCheckpoint::FirstRowGroup
            } else {
                DynamicFilterCheckpoint::NextRowGroup
            };
            match consult_dynamic_filter(&self.dynamic_filter, checkpoint) {
                DynamicFilterVerdict::ReadEverything => {}
            }

            let next = reader.next_batch().map_err(map_file_error)?;
            self.completed_bytes = reader.metrics_snapshot().bytes_read;
            let Some(file_batch) = next else {
                self.finished = true;
                self.state = ReaderState::Drained;
                return Ok(None);
            };

            let positions = file_batch.physical_row_positions;
            if *positions_required && positions.is_none() {
                return Err(corrupt(format!(
                    "iceberg data file {} did not report the absolute row position of the split's first row group",
                    self.split.path()
                )));
            }
            if let Some(positions) = positions.as_ref() {
                self.row_window.observe(positions, self.split.path())?;
                if let Some(end) = self.row_window.end_row_position()
                    && end > self.split.file_record_count() as u64
                {
                    return Err(corrupt(format!(
                        "iceberg data file {} produced absolute row position {} beyond its {} records",
                        self.split.path(),
                        end - 1,
                        self.split.file_record_count()
                    )));
                }
            }

            let facts = IcebergSplitFacts {
                path: self.split.path(),
                partition_data_json: self.split.partition_data_json(),
                file_first_row_id: self.split.file_first_row_id(),
                data_sequence_number: self.split.data_sequence_number(),
            };
            let columns = binding.materialize(&file_batch.batch, positions.as_ref(), &facts)?;
            let rows = file_batch.batch.num_rows();
            let page_batch = RecordBatch::try_new_with_options(
                Arc::clone(page_schema),
                columns.clone(),
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(rows)),
            )
            .map_err(|error| {
                corrupt(format!(
                    "iceberg page assembly for {} failed: {error}",
                    self.split.path()
                ))
            })?;

            // Deletes and the remaining predicate both judge the complete page,
            // hidden suffix included, before the suffix is dropped.
            let mut keep = if self.delete_filter.is_empty() {
                None
            } else {
                let positions = positions.as_ref().ok_or_else(|| {
                    corrupt(format!(
                        "iceberg deletes for {} need absolute row positions",
                        self.split.path()
                    ))
                })?;
                Some(self.delete_filter.evaluate(&page_batch, positions)?)
            };
            for check in checks.iter() {
                let mask = evaluate_domain(
                    page_batch.column(check.channel),
                    &check.domain,
                    self.split.path(),
                )?;
                keep = Some(match keep {
                    None => mask,
                    Some(previous) => arrow::compute::and(&previous, &mask).map_err(|error| {
                        corrupt(format!("iceberg row predicate conjunction failed: {error}"))
                    })?,
                });
            }

            let mut page = SourcePage::try_new(rows, columns)?;
            if let Some(keep) = keep {
                let selected = surviving_positions(&keep);
                if selected.len() != rows {
                    page.select_positions(&selected)?;
                }
            }
            page.truncate_channels(self.prefix_len)?;
            if page.position_count() == 0 {
                // Everything in this batch was deleted or filtered out. The
                // split is not finished, so the next row group is read rather
                // than reporting an empty page as if it were data.
                continue;
            }
            self.completed_positions = self
                .completed_positions
                .saturating_add(page.position_count() as u64);
            return Ok(Some(page));
        }
    }
}

fn surviving_positions(keep: &BooleanArray) -> Vec<u32> {
    (0..keep.len())
        .filter(|row| !keep.is_null(*row) && keep.value(*row))
        .map(|row| row as u32)
        .collect()
}

/// Prove one column domain row by row.
fn evaluate_domain(
    column: &ArrayRef,
    domain: &Domain,
    path: &str,
) -> Result<BooleanArray, ConnectorError> {
    let null_allowed = domain.null_allowed();
    let values = domain.values();
    let mut keep = Vec::with_capacity(column.len());
    for row in 0..column.len() {
        if column.is_null(row) {
            keep.push(null_allowed);
            continue;
        }
        let value = connector_value_at(column, domain.value_type(), row, path)?;
        keep.push(values.contains_value(&value)?);
    }
    Ok(BooleanArray::from(keep))
}

/// Read one exactly typed value out of a physical column.
///
/// The domain's own type is the authority: a column whose carrier cannot
/// produce that type is a binding failure, never a coercion opportunity.
fn connector_value_at(
    column: &ArrayRef,
    value_type: ConnectorValueType,
    row: usize,
    path: &str,
) -> Result<ConnectorValue, ConnectorError> {
    fn downcast<'a, T: 'static>(column: &'a ArrayRef, path: &str) -> Result<&'a T, ConnectorError> {
        column.as_any().downcast_ref::<T>().ok_or_else(|| {
            corrupt(format!(
                "iceberg predicate column of {path} has carrier {:?}, which cannot be compared",
                column.data_type()
            ))
        })
    }

    Ok(match value_type {
        ConnectorValueType::Boolean => {
            ConnectorValue::Boolean(downcast::<BooleanArray>(column, path)?.value(row))
        }
        ConnectorValueType::Integer => {
            ConnectorValue::Integer(downcast::<Int32Array>(column, path)?.value(row))
        }
        ConnectorValueType::BigInt => {
            ConnectorValue::BigInt(downcast::<Int64Array>(column, path)?.value(row))
        }
        ConnectorValueType::Real => {
            ConnectorValue::Real(downcast::<Float32Array>(column, path)?.value(row))
        }
        ConnectorValueType::Double => {
            ConnectorValue::Double(downcast::<Float64Array>(column, path)?.value(row))
        }
        ConnectorValueType::Decimal { precision, scale } => ConnectorValue::Decimal {
            unscaled: downcast::<Decimal128Array>(column, path)?.value(row),
            precision,
            scale,
        },
        ConnectorValueType::Date => {
            ConnectorValue::Date(downcast::<Date32Array>(column, path)?.value(row))
        }
        ConnectorValueType::TimeMicros => {
            ConnectorValue::TimeMicros(downcast::<Time64MicrosecondArray>(column, path)?.value(row))
        }
        ConnectorValueType::TimestampMicros => ConnectorValue::TimestampMicros(
            downcast::<TimestampMicrosecondArray>(column, path)?.value(row),
        ),
        ConnectorValueType::TimestampTzMicros => ConnectorValue::TimestampTzMicros(
            downcast::<TimestampMicrosecondArray>(column, path)?.value(row),
        ),
        ConnectorValueType::TimestampNanos => ConnectorValue::TimestampNanos(
            downcast::<TimestampNanosecondArray>(column, path)?.value(row),
        ),
        ConnectorValueType::TimestampTzNanos => ConnectorValue::TimestampTzNanos(
            downcast::<TimestampNanosecondArray>(column, path)?.value(row),
        ),
        ConnectorValueType::Varchar => match column.data_type() {
            arrow::datatypes::DataType::LargeUtf8 => ConnectorValue::Varchar(Arc::from(
                downcast::<LargeStringArray>(column, path)?.value(row),
            )),
            _ => ConnectorValue::Varchar(Arc::from(
                downcast::<StringArray>(column, path)?.value(row),
            )),
        },
        ConnectorValueType::Varbinary => match column.data_type() {
            arrow::datatypes::DataType::LargeBinary => ConnectorValue::Varbinary(Arc::from(
                downcast::<LargeBinaryArray>(column, path)?.value(row),
            )),
            _ => ConnectorValue::Varbinary(Arc::from(
                downcast::<BinaryArray>(column, path)?.value(row),
            )),
        },
        ConnectorValueType::Uuid => {
            let bytes = downcast::<FixedSizeBinaryArray>(column, path)?.value(row);
            ConnectorValue::Uuid(<[u8; 16]>::try_from(bytes).map_err(|_| {
                corrupt(format!(
                    "iceberg uuid column of {path} is not sixteen bytes wide"
                ))
            })?)
        }
        ConnectorValueType::Fixed { .. } => ConnectorValue::Fixed(Arc::from(
            downcast::<FixedSizeBinaryArray>(column, path)?.value(row),
        )),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use novarocks_fs::{
        FileCancellation, FileIoRuntime, FileTaskSpawner, FsAccessResolver, TokioFileIoRuntime,
        TokioFileTaskSpawner,
    };
    use novarocks_spi::connector::ConnectorErrorKind;
    use novarocks_spi::connector::read_stack::{SplitWeight, TupleDomain};
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
    use parquet::file::properties::WriterProperties;

    use super::*;
    use crate::iceberg::spec::{
        NestedField, PrimitiveType, Schema as IcebergSchema, Transform, Type,
    };
    use crate::typed_read::split::{
        IcebergDeleteFile, IcebergDeleteFileContent, IcebergDeleteFileParams, IcebergSplitParams,
    };
    use crate::typed_read::table_handle::{IcebergTableHandle, IcebergTableHandleParams};

    const ROWS_PER_GROUP: usize = 4;

    fn tokio_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Runtime::new().expect("build Tokio runtime")
    }

    fn read_context(runtime: &tokio::runtime::Runtime) -> FileReadContext {
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        FileReadContext {
            cancellation: FileCancellation::new(),
            deadline: Some(Instant::now() + Duration::from_secs(60)),
            runtime: file_runtime,
            task_spawner,
        }
    }

    fn read_binding(runtime: &tokio::runtime::Runtime) -> IcebergReadBinding {
        let file_runtime: Arc<dyn FileIoRuntime> =
            Arc::new(TokioFileIoRuntime::new(runtime.handle().clone()));
        let task_spawner: Arc<dyn FileTaskSpawner> =
            Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone()));
        IcebergReadBinding::new(None, FsAccessResolver::new(), file_runtime, task_spawner)
    }

    fn iceberg_schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("frozen table schema")
    }

    fn arrow_file_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false).with_metadata(
                [(PARQUET_FIELD_ID_META_KEY.to_owned(), "1".to_owned())]
                    .into_iter()
                    .collect(),
            ),
            Field::new("region", DataType::Utf8, true).with_metadata(
                [(PARQUET_FIELD_ID_META_KEY.to_owned(), "2".to_owned())]
                    .into_iter()
                    .collect(),
            ),
        ]))
    }

    /// Write `groups * ROWS_PER_GROUP` rows into that many Parquet row groups.
    fn write_data_file(path: &Path, groups: usize) -> u64 {
        let schema = arrow_file_schema();
        let file = fs::File::create(path).expect("create data file");
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROWS_PER_GROUP))
            .build();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
            .expect("create parquet writer");
        for group in 0..groups {
            let base = (group * ROWS_PER_GROUP) as i64;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(
                        (0..ROWS_PER_GROUP as i64)
                            .map(|row| base + row)
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        (0..ROWS_PER_GROUP)
                            .map(|row| format!("r{}", base as usize + row))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("build data batch");
            writer.write(&batch).expect("write data batch");
            writer.flush().expect("close row group");
        }
        writer.close().expect("close parquet writer");
        fs::metadata(path).expect("stat data file").len()
    }

    /// The byte offset at which each row group's data starts.
    fn row_group_offsets(path: &Path) -> Vec<u64> {
        let file = fs::File::open(path).expect("open data file");
        let reader =
            parquet::file::reader::SerializedFileReader::new(file).expect("open parquet footer");
        parquet::file::reader::FileReader::metadata(&reader)
            .row_groups()
            .iter()
            .map(|group| {
                let column = group.column(0);
                let offset = column
                    .dictionary_page_offset()
                    .unwrap_or_else(|| column.data_page_offset())
                    .min(column.data_page_offset());
                u64::try_from(offset).expect("nonnegative row group offset")
            })
            .collect()
    }

    fn table_handle(schema: &IcebergSchema, partitioned: bool) -> IcebergTableHandle {
        let mut partition_spec_jsons = std::collections::BTreeMap::new();
        let spec = if partitioned {
            crate::iceberg::spec::PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .add_partition_field("region", "region", Transform::Identity)
                .expect("identity partition field")
                .build()
                .expect("partition spec")
        } else {
            crate::iceberg::spec::PartitionSpec::builder(schema.clone())
                .with_spec_id(0)
                .build()
                .expect("unpartitioned spec")
        };
        partition_spec_jsons.insert(
            0,
            serde_json::to_string(&spec).expect("serialize partition spec"),
        );
        IcebergTableHandle::try_new(IcebergTableHandleParams {
            schema_table_name: novarocks_spi::connector::read_stack::SchemaTableName::try_new(
                "sales", "orders",
            )
            .expect("schema table name"),
            snapshot_id: Some(11),
            table_schema_json: serde_json::to_string(schema).expect("serialize schema"),
            spec_id: Some(0),
            partition_spec_jsons,
            format_version: 2,
            unenforced_predicate: TupleDomain::all(),
            enforced_predicate: TupleDomain::all(),
            limit: None,
            projected_columns: Default::default(),
            name_mapping_json: None,
            table_location: "/tmp/iceberg/orders".to_owned(),
            storage_properties: Default::default(),
        })
        .expect("table handle")
    }

    struct SplitOptions {
        start: i64,
        length: i64,
        file_size: i64,
        file_record_count: i64,
        format: IcebergFileFormat,
        partition_data_json: String,
        deletes: Vec<IcebergDeleteFile>,
        first_row_id: Option<i64>,
        decryption: Option<ParquetFileDecryptionData>,
    }

    impl SplitOptions {
        fn whole_file(file_size: u64, records: i64) -> Self {
            Self {
                start: 0,
                length: file_size as i64,
                file_size: file_size as i64,
                file_record_count: records,
                format: IcebergFileFormat::Parquet,
                partition_data_json: "{}".to_owned(),
                deletes: Vec::new(),
                first_row_id: None,
                decryption: None,
            }
        }
    }

    fn build_split(name: &str, options: SplitOptions) -> IcebergSplit {
        IcebergSplit::try_new(IcebergSplitParams {
            path: name.to_owned(),
            start: options.start,
            length: options.length,
            file_size: options.file_size,
            file_record_count: options.file_record_count,
            file_format: options.format,
            partition_spec_id: 0,
            partition_data_json: options.partition_data_json,
            deletes: options.deletes,
            file_statistics_domain: TupleDomain::all(),
            data_sequence_number: Some(3),
            file_first_row_id: options.first_row_id,
            decryption_data: options.decryption,
            split_weight: SplitWeight::STANDARD,
            affinity_key: None,
        })
        .expect("split")
    }

    struct Harness {
        _runtime: tokio::runtime::Runtime,
        _directory: tempfile::TempDir,
        binding: IcebergReadBinding,
        context: FileReadContext,
        footers: Arc<ParquetFooterCache>,
        delete_manager: Arc<DeleteManager>,
        file_size: u64,
        offsets: Vec<u64>,
        file_name: String,
    }

    fn harness(groups: usize) -> Harness {
        let runtime = tokio_runtime();
        let directory = tempfile::tempdir().expect("temporary directory");
        let data_path = directory.path().join("data.parquet");
        let file_size = write_data_file(&data_path, groups);
        let offsets = row_group_offsets(&data_path);
        let context = read_context(&runtime);
        let access_binding = read_binding(&runtime);
        let delete_manager = Arc::new(DeleteManager::new(access_binding.clone(), context.clone()));
        Harness {
            _runtime: runtime,
            _directory: directory,
            binding: access_binding,
            context,
            footers: Arc::new(ParquetFooterCache::new()),
            delete_manager,
            file_size,
            offsets,
            file_name: data_path.to_string_lossy().to_string(),
        }
    }

    impl Harness {
        fn page_source(
            &self,
            split: &IcebergSplit,
            handle: &IcebergTableHandle,
            columns: &[IcebergColumnHandle],
        ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError> {
            create_iceberg_page_source(IcebergPageSourceRequest {
                table_handle: handle,
                split,
                columns,
                delete_manager: Arc::clone(&self.delete_manager),
                footers: Arc::clone(&self.footers),
                access_binding: self.binding.clone(),
                context: self.context.clone(),
                budget: FileReadBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero"),
                    max_bytes: NonZeroUsize::new(8 * 1024 * 1024).expect("nonzero"),
                },
                reader_options: FileReaderOptions::default(),
                dynamic_filter: DynamicFilterObservation::complete_all(),
            })
        }
    }

    fn drain_ids(source: &mut Box<dyn ConnectorPageSource>) -> Vec<i64> {
        let mut ids = Vec::new();
        while !source.is_finished() {
            let Some(page) = source.next_source_page().expect("page") else {
                continue;
            };
            let (rows, columns) = page.into_columns().expect("materialize");
            assert_eq!(columns[0].len(), rows);
            let values = columns[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 ids");
            ids.extend(values.values().iter().copied());
        }
        ids
    }

    fn id_column(schema: &IcebergSchema) -> Vec<IcebergColumnHandle> {
        vec![IcebergColumnHandle::base_column_of(schema, 1).expect("id handle")]
    }

    #[test]
    fn a_whole_file_split_reads_every_row_with_absolute_positions() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = build_split(
            &harness.file_name,
            SplitOptions::whole_file(harness.file_size, (3 * ROWS_PER_GROUP) as i64),
        );
        let mut source = harness
            .page_source(&split, &handle, &id_column(&schema))
            .expect("page source");
        let ids = drain_ids(&mut source);
        assert_eq!(ids, (0..(3 * ROWS_PER_GROUP) as i64).collect::<Vec<_>>());
        assert!(source.is_finished());
    }

    #[test]
    fn byte_range_selection_takes_a_row_group_at_the_lower_bound_and_not_at_the_upper() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let records = (3 * ROWS_PER_GROUP) as i64;

        // A range that starts exactly on the second row group's first byte and
        // ends exactly on the third's takes only the second: the range is
        // half-open.
        let start = harness.offsets[1];
        let end = harness.offsets[2];
        let split = build_split(
            &harness.file_name,
            SplitOptions {
                start: start as i64,
                length: (end - start) as i64,
                file_size: harness.file_size as i64,
                file_record_count: records,
                ..SplitOptions::whole_file(harness.file_size, records)
            },
        );
        let mut source = harness
            .page_source(&split, &handle, &id_column(&schema))
            .expect("page source");
        let ids = drain_ids(&mut source);
        assert_eq!(
            ids,
            (ROWS_PER_GROUP as i64..(2 * ROWS_PER_GROUP) as i64).collect::<Vec<_>>(),
            "the row group starting at the exclusive upper bound must not be read"
        );
    }

    #[test]
    fn absolute_row_positions_survive_row_group_pruning() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let records = (3 * ROWS_PER_GROUP) as i64;
        let start = harness.offsets[2];
        let split = build_split(
            &harness.file_name,
            SplitOptions {
                start: start as i64,
                length: (harness.file_size - start) as i64,
                file_size: harness.file_size as i64,
                file_record_count: records,
                first_row_id: Some(1_000),
                ..SplitOptions::whole_file(harness.file_size, records)
            },
        );
        // `$row_id` is `first_row_id + absolute position`, so it proves the
        // positions were not renumbered from the start of the split.
        let row_id = IcebergColumnHandle::try_new(
            crate::typed_read::column_handle::IcebergColumnHandleParams {
                base_column_identity: crate::typed_read::column_handle::ColumnIdentity::try_new(
                    crate::row_lineage_synth::ICEBERG_RESERVED_FIELD_ID_ROW_ID,
                    "$row_id",
                    crate::typed_read::column_handle::ColumnIdentityCategory::Primitive,
                    Vec::new(),
                )
                .expect("identity"),
                base_type_json: "\"long\"".to_owned(),
                field_id_path: Vec::new(),
                type_json: "\"long\"".to_owned(),
                nullable: false,
                comment: None,
            },
        )
        .expect("row id handle");
        let mut source = harness
            .page_source(&split, &handle, &[row_id])
            .expect("page source");
        let ids = drain_ids(&mut source);
        assert_eq!(
            ids,
            (1_000 + 2 * ROWS_PER_GROUP as i64..1_000 + 3 * ROWS_PER_GROUP as i64)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn none_is_not_end_of_stream_and_close_is_idempotent() {
        let harness = harness(1);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = build_split(
            &harness.file_name,
            SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64),
        );
        let mut source = harness
            .page_source(&split, &handle, &id_column(&schema))
            .expect("page source");

        assert!(!source.is_finished(), "a fresh page source is not finished");
        let first = source.next_source_page().expect("first page");
        assert!(first.is_some());
        assert!(
            !source.is_finished(),
            "a produced page never terminates the source on its own"
        );
        let second = source.next_source_page().expect("second call");
        assert!(second.is_none(), "the reader is drained");
        assert!(source.is_finished(), "only is_finished is terminal");

        source.close().expect("close");
        source.close().expect("close is idempotent");
        assert!(source.next_source_page().expect("after close").is_none());
    }

    #[test]
    fn a_zero_column_scan_still_counts_its_rows() {
        let harness = harness(2);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let split = build_split(
            &harness.file_name,
            SplitOptions::whole_file(harness.file_size, (2 * ROWS_PER_GROUP) as i64),
        );
        let mut source = harness
            .page_source(&split, &handle, &[])
            .expect("page source");
        let mut positions = 0usize;
        while !source.is_finished() {
            let Some(page) = source.next_source_page().expect("page") else {
                continue;
            };
            assert_eq!(page.channel_count(), 0, "a zero-column page is legal");
            positions += page.position_count();
        }
        assert_eq!(positions, 2 * ROWS_PER_GROUP);
    }

    #[test]
    fn the_partition_only_fast_path_never_opens_the_data_file() {
        let harness = harness(2);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, true);
        let records = (2 * ROWS_PER_GROUP) as i64;
        let split = build_split(
            "s3://bucket/this/file/does/not/exist.parquet",
            SplitOptions {
                partition_data_json: "{\"1000\":\"emea\"}".to_owned(),
                ..SplitOptions::whole_file(harness.file_size, records)
            },
        );
        let region = IcebergColumnHandle::base_column_of(&schema, 2).expect("region handle");
        let mut source = harness
            .page_source(&split, &handle, &[region])
            .expect("page source");

        let mut positions = 0usize;
        while !source.is_finished() {
            let Some(mut page) = source.next_source_page().expect("page") else {
                continue;
            };
            let column = page.block(0).expect("partition constant").clone();
            let regions = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 constant");
            for row in 0..regions.len() {
                assert_eq!(regions.value(row), "emea");
            }
            positions += page.position_count();
        }
        assert_eq!(
            positions as i64, records,
            "the fast path must account for every record of the file"
        );
        assert!(
            harness.footers.is_empty().expect("footer cache"),
            "the fast path reads no footer at all"
        );
    }

    #[test]
    fn the_partition_only_fast_path_may_emit_zero_column_pages() {
        let harness = harness(1);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, true);
        let split = build_split(
            "s3://bucket/absent.parquet",
            SplitOptions {
                partition_data_json: "{\"1000\":\"emea\"}".to_owned(),
                ..SplitOptions::whole_file(harness.file_size, 7)
            },
        );
        let mut source = harness
            .page_source(&split, &handle, &[])
            .expect("page source");
        let page = source
            .next_source_page()
            .expect("page")
            .expect("a zero-column page is still a page");
        assert_eq!(page.channel_count(), 0);
        assert_eq!(page.position_count(), 7);
        assert!(source.is_finished());
    }

    #[test]
    fn orc_and_avro_are_rejected_at_page_source_admission() {
        let harness = harness(1);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        for format in [IcebergFileFormat::Orc, IcebergFileFormat::Avro] {
            let split = build_split(
                &harness.file_name,
                SplitOptions {
                    format,
                    ..SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64)
                },
            );
            let error = harness
                .page_source(&split, &handle, &id_column(&schema))
                .err()
                .expect("only parquet is implemented");
            assert_eq!(error.kind(), ConnectorErrorKind::Unsupported, "{format:?}");
        }
    }

    #[test]
    fn decryption_material_is_rejected_without_leaking_it() {
        let harness = harness(1);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let secret = b"super-secret-key-metadata".to_vec();
        let split = build_split(
            &harness.file_name,
            SplitOptions {
                decryption: Some(
                    ParquetFileDecryptionData::try_new(secret.clone(), Vec::new())
                        .expect("decryption material"),
                ),
                ..SplitOptions::whole_file(harness.file_size, ROWS_PER_GROUP as i64)
            },
        );
        let error = harness
            .page_source(&split, &handle, &id_column(&schema))
            .err()
            .expect("modular encryption is not implemented");
        assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
        let rendered = format!("{} {:?}", error.message(), error);
        assert!(
            !rendered.contains("super-secret"),
            "key material must never reach a message: {rendered}"
        );
        // The redacted `Debug` of the material itself is the same guarantee.
        let material =
            ParquetFileDecryptionData::try_new(secret, Vec::new()).expect("decryption material");
        assert!(!format!("{material:?}").contains("super-secret"));
    }

    #[test]
    fn a_delete_closure_adds_a_hidden_suffix_that_is_truncated_after_evaluation() {
        let harness = harness(2);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let records = (2 * ROWS_PER_GROUP) as i64;

        // An equality delete on `region` needs `region` in the page even though
        // the scan only asked for `id`.
        let delete_path = std::path::Path::new(&harness.file_name)
            .parent()
            .expect("data directory")
            .join("eq-delete.parquet");
        write_equality_delete(&delete_path, &["r2", "r5"]);
        let delete = IcebergDeleteFile::try_new(IcebergDeleteFileParams {
            content: IcebergDeleteFileContent::EqualityDeletes,
            path: delete_path.to_string_lossy().to_string(),
            format: IcebergFileFormat::Parquet,
            record_count: 2,
            file_size_in_bytes: fs::metadata(&delete_path).expect("stat").len() as i64,
            equality_field_ids: vec![2],
            row_position_lower_bound: None,
            row_position_upper_bound: None,
            data_sequence_number: 9,
            content_offset: None,
            content_size_in_bytes: None,
            decryption_data: None,
        })
        .expect("delete descriptor");

        let split = build_split(
            &harness.file_name,
            SplitOptions {
                deletes: vec![delete],
                ..SplitOptions::whole_file(harness.file_size, records)
            },
        );
        let mut source = harness
            .page_source(&split, &handle, &id_column(&schema))
            .expect("page source");

        let mut ids = Vec::new();
        while !source.is_finished() {
            let Some(page) = source.next_source_page().expect("page") else {
                continue;
            };
            assert_eq!(
                page.channel_count(),
                1,
                "the hidden delete suffix must be dropped before the page leaves"
            );
            let (_, columns) = page.into_columns().expect("materialize");
            let values = columns[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 ids");
            ids.extend(values.values().iter().copied());
        }
        assert_eq!(ids, vec![0, 1, 3, 4, 6, 7]);
    }

    fn write_equality_delete(path: &Path, regions: &[&str]) {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("region", DataType::Utf8, true).with_metadata(
                [(PARQUET_FIELD_ID_META_KEY.to_owned(), "2".to_owned())]
                    .into_iter()
                    .collect(),
            ),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(regions.to_vec()))],
        )
        .expect("build equality delete batch");
        let file = fs::File::create(path).expect("create delete file");
        let mut writer = ArrowWriter::try_new(file, schema, None).expect("create parquet writer");
        writer.write(&batch).expect("write delete batch");
        writer.close().expect("close parquet writer");
    }

    #[test]
    fn a_bounded_split_records_its_half_open_row_position_window() {
        let harness = harness(3);
        let schema = iceberg_schema();
        let handle = table_handle(&schema, false);
        let records = (3 * ROWS_PER_GROUP) as i64;
        let start = harness.offsets[1];
        let end = harness.offsets[2];
        let split = build_split(
            &harness.file_name,
            SplitOptions {
                start: start as i64,
                length: (end - start) as i64,
                file_size: harness.file_size as i64,
                file_record_count: records,
                ..SplitOptions::whole_file(harness.file_size, records)
            },
        );
        let mut window = ReaderPageSourceWithRowPositions::default();
        window
            .observe(
                &UInt64Array::from((4_u64..8).collect::<Vec<_>>()),
                "data.parquet",
            )
            .expect("observe");
        assert_eq!(window.start_row_position, Some(4));
        assert_eq!(window.end_row_position, Some(8));
        assert!(
            window
                .observe(&UInt64Array::from(vec![6_u64]), "data.parquet")
                .is_err(),
            "a revisited row position is corrupt"
        );

        let mut source = harness
            .page_source(&split, &handle, &id_column(&schema))
            .expect("page source");
        let _ = drain_ids(&mut source);
    }
}
