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

//! Iceberg's concrete write domain for the provider-neutral write stack.
//!
//! Three values live here, and each one owns its cross-field validation in its
//! constructor so an invalid value cannot be built at all:
//!
//! * [`IcebergCommitHandle`] is the frontend-only write session. It carries the
//!   sealed logical target set, the frozen table/ref/base facts, and the single
//!   terminal state a commit may reach.
//! * [`IcebergWriterHandle`] is one logical write recipe. It carries table
//!   format facts and — for a delete branch — *exact references* to the old
//!   delete artifacts a writer must re-read. It never carries a credential and
//!   never carries an already-read bulk object such as a materialized deletion
//!   vector.
//! * [`IcebergCommitFragment`] describes exactly one artifact: one written data
//!   file, one position-delete file, or one deletion vector. It is not a report
//!   document and carries no writer identity.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_spi::connector::write_stack::WriteTargetOrdinal;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorManagedPublicationEmptyInputDisposition,
    ConnectorManagedPublicationTechnique, ConnectorWriteAdmissionPurpose, ConnectorWriteIntent,
};
use parquet::basic::Compression;

use crate::commit::CommitOpKind;
use crate::commit::report::IcebergColumnStats;
use crate::commit::write_stack::old_delete::IcebergOldDeleteMergeTarget;
use crate::delete_file::IcebergFileFormat;
use crate::scan_model::IcebergSchemaDef;
use crate::write_descriptor::IcebergPartitionDescriptor;

pub(crate) fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message.into())
}

pub(crate) fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message.into())
}

/// Reject a value that is empty, carries a NUL byte, or embeds a credential.
///
/// A handle, a fragment, and an old-delete reference are all values the
/// frontend hands to a backend, so a location that smuggles a secret through a
/// URL userinfo or query parameter must never be constructible.
pub(crate) fn validate_location(subject: &str, value: &str) -> Result<(), ConnectorError> {
    if value.is_empty() {
        return Err(invalid(format!("Iceberg {subject} must not be empty")));
    }
    if value.contains('\0') {
        return Err(invalid(format!("Iceberg {subject} contains a NUL byte")));
    }
    if let Ok(url) = url::Url::parse(value) {
        if !url.username().is_empty() || url.password().is_some() {
            return Err(invalid(format!(
                "Iceberg {subject} must not embed credentials"
            )));
        }
        for (key, _) in url.query_pairs() {
            if matches!(
                key.to_ascii_lowercase().as_str(),
                "access_key"
                    | "access_key_id"
                    | "secret"
                    | "secret_key"
                    | "session_token"
                    | "token"
            ) {
                return Err(invalid(format!(
                    "Iceberg {subject} must not embed credentials"
                )));
            }
        }
    }
    Ok(())
}

/// Which physical branch one logical write target drives.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergWriteBranch {
    /// New data files, optionally carrying Iceberg row lineage.
    Data,
    /// Parquet position-delete files.
    PositionDelete,
    /// Puffin deletion vectors, one blob per referenced data file.
    DeletionVector,
}

impl IcebergWriteBranch {
    pub const fn writes_deletes(self) -> bool {
        matches!(self, Self::PositionDelete | Self::DeletionVector)
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Data => "data",
            Self::PositionDelete => "position-delete",
            Self::DeletionVector => "deletion-vector",
        }
    }
}

/// Every SQL-level write shape Iceberg supports, mapped onto one commit op.
///
/// This enum is provider-private on purpose: the neutral write stack never
/// learns that Iceberg distinguishes a dynamic partition overwrite from a
/// static one, or a copy-on-write mutation from a merge-on-read one.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum IcebergWriteFlavor {
    /// `INSERT INTO`.
    Append,
    /// `INSERT OVERWRITE` replacing the whole target.
    Overwrite,
    /// `INSERT OVERWRITE` replacing only the touched partitions.
    PartitionOverwrite,
    /// Merge-on-read row mutation staging Parquet position deletes.
    RowMutationPositionDelete,
    /// Merge-on-read row mutation staging Puffin deletion vectors.
    RowMutationDeletionVector,
    /// Copy-on-write row mutation rewriting whole data files.
    RowMutationCopyOnWrite,
    /// `CREATE TABLE AS SELECT` publishing into a staged table.
    StagedCreate,
    /// Materialized-view refresh publication.
    ManagedPublication,
    /// Distributed compaction / rewrite of a frozen file set.
    DistributedRewrite,
    /// Provider-owned maintenance such as `TRUNCATE`.
    TableMaintenance,
}

impl IcebergWriteFlavor {
    /// The single external commit op this flavor performs.
    ///
    /// The mapping is the one the provider already commits today: a
    /// deletion-vector mutation lands on `RowDeltaDvFromFiles` because a Puffin
    /// artifact makes the delta file-based, and a distributed rewrite lands on
    /// `SelectedRewrite` because its input file set is frozen at planning.
    pub const fn commit_op_kind(self) -> CommitOpKind {
        match self {
            Self::Append | Self::StagedCreate | Self::ManagedPublication => {
                CommitOpKind::FastAppend
            }
            Self::Overwrite => CommitOpKind::Overwrite,
            Self::PartitionOverwrite => CommitOpKind::OverwritePartitions,
            Self::RowMutationPositionDelete => CommitOpKind::RowDelta,
            Self::RowMutationDeletionVector => CommitOpKind::RowDeltaDvFromFiles,
            Self::RowMutationCopyOnWrite => CommitOpKind::CowUpdate,
            Self::DistributedRewrite => CommitOpKind::SelectedRewrite,
            Self::TableMaintenance => CommitOpKind::Truncate,
        }
    }

    /// The physical branches this flavor may seal as logical targets, in the
    /// exact order their ordinals are assigned.
    pub fn branches(self) -> &'static [IcebergWriteBranch] {
        match self {
            Self::Append
            | Self::Overwrite
            | Self::PartitionOverwrite
            | Self::RowMutationCopyOnWrite
            | Self::StagedCreate
            | Self::ManagedPublication
            | Self::DistributedRewrite
            | Self::TableMaintenance => &[IcebergWriteBranch::Data],
            Self::RowMutationPositionDelete => {
                &[IcebergWriteBranch::Data, IcebergWriteBranch::PositionDelete]
            }
            Self::RowMutationDeletionVector => {
                &[IcebergWriteBranch::Data, IcebergWriteBranch::DeletionVector]
            }
        }
    }

    /// Whether this flavor may reach `finish_write` with no fragment at all.
    ///
    /// Every flavor may: a zero-row `INSERT` still commits an empty snapshot,
    /// and a mutation matching no row still needs its session terminated. The
    /// method exists to make that decision explicit rather than incidental.
    pub const fn accepts_empty_prepared_set(self) -> bool {
        true
    }

    /// Whether one sealed session may seal the same physical branch more than
    /// once.
    ///
    /// Only a distributed rewrite may. Its logical targets are all data
    /// branches — one per frozen rewrite group — and the ordinal, not the
    /// branch, is what tells the group apart. Every other flavor keeps the
    /// stricter rule, because a repeated delete branch would make the old-delete
    /// merge owner ambiguous.
    pub const fn seals_one_target_per_branch(self) -> bool {
        !matches!(self, Self::DistributedRewrite)
    }

    /// Whether this flavor's write must be serialized behind the distributed
    /// external write fence.
    ///
    /// A distributed rewrite must not be: it is arbitrated by the ordinary
    /// Iceberg base-state compare and swap that `dispatch_commit` already
    /// performs, and taking the fence would serialize it against ordinary DML
    /// it does not conflict with. Every other flavor keeps the fence, because a
    /// DML write that skipped it would lose that protection.
    pub const fn requires_external_write_fence(self) -> bool {
        !matches!(self, Self::DistributedRewrite)
    }
}

/// The publication facts one managed write session keeps.
///
/// Deliberately *not* the publication: the `LakePublicationId` that names it
/// stays in the frontend layer that owns it. Nothing here reaches a writer
/// recipe, a commit fragment, or a backend — the provider needs only the
/// technique it must commit as, and what an empty input means.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IcebergManagedPublicationFacts {
    technique: ConnectorManagedPublicationTechnique,
    empty_input: ConnectorManagedPublicationEmptyInputDisposition,
}

impl IcebergManagedPublicationFacts {
    pub const fn new(
        technique: ConnectorManagedPublicationTechnique,
        empty_input: ConnectorManagedPublicationEmptyInputDisposition,
    ) -> Self {
        Self {
            technique,
            empty_input,
        }
    }

    pub const fn technique(self) -> ConnectorManagedPublicationTechnique {
        self.technique
    }

    pub const fn empty_input(self) -> ConnectorManagedPublicationEmptyInputDisposition {
        self.empty_input
    }

    /// The single external commit op a publication of this technique performs.
    ///
    /// A full refresh republishes the whole target, so it replaces what the ref
    /// already holds; an incremental refresh adds to it. Committing a full
    /// refresh as an append would leave the superseded rows live.
    pub const fn commit_op_kind(self) -> CommitOpKind {
        match self.technique {
            ConnectorManagedPublicationTechnique::Full => CommitOpKind::Overwrite,
            ConnectorManagedPublicationTechnique::Incremental => CommitOpKind::FastAppend,
        }
    }

    /// The neutral write intent a publication of this technique is admitted
    /// as.
    pub const fn connector_intent(self) -> ConnectorWriteIntent {
        match self.technique {
            ConnectorManagedPublicationTechnique::Full => ConnectorWriteIntent::Overwrite,
            ConnectorManagedPublicationTechnique::Incremental => ConnectorWriteIntent::Append,
        }
    }
}

/// What an *empty* prepared write set means for one sealed session.
///
/// The decision stays inside the provider on purpose. A generic frontend that
/// guessed from "there were no fragments" would have to invent a policy for a
/// publication that asked to abort, and would turn a rewrite with nothing to
/// rewrite into a failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IcebergEmptyWriteDecision {
    /// Publish the empty write as an ordinary snapshot. This is what a zero-row
    /// `INSERT` has always done.
    Commit,
    /// Terminate the session with no external commit at all. The target ref
    /// keeps the head the session was frozen against.
    SkipExternalCommit,
}

impl IcebergWriteFlavor {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Overwrite => "overwrite",
            Self::PartitionOverwrite => "partition-overwrite",
            Self::RowMutationPositionDelete => "row-mutation-position-delete",
            Self::RowMutationDeletionVector => "row-mutation-deletion-vector",
            Self::RowMutationCopyOnWrite => "row-mutation-copy-on-write",
            Self::StagedCreate => "staged-create",
            Self::ManagedPublication => "managed-publication",
            Self::DistributedRewrite => "distributed-rewrite",
            Self::TableMaintenance => "table-maintenance",
        }
    }

    /// The neutral write intent this flavor presents to admission.
    pub const fn connector_intent(self) -> ConnectorWriteIntent {
        match self {
            Self::Append | Self::StagedCreate | Self::ManagedPublication => {
                ConnectorWriteIntent::Append
            }
            Self::Overwrite | Self::DistributedRewrite | Self::TableMaintenance => {
                ConnectorWriteIntent::Overwrite
            }
            Self::PartitionOverwrite => ConnectorWriteIntent::PartitionOverwrite,
            Self::RowMutationPositionDelete
            | Self::RowMutationDeletionVector
            | Self::RowMutationCopyOnWrite => ConnectorWriteIntent::RowDelta,
        }
    }
}

/// The exact table generation one write session is frozen against.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergWriteTableFacts {
    table_uuid: String,
    namespace: String,
    table_name: String,
    table_location: String,
    data_location: String,
    target_ref: String,
    base_snapshot_id: Option<i64>,
    base_sequence_number: i64,
    schema_id: i32,
    default_partition_spec_id: i32,
    format_version: u8,
}

impl IcebergWriteTableFacts {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        table_uuid: String,
        namespace: String,
        table_name: String,
        table_location: String,
        data_location: String,
        target_ref: String,
        base_snapshot_id: Option<i64>,
        base_sequence_number: i64,
        schema_id: i32,
        default_partition_spec_id: i32,
        format_version: u8,
    ) -> Result<Self, ConnectorError> {
        if table_uuid.is_empty() {
            return Err(invalid("Iceberg write session requires a table UUID"));
        }
        if namespace.is_empty() || table_name.is_empty() {
            return Err(invalid(
                "Iceberg write session requires a namespace and a table name",
            ));
        }
        if target_ref.is_empty() || target_ref.chars().any(char::is_control) {
            return Err(invalid(
                "Iceberg write session target ref must be non-empty and control-free",
            ));
        }
        validate_location("table location", &table_location)?;
        validate_location("data location", &data_location)?;
        if base_sequence_number < 0 {
            return Err(invalid(
                "Iceberg write session base sequence number must not be negative",
            ));
        }
        if !(1..=3).contains(&format_version) {
            return Err(invalid(
                "Iceberg write session format version must be 1, 2, or 3",
            ));
        }
        Ok(Self {
            table_uuid,
            namespace,
            table_name,
            table_location,
            data_location,
            target_ref,
            base_snapshot_id,
            base_sequence_number,
            schema_id,
            default_partition_spec_id,
            format_version,
        })
    }

    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }
    pub fn namespace(&self) -> &str {
        &self.namespace
    }
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
    pub fn table_location(&self) -> &str {
        &self.table_location
    }
    pub fn data_location(&self) -> &str {
        &self.data_location
    }
    pub fn target_ref(&self) -> &str {
        &self.target_ref
    }
    pub const fn base_snapshot_id(&self) -> Option<i64> {
        self.base_snapshot_id
    }
    pub const fn base_sequence_number(&self) -> i64 {
        self.base_sequence_number
    }
    pub const fn schema_id(&self) -> i32 {
        self.schema_id
    }
    pub const fn default_partition_spec_id(&self) -> i32 {
        self.default_partition_spec_id
    }
    pub const fn format_version(&self) -> u8 {
        self.format_version
    }
}

/// Physical output settings frozen for one logical writer.
#[derive(Clone, Debug)]
pub struct IcebergWriterOutput {
    file_format: IcebergFileFormat,
    compression: Compression,
    parquet_row_group_size_bytes: Option<u64>,
}

impl IcebergWriterOutput {
    pub fn try_new(
        file_format: IcebergFileFormat,
        compression: Compression,
        parquet_row_group_size_bytes: Option<u64>,
    ) -> Result<Self, ConnectorError> {
        if file_format == IcebergFileFormat::Unknown {
            return Err(invalid(
                "Iceberg writer output requires an exact file format",
            ));
        }
        if let Some(size) = parquet_row_group_size_bytes
            && (size == 0 || file_format != IcebergFileFormat::Parquet)
        {
            return Err(invalid(
                "Iceberg Parquet row group size requires a positive value on a Parquet writer",
            ));
        }
        Ok(Self {
            file_format,
            compression,
            parquet_row_group_size_bytes,
        })
    }

    pub const fn file_format(&self) -> IcebergFileFormat {
        self.file_format
    }
    pub const fn compression(&self) -> Compression {
        self.compression
    }
    pub const fn parquet_row_group_size_bytes(&self) -> Option<u64> {
        self.parquet_row_group_size_bytes
    }
}

/// The data-branch recipe: schema, partitioning, and row-lineage facts.
#[derive(Clone, Debug)]
pub struct IcebergDataBranchRecipe {
    input_schema: Option<IcebergSchemaDef>,
    partition_source_column_names: Vec<String>,
    partition_column_names: Vec<String>,
    transform_exprs: Vec<String>,
    row_lineage: bool,
}

impl IcebergDataBranchRecipe {
    pub fn try_new(
        input_schema: Option<IcebergSchemaDef>,
        partition_source_column_names: Vec<String>,
        partition_column_names: Vec<String>,
        transform_exprs: Vec<String>,
        row_lineage: bool,
    ) -> Result<Self, ConnectorError> {
        if partition_source_column_names.len() != partition_column_names.len()
            || partition_column_names.len() != transform_exprs.len()
        {
            return Err(invalid(
                "Iceberg data writer partition sources, names, and transforms must be parallel",
            ));
        }
        for transform in &transform_exprs {
            validate_location("partition transform", transform)?;
        }
        let unique = partition_column_names.iter().collect::<BTreeSet<_>>();
        if unique.len() != partition_column_names.len() {
            return Err(invalid(
                "Iceberg data writer repeats a partition column name",
            ));
        }
        Ok(Self {
            input_schema,
            partition_source_column_names,
            partition_column_names,
            transform_exprs,
            row_lineage,
        })
    }

    pub fn input_schema(&self) -> Option<&IcebergSchemaDef> {
        self.input_schema.as_ref()
    }
    pub fn partition_source_column_names(&self) -> &[String] {
        &self.partition_source_column_names
    }
    pub fn partition_column_names(&self) -> &[String] {
        &self.partition_column_names
    }
    pub fn transform_exprs(&self) -> &[String] {
        &self.transform_exprs
    }
    pub const fn row_lineage(&self) -> bool {
        self.row_lineage
    }
}

/// One logical write recipe, copied to every physical writer placement.
///
/// A delete branch carries `old_deletes`: exact, bounded *references* to the
/// artifacts a writer must re-read through its own query-leased storage. The
/// FE deliberately does not read them.
#[derive(Clone, Debug)]
pub struct IcebergWriterHandle {
    branch: IcebergWriteBranch,
    table: IcebergWriteTableFacts,
    output: IcebergWriterOutput,
    data: Option<IcebergDataBranchRecipe>,
    old_deletes: BTreeMap<String, IcebergOldDeleteMergeTarget>,
}

impl IcebergWriterHandle {
    /// Build the data branch's recipe.
    pub fn try_new_data(
        table: IcebergWriteTableFacts,
        output: IcebergWriterOutput,
        data: IcebergDataBranchRecipe,
    ) -> Result<Self, ConnectorError> {
        if output.file_format() != IcebergFileFormat::Parquet {
            return Err(invalid("Iceberg data writer must produce Parquet"));
        }
        Ok(Self {
            branch: IcebergWriteBranch::Data,
            table,
            output,
            data: Some(data),
            old_deletes: BTreeMap::new(),
        })
    }

    /// Build a delete branch's recipe from its frozen old-delete references.
    ///
    /// Every merge target must name the data file it keys, and a deletion
    /// vector branch must write Puffin while a position-delete branch must
    /// write Parquet. Both rules are structural, so neither can be bypassed by
    /// a caller assembling the parts by hand.
    pub fn try_new_delete(
        branch: IcebergWriteBranch,
        table: IcebergWriteTableFacts,
        output: IcebergWriterOutput,
        old_deletes: Vec<IcebergOldDeleteMergeTarget>,
    ) -> Result<Self, ConnectorError> {
        let expected_format = match branch {
            IcebergWriteBranch::PositionDelete => IcebergFileFormat::Parquet,
            IcebergWriteBranch::DeletionVector => IcebergFileFormat::Puffin,
            IcebergWriteBranch::Data => {
                return Err(invalid(
                    "Iceberg data branch cannot be built as a delete branch",
                ));
            }
        };
        if output.file_format() != expected_format {
            return Err(invalid(format!(
                "Iceberg {} branch must produce {expected_format:?}",
                branch.as_str()
            )));
        }
        let base_snapshot_id = table
            .base_snapshot_id()
            .ok_or_else(|| invalid("Iceberg row-level write requires a frozen target snapshot"))?;
        let mut frozen = BTreeMap::new();
        for target in old_deletes {
            if target.base_snapshot_id() != base_snapshot_id {
                return Err(invalid(
                    "Iceberg old-delete merge target does not name the session's frozen base snapshot",
                ));
            }
            if frozen
                .insert(target.data_file_path().to_string(), target)
                .is_some()
            {
                return Err(invalid(
                    "Iceberg delete writer handle repeats a referenced data file",
                ));
            }
        }
        Ok(Self {
            branch,
            table,
            output,
            data: None,
            old_deletes: frozen,
        })
    }

    pub const fn branch(&self) -> IcebergWriteBranch {
        self.branch
    }
    pub const fn table(&self) -> &IcebergWriteTableFacts {
        &self.table
    }
    pub const fn output(&self) -> &IcebergWriterOutput {
        &self.output
    }
    pub fn data(&self) -> Option<&IcebergDataBranchRecipe> {
        self.data.as_ref()
    }

    /// The exact old-delete artifacts a writer must re-read, keyed by the data
    /// file each one belongs to.
    pub const fn old_deletes(&self) -> &BTreeMap<String, IcebergOldDeleteMergeTarget> {
        &self.old_deletes
    }

    /// The data files this handle owns exclusively for the old-delete merge.
    pub fn owned_data_files(&self) -> impl Iterator<Item = &str> {
        self.old_deletes.keys().map(String::as_str)
    }
}

/// A partition, as one staged artifact reports it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IcebergArtifactPartition {
    partition_path: String,
    null_fingerprint: String,
    partition_spec_id: i32,
    descriptor: IcebergPartitionDescriptor,
}

impl IcebergArtifactPartition {
    pub fn try_new(
        partition_path: String,
        null_fingerprint: String,
        partition_spec_id: i32,
        descriptor: IcebergPartitionDescriptor,
    ) -> Result<Self, ConnectorError> {
        if partition_spec_id < 0 {
            return Err(invalid(
                "Iceberg artifact partition spec id must not be negative",
            ));
        }
        if partition_path.contains('\0') || null_fingerprint.contains('\0') {
            return Err(invalid(
                "Iceberg artifact partition path contains a NUL byte",
            ));
        }
        for (index, value) in descriptor.values.iter().enumerate() {
            match (value.is_null, value.datum_bytes.as_ref()) {
                (true, Some(_)) => {
                    return Err(corrupt(format!(
                        "Iceberg partition descriptor value {index} is null but carries a payload"
                    )));
                }
                (false, None) => {
                    return Err(corrupt(format!(
                        "Iceberg partition descriptor value {index} is non-null but has no payload"
                    )));
                }
                _ => {}
            }
        }
        Ok(Self {
            partition_path,
            null_fingerprint,
            partition_spec_id,
            descriptor,
        })
    }

    pub fn partition_path(&self) -> &str {
        &self.partition_path
    }
    pub fn null_fingerprint(&self) -> &str {
        &self.null_fingerprint
    }
    pub const fn partition_spec_id(&self) -> i32 {
        self.partition_spec_id
    }
    pub const fn descriptor(&self) -> &IcebergPartitionDescriptor {
        &self.descriptor
    }
}

/// The physical metrics one staged artifact reports.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct IcebergArtifactMetrics {
    record_count: u64,
    file_size_in_bytes: u64,
    split_offsets: Vec<i64>,
    column_stats: Option<IcebergColumnStats>,
}

impl IcebergArtifactMetrics {
    pub fn try_new(
        record_count: u64,
        file_size_in_bytes: u64,
        split_offsets: Vec<i64>,
        column_stats: Option<IcebergColumnStats>,
    ) -> Result<Self, ConnectorError> {
        if file_size_in_bytes == 0 {
            return Err(invalid(
                "Iceberg staged artifact must report a positive file size",
            ));
        }
        i64::try_from(record_count)
            .map_err(|_| invalid("Iceberg staged artifact record count overflows i64"))?;
        let size = i64::try_from(file_size_in_bytes)
            .map_err(|_| invalid("Iceberg staged artifact file size overflows i64"))?;
        let mut previous: Option<i64> = None;
        for offset in &split_offsets {
            if *offset < 0 || *offset >= size {
                return Err(invalid(
                    "Iceberg staged artifact split offset falls outside the file",
                ));
            }
            if previous.is_some_and(|last| *offset <= last) {
                return Err(invalid(
                    "Iceberg staged artifact split offsets must strictly ascend",
                ));
            }
            previous = Some(*offset);
        }
        if let Some(stats) = &column_stats {
            for counts in [
                &stats.column_sizes,
                &stats.value_counts,
                &stats.null_value_counts,
                &stats.nan_value_counts,
            ] {
                if counts.values().any(|value| *value < 0) {
                    return Err(corrupt(
                        "Iceberg staged artifact column statistic count is negative",
                    ));
                }
            }
        }
        Ok(Self {
            record_count,
            file_size_in_bytes,
            split_offsets,
            column_stats,
        })
    }

    pub const fn record_count(&self) -> u64 {
        self.record_count
    }
    pub const fn file_size_in_bytes(&self) -> u64 {
        self.file_size_in_bytes
    }
    pub fn split_offsets(&self) -> &[i64] {
        &self.split_offsets
    }
    pub fn column_stats(&self) -> Option<&IcebergColumnStats> {
        self.column_stats.as_ref()
    }
}

/// The bounded byte range one Puffin blob occupies inside its file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IcebergContentRange {
    offset: i64,
    size_in_bytes: i64,
}

impl IcebergContentRange {
    pub fn try_new(offset: i64, size_in_bytes: i64) -> Result<Self, ConnectorError> {
        if offset < 0 {
            return Err(invalid("Iceberg content range offset must not be negative"));
        }
        if size_in_bytes <= 0 {
            return Err(invalid("Iceberg content range size must be positive"));
        }
        offset
            .checked_add(size_in_bytes)
            .ok_or_else(|| invalid("Iceberg content range end overflows i64"))?;
        Ok(Self {
            offset,
            size_in_bytes,
        })
    }

    pub const fn offset(&self) -> i64 {
        self.offset
    }
    pub const fn size_in_bytes(&self) -> i64 {
        self.size_in_bytes
    }
    pub const fn end(&self) -> i64 {
        self.offset + self.size_in_bytes
    }
}

/// One written data file.
#[derive(Clone, Debug)]
pub struct IcebergDataFileArtifact {
    path: String,
    file_format: IcebergFileFormat,
    partition: IcebergArtifactPartition,
    metrics: IcebergArtifactMetrics,
    first_row_id: Option<i64>,
}

impl IcebergDataFileArtifact {
    pub fn try_new(
        path: String,
        file_format: IcebergFileFormat,
        partition: IcebergArtifactPartition,
        metrics: IcebergArtifactMetrics,
        first_row_id: Option<i64>,
    ) -> Result<Self, ConnectorError> {
        validate_location("staged data file", &path)?;
        if file_format != IcebergFileFormat::Parquet {
            return Err(invalid("Iceberg staged data file must be Parquet"));
        }
        if let Some(first_row_id) = first_row_id
            && first_row_id < 0
        {
            return Err(invalid(
                "Iceberg staged data file row lineage must not be negative",
            ));
        }
        Ok(Self {
            path,
            file_format,
            partition,
            metrics,
            first_row_id,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub const fn file_format(&self) -> IcebergFileFormat {
        self.file_format
    }
    pub const fn partition(&self) -> &IcebergArtifactPartition {
        &self.partition
    }
    pub const fn metrics(&self) -> &IcebergArtifactMetrics {
        &self.metrics
    }
    pub const fn first_row_id(&self) -> Option<i64> {
        self.first_row_id
    }
}

/// One written Parquet position-delete file.
#[derive(Clone, Debug)]
pub struct IcebergPositionDeleteFileArtifact {
    path: String,
    partition: IcebergArtifactPartition,
    metrics: IcebergArtifactMetrics,
    referenced_data_file: String,
    merged_old_references: Vec<String>,
}

impl IcebergPositionDeleteFileArtifact {
    pub fn try_new(
        path: String,
        partition: IcebergArtifactPartition,
        metrics: IcebergArtifactMetrics,
        referenced_data_file: String,
        merged_old_references: Vec<String>,
    ) -> Result<Self, ConnectorError> {
        validate_location("staged position-delete file", &path)?;
        validate_location("referenced data file", &referenced_data_file)?;
        if metrics.record_count() == 0 {
            return Err(invalid(
                "Iceberg staged position-delete file must delete at least one row",
            ));
        }
        validate_merged_references(&merged_old_references)?;
        Ok(Self {
            path,
            partition,
            metrics,
            referenced_data_file,
            merged_old_references,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub const fn partition(&self) -> &IcebergArtifactPartition {
        &self.partition
    }
    pub const fn metrics(&self) -> &IcebergArtifactMetrics {
        &self.metrics
    }
    pub fn referenced_data_file(&self) -> &str {
        &self.referenced_data_file
    }
    /// The exact old artifacts this file superseded, in sorted order.
    pub fn merged_old_references(&self) -> &[String] {
        &self.merged_old_references
    }
}

/// One written Puffin deletion vector.
#[derive(Clone, Debug)]
pub struct IcebergDeletionVectorArtifact {
    path: String,
    partition: IcebergArtifactPartition,
    metrics: IcebergArtifactMetrics,
    referenced_data_file: String,
    content_range: IcebergContentRange,
    cardinality: u64,
    merged_old_references: Vec<String>,
}

impl IcebergDeletionVectorArtifact {
    pub fn try_new(
        path: String,
        partition: IcebergArtifactPartition,
        metrics: IcebergArtifactMetrics,
        referenced_data_file: String,
        content_range: IcebergContentRange,
        cardinality: u64,
        merged_old_references: Vec<String>,
    ) -> Result<Self, ConnectorError> {
        validate_location("staged deletion vector", &path)?;
        validate_location("referenced data file", &referenced_data_file)?;
        if cardinality == 0 {
            return Err(invalid(
                "Iceberg staged deletion vector must delete at least one row",
            ));
        }
        if cardinality != metrics.record_count() {
            return Err(corrupt(
                "Iceberg staged deletion vector cardinality differs from its record count",
            ));
        }
        let size = i64::try_from(metrics.file_size_in_bytes())
            .map_err(|_| invalid("Iceberg staged deletion vector file size overflows i64"))?;
        if content_range.end() > size {
            return Err(corrupt(
                "Iceberg staged deletion vector blob range extends past its Puffin file",
            ));
        }
        validate_merged_references(&merged_old_references)?;
        Ok(Self {
            path,
            partition,
            metrics,
            referenced_data_file,
            content_range,
            cardinality,
            merged_old_references,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub const fn partition(&self) -> &IcebergArtifactPartition {
        &self.partition
    }
    pub const fn metrics(&self) -> &IcebergArtifactMetrics {
        &self.metrics
    }
    pub fn referenced_data_file(&self) -> &str {
        &self.referenced_data_file
    }
    pub const fn content_range(&self) -> IcebergContentRange {
        self.content_range
    }
    pub const fn cardinality(&self) -> u64 {
        self.cardinality
    }
    pub fn merged_old_references(&self) -> &[String] {
        &self.merged_old_references
    }
}

fn validate_merged_references(references: &[String]) -> Result<(), ConnectorError> {
    let mut previous: Option<&str> = None;
    for reference in references {
        validate_location("merged old delete reference", reference)?;
        if previous.is_some_and(|last| last >= reference.as_str()) {
            return Err(corrupt(
                "Iceberg merged old-delete references must be sorted and unique",
            ));
        }
        previous = Some(reference);
    }
    Ok(())
}

/// The one artifact a commit fragment describes.
#[derive(Clone, Debug)]
pub enum IcebergCommitArtifact {
    DataFile(IcebergDataFileArtifact),
    PositionDeleteFile(IcebergPositionDeleteFileArtifact),
    DeletionVector(IcebergDeletionVectorArtifact),
}

/// Exactly one staged Iceberg artifact.
///
/// A fragment carries no writer identity, no attempt id, and no aggregate
/// summary: those are properties of an execution, not of an artifact, and
/// putting them here would make two identical artifacts look different.
#[derive(Clone, Debug)]
pub struct IcebergCommitFragment {
    artifact: IcebergCommitArtifact,
}

impl IcebergCommitFragment {
    pub const fn new(artifact: IcebergCommitArtifact) -> Self {
        Self { artifact }
    }

    pub fn data_file(artifact: IcebergDataFileArtifact) -> Self {
        Self::new(IcebergCommitArtifact::DataFile(artifact))
    }

    pub fn position_delete_file(artifact: IcebergPositionDeleteFileArtifact) -> Self {
        Self::new(IcebergCommitArtifact::PositionDeleteFile(artifact))
    }

    pub fn deletion_vector(artifact: IcebergDeletionVectorArtifact) -> Self {
        Self::new(IcebergCommitArtifact::DeletionVector(artifact))
    }

    pub const fn artifact(&self) -> &IcebergCommitArtifact {
        &self.artifact
    }

    /// The staged path, which is unique across one prepared write set.
    pub fn path(&self) -> &str {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(file) => file.path(),
            IcebergCommitArtifact::PositionDeleteFile(file) => file.path(),
            IcebergCommitArtifact::DeletionVector(file) => file.path(),
        }
    }

    pub fn partition(&self) -> &IcebergArtifactPartition {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(file) => file.partition(),
            IcebergCommitArtifact::PositionDeleteFile(file) => file.partition(),
            IcebergCommitArtifact::DeletionVector(file) => file.partition(),
        }
    }

    pub fn metrics(&self) -> &IcebergArtifactMetrics {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(file) => file.metrics(),
            IcebergCommitArtifact::PositionDeleteFile(file) => file.metrics(),
            IcebergCommitArtifact::DeletionVector(file) => file.metrics(),
        }
    }

    /// The data file a delete artifact supersedes deletes for, if any.
    pub fn referenced_data_file(&self) -> Option<&str> {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(_) => None,
            IcebergCommitArtifact::PositionDeleteFile(file) => Some(file.referenced_data_file()),
            IcebergCommitArtifact::DeletionVector(file) => Some(file.referenced_data_file()),
        }
    }

    pub fn merged_old_references(&self) -> &[String] {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(_) => &[],
            IcebergCommitArtifact::PositionDeleteFile(file) => file.merged_old_references(),
            IcebergCommitArtifact::DeletionVector(file) => file.merged_old_references(),
        }
    }

    /// The branch that may legally produce this artifact.
    pub const fn branch(&self) -> IcebergWriteBranch {
        match &self.artifact {
            IcebergCommitArtifact::DataFile(_) => IcebergWriteBranch::Data,
            IcebergCommitArtifact::PositionDeleteFile(_) => IcebergWriteBranch::PositionDelete,
            IcebergCommitArtifact::DeletionVector(_) => IcebergWriteBranch::DeletionVector,
        }
    }
}

/// One sealed logical target inside a begin session.
///
/// A delete branch's map keys are every old data file routed to it, and each
/// value is the sorted set of old delete artifacts the session froze for that
/// file. An empty value means "this data file has no old deletes" — a decision,
/// not a read result.
#[derive(Clone, Debug)]
pub struct IcebergSealedWriteTarget {
    ordinal: WriteTargetOrdinal,
    branch: IcebergWriteBranch,
    owned_data_files: BTreeMap<String, Vec<String>>,
}

impl IcebergSealedWriteTarget {
    pub const fn new(
        ordinal: WriteTargetOrdinal,
        branch: IcebergWriteBranch,
        owned_data_files: BTreeMap<String, Vec<String>>,
    ) -> Self {
        Self {
            ordinal,
            branch,
            owned_data_files,
        }
    }

    pub const fn ordinal(&self) -> WriteTargetOrdinal {
        self.ordinal
    }
    pub const fn branch(&self) -> IcebergWriteBranch {
        self.branch
    }
    pub const fn owned_data_files(&self) -> &BTreeMap<String, Vec<String>> {
        &self.owned_data_files
    }
    pub fn data_files(&self) -> impl Iterator<Item = &String> {
        self.owned_data_files.keys()
    }
}

/// The frontend-only write session identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IcebergWriteSessionId([u8; 16]);

impl IcebergWriteSessionId {
    pub fn new() -> Self {
        Self(*uuid::Uuid::new_v4().as_bytes())
    }

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl Default for IcebergWriteSessionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for IcebergWriteSessionId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", uuid::Uuid::from_bytes(self.0))
    }
}

/// The frontend-only Iceberg write session.
///
/// It is never `Clone`: exactly one owner may finish, abort, or reconcile it.
/// Its terminal state lives behind a mutex because the neutral contract borrows
/// the handle on every terminal call.
#[derive(Debug)]
pub struct IcebergCommitHandle {
    session_id: IcebergWriteSessionId,
    table: IcebergWriteTableFacts,
    flavor: IcebergWriteFlavor,
    purpose: ConnectorWriteAdmissionPurpose,
    base_version_digest: Option<[u8; 32]>,
    publication: Option<IcebergManagedPublicationFacts>,
    targets: Vec<IcebergSealedWriteTarget>,
    delete_owner: BTreeMap<String, WriteTargetOrdinal>,
    state: std::sync::Mutex<IcebergWriteSessionState>,
}

/// The terminal state a write session may reach. It mirrors the provider's
/// existing commit verdicts exactly: a session never reports a commit it did
/// not observe, and an unknown outcome stays unknown until reconciliation
/// resolves it.
#[derive(Clone, Debug)]
pub enum IcebergWriteSessionState {
    /// No external effect has happened yet.
    Active,
    /// A commit is in flight. A second terminal call must not start another.
    Committing,
    /// The external commit is proven to have happened.
    KnownCommitted { snapshot_id: i64 },
    /// The external commit is proven not to have happened.
    KnownUncommitted { message: String },
    /// The external outcome is unknown and staged files were deliberately left
    /// in place for reconciliation.
    CommitUnknown {
        message: String,
        staging_dir: String,
    },
}

impl IcebergCommitHandle {
    /// Seal a begin session.
    ///
    /// Target ordinals must be dense from zero and each branch must appear at
    /// most once, because a duplicate branch would make the delete-owner route
    /// ambiguous.
    pub fn try_new(
        session_id: IcebergWriteSessionId,
        table: IcebergWriteTableFacts,
        flavor: IcebergWriteFlavor,
        purpose: ConnectorWriteAdmissionPurpose,
        base_version_digest: Option<[u8; 32]>,
        targets: Vec<IcebergSealedWriteTarget>,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_with_publication(
            session_id,
            table,
            flavor,
            purpose,
            base_version_digest,
            None,
            targets,
        )
    }

    /// Seal a begin session that also carries its managed publication facts.
    ///
    /// The facts are admissible only on the managed-publication flavor: a
    /// session that carried a technique it does not publish under would decide
    /// its commit op from a fact its own flavor contradicts.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new_with_publication(
        session_id: IcebergWriteSessionId,
        table: IcebergWriteTableFacts,
        flavor: IcebergWriteFlavor,
        purpose: ConnectorWriteAdmissionPurpose,
        base_version_digest: Option<[u8; 32]>,
        publication: Option<IcebergManagedPublicationFacts>,
        targets: Vec<IcebergSealedWriteTarget>,
    ) -> Result<Self, ConnectorError> {
        if publication.is_some() && flavor != IcebergWriteFlavor::ManagedPublication {
            return Err(invalid(format!(
                "Iceberg {} write session cannot carry managed publication facts",
                flavor.as_str()
            )));
        }
        let ordinals = targets
            .iter()
            .map(IcebergSealedWriteTarget::ordinal)
            .collect::<Vec<_>>();
        novarocks_spi::connector::write_stack::validate_dense_target_ordinals(&ordinals)?;
        let mut branches = BTreeSet::new();
        for target in &targets {
            if !branches.insert(target.branch()) && flavor.seals_one_target_per_branch() {
                return Err(invalid(
                    "Iceberg write session repeats a physical write branch",
                ));
            }
        }
        let allowed = flavor.branches().iter().copied().collect::<BTreeSet<_>>();
        if !branches.is_subset(&allowed) {
            return Err(invalid(format!(
                "Iceberg {} write session seals a branch the flavor does not own",
                flavor.as_str()
            )));
        }
        let delete_owner = super::planning::prove_unique_delete_owner(&targets)?;
        Ok(Self {
            session_id,
            table,
            flavor,
            purpose,
            base_version_digest,
            publication,
            targets,
            delete_owner,
            state: std::sync::Mutex::new(IcebergWriteSessionState::Active),
        })
    }

    pub const fn session_id(&self) -> IcebergWriteSessionId {
        self.session_id
    }
    pub const fn table(&self) -> &IcebergWriteTableFacts {
        &self.table
    }
    pub const fn flavor(&self) -> IcebergWriteFlavor {
        self.flavor
    }
    pub const fn purpose(&self) -> ConnectorWriteAdmissionPurpose {
        self.purpose
    }
    pub const fn base_version_digest(&self) -> Option<[u8; 32]> {
        self.base_version_digest
    }
    /// The managed publication facts, present exactly on a publication session.
    pub const fn publication(&self) -> Option<IcebergManagedPublicationFacts> {
        self.publication
    }
    pub fn targets(&self) -> &[IcebergSealedWriteTarget] {
        &self.targets
    }

    /// The single external commit op this session performs.
    ///
    /// A managed publication decides it from its technique — a full refresh
    /// replaces, an incremental one appends — and every other session decides
    /// it from its flavor alone.
    pub fn commit_op_kind(&self) -> CommitOpKind {
        match self.publication {
            Some(publication) => publication.commit_op_kind(),
            None => self.flavor.commit_op_kind(),
        }
    }

    /// Whether this session must be serialized behind the distributed external
    /// write fence.
    ///
    /// Read off the sealed session rather than re-derived, so the exemption a
    /// rewrite relies on is stated once and cannot drift from what the session
    /// actually is.
    pub const fn requires_external_write_fence(&self) -> bool {
        self.flavor.requires_external_write_fence()
    }

    /// What an empty prepared write set means for this session.
    ///
    /// Two sessions terminate without any external commit: a managed
    /// publication whose caller declared `AbortWithoutExternalCommit`, and a
    /// distributed rewrite that found nothing to rewrite. Neither is a failure,
    /// and neither is derivable from "there were no fragments" alone — the
    /// first depends on a disposition only the publication carries, and the
    /// second differs from a zero-row `INSERT`, which still publishes.
    pub fn empty_write_decision(&self) -> IcebergEmptyWriteDecision {
        if self.flavor == IcebergWriteFlavor::DistributedRewrite {
            return IcebergEmptyWriteDecision::SkipExternalCommit;
        }
        match self
            .publication
            .map(IcebergManagedPublicationFacts::empty_input)
        {
            Some(ConnectorManagedPublicationEmptyInputDisposition::AbortWithoutExternalCommit) => {
                IcebergEmptyWriteDecision::SkipExternalCommit
            }
            _ => IcebergEmptyWriteDecision::Commit,
        }
    }

    /// The proven route from an old data file routed to a delete branch to its
    /// single owning logical target. It is a superset of the files that
    /// actually need a merge, because a file with no old deletes still needs a
    /// unique writer.
    pub const fn delete_owner(&self) -> &BTreeMap<String, WriteTargetOrdinal> {
        &self.delete_owner
    }

    /// The frozen old-delete references, keyed by target and data file.
    ///
    /// `finish_write` uses this to prove each staged artifact superseded
    /// exactly the references the session froze.
    pub fn frozen_old_references(
        &self,
    ) -> BTreeMap<WriteTargetOrdinal, BTreeMap<String, Vec<String>>> {
        self.targets
            .iter()
            .filter(|target| target.branch().writes_deletes())
            .map(|target| (target.ordinal(), target.owned_data_files().clone()))
            .collect()
    }

    pub fn expected_targets(&self) -> Vec<WriteTargetOrdinal> {
        self.targets
            .iter()
            .map(IcebergSealedWriteTarget::ordinal)
            .collect()
    }

    pub fn branch_of(&self, ordinal: WriteTargetOrdinal) -> Option<IcebergWriteBranch> {
        self.targets
            .iter()
            .find(|target| target.ordinal() == ordinal)
            .map(IcebergSealedWriteTarget::branch)
    }

    /// The staging directory this session owns, used for recovery evidence.
    pub fn staging_dir(&self) -> String {
        format!(
            "{}/_staging/{}",
            self.table.data_location().trim_end_matches('/'),
            self.session_id
        )
    }

    pub fn state(&self) -> Result<IcebergWriteSessionState, ConnectorError> {
        self.state
            .lock()
            .map(|state| state.clone())
            .map_err(|_| corrupt("Iceberg write session state lock is poisoned"))
    }

    /// Claim the single commit attempt this session allows.
    ///
    /// A second claim fails: exactly one external snapshot commit may be
    /// attempted per session, and a caller that already saw a terminal verdict
    /// must not silently start a second one.
    pub fn begin_commit(&self) -> Result<(), ConnectorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("Iceberg write session state lock is poisoned"))?;
        match &*state {
            IcebergWriteSessionState::Active => {
                *state = IcebergWriteSessionState::Committing;
                Ok(())
            }
            IcebergWriteSessionState::Committing => Err(invalid(
                "Iceberg write session already has a commit in flight",
            )),
            IcebergWriteSessionState::KnownCommitted { .. } => {
                Err(invalid("Iceberg write session is already known committed"))
            }
            IcebergWriteSessionState::KnownUncommitted { .. } => Err(invalid(
                "Iceberg write session is already known uncommitted",
            )),
            IcebergWriteSessionState::CommitUnknown { .. } => Err(invalid(
                "Iceberg write session outcome is unknown and requires reconciliation",
            )),
        }
    }

    pub fn settle(&self, terminal: IcebergWriteSessionState) -> Result<(), ConnectorError> {
        if matches!(
            terminal,
            IcebergWriteSessionState::Active | IcebergWriteSessionState::Committing
        ) {
            return Err(invalid(
                "Iceberg write session cannot settle into a non-terminal state",
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| corrupt("Iceberg write session state lock is poisoned"))?;
        *state = terminal;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::write_stack::test_support::{
        dv_artifact, sample_metrics, sample_partition, table_facts,
    };

    #[test]
    fn every_flavor_maps_to_one_commit_op_and_owns_its_branches() {
        for flavor in [
            IcebergWriteFlavor::Append,
            IcebergWriteFlavor::Overwrite,
            IcebergWriteFlavor::PartitionOverwrite,
            IcebergWriteFlavor::RowMutationPositionDelete,
            IcebergWriteFlavor::RowMutationDeletionVector,
            IcebergWriteFlavor::RowMutationCopyOnWrite,
            IcebergWriteFlavor::StagedCreate,
            IcebergWriteFlavor::ManagedPublication,
            IcebergWriteFlavor::DistributedRewrite,
            IcebergWriteFlavor::TableMaintenance,
        ] {
            let branches = flavor.branches();
            assert!(!branches.is_empty(), "{} has no branch", flavor.as_str());
            assert_eq!(
                branches[0],
                IcebergWriteBranch::Data,
                "{} must open its data branch first",
                flavor.as_str()
            );
            // A commit op is a total function of the flavor; calling it must
            // never panic and must never be inferred from a fragment.
            let _ = flavor.commit_op_kind();
            let _ = flavor.connector_intent();
            assert!(flavor.accepts_empty_prepared_set());
        }
        assert_eq!(
            IcebergWriteFlavor::RowMutationDeletionVector.branches(),
            &[IcebergWriteBranch::Data, IcebergWriteBranch::DeletionVector]
        );
    }

    #[test]
    fn a_location_that_embeds_a_credential_is_rejected() {
        assert!(validate_location("table location", "s3://bucket/warehouse").is_ok());
        assert!(validate_location("table location", "").is_err());
        assert!(validate_location("table location", "s3://a\0b").is_err());
        assert!(validate_location("table location", "s3://key:secret@bucket/x").is_err());
        assert!(
            validate_location("table location", "s3://bucket/x?session_token=abc").is_err(),
            "a query-string credential must not survive into a handle"
        );
    }

    #[test]
    fn artifact_metrics_reject_a_zero_size_or_unsorted_split_offsets() {
        assert!(IcebergArtifactMetrics::try_new(1, 0, Vec::new(), None).is_err());
        assert!(IcebergArtifactMetrics::try_new(1, 100, vec![0, 40, 80], None).is_ok());
        assert!(IcebergArtifactMetrics::try_new(1, 100, vec![0, 80, 40], None).is_err());
        assert!(IcebergArtifactMetrics::try_new(1, 100, vec![0, 100], None).is_err());
        assert!(IcebergArtifactMetrics::try_new(1, 100, vec![-1], None).is_err());
    }

    #[test]
    fn a_deletion_vector_artifact_must_agree_with_its_own_blob_range() {
        assert!(dv_artifact("s3://b/dv.puffin", "s3://b/a.parquet", 3, 1024, 0, 64).is_ok());
        assert_eq!(
            dv_artifact("s3://b/dv.puffin", "s3://b/a.parquet", 0, 1024, 0, 64)
                .expect_err("empty vector")
                .kind(),
            ConnectorErrorKind::InvalidRequest
        );
        assert_eq!(
            dv_artifact("s3://b/dv.puffin", "s3://b/a.parquet", 3, 32, 0, 64)
                .expect_err("blob past end of file")
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn a_deletion_vector_cardinality_must_equal_its_record_count() {
        let partition = sample_partition();
        let metrics = sample_metrics(3, 1024);
        let error = IcebergDeletionVectorArtifact::try_new(
            "s3://b/dv.puffin".to_string(),
            partition,
            metrics,
            "s3://b/a.parquet".to_string(),
            IcebergContentRange::try_new(0, 64).expect("range"),
            4,
            Vec::new(),
        )
        .expect_err("cardinality disagreement");
        assert_eq!(error.kind(), ConnectorErrorKind::CorruptData);
    }

    #[test]
    fn merged_old_references_must_be_sorted_and_unique() {
        let build = |references: Vec<String>| {
            IcebergPositionDeleteFileArtifact::try_new(
                "s3://b/d.parquet".to_string(),
                sample_partition(),
                sample_metrics(2, 512),
                "s3://b/a.parquet".to_string(),
                references,
            )
        };
        assert!(build(vec!["s3://b/1".to_string(), "s3://b/2".to_string()]).is_ok());
        assert!(build(vec!["s3://b/2".to_string(), "s3://b/1".to_string()]).is_err());
        assert!(build(vec!["s3://b/1".to_string(), "s3://b/1".to_string()]).is_err());
    }

    #[test]
    fn a_session_seals_dense_ordinals_and_refuses_a_repeated_branch() {
        let target = |ordinal: u32, branch| {
            IcebergSealedWriteTarget::new(
                WriteTargetOrdinal::try_new(ordinal).expect("ordinal"),
                branch,
                BTreeMap::new(),
            )
        };
        assert!(
            IcebergCommitHandle::try_new(
                IcebergWriteSessionId::new(),
                table_facts(),
                IcebergWriteFlavor::RowMutationDeletionVector,
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
                None,
                vec![
                    target(0, IcebergWriteBranch::Data),
                    target(1, IcebergWriteBranch::DeletionVector),
                ],
            )
            .is_ok()
        );
        assert!(
            IcebergCommitHandle::try_new(
                IcebergWriteSessionId::new(),
                table_facts(),
                IcebergWriteFlavor::RowMutationDeletionVector,
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
                None,
                vec![
                    target(0, IcebergWriteBranch::Data),
                    target(1, IcebergWriteBranch::Data),
                ],
            )
            .is_err(),
            "a repeated branch makes the delete route ambiguous"
        );
        assert!(
            IcebergCommitHandle::try_new(
                IcebergWriteSessionId::new(),
                table_facts(),
                IcebergWriteFlavor::Append,
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
                None,
                vec![
                    target(0, IcebergWriteBranch::Data),
                    target(1, IcebergWriteBranch::DeletionVector),
                ],
            )
            .is_err(),
            "an append flavor does not own a deletion-vector branch"
        );
        assert!(
            IcebergCommitHandle::try_new(
                IcebergWriteSessionId::new(),
                table_facts(),
                IcebergWriteFlavor::Append,
                ConnectorWriteAdmissionPurpose::OrdinaryDml,
                None,
                vec![target(1, IcebergWriteBranch::Data)],
            )
            .is_err(),
            "ordinals must be dense from zero"
        );
    }

    #[test]
    fn a_session_allows_exactly_one_commit_attempt() {
        let handle = IcebergCommitHandle::try_new(
            IcebergWriteSessionId::new(),
            table_facts(),
            IcebergWriteFlavor::Append,
            ConnectorWriteAdmissionPurpose::OrdinaryDml,
            None,
            vec![IcebergSealedWriteTarget::new(
                WriteTargetOrdinal::try_new(0).expect("ordinal"),
                IcebergWriteBranch::Data,
                BTreeMap::new(),
            )],
        )
        .expect("session");
        assert!(matches!(
            handle.state().expect("state"),
            IcebergWriteSessionState::Active
        ));
        handle.begin_commit().expect("first attempt");
        assert!(handle.begin_commit().is_err());
        handle
            .settle(IcebergWriteSessionState::KnownCommitted { snapshot_id: 7 })
            .expect("settle");
        assert!(handle.begin_commit().is_err());
        assert!(handle.settle(IcebergWriteSessionState::Active).is_err());
    }
}
