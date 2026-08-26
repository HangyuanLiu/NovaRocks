//! Structural validation for the closed split categories.
//!
//! A split is validated as shape only: encoded size, scalar budget, required
//! presence, known enums, uniqueness, and cross-field agreement. The generic
//! scheduler reads the neutral envelope promoted onto `ConnectorSplit`; the
//! provider variant is carried through untouched and interpreted only by the
//! provider that produced it.

use std::collections::BTreeSet;

use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::read_stack::SplitWeight;
use prost::Message;

use crate::{FieldPath, ProtocolError};

use super::predicate::decode_tuple_domain;
use super::{
    MAX_AFFINITY_KEY_BYTES, MAX_DELETES_PER_SPLIT, MAX_EQUALITY_FIELD_IDS, MAX_JSON_BYTES,
    MAX_NAME_BYTES, MAX_PATH_BYTES, MAX_SPLIT_ADDRESSES, MAX_SPLIT_ENCODED_BYTES,
    MAX_SPLIT_SCALAR_TOTAL_BYTES, bounded_text, inconsistent, invalid, invalid_enum, missing, nest,
    nonnegative_i64, out_of_range, unsupported,
};

/// A host name is bounded by the proto contract, not by the generic name bound.
const MAX_HOST_BYTES: usize = 255;

/// The optional row narrowing carried by a change split's added rows.
const MAX_RESTRICTED_ROW_IDS: usize = 4096;

/// The largest manifest content code this contract accepts. The code reaches
/// the `$files` relation unchanged; only its range is owned here.
const MAX_MANIFEST_CONTENT: i32 = 2;

/// Which closed category a split belongs to. The generic scheduler may branch
/// on this; it must never interpret the provider variant inside.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SplitCategory {
    Data,
    TableChanges,
    ChangeWindow,
    SystemFiles,
    RewritePositionDeleteFiles,
}

/// A structurally validated `ConnectorSplit`.
///
/// The generated message is retained unchanged, so a validated split re-encodes
/// to the bytes it arrived as. Only the neutral envelope is exposed: a consumer
/// that needs provider facts reads the raw message and owns their meaning.
#[derive(Clone, Debug)]
pub struct ValidatedConnectorSplit {
    raw: dto::ConnectorSplit,
    weight: SplitWeight,
    category: SplitCategory,
}

impl ValidatedConnectorSplit {
    pub fn parse(raw: dto::ConnectorSplit, path: FieldPath) -> Result<Self, ProtocolError> {
        // Size is checked before anything walks the contents, so an oversized
        // split costs one length computation rather than a full traversal.
        if raw.encoded_len() > MAX_SPLIT_ENCODED_BYTES {
            return Err(out_of_range(
                path,
                format!("split exceeds {MAX_SPLIT_ENCODED_BYTES} encoded bytes"),
            ));
        }

        let mut budget = ScalarBudget::new();
        let weight = SplitWeight::try_from_raw(raw.split_weight_raw).map_err(|error| {
            out_of_range(path.field("split_weight_raw"), error.message().to_owned())
        })?;
        validate_addresses(&raw.addresses, &path, &mut budget)?;
        if let Some(affinity_key) = raw.affinity_key.as_deref() {
            // An affinity key is optional, so a present empty key would be a
            // second spelling of "absent" that co-locates unrelated splits.
            budget.text(
                affinity_key,
                MAX_AFFINITY_KEY_BYTES,
                path.field("affinity_key"),
                false,
            )?;
        }

        let category = raw
            .category
            .as_ref()
            .ok_or_else(|| missing(path.clone(), "split category must be present"))?;
        let category = validate_category(category, &path, &mut budget)?;

        Ok(Self {
            raw,
            weight,
            category,
        })
    }

    /// Relative scheduling cost. Weight never changes what a split reads.
    pub const fn split_weight(&self) -> SplitWeight {
        self.weight
    }

    /// Whether any worker may run this split.
    pub const fn is_remotely_accessible(&self) -> bool {
        self.raw.remotely_accessible
    }

    /// Addresses this split must or prefers to run on.
    pub fn addresses(&self) -> &[dto::HostAddress] {
        &self.raw.addresses
    }

    /// A stable key used to co-locate related splits; never an identity.
    pub fn affinity_key(&self) -> Option<&str> {
        self.raw.affinity_key.as_deref()
    }

    pub const fn retained_size_in_bytes(&self) -> u64 {
        self.raw.retained_size_in_bytes
    }

    pub const fn category(&self) -> SplitCategory {
        self.category
    }

    pub const fn as_proto(&self) -> &dto::ConnectorSplit {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorSplit {
        self.raw
    }
}

/// The split-wide scalar budget.
///
/// Per-field bounds cannot stop a split that carries thousands of individually
/// legal strings, so every string and bytes field walked by one split is
/// charged against one shared budget.
struct ScalarBudget {
    used: usize,
}

impl ScalarBudget {
    const fn new() -> Self {
        Self { used: 0 }
    }

    fn charge(&mut self, len: usize, path: &FieldPath) -> Result<(), ProtocolError> {
        self.used = self.used.saturating_add(len);
        if self.used > MAX_SPLIT_SCALAR_TOTAL_BYTES {
            return Err(out_of_range(
                path.clone(),
                format!("split scalar fields exceed {MAX_SPLIT_SCALAR_TOTAL_BYTES} bytes in total"),
            ));
        }
        Ok(())
    }

    /// A bounded text field that also consumes the split-wide budget.
    fn text(
        &mut self,
        value: &str,
        max_bytes: usize,
        path: FieldPath,
        allow_empty: bool,
    ) -> Result<(), ProtocolError> {
        bounded_text(value, max_bytes, path.clone(), allow_empty)?;
        self.charge(value.len(), &path)
    }
}

fn nonnegative_i32(value: i32, path: FieldPath, label: &'static str) -> Result<i32, ProtocolError> {
    if value < 0 {
        return Err(out_of_range(path, format!("{label} must be nonnegative")));
    }
    Ok(value)
}

fn validate_addresses(
    raw: &[dto::HostAddress],
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<(), ProtocolError> {
    if raw.len() > MAX_SPLIT_ADDRESSES {
        return Err(out_of_range(
            path.field("addresses"),
            "split address count exceeds the hard limit",
        ));
    }
    for (index, address) in raw.iter().enumerate() {
        let address_path = path.field("addresses").index(index);
        budget.text(
            &address.host,
            MAX_HOST_BYTES,
            address_path.field("host"),
            false,
        )?;
        if address.port == 0 || address.port > u32::from(u16::MAX) {
            return Err(out_of_range(
                address_path.field("port"),
                "host address port must be within 1..=65535",
            ));
        }
    }
    Ok(())
}

fn validate_category(
    raw: &dto::connector_split::Category,
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<SplitCategory, ProtocolError> {
    match raw {
        dto::connector_split::Category::Data(data) => {
            let data_path = path.field("data");
            let provider = data
                .provider
                .as_ref()
                .ok_or_else(|| missing(data_path.clone(), "data split provider must be present"))?;
            match provider {
                dto::data_split::Provider::Iceberg(iceberg) => {
                    validate_iceberg_split(iceberg, &data_path.field("iceberg"), budget)?;
                }
            }
            Ok(SplitCategory::Data)
        }
        dto::connector_split::Category::TableChanges(changes) => {
            let changes_path = path.field("table_changes");
            let provider = changes.provider.as_ref().ok_or_else(|| {
                missing(
                    changes_path.clone(),
                    "table changes split provider must be present",
                )
            })?;
            match provider {
                dto::table_changes_split_category::Provider::Iceberg(iceberg) => {
                    validate_table_changes_split(iceberg, &changes_path.field("iceberg"), budget)?;
                }
            }
            Ok(SplitCategory::TableChanges)
        }
        dto::connector_split::Category::ChangeWindow(window) => {
            let window_path = path.field("change_window");
            let provider = window.provider.as_ref().ok_or_else(|| {
                missing(
                    window_path.clone(),
                    "change window split provider must be present",
                )
            })?;
            match provider {
                dto::change_window_split_category::Provider::Iceberg(iceberg) => {
                    validate_change_split(iceberg, &window_path.field("iceberg"), budget)?;
                }
            }
            Ok(SplitCategory::ChangeWindow)
        }
        dto::connector_split::Category::SystemFiles(files) => {
            let files_path = path.field("system_files");
            let provider = files.provider.as_ref().ok_or_else(|| {
                missing(
                    files_path.clone(),
                    "system files split provider must be present",
                )
            })?;
            match provider {
                dto::system_files_split_category::Provider::Iceberg(iceberg) => {
                    validate_files_table_split(iceberg, &files_path.field("iceberg"), budget)?;
                }
            }
            Ok(SplitCategory::SystemFiles)
        }
        dto::connector_split::Category::RewritePositionDeleteFiles(rewrite) => {
            let rewrite_path = path.field("rewrite_position_delete_files");
            let provider = rewrite.provider.as_ref().ok_or_else(|| {
                missing(
                    rewrite_path.clone(),
                    "rewrite position delete files split provider must be present",
                )
            })?;
            match provider {
                dto::rewrite_position_delete_files_split_category::Provider::Iceberg(iceberg) => {
                    validate_rewrite_split(iceberg, &rewrite_path.field("iceberg"), budget)?;
                }
            }
            Ok(SplitCategory::RewritePositionDeleteFiles)
        }
    }
}

/// The format of a file this contract's readers open.
///
/// The format field is the only format authority: a reader never infers a
/// format from a path suffix, so a format this contract cannot read is refused
/// here instead of being discovered mid-scan.
fn known_data_file_format(value: i32, path: FieldPath) -> Result<(), ProtocolError> {
    match known_file_format(value, path.clone())? {
        dto::IcebergFileFormat::Parquet => Ok(()),
        dto::IcebergFileFormat::Orc => Err(unsupported(
            path,
            "iceberg ORC data files are not supported by this contract",
        )),
        dto::IcebergFileFormat::Avro => Err(unsupported(
            path,
            "iceberg AVRO data files are not supported by this contract",
        )),
        // Puffin is a delete-artifact container, so naming it as a data file
        // is a contract error rather than a missing capability.
        dto::IcebergFileFormat::Puffin => {
            Err(invalid(path, "a data file is never a puffin container"))
        }
        dto::IcebergFileFormat::Unspecified => {
            unreachable!("known_file_format rejects the unspecified format")
        }
    }
}

fn known_file_format(value: i32, path: FieldPath) -> Result<dto::IcebergFileFormat, ProtocolError> {
    let format = dto::IcebergFileFormat::try_from(value)
        .map_err(|_| invalid_enum(path.clone(), "unknown iceberg file format"))?;
    match format {
        dto::IcebergFileFormat::Unspecified => {
            Err(invalid_enum(path, "iceberg file format must be specified"))
        }
        dto::IcebergFileFormat::Orc
        | dto::IcebergFileFormat::Parquet
        | dto::IcebergFileFormat::Avro
        | dto::IcebergFileFormat::Puffin => Ok(format),
    }
}

fn known_delete_content(
    value: i32,
    path: FieldPath,
) -> Result<dto::IcebergDeleteFileContent, ProtocolError> {
    let content = dto::IcebergDeleteFileContent::try_from(value)
        .map_err(|_| invalid_enum(path.clone(), "unknown iceberg delete file content"))?;
    match content {
        dto::IcebergDeleteFileContent::Unspecified => Err(invalid_enum(
            path,
            "iceberg delete file content must be specified",
        )),
        dto::IcebergDeleteFileContent::PositionDeletes
        | dto::IcebergDeleteFileContent::EqualityDeletes => Ok(content),
    }
}

/// A byte range must name real bytes of the file it claims to read.
fn validate_byte_range(
    start: i64,
    length: i64,
    file_size: i64,
    path: &FieldPath,
) -> Result<(), ProtocolError> {
    let end = start.checked_add(length).ok_or_else(|| {
        out_of_range(
            path.field("length"),
            "split start plus length overflows a signed 64-bit range",
        )
    })?;
    if end > file_size {
        return Err(out_of_range(
            path.field("length"),
            "split range extends past the file size",
        ));
    }
    Ok(())
}

/// Parquet modular encryption is not implemented in this contract.
///
/// The typed extension point may be present but must be empty. The rejection
/// names only the field that was non-empty: key material never reaches a
/// detail string, a log line, or a debug rendering.
fn validate_decryption_data(
    raw: Option<&dto::ParquetFileDecryptionData>,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let Some(decryption_data) = raw else {
        return Ok(());
    };
    if !decryption_data.key_metadata.is_empty() {
        return Err(unsupported(
            path.field("key_metadata"),
            "parquet modular encryption is not supported by this contract",
        ));
    }
    if !decryption_data.aad_prefix.is_empty() {
        return Err(unsupported(
            path.field("aad_prefix"),
            "parquet modular encryption is not supported by this contract",
        ));
    }
    Ok(())
}

fn validate_iceberg_split(
    raw: &dto::IcebergSplit,
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<(), ProtocolError> {
    budget.text(&raw.path, MAX_PATH_BYTES, path.field("path"), false)?;
    let start = nonnegative_i64(raw.start, path.field("start"), "split start")?;
    let length = nonnegative_i64(raw.length, path.field("length"), "split length")?;
    let file_size = nonnegative_i64(raw.file_size, path.field("file_size"), "file size")?;
    nonnegative_i64(
        raw.file_record_count,
        path.field("file_record_count"),
        "file record count",
    )?;
    validate_byte_range(start, length, file_size, path)?;
    known_data_file_format(raw.file_format, path.field("file_format"))?;
    nonnegative_i32(
        raw.partition_spec_id,
        path.field("partition_spec_id"),
        "partition spec id",
    )?;
    budget.text(
        &raw.partition_data_json,
        MAX_JSON_BYTES,
        path.field("partition_data_json"),
        true,
    )?;
    validate_delete_files(&raw.deletes, &path.field("deletes"), budget)?;

    let statistics_path = path.field("file_statistics_domain");
    let statistics = raw.file_statistics_domain.as_ref().ok_or_else(|| {
        missing(
            statistics_path.clone(),
            "iceberg split requires its file statistics domain",
        )
    })?;
    // Predicate decoding owns each value's own bound. The split-wide budget
    // charges the whole encoded predicate, a conservative upper bound on the
    // scalar bytes inside it, so statistics cannot smuggle unbounded payload.
    budget.charge(statistics.encoded_len(), &statistics_path)?;
    decode_tuple_domain(statistics, FieldPath::root("tuple_domain"))
        .map_err(|error| nest(statistics_path, error))?;

    if let Some(data_sequence_number) = raw.data_sequence_number {
        nonnegative_i64(
            data_sequence_number,
            path.field("data_sequence_number"),
            "data sequence number",
        )?;
    }
    if let Some(file_first_row_id) = raw.file_first_row_id {
        nonnegative_i64(
            file_first_row_id,
            path.field("file_first_row_id"),
            "file first row id",
        )?;
    }
    validate_decryption_data(raw.decryption_data.as_ref(), path.field("decryption_data"))
}

fn validate_delete_files(
    raw: &[dto::IcebergDeleteFile],
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<(), ProtocolError> {
    if raw.len() > MAX_DELETES_PER_SPLIT {
        return Err(out_of_range(
            path.clone(),
            "delete file count exceeds the per-split hard limit",
        ));
    }
    let mut paths = BTreeSet::new();
    for (index, delete) in raw.iter().enumerate() {
        let delete_path = path.index(index);
        validate_delete_file(delete, &delete_path, budget)?;
        // A delete closure is a set. The same file twice would apply its work
        // twice and hide the planner error that produced it.
        if !paths.insert(delete.path.as_str()) {
            return Err(inconsistent(
                delete_path.field("path"),
                "delete closure repeats a delete file path",
            ));
        }
    }
    Ok(())
}

fn validate_delete_file(
    raw: &dto::IcebergDeleteFile,
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<(), ProtocolError> {
    let content = known_delete_content(raw.content, path.field("content"))?;
    let format = known_file_format(raw.format, path.field("format"))?;
    budget.text(&raw.path, MAX_PATH_BYTES, path.field("path"), false)?;
    nonnegative_i64(raw.record_count, path.field("record_count"), "record count")?;
    let file_size = nonnegative_i64(
        raw.file_size_in_bytes,
        path.field("file_size_in_bytes"),
        "file size",
    )?;
    nonnegative_i64(
        raw.data_sequence_number,
        path.field("data_sequence_number"),
        "data sequence number",
    )?;
    validate_equality_field_ids(&raw.equality_field_ids, path)?;
    validate_delete_content_agreement(content, &raw.equality_field_ids, path)?;
    validate_content_range(raw, content, format, file_size, path)?;
    validate_row_position_bounds(raw, path)?;
    validate_decryption_data(raw.decryption_data.as_ref(), path.field("decryption_data"))
}

fn validate_equality_field_ids(raw: &[i32], path: &FieldPath) -> Result<(), ProtocolError> {
    if raw.len() > MAX_EQUALITY_FIELD_IDS {
        return Err(out_of_range(
            path.field("equality_field_ids"),
            "equality field id count exceeds the hard limit",
        ));
    }
    // Schema order is the producer's choice, so the list is not required to be
    // sorted; it must still name each field once.
    let mut seen = BTreeSet::new();
    for (index, field_id) in raw.iter().enumerate() {
        let field_path = path.field("equality_field_ids").index(index);
        if *field_id <= 0 {
            return Err(out_of_range(
                field_path,
                "equality field id must be positive",
            ));
        }
        if !seen.insert(*field_id) {
            return Err(inconsistent(
                field_path,
                "equality field ids repeat a field",
            ));
        }
    }
    Ok(())
}

fn validate_delete_content_agreement(
    content: dto::IcebergDeleteFileContent,
    equality_field_ids: &[i32],
    path: &FieldPath,
) -> Result<(), ProtocolError> {
    match content {
        dto::IcebergDeleteFileContent::EqualityDeletes => {
            if equality_field_ids.is_empty() {
                return Err(inconsistent(
                    path.field("equality_field_ids"),
                    "an equality delete file requires at least one equality field id",
                ));
            }
            Ok(())
        }
        dto::IcebergDeleteFileContent::PositionDeletes => {
            if !equality_field_ids.is_empty() {
                return Err(inconsistent(
                    path.field("equality_field_ids"),
                    "a position delete file must not carry equality field ids",
                ));
            }
            Ok(())
        }
        dto::IcebergDeleteFileContent::Unspecified => {
            unreachable!("known_delete_content rejects the unspecified content")
        }
    }
}

/// A content range addresses one blob inside a Puffin deletion vector file.
///
/// The format field is the marker: only a Puffin position-delete file carries
/// a range, and it must carry one, because the blob is not the whole file. A
/// Parquet position-delete file is delete data end to end and never has one.
fn validate_content_range(
    raw: &dto::IcebergDeleteFile,
    content: dto::IcebergDeleteFileContent,
    format: dto::IcebergFileFormat,
    file_size: i64,
    path: &FieldPath,
) -> Result<(), ProtocolError> {
    let (offset, size) = match (raw.content_offset, raw.content_size_in_bytes) {
        (None, None) => return Ok(()),
        (Some(_), None) => {
            return Err(inconsistent(
                path.field("content_size_in_bytes"),
                "a deletion vector content range requires both an offset and a size",
            ));
        }
        (None, Some(_)) => {
            return Err(inconsistent(
                path.field("content_offset"),
                "a deletion vector content range requires both an offset and a size",
            ));
        }
        (Some(offset), Some(size)) => (offset, size),
    };

    match content {
        dto::IcebergDeleteFileContent::PositionDeletes => {}
        dto::IcebergDeleteFileContent::EqualityDeletes => {
            return Err(inconsistent(
                path.field("content_offset"),
                "an equality delete file must not carry a deletion vector content range",
            ));
        }
        dto::IcebergDeleteFileContent::Unspecified => {
            unreachable!("known_delete_content rejects the unspecified content")
        }
    }
    match format {
        dto::IcebergFileFormat::Puffin => {}
        dto::IcebergFileFormat::Parquet
        | dto::IcebergFileFormat::Orc
        | dto::IcebergFileFormat::Avro => {
            return Err(inconsistent(
                path.field("content_offset"),
                "only a puffin delete file carries a deletion vector content range",
            ));
        }
        dto::IcebergFileFormat::Unspecified => {
            unreachable!("known_file_format rejects the unspecified format")
        }
    }

    let offset = nonnegative_i64(offset, path.field("content_offset"), "content offset")?;
    let size = nonnegative_i64(size, path.field("content_size_in_bytes"), "content size")?;
    let end = offset.checked_add(size).ok_or_else(|| {
        out_of_range(
            path.field("content_size_in_bytes"),
            "content offset plus size overflows a signed 64-bit range",
        )
    })?;
    if end > file_size {
        return Err(out_of_range(
            path.field("content_size_in_bytes"),
            "content range extends past the delete file size",
        ));
    }
    Ok(())
}

fn validate_row_position_bounds(
    raw: &dto::IcebergDeleteFile,
    path: &FieldPath,
) -> Result<(), ProtocolError> {
    if let Some(lower) = raw.row_position_lower_bound {
        nonnegative_i64(
            lower,
            path.field("row_position_lower_bound"),
            "row position lower bound",
        )?;
    }
    if let Some(upper) = raw.row_position_upper_bound {
        nonnegative_i64(
            upper,
            path.field("row_position_upper_bound"),
            "row position upper bound",
        )?;
    }
    if let (Some(lower), Some(upper)) = (raw.row_position_lower_bound, raw.row_position_upper_bound)
        && lower > upper
    {
        return Err(inconsistent(
            path.field("row_position_upper_bound"),
            "row position upper bound must not precede the lower bound",
        ));
    }
    Ok(())
}

fn validate_table_changes_split(
    raw: &dto::TableChangesSplit,
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<(), ProtocolError> {
    let change_type = dto::TableChangesChangeType::try_from(raw.change_type).map_err(|_| {
        invalid_enum(
            path.field("change_type"),
            "unknown table changes change type",
        )
    })?;
    match change_type {
        dto::TableChangesChangeType::AddedFile | dto::TableChangesChangeType::DeletedFile => {}
        dto::TableChangesChangeType::Unspecified => {
            return Err(invalid_enum(
                path.field("change_type"),
                "table changes change type must be specified",
            ));
        }
    }
    nonnegative_i64(raw.snapshot_id, path.field("snapshot_id"), "snapshot id")?;
    nonnegative_i64(
        raw.change_ordinal,
        path.field("change_ordinal"),
        "change ordinal",
    )?;
    budget.text(&raw.path, MAX_PATH_BYTES, path.field("path"), false)?;
    let start = nonnegative_i64(raw.start, path.field("start"), "split start")?;
    let length = nonnegative_i64(raw.length, path.field("length"), "split length")?;
    let file_size = nonnegative_i64(raw.file_size, path.field("file_size"), "file size")?;
    nonnegative_i64(
        raw.file_record_count,
        path.field("file_record_count"),
        "file record count",
    )?;
    validate_byte_range(start, length, file_size, path)?;
    known_data_file_format(raw.file_format, path.field("file_format"))?;
    nonnegative_i32(
        raw.partition_spec_id,
        path.field("partition_spec_id"),
        "partition spec id",
    )?;
    budget.text(
        &raw.partition_data_json,
        MAX_JSON_BYTES,
        path.field("partition_data_json"),
        true,
    )?;
    validate_decryption_data(raw.decryption_data.as_ref(), path.field("decryption_data"))
}

fn validate_change_split(
    raw: &dto::IcebergChangeSplit,
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<(), ProtocolError> {
    let rows = raw
        .rows
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "change split row variant must be present"))?;
    match rows {
        dto::iceberg_change_split::Rows::AddedRows(added) => {
            let added_path = path.field("added_rows");
            validate_change_data(added.data.as_ref(), &added_path, budget)?;
            validate_restricted_row_ids(&added.restricted_row_ids, &added_path)?;
        }
        dto::iceberg_change_split::Rows::PositionDeletedRows(deleted) => {
            let deleted_path = path.field("position_deleted_rows");
            validate_change_data(deleted.data.as_ref(), &deleted_path, budget)?;
            validate_delete_files(
                &deleted.newly_applied_deletes,
                &deleted_path.field("newly_applied_deletes"),
                budget,
            )?;
            validate_delete_files(
                &deleted.previously_applied_deletes,
                &deleted_path.field("previously_applied_deletes"),
                budget,
            )?;
        }
        dto::iceberg_change_split::Rows::EqualityDeletedRows(deleted) => {
            let deleted_path = path.field("equality_deleted_rows");
            validate_change_data(deleted.data.as_ref(), &deleted_path, budget)?;
            validate_delete_files(
                &deleted.newly_applied_equality_deletes,
                &deleted_path.field("newly_applied_equality_deletes"),
                budget,
            )?;
            validate_delete_files(
                &deleted.previously_applied_deletes,
                &deleted_path.field("previously_applied_deletes"),
                budget,
            )?;
        }
        dto::iceberg_change_split::Rows::DeletedDataFileRows(deleted) => {
            let deleted_path = path.field("deleted_data_file_rows");
            validate_change_data(deleted.data.as_ref(), &deleted_path, budget)?;
            validate_delete_files(
                &deleted.previously_applied_deletes,
                &deleted_path.field("previously_applied_deletes"),
                budget,
            )?;
        }
    }
    Ok(())
}

fn validate_change_data(
    raw: Option<&dto::IcebergSplit>,
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<(), ProtocolError> {
    let data = raw.ok_or_else(|| {
        missing(
            path.field("data"),
            "change split rows require their data split",
        )
    })?;
    validate_iceberg_split(data, &path.field("data"), budget)
}

/// The narrowing is a set of row positions, so a repeat is a producer error
/// rather than a second row.
fn validate_restricted_row_ids(raw: &[i64], path: &FieldPath) -> Result<(), ProtocolError> {
    if raw.len() > MAX_RESTRICTED_ROW_IDS {
        return Err(out_of_range(
            path.field("restricted_row_ids"),
            "restricted row id count exceeds the hard limit",
        ));
    }
    let mut seen = BTreeSet::new();
    for (index, row_id) in raw.iter().enumerate() {
        let row_path = path.field("restricted_row_ids").index(index);
        nonnegative_i64(*row_id, row_path.clone(), "restricted row id")?;
        if !seen.insert(*row_id) {
            return Err(inconsistent(row_path, "restricted row ids repeat a row"));
        }
    }
    Ok(())
}

fn validate_files_table_split(
    raw: &dto::FilesTableSplit,
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<(), ProtocolError> {
    let manifest = raw.manifest.as_ref().ok_or_else(|| {
        missing(
            path.field("manifest"),
            "files table split requires its manifest",
        )
    })?;
    validate_manifest_file(manifest, &path.field("manifest"), budget)?;
    budget.text(
        &raw.table_schema_json,
        MAX_JSON_BYTES,
        path.field("table_schema_json"),
        false,
    )?;
    budget.text(
        &raw.metadata_table_schema_json,
        MAX_JSON_BYTES,
        path.field("metadata_table_schema_json"),
        false,
    )?;
    // Generated maps iterate in an unspecified order, so the spec ids are
    // walked sorted and the first rejection is the same on every host.
    let mut spec_ids = raw.partition_spec_jsons.keys().copied().collect::<Vec<_>>();
    spec_ids.sort_unstable();
    for spec_id in spec_ids {
        let spec_json = &raw.partition_spec_jsons[&spec_id];
        budget.text(
            spec_json,
            MAX_JSON_BYTES,
            path.field("partition_spec_jsons")
                .map_key(spec_id.to_string()),
            false,
        )?;
    }
    if let Some(partition_column_type_json) = raw.partition_column_type_json.as_deref() {
        budget.text(
            partition_column_type_json,
            MAX_JSON_BYTES,
            path.field("partition_column_type_json"),
            false,
        )?;
    }
    if let Some(bounds_column_type_json) = raw.bounds_column_type_json.as_deref() {
        budget.text(
            bounds_column_type_json,
            MAX_JSON_BYTES,
            path.field("bounds_column_type_json"),
            false,
        )?;
    }
    if let Some(encryption_key_id) = raw.encryption_key_id.as_deref() {
        budget.text(
            encryption_key_id,
            MAX_NAME_BYTES,
            path.field("encryption_key_id"),
            false,
        )?;
    }
    Ok(())
}

fn validate_manifest_file(
    raw: &dto::TrinoManifestFile,
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<(), ProtocolError> {
    budget.text(&raw.path, MAX_PATH_BYTES, path.field("path"), false)?;
    if raw.length <= 0 {
        return Err(out_of_range(
            path.field("length"),
            "manifest length must be positive",
        ));
    }
    nonnegative_i32(
        raw.partition_spec_id,
        path.field("partition_spec_id"),
        "partition spec id",
    )?;
    if raw.content < 0 || raw.content > MAX_MANIFEST_CONTENT {
        return Err(out_of_range(
            path.field("content"),
            format!("manifest content must be within 0..={MAX_MANIFEST_CONTENT}"),
        ));
    }
    for (value, field, label) in [
        (
            raw.added_files_count,
            "added_files_count",
            "added files count",
        ),
        (
            raw.existing_files_count,
            "existing_files_count",
            "existing files count",
        ),
        (
            raw.deleted_files_count,
            "deleted_files_count",
            "deleted files count",
        ),
    ] {
        if let Some(value) = value {
            nonnegative_i32(value, path.field(field), label)?;
        }
    }
    for (value, field, label) in [
        (raw.added_rows_count, "added_rows_count", "added rows count"),
        (
            raw.existing_rows_count,
            "existing_rows_count",
            "existing rows count",
        ),
        (
            raw.deleted_rows_count,
            "deleted_rows_count",
            "deleted rows count",
        ),
        (raw.first_row_id, "first_row_id", "first row id"),
    ] {
        if let Some(value) = value {
            nonnegative_i64(value, path.field(field), label)?;
        }
    }
    // Encrypted manifests are out of contract: the reader has no key source,
    // so the key metadata is refused rather than carried and ignored.
    if !raw.key_metadata.is_empty() {
        return Err(unsupported(
            path.field("key_metadata"),
            "encrypted iceberg manifests are not supported by this contract",
        ));
    }
    Ok(())
}

fn validate_rewrite_split(
    raw: &dto::IcebergRewritePositionDeleteFilesSplit,
    path: &FieldPath,
    budget: &mut ScalarBudget,
) -> Result<(), ProtocolError> {
    budget.text(
        &raw.data_file_path,
        MAX_PATH_BYTES,
        path.field("data_file_path"),
        false,
    )?;
    nonnegative_i64(
        raw.data_file_size,
        path.field("data_file_size"),
        "data file size",
    )?;
    nonnegative_i32(
        raw.partition_spec_id,
        path.field("partition_spec_id"),
        "partition spec id",
    )?;
    budget.text(
        &raw.partition_data_json,
        MAX_JSON_BYTES,
        path.field("partition_data_json"),
        true,
    )?;

    let deletes_path = path.field("selected_position_deletes");
    if raw.selected_position_deletes.is_empty() {
        return Err(missing(
            deletes_path.clone(),
            "a rewrite split requires at least one selected position delete",
        ));
    }
    validate_delete_files(&raw.selected_position_deletes, &deletes_path, budget)?;
    for (index, delete) in raw.selected_position_deletes.iter().enumerate() {
        let delete_path = deletes_path.index(index);
        // The rewrite reads one blob range per delete, so a whole-file delete
        // has nothing this procedure can rewrite.
        match known_delete_content(delete.content, delete_path.field("content"))? {
            dto::IcebergDeleteFileContent::PositionDeletes => {}
            dto::IcebergDeleteFileContent::EqualityDeletes => {
                return Err(inconsistent(
                    delete_path.field("content"),
                    "a rewrite split selects position deletes only",
                ));
            }
            dto::IcebergDeleteFileContent::Unspecified => {
                unreachable!("known_delete_content rejects the unspecified content")
            }
        }
        if delete.content_offset.is_none() || delete.content_size_in_bytes.is_none() {
            return Err(inconsistent(
                delete_path.field("content_offset"),
                "a rewrite split selects puffin deletion vectors only",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProtocolErrorKind;

    fn root() -> FieldPath {
        FieldPath::root("connector_split")
    }

    fn unconstrained_statistics() -> dto::TupleDomain {
        dto::TupleDomain {
            none: false,
            column_domains: Vec::new(),
        }
    }

    fn iceberg_split() -> dto::IcebergSplit {
        dto::IcebergSplit {
            path: "s3://bucket/table/data/0001.parquet".to_owned(),
            start: 0,
            length: 4096,
            file_size: 8192,
            file_record_count: 128,
            file_format: dto::IcebergFileFormat::Parquet as i32,
            partition_spec_id: 0,
            partition_data_json: "{\"partitionValues\":[]}".to_owned(),
            deletes: Vec::new(),
            file_statistics_domain: Some(unconstrained_statistics()),
            data_sequence_number: Some(7),
            file_first_row_id: Some(0),
            decryption_data: None,
        }
    }

    fn position_delete() -> dto::IcebergDeleteFile {
        dto::IcebergDeleteFile {
            content: dto::IcebergDeleteFileContent::PositionDeletes as i32,
            path: "s3://bucket/table/data/0001-deletes.parquet".to_owned(),
            format: dto::IcebergFileFormat::Parquet as i32,
            record_count: 4,
            file_size_in_bytes: 1024,
            equality_field_ids: Vec::new(),
            row_position_lower_bound: Some(0),
            row_position_upper_bound: Some(127),
            data_sequence_number: 8,
            content_offset: None,
            content_size_in_bytes: None,
            decryption_data: None,
        }
    }

    /// A Puffin deletion vector is one addressed blob inside a container file,
    /// so it is the only delete shape that carries a content range.
    fn deletion_vector() -> dto::IcebergDeleteFile {
        dto::IcebergDeleteFile {
            format: dto::IcebergFileFormat::Puffin as i32,
            path: "s3://bucket/table/data/0001-deletes.puffin".to_owned(),
            content_offset: Some(4),
            content_size_in_bytes: Some(64),
            ..position_delete()
        }
    }

    fn equality_delete() -> dto::IcebergDeleteFile {
        dto::IcebergDeleteFile {
            content: dto::IcebergDeleteFileContent::EqualityDeletes as i32,
            path: "s3://bucket/table/data/0001-eq-deletes.parquet".to_owned(),
            equality_field_ids: vec![1, 3],
            row_position_lower_bound: None,
            row_position_upper_bound: None,
            ..position_delete()
        }
    }

    fn envelope(category: dto::connector_split::Category) -> dto::ConnectorSplit {
        dto::ConnectorSplit {
            split_weight_raw: 100,
            remotely_accessible: true,
            addresses: vec![dto::HostAddress {
                host: "be-1.novarocks.internal".to_owned(),
                port: 9060,
            }],
            affinity_key: Some("s3://bucket/table/data/0001.parquet".to_owned()),
            retained_size_in_bytes: 4096,
            category: Some(category),
        }
    }

    fn data_category(split: dto::IcebergSplit) -> dto::connector_split::Category {
        dto::connector_split::Category::Data(dto::DataSplit {
            provider: Some(dto::data_split::Provider::Iceberg(split)),
        })
    }

    fn data_split(split: dto::IcebergSplit) -> dto::ConnectorSplit {
        envelope(data_category(split))
    }

    fn change_category(rows: dto::iceberg_change_split::Rows) -> dto::connector_split::Category {
        dto::connector_split::Category::ChangeWindow(dto::ChangeWindowSplitCategory {
            provider: Some(dto::change_window_split_category::Provider::Iceberg(
                dto::IcebergChangeSplit { rows: Some(rows) },
            )),
        })
    }

    fn manifest() -> dto::TrinoManifestFile {
        dto::TrinoManifestFile {
            path: "s3://bucket/table/metadata/snap-1.avro".to_owned(),
            length: 2048,
            partition_spec_id: 0,
            content: 0,
            sequence_number: 3,
            min_sequence_number: 1,
            added_snapshot_id: 42,
            added_files_count: Some(2),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: Some(256),
            existing_rows_count: Some(0),
            deleted_rows_count: Some(0),
            first_row_id: Some(0),
            key_metadata: Vec::new(),
        }
    }

    fn files_category(manifest: dto::TrinoManifestFile) -> dto::connector_split::Category {
        dto::connector_split::Category::SystemFiles(dto::SystemFilesSplitCategory {
            provider: Some(dto::system_files_split_category::Provider::Iceberg(
                dto::FilesTableSplit {
                    manifest: Some(manifest),
                    table_schema_json: "{\"type\":\"struct\"}".to_owned(),
                    metadata_table_schema_json: "{\"type\":\"struct\"}".to_owned(),
                    partition_spec_jsons: [(0, "{\"spec-id\":0}".to_owned())].into_iter().collect(),
                    partition_column_type_json: None,
                    bounds_column_type_json: None,
                    encryption_key_id: None,
                },
            )),
        })
    }

    fn rewrite_category(deletes: Vec<dto::IcebergDeleteFile>) -> dto::connector_split::Category {
        dto::connector_split::Category::RewritePositionDeleteFiles(
            dto::RewritePositionDeleteFilesSplitCategory {
                provider: Some(
                    dto::rewrite_position_delete_files_split_category::Provider::Iceberg(
                        dto::IcebergRewritePositionDeleteFilesSplit {
                            data_file_path: "s3://bucket/table/data/0001.parquet".to_owned(),
                            data_file_size: 8192,
                            partition_spec_id: 0,
                            partition_data_json: "{\"partitionValues\":[]}".to_owned(),
                            selected_position_deletes: deletes,
                        },
                    ),
                ),
            },
        )
    }

    #[test]
    fn a_full_data_split_round_trips_through_its_neutral_envelope() {
        let mut split = iceberg_split();
        split.deletes = vec![position_delete(), equality_delete(), deletion_vector()];
        let raw = data_split(split);

        let validated = ValidatedConnectorSplit::parse(raw.clone(), root()).expect("valid split");
        assert_eq!(validated.split_weight(), SplitWeight::STANDARD);
        assert!(validated.is_remotely_accessible());
        assert_eq!(validated.addresses().len(), 1);
        assert_eq!(validated.addresses()[0].port, 9060);
        assert_eq!(
            validated.affinity_key(),
            Some("s3://bucket/table/data/0001.parquet")
        );
        assert_eq!(validated.retained_size_in_bytes(), 4096);
        assert_eq!(validated.category(), SplitCategory::Data);
        assert_eq!(validated.as_proto(), &raw);
        assert_eq!(validated.into_proto(), raw);
    }

    #[test]
    fn an_absent_category_is_a_missing_field() {
        let mut raw = data_split(iceberg_split());
        raw.category = None;
        let error = ValidatedConnectorSplit::parse(raw, root()).expect_err("absent category");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(error.path().to_string(), "connector_split");
    }

    #[test]
    fn an_absent_provider_variant_is_a_missing_field() {
        let raw = envelope(dto::connector_split::Category::Data(dto::DataSplit {
            provider: None,
        }));
        let error = ValidatedConnectorSplit::parse(raw, root()).expect_err("absent provider");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(error.path().to_string(), "connector_split.data");
    }

    #[test]
    fn an_absent_split_weight_is_rejected() {
        let mut raw = data_split(iceberg_split());
        raw.split_weight_raw = 0;
        let error = ValidatedConnectorSplit::parse(raw, root()).expect_err("zero weight");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(error.path().to_string(), "connector_split.split_weight_raw");
    }

    #[test]
    fn a_zero_port_address_is_rejected() {
        let mut raw = data_split(iceberg_split());
        raw.addresses[0].port = 0;
        let error = ValidatedConnectorSplit::parse(raw, root()).expect_err("zero port");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "connector_split.addresses[0].port"
        );
    }

    #[test]
    fn orc_and_avro_data_files_are_unsupported() {
        for format in [dto::IcebergFileFormat::Orc, dto::IcebergFileFormat::Avro] {
            let mut split = iceberg_split();
            split.file_format = format as i32;
            let error = ValidatedConnectorSplit::parse(data_split(split), root())
                .expect_err("unreadable format");
            assert_eq!(error.kind(), ProtocolErrorKind::Unsupported);
            assert_eq!(
                error.path().to_string(),
                "connector_split.data.iceberg.file_format"
            );
        }
    }

    #[test]
    fn an_unspecified_data_file_format_is_an_invalid_enum() {
        let mut split = iceberg_split();
        split.file_format = dto::IcebergFileFormat::Unspecified as i32;
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("unspecified");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidEnum);
    }

    #[test]
    fn a_non_empty_decryption_blob_is_unsupported_and_never_echoed() {
        let mut split = iceberg_split();
        split.decryption_data = Some(dto::ParquetFileDecryptionData {
            key_metadata: b"SUPER-SECRET-KEY-MATERIAL".to_vec(),
            aad_prefix: Vec::new(),
        });
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("encrypted");
        assert_eq!(error.kind(), ProtocolErrorKind::Unsupported);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.decryption_data.key_metadata"
        );
        assert_eq!(
            error.detail(),
            "parquet modular encryption is not supported by this contract"
        );
        assert!(!error.detail().contains("SECRET"));
        assert!(!error.to_string().contains("SECRET"));

        let mut aad = iceberg_split();
        aad.decryption_data = Some(dto::ParquetFileDecryptionData {
            key_metadata: Vec::new(),
            aad_prefix: b"SUPER-SECRET-AAD".to_vec(),
        });
        let error = ValidatedConnectorSplit::parse(data_split(aad), root()).expect_err("aad");
        assert_eq!(error.kind(), ProtocolErrorKind::Unsupported);
        assert!(!error.to_string().contains("SECRET"));
    }

    #[test]
    fn an_empty_decryption_extension_point_is_accepted() {
        let mut split = iceberg_split();
        split.decryption_data = Some(dto::ParquetFileDecryptionData::default());
        ValidatedConnectorSplit::parse(data_split(split), root()).expect("empty extension point");
    }

    #[test]
    fn a_range_past_the_file_size_is_rejected() {
        let mut split = iceberg_split();
        split.start = 4096;
        split.length = 4097;
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("past file size");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.length"
        );
        assert_eq!(error.detail(), "split range extends past the file size");
    }

    #[test]
    fn a_negative_offset_is_rejected() {
        let mut split = iceberg_split();
        split.start = -1;
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("negative start");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.start"
        );
    }

    #[test]
    fn an_overflowing_start_plus_length_is_rejected() {
        let mut split = iceberg_split();
        split.start = i64::MAX;
        split.length = 1;
        split.file_size = i64::MAX;
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("overflow");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.length"
        );
        assert!(error.detail().contains("overflows"));
    }

    #[test]
    fn an_absent_file_statistics_domain_is_a_missing_field() {
        let mut split = iceberg_split();
        split.file_statistics_domain = None;
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("no statistics");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.file_statistics_domain"
        );
    }

    #[test]
    fn a_nested_statistics_error_keeps_its_own_field_path() {
        let mut split = iceberg_split();
        split.file_statistics_domain = Some(dto::TupleDomain {
            none: true,
            column_domains: vec![dto::ColumnDomain {
                column: None,
                domain: None,
            }],
        });
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("bad statistics");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.file_statistics_domain.column_domains"
        );
    }

    #[test]
    fn a_duplicate_delete_path_is_rejected() {
        let mut split = iceberg_split();
        split.deletes = vec![position_delete(), position_delete()];
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("duplicate");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.deletes[1].path"
        );
    }

    #[test]
    fn equality_deletes_require_equality_field_ids() {
        let mut delete = equality_delete();
        delete.equality_field_ids = Vec::new();
        let mut split = iceberg_split();
        split.deletes = vec![delete];
        let error = ValidatedConnectorSplit::parse(data_split(split), root())
            .expect_err("equality without ids");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.deletes[0].equality_field_ids"
        );
    }

    #[test]
    fn position_deletes_reject_equality_field_ids() {
        let mut delete = position_delete();
        delete.equality_field_ids = vec![1];
        let mut split = iceberg_split();
        split.deletes = vec![delete];
        let error = ValidatedConnectorSplit::parse(data_split(split), root())
            .expect_err("position with ids");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.deletes[0].equality_field_ids"
        );
    }

    #[test]
    fn equality_field_ids_must_be_positive_and_unique() {
        let mut nonpositive = equality_delete();
        nonpositive.equality_field_ids = vec![0];
        let mut split = iceberg_split();
        split.deletes = vec![nonpositive];
        let error = ValidatedConnectorSplit::parse(data_split(split), root())
            .expect_err("nonpositive field id");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);

        let mut duplicated = equality_delete();
        duplicated.equality_field_ids = vec![3, 1, 3];
        let mut split = iceberg_split();
        split.deletes = vec![duplicated];
        let error = ValidatedConnectorSplit::parse(data_split(split), root())
            .expect_err("duplicate field id");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.deletes[0].equality_field_ids[2]"
        );
    }

    #[test]
    fn a_half_present_content_range_is_rejected() {
        let mut offset_only = deletion_vector();
        offset_only.content_size_in_bytes = None;
        let mut split = iceberg_split();
        split.deletes = vec![offset_only];
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("offset only");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.deletes[0].content_size_in_bytes"
        );

        let mut size_only = deletion_vector();
        size_only.content_offset = None;
        let mut split = iceberg_split();
        split.deletes = vec![size_only];
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("size only");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.deletes[0].content_offset"
        );
    }

    #[test]
    fn a_parquet_position_delete_cannot_carry_a_content_range() {
        let mut delete = deletion_vector();
        delete.format = dto::IcebergFileFormat::Parquet as i32;
        let mut split = iceberg_split();
        split.deletes = vec![delete];
        let error = ValidatedConnectorSplit::parse(data_split(split), root())
            .expect_err("parquet content range");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.deletes[0].content_offset"
        );
    }

    #[test]
    fn a_content_range_past_the_delete_file_size_is_rejected() {
        let mut delete = deletion_vector();
        delete.content_offset = Some(1000);
        delete.content_size_in_bytes = Some(25);
        delete.file_size_in_bytes = 1024;
        let mut split = iceberg_split();
        split.deletes = vec![delete];
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("past size");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.deletes[0].content_size_in_bytes"
        );
    }

    #[test]
    fn inverted_row_position_bounds_are_rejected() {
        let mut delete = position_delete();
        delete.row_position_lower_bound = Some(10);
        delete.row_position_upper_bound = Some(9);
        let mut split = iceberg_split();
        split.deletes = vec![delete];
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("inverted");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.deletes[0].row_position_upper_bound"
        );
    }

    #[test]
    fn a_table_changes_split_validates_its_change_type_and_range() {
        let split = dto::TableChangesSplit {
            change_type: dto::TableChangesChangeType::AddedFile as i32,
            snapshot_id: 42,
            snapshot_timestamp_millis: 1_700_000_000_000,
            change_ordinal: 0,
            path: "s3://bucket/table/data/0002.parquet".to_owned(),
            start: 0,
            length: 512,
            file_size: 512,
            file_record_count: 4,
            file_format: dto::IcebergFileFormat::Parquet as i32,
            partition_spec_id: 0,
            partition_data_json: "{}".to_owned(),
            decryption_data: None,
        };
        let category =
            dto::connector_split::Category::TableChanges(dto::TableChangesSplitCategory {
                provider: Some(dto::table_changes_split_category::Provider::Iceberg(
                    split.clone(),
                )),
            });
        let validated =
            ValidatedConnectorSplit::parse(envelope(category), root()).expect("valid changes");
        assert_eq!(validated.category(), SplitCategory::TableChanges);

        let mut unspecified = split;
        unspecified.change_type = dto::TableChangesChangeType::Unspecified as i32;
        let category =
            dto::connector_split::Category::TableChanges(dto::TableChangesSplitCategory {
                provider: Some(dto::table_changes_split_category::Provider::Iceberg(
                    unspecified,
                )),
            });
        let error = ValidatedConnectorSplit::parse(envelope(category), root())
            .expect_err("unspecified change type");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidEnum);
        assert_eq!(
            error.path().to_string(),
            "connector_split.table_changes.iceberg.change_type"
        );
    }

    #[test]
    fn every_change_split_variant_validates_its_embedded_split() {
        let variants = [
            dto::iceberg_change_split::Rows::AddedRows(dto::IcebergAddedRows {
                data: Some(iceberg_split()),
                restricted_row_ids: vec![0, 3, 9],
            }),
            dto::iceberg_change_split::Rows::PositionDeletedRows(dto::IcebergPositionDeletedRows {
                data: Some(iceberg_split()),
                newly_applied_deletes: vec![deletion_vector()],
                previously_applied_deletes: vec![position_delete()],
            }),
            dto::iceberg_change_split::Rows::EqualityDeletedRows(dto::IcebergEqualityDeletedRows {
                data: Some(iceberg_split()),
                newly_applied_equality_deletes: vec![equality_delete()],
                previously_applied_deletes: vec![position_delete()],
            }),
            dto::iceberg_change_split::Rows::DeletedDataFileRows(dto::IcebergDeletedDataFileRows {
                data: Some(iceberg_split()),
                previously_applied_deletes: vec![position_delete()],
            }),
        ];
        for rows in variants {
            let validated = ValidatedConnectorSplit::parse(envelope(change_category(rows)), root())
                .expect("valid change split");
            assert_eq!(validated.category(), SplitCategory::ChangeWindow);
        }

        let absent = envelope(dto::connector_split::Category::ChangeWindow(
            dto::ChangeWindowSplitCategory {
                provider: Some(dto::change_window_split_category::Provider::Iceberg(
                    dto::IcebergChangeSplit { rows: None },
                )),
            },
        ));
        let error = ValidatedConnectorSplit::parse(absent, root()).expect_err("absent rows");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "connector_split.change_window.iceberg"
        );

        let bad_data = envelope(change_category(
            dto::iceberg_change_split::Rows::DeletedDataFileRows(dto::IcebergDeletedDataFileRows {
                data: None,
                previously_applied_deletes: Vec::new(),
            }),
        ));
        let error = ValidatedConnectorSplit::parse(bad_data, root()).expect_err("absent data");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "connector_split.change_window.iceberg.deleted_data_file_rows.data"
        );
    }

    #[test]
    fn duplicate_restricted_row_ids_are_rejected() {
        let rows = dto::iceberg_change_split::Rows::AddedRows(dto::IcebergAddedRows {
            data: Some(iceberg_split()),
            restricted_row_ids: vec![1, 1],
        });
        let error = ValidatedConnectorSplit::parse(envelope(change_category(rows)), root())
            .expect_err("duplicate row id");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.change_window.iceberg.added_rows.restricted_row_ids[1]"
        );
    }

    #[test]
    fn a_files_table_split_rejects_a_non_empty_manifest_key_metadata() {
        let validated =
            ValidatedConnectorSplit::parse(envelope(files_category(manifest())), root())
                .expect("valid files split");
        assert_eq!(validated.category(), SplitCategory::SystemFiles);

        let mut encrypted = manifest();
        encrypted.key_metadata = b"wrapped-key".to_vec();
        let error = ValidatedConnectorSplit::parse(envelope(files_category(encrypted)), root())
            .expect_err("encrypted manifest");
        assert_eq!(error.kind(), ProtocolErrorKind::Unsupported);
        assert_eq!(
            error.path().to_string(),
            "connector_split.system_files.iceberg.manifest.key_metadata"
        );
    }

    #[test]
    fn a_manifest_requires_a_positive_length_and_known_content() {
        let mut empty = manifest();
        empty.length = 0;
        let error = ValidatedConnectorSplit::parse(envelope(files_category(empty)), root())
            .expect_err("zero length");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "connector_split.system_files.iceberg.manifest.length"
        );

        let mut unknown = manifest();
        unknown.content = 3;
        let error = ValidatedConnectorSplit::parse(envelope(files_category(unknown)), root())
            .expect_err("unknown content");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "connector_split.system_files.iceberg.manifest.content"
        );
    }

    #[test]
    fn a_rewrite_split_selects_puffin_deletion_vectors_only() {
        let validated = ValidatedConnectorSplit::parse(
            envelope(rewrite_category(vec![deletion_vector()])),
            root(),
        )
        .expect("valid rewrite split");
        assert_eq!(
            validated.category(),
            SplitCategory::RewritePositionDeleteFiles
        );

        let error = ValidatedConnectorSplit::parse(
            envelope(rewrite_category(vec![position_delete()])),
            root(),
        )
        .expect_err("whole-file delete");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.rewrite_position_delete_files.iceberg.selected_position_deletes[0].content_offset"
        );

        let error = ValidatedConnectorSplit::parse(envelope(rewrite_category(Vec::new())), root())
            .expect_err("no deletes");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "connector_split.rewrite_position_delete_files.iceberg.selected_position_deletes"
        );
    }

    #[test]
    fn a_rewrite_split_rejects_an_equality_delete() {
        let mut delete = equality_delete();
        delete.content_offset = Some(4);
        delete.content_size_in_bytes = Some(64);
        delete.format = dto::IcebergFileFormat::Avro as i32;
        let error =
            ValidatedConnectorSplit::parse(envelope(rewrite_category(vec![delete])), root())
                .expect_err("equality delete");
        // The content range is refused before the rewrite-local rule, because a
        // range on an equality delete is malformed for every reader.
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_split.rewrite_position_delete_files.iceberg.selected_position_deletes[0].content_offset"
        );
    }

    #[test]
    fn an_oversized_split_is_rejected_before_its_contents_are_walked() {
        let mut split = iceberg_split();
        // Larger than the whole-split budget and than the JSON bound, so only
        // the encoded-size gate can produce this rejection.
        split.partition_data_json = "x".repeat(MAX_SPLIT_ENCODED_BYTES + 1);
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("oversized");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(error.path().to_string(), "connector_split");
    }

    #[test]
    fn a_split_exceeding_the_aggregate_scalar_budget_is_rejected() {
        let mut split = iceberg_split();
        // Individually legal JSON, over the whole-split scalar budget.
        split.partition_data_json = "x".repeat(MAX_SPLIT_SCALAR_TOTAL_BYTES + 1);
        assert!(split.partition_data_json.len() <= MAX_JSON_BYTES);
        let error =
            ValidatedConnectorSplit::parse(data_split(split), root()).expect_err("scalar budget");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "connector_split.data.iceberg.partition_data_json"
        );
    }

    #[test]
    fn the_scalar_budget_accumulates_across_delete_files() {
        let mut split = iceberg_split();
        // Each path is individually legal; together they exhaust the budget.
        let per_delete = MAX_PATH_BYTES;
        let count = MAX_SPLIT_SCALAR_TOTAL_BYTES / per_delete + 2;
        split.deletes = (0..count)
            .map(|index| dto::IcebergDeleteFile {
                path: format!("s3://bucket/{index:0>width$}", width = per_delete - 12),
                ..position_delete()
            })
            .collect();
        let error = ValidatedConnectorSplit::parse(data_split(split), root())
            .expect_err("accumulated scalars");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert!(error.detail().contains("in total"));
    }
}
