//! Validated carriers for the closed connector handle families.
//!
//! Every family is a newtype over its generated message: holding one is the
//! proof that presence, bounds, known enums, uniqueness, and the cross-field
//! rules held at ingress. Protocol validates structure only -- which provider a
//! variant belongs to, and what it means, stays with that provider.

use std::collections::{BTreeMap, BTreeSet};

use novarocks_proto_models::connector_read as dto;
use novarocks_spi::connector::CatalogHandle;

use crate::catalog::decode_catalog_handle;
use crate::{FieldPath, ProtocolError};

use super::{
    MAX_JSON_BYTES, MAX_NAME_BYTES, MAX_PATH_BYTES, MAX_SCHEMA_TABLE_NAME_BYTES,
    ValidatedColumnHandle, bounded_text, decode_tuple_domain, exact_bytes, inconsistent, invalid,
    invalid_enum, missing, nest, nonnegative_i64, out_of_range, unsupported,
};

// Hard bounds owned by the handle families. Each one is a wire-visible budget:
// exceeding it is a typed rejection before any catalog or object-store I/O.
const MAX_HANDLE_COLUMNS: usize = 4096;
const MAX_PARTITION_SPECS: usize = 4096;
const MAX_STORAGE_PROPERTIES: usize = 256;
const MAX_PINNED_DATA_FILES: usize = 4096;

const TRANSACTION_UUID_BYTES: usize = 16;
const ARTIFACT_DIGEST_HEX_CHARS: usize = 64;
const UUID_TEXT_CHARS: usize = 36;
const UUID_GROUP_CHARS: [usize; 5] = [8, 4, 4, 4, 12];
const MIN_FORMAT_VERSION: i32 = 1;
const MAX_FORMAT_VERSION: i32 = 3;

/// Substrings that mark a storage property as credential material.
///
/// A connector session or handle never carries a secret: a worker resolves
/// credentials from its own authorized configuration, so a credential-shaped
/// key on the wire is a contract violation rather than an accepted input.
const CREDENTIAL_KEY_MARKERS: [&str; 6] = [
    "secret",
    "password",
    "token",
    "credential",
    "access-key",
    "access_key",
];

// ---------------------------------------------------------------------------
// Shared field validators
// ---------------------------------------------------------------------------

fn validate_schema_table_name(
    raw: Option<&dto::SchemaTableName>,
    path: FieldPath,
    detail: &'static str,
) -> Result<(), ProtocolError> {
    let name = raw.ok_or_else(|| missing(path.clone(), detail))?;
    bounded_text(
        &name.schema_name,
        MAX_SCHEMA_TABLE_NAME_BYTES,
        path.field("schema_name"),
        false,
    )?;
    bounded_text(
        &name.table_name,
        MAX_SCHEMA_TABLE_NAME_BYTES,
        path.field("table_name"),
        false,
    )
}

fn validate_format_version(value: i32, path: FieldPath) -> Result<(), ProtocolError> {
    if !(MIN_FORMAT_VERSION..=MAX_FORMAT_VERSION).contains(&value) {
        return Err(out_of_range(
            path,
            "iceberg format version must be 1, 2, or 3",
        ));
    }
    Ok(())
}

fn validate_name_mapping_json(raw: Option<&str>, path: FieldPath) -> Result<(), ProtocolError> {
    match raw {
        None => Ok(()),
        Some(json) => bounded_text(json, MAX_JSON_BYTES, path, true),
    }
}

/// Decode a required tuple domain purely to prove it is structurally sound.
///
/// `decode_tuple_domain` roots its own path, so its errors are re-rooted onto
/// this field rather than rebuilt.
fn validate_tuple_domain(
    raw: Option<&dto::TupleDomain>,
    path: FieldPath,
    detail: &'static str,
) -> Result<(), ProtocolError> {
    let domain = raw.ok_or_else(|| missing(path.clone(), detail))?;
    decode_tuple_domain(domain, FieldPath::root("tuple_domain"))
        .map_err(|error| nest(path, error))?;
    Ok(())
}

fn validate_iceberg_columns(
    columns: &[dto::IcebergColumnHandle],
    path: FieldPath,
    allow_empty: bool,
    empty_detail: &'static str,
) -> Result<(), ProtocolError> {
    if !allow_empty && columns.is_empty() {
        return Err(invalid(path, empty_detail));
    }
    if columns.len() > MAX_HANDLE_COLUMNS {
        return Err(out_of_range(path, "column count exceeds the hard limit"));
    }
    let mut seen = BTreeSet::new();
    for (index, column) in columns.iter().enumerate() {
        let entry = path.index(index);
        // Validate and key through the closed column-handle carrier: "the same
        // column twice" must be decided by the same canonical bytes the
        // predicate side keys on, not by a second notion of column identity.
        let validated = ValidatedColumnHandle::parse(
            dto::ColumnHandle {
                handle: Some(dto::column_handle::Handle::Iceberg(column.clone())),
            },
            entry.clone(),
        )?;
        if !seen.insert(validated) {
            return Err(inconsistent(
                entry,
                "column list contains a duplicate column",
            ));
        }
    }
    Ok(())
}

fn validate_partition_spec_jsons(
    specs: &BTreeMap<i32, String>,
    spec_id: Option<i32>,
    path: &FieldPath,
) -> Result<(), ProtocolError> {
    let specs_path = path.field("partition_spec_jsons");
    if specs.len() > MAX_PARTITION_SPECS {
        return Err(out_of_range(
            specs_path,
            "partition spec count exceeds the hard limit",
        ));
    }
    // Connector-read maps are generated as BTreeMap, so this walk is in the
    // same key order the message encodes in: the same bytes always fail at the
    // same field.
    for (id, json) in specs {
        bounded_text(
            json,
            MAX_JSON_BYTES,
            specs_path.map_key(id.to_string()),
            false,
        )?;
    }
    // A spec id that names no carried spec would leave the worker to resolve
    // the partition spec through the catalog, which is exactly the fallback
    // this handle exists to remove.
    if let Some(spec_id) = spec_id
        && !specs.contains_key(&spec_id)
    {
        return Err(inconsistent(
            path.field("spec_id"),
            "spec id does not name a carried partition spec",
        ));
    }
    Ok(())
}

fn validate_storage_properties(
    properties: &BTreeMap<String, String>,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if properties.len() > MAX_STORAGE_PROPERTIES {
        return Err(out_of_range(
            path,
            "storage property count exceeds the hard limit",
        ));
    }
    for (key, value) in properties {
        // A key only enters the error path once it is known bounded, so an
        // oversized key cannot become an unbounded field path.
        bounded_text(key, MAX_NAME_BYTES, path.clone(), false)?;
        let entry = path.map_key(key.clone());
        if is_credential_key(key) {
            return Err(unsupported(
                entry,
                "storage property key names a credential; a connector handle never carries a secret",
            ));
        }
        bounded_text(value, MAX_PATH_BYTES, entry, true)?;
    }
    Ok(())
}

fn is_credential_key(key: &str) -> bool {
    let lowered = key.to_ascii_lowercase();
    CREDENTIAL_KEY_MARKERS
        .iter()
        .any(|marker| lowered.contains(marker))
}

/// A frozen artifact digest is content identity produced elsewhere. Protocol
/// proves only the spelling; it never derives or re-derives a digest.
fn validate_lowercase_hex(
    value: &str,
    expected_chars: usize,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    if value.len() != expected_chars || !value.bytes().all(is_lowercase_hex_digit) {
        return Err(invalid(
            path,
            format!("value must be exactly {expected_chars} lowercase hex characters"),
        ));
    }
    Ok(())
}

const fn is_lowercase_hex_digit(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')
}

/// The table UUID is metadata identity the selected backend re-checks against
/// the metadata file, so the wire must carry the one canonical spelling. The
/// shape is checked directly rather than through a UUID parser, which would
/// accept braced, urn, and uppercase forms.
fn validate_canonical_uuid_text(value: &str, path: FieldPath) -> Result<(), ProtocolError> {
    let groups = value.split('-').collect::<Vec<_>>();
    let canonical = value.len() == UUID_TEXT_CHARS
        && groups.len() == UUID_GROUP_CHARS.len()
        && groups.iter().zip(UUID_GROUP_CHARS).all(|(group, chars)| {
            group.len() == chars && group.bytes().all(is_lowercase_hex_digit)
        });
    if !canonical {
        return Err(invalid(
            path,
            "value must be a canonical lowercase 36-character uuid",
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction handles
// ---------------------------------------------------------------------------

fn validate_connector_transaction_handle(
    raw: &dto::ConnectorTransactionHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let handle = raw
        .handle
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "transaction handle variant must be present"))?;
    match handle {
        dto::connector_transaction_handle::Handle::Iceberg(iceberg) => exact_bytes(
            &iceberg.uuid,
            TRANSACTION_UUID_BYTES,
            path.field("iceberg").field("uuid"),
        ),
    }
}

/// A validated transaction marker. The frontend transaction manager is its only
/// owner; a worker carries it and never resolves it.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedTransactionHandle {
    raw: dto::ConnectorTransactionHandle,
}

impl ValidatedTransactionHandle {
    pub fn parse(
        raw: dto::ConnectorTransactionHandle,
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        validate_connector_transaction_handle(&raw, path)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &dto::ConnectorTransactionHandle {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorTransactionHandle {
        self.raw
    }

    fn iceberg(&self) -> &dto::HiveTransactionHandle {
        match self.raw.handle.as_ref() {
            Some(dto::connector_transaction_handle::Handle::Iceberg(iceberg)) => iceberg,
            None => unreachable!("a validated transaction handle always carries a variant"),
        }
    }

    pub fn auto_commit(&self) -> bool {
        self.iceberg().auto_commit
    }

    pub fn uuid(&self) -> [u8; TRANSACTION_UUID_BYTES] {
        let mut bytes = [0_u8; TRANSACTION_UUID_BYTES];
        bytes.copy_from_slice(&self.iceberg().uuid);
        bytes
    }
}

// ---------------------------------------------------------------------------
// Table handles
// ---------------------------------------------------------------------------

/// A pinned file set names the exact files one read may touch.
///
/// The list is bounded because it is the only unbounded-by-nature field on a
/// table handle: a cohort's list is small, and one that is not is a wire
/// hazard rather than a larger rewrite. Sortedness and uniqueness are checked
/// rather than repaired: two spellings of the same file, or a list whose order
/// depends on who built it, would make the same pinned read two different
/// reads.
fn validate_pinned_data_file_set(
    raw: Option<&dto::IcebergPinnedDataFileSet>,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let Some(pinned) = raw else {
        return Ok(());
    };
    if pinned.paths.len() > MAX_PINNED_DATA_FILES {
        return Err(out_of_range(
            path,
            "pinned data file count exceeds the hard limit",
        ));
    }
    for (ordinal, file) in pinned.paths.iter().enumerate() {
        bounded_text(file, MAX_PATH_BYTES, path.index(ordinal), false)?;
    }
    if pinned.paths.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(inconsistent(
            path,
            "pinned data file paths must be sorted and unique",
        ));
    }
    Ok(())
}

fn validate_iceberg_table_handle(
    raw: &dto::IcebergTableHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    validate_schema_table_name(
        raw.schema_table_name.as_ref(),
        path.field("schema_table_name"),
        "iceberg table handle requires a schema table name",
    )?;
    if let Some(snapshot_id) = raw.snapshot_id {
        nonnegative_i64(snapshot_id, path.field("snapshot_id"), "snapshot id")?;
    }
    bounded_text(
        &raw.table_schema_json,
        MAX_JSON_BYTES,
        path.field("table_schema_json"),
        false,
    )?;
    validate_name_mapping_json(
        raw.name_mapping_json.as_deref(),
        path.field("name_mapping_json"),
    )?;
    validate_format_version(raw.format_version, path.field("format_version"))?;
    validate_partition_spec_jsons(&raw.partition_spec_jsons, raw.spec_id, &path)?;
    validate_tuple_domain(
        raw.unenforced_predicate.as_ref(),
        path.field("unenforced_predicate"),
        "iceberg table handle requires an unenforced predicate",
    )?;
    validate_tuple_domain(
        raw.enforced_predicate.as_ref(),
        path.field("enforced_predicate"),
        "iceberg table handle requires an enforced predicate",
    )?;
    validate_iceberg_columns(
        &raw.projected_columns,
        path.field("projected_columns"),
        true,
        "iceberg table handle requires projected columns",
    )?;
    bounded_text(
        &raw.table_location,
        MAX_PATH_BYTES,
        path.field("table_location"),
        false,
    )?;
    validate_pinned_data_file_set(
        raw.pinned_data_files.as_ref(),
        path.field("pinned_data_files"),
    )?;
    validate_storage_properties(&raw.storage_properties, path.field("storage_properties"))
}

fn validate_connector_table_handle(
    raw: &dto::ConnectorTableHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let handle = raw
        .handle
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "table handle variant must be present"))?;
    match handle {
        dto::connector_table_handle::Handle::Iceberg(iceberg) => {
            validate_iceberg_table_handle(iceberg, path.field("iceberg"))
        }
    }
}

/// A validated worker-visible data-relation handle.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedConnectorTableHandle {
    raw: dto::ConnectorTableHandle,
}

impl ValidatedConnectorTableHandle {
    pub fn parse(raw: dto::ConnectorTableHandle, path: FieldPath) -> Result<Self, ProtocolError> {
        validate_connector_table_handle(&raw, path)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &dto::ConnectorTableHandle {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorTableHandle {
        self.raw
    }

    pub fn iceberg(&self) -> &dto::IcebergTableHandle {
        match self.raw.handle.as_ref() {
            Some(dto::connector_table_handle::Handle::Iceberg(iceberg)) => iceberg,
            None => unreachable!("a validated table handle always carries a variant"),
        }
    }

    /// Validated to 1, 2, or 3.
    pub fn format_version(&self) -> u8 {
        self.iceberg().format_version as u8
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.iceberg().snapshot_id
    }
}

// ---------------------------------------------------------------------------
// Table function handles
// ---------------------------------------------------------------------------

/// Both endpoints name real snapshots, and a window whose endpoints are the
/// same snapshot describes no rows: accepting it would silently turn an empty
/// window into a full rescan.
fn validate_snapshot_endpoints(
    lower: i64,
    upper: i64,
    lower_path: FieldPath,
    upper_path: FieldPath,
    detail: &'static str,
) -> Result<(), ProtocolError> {
    nonnegative_i64(lower, lower_path, "snapshot id")?;
    nonnegative_i64(upper, upper_path.clone(), "snapshot id")?;
    if lower == upper {
        return Err(inconsistent(upper_path, detail));
    }
    Ok(())
}

fn validate_table_changes_function_handle(
    raw: &dto::TableChangesFunctionHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    validate_schema_table_name(
        raw.schema_table_name.as_ref(),
        path.field("schema_table_name"),
        "table changes function handle requires a schema table name",
    )?;
    bounded_text(
        &raw.table_schema_json,
        MAX_JSON_BYTES,
        path.field("table_schema_json"),
        false,
    )?;
    validate_name_mapping_json(
        raw.name_mapping_json.as_deref(),
        path.field("name_mapping_json"),
    )?;
    validate_iceberg_columns(
        &raw.columns,
        path.field("columns"),
        false,
        "table changes function handle requires at least one output column",
    )?;
    validate_snapshot_endpoints(
        raw.start_snapshot_id,
        raw.end_snapshot_id,
        path.field("start_snapshot_id"),
        path.field("end_snapshot_id"),
        "table changes endpoints must name two different snapshots",
    )
}

fn validate_connector_table_function_handle(
    raw: &dto::ConnectorTableFunctionHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let handle = raw.handle.as_ref().ok_or_else(|| {
        missing(
            path.clone(),
            "table function handle variant must be present",
        )
    })?;
    match handle {
        dto::connector_table_function_handle::Handle::IcebergTableChanges(changes) => {
            validate_table_changes_function_handle(changes, path.field("iceberg_table_changes"))
        }
    }
}

/// A validated table-function relation handle.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedConnectorTableFunctionHandle {
    raw: dto::ConnectorTableFunctionHandle,
}

impl ValidatedConnectorTableFunctionHandle {
    pub fn parse(
        raw: dto::ConnectorTableFunctionHandle,
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        validate_connector_table_function_handle(&raw, path)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &dto::ConnectorTableFunctionHandle {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorTableFunctionHandle {
        self.raw
    }

    pub fn iceberg_table_changes(&self) -> &dto::TableChangesFunctionHandle {
        match self.raw.handle.as_ref() {
            Some(dto::connector_table_function_handle::Handle::IcebergTableChanges(changes)) => {
                changes
            }
            None => unreachable!("a validated table function handle always carries a variant"),
        }
    }

    pub fn start_snapshot_id(&self) -> i64 {
        self.iceberg_table_changes().start_snapshot_id
    }

    pub fn end_snapshot_id(&self) -> i64 {
        self.iceberg_table_changes().end_snapshot_id
    }
}

// ---------------------------------------------------------------------------
// Change-window handles
// ---------------------------------------------------------------------------

fn validate_iceberg_change_window_handle(
    raw: &dto::IcebergChangeWindowHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    validate_schema_table_name(
        raw.schema_table_name.as_ref(),
        path.field("schema_table_name"),
        "iceberg change window handle requires a schema table name",
    )?;
    bounded_text(
        &raw.table_schema_json,
        MAX_JSON_BYTES,
        path.field("table_schema_json"),
        false,
    )?;
    validate_name_mapping_json(
        raw.name_mapping_json.as_deref(),
        path.field("name_mapping_json"),
    )?;
    validate_iceberg_columns(
        &raw.columns,
        path.field("columns"),
        false,
        "iceberg change window handle requires at least one output column",
    )?;
    validate_snapshot_endpoints(
        raw.from_snapshot_id_exclusive,
        raw.to_snapshot_id_inclusive,
        path.field("from_snapshot_id_exclusive"),
        path.field("to_snapshot_id_inclusive"),
        "change window endpoints must name two different snapshots",
    )?;
    // No single selected spec: a window's splits each name their own, so there
    // is nothing here for a `spec_id` to point at.
    validate_partition_spec_jsons(&raw.partition_spec_jsons, None, &path)
}

fn validate_connector_change_window_handle(
    raw: &dto::ConnectorChangeWindowHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let handle = raw
        .handle
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "change window handle variant must be present"))?;
    match handle {
        dto::connector_change_window_handle::Handle::Iceberg(iceberg) => {
            validate_iceberg_change_window_handle(iceberg, path.field("iceberg"))
        }
    }
}

/// A validated incremental change-window relation handle.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedConnectorChangeWindowHandle {
    raw: dto::ConnectorChangeWindowHandle,
}

impl ValidatedConnectorChangeWindowHandle {
    pub fn parse(
        raw: dto::ConnectorChangeWindowHandle,
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        validate_connector_change_window_handle(&raw, path)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &dto::ConnectorChangeWindowHandle {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorChangeWindowHandle {
        self.raw
    }

    pub fn iceberg(&self) -> &dto::IcebergChangeWindowHandle {
        match self.raw.handle.as_ref() {
            Some(dto::connector_change_window_handle::Handle::Iceberg(iceberg)) => iceberg,
            None => unreachable!("a validated change window handle always carries a variant"),
        }
    }

    /// The window's `from_snapshot_id_exclusive` endpoint.
    pub fn lower_snapshot_id_exclusive(&self) -> i64 {
        self.iceberg().from_snapshot_id_exclusive
    }

    /// The window's `to_snapshot_id_inclusive` endpoint.
    pub fn upper_snapshot_id_inclusive(&self) -> i64 {
        self.iceberg().to_snapshot_id_inclusive
    }
}

// ---------------------------------------------------------------------------
// System relation references
// ---------------------------------------------------------------------------

fn validate_iceberg_system_table_reference(
    raw: &dto::IcebergSystemTableReference,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    validate_schema_table_name(
        raw.schema_table_name.as_ref(),
        path.field("schema_table_name"),
        "iceberg system table reference requires a schema table name",
    )?;
    let system_table_type =
        dto::IcebergSystemTableType::try_from(raw.system_table_type).map_err(|_| {
            invalid_enum(
                path.field("system_table_type"),
                "unknown iceberg system table type",
            )
        })?;
    match system_table_type {
        dto::IcebergSystemTableType::Unspecified => {
            return Err(invalid_enum(
                path.field("system_table_type"),
                "iceberg system table type must be specified",
            ));
        }
        // The closed worker-visible set. There is no ALL_* variant, so a
        // pinned reference can never widen into a scan of every snapshot.
        dto::IcebergSystemTableType::Files
        | dto::IcebergSystemTableType::Entries
        | dto::IcebergSystemTableType::Snapshots
        | dto::IcebergSystemTableType::History
        | dto::IcebergSystemTableType::Refs
        | dto::IcebergSystemTableType::Manifests
        | dto::IcebergSystemTableType::Partitions => {}
    }
    bounded_text(
        &raw.metadata_file_location,
        MAX_PATH_BYTES,
        path.field("metadata_file_location"),
        false,
    )?;
    validate_canonical_uuid_text(&raw.table_uuid, path.field("table_uuid"))?;
    if let Some(snapshot_id) = raw.snapshot_id {
        nonnegative_i64(snapshot_id, path.field("snapshot_id"), "snapshot id")?;
    }
    Ok(())
}

fn validate_connector_system_table_reference(
    raw: &dto::ConnectorSystemTableReference,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let reference = raw.reference.as_ref().ok_or_else(|| {
        missing(
            path.clone(),
            "system table reference variant must be present",
        )
    })?;
    match reference {
        dto::connector_system_table_reference::Reference::Iceberg(iceberg) => {
            validate_iceberg_system_table_reference(iceberg, path.field("iceberg"))
        }
    }
}

/// A validated immutable system-relation reference.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedConnectorSystemTableReference {
    raw: dto::ConnectorSystemTableReference,
}

impl ValidatedConnectorSystemTableReference {
    pub fn parse(
        raw: dto::ConnectorSystemTableReference,
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        validate_connector_system_table_reference(&raw, path)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &dto::ConnectorSystemTableReference {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorSystemTableReference {
        self.raw
    }

    pub fn iceberg(&self) -> &dto::IcebergSystemTableReference {
        match self.raw.reference.as_ref() {
            Some(dto::connector_system_table_reference::Reference::Iceberg(iceberg)) => iceberg,
            None => unreachable!("a validated system table reference always carries a variant"),
        }
    }

    /// Validated to a known, non-`UNSPECIFIED` variant.
    pub fn system_table_type(&self) -> dto::IcebergSystemTableType {
        match dto::IcebergSystemTableType::try_from(self.iceberg().system_table_type) {
            Ok(system_table_type) => system_table_type,
            Err(_) => unreachable!("a validated system table reference always has a known type"),
        }
    }

    pub fn metadata_file_location(&self) -> &str {
        &self.iceberg().metadata_file_location
    }

    pub fn table_uuid(&self) -> &str {
        &self.iceberg().table_uuid
    }

    pub fn snapshot_id(&self) -> Option<i64> {
        self.iceberg().snapshot_id
    }
}

// ---------------------------------------------------------------------------
// Table execute handles
// ---------------------------------------------------------------------------

/// Which distributed procedure handle a procedure id demands. Everything else
/// runs on the coordinator and has no distributed split, so it must carry no
/// procedure handle at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequiredProcedureHandle {
    Optimize,
    RewritePositionDeleteFiles,
    None,
}

fn required_procedure_handle(
    procedure_id: dto::IcebergProcedureId,
    path: &FieldPath,
) -> Result<RequiredProcedureHandle, ProtocolError> {
    match procedure_id {
        dto::IcebergProcedureId::Unspecified => Err(invalid_enum(
            path.field("procedure_id"),
            "iceberg procedure id must be specified",
        )),
        dto::IcebergProcedureId::Optimize => Ok(RequiredProcedureHandle::Optimize),
        dto::IcebergProcedureId::RewritePositionDeleteFiles => {
            Ok(RequiredProcedureHandle::RewritePositionDeleteFiles)
        }
        dto::IcebergProcedureId::OptimizeManifests
        | dto::IcebergProcedureId::DropExtendedStats
        | dto::IcebergProcedureId::RollbackToSnapshot
        | dto::IcebergProcedureId::ExpireSnapshots
        | dto::IcebergProcedureId::RemoveOrphanFiles
        | dto::IcebergProcedureId::AddFiles
        | dto::IcebergProcedureId::AddFilesFromTable => Ok(RequiredProcedureHandle::None),
    }
}

fn validate_iceberg_optimize_handle(
    raw: &dto::IcebergOptimizeHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let table_handle = raw.table_handle.as_ref().ok_or_else(|| {
        missing(
            path.field("table_handle"),
            "iceberg optimize handle requires a table handle",
        )
    })?;
    validate_iceberg_table_handle(table_handle, path.field("table_handle"))
}

fn validate_iceberg_rewrite_position_delete_files_handle(
    raw: &dto::IcebergRewritePositionDeleteFilesHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let table_handle = raw.table_handle.as_ref().ok_or_else(|| {
        missing(
            path.field("table_handle"),
            "iceberg rewrite position delete files handle requires a table handle",
        )
    })?;
    validate_iceberg_table_handle(table_handle, path.field("table_handle"))?;
    let artifact = raw.artifact.as_ref().ok_or_else(|| {
        missing(
            path.field("artifact"),
            "iceberg rewrite position delete files handle requires an artifact",
        )
    })?;
    let artifact_path = path.field("artifact");
    bounded_text(
        &artifact.artifact_location,
        MAX_PATH_BYTES,
        artifact_path.field("artifact_location"),
        false,
    )?;
    validate_lowercase_hex(
        &artifact.artifact_digest_hex,
        ARTIFACT_DIGEST_HEX_CHARS,
        artifact_path.field("artifact_digest_hex"),
    )?;
    validate_lowercase_hex(
        &raw.group_digest_hex,
        ARTIFACT_DIGEST_HEX_CHARS,
        path.field("group_digest_hex"),
    )
}

fn validate_iceberg_table_execute_handle(
    raw: &dto::IcebergTableExecuteHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    validate_schema_table_name(
        raw.schema_table_name.as_ref(),
        path.field("schema_table_name"),
        "iceberg table execute handle requires a schema table name",
    )?;
    let procedure_id = dto::IcebergProcedureId::try_from(raw.procedure_id)
        .map_err(|_| invalid_enum(path.field("procedure_id"), "unknown iceberg procedure id"))?;
    let required = required_procedure_handle(procedure_id, &path)?;
    bounded_text(
        &raw.table_location,
        MAX_PATH_BYTES,
        path.field("table_location"),
        false,
    )?;
    // The procedure id and the procedure handle are one fact stated twice. A
    // disagreement means the coordinator and the worker would run different
    // procedures, so it is rejected instead of resolved in favour of either.
    match raw.procedure_handle.as_ref() {
        None => {
            if required != RequiredProcedureHandle::None {
                return Err(inconsistent(
                    path.field("procedure_handle"),
                    "this procedure id requires its own procedure handle",
                ));
            }
            Ok(())
        }
        Some(dto::iceberg_table_execute_handle::ProcedureHandle::Optimize(optimize)) => {
            if required != RequiredProcedureHandle::Optimize {
                return Err(inconsistent(
                    path.field("procedure_handle"),
                    "an optimize procedure handle requires the optimize procedure id",
                ));
            }
            validate_iceberg_optimize_handle(optimize, path.field("optimize"))
        }
        Some(dto::iceberg_table_execute_handle::ProcedureHandle::RewritePositionDeleteFiles(
            rewrite,
        )) => {
            if required != RequiredProcedureHandle::RewritePositionDeleteFiles {
                return Err(inconsistent(
                    path.field("procedure_handle"),
                    "a rewrite position delete files procedure handle requires the matching procedure id",
                ));
            }
            validate_iceberg_rewrite_position_delete_files_handle(
                rewrite,
                path.field("rewrite_position_delete_files"),
            )
        }
    }
}

fn validate_connector_table_execute_handle(
    raw: &dto::ConnectorTableExecuteHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let handle = raw
        .handle
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "table execute handle variant must be present"))?;
    match handle {
        dto::connector_table_execute_handle::Handle::Iceberg(iceberg) => {
            validate_iceberg_table_execute_handle(iceberg, path.field("iceberg"))
        }
    }
}

/// The distributed procedure handle a table-execute handle carries. A
/// coordinator-only procedure carries none, so the accessor is `Option`-shaped
/// rather than widening this closed set with an "absent" variant.
#[derive(Clone, Copy, Debug)]
pub enum TableExecuteProcedure<'a> {
    Optimize(&'a dto::IcebergOptimizeHandle),
    RewritePositionDeleteFiles(&'a dto::IcebergRewritePositionDeleteFilesHandle),
}

/// A validated table-execute relation handle.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedConnectorTableExecuteHandle {
    raw: dto::ConnectorTableExecuteHandle,
}

impl ValidatedConnectorTableExecuteHandle {
    pub fn parse(
        raw: dto::ConnectorTableExecuteHandle,
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        validate_connector_table_execute_handle(&raw, path)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &dto::ConnectorTableExecuteHandle {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorTableExecuteHandle {
        self.raw
    }

    pub fn iceberg(&self) -> &dto::IcebergTableExecuteHandle {
        match self.raw.handle.as_ref() {
            Some(dto::connector_table_execute_handle::Handle::Iceberg(iceberg)) => iceberg,
            None => unreachable!("a validated table execute handle always carries a variant"),
        }
    }

    /// Validated to a known, non-`UNSPECIFIED` procedure.
    pub fn procedure_id(&self) -> dto::IcebergProcedureId {
        match dto::IcebergProcedureId::try_from(self.iceberg().procedure_id) {
            Ok(procedure_id) => procedure_id,
            Err(_) => unreachable!("a validated table execute handle always has a known procedure"),
        }
    }

    /// `None` for the coordinator-only procedures, which have no distributed
    /// split and therefore carry no procedure handle.
    pub fn procedure(&self) -> Option<TableExecuteProcedure<'_>> {
        match self.iceberg().procedure_handle.as_ref() {
            None => None,
            Some(dto::iceberg_table_execute_handle::ProcedureHandle::Optimize(optimize)) => {
                Some(TableExecuteProcedure::Optimize(optimize))
            }
            Some(
                dto::iceberg_table_execute_handle::ProcedureHandle::RewritePositionDeleteFiles(
                    rewrite,
                ),
            ) => Some(TableExecuteProcedure::RewritePositionDeleteFiles(rewrite)),
        }
    }
}

// ---------------------------------------------------------------------------
// Merge table handles
// ---------------------------------------------------------------------------

fn validate_iceberg_insert_table_handle(
    raw: &dto::IcebergInsertTableHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    validate_schema_table_name(
        raw.schema_table_name.as_ref(),
        path.field("schema_table_name"),
        "iceberg insert table handle requires a schema table name",
    )?;
    bounded_text(
        &raw.table_schema_json,
        MAX_JSON_BYTES,
        path.field("table_schema_json"),
        false,
    )?;
    bounded_text(
        &raw.table_location,
        MAX_PATH_BYTES,
        path.field("table_location"),
        false,
    )?;
    validate_format_version(raw.format_version, path.field("format_version"))
}

fn validate_iceberg_merge_table_handle(
    raw: &dto::IcebergMergeTableHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let table_handle = raw.table_handle.as_ref().ok_or_else(|| {
        missing(
            path.field("table_handle"),
            "iceberg merge table handle requires a table handle",
        )
    })?;
    validate_iceberg_table_handle(table_handle, path.field("table_handle"))?;
    let insert_table_handle = raw.insert_table_handle.as_ref().ok_or_else(|| {
        missing(
            path.field("insert_table_handle"),
            "iceberg merge table handle requires an insert table handle",
        )
    })?;
    validate_iceberg_insert_table_handle(insert_table_handle, path.field("insert_table_handle"))
}

fn validate_connector_merge_table_handle(
    raw: &dto::ConnectorMergeTableHandle,
    path: FieldPath,
) -> Result<(), ProtocolError> {
    let handle = raw
        .handle
        .as_ref()
        .ok_or_else(|| missing(path.clone(), "merge table handle variant must be present"))?;
    match handle {
        dto::connector_merge_table_handle::Handle::Iceberg(iceberg) => {
            validate_iceberg_merge_table_handle(iceberg, path.field("iceberg"))
        }
    }
}

/// A validated merge relation handle.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedConnectorMergeTableHandle {
    raw: dto::ConnectorMergeTableHandle,
}

impl ValidatedConnectorMergeTableHandle {
    pub fn parse(
        raw: dto::ConnectorMergeTableHandle,
        path: FieldPath,
    ) -> Result<Self, ProtocolError> {
        validate_connector_merge_table_handle(&raw, path)?;
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &dto::ConnectorMergeTableHandle {
        &self.raw
    }

    pub fn into_proto(self) -> dto::ConnectorMergeTableHandle {
        self.raw
    }

    pub fn iceberg(&self) -> &dto::IcebergMergeTableHandle {
        match self.raw.handle.as_ref() {
            Some(dto::connector_merge_table_handle::Handle::Iceberg(iceberg)) => iceberg,
            None => unreachable!("a validated merge table handle always carries a variant"),
        }
    }

    pub fn table_handle(&self) -> &dto::IcebergTableHandle {
        match self.iceberg().table_handle.as_ref() {
            Some(table_handle) => table_handle,
            None => unreachable!("a validated merge table handle always carries a table handle"),
        }
    }

    pub fn insert_table_handle(&self) -> &dto::IcebergInsertTableHandle {
        match self.iceberg().insert_table_handle.as_ref() {
            Some(insert_table_handle) => insert_table_handle,
            None => unreachable!("a validated merge table handle always carries an insert handle"),
        }
    }
}

// ---------------------------------------------------------------------------
// The catalog-scoped relation
// ---------------------------------------------------------------------------

/// Which relation kind a catalog table handle names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorRelationKind {
    Table,
    TableFunction,
    ChangeWindow,
    SystemTable,
    TableExecute,
    MergeTable,
}

/// The one relation a validated catalog table handle names, borrowed from it.
///
/// Callers match this closed set instead of re-reading the generated oneof, so
/// a new relation kind is a compile error at every consumer rather than a
/// silently ignored variant.
#[derive(Clone, Copy, Debug)]
pub enum ConnectorRelation<'a> {
    Table(&'a dto::ConnectorTableHandle),
    TableFunction(&'a dto::ConnectorTableFunctionHandle),
    ChangeWindow(&'a dto::ConnectorChangeWindowHandle),
    SystemTable(&'a dto::ConnectorSystemTableReference),
    TableExecute(&'a dto::ConnectorTableExecuteHandle),
    MergeTable(&'a dto::ConnectorMergeTableHandle),
}

impl ConnectorRelation<'_> {
    pub fn kind(self) -> ConnectorRelationKind {
        match self {
            Self::Table(_) => ConnectorRelationKind::Table,
            Self::TableFunction(_) => ConnectorRelationKind::TableFunction,
            Self::ChangeWindow(_) => ConnectorRelationKind::ChangeWindow,
            Self::SystemTable(_) => ConnectorRelationKind::SystemTable,
            Self::TableExecute(_) => ConnectorRelationKind::TableExecute,
            Self::MergeTable(_) => ConnectorRelationKind::MergeTable,
        }
    }
}

/// The catalog-scoped, transaction-bound relation a scan reads.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogTableHandle {
    raw: dto::CatalogTableHandle,
    catalog_handle: CatalogHandle,
}

impl CatalogTableHandle {
    pub fn parse(raw: dto::CatalogTableHandle, path: FieldPath) -> Result<Self, ProtocolError> {
        let raw_catalog_handle = raw.catalog_handle.clone().ok_or_else(|| {
            missing(
                path.clone().field("catalog_handle"),
                "catalog table handle requires a catalog handle",
            )
        })?;
        bounded_text(
            &raw_catalog_handle.catalog_name,
            MAX_NAME_BYTES,
            path.clone().field("catalog_handle").field("catalog_name"),
            false,
        )?;
        let catalog_handle =
            decode_catalog_handle(raw_catalog_handle, path.clone().field("catalog_handle"))?;
        let transaction = raw.transaction.as_ref().ok_or_else(|| {
            missing(
                path.field("transaction"),
                "catalog table handle requires a transaction handle",
            )
        })?;
        validate_connector_transaction_handle(transaction, path.field("transaction"))?;
        let relation = raw.relation.as_ref().ok_or_else(|| {
            missing(
                path.clone(),
                "catalog table handle relation must be present",
            )
        })?;
        match relation {
            dto::catalog_table_handle::Relation::Table(handle) => {
                validate_connector_table_handle(handle, path.field("table"))?;
            }
            dto::catalog_table_handle::Relation::TableFunction(handle) => {
                validate_connector_table_function_handle(handle, path.field("table_function"))?;
            }
            dto::catalog_table_handle::Relation::ChangeWindow(handle) => {
                validate_connector_change_window_handle(handle, path.field("change_window"))?;
            }
            dto::catalog_table_handle::Relation::SystemTable(reference) => {
                validate_connector_system_table_reference(reference, path.field("system_table"))?;
            }
            dto::catalog_table_handle::Relation::TableExecute(handle) => {
                validate_connector_table_execute_handle(handle, path.field("table_execute"))?;
            }
            dto::catalog_table_handle::Relation::MergeTable(handle) => {
                validate_connector_merge_table_handle(handle, path.field("merge_table"))?;
            }
        }
        Ok(Self {
            raw,
            catalog_handle,
        })
    }

    pub const fn as_proto(&self) -> &dto::CatalogTableHandle {
        &self.raw
    }

    pub fn into_proto(self) -> dto::CatalogTableHandle {
        self.raw
    }

    /// The immutable catalog content identity this relation belongs to.
    pub const fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog_handle
    }

    /// The already-normalized connector instance id this relation belongs to.
    pub fn catalog_name(&self) -> &str {
        self.catalog_handle.catalog_name().as_str()
    }

    pub fn transaction(&self) -> &dto::ConnectorTransactionHandle {
        match self.raw.transaction.as_ref() {
            Some(transaction) => transaction,
            None => unreachable!("a validated catalog table handle always carries a transaction"),
        }
    }

    pub fn relation(&self) -> ConnectorRelation<'_> {
        match self.raw.relation.as_ref() {
            Some(dto::catalog_table_handle::Relation::Table(handle)) => {
                ConnectorRelation::Table(handle)
            }
            Some(dto::catalog_table_handle::Relation::TableFunction(handle)) => {
                ConnectorRelation::TableFunction(handle)
            }
            Some(dto::catalog_table_handle::Relation::ChangeWindow(handle)) => {
                ConnectorRelation::ChangeWindow(handle)
            }
            Some(dto::catalog_table_handle::Relation::SystemTable(reference)) => {
                ConnectorRelation::SystemTable(reference)
            }
            Some(dto::catalog_table_handle::Relation::TableExecute(handle)) => {
                ConnectorRelation::TableExecute(handle)
            }
            Some(dto::catalog_table_handle::Relation::MergeTable(handle)) => {
                ConnectorRelation::MergeTable(handle)
            }
            None => unreachable!("a validated catalog table handle always carries a relation"),
        }
    }

    pub fn relation_kind(&self) -> ConnectorRelationKind {
        self.relation().kind()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ProtocolErrorKind;

    fn catalog_root() -> FieldPath {
        FieldPath::root("catalog_table_handle")
    }

    fn schema_table_name() -> dto::SchemaTableName {
        dto::SchemaTableName {
            schema_name: "sales".to_owned(),
            table_name: "orders".to_owned(),
        }
    }

    fn column(field_id: i32) -> dto::IcebergColumnHandle {
        dto::IcebergColumnHandle {
            base_column_identity: Some(dto::ColumnIdentity {
                field_id,
                name: format!("c{field_id}"),
                category: dto::ColumnIdentityCategory::Primitive as i32,
                children: Vec::new(),
            }),
            base_type_json: "\"long\"".to_owned(),
            field_id_path: Vec::new(),
            type_json: "\"long\"".to_owned(),
            nullable: true,
            comment: None,
        }
    }

    fn unconstrained() -> dto::TupleDomain {
        dto::TupleDomain {
            none: false,
            column_domains: Vec::new(),
        }
    }

    fn iceberg_table_handle() -> dto::IcebergTableHandle {
        dto::IcebergTableHandle {
            schema_table_name: Some(schema_table_name()),
            snapshot_id: Some(17),
            table_schema_json: "{\"type\":\"struct\"}".to_owned(),
            spec_id: Some(0),
            partition_spec_jsons: BTreeMap::from([(0, "{\"spec-id\":0}".to_owned())]),
            format_version: 2,
            unenforced_predicate: Some(unconstrained()),
            enforced_predicate: Some(unconstrained()),
            limit: None,
            projected_columns: vec![column(1), column(2)],
            name_mapping_json: None,
            pinned_data_files: None,
            table_location: "s3://bucket/warehouse/sales/orders".to_owned(),
            storage_properties: BTreeMap::from([(
                "s3.endpoint".to_owned(),
                "http://minio:9000".to_owned(),
            )]),
        }
    }

    fn table_handle() -> dto::ConnectorTableHandle {
        dto::ConnectorTableHandle {
            handle: Some(dto::connector_table_handle::Handle::Iceberg(
                iceberg_table_handle(),
            )),
        }
    }

    fn table_function_handle() -> dto::ConnectorTableFunctionHandle {
        dto::ConnectorTableFunctionHandle {
            handle: Some(
                dto::connector_table_function_handle::Handle::IcebergTableChanges(
                    dto::TableChangesFunctionHandle {
                        schema_table_name: Some(schema_table_name()),
                        table_schema_json: "{\"type\":\"struct\"}".to_owned(),
                        columns: vec![column(1)],
                        name_mapping_json: None,
                        start_snapshot_id: 3,
                        end_snapshot_id: 9,
                    },
                ),
            ),
        }
    }

    fn change_window_handle() -> dto::ConnectorChangeWindowHandle {
        dto::ConnectorChangeWindowHandle {
            handle: Some(dto::connector_change_window_handle::Handle::Iceberg(
                dto::IcebergChangeWindowHandle {
                    schema_table_name: Some(schema_table_name()),
                    table_schema_json: "{\"type\":\"struct\"}".to_owned(),
                    columns: vec![column(1)],
                    name_mapping_json: None,
                    from_snapshot_id_exclusive: 3,
                    to_snapshot_id_inclusive: 9,
                    partition_spec_jsons: BTreeMap::from([(0, "{\"spec-id\":0}".to_owned())]),
                },
            )),
        }
    }

    fn system_table_reference() -> dto::ConnectorSystemTableReference {
        dto::ConnectorSystemTableReference {
            reference: Some(dto::connector_system_table_reference::Reference::Iceberg(
                dto::IcebergSystemTableReference {
                    schema_table_name: Some(schema_table_name()),
                    system_table_type: dto::IcebergSystemTableType::Files as i32,
                    metadata_file_location: "s3://bucket/warehouse/sales/orders/metadata/v3.json"
                        .to_owned(),
                    table_uuid: "6b1c2f0a-9d4e-4f7b-8a31-0c5d7e9f1234".to_owned(),
                    snapshot_id: Some(17),
                },
            )),
        }
    }

    fn digest_hex(fill: char) -> String {
        std::iter::repeat_n(fill, ARTIFACT_DIGEST_HEX_CHARS).collect()
    }

    fn table_execute_handle(
        procedure_id: dto::IcebergProcedureId,
        procedure_handle: Option<dto::iceberg_table_execute_handle::ProcedureHandle>,
    ) -> dto::ConnectorTableExecuteHandle {
        dto::ConnectorTableExecuteHandle {
            handle: Some(dto::connector_table_execute_handle::Handle::Iceberg(
                dto::IcebergTableExecuteHandle {
                    schema_table_name: Some(schema_table_name()),
                    procedure_id: procedure_id as i32,
                    table_location: "s3://bucket/warehouse/sales/orders".to_owned(),
                    procedure_handle,
                },
            )),
        }
    }

    fn optimize_procedure() -> dto::iceberg_table_execute_handle::ProcedureHandle {
        dto::iceberg_table_execute_handle::ProcedureHandle::Optimize(dto::IcebergOptimizeHandle {
            table_handle: Some(iceberg_table_handle()),
            min_file_size_bytes: 1024,
        })
    }

    fn rewrite_procedure() -> dto::iceberg_table_execute_handle::ProcedureHandle {
        dto::iceberg_table_execute_handle::ProcedureHandle::RewritePositionDeleteFiles(
            dto::IcebergRewritePositionDeleteFilesHandle {
                table_handle: Some(iceberg_table_handle()),
                artifact: Some(dto::IcebergRewriteArtifactContentId {
                    artifact_location: "s3://bucket/warehouse/_rewrite/plan.avro".to_owned(),
                    artifact_digest_hex: digest_hex('a'),
                }),
                group_digest_hex: digest_hex('b'),
            },
        )
    }

    fn merge_table_handle() -> dto::ConnectorMergeTableHandle {
        dto::ConnectorMergeTableHandle {
            handle: Some(dto::connector_merge_table_handle::Handle::Iceberg(
                dto::IcebergMergeTableHandle {
                    table_handle: Some(iceberg_table_handle()),
                    insert_table_handle: Some(dto::IcebergInsertTableHandle {
                        schema_table_name: Some(schema_table_name()),
                        table_schema_json: "{\"type\":\"struct\"}".to_owned(),
                        table_location: "s3://bucket/warehouse/sales/orders".to_owned(),
                        format_version: 2,
                        spec_id: Some(0),
                    }),
                },
            )),
        }
    }

    fn transaction() -> dto::ConnectorTransactionHandle {
        dto::ConnectorTransactionHandle {
            handle: Some(dto::connector_transaction_handle::Handle::Iceberg(
                dto::HiveTransactionHandle {
                    auto_commit: true,
                    uuid: vec![9_u8; TRANSACTION_UUID_BYTES],
                },
            )),
        }
    }

    fn catalog_handle(relation: dto::catalog_table_handle::Relation) -> dto::CatalogTableHandle {
        dto::CatalogTableHandle {
            catalog_handle: Some(novarocks_proto_models::catalog::CatalogHandle {
                catalog_name: "lake.analytics".to_owned(),
                version: vec![4_u8; 32],
            }),
            transaction: Some(transaction()),
            relation: Some(relation),
        }
    }

    fn every_relation() -> Vec<(ConnectorRelationKind, dto::catalog_table_handle::Relation)> {
        vec![
            (
                ConnectorRelationKind::Table,
                dto::catalog_table_handle::Relation::Table(table_handle()),
            ),
            (
                ConnectorRelationKind::TableFunction,
                dto::catalog_table_handle::Relation::TableFunction(table_function_handle()),
            ),
            (
                ConnectorRelationKind::ChangeWindow,
                dto::catalog_table_handle::Relation::ChangeWindow(change_window_handle()),
            ),
            (
                ConnectorRelationKind::SystemTable,
                dto::catalog_table_handle::Relation::SystemTable(system_table_reference()),
            ),
            (
                ConnectorRelationKind::TableExecute,
                dto::catalog_table_handle::Relation::TableExecute(table_execute_handle(
                    dto::IcebergProcedureId::Optimize,
                    Some(optimize_procedure()),
                )),
            ),
            (
                ConnectorRelationKind::MergeTable,
                dto::catalog_table_handle::Relation::MergeTable(merge_table_handle()),
            ),
        ]
    }

    #[test]
    fn every_handle_family_round_trips_through_its_validated_carrier() {
        for (kind, relation) in every_relation() {
            let raw = catalog_handle(relation);
            let parsed = CatalogTableHandle::parse(raw.clone(), catalog_root())
                .expect("every handle family is valid");
            assert_eq!(parsed.relation_kind(), kind);
            assert_eq!(parsed.catalog_name(), "lake.analytics");
            assert_eq!(parsed.catalog_handle().version().as_bytes(), &[4_u8; 32]);
            assert_eq!(parsed.transaction(), &transaction());
            assert_eq!(parsed.as_proto(), &raw);
            assert_eq!(parsed.into_proto(), raw);
        }

        let transaction_handle =
            ValidatedTransactionHandle::parse(transaction(), FieldPath::root("transaction"))
                .expect("valid transaction");
        assert!(transaction_handle.auto_commit());
        assert_eq!(transaction_handle.uuid(), [9_u8; TRANSACTION_UUID_BYTES]);
        assert_eq!(transaction_handle.into_proto(), transaction());

        let table = ValidatedConnectorTableHandle::parse(
            table_handle(),
            FieldPath::root("connector_table_handle"),
        )
        .expect("valid table handle");
        assert_eq!(table.format_version(), 2);
        assert_eq!(table.snapshot_id(), Some(17));
        assert_eq!(table.into_proto(), table_handle());

        let function = ValidatedConnectorTableFunctionHandle::parse(
            table_function_handle(),
            FieldPath::root("connector_table_function_handle"),
        )
        .expect("valid table function handle");
        assert_eq!(function.start_snapshot_id(), 3);
        assert_eq!(function.end_snapshot_id(), 9);
        assert_eq!(function.into_proto(), table_function_handle());

        let window = ValidatedConnectorChangeWindowHandle::parse(
            change_window_handle(),
            FieldPath::root("connector_change_window_handle"),
        )
        .expect("valid change window handle");
        assert_eq!(window.lower_snapshot_id_exclusive(), 3);
        assert_eq!(window.upper_snapshot_id_inclusive(), 9);
        assert_eq!(window.into_proto(), change_window_handle());

        let system = ValidatedConnectorSystemTableReference::parse(
            system_table_reference(),
            FieldPath::root("connector_system_table_reference"),
        )
        .expect("valid system table reference");
        assert_eq!(
            system.system_table_type(),
            dto::IcebergSystemTableType::Files
        );
        assert_eq!(system.snapshot_id(), Some(17));
        assert!(system.metadata_file_location().ends_with("v3.json"));
        assert_eq!(system.table_uuid().len(), UUID_TEXT_CHARS);
        assert_eq!(system.into_proto(), system_table_reference());

        let execute = ValidatedConnectorTableExecuteHandle::parse(
            table_execute_handle(
                dto::IcebergProcedureId::Optimize,
                Some(optimize_procedure()),
            ),
            FieldPath::root("connector_table_execute_handle"),
        )
        .expect("valid table execute handle");
        assert_eq!(execute.procedure_id(), dto::IcebergProcedureId::Optimize);
        assert!(matches!(
            execute.procedure(),
            Some(TableExecuteProcedure::Optimize(_))
        ));

        let merge = ValidatedConnectorMergeTableHandle::parse(
            merge_table_handle(),
            FieldPath::root("connector_merge_table_handle"),
        )
        .expect("valid merge table handle");
        assert_eq!(merge.table_handle().format_version, 2);
        assert_eq!(merge.insert_table_handle().format_version, 2);
        assert_eq!(merge.into_proto(), merge_table_handle());
    }

    #[test]
    fn an_absent_oneof_variant_is_a_missing_field_at_its_own_path() {
        let no_catalog_handle = dto::CatalogTableHandle {
            catalog_handle: None,
            ..catalog_handle(dto::catalog_table_handle::Relation::Table(table_handle()))
        };
        let error = CatalogTableHandle::parse(no_catalog_handle, catalog_root())
            .expect_err("catalog handle");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "catalog_table_handle.catalog_handle"
        );

        let no_relation = dto::CatalogTableHandle {
            relation: None,
            ..catalog_handle(dto::catalog_table_handle::Relation::Table(table_handle()))
        };
        let error = CatalogTableHandle::parse(no_relation, catalog_root()).expect_err("relation");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(error.path().to_string(), "catalog_table_handle");

        let no_transaction_variant = dto::CatalogTableHandle {
            transaction: Some(dto::ConnectorTransactionHandle { handle: None }),
            ..catalog_handle(dto::catalog_table_handle::Relation::Table(table_handle()))
        };
        let error =
            CatalogTableHandle::parse(no_transaction_variant, catalog_root()).expect_err("variant");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(error.path().to_string(), "catalog_table_handle.transaction");

        let no_transaction = dto::CatalogTableHandle {
            transaction: None,
            ..catalog_handle(dto::catalog_table_handle::Relation::Table(table_handle()))
        };
        assert_eq!(
            CatalogTableHandle::parse(no_transaction, catalog_root())
                .expect_err("transaction")
                .kind(),
            ProtocolErrorKind::MissingField
        );

        for empty in [
            dto::catalog_table_handle::Relation::Table(dto::ConnectorTableHandle { handle: None }),
            dto::catalog_table_handle::Relation::TableFunction(dto::ConnectorTableFunctionHandle {
                handle: None,
            }),
            dto::catalog_table_handle::Relation::ChangeWindow(dto::ConnectorChangeWindowHandle {
                handle: None,
            }),
            dto::catalog_table_handle::Relation::SystemTable(dto::ConnectorSystemTableReference {
                reference: None,
            }),
            dto::catalog_table_handle::Relation::TableExecute(dto::ConnectorTableExecuteHandle {
                handle: None,
            }),
            dto::catalog_table_handle::Relation::MergeTable(dto::ConnectorMergeTableHandle {
                handle: None,
            }),
        ] {
            assert_eq!(
                CatalogTableHandle::parse(catalog_handle(empty), catalog_root())
                    .expect_err("absent provider variant")
                    .kind(),
                ProtocolErrorKind::MissingField
            );
        }
    }

    #[test]
    fn an_unspecified_or_unknown_system_table_type_is_an_invalid_enum() {
        for system_table_type in [dto::IcebergSystemTableType::Unspecified as i32, 4242] {
            let mut reference = system_table_reference();
            if let Some(dto::connector_system_table_reference::Reference::Iceberg(iceberg)) =
                reference.reference.as_mut()
            {
                iceberg.system_table_type = system_table_type;
            }
            let error = ValidatedConnectorSystemTableReference::parse(
                reference,
                FieldPath::root("connector_system_table_reference"),
            )
            .expect_err("system table type");
            assert_eq!(error.kind(), ProtocolErrorKind::InvalidEnum);
            assert_eq!(
                error.path().to_string(),
                "connector_system_table_reference.iceberg.system_table_type"
            );
        }
    }

    #[test]
    fn an_unspecified_or_unknown_procedure_id_is_an_invalid_enum() {
        for procedure_id in [dto::IcebergProcedureId::Unspecified as i32, 4242] {
            let mut handle = table_execute_handle(dto::IcebergProcedureId::Optimize, None);
            if let Some(dto::connector_table_execute_handle::Handle::Iceberg(iceberg)) =
                handle.handle.as_mut()
            {
                iceberg.procedure_id = procedure_id;
            }
            let error = ValidatedConnectorTableExecuteHandle::parse(
                handle,
                FieldPath::root("connector_table_execute_handle"),
            )
            .expect_err("procedure id");
            assert_eq!(error.kind(), ProtocolErrorKind::InvalidEnum);
            assert_eq!(
                error.path().to_string(),
                "connector_table_execute_handle.iceberg.procedure_id"
            );
        }
    }

    #[test]
    fn the_procedure_id_and_the_procedure_handle_must_agree_in_both_directions() {
        let root = FieldPath::root("connector_table_execute_handle");

        // A distributed procedure with the wrong handle, or with none at all.
        for (procedure_id, procedure_handle) in [
            (dto::IcebergProcedureId::Optimize, None),
            (dto::IcebergProcedureId::Optimize, Some(rewrite_procedure())),
            (dto::IcebergProcedureId::RewritePositionDeleteFiles, None),
            (
                dto::IcebergProcedureId::RewritePositionDeleteFiles,
                Some(optimize_procedure()),
            ),
        ] {
            let error = ValidatedConnectorTableExecuteHandle::parse(
                table_execute_handle(procedure_id, procedure_handle),
                root.clone(),
            )
            .expect_err("mismatched procedure handle");
            assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
            assert_eq!(
                error.path().to_string(),
                "connector_table_execute_handle.iceberg.procedure_handle"
            );
        }

        // A coordinator-only procedure must carry no procedure handle at all.
        for coordinator_only in [
            dto::IcebergProcedureId::OptimizeManifests,
            dto::IcebergProcedureId::DropExtendedStats,
            dto::IcebergProcedureId::RollbackToSnapshot,
            dto::IcebergProcedureId::ExpireSnapshots,
            dto::IcebergProcedureId::RemoveOrphanFiles,
            dto::IcebergProcedureId::AddFiles,
            dto::IcebergProcedureId::AddFilesFromTable,
        ] {
            let valid = ValidatedConnectorTableExecuteHandle::parse(
                table_execute_handle(coordinator_only, None),
                root.clone(),
            )
            .expect("a coordinator-only procedure carries no procedure handle");
            assert_eq!(valid.procedure_id(), coordinator_only);
            assert!(valid.procedure().is_none());

            assert_eq!(
                ValidatedConnectorTableExecuteHandle::parse(
                    table_execute_handle(coordinator_only, Some(optimize_procedure())),
                    root.clone(),
                )
                .expect_err("stray procedure handle")
                .kind(),
                ProtocolErrorKind::InconsistentFields
            );
        }

        // Both matching pairs are accepted.
        for (procedure_id, procedure_handle) in [
            (dto::IcebergProcedureId::Optimize, optimize_procedure()),
            (
                dto::IcebergProcedureId::RewritePositionDeleteFiles,
                rewrite_procedure(),
            ),
        ] {
            assert!(
                ValidatedConnectorTableExecuteHandle::parse(
                    table_execute_handle(procedure_id, Some(procedure_handle)),
                    root.clone(),
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn a_dangling_spec_id_is_inconsistent_with_the_carried_partition_specs() {
        let mut iceberg = iceberg_table_handle();
        iceberg.spec_id = Some(7);
        let error = ValidatedConnectorTableHandle::parse(
            dto::ConnectorTableHandle {
                handle: Some(dto::connector_table_handle::Handle::Iceberg(iceberg)),
            },
            FieldPath::root("connector_table_handle"),
        )
        .expect_err("dangling spec id");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_table_handle.iceberg.spec_id"
        );

        let mut empty_spec = iceberg_table_handle();
        empty_spec.partition_spec_jsons = BTreeMap::from([(0, String::new())]);
        assert_eq!(
            ValidatedConnectorTableHandle::parse(
                dto::ConnectorTableHandle {
                    handle: Some(dto::connector_table_handle::Handle::Iceberg(empty_spec)),
                },
                FieldPath::root("connector_table_handle"),
            )
            .expect_err("empty partition spec json")
            .kind(),
            ProtocolErrorKind::InvalidValue
        );
    }

    /// A pinned file set is the whole definition of the read that carries it,
    /// so its spelling has to be canonical: an unsorted or repeated list would
    /// make the same pinned read two different reads on the wire, and an
    /// unbounded one is a wire hazard rather than a larger rewrite.
    #[test]
    fn a_pinned_data_file_set_must_be_bounded_sorted_and_unique() {
        let pinned = |paths: Vec<String>| {
            let mut iceberg = iceberg_table_handle();
            iceberg.pinned_data_files = Some(dto::IcebergPinnedDataFileSet { paths });
            ValidatedConnectorTableHandle::parse(
                dto::ConnectorTableHandle {
                    handle: Some(dto::connector_table_handle::Handle::Iceberg(iceberg)),
                },
                FieldPath::root("connector_table_handle"),
            )
        };

        // An empty pin is a legal read of no rows, not an absent restriction.
        assert!(pinned(Vec::new()).is_ok());
        assert!(pinned(vec!["s3://b/a".to_owned(), "s3://b/b".to_owned()]).is_ok());

        let unsorted = pinned(vec!["s3://b/b".to_owned(), "s3://b/a".to_owned()])
            .expect_err("unsorted pinned data files");
        assert_eq!(unsorted.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            unsorted.path().to_string(),
            "connector_table_handle.iceberg.pinned_data_files"
        );
        assert_eq!(
            pinned(vec!["s3://b/a".to_owned(), "s3://b/a".to_owned()])
                .expect_err("repeated pinned data file")
                .kind(),
            ProtocolErrorKind::InconsistentFields
        );
        assert_eq!(
            pinned(vec![String::new()])
                .expect_err("empty pinned data file")
                .kind(),
            ProtocolErrorKind::InvalidValue
        );
        assert_eq!(
            pinned(
                (0..=MAX_PINNED_DATA_FILES)
                    .map(|ordinal| format!("s3://b/{ordinal:08}"))
                    .collect()
            )
            .expect_err("too many pinned data files")
            .kind(),
            ProtocolErrorKind::OutOfRange
        );
    }

    #[test]
    fn a_duplicate_projected_column_is_rejected() {
        let mut iceberg = iceberg_table_handle();
        iceberg.projected_columns = vec![column(1), column(1)];
        let error = ValidatedConnectorTableHandle::parse(
            dto::ConnectorTableHandle {
                handle: Some(dto::connector_table_handle::Handle::Iceberg(iceberg)),
            },
            FieldPath::root("connector_table_handle"),
        )
        .expect_err("duplicate projected column");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_table_handle.iceberg.projected_columns[1]"
        );
    }

    #[test]
    fn output_columns_must_be_present_and_unique() {
        let mut empty = table_function_handle();
        if let Some(dto::connector_table_function_handle::Handle::IcebergTableChanges(changes)) =
            empty.handle.as_mut()
        {
            changes.columns.clear();
        }
        assert_eq!(
            ValidatedConnectorTableFunctionHandle::parse(
                empty,
                FieldPath::root("connector_table_function_handle")
            )
            .expect_err("no output columns")
            .kind(),
            ProtocolErrorKind::InvalidValue
        );

        let mut duplicated = change_window_handle();
        if let Some(dto::connector_change_window_handle::Handle::Iceberg(iceberg)) =
            duplicated.handle.as_mut()
        {
            iceberg.columns = vec![column(1), column(1)];
        }
        let error = ValidatedConnectorChangeWindowHandle::parse(
            duplicated,
            FieldPath::root("connector_change_window_handle"),
        )
        .expect_err("duplicate output column");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_change_window_handle.iceberg.columns[1]"
        );
    }

    #[test]
    fn a_credential_shaped_storage_property_key_is_unsupported_and_never_echoes_the_value() {
        const SECRET_VALUE: &str = "AKIAEXAMPLEVALUE";

        for key in [
            "s3.secret-access-key",
            "S3.Access-Key",
            "hive.metastore.access_key",
            "gcs.oauth.TOKEN",
            "azure.password",
            "vault.Credential",
        ] {
            let mut iceberg = iceberg_table_handle();
            iceberg.storage_properties =
                BTreeMap::from([(key.to_owned(), SECRET_VALUE.to_owned())]);
            let error = ValidatedConnectorTableHandle::parse(
                dto::ConnectorTableHandle {
                    handle: Some(dto::connector_table_handle::Handle::Iceberg(iceberg)),
                },
                FieldPath::root("connector_table_handle"),
            )
            .expect_err("credential-shaped storage property key");
            assert_eq!(error.kind(), ProtocolErrorKind::Unsupported);
            assert!(!error.detail().contains(SECRET_VALUE));
            assert!(!error.to_string().contains(SECRET_VALUE));
            assert_eq!(
                error.path().to_string(),
                format!("connector_table_handle.iceberg.storage_properties[{key:?}]")
            );
        }

        let mut empty_key = iceberg_table_handle();
        empty_key.storage_properties = BTreeMap::from([(String::new(), "value".to_owned())]);
        assert_eq!(
            ValidatedConnectorTableHandle::parse(
                dto::ConnectorTableHandle {
                    handle: Some(dto::connector_table_handle::Handle::Iceberg(empty_key)),
                },
                FieldPath::root("connector_table_handle"),
            )
            .expect_err("empty storage property key")
            .kind(),
            ProtocolErrorKind::InvalidValue
        );
    }

    #[test]
    fn fixed_width_catalog_versions_must_be_exactly_thirty_two_bytes() {
        for version in [vec![4_u8; 31], vec![4_u8; 33], Vec::new()] {
            let raw = dto::CatalogTableHandle {
                catalog_handle: Some(novarocks_proto_models::catalog::CatalogHandle {
                    catalog_name: "lake.analytics".to_owned(),
                    version,
                }),
                ..catalog_handle(dto::catalog_table_handle::Relation::Table(table_handle()))
            };
            let error = CatalogTableHandle::parse(raw, catalog_root()).expect_err("version");
            assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
            assert_eq!(
                error.path().to_string(),
                "catalog_table_handle.catalog_handle.version"
            );
        }

        let short_uuid = dto::ConnectorTransactionHandle {
            handle: Some(dto::connector_transaction_handle::Handle::Iceberg(
                dto::HiveTransactionHandle {
                    auto_commit: false,
                    uuid: vec![9_u8; 15],
                },
            )),
        };
        let error = ValidatedTransactionHandle::parse(short_uuid, FieldPath::root("transaction"))
            .expect_err("transaction uuid");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(error.path().to_string(), "transaction.iceberg.uuid");
    }

    #[test]
    fn a_table_uuid_must_be_canonical_lowercase_text() {
        for table_uuid in [
            "6B1C2F0A-9D4E-4F7B-8A31-0C5D7E9F1234",
            "6b1c2f0a9d4e4f7b8a310c5d7e9f1234",
            "6b1c2f0a-9d4e-4f7b-8a31-0c5d7e9f123",
            "{6b1c2f0a-9d4e-4f7b-8a31-0c5d7e9f1234}",
            "6b1c2f0a-9d4e-4f7b-8a31-0c5d7e9fzzzz",
            "",
        ] {
            let mut reference = system_table_reference();
            if let Some(dto::connector_system_table_reference::Reference::Iceberg(iceberg)) =
                reference.reference.as_mut()
            {
                iceberg.table_uuid = table_uuid.to_owned();
            }
            let error = ValidatedConnectorSystemTableReference::parse(
                reference,
                FieldPath::root("connector_system_table_reference"),
            )
            .expect_err("non-canonical table uuid");
            assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
            assert_eq!(
                error.path().to_string(),
                "connector_system_table_reference.iceberg.table_uuid"
            );
        }
    }

    #[test]
    fn an_out_of_range_format_version_is_rejected_on_both_table_and_insert_handles() {
        for format_version in [0, 4, -1, i32::MAX] {
            let mut iceberg = iceberg_table_handle();
            iceberg.format_version = format_version;
            let error = ValidatedConnectorTableHandle::parse(
                dto::ConnectorTableHandle {
                    handle: Some(dto::connector_table_handle::Handle::Iceberg(iceberg)),
                },
                FieldPath::root("connector_table_handle"),
            )
            .expect_err("format version");
            assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
            assert_eq!(
                error.path().to_string(),
                "connector_table_handle.iceberg.format_version"
            );
        }

        let mut merge = merge_table_handle();
        if let Some(dto::connector_merge_table_handle::Handle::Iceberg(iceberg)) =
            merge.handle.as_mut()
        {
            iceberg
                .insert_table_handle
                .as_mut()
                .expect("insert handle")
                .format_version = 4;
        }
        let error = ValidatedConnectorMergeTableHandle::parse(
            merge,
            FieldPath::root("connector_merge_table_handle"),
        )
        .expect_err("insert format version");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "connector_merge_table_handle.iceberg.insert_table_handle.format_version"
        );
    }

    #[test]
    fn a_rewrite_artifact_digest_must_be_sixty_four_lowercase_hex_characters() {
        for digest in [
            digest_hex('A'),
            digest_hex('z'),
            "abc".to_owned(),
            String::new(),
            format!("{}0", digest_hex('a')),
        ] {
            let mut procedure = rewrite_procedure();
            if let dto::iceberg_table_execute_handle::ProcedureHandle::RewritePositionDeleteFiles(
                rewrite,
            ) = &mut procedure
            {
                rewrite
                    .artifact
                    .as_mut()
                    .expect("artifact")
                    .artifact_digest_hex = digest.clone();
            }
            let error = ValidatedConnectorTableExecuteHandle::parse(
                table_execute_handle(
                    dto::IcebergProcedureId::RewritePositionDeleteFiles,
                    Some(procedure),
                ),
                FieldPath::root("connector_table_execute_handle"),
            )
            .expect_err("artifact digest");
            assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
            assert_eq!(
                error.path().to_string(),
                "connector_table_execute_handle.iceberg.rewrite_position_delete_files.artifact.artifact_digest_hex"
            );
        }

        let mut procedure = rewrite_procedure();
        if let dto::iceberg_table_execute_handle::ProcedureHandle::RewritePositionDeleteFiles(
            rewrite,
        ) = &mut procedure
        {
            rewrite.group_digest_hex = digest_hex('A');
        }
        let error = ValidatedConnectorTableExecuteHandle::parse(
            table_execute_handle(
                dto::IcebergProcedureId::RewritePositionDeleteFiles,
                Some(procedure),
            ),
            FieldPath::root("connector_table_execute_handle"),
        )
        .expect_err("group digest");
        assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
        assert_eq!(
            error.path().to_string(),
            "connector_table_execute_handle.iceberg.rewrite_position_delete_files.group_digest_hex"
        );
    }

    #[test]
    fn change_endpoints_must_be_distinct_and_nonnegative() {
        let mut same = table_function_handle();
        if let Some(dto::connector_table_function_handle::Handle::IcebergTableChanges(changes)) =
            same.handle.as_mut()
        {
            changes.start_snapshot_id = 5;
            changes.end_snapshot_id = 5;
        }
        let error = ValidatedConnectorTableFunctionHandle::parse(
            same,
            FieldPath::root("connector_table_function_handle"),
        )
        .expect_err("identical endpoints");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_table_function_handle.iceberg_table_changes.end_snapshot_id"
        );

        let mut negative = change_window_handle();
        if let Some(dto::connector_change_window_handle::Handle::Iceberg(iceberg)) =
            negative.handle.as_mut()
        {
            iceberg.from_snapshot_id_exclusive = -1;
        }
        let error = ValidatedConnectorChangeWindowHandle::parse(
            negative,
            FieldPath::root("connector_change_window_handle"),
        )
        .expect_err("negative endpoint");
        assert_eq!(error.kind(), ProtocolErrorKind::OutOfRange);
        assert_eq!(
            error.path().to_string(),
            "connector_change_window_handle.iceberg.from_snapshot_id_exclusive"
        );

        let mut identical_window = change_window_handle();
        if let Some(dto::connector_change_window_handle::Handle::Iceberg(iceberg)) =
            identical_window.handle.as_mut()
        {
            iceberg.to_snapshot_id_inclusive = iceberg.from_snapshot_id_exclusive;
        }
        assert_eq!(
            ValidatedConnectorChangeWindowHandle::parse(
                identical_window,
                FieldPath::root("connector_change_window_handle")
            )
            .expect_err("identical window endpoints")
            .kind(),
            ProtocolErrorKind::InconsistentFields
        );
    }

    #[test]
    fn a_catalog_name_must_arrive_already_normalized() {
        for catalog_name in ["Lake.Analytics", "LAKE", "lake analytics", "1lake", ""] {
            let raw = dto::CatalogTableHandle {
                catalog_handle: Some(novarocks_proto_models::catalog::CatalogHandle {
                    catalog_name: catalog_name.to_owned(),
                    version: vec![4_u8; 32],
                }),
                ..catalog_handle(dto::catalog_table_handle::Relation::Table(table_handle()))
            };
            let error = CatalogTableHandle::parse(raw, catalog_root()).expect_err("catalog name");
            assert_eq!(error.kind(), ProtocolErrorKind::InvalidValue);
            assert_eq!(
                error.path().to_string(),
                "catalog_table_handle.catalog_handle.catalog_name"
            );
        }

        let oversized = dto::CatalogTableHandle {
            catalog_handle: Some(novarocks_proto_models::catalog::CatalogHandle {
                catalog_name: "a".repeat(MAX_NAME_BYTES + 1),
                version: vec![4_u8; 32],
            }),
            ..catalog_handle(dto::catalog_table_handle::Relation::Table(table_handle()))
        };
        assert_eq!(
            CatalogTableHandle::parse(oversized, catalog_root())
                .expect_err("oversized catalog name")
                .kind(),
            ProtocolErrorKind::OutOfRange
        );
    }

    #[test]
    fn a_required_predicate_must_be_present_and_structurally_sound() {
        let mut absent = iceberg_table_handle();
        absent.enforced_predicate = None;
        let error = ValidatedConnectorTableHandle::parse(
            dto::ConnectorTableHandle {
                handle: Some(dto::connector_table_handle::Handle::Iceberg(absent)),
            },
            FieldPath::root("connector_table_handle"),
        )
        .expect_err("absent predicate");
        assert_eq!(error.kind(), ProtocolErrorKind::MissingField);
        assert_eq!(
            error.path().to_string(),
            "connector_table_handle.iceberg.enforced_predicate"
        );

        // An unsatisfiable predicate that still carries column domains is a
        // predicate-level contradiction, re-rooted onto this field.
        let mut contradictory = iceberg_table_handle();
        contradictory.unenforced_predicate = Some(dto::TupleDomain {
            none: true,
            column_domains: vec![dto::ColumnDomain {
                column: None,
                domain: None,
            }],
        });
        let error = ValidatedConnectorTableHandle::parse(
            dto::ConnectorTableHandle {
                handle: Some(dto::connector_table_handle::Handle::Iceberg(contradictory)),
            },
            FieldPath::root("connector_table_handle"),
        )
        .expect_err("contradictory predicate");
        assert_eq!(error.kind(), ProtocolErrorKind::InconsistentFields);
        assert_eq!(
            error.path().to_string(),
            "connector_table_handle.iceberg.unenforced_predicate.column_domains"
        );
    }
}
