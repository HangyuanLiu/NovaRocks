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

//! Provider-private ADD FILES planning and revalidation primitives.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use bytes::Bytes;
use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct, Type};
use iceberg::table::Table;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use sha2::{Digest, Sha256};

use crate::connector::iceberg::catalog::registry::block_on_iceberg;
use crate::connector::iceberg::fs_io;
use novarocks_fs::ObjectStoreConfig;
use novarocks_spi::connector::{
    MAX_CONNECTOR_DATA_MUTATION_FILE_LOCATION_BYTES, MAX_CONNECTOR_DATA_MUTATION_FILES,
    MAX_CONNECTOR_DATA_MUTATION_PARQUET_FOOTER_BYTES,
    MAX_CONNECTOR_DATA_MUTATION_TOTAL_FOOTER_BYTES,
};

const MANIFEST_DIGEST_DOMAIN: &[u8] = b"novarocks.iceberg.add-files-manifest.v1\0";
const SCHEMA_DIGEST_DOMAIN: &[u8] = b"novarocks.iceberg.add-files-schema.v1\0";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AddFilesSchemaIdentityMode {
    EmbeddedFieldIds,
    ExistingNameMapping,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AddFilesManifestRecord {
    pub(crate) location: String,
    pub(crate) size: u64,
    pub(crate) object_identity: Option<String>,
    pub(crate) footer_digest: [u8; 32],
    pub(crate) footer_bytes: u64,
    pub(crate) row_count: u64,
    pub(crate) schema_identity_digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AddFilesManifest {
    pub(crate) records: Vec<AddFilesManifestRecord>,
    pub(crate) digest: [u8; 32],
    pub(crate) total_bytes: u64,
    pub(crate) total_rows: u64,
    pub(crate) total_footer_bytes: u64,
    pub(crate) schema_identity_mode: AddFilesSchemaIdentityMode,
    pub(crate) canonical_name_mapping: Option<String>,
}

impl AddFilesManifest {
    pub(crate) fn to_data_files(&self) -> Result<Vec<iceberg::spec::DataFile>, String> {
        self.records
            .iter()
            .map(|record| {
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_path(record.location.clone())
                    .file_format(DataFileFormat::Parquet)
                    .file_size_in_bytes(record.size)
                    .record_count(record.row_count)
                    .partition(Struct::empty())
                    .partition_spec_id(0)
                    .build()
                    .map_err(|error| format!("build ADD FILES DataFile: {error}"))
            })
            .collect()
    }
}

pub(crate) fn plan_manifest_for_table(
    table: &Table,
    source_directory: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<AddFilesManifest, String> {
    if !table.metadata().default_partition_spec().is_unpartitioned() {
        return Err("ADD FILES supports only unpartitioned Iceberg tables".to_string());
    }
    let target_schema = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
            .map_err(|error| format!("convert ADD FILES target schema: {error}"))?,
    );
    let canonical_name_mapping = table
        .metadata()
        .properties()
        .get(iceberg::spec::DEFAULT_SCHEMA_NAME_MAPPING)
        .map(|mapping| canonical_name_mapping(mapping))
        .transpose()?;
    let default_ids = initial_default_ids(table.metadata().current_schema().as_struct());
    plan_manifest(
        source_directory,
        object_store_config,
        &target_schema,
        &default_ids,
        canonical_name_mapping,
    )
}

pub(crate) fn revalidate_manifest_for_table(
    table: &Table,
    source_directory: &str,
    object_store_config: Option<&ObjectStoreConfig>,
    expected: &AddFilesManifest,
) -> Result<AddFilesManifest, String> {
    let actual = plan_manifest_for_table(table, source_directory, object_store_config)?;
    if actual.digest != expected.digest {
        return Err("ADD FILES source manifest changed after planning".to_string());
    }
    Ok(actual)
}

fn plan_manifest(
    source_directory: &str,
    object_store_config: Option<&ObjectStoreConfig>,
    target_schema: &SchemaRef,
    initial_default_ids: &HashSet<i32>,
    canonical_name_mapping: Option<String>,
) -> Result<AddFilesManifest, String> {
    let mapping = canonical_name_mapping
        .as_deref()
        .map(serde_json::from_str::<iceberg::spec::NameMapping>)
        .transpose()
        .map_err(|error| format!("decode canonical ADD FILES name mapping: {error}"))?;
    if let Some(mapping) = mapping.as_ref() {
        validate_name_mapping_for_target(mapping, target_schema)?;
    }
    let files = list_direct_files(source_directory, object_store_config)?;
    if files.is_empty() {
        return Err(format!(
            "ADD FILES: no visible Parquet files found under {source_directory}"
        ));
    }
    let mut records = Vec::with_capacity(files.len());
    let mut expected_mode = None;
    let mut total_footer_bytes = 0u64;
    for file in files {
        let footer = read_parquet_footer(&file.location, file.size, object_store_config)?;
        total_footer_bytes = total_footer_bytes
            .checked_add(footer.footer_bytes)
            .ok_or_else(|| "ADD FILES footer byte total overflow".to_string())?;
        if total_footer_bytes
            > u64::try_from(MAX_CONNECTOR_DATA_MUTATION_TOTAL_FOOTER_BYTES)
                .expect("footer bound fits u64")
        {
            return Err("ADD FILES Parquet footer total exceeds 64 MiB".to_string());
        }
        let (identified, total) = super::super::reader::schema_field_id_coverage(&footer.schema)?;
        let (mode, source_schema) = if identified == total {
            (AddFilesSchemaIdentityMode::EmbeddedFieldIds, footer.schema)
        } else if identified != 0 {
            return Err(format!(
                "ADD FILES file {} mixes fields with and without field IDs",
                file.location
            ));
        } else {
            let mapping = mapping.as_ref().ok_or_else(|| {
                format!(
                    "ADD FILES file {} has no field IDs and the target table has no schema.name-mapping.default",
                    file.location
                )
            })?;
            (
                AddFilesSchemaIdentityMode::ExistingNameMapping,
                super::super::reader::apply_name_mapping_to_schema(&footer.schema, mapping)?,
            )
        };
        if expected_mode.is_some_and(|expected| expected != mode) {
            return Err(
                "ADD FILES cannot mix embedded field IDs and name-mapped files".to_string(),
            );
        }
        expected_mode = Some(mode);
        validate_schema(&source_schema, target_schema, initial_default_ids)?;
        records.push(AddFilesManifestRecord {
            location: file.location,
            size: file.size,
            object_identity: file.object_identity,
            footer_digest: footer.footer_digest,
            footer_bytes: footer.footer_bytes,
            row_count: footer.row_count,
            schema_identity_digest: schema_identity_digest(&source_schema)?,
        });
    }
    records.sort_by(|left, right| left.location.cmp(&right.location));
    let digest = manifest_digest(&records);
    let total_bytes = records.iter().try_fold(0u64, |total, record| {
        total
            .checked_add(record.size)
            .ok_or_else(|| "ADD FILES byte total overflow".to_string())
    })?;
    let total_rows = records.iter().try_fold(0u64, |total, record| {
        total
            .checked_add(record.row_count)
            .ok_or_else(|| "ADD FILES row total overflow".to_string())
    })?;
    Ok(AddFilesManifest {
        records,
        digest,
        total_bytes,
        total_rows,
        total_footer_bytes,
        schema_identity_mode: expected_mode.expect("nonempty manifest has a schema mode"),
        canonical_name_mapping,
    })
}

#[derive(Debug)]
struct ListedFile {
    location: String,
    size: u64,
    object_identity: Option<String>,
}

fn list_direct_files(
    directory: &str,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<Vec<ListedFile>, String> {
    let access = fs_io::resolve_access_for_location(directory, object_store_config)
        .map_err(|error| format!("resolve ADD FILES directory {directory}: {error}"))?;
    let relative_directory = access.single_relative_path()?;
    let prefix = if relative_directory.ends_with('/') {
        relative_directory.to_string()
    } else {
        format!("{relative_directory}/")
    };
    let operator = access.operator();
    block_on_iceberg(async {
        let entries = operator
            .list(&prefix)
            .await
            .map_err(|error| format!("list ADD FILES directory {directory}: {error}"))?;
        let mut files = Vec::new();
        for entry in entries {
            let relative = entry
                .path()
                .strip_prefix(&prefix)
                .unwrap_or(entry.path())
                .trim_end_matches('/');
            let name = relative.rsplit('/').next().unwrap_or(relative);
            if name.starts_with('.') || name.starts_with('_') {
                continue;
            }
            if relative.contains('/') {
                return Err(format!(
                    "ADD FILES does not allow recursive visible entry {}",
                    entry.path()
                ));
            }
            let metadata = operator
                .stat(entry.path())
                .await
                .map_err(|error| format!("stat ADD FILES entry {}: {error}", entry.path()))?;
            if metadata.mode().is_dir() {
                return Err(format!(
                    "ADD FILES visible child {} is a directory",
                    entry.path()
                ));
            }
            if !metadata.mode().is_file() {
                return Err(format!(
                    "ADD FILES visible child {} is not a regular file",
                    entry.path()
                ));
            }
            if !name.to_ascii_lowercase().ends_with(".parquet") {
                return Err(format!(
                    "ADD FILES visible child {} is not a Parquet file",
                    entry.path()
                ));
            }
            let location = fs_io::format_resolved_location(access.handle(), entry.path())?;
            if location.len() > MAX_CONNECTOR_DATA_MUTATION_FILE_LOCATION_BYTES {
                return Err("ADD FILES canonical file location exceeds 16 KiB".to_string());
            }
            let object_identity = metadata
                .version()
                .map(|value| format!("version:{value}"))
                .or_else(|| metadata.etag().map(|value| format!("etag:{value}")))
                .or_else(|| metadata.content_md5().map(|value| format!("md5:{value}")))
                .or_else(|| {
                    metadata
                        .last_modified()
                        .map(|value| format!("mtime:{value}"))
                });
            files.push(ListedFile {
                location,
                size: metadata.content_length(),
                object_identity,
            });
            if files.len()
                > usize::try_from(MAX_CONNECTOR_DATA_MUTATION_FILES).expect("file bound fits usize")
            {
                return Err("ADD FILES file count exceeds 4096".to_string());
            }
        }
        files.sort_by(|left, right| left.location.cmp(&right.location));
        Ok(files)
    })
    .map_err(|error| format!("ADD FILES list runtime: {error}"))?
}

struct ParquetFooterFacts {
    footer_bytes: u64,
    footer_digest: [u8; 32],
    row_count: u64,
    schema: SchemaRef,
}

fn read_parquet_footer(
    location: &str,
    file_size: u64,
    object_store_config: Option<&ObjectStoreConfig>,
) -> Result<ParquetFooterFacts, String> {
    let access = fs_io::resolve_access_for_location(location, object_store_config)
        .map_err(|error| format!("resolve ADD FILES Parquet file {location}: {error}"))?;
    let key = access.single_relative_path()?.to_string();
    let operator = access.operator();
    block_on_iceberg(async {
        if file_size < 12 {
            return Err(format!("ADD FILES Parquet file is too small: {location}"));
        }
        let tail = operator
            .read_with(&key)
            .range(file_size - 8..file_size)
            .await
            .map_err(|error| format!("read ADD FILES footer tail: {error}"))?
            .to_bytes();
        if tail.len() != 8 || &tail[4..] != b"PAR1" {
            return Err(format!("invalid ADD FILES Parquet footer: {location}"));
        }
        let footer_len = u32::from_le_bytes(tail[..4].try_into().expect("footer length")) as u64;
        if footer_len
            > u64::try_from(MAX_CONNECTOR_DATA_MUTATION_PARQUET_FOOTER_BYTES)
                .expect("footer bound fits u64")
        {
            return Err(format!(
                "ADD FILES Parquet footer exceeds 8 MiB: {location}"
            ));
        }
        let footer_start = file_size
            .checked_sub(8 + footer_len)
            .ok_or_else(|| format!("invalid ADD FILES Parquet footer length: {location}"))?;
        let footer = operator
            .read_with(&key)
            .range(footer_start..file_size - 8)
            .await
            .map_err(|error| format!("read ADD FILES footer: {error}"))?
            .to_bytes();
        let mut suffix = Vec::with_capacity(footer.len() + 8);
        suffix.extend_from_slice(&footer);
        suffix.extend_from_slice(&tail);
        let suffix = Bytes::from(suffix);
        let mut reader = parquet::file::metadata::ParquetMetaDataReader::new();
        reader
            .try_parse_sized(&suffix, file_size)
            .map_err(|error| format!("parse ADD FILES Parquet metadata: {error}"))?;
        let metadata = Arc::new(
            reader
                .finish()
                .map_err(|error| format!("finish ADD FILES Parquet metadata: {error}"))?,
        );
        let row_count = u64::try_from(metadata.file_metadata().num_rows())
            .map_err(|_| format!("ADD FILES Parquet file has negative row count: {location}"))?;
        let arrow = ArrowReaderMetadata::try_new(metadata, ArrowReaderOptions::new())
            .map_err(|error| format!("decode ADD FILES Arrow schema: {error}"))?;
        let mut hasher = Sha256::new();
        hasher.update(&suffix);
        Ok(ParquetFooterFacts {
            footer_bytes: footer_len + 8,
            footer_digest: hasher.finalize().into(),
            row_count,
            schema: Arc::clone(arrow.schema()),
        })
    })
    .map_err(|error| format!("ADD FILES footer runtime: {error}"))?
}

fn validate_schema(
    source: &SchemaRef,
    target: &SchemaRef,
    initial_default_ids: &HashSet<i32>,
) -> Result<(), String> {
    let mut target_by_id = HashMap::new();
    collect_fields_by_id(target.fields(), &mut target_by_id, "target")?;
    let mut source_by_id = HashMap::new();
    collect_fields_by_id(source.fields(), &mut source_by_id, "source")?;
    for (id, source_field) in &source_by_id {
        let target_field = target_by_id
            .get(id)
            .ok_or_else(|| format!("ADD FILES source contains unknown Iceberg field ID {id}"))?;
        if !read_type_compatible(source_field.data_type(), target_field.data_type()) {
            return Err(format!(
                "ADD FILES field ID {id} has incompatible type {:?}; target is {:?}",
                source_field.data_type(),
                target_field.data_type()
            ));
        }
        if source_field.is_nullable() && !target_field.is_nullable() {
            return Err(format!(
                "ADD FILES nullable source field ID {id} cannot satisfy a required target"
            ));
        }
    }
    for (id, target_field) in target_by_id {
        if !target_field.is_nullable()
            && !source_by_id.contains_key(&id)
            && !initial_default_ids.contains(&id)
        {
            return Err(format!(
                "ADD FILES source is missing required target field ID {id} without initial default"
            ));
        }
    }
    Ok(())
}

fn validate_name_mapping_for_target(
    mapping: &iceberg::spec::NameMapping,
    target: &SchemaRef,
) -> Result<(), String> {
    fn collect_mapping_ids(
        fields: &[iceberg::spec::MappedField],
        output: &mut HashSet<i32>,
    ) -> Result<(), String> {
        for field in fields {
            let id = field.field_id().ok_or_else(|| {
                "Iceberg name mapping contains a field without field-id".to_string()
            })?;
            if id <= 0 || !output.insert(id) {
                return Err(format!(
                    "Iceberg name mapping has duplicate or invalid ID {id}"
                ));
            }
            let children = field
                .fields()
                .iter()
                .map(|field| field.as_ref().clone())
                .collect::<Vec<_>>();
            collect_mapping_ids(&children, output)?;
        }
        Ok(())
    }

    let mut target_by_id = HashMap::new();
    collect_fields_by_id(target.fields(), &mut target_by_id, "target")?;
    let mut mapping_ids = HashSet::new();
    collect_mapping_ids(mapping.fields(), &mut mapping_ids)?;
    let target_ids = target_by_id.keys().copied().collect::<HashSet<_>>();
    if mapping_ids != target_ids {
        let mut missing = target_ids
            .difference(&mapping_ids)
            .copied()
            .collect::<Vec<_>>();
        let mut unknown = mapping_ids
            .difference(&target_ids)
            .copied()
            .collect::<Vec<_>>();
        missing.sort_unstable();
        unknown.sort_unstable();
        return Err(format!(
            "Iceberg name mapping does not exactly cover the target schema: missing={missing:?}, unknown={unknown:?}"
        ));
    }
    Ok(())
}

fn collect_fields_by_id<'a>(
    fields: &'a [FieldRef],
    output: &mut HashMap<i32, &'a Field>,
    label: &str,
) -> Result<(), String> {
    for field in fields {
        let id = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .ok_or_else(|| format!("ADD FILES {label} field {} has no field ID", field.name()))?
            .parse::<i32>()
            .map_err(|error| {
                format!(
                    "ADD FILES {label} field {} has invalid field ID: {error}",
                    field.name()
                )
            })?;
        if id <= 0 || output.insert(id, field.as_ref()).is_some() {
            return Err(format!(
                "ADD FILES {label} schema has duplicate or invalid field ID {id}"
            ));
        }
        match field.data_type() {
            DataType::Struct(children) => collect_fields_by_id(children, output, label)?,
            DataType::List(child)
            | DataType::LargeList(child)
            | DataType::FixedSizeList(child, _) => {
                collect_fields_by_id(std::slice::from_ref(child), output, label)?
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(children) = entries.data_type() else {
                    return Err(format!(
                        "ADD FILES {label} map field {} has non-struct entries",
                        field.name()
                    ));
                };
                collect_fields_by_id(children, output, label)?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn read_type_compatible(source: &DataType, target: &DataType) -> bool {
    if source == target {
        return true;
    }
    match (source, target) {
        (DataType::Int32, DataType::Int64) | (DataType::Float32, DataType::Float64) => true,
        (DataType::Decimal128(sp, ss), DataType::Decimal128(tp, ts))
        | (DataType::Decimal256(sp, ss), DataType::Decimal256(tp, ts)) => ss == ts && sp <= tp,
        (DataType::Struct(_), DataType::Struct(_)) => true,
        (DataType::List(_), DataType::List(_))
        | (DataType::LargeList(_), DataType::LargeList(_))
        | (DataType::FixedSizeList(_, _), DataType::FixedSizeList(_, _))
        | (DataType::Map(_, _), DataType::Map(_, _)) => true,
        _ => false,
    }
}

fn initial_default_ids(schema: &iceberg::spec::StructType) -> HashSet<i32> {
    fn visit(field: &iceberg::spec::NestedField, ids: &mut HashSet<i32>) {
        if field.initial_default.is_some() {
            ids.insert(field.id);
        }
        match field.field_type.as_ref() {
            Type::Struct(struct_type) => {
                for child in struct_type.fields() {
                    visit(child, ids);
                }
            }
            Type::List(list) => visit(&list.element_field, ids),
            Type::Map(map) => {
                visit(&map.key_field, ids);
                visit(&map.value_field, ids);
            }
            Type::Primitive(_) => {}
        }
    }
    let mut ids = HashSet::new();
    for field in schema.fields() {
        visit(field, &mut ids);
    }
    ids
}

pub(crate) fn canonical_name_mapping(raw: &str) -> Result<String, String> {
    let value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| format!("decode schema.name-mapping.default: {error}"))?;
    validate_mapping_json(&value)?;
    let mapping: iceberg::spec::NameMapping = serde_json::from_value(value)
        .map_err(|error| format!("decode schema.name-mapping.default: {error}"))?;
    serde_json::to_string(&mapping)
        .map_err(|error| format!("encode canonical schema.name-mapping.default: {error}"))
}

fn validate_mapping_json(value: &serde_json::Value) -> Result<(), String> {
    fn visit(fields: &serde_json::Value, ids: &mut HashSet<i64>) -> Result<(), String> {
        let fields = fields
            .as_array()
            .ok_or_else(|| "Iceberg name mapping root/fields must be an array".to_string())?;
        let mut sibling_aliases = HashSet::new();
        for field in fields {
            let object = field
                .as_object()
                .ok_or_else(|| "Iceberg name mapping field must be an object".to_string())?;
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "field-id" | "names" | "fields"))
            {
                return Err("Iceberg name mapping contains an unknown field".to_string());
            }
            let id = object
                .get("field-id")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| "Iceberg name mapping field-id is required".to_string())?;
            if id <= 0 || !ids.insert(id) {
                return Err(format!(
                    "Iceberg name mapping has duplicate or invalid ID {id}"
                ));
            }
            let names = object
                .get("names")
                .and_then(serde_json::Value::as_array)
                .ok_or_else(|| "Iceberg name mapping names must be an array".to_string())?;
            if names.is_empty() {
                return Err("Iceberg name mapping names must not be empty".to_string());
            }
            for name in names {
                let name = name
                    .as_str()
                    .filter(|name| !name.is_empty())
                    .ok_or_else(|| "Iceberg name mapping alias must be nonempty".to_string())?;
                if !sibling_aliases.insert(name.to_string()) {
                    return Err(format!("Iceberg name mapping has duplicate alias {name}"));
                }
            }
            if let Some(children) = object.get("fields")
                && !children.is_null()
            {
                visit(children, ids)?;
            }
        }
        Ok(())
    }
    let mut ids = HashSet::new();
    visit(value, &mut ids)
}

fn schema_identity_digest(schema: &SchemaRef) -> Result<[u8; 32], String> {
    let mut fields = HashMap::new();
    collect_fields_by_id(schema.fields(), &mut fields, "source")?;
    let mut ordered = fields.into_iter().collect::<Vec<_>>();
    ordered.sort_by_key(|(id, _)| *id);
    let mut hasher = Sha256::new();
    hasher.update(SCHEMA_DIGEST_DOMAIN);
    for (id, field) in ordered {
        hasher.update(id.to_be_bytes());
        digest_bytes(&mut hasher, field.name().as_bytes());
        digest_bytes(&mut hasher, format!("{:?}", field.data_type()).as_bytes());
        hasher.update([u8::from(field.is_nullable())]);
    }
    Ok(hasher.finalize().into())
}

fn manifest_digest(records: &[AddFilesManifestRecord]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(MANIFEST_DIGEST_DOMAIN);
    hasher.update((records.len() as u64).to_be_bytes());
    for record in records {
        digest_bytes(&mut hasher, record.location.as_bytes());
        hasher.update(record.size.to_be_bytes());
        digest_bytes(
            &mut hasher,
            record.object_identity.as_deref().unwrap_or("").as_bytes(),
        );
        hasher.update(record.footer_digest);
        hasher.update(record.footer_bytes.to_be_bytes());
        hasher.update(record.row_count.to_be_bytes());
        hasher.update(record.schema_identity_digest);
    }
    hasher.finalize().into()
}

fn digest_bytes(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};

    use super::{
        canonical_name_mapping, list_direct_files, read_parquet_footer, read_type_compatible,
        validate_name_mapping_for_target, validate_schema,
    };

    fn field_with_id(name: &str, id: i32, data_type: DataType, nullable: bool) -> Field {
        Field::new(name, data_type, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_string(),
            id.to_string(),
        )]))
    }

    #[test]
    fn direct_listing_ignores_hidden_but_rejects_visible_non_parquet() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("a.parquet"), b"data").expect("file");
        std::fs::write(dir.path().join("_hidden.txt"), b"hidden").expect("hidden");
        let directory = format!("file://{}", dir.path().display());
        let files = list_direct_files(&directory, None).expect("listing");
        assert_eq!(files.len(), 1);

        std::fs::write(dir.path().join("visible.txt"), b"visible").expect("visible");
        assert!(
            list_direct_files(&directory, None)
                .expect_err("visible non-Parquet must fail")
                .contains("not a Parquet")
        );
    }

    #[test]
    fn parquet_footer_carries_rows_schema_and_digest() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("rows.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("batch");
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).expect("file"), schema, None)
                .expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
        let size = std::fs::metadata(&path).expect("metadata").len();
        let footer =
            read_parquet_footer(&format!("file://{}", path.display()), size, None).expect("footer");
        assert_eq!(footer.row_count, 3);
        assert_ne!(footer.footer_digest, [0; 32]);
    }

    #[test]
    fn name_mapping_is_strict_and_canonical() {
        let raw = r#"[{"names":["legacy_id"],"field-id":1}]"#;
        assert_eq!(
            canonical_name_mapping(raw).expect("mapping"),
            r#"[{"field-id":1,"names":["legacy_id"]}]"#
        );
        assert!(
            canonical_name_mapping(r#"[{"field-id":1,"names":["id"],"credential":"secret"}]"#)
                .is_err()
        );
        assert!(
            canonical_name_mapping(
                r#"[
                {"field-id":1,"names":["left"],"fields":[{"field-id":2,"names":["id"]}]},
                {"field-id":3,"names":["right"],"fields":[{"field-id":4,"names":["id"]}]}
            ]"#
            )
            .is_ok()
        );
    }

    #[test]
    fn name_mapping_must_exactly_cover_target_field_ids() {
        let target = Arc::new(Schema::new(vec![
            field_with_id("id", 1, DataType::Int32, false),
            field_with_id("note", 2, DataType::Utf8, true),
        ]));
        let complete: iceberg::spec::NameMapping = serde_json::from_str(
            r#"[{"field-id":1,"names":["old_id"]},{"field-id":2,"names":["old_note"]}]"#,
        )
        .expect("mapping");
        validate_name_mapping_for_target(&complete, &target).expect("complete mapping");

        let incomplete: iceberg::spec::NameMapping =
            serde_json::from_str(r#"[{"field-id":1,"names":["old_id"]}]"#).expect("mapping");
        assert!(
            validate_name_mapping_for_target(&incomplete, &target)
                .expect_err("incomplete mapping")
                .contains("missing=[2]")
        );
        let unknown: iceberg::spec::NameMapping = serde_json::from_str(
            r#"[{"field-id":1,"names":["old_id"]},{"field-id":9,"names":["extra"]}]"#,
        )
        .expect("mapping");
        assert!(
            validate_name_mapping_for_target(&unknown, &target)
                .expect_err("unknown mapping ID")
                .contains("unknown=[9]")
        );
    }

    #[test]
    fn required_target_rejects_nullable_source_even_with_initial_default() {
        let source = Arc::new(Schema::new(vec![field_with_id(
            "id",
            1,
            DataType::Int32,
            true,
        )]));
        let target = Arc::new(Schema::new(vec![field_with_id(
            "id",
            1,
            DataType::Int32,
            false,
        )]));
        assert!(
            validate_schema(&source, &target, &HashSet::from([1]))
                .expect_err("nullable source")
                .contains("nullable source")
        );

        let missing = Arc::new(Schema::empty());
        validate_schema(&missing, &target, &HashSet::from([1]))
            .expect("initial default supplies an absent source field");
    }

    #[test]
    fn schema_promotions_are_narrow() {
        assert!(read_type_compatible(&DataType::Int32, &DataType::Int64));
        assert!(read_type_compatible(&DataType::Float32, &DataType::Float64));
        assert!(read_type_compatible(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(12, 2)
        ));
        assert!(!read_type_compatible(&DataType::Int64, &DataType::Int32));
        assert!(!read_type_compatible(
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(12, 3)
        ));
    }
}
