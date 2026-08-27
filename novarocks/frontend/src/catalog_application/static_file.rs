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

//! Bounded, one-shot parsing for the StaticFile catalog desired-state source.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::Read;
use std::path::Path;

use serde::Deserialize;
use uuid::Uuid;

use super::desired_state::{
    CatalogCredentialReference, CatalogDesiredStateEntry, CatalogDesiredStateSnapshot,
    CatalogDesiredStateSourceMode, CatalogLogicalConfig, CatalogSourceEntryIdentity,
};
use super::{CatalogApplicationError, CatalogApplicationErrorKind};
use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId};

const MAX_FILE_BYTES: usize = 4 * 1024 * 1024;
const MAX_CATALOGS: usize = 1024;
const MAX_PROPERTIES_PER_CATALOG: usize = 256;
const MAX_CREDENTIAL_REFERENCES_PER_CATALOG: usize = 16;
const MAX_DISPLAY_NAME_BYTES: usize = 256;
const MAX_CREDENTIAL_REFERENCE_BYTES: usize = 256;
const STATIC_FILE_FORMAT_VERSION: u8 = 1;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticCatalogFileWire {
    format_version: u8,
    catalogs: Vec<StaticCatalogWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StaticCatalogWire {
    instance_id: String,
    provider_id: String,
    display_name: String,
    config_format_version: u8,
    #[serde(default)]
    credential_references: Vec<String>,
    properties: BTreeMap<String, String>,
}

/// Loads one complete StaticFile desired-state snapshot.
///
/// This function intentionally owns the only file read. It reads at most one
/// byte beyond the configured bound, then parses and validates the complete
/// document before constructing any entry. Every error therefore means the
/// whole snapshot is untrustworthy; callers must not publish a subset.
pub fn load_static_file_snapshot(
    path: &Path,
) -> Result<CatalogDesiredStateSnapshot, CatalogApplicationError> {
    let source = read_bounded_utf8(path)?;
    let file: StaticCatalogFileWire = toml::from_str(&source).map_err(|error| {
        whole_file_error(format!(
            "parse static catalog file {}: {error}",
            path.display()
        ))
    })?;
    if file.format_version != STATIC_FILE_FORMAT_VERSION {
        return Err(whole_file_error(format!(
            "static catalog file {} declares unsupported format_version {}; expected 1",
            path.display(),
            file.format_version
        )));
    }
    if file.catalogs.len() > MAX_CATALOGS {
        return Err(whole_file_error(format!(
            "static catalog file {} declares more than {MAX_CATALOGS} catalogs",
            path.display()
        )));
    }

    let entries = file
        .catalogs
        .into_iter()
        .map(parse_catalog)
        .collect::<Result<Vec<_>, _>>()?;
    CatalogDesiredStateSnapshot::try_new(CatalogDesiredStateSourceMode::StaticFile, entries)
}

fn read_bounded_utf8(path: &Path) -> Result<String, CatalogApplicationError> {
    let mut file = File::open(path).map_err(|error| {
        whole_file_error(format!(
            "read static catalog file {}: {error}",
            path.display()
        ))
    })?;
    let mut bytes = Vec::with_capacity(MAX_FILE_BYTES.min(64 * 1024));
    file.by_ref()
        .take((MAX_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            whole_file_error(format!(
                "read static catalog file {}: {error}",
                path.display()
            ))
        })?;
    if bytes.len() > MAX_FILE_BYTES {
        return Err(whole_file_error(format!(
            "static catalog file {} exceeds the {MAX_FILE_BYTES}-byte limit",
            path.display()
        )));
    }
    String::from_utf8(bytes).map_err(|error| {
        whole_file_error(format!(
            "static catalog file {} is not valid UTF-8: {error}",
            path.display()
        ))
    })
}

fn parse_catalog(
    wire: StaticCatalogWire,
) -> Result<CatalogDesiredStateEntry, CatalogApplicationError> {
    if wire.config_format_version != STATIC_FILE_FORMAT_VERSION {
        return Err(whole_file_error(format!(
            "catalog `{}` declares unsupported config_format_version {}; expected 1",
            wire.instance_id, wire.config_format_version
        )));
    }
    if wire.display_name.trim().is_empty() || wire.display_name.len() > MAX_DISPLAY_NAME_BYTES {
        return Err(whole_file_error(format!(
            "catalog `{}` display_name must be non-empty and at most {MAX_DISPLAY_NAME_BYTES} UTF-8 bytes",
            wire.instance_id
        )));
    }
    if wire.properties.len() > MAX_PROPERTIES_PER_CATALOG {
        return Err(whole_file_error(format!(
            "catalog `{}` declares more than {MAX_PROPERTIES_PER_CATALOG} properties",
            wire.instance_id
        )));
    }
    if wire.credential_references.len() > MAX_CREDENTIAL_REFERENCES_PER_CATALOG {
        return Err(whole_file_error(format!(
            "catalog `{}` declares more than {MAX_CREDENTIAL_REFERENCES_PER_CATALOG} credential references",
            wire.instance_id
        )));
    }
    let instance_id = ConnectorInstanceId::parse(&wire.instance_id).map_err(|error| {
        whole_file_error(format!(
            "invalid catalog instance_id `{}`: {error}",
            wire.instance_id
        ))
    })?;
    let provider_id = ConnectorProviderId::parse(&wire.provider_id).map_err(|error| {
        whole_file_error(format!(
            "invalid catalog provider_id `{}`: {error}",
            wire.provider_id
        ))
    })?;
    let properties = wire
        .properties
        .into_iter()
        .map(|(key, value)| validate_durable_property_key(&key).map(|()| (key, value)))
        .collect::<Result<Vec<_>, _>>()?;
    let mut references = BTreeSet::new();
    for reference in wire.credential_references {
        if !valid_credential_reference(&reference) {
            return Err(whole_file_error(format!(
                "catalog `{}` has an invalid credential reference `{reference}`",
                instance_id.as_str()
            )));
        }
        references.insert(reference);
    }
    Ok(CatalogDesiredStateEntry::new(
        CatalogSourceEntryIdentity::new(Uuid::now_v7()),
        CatalogLogicalConfig::new(
            instance_id,
            provider_id,
            wire.display_name,
            properties,
            references
                .into_iter()
                .map(CatalogCredentialReference::new)
                .collect(),
            STATIC_FILE_FORMAT_VERSION,
        ),
    ))
}

fn validate_durable_property_key(key: &str) -> Result<(), CatalogApplicationError> {
    if key.trim().is_empty() {
        return Err(whole_file_error("catalog property key must not be empty"));
    }
    let normalized = key.to_ascii_lowercase();
    if [
        "password",
        "secret",
        "token",
        "credential",
        "access-key",
        "access_key",
        "private-key",
        "private_key",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        return Err(whole_file_error(format!(
            "credential-like catalog property cannot be durable: {key}"
        )));
    }
    Ok(())
}

fn valid_credential_reference(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= MAX_CREDENTIAL_REFERENCE_BYTES
        && bytes[0].is_ascii_lowercase()
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
}

fn whole_file_error(message: impl Into<String>) -> CatalogApplicationError {
    CatalogApplicationError::new(
        CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete,
        message,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_file(contents: &[u8]) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("temporary file");
        file.write_all(contents).expect("write fixture");
        file
    }

    #[test]
    fn canonicalizes_logical_configuration_and_remints_process_identity() {
        let first = write_file(
            br#"format_version = 1
[[catalogs]]
instance_id = "catalog.analytics"
provider_id = "iceberg"
display_name = "Analytics"
config_format_version = 1
credential_references = ["connector.object_store", "connector.object_store"]
[catalogs.properties]
z = "last"
a = "first"
"#,
        );
        let second = write_file(
            br#"format_version = 1
[[catalogs]]
instance_id = "catalog.analytics"
provider_id = "iceberg"
display_name = "Analytics"
config_format_version = 1
credential_references = ["connector.object_store"]
[catalogs.properties]
a = "first"
z = "last"
"#,
        );
        let one = load_static_file_snapshot(first.path()).expect("first snapshot");
        let two = load_static_file_snapshot(second.path()).expect("second snapshot");
        assert_eq!(one.identity(), two.identity());
        let first_entry = one.into_entries().next().expect("first entry");
        let second_entry = two.into_entries().next().expect("second entry");
        assert_ne!(first_entry.identity(), second_entry.identity());
    }

    #[test]
    fn structural_errors_fail_the_whole_snapshot() {
        for contents in [
            b"format_version = 2\ncatalogs = []\n".as_slice(),
            b"format_version = 1\nunknown = true\ncatalogs = []\n".as_slice(),
            b"format_version = 1\n[[catalogs]]\ninstance_id = 'catalog.a'\nprovider_id = 'iceberg'\ndisplay_name = 'a'\nconfig_format_version = 1\ncredential_references = ['BAD']\n[catalogs.properties]\ntype = 'iceberg'\n".as_slice(),
        ] {
            let file = write_file(contents);
            assert_eq!(
                load_static_file_snapshot(file.path())
                    .expect_err("invalid StaticFile must not produce a subset")
                    .kind(),
                CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete
            );
        }
    }
}
