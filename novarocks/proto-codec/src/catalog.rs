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

//! Validated generated catalog carriers.

use prost::Message;

use crate::{FieldPath, ProtocolError, ProtocolErrorKind};
use novarocks_proto_models::catalog as wire;
use novarocks_spi::connector::{
    CatalogCredentialReference, CatalogHandle, CatalogProperties, CatalogProperty, CatalogProviderKind,
    CatalogVersion, ConnectorInstanceId, CATALOG_VERSION_BYTES, MAX_CATALOGS_PER_QUERY,
    MAX_CATALOG_SET_BYTES,
};

/// One exact, validated query-wide catalog contribution.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogSet {
    raw: wire::CatalogSet,
}

impl CatalogSet {
    pub fn new(catalogs: impl IntoIterator<Item = CatalogProperties>) -> Result<Self, ProtocolError> {
        Self::parse(wire::CatalogSet {
            catalogs: catalogs.into_iter().map(encode_catalog_properties).collect(),
        })
    }

    pub fn parse(raw: wire::CatalogSet) -> Result<Self, ProtocolError> {
        if raw.encoded_len() > MAX_CATALOG_SET_BYTES {
            return Err(resource_exhausted(
                FieldPath::root("catalog_set"),
                "encoded catalog set exceeds 1 MiB",
            ));
        }
        if raw.catalogs.len() > MAX_CATALOGS_PER_QUERY {
            return Err(resource_exhausted(
                FieldPath::root("catalog_set").field("catalogs"),
                "catalog set exceeds 256 entries",
            ));
        }
        let mut previous_name = None;
        for (index, properties) in raw.catalogs.iter().cloned().enumerate() {
            let properties = decode_catalog_properties(
                properties,
                FieldPath::root("catalog_set").field("catalogs").index(index),
            )?;
            let name = properties.handle().catalog_name().as_str().to_owned();
            if previous_name
                .as_deref()
                .is_some_and(|previous| previous >= name.as_str())
            {
                return Err(invalid(
                    FieldPath::root("catalog_set").field("catalogs").index(index),
                    "catalogs must be strictly sorted by catalog name with no duplicate name",
                ));
            }
            previous_name = Some(name);
        }
        Ok(Self { raw })
    }

    pub const fn as_proto(&self) -> &wire::CatalogSet {
        &self.raw
    }

    pub fn catalogs(&self) -> Result<Vec<CatalogProperties>, ProtocolError> {
        self.raw
            .catalogs
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, properties)| {
                decode_catalog_properties(
                    properties,
                    FieldPath::root("catalog_set").field("catalogs").index(index),
                )
            })
            .collect()
    }
}

pub fn encode_catalog_handle(handle: &CatalogHandle) -> wire::CatalogHandle {
    wire::CatalogHandle {
        catalog_name: handle.catalog_name().as_str().to_owned(),
        version: handle.version().as_bytes().to_vec(),
    }
}

pub fn decode_catalog_handle(
    raw: wire::CatalogHandle,
    root: FieldPath,
) -> Result<CatalogHandle, ProtocolError> {
    let catalog_name = ConnectorInstanceId::try_from_canonical(&raw.catalog_name)
        .map_err(|error| invalid(root.clone().field("catalog_name"), error.to_string()))?;
    let version: [u8; CATALOG_VERSION_BYTES] = raw.version.try_into().map_err(|_| {
        invalid(
            root.field("version"),
            "catalog version must contain exactly 32 bytes",
        )
    })?;
    Ok(CatalogHandle::new(
        catalog_name,
        CatalogVersion::from_bytes(version),
    ))
}

pub fn encode_catalog_properties(properties: CatalogProperties) -> wire::CatalogProperties {
    wire::CatalogProperties {
        handle: Some(encode_catalog_handle(properties.handle())),
        provider_kind: encode_provider_kind(properties.provider_kind()) as i32,
        config_format_version: properties.config_format_version(),
        execution_properties: properties
            .execution_properties()
            .iter()
            .map(|property| wire::CatalogProperty {
                key: property.key().to_owned(),
                value: property.value().to_owned(),
            })
            .collect(),
        credential_references: properties
            .credential_references()
            .iter()
            .map(|reference| wire::CatalogCredentialReference {
                name: reference.name().to_owned(),
                revision: reference.revision().map(str::to_owned),
            })
            .collect(),
    }
}

pub fn decode_catalog_properties(
    raw: wire::CatalogProperties,
    root: FieldPath,
) -> Result<CatalogProperties, ProtocolError> {
    let handle = raw
        .handle
        .ok_or_else(|| missing(root.clone().field("handle"), "catalog handle is required"))
        .and_then(|handle| decode_catalog_handle(handle, root.clone().field("handle")))?;
    let provider_kind = decode_provider_kind(raw.provider_kind, root.clone().field("provider_kind"))?;
    let mut properties = Vec::with_capacity(raw.execution_properties.len());
    for (index, property) in raw.execution_properties.into_iter().enumerate() {
        let decoded = CatalogProperty::new(&property.key, &property.value).map_err(|error| {
            invalid(
                root.clone().field("execution_properties").index(index),
                error.to_string(),
            )
        })?;
        if properties
            .last()
            .is_some_and(|previous: &CatalogProperty| previous.key() >= decoded.key())
        {
            return Err(invalid(
                root.clone().field("execution_properties").index(index),
                "catalog properties must be strictly sorted by key with no duplicate key",
            ));
        }
        properties.push(decoded);
    }
    let mut references = Vec::with_capacity(raw.credential_references.len());
    for (index, reference) in raw.credential_references.into_iter().enumerate() {
        let decoded = CatalogCredentialReference::new(&reference.name, reference.revision.as_deref())
            .map_err(|error| {
                invalid(
                    root.clone().field("credential_references").index(index),
                    error.to_string(),
                )
            })?;
        if references.last().is_some_and(|previous| previous >= &decoded) {
            return Err(invalid(
                root.clone().field("credential_references").index(index),
                "catalog credential references must be strictly sorted with no duplicate",
            ));
        }
        references.push(decoded);
    }
    CatalogProperties::new(
        handle,
        provider_kind,
        raw.config_format_version,
        properties,
        references,
    )
    .map_err(|error| invalid(root, error.to_string()))
}

fn encode_provider_kind(value: CatalogProviderKind) -> wire::CatalogProviderKind {
    match value {
        CatalogProviderKind::Iceberg => wire::CatalogProviderKind::Iceberg,
        CatalogProviderKind::StarRocks => wire::CatalogProviderKind::Starrocks,
    }
}

fn decode_provider_kind(value: i32, root: FieldPath) -> Result<CatalogProviderKind, ProtocolError> {
    match wire::CatalogProviderKind::try_from(value) {
        Ok(wire::CatalogProviderKind::Iceberg) => Ok(CatalogProviderKind::Iceberg),
        Ok(wire::CatalogProviderKind::Starrocks) => Ok(CatalogProviderKind::StarRocks),
        _ => Err(invalid(root, "catalog provider kind is required and must be known")),
    }
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::MissingField, detail)
}

fn resource_exhausted(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::Capacity, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn properties(name: &str, version: u8) -> CatalogProperties {
        CatalogProperties::new(
            CatalogHandle::new(
                ConnectorInstanceId::try_from_canonical(name).unwrap(),
                CatalogVersion::from_bytes([version; CATALOG_VERSION_BYTES]),
            ),
            CatalogProviderKind::Iceberg,
            1,
            vec![CatalogProperty::new("warehouse", "s3://warehouse").unwrap()],
            vec![CatalogCredentialReference::new("object_store", Some("v1")).unwrap()],
        )
        .unwrap()
    }

    #[test]
    fn catalog_set_round_trips_exact_sorted_values() {
        let set = CatalogSet::new([properties("alpha", 1), properties("beta", 2)]).unwrap();
        let decoded = CatalogSet::parse(set.as_proto().clone()).unwrap().catalogs().unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[1].handle().catalog_name().as_str(), "beta");
    }

    #[test]
    fn catalog_set_rejects_noncanonical_versions_and_order() {
        let mut raw = CatalogSet::new([properties("alpha", 1)]).unwrap().as_proto().clone();
        raw.catalogs[0].handle.as_mut().unwrap().version.clear();
        assert!(CatalogSet::parse(raw).is_err());

        let raw = wire::CatalogSet {
            catalogs: vec![
                encode_catalog_properties(properties("beta", 2)),
                encode_catalog_properties(properties("alpha", 1)),
            ],
        };
        assert!(CatalogSet::parse(raw).is_err());
    }
}
