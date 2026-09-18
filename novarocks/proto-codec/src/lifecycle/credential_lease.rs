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

//! Confidential query-attempt credential lease protocol values.
//!
//! Only descriptor metadata is manifest/digest material. The SPI confidential
//! envelope owns secret wrapping; this codec only validates and encodes the
//! TLS-only wire carrier.

use novarocks_proto_models::novarocks;
use novarocks_spi::connector::{
    CatalogCredentialMode, CatalogCredentialPurpose, CredentialConsumerRole,
    CredentialLeaseDescriptor, CredentialLeaseId, CredentialLeaseProvider,
    CredentialLoadTableDelegation, CredentialRenewalPath, MAX_CREDENTIAL_LEASE_ID_BYTES,
    MAX_CREDENTIAL_LEASE_PREFIXES, MAX_CREDENTIAL_LEASES_PER_QUERY, StorageAccessDomainId,
    StorageCredentialScopePrefix,
};

use crate::catalog::{CatalogSet, decode_catalog_handle, encode_catalog_handle};
use crate::{FieldPath, ProtocolError, ProtocolErrorKind};

/// Parses and encodes only the descriptor portion of the credential contract.
pub fn encode_credential_lease_descriptor(
    descriptor: &CredentialLeaseDescriptor,
) -> novarocks::CredentialLeaseDescriptor {
    novarocks::CredentialLeaseDescriptor {
        lease_id: descriptor.lease_id().as_bytes().to_vec(),
        epoch: descriptor.epoch(),
        owner: Some(encode_catalog_handle(descriptor.owner())),
        provider: match descriptor.provider() {
            CredentialLeaseProvider::S3 => novarocks::CredentialLeaseProvider::S3 as i32,
        },
        prefixes: descriptor
            .prefixes()
            .iter()
            .map(|prefix| prefix.as_str().to_owned())
            .collect(),
        not_after_unix_ms: descriptor.not_after_unix_ms(),
        refresh_capable: descriptor.refresh_capable(),
        storage_access_domain_id: descriptor.storage_access_domain_id().as_bytes().to_vec(),
        renewal_path: descriptor.renewal_path().map(encode_renewal_path),
    }
}

fn encode_renewal_path(path: &CredentialRenewalPath) -> novarocks::CredentialRenewalPath {
    novarocks::CredentialRenewalPath {
        path: Some(match path {
            CredentialRenewalPath::CredentialsEndpoint(endpoint) => {
                novarocks::credential_renewal_path::Path::CredentialsEndpoint(
                    endpoint.as_ref().to_owned(),
                )
            }
            CredentialRenewalPath::LoadTableDelegation(delegation) => {
                novarocks::credential_renewal_path::Path::LoadTable(
                    novarocks::CredentialLoadTableDelegation {
                        namespace: delegation
                            .namespace()
                            .iter()
                            .map(|level| level.as_ref().to_owned())
                            .collect(),
                        table: delegation.table().to_owned(),
                        table_uuid: delegation.table_uuid().to_owned(),
                    },
                )
            }
        }),
    }
}

/// Decode one advertised acquisition path.
///
/// An absent message is "the catalog advertised none", which is a real answer
/// rather than a missing field. A present message with no variant set is not:
/// it is a producer that failed to say which path it meant.
fn decode_renewal_path(
    raw: Option<novarocks::CredentialRenewalPath>,
    root: FieldPath,
) -> Result<Option<CredentialRenewalPath>, ProtocolError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let path = raw.path.ok_or_else(|| {
        invalid(
            root.clone(),
            "credential renewal path must name exactly one acquisition path",
        )
    })?;
    match path {
        novarocks::credential_renewal_path::Path::CredentialsEndpoint(endpoint) => {
            if endpoint.is_empty() {
                return Err(invalid(
                    root.field("credentials_endpoint"),
                    "advertised credentials endpoint must not be empty",
                ));
            }
            Ok(Some(CredentialRenewalPath::CredentialsEndpoint(
                std::sync::Arc::from(endpoint),
            )))
        }
        novarocks::credential_renewal_path::Path::LoadTable(delegation) => {
            let namespace = delegation
                .namespace
                .into_iter()
                .map(std::sync::Arc::from)
                .collect();
            CredentialLoadTableDelegation::try_new(
                namespace,
                std::sync::Arc::from(delegation.table),
                std::sync::Arc::from(delegation.table_uuid),
            )
            .map(|delegation| Some(CredentialRenewalPath::LoadTableDelegation(delegation)))
            .map_err(|error| invalid(root.field("load_table"), error.to_string()))
        }
    }
}

pub fn decode_credential_lease_descriptor(
    raw: novarocks::CredentialLeaseDescriptor,
    root: FieldPath,
) -> Result<CredentialLeaseDescriptor, ProtocolError> {
    let lease_id = decode_lease_id(&raw.lease_id, root.clone().field("lease_id"))?;
    if raw.epoch == 0 {
        return Err(invalid(
            root.clone().field("epoch"),
            "credential lease epoch must be nonzero",
        ));
    }
    let owner = raw.owner.ok_or_else(|| {
        missing(
            root.clone().field("owner"),
            "credential lease owner is required",
        )
    })?;
    let owner = decode_catalog_handle(owner, root.clone().field("owner"))?;
    let provider = match novarocks::CredentialLeaseProvider::try_from(raw.provider) {
        Ok(novarocks::CredentialLeaseProvider::S3) => CredentialLeaseProvider::S3,
        _ => {
            return Err(invalid(
                root.clone().field("provider"),
                "credential lease provider must be S3",
            ));
        }
    };
    if raw.prefixes.is_empty() || raw.prefixes.len() > MAX_CREDENTIAL_LEASE_PREFIXES {
        return Err(resource_exhausted(
            root.clone().field("prefixes"),
            "credential lease prefixes must contain 1..=64 entries",
        ));
    }
    let mut prefixes = Vec::with_capacity(raw.prefixes.len());
    for (index, value) in raw.prefixes.iter().enumerate() {
        let prefix = StorageCredentialScopePrefix::try_from_normalized(value).map_err(|error| {
            invalid(
                root.clone().field("prefixes").index(index),
                format!("invalid canonical S3 credential prefix: {error}"),
            )
        })?;
        if prefixes
            .last()
            .is_some_and(|previous: &StorageCredentialScopePrefix| previous >= &prefix)
        {
            return Err(invalid(
                root.clone().field("prefixes").index(index),
                "credential lease prefixes must be strictly sorted and unique",
            ));
        }
        prefixes.push(prefix);
    }
    if raw.not_after_unix_ms == 0 {
        return Err(invalid(
            root.clone().field("not_after_unix_ms"),
            "credential lease expiration must be nonzero",
        ));
    }
    let domain: [u8; 32] = raw.storage_access_domain_id.try_into().map_err(|_| {
        invalid(
            root.clone().field("storage_access_domain_id"),
            "storage access domain id must contain exactly 32 bytes",
        )
    })?;
    CredentialLeaseDescriptor::try_new(
        lease_id,
        raw.epoch,
        owner,
        provider,
        prefixes,
        raw.not_after_unix_ms,
        raw.refresh_capable,
        decode_renewal_path(raw.renewal_path, root.field("renewal_path"))?,
        StorageAccessDomainId::from_bytes(domain),
    )
    .map_err(|error| invalid(root, error.to_string()))
}

/// Validates one ordered descriptor contribution against the exact catalog set
/// frozen in the same participant manifest.
pub fn validate_credential_lease_descriptors(
    descriptors: &[novarocks::CredentialLeaseDescriptor],
    catalog_set: &CatalogSet,
    root: FieldPath,
) -> Result<(), ProtocolError> {
    if descriptors.len() > MAX_CREDENTIAL_LEASES_PER_QUERY {
        return Err(resource_exhausted(
            root.field("credential_lease_descriptors"),
            "credential lease contribution exceeds 64 entries",
        ));
    }
    let catalogs = catalog_set.catalogs()?;
    let mut previous = None;
    for (index, raw) in descriptors.iter().cloned().enumerate() {
        let path = root
            .clone()
            .field("credential_lease_descriptors")
            .index(index);
        let descriptor = decode_credential_lease_descriptor(raw, path.clone())?;
        let current = descriptor.lease_id();
        if previous.is_some_and(|previous: CredentialLeaseId| previous >= current) {
            return Err(invalid(
                path.field("lease_id"),
                "credential lease descriptors must be strictly sorted and unique by lease id",
            ));
        }
        previous = Some(current);
        let owner = catalogs
            .iter()
            .find(|properties| properties.handle() == descriptor.owner())
            .ok_or_else(|| {
                invalid(
                    path.clone().field("owner"),
                    "credential lease owner is not present in the participant catalog set",
                )
            })?;
        let has_vended_data_binding = owner.credential_bindings().iter().any(|binding| {
            binding.purpose() == CatalogCredentialPurpose::ObjectStoreData
                && binding.consumer_role() == CredentialConsumerRole::Backend
                && matches!(binding.mode(), CatalogCredentialMode::Vended)
        });
        if !has_vended_data_binding {
            return Err(invalid(
                path.field("owner"),
                "credential lease owner does not declare vended object-store data credentials",
            ));
        }
    }
    Ok(())
}

/// Validates one InitQuery descriptor set.
///
/// It used to validate descriptor/envelope pairing as well. Nothing pairs any
/// more: material is acquired by the node that consumes it and never appears
/// on this transport, so a descriptor set stands alone (CAD-1 D1).
pub fn validate_initial_credential_lease_descriptors(
    descriptors: &[novarocks::CredentialLeaseDescriptor],
    root: FieldPath,
) -> Result<(), ProtocolError> {
    if descriptors.len() > MAX_CREDENTIAL_LEASES_PER_QUERY {
        return Err(resource_exhausted(
            root.field("credential_lease_descriptors"),
            "credential lease descriptor contribution exceeds 64 entries",
        ));
    }
    let mut previous = None;
    for (index, raw) in descriptors.iter().cloned().enumerate() {
        let path = root
            .clone()
            .field("credential_lease_descriptors")
            .index(index);
        let descriptor = decode_credential_lease_descriptor(raw, path.clone())?;
        if previous.is_some_and(|previous: CredentialLeaseId| previous >= descriptor.lease_id()) {
            return Err(invalid(
                path.field("lease_id"),
                "credential lease descriptors must be strictly sorted and unique by lease id",
            ));
        }
        previous = Some(descriptor.lease_id());
    }
    Ok(())
}

pub fn decode_lease_id(raw: &[u8], root: FieldPath) -> Result<CredentialLeaseId, ProtocolError> {
    let bytes: [u8; MAX_CREDENTIAL_LEASE_ID_BYTES] = raw.try_into().map_err(|_| {
        invalid(
            root.clone(),
            "credential lease id must contain exactly 16 bytes",
        )
    })?;
    CredentialLeaseId::try_from_bytes(bytes).map_err(|error| invalid(root, error.to_string()))
}

pub fn validate_lease_epoch(
    lease_id: &[u8],
    epoch: u64,
    root: FieldPath,
) -> Result<(CredentialLeaseId, u64), ProtocolError> {
    let lease_id = decode_lease_id(lease_id, root.clone().field("lease_id"))?;
    if epoch == 0 {
        return Err(invalid(
            root.field("epoch"),
            "credential lease epoch must be nonzero",
        ));
    }
    Ok((lease_id, epoch))
}

fn invalid(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn missing(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::InvalidValue, detail)
}

fn resource_exhausted(path: FieldPath, detail: impl Into<String>) -> ProtocolError {
    ProtocolError::new(path, ProtocolErrorKind::Capacity, detail)
}

#[cfg(test)]
mod tests {
    use super::{
        decode_credential_lease_descriptor, encode_credential_lease_descriptor,
        validate_initial_credential_lease_descriptors,
    };
    use crate::FieldPath;
    use novarocks_proto_models::novarocks;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseDescriptor,
        CredentialLeaseId, CredentialLeaseProvider, CredentialLoadTableDelegation,
        CredentialRenewalPath, StorageAccessDomainId, StorageCredentialScopePrefix,
    };

    fn descriptor() -> CredentialLeaseDescriptor {
        CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            3,
            CatalogHandle::new(
                ConnectorInstanceId::parse("warehouse").expect("catalog"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            CredentialLeaseProvider::S3,
            vec![
                StorageCredentialScopePrefix::try_from_normalized("s3://bucket/data")
                    .expect("prefix"),
            ],
            99,
            true,
            None,
            StorageAccessDomainId::from_bytes([8; 32]),
        )
        .expect("descriptor")
    }

    #[test]
    fn descriptor_round_trip_has_no_secret_material() {
        let raw = encode_credential_lease_descriptor(&descriptor());
        let decoded =
            decode_credential_lease_descriptor(raw, FieldPath::root("credential_lease_descriptor"))
                .expect("descriptor");
        assert_eq!(decoded, descriptor());
        assert!(!format!("{decoded:?}").contains("canary"));
    }

    #[test]
    fn the_acquisition_address_crosses_the_wire_and_its_absence_is_an_answer() {
        // CAD-1 D1 depends on this field reaching the node that acquires: the
        // descriptor is where a non-secret capability announcement belongs, and
        // an execution node with no address cannot renew (D11).
        let announced = CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            3,
            CatalogHandle::new(
                ConnectorInstanceId::parse("warehouse").expect("catalog"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            CredentialLeaseProvider::S3,
            vec![
                StorageCredentialScopePrefix::try_from_normalized("s3://bucket/data")
                    .expect("prefix"),
            ],
            99,
            true,
            Some(CredentialRenewalPath::CredentialsEndpoint(
                std::sync::Arc::from("https://rest/v1/credentials"),
            )),
            StorageAccessDomainId::from_bytes([8; 32]),
        )
        .expect("descriptor");

        let raw = encode_credential_lease_descriptor(&announced);
        let decoded =
            decode_credential_lease_descriptor(raw, FieldPath::root("credential_lease_descriptor"))
                .expect("descriptor");
        assert_eq!(
            decoded.credentials_endpoint(),
            Some("https://rest/v1/credentials")
        );

        // The other advertised path names a table rather than an address, and
        // it must survive the same round trip: an execution node that lost the
        // uuid could install material for a different table.
        let load_table = CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            3,
            CatalogHandle::new(
                ConnectorInstanceId::parse("warehouse").expect("catalog"),
                CatalogVersion::from_bytes([7; 32]),
            ),
            CredentialLeaseProvider::S3,
            vec![
                StorageCredentialScopePrefix::try_from_normalized("s3://bucket/data")
                    .expect("prefix"),
            ],
            99,
            true,
            Some(CredentialRenewalPath::LoadTableDelegation(
                CredentialLoadTableDelegation::try_new(
                    vec![std::sync::Arc::from("sales")],
                    std::sync::Arc::from("orders"),
                    std::sync::Arc::from("8f1d0c6e-0000-4000-8000-000000000001"),
                )
                .expect("delegation"),
            )),
            StorageAccessDomainId::from_bytes([8; 32]),
        )
        .expect("descriptor");
        let decoded = decode_credential_lease_descriptor(
            encode_credential_lease_descriptor(&load_table),
            FieldPath::root("descriptor"),
        )
        .expect("descriptor");
        assert_eq!(decoded, load_table);
        assert_eq!(decoded.credentials_endpoint(), None);

        // An absent message is "the catalog advertised none", not a field that
        // failed to arrive; a present message naming no path is a producer bug.
        let mut silent = encode_credential_lease_descriptor(&announced);
        silent.renewal_path = None;
        let decoded = decode_credential_lease_descriptor(silent, FieldPath::root("descriptor"))
            .expect("descriptor");
        assert_eq!(decoded.renewal_path(), None);

        let mut unnamed = encode_credential_lease_descriptor(&announced);
        unnamed.renewal_path = Some(novarocks::CredentialRenewalPath { path: None });
        assert!(
            decode_credential_lease_descriptor(unnamed, FieldPath::root("descriptor")).is_err()
        );
    }

    #[test]
    fn an_init_announcement_must_be_sorted_and_unique_by_lease_id() {
        // This used to check descriptor-to-envelope pairing. Nothing pairs any
        // more, so what remains is the ordering rule that keeps one
        // announcement from silently replacing another inside one rotation.
        let encoded = encode_credential_lease_descriptor(&descriptor());
        validate_initial_credential_lease_descriptors(
            std::slice::from_ref(&encoded),
            FieldPath::root("init_query_request"),
        )
        .expect("one announcement");
        assert!(
            validate_initial_credential_lease_descriptors(
                &[encoded.clone(), encoded],
                FieldPath::root("init_query_request"),
            )
            .is_err()
        );
    }
}
