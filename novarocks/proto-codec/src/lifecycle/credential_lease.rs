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
    CredentialLoadTableDelegation, CredentialRenewalPath, CredentialRoutingContext,
    MAX_CREDENTIAL_LEASE_ID_BYTES, MAX_CREDENTIAL_LEASE_PREFIXES, MAX_CREDENTIAL_LEASES_PER_QUERY,
    StorageAccessDomainId, StorageCredentialScopePrefix,
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
            CredentialRenewalPath::CredentialsEndpoint(routing) => {
                novarocks::credential_renewal_path::Path::CredentialsEndpoint(
                    encode_routing_context(routing),
                )
            }
            CredentialRenewalPath::LoadTableDelegation { routing, table } => {
                novarocks::credential_renewal_path::Path::LoadTable(
                    novarocks::CredentialLoadTableDelegation {
                        namespace: table
                            .namespace()
                            .iter()
                            .map(|level| level.as_ref().to_owned())
                            .collect(),
                        table: table.table().to_owned(),
                        table_uuid: table.table_uuid().to_owned(),
                        routing: Some(encode_routing_context(routing)),
                    },
                )
            }
        }),
    }
}

/// Encodes the routing context in its canonical header order.
///
/// The SPI type already holds headers sorted and unique by name, and the
/// replay comparison of an announcement is message-exact, so equal contexts
/// must encode to equal messages.
fn encode_routing_context(
    routing: &CredentialRoutingContext,
) -> novarocks::CredentialRoutingContext {
    novarocks::CredentialRoutingContext {
        url: routing.url().to_owned(),
        headers: routing
            .headers()
            .iter()
            .map(|header| novarocks::CredentialRoutingHeader {
                name: header.name().to_owned(),
                value: header.value().to_owned(),
            })
            .collect(),
    }
}

/// Decodes one routing context through the SPI's own validation.
///
/// Headers must already arrive in canonical order, like prefixes: the replay
/// comparison is message-exact, so a producer that reordered them would make
/// one context look like two. The error detail names the rule, never a header
/// value: the SPI error text carries only the violated subject.
fn decode_routing_context(
    raw: Option<novarocks::CredentialRoutingContext>,
    root: FieldPath,
) -> Result<CredentialRoutingContext, ProtocolError> {
    let raw = raw.ok_or_else(|| {
        missing(
            root.clone(),
            "credential renewal path must carry its routing context",
        )
    })?;
    let canonical = raw
        .headers
        .windows(2)
        .all(|pair| pair[0].name < pair[1].name);
    let routing = CredentialRoutingContext::try_new(
        raw.url,
        raw.headers.into_iter().map(|header| {
            (
                std::sync::Arc::from(header.name),
                std::sync::Arc::from(header.value),
            )
        }),
    )
    .map_err(|error| invalid(root.clone(), error.to_string()))?;
    if !canonical {
        return Err(invalid(
            root.field("headers"),
            "credential routing headers must be strictly sorted and unique by name",
        ));
    }
    Ok(routing)
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
        novarocks::credential_renewal_path::Path::CredentialsEndpoint(routing) => {
            decode_routing_context(Some(routing), root.field("credentials_endpoint"))
                .map(|routing| Some(CredentialRenewalPath::CredentialsEndpoint(routing)))
        }
        novarocks::credential_renewal_path::Path::LoadTable(delegation) => {
            let root = root.field("load_table");
            let routing =
                decode_routing_context(delegation.routing, root.clone().field("routing"))?;
            let namespace = delegation
                .namespace
                .into_iter()
                .map(std::sync::Arc::from)
                .collect();
            let table = CredentialLoadTableDelegation::try_new(
                namespace,
                std::sync::Arc::from(delegation.table),
                std::sync::Arc::from(delegation.table_uuid),
            )
            .map_err(|error| invalid(root, error.to_string()))?;
            Ok(Some(CredentialRenewalPath::LoadTableDelegation {
                routing,
                table,
            }))
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
        CredentialRenewalPath, CredentialRoutingContext, StorageAccessDomainId,
        StorageCredentialScopePrefix,
    };
    use std::sync::Arc;

    fn routing(url: &str, headers: &[(&str, &str)]) -> CredentialRoutingContext {
        CredentialRoutingContext::try_new(
            url,
            headers
                .iter()
                .map(|(name, value)| (Arc::from(*name), Arc::from(*value))),
        )
        .expect("routing context")
    }

    fn announcing(path: CredentialRenewalPath) -> CredentialLeaseDescriptor {
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
            Some(path),
            StorageAccessDomainId::from_bytes([8; 32]),
        )
        .expect("descriptor")
    }

    fn load_table_path(routing: CredentialRoutingContext) -> CredentialRenewalPath {
        CredentialRenewalPath::LoadTableDelegation {
            routing,
            table: CredentialLoadTableDelegation::try_new(
                vec![Arc::from("sales")],
                Arc::from("orders"),
                Arc::from("8f1d0c6e-0000-4000-8000-000000000001"),
            )
            .expect("delegation"),
        }
    }

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
            Some(CredentialRenewalPath::CredentialsEndpoint(routing(
                "https://rest/v1/credentials",
                &[],
            ))),
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
            Some(load_table_path(routing(
                "https://rest/v1/prefix/namespaces/sales/tables/orders",
                &[],
            ))),
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

    #[test]
    fn both_paths_carry_their_routing_context_across_the_wire_in_canonical_order() {
        let routing = routing(
            "https://rest/v1/p1/namespaces/sales/tables/orders",
            &[
                ("x-project-id", "project-canary"),
                ("polaris-realm", "realm-canary"),
            ],
        );
        for path in [
            CredentialRenewalPath::CredentialsEndpoint(routing.clone()),
            load_table_path(routing.clone()),
        ] {
            let descriptor = announcing(path);
            let raw = encode_credential_lease_descriptor(&descriptor);
            let wire_routing = match raw
                .renewal_path
                .as_ref()
                .and_then(|path| path.path.as_ref())
                .expect("encoded path")
            {
                novarocks::credential_renewal_path::Path::CredentialsEndpoint(routing) => routing,
                novarocks::credential_renewal_path::Path::LoadTable(delegation) => {
                    delegation.routing.as_ref().expect("encoded routing")
                }
            };
            assert_eq!(wire_routing.url, routing.url());
            assert_eq!(
                wire_routing
                    .headers
                    .iter()
                    .map(|header| (header.name.as_str(), header.value.as_str()))
                    .collect::<Vec<_>>(),
                [
                    ("polaris-realm", "realm-canary"),
                    ("x-project-id", "project-canary"),
                ]
            );
            let decoded = decode_credential_lease_descriptor(raw, FieldPath::root("descriptor"))
                .expect("descriptor");
            assert_eq!(decoded, descriptor);
            assert_eq!(
                decoded.renewal_path().map(|path| path.routing()),
                Some(&routing)
            );
            assert!(!format!("{decoded:?}").contains("canary"));
        }
    }

    type RoutingEdit = dyn Fn(&mut Option<novarocks::CredentialRoutingContext>);

    /// Encodes `path`, then lets `edit` tamper with the wire routing context.
    /// The endpoint variant holds its context directly, so an edit that drops
    /// it leaves the empty message a producer would have sent.
    fn with_wire_routing(
        path: CredentialRenewalPath,
        edit: impl Fn(&mut Option<novarocks::CredentialRoutingContext>),
    ) -> novarocks::CredentialLeaseDescriptor {
        let mut raw = encode_credential_lease_descriptor(&announcing(path));
        match raw
            .renewal_path
            .as_mut()
            .and_then(|path| path.path.as_mut())
            .expect("encoded path")
        {
            novarocks::credential_renewal_path::Path::CredentialsEndpoint(routing) => {
                let mut slot = Some(std::mem::take(routing));
                edit(&mut slot);
                *routing = slot.unwrap_or_default();
            }
            novarocks::credential_renewal_path::Path::LoadTable(delegation) => {
                edit(&mut delegation.routing);
            }
        }
        raw
    }

    #[test]
    fn a_routing_context_that_violates_its_contract_is_refused_on_decode() {
        let base = routing(
            "https://rest/v1/p1/credentials",
            &[("polaris-realm", "r1"), ("x-project-id", "p1")],
        );
        let edits: [(&str, Box<RoutingEdit>); 5] = [
            ("absent", Box::new(|routing| *routing = None)),
            (
                "not canonical",
                Box::new(|routing| routing.as_mut().expect("routing").headers.reverse()),
            ),
            (
                "duplicate",
                Box::new(|routing| {
                    let routing = routing.as_mut().expect("routing");
                    routing.headers[1].name = routing.headers[0].name.clone();
                }),
            ),
            (
                "not a lowercase token",
                Box::new(|routing| {
                    routing.as_mut().expect("routing").headers[0].name = "Polaris-Realm".into()
                }),
            ),
            (
                "not an http address",
                Box::new(|routing| {
                    routing.as_mut().expect("routing").url = "file:///v1/credentials".into()
                }),
            ),
        ];
        for (case, edit) in &edits {
            for path in [
                CredentialRenewalPath::CredentialsEndpoint(base.clone()),
                load_table_path(base.clone()),
            ] {
                let raw = with_wire_routing(path, edit);
                assert!(
                    decode_credential_lease_descriptor(raw, FieldPath::root("descriptor")).is_err(),
                    "{case} routing context must be refused"
                );
            }
        }
    }

    #[test]
    fn a_header_value_never_appears_in_a_decode_error() {
        let raw = with_wire_routing(
            CredentialRenewalPath::CredentialsEndpoint(routing(
                "https://rest/v1/credentials",
                &[("x-project-id", "p1")],
            )),
            |routing| {
                routing.as_mut().expect("routing").headers[0].value =
                    "value-canary\r\nx-injected: yes".into()
            },
        );
        let error = decode_credential_lease_descriptor(raw, FieldPath::root("descriptor"))
            .expect_err("a value that could split a header is refused");
        assert!(!error.to_string().contains("canary"), "{error}");
        assert!(!format!("{error:?}").contains("canary"), "{error:?}");
    }
}
