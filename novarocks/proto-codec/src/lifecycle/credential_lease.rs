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
//! Only descriptor metadata is manifest/digest material. This module projects
//! wire secret scalars into `SecretValue` immediately and implements manual
//! debug output for every value that can contain a credential.

use std::fmt;

use novarocks_proto_models::novarocks;
use novarocks_secret::SecretValue;
use novarocks_spi::connector::{
    CatalogCredentialMode, CatalogCredentialPurpose, CredentialConsumerRole,
    CredentialLeaseDescriptor, CredentialLeaseId, CredentialLeaseProvider,
    MAX_CREDENTIAL_LEASE_ID_BYTES, MAX_CREDENTIAL_LEASE_PREFIXES,
    MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES, MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES,
    MAX_CREDENTIAL_LEASES_PER_QUERY, StorageAccessDomainId, StorageCredentialScopePrefix,
};
use prost::Message;

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
                && binding.consumer_role() == CredentialConsumerRole::FrontendAndBackend
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

/// A parsed secret envelope. It never derives `Debug`; values are rendered as
/// a single redaction marker even when nested in another protocol wrapper.
#[derive(Clone, Eq, PartialEq)]
pub struct CredentialLeaseSecretEnvelope {
    lease_id: CredentialLeaseId,
    epoch: u64,
    access_key_id: SecretValue,
    secret_access_key: SecretValue,
    session_token: SecretValue,
    session_token_expires_at_unix_ms: u64,
}

impl fmt::Debug for CredentialLeaseSecretEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialLeaseSecretEnvelope")
            .field("lease_id", &self.lease_id)
            .field("epoch", &self.epoch)
            .field("secret_scalar_count", &3)
            .field(
                "session_token_expires_at_unix_ms",
                &self.session_token_expires_at_unix_ms,
            )
            .field("material", &"[REDACTED]")
            .finish()
    }
}

impl CredentialLeaseSecretEnvelope {
    pub fn try_new(
        lease_id: CredentialLeaseId,
        epoch: u64,
        access_key_id: SecretValue,
        secret_access_key: SecretValue,
        session_token: SecretValue,
        session_token_expires_at_unix_ms: u64,
    ) -> Result<Self, ProtocolError> {
        validate_secret_scalar(
            access_key_id.expose_secret(),
            FieldPath::root("credential_lease_secret_envelope")
                .field("s3")
                .field("access_key_id"),
        )?;
        validate_secret_scalar(
            secret_access_key.expose_secret(),
            FieldPath::root("credential_lease_secret_envelope")
                .field("s3")
                .field("secret_access_key"),
        )?;
        validate_secret_scalar(
            session_token.expose_secret(),
            FieldPath::root("credential_lease_secret_envelope")
                .field("s3")
                .field("session_token"),
        )?;
        if epoch == 0 || session_token_expires_at_unix_ms == 0 {
            return Err(invalid(
                FieldPath::root("credential_lease_secret_envelope"),
                "credential lease epoch and expiration must be nonzero",
            ));
        }
        Ok(Self {
            lease_id,
            epoch,
            access_key_id,
            secret_access_key,
            session_token,
            session_token_expires_at_unix_ms,
        })
    }

    pub fn parse(
        raw: novarocks::CredentialLeaseSecretEnvelope,
        root: FieldPath,
    ) -> Result<Self, ProtocolError> {
        if raw.encoded_len() > MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES {
            return Err(resource_exhausted(
                root,
                "credential lease secret envelope exceeds 256 KiB",
            ));
        }
        let lease_id = decode_lease_id(&raw.lease_id, root.clone().field("lease_id"))?;
        let material = raw.s3.ok_or_else(|| {
            missing(
                root.clone().field("s3"),
                "credential lease S3 material is required",
            )
        })?;
        validate_secret_scalar(
            &material.access_key_id,
            root.clone().field("s3").field("access_key_id"),
        )?;
        validate_secret_scalar(
            &material.secret_access_key,
            root.clone().field("s3").field("secret_access_key"),
        )?;
        validate_secret_scalar(
            &material.session_token,
            root.clone().field("s3").field("session_token"),
        )?;
        Self::try_new(
            lease_id,
            raw.epoch,
            SecretValue::new(material.access_key_id),
            SecretValue::new(material.secret_access_key),
            SecretValue::new(material.session_token),
            material.session_token_expires_at_unix_ms,
        )
        .map_err(|error| prefix_path(root, error))
    }

    pub fn to_proto(&self) -> novarocks::CredentialLeaseSecretEnvelope {
        novarocks::CredentialLeaseSecretEnvelope {
            lease_id: self.lease_id.as_bytes().to_vec(),
            epoch: self.epoch,
            s3: Some(novarocks::CredentialLeaseS3SecretMaterial {
                access_key_id: self.access_key_id.expose_secret().to_owned(),
                secret_access_key: self.secret_access_key.expose_secret().to_owned(),
                session_token: self.session_token.expose_secret().to_owned(),
                session_token_expires_at_unix_ms: self.session_token_expires_at_unix_ms,
            }),
        }
    }

    pub const fn lease_id(&self) -> CredentialLeaseId {
        self.lease_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn session_token_expires_at_unix_ms(&self) -> u64 {
        self.session_token_expires_at_unix_ms
    }

    pub const fn access_key_id(&self) -> &SecretValue {
        &self.access_key_id
    }

    pub const fn secret_access_key(&self) -> &SecretValue {
        &self.secret_access_key
    }

    pub const fn session_token(&self) -> &SecretValue {
        &self.session_token
    }

    pub fn matches_descriptor(&self, descriptor: &CredentialLeaseDescriptor) -> bool {
        self.lease_id == descriptor.lease_id()
            && self.epoch == descriptor.epoch()
            && self.session_token_expires_at_unix_ms == descriptor.not_after_unix_ms()
    }
}

/// Validates exact InitQuery descriptor/value pairing without placing values in
/// a digest. The caller is still responsible for checking the actual native
/// transport is TLS before accepting these values.
pub fn validate_initial_credential_lease_envelopes(
    descriptors: &[novarocks::CredentialLeaseDescriptor],
    envelopes: &[novarocks::CredentialLeaseSecretEnvelope],
    root: FieldPath,
) -> Result<(), ProtocolError> {
    if envelopes.len() > MAX_CREDENTIAL_LEASES_PER_QUERY {
        return Err(resource_exhausted(
            root.field("credential_lease_envelopes"),
            "credential lease envelope contribution exceeds 64 entries",
        ));
    }
    if descriptors.len() != envelopes.len() {
        return Err(invalid(
            root.field("credential_lease_envelopes"),
            "credential lease descriptors and confidential envelopes must have identical cardinality",
        ));
    }
    let mut total_encoded_bytes = 0usize;
    let mut previous = None;
    for (index, raw) in envelopes.iter().cloned().enumerate() {
        total_encoded_bytes = total_encoded_bytes.saturating_add(raw.encoded_len());
        if total_encoded_bytes > MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES {
            return Err(resource_exhausted(
                root.clone().field("credential_lease_envelopes"),
                "credential lease secret envelopes exceed 256 KiB",
            ));
        }
        let path = root
            .clone()
            .field("credential_lease_envelopes")
            .index(index);
        let envelope = CredentialLeaseSecretEnvelope::parse(raw, path.clone())?;
        if previous.is_some_and(|previous: CredentialLeaseId| previous >= envelope.lease_id()) {
            return Err(invalid(
                path.field("lease_id"),
                "credential lease envelopes must be strictly sorted and unique by lease id",
            ));
        }
        previous = Some(envelope.lease_id());
        let descriptor = descriptors
            .get(index)
            .cloned()
            .ok_or_else(|| invalid(path.clone(), "credential lease descriptor is missing"))?;
        let descriptor = decode_credential_lease_descriptor(
            descriptor,
            root.clone()
                .field("credential_lease_descriptors")
                .index(index),
        )?;
        if !envelope.matches_descriptor(&descriptor) {
            return Err(invalid(
                path,
                "credential lease envelope does not exactly match its descriptor",
            ));
        }
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

fn validate_secret_scalar(value: &str, root: FieldPath) -> Result<(), ProtocolError> {
    if value.is_empty() || value.len() > MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES {
        return Err(invalid(
            root,
            "credential lease secret scalar must contain 1..=8192 UTF-8 bytes",
        ));
    }
    Ok(())
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

fn prefix_path(prefix: FieldPath, error: ProtocolError) -> ProtocolError {
    ProtocolError::new(
        prefix.append_segments(error.path().segments().iter().skip(1).cloned()),
        error.kind(),
        error.detail(),
    )
}

#[cfg(test)]
mod tests {
    use super::{
        CredentialLeaseSecretEnvelope, decode_credential_lease_descriptor,
        encode_credential_lease_descriptor, validate_initial_credential_lease_envelopes,
    };
    use crate::FieldPath;
    use novarocks_proto_models::novarocks;
    use novarocks_secret::SecretValue;
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseDescriptor,
        CredentialLeaseId, CredentialLeaseProvider, StorageAccessDomainId,
        StorageCredentialScopePrefix,
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
            StorageAccessDomainId::from_bytes([8; 32]),
        )
        .expect("descriptor")
    }

    fn envelope(value: &str) -> CredentialLeaseSecretEnvelope {
        CredentialLeaseSecretEnvelope::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            3,
            SecretValue::new("access-canary"),
            SecretValue::new(value),
            SecretValue::new("token-canary"),
            99,
        )
        .expect("envelope")
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
    fn envelope_debug_redacts_and_exact_init_pairing_rejects_different_value() {
        let first = envelope("secret-canary-a");
        let second = envelope("secret-canary-b");
        let rendered = format!("{first:?}");
        assert!(!rendered.contains("secret-canary-a"));
        assert!(rendered.contains("[REDACTED]"));
        assert_ne!(first, second);
        let encoded_descriptor = encode_credential_lease_descriptor(&descriptor());
        validate_initial_credential_lease_envelopes(
            &[encoded_descriptor],
            &[first.to_proto()],
            FieldPath::root("init_query_request"),
        )
        .expect("exact pairing");
        let mut mismatched = second.to_proto();
        mismatched.epoch = 4;
        assert!(
            validate_initial_credential_lease_envelopes(
                &[encode_credential_lease_descriptor(&descriptor())],
                &[mismatched],
                FieldPath::root("init_query_request"),
            )
            .is_err()
        );
    }

    #[test]
    fn envelope_rejects_missing_or_oversized_s3_scalars() {
        let error = CredentialLeaseSecretEnvelope::parse(
            novarocks::CredentialLeaseSecretEnvelope {
                lease_id: vec![1; 16],
                epoch: 3,
                s3: Some(novarocks::CredentialLeaseS3SecretMaterial {
                    access_key_id: String::new(),
                    secret_access_key: "secret".to_owned(),
                    session_token: "token".to_owned(),
                    session_token_expires_at_unix_ms: 1,
                }),
            },
            FieldPath::root("credential_lease_secret_envelope"),
        )
        .expect_err("empty scalar rejects");
        assert!(error.detail().contains("secret scalar"));
    }
}
