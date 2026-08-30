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

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use novarocks_secret::SecretValue;

use super::{
    CatalogHandle, ConnectorError, ConnectorErrorKind, CredentialLeaseId,
    MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES, MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
    MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES, StorageAccessDomainId, StorageCredentialScopePrefix,
};

pub trait ConnectorCancellation: Send + Sync {
    fn is_cancelled(&self) -> bool;
}

/// A non-secret target presented to the query-scoped storage capability.
///
/// The owner is the exact catalog generation already carried by scan/writer
/// handles. It deliberately contains neither a credential reference, an
/// access domain, nor any query identity: the query attempt and its lease
/// domain are selected only inside the process-local resolver.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageAccessRequest {
    owner: CatalogHandle,
    location: Arc<str>,
}

impl StorageAccessRequest {
    pub fn try_new(
        owner: CatalogHandle,
        location: impl AsRef<str>,
    ) -> Result<Self, ConnectorError> {
        let location = location.as_ref();
        if location.is_empty()
            || location.len() > MAX_STORAGE_CREDENTIAL_SCOPE_PREFIX_BYTES
            || !location.is_ascii()
            || location.bytes().any(|byte| byte.is_ascii_whitespace())
            || location
                .bytes()
                .any(|byte| matches!(byte, b'?' | b'#' | b'\\'))
        {
            return Err(invalid_storage_route());
        }
        let parsed = url::Url::parse(location).map_err(|_| invalid_storage_route())?;
        if parsed.scheme() != "s3"
            || parsed.host_str().is_none_or(str::is_empty)
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.port().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || parsed.as_str() != location
        {
            return Err(invalid_storage_route());
        }
        Ok(Self {
            owner,
            location: Arc::from(location),
        })
    }

    pub fn owner(&self) -> &CatalogHandle {
        &self.owner
    }

    pub fn location(&self) -> &str {
        &self.location
    }
}

/// Process-local, query-attempt-bound storage access authority.
///
/// The native wire never contains this trait object. A request context receives
/// it only after the BE has admitted the exact query attempt and captured its
/// lifecycle lease state.
pub trait ConnectorStorageResolver: Send + Sync {
    fn resolve_vended_s3(
        &self,
        request: &StorageAccessRequest,
    ) -> Result<ResolvedVendedS3Access, ConnectorError>;
}

/// One successful vended S3 selection. The material is redacted from Debug
/// and never derives a serialization trait.
#[derive(Clone)]
pub struct ResolvedVendedS3Access {
    storage_access_domain_id: StorageAccessDomainId,
    lease_id: CredentialLeaseId,
    epoch: u64,
    matched_prefix: StorageCredentialScopePrefix,
    not_after_unix_ms: u64,
    access_key_id: SecretValue,
    secret_access_key: SecretValue,
    session_token: SecretValue,
}

impl ResolvedVendedS3Access {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        storage_access_domain_id: StorageAccessDomainId,
        lease_id: CredentialLeaseId,
        epoch: u64,
        matched_prefix: StorageCredentialScopePrefix,
        not_after_unix_ms: u64,
        access_key_id: SecretValue,
        secret_access_key: SecretValue,
        session_token: SecretValue,
    ) -> Self {
        Self {
            storage_access_domain_id,
            lease_id,
            epoch,
            matched_prefix,
            not_after_unix_ms,
            access_key_id,
            secret_access_key,
            session_token,
        }
    }

    pub const fn storage_access_domain_id(&self) -> StorageAccessDomainId {
        self.storage_access_domain_id
    }

    pub const fn lease_id(&self) -> CredentialLeaseId {
        self.lease_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn matched_prefix(&self) -> &StorageCredentialScopePrefix {
        &self.matched_prefix
    }

    pub const fn not_after_unix_ms(&self) -> u64 {
        self.not_after_unix_ms
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
}

impl fmt::Debug for ResolvedVendedS3Access {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedVendedS3Access")
            .field("storage_access_domain_id", &self.storage_access_domain_id)
            .field("lease_id", &self.lease_id)
            .field("epoch", &self.epoch)
            .field("matched_prefix", &self.matched_prefix)
            .field("not_after_unix_ms", &self.not_after_unix_ms)
            .field("material", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone)]
pub struct ConnectorRequestContext {
    deadline: Instant,
    cancellation: Arc<dyn ConnectorCancellation>,
    max_handle_payload_bytes: usize,
    max_total_payload_bytes: usize,
    storage_resolver: Option<Arc<dyn ConnectorStorageResolver>>,
}

impl ConnectorRequestContext {
    pub fn try_new(
        deadline: Instant,
        cancellation: Arc<dyn ConnectorCancellation>,
        max_handle_payload_bytes: usize,
        max_total_payload_bytes: usize,
    ) -> Result<Self, ConnectorError> {
        if max_handle_payload_bytes == 0
            || max_total_payload_bytes == 0
            || max_handle_payload_bytes > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES
            || max_total_payload_bytes > MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES
            || max_total_payload_bytes < max_handle_payload_bytes
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "invalid connector payload budget",
            ));
        }
        Ok(Self {
            deadline,
            cancellation,
            max_handle_payload_bytes,
            max_total_payload_bytes,
            storage_resolver: None,
        })
    }

    /// Installs a local capability after query admission. It is intentionally
    /// a builder step rather than a wire constructor argument.
    pub fn with_storage_resolver(
        mut self,
        storage_resolver: Arc<dyn ConnectorStorageResolver>,
    ) -> Self {
        self.storage_resolver = Some(storage_resolver);
        self
    }

    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn cancellation(&self) -> &Arc<dyn ConnectorCancellation> {
        &self.cancellation
    }

    pub const fn max_handle_payload_bytes(&self) -> usize {
        self.max_handle_payload_bytes
    }

    pub const fn max_total_payload_bytes(&self) -> usize {
        self.max_total_payload_bytes
    }

    pub fn storage_resolver(&self) -> Option<&Arc<dyn ConnectorStorageResolver>> {
        self.storage_resolver.as_ref()
    }
}

fn invalid_storage_route() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "invalid vended storage access route",
    )
}

#[cfg(test)]
mod tests {
    use super::{ResolvedVendedS3Access, StorageAccessRequest};
    use crate::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, CredentialLeaseId,
        StorageAccessDomainId, StorageCredentialScopePrefix,
    };
    use novarocks_secret::SecretValue;

    #[test]
    fn storage_request_rejects_noncanonical_or_credentialed_location() {
        let owner = CatalogHandle::new(
            ConnectorInstanceId::parse("request-test").expect("catalog"),
            CatalogVersion::from_bytes([1; 32]),
        );
        assert!(
            StorageAccessRequest::try_new(owner.clone(), "s3://bucket/table/data.parquet").is_ok()
        );
        for location in [
            "https://bucket/table/data.parquet",
            "s3://user:secret@bucket/table/data.parquet",
            "s3://bucket/table/data.parquet?token=secret",
            "s3://bucket/table/../other",
        ] {
            assert!(
                StorageAccessRequest::try_new(owner.clone(), location).is_err(),
                "{location}"
            );
        }
    }

    #[test]
    fn resolved_access_debug_redacts_secret_material() {
        let access = ResolvedVendedS3Access::new(
            StorageAccessDomainId::from_bytes([1; 32]),
            CredentialLeaseId::try_from_bytes([2; 16]).expect("lease"),
            1,
            StorageCredentialScopePrefix::try_from_normalized("s3://bucket/table").expect("prefix"),
            42,
            SecretValue::new("access-canary"),
            SecretValue::new("secret-canary"),
            SecretValue::new("token-canary"),
        );
        let rendered = format!("{access:?}");
        assert!(!rendered.contains("access-canary"));
        assert!(!rendered.contains("secret-canary"));
        assert!(!rendered.contains("token-canary"));
    }
}
