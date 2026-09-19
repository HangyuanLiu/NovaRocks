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

//! Secret-free query-attempt credential lease metadata.
//!
//! A descriptor is immutable attempt metadata and may be included in the
//! participant-manifest digest. Credential values intentionally do not appear
//! here: the native lifecycle confidential carrier owns their short-lived
//! transport and execution-side installation.

use std::num::NonZeroU8;
use std::sync::Arc;
use std::time::Duration;

use novarocks_secret::SecretValue;

use super::{
    CatalogHandle, CatalogProperties, ConnectorError, ConnectorErrorKind, StorageAccessDomainId,
    StorageCredentialScopePrefix,
};

pub const MAX_CREDENTIAL_LEASES_PER_QUERY: usize = 64;
pub const MAX_CREDENTIAL_LEASE_PREFIXES: usize = 64;
/// Bounded like every other wire string here: an address a server advertises
/// is still input, and an unbounded one is a way to make a descriptor large.
pub const MAX_CREDENTIALS_ENDPOINT_BYTES: usize = 2 * 1024;
/// Namespace depth a load-table acquisition path may name.
pub const MAX_CREDENTIAL_NAMESPACE_LEVELS: usize = 16;
/// Byte bound on one namespace level, table name or table uuid.
pub const MAX_CREDENTIAL_IDENTIFIER_BYTES: usize = 512;
pub const MAX_CREDENTIAL_LEASE_ID_BYTES: usize = 16;
pub const MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES: usize = 8 * 1024;
pub const MAX_CREDENTIAL_LEASE_SECRET_ENVELOPE_BYTES: usize = 256 * 1024;

/// Stable, non-secret identity for one query-attempt credential lease.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CredentialLeaseId([u8; MAX_CREDENTIAL_LEASE_ID_BYTES]);

impl CredentialLeaseId {
    pub fn try_from_bytes(
        bytes: [u8; MAX_CREDENTIAL_LEASE_ID_BYTES],
    ) -> Result<Self, ConnectorError> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(invalid("credential lease id"));
        }
        Ok(Self(bytes))
    }

    pub const fn as_bytes(&self) -> &[u8; MAX_CREDENTIAL_LEASE_ID_BYTES] {
        &self.0
    }
}

/// Closed provider family accepted by the M2 v1 confidential lease carrier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CredentialLeaseProvider {
    S3,
}

/// One move-only S3 credential entry contributed by a provider while it still
/// owns a vended metadata response.
///
/// The entry deliberately has no `Clone`, `Debug`, or serialization trait.
/// Secret values move only into the query-attempt collector that owns
/// confidential lifecycle installation.
pub struct VendedS3CredentialLeaseEntry {
    prefix: StorageCredentialScopePrefix,
    not_after_unix_ms: u64,
    access_key_id: SecretValue,
    secret_access_key: SecretValue,
    session_token: SecretValue,
}

impl VendedS3CredentialLeaseEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        prefix: StorageCredentialScopePrefix,
        not_after_unix_ms: u64,
        access_key_id: SecretValue,
        secret_access_key: SecretValue,
        session_token: SecretValue,
    ) -> Result<Self, ConnectorError> {
        if not_after_unix_ms == 0 {
            return Err(invalid("vended S3 credential expiration"));
        }
        Ok(Self {
            prefix,
            not_after_unix_ms,
            access_key_id,
            secret_access_key,
            session_token,
        })
    }

    pub fn prefix(&self) -> &StorageCredentialScopePrefix {
        &self.prefix
    }

    pub const fn not_after_unix_ms(&self) -> u64 {
        self.not_after_unix_ms
    }

    /// Transfer the secret scalars to the sole query-attempt lease owner.
    /// Callers must not retain this entry after invoking this method.
    pub fn into_parts(
        self,
    ) -> (
        StorageCredentialScopePrefix,
        u64,
        SecretValue,
        SecretValue,
        SecretValue,
    ) {
        (
            self.prefix,
            self.not_after_unix_ms,
            self.access_key_id,
            self.secret_access_key,
            self.session_token,
        )
    }
}

/// Provider-neutral, response-local contribution for one vended S3 lease
/// scope.
///
/// This value is deliberately move-only. It may travel only from a provider's
/// metadata-response adapter to an in-process query-attempt collector; it is
/// never a table attribute, cache value, SQL plan field, or native wire value.
pub struct VendedS3CredentialLeaseContribution {
    entries: Vec<VendedS3CredentialLeaseEntry>,
    /// What the provider observed about how this scope can be re-acquired.
    ///
    /// It is announced to consumers rather than kept private because a
    /// consumer that must acquire for itself cannot derive it: the path is the
    /// catalog's statement, not a canonical route a client may assume
    /// (CAD-1 D2).
    renewal_path: Option<CredentialRenewalPath>,
    refresher: Option<Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>>,
}

impl VendedS3CredentialLeaseContribution {
    pub fn try_new(
        mut entries: Vec<VendedS3CredentialLeaseEntry>,
        renewal_path: Option<CredentialRenewalPath>,
    ) -> Result<Self, ConnectorError> {
        if entries.is_empty() || entries.len() > MAX_CREDENTIAL_LEASE_PREFIXES {
            return Err(exhausted("vended S3 credential entry set"));
        }
        entries.sort_by(|left, right| left.prefix.cmp(&right.prefix));
        if entries
            .windows(2)
            .any(|pair| pair[0].prefix == pair[1].prefix)
        {
            return Err(invalid("duplicate vended S3 credential prefix"));
        }
        if let Some(CredentialRenewalPath::CredentialsEndpoint(endpoint)) = &renewal_path
            && endpoint.is_empty()
        {
            return Err(invalid("vended S3 credential refresh endpoint"));
        }
        Ok(Self {
            entries,
            renewal_path,
            refresher: None,
        })
    }

    /// Attach the provider-owned, FE-local source for a later refresh. The
    /// source is a capability, not a wire field or a table property; it can be
    /// consumed only by the query-attempt collector that receives this
    /// response-local contribution.
    pub fn with_refresher(
        mut self,
        refresher: Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>,
    ) -> Result<Self, ConnectorError> {
        self.refresher = Some(refresher);
        Ok(self)
    }

    pub fn entries(&self) -> &[VendedS3CredentialLeaseEntry] {
        &self.entries
    }

    pub fn renewal_path(&self) -> Option<&CredentialRenewalPath> {
        self.renewal_path.as_ref()
    }

    pub fn refresh_endpoint(&self) -> Option<&str> {
        match &self.renewal_path {
            Some(CredentialRenewalPath::CredentialsEndpoint(endpoint)) => Some(endpoint),
            _ => None,
        }
    }

    /// Replace the announced acquisition path with the one the provider
    /// resolved for this exact response.
    ///
    /// A provider that also builds a refresher knows more than the response
    /// alone says — which table a load-table acquisition names, for instance —
    /// and the announcement must carry the same fact the refresher acts on.
    pub fn with_renewal_path(mut self, renewal_path: CredentialRenewalPath) -> Self {
        self.renewal_path = Some(renewal_path);
        self
    }

    pub fn refresher(&self) -> Option<&Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>> {
        self.refresher.as_ref()
    }

    /// Transfer the complete response-local contribution to the sole
    /// query-attempt collector.
    pub fn into_parts(
        self,
    ) -> (
        Vec<VendedS3CredentialLeaseEntry>,
        Option<CredentialRenewalPath>,
    ) {
        (self.entries, self.renewal_path)
    }

    /// Transfer both the confidential entries and the provider refresh source
    /// to the only query-attempt collector. Existing callers that do not own a
    /// refresher should use [`Self::into_parts`].
    pub fn into_parts_with_refresher(
        self,
    ) -> (
        Vec<VendedS3CredentialLeaseEntry>,
        Option<CredentialRenewalPath>,
        Option<Arc<dyn ConnectorVendedS3CredentialLeaseRefresher>>,
    ) {
        (self.entries, self.renewal_path, self.refresher)
    }
}

/// Provider-neutral, move-only values returned by one FE-owned vended S3
/// refresh. The provider retains all HTTP/catalog identity; the lifecycle
/// owner receives only another closed set of response-local credential values.
pub struct VendedS3CredentialLeaseRefresh {
    entries: Vec<VendedS3CredentialLeaseEntry>,
}

impl VendedS3CredentialLeaseRefresh {
    pub fn try_new(mut entries: Vec<VendedS3CredentialLeaseEntry>) -> Result<Self, ConnectorError> {
        if entries.is_empty() || entries.len() > MAX_CREDENTIAL_LEASE_PREFIXES {
            return Err(exhausted("refreshed vended S3 credential entry set"));
        }
        entries.sort_by(|left, right| left.prefix.cmp(&right.prefix));
        if entries
            .windows(2)
            .any(|pair| pair[0].prefix == pair[1].prefix)
        {
            return Err(invalid("duplicate refreshed vended S3 credential prefix"));
        }
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[VendedS3CredentialLeaseEntry] {
        &self.entries
    }

    /// Transfer all refreshed values to the FE lifecycle owner. It is
    /// responsible for selecting the existing lease's exact scope and forming
    /// the next confidential epoch.
    pub fn into_entries(self) -> Vec<VendedS3CredentialLeaseEntry> {
        self.entries
    }
}

/// Secret-free observation used immediately before a provider refresh dispatch.
///
/// This is intentionally a process-local capability rather than a serialized
/// cancellation token. The provider only learns whether another external
/// request remains permitted; it cannot obtain an attempt identity, a
/// credential, or a runtime registry from this port.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VendedS3CredentialRefreshDispatch {
    Permitted,
    Fenced,
}

/// Attempt-local authority over provider credential-refresh dispatches.
///
/// Providers must check this before every external dispatch, including a
/// retry after a retryable response. A `Fenced` result means that no new
/// request may be started; it never claims that an already-issued request was
/// forcibly cancelled.
pub trait VendedS3CredentialRefreshDispatchGuard: Send + Sync {
    fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch;
}

/// Secret-free, FE-local bound for one provider credential-refresh call.
///
/// The provider starts one monotonic deadline when it receives this policy.
/// Every attempt, its transport setup, response read, and retry wait must fit
/// inside that one budget.  This is deliberately a synchronous call policy:
/// it does not claim that a caller can forcibly abort an already-issued I/O
/// operation.
#[derive(Clone)]
pub struct VendedS3CredentialRefreshCallPolicy {
    remaining: Duration,
    max_attempts: NonZeroU8,
    retry_backoff: Duration,
    dispatch_guard: Arc<dyn VendedS3CredentialRefreshDispatchGuard>,
}

impl std::fmt::Debug for VendedS3CredentialRefreshCallPolicy {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VendedS3CredentialRefreshCallPolicy")
            .field("remaining", &self.remaining)
            .field("max_attempts", &self.max_attempts)
            .field("retry_backoff", &self.retry_backoff)
            .field("dispatch_guard", &"<process-local>")
            .finish()
    }
}

impl VendedS3CredentialRefreshCallPolicy {
    pub fn try_new(
        remaining: Duration,
        max_attempts: NonZeroU8,
        retry_backoff: Duration,
        dispatch_guard: Arc<dyn VendedS3CredentialRefreshDispatchGuard>,
    ) -> Result<Self, ConnectorError> {
        if remaining.is_zero() {
            return Err(invalid("vended S3 credential refresh remaining budget"));
        }
        if max_attempts.get() > 1 && retry_backoff.is_zero() {
            return Err(invalid("vended S3 credential refresh retry backoff"));
        }
        Ok(Self {
            remaining,
            max_attempts,
            retry_backoff,
            dispatch_guard,
        })
    }

    pub const fn remaining(&self) -> Duration {
        self.remaining
    }

    pub const fn max_attempts(&self) -> NonZeroU8 {
        self.max_attempts
    }

    pub const fn retry_backoff(&self) -> Duration {
        self.retry_backoff
    }

    /// Reads the same attempt-local dispatch authority used by the caller.
    /// The result must be checked at each actual provider request boundary.
    pub fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch {
        self.dispatch_guard.provider_dispatch()
    }
}

/// FE-local provider capability for refreshing an already-admitted vended S3
/// lease. Implementations must use their own catalog identity and may never
/// be serialized or attached to a BE request.
pub trait ConnectorVendedS3CredentialLeaseRefresher: Send + Sync {
    fn refresh_vended_s3_credentials(
        &self,
        policy: VendedS3CredentialRefreshCallPolicy,
    ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError>;
}

/// FE-local receiver for provider vended-credential contributions.
///
/// The trait object is carried only by the request context. It is not
/// serializable and must never be captured by table, plan, fragment, or cache
/// state.
pub trait ConnectorVendedCredentialLeaseSink: Send + Sync {
    fn offer_vended_s3_credential_lease(
        &self,
        catalog_properties: &CatalogProperties,
        contribution: VendedS3CredentialLeaseContribution,
    ) -> Result<(), ConnectorError>;
}

/// A request-local, query-wide vended-credential sink attachment.
///
/// A query can touch multiple catalog generations, so each metadata call
/// clones the query-wide context and decorates its own collection port with
/// the exact catalog properties before invoking the provider.
#[derive(Clone)]
pub struct ConnectorVendedCredentialLeaseCollectionPort {
    catalog_properties: CatalogProperties,
    sink: Arc<dyn ConnectorVendedCredentialLeaseSink>,
}

/// Confidential, in-process material for one query-attempt credential lease.
///
/// This value deliberately belongs to SPI rather than a wire codec: it owns
/// secret wrappers and exposes values only at the concrete protocol or storage
/// consumer boundary. It is never manifest/digest material.
#[derive(Clone, Eq, PartialEq)]
pub struct CredentialLeaseSecretEnvelope {
    lease_id: CredentialLeaseId,
    epoch: u64,
    access_key_id: SecretValue,
    secret_access_key: SecretValue,
    session_token: SecretValue,
    session_token_expires_at_unix_ms: u64,
}

impl std::fmt::Debug for CredentialLeaseSecretEnvelope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
    ) -> Result<Self, ConnectorError> {
        validate_secret_scalar(access_key_id.expose_secret())?;
        validate_secret_scalar(secret_access_key.expose_secret())?;
        validate_secret_scalar(session_token.expose_secret())?;
        if epoch == 0 || session_token_expires_at_unix_ms == 0 {
            return Err(invalid("credential lease epoch or expiration"));
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

    /// Wraps plain scalars a coordinator-local provider response produced.
    ///
    /// No longer a wire boundary: nothing decodes material from the native
    /// transport any more. It remains because the provider hands its response
    /// over as strings (CAD-1 C07b).
    pub fn try_new_from_scalars(
        lease_id: CredentialLeaseId,
        epoch: u64,
        access_key_id: String,
        secret_access_key: String,
        session_token: String,
        session_token_expires_at_unix_ms: u64,
    ) -> Result<Self, ConnectorError> {
        Self::try_new(
            lease_id,
            epoch,
            SecretValue::new(access_key_id),
            SecretValue::new(secret_access_key),
            SecretValue::new(session_token),
            session_token_expires_at_unix_ms,
        )
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

impl ConnectorVendedCredentialLeaseCollectionPort {
    pub fn new(
        catalog_properties: CatalogProperties,
        sink: Arc<dyn ConnectorVendedCredentialLeaseSink>,
    ) -> Self {
        Self {
            catalog_properties,
            sink,
        }
    }

    pub const fn catalog_properties(&self) -> &CatalogProperties {
        &self.catalog_properties
    }

    /// Immediately transfer one provider response into the attached
    /// query-attempt collector. The exact catalog-generation properties are
    /// attached by query materialization before this provider call starts.
    pub fn offer_vended_s3_credential_lease(
        &self,
        contribution: VendedS3CredentialLeaseContribution,
    ) -> Result<(), ConnectorError> {
        self.sink
            .offer_vended_s3_credential_lease(&self.catalog_properties, contribution)
    }
}

/// Immutable, secret-free lease metadata frozen for one query participant.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialLeaseDescriptor {
    lease_id: CredentialLeaseId,
    epoch: u64,
    owner: CatalogHandle,
    provider: CredentialLeaseProvider,
    prefixes: Vec<StorageCredentialScopePrefix>,
    not_after_unix_ms: u64,
    refresh_capable: bool,
    /// How a consumer of this lease acquires for itself, when the server
    /// advertised a path at all.
    ///
    /// Non-secret on purpose: an address or a table identity, never material,
    /// and it belongs on the descriptor rather than beside the secret because
    /// that is what lets a consumer acquire for itself. The client never
    /// constructs this path — the spec has the server advertise it, and a
    /// client that guessed the canonical route would be asserting a capability
    /// the deployment may not have (CAD-1 D2).
    renewal_path: Option<CredentialRenewalPath>,
    storage_access_domain_id: StorageAccessDomainId,
}

/// The closed set of acquisition paths a catalog can advertise.
///
/// Two variants because the REST specification has two: a catalog may serve a
/// scope's credentials from its own address, or it may vend them only inside a
/// load-table response. A consumer selects one and never probes the other
/// after a failure (CAD-1 D2, D11).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialRenewalPath {
    CredentialsEndpoint(Arc<str>),
    LoadTableDelegation(CredentialLoadTableDelegation),
}

/// The exact table a load-table acquisition names.
///
/// The uuid travels with the identity rather than being re-derived: a later
/// response that answered for a different table would otherwise install
/// material for the wrong authority (CAD-1 D11b).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CredentialLoadTableDelegation {
    namespace: Vec<Arc<str>>,
    table: Arc<str>,
    table_uuid: Arc<str>,
}

impl CredentialLoadTableDelegation {
    pub fn try_new(
        namespace: Vec<Arc<str>>,
        table: Arc<str>,
        table_uuid: Arc<str>,
    ) -> Result<Self, ConnectorError> {
        if namespace.is_empty() || namespace.len() > MAX_CREDENTIAL_NAMESPACE_LEVELS {
            return Err(exhausted("credential lease load-table namespace"));
        }
        if namespace
            .iter()
            .any(|level| level.is_empty() || level.len() > MAX_CREDENTIAL_IDENTIFIER_BYTES)
        {
            return Err(invalid("credential lease load-table namespace level"));
        }
        if table.is_empty() || table.len() > MAX_CREDENTIAL_IDENTIFIER_BYTES {
            return Err(invalid("credential lease load-table name"));
        }
        if table_uuid.is_empty() || table_uuid.len() > MAX_CREDENTIAL_IDENTIFIER_BYTES {
            return Err(invalid("credential lease load-table uuid"));
        }
        Ok(Self {
            namespace,
            table,
            table_uuid,
        })
    }

    pub fn namespace(&self) -> &[Arc<str>] {
        &self.namespace
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn table_uuid(&self) -> &str {
        &self.table_uuid
    }
}

impl CredentialLeaseDescriptor {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        lease_id: CredentialLeaseId,
        epoch: u64,
        owner: CatalogHandle,
        provider: CredentialLeaseProvider,
        mut prefixes: Vec<StorageCredentialScopePrefix>,
        not_after_unix_ms: u64,
        refresh_capable: bool,
        renewal_path: Option<CredentialRenewalPath>,
        storage_access_domain_id: StorageAccessDomainId,
    ) -> Result<Self, ConnectorError> {
        if epoch == 0 {
            return Err(invalid("credential lease epoch"));
        }
        if prefixes.is_empty() || prefixes.len() > MAX_CREDENTIAL_LEASE_PREFIXES {
            return Err(exhausted("credential lease prefix set"));
        }
        prefixes.sort();
        if prefixes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(invalid("duplicate credential lease prefix"));
        }
        if not_after_unix_ms == 0 {
            return Err(invalid("credential lease expiration"));
        }
        if let Some(CredentialRenewalPath::CredentialsEndpoint(endpoint)) = &renewal_path
            && (endpoint.is_empty() || endpoint.len() > MAX_CREDENTIALS_ENDPOINT_BYTES)
        {
            return Err(invalid("credential lease credentials endpoint"));
        }
        Ok(Self {
            lease_id,
            epoch,
            owner,
            provider,
            prefixes,
            not_after_unix_ms,
            refresh_capable,
            renewal_path,
            storage_access_domain_id,
        })
    }

    /// The advertised acquisition path for this scope, when there is one.
    ///
    /// Its absence is meaningful rather than incidental: a catalog that
    /// advertises no path offers no renewal, and a consumer holding such a
    /// lease is the seeded-without-renewal shape (CAD-1 D11).
    pub fn renewal_path(&self) -> Option<&CredentialRenewalPath> {
        self.renewal_path.as_ref()
    }

    /// The advertised acquisition address, when that is the path this lease
    /// carries.
    pub fn credentials_endpoint(&self) -> Option<&str> {
        match &self.renewal_path {
            Some(CredentialRenewalPath::CredentialsEndpoint(endpoint)) => Some(endpoint),
            _ => None,
        }
    }

    pub const fn lease_id(&self) -> CredentialLeaseId {
        self.lease_id
    }

    pub const fn epoch(&self) -> u64 {
        self.epoch
    }

    pub const fn owner(&self) -> &CatalogHandle {
        &self.owner
    }

    pub const fn provider(&self) -> CredentialLeaseProvider {
        self.provider
    }

    pub fn prefixes(&self) -> &[StorageCredentialScopePrefix] {
        &self.prefixes
    }

    pub const fn not_after_unix_ms(&self) -> u64 {
        self.not_after_unix_ms
    }

    pub const fn refresh_capable(&self) -> bool {
        self.refresh_capable
    }

    pub const fn storage_access_domain_id(&self) -> StorageAccessDomainId {
        self.storage_access_domain_id
    }

    /// Refresh may advance only epoch and expiration. Any owner, provider,
    /// scope, or access-domain change is a new attempt, never a refresh.
    pub fn has_same_refresh_scope(&self, other: &Self) -> bool {
        self.lease_id == other.lease_id
            && self.owner == other.owner
            && self.provider == other.provider
            && self.prefixes == other.prefixes
            && self.refresh_capable == other.refresh_capable
            && self.storage_access_domain_id == other.storage_access_domain_id
    }
}

fn invalid(subject: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        format!("invalid {subject}"),
    )
}

fn exhausted(subject: &'static str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        format!("{subject} exceeds configured bounds"),
    )
}

fn validate_secret_scalar(value: &str) -> Result<(), ConnectorError> {
    if value.is_empty() || value.len() > MAX_CREDENTIAL_LEASE_SECRET_SCALAR_BYTES {
        return Err(invalid("credential lease secret scalar"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU8;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use super::{
        ConnectorVendedCredentialLeaseCollectionPort, ConnectorVendedCredentialLeaseSink,
        ConnectorVendedS3CredentialLeaseRefresher, CredentialLeaseDescriptor, CredentialLeaseId,
        CredentialLeaseProvider, CredentialRenewalPath, MAX_CREDENTIAL_LEASE_PREFIXES,
        VendedS3CredentialLeaseContribution, VendedS3CredentialLeaseEntry,
        VendedS3CredentialLeaseRefresh, VendedS3CredentialRefreshCallPolicy,
        VendedS3CredentialRefreshDispatch, VendedS3CredentialRefreshDispatchGuard,
    };
    use crate::connector::{
        CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
        CatalogProperties, CatalogVersion, ConnectorError, ConnectorInstanceId,
        ConnectorProviderId, CredentialConsumerRole, StorageAccessDomainId,
        StorageCredentialScopePrefix,
    };
    use novarocks_secret::SecretValue;

    fn owner() -> CatalogHandle {
        CatalogHandle::new(
            ConnectorInstanceId::parse("warehouse").expect("catalog"),
            CatalogVersion::from_bytes([7; 32]),
        )
    }

    fn prefix(value: &str) -> StorageCredentialScopePrefix {
        StorageCredentialScopePrefix::try_from_normalized(value).expect("prefix")
    }

    struct ProviderLocalRefresher;

    struct PermittedDispatch;

    impl VendedS3CredentialRefreshDispatchGuard for PermittedDispatch {
        fn provider_dispatch(&self) -> VendedS3CredentialRefreshDispatch {
            VendedS3CredentialRefreshDispatch::Permitted
        }
    }

    impl ConnectorVendedS3CredentialLeaseRefresher for ProviderLocalRefresher {
        fn refresh_vended_s3_credentials(
            &self,
            _policy: VendedS3CredentialRefreshCallPolicy,
        ) -> Result<VendedS3CredentialLeaseRefresh, ConnectorError> {
            panic!("the capability is not invoked by this construction test")
        }
    }

    fn descriptor(
        epoch: u64,
        prefixes: Vec<StorageCredentialScopePrefix>,
    ) -> CredentialLeaseDescriptor {
        CredentialLeaseDescriptor::try_new(
            CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
            epoch,
            owner(),
            CredentialLeaseProvider::S3,
            prefixes,
            10,
            true,
            None,
            StorageAccessDomainId::from_bytes([9; 32]),
        )
        .expect("descriptor")
    }

    #[test]
    fn refresh_call_policy_requires_a_live_budget_and_nonbusy_retry() {
        assert!(
            VendedS3CredentialRefreshCallPolicy::try_new(
                Duration::ZERO,
                NonZeroU8::new(1).expect("nonzero attempts"),
                Duration::ZERO,
                Arc::new(PermittedDispatch),
            )
            .is_err()
        );
        assert!(
            VendedS3CredentialRefreshCallPolicy::try_new(
                Duration::from_millis(1),
                NonZeroU8::new(2).expect("nonzero attempts"),
                Duration::ZERO,
                Arc::new(PermittedDispatch),
            )
            .is_err()
        );

        let policy = VendedS3CredentialRefreshCallPolicy::try_new(
            Duration::from_millis(20),
            NonZeroU8::new(2).expect("nonzero attempts"),
            Duration::from_millis(1),
            Arc::new(PermittedDispatch),
        )
        .expect("valid bounded policy");
        assert_eq!(policy.max_attempts().get(), 2);
    }

    #[test]
    fn descriptor_canonicalizes_prefixes_and_refresh_scope_excludes_epoch_and_expiry() {
        let first = descriptor(1, vec![prefix("s3://bucket/z"), prefix("s3://bucket/a")]);
        assert_eq!(first.prefixes()[0].as_str(), "s3://bucket/a");
        let second = CredentialLeaseDescriptor::try_new(
            first.lease_id(),
            2,
            owner(),
            CredentialLeaseProvider::S3,
            first.prefixes().to_vec(),
            20,
            true,
            None,
            StorageAccessDomainId::from_bytes([9; 32]),
        )
        .expect("refresh descriptor");
        assert!(first.has_same_refresh_scope(&second));
    }

    #[test]
    fn a_provider_local_refresher_does_not_require_a_public_endpoint() {
        let contribution = VendedS3CredentialLeaseContribution::try_new(
            vec![
                VendedS3CredentialLeaseEntry::try_new(
                    prefix("s3://bucket/table"),
                    10,
                    SecretValue::new("access"),
                    SecretValue::new("secret"),
                    SecretValue::new("token"),
                )
                .expect("entry"),
            ],
            None,
        )
        .expect("contribution")
        .with_refresher(Arc::new(ProviderLocalRefresher))
        .expect("provider-local refresh needs no fabricated endpoint");

        assert!(contribution.refresh_endpoint().is_none());
        assert!(contribution.refresher().is_some());
    }

    #[test]
    fn descriptor_rejects_empty_duplicate_and_overbound_prefix_sets() {
        assert!(
            CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                1,
                owner(),
                CredentialLeaseProvider::S3,
                vec![],
                10,
                false,
                None,
                StorageAccessDomainId::from_bytes([9; 32]),
            )
            .is_err()
        );
        assert!(
            CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                1,
                owner(),
                CredentialLeaseProvider::S3,
                vec![prefix("s3://bucket/a"), prefix("s3://bucket/a")],
                10,
                false,
                None,
                StorageAccessDomainId::from_bytes([9; 32]),
            )
            .is_err()
        );
        let prefixes = (0..=MAX_CREDENTIAL_LEASE_PREFIXES)
            .map(|index| prefix(&format!("s3://bucket/{index}")))
            .collect();
        assert!(
            CredentialLeaseDescriptor::try_new(
                CredentialLeaseId::try_from_bytes([1; 16]).expect("lease"),
                1,
                owner(),
                CredentialLeaseProvider::S3,
                prefixes,
                10,
                false,
                None,
                StorageAccessDomainId::from_bytes([9; 32]),
            )
            .is_err()
        );
    }

    struct RecordingVendedSink {
        seen: Mutex<Vec<(CatalogHandle, usize, Option<String>)>>,
    }

    impl ConnectorVendedCredentialLeaseSink for RecordingVendedSink {
        fn offer_vended_s3_credential_lease(
            &self,
            catalog_properties: &CatalogProperties,
            contribution: VendedS3CredentialLeaseContribution,
        ) -> Result<(), ConnectorError> {
            let (entries, renewal_path) = contribution.into_parts();
            self.seen.lock().expect("record sink").push((
                catalog_properties.handle().clone(),
                entries.len(),
                match renewal_path {
                    Some(CredentialRenewalPath::CredentialsEndpoint(endpoint)) => {
                        Some(endpoint.to_string())
                    }
                    Some(CredentialRenewalPath::LoadTableDelegation(delegation)) => {
                        Some(format!("load-table:{}", delegation.table()))
                    }
                    None => None,
                },
            ));
            Ok(())
        }
    }

    fn vended_catalog_properties() -> CatalogProperties {
        CatalogProperties::new(
            owner(),
            ConnectorProviderId::parse("iceberg").expect("static provider ID"),
            1,
            vec![],
            vec![
                CatalogCredentialBinding::try_new(
                    CatalogCredentialPurpose::ObjectStoreData,
                    CredentialConsumerRole::Backend,
                    CatalogCredentialMode::Vended,
                )
                .expect("vended binding"),
            ],
        )
        .expect("catalog properties")
    }

    #[test]
    fn collection_port_forwards_exact_catalog_properties_and_move_only_contribution() {
        let sink = Arc::new(RecordingVendedSink {
            seen: Mutex::new(Vec::new()),
        });
        let port = ConnectorVendedCredentialLeaseCollectionPort::new(
            vended_catalog_properties(),
            sink.clone(),
        );
        let contribution = VendedS3CredentialLeaseContribution::try_new(
            vec![
                VendedS3CredentialLeaseEntry::try_new(
                    prefix("s3://bucket/table"),
                    100,
                    SecretValue::new("access-canary"),
                    SecretValue::new("secret-canary"),
                    SecretValue::new("token-canary"),
                )
                .expect("entry"),
            ],
            Some(CredentialRenewalPath::CredentialsEndpoint(Arc::from(
                "https://catalog.example.test/v1/credentials",
            ))),
        )
        .expect("contribution");

        port.offer_vended_s3_credential_lease(contribution)
            .expect("offer contribution");

        let seen = sink.seen.lock().expect("record sink");
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, owner());
        assert_eq!(seen[0].1, 1);
        assert_eq!(
            seen[0].2.as_deref(),
            Some("https://catalog.example.test/v1/credentials")
        );
    }
}
