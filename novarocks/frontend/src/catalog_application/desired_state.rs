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

//! The single typed contract for catalog desired state.
//!
//! A deployment selects exactly one [`CatalogDesiredStateSourceMode`] before
//! startup produces any side effect, and every mode answers the same question
//! in the same type: what is the *complete* set of catalogs this deployment
//! wants, right now.  That answer is a [`CatalogDesiredStateSnapshot`], and it
//! is the only input the projection materializes from.
//!
//! Three properties are worth stating because losing any of them silently
//! breaks a user-visible guarantee:
//!
//! 1. **A snapshot is total truth, not additive seeds.** A catalog absent from
//!    the source is not "unmentioned"; it is not wanted.  Bootstrapping must
//!    therefore never revive it.
//! 2. **A failed or partial enumeration is not a smaller snapshot.** It cannot
//!    be represented as one either: the only way to obtain a snapshot is
//!    [`CatalogDesiredStateSnapshot::try_new`], and a source that could not
//!    finish enumerating returns
//!    [`CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete`]
//!    instead.  Degrading it into "a valid snapshot that happens to hold zero
//!    catalogs" would retire every catalog in the deployment.
//! 3. **Two authorities never write one truth.** SQL mutation admission is a
//!    function of the selected mode alone, so a file- or controller-sourced
//!    deployment rejects `CREATE`/`DROP CATALOG` rather than applying it to a
//!    store nothing reads.
//!
//! Dynamic StateStore and StaticFile are implemented by distinct authorities.
//! ManagedController remains a frozen port position: selecting it fails with a
//! typed error before startup opens anything, and there is deliberately no arm
//! anywhere that lets it borrow another authority.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::{CatalogApplicationError, CatalogApplicationErrorKind};
use crate::catalog_attachment::{CatalogAttachment, CatalogAttachmentRepository};
use crate::state_family::StateFamily;

/// Domain separator for the snapshot identity digest.
const SNAPSHOT_IDENTITY_DOMAIN: &[u8] = b"novarocks/frontend/catalog/desired-state/snapshot/v1";

/// The config format version the dynamic StateStore mode stamps on every entry
/// it enumerates.
///
/// Taken from the state family manifest rather than re-declared here: the
/// durable attachment record version *is* the config format version that mode
/// produces, and the manifest is the single definition point for it.
const DYNAMIC_STATE_STORE_CONFIG_FORMAT_VERSION: u8 =
    match StateFamily::CatalogDesiredState.record_version() {
        Some(version) => version,
        // The manifest registers this family as a durable external projection,
        // so it always declares exactly one record version.
        None => panic!("the catalog desired-state family declares a record version"),
    };

/// The closed set of catalog desired-state source modes.
///
/// The modes are mutually exclusive by construction: a deployment names one,
/// and [`CatalogDesiredStateSource`] binds that one to an authority.  There is
/// no merge of two modes into a dual authority and no runtime switch.
// Design: ADR-0115 (docs/adr/ADR-0115-catalog-desired-state-source-modes.md)
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogDesiredStateSourceMode {
    /// StateStore attachment records are the desired-state authority, and SQL
    /// `CREATE`/`DROP CATALOG` write them.
    DynamicStateStore,
    /// Pre-deployment configuration files are the authority.
    StaticFile,
    /// An external controller delivers desired state.  Port position only; the
    /// delivery protocol is not designed yet.
    ManagedController,
}

impl CatalogDesiredStateSourceMode {
    /// Stable identifier for logs and operator-facing errors.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DynamicStateStore => "dynamic-state-store",
            Self::StaticFile => "static-file",
            Self::ManagedController => "managed-controller",
        }
    }

    /// Whether SQL may write catalog desired state under this mode.
    ///
    /// This is the whole of the admission rule: it depends on the selected mode
    /// and on nothing else — not on whether a StateStore happens to be
    /// configured, and not on whether a write would succeed.  A deployment
    /// whose desired state comes from a file or a controller must reject SQL
    /// mutation even when a store is present, because a store nobody reads is
    /// worse than a refusal.
    pub const fn sql_mutation_admission(self) -> CatalogSqlMutationAdmission {
        match self {
            Self::DynamicStateStore => CatalogSqlMutationAdmission::Accepted,
            Self::StaticFile | Self::ManagedController => CatalogSqlMutationAdmission::Rejected,
        }
    }

    /// Rejects a mode this binary implements no authority for.
    ///
    /// Startup calls this before it opens the attachment repository, starts the
    /// projection controller, or publishes any catalog runtime, so selecting an
    /// unimplemented mode fails with nothing to undo.  The `match` is
    /// exhaustive and has no fallback arm on purpose: adding a mode forces a
    /// decision here instead of letting it be served by the dynamic StateStore
    /// implementation.
    pub fn require_implemented(self) -> Result<(), CatalogApplicationError> {
        match self {
            Self::DynamicStateStore | Self::StaticFile => Ok(()),
            Self::ManagedController => Err(unsupported_mode(self, "serve catalog desired state")),
        }
    }
}

/// Whether SQL statements may write this deployment's catalog desired state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogSqlMutationAdmission {
    /// SQL is the authority: `CREATE`/`DROP CATALOG` commit desired state.
    Accepted,
    /// Another authority owns desired state under the selected mode.  Admitting
    /// SQL as well would give one truth two writers.
    Rejected,
}

/// A named reference to a credential, never the credential itself.
///
/// The dynamic StateStore mode produces none: the attachment record refuses
/// credential-like durable properties outright, so there is nothing under that
/// mode to reference yet.  The field exists on
/// [`CatalogLogicalConfig`] because the logical config is one contract shared
/// by all three modes, and a file or controller source names credentials by
/// reference — a snapshot must be able to carry that without a second snapshot
/// type appearing beside this one.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CatalogCredentialReference(String);

impl CatalogCredentialReference {
    pub fn new(reference: impl Into<String>) -> Self {
        Self(reference.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One catalog's logical configuration, exactly as its source declares it.
///
/// The field set is closed and deliberately narrow.  A resolved secret, the
/// attachment identity, a CAS version, a runtime generation, a Ready or
/// Unavailable state, and a background cursor are all absent — not omitted for
/// brevity, but excluded because each names a fact about *one* deployment's
/// current process or store rather than what the operator asked for.  That
/// exclusion is what makes this type the unit a `SemanticRebind` clone carries
/// across a deployment boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogLogicalConfig {
    instance_id: ConnectorInstanceId,
    provider_id: ConnectorProviderId,
    display_name: String,
    durable_properties: Vec<(String, String)>,
    credential_references: Vec<CatalogCredentialReference>,
    config_format_version: u8,
}

impl CatalogLogicalConfig {
    pub fn new(
        instance_id: ConnectorInstanceId,
        provider_id: ConnectorProviderId,
        display_name: impl Into<String>,
        durable_properties: Vec<(String, String)>,
        credential_references: Vec<CatalogCredentialReference>,
        config_format_version: u8,
    ) -> Self {
        Self {
            instance_id,
            provider_id,
            display_name: display_name.into(),
            durable_properties,
            credential_references,
            config_format_version,
        }
    }

    /// The catalog's SQL name.
    pub const fn instance_id(&self) -> &ConnectorInstanceId {
        &self.instance_id
    }

    /// The connector provider that serves this catalog, i.e. its `type`.
    pub const fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider_id
    }

    pub fn display_name(&self) -> &str {
        &self.display_name
    }

    /// The connector properties the source declares, sorted by key.
    pub fn durable_properties(&self) -> &[(String, String)] {
        &self.durable_properties
    }

    pub fn credential_references(&self) -> &[CatalogCredentialReference] {
        &self.credential_references
    }

    pub const fn config_format_version(&self) -> u8 {
        self.config_format_version
    }

    /// Length-framed contribution to the snapshot identity digest.
    ///
    /// Framing every variable-length field keeps two different configurations
    /// from hashing to one identity through boundary ambiguity.
    fn update_digest(&self, hasher: &mut Sha256) {
        update_framed(hasher, self.instance_id.as_str().as_bytes());
        update_framed(hasher, self.provider_id.as_str().as_bytes());
        update_framed(hasher, self.display_name.as_bytes());
        hasher.update((self.durable_properties.len() as u64).to_be_bytes());
        for (key, value) in &self.durable_properties {
            update_framed(hasher, key.as_bytes());
            update_framed(hasher, value.as_bytes());
        }
        hasher.update((self.credential_references.len() as u64).to_be_bytes());
        for reference in &self.credential_references {
            update_framed(hasher, reference.as_str().as_bytes());
        }
        hasher.update([self.config_format_version]);
    }
}

/// The identity the selected source gives one located desired-state entry.
///
/// Carried beside the logical config instead of inside it: under the dynamic
/// StateStore mode this is the durable attachment id, which names a record in
/// *this* deployment's store.  A `SemanticRebind` clone re-establishes it
/// rather than copying it, which is exactly why
/// [`CatalogLogicalConfig`] must not contain it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogSourceEntryIdentity(Uuid);

impl CatalogSourceEntryIdentity {
    pub const fn new(identity: Uuid) -> Self {
        Self(identity)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

/// One catalog the source located: its logical config plus the identity that
/// source gave it.
///
/// This is the unit of per-catalog materialization, and the reason the two
/// bootstrap failure scopes can be told apart at all: an entry exists only
/// because the enumeration completed, so a failure to materialize *this* entry
/// is a failure of one catalog and never of the snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogDesiredStateEntry {
    identity: CatalogSourceEntryIdentity,
    config: CatalogLogicalConfig,
}

impl CatalogDesiredStateEntry {
    pub const fn new(identity: CatalogSourceEntryIdentity, config: CatalogLogicalConfig) -> Self {
        Self { identity, config }
    }

    pub const fn identity(&self) -> CatalogSourceEntryIdentity {
        self.identity
    }

    pub const fn config(&self) -> &CatalogLogicalConfig {
        &self.config
    }

    /// Projects a durable attachment record into a located entry.
    ///
    /// `created_at_ms` is deliberately dropped: it records when *this* store
    /// first admitted the catalog, which is a fact about the deployment rather
    /// than part of what the operator asked for.
    pub(crate) fn from_attachment(attachment: &CatalogAttachment) -> Self {
        Self {
            identity: CatalogSourceEntryIdentity::new(attachment.attachment_id),
            config: CatalogLogicalConfig {
                instance_id: attachment.instance_id.clone(),
                provider_id: attachment.provider_id.clone(),
                display_name: attachment.display_name.clone(),
                durable_properties: attachment.durable_properties.clone(),
                credential_references: Vec::new(),
                config_format_version: DYNAMIC_STATE_STORE_CONFIG_FORMAT_VERSION,
            },
        }
    }
}

/// A content identity for one snapshot, for diagnostics and logical export.
///
/// The digest covers the logical configs and nothing else, so it is stable
/// across a `SemanticRebind` clone and across a source re-minting entry
/// identities: two deployments that want the same catalogs report the same
/// snapshot identity.  It deliberately does not identify the *store* the
/// snapshot came from — the mode does that.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogDesiredStateSnapshotIdentity {
    mode: CatalogDesiredStateSourceMode,
    catalog_count: usize,
    digest: [u8; 32],
}

impl CatalogDesiredStateSnapshotIdentity {
    pub const fn mode(&self) -> CatalogDesiredStateSourceMode {
        self.mode
    }

    pub const fn catalog_count(&self) -> usize {
        self.catalog_count
    }

    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }

    /// Short, stable rendering for logs.
    pub fn short_digest(&self) -> String {
        hex::encode(&self.digest[..8])
    }
}

/// The exact, complete set of catalog logical configs one enumeration produced.
///
/// "Complete" is the load-bearing word.  A reconcile treats every catalog
/// missing from a snapshot as no longer wanted, so a snapshot that lost entries
/// would silently retire live catalogs.  That is why the only constructor
/// validates, why a source that cannot finish enumerating reports
/// [`CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete`] rather
/// than an empty snapshot, and why the identity is computed here instead of
/// being supplied — a caller cannot hand over an identity that disagrees with
/// the content it describes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogDesiredStateSnapshot {
    identity: CatalogDesiredStateSnapshotIdentity,
    entries: BTreeMap<ConnectorInstanceId, CatalogDesiredStateEntry>,
}

impl CatalogDesiredStateSnapshot {
    /// Validates one complete enumeration into a snapshot.
    ///
    /// Rejecting a duplicate catalog name here rather than letting the last
    /// entry win is what keeps "complete" honest: a source that declared one
    /// name twice has not told us which configuration it wants, and picking one
    /// would materialize a catalog nobody asked for.
    pub fn try_new(
        mode: CatalogDesiredStateSourceMode,
        entries: impl IntoIterator<Item = CatalogDesiredStateEntry>,
    ) -> Result<Self, CatalogApplicationError> {
        let mut located = BTreeMap::new();
        for entry in entries {
            if entry.config().config_format_version() == 0 {
                return Err(untrustworthy_enumeration(
                    mode,
                    format!(
                        "catalog `{}` declares no config format version",
                        entry.config().instance_id().as_str()
                    ),
                ));
            }
            match located.entry(entry.config().instance_id().clone()) {
                Entry::Vacant(vacant) => {
                    vacant.insert(entry);
                }
                Entry::Occupied(occupied) => {
                    return Err(untrustworthy_enumeration(
                        mode,
                        format!(
                            "catalog `{}` is declared more than once",
                            occupied.key().as_str()
                        ),
                    ));
                }
            }
        }
        let mut hasher = Sha256::new();
        hasher.update(SNAPSHOT_IDENTITY_DOMAIN);
        hasher.update((located.len() as u64).to_be_bytes());
        for entry in located.values() {
            entry.config().update_digest(&mut hasher);
        }
        Ok(Self {
            identity: CatalogDesiredStateSnapshotIdentity {
                mode,
                catalog_count: located.len(),
                digest: hasher.finalize().into(),
            },
            entries: located,
        })
    }

    pub const fn identity(&self) -> &CatalogDesiredStateSnapshotIdentity {
        &self.identity
    }

    pub const fn mode(&self) -> CatalogDesiredStateSourceMode {
        self.identity.mode
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether the source wants this catalog.
    pub fn wants(&self, instance_id: &ConnectorInstanceId) -> bool {
        self.entries.contains_key(instance_id)
    }

    /// The logical export: what a `SemanticRebind` clone carries.
    pub fn logical_configs(&self) -> impl Iterator<Item = &CatalogLogicalConfig> {
        self.entries.values().map(CatalogDesiredStateEntry::config)
    }

    /// The located entries, in catalog-name order, for materialization.
    pub(crate) fn into_entries(self) -> impl Iterator<Item = CatalogDesiredStateEntry> {
        self.entries.into_values()
    }

    /// Returns the entry from this exact snapshot, without consulting a
    /// second authority. StaticFile deliberately has no reload/watch path.
    pub(crate) fn locate(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Option<CatalogDesiredStateEntry> {
        self.entries.get(instance_id).cloned()
    }
}

/// The complete source input composition validates before it opens runtime
/// resources. StaticFile carries its already parsed, bounded snapshot; it is
/// not a path that a later owner can reopen or reinterpret.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CatalogDesiredStateSourceInput {
    DynamicStateStore,
    StaticFile(CatalogDesiredStateSnapshot),
    ManagedControllerUnsupported,
}

impl CatalogDesiredStateSourceInput {
    pub const fn mode(&self) -> CatalogDesiredStateSourceMode {
        match self {
            Self::DynamicStateStore => CatalogDesiredStateSourceMode::DynamicStateStore,
            Self::StaticFile(_) => CatalogDesiredStateSourceMode::StaticFile,
            Self::ManagedControllerUnsupported => CatalogDesiredStateSourceMode::ManagedController,
        }
    }
}

/// The desired-state source this frontend was composed with.
///
/// A source is bound once, from one mode, and never rebound: the type holds no
/// interior mutability and the port that owns it takes it by value at
/// construction.  "Which authority serves catalog desired state" is therefore a
/// composition-time fact, not a runtime one.
pub struct CatalogDesiredStateSource {
    mode: CatalogDesiredStateSourceMode,
    authority: CatalogDesiredStateAuthority,
}

/// The authority behind a selected mode.
///
/// There is no arm here that falls back to another mode's authority.  A mode
/// this binary does not implement holds
/// [`CatalogDesiredStateAuthority::Unimplemented`], which answers every
/// enumeration and every mutation with a typed error — the one behaviour that
/// cannot be mistaken for "it worked against some other store".
enum CatalogDesiredStateAuthority {
    /// StateStore attachment records are the desired-state authority.
    DynamicStateStore(CatalogAttachmentRepository),
    /// One startup-validated StaticFile snapshot. It is immutable for this
    /// process and therefore cannot accidentally turn a partial reread into a
    /// smaller desired state.
    StaticFile(CatalogDesiredStateSnapshot),
    /// The mode's port position exists; this binary implements no authority for
    /// it.
    Unimplemented,
}

impl CatalogDesiredStateSource {
    /// Binds an already validated composition input to its sole authority.
    pub fn from_input(
        input: CatalogDesiredStateSourceInput,
        attachments: Option<CatalogAttachmentRepository>,
    ) -> Result<Self, CatalogApplicationError> {
        match input {
            CatalogDesiredStateSourceInput::DynamicStateStore => Self::select(
                CatalogDesiredStateSourceMode::DynamicStateStore,
                attachments,
            ),
            CatalogDesiredStateSourceInput::StaticFile(snapshot) => Self::static_file(snapshot),
            CatalogDesiredStateSourceInput::ManagedControllerUnsupported => Err(unsupported_mode(
                CatalogDesiredStateSourceMode::ManagedController,
                "serve catalog desired state",
            )),
        }
    }

    /// Binds the deployment's selected mode to the authority that serves it.
    ///
    /// The `match` is exhaustive over the closed mode enum, and this is the
    /// only place a source's authority is chosen, so a future mode has to state
    /// its own rather than inheriting the dynamic StateStore one.
    /// `attachments` is consumed by that one mode and dropped by every other,
    /// which is what stops an unimplemented mode from quietly serving
    /// StateStore records.
    pub fn select(
        mode: CatalogDesiredStateSourceMode,
        attachments: Option<CatalogAttachmentRepository>,
    ) -> Result<Self, CatalogApplicationError> {
        match mode {
            CatalogDesiredStateSourceMode::DynamicStateStore => {
                Ok(Self::dynamic_state_store(attachments.ok_or_else(|| {
                    CatalogApplicationError::new(
                        CatalogApplicationErrorKind::Unavailable,
                        "the dynamic StateStore catalog source requires a configured Frontend StateStore",
                    )
                })?))
            }
            CatalogDesiredStateSourceMode::StaticFile => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::Unavailable,
                "the static-file catalog source requires a startup-validated snapshot",
            )),
            CatalogDesiredStateSourceMode::ManagedController => Ok(Self {
                mode,
                authority: CatalogDesiredStateAuthority::Unimplemented,
            }),
        }
    }

    /// The dynamic StateStore source, the only implemented mode.
    pub fn dynamic_state_store(attachments: CatalogAttachmentRepository) -> Self {
        Self {
            mode: CatalogDesiredStateSourceMode::DynamicStateStore,
            authority: CatalogDesiredStateAuthority::DynamicStateStore(attachments),
        }
    }

    /// The StaticFile source reads only the exact snapshot preflight created.
    pub fn static_file(
        snapshot: CatalogDesiredStateSnapshot,
    ) -> Result<Self, CatalogApplicationError> {
        if snapshot.mode() != CatalogDesiredStateSourceMode::StaticFile {
            return Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::InvalidRequest,
                "the static-file catalog source requires a static-file snapshot",
            ));
        }
        Ok(Self {
            mode: CatalogDesiredStateSourceMode::StaticFile,
            authority: CatalogDesiredStateAuthority::StaticFile(snapshot),
        })
    }

    pub const fn mode(&self) -> CatalogDesiredStateSourceMode {
        self.mode
    }

    /// Whether SQL may write catalog desired state through this source.
    pub const fn sql_mutation_admission(&self) -> CatalogSqlMutationAdmission {
        self.mode.sql_mutation_admission()
    }

    /// The complete set of catalogs this source currently declares.
    ///
    /// Every failure here is a failure of the whole snapshot.  The enumeration
    /// is the snapshot's identity, so a store read that failed halfway proves
    /// nothing about what the source wants; reporting the records it did manage
    /// to read would be reporting a desired state the operator never declared.
    pub async fn enumerate(
        &self,
        page_size: usize,
    ) -> Result<CatalogDesiredStateSnapshot, CatalogApplicationError> {
        match &self.authority {
            CatalogDesiredStateAuthority::DynamicStateStore(attachments) => {
                let located = attachments
                    .list_with_page_size(page_size)
                    .await
                    .map_err(|error| untrustworthy_enumeration(self.mode, error))?;
                CatalogDesiredStateSnapshot::try_new(
                    self.mode,
                    located
                        .iter()
                        .map(|versioned| {
                            CatalogDesiredStateEntry::from_attachment(&versioned.attachment)
                        })
                        .collect::<Vec<_>>(),
                )
            }
            CatalogDesiredStateAuthority::StaticFile(snapshot) => Ok(snapshot.clone()),
            CatalogDesiredStateAuthority::Unimplemented => {
                Err(unsupported_mode(self.mode, "enumerate desired state"))
            }
        }
    }

    /// Re-reads one catalog, so a caller can tell "the enumeration did not
    /// observe this entry" apart from "the source no longer declares it".
    ///
    /// A snapshot is a point-in-time observation, and a catalog created after
    /// that observation began is genuinely absent from it while being genuinely
    /// wanted.  Only a read issued *after* the local evidence was observed can
    /// separate the two, which is why this exists as its own operation instead
    /// of being answered from the snapshot.
    pub(crate) async fn locate(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Option<CatalogDesiredStateEntry>, CatalogApplicationError> {
        match &self.authority {
            CatalogDesiredStateAuthority::DynamicStateStore(attachments) => Ok(attachments
                .get(instance_id)
                .await
                .map_err(|error| untrustworthy_enumeration(self.mode, error))?
                .map(|versioned| CatalogDesiredStateEntry::from_attachment(&versioned.attachment))),
            CatalogDesiredStateAuthority::StaticFile(snapshot) => Ok(snapshot.locate(instance_id)),
            CatalogDesiredStateAuthority::Unimplemented => {
                Err(unsupported_mode(self.mode, "locate a catalog"))
            }
        }
    }

    /// The authority a SQL `CREATE`/`DROP CATALOG` writes through.
    ///
    /// The single admission point for SQL catalog mutation: a mode that does
    /// not accept SQL never reaches a repository at all, so a file or
    /// controller source and SQL can never both write one truth.
    pub(crate) fn sql_mutation_authority(
        &self,
    ) -> Result<&CatalogAttachmentRepository, CatalogApplicationError> {
        match self.sql_mutation_admission() {
            CatalogSqlMutationAdmission::Accepted => match &self.authority {
                CatalogDesiredStateAuthority::DynamicStateStore(attachments) => Ok(attachments),
                CatalogDesiredStateAuthority::StaticFile(_) => Err(CatalogApplicationError::new(
                    CatalogApplicationErrorKind::UnsupportedSourceMode,
                    "the static-file catalog source does not admit SQL catalog mutation",
                )),
                // Unreachable through `select`, which pairs the accepting mode
                // with this authority. Reported rather than asserted so a
                // future mode that accepts SQL cannot reach a store it has not
                // been given.
                CatalogDesiredStateAuthority::Unimplemented => {
                    Err(unsupported_mode(self.mode, "mutate catalog desired state"))
                }
            },
            CatalogSqlMutationAdmission::Rejected => Err(CatalogApplicationError::new(
                CatalogApplicationErrorKind::UnsupportedSourceMode,
                format!(
                    "catalog desired state is owned by the `{}` source of this deployment; \
                     SQL CREATE/DROP CATALOG is not admitted",
                    self.mode.as_str()
                ),
            )),
        }
    }
}

fn unsupported_mode(
    mode: CatalogDesiredStateSourceMode,
    operation: &str,
) -> CatalogApplicationError {
    CatalogApplicationError::new(
        CatalogApplicationErrorKind::UnsupportedSourceMode,
        format!(
            "this build implements no catalog desired-state authority for the `{}` source mode \
             and cannot {operation}",
            mode.as_str()
        ),
    )
}

fn untrustworthy_enumeration(
    mode: CatalogDesiredStateSourceMode,
    detail: impl std::fmt::Display,
) -> CatalogApplicationError {
    CatalogApplicationError::new(
        CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete,
        format!(
            "catalog desired state could not be enumerated completely from the `{}` source: \
             {detail}",
            mode.as_str()
        ),
    )
}

fn update_framed(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(name: &str, display: &str) -> CatalogLogicalConfig {
        CatalogLogicalConfig::new(
            ConnectorInstanceId::parse(name).expect("instance ID"),
            ConnectorProviderId::parse("iceberg").expect("provider ID"),
            display,
            vec![("type".to_string(), "iceberg".to_string())],
            Vec::new(),
            DYNAMIC_STATE_STORE_CONFIG_FORMAT_VERSION,
        )
    }

    fn entry(name: &str, display: &str) -> CatalogDesiredStateEntry {
        CatalogDesiredStateEntry::new(
            CatalogSourceEntryIdentity::new(Uuid::now_v7()),
            config(name, display),
        )
    }

    #[test]
    fn snapshot_identity_covers_logical_configuration_and_ignores_entry_identity() {
        let first = CatalogDesiredStateSnapshot::try_new(
            CatalogDesiredStateSourceMode::DynamicStateStore,
            [
                entry("catalog.analytics", "analytics"),
                entry("catalog.raw", "raw"),
            ],
        )
        .expect("snapshot");
        // Same logical configuration, freshly minted entry identities and the
        // opposite enumeration order: a `SemanticRebind` clone must report the
        // same snapshot identity as its source.
        let second = CatalogDesiredStateSnapshot::try_new(
            CatalogDesiredStateSourceMode::DynamicStateStore,
            [
                entry("catalog.raw", "raw"),
                entry("catalog.analytics", "analytics"),
            ],
        )
        .expect("snapshot");
        assert_eq!(first.identity(), second.identity());
        assert_eq!(first.identity().catalog_count(), 2);

        let renamed = CatalogDesiredStateSnapshot::try_new(
            CatalogDesiredStateSourceMode::DynamicStateStore,
            [
                entry("catalog.analytics", "analytics"),
                entry("catalog.raw", "raw-2"),
            ],
        )
        .expect("snapshot");
        assert_ne!(
            first.identity(),
            renamed.identity(),
            "a changed logical configuration must change the snapshot identity"
        );
    }

    #[test]
    fn a_duplicate_catalog_name_is_an_untrustworthy_enumeration_not_a_last_writer_win() {
        let error = CatalogDesiredStateSnapshot::try_new(
            CatalogDesiredStateSourceMode::DynamicStateStore,
            [
                entry("catalog.analytics", "first"),
                entry("catalog.analytics", "second"),
            ],
        )
        .expect_err("a duplicated catalog name must fail the whole snapshot");
        assert_eq!(
            error.kind(),
            CatalogApplicationErrorKind::DesiredStateEnumerationIncomplete
        );
    }

    #[test]
    fn managed_controller_never_borrows_the_dynamic_authority() {
        for mode in [CatalogDesiredStateSourceMode::ManagedController] {
            assert_eq!(
                mode.require_implemented()
                    .expect_err("an unimplemented mode must be rejected before startup")
                    .kind(),
                CatalogApplicationErrorKind::UnsupportedSourceMode
            );
            assert_eq!(
                mode.sql_mutation_admission(),
                CatalogSqlMutationAdmission::Rejected
            );
            let source = CatalogDesiredStateSource::select(mode, None).expect("select source");
            let rejected = source
                .sql_mutation_authority()
                .err()
                .expect("SQL mutation must be rejected");
            assert_eq!(
                rejected.kind(),
                CatalogApplicationErrorKind::UnsupportedSourceMode
            );
        }
        assert!(
            CatalogDesiredStateSourceMode::DynamicStateStore
                .require_implemented()
                .is_ok()
        );
        assert!(
            CatalogDesiredStateSourceMode::StaticFile
                .require_implemented()
                .is_ok()
        );
        assert_eq!(
            CatalogDesiredStateSourceMode::DynamicStateStore.sql_mutation_admission(),
            CatalogSqlMutationAdmission::Accepted
        );
    }

    #[test]
    fn the_dynamic_mode_stamps_the_manifest_record_version() {
        assert_eq!(
            DYNAMIC_STATE_STORE_CONFIG_FORMAT_VERSION,
            StateFamily::CatalogDesiredState
                .record_version()
                .expect("the catalog desired-state family declares a record version")
        );
    }

    #[tokio::test]
    async fn static_file_uses_its_exact_snapshot_and_rejects_sql_mutation() {
        let snapshot = CatalogDesiredStateSnapshot::try_new(
            CatalogDesiredStateSourceMode::StaticFile,
            [entry("catalog.analytics", "analytics")],
        )
        .expect("static snapshot");
        let source =
            CatalogDesiredStateSource::static_file(snapshot.clone()).expect("static source");
        assert_eq!(source.mode(), CatalogDesiredStateSourceMode::StaticFile);
        assert_eq!(
            source
                .enumerate(1)
                .await
                .expect("static enumeration")
                .identity(),
            snapshot.identity()
        );
        let error = match source.sql_mutation_authority() {
            Ok(_) => panic!("StaticFile must reject SQL mutation"),
            Err(error) => error,
        };
        assert_eq!(
            error.kind(),
            CatalogApplicationErrorKind::UnsupportedSourceMode
        );
    }
}
