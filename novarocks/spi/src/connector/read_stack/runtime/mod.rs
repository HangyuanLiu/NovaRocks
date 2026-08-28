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

//! Transport-neutral connector read runtime contracts.
//!
//! The public values in this module deliberately reveal only a binding and
//! neutral scheduling or planning facts.  Their provider payloads are private
//! to this crate.  A provider supplies concrete associated types to the
//! generic adapter in the sibling module; FE and BE consume only these values
//! and the service traits below.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::connector::read_stack::{
    Assignment, ColumnHandle, ConnectorExpression, ConnectorPageSource, ConnectorSession,
    ConnectorSplitBatch, Constraint, DynamicFilter, DynamicFilterSnapshot, HostAddress,
    SchemaTableName, SplitWeight, SystemTableDistribution, TupleDomain,
};
use crate::connector::{
    ConnectorError, ConnectorInstanceDescriptor, ConnectorInstanceIncarnation,
    ConnectorPinnedFileSet, ConnectorRequestContext,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadBinding {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ConnectorInstanceIncarnation,
}

impl ConnectorReadBinding {
    pub const fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
    ) -> Self {
        Self {
            descriptor,
            incarnation,
        }
    }

    pub const fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }

    pub const fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation
    }
}

#[derive(Clone)]
struct OpaquePayload(Arc<dyn Any + Send + Sync>);

impl OpaquePayload {
    fn new<T: Send + Sync + 'static>(value: T) -> Self {
        Self(Arc::new(value))
    }

    fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}

macro_rules! opaque_handle {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone)]
        pub struct $name {
            binding: ConnectorReadBinding,
            #[allow(dead_code)] // Transaction recovery is introduced by the codec boundary in T30.
            payload: OpaquePayload,
        }

        impl $name {
            pub const fn binding(&self) -> &ConnectorReadBinding {
                &self.binding
            }
        }

        impl Debug for $name {
            fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
                formatter
                    .debug_struct(stringify!($name))
                    .field("binding", &self.binding)
                    .finish_non_exhaustive()
            }
        }
    };
}

opaque_handle!(
    /// A provider-bound table handle whose payload is intentionally inaccessible.
    ///
    /// ```compile_fail
    /// use novarocks_spi::connector::read_stack::ConnectorReadTableHandle;
    ///
    /// fn leak(handle: ConnectorReadTableHandle) {
    ///     let _ = handle.payload;
    /// }
    /// ```
    ConnectorReadTableHandle
);
opaque_handle!(ConnectorReadTransactionHandle);

#[derive(Clone)]
pub struct ConnectorReadSplit {
    binding: ConnectorReadBinding,
    facts: ConnectorReadSplitFacts,
    payload: OpaquePayload,
}

impl ConnectorReadSplit {
    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }

    pub const fn facts(&self) -> &ConnectorReadSplitFacts {
        &self.facts
    }
}

impl Debug for ConnectorReadSplit {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorReadSplit")
            .field("binding", &self.binding)
            .field("facts", &self.facts)
            .finish_non_exhaustive()
    }
}

/// A column's equality and ordering delegate to its provider's concrete
/// `ColumnHandle`, without exposing that payload outside the adapter.
#[derive(Clone)]
pub struct ConnectorReadColumnHandle {
    binding: ConnectorReadBinding,
    payload: OpaquePayload,
    comparison: Arc<dyn ErasedColumnComparison>,
}

/// Private provider-erased ordering for an opaque column payload. It is never
/// derived from wire bytes, pointer identity, or a role-visible token.
trait ErasedColumnComparison: Send + Sync {
    fn compare(&self, left: &OpaquePayload, right: &OpaquePayload) -> Option<Ordering>;
}

struct TypedColumnComparison<C>(PhantomData<fn() -> C>);

impl<C: ColumnHandle> ErasedColumnComparison for TypedColumnComparison<C> {
    fn compare(&self, left: &OpaquePayload, right: &OpaquePayload) -> Option<Ordering> {
        Some(left.downcast_ref::<C>()?.cmp(right.downcast_ref::<C>()?))
    }
}

impl ConnectorReadColumnHandle {
    pub const fn binding(&self) -> &ConnectorReadBinding {
        &self.binding
    }
}

impl Debug for ConnectorReadColumnHandle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectorReadColumnHandle")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl PartialEq for ConnectorReadColumnHandle {
    fn eq(&self, other: &Self) -> bool {
        self.binding == other.binding
            && self
                .comparison
                .compare(&self.payload, &other.payload)
                .is_some_and(Ordering::is_eq)
    }
}

impl Eq for ConnectorReadColumnHandle {}

impl PartialOrd for ConnectorReadColumnHandle {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ConnectorReadColumnHandle {
    fn cmp(&self, other: &Self) -> Ordering {
        (
            self.binding.descriptor.provider_id.as_str(),
            self.binding.descriptor.instance_id.as_str(),
            self.binding.incarnation.to_bytes(),
        )
            .cmp(&(
                other.binding.descriptor.provider_id.as_str(),
                other.binding.descriptor.instance_id.as_str(),
                other.binding.incarnation.to_bytes(),
            ))
            .then_with(|| {
                self.comparison
                    .compare(&self.payload, &other.payload)
                    .expect("same read binding must carry one concrete column family")
            })
    }
}

impl ColumnHandle for ConnectorReadColumnHandle {}

/// Neutral scheduling facts carried alongside a provider split.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadSplitFacts {
    remotely_accessible: bool,
    addresses: Arc<[HostAddress]>,
    affinity_key: Option<Arc<str>>,
    split_weight: SplitWeight,
    retained_size_in_bytes: u64,
}

impl ConnectorReadSplitFacts {
    pub fn new(
        remotely_accessible: bool,
        addresses: Vec<HostAddress>,
        affinity_key: Option<impl AsRef<str>>,
        split_weight: SplitWeight,
        retained_size_in_bytes: u64,
    ) -> Self {
        Self {
            remotely_accessible,
            addresses: Arc::from(addresses),
            affinity_key: affinity_key.map(|value| Arc::from(value.as_ref())),
            split_weight,
            retained_size_in_bytes,
        }
    }

    pub const fn remotely_accessible(&self) -> bool {
        self.remotely_accessible
    }

    pub fn addresses(&self) -> &[HostAddress] {
        &self.addresses
    }

    pub fn affinity_key(&self) -> Option<&str> {
        self.affinity_key.as_deref()
    }

    pub const fn split_weight(&self) -> SplitWeight {
        self.split_weight
    }

    pub const fn retained_size_in_bytes(&self) -> u64 {
        self.retained_size_in_bytes
    }
}

pub type ConnectorReadConstraint = Constraint<ConnectorReadColumnHandle>;
pub type ConnectorReadDynamicFilterSnapshot = DynamicFilterSnapshot<ConnectorReadColumnHandle>;
pub type ConnectorReadDynamicFilter = dyn DynamicFilter<ConnectorReadColumnHandle>;
pub type ConnectorReadAssignment = Assignment<ConnectorReadColumnHandle>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadColumnBinding {
    name: Arc<str>,
    column: ConnectorReadColumnHandle,
    hidden: bool,
}

impl ConnectorReadColumnBinding {
    pub fn new(name: impl AsRef<str>, column: ConnectorReadColumnHandle, hidden: bool) -> Self {
        Self {
            name: Arc::from(name.as_ref()),
            column,
            hidden,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn column(&self) -> &ConnectorReadColumnHandle {
        &self.column
    }

    pub const fn is_hidden(&self) -> bool {
        self.hidden
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorReadFilterApplication {
    handle: ConnectorReadTableHandle,
    remaining_constraint: ConnectorReadConstraint,
    remaining_expression: Option<ConnectorExpression>,
}

impl ConnectorReadFilterApplication {
    pub const fn new(
        handle: ConnectorReadTableHandle,
        remaining_constraint: ConnectorReadConstraint,
        remaining_expression: Option<ConnectorExpression>,
    ) -> Self {
        Self {
            handle,
            remaining_constraint,
            remaining_expression,
        }
    }

    pub const fn handle(&self) -> &ConnectorReadTableHandle {
        &self.handle
    }

    pub fn into_handle(self) -> ConnectorReadTableHandle {
        self.handle
    }

    pub const fn remaining_constraint(&self) -> &ConnectorReadConstraint {
        &self.remaining_constraint
    }

    pub const fn remaining_expression(&self) -> Option<&ConnectorExpression> {
        self.remaining_expression.as_ref()
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorReadLimitApplication {
    handle: ConnectorReadTableHandle,
    limit_guaranteed: bool,
}

impl ConnectorReadLimitApplication {
    pub const fn new(handle: ConnectorReadTableHandle, limit_guaranteed: bool) -> Self {
        Self {
            handle,
            limit_guaranteed,
        }
    }

    pub const fn handle(&self) -> &ConnectorReadTableHandle {
        &self.handle
    }

    pub fn into_handle(self) -> ConnectorReadTableHandle {
        self.handle
    }

    pub const fn limit_guaranteed(&self) -> bool {
        self.limit_guaranteed
    }
}

#[derive(Clone, Debug)]
pub struct ConnectorReadSystemTablePlan {
    handle: ConnectorReadTableHandle,
    distribution: SystemTableDistribution,
}

impl ConnectorReadSystemTablePlan {
    pub const fn new(
        handle: ConnectorReadTableHandle,
        distribution: SystemTableDistribution,
    ) -> Self {
        Self {
            handle,
            distribution,
        }
    }

    pub const fn handle(&self) -> &ConnectorReadTableHandle {
        &self.handle
    }

    pub fn into_handle(self) -> ConnectorReadTableHandle {
        self.handle
    }

    pub const fn distribution(&self) -> SystemTableDistribution {
        self.distribution
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadRelationVersion {
    Current,
    SnapshotId(i64),
    Reference,
}

/// The neutral category of a frozen read relation. The central IDL remains
/// the closed source of truth for its wire representation; this enum retains
/// only the business category after a codec has decoded the carrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadRelationKind {
    Table,
    TableFunction,
    ChangeWindow,
    SystemTable,
    TableExecute,
    MergeTable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectorReadWorkSource {
    RuntimeSplits,
    WholeRelation,
}

#[derive(Clone, Debug)]
pub struct ConnectorReadRelation {
    kind: ConnectorReadRelationKind,
    table: ConnectorReadTableHandle,
    transaction: ConnectorReadTransactionHandle,
}

impl ConnectorReadRelation {
    pub const fn new(
        kind: ConnectorReadRelationKind,
        table: ConnectorReadTableHandle,
        transaction: ConnectorReadTransactionHandle,
    ) -> Self {
        Self {
            kind,
            table,
            transaction,
        }
    }

    pub const fn kind(&self) -> ConnectorReadRelationKind {
        self.kind
    }

    pub const fn table(&self) -> &ConnectorReadTableHandle {
        &self.table
    }

    pub const fn transaction(&self) -> &ConnectorReadTransactionHandle {
        &self.transaction
    }

    pub fn into_parts(
        self,
    ) -> (
        ConnectorReadRelationKind,
        ConnectorReadTableHandle,
        ConnectorReadTransactionHandle,
    ) {
        (self.kind, self.table, self.transaction)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectorReadChangeWindow {
    from_snapshot_id: i64,
    to_snapshot_id: i64,
}

impl ConnectorReadChangeWindow {
    pub const fn new(from_snapshot_id: i64, to_snapshot_id: i64) -> Self {
        Self {
            from_snapshot_id,
            to_snapshot_id,
        }
    }

    pub const fn from_snapshot_id(&self) -> i64 {
        self.from_snapshot_id
    }

    pub const fn to_snapshot_id(&self) -> i64 {
        self.to_snapshot_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorReadFrozenRewriteGroup {
    artifact_location: Arc<str>,
    artifact_digest_hex: Arc<str>,
    group_digest_hex: Arc<str>,
}

impl ConnectorReadFrozenRewriteGroup {
    pub fn new(
        artifact_location: impl AsRef<str>,
        artifact_digest_hex: impl AsRef<str>,
        group_digest_hex: impl AsRef<str>,
    ) -> Self {
        Self {
            artifact_location: Arc::from(artifact_location.as_ref()),
            artifact_digest_hex: Arc::from(artifact_digest_hex.as_ref()),
            group_digest_hex: Arc::from(group_digest_hex.as_ref()),
        }
    }

    pub fn artifact_location(&self) -> &str {
        &self.artifact_location
    }

    pub fn artifact_digest_hex(&self) -> &str {
        &self.artifact_digest_hex
    }

    pub fn group_digest_hex(&self) -> &str {
        &self.group_digest_hex
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectorReadTableExecuteProcedure {
    RewritePositionDeleteFiles(ConnectorReadFrozenRewriteGroup),
}

/// Coordinator-facing read services.  They are transport neutral and never
/// expose the concrete payload of a returned handle.
pub trait ConnectorReadMetadata: Send + Sync {
    /// Freeze an opaque table with the transaction of its exact installed
    /// provider binding.  Roles can retain the resulting relation but cannot
    /// inspect or manufacture its transaction payload.
    fn relation(
        &self,
        kind: ConnectorReadRelationKind,
        table: ConnectorReadTableHandle,
    ) -> Result<ConnectorReadRelation, ConnectorError>;

    fn get_table_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        version: ConnectorReadRelationVersion,
        reference: Option<&str>,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError>;

    fn get_pinned_file_set_handle(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        pinned: &ConnectorPinnedFileSet,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError>;

    fn get_column_bindings(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
    ) -> Result<Vec<ConnectorReadColumnBinding>, ConnectorError>;

    fn apply_filter(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        constraint: &ConnectorReadConstraint,
    ) -> Result<Option<ConnectorReadFilterApplication>, ConnectorError>;

    fn apply_projection(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        assignments: &[ConnectorReadAssignment],
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError>;

    fn apply_limit(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        limit: u64,
    ) -> Result<Option<ConnectorReadLimitApplication>, ConnectorError>;

    fn get_system_table_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
    ) -> Result<Option<ConnectorReadSystemTablePlan>, ConnectorError>;

    fn get_change_window_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        window: ConnectorReadChangeWindow,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError>;

    fn get_table_execute_plan(
        &self,
        session: &ConnectorSession,
        name: &SchemaTableName,
        procedure: ConnectorReadTableExecuteProcedure,
    ) -> Result<Option<ConnectorReadTableHandle>, ConnectorError>;
}

pub trait ConnectorReadSplitSource: Send {
    /// A connector may ask the coordinator to wait briefly for an initial
    /// dynamic-filter snapshot before it expands its first file.  The
    /// coordinator owns the actual cap and fairness policy; zero means this
    /// source must start immediately.
    fn initial_dynamic_filter_wait_request(&self) -> std::time::Duration {
        std::time::Duration::ZERO
    }

    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &ConnectorReadDynamicFilterSnapshot,
    ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError>;

    fn is_finished(&self) -> bool;

    fn close(&mut self) -> Result<(), ConnectorError>;
}

pub trait ConnectorReadSplitManager: Send + Sync {
    fn get_splits(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        columns: &[ConnectorReadAssignment],
        dynamic_filter_columns: &BTreeSet<ConnectorReadColumnHandle>,
        constraint: &ConnectorReadConstraint,
    ) -> Result<Box<dyn ConnectorReadSplitSource>, ConnectorError>;
}

/// A role-local lease that keeps one exact read-control registration alive.
///
/// The connector that owns a control generation retains the strong lease with
/// its generation metadata.  Role registries retain only a weak reference, so
/// dropping the generation can remove exactly its local slot without turning
/// registry retirement into a Host or RPC operation.
///
/// This is deliberately a marker: it carries no provider payload, wire value,
/// or role authority.
pub trait ConnectorReadRegistrationLease: Send + Sync {}

pub trait ConnectorReadPageSourceProvider: Send + Sync {
    fn create_page_source(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        split: &ConnectorReadSplit,
        scheduled_split_sequence_id: u64,
        columns: &[ConnectorReadAssignment],
        dynamic_filter: &Arc<ConnectorReadDynamicFilter>,
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError>;
}

pub trait ConnectorReadSystemTableProvider: Send + Sync {
    fn create_system_page_source(
        &self,
        session: &ConnectorSession,
        table: &ConnectorReadTableHandle,
        columns: &[ConnectorReadAssignment],
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError>;
}

pub trait ConnectorReadProviderFactory: Send + Sync {
    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
        options: super::ConnectorPageSourceProviderOptions,
    ) -> Result<Arc<dyn ConnectorReadPageSourceProvider>, ConnectorError>;

    fn create_system_table_provider(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn ConnectorReadSystemTableProvider>, ConnectorError>;
}

// These helpers are intentionally crate-private.  Concrete connectors use the
// generic adapter; no caller can obtain an `Any` payload or arbitrarily
// downcast an installed handle.
pub(crate) fn table_handle<T: Send + Sync + 'static>(
    binding: ConnectorReadBinding,
    value: T,
) -> ConnectorReadTableHandle {
    ConnectorReadTableHandle {
        binding,
        payload: OpaquePayload::new(value),
    }
}

pub(crate) fn transaction_handle<T: Send + Sync + 'static>(
    binding: ConnectorReadBinding,
    value: T,
) -> ConnectorReadTransactionHandle {
    ConnectorReadTransactionHandle {
        binding,
        payload: OpaquePayload::new(value),
    }
}

pub(crate) fn column_handle<T: ColumnHandle>(
    binding: ConnectorReadBinding,
    value: T,
) -> ConnectorReadColumnHandle {
    ConnectorReadColumnHandle {
        binding,
        payload: OpaquePayload::new(value),
        comparison: Arc::new(TypedColumnComparison::<T>(PhantomData)),
    }
}

pub(crate) fn split_handle<T: Send + Sync + 'static>(
    binding: ConnectorReadBinding,
    facts: ConnectorReadSplitFacts,
    value: T,
) -> ConnectorReadSplit {
    ConnectorReadSplit {
        binding,
        facts,
        payload: OpaquePayload::new(value),
    }
}

pub(crate) fn table_value<T: 'static>(handle: &ConnectorReadTableHandle) -> Option<&T> {
    handle.payload.downcast_ref()
}

pub(crate) fn transaction_value<T: 'static>(handle: &ConnectorReadTransactionHandle) -> Option<&T> {
    handle.payload.downcast_ref()
}

pub(crate) fn column_value<T: 'static>(handle: &ConnectorReadColumnHandle) -> Option<&T> {
    handle.payload.downcast_ref()
}

pub(crate) fn split_value<T: 'static>(handle: &ConnectorReadSplit) -> Option<&T> {
    handle.payload.downcast_ref()
}

pub(crate) fn binding_error() -> ConnectorError {
    ConnectorError::new(
        crate::connector::ConnectorErrorKind::InvalidRequest,
        "connector read handle belongs to another binding",
    )
}

pub(crate) fn type_error() -> ConnectorError {
    ConnectorError::new(
        crate::connector::ConnectorErrorKind::InvalidRequest,
        "connector read handle has an incompatible concrete type",
    )
}

pub(crate) fn map_constraint<C: Ord + Clone + Debug>(
    constraint: &ConnectorReadConstraint,
    mut map: impl FnMut(&ConnectorReadColumnHandle) -> Result<C, ConnectorError>,
) -> Result<Constraint<C>, ConnectorError> {
    let summary = map_tuple_domain(constraint.summary(), &mut map)?;
    let assignments = constraint
        .assignments()
        .iter()
        .map(|(name, column)| map(column).map(|column| (name.clone(), column)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    Constraint::try_new(summary, constraint.expression().clone(), assignments)
}

pub(crate) fn map_tuple_domain<C: Ord + Clone + Debug>(
    domain: &TupleDomain<ConnectorReadColumnHandle>,
    mut map: impl FnMut(&ConnectorReadColumnHandle) -> Result<C, ConnectorError>,
) -> Result<TupleDomain<C>, ConnectorError> {
    let Some(domains) = domain.domains() else {
        return Ok(TupleDomain::none());
    };
    let mapped = domains
        .iter()
        .map(|(column, domain)| map(column).map(|column| (column, domain.clone())))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    TupleDomain::with_column_domains(mapped)
}
