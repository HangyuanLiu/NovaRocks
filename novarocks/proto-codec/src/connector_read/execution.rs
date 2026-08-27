//! The role-facing boundary between an engine role and a typed connector.
//!
//! A frontend or backend never links a provider crate: it holds one of these
//! trait objects, hands it protocol-validated carriers, and receives generic
//! SPI values back. The provider is the only side that matches its own closed
//! variant, so no engine code downcasts and no opaque payload crosses here.
// Design: ADR-0114 (docs/adr/ADR-0114-trino-aligned-typed-connector-read-stack.md)

use std::collections::BTreeSet;

use std::sync::Arc;

use novarocks_spi::connector::read_stack::{
    ConnectorPageSource, ConnectorSession, ConnectorSplitBatch, Constraint, DynamicFilter,
    DynamicFilterSnapshot,
};
use novarocks_spi::connector::{ConnectorError, ConnectorRequestContext};

use super::handle::CatalogTableHandle;
use super::predicate::ValidatedColumnHandle;
use super::scan::ScanAssignment;
use super::split::ValidatedConnectorSplit;

/// The predicate algebra every typed connector boundary speaks.
pub type WireConstraint = Constraint<ValidatedColumnHandle>;

/// A dynamic-filter observation over wire column handles.
pub type WireDynamicFilterSnapshot = DynamicFilterSnapshot<ValidatedColumnHandle>;

/// Live dynamic-filter state over wire column handles.
pub type WireDynamicFilter = dyn DynamicFilter<ValidatedColumnHandle>;

/// A connector-owned, lazily advancing split enumerator seen by the engine.
///
/// It is owned by exactly one execution round: aborting a round closes it, and
/// a replacement round builds a new one rather than resuming this.
pub trait TypedConnectorSplitSource: Send {
    /// Produce up to `max_size` splits. An empty batch means "nothing right
    /// now", never "finished".
    fn next_batch(
        &mut self,
        max_size: usize,
        dynamic_filter: &WireDynamicFilterSnapshot,
    ) -> Result<ConnectorSplitBatch<ValidatedConnectorSplit>, ConnectorError>;

    fn is_finished(&self) -> bool;

    /// Idempotent, and may race with an outstanding batch request.
    fn close(&mut self) -> Result<(), ConnectorError>;
}

/// The coordinator-side entry point a typed connector implements.
pub trait TypedConnectorSplitManager: Send + Sync {
    fn get_splits(
        &self,
        session: &ConnectorSession,
        table: &CatalogTableHandle,
        columns: &[ScanAssignment],
        dynamic_filter_columns: &BTreeSet<ValidatedColumnHandle>,
        constraint: &WireConstraint,
    ) -> Result<Box<dyn TypedConnectorSplitSource>, ConnectorError>;
}

/// The worker-side entry point a typed connector implements.
///
/// One split creates one page source. The provider instance itself lives for a
/// fragment instance and scan node, so a footer cache and delete manager can be
/// shared across the splits of one scan without any process-global state.
pub trait TypedConnectorPageSourceProvider: Send + Sync {
    /// `scheduled_split_sequence_id` names this split within its task attempt.
    /// It is the only scheduling identity a page source may use, and it is what
    /// lets a row-group observation be attributed without a membership digest.
    ///
    /// The dynamic filter arrives as a shared handle rather than a borrow: the
    /// returned page source outlives this call and must be able to re-read the
    /// filter before each row group it has not read yet.
    fn create_page_source(
        &self,
        session: &ConnectorSession,
        table: &CatalogTableHandle,
        split: &ValidatedConnectorSplit,
        scheduled_split_sequence_id: u64,
        columns: &[ScanAssignment],
        dynamic_filter: &Arc<WireDynamicFilter>,
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError>;
}

/// Builds the worker-side providers for one fragment instance and scan node.
///
/// The provider itself is deliberately not process-wide: it owns a footer cache
/// and a delete manager that must not outlive the query that opened them, and
/// it needs that request's deadline and cancellation. The registry therefore
/// holds this factory, which is generation-scoped and stateless.
pub trait TypedConnectorProviderFactory: Send + Sync {
    fn create_page_source_provider(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn TypedConnectorPageSourceProvider>, ConnectorError>;

    fn create_system_table_provider(
        &self,
        request: &ConnectorRequestContext,
    ) -> Result<Arc<dyn TypedConnectorSystemTableProvider>, ConnectorError>;
}

/// A system relation the coordinator resolves to exactly one backend.
///
/// Its page source reads an immutable metadata file directly, so it needs no
/// split at all; synthesizing one would invent scheduling identity that has no
/// corresponding work.
pub trait TypedConnectorSystemTableProvider: Send + Sync {
    fn create_system_page_source(
        &self,
        session: &ConnectorSession,
        table: &CatalogTableHandle,
        columns: &[ScanAssignment],
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError>;
}
