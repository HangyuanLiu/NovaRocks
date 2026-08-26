//! The role-facing boundary between an engine role and a typed connector.
//!
//! A frontend or backend never links a provider crate: it holds one of these
//! trait objects, hands it protocol-validated carriers, and receives generic
//! SPI values back. The provider is the only side that matches its own closed
//! variant, so no engine code downcasts and no opaque payload crosses here.

use std::collections::BTreeSet;

use novarocks_spi::connector::ConnectorError;
use novarocks_spi::connector::read_stack::{
    ConnectorPageSource, ConnectorSession, ConnectorSplitBatch, Constraint, DynamicFilter,
    DynamicFilterSnapshot,
};

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
    fn create_page_source(
        &self,
        session: &ConnectorSession,
        table: &CatalogTableHandle,
        split: &ValidatedConnectorSplit,
        columns: &[ScanAssignment],
        dynamic_filter: &WireDynamicFilter,
    ) -> Result<Box<dyn ConnectorPageSource>, ConnectorError>;
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
