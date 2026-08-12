// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Neutral MV target binding for the refresh / plan / commit call chain.
//!
//! Before SPI-5I the MV apply path threaded
//! `(IcebergCatalogEntry, Arc<dyn Catalog>, IcebergLoadedTable)` through every
//! refresh function and read provider `TableMetadata` directly. This module
//! replaces that triple with a single value that carries only:
//!
//!   - the neutral [`ConnectorTableMetadata`] loaded from one exact generation
//!     (Arrow schema, bounded planning facts, opaque table handle);
//!   - the [`ConnectorControlPlanningLease`] that produced it, retained so
//!     every downstream mutation and write acts on the same generation;
//!   - the [`MvRefreshTargetObservation`] holding the refresh-time snapshot and
//!     ref identity Core legitimately owns.
//!
//! Core never interprets the opaque handle. Physical storage facts a writer
//! needs (table location, sequence numbers, partition spec objects) are
//! deliberately absent — they belong to Provider write preparation.

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorRequestContext, ConnectorTableHandle,
    ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableResolution,
};

use novarocks_catalog::identifier::TableIdentity;

use crate::mv::persistence::schema::MvPartitionContract;
use crate::mv::storage_observation::MvRefreshTargetObservation;

/// One MV target, resolved once against a single provider generation.
///
/// Cloning is cheap enough for the refresh call chain: the metadata's schema is
/// an `Arc`, and the observation's payload is bounded by its own validator.
///
/// Deliberately not `Debug`: `ConnectorTableMetadata` and
/// `ConnectorControlPlanningLease` are not `Debug` precisely so an opaque
/// handle and a live generation cannot end up in a log line.
#[derive(Clone)]
pub(crate) struct MvTargetBinding {
    metadata: ConnectorTableMetadata,
    lease: ConnectorControlPlanningLease,
    observation: MvRefreshTargetObservation,
}

impl MvTargetBinding {
    pub(crate) const fn new(
        metadata: ConnectorTableMetadata,
        lease: ConnectorControlPlanningLease,
        observation: MvRefreshTargetObservation,
    ) -> Self {
        Self {
            metadata,
            lease,
            observation,
        }
    }

    /// The exact generation that produced every fact in this binding.
    ///
    /// Downstream mutation and write preparation must reuse this lease rather
    /// than re-resolving `latest`, otherwise a concurrent commit could split
    /// one refresh attempt across two generations.
    pub(crate) const fn lease(&self) -> &ConnectorControlPlanningLease {
        &self.lease
    }

    pub(crate) const fn metadata(&self) -> &ConnectorTableMetadata {
        &self.metadata
    }

    /// Opaque provider handle. Core passes it through and never decodes it.
    pub(crate) const fn handle(&self) -> &ConnectorTableHandle {
        &self.metadata.table
    }

    pub(crate) const fn identity(&self) -> &ConnectorTableIdentity {
        &self.metadata.identity
    }

    pub(crate) fn arrow_schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.metadata.schema
    }

    pub(crate) const fn observation(&self) -> &MvRefreshTargetObservation {
        &self.observation
    }

    pub(crate) fn table_uuid(&self) -> &str {
        self.observation.table_uuid()
    }

    pub(crate) const fn schema_id(&self) -> i32 {
        self.observation.schema_id()
    }

    pub(crate) const fn partition(&self) -> &MvPartitionContract {
        self.observation.partition()
    }

    pub(crate) const fn current_snapshot_id(&self) -> Option<i64> {
        self.observation.current_snapshot_id()
    }

    pub(crate) fn snapshot_id_for_ref(&self, ref_name: &str) -> Option<i64> {
        self.observation.snapshot_id_for_ref(ref_name)
    }
}

/// Resolve an MV target into a neutral binding.
///
/// Mirrors `observe_schema_validation_for_table`: acquire one planning lease,
/// load metadata through it, then observe refresh facts on that same lease so
/// the schema, handle, snapshot and refs cannot drift apart.
pub(crate) fn load_mv_target_binding(
    state: &Arc<crate::engine::StandaloneState>,
    table: &TableIdentity,
    connector_context: &ConnectorRequestContext,
) -> Result<MvTargetBinding, String> {
    let exact_lease = crate::connector::acquire_metadata_planning_lease(
        state.connector_control.as_ref(),
        &table.catalog,
    )?;
    let metadata = crate::connector::metadata_load_connector_table_with_planning_lease(
        &exact_lease,
        connector_context.clone(),
        &table.namespace,
        &table.table,
        ConnectorTableResolution::StrictBaseTable,
    )?;
    let observation = state
        .mv_storage_observation
        .observe_refresh_target(&exact_lease, &metadata, connector_context.clone())
        .map_err(|error| {
            format!(
                "observe MV refresh target facts for {}: {error}",
                table.fqn()
            )
        })?;
    Ok(MvTargetBinding::new(metadata, exact_lease, observation))
}
