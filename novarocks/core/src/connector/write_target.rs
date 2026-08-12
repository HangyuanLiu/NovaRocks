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

//! Neutral write-target binding for SQL write statements.
//!
//! One statement resolves its write target exactly once, against a single
//! provider generation, and carries:
//!
//!   - the [`ConnectorTableMetadata`] that generation produced (neutral Arrow
//!     schema, bounded planning facts, opaque table handle);
//!   - the [`ConnectorControlPlanningLease`] that produced it, retained so the
//!     write lease derived from it acts on that same generation.
//!
//! Core never interprets the opaque handle. Physical write facts a writer
//! needs — staging location, sequence numbers, partition spec objects, commit
//! vocabulary, abort cleanup — are deliberately absent: they belong to Provider
//! write preparation, reached through the derived write lease.
//!
//! This is the write-path sibling of the MV refresh binding in
//! `crate::mv::refresh::target_binding`. The two are deliberately separate
//! types: the MV one additionally carries MV refresh-ledger identity
//! (refresh markers, bootstrap state, main-ancestor lineage) that has no
//! meaning for an INSERT or a row mutation.

use novarocks_spi::connector::{
    ConnectorControlPlanningLease, ConnectorControlResolver, ConnectorRequestContext,
    ConnectorTableHandle, ConnectorTableIdentity, ConnectorTableMetadata, ConnectorTableResolution,
    ConnectorWriteLease,
};

/// One write target, resolved once against a single provider generation.
///
/// Cloning is cheap: the metadata's schema is an `Arc` and the lease is a
/// handle onto an already-resolved generation.
///
/// Deliberately not `Debug`: neither [`ConnectorTableMetadata`] nor
/// [`ConnectorControlPlanningLease`] is `Debug`, precisely so an opaque
/// provider handle and a live generation cannot end up in a log line.
#[derive(Clone)]
pub(crate) struct ConnectorWriteTargetBinding {
    metadata: ConnectorTableMetadata,
    lease: ConnectorControlPlanningLease,
}

impl ConnectorWriteTargetBinding {
    pub(crate) const fn new(
        metadata: ConnectorTableMetadata,
        lease: ConnectorControlPlanningLease,
    ) -> Self {
        Self { metadata, lease }
    }

    /// The exact generation that produced every fact in this binding.
    ///
    /// Write preparation must derive its lease from this one rather than
    /// re-resolving `latest`, otherwise a concurrent commit could split one
    /// statement across two generations.
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

    /// The write target's neutral Arrow schema.
    ///
    /// This replaces reading `current_schema()` off a concrete provider table:
    /// column shaping, projection and default filling all work from here.
    pub(crate) fn arrow_schema(&self) -> &arrow::datatypes::SchemaRef {
        &self.metadata.schema
    }

    /// Derive the write lease for this statement from the same generation.
    pub(crate) fn derive_write_lease(&self) -> Result<ConnectorWriteLease, String> {
        self.lease
            .derive_write_lease()
            .map_err(|error| error.to_string())
    }
}

/// Resolve a SQL write target into a neutral binding.
///
/// Mirrors `load_mv_target_binding`: acquire one planning lease, then load the
/// table metadata through that same lease, so the schema, planning facts and
/// opaque handle cannot drift apart.
pub(crate) fn load_write_target_binding(
    controls: &dyn ConnectorControlResolver,
    catalog: &str,
    namespace: &str,
    table: &str,
    resolution: ConnectorTableResolution,
    context: ConnectorRequestContext,
) -> Result<ConnectorWriteTargetBinding, String> {
    let lease = super::acquire_metadata_planning_lease(controls, catalog)?;
    let metadata = super::metadata_load_connector_table_with_planning_lease(
        &lease, context, namespace, table, resolution,
    )?;
    Ok(ConnectorWriteTargetBinding::new(metadata, lease))
}
