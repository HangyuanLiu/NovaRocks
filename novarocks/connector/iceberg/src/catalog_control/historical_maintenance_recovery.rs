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

//! Reading what a dead Iceberg generation left in the table.
//!
//! The live generation rebuilds the marker the old attempt would have
//! committed under — using the *old* incarnation carried in the descriptor —
//! and looks for it in the current table. A match is proof the operation
//! committed, and that is the only thing this can prove. Absence is not proof
//! of the opposite: a marker can live in a snapshot summary that has since
//! been expired, so anything other than a match is reported as ambiguous and
//! the operation stays unresolved.

use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorExecutionBindingKey,
    ConnectorHistoricalMaintenanceCleanupReceipt, ConnectorHistoricalMaintenanceCleanupRequest,
    ConnectorHistoricalMaintenanceDescriptor, ConnectorHistoricalMaintenanceDisposition,
    ConnectorHistoricalMaintenanceFamily, ConnectorHistoricalMaintenanceObservation,
    ConnectorHistoricalMaintenanceOutcome, ConnectorHistoricalMaintenanceProof,
    ConnectorHistoricalMaintenanceRecovery, ConnectorInstanceDescriptor,
    ConnectorMutationOperationId, ConnectorRequestContext, ExternalMutationEvidence,
    ExternalMutationOutcome,
};

use crate::control_runtime::IcebergControlRuntime;

use super::cleanup_maintenance::classify_historical_cleanup_batch;
use super::metadata_maintenance::{
    MetadataMaintenanceMarkerMatch, lookup_historical_metadata_marker,
};

// Design: ADR-0067 (docs/adr/ADR-0067-historical-maintenance-recovery-is-a-separate-capability.md)
pub struct IcebergHistoricalMaintenanceRecovery {
    key: ConnectorExecutionBindingKey,
    #[allow(dead_code)]
    descriptor: ConnectorInstanceDescriptor,
    runtime: Arc<IcebergControlRuntime>,
}

impl IcebergHistoricalMaintenanceRecovery {
    pub fn new(
        key: ConnectorExecutionBindingKey,
        runtime: Arc<IcebergControlRuntime>,
    ) -> Result<Self, ConnectorError> {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")?,
            instance_id: key.instance_id.clone(),
        };
        Ok(Self {
            key,
            descriptor,
            runtime,
        })
    }
}

impl ConnectorHistoricalMaintenanceRecovery for IcebergHistoricalMaintenanceRecovery {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn inspect(
        &self,
        descriptor: ConnectorHistoricalMaintenanceDescriptor,
        _context: ConnectorRequestContext,
    ) -> Result<ConnectorHistoricalMaintenanceObservation, ConnectorError> {
        match descriptor.family {
            ConnectorHistoricalMaintenanceFamily::MetadataMaintenance => {
                self.inspect_metadata(descriptor)
            }
            ConnectorHistoricalMaintenanceFamily::Cleanup => self.inspect_cleanup(descriptor),
            // A distributed rewrite commit writes no marker into the snapshot
            // summary, so it cannot be classified by reading the table. Its
            // evidence is the per-cohort attempt artifacts, which this does not
            // read yet -- and answering without reading them would be a guess,
            // which is the one thing this capability exists to avoid.
            family => Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!(
                    "Iceberg historical maintenance recovery does not classify {family:?} \
                     operations yet"
                ),
            )),
        }
    }

    fn cleanup(
        &self,
        _request: ConnectorHistoricalMaintenanceCleanupRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalMaintenanceCleanupReceipt>, ConnectorError>
    {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg historical maintenance recovery does not remove staged artifacts yet",
        ))
    }

    fn reconcile_cleanup(
        &self,
        _operation_id: ConnectorMutationOperationId,
        _evidence: ExternalMutationEvidence,
        _context: ConnectorRequestContext,
    ) -> Result<ExternalMutationOutcome<ConnectorHistoricalMaintenanceCleanupReceipt>, ConnectorError>
    {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "Iceberg historical maintenance recovery does not remove staged artifacts yet",
        ))
    }
}

impl IcebergHistoricalMaintenanceRecovery {
    fn inspect_metadata(
        &self,
        descriptor: ConnectorHistoricalMaintenanceDescriptor,
    ) -> Result<ConnectorHistoricalMaintenanceObservation, ConnectorError> {
        let matched = lookup_historical_metadata_marker(
            self.runtime.as_ref(),
            &self.key.instance_id,
            &descriptor,
        )?;
        let (disposition, outcome, proof_note) = match matched {
            MetadataMaintenanceMarkerMatch::Committed { snapshot_id } => (
                ConnectorHistoricalMaintenanceDisposition::Applied,
                ConnectorHistoricalMaintenanceOutcome::MetadataMaintenance {
                    committed_version: None,
                    marker_present: true,
                },
                format!(
                    "iceberg-historical-metadata-marker:committed:snapshot={}",
                    snapshot_id.unwrap_or_default()
                ),
            ),
            // A different attempt's marker, or none at all. Neither proves this
            // operation did not commit: the marker property holds one value and
            // a snapshot carrying it can be expired away.
            MetadataMaintenanceMarkerMatch::Undecided { reason } => (
                ConnectorHistoricalMaintenanceDisposition::Ambiguous,
                ConnectorHistoricalMaintenanceOutcome::MetadataMaintenance {
                    committed_version: None,
                    marker_present: false,
                },
                format!("iceberg-historical-metadata-marker:undecided:{reason}"),
            ),
        };
        let proof = ConnectorHistoricalMaintenanceProof::try_new(bytes::Bytes::from(proof_note))?;
        ConnectorHistoricalMaintenanceObservation::try_new(
            &descriptor,
            disposition,
            outcome,
            proof,
            None,
        )
    }

    fn inspect_cleanup(
        &self,
        descriptor: ConnectorHistoricalMaintenanceDescriptor,
    ) -> Result<ConnectorHistoricalMaintenanceObservation, ConnectorError> {
        let plan = artifact(&descriptor, "cleanup-plan")?;
        let prepared = artifact(&descriptor, "cleanup-prepared-batch")?;
        let manifest_digest_bytes = artifact(&descriptor, "cleanup-manifest-digest")?;
        let manifest_digest: [u8; 32] =
            manifest_digest_bytes.as_ref().try_into().map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::CorruptData,
                    "historical cleanup manifest digest is not 32 bytes",
                )
            })?;
        let counts = classify_historical_cleanup_batch(
            self.runtime.as_ref(),
            descriptor.operation_id,
            plan.as_ref(),
            prepared.as_ref(),
            manifest_digest,
        )?;
        // An object that could not be stated is genuinely unknown, and an
        // unknown delete must never be retried, so it drags the whole batch to
        // ambiguous rather than being rounded down to a failure.
        let disposition = if counts.unknown > 0 {
            ConnectorHistoricalMaintenanceDisposition::Ambiguous
        } else if counts.failed > 0 {
            ConnectorHistoricalMaintenanceDisposition::PartiallyApplied
        } else {
            ConnectorHistoricalMaintenanceDisposition::Applied
        };
        let proof = ConnectorHistoricalMaintenanceProof::try_new(bytes::Bytes::from(format!(
            "iceberg-historical-cleanup-batch:deleted={},absent={},failed={},unknown={}",
            counts.deleted, counts.already_absent, counts.failed, counts.unknown
        )))?;
        ConnectorHistoricalMaintenanceObservation::try_new(
            &descriptor,
            disposition,
            ConnectorHistoricalMaintenanceOutcome::Cleanup {
                deleted_count: counts.deleted,
                already_absent_count: counts.already_absent,
                skipped_count: 0,
                failed_count: counts.failed,
                unknown_count: counts.unknown,
            },
            proof,
            None,
        )
    }
}

/// Pull one required artifact out of a descriptor by kind.
fn artifact(
    descriptor: &ConnectorHistoricalMaintenanceDescriptor,
    kind: &str,
) -> Result<bytes::Bytes, ConnectorError> {
    descriptor
        .artifacts
        .iter()
        .find(|artifact| artifact.kind() == kind)
        .map(|artifact| artifact.handle().clone())
        .ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                format!("historical maintenance descriptor carries no `{kind}` artifact"),
            )
        })
}
