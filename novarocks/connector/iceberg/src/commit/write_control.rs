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

//! Generation-local Iceberg write admission.
//!
//! Everything a write *does* — sealing branches, opening writers, and the one
//! external commit — belongs to
//! [`write_stack`](crate::commit::write_stack). What is left here is the
//! admission half that runs before any session exists: reading the frozen
//! admitted table and answering, from Iceberg facts alone, whether the
//! statement can be written at all and in which physical shape.
//!
//! Those three answers have no write-stack equivalent because they are needed
//! *before* the caller can build a begin request:
//!
//! * [`prepare_write`](super::write_preparation::prepare_write) runs the
//!   Iceberg write-support guards and signs the Arrow input;
//! * [`prepare_row_mutation`](super::row_mutation_preparation::prepare_row_mutation)
//!   picks merge-on-read or copy-on-write and signs the match contract whose
//!   effect column the session later reads; and
//! * `preview_managed_partition_spec` answers which partitioning a managed
//!   replacement would establish, without proposing it.
//!
//! It owns no operation table, no activation, and no commit authority.

use std::sync::Arc;
use std::time::Instant;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor,
    ConnectorManagedPartitionSpecPreview, ConnectorManagedPartitionSpecPreviewRequest,
    ConnectorProviderBindingKey, ConnectorRequestContext, ConnectorRowMutationPreparationOutcome,
    ConnectorRowMutationPreparationRequest, ConnectorWriteAbortOutcome, ConnectorWriteAbortRequest,
    ConnectorWriteCommitRequest, ConnectorWriteControl, ConnectorWritePlan,
    ConnectorWritePlanningRequest, ConnectorWritePreparationOutcome,
    ConnectorWritePreparationRequest, ConnectorWriteReceipt, ConnectorWriteReconcileRequest,
    ExternalMutationOutcome, ProviderBindingEpoch,
};

use crate::metadata::IcebergMetadata;
use crate::metadata_context::IcebergMetadataContext;

/// Concrete write-admission capability assembled with every other capability
/// of one Iceberg control generation.
#[derive(Clone)]
pub struct IcebergWriteControl {
    key: ConnectorProviderBindingKey,
    provider: IcebergMetadata,
}

impl IcebergWriteControl {
    /// Construct an exact-generation capability over `runtime`; callers cannot
    /// inject an independent catalog client or registry.
    pub fn new(
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ProviderBindingEpoch,
        runtime: Arc<IcebergMetadataContext>,
    ) -> Self {
        let key = ConnectorProviderBindingKey {
            instance_id: descriptor.instance_id.clone(),
            incarnation,
        };
        let provider = IcebergMetadata::new(descriptor, incarnation, runtime);
        Self { key, provider }
    }
}

impl ConnectorWriteControl for IcebergWriteControl {
    fn binding_key(&self) -> &ConnectorProviderBindingKey {
        &self.key
    }

    fn prepare_write(
        &self,
        request: ConnectorWritePreparationRequest,
    ) -> Result<ConnectorWritePreparationOutcome, ConnectorError> {
        super::write_preparation::prepare_write(request, &self.key)
    }

    fn prepare_row_mutation(
        &self,
        request: ConnectorRowMutationPreparationRequest,
    ) -> Result<ConnectorRowMutationPreparationOutcome, ConnectorError> {
        super::row_mutation_preparation::prepare_row_mutation(request, &self.key)
    }

    fn preview_managed_partition_spec(
        &self,
        request: ConnectorManagedPartitionSpecPreviewRequest,
    ) -> Result<ConnectorManagedPartitionSpecPreview, ConnectorError> {
        validate_context(request.context())?;
        request.validate(&self.key)?;
        let table = self.provider.table_payload(request.table())?;
        if table.metadata_table_type.is_some() {
            return Err(invalid(
                "Iceberg metadata tables cannot be repartition targets",
            ));
        }
        let table_info = table.table_info.ok_or_else(|| {
            corrupt("admitted Iceberg repartition target is missing its frozen table descriptor")
        })?;
        let serialized = table_info.serialized_metadata.as_deref().ok_or_else(|| {
            corrupt("admitted Iceberg repartition target is missing frozen metadata")
        })?;
        let metadata = serde_json::from_str(serialized).map_err(|error| {
            corrupt(format!(
                "decode admitted Iceberg repartition target metadata: {error}"
            ))
        })?;
        // The same preview the write session runs when it prepares the real
        // transition, so an accepted preview and the transition that follows it
        // cannot disagree about the partitioning they establish.
        let prepared = super::write_stack::repartition::preview_managed_repartition(
            &metadata,
            request.replacement(),
        )?;
        ConnectorManagedPartitionSpecPreview::try_new(
            self.key.clone(),
            request.operation_id(),
            prepared.committed().clone(),
        )
    }

    fn plan_write(
        &self,
        _request: ConnectorWritePlanningRequest,
    ) -> Result<ConnectorWritePlan, ConnectorError> {
        Err(retired("writer placement planning"))
    }

    fn commit(
        &self,
        _request: ConnectorWriteCommitRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        Err(retired("write commit"))
    }

    fn abort(
        &self,
        _request: ConnectorWriteAbortRequest,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        Err(retired("write abort"))
    }

    fn reconcile(
        &self,
        _request: ConnectorWriteReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        Err(retired("write reconciliation"))
    }
}

fn validate_context(context: &ConnectorRequestContext) -> Result<(), ConnectorError> {
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

/// The four methods the retired write-operation aggregate implemented and the
/// write session now owns. They have no caller; the trait declares them
/// without a default, so they are rejected here until the trait itself loses
/// them.
fn retired(subject: &str) -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Unsupported,
        format!("Iceberg {subject} belongs to the write session, not the write control"),
    )
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::InvalidRequest, message)
}

fn corrupt(message: impl Into<String>) -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::CorruptData, message)
}
