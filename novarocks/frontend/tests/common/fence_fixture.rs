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

//! Shared external-write-fence fixture for the DML route integration tests.
//!
//! Distributed write now fails closed until a fence is established, so a fake
//! engine that does not expose a write authority cannot dispatch anything. That
//! is the production invariant, not a test artifact — so route tests have to
//! establish a real fence rather than opt out of one.
//!
//! This fixture supplies the connector half exactly the way a production route
//! does: the fence is sealed by the caller's own coordination attempt and then
//! established on an exact write lease. Nothing here weakens the invariant; it
//! only provides an authority for the fake engine to fence against.

#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorCancellation, ConnectorError, ConnectorEstablishedWriteFence,
    ConnectorExecutionBindingKey, ConnectorExternalFenceReceipt, ConnectorExternalFenceRequest,
    ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorRequestContext,
    ConnectorTableIdentity, ConnectorWriteAbortOutcome, ConnectorWriteAbortRequest,
    ConnectorWriteCommitRequest, ConnectorWriteControl, ConnectorWriteLease,
    ConnectorWriteOperationId, ConnectorWritePlan, ConnectorWritePlanningRequest,
    ConnectorWriteReceipt, ConnectorWriteReconcileRequest, ConnectorWriteTargetRef,
    ExternalMutationOutcome,
};

const FENCE_MARKER: &[u8] = b"dml-route-test-fence-marker";

struct NeverCancelled;

impl ConnectorCancellation for NeverCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

pub fn connector_context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(5),
        Arc::new(NeverCancelled),
        1024,
        4096,
    )
    .expect("connector request context")
}

pub fn binding_key() -> ConnectorExecutionBindingKey {
    ConnectorExecutionBindingKey {
        instance_id: ConnectorInstanceId::parse("dml-route-test").expect("instance id"),
        incarnation: ConnectorInstanceIncarnation::from_bytes([5; 16]),
    }
}

pub fn connector_table() -> ConnectorTableIdentity {
    ConnectorTableIdentity {
        instance_id: binding_key().instance_id,
        namespace: Arc::from("db"),
        table: Arc::from("target"),
    }
}

pub fn connector_write_operation_id() -> ConnectorWriteOperationId {
    ConnectorWriteOperationId::from_bytes([9; 16])
}

/// A write control that can do exactly one thing: establish a fence.
///
/// Every other terminal method refuses, so a test that accidentally reaches an
/// ordinary write path fails loudly instead of silently passing.
struct FenceControl {
    key: ConnectorExecutionBindingKey,
}

impl ConnectorWriteControl for FenceControl {
    fn binding_key(&self) -> &ConnectorExecutionBindingKey {
        &self.key
    }

    fn establish_external_fence(
        &self,
        request: ConnectorExternalFenceRequest,
    ) -> Result<ConnectorExternalFenceReceipt, ConnectorError> {
        request.validate(&self.key)?;
        ConnectorExternalFenceReceipt::try_new(&request.fence, Bytes::from_static(FENCE_MARKER))
    }

    fn plan_write(
        &self,
        _request: ConnectorWritePlanningRequest,
    ) -> Result<ConnectorWritePlan, ConnectorError> {
        Err(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "fence fixture does not plan writes",
        ))
    }

    fn commit(
        &self,
        _request: ConnectorWriteCommitRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        Err(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "fence fixture does not commit",
        ))
    }

    fn abort(
        &self,
        _request: ConnectorWriteAbortRequest,
    ) -> Result<ConnectorWriteAbortOutcome, ConnectorError> {
        Err(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "fence fixture does not abort",
        ))
    }

    fn reconcile(
        &self,
        _request: ConnectorWriteReconcileRequest,
    ) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
        Err(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unsupported,
            "fence fixture does not reconcile",
        ))
    }
}

pub fn fence_lease() -> ConnectorWriteLease {
    let key = binding_key();
    ConnectorWriteLease::new(key.clone(), Arc::new(FenceControl { key }), || {})
        .expect("exact write lease")
}

/// Complete a proposal the way a production route must: the connector half
/// comes from the write authority, never from the frontend.
pub fn establish_from_proposal<F>(seal: F) -> Result<ConnectorEstablishedWriteFence, ConnectorError>
where
    F: FnOnce(
        ConnectorWriteOperationId,
        ConnectorTableIdentity,
        ConnectorWriteTargetRef,
    )
        -> Result<novarocks_spi::connector::ConnectorExternalOperationFence, ConnectorError>,
{
    let fence = seal(
        connector_write_operation_id(),
        connector_table(),
        ConnectorWriteTargetRef::main(),
    )?;
    fence_lease().establish_external_fence(fence, connector_context())
}
