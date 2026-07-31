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

//! Application-side execution of one external catalog mutation.

use novarocks_spi::connector::{
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationReceipt,
    ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
    ConnectorCatalogMutationResolver, ConnectorExecutionBindingKey, ConnectorInstanceId,
    ConnectorMutationOperationId, ConnectorRequestContext, ExternalMutationEffect,
    ExternalMutationFinalization, ExternalMutationOutcome,
};

use crate::common::engine_error::EngineError;

#[derive(Clone, Debug)]
pub(crate) struct CompletedCatalogMutation {
    pub(crate) effect: ExternalMutationEffect,
    pub(crate) receipt: ConnectorCatalogMutationReceipt,
    pub(crate) finalization: ExternalMutationFinalization,
}

/// Executes an external mutation once. A provider may return `CommitUnknown`
/// only with evidence; this adapter reconciles that evidence on the same
/// generation-fenced lease and deliberately never replays the mutation.
// Design: ADR-0017 (docs/adr/ADR-0017-connector-catalog-mutation-outcomes.md)
pub(crate) fn execute_catalog_mutation(
    resolver: &dyn ConnectorCatalogMutationResolver,
    instance_id: &ConnectorInstanceId,
    operation: ConnectorCatalogMutationOperation,
    context: ConnectorRequestContext,
) -> Result<CompletedCatalogMutation, String> {
    let lease = resolver
        .acquire_current_mutation(instance_id)
        .map_err(|error| error.to_string())?;
    let request = ConnectorCatalogMutationRequest {
        operation_id: ConnectorMutationOperationId::new(),
        target: ConnectorExecutionBindingKey {
            instance_id: lease.descriptor().instance_id.clone(),
            incarnation: lease.incarnation(),
        },
        operation,
        context: context.clone(),
    };
    let outcome = lease.execute(request).map_err(|error| error.to_string())?;
    resolve_outcome(&lease, outcome, context)
}

fn resolve_outcome(
    lease: &novarocks_spi::connector::ConnectorCatalogMutationLease,
    outcome: ExternalMutationOutcome<ConnectorCatalogMutationReceipt>,
    context: ConnectorRequestContext,
) -> Result<CompletedCatalogMutation, String> {
    let outcome = match outcome {
        ExternalMutationOutcome::CommitUnknown { evidence, .. } => lease
            .reconcile(ConnectorCatalogMutationReconcileRequest { evidence, context })
            .map_err(|error| error.to_string())?,
        outcome => outcome,
    };
    match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect,
            receipt,
            finalization,
        } => {
            if let ExternalMutationFinalization::Failed(failure) = &finalization {
                return Err(EngineError::commit_known_committed_finalize_failed(
                    failure.to_string(),
                )
                .to_string());
            }
            Ok(CompletedCatalogMutation {
                effect,
                receipt,
                finalization,
            })
        }
        ExternalMutationOutcome::KnownUncommitted { failure } => {
            Err(EngineError::commit_known_uncommitted(failure.to_string()).to_string())
        }
        ExternalMutationOutcome::CommitUnknown { failure, .. } => {
            Err(EngineError::commit_unknown(failure.to_string()).to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_spi::connector::{
        ConnectorCatalogMutation, ConnectorCatalogMutationReceipt,
        ConnectorCatalogMutationReconcileRequest, ConnectorCatalogMutationRequest,
        ConnectorCatalogMutationResolver, ConnectorError, ConnectorInstanceDescriptor,
        ConnectorInstanceId, ConnectorInstanceIncarnation, ConnectorMutationFailure,
        ConnectorMutationFailureKind, ConnectorProviderId, ConnectorRequestContext, CreatePolicy,
        ExternalMutationEffect, ExternalMutationEvidence, ExternalMutationFinalization,
        ExternalMutationOutcome,
    };

    use super::*;

    struct NeverCancelled;
    impl novarocks_spi::connector::ConnectorCancellation for NeverCancelled {
        fn is_cancelled(&self) -> bool {
            false
        }
    }

    struct UnknownMutation {
        descriptor: ConnectorInstanceDescriptor,
        incarnation: ConnectorInstanceIncarnation,
    }

    impl ConnectorCatalogMutation for UnknownMutation {
        fn descriptor(&self) -> &ConnectorInstanceDescriptor {
            &self.descriptor
        }
        fn incarnation(&self) -> ConnectorInstanceIncarnation {
            self.incarnation
        }
        fn execute(
            &self,
            request: ConnectorCatalogMutationRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
        {
            Ok(ExternalMutationOutcome::CommitUnknown {
                failure: ConnectorMutationFailure::new(
                    ConnectorMutationFailureKind::Unavailable,
                    "response lost",
                ),
                evidence: ExternalMutationEvidence::try_new(
                    1,
                    self.descriptor.clone(),
                    self.incarnation,
                    request.operation_id,
                    request.operation.kind(),
                    Bytes::from_static(b"test"),
                )?,
            })
        }
        fn reconcile(
            &self,
            request: ConnectorCatalogMutationReconcileRequest,
        ) -> Result<ExternalMutationOutcome<ConnectorCatalogMutationReceipt>, ConnectorError>
        {
            Ok(ExternalMutationOutcome::KnownCommitted {
                effect: ExternalMutationEffect::Applied,
                receipt: ConnectorCatalogMutationReceipt::try_new(
                    self.descriptor.clone(),
                    self.incarnation,
                    request.evidence.operation_id(),
                    request.evidence.operation_kind(),
                    None,
                )?,
                finalization: ExternalMutationFinalization::Complete,
            })
        }
    }

    struct Resolver(Arc<UnknownMutation>);
    impl ConnectorCatalogMutationResolver for Resolver {
        fn acquire_current_mutation(
            &self,
            _instance_id: &ConnectorInstanceId,
        ) -> Result<novarocks_spi::connector::ConnectorCatalogMutationLease, ConnectorError>
        {
            novarocks_spi::connector::ConnectorCatalogMutationLease::new(
                self.0.descriptor.clone(),
                self.0.incarnation,
                self.0.clone(),
                || {},
            )
        }
    }

    #[test]
    fn unknown_is_reconciled_once_without_replaying_execute() {
        let descriptor = ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            instance_id: ConnectorInstanceId::parse("catalog.analytics").expect("instance"),
        };
        let mutation = Arc::new(UnknownMutation {
            descriptor: descriptor.clone(),
            incarnation: ConnectorInstanceIncarnation::from_bytes([3; 16]),
        });
        let result = execute_catalog_mutation(
            &Resolver(mutation),
            &descriptor.instance_id,
            ConnectorCatalogMutationOperation::CreateNamespace {
                namespace: novarocks_spi::connector::ConnectorNamespaceIdentity {
                    instance_id: descriptor.instance_id.clone(),
                    namespace: Arc::from("db"),
                },
                policy: CreatePolicy::FailIfExists,
            },
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(1),
                Arc::new(NeverCancelled),
                1024,
                1024,
            )
            .expect("context"),
        )
        .expect("reconciled committed result");
        assert_eq!(result.effect, ExternalMutationEffect::Applied);
    }
}
