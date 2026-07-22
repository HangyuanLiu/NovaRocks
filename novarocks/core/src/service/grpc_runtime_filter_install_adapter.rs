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

use std::sync::Arc;

use crate::proto;
use crate::protocol::native::{decode_abort_runtime_filter_deployment, decode_participant_install};
use crate::runtime::query_context::{
    QueryContextManager, QueryId, RuntimeFilterDeploymentAbortOutcome,
    RuntimeFilterDeploymentInstallError, RuntimeFilterDeploymentInstallErrorKind,
    RuntimeFilterDeploymentInstallOutcome, query_context_manager,
};

pub(crate) trait RuntimeFilterDeploymentIngress: Send + Sync {
    fn install(
        &self,
        request: proto::filter::InstallRuntimeFilterDeploymentRequest,
    ) -> proto::filter::InstallRuntimeFilterDeploymentResponse;

    fn abort(
        &self,
        request: proto::filter::AbortRuntimeFilterDeploymentRequest,
    ) -> proto::filter::AbortRuntimeFilterDeploymentResponse;
}

pub(crate) fn query_scoped_runtime_filter_deployment_ingress()
-> Arc<dyn RuntimeFilterDeploymentIngress> {
    Arc::new(QueryScopedRuntimeFilterDeploymentIngress {
        manager: query_context_manager(),
    })
}

#[cfg(test)]
pub(crate) fn query_scoped_runtime_filter_deployment_ingress_with_manager(
    manager: Arc<QueryContextManager>,
) -> Arc<dyn RuntimeFilterDeploymentIngress> {
    Arc::new(QueryScopedRuntimeFilterDeploymentIngress { manager })
}

struct QueryScopedRuntimeFilterDeploymentIngress {
    manager: Arc<QueryContextManager>,
}

impl RuntimeFilterDeploymentIngress for QueryScopedRuntimeFilterDeploymentIngress {
    fn install(
        &self,
        request: proto::filter::InstallRuntimeFilterDeploymentRequest,
    ) -> proto::filter::InstallRuntimeFilterDeploymentResponse {
        let decoded = match decode_participant_install(&request) {
            Ok(decoded) => decoded,
            Err(error) => return install_rejected(invalid_request_rejection(error.to_string())),
        };
        let query_id = QueryId {
            hi: decoded.query_id.hi,
            lo: decoded.query_id.lo,
        };
        match self.manager.install_runtime_filter_deployment(
            query_id,
            decoded.lifecycle,
            decoded.install,
        ) {
            Ok(RuntimeFilterDeploymentInstallOutcome::Applied) => {
                install_accepted(proto::filter::RuntimeFilterDeploymentResponseStatus::Applied)
            }
            Ok(RuntimeFilterDeploymentInstallOutcome::Idempotent) => {
                install_accepted(proto::filter::RuntimeFilterDeploymentResponseStatus::Idempotent)
            }
            Err(error) => install_rejected(rejection_for_error(error)),
        }
    }

    fn abort(
        &self,
        request: proto::filter::AbortRuntimeFilterDeploymentRequest,
    ) -> proto::filter::AbortRuntimeFilterDeploymentResponse {
        let decoded = match decode_abort_runtime_filter_deployment(&request) {
            Ok(decoded) => decoded,
            Err(error) => return abort_rejected(invalid_request_rejection(error.to_string())),
        };
        let query_id = QueryId {
            hi: decoded.query_id.hi,
            lo: decoded.query_id.lo,
        };
        match self
            .manager
            .abort_runtime_filter_deployment(query_id, decoded.epoch)
        {
            Ok(RuntimeFilterDeploymentAbortOutcome::Applied) => {
                abort_accepted(proto::filter::RuntimeFilterDeploymentResponseStatus::Applied)
            }
            Ok(RuntimeFilterDeploymentAbortOutcome::Idempotent) => {
                abort_accepted(proto::filter::RuntimeFilterDeploymentResponseStatus::Idempotent)
            }
            Err(error) => abort_rejected(rejection_for_error(error)),
        }
    }
}

fn invalid_request_rejection(reason: String) -> proto::filter::RuntimeFilterDeploymentRejection {
    proto::filter::RuntimeFilterDeploymentRejection {
        code: proto::filter::RuntimeFilterDeploymentRejectionCode::InvalidRequest as i32,
        reason,
    }
}

fn rejection_for_error(
    error: RuntimeFilterDeploymentInstallError,
) -> proto::filter::RuntimeFilterDeploymentRejection {
    let code = match error.kind() {
        RuntimeFilterDeploymentInstallErrorKind::StaleEpoch => {
            proto::filter::RuntimeFilterDeploymentRejectionCode::StaleEpoch
        }
        RuntimeFilterDeploymentInstallErrorKind::ConflictingDeployment => {
            proto::filter::RuntimeFilterDeploymentRejectionCode::ConflictingDeployment
        }
        RuntimeFilterDeploymentInstallErrorKind::QueryAborted => {
            proto::filter::RuntimeFilterDeploymentRejectionCode::QueryAborted
        }
        RuntimeFilterDeploymentInstallErrorKind::ActiveFragments => {
            proto::filter::RuntimeFilterDeploymentRejectionCode::ActiveFragments
        }
        RuntimeFilterDeploymentInstallErrorKind::Internal => {
            proto::filter::RuntimeFilterDeploymentRejectionCode::Internal
        }
    };
    proto::filter::RuntimeFilterDeploymentRejection {
        code: code as i32,
        reason: error.detail().to_string(),
    }
}

fn install_accepted(
    status: proto::filter::RuntimeFilterDeploymentResponseStatus,
) -> proto::filter::InstallRuntimeFilterDeploymentResponse {
    proto::filter::InstallRuntimeFilterDeploymentResponse {
        status: status as i32,
        rejection: None,
    }
}

fn install_rejected(
    rejection: proto::filter::RuntimeFilterDeploymentRejection,
) -> proto::filter::InstallRuntimeFilterDeploymentResponse {
    proto::filter::InstallRuntimeFilterDeploymentResponse {
        status: proto::filter::RuntimeFilterDeploymentResponseStatus::Rejected as i32,
        rejection: Some(rejection),
    }
}

fn abort_accepted(
    status: proto::filter::RuntimeFilterDeploymentResponseStatus,
) -> proto::filter::AbortRuntimeFilterDeploymentResponse {
    proto::filter::AbortRuntimeFilterDeploymentResponse {
        status: status as i32,
        rejection: None,
    }
}

fn abort_rejected(
    rejection: proto::filter::RuntimeFilterDeploymentRejection,
) -> proto::filter::AbortRuntimeFilterDeploymentResponse {
    proto::filter::AbortRuntimeFilterDeploymentResponse {
        status: proto::filter::RuntimeFilterDeploymentResponseStatus::Rejected as i32,
        rejection: Some(rejection),
    }
}

#[cfg(test)]
mod tests {
    use crate::common::types::UniqueId;
    use crate::proto;
    use crate::runtime::query_context::{QueryContextManager, QueryId};
    use crate::runtime_filter::port::identity::{DeploymentEpoch, ProducerSequence};
    use crate::runtime_filter::port::transport::{
        ContributionRouteIdentity, RuntimeFilterAcceptStatus, RuntimeFilterEnvelope,
        RuntimeFilterEnvelopeKind, RuntimeFilterRouteIdentity,
    };
    use crate::service::runtime_filter_envelope_ingress::query_scoped_runtime_filter_envelope_ingress_with_manager;

    use super::query_scoped_runtime_filter_deployment_ingress_with_manager;

    const QUERY: QueryId = QueryId { hi: 931, lo: 932 };

    #[test]
    fn invalid_install_is_rejected_before_context_creation() {
        let manager = QueryContextManager::new_for_test();
        let ingress = query_scoped_runtime_filter_deployment_ingress_with_manager(manager.clone());

        let response =
            ingress.install(proto::filter::InstallRuntimeFilterDeploymentRequest::default());

        assert_eq!(
            response.status,
            proto::filter::RuntimeFilterDeploymentResponseStatus::Rejected as i32
        );
        assert_eq!(
            response.rejection.expect("typed rejection").code,
            proto::filter::RuntimeFilterDeploymentRejectionCode::InvalidRequest as i32
        );
        assert_eq!(manager.fragment_counts_for_test(QUERY), None);
        assert!(manager.runtime_filter_service_for_ingress(QUERY).is_none());
    }

    #[test]
    fn ordinary_envelope_ingress_remains_lookup_only() {
        let manager = QueryContextManager::new_for_test();
        let ingress = query_scoped_runtime_filter_envelope_ingress_with_manager(manager.clone());
        assert_eq!(manager.fragment_counts_for_test(QUERY), None);
        assert!(manager.runtime_filter_service_for_ingress(QUERY).is_none());
        let envelope = RuntimeFilterEnvelope::try_new(
            RuntimeFilterEnvelopeKind::ProducerClosed,
            UniqueId {
                hi: QUERY.hi,
                lo: QUERY.lo,
            },
            crate::runtime_filter::model::contract::ChannelId::new(1),
            DeploymentEpoch::new(7),
            RuntimeFilterRouteIdentity::contribution(
                ContributionRouteIdentity::try_new(
                    crate::runtime_filter::model::contract::BindingId::new(1),
                    UniqueId { hi: 933, lo: 934 },
                    crate::runtime_filter::port::identity::PartitionId::new(0),
                    ProducerSequence::new(1),
                )
                .expect("valid contribution identity"),
            ),
            Some(
                crate::runtime_filter::port::transport::ProducerOpenMetadata::try_new(1)
                    .expect("valid producer metadata"),
            ),
            None,
            &[0; 32],
            Vec::new(),
        )
        .expect("valid envelope");

        let result = ingress.accept(envelope);

        assert_eq!(result.accept_status(), RuntimeFilterAcceptStatus::Rejected);
        assert_eq!(manager.fragment_counts_for_test(QUERY), None);
        assert!(manager.runtime_filter_service_for_ingress(QUERY).is_none());
    }
}
