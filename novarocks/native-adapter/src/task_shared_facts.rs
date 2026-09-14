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

//! Native projections of task-protocol shared facts.
//!
//! Worker owns the lifecycle verdict and role products consume the typed
//! result.  This adapter is the sole place that recovers generated payloads
//! from codec-owned handles.

use std::sync::Arc;

use novarocks_execution::runtime::query_options::QueryOptions;
use novarocks_execution_contract::task_execution::descriptor::PhysicalFragmentPlan;
use novarocks_execution_contract::task_execution::domain::CodecOwnedContent;
use novarocks_execution_contract::task_execution::operation::CredentialUpdate;
use novarocks_execution_contract::task_execution::status::TaskFailureCategory;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_codec::catalog::CatalogSet;
use novarocks_proto_codec::lifecycle::terminal::QueryTerminalProfileContributionTelemetry;
use novarocks_proto_models::novarocks as proto;
use novarocks_spi::connector::CatalogProperties;
use novarocks_task_codec::descriptor::WireFragmentPlan;
use novarocks_task_codec::domain::{
    WireContent, WireCredential, stored_credential, stored_message,
};
use novarocks_worker::{HostRejection, ReleasedContextEvidence, RuntimeFilterReleaseObservation};

use crate::query_options::decode_query_options_at;

const RELEASE_RUNTIME_FILTER_EVIDENCE_DOMAIN_TAG: &[u8] =
    b"novarocks.task_execution.release.runtime_filter_evidence.v1";

pub fn catalog_bindings(
    payload: &dyn CodecOwnedContent,
) -> Result<Vec<CatalogProperties>, HostRejection> {
    let raw = stored_message::<novarocks_proto_models::catalog::CatalogSet>(payload)
        .ok_or_else(|| internal("catalog binding payload is not a catalog set"))?;
    CatalogSet::parse(raw.clone())
        .and_then(|catalog_set| catalog_set.catalogs())
        .map_err(|error| protocol(&format!("catalog binding is invalid: {error}")))
}

pub fn runtime_filter_install(
    payload: &dyn CodecOwnedContent,
) -> Result<&proto::RuntimeFilterContribution, HostRejection> {
    stored_message::<proto::RuntimeFilterContribution>(payload)
        .ok_or_else(|| internal("runtime filter payload is not a participant contribution"))
}

pub fn sealed_runtime_filter_evidence(
    telemetry: QueryTerminalProfileContributionTelemetry,
) -> ReleasedContextEvidence {
    let observation = if telemetry.available().is_some() {
        RuntimeFilterReleaseObservation::Available
    } else {
        RuntimeFilterReleaseObservation::Unavailable
    };
    ReleasedContextEvidence::with_runtime_filter(
        Arc::new(WireContent::new(
            RELEASE_RUNTIME_FILTER_EVIDENCE_DOMAIN_TAG,
            telemetry.as_proto().clone(),
        )),
        observation,
    )
}

pub fn release_runtime_filter_telemetry(
    evidence: &ReleasedContextEvidence,
) -> Result<Option<QueryTerminalProfileContributionTelemetry>, HostRejection> {
    evidence
        .runtime_filter()
        .map(|payload| {
            let raw = stored_message::<proto::QueryTerminalProfileContributionTelemetry>(
                payload.as_ref(),
            )
            .ok_or_else(|| internal("release runtime-filter evidence is not terminal telemetry"))?;
            QueryTerminalProfileContributionTelemetry::parse(raw.clone()).map_err(|error| {
                internal(&format!(
                    "release runtime-filter evidence violates the terminal contract: {error}"
                ))
            })
        })
        .transpose()
}

pub fn query_options(payload: &dyn CodecOwnedContent) -> Result<QueryOptions, HostRejection> {
    let raw = stored_message::<proto::QueryOptions>(payload)
        .ok_or_else(|| internal("query options payload is not a native query options message"))?;
    decode_query_options_at(raw, FieldPath::root("establish").field("query_options"))
        .map_err(|error| protocol(&format!("query options are invalid: {error}")))
}

pub fn credential_material(update: &CredentialUpdate) -> Result<&WireCredential, HostRejection> {
    stored_credential(update.material().as_ref())
        .ok_or_else(|| internal("credential payload is not a decoded credential rotation"))
}

pub fn fragment_plan(plan: &dyn PhysicalFragmentPlan) -> Result<&WireFragmentPlan, HostRejection> {
    plan.stored_representation()
        .and_then(|stored| stored.downcast_ref::<WireFragmentPlan>())
        .ok_or_else(|| internal("task descriptor plan is not a codec-produced fragment plan"))
}

fn internal(detail: &str) -> HostRejection {
    HostRejection::new(TaskFailureCategory::Internal, detail)
}

fn protocol(detail: &str) -> HostRejection {
    HostRejection::new(TaskFailureCategory::Protocol, detail)
}

#[cfg(test)]
mod tests {
    use super::{
        catalog_bindings, credential_material, release_runtime_filter_telemetry,
        runtime_filter_install,
    };
    use novarocks_execution_contract::task_execution::domain::{
        CodecOwnedContent, CredentialEpoch, CredentialLeaseId,
    };
    use novarocks_execution_contract::task_execution::operation::CredentialUpdate;
    use novarocks_proto_codec::FieldPath;
    use novarocks_proto_models::{catalog, filter, novarocks as proto};
    use novarocks_task_codec::domain::{WireContent, WireCredential};
    use novarocks_worker::{ReleasedContextEvidence, RuntimeFilterReleaseObservation};
    use std::sync::Arc;

    fn wire<T: prost::Message + 'static>(
        tag: &'static [u8],
        value: T,
    ) -> Arc<dyn CodecOwnedContent> {
        Arc::new(WireContent::new(tag, value))
    }

    #[test]
    fn foreign_domain_payloads_are_not_reinterpreted() {
        let filter_envelope = wire(b"filter", filter::RuntimeFilterEnvelope::default());
        let contribution = wire(b"contribution", proto::RuntimeFilterContribution::default());
        let catalog_set = wire(b"catalog", catalog::CatalogSet::default());
        assert!(catalog_bindings(filter_envelope.as_ref()).is_err());
        assert!(catalog_bindings(contribution.as_ref()).is_err());
        assert!(runtime_filter_install(catalog_set.as_ref()).is_err());
        assert!(runtime_filter_install(filter_envelope.as_ref()).is_err());
        assert!(runtime_filter_install(contribution.as_ref()).is_ok());
    }

    #[test]
    fn empty_catalog_set_projects_to_no_bindings() {
        let payload = wire(b"catalog", catalog::CatalogSet::default());
        assert!(
            catalog_bindings(payload.as_ref())
                .expect("empty set is legal")
                .is_empty()
        );
    }

    #[test]
    fn credential_projection_accepts_only_codec_owned_confidential_material() {
        let material = WireCredential::decode(&[], &[], FieldPath::root("credential"))
            .expect("empty rotation");
        let update = CredentialUpdate::new(
            CredentialLeaseId::new(1),
            CredentialEpoch::new(1).expect("epoch"),
            Arc::new(material) as Arc<dyn novarocks_execution_contract::ConfidentialContent>,
        );
        assert!(
            credential_material(&update)
                .expect("codec-owned material")
                .is_empty()
        );
        assert!(format!("{update:?}").contains("<redacted>"));
    }

    #[test]
    fn foreign_release_evidence_is_refused_rather_than_encoded_as_absent() {
        let evidence = ReleasedContextEvidence::with_runtime_filter(
            wire(b"catalog", catalog::CatalogSet::default()),
            RuntimeFilterReleaseObservation::Unavailable,
        );
        let rejection = release_runtime_filter_telemetry(&evidence)
            .expect_err("only terminal telemetry may be encoded on release");
        assert_eq!(
            rejection.category(),
            novarocks_execution_contract::task_execution::status::TaskFailureCategory::Internal
        );
    }
}
