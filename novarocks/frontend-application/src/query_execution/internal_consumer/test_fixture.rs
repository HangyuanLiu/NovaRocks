// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Closed validated fixtures for actual fixed-consumer component tests.
use crate::query_execution::lifecycle_plan::{
    AttemptCredentialLeaseCollector, AttemptCredentialStorage,
};
use novarocks_coordination_contract::{ExecutionEffect, RecoveryMode};
use novarocks_query_application::api::QueryExecutionKind;
use novarocks_query_application::preparation::{
    CompletedPhysicalPlanCandidate, ExecutionResourceRequirements, FrozenCostEstimate,
    FrozenEstimateUnknownReason, FrozenExecutionDescription, OutputContract,
};
use novarocks_secret::SecretValue;
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
    CatalogProperties, CatalogVersion, ConnectorInstanceId, ConnectorProviderId,
    ConnectorVendedCredentialLeaseSink, CredentialConsumerRole, StorageCredentialScopePrefix,
    VendedS3CredentialLeaseContribution, VendedS3CredentialLeaseEntry,
};
use novarocks_types::QueryExecutionId;
use std::sync::Arc;
pub(crate) fn internal_count_description() -> (
    FrozenExecutionDescription,
    novarocks_query_application::api::PlanSeal,
) {
    use novarocks_physical_plan::{
        FragmentBuilder, FragmentId, FragmentSink, OutputPort, PipelineDopDomain, PlanBuilder,
        PlanVersionId, ResultPort,
    };
    use novarocks_result_contract::{FrozenRootOutput, RootOutputContract, RootProfileId};
    // Diagnostics are carrier-independent. A validated zero-row CountOnly
    // program keeps this test focused on the real actor End/Ready boundary.
    let fragment_id = FragmentId::new(0);
    let mut fragment = FragmentBuilder::new(fragment_id);
    let node = fragment.reserve_node_id().unwrap();
    fragment
        .add_values(node, Box::default(), Box::default())
        .unwrap();
    let fragment = fragment
        .finish_definition(
            node,
            FragmentSink::RootResult(Box::new(RootOutputContract::new(
                RootProfileId::V1,
                FrozenRootOutput::CountOnly,
            ))),
            PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: true,
            },
        )
        .unwrap();
    let mut plan = PlanBuilder::new(PlanVersionId::try_new([151; 16]).unwrap());
    plan.add_fragment(fragment).unwrap();
    plan.set_result_port(ResultPort {
        scalar_schema: None,
        fragment: fragment_id,
        output: OutputPort {
            node,
            columns: Box::default(),
        },
        fields: Box::default(),
    })
    .unwrap();
    let candidate = CompletedPhysicalPlanCandidate::for_program(
        plan.finish().unwrap(),
        &novarocks_sql::compiler::SqlCompileControl::unbounded(),
    )
    .unwrap();
    let output =
        OutputContract::from_completed_candidate(QueryExecutionKind::Statistics, &candidate)
            .unwrap();
    let description = FrozenExecutionDescription::for_completed_plan(
        QueryExecutionKind::Statistics,
        candidate,
        Vec::new(),
        output,
        ExecutionEffect::None,
        RecoveryMode::NoRecovery,
        Vec::new(),
        FrozenCostEstimate::unknown(FrozenEstimateUnknownReason::NotProjected),
        ExecutionResourceRequirements::unknown(FrozenEstimateUnknownReason::NotProjected),
    )
    .unwrap()
    .into_internal_computation()
    .unwrap();
    let plan = novarocks_query_application::api::PlanSeal::Version(
        PlanVersionId::try_new([151; 16]).unwrap(),
    );
    assert!(description.matches_plan_seal(plan));
    (description, plan)
}

/// One actual table via the original lifecycle author, never a re-created
/// terminal resolver. Tests compare Arc identity and print no secret material.
pub(crate) fn original_credentials(execution: QueryExecutionId) -> Arc<AttemptCredentialStorage> {
    let collector = AttemptCredentialLeaseCollector::new(execution);
    let properties = CatalogProperties::new(
        CatalogHandle::new(
            ConnectorInstanceId::try_from_canonical("catalog.handoff").unwrap(),
            CatalogVersion::from_bytes([0x24; 32]),
        ),
        ConnectorProviderId::parse("iceberg").unwrap(),
        1,
        vec![],
        vec![
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreData,
                CredentialConsumerRole::Backend,
                CatalogCredentialMode::Vended,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let contribution = VendedS3CredentialLeaseContribution::try_new(
        vec![
            VendedS3CredentialLeaseEntry::try_new(
                StorageCredentialScopePrefix::try_from_normalized("s3://warehouse/data").unwrap(),
                u64::MAX,
                SecretValue::new("handoff-test-access".to_owned()),
                SecretValue::new("handoff-test-secret".to_owned()),
                SecretValue::new("handoff-test-token".to_owned()),
            )
            .unwrap(),
        ],
        None,
    )
    .unwrap();
    collector
        .offer_vended_s3_credential_lease(&properties, contribution)
        .unwrap();
    collector
        .into_credential_leases()
        .unwrap()
        .into_attempt_storage_resolver()
        .expect("original vended table")
}
