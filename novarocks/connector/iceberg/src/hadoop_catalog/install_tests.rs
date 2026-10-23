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

//! The common production installer owns CREATE/UPDATE evidence. These local
//! correctness cases use isolated directories, explicit inputs and a joining
//! catalog runtime bridge. No local synchronous installation runs on a Tokio
//! worker, and every bridge has exited before the directory owner is dropped.
use super::*;
use crate::catalog::publication::CatalogEvidencePolicy;
use novarocks_spi::connector::{ConnectorStopOwner, ExternalMutationEffect};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::time::Instant;

struct InstallFixture {
    catalog: Arc<HadoopFileSystemCatalog>,
    installation: FrozenInstallation,
    binding: IcebergReadBinding,
    evidence: EvidenceContext,
}

fn with_install<R: Send + 'static, F: Future<Output = R> + Send + 'static>(
    test: impl FnOnce(InstallFixture) -> F,
) -> R {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let bridge = crate::resources::IcebergCatalogRuntime::new(runtime.handle().clone());
    let directory = tempfile::tempdir().unwrap();
    let catalog = {
        let _entered = runtime.enter();
        Arc::new(super::super::tests::test_catalog(
            directory.path().to_str().unwrap(),
        ))
    };
    let attempt = catalog
        .prepare_create_attempt(
            &NamespaceIdent::new("db".into()),
            super::super::tests::test_creation("t"),
            "installer-test-operation".into(),
        )
        .unwrap();
    let installation =
        FrozenInstallation::new(attempt.facts.metadata_location, &attempt.metadata).unwrap();
    let preparation = ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(10),
        ConnectorStopOwner::new().view(),
        65536,
        131072,
    )
    .unwrap();
    let evidence = CatalogEvidencePolicy::default()
        .begin_evidence(&preparation)
        .unwrap();
    let binding =
        phase_binding(&original_binding(&catalog, &preparation).unwrap(), evidence).unwrap();
    bridge
        .block_on(test(InstallFixture {
            catalog,
            installation,
            binding,
            evidence,
        }))
        .expect("joining catalog bridge must exit normally")
}

fn seed(fixture: &InstallFixture, bytes: &[u8]) {
    let target = std::path::Path::new(&fixture.installation.location);
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::write(target, bytes).unwrap();
}

fn foreign_bytes(fixture: &InstallFixture, different_uuid: bool) -> Vec<u8> {
    let mut metadata: serde_json::Value =
        serde_json::from_slice(&fixture.installation.bytes).unwrap();
    metadata["properties"]["test.install-owner"] = "another-request".into();
    if different_uuid {
        let foreign = uuid::Uuid::from_bytes([0x6c; 16]);
        assert_ne!(foreign, fixture.installation.uuid);
        metadata["table-uuid"] = foreign.to_string().into();
    }
    let bytes = serde_json::to_vec_pretty(&metadata).unwrap();
    let parsed: TableMetadata =
        serde_json::from_slice(&bytes).expect("independently prepared complete metadata");
    assert_eq!(parsed.uuid() != fixture.installation.uuid, different_uuid);
    assert_ne!(bytes.as_slice(), fixture.installation.bytes.as_ref());
    bytes
}

fn assert_applied(evidence: DispatchEvidence, fixture: &InstallFixture) {
    let DispatchEvidence::Applied { proof } = evidence else {
        panic!("the exact installation must be Applied")
    };
    assert_eq!(proof.effect, ExternalMutationEffect::Applied);
    assert_eq!(
        proof.table_uuid.as_deref(),
        Some(fixture.installation.uuid.to_string().as_str())
    );
    assert_eq!(
        proof.metadata_location.as_deref(),
        Some(fixture.installation.location.as_str())
    );
    let expected_digest = format!("{:x}", Sha256::digest(&fixture.installation.bytes));
    assert_eq!(
        proof.metadata_digest.as_deref(),
        Some(expected_digest.as_str())
    );
    assert_eq!(
        std::fs::read(&fixture.installation.location).unwrap(),
        fixture.installation.bytes
    );
}

#[test]
fn created_and_exact_already_existing_slots_are_applied_for_create_and_update() {
    for existing in [false, true] {
        for (kind, no_op) in [
            (ConnectorMutationFailureKind::AlreadyExists, false),
            (ConnectorMutationFailureKind::AlreadyExists, true),
            (ConnectorMutationFailureKind::Conflict, false),
        ] {
            with_install(move |fixture| async move {
                if existing {
                    seed(&fixture, &fixture.installation.bytes);
                }
                let outcome = fixture
                    .installation
                    .install(
                        &fixture.catalog,
                        &fixture.binding,
                        fixture.evidence,
                        kind,
                        no_op,
                    )
                    .await;
                assert_applied(outcome, &fixture);
            });
        }
    }
}

#[test]
fn complete_foreign_slots_keep_create_policy_and_update_conflict_without_replacement() {
    for different_uuid in [false, true] {
        for (kind, no_op) in [
            (ConnectorMutationFailureKind::AlreadyExists, false),
            (ConnectorMutationFailureKind::AlreadyExists, true),
            (ConnectorMutationFailureKind::Conflict, false),
        ] {
            with_install(move |fixture| async move {
                let foreign = foreign_bytes(&fixture, different_uuid);
                seed(&fixture, &foreign);
                let outcome = fixture
                    .installation
                    .install(
                        &fixture.catalog,
                        &fixture.binding,
                        fixture.evidence,
                        kind,
                        no_op,
                    )
                    .await;
                if no_op {
                    let DispatchEvidence::Applied { proof } = outcome else {
                        panic!("conditional CREATE must be NoOp")
                    };
                    assert_eq!(proof.effect, ExternalMutationEffect::NoOp);
                    assert!(
                        proof.table_uuid.is_none(),
                        "NoOp cannot borrow the attempted CREATE identity"
                    );
                    assert!(proof.metadata_location.is_none());
                    assert!(proof.metadata_digest.is_none());
                } else {
                    let DispatchEvidence::Rejected { failure, .. } = outcome else {
                        panic!("a complete occupied slot must be rejected")
                    };
                    assert_eq!(failure.kind(), kind);
                }
                assert_eq!(
                    std::fs::read(&fixture.installation.location).unwrap(),
                    foreign
                );
            });
        }
    }
}

#[test]
fn already_exists_with_invalid_or_failed_readback_remains_unknown_even_for_noop_create() {
    for invalid in [false, true] {
        for (kind, no_op) in [
            (ConnectorMutationFailureKind::AlreadyExists, false),
            (ConnectorMutationFailureKind::AlreadyExists, true),
            (ConnectorMutationFailureKind::Conflict, false),
        ] {
            with_install(move |fixture| async move {
                if invalid {
                    seed(&fixture, b"not-json");
                } else {
                    seed(&fixture, &fixture.installation.bytes);
                    fixture
                        .catalog
                        .inject_test_fault(HadoopCatalogTestFault::AuthoritativeV1Read);
                }
                let outcome = fixture
                    .installation
                    .install(
                        &fixture.catalog,
                        &fixture.binding,
                        fixture.evidence,
                        kind,
                        no_op,
                    )
                    .await;
                assert!(
                    matches!(outcome, DispatchEvidence::Unknown { .. }),
                    "an unreadable occupied slot proves neither this install nor another complete request"
                );
                let observed = std::fs::read(&fixture.installation.location).unwrap();
                if invalid {
                    assert_eq!(observed, b"not-json");
                } else {
                    assert_eq!(observed, fixture.installation.bytes);
                }
            });
        }
    }
}

#[test]
fn ambiguous_install_errors_only_accept_exact_positive_readback_for_create_and_update() {
    for kind in [
        ConnectorMutationFailureKind::AlreadyExists,
        ConnectorMutationFailureKind::Conflict,
    ] {
        for existing in [false, true] {
            with_install(move |fixture| async move {
                if existing {
                    seed(&fixture, &fixture.installation.bytes);
                }
                fixture.catalog.inject_test_fault(if existing {
                    HadoopCatalogTestFault::AfterExistingConditionalResponseLoss
                } else {
                    HadoopCatalogTestFault::AfterConditionalResponseLoss
                });
                let outcome = fixture
                    .installation
                    .install(
                        &fixture.catalog,
                        &fixture.binding,
                        fixture.evidence,
                        kind,
                        false,
                    )
                    .await;
                assert_applied(outcome, &fixture);
            });
        }
        for invalid in [false, true] {
            with_install(move |fixture| async move {
                let original = if invalid {
                    b"not-json".to_vec()
                } else {
                    foreign_bytes(&fixture, true)
                };
                seed(&fixture, &original);
                fixture.catalog.inject_test_fault(
                    HadoopCatalogTestFault::AfterExistingConditionalResponseLoss,
                );
                let outcome = fixture
                    .installation
                    .install(
                        &fixture.catalog,
                        &fixture.binding,
                        fixture.evidence,
                        kind,
                        kind == ConnectorMutationFailureKind::AlreadyExists,
                    )
                    .await;
                let DispatchEvidence::Unknown { failure, .. } = outcome else {
                    panic!(
                        "foreign/invalid readback cannot reject or NoOp an ambiguous installation"
                    )
                };
                assert!(
                    failure
                        .message()
                        .contains("injected conditional existing response loss"),
                    "retain the install error when readback is invalid or foreign"
                );
                assert_eq!(
                    std::fs::read(&fixture.installation.location).unwrap(),
                    original
                );
            });
        }
    }
}

#[test]
fn ambiguous_install_and_readback_failures_preserve_original_unsupported_reason() {
    use novarocks_failure_contract::FailureProjection;
    use novarocks_spi::connector::ConnectorErrorKind;
    with_install(|fixture| async move {
        fixture
            .catalog
            .inject_test_fault(HadoopCatalogTestFault::AfterConditionalUnsupported);
        fixture
            .catalog
            .inject_test_fault(HadoopCatalogTestFault::AuthoritativeV1Read);
        let outcome = fixture
            .installation
            .install(
                &fixture.catalog,
                &fixture.binding,
                fixture.evidence,
                ConnectorMutationFailureKind::AlreadyExists,
                false,
            )
            .await;
        let DispatchEvidence::Unknown { failure, .. } = outcome else {
            panic!("an unsupported response after install does not prove no effect")
        };
        assert_eq!(failure.kind(), ConnectorMutationFailureKind::Unsupported);
        let original = ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "independent expected storage classification",
        );
        assert_eq!(failure.failure_fact(), original.failure_fact());
        assert!(
            failure
                .message()
                .contains("injected unsupported response after conditional install")
        );
        assert!(failure.connector_source().is_some());
        assert_eq!(
            std::fs::read(&fixture.installation.location).unwrap(),
            fixture.installation.bytes
        );
    });
}

#[test]
fn failure_before_conditional_dispatch_keeps_target_absent() {
    with_install(|fixture| async move {
        fixture
            .catalog
            .inject_test_fault(HadoopCatalogTestFault::BeforeConditionalRequest);
        let outcome = fixture
            .installation
            .install(
                &fixture.catalog,
                &fixture.binding,
                fixture.evidence,
                ConnectorMutationFailureKind::AlreadyExists,
                false,
            )
            .await;
        let DispatchEvidence::NotDispatched { failure } = outcome else {
            panic!("failure before the conditional request must be NotDispatched")
        };
        assert!(
            failure
                .message()
                .contains("injected failure before Hadoop conditional install")
        );
        assert!(!std::path::Path::new(&fixture.installation.location).exists());
    });
}

#[test]
fn same_storage_failure_before_and_after_dispatch_keeps_fact_but_changes_evidence() {
    use novarocks_failure_contract::FailureProjection;
    let expected = ConnectorError::from(FileError::new(FileErrorKind::Unsupported, "expected"));
    for after_dispatch in [false, true] {
        let expected = expected.failure_fact();
        with_install(move |fixture| async move {
            fixture.catalog.inject_test_fault(if after_dispatch {
                HadoopCatalogTestFault::AfterConditionalUnsupported
            } else {
                HadoopCatalogTestFault::BeforeConditionalUnsupported
            });
            if after_dispatch {
                fixture
                    .catalog
                    .inject_test_fault(HadoopCatalogTestFault::AuthoritativeV1Read);
            }
            let outcome = fixture
                .installation
                .install(
                    &fixture.catalog,
                    &fixture.binding,
                    fixture.evidence,
                    ConnectorMutationFailureKind::AlreadyExists,
                    false,
                )
                .await;
            let failure = match outcome {
                DispatchEvidence::Unknown { failure, .. } if after_dispatch => failure,
                DispatchEvidence::NotDispatched { failure } if !after_dispatch => failure,
                other => panic!("unexpected phase evidence: {other:?}"),
            };
            assert_eq!(failure.failure_fact(), expected);
            assert_eq!(
                std::path::Path::new(&fixture.installation.location).exists(),
                after_dispatch
            );
        });
    }
}

#[test]
fn conditional_rejection_with_missing_readback_is_unknown() {
    with_install(|fixture| async move {
        seed(&fixture, &foreign_bytes(&fixture, true));
        fixture
            .catalog
            .inject_test_fault(HadoopCatalogTestFault::BeforeConditionalReadDelete);
        let outcome = fixture
            .installation
            .install(
                &fixture.catalog,
                &fixture.binding,
                fixture.evidence,
                ConnectorMutationFailureKind::AlreadyExists,
                true,
            )
            .await;
        assert!(matches!(outcome, DispatchEvidence::Unknown { .. }));
        assert!(
            !fixture
                .catalog
                .take_test_fault(HadoopCatalogTestFault::BeforeConditionalReadDelete)
        );
        assert!(!std::path::Path::new(&fixture.installation.location).exists());
    });
}

#[test]
fn conditional_rejection_cannot_invent_negative_evidence_after_readback_deadline() {
    with_install(|mut fixture| async move {
        seed(&fixture, &foreign_bytes(&fixture, true));
        let preparation = fixture.binding.request_context().unwrap().clone();
        fixture.evidence = CatalogEvidencePolicy {
            // Permit ordinary operator setup; the exact post-install fault
            // expires this grant only after AlreadyExists has been observed.
            evidence: Duration::from_secs(5),
            ..Default::default()
        }
        .begin_evidence(&preparation)
        .unwrap();
        fixture.binding = phase_binding(&fixture.binding, fixture.evidence).unwrap();
        fixture
            .catalog
            .inject_test_fault(HadoopCatalogTestFault::BeforeConditionalReadEvidenceExpiry);
        let outcome = fixture
            .installation
            .install(
                &fixture.catalog,
                &fixture.binding,
                fixture.evidence,
                ConnectorMutationFailureKind::AlreadyExists,
                true,
            )
            .await;
        assert!(
            !fixture
                .catalog
                .take_test_fault(HadoopCatalogTestFault::BeforeConditionalReadEvidenceExpiry),
            "the exact post-install readback fault must be reached: {outcome:?}"
        );
        assert!(
            matches!(&outcome, DispatchEvidence::Unknown { failure, .. } if failure.kind() == ConnectorMutationFailureKind::DeadlineExceeded),
            "expired readback cannot establish NoOp or a definite rejection: {outcome:?}"
        );
    });
}
