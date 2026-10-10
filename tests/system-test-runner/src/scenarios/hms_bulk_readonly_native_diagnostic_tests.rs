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

use super::*;
use std::sync::Arc;

fn bounded_err(message: &str) -> BoundedCommandError {
    let mut raw = b"\xff\x51\x04#HY000".to_vec();
    raw.extend_from_slice(message.as_bytes());
    BoundedCommandError {
        error: BoundedMysqlError {
            sequence: 1,
            payload_bytes: raw.len(),
            code: 1105,
            sqlstate: "HY000".into(),
            message: message.into(),
            payload_hex: raw_hex(&raw),
        },
        original_payload: raw,
    }
}
fn control_error(stage: InitialStage, reply: BoundedCommandError) -> anyhow::Error {
    staged_error(
        stage,
        require_ok_reply(BoundedCommandResponse::Error(reply)).unwrap_err(),
    )
}

#[test]
fn role_overlays_are_exact_current_credentials_with_refs_only() {
    let mut launch = ScenarioLaunchConfig::default();
    install_credential_overlays(&mut launch).unwrap();
    let common = concat!(
        "name = \"iceberg-test-data\"\n",
        "generation = \"v1\"\n",
        "kind = \"s3\"\n",
        "access_key_id = \"${ENV:AWS_S3_ACCESS_KEY_ID}\"\n",
        "access_key_secret = \"${ENV:AWS_S3_SECRET_ACCESS_KEY}\"\n"
    );
    assert_eq!(
        launch.config_overlay.fe.as_deref(),
        Some(
            format!("[[connector.credentials]]\npurpose = \"object-store-metadata\"\n{common}")
                .as_str()
        )
    );
    assert_eq!(
        launch.config_overlay.be.as_deref(),
        Some(
            format!("[[connector.credentials]]\npurpose = \"object-store-data\"\n{common}")
                .as_str()
        )
    );
    assert!(launch.child_environment.fe.is_empty());
    assert!(launch.child_environment.be.is_empty());
    assert!(launch.config_overlay.be_by_index.is_empty());
    assert!(install_credential_overlays(&mut launch).is_err());
    assert!(credential_overlay("object-store").is_err());
}

#[test]
fn existing_role_overlay_is_not_silently_overwritten() {
    let mut launch = ScenarioLaunchConfig::default();
    launch.config_overlay.fe = Some("original-overlay".into());
    assert!(install_credential_overlays(&mut launch).is_err());
    assert_eq!(
        launch.config_overlay.fe.as_deref(),
        Some("original-overlay")
    );
    assert!(launch.config_overlay.be.is_none());
}

#[test]
fn whole_original_err_allocation_and_exact_create_or_use_stage_survive() {
    for (stage, expected) in [
        (InitialStage::CatalogCreate, "catalog-create"),
        (InitialStage::CatalogUse, "catalog-use"),
    ] {
        let reply = bounded_err("private-canary-message");
        let pointer = reply.original_payload.as_ptr();
        let error = control_error(stage, reply);
        let staged = error.downcast_ref::<StageFailure>().unwrap();
        let retained = staged
            .cause
            .downcast_ref::<UnexpectedControlReply>()
            .unwrap();
        assert_eq!(retained.0.original_payload.as_ptr(), pointer);
        let dto = failure_diagnostic(&error).unwrap();
        assert_eq!(dto.stage, Some(expected));
        assert_eq!(dto.class, "complete-bounded-mysql-ERR");
        let err = dto.mysql_err.unwrap();
        assert_eq!(
            (err.sequence, err.code, err.sqlstate.as_str()),
            (1, 1105, "HY000")
        );
        assert_eq!(
            err.raw_payload_hex,
            raw_hex(b"\xff\x51\x04#HY000private-canary-message")
        );
        assert_eq!(
            err.raw_payload_sha256,
            sha(b"\xff\x51\x04#HY000private-canary-message")
        );
        assert_eq!(err.message, "private-canary-message");
        assert!(!format!("{error:?} {error}").contains("private-canary"));
    }
}

#[test]
fn err_dto_refuses_mismatched_raw_facts_and_oversize() {
    for field in 0..6 {
        let mut reply = bounded_err("message");
        match field {
            0 => reply.error.sequence = 2,
            1 => reply.error.code = 1317,
            2 => reply.error.sqlstate = "70100".into(),
            3 => reply.error.message = "invented".into(),
            4 => reply.error.payload_bytes += 1,
            _ => reply.error.payload_hex = "00".into(),
        }
        assert!(failure_diagnostic(&control_error(InitialStage::CatalogUse, reply)).is_err());
    }
    let reply = bounded_err(&"x".repeat(4096));
    assert!(failure_diagnostic(&control_error(InitialStage::CatalogCreate, reply)).is_err());
}

struct Canary(Arc<()>);
impl fmt::Debug for Canary {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("do-not-format-original-Debug")
    }
}
impl fmt::Display for Canary {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        panic!("do-not-format-original-Display")
    }
}
impl std::error::Error for Canary {}

#[test]
fn primary_then_diagnostic_and_evidence_cleanup_keep_original_objects() {
    let reply = bounded_err("private-message");
    let pointer = reply.original_payload.as_ptr();
    let diagnostic_cause = Arc::new(());
    let evidence_cause = Arc::new(());
    let failure = finish_evidence_errors(
        Some(control_error(InitialStage::CatalogUse, reply)),
        Some(Err(anyhow::Error::new(Canary(diagnostic_cause.clone())))),
        Err(anyhow::Error::new(Canary(evidence_cause.clone()))),
    )
    .unwrap_err();
    let retained = failure.downcast_ref::<Failure>().unwrap();
    assert_eq!(retained.rest.len(), 2);
    assert!(Arc::ptr_eq(
        &retained.rest[0].downcast_ref::<Canary>().unwrap().0,
        &diagnostic_cause
    ));
    assert!(Arc::ptr_eq(
        &retained.rest[1].downcast_ref::<Canary>().unwrap().0,
        &evidence_cause
    ));
    let primary = retained.first.downcast_ref::<StageFailure>().unwrap();
    assert_eq!(
        primary
            .cause
            .downcast_ref::<UnexpectedControlReply>()
            .unwrap()
            .0
            .original_payload
            .as_ptr(),
        pointer
    );
    let dto = failure_diagnostic(&failure).unwrap();
    assert_eq!(dto.stage, Some("catalog-use"));
    assert_eq!(dto.secondary_sources_retained, 2);
    assert!(!format!("{failure:?} {failure}").contains("private-message"));
}

#[test]
fn before_metrics_and_sampler_stage_errors_do_not_invent_a_sql_err() {
    for stage in [
        InitialStage::AllocatorBefore,
        InitialStage::SamplerSpawn,
        InitialStage::SamplerJoin,
        InitialStage::AllocatorAfter,
    ] {
        let marker = Arc::new(());
        let error: anyhow::Error =
            at_stage::<()>(stage, || Err(anyhow::Error::new(Canary(marker.clone())))).unwrap_err();
        let actual = error.downcast_ref::<StageFailure>().unwrap();
        assert!(Arc::ptr_eq(
            &actual.cause.downcast_ref::<Canary>().unwrap().0,
            &marker
        ));
        let dto = failure_diagnostic(&error).unwrap();
        assert_eq!(dto.stage, Some(stage.label()));
        assert!(dto.mysql_err.is_none());
        assert_eq!(dto.class, "opaque-original-source-retained");
        let _ = serde_json::to_vec(&dto).unwrap();
    }
}

#[test]
fn partial_header_is_not_a_complete_err_and_retains_original_io() {
    let error = anyhow::Error::new(CommandResponseFailure {
        header: [12, 0, 0, 1],
        header_received: 2,
        expected_payload_bytes: None,
        payload_prefix: Vec::new(),
        actual_cause: std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "raw-private-canary")
            .into(),
    });
    let error = staged_error(InitialStage::CatalogCreate, error);
    let dto = failure_diagnostic(&error).unwrap();
    assert_eq!(dto.class, "partial-original-mysql-response");
    assert!(dto.mysql_err.is_none());
    let partial = dto.mysql_partial.unwrap();
    assert_eq!(partial.header_received, 2);
    assert_eq!(partial.payload_prefix_sha256, sha(b""));
    let actual = error
        .downcast_ref::<StageFailure>()
        .unwrap()
        .cause
        .downcast_ref::<CommandResponseFailure>()
        .unwrap();
    assert_eq!(
        actual
            .actual_cause
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    assert!(!format!("{error:?} {error}").contains("raw-private-canary"));
}

#[test]
fn panic_payload_is_retained_without_formatter_or_false_phase_detail() {
    let payload = Arc::new(());
    let error = staged_error(
        InitialStage::MeasuredOperation,
        anyhow::Error::new(Panic(Mutex::new(Box::new(payload.clone())))),
    );
    let dto = failure_diagnostic(&error).unwrap();
    assert_eq!(dto.class, "original-panic-payload-retained");
    assert_eq!(dto.stage, Some("measured-operation"));
    let original = error
        .downcast_ref::<StageFailure>()
        .unwrap()
        .cause
        .downcast_ref::<Panic>()
        .unwrap();
    assert!(Arc::ptr_eq(
        original
            .0
            .lock()
            .unwrap()
            .downcast_ref::<Arc<()>>()
            .unwrap(),
        &payload
    ));
}

#[cfg(unix)]
#[test]
fn bounded_original_err_private_file_is_0600_and_collision_keeps_primary() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let error = control_error(
        InitialStage::CatalogCreate,
        bounded_err("private-file-message"),
    );
    let receipt = save_failure_diagnostic(root.path(), &error).unwrap();
    let path = root.path().join("hms-bulk-original-failure.json");
    let raw = fs::read(&path).unwrap();
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(receipt["sha256"], sha(&raw));
    assert!(raw.len() <= FILE_CAP);
    let dto: Value = serde_json::from_slice(&raw).unwrap();
    assert_eq!(dto["mysql_err"]["message"], "private-file-message");
    let save_error = save_failure_diagnostic(root.path(), &error).unwrap_err();
    let original = finish_evidence_errors(Some(error), Some(Err(save_error)), Ok(())).unwrap_err();
    let retained = original.downcast_ref::<Failure>().unwrap();
    assert_eq!(
        retained.rest[0]
            .downcast_ref::<std::io::Error>()
            .unwrap()
            .kind(),
        std::io::ErrorKind::AlreadyExists
    );
    assert_eq!(
        failure_diagnostic(&original).unwrap().stage,
        Some("catalog-create")
    );
    assert_eq!(fs::read(path).unwrap(), raw);
}

#[test]
fn expired_original_clock_is_stage_failure_without_a_refreshed_deadline() {
    let deadline = Instant::now() - Duration::from_millis(1);
    let error = at_stage(InitialStage::PhaseClock, || remaining(deadline)).unwrap_err();
    assert_eq!(
        failure_diagnostic(&error).unwrap().stage,
        Some("phase-clock")
    );
    assert!(deadline < Instant::now());
}

#[test]
fn shared_register_preserves_exact_bulk_sql_and_role_references() {
    let input = HmsRegistration {
        catalog: CATALOG,
        namespace: "cl_ns_0000",
        hms_uri: "thrift://127.0.0.1:28001",
        warehouse: "s3://warehouse/private",
        object_store_endpoint: "http://127.0.0.1:28000",
    };
    let sql = catalog_sql(&input);
    assert!(sql.starts_with("CREATE EXTERNAL CATALOG m07_hms_readonly_cl PROPERTIES ("));
    for reference in [
        "credential.object-store-metadata.consumer-role\"=\"frontend",
        "credential.object-store-data.consumer-role\"=\"backend",
    ] {
        assert!(sql.contains(reference));
    }
    assert!(!sql.contains("access_key_id"));
    assert!(!sql.contains("access_key_secret"));
    let small = HmsRegistration {
        catalog: "m07_small",
        namespace: "cap_ns",
        ..input
    };
    assert_eq!(catalog_sql(&small), sql.replacen(CATALOG, "m07_small", 1));
}

#[test]
fn shared_registration_err_retains_original_allocation_for_stage_diagnostic() {
    let reply = bounded_err("original-private-register-error");
    let pointer = reply.original_payload.as_ptr();
    let raw = reply.original_payload.clone();
    let failure = staged_error(
        InitialStage::CatalogCreate,
        registration_ok_reply(BoundedCommandResponse::Error(reply)).unwrap_err(),
    );
    let staged = failure.downcast_ref::<StageFailure>().unwrap();
    let retained = staged
        .cause
        .downcast_ref::<UnexpectedControlReply>()
        .unwrap();
    assert_eq!(retained.0.original_payload.as_ptr(), pointer);
    let dto = failure_diagnostic(&failure).unwrap();
    assert_eq!(dto.stage, Some("catalog-create"));
    assert_eq!(dto.mysql_err.unwrap().raw_payload_hex, raw_hex(&raw));
}

#[test]
fn registration_success_receipt_projects_actual_complete_ok() {
    use crate::actors::mysql_stream::BoundedCommandOk;
    let raw = vec![0, 0, 0, 2, 0, 0, 0];
    let observation = registration_ok_reply(BoundedCommandResponse::Ok(BoundedCommandOk {
        affected_rows: 0,
        last_insert_id: 0,
        status_flags: 2,
        warnings: 0,
        info: String::new(),
        original_payload: raw.clone(),
    }))
    .unwrap();
    assert_eq!(observation["verdict"], "complete-bounded-mysql-OK");
    assert_eq!(observation["payload_bytes"], 7);
    assert_eq!(observation["payload_sha256"], sha(&raw));
    assert_eq!(observation["status_flags"], 2);
    assert!(observation.get("info").is_none());
}
