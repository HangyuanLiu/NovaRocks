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

use novarocks_spi::connector::{
    ConnectorDocumentVersion, ConnectorSnapshotRecord, KeyedPublication, KeyedPublicationIntent,
};

struct PreparedKeyed {
    declaration: ConnectorDocumentPublicationDeclaration,
    intent: KeyedPublicationIntent,
    base_snapshot_id: Option<i64>,
    records: BTreeMap<String, String>,
    expected_properties: HashMap<String, String>,
}

async fn keyed_fixture() -> Fixture {
    let mut fixture = fixture().await;
    // The shared legacy fixture deliberately has three old management keys.
    // Remove them through the actual catalog before testing the new format;
    // otherwise LegacyFormat would reject the first publication, correctly.
    fixture.table = fixture
        .catalog
        .update_table(
            crate::iceberg::TableCommit::builder()
                .ident(fixture.table.identifier().clone())
                .requirements(vec![TableRequirement::UuidMatch {
                    uuid: fixture.table.metadata().uuid(),
                }])
                .updates(vec![TableUpdate::RemoveProperties {
                    removals: vec![
                        "novarocks.managed.kind".into(),
                        "novarocks.managed.owner".into(),
                        "novarocks.managed.incarnation".into(),
                    ],
                }])
                .build(),
        )
        .await
        .unwrap();
    fixture.catalog.clear();
    fixture
        .provider
        .runtime()
        .control_state()
        .invalidate_table_cache("db", "t");
    fixture
}

async fn prepare_keyed(fixture: &Fixture, payload: &'static [u8]) -> PreparedKeyed {
    let loaded = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await
        .unwrap();
    let metadata = loaded.metadata();
    let uuid = metadata.uuid().to_string();
    let base_snapshot_id = metadata.current_snapshot_id();
    let object = ConnectorTableObjectId::try_new(Bytes::copy_from_slice(uuid.as_bytes())).unwrap();
    let publication_id = LakePublicationId::new_v7();
    let admission = document_storage_lease(fixture)
        .admit_management(
            ConnectorDocumentManagementAdmissionRequest::try_new(
                ConnectorProviderBindingKey {
                    instance_id: fixture.provider.descriptor().instance_id.clone(),
                    incarnation: fixture.provider.incarnation(),
                },
                fixture.catalog_handle.clone(),
                ConnectorMutationOperationId::from_bytes(publication_id.to_bytes()),
                ConnectorTableIdentity {
                    instance_id: fixture.provider.descriptor().instance_id.clone(),
                    namespace: "db".into(),
                    table: "t".into(),
                },
                Some(object.clone()),
                ConnectorDocumentManagementOperation::Publication,
                context(),
            )
            .unwrap(),
        )
        .unwrap();
    let base = ConnectorWriteBaseVersion::try_new(Bytes::from(format!(
        "iceberg/write-base/v1/{uuid}/main/{}",
        crate::commit::write_shared::snapshot_token(base_snapshot_id)
    )))
    .unwrap();
    let declaration = ConnectorDocumentPublicationDeclaration::try_new(
        publication_id,
        admission,
        object,
        base,
        ConnectorManagedPublicationTechnique::Full,
        ConnectorManagedPublicationEmptyInputDisposition::CommitEmptyWrite,
        None,
        None,
    )
    .unwrap();
    // Provider-neutral opaque payloads. The provider owns framing/closure, not
    // the MV-specific interpretation of these D/L/B-shaped facts.
    let make = |name, version, refs, content: &'static [u8]| {
        ConnectorDocumentVersion::try_new(
            "novarocks.test",
            name,
            ConnectorDocumentFormat::try_new("novarocks.test", name, version).unwrap(),
            refs,
            Bytes::from_static(content),
        )
        .unwrap()
    };
    let d = make("definition", 1, vec![], b"definition");
    let l = make(
        "interpretation",
        2,
        vec![("definition".into(), d.identity())],
        b"layout",
    );
    let b = make(
        "binding",
        1,
        vec![
            ("definition".into(), d.identity()),
            ("interpretation".into(), l.identity()),
        ],
        b"binding",
    );
    let encoded_payload = match payload {
        b"result" => "cmVzdWx0",
        b"next" => "bmV4dA",
        _ => panic!("add a literal expected payload, not an encoder oracle"),
    };
    let records = BTreeMap::from([(
        "novarocks.doc.v2.record.novarocks.test~publication".into(),
        format!("2.{}.{encoded_payload}", b.identity().to_hex()),
    )]);
    // Independent literal JSON field order and base64url payloads. These
    // expectations do not call the product envelope encoder or property parser.
    let version_json = |name: &str, version: u32, refs: &str, content: &str, id: String| {
        format!(
            r#"{{"v":2,"owner":"novarocks.test","name":"{name}","format":{{"owner":"novarocks.test","name":"{name}","version":{version}}},"refs":{refs},"content":"{content}","id":"{id}"}}"#
        )
    };
    let d_id = d.identity().to_hex();
    let l_id = l.identity().to_hex();
    let b_id = b.identity().to_hex();
    let expected_properties = HashMap::from([
        (
            format!("novarocks.doc.v2.rev.{d_id}"),
            version_json("definition", 1, "[]", "ZGVmaW5pdGlvbg", d_id.clone()),
        ),
        (
            format!("novarocks.doc.v2.rev.{l_id}"),
            version_json(
                "interpretation",
                2,
                &format!(r#"[{{"rel":"definition","id":"{d_id}"}}]"#),
                "bGF5b3V0",
                l_id.clone(),
            ),
        ),
        (
            format!("novarocks.doc.v2.rev.{b_id}"),
            version_json(
                "binding",
                1,
                &format!(
                    r#"[{{"rel":"definition","id":"{d_id}"}},{{"rel":"interpretation","id":"{l_id}"}}]"#
                ),
                "YmluZGluZw",
                b_id.clone(),
            ),
        ),
        ("novarocks.doc.v2.root.novarocks.test".into(), b_id),
    ]);
    let documents = KeyedPublication {
        records: vec![ConnectorSnapshotRecord {
            owner: "novarocks.test".into(),
            name: "publication".into(),
            binding: Some(b.identity()),
            payload: Bytes::from_static(payload),
        }],
        versions: vec![d, l, b.clone()],
        roots: BTreeMap::from([("novarocks.test".into(), b.identity())]),
        markers_set: BTreeMap::new(),
        markers_remove: Default::default(),
    };
    PreparedKeyed {
        intent: KeyedPublicationIntent {
            declaration: declaration.clone(),
            documents,
        },
        declaration,
        base_snapshot_id,
        records,
        expected_properties,
    }
}

fn keyed_begin(prepared: &PreparedKeyed) -> ConnectorWriteBeginRequest {
    ConnectorWriteBeginRequest {
        table: Arc::from("db.t"),
        target_ref: ConnectorWriteTargetRef::main(),
        intent: ConnectorWriteIntent::Overwrite,
        purpose: ConnectorWriteAdmissionPurpose::MaterializedViewRefresh,
        input: data_input(),
        base: Some(prepared.declaration.expected_base().clone()),
        flavor: ConnectorWriteSessionFlavor::ApplicationDocumentPublication {
            declaration: prepared.declaration.clone(),
            shape: ConnectorManagedPublicationShape::Data,
        },
        context: context(),
    }
}

fn empty_keyed_finish<'a>(
    plan: &'a novarocks_spi::connector::write_stack::session::ConnectorWriteSessionPlan,
    prepared: &PreparedKeyed,
) -> ConnectorWriteFinishRequest<'a> {
    ConnectorWriteFinishRequest {
        commit: plan.commit_handle(),
        prepared: ConnectorPreparedWriteSet::try_new(0, vec![], &plan.expected_targets()).unwrap(),
        statistics: vec![],
        publication: ConnectorWriteFinishPublication::KeyedDocuments(prepared.intent.clone()),
        context: context(),
    }
}

fn expected_record_digest(records: &BTreeMap<String, String>) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hash = Sha256::new();
    for (key, value) in records {
        for bytes in [key.as_bytes(), value.as_bytes()] {
            hash.update((bytes.len() as u32).to_be_bytes());
            hash.update(bytes);
        }
    }
    hash.finalize().into()
}

fn assert_keyed_evidence(
    fixture: &Fixture,
    plan: &novarocks_spi::connector::write_stack::session::ConnectorWriteSessionPlan,
    prepared: &PreparedKeyed,
) -> serde_json::Value {
    let evidence = iceberg_handle(fixture, plan).recovery_evidence().unwrap();
    assert_eq!(evidence.schema_version(), 4);
    let payload =
        crate::commit::write_stack::control::decode_write_recovery_facts_for_test(&evidence)
            .unwrap();
    assert_eq!(payload["version"], serde_json::json!(4));
    assert!(payload["document_manifest_digest"].is_null());
    assert_eq!(
        payload["document_record_digest"],
        serde_json::json!(expected_record_digest(&prepared.records))
    );
    payload
}

fn actual_snapshot_updates(fixture: &Fixture) -> Vec<crate::iceberg::spec::Snapshot> {
    fixture
        .catalog
        .raw_updates()
        .iter()
        .flatten()
        .filter_map(|update| match update {
            TableUpdate::AddSnapshot { snapshot } => Some(snapshot.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn real_keyed_empty_full_begin_finish_reasserts_closure_with_v4_evidence() {
    let fixture = keyed_fixture().await;
    // Both no-parent and existing-empty-parent Full publications have no file
    // delta and must append. Logical rows are not used to choose the operation.
    for expected_parent in [None, Some(())] {
        let prepared = prepare_keyed(
            &fixture,
            if expected_parent.is_none() {
                b"result"
            } else {
                b"next"
            },
        )
        .await;
        assert_eq!(
            prepared.base_snapshot_id.is_some(),
            expected_parent.is_some()
        );
        fixture.catalog.clear();
        fixture.load_modes.lock().unwrap().clear(); // passive owner trace extension
        let control = write_control(&fixture);
        let plan = control.begin_write(keyed_begin(&prepared)).unwrap();
        let result = control
            .finish_write(empty_keyed_finish(&plan, &prepared))
            .unwrap();
        let ExternalMutationOutcome::KnownCommitted {
            receipt,
            finalization: ExternalMutationFinalization::Complete,
            ..
        } = result
        else {
            panic!("keyed finish must commit")
        };
        let id = receipt
            .committed_version()
            .and_then(|v| v.snapshot_id())
            .unwrap();
        let snapshots = actual_snapshot_updates(&fixture);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].snapshot_id(), id);
        assert_eq!(snapshots[0].parent_snapshot_id(), prepared.base_snapshot_id);
        assert_eq!(
            snapshots[0].summary().operation,
            crate::iceberg::spec::Operation::Append
        );
        for (key, value) in &prepared.records {
            assert_eq!(
                snapshots[0].summary().additional_properties.get(key),
                Some(value)
            );
        }
        let commits = fixture.catalog.commits();
        assert_eq!(commits.len(), 1);
        assert_eq!(
            commits[0].uuid_requirements,
            vec![fixture.table.metadata().uuid()]
        );
        assert_eq!(
            commits[0].ref_requirements,
            vec![("main".into(), prepared.base_snapshot_id)]
        );
        assert_eq!(commits[0].ref_updates, vec![("main".into(), id)]);
        let updates = fixture.catalog.raw_updates();
        let properties: HashMap<_, _> = updates
            .iter()
            .flatten()
            .filter_map(|u| match u {
                TableUpdate::SetProperties { updates } => Some(updates),
                _ => None,
            })
            .flat_map(|p| p.iter())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(properties, prepared.expected_properties);
        assert!(
            snapshots[0]
                .summary()
                .additional_properties
                .keys()
                .all(|k| !k.starts_with("novarocks.managed.") && k != "novarocks.documents.v1")
        );
        let payload = assert_keyed_evidence(&fixture, &plan, &prepared);
        assert_eq!(payload["attempt_owned"].as_array().unwrap().len(), 1);
        assert_eq!(payload["published_snapshot_id"], serde_json::json!(id));
        let modes = fixture.load_modes.lock().unwrap();
        assert!(!modes.is_empty());
        assert!(
            modes
                .iter()
                .all(|m| *m == crate::catalog::SnapshotLoading::Refs)
        );
    }
}

#[tokio::test]
async fn keyed_full_zero_output_that_removes_live_files_is_delete() {
    let fixture = keyed_fixture().await;
    append_snapshot_for_test(
        Arc::clone(fixture.provider.runtime()),
        fixture.table.clone(),
        vec![written_file(&fixture.table, "keyed-old-data")],
        "main".into(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    let prepared = prepare_keyed(&fixture, b"result").await;
    fixture.catalog.clear();
    let control = write_control(&fixture);
    let plan = control.begin_write(keyed_begin(&prepared)).unwrap();
    assert!(matches!(
        control
            .finish_write(empty_keyed_finish(&plan, &prepared))
            .unwrap(),
        ExternalMutationOutcome::KnownCommitted { .. }
    ));
    assert_eq!(
        actual_snapshot_updates(&fixture)[0].summary().operation,
        crate::iceberg::spec::Operation::Delete
    );
    let committed = fixture
        .catalog
        .load_table(fixture.table.identifier())
        .await
        .unwrap();
    assert!(live_data_paths(&fixture, &committed).await.is_empty());
    assert_eq!(fixture.catalog.commits().len(), 1);
}

// A real local Parquet object represented by the existing neutral fragment API,
// then finished by the real control. Reuses the cross-cutting write-stack test
// constructors rather than inventing writer/loader protocol state.
fn keyed_data_finish<'a>(
    fixture: &Fixture,
    plan: &'a novarocks_spi::connector::write_stack::session::ConnectorWriteSessionPlan,
    prepared: &PreparedKeyed,
) -> (ConnectorWriteFinishRequest<'a>, String) {
    use crate::commit::write_stack::domain::{IcebergCommitFragment, IcebergDataFileArtifact};
    use crate::commit::write_stack::test_support::{sample_metrics, sample_partition};
    let path = format!(
        "{}/keyed-data.parquet",
        iceberg_handle(fixture, plan)
            .staging_dir()
            .trim_end_matches('/')
    );
    let local = std::path::Path::new(path.strip_prefix("file://").unwrap());
    std::fs::create_dir_all(local.parent().unwrap()).unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "id",
        DataType::Int64,
        false,
    )]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(arrow::array::Int64Array::from(vec![7]))],
    )
    .unwrap();
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(std::fs::File::create(local).unwrap(), schema, None)
            .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let adapter = crate::commit::write_stack::runtime::build_write_adapter(
        fixture.provider.descriptor().clone(),
        fixture.catalog_handle.clone(),
    );
    let fragment = adapter.wrap_commit_fragment(IcebergCommitFragment::data_file(
        IcebergDataFileArtifact::try_new(
            path.clone(),
            crate::delete_file::IcebergFileFormat::Parquet,
            sample_partition(),
            sample_metrics(1, std::fs::metadata(local).unwrap().len()),
            None,
        )
        .unwrap(),
    ));
    let expected = plan.expected_targets();
    assert_eq!(expected.len(), 1);
    (
        ConnectorWriteFinishRequest {
            commit: plan.commit_handle(),
            prepared: ConnectorPreparedWriteSet::try_new(
                1,
                vec![(expected[0], fragment)],
                &expected,
            )
            .unwrap(),
            statistics: vec![],
            publication: ConnectorWriteFinishPublication::KeyedDocuments(prepared.intent.clone()),
            context: context(),
        },
        path,
    )
}

#[tokio::test]
async fn keyed_unknown_retains_session_data_and_manifest_without_redispatch() {
    let fixture = keyed_fixture().await;
    let prepared = prepare_keyed(&fixture, b"result").await;
    let control = write_control(&fixture);
    let plan = control.begin_write(keyed_begin(&prepared)).unwrap();
    let (request, data) = keyed_data_finish(&fixture, &plan, &prepared);
    fixture.catalog.clear();
    fixture
        .catalog
        .fail_next(crate::iceberg::ErrorKind::Unexpected);
    let ExternalMutationOutcome::CommitUnknown { evidence, .. } =
        control.finish_write(request).unwrap()
    else {
        panic!("ambiguous dispatch must stay unknown")
    };
    let payload = assert_keyed_evidence(&fixture, &plan, &prepared);
    let paths: Vec<_> = payload["artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["path"].as_str().unwrap().to_string())
        .collect();
    assert!(paths.contains(&data));
    assert!(paths.len() > 1);
    let session_indices = payload["session_references"].as_array().unwrap();
    assert_eq!(session_indices.len(), 1);
    let data_index = session_indices[0].as_u64().unwrap() as usize;
    assert_eq!(
        payload["artifacts"][data_index]["path"],
        serde_json::json!(data)
    );
    assert_eq!(
        payload["artifacts"][data_index]["class"],
        serde_json::json!("session_data")
    );
    for path in &paths {
        assert!(std::path::Path::new(path.strip_prefix("file://").unwrap()).exists());
    }
    let lists = actual_snapshot_updates(&fixture);
    assert_eq!(lists.len(), 1);
    assert!(paths.contains(&lists[0].manifest_list().to_string()));
    assert_eq!(fixture.catalog.commits().len(), 1);
    assert!(matches!(
        control
            .reconcile_write(ConnectorWriteSessionReconcileRequest {
                commit: plan.commit_handle(),
                evidence,
                context: context()
            })
            .unwrap(),
        ExternalMutationOutcome::CommitUnknown { .. }
    ));
    assert_eq!(fixture.catalog.commits().len(), 1);
    for path in &paths {
        assert!(std::path::Path::new(path.strip_prefix("file://").unwrap()).exists());
    }
}

#[tokio::test]
async fn keyed_conflict_cleans_owned_files_without_another_prepare() {
    let fixture = keyed_fixture().await;
    let prepared = prepare_keyed(&fixture, b"result").await;
    let control = write_control(&fixture);
    let plan = control.begin_write(keyed_begin(&prepared)).unwrap();
    let (request, data) = keyed_data_finish(&fixture, &plan, &prepared);
    fixture.catalog.clear();
    fixture
        .catalog
        .fail_next(crate::iceberg::ErrorKind::CatalogCommitConflicts);
    let ExternalMutationOutcome::KnownUncommitted {
        failure,
        cleanup: ExternalMutationFinalization::Complete,
    } = control.finish_write(request).unwrap()
    else {
        panic!("definite conflict must reject")
    };
    assert_eq!(
        failure.kind(),
        novarocks_spi::connector::ConnectorMutationFailureKind::Conflict
    );
    assert_eq!(fixture.catalog.commits().len(), 1);
    let snapshot = actual_snapshot_updates(&fixture);
    assert_eq!(snapshot.len(), 1);
    assert!(!std::path::Path::new(data.strip_prefix("file://").unwrap()).exists());
    assert!(
        !std::path::Path::new(snapshot[0].manifest_list().strip_prefix("file://").unwrap())
            .exists()
    );
    assert!(
        fixture
            .catalog
            .load_table(fixture.table.identifier())
            .await
            .unwrap()
            .metadata()
            .current_snapshot_id()
            .is_none()
    );
}
