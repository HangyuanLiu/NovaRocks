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

//! Independent Java REST evidence for canonical operation requests.
//! These ignored tests require a pinned fixture publication; they are not native SQL acceptance.

use super::*;
use crate::access_binding::IcebergReadBinding;
use crate::catalog::error::{CatalogCommitEvidence, CatalogOutcome};
use crate::catalog::transaction::CommitProof;
use crate::commit::attempt::{self, Publisher, RetryPolicy};
use crate::commit::dependency::ValidationInputs;
use crate::commit::fast_append::FastAppendPreparer;
use crate::commit::model::*;
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation, OperationLimits};
use crate::commit::recovery::FrozenPublicationFacts;
use crate::commit::rewrite_data_files::RewriteDataFilesPreparer;
use crate::commit::statistics::StatisticsPreparer;
use crate::iceberg::spec::*;
use crate::iceberg::{
    Error, ErrorKind, NamespaceIdent, Result, TableIdent, TableRequirement, TableUpdate,
};
use crate::iceberg_catalog_rest::{
    CommitTableRequest, CommitTableResponse, CreateTableRequest, LoadTableResult,
};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use novarocks_fs::{
    FsAccessResolver, ObjectStoreConfig, SecretValue, TokioFileIoRuntime, TokioFileTaskSpawner,
};
use novarocks_spi::connector::{
    ConnectorMutationFailureKind, ConnectorRequestContext, ConnectorStopOwner,
    ConnectorWriteOperationId, ExternalMutationEffect, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, StatisticsArtifactDraft,
};
use parquet::arrow::ArrowWriter;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use url::Url;
use uuid::Uuid;

fn required(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("Pinned interop driver must set {key}"))
}

fn error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Unexpected, message.into())
}

fn schema() -> Schema {
    Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap()
}

fn semantic_facts(metadata: &TableMetadata) -> Value {
    let mut facts = serde_json::to_value(metadata).unwrap();
    for field in ["last-updated-ms", "metadata-log", "snapshot-log"] {
        facts.as_object_mut().unwrap().remove(field);
    }
    for field in [
        "schemas",
        "partition-specs",
        "sort-orders",
        "snapshots",
        "statistics",
    ] {
        if let Some(values) = facts[field].as_array_mut() {
            values.sort_by_key(Value::to_string);
        }
    }
    facts
}

#[derive(Clone)]
struct Java {
    client: Client,
    root: Url,
    prefix: Vec<String>,
    namespace: NamespaceIdent,
    binding: IcebergReadBinding,
    artifacts: PathBuf,
}

impl Java {
    async fn new(case: &str) -> Self {
        let namespace = NamespaceIdent::new(format!(
            "{}_{}",
            required("NR_IRU5_INTEROP_NAMESPACE"),
            case
        ));
        let artifacts = PathBuf::from(required("NR_IRU5_INTEROP_ARTIFACT_DIR")).join(case);
        let java = Self::attach(namespace, artifacts).await;
        let result = java
            .client
            .post(java.endpoint(&["namespaces"]))
            .json(&json!({"namespace": java.namespace.as_ref(), "properties": {}}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            result.status(),
            StatusCode::OK,
            "unique namespace must be created exactly once"
        );
        java.record("fixture", &json!({"publication": required("NOVA_ENV_REST_ENV_FILE"), "prefix": java.prefix, "namespace": java.namespace.as_ref()}));
        java
    }

    /// Attach the independent raw-HTTP client to the pinned server without
    /// creating `namespace`.
    async fn attach(namespace: NamespaceIdent, artifacts: PathBuf) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let mut root = Url::parse(&required("NOVAROCKS_ICEBERG_REST_URI")).unwrap();
        assert!(root.query().is_none() && root.fragment().is_none());
        assert!(root.username().is_empty() && root.password().is_none());
        root.set_path(&format!("{}/", root.path().trim_end_matches('/')));
        let config_url = root.join("v1/config").unwrap();
        let response = client
            .get(config_url)
            .query(&[("warehouse", required("NOVAROCKS_ICEBERG_REST_WAREHOUSE"))])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let config: Value = response.json().await.unwrap();
        let mut props = BTreeMap::new();
        for section in ["defaults", "overrides"] {
            if let Some(values) = config[section].as_object() {
                for (key, value) in values {
                    props.insert(
                        key.clone(),
                        value.as_str().expect("string REST config").to_owned(),
                    );
                }
            }
        }
        let prefix = props
            .get("prefix")
            .map(|p| {
                p.split('/')
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        std::fs::create_dir(&artifacts).unwrap();
        let runtime = tokio::runtime::Handle::current();
        let binding = IcebergReadBinding::new(
            Some(ObjectStoreConfig {
                endpoint: required("AWS_S3_ENDPOINT"),
                access_key_id: SecretValue::new(required("AWS_S3_ACCESS_KEY_ID")),
                access_key_secret: SecretValue::new(required("AWS_S3_SECRET_ACCESS_KEY")),
                session_token: std::env::var("AWS_SESSION_TOKEN")
                    .ok()
                    .map(SecretValue::new),
                enable_path_style_access: Some(true),
                region: std::env::var("AWS_REGION").ok(),
                retry_max_times: None,
                retry_min_delay_ms: None,
                retry_max_delay_ms: None,
                timeout_ms: None,
                io_timeout_ms: None,
            }),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        );
        Self {
            client,
            root,
            prefix,
            namespace,
            binding,
            artifacts,
        }
    }

    fn independent(&self) -> Self {
        let mut independent = self.clone();
        independent.client = Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        independent
    }

    fn endpoint(&self, parts: &[&str]) -> Url {
        let mut url = self.root.clone();
        url.path_segments_mut()
            .unwrap()
            .pop_if_empty()
            .push("v1")
            .extend(self.prefix.iter().map(String::as_str))
            .extend(parts.iter().copied());
        url
    }

    fn table_url(&self, ident: &TableIdent) -> Url {
        self.endpoint(&[
            "namespaces",
            &ident.namespace.to_url_string(),
            "tables",
            &ident.name,
        ])
    }

    fn ident(&self, name: &str) -> TableIdent {
        TableIdent::new(self.namespace.clone(), name.to_owned())
    }

    fn record(&self, name: &str, value: &Value) {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.artifacts.join(format!("{name}.json")))
            .unwrap();
        file.write_all(&serde_json::to_vec_pretty(value).unwrap())
            .unwrap();
    }

    async fn create(&self, name: &str, staged: bool) -> LoadTableResult {
        let response = self
            .client
            .post(self.endpoint(&["namespaces", &self.namespace.to_url_string(), "tables"]))
            .json(&CreateTableRequest {
                name: name.to_owned(),
                location: None,
                schema: schema(),
                partition_spec: Some(PartitionSpec::unpartition_spec().into_unbound()),
                write_order: Some(SortOrder::unsorted_order()),
                stage_create: Some(staged),
                properties: HashMap::from([
                    ("format-version".into(), "3".into()),
                    ("write.row-lineage".into(), "true".into()),
                ]),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let loaded: LoadTableResult = response.json().await.unwrap();
        assert_eq!(loaded.metadata.format_version(), FormatVersion::V3);
        assert!(
            loaded.metadata.location().starts_with("s3://"),
            "use actual Java S3 location"
        );
        assert_eq!(loaded.metadata_location.is_none(), staged);
        self.record(&format!("{name}-initial"), &json!({"staged": staged, "metadata": loaded.metadata, "metadata-location": loaded.metadata_location}));
        loaded
    }

    async fn load(&self, ident: &TableIdent) -> Result<LoadTableResult> {
        let response = self
            .client
            .get(self.table_url(ident))
            .send()
            .await
            .map_err(|e| error(format!("Java load failed: {e}")))?;
        if response.status() != StatusCode::OK {
            return Err(error(format!("Java load returned {}", response.status())));
        }
        response
            .json()
            .await
            .map_err(|e| error(format!("Java load decode failed: {e}")))
    }

    async fn post(&self, label: &str, request: &FrozenRequest) -> Result<(StatusCode, Bytes)> {
        let envelope = CommitTableRequest {
            identifier: Some(request.identifier().clone()),
            requirements: request.requirements().to_vec(),
            updates: request.updates().to_vec(),
        };
        let body = serde_json::to_value(&envelope).unwrap();
        assert_eq!(body, request.to_rest_json().unwrap());
        self.record(&format!("{label}-request"), &body);
        // One POST only. Unknown transport/5xx outcomes are never replayed by this helper.
        let response = self
            .client
            .post(self.table_url(request.identifier()))
            .json(&envelope)
            .send()
            .await
            .map_err(|e| error(format!("Java commit transport outcome unknown: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| error(format!("Java commit body outcome unknown: {e}")))?;
        let observed = if status == StatusCode::OK {
            let result: CommitTableResponse = serde_json::from_slice(&bytes)
                .map_err(|e| error(format!("Java committed response decode unknown: {e}")))?;
            json!({"status": status.as_u16(), "metadata": result.metadata, "metadata-location": result.metadata_location})
        } else {
            // Retain the wire status without persisting arbitrary server error text or headers.
            json!({"status": status.as_u16(), "error-body-bytes": bytes.len()})
        };
        self.record(&format!("{label}-response"), &observed);
        Ok((status, bytes))
    }

    fn operation(&self, metadata: &TableMetadata) -> IcebergCommitOperation {
        let stop = ConnectorStopOwner::new();
        IcebergCommitOperation::new(
            OperationToken::from_write(ConnectorWriteOperationId::from_bytes(
                *Uuid::now_v7().as_bytes(),
            )),
            metadata.location(),
            self.binding.clone(),
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(120),
                stop.view(),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .unwrap(),
            crate::resources::IcebergCatalogRuntime::new(tokio::runtime::Handle::current()),
            OperationLimits::default(),
        )
        .unwrap()
    }

    async fn data(
        &self,
        operation: &IcebergCommitOperation,
        metadata: &TableMetadata,
        values: &[i64],
    ) -> DataFile {
        let field = Field::new("id", DataType::Int64, false)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "1".into())]));
        let arrow = Arc::new(ArrowSchema::new(vec![field]));
        let batch = RecordBatch::try_new(
            arrow.clone(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, arrow, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        // Session writers produce immutable data before commit admission; the operation adopts that exact object.
        let object = ObjectIdentity::new(format!(
            "{}/data/{}-{}.parquet",
            metadata.location().trim_end_matches('/'),
            operation.token().path_component(),
            Uuid::now_v7()
        ))
        .unwrap();
        let io = crate::fs_io::build_file_io_for_location(object.path(), self.binding.clone());
        let mut output = io
            .new_output(object.path())
            .unwrap()
            .writer()
            .await
            .unwrap();
        output.write(Bytes::copy_from_slice(&bytes)).await.unwrap();
        output.close().await.unwrap();
        operation.adopt_session_data(object.clone()).unwrap();
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(object.path().into())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(values.len() as u64)
            .file_size_in_bytes(bytes.len() as u64)
            .build()
            .unwrap()
    }
}

fn intent(
    java: &Java,
    name: &str,
    metadata: &TableMetadata,
    operation: &IcebergCommitOperation,
    shape: RequestShape,
    added: Vec<AddedContent>,
    removed: Vec<FrozenEntry>,
    dependency: Dependency,
) -> OperationIntent {
    OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: java.ident(name),
            uuid: (shape != RequestShape::Create).then_some(metadata.uuid()),
        },
        target_ref: "main".into(),
        start: metadata.current_snapshot().map(|s| StartSnapshot {
            snapshot_id: s.snapshot_id(),
            sequence_number: s.sequence_number(),
        }),
        changes: FileChanges { added, removed },
        dependencies: vec![dependency],
        isolation: IsolationLevel::Snapshot,
        shape,
        summary: BTreeMap::from([("iru5-interop".into(), operation.token().path_component())]),
        token: operation.token(),
    })
    .unwrap()
}

fn base(loaded: &LoadTableResult) -> StagingBase {
    StagingBase::Existing {
        metadata: loaded.metadata.clone(),
        metadata_location: loaded.metadata_location.clone().unwrap(),
    }
}

fn added_references(intent: &OperationIntent) -> Vec<ObjectIdentity> {
    intent
        .changes()
        .added
        .iter()
        .map(|a| ObjectIdentity::new(a.file().file_path()).unwrap())
        .collect()
}

fn bounded_preflight(request: &FrozenRequest, operation: &IcebergCommitOperation) -> Result<()> {
    let facts = FrozenPublicationFacts::from_request(request, operation)?;
    let bytes = serde_json::to_vec(&facts).map_err(|e| error(e.to_string()))?;
    if bytes.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(error("Recovery ledger exceeds handle payload bound"));
    }
    let decoded: FrozenPublicationFacts =
        serde_json::from_slice(&bytes).map_err(|e| error(e.to_string()))?;
    decoded.validate(operation.token())?;
    Ok(())
}

async fn freeze_append(
    loaded: &LoadTableResult,
    intent: &OperationIntent,
    attempt: &IcebergCommitAttempt,
) -> (TableMetadata, FrozenRequest) {
    let mut engine = StagingEngine::begin(base(loaded), intent, attempt).unwrap();
    engine.stage(&FastAppendPreparer).await.unwrap();
    let expected = engine.metadata().clone();
    let request = engine.freeze(&added_references(intent)).unwrap();
    (expected, request)
}

async fn publish(
    java: &Java,
    label: &str,
    expected: &TableMetadata,
    request: &FrozenRequest,
    operation: &IcebergCommitOperation,
) -> LoadTableResult {
    bounded_preflight(request, operation).unwrap();
    let (status, _) = java.post(label, request).await.unwrap();
    assert_eq!(status, StatusCode::OK);
    let actual = java.load(request.identifier()).await.unwrap();
    assert_eq!(
        semantic_facts(&actual.metadata),
        semantic_facts(expected),
        "Java is the independent metadata oracle"
    );
    java.record(&format!("{label}-readback"), &json!({"facts": semantic_facts(&actual.metadata), "metadata-location": actual.metadata_location}));
    actual
}

async fn statistics(attempt: &IcebergCommitAttempt, snapshot: &Snapshot) -> StatisticsFile {
    let draft = StatisticsArtifactDraft::try_new(
        vec![1],
        "iru5/test-statistics-v1",
        Bytes::from_static(b"independent-interop"),
        BTreeMap::new(),
    )
    .unwrap();
    crate::stats_assembler::write_puffin_artifacts_allocated(
        attempt,
        snapshot.snapshot_id(),
        snapshot.sequence_number(),
        &[draft],
    )
    .await
    .unwrap()
    .unwrap()
}

async fn physical(
    java: &Java,
    label: &str,
    metadata: &TableMetadata,
    snapshot_id: i64,
    attempt: &IcebergCommitAttempt,
) -> Vec<(ManifestFile, ManifestEntry)> {
    let snapshot = metadata.snapshot_by_id(snapshot_id).unwrap();
    let list = crate::manifest::load_manifest_list(&snapshot, attempt.file_io(), metadata)
        .await
        .unwrap();
    let mut raw = Vec::new();
    for manifest in list.entries() {
        let bytes = attempt
            .file_io()
            .new_input(&manifest.manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        for entry in Manifest::parse_avro(&bytes).unwrap().entries() {
            raw.push((manifest.clone(), entry.as_ref().clone()));
        }
    }
    java.record(
        label,
        &json!({
            "snapshot": {
                "snapshot-id": snapshot.snapshot_id(),
                "parent-snapshot-id": snapshot.parent_snapshot_id(),
                "sequence-number": snapshot.sequence_number(),
                "timestamp-ms": snapshot.timestamp_ms(),
                "manifest-list": snapshot.manifest_list(),
                "schema-id": snapshot.schema_id(),
                "first-row-id": snapshot.first_row_id(),
                "added-rows": snapshot.added_rows_count(),
                "operation": format!("{:?}", snapshot.summary().operation),
                "summary": snapshot.summary().additional_properties,
            },
            "manifest-list": list.entries().iter().map(|manifest| json!({
                "manifest-path": manifest.manifest_path,
                "manifest-length": manifest.manifest_length,
                "partition-spec-id": manifest.partition_spec_id,
                "content": format!("{:?}", manifest.content),
                "sequence-number": manifest.sequence_number,
                "min-sequence-number": manifest.min_sequence_number,
                "added-snapshot-id": manifest.added_snapshot_id,
                "added-files-count": manifest.added_files_count,
                "existing-files-count": manifest.existing_files_count,
                "deleted-files-count": manifest.deleted_files_count,
                "added-rows-count": manifest.added_rows_count,
                "existing-rows-count": manifest.existing_rows_count,
                "deleted-rows-count": manifest.deleted_rows_count,
                "first-row-id": manifest.first_row_id,
            })).collect::<Vec<_>>(),
            "entries": raw.iter().map(|(manifest, entry)| json!({
                "manifest": manifest.manifest_path,
                "spec-id": manifest.partition_spec_id,
                "status": format!("{:?}", entry.status()),
                "path": entry.data_file().file_path(),
                "data-sequence": entry.sequence_number,
                "file-sequence": entry.file_sequence_number,
                "first-row-id": entry.data_file().first_row_id(),
            })).collect::<Vec<_>>(),
        }),
    );
    raw
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru5_java_rest_three_shapes_historic_spec_and_composition() {
    let java = Java::new("shapes").await;
    let staged = java.create("created", true).await;
    let operation = java.operation(&staged.metadata);
    let file = java.data(&operation, &staged.metadata, &[10, 11]).await;
    let create = intent(
        &java,
        "created",
        &staged.metadata,
        &operation,
        RequestShape::Create,
        vec![
            AddedContent::new_logical_data(file, staged.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let attempt = operation.begin_attempt().unwrap();
    let mut engine = StagingEngine::begin(
        StagingBase::Create {
            staged: StagedCreateIdentity::new(operation.token(), Arc::new(staged.metadata.clone())),
            initialization_updates: super::staged_create_updates(&staged.metadata).unwrap(),
        },
        &create,
        &attempt,
    )
    .unwrap();
    engine.stage(&FastAppendPreparer).await.unwrap();
    let expected = engine.metadata().clone();
    let request = engine.freeze(&added_references(&create)).unwrap();
    assert_eq!(request.requirements(), &[TableRequirement::NotExist]);
    let created = publish(&java, "create", &expected, &request, &operation).await;
    assert_eq!(
        created
            .metadata
            .current_snapshot()
            .unwrap()
            .added_rows_count(),
        Some(2)
    );

    let operation = java.operation(&created.metadata);
    let file = java.data(&operation, &created.metadata, &[12]).await;
    let append = intent(
        &java,
        "created",
        &created.metadata,
        &operation,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(file, created.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let attempt = operation.begin_attempt().unwrap();
    let mut engine = StagingEngine::begin(base(&created), &append, &attempt).unwrap();
    let historical = created.metadata.default_partition_spec_id();
    let partitioned: UnboundPartitionSpec = serde_json::from_value(json!({"spec-id":1,"fields":[{"source-id":1,"field-id":1000,"name":"id_part","transform":"identity"}]})).unwrap();
    engine
        .stage_change(PreparedChange {
            updates: vec![
                TableUpdate::AddSpec { spec: partitioned },
                TableUpdate::SetDefaultSpec { spec_id: -1 },
            ],
            requirements: vec![],
        })
        .unwrap();
    assert_ne!(engine.metadata().default_partition_spec_id(), historical);
    java.record(
        "composition-partition-prefix",
        &semantic_facts(engine.metadata()),
    );
    engine
        .stage_change(PreparedChange {
            updates: vec![
                TableUpdate::AddSpec {
                    spec: created
                        .metadata
                        .default_partition_spec()
                        .as_ref()
                        .clone()
                        .into_unbound(),
                },
                TableUpdate::SetDefaultSpec { spec_id: -1 },
            ],
            requirements: vec![],
        })
        .unwrap();
    assert_eq!(engine.metadata().default_partition_spec_id(), historical);
    assert!(engine.updates().contains(&TableUpdate::SetDefaultSpec {
        spec_id: historical
    }));
    java.record(
        "composition-historical-prefix",
        &semantic_facts(engine.metadata()),
    );
    engine.stage(&FastAppendPreparer).await.unwrap();
    java.record(
        "composition-append-prefix",
        &semantic_facts(engine.metadata()),
    );
    let stats = statistics(&attempt, engine.metadata().current_snapshot().unwrap()).await;
    engine
        .stage(&StatisticsPreparer {
            statistics: stats.clone(),
        })
        .await
        .unwrap();
    java.record(
        "composition-statistics-prefix",
        &semantic_facts(engine.metadata()),
    );
    let expected = engine.metadata().clone();
    // Attempt-owned Puffin is frozen through the attempt ledger; only session data crosses attempts.
    let request = engine.freeze(&added_references(&append)).unwrap();
    assert!(
        request
            .artifacts()
            .attempt_owned()
            .iter()
            .any(|object| object.path() == stats.statistics_path)
    );
    let appended = publish(&java, "composition", &expected, &request, &operation).await;
    assert_eq!(appended.metadata.statistics_iter().len(), 1);
    assert_eq!(
        appended
            .metadata
            .current_snapshot()
            .unwrap()
            .added_rows_count(),
        Some(1)
    );

    let operation = java.operation(&appended.metadata);
    let snapshot = appended.metadata.current_snapshot().unwrap();
    let measured = intent(
        &java,
        "created",
        &appended.metadata,
        &operation,
        RequestShape::MetadataOnly,
        vec![],
        vec![],
        Dependency::MeasuredSnapshotExists(snapshot.snapshot_id()),
    );
    let attempt = operation.begin_attempt().unwrap();
    let parent = Some(snapshot.snapshot_id());
    assert_eq!(
        crate::commit::dependency::validate(&measured, &appended.metadata, parent, &attempt)
            .await
            .unwrap(),
        crate::commit::dependency::Verdict::Holds
    );
    let stats = statistics(&attempt, snapshot).await;
    let mut engine = StagingEngine::begin(base(&appended), &measured, &attempt).unwrap();
    engine
        .stage(&StatisticsPreparer {
            statistics: stats.clone(),
        })
        .await
        .unwrap();
    let expected = engine.metadata().clone();
    // This metadata-only request owns no cross-attempt/session references.
    let request = engine.freeze(&[]).unwrap();
    assert!(
        request
            .artifacts()
            .attempt_owned()
            .iter()
            .any(|object| object.path() == stats.statistics_path)
    );
    assert!(
        !request
            .requirements()
            .iter()
            .any(|r| matches!(r, TableRequirement::RefSnapshotIdMatch { .. }))
    );
    assert!(!request.updates().iter().any(|u| matches!(
        u,
        TableUpdate::AddSnapshot { .. } | TableUpdate::SetSnapshotRef { .. }
    )));
    let actual = publish(&java, "metadata-only", &expected, &request, &operation).await;
    assert_eq!(
        actual.metadata.current_snapshot_id(),
        appended.metadata.current_snapshot_id()
    );
    assert_eq!(actual.metadata.snapshots().len(), 2);
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru5_java_rest_d12_inheritance_and_physical_rewrite() {
    let java = Java::new("d12").await;
    let loaded = java.create("data", false).await;
    let operation = java.operation(&loaded.metadata);
    let file = java.data(&operation, &loaded.metadata, &[20, 21, 22]).await;
    let append = intent(
        &java,
        "data",
        &loaded.metadata,
        &operation,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(file, loaded.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let attempt = operation.begin_attempt().unwrap();
    let (expected, request) = freeze_append(&loaded, &append, &attempt).await;
    let added = publish(&java, "append", &expected, &request, &operation).await;
    let source = added.metadata.current_snapshot().unwrap();
    let raw = physical(
        &java,
        "append-d12",
        &added.metadata,
        source.snapshot_id(),
        &attempt,
    )
    .await;
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].1.sequence_number, None);
    assert_eq!(raw[0].1.file_sequence_number, None);
    assert_eq!(raw[0].1.data_file().first_row_id(), None);
    assert_eq!(raw[0].0.first_row_id, source.first_row_id());
    let inherited = crate::manifest::load_manifest(&raw[0].0, attempt.file_io())
        .await
        .unwrap();
    assert_eq!(
        inherited.entries()[0].sequence_number,
        Some(source.sequence_number())
    );
    assert_eq!(
        inherited.entries()[0].file_sequence_number,
        Some(source.sequence_number())
    );

    let operation = java.operation(&added.metadata);
    let attempt = operation.begin_attempt().unwrap();
    let mut inputs = ValidationInputs::new(&added.metadata, Some(source.snapshot_id()), &attempt);
    let live = inputs.live_set().await.unwrap();
    let frozen: Vec<_> = live.values().map(|v| v.frozen.clone()).collect();
    assert_eq!(frozen.len(), 1);
    let first = frozen[0].facts().first_row_id.unwrap();
    let file = java.data(&operation, &added.metadata, &[20, 21, 22]).await;
    let file = crate::commit::data_file::clone_data_file_with_first_row_id(
        &file,
        frozen[0].facts().partition_spec_id,
        Some(first),
    )
    .unwrap();
    let rewrite = intent(
        &java,
        "data",
        &added.metadata,
        &operation,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::rewritten_data(
                file,
                frozen[0].facts().partition_spec_id,
                source.sequence_number(),
            )
            .unwrap(),
        ],
        frozen,
        Dependency::RefUnchanged,
    );
    let mut engine = StagingEngine::begin(base(&added), &rewrite, &attempt).unwrap();
    engine.stage(&RewriteDataFilesPreparer).await.unwrap();
    let expected = engine.metadata().clone();
    let request = engine.freeze(&added_references(&rewrite)).unwrap();
    let rewritten = publish(&java, "rewrite", &expected, &request, &operation).await;
    let snapshot = rewritten.metadata.current_snapshot().unwrap();
    assert_eq!(snapshot.added_rows_count(), Some(0));
    assert_eq!(
        rewritten.metadata.next_row_id(),
        added.metadata.next_row_id()
    );
    let raw = physical(
        &java,
        "rewrite-d12",
        &rewritten.metadata,
        snapshot.snapshot_id(),
        &attempt,
    )
    .await;
    let replacement = raw
        .iter()
        .find(|(_, entry)| entry.status() == ManifestStatus::Added)
        .unwrap();
    assert_eq!(
        replacement.1.sequence_number,
        Some(source.sequence_number())
    );
    assert_eq!(replacement.1.file_sequence_number, None);
    assert_eq!(replacement.1.data_file().first_row_id(), Some(first));
    let inherited = crate::manifest::load_manifest(&replacement.0, attempt.file_io())
        .await
        .unwrap();
    let replacement = inherited
        .entries()
        .iter()
        .find(|e| e.status() == ManifestStatus::Added)
        .unwrap();
    assert_eq!(replacement.sequence_number, Some(source.sequence_number()));
    assert_eq!(
        replacement.file_sequence_number,
        Some(snapshot.sequence_number())
    );
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru5_java_rest_two_canonical_clients_real_cas() {
    let java = Java::new("cas").await;
    let other = java.independent();
    let loaded = java.create("race", false).await;
    let left = java.operation(&loaded.metadata);
    let right = other.operation(&loaded.metadata);
    let left_data = java.data(&left, &loaded.metadata, &[31]).await;
    let right_data = other.data(&right, &loaded.metadata, &[32]).await;
    let left_intent = intent(
        &java,
        "race",
        &loaded.metadata,
        &left,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(left_data, loaded.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let right_intent = intent(
        &other,
        "race",
        &loaded.metadata,
        &right,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(right_data, loaded.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let left_attempt = left.begin_attempt().unwrap();
    let right_attempt = right.begin_attempt().unwrap();
    let (expected, left_request) = freeze_append(&loaded, &left_intent, &left_attempt).await;
    let (_, right_request) = freeze_append(&loaded, &right_intent, &right_attempt).await;
    assert_ne!(left.token(), right.token());
    assert_eq!(left_request.requirements(), right_request.requirements());
    assert!(
        matches!((left_request.base(),right_request.base()), (BaseIdentity::Existing { uuid:a,parent:b,metadata_location:c },BaseIdentity::Existing { uuid:d,parent:e,metadata_location:f }) if a==d && b==e && c==f)
    );
    bounded_preflight(&right_request, &right).unwrap();
    publish(&java, "winner", &expected, &left_request, &left).await;
    let (status, _) = other.post("stale-loser", &right_request).await.unwrap();
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "require real Java requirement rejection"
    );
    let actual = java.load(&java.ident("race")).await.unwrap();
    assert_eq!(semantic_facts(&actual.metadata), semantic_facts(&expected));
    assert_eq!(actual.metadata.snapshots().len(), 1);
    let mut inputs = ValidationInputs::new(
        &actual.metadata,
        actual.metadata.current_snapshot_id(),
        &left_attempt,
    );
    let live = inputs.live_set().await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(
        live.values().next().unwrap().file.file_path(),
        left_intent.changes().added[0].file().file_path()
    );
    assert_ne!(
        live.values().next().unwrap().file.file_path(),
        right_intent.changes().added[0].file().file_path()
    );
}

struct InterleavingPublisher {
    java: Java,
    ident: TableIdent,
    operation: IcebergCommitOperation,
    competitor: Mutex<Option<(Java, IcebergCommitOperation, TableMetadata, FrozenRequest)>>,
    dispatched: Mutex<Vec<Value>>,
}

#[async_trait]
impl Publisher for InterleavingPublisher {
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
        attempt.check_active()?;
        let loaded = self.java.load(&self.ident).await?;
        attempt.check_active()?;
        Ok(base(&loaded))
    }

    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()> {
        bounded_preflight(request, operation)
    }

    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        let competitor = self.competitor.lock().unwrap().take();
        if let Some((java, operation, expected, competing)) = competitor {
            // The first canonical request is already frozen. An independent client now wins its real CAS.
            publish(&java, "competitor", &expected, &competing, &operation).await;
        }
        let ordinal = request.artifacts().attempt().ordinal();
        let snapshot = request.ref_snapshot_after("main");
        let envelope = request.to_rest_json().unwrap();
        self.dispatched
            .lock()
            .unwrap()
            .push(json!({"attempt":ordinal,"snapshot":snapshot,"request":envelope}));
        let evidence = CatalogCommitEvidence::for_target(format!(
            "{}.{}",
            self.ident.namespace, self.ident.name
        ))
        .with_commit_uuid(self.operation.token().path_component());
        let (status, body) = match self
            .java
            .post(&format!("attempt-{ordinal}"), &request)
            .await
        {
            Ok(result) => result,
            Err(error) => return CatalogOutcome::unknown(error.to_string(), evidence),
        };
        if status == StatusCode::CONFLICT {
            return CatalogOutcome::uncommitted(
                ConnectorMutationFailureKind::Conflict,
                "Java REST rejected stale requirements",
            );
        }
        if status.is_server_error() {
            return CatalogOutcome::unknown(format!("Java REST returned {status}"), evidence);
        }
        if status != StatusCode::OK {
            return CatalogOutcome::uncommitted(
                ConnectorMutationFailureKind::InvalidRequest,
                format!("Java REST returned {status}"),
            );
        }
        let response: CommitTableResponse = match serde_json::from_slice(&body) {
            Ok(response) => response,
            Err(error) => return CatalogOutcome::unknown(error.to_string(), evidence),
        };
        if response.metadata.current_snapshot_id() != snapshot {
            return CatalogOutcome::unknown(
                "Java response did not match exact frozen snapshot",
                evidence,
            );
        }
        CatalogOutcome::committed(
            CommitProof::applied(snapshot).with_table_uuid(response.metadata.uuid().to_string()),
            ExternalMutationEffect::Applied,
        )
    }
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru5_java_rest_attempt_runner_reloads_and_reprepares_after_real_cas() {
    let java = Java::new("reprepare").await;
    let other = java.independent();
    let loaded = java.create("race", false).await;
    let operation = java.operation(&loaded.metadata);
    let competitor = other.operation(&loaded.metadata);
    let data = java.data(&operation, &loaded.metadata, &[41]).await;
    let other_data = other.data(&competitor, &loaded.metadata, &[42]).await;
    let append = intent(
        &java,
        "race",
        &loaded.metadata,
        &operation,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(data, loaded.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let other_intent = intent(
        &other,
        "race",
        &loaded.metadata,
        &competitor,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(other_data, loaded.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let other_attempt = competitor.begin_attempt().unwrap();
    let (other_expected, other_request) =
        freeze_append(&loaded, &other_intent, &other_attempt).await;
    let other_snapshot = other_request.ref_snapshot_after("main").unwrap();
    let publisher = InterleavingPublisher {
        java: java.clone(),
        ident: java.ident("race"),
        operation: operation.clone(),
        competitor: Mutex::new(Some((other, competitor, other_expected, other_request))),
        dispatched: Mutex::new(vec![]),
    };
    let policy = RetryPolicy::from_properties(&HashMap::from([
        ("commit.retry.num-retries".into(), "1".into()),
        ("commit.retry.min-wait-ms".into(), "1".into()),
        ("commit.retry.max-wait-ms".into(), "2".into()),
        ("commit.retry.total-timeout-ms".into(), "60000".into()),
    ]))
    .unwrap();
    let report = attempt::run(
        &operation,
        &append,
        &PreparedChange::default(),
        &[&FastAppendPreparer],
        &publisher,
        policy,
    )
    .await;
    let proof = match report.publication {
        PublicationOutcome::Committed(proof) => proof,
        other => panic!("real retry must commit: {other:?}"),
    };
    assert!(matches!(
        report.cleanup,
        IcebergCleanupReport::Complete { .. }
    ));
    let traces = publisher.dispatched.lock().unwrap().clone();
    assert_eq!(traces.len(), 2);
    assert_eq!(traces[0]["attempt"], 0);
    assert_eq!(traces[1]["attempt"], 1);
    assert_ne!(traces[0]["snapshot"], traces[1]["snapshot"]);
    let snapshots = |trace: &Value| {
        trace["request"]["updates"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|u| u["action"] == "add-snapshot")
            .map(|u| u["snapshot"].clone())
            .collect::<Vec<_>>()
    };
    let first = snapshots(&traces[0]);
    let second = snapshots(&traces[1]);
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_ne!(first[0]["manifest-list"], second[0]["manifest-list"]);
    assert_eq!(second[0]["parent-snapshot-id"], other_snapshot);
    let actual = java.load(&java.ident("race")).await.unwrap();
    assert_eq!(
        actual.metadata.snapshots().len(),
        2,
        "failed first attempt must not publish a snapshot"
    );
    assert_eq!(actual.metadata.current_snapshot_id(), proof.snapshot_id);
    assert_eq!(
        actual
            .metadata
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id(),
        Some(other_snapshot)
    );
    assert!(
        actual
            .metadata
            .snapshot_by_id(first[0]["snapshot-id"].as_i64().unwrap())
            .is_none()
    );
    assert_eq!(actual.metadata.next_row_id(), 2);
    let attempt = operation.begin_attempt().unwrap();
    let mut inputs = ValidationInputs::new(
        &actual.metadata,
        actual.metadata.current_snapshot_id(),
        &attempt,
    );
    let paths: BTreeSet<_> = inputs
        .live_set()
        .await
        .unwrap()
        .values()
        .map(|v| v.file.file_path().to_owned())
        .collect();
    assert_eq!(
        paths,
        BTreeSet::from([
            append.changes().added[0].file().file_path().to_owned(),
            other_intent.changes().added[0]
                .file()
                .file_path()
                .to_owned()
        ])
    );
    for record in operation.artifacts().unwrap().iter().filter(|r| {
        r.class == ArtifactClass::Attempt && r.attempt.is_some_and(|t| t.ordinal() == 0)
    }) {
        assert!(
            !attempt
                .file_io()
                .new_input(record.object.path())
                .unwrap()
                .exists()
                .await
                .unwrap(),
            "failed attempt object must be removed"
        );
    }
    java.record("retry-traces",&json!({"dispatches":traces,"metadata":semantic_facts(&actual.metadata),"cleanup":format!("{:?}",report.cleanup)}));
}

// ---------------------------------------------------------------------------
// IRU-13a: every NovaRocks-issued REST request against the Java reference
// server, judged by NovaRocks' own D13 evidence and cross-checked through the
// independent raw-HTTP client above.
// ---------------------------------------------------------------------------

use crate::catalog_runtime::RestAccessDelegationMode;
use crate::iceberg::{Catalog as _, TableCommit, ViewCommit, ViewCreation, ViewRequirement};
use crate::rest_protocol::adapter::{RestTransitionalAdapter, rest_evidence};
use crate::rest_protocol::endpoints::RestEndpoints;
use crate::rest_protocol::evidence::{
    AttributionExpectation, ExpectedSnapshot, MutationEndpoint, RefRelation, RestEvidence,
    RestReceipt, UnknownReason,
};
use crate::rest_protocol::mutation::MutationOutcome;
use novarocks_spi::connector::ConnectorErrorKind;

/// One NovaRocks REST generation built exactly as production builds a static
/// catalog, plus the independent Java oracle for the same namespace.
struct NovaRocksRest {
    java: Java,
    endpoints: Arc<RestEndpoints>,
}

impl NovaRocksRest {
    /// The namespace is created later, by a NovaRocks request. `driver` is
    /// the environment prefix of the interop driver that owns this run, e.g.
    /// `NR_IRU13A`.
    async fn new(driver: &str, case: &str) -> Self {
        let namespace = NamespaceIdent::new(format!(
            "{}_{}",
            required(&format!("{driver}_INTEROP_NAMESPACE")),
            case
        ));
        let artifacts =
            PathBuf::from(required(&format!("{driver}_INTEROP_ARTIFACT_DIR"))).join(case);
        let java = Java::attach(namespace, artifacts).await;
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "iru13a",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), required("NOVAROCKS_ICEBERG_REST_URI")),
                (
                    "warehouse".to_string(),
                    required("NOVAROCKS_ICEBERG_REST_WAREHOUSE"),
                ),
            ],
        )
        .unwrap();
        let client = crate::catalog_runtime::build_catalog_client(
            &configuration,
            java.binding.clone(),
            crate::iceberg::Runtime::try_current().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            client.rest_access_delegation(),
            RestAccessDelegationMode::Static
        );
        let endpoints = Arc::clone(client.rest_endpoints().expect("REST generation endpoints"));
        java.record("fixture", &json!({"publication": required("NOVA_ENV_REST_ENV_FILE"), "prefix": java.prefix, "namespace": java.namespace.as_ref()}));
        Self { java, endpoints }
    }

    fn namespace(&self) -> &NamespaceIdent {
        &self.java.namespace
    }

    /// Record the evidence of one request; its `Debug` carries no server text.
    fn record(&self, label: &str, outcome: &MutationOutcome) {
        self.java.record(
            label,
            &json!({"evidence": format!("{:?}", outcome.evidence)}),
        );
    }

    async fn create_namespace(&self) {
        let outcome = self
            .endpoints
            .create_namespace(self.namespace(), HashMap::new())
            .await;
        self.record("create-namespace", &outcome);
        applied(&outcome, MutationEndpoint::CreateNamespace);
    }

    async fn drop_namespace(&self) {
        let outcome = self.endpoints.drop_namespace(self.namespace()).await;
        self.record("drop-namespace", &outcome);
        applied(&outcome, MutationEndpoint::DropNamespace);
    }

    /// Create a table immediately (static access) and return the applied
    /// response document.
    async fn create_table(&self, label: &str, name: &str) -> LoadTableResult {
        let outcome = self
            .endpoints
            .create_table(self.namespace(), create_request(name))
            .await;
        self.record(label, &outcome);
        let receipt = applied(&outcome, MutationEndpoint::CreateTable);
        let created: LoadTableResult =
            serde_json::from_slice(outcome.document.as_deref().expect("applied document")).unwrap();
        assert_eq!(receipt.uuid, Some(created.metadata.uuid()));
        assert!(receipt.metadata_location.is_some());
        assert_eq!(receipt.metadata_location, created.metadata_location);
        assert_eq!(created.metadata.format_version(), FormatVersion::V3);
        let oracle = self.java.load(&self.java.ident(name)).await.unwrap();
        assert_eq!(oracle.metadata.uuid(), created.metadata.uuid());
        assert_eq!(oracle.metadata_location, created.metadata_location);
        created
    }

    async fn drop_table(&self, label: &str, name: &str) {
        let outcome = self.endpoints.drop_table(&self.java.ident(name)).await;
        self.record(label, &outcome);
        applied(&outcome, MutationEndpoint::DropTable);
    }

    /// The raw status of an independent load.
    async fn load_status(&self, ident: &TableIdent) -> StatusCode {
        self.java
            .client
            .get(self.java.table_url(ident))
            .send()
            .await
            .unwrap()
            .status()
    }
}

/// The receipt of an applied request; anything else fails with its evidence.
fn applied(outcome: &MutationOutcome, endpoint: MutationEndpoint) -> RestReceipt {
    match &outcome.evidence {
        RestEvidence::Applied { receipt } if receipt.endpoint == endpoint => receipt.clone(),
        other => panic!("Java REST must apply the NovaRocks {endpoint:?} request: {other:?}"),
    }
}

fn create_request(name: &str) -> CreateTableRequest {
    CreateTableRequest {
        name: name.to_owned(),
        location: None,
        schema: schema(),
        partition_spec: Some(PartitionSpec::unpartition_spec().into_unbound()),
        write_order: Some(SortOrder::unsorted_order()),
        stage_create: None,
        properties: HashMap::from([
            ("format-version".into(), "3".into()),
            ("write.row-lineage".into(), "true".into()),
        ]),
    }
}

fn commit_of(request: &FrozenRequest, extra: Vec<TableUpdate>) -> TableCommit {
    let mut updates = request.updates().to_vec();
    updates.extend(extra);
    TableCommit::builder()
        .ident(request.identifier().clone())
        .requirements(request.requirements().to_vec())
        .updates(updates)
        .build()
}

fn set_property(key: &str, value: &str) -> TableUpdate {
    TableUpdate::SetProperties {
        updates: HashMap::from([(key.to_owned(), value.to_owned())]),
    }
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13a_java_rest_namespace_table_lifecycle_and_interleaved_reads() {
    let rest = NovaRocksRest::new("NR_IRU13A", "lifecycle").await;
    let java = &rest.java;
    let endpoints = &rest.endpoints;
    let namespace = rest.namespace();

    // Upstream reads and NovaRocks requests share one session binding.
    let upstream = endpoints.upstream().await.unwrap();
    assert!(!upstream.namespace_exists(namespace).await.unwrap());
    rest.create_namespace().await;
    assert!(upstream.namespace_exists(namespace).await.unwrap());
    let again = endpoints.create_namespace(namespace, HashMap::new()).await;
    rest.record("create-namespace-again", &again);
    assert_eq!(
        again.evidence,
        RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::AlreadyExists
        }
    );

    // Single-page namespace listing, following the server's tokens.
    let mut seen = BTreeSet::new();
    let mut found = false;
    let mut token: Option<String> = None;
    loop {
        let page = endpoints
            .list_namespaces_page(token.as_deref(), 50)
            .await
            .unwrap();
        assert!(page.namespaces.len() <= 50);
        found |= page.namespaces.contains(namespace);
        match page.next_page_token {
            Some(next) => {
                assert!(seen.insert(next.clone()), "server repeated a page token");
                token = Some(next);
            }
            None => break,
        }
    }
    assert!(
        found,
        "paged namespace listing must contain the new namespace"
    );

    let names = ["t0", "t1", "t2"];
    let mut created = BTreeMap::new();
    for name in names {
        assert!(!endpoints.table_exists(&java.ident(name)).await.unwrap());
        let table = rest.create_table(&format!("create-{name}"), name).await;
        assert!(endpoints.table_exists(&java.ident(name)).await.unwrap());
        created.insert(name, table);
    }
    assert!(!endpoints.table_exists(&java.ident("absent")).await.unwrap());

    // Paged table listing with a page smaller than the namespace.
    let mut listed = Vec::new();
    let mut pages = 0;
    let mut seen = BTreeSet::new();
    let mut token: Option<String> = None;
    loop {
        let page = endpoints
            .list_tables_page(namespace, token.as_deref(), 2)
            .await
            .unwrap();
        pages += 1;
        assert!(page.identifiers.len() <= 2);
        listed.extend(page.identifiers.into_iter().map(|ident| ident.name));
        match page.next_page_token {
            Some(next) => {
                assert!(seen.insert(next.clone()), "server repeated a page token");
                token = Some(next);
            }
            None => break,
        }
    }
    assert!(pages >= 2, "a page size of two must page three tables");
    listed.sort();
    assert_eq!(listed, names);
    let mut upstream_listed = upstream
        .list_tables(namespace)
        .await
        .unwrap()
        .into_iter()
        .map(|ident| ident.name)
        .collect::<Vec<_>>();
    upstream_listed.sort();
    assert_eq!(upstream_listed, listed);

    // Drop never requests a purge: the dropped table's metadata file survives.
    let dropped_location = created["t0"].metadata_location.clone().unwrap();
    rest.drop_table("drop-t0", "t0").await;
    assert!(!endpoints.table_exists(&java.ident("t0")).await.unwrap());
    assert_eq!(
        rest.load_status(&java.ident("t0")).await,
        StatusCode::NOT_FOUND
    );
    let io = crate::fs_io::build_file_io_for_location(&dropped_location, java.binding.clone());
    assert!(
        io.exists(&dropped_location).await.unwrap(),
        "drop without purgeRequested keeps the table files"
    );
    let mut remaining = upstream
        .list_tables(namespace)
        .await
        .unwrap()
        .into_iter()
        .map(|ident| ident.name)
        .collect::<Vec<_>>();
    remaining.sort();
    assert_eq!(remaining, ["t1", "t2"]);

    rest.drop_table("drop-t1", "t1").await;
    rest.drop_table("drop-t2", "t2").await;
    let empty = endpoints
        .list_tables_page(namespace, None, 2)
        .await
        .unwrap();
    assert!(empty.identifiers.is_empty());
    assert!(upstream.list_tables(namespace).await.unwrap().is_empty());
    rest.drop_namespace().await;
    assert!(!upstream.namespace_exists(namespace).await.unwrap());
    // D13: a drop's 404 may follow a publication, so it is unknown.
    let missing = endpoints.drop_namespace(namespace).await;
    rest.record("drop-namespace-again", &missing);
    assert!(matches!(
        &missing.evidence,
        RestEvidence::Unknown { diagnosis }
            if diagnosis.reason == UnknownReason::PostPublishPossible
                && diagnosis.status == Some(404)
    ));
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13a_java_rest_table_commit_shapes() {
    let rest = NovaRocksRest::new("NR_IRU13A", "commits").await;
    let java = &rest.java;
    let endpoints = &rest.endpoints;
    rest.create_namespace().await;
    let created = rest.create_table("create", "shapes").await;
    let ident = java.ident("shapes");
    let uuid = created.metadata.uuid();

    // Snapshot-producing: an append adding S, attributed by S and the UUID.
    let operation = java.operation(&created.metadata);
    let file = java.data(&operation, &created.metadata, &[1, 2]).await;
    let append = intent(
        java,
        "shapes",
        &created.metadata,
        &operation,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(file, created.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let attempt = operation.begin_attempt().unwrap();
    let (expected, request) = freeze_append(&created, &append, &attempt).await;
    assert!(
        request
            .requirements()
            .contains(&TableRequirement::UuidMatch { uuid })
    );
    let snapshot = request.ref_snapshot_after("main").unwrap();
    let expect = AttributionExpectation::table_commit(
        uuid,
        Some(ExpectedSnapshot::new(snapshot, Some("main".into()))),
    )
    .unwrap();
    let outcome = endpoints
        .commit_table(commit_of(&request, vec![]), &expect, false)
        .await;
    rest.record("append", &outcome);
    let receipt = applied(&outcome, MutationEndpoint::TableCommit);
    assert_eq!(receipt.snapshot_id, Some(snapshot));
    assert_eq!(receipt.uuid, Some(uuid));
    assert_eq!(receipt.ref_relation, RefRelation::AtExpectedSnapshot);
    let appended = java.load(&ident).await.unwrap();
    assert_eq!(
        semantic_facts(&appended.metadata),
        semantic_facts(&expected),
        "Java is the independent metadata oracle"
    );
    assert_eq!(appended.metadata_location, receipt.metadata_location);

    // Metadata-only: attributed by the frozen UUID alone.
    let commit = crate::commit::existing_commit::existing_table_commit(
        ident.clone(),
        &appended.metadata,
        vec![],
        vec![set_property("iru13a.metadata-only", "applied")],
    )
    .unwrap();
    let expect = AttributionExpectation::table_commit(uuid, None).unwrap();
    let outcome = endpoints.commit_table(commit, &expect, false).await;
    rest.record("metadata-only", &outcome);
    let receipt = applied(&outcome, MutationEndpoint::TableCommit);
    assert_eq!(receipt.snapshot_id, None);
    assert_eq!(receipt.uuid, Some(uuid));
    let updated = java.load(&ident).await.unwrap();
    assert_eq!(
        updated
            .metadata
            .properties()
            .get("iru13a.metadata-only")
            .map(String::as_str),
        Some("applied")
    );
    assert_eq!(updated.metadata.current_snapshot_id(), Some(snapshot));
    assert_eq!(updated.metadata_location, receipt.metadata_location);

    // A failing requirement: produced before the append, when main was absent.
    let commit = crate::commit::existing_commit::existing_table_commit(
        ident.clone(),
        &created.metadata,
        vec![TableRequirement::RefSnapshotIdMatch {
            r#ref: "main".into(),
            snapshot_id: None,
        }],
        vec![set_property("iru13a.stale", "applied")],
    )
    .unwrap();
    let outcome = endpoints.commit_table(commit, &expect, false).await;
    rest.record("stale-requirement", &outcome);
    assert_eq!(
        outcome.evidence,
        RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::Conflict
        }
    );
    let unchanged = java.load(&ident).await.unwrap();
    assert_eq!(unchanged.metadata_location, updated.metadata_location);
    assert!(!unchanged.metadata.properties().contains_key("iru13a.stale"));

    rest.drop_table("drop", "shapes").await;
    rest.drop_namespace().await;
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13a_java_rest_stage_create_and_ctas_final_commit() {
    let rest = NovaRocksRest::new("NR_IRU13A", "ctas").await;
    let java = &rest.java;
    let endpoints = &rest.endpoints;
    rest.create_namespace().await;
    let ident = java.ident("ctas");

    let outcome = endpoints
        .stage_create(rest.namespace(), create_request("ctas"))
        .await;
    rest.record("stage", &outcome);
    let receipt = applied(&outcome, MutationEndpoint::StageCreate);
    assert_eq!(receipt.metadata_location, None);
    let staged: LoadTableResult =
        serde_json::from_slice(outcome.document.as_deref().expect("applied document")).unwrap();
    assert!(staged.metadata_location.is_none());
    let staged_uuid = staged.metadata.uuid();
    assert_eq!(receipt.uuid, Some(staged_uuid));
    // A stage publishes nothing.
    assert!(!endpoints.table_exists(&ident).await.unwrap());

    // The connector derives the initialization updates, as the stage flow does.
    let initialization_updates = super::staged_create_updates(&staged.metadata).unwrap();
    let operation = java.operation(&staged.metadata);
    let file = java.data(&operation, &staged.metadata, &[7, 8]).await;
    let create = intent(
        java,
        "ctas",
        &staged.metadata,
        &operation,
        RequestShape::Create,
        vec![
            AddedContent::new_logical_data(file, staged.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let attempt = operation.begin_attempt().unwrap();
    let mut engine = StagingEngine::begin(
        StagingBase::Create {
            staged: StagedCreateIdentity::new(operation.token(), Arc::new(staged.metadata.clone())),
            initialization_updates,
        },
        &create,
        &attempt,
    )
    .unwrap();
    engine.stage(&FastAppendPreparer).await.unwrap();
    let request = engine.freeze(&added_references(&create)).unwrap();
    assert_eq!(request.requirements(), &[TableRequirement::NotExist]);
    let snapshot = request.ref_snapshot_after("main").unwrap();
    let marker = (
        "novarocks.ctas.operation-id".to_string(),
        operation.token().path_component(),
    );
    let expect = AttributionExpectation::ctas_final_commit(
        staged_uuid,
        Some(ExpectedSnapshot::new(snapshot, Some("main".into()))),
        vec![marker.clone()],
    )
    .unwrap();
    let outcome = endpoints
        .commit_table(
            commit_of(&request, vec![set_property(&marker.0, &marker.1)]),
            &expect,
            false,
        )
        .await;
    rest.record("ctas-final-commit", &outcome);
    let receipt = applied(&outcome, MutationEndpoint::CtasFinalCommit);
    assert_eq!(receipt.snapshot_id, Some(snapshot));
    assert_eq!(receipt.uuid, Some(staged_uuid));
    assert_eq!(receipt.ref_relation, RefRelation::AtExpectedSnapshot);
    let published = java.load(&ident).await.unwrap();
    assert_eq!(published.metadata.uuid(), staged_uuid);
    assert_eq!(published.metadata.current_snapshot_id(), Some(snapshot));
    assert_eq!(
        published.metadata.properties().get(&marker.0),
        Some(&marker.1)
    );
    assert_eq!(
        published
            .metadata
            .current_snapshot()
            .unwrap()
            .added_rows_count(),
        Some(2)
    );
    assert_eq!(published.metadata_location, receipt.metadata_location);

    // The same final commit again: `assert-create` now fails on the server.
    let outcome = endpoints
        .commit_table(
            commit_of(&request, vec![set_property(&marker.0, &marker.1)]),
            &expect,
            false,
        )
        .await;
    rest.record("ctas-final-commit-again", &outcome);
    assert_eq!(
        outcome.evidence,
        RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::AlreadyExists
        }
    );
    let unchanged = java.load(&ident).await.unwrap();
    assert_eq!(unchanged.metadata_location, published.metadata_location);

    rest.drop_table("drop", "ctas").await;
    rest.drop_namespace().await;
}

fn sql_view(sql: &str) -> ViewRepresentations {
    ViewRepresentations::new(vec![ViewRepresentation::Sql(SqlViewRepresentation {
        sql: sql.to_owned(),
        dialect: "starrocks".to_owned(),
    })])
}

/// The replacement's columns differ from the original's: the vendored view
/// builder records a reused schema as `add-schema` plus a version on schema
/// `-1`, which the Java server refuses with 400 when nothing was added.
fn replaced_view_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .unwrap()
}

fn current_sql(metadata: &ViewMetadata) -> String {
    match metadata.current_version().representations().iter().next() {
        Some(ViewRepresentation::Sql(sql)) => sql.sql.clone(),
        None => panic!("view version has no representation"),
    }
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13a_java_rest_view_create_replace_drop() {
    let rest = NovaRocksRest::new("NR_IRU13A", "views").await;
    let endpoints = &rest.endpoints;
    rest.create_namespace().await;
    let view = rest.java.ident("v");
    let creation = || {
        ViewCreation::builder()
            .name("v".to_owned())
            .location(None)
            .representations(sql_view("select 1 as id"))
            .schema(schema())
            .properties(HashMap::new())
            .default_namespace(rest.namespace().clone())
            .default_catalog(Some("iru13a".to_owned()))
            .summary(HashMap::from([(
                "engine-name".to_owned(),
                "novarocks".to_owned(),
            )]))
            .build()
    };

    assert!(!endpoints.view_exists(&view).await.unwrap());
    let outcome = endpoints.create_view(rest.namespace(), creation()).await;
    rest.record("create-view", &outcome);
    let receipt = applied(&outcome, MutationEndpoint::CreateView);
    let view_uuid = receipt.uuid.expect("view UUID");
    assert!(receipt.metadata_location.is_some());
    assert!(endpoints.view_exists(&view).await.unwrap());
    let current = endpoints.load_view(&view).await.unwrap();
    assert_eq!(current.uuid(), view_uuid);
    assert_eq!(current_sql(&current), "select 1 as id");

    let again = endpoints.create_view(rest.namespace(), creation()).await;
    rest.record("create-view-again", &again);
    assert_eq!(
        again.evidence,
        RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::AlreadyExists
        }
    );

    // Replace exactly as the view statement does: a new current version,
    // with the frozen view UUID as requirement and attribution.
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let version = ViewVersion::builder()
        .with_version_id(1)
        .with_schema_id(replaced_view_schema().schema_id())
        .with_timestamp_ms(timestamp_ms)
        .with_summary(HashMap::from([(
            "engine-name".to_owned(),
            "novarocks".to_owned(),
        )]))
        .with_representations(sql_view("select 2 as id, 'x' as name"))
        .with_default_catalog(Some("iru13a".to_owned()))
        .with_default_namespace(rest.namespace().clone())
        .build();
    let changes = current
        .clone()
        .into_builder()
        .set_current_version(version, replaced_view_schema())
        .unwrap()
        .build()
        .unwrap()
        .changes;
    let replace = |uuid| {
        ViewCommit::builder()
            .ident(view.clone())
            .requirements(vec![ViewRequirement::UuidMatch { uuid }])
            .updates(changes.clone())
            .build()
    };
    let outcome = endpoints
        .replace_view(
            replace(view_uuid),
            &AttributionExpectation::replace_view(view_uuid),
        )
        .await;
    rest.record("replace-view", &outcome);
    let receipt = applied(&outcome, MutationEndpoint::ReplaceView);
    assert_eq!(receipt.uuid, Some(view_uuid));
    let replaced = endpoints.load_view(&view).await.unwrap();
    assert_eq!(replaced.uuid(), view_uuid);
    assert_eq!(current_sql(&replaced), "select 2 as id, 'x' as name");
    assert_eq!(
        replaced.current_version().schema_id(),
        replaced
            .schemas_iter()
            .map(|schema| schema.schema_id())
            .max()
            .unwrap()
    );

    // A replacement produced from another view incarnation is refused.
    let other = Uuid::now_v7();
    let outcome = endpoints
        .replace_view(replace(other), &AttributionExpectation::replace_view(other))
        .await;
    rest.record("replace-view-other-incarnation", &outcome);
    assert_eq!(
        outcome.evidence,
        RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::Conflict
        }
    );
    let unchanged = endpoints.load_view(&view).await.unwrap();
    assert_eq!(
        unchanged.current_version_id(),
        replaced.current_version_id()
    );

    let outcome = endpoints.drop_view(&view).await;
    rest.record("drop-view", &outcome);
    applied(&outcome, MutationEndpoint::DropView);
    let missing = endpoints.load_view(&view).await.unwrap_err();
    assert_eq!(missing.kind(), ConnectorErrorKind::NotFound);
    assert!(!endpoints.view_exists(&view).await.unwrap());
    // D13: a drop's 404 may follow a publication, so it is unknown.
    let outcome = endpoints.drop_view(&view).await;
    rest.record("drop-view-again", &outcome);
    assert!(matches!(
        &outcome.evidence,
        RestEvidence::Unknown { diagnosis }
            if diagnosis.reason == UnknownReason::PostPublishPossible
                && diagnosis.status == Some(404)
    ));
    rest.drop_namespace().await;
}

/// The partition evolution the `ALTER TABLE ... ADD PARTITION COLUMN` path
/// builds from `base`: identity on `id`, guarded by the base default spec.
fn partition_evolution(ident: &TableIdent, base: &TableMetadata) -> TableCommit {
    let base_spec_id = base.default_partition_spec_id();
    let spec = UnboundPartitionSpecBuilder::new()
        .add_partition_fields([UnboundPartitionField {
            source_id: 1,
            field_id: None,
            name: "id_part".to_owned(),
            transform: Transform::Identity,
        }])
        .unwrap()
        .build();
    let build = TableMetadataBuilder::new_from_metadata(base.clone(), None)
        .add_default_partition_spec(spec)
        .unwrap()
        .build()
        .unwrap();
    let new_spec_id = build.metadata.default_partition_spec_id();
    assert_ne!(new_spec_id, base_spec_id);
    let mut updates = build.changes;
    if let Some(TableUpdate::AddSpec { spec }) = updates.first_mut() {
        *spec = build
            .metadata
            .partition_spec_by_id(new_spec_id)
            .unwrap()
            .as_ref()
            .clone()
            .into_unbound();
    }
    crate::commit::existing_commit::existing_table_commit(
        ident.clone(),
        base,
        vec![TableRequirement::DefaultSpecIdMatch {
            default_spec_id: base_spec_id,
        }],
        updates,
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13a_java_rest_recreated_table_rejects_stale_partition_evolution() {
    let rest = NovaRocksRest::new("NR_IRU13A", "recreated").await;
    let java = &rest.java;
    rest.create_namespace().await;
    let ident = java.ident("recreated");
    rest.create_table("create-old", "recreated").await;
    let old = java.load(&ident).await.unwrap();
    rest.drop_table("drop-old", "recreated").await;
    rest.create_table("create-new", "recreated").await;
    let before = java.load(&ident).await.unwrap();
    assert_ne!(before.metadata.uuid(), old.metadata.uuid());
    assert_eq!(
        before.metadata.current_schema(),
        old.metadata.current_schema()
    );
    assert_eq!(
        before.metadata.default_partition_spec_id(),
        old.metadata.default_partition_spec_id()
    );
    assert_eq!(
        before.metadata.default_partition_spec(),
        old.metadata.default_partition_spec()
    );

    // Produced from the dropped incarnation: every requirement but the frozen
    // UUID also holds on the recreated table.
    let adapter = RestTransitionalAdapter::new(Arc::clone(&rest.endpoints));
    let error = adapter
        .update_table(partition_evolution(&ident, &old.metadata))
        .await
        .expect_err("a commit produced from the dropped table must not apply");
    assert_eq!(error.kind(), ErrorKind::CatalogCommitConflicts);
    assert_eq!(
        rest_evidence(&error),
        Some(&RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::Conflict
        })
    );
    java.record(
        "stale-partition-evolution",
        &json!({"evidence": format!("{:?}", rest_evidence(&error))}),
    );
    let after = java.load(&ident).await.unwrap();
    assert_eq!(after.metadata_location, before.metadata_location);
    assert_eq!(
        semantic_facts(&after.metadata),
        semantic_facts(&before.metadata)
    );

    // Control: the same evolution produced from the recreated table applies.
    let table = adapter
        .update_table(partition_evolution(&ident, &before.metadata))
        .await
        .expect("a commit produced from the current table applies");
    assert_eq!(table.metadata().uuid(), before.metadata.uuid());
    let evolved = java.load(&ident).await.unwrap();
    assert_ne!(
        evolved.metadata.default_partition_spec_id(),
        before.metadata.default_partition_spec_id()
    );
    java.record(
        "current-partition-evolution",
        &json!({"default-spec-id": evolved.metadata.default_partition_spec_id()}),
    );

    rest.drop_table("drop-new", "recreated").await;
    rest.drop_namespace().await;
}

// IRU-13b: NovaRocks owns the REST request vocabulary that core patches used
// to provide. Java fixture read-back is the independent oracle.

const IRU13B: &str = "NR_IRU13B";

/// `id` (1), `a` (2), `b` (3): the highest field is the last one.
fn three_column_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "a",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                3,
                "b",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .unwrap()
}

fn three_column_request(name: &str, format_version: u8) -> CreateTableRequest {
    CreateTableRequest {
        name: name.to_owned(),
        location: None,
        schema: three_column_schema(),
        partition_spec: Some(PartitionSpec::unpartition_spec().into_unbound()),
        write_order: Some(SortOrder::unsorted_order()),
        stage_create: None,
        properties: HashMap::from([("format-version".into(), format_version.to_string())]),
    }
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13b_java_rest_ctas_publishes_staged_metadata_for_every_format_version() {
    let rest = NovaRocksRest::new(IRU13B, "ctas").await;
    let java = &rest.java;
    let endpoints = &rest.endpoints;
    rest.create_namespace().await;

    for (format_version, number) in [
        (FormatVersion::V1, 1),
        (FormatVersion::V2, 2),
        (FormatVersion::V3, 3),
    ] {
        let name = format!("ctas_v{number}");
        let ident = java.ident(&name);
        let outcome = endpoints
            .stage_create(rest.namespace(), three_column_request(&name, number))
            .await;
        rest.record(&format!("{name}-stage"), &outcome);
        applied(&outcome, MutationEndpoint::StageCreate);
        let staged: LoadTableResult =
            serde_json::from_slice(outcome.document.as_deref().expect("applied document")).unwrap();
        let staged = staged.metadata;
        assert_eq!(staged.format_version(), format_version);

        let initialization_updates = super::staged_create_updates(&staged).unwrap();
        let operation = java.operation(&staged);
        let file = java.data(&operation, &staged, &[7, 8]).await;
        let create = intent(
            java,
            &name,
            &staged,
            &operation,
            RequestShape::Create,
            vec![AddedContent::new_logical_data(file, staged.default_partition_spec_id()).unwrap()],
            vec![],
            Dependency::NoReadDependency,
        );
        let attempt = operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Create {
                staged: StagedCreateIdentity::new(operation.token(), Arc::new(staged.clone())),
                initialization_updates,
            },
            &create,
            &attempt,
        )
        .unwrap();
        engine.stage(&FastAppendPreparer).await.unwrap();
        let request = engine.freeze(&added_references(&create)).unwrap();
        let body = request.to_rest_json().unwrap();
        java.record(&format!("{name}-request"), &body);
        let add_schema = body["updates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|update| update["action"] == "add-schema")
            .expect("initialization adds the staged schema");
        assert!(add_schema.get("last-column-id").is_none(), "{add_schema}");
        assert!(
            body["updates"]
                .as_array()
                .unwrap()
                .iter()
                .any(|update| update["action"] == "upgrade-format-version"
                    && update["format-version"] == number),
            "every format version is selected explicitly"
        );

        let snapshot = request.ref_snapshot_after("main").unwrap();
        let marker = (
            "novarocks.ctas.operation-id".to_string(),
            operation.token().path_component(),
        );
        let expect = AttributionExpectation::ctas_final_commit(
            staged.uuid(),
            Some(ExpectedSnapshot::new(snapshot, Some("main".into()))),
            vec![marker.clone()],
        )
        .unwrap();
        let outcome = endpoints
            .commit_table(
                commit_of(&request, vec![set_property(&marker.0, &marker.1)]),
                &expect,
                false,
            )
            .await;
        rest.record(&format!("{name}-final-commit"), &outcome);
        let receipt = applied(&outcome, MutationEndpoint::CtasFinalCommit);
        assert_eq!(receipt.snapshot_id, Some(snapshot));

        let published = java.load(&ident).await.unwrap().metadata;
        assert_eq!(published.format_version(), format_version);
        assert_eq!(published.uuid(), staged.uuid());
        assert_eq!(published.location(), staged.location());
        assert_eq!(published.current_schema(), staged.current_schema());
        assert_eq!(
            published.default_partition_spec(),
            staged.default_partition_spec()
        );
        assert_eq!(published.default_sort_order(), staged.default_sort_order());
        let mut properties = published.properties().clone();
        assert_eq!(properties.remove(&marker.0), Some(marker.1.clone()));
        assert_eq!(&properties, staged.properties());
        assert_eq!(
            published.last_column_id(),
            staged.current_schema().highest_field_id()
        );
        assert_eq!(published.last_column_id(), 3);
        assert_eq!(published.current_snapshot_id(), Some(snapshot));
        java.record(
            &format!("{name}-published"),
            &json!({"format-version": number, "last-column-id": published.last_column_id()}),
        );
        rest.drop_table(&format!("{name}-drop"), &name).await;
    }
    rest.drop_namespace().await;
}

fn root_column(name: &str) -> novarocks_spi::connector::ConnectorColumnPath {
    novarocks_spi::connector::ConnectorColumnPath {
        segments: vec![name.into()],
    }
}

fn drop_column(name: &str) -> novarocks_spi::connector::ConnectorSchemaChange {
    novarocks_spi::connector::ConnectorSchemaChange::DropColumn {
        path: root_column(name),
    }
}

impl NovaRocksRest {
    /// Apply one schema change exactly as `ALTER TABLE` builds it, and return
    /// the metadata Java holds afterwards.
    async fn alter(
        &self,
        label: &str,
        name: &str,
        change: novarocks_spi::connector::ConnectorSchemaChange,
    ) -> TableMetadata {
        let ident = self.java.ident(name);
        let base = self.java.load(&ident).await.unwrap().metadata;
        let commit = crate::catalog_control::catalog_mutation::schema_change_commit(
            ident.clone(),
            &base,
            &change,
        )
        .unwrap()
        .expect("the change is not a no-op");
        let expect = AttributionExpectation::table_commit(base.uuid(), None).unwrap();
        let outcome = self.endpoints.commit_table(commit, &expect, false).await;
        self.record(label, &outcome);
        applied(&outcome, MutationEndpoint::TableCommit);
        let altered = self.java.load(&ident).await.unwrap().metadata;
        self.java.record(
            &format!("{label}-result"),
            &json!({
                "last-column-id": altered.last_column_id(),
                "schema": altered.current_schema().as_ref(),
            }),
        );
        altered
    }

    async fn create_three_column_table(&self, name: &str) {
        let outcome = self
            .endpoints
            .create_table(self.namespace(), three_column_request(name, 2))
            .await;
        self.record(&format!("{name}-create"), &outcome);
        applied(&outcome, MutationEndpoint::CreateTable);
    }
}

fn field_ids(metadata: &TableMetadata) -> Vec<(String, i32)> {
    metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.id))
        .collect()
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13b_java_rest_schema_changes_keep_the_column_high_water_mark() {
    let rest = NovaRocksRest::new(IRU13B, "schema").await;
    rest.create_namespace().await;

    // Dropping only the highest field: the high-water mark stays.
    rest.create_three_column_table("drop_highest").await;
    let dropped = rest
        .alter("drop-highest", "drop_highest", drop_column("b"))
        .await;
    assert_eq!(field_ids(&dropped), vec![("id".into(), 1), ("a".into(), 2)]);
    assert_eq!(dropped.last_column_id(), 3);

    // Adding after the drop: the new field is numbered past the old mark.
    rest.create_three_column_table("drop_then_add").await;
    rest.alter("drop-then-add-drop", "drop_then_add", drop_column("b"))
        .await;
    let added = rest
        .alter(
            "drop-then-add-add",
            "drop_then_add",
            novarocks_spi::connector::ConnectorSchemaChange::AddColumn {
                parent: novarocks_spi::connector::ConnectorColumnPath { segments: vec![] },
                column: novarocks_spi::connector::ConnectorColumnDefinition {
                    name: "c".into(),
                    data_type: novarocks_spi::connector::ConnectorDataType::String,
                    nullable: true,
                    aggregation: None,
                    default: None,
                },
                position: novarocks_spi::connector::ConnectorColumnPosition::Default,
            },
        )
        .await;
    assert_eq!(
        field_ids(&added),
        vec![("id".into(), 1), ("a".into(), 2), ("c".into(), 4)]
    );
    assert_eq!(added.last_column_id(), 4);

    // Renaming after the drop adds no field; the mark is unchanged.
    rest.create_three_column_table("drop_then_rename").await;
    rest.alter(
        "drop-then-rename-drop",
        "drop_then_rename",
        drop_column("b"),
    )
    .await;
    let renamed = rest
        .alter(
            "drop-then-rename-rename",
            "drop_then_rename",
            novarocks_spi::connector::ConnectorSchemaChange::RenameColumn {
                path: root_column("a"),
                to: "a2".into(),
            },
        )
        .await;
    assert_eq!(
        field_ids(&renamed),
        vec![("id".into(), 1), ("a2".into(), 2)]
    );
    assert_eq!(renamed.last_column_id(), 3);

    for name in ["drop_highest", "drop_then_add", "drop_then_rename"] {
        rest.drop_table(&format!("{name}-drop"), name).await;
    }
    rest.drop_namespace().await;
}
