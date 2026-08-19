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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_connector_iceberg::catalog_config::{
    IcebergCatalogConfiguration, parse_catalog_configuration_with_object_store_binding,
};
use novarocks_connector_iceberg::catalog_runtime::build_hadoop_catalog;
use novarocks_connector_iceberg::iceberg::spec::{
    FormatVersion, NestedField, PrimitiveType, Schema, TableMetadata, Type,
};
use novarocks_connector_iceberg::iceberg::{
    Catalog, ErrorKind, NamespaceIdent, TableCreation, TableIdent,
};
use novarocks_connector_iceberg::novarocks_fs::ObjectStoreConfig;
use sha2::{Digest, Sha256};

const CHILD_ENV: &str = "NR_HADOOP_FENCE_CHILD";
const WAREHOUSE_ENV: &str = "NR_HADOOP_FENCE_WAREHOUSE";
const NAMESPACE_ENV: &str = "NR_HADOOP_FENCE_NAMESPACE";
const TABLE_ENV: &str = "NR_HADOOP_FENCE_TABLE";
const POLICY_ENV: &str = "NR_HADOOP_FENCE_POLICY";
const BARRIER_ENV: &str = "NR_HADOOP_FENCE_BARRIER";
const CHILD_INDEX_ENV: &str = "NR_HADOOP_FENCE_CHILD_INDEX";
const RESULT_PREFIX: &str = "HADOOP_FENCE_RESULT ";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Policy {
    Strict,
    NoOp,
}

impl Policy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::NoOp => "noop",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "strict" => Self::Strict,
            "noop" => Self::NoOp,
            other => panic!("unknown child create policy: {other}"),
        }
    }
}

#[derive(Debug)]
struct ChildResult {
    pid: u32,
    outcome: String,
    uuid: String,
    location: String,
}

#[derive(Debug)]
struct PrefixAudit {
    uuid: String,
    location: String,
    digest: String,
    entries: Vec<String>,
}

#[test]
fn local_strict_two_process_fencing() {
    let warehouse = tempfile::tempdir().expect("local warehouse");
    run_race(
        &warehouse.path().to_string_lossy(),
        "local_strict",
        Policy::Strict,
    );
}

#[test]
fn local_noop_two_process_fencing() {
    let warehouse = tempfile::tempdir().expect("local warehouse");
    run_race(
        &warehouse.path().to_string_lossy(),
        "local_noop",
        Policy::NoOp,
    );
}

#[test]
#[ignore = "requires the shared MinIO fixture"]
fn minio_strict_and_noop_two_process_fencing() {
    let warehouse = std::env::var("NR_HADOOP_FENCE_MINIO_WAREHOUSE")
        .expect("MinIO runner must set NR_HADOOP_FENCE_MINIO_WAREHOUSE");
    run_race(&warehouse, "minio_strict", Policy::Strict);
    run_race(&warehouse, "minio_noop", Policy::NoOp);
}

/// The parent process invokes this exact libtest entry in two independent
/// processes. Running it during an ordinary test invocation is a no-op.
#[test]
fn hadoop_fencing_child_process_entry() {
    if std::env::var_os(CHILD_ENV).is_none() {
        return;
    }

    let warehouse = required_env(WAREHOUSE_ENV);
    let namespace = required_env(NAMESPACE_ENV);
    let table_name = required_env(TABLE_ENV);
    let policy = Policy::parse(&required_env(POLICY_ENV));
    let barrier = PathBuf::from(required_env(BARRIER_ENV));
    let child_index = required_env(CHILD_INDEX_ENV);

    let config = catalog_configuration(&warehouse);
    let catalog = build_hadoop_catalog(&config).expect("build child Hadoop catalog");
    let namespace_ident = NamespaceIdent::new(namespace.clone());
    let table_ident = TableIdent::new(namespace_ident.clone(), table_name.clone());

    std::fs::write(barrier.join(format!("ready-{child_index}")), b"ready\n")
        .expect("publish child barrier readiness");
    wait_for_both_children(&barrier);

    let runtime = tokio::runtime::Runtime::new().expect("child runtime");
    let outcome = runtime.block_on(async {
        match catalog
            .create_table(&namespace_ident, table_creation(&table_name))
            .await
        {
            Ok(_) => "applied",
            Err(error) if error.kind() == ErrorKind::TableAlreadyExists => match policy {
                Policy::Strict => "conflict",
                Policy::NoOp => "noop",
            },
            Err(error) => panic!("child create returned an unexpected error: {error}"),
        }
    });
    let loaded = runtime
        .block_on(catalog.load_table(&table_ident))
        .expect("load authoritative table after child create");
    let location = loaded
        .metadata_location()
        .expect("Hadoop table has a metadata location");
    println!(
        "{RESULT_PREFIX}pid={} outcome={outcome} uuid={} location={location}",
        std::process::id(),
        loaded.metadata().uuid()
    );
}

fn run_race(warehouse: &str, table_name: &str, policy: Policy) {
    let namespace = "fencing";
    let metadata_location = format!(
        "{}/{}/{}/metadata/v1.metadata.json",
        warehouse.trim_end_matches('/'),
        namespace,
        table_name
    );
    let config = catalog_configuration(warehouse);
    let access = novarocks_connector_iceberg::fs_io::resolve_access_for_location(
        &metadata_location,
        config.object_store_config.as_ref(),
    )
    .expect("resolve v1 metadata access");
    assert!(
        access.supports_conditional_create(),
        "storage must advertise native create-if-absent for {metadata_location}"
    );

    let barrier = tempfile::tempdir().expect("process barrier");
    let mut children = Vec::new();
    for index in 0..2 {
        let child = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "hadoop_fencing_child_process_entry",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_ENV, "1")
            .env(WAREHOUSE_ENV, warehouse)
            .env(NAMESPACE_ENV, namespace)
            .env(TABLE_ENV, table_name)
            .env(POLICY_ENV, policy.as_str())
            .env(BARRIER_ENV, barrier.path())
            .env(CHILD_INDEX_ENV, index.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn independent catalog child");
        children.push(child);
    }

    let outputs = children
        .into_iter()
        .map(|child| child.wait_with_output().expect("wait for catalog child"))
        .collect::<Vec<_>>();
    let results = outputs.iter().map(parse_child_result).collect::<Vec<_>>();
    assert_ne!(results[0].pid, results[1].pid, "children must be processes");
    assert_eq!(
        results
            .iter()
            .filter(|result| result.outcome == "applied")
            .count(),
        1,
        "exactly one process must own v1: {results:?}"
    );
    let expected_other = match policy {
        Policy::Strict => "conflict",
        Policy::NoOp => "noop",
    };
    assert_eq!(
        results
            .iter()
            .filter(|result| result.outcome == expected_other)
            .count(),
        1,
        "loser outcome must follow create policy: {results:?}"
    );
    assert_eq!(results[0].uuid, results[1].uuid);
    assert_eq!(results[0].location, results[1].location);

    let audit = audit_prefix(&config, namespace, table_name);
    assert_eq!(audit.uuid, results[0].uuid);
    assert_eq!(audit.location, results[0].location);
    assert!(
        audit.location.ends_with(&format!(
            "/{namespace}/{table_name}/metadata/v1.metadata.json"
        )),
        "canonical metadata location must retain the Hadoop v1 suffix: {}",
        audit.location
    );
    assert_eq!(
        audit.entries,
        vec!["v1.metadata.json", "version-hint.text"],
        "the successful create must leave one v1 and its hint"
    );
    println!(
        "HADOOP_FENCE_AUDIT policy={} warehouse={} pids={},{} uuid={} location={} digest={} entries={}",
        policy.as_str(),
        warehouse,
        results[0].pid,
        results[1].pid,
        audit.uuid,
        audit.location,
        audit.digest,
        audit.entries.join(",")
    );
}

fn audit_prefix(
    config: &IcebergCatalogConfiguration,
    namespace: &str,
    table_name: &str,
) -> PrefixAudit {
    let catalog = build_hadoop_catalog(config).expect("build audit Hadoop catalog");
    let ident = TableIdent::from_strs([namespace, table_name]).expect("audit table identity");
    let runtime = tokio::runtime::Runtime::new().expect("audit runtime");
    let table = runtime
        .block_on(catalog.load_table(&ident))
        .expect("load authoritative table for audit");
    let location = table
        .metadata_location()
        .expect("audit table metadata location")
        .to_string();
    let access = novarocks_connector_iceberg::fs_io::resolve_access_for_location(
        &location,
        config.object_store_config.as_ref(),
    )
    .expect("resolve audit metadata access");
    let relative_path = access
        .single_relative_path()
        .expect("single audit metadata path")
        .to_string();
    let metadata_dir = Path::new(&relative_path)
        .parent()
        .expect("metadata parent")
        .to_string_lossy()
        .replace('\\', "/");
    let metadata_dir = format!("{}/", metadata_dir.trim_end_matches('/'));
    let operator = access.operator();
    let bytes = runtime
        .block_on(operator.read(&relative_path))
        .expect("read authoritative v1")
        .to_bytes();
    let decoded: TableMetadata =
        serde_json::from_slice(&bytes).expect("decode authoritative v1 metadata");
    let mut entries = runtime
        .block_on(operator.list(&metadata_dir))
        .expect("list metadata prefix")
        .into_iter()
        .filter(|entry| entry.metadata().mode().is_file())
        .map(|entry| {
            entry
                .path()
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .expect("metadata entry name")
                .to_string()
        })
        .collect::<Vec<_>>();
    entries.sort();
    PrefixAudit {
        uuid: decoded.uuid().to_string(),
        location,
        digest: hex_digest(&bytes),
        entries,
    }
}

fn parse_child_result(output: &Output) -> ChildResult {
    if !output.status.success() {
        panic!(
            "catalog child failed with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8(output.stdout.clone()).expect("child stdout is UTF-8");
    let line = stdout
        .lines()
        .find_map(|line| line.find(RESULT_PREFIX).map(|offset| &line[offset..]))
        .unwrap_or_else(|| panic!("child emitted no machine-readable result:\n{stdout}"));
    let fields = line[RESULT_PREFIX.len()..]
        .split_ascii_whitespace()
        .map(|field| {
            field
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid child result field: {field}"))
        })
        .collect::<BTreeMap<_, _>>();
    ChildResult {
        pid: fields["pid"].parse().expect("child pid"),
        outcome: fields["outcome"].to_string(),
        uuid: fields["uuid"].to_string(),
        location: fields["location"].to_string(),
    }
}

fn wait_for_both_children(barrier: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !(barrier.join("ready-0").exists() && barrier.join("ready-1").exists()) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for both independent child processes"
        );
        // This sleep only avoids busy-polling the readiness barrier. Both
        // children pass the same barrier before the storage request, so it
        // cannot select or order the eventual v1 owner.
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn catalog_configuration(warehouse: &str) -> IcebergCatalogConfiguration {
    let properties = vec![
        ("type".to_string(), "iceberg".to_string()),
        ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
        (
            "iceberg.catalog.warehouse".to_string(),
            warehouse.to_string(),
        ),
    ];
    let object_store_config = object_store_config(warehouse);
    parse_catalog_configuration_with_object_store_binding(
        "hadoop_fencing",
        &properties,
        object_store_config.as_ref(),
    )
    .expect("parse Hadoop catalog configuration")
}

fn object_store_config(warehouse: &str) -> Option<ObjectStoreConfig> {
    if !warehouse.starts_with("s3://") {
        return None;
    }
    Some(ObjectStoreConfig {
        endpoint: required_env("AWS_S3_ENDPOINT"),
        access_key_id: required_env("AWS_S3_ACCESS_KEY_ID"),
        access_key_secret: required_env("AWS_S3_SECRET_ACCESS_KEY"),
        session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        enable_path_style_access: Some(true),
        region: std::env::var("AWS_REGION").ok(),
        retry_max_times: None,
        retry_min_delay_ms: None,
        retry_max_delay_ms: None,
        timeout_ms: None,
        io_timeout_ms: None,
    })
}

fn table_creation(name: &str) -> TableCreation {
    let schema = Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .expect("test schema");
    TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .format_version(FormatVersion::V2)
        .build()
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("required environment variable {name} is unset"))
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
