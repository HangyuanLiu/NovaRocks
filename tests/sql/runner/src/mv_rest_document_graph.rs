// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use opendal::Operator;
use serde_json::Value;
use sha2::{Digest, Sha256};

const MANIFEST_KEY: &str = "novarocks.documents.v1";

pub(crate) struct GraphExpectation<'a> {
    pub(crate) namespace: &'a str,
    pub(crate) table: &'a str,
    pub(crate) publications: usize,
    pub(crate) table_commits: Option<usize>,
    pub(crate) failed_table_commits: usize,
    deferred_sidecars_min: usize,
    metadata_only_last: bool,
    full_overwrite_last: bool,
    append_last: bool,
}

/// Read the product's own REST metadata while the runner-owned isolated
/// catalog and native cluster are still alive. SQL result rows alone cannot
/// prove the D/L/P attachment graph or exact output binding.
pub(crate) fn assert_graph(suite: &str, directive: &str) -> Result<String> {
    ensure!(
        matches!(
            suite,
            "mv-storage-contract" | "mv-storage-physical-occ" | "mv-publication-v11"
        ),
        "@mv_rest_document_graph requires an isolated MV publication suite"
    );
    let expectation = parse_expectation(directive)?;
    let (namespace, table) = (expectation.namespace, expectation.table);
    let rest =
        std::env::var("NOVAROCKS_ICEBERG_REST_URI").context("isolated REST URI is unavailable")?;
    let url = format!(
        "{}/v1/namespaces/{namespace}/tables/{table}",
        rest.trim_end_matches('/')
    );
    let response = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?
        .get(&url)
        .send()
        .with_context(|| format!("read exact MV REST metadata at {url}"))?
        .error_for_status()
        .with_context(|| format!("REST did not return MV metadata at {url}"))?;
    let value: Value = response.json().context("decode MV REST metadata")?;
    verify_graph(&value, &expectation)
}

pub(crate) fn parse_expectation(directive: &str) -> Result<GraphExpectation<'_>> {
    let (target, parameters) = directive
        .split_once(",publications=")
        .context("@mv_rest_document_graph requires <namespace>.<table>,publications=<count>")?;
    let mut parameters = parameters.split(',');
    let count = parameters.next().context("missing publication count")?;
    let mut metadata_only_last = false;
    let mut full_overwrite_last = false;
    let mut append_last = false;
    let mut table_commits = None;
    let mut failed_table_commits = None;
    let mut deferred_sidecars_min = None;
    for parameter in parameters {
        match parameter {
            "metadata-only-last=true" if !metadata_only_last => metadata_only_last = true,
            "full-overwrite-last=true" if !full_overwrite_last => full_overwrite_last = true,
            "append-last=true" if !append_last => append_last = true,
            value if value.starts_with("table-commits=") && table_commits.is_none() => {
                table_commits = Some(
                    value["table-commits=".len()..]
                        .parse::<usize>()
                        .context("invalid table-commits count")?,
                );
            }
            value
                if value.starts_with("failed-table-commits=") && failed_table_commits.is_none() =>
            {
                failed_table_commits = Some(
                    value["failed-table-commits=".len()..]
                        .parse::<usize>()
                        .context("invalid failed-table-commits count")?,
                );
            }
            value
                if value.starts_with("deferred-sidecars-min=")
                    && deferred_sidecars_min.is_none() =>
            {
                deferred_sidecars_min = Some(
                    value["deferred-sidecars-min=".len()..]
                        .parse::<usize>()
                        .context("invalid deferred-sidecars-min count")?,
                );
            }
            _ => anyhow::bail!("unknown or repeated MV document graph parameter {parameter}"),
        }
    }
    let publications = count
        .parse::<usize>()
        .context("invalid publication count")?;
    ensure!(
        !metadata_only_last || publications >= 2,
        "metadata-only-last requires at least two publications"
    );
    ensure!(
        !full_overwrite_last || publications >= 2,
        "full-overwrite-last requires at least two publications"
    );
    ensure!(
        !append_last || publications >= 2,
        "append-last requires at least two publications"
    );
    ensure!(
        usize::from(metadata_only_last)
            + usize::from(full_overwrite_last)
            + usize::from(append_last)
            <= 1,
        "last publication can have only one expected physical write shape"
    );
    ensure!(
        table_commits.is_none_or(|commits| commits >= publications + 1),
        "table-commits cannot be less than CREATE plus publication commits"
    );
    let (namespace, table) = target
        .split_once('.')
        .context("document graph target requires <namespace>.<table>")?;
    ensure!(
        [namespace, table]
            .iter()
            .all(|part| !part.is_empty()
                && part.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')),
        "document graph target has an invalid REST identifier"
    );
    Ok(GraphExpectation {
        namespace,
        table,
        publications,
        table_commits,
        failed_table_commits: failed_table_commits.unwrap_or_default(),
        deferred_sidecars_min: deferred_sidecars_min.unwrap_or_default(),
        metadata_only_last,
        full_overwrite_last,
        append_last,
    })
}

fn verify_graph(response: &Value, expectation: &GraphExpectation<'_>) -> Result<String> {
    let expected = expectation.publications;
    let metadata = response
        .get("metadata")
        .context("REST response lacks metadata")?;
    let table_manifest = manifest_at(&metadata["properties"], "table metadata")?;
    let mut table_docs = HashMap::new();
    let mut retained_docs = Vec::new();
    for document in documents(&table_manifest, "table metadata")? {
        let name = document["name"]
            .as_str()
            .context("table document lacks name")?;
        ensure!(
            document["attachment"]["kind"] == "table-metadata",
            "{name} is not a table-level attachment"
        );
        ensure!(
            table_docs.insert(name, document).is_none(),
            "duplicate table document {name}"
        );
        retained_docs.push(document.clone());
    }
    ensure!(
        table_docs.contains_key("definition")
            && table_docs.contains_key("interpretation")
            && table_docs.contains_key("configuration"),
        "table metadata lacks D, L or C"
    );

    let snapshots = metadata["snapshots"]
        .as_array()
        .context("REST metadata lacks snapshots")?;
    if expected == 0 {
        ensure!(
            snapshots.is_empty(),
            "unpublished MV already has a snapshot"
        );
        ensure!(
            metadata["current-snapshot-id"].is_null(),
            "unpublished MV already has a current snapshot"
        );
        ensure!(
            !table_docs.contains_key("publication"),
            "unpublished MV has a table-level P"
        );
        verify_deferred_sidecars(metadata, &retained_docs, expectation.deferred_sidecars_min)?;
        return Ok("D/L/C present with no snapshot or P".to_string());
    }
    ensure!(
        snapshots.len() == expected,
        "expected {expected} retained publication snapshots, observed {}",
        snapshots.len()
    );
    let mut revisions = Vec::with_capacity(expected);
    let mut ids = Vec::with_capacity(expected);
    for snapshot in snapshots {
        let snapshot_id = snapshot["snapshot-id"]
            .as_i64()
            .context("snapshot lacks exact ID")?;
        let manifest = manifest_at(&snapshot["summary"], "snapshot summary")?;
        let docs = documents(&manifest, "snapshot summary")?;
        ensure!(
            docs.len() == 1,
            "snapshot {snapshot_id} has {} documents, expected one P",
            docs.len()
        );
        let publication = &docs[0];
        retained_docs.push(publication.clone());
        ensure!(
            publication["name"] == "publication",
            "snapshot {snapshot_id} lacks P"
        );
        ensure!(
            publication["attachment"]["kind"] == "exact-output"
                && publication["attachment"]["snapshot_id"].as_i64() == Some(snapshot_id),
            "P on snapshot {snapshot_id} does not bind that exact output"
        );
        let references = publication["references"]
            .as_array()
            .context("P lacks references")?;
        ensure!(
            references.len() == 2,
            "P on snapshot {snapshot_id} does not have exactly D/L references"
        );
        let mut seen = HashMap::new();
        for reference in references {
            let name = reference["name"]
                .as_str()
                .context("P reference lacks name")?;
            ensure!(
                seen.insert(name, ()).is_none(),
                "P repeats reference {name}"
            );
            let table_document = table_docs
                .get(name)
                .with_context(|| format!("P references absent table document {name}"))?;
            ensure!(
                reference["revision"] == table_document["revision"],
                "P on snapshot {snapshot_id} does not reference exact {name} revision"
            );
        }
        ensure!(
            seen.contains_key("definition") && seen.contains_key("interpretation"),
            "P on snapshot {snapshot_id} does not reference D and L"
        );
        revisions.push(publication["revision"].clone());
        ids.push(snapshot_id);
    }
    for (index, revision) in revisions.iter().enumerate() {
        ensure!(
            !revisions[..index].contains(revision),
            "two retained outputs carry the same P revision"
        );
    }
    let current = metadata["current-snapshot-id"]
        .as_i64()
        .context("REST metadata has no current snapshot")?;
    ensure!(ids.contains(&current), "current snapshot has no exact P");
    if expectation.metadata_only_last {
        let last = snapshots
            .last()
            .context("missing last publication snapshot")?;
        ensure!(
            last["snapshot-id"].as_i64() == Some(current),
            "metadata-only publication is not the current snapshot"
        );
        let summary = &last["summary"];
        ensure!(
            summary["added-data-files"] == "0"
                && summary["added-records"] == "0"
                && (summary["deleted-data-files"].is_null()
                    || summary["deleted-data-files"] == "0"),
            "metadata-only publication added or deleted data files or records: {summary}"
        );
    }
    if expectation.full_overwrite_last {
        let last = snapshots
            .last()
            .context("missing full-overwrite snapshot")?;
        ensure!(
            last["snapshot-id"].as_i64() == Some(current),
            "full overwrite is not the current snapshot"
        );
        ensure!(
            last["summary"]["operation"] == "overwrite",
            "FULL refresh did not publish an Iceberg overwrite: {}",
            last["summary"]
        );
    }
    if expectation.append_last {
        let last = snapshots.last().context("missing append snapshot")?;
        ensure!(
            last["snapshot-id"].as_i64() == Some(current),
            "append publication is not the current snapshot"
        );
        let summary = &last["summary"];
        ensure!(
            summary["operation"] == "append"
                && summary["added-data-files"]
                    .as_str()
                    .and_then(|value| value.parse::<u64>().ok())
                    .is_some_and(|count| count > 0),
            "incremental publication did not append data files: {summary}"
        );
    }
    verify_deferred_sidecars(metadata, &retained_docs, expectation.deferred_sidecars_min)?;
    Ok(format!(
        "{expected} exact P attachments share table-level D/L; current={current}; metadata-only-last={}; full-overwrite-last={}; append-last={}",
        expectation.metadata_only_last, expectation.full_overwrite_last, expectation.append_last
    ))
}

fn verify_deferred_sidecars(metadata: &Value, documents: &[Value], minimum: usize) -> Result<()> {
    if minimum == 0 {
        return Ok(());
    }
    let endpoint = std::env::var("AWS_S3_ENDPOINT").context("MinIO endpoint is unavailable")?;
    let access_key =
        std::env::var("AWS_S3_ACCESS_KEY_ID").context("MinIO access key is unavailable")?;
    let secret_key =
        std::env::var("AWS_S3_SECRET_ACCESS_KEY").context("MinIO secret key is unavailable")?;
    let table_location = metadata["location"]
        .as_str()
        .context("REST MV metadata has no table location")?;
    let (table_bucket, _) = s3_bucket_and_key(table_location)?;
    let operator = Operator::new(
        opendal::services::S3::default()
            .endpoint(&endpoint)
            .bucket(table_bucket)
            .region("us-east-1")
            .access_key_id(&access_key)
            .secret_access_key(&secret_key),
    )?
    .finish();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let mut deferred = 0usize;
    for document in documents {
        let carrier = &document["carrier"];
        if carrier["kind"] != "deferred" {
            continue;
        }
        deferred += 1;
        let location = carrier["location"]
            .as_str()
            .context("deferred MV document has no location")?;
        let (bucket, key) = s3_bucket_and_key(location)?;
        ensure!(
            bucket == table_bucket,
            "MV sidecar is outside its table bucket"
        );
        let content = runtime
            .block_on(operator.read(key))
            .with_context(|| format!("read retained MV sidecar {location}"))?
            .to_bytes();
        let encoded_len = document["encoded_len"]
            .as_u64()
            .context("deferred MV document has no encoded length")?;
        ensure!(
            content.len() as u64 == encoded_len,
            "MV sidecar {location} has the wrong encoded length"
        );
        let revision: Vec<u8> = serde_json::from_value(document["revision"].clone())
            .context("deferred MV document has an invalid revision")?;
        ensure!(
            revision.as_slice() == Sha256::digest(&content).as_slice(),
            "MV sidecar {location} does not match its document revision"
        );
    }
    ensure!(
        deferred >= minimum,
        "expected at least {minimum} deferred MV sidecars, observed {deferred}"
    );
    Ok(())
}

fn s3_bucket_and_key(location: &str) -> Result<(&str, &str)> {
    let path = location
        .strip_prefix("s3://")
        .with_context(|| format!("MV sidecar location is not S3: {location}"))?;
    let (bucket, key) = path
        .split_once('/')
        .with_context(|| format!("MV sidecar location has no object key: {location}"))?;
    ensure!(
        !bucket.is_empty() && !key.is_empty(),
        "invalid S3 location {location}"
    );
    Ok((bucket, key))
}

fn manifest_at(properties: &Value, owner: &str) -> Result<Value> {
    let encoded = properties[MANIFEST_KEY]
        .as_str()
        .with_context(|| format!("{owner} lacks {MANIFEST_KEY}"))?;
    serde_json::from_str(encoded).with_context(|| format!("decode {owner} document manifest"))
}

fn documents<'a>(manifest: &'a Value, owner: &str) -> Result<&'a [Value]> {
    let version = manifest["version"].as_u64();
    ensure!(
        version == Some(1),
        "{owner} has unsupported document manifest version {version:?}"
    );
    manifest["documents"]
        .as_array()
        .map(Vec::as_slice)
        .with_context(|| format!("{owner} manifest lacks documents"))
}
