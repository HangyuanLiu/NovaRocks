// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde_json::Value;

const MANIFEST_KEY: &str = "novarocks.documents.v1";

/// Read the product's own REST metadata while the runner-owned isolated
/// catalog and native cluster are still alive. SQL result rows alone cannot
/// prove the D/L/P attachment graph or exact output binding.
pub(crate) fn assert_graph(suite: &str, directive: &str) -> Result<String> {
    ensure!(
        suite == "mv-storage-contract",
        "@mv_rest_document_graph requires the isolated mv-storage-contract suite"
    );
    let (target, count) = directive
        .split_once(",publications=")
        .context("@mv_rest_document_graph requires <namespace>.<table>,publications=<count>")?;
    let expected = count
        .parse::<usize>()
        .context("invalid publication count")?;
    ensure!(
        expected == 0 || expected >= 2,
        "document graph oracle requires zero or at least two publications"
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
    verify_graph(&value, expected)
}

fn verify_graph(response: &Value, expected: usize) -> Result<String> {
    let metadata = response
        .get("metadata")
        .context("REST response lacks metadata")?;
    let table_manifest = manifest_at(&metadata["properties"], "table metadata")?;
    let mut table_docs = HashMap::new();
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
    Ok(format!(
        "{expected} exact P attachments share table-level D/L; current={current}"
    ))
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
