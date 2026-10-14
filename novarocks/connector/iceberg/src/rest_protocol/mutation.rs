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

//! NovaRocks-issued REST mutations and their D13 evidence.
//!
//! Every mutation is written at most once on the generation's session
//! binding, never follows a redirect, and is classified from what the send
//! observed plus the minimal view of a success body (spec 5.7). A caller that
//! must hand a full object back (the transitional adapter, stage, view
//! writes) keeps the complete success document; owner commits decode only the
//! minimal view, chunk by chunk.

use std::collections::HashMap;

use reqwest::Method;
use reqwest::header::HeaderValue;
use serde::Serialize;

use crate::iceberg::spec::{Schema, ViewVersion};
use crate::iceberg::{
    NamespaceIdent, TableCommit, TableIdent, ViewCommit, ViewCreation, ViewRequirement, ViewUpdate,
};
use crate::iceberg_catalog_rest::{CommitTableRequest, CreateNamespaceRequest, CreateTableRequest};
use crate::rest_protocol::binding::SessionBinding;
use crate::rest_protocol::endpoints::{RestEndpoints, VENDED_CREDENTIALS};
use crate::rest_protocol::evidence::{
    AttributionExpectation, BodyRead, BodyReadFailure, DispatchStage, NotWrittenCause,
    ResponseShape, RestEvidence, classify,
};
use crate::rest_protocol::http::{BodyReadError, RequestContext, ResponseBody, RestRequest};
use crate::rest_protocol::json::{JsonScanner, ScanError, ViewSink, scan_complete};
use crate::rest_protocol::minimal_view::{
    MutationView, NamespaceSink, TableResponseSink, ViewMutationSink,
};

/// `POST .../namespaces/{ns}/views`. NovaRocks owns the view wire shapes: the
/// upstream REST crate has no view API.
#[derive(Serialize)]
#[serde(rename_all = "kebab-case")]
struct CreateViewRequest {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
    schema: Schema,
    view_version: ViewVersion,
    properties: HashMap<String, String>,
}

/// `POST .../namespaces/{ns}/views/{view}`.
#[derive(Serialize)]
struct CommitViewRequest {
    identifier: TableIdent,
    requirements: Vec<ViewRequirement>,
    updates: Vec<ViewUpdate>,
}

/// The classified result of one mutation.
pub(crate) struct MutationOutcome {
    pub(crate) evidence: RestEvidence,
    /// The complete success document, kept only when the caller asked for it
    /// and the response was attributed (`Applied`).
    pub(crate) document: Option<Vec<u8>>,
}

impl RestEndpoints {
    /// Commit to an existing table, or publish a CTAS staged create
    /// (`assert-create`), as decided by `expect`.
    pub(crate) async fn commit_table(
        &self,
        mut commit: TableCommit,
        expect: &AttributionExpectation,
        keep_document: bool,
    ) -> MutationOutcome {
        let ident = commit.identifier().clone();
        let body = CommitTableRequest {
            identifier: Some(ident.clone()),
            requirements: commit.take_requirements(),
            updates: commit.take_updates(),
        };
        self.mutate(
            Method::POST,
            |binding| binding.table_url(&ident),
            Some(&body),
            None,
            expect,
            keep_document,
        )
        .await
    }

    /// Create a table immediately (static access only).
    pub(crate) async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        request: CreateTableRequest,
    ) -> MutationOutcome {
        self.mutate(
            Method::POST,
            |binding| binding.tables_url(namespace),
            Some(&request),
            None,
            &AttributionExpectation::create_table(),
            true,
        )
        .await
    }

    /// Stage a create (`stage-create: true`), asking for vended credentials in
    /// both access modes, as today (L23).
    pub(crate) async fn stage_create(
        &self,
        namespace: &NamespaceIdent,
        mut request: CreateTableRequest,
    ) -> MutationOutcome {
        request.stage_create = Some(true);
        self.mutate(
            Method::POST,
            |binding| binding.tables_url(namespace),
            Some(&request),
            Some(VENDED_CREDENTIALS),
            &AttributionExpectation::stage_create(),
            true,
        )
        .await
    }

    pub(crate) async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> MutationOutcome {
        let request = CreateNamespaceRequest {
            namespace: namespace.clone(),
            properties,
        };
        self.mutate(
            Method::POST,
            SessionBinding::namespaces_url,
            Some(&request),
            None,
            &AttributionExpectation::create_namespace(namespace.as_ref().clone()),
            false,
        )
        .await
    }

    pub(crate) async fn drop_namespace(&self, namespace: &NamespaceIdent) -> MutationOutcome {
        self.mutate::<()>(
            Method::DELETE,
            |binding| binding.namespace_url(namespace),
            None,
            None,
            &AttributionExpectation::drop_namespace(),
            false,
        )
        .await
    }

    /// Drop a table. `purgeRequested` is never sent.
    pub(crate) async fn drop_table(&self, table: &TableIdent) -> MutationOutcome {
        self.mutate::<()>(
            Method::DELETE,
            |binding| binding.table_url(table),
            None,
            None,
            &AttributionExpectation::drop_table(),
            false,
        )
        .await
    }

    /// Create a view. The first version is built exactly as the vendored
    /// client built it.
    pub(crate) async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        creation: ViewCreation,
    ) -> MutationOutcome {
        let request = match view_creation_request(creation) {
            Ok(request) => request,
            Err(cause) => return not_written(cause, &AttributionExpectation::create_view()),
        };
        self.mutate(
            Method::POST,
            |binding| binding.views_url(namespace),
            Some(&request),
            None,
            &AttributionExpectation::create_view(),
            true,
        )
        .await
    }

    /// Replace a view's definition; `expect` carries the frozen view UUID.
    pub(crate) async fn replace_view(
        &self,
        mut commit: ViewCommit,
        expect: &AttributionExpectation,
    ) -> MutationOutcome {
        let ident = commit.identifier().clone();
        let request = CommitViewRequest {
            identifier: ident.clone(),
            requirements: commit.take_requirements(),
            updates: commit.take_updates(),
        };
        self.mutate(
            Method::POST,
            |binding| binding.view_url(&ident),
            Some(&request),
            None,
            expect,
            true,
        )
        .await
    }

    pub(crate) async fn drop_view(&self, view: &TableIdent) -> MutationOutcome {
        self.mutate::<()>(
            Method::DELETE,
            |binding| binding.view_url(view),
            None,
            None,
            &AttributionExpectation::drop_view(),
            false,
        )
        .await
    }

    /// Issue one mutation and classify it.
    async fn mutate<B: Serialize>(
        &self,
        method: Method,
        url: impl FnOnce(&SessionBinding) -> String,
        body: Option<&B>,
        delegation: Option<HeaderValue>,
        expect: &AttributionExpectation,
        keep_document: bool,
    ) -> MutationOutcome {
        let binding = match self.session().await {
            Ok(binding) => binding,
            Err(failure) => {
                return not_written(NotWrittenCause::Binding(failure.step), expect);
            }
        };
        let mut headers = binding.request_headers(delegation);
        if let Err(failure) = self.owner().authorize(&mut headers).await {
            return not_written(NotWrittenCause::Binding(failure.step), expect);
        }
        let request = RestRequest::new(method, url(&binding), headers);
        let request = match body {
            Some(body) => request.with_json_body(body),
            None => Ok(request),
        };
        let (stage, body) = self
            .owner()
            .http()
            .send_once(
                request,
                &[expect.endpoint().success_status()],
                &RequestContext::default(),
            )
            .await;
        let (read, document) = match (expect.endpoint().response_shape(), body) {
            (Some(shape), Some(body)) => read_view(shape, expect, body, keep_document).await,
            _ => (BodyRead::NotRead, None),
        };
        let evidence = classify(&stage, read, expect);
        let document = document.filter(|_| matches!(evidence, RestEvidence::Applied { .. }));
        MutationOutcome { evidence, document }
    }
}

fn not_written(cause: NotWrittenCause, expect: &AttributionExpectation) -> MutationOutcome {
    MutationOutcome {
        evidence: classify(&DispatchStage::NotWritten(cause), BodyRead::NotRead, expect),
        document: None,
    }
}

fn view_creation_request(creation: ViewCreation) -> Result<CreateViewRequest, NotWrittenCause> {
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| NotWrittenCause::Build)?
        .as_millis() as i64;
    let view_version = ViewVersion::builder()
        .with_version_id(1)
        .with_schema_id(creation.schema.schema_id())
        .with_timestamp_ms(timestamp_ms)
        .with_summary(creation.summary)
        .with_representations(creation.representations)
        .with_default_catalog(creation.default_catalog)
        .with_default_namespace(creation.default_namespace)
        .build();
    Ok(CreateViewRequest {
        name: creation.name,
        location: creation.location,
        schema: creation.schema,
        view_version,
        properties: creation.properties,
    })
}

/// Read a success body into the endpoint's minimal view: chunk by chunk, or,
/// when the caller keeps the document, completely and then scanned once.
async fn read_view(
    shape: ResponseShape,
    expect: &AttributionExpectation,
    body: ResponseBody,
    keep_document: bool,
) -> (BodyRead<MutationView>, Option<Vec<u8>>) {
    if keep_document {
        let bytes = match body.read_to_end().await {
            Ok(bytes) => bytes,
            Err(error) => return (BodyRead::Failed(read_failure(error)), None),
        };
        let view = match shape {
            ResponseShape::Table => {
                scan_complete(table_sink(expect), &bytes).map(MutationView::Table)
            }
            ResponseShape::Namespace => {
                scan_complete(NamespaceSink::new(), &bytes).map(MutationView::Namespace)
            }
            ResponseShape::View => {
                scan_complete(ViewMutationSink::new(), &bytes).map(MutationView::View)
            }
        };
        return match view {
            Ok(view) => (BodyRead::Complete(view), Some(bytes)),
            Err(error) => (BodyRead::Failed(BodyReadFailure::Scan(error)), None),
        };
    }
    let view = match shape {
        ResponseShape::Table => stream(body, table_sink(expect))
            .await
            .map(MutationView::Table),
        ResponseShape::Namespace => stream(body, NamespaceSink::new())
            .await
            .map(MutationView::Namespace),
        ResponseShape::View => stream(body, ViewMutationSink::new())
            .await
            .map(MutationView::View),
    };
    match view {
        Ok(view) => (BodyRead::Complete(view), None),
        Err(failure) => (BodyRead::Failed(failure), None),
    }
}

fn table_sink(expect: &AttributionExpectation) -> TableResponseSink {
    TableResponseSink::new(expect.table_view_request().cloned().unwrap_or_default())
}

/// Scan a body chunk by chunk; no raw byte outlives its chunk (L8).
async fn stream<S: ViewSink>(mut body: ResponseBody, sink: S) -> Result<S::View, BodyReadFailure> {
    let mut scanner = JsonScanner::new(sink);
    while let Some(chunk) = body.next_chunk().await.map_err(read_failure)? {
        scanner.feed(&chunk).map_err(scan_failure)?;
    }
    scanner.finish().map_err(scan_failure)
}

fn scan_failure(error: ScanError) -> BodyReadFailure {
    BodyReadFailure::Scan(error)
}

fn read_failure(error: BodyReadError) -> BodyReadFailure {
    match error {
        BodyReadError::DeadlineExceeded => BodyReadFailure::DeadlineExceeded,
        BodyReadError::Transport => BodyReadFailure::Transport,
    }
}
