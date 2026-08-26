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

//! Single-dispatch commit mechanisms shared by the concrete catalogs.
//!
//! Design: ADR-0110 (docs/adr/ADR-0110-iceberg-provider-private-catalog-owner.md)

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

use crate::iceberg::{Catalog, TableCommit, TableIdent};

use super::transaction::{CatalogCommitDispatch, CommitProof, StagedCommit};

/// Publishes to an existing table with exactly one `update_table`.
///
/// This is the plain path all three catalogs share. It deliberately does not
/// go through the vendored `Transaction::commit`, which would retry on
/// `Error::retryable()` and therefore resend a request whose outcome is
/// unknown.
///
/// Adjudication looks for the publication's own snapshot-summary marker. It is
/// keyed on the exact marker rather than on a snapshot id, because a snapshot
/// id says only "something committed" while the marker says "*this* attempt
/// committed". Absence is never treated as proof.
#[derive(Debug)]
pub(super) struct UpdateTableDispatch {
    client: Arc<dyn Catalog>,
    ident: TableIdent,
    /// Property key and value that identify this exact publication, when the
    /// operation stamps one. Without it, adjudication cannot answer and must
    /// keep saying "unknown" rather than guess.
    marker: Option<(Arc<str>, Arc<str>)>,
}

impl UpdateTableDispatch {
    pub(super) fn new(
        client: Arc<dyn Catalog>,
        ident: TableIdent,
        marker: Option<(Arc<str>, Arc<str>)>,
    ) -> Self {
        Self {
            client,
            ident,
            marker,
        }
    }
}

#[async_trait]
impl CatalogCommitDispatch for UpdateTableDispatch {
    async fn dispatch_once(
        &self,
        staged: StagedCommit,
    ) -> Result<CommitProof, crate::iceberg::Error> {
        if staged.is_empty() {
            // Nothing to publish, and nothing was sent. This is a proven no-op
            // rather than a commit, and it must not reach the catalog.
            return Ok(CommitProof::no_op());
        }
        let mut commit = TableCommit::builder()
            .ident(self.ident.clone())
            .updates(staged.updates)
            .requirements(staged.requirements)
            .build();
        let _ = &mut commit;
        let table = self.client.update_table(commit).await?;
        let snapshot_id = table
            .metadata()
            .current_snapshot()
            .map(|snapshot| snapshot.snapshot_id());
        Ok(CommitProof {
            snapshot_id,
            table_uuid: Some(Arc::from(table.metadata().uuid().to_string())),
            effect: novarocks_spi::connector::ExternalMutationEffect::Applied,
        })
    }

    async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError> {
        let Some((key, value)) = &self.marker else {
            // No marker was stamped, so this publication left nothing that
            // distinguishes it from any other. Refusing to answer is the only
            // honest result; claiming absence here would authorize cleanup of
            // data that may be live.
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "this Iceberg publication stamped no operation marker, so its outcome cannot be \
                 adjudicated after the fact",
            ));
        };
        let table = self
            .client
            .load_table(&self.ident)
            .await
            .map_err(|error| super::error::map_read_error(&error))?;
        let table_uuid: Arc<str> = Arc::from(table.metadata().uuid().to_string());
        let mut matched = None;
        for snapshot in table.metadata().snapshots() {
            if snapshot.summary().additional_properties.get(key.as_ref())
                == Some(&value.to_string())
            {
                if matched.is_some() {
                    return Err(ConnectorError::new(
                        ConnectorErrorKind::CorruptData,
                        "more than one Iceberg snapshot carries the same publication marker",
                    ));
                }
                matched = Some(snapshot.snapshot_id());
            }
        }
        Ok(matched.map(|snapshot_id| CommitProof {
            snapshot_id: Some(snapshot_id),
            table_uuid: Some(table_uuid.clone()),
            effect: novarocks_spi::connector::ExternalMutationEffect::Applied,
        }))
    }
}

/// Creates a table with exactly one `create_table`.
///
/// Used by catalogs whose create is already a single atomic catalog request.
/// The Hadoop implementation does not use this: its linearization point is a
/// conditional metadata write in storage (ADR-0077), not a catalog call.
#[derive(Debug)]
pub(super) struct CreateTableDispatch {
    client: Arc<dyn Catalog>,
    namespace: crate::iceberg::NamespaceIdent,
    creation: std::sync::Mutex<Option<crate::iceberg::TableCreation>>,
    ident: TableIdent,
}

impl CreateTableDispatch {
    pub(super) fn new(
        client: Arc<dyn Catalog>,
        namespace: crate::iceberg::NamespaceIdent,
        creation: crate::iceberg::TableCreation,
        ident: TableIdent,
    ) -> Self {
        Self {
            client,
            namespace,
            creation: std::sync::Mutex::new(Some(creation)),
            ident,
        }
    }
}

#[async_trait]
impl CatalogCommitDispatch for CreateTableDispatch {
    async fn dispatch_once(
        &self,
        _staged: StagedCommit,
    ) -> Result<CommitProof, crate::iceberg::Error> {
        let creation = self
            .creation
            .lock()
            .map_err(|_| {
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::Unexpected,
                    "Iceberg create-table dispatch state was poisoned",
                )
            })?
            .take()
            .ok_or_else(|| {
                // The transaction already refuses a second commit; this is the
                // belt-and-braces guard at the mechanism itself.
                crate::iceberg::Error::new(
                    crate::iceberg::ErrorKind::Unexpected,
                    "Iceberg create-table dispatch was already consumed",
                )
            })?;
        let table = self.client.create_table(&self.namespace, creation).await?;
        Ok(CommitProof {
            snapshot_id: table
                .metadata()
                .current_snapshot()
                .map(|snapshot| snapshot.snapshot_id()),
            table_uuid: Some(Arc::from(table.metadata().uuid().to_string())),
            effect: novarocks_spi::connector::ExternalMutationEffect::Applied,
        })
    }

    async fn adjudicate(&self) -> Result<Option<CommitProof>, ConnectorError> {
        // A create is adjudicated by presence of the target itself. Any table
        // at the identity proves *a* create landed; whether it was this attempt
        // is settled by the caller comparing the expected UUID, which is why
        // the UUID travels in the proof.
        match self.client.load_table(&self.ident).await {
            Ok(table) => Ok(Some(CommitProof {
                snapshot_id: table
                    .metadata()
                    .current_snapshot()
                    .map(|snapshot| snapshot.snapshot_id()),
                table_uuid: Some(Arc::from(table.metadata().uuid().to_string())),
                effect: novarocks_spi::connector::ExternalMutationEffect::Applied,
            })),
            Err(error)
                if matches!(
                    error.kind(),
                    crate::iceberg::ErrorKind::TableNotFound
                        | crate::iceberg::ErrorKind::NamespaceNotFound
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(super::error::map_read_error(&error)),
        }
    }
}
