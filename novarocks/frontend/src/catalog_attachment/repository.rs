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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId};
use novarocks_spi::state_store::{
    CommitResolution, Direction, KeyRange, Precondition, RangeRequest, StateRecord, StateStore,
    StateStoreError, StateStoreErrorKind, VersionToken,
};
use novarocks_state_store::metrics::StateStoreMetrics;
use novarocks_state_store::{OperationId, RunFailure, run_side_effect_free};
use uuid::Uuid;

use super::codec::{StoredCatalogAttachment, StoredProperty, decode, encode};
use super::key::{attachment_key, attachment_prefix};

const DEFAULT_ATTACHMENT_SCAN_PAGE_SIZE: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogAttachment {
    pub attachment_id: Uuid,
    pub instance_id: ConnectorInstanceId,
    pub provider_id: ConnectorProviderId,
    pub display_name: String,
    pub durable_properties: Vec<(String, String)>,
    pub created_at_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogAttachmentVersioned {
    pub attachment: CatalogAttachment,
    pub version: VersionToken,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CatalogAttachmentErrorKind {
    InvalidRequest,
    NotFound,
    AlreadyExists,
    Conflict,
    Corruption,
    Unavailable,
    CommitUnknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogAttachmentError {
    kind: CatalogAttachmentErrorKind,
    message: String,
}

impl CatalogAttachmentError {
    pub fn kind(&self) -> CatalogAttachmentErrorKind {
        self.kind
    }

    fn new(kind: CatalogAttachmentErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for CatalogAttachmentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for CatalogAttachmentError {}

#[derive(Clone)]
pub struct CatalogAttachmentRepository {
    store: Arc<dyn StateStore>,
    metrics: Arc<StateStoreMetrics>,
}

impl CatalogAttachmentRepository {
    pub async fn open(store: Arc<dyn StateStore>) -> Result<Self, CatalogAttachmentError> {
        let repository = Self {
            metrics: Arc::new(StateStoreMetrics::new(
                novarocks_spi::state_store::StateStoreProviderId::new("frontend-catalog"),
            )),
            store,
        };
        repository.list().await?;
        Ok(repository)
    }

    pub async fn get(
        &self,
        instance_id: &ConnectorInstanceId,
    ) -> Result<Option<CatalogAttachmentVersioned>, CatalogAttachmentError> {
        let key = attachment_key(instance_id).map_err(invalid)?;
        let mut transaction = self.store.begin_read().await.map_err(store)?;
        let record = transaction.get(&key).await.map_err(store)?;
        transaction.abort().await.map_err(store)?;
        record.map(decode_record).transpose()
    }

    pub async fn list(&self) -> Result<Vec<CatalogAttachmentVersioned>, CatalogAttachmentError> {
        self.list_with_page_size(DEFAULT_ATTACHMENT_SCAN_PAGE_SIZE)
            .await
    }

    pub async fn list_with_page_size(
        &self,
        page_size: usize,
    ) -> Result<Vec<CatalogAttachmentVersioned>, CatalogAttachmentError> {
        if page_size == 0 || page_size > self.store.limits().max_page_size {
            return Err(CatalogAttachmentError::new(
                CatalogAttachmentErrorKind::InvalidRequest,
                "catalog attachment scan page size is outside StateStore limits",
            ));
        }
        let prefix = attachment_prefix().map_err(invalid)?;
        let range = KeyRange::for_prefix(prefix).map_err(store)?;
        let mut transaction = self.store.begin_read().await.map_err(store)?;
        let mut request = RangeRequest {
            range,
            direction: Direction::Forward,
            page_size,
            continuation: None,
        };
        let mut attachments = Vec::new();
        loop {
            let page = transaction.range(&request).await.map_err(store)?;
            attachments.extend(
                page.records
                    .into_iter()
                    .map(decode_record)
                    .collect::<Result<Vec<_>, _>>()?,
            );
            let Some(continuation) = page.continuation else {
                break;
            };
            request.continuation = Some(continuation);
        }
        transaction.abort().await.map_err(store)?;
        attachments.sort_by(|left, right| {
            left.attachment
                .instance_id
                .cmp(&right.attachment.instance_id)
        });
        Ok(attachments)
    }

    pub async fn create(
        &self,
        attachment: CatalogAttachment,
    ) -> Result<CatalogAttachmentVersioned, CatalogAttachmentError> {
        validate_attachment(&attachment)?;
        let operation_id = OperationId::new_v7();
        self.create_with_operation(operation_id, attachment).await
    }

    pub async fn drop_exact(
        &self,
        expected: CatalogAttachmentVersioned,
    ) -> Result<(), CatalogAttachmentError> {
        let operation_id = OperationId::new_v7();
        self.drop_with_operation(operation_id, expected).await
    }

    async fn create_with_operation(
        &self,
        operation_id: OperationId,
        attachment: CatalogAttachment,
    ) -> Result<CatalogAttachmentVersioned, CatalogAttachmentError> {
        let key = attachment_key(&attachment.instance_id).map_err(invalid)?;
        let value = Bytes::from(encode(&stored_from(&attachment)).map_err(corruption)?);
        let outcome = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            operation_id,
            "create catalog attachment",
            |transaction| {
                let key = key.clone();
                let value = value.clone();
                Box::pin(async move {
                    transaction
                        .put(key, value.try_into()?, Precondition::Absent)
                        .await?;
                    Ok(())
                })
            },
        )
        .await;
        match outcome {
            Ok(_) => self.require_matching(&attachment).await,
            Err(RunFailure::Operation(error))
                if error.kind() == StateStoreErrorKind::PreconditionFailed =>
            {
                Err(CatalogAttachmentError::new(
                    CatalogAttachmentErrorKind::AlreadyExists,
                    "catalog attachment already exists",
                ))
            }
            Err(RunFailure::RetryExhausted(error))
                if error.kind() == StateStoreErrorKind::PreconditionFailed =>
            {
                Err(CatalogAttachmentError::new(
                    CatalogAttachmentErrorKind::AlreadyExists,
                    "catalog attachment already exists",
                ))
            }
            Err(RunFailure::CommitUnknown {
                transaction_id,
                error,
            }) => {
                self.resolve_create_unknown(operation_id, transaction_id, attachment, error)
                    .await
            }
            Err(error) => Err(run_failure("create catalog attachment", error)),
        }
    }

    async fn resolve_create_unknown(
        &self,
        operation_id: OperationId,
        transaction_id: novarocks_spi::state_store::TransactionId,
        attachment: CatalogAttachment,
        original: StateStoreError,
    ) -> Result<CatalogAttachmentVersioned, CatalogAttachmentError> {
        match self
            .store
            .resolve_commit(&transaction_id)
            .await
            .map_err(store)?
        {
            CommitResolution::Committed(_) => self.require_matching(&attachment).await,
            CommitResolution::NotCommitted => {
                Box::pin(self.create_with_operation(operation_id, attachment)).await
            }
            CommitResolution::Unresolved => match self.matching(&attachment).await? {
                Some(found) => Ok(found),
                None => Err(CatalogAttachmentError::new(
                    CatalogAttachmentErrorKind::CommitUnknown,
                    format!("create catalog attachment commit outcome is unknown: {original}"),
                )),
            },
        }
    }

    async fn drop_with_operation(
        &self,
        operation_id: OperationId,
        expected: CatalogAttachmentVersioned,
    ) -> Result<(), CatalogAttachmentError> {
        let key = attachment_key(&expected.attachment.instance_id).map_err(invalid)?;
        let version = expected.version.clone();
        let outcome = run_side_effect_free(
            self.store.as_ref(),
            self.metrics.as_ref(),
            operation_id,
            "drop catalog attachment",
            |transaction| {
                let key = key.clone();
                let version = version.clone();
                Box::pin(async move {
                    transaction
                        .delete(key, Precondition::Version(version))
                        .await?;
                    Ok(())
                })
            },
        )
        .await;
        match outcome {
            Ok(_) => Ok(()),
            Err(RunFailure::Operation(error))
                if error.kind() == StateStoreErrorKind::PreconditionFailed =>
            {
                Err(CatalogAttachmentError::new(
                    CatalogAttachmentErrorKind::Conflict,
                    "catalog attachment changed before drop",
                ))
            }
            Err(RunFailure::RetryExhausted(error))
                if error.kind() == StateStoreErrorKind::PreconditionFailed =>
            {
                Err(CatalogAttachmentError::new(
                    CatalogAttachmentErrorKind::Conflict,
                    "catalog attachment changed before drop",
                ))
            }
            Err(RunFailure::CommitUnknown {
                transaction_id,
                error,
            }) => {
                match self
                    .store
                    .resolve_commit(&transaction_id)
                    .await
                    .map_err(store)?
                {
                    CommitResolution::Committed(_) => Ok(()),
                    CommitResolution::NotCommitted => {
                        Box::pin(self.drop_with_operation(operation_id, expected)).await
                    }
                    CommitResolution::Unresolved => {
                        match self.get(&expected.attachment.instance_id).await? {
                            Some(current)
                                if current.attachment.attachment_id
                                    == expected.attachment.attachment_id =>
                            {
                                Err(CatalogAttachmentError::new(
                                    CatalogAttachmentErrorKind::CommitUnknown,
                                    format!(
                                        "drop catalog attachment commit outcome is unknown: {error}"
                                    ),
                                ))
                            }
                            _ => Ok(()),
                        }
                    }
                }
            }
            Err(error) => Err(run_failure("drop catalog attachment", error)),
        }
    }

    async fn matching(
        &self,
        expected: &CatalogAttachment,
    ) -> Result<Option<CatalogAttachmentVersioned>, CatalogAttachmentError> {
        Ok(self
            .get(&expected.instance_id)
            .await?
            .filter(|current| current.attachment.attachment_id == expected.attachment_id))
    }

    async fn require_matching(
        &self,
        expected: &CatalogAttachment,
    ) -> Result<CatalogAttachmentVersioned, CatalogAttachmentError> {
        self.matching(expected).await?.ok_or_else(|| {
            CatalogAttachmentError::new(
                CatalogAttachmentErrorKind::CommitUnknown,
                "catalog attachment commit resolved but authoritative record does not match",
            )
        })
    }
}

/// Re-check frozen attachment observations inside a caller-owned StateStore
/// write transaction.  This is deliberately crate-visible rather than a
/// repository cross-call: the caller owns the transaction which couples an MV
/// definition/index write to catalog attachment existence.
pub(crate) async fn assert_attachment_versions(
    transaction: &mut dyn novarocks_spi::state_store::WriteTransaction,
    expected: &[CatalogAttachmentVersioned],
) -> Result<(), StateStoreError> {
    for expected in expected {
        let key = attachment_key(&expected.attachment.instance_id).map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::InvalidRequest,
                "invalid catalog attachment observation key",
            )
        })?;
        let Some(record) = transaction.get(&key).await? else {
            return Err(StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "catalog attachment disappeared before materialized view write",
            ));
        };
        if record.version != expected.version {
            return Err(StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "catalog attachment changed before materialized view write",
            ));
        }
        let current = decode_record(record).map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::Corruption,
                "catalog attachment observation is corrupt",
            )
        })?;
        if current.attachment.attachment_id != expected.attachment.attachment_id {
            return Err(StateStoreError::new(
                StateStoreErrorKind::Conflict,
                "catalog attachment identity changed before materialized view write",
            ));
        }
    }
    Ok(())
}

fn decode_record(
    record: StateRecord,
) -> Result<CatalogAttachmentVersioned, CatalogAttachmentError> {
    let stored = decode(record.value.as_bytes()).map_err(corruption)?;
    let attachment = attachment_from(stored)?;
    let expected_key = attachment_key(&attachment.instance_id).map_err(corruption)?;
    if record.key != expected_key {
        return Err(corruption(
            "catalog attachment key does not match record identity",
        ));
    }
    Ok(CatalogAttachmentVersioned {
        attachment,
        version: record.version,
    })
}

fn stored_from(attachment: &CatalogAttachment) -> StoredCatalogAttachment {
    StoredCatalogAttachment {
        attachment_id: attachment.attachment_id.to_string(),
        instance_id: attachment.instance_id.as_str().to_string(),
        provider_id: attachment.provider_id.as_str().to_string(),
        display_name: attachment.display_name.clone(),
        durable_properties: attachment
            .durable_properties
            .iter()
            .map(|(key, value)| StoredProperty {
                key: key.clone(),
                value: value.clone(),
            })
            .collect(),
        created_at_ms: attachment.created_at_ms,
    }
}

fn attachment_from(
    stored: StoredCatalogAttachment,
) -> Result<CatalogAttachment, CatalogAttachmentError> {
    let attachment = CatalogAttachment {
        attachment_id: Uuid::parse_str(&stored.attachment_id)
            .map_err(|error| corruption(format!("invalid catalog attachment UUID: {error}")))?,
        instance_id: ConnectorInstanceId::parse(&stored.instance_id)
            .map_err(|error| corruption(error.to_string()))?,
        provider_id: ConnectorProviderId::parse(&stored.provider_id)
            .map_err(|error| corruption(error.to_string()))?,
        display_name: stored.display_name,
        durable_properties: stored
            .durable_properties
            .into_iter()
            .map(|property| (property.key, property.value))
            .collect(),
        created_at_ms: stored.created_at_ms,
    };
    validate_attachment(&attachment).map_err(|error| {
        CatalogAttachmentError::new(CatalogAttachmentErrorKind::Corruption, error.message)
    })?;
    Ok(attachment)
}

fn validate_attachment(attachment: &CatalogAttachment) -> Result<(), CatalogAttachmentError> {
    if attachment.display_name.trim().is_empty() {
        return Err(invalid("catalog attachment display name must not be empty"));
    }
    let mut previous = None;
    let mut keys = BTreeSet::new();
    for (key, _) in &attachment.durable_properties {
        if key.trim().is_empty() {
            return Err(invalid("catalog attachment property key must not be empty"));
        }
        if !keys.insert(key.as_str()) {
            return Err(invalid(format!(
                "duplicate catalog attachment property: {key}"
            )));
        }
        if previous.is_some_and(|last: &str| last >= key.as_str()) {
            return Err(invalid(
                "catalog attachment properties must be sorted by key",
            ));
        }
        let normalized = key.to_ascii_lowercase();
        if [
            "password",
            "secret",
            "token",
            "credential",
            "access-key",
            "access_key",
            "private-key",
            "private_key",
        ]
        .iter()
        .any(|marker| normalized.contains(marker))
        {
            return Err(invalid(format!(
                "credential-like catalog attachment property cannot be durable: {key}"
            )));
        }
        previous = Some(key);
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> CatalogAttachmentError {
    CatalogAttachmentError::new(CatalogAttachmentErrorKind::InvalidRequest, message)
}

fn corruption(message: impl Into<String>) -> CatalogAttachmentError {
    CatalogAttachmentError::new(CatalogAttachmentErrorKind::Corruption, message)
}

fn store(error: StateStoreError) -> CatalogAttachmentError {
    CatalogAttachmentError::new(CatalogAttachmentErrorKind::Unavailable, error.to_string())
}

fn run_failure(context: &str, failure: RunFailure) -> CatalogAttachmentError {
    let (kind, message) = match failure {
        RunFailure::Operation(error) if error.kind() == StateStoreErrorKind::PreconditionFailed => {
            (CatalogAttachmentErrorKind::Conflict, error.to_string())
        }
        RunFailure::CommitUnknown { error, .. } => {
            (CatalogAttachmentErrorKind::CommitUnknown, error.to_string())
        }
        error => (
            CatalogAttachmentErrorKind::Unavailable,
            format!("{error:?}"),
        ),
    };
    CatalogAttachmentError::new(kind, format!("{context}: {message}"))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use novarocks_spi::state_store::{
        FeDeploymentView, StateStore,
        conformance::{FaultGate, FaultInjectingStateStore},
    };
    use novarocks_state_store::{
        SQLITE_STATE_STORE_PROVIDER_ID, StateStoreAppConfig, StateStoreConfig, StateStoreHost,
        StateStoreHostConfig, StateStoreLimitOverrides, StateStoreProviderConfig,
        builtin_state_store_provider_registry,
    };

    use super::*;

    fn attachment(properties: Vec<(String, String)>) -> CatalogAttachment {
        CatalogAttachment {
            attachment_id: Uuid::now_v7(),
            instance_id: ConnectorInstanceId::parse("Warehouse.Main").expect("instance"),
            provider_id: ConnectorProviderId::parse("iceberg").expect("provider"),
            display_name: "Warehouse.Main".to_string(),
            durable_properties: properties,
            created_at_ms: 1,
        }
    }

    #[test]
    fn durable_properties_are_sorted_and_non_sensitive() {
        assert!(validate_attachment(&attachment(vec![("type".into(), "iceberg".into())])).is_ok());
        assert_eq!(
            validate_attachment(&attachment(vec![("password".into(), "x".into())]))
                .expect_err("credential-like property must fail")
                .kind(),
            CatalogAttachmentErrorKind::InvalidRequest
        );
        assert!(
            validate_attachment(&attachment(vec![
                ("z".into(), "1".into()),
                ("a".into(), "2".into()),
            ]))
            .is_err()
        );
    }

    #[tokio::test]
    async fn sqlite_create_is_absent_cas_and_drop_requires_the_frozen_version() {
        let directory = tempfile::tempdir().expect("temporary SQLite StateStore directory");
        let registry =
            builtin_state_store_provider_registry().expect("builtin StateStore registry");
        let mut host = StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "catalog-attachment-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: directory.path().join("state-store.sqlite"),
                            deployment_owner: "catalog-attachment-test".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).expect("non-zero FE count"),
                topology_revision: Bytes::from_static(b"catalog-attachment-test-r1"),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        assert_eq!(host.provider_id(), SQLITE_STATE_STORE_PROVIDER_ID);
        let store = host.state_store().expect("ready StateStore");
        let repository = CatalogAttachmentRepository::open(Arc::clone(&store))
            .await
            .expect("open catalog attachment repository");

        let first = repository
            .create(attachment(vec![("type".into(), "iceberg".into())]))
            .await
            .expect("first create");
        assert_eq!(
            repository
                .create(attachment(vec![("type".into(), "iceberg".into())]))
                .await
                .expect_err("second create must conflict")
                .kind(),
            CatalogAttachmentErrorKind::AlreadyExists
        );
        repository
            .drop_exact(first.clone())
            .await
            .expect("exact drop");
        let replacement = repository
            .create(attachment(vec![("type".into(), "iceberg".into())]))
            .await
            .expect("recreate after drop");
        assert_ne!(
            first.attachment.attachment_id,
            replacement.attachment.attachment_id
        );
        assert_eq!(
            repository
                .drop_exact(first)
                .await
                .expect_err("stale exact delete must conflict")
                .kind(),
            CatalogAttachmentErrorKind::Conflict
        );

        drop(repository);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }

    #[tokio::test]
    async fn commit_response_loss_recovers_create_and_exact_drop_without_new_identity() {
        let directory = tempfile::tempdir().expect("temporary SQLite StateStore directory");
        let registry =
            builtin_state_store_provider_registry().expect("builtin StateStore registry");
        let mut host = StateStoreHost::open(
            &registry,
            StateStoreHostConfig {
                state_store: StateStoreAppConfig {
                    store: StateStoreConfig {
                        cluster_id: "catalog-attachment-commit-unknown-test".to_string(),
                        limits: StateStoreLimitOverrides::default(),
                        provider: StateStoreProviderConfig::Sqlite {
                            path: directory.path().join("state-store.sqlite"),
                            deployment_owner: "catalog-attachment-commit-unknown-test".to_string(),
                        },
                    },
                    mysql_client: None,
                },
                foundationdb_client: None,
            },
            FeDeploymentView {
                active_fe_count: NonZeroUsize::new(1).expect("non-zero FE count"),
                topology_revision: Bytes::from_static(b"catalog-attachment-commit-unknown-r1"),
            },
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .expect("open SQLite StateStore");
        let store = host.state_store().expect("ready StateStore");
        let fault = FaultInjectingStateStore::new(Arc::clone(&store));
        let fault_store: Arc<dyn StateStore> = fault.clone();
        let repository = CatalogAttachmentRepository::open(fault_store)
            .await
            .expect("open catalog attachment repository");

        let requested = attachment(vec![("type".into(), "iceberg".into())]);
        let create_gate = FaultGate::new();
        fault.lose_next_post_dispatch_response(create_gate.clone());
        let create_task = tokio::spawn({
            let repository = repository.clone();
            let requested = requested.clone();
            async move { repository.create(requested).await }
        });
        create_gate.wait_reached().await;
        create_gate.release().await;
        let created = create_task
            .await
            .expect("create task joins")
            .expect("commit resolution recovers create");
        assert_eq!(created.attachment.attachment_id, requested.attachment_id);
        assert_eq!(repository.list().await.expect("list attachments").len(), 1);

        let drop_gate = FaultGate::new();
        fault.lose_next_post_dispatch_response(drop_gate.clone());
        let drop_task = tokio::spawn({
            let repository = repository.clone();
            async move { repository.drop_exact(created).await }
        });
        drop_gate.wait_reached().await;
        drop_gate.release().await;
        drop_task
            .await
            .expect("drop task joins")
            .expect("commit resolution recovers exact drop");
        assert!(repository.list().await.expect("list after drop").is_empty());

        drop(repository);
        drop(fault);
        drop(store);
        host.shutdown(Instant::now() + Duration::from_secs(5))
            .await
            .expect("shutdown SQLite StateStore");
    }
}
