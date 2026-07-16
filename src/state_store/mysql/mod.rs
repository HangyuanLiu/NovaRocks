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

pub(crate) mod budget;
pub(crate) mod changes;
pub(crate) mod client;
pub(crate) mod codec;
pub(crate) mod commit;
pub(crate) mod error;
pub(crate) mod identity;
#[cfg(feature = "state-store-test-hooks")]
pub(crate) mod open_test_hooks;
pub(crate) mod range;
pub(crate) mod schema;
pub(crate) mod txn;

#[doc(hidden)]
pub mod test_support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio::time::Instant;

use self::identity::MysqlIdentitySnapshot;
use super::runtime::MysqlProviderHandle;
use super::{
    ChangePage, ChangePollRequest, CommitResolution, ReadTransaction, StateStore, StateStoreError,
    StateStoreErrorKind, StateStoreLimits, StateStoreMetrics, StateStoreMetricsSnapshot,
    StoreIdentity, TransactionId, WriteTransaction,
};

pub(super) struct MysqlStateStore {
    lease: MysqlProviderHandle,
    identity: MysqlIdentitySnapshot,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
}

#[derive(Clone)]
pub(super) struct MysqlOpenCancellation {
    cancelled: Arc<AtomicBool>,
}

impl MysqlStateStore {
    pub(super) async fn open(
        lease: MysqlProviderHandle,
        database: String,
        cluster_id: String,
        limits: StateStoreLimits,
        deadline: Instant,
        cancellation: MysqlOpenCancellation,
    ) -> Result<Self, StateStoreError> {
        let (identity, _) = schema::validate_store_readiness(
            lease.pool(),
            &database,
            &cluster_id,
            limits.max_key_bytes,
            deadline,
            &cancellation,
        )
        .await?;
        cancellation.check()?;
        tracing::info!(
            provider = "mysql",
            client_status = "ready",
            identity_hash = %codec::redacted_identity_hash(
                format!("{database}\0{cluster_id}").as_bytes()
            ),
            "MySQL state store client is ready"
        );
        Ok(Self {
            lease,
            identity,
            limits,
            metrics: Arc::new(StateStoreMetrics::new("mysql")),
        })
    }
}

impl MysqlOpenCancellation {
    pub(super) fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(super) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub(super) fn check(&self) -> Result<(), StateStoreError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "MySQL state store open waiter was cancelled",
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl StateStore for MysqlStateStore {
    fn provider_name(&self) -> &'static str {
        "mysql"
    }

    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.metrics.snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        let operation = self.lease.acquire_operation()?;
        Ok(Box::new(
            txn::begin_read(
                self.lease.pool(),
                operation,
                self.limits.clone(),
                Arc::clone(&self.metrics),
            )
            .await?,
        ))
    }

    async fn begin_write(
        &self,
        transaction_id: TransactionId,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        let operation = self.lease.acquire_operation()?;
        Ok(Box::new(
            txn::begin_write(
                self.lease.pool(),
                operation,
                transaction_id,
                self.limits.clone(),
                Arc::clone(&self.metrics),
            )
            .await?,
        ))
    }

    async fn poll_changes(
        &self,
        request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        changes::poll_changes(self.lease.pool(), &self.identity, request, &self.limits).await
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Ok(self.identity.identity.clone())
    }

    async fn resolve_commit(
        &self,
        transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        let operation = self.lease.acquire_operation()?;
        let codec = codec::MysqlCodec::new(self.limits.max_key_bytes)?;
        let pool = self.lease.pool();
        let transaction_id = *transaction_id;
        let deadline = Instant::now() + self.limits.transaction_deadline;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let _operation = operation;
            let result = commit::resolve_commit(pool, &codec, &transaction_id, deadline).await;
            let _ = sender.send(result);
        });
        receiver.await.map_err(|_| {
            StateStoreError::new(
                StateStoreErrorKind::ProviderUnavailable,
                "MySQL commit resolution supervisor stopped unexpectedly",
            )
        })?
    }
}
