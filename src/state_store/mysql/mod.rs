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

pub(crate) mod client;
pub(crate) mod codec;
pub(crate) mod error;
pub(crate) mod identity;
pub(crate) mod schema;

#[doc(hidden)]
pub mod test_support;

use std::sync::Arc;

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
    _database: String,
    identity: MysqlIdentitySnapshot,
    limits: StateStoreLimits,
    metrics: Arc<StateStoreMetrics>,
}

impl MysqlStateStore {
    pub(super) async fn open(
        lease: MysqlProviderHandle,
        database: String,
        cluster_id: String,
        limits: StateStoreLimits,
        deadline: Instant,
    ) -> Result<Self, StateStoreError> {
        let (identity, _) = schema::validate_store_readiness(
            lease.pool(),
            &database,
            &cluster_id,
            limits.max_key_bytes,
            deadline,
        )
        .await?;
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
            _database: database,
            identity,
            limits,
            metrics: Arc::new(StateStoreMetrics::new("mysql")),
        })
    }

    fn transactions_unavailable() -> StateStoreError {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "MySQL state transactions are not initialized",
        )
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
        let _operation = self.lease.acquire_operation()?;
        Err(Self::transactions_unavailable())
    }

    async fn begin_write(
        &self,
        _transaction_id: TransactionId,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Err(Self::transactions_unavailable())
    }

    async fn poll_changes(
        &self,
        _request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Err(Self::transactions_unavailable())
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Ok(self.identity.identity.clone())
    }

    async fn resolve_commit(
        &self,
        _transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Err(Self::transactions_unavailable())
    }
}
