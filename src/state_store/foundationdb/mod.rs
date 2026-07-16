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

mod codec;
mod identity;

use async_trait::async_trait;
use uuid::Uuid;

use self::codec::KeyspaceCodec;
use self::identity::open_identity;
use super::runtime::ProviderHandle;
use super::{
    ChangePage, ChangePollRequest, CommitResolution, ReadTransaction, StateStore, StateStoreError,
    StateStoreErrorKind, StateStoreLimits, StateStoreMetrics, StateStoreMetricsSnapshot,
    StoreIdentity, TransactionId, WriteTransaction,
};

pub(super) struct FoundationDbStateStore {
    lease: ProviderHandle,
    codec: KeyspaceCodec,
    identity: StoreIdentity,
    limits: StateStoreLimits,
    metrics: StateStoreMetrics,
}

impl FoundationDbStateStore {
    pub async fn open(
        lease: ProviderHandle,
        limits: StateStoreLimits,
        cluster_id: String,
        keyspace_id: Uuid,
    ) -> Result<Self, StateStoreError> {
        let database = lease.database()?;
        let codec = KeyspaceCodec::new(keyspace_id);
        let identity = open_identity(database.as_ref(), &codec, &cluster_id)
            .await?
            .identity;
        Ok(Self {
            lease,
            codec,
            identity,
            limits,
            metrics: StateStoreMetrics::new("foundationdb"),
        })
    }

    fn unavailable() -> StateStoreError {
        StateStoreError::new(
            StateStoreErrorKind::ProviderUnavailable,
            "FoundationDB state transactions are not initialized",
        )
    }
}

#[async_trait]
impl StateStore for FoundationDbStateStore {
    fn provider_name(&self) -> &'static str {
        "foundationdb"
    }

    fn limits(&self) -> &StateStoreLimits {
        &self.limits
    }

    fn metrics_snapshot(&self) -> StateStoreMetricsSnapshot {
        self.metrics.snapshot()
    }

    async fn begin_read(&self) -> Result<Box<dyn ReadTransaction>, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        let _ = &self.codec;
        Err(Self::unavailable())
    }

    async fn begin_write(
        &self,
        _transaction_id: TransactionId,
        _purpose: &str,
    ) -> Result<Box<dyn WriteTransaction>, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Err(Self::unavailable())
    }

    async fn poll_changes(
        &self,
        _request: &ChangePollRequest,
    ) -> Result<ChangePage, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Err(Self::unavailable())
    }

    async fn identity(&self) -> Result<StoreIdentity, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Ok(self.identity.clone())
    }

    async fn resolve_commit(
        &self,
        _transaction_id: &TransactionId,
    ) -> Result<CommitResolution, StateStoreError> {
        let _operation = self.lease.acquire_operation()?;
        Err(Self::unavailable())
    }
}
