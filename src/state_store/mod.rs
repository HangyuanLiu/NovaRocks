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

pub mod config;
pub mod contract;
pub mod error;
pub mod limits;
pub mod metrics;
pub mod range;
pub mod runner;

pub use config::{StateStoreConfig, StateStoreProviderConfig};
pub use contract::{
    ChangeHint, ChangePage, ChangePollRequest, CommitOutcome, CommitReceipt, CommitResolution,
    FeDeploymentView, Key, OperationId, Precondition, RangePage, ReadTransaction, StateRecord,
    StateStore, StoreIdentity, StoreRevision, TransactionId, Value, VersionToken, WriteTransaction,
};
pub use error::{StateStoreError, StateStoreErrorKind};
pub use limits::{StateStoreLimitOverrides, StateStoreLimits};
pub use metrics::{
    STATE_STORE_OPERATION_COUNT, STATE_STORE_OUTCOME_COUNT, StateStoreMetrics,
    StateStoreMetricsSnapshot, StateStoreOperation, StateStoreOutcome,
};
pub use range::{ChangeCursor, ContinuationToken, Direction, KeyRange, RangeRequest};
pub use runner::{RunFailure, RunSuccess, derive_transaction_id, run_side_effect_free};
