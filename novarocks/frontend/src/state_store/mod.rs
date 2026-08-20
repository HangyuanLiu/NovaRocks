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

//! Frontend-owned StateStore consumer runtime.

pub mod coordination;
mod host;
pub mod host_error;
pub mod metrics;
pub mod provider;
pub mod runner;

#[cfg(test)]
pub(crate) mod testing;

pub use host::{StateStoreHost, StateStoreHostLifecycle};
pub use host_error::{StateStoreHostError, StateStoreHostErrorKind};
pub use provider::{
    StateStoreHostInput, StateStoreProviderRegistration, StateStoreProviderRegistry,
};
pub use runner::{
    OperationId, RunFailure, RunSuccess, derive_transaction_id, run_side_effect_free,
};
