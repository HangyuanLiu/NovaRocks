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

//! Backend attempt-owner injection for the Native runtime-filter bridge.

//! Backend owns exact query-context installation and retirement. Native
//! Adapter owns the envelope/transport bridge it installs around a Worker
//! participant; this module deliberately keeps only the narrow factory port
//! needed by the attempt owner and its tests.

use std::sync::Arc;

use novarocks_native_adapter::{
    BackendDataRuntime,
    runtime_filter_install::DecodedRuntimeFilterContribution,
    runtime_filter_participant::{NativeRuntimeFilterParticipantFactory, RuntimeFilterParticipant},
};
use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_worker::RuntimeFilterContractError;

/// Backend-private injection port for an attempt-owned participant.
///
/// The context host exclusively owns the returned participant and performs
/// exact attempt lookup. Implementations may construct only the Native bridge
/// around a Worker participant; they may not recover a participant by query
/// id or replace the host's ownership decision.
pub(crate) trait RuntimeFilterParticipantFactory: Send + Sync + 'static {
    fn install(
        &self,
        execution_id: QueryExecutionId,
        contribution: DecodedRuntimeFilterContribution,
    ) -> Result<Arc<RuntimeFilterParticipant>, RuntimeFilterContractError>;
}

/// Production composition of the Backend-owned factory port.
pub(crate) struct BackendRuntimeFilterParticipantFactory {
    native: NativeRuntimeFilterParticipantFactory,
}

impl BackendRuntimeFilterParticipantFactory {
    pub(crate) fn new(runtime: BackendDataRuntime) -> Self {
        Self {
            native: NativeRuntimeFilterParticipantFactory::new(runtime),
        }
    }
}

impl RuntimeFilterParticipantFactory for BackendRuntimeFilterParticipantFactory {
    fn install(
        &self,
        execution_id: QueryExecutionId,
        contribution: DecodedRuntimeFilterContribution,
    ) -> Result<Arc<RuntimeFilterParticipant>, RuntimeFilterContractError> {
        self.native.install(execution_id, contribution)
    }
}
