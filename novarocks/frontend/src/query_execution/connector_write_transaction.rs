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

//! Provider-neutral terminal transaction handoff for distributed writers.
//!
//! The query coordinator owns collection and completeness validation.  Once
//! it produces a [`ConnectorWriteCompletion`], this module is the sole core
//! path that invokes the retained FE control capability.  It deliberately
//! returns the SPI outcome unchanged: provider-specific journal mapping is a
//! caller concern and must not reintroduce an Iceberg carrier into core.

use novarocks_spi::connector::{ConnectorError, ConnectorWriteReceipt, ExternalMutationOutcome};

use crate::query_execution::outcome::ConnectorWriteCompletion;

/// Commit a complete staged writer manifest through exactly the FE generation
/// that planned it.  No registry lookup, generation substitution, or payload
/// reconstruction is permitted here.
#[allow(
    dead_code,
    reason = "The typed connector commit boundary remains available to target-gated write completion paths."
)]
pub(crate) fn commit(
    completion: &ConnectorWriteCompletion,
) -> Result<ExternalMutationOutcome<ConnectorWriteReceipt>, ConnectorError> {
    completion
        .session()
        .commit(completion.commit_context().clone())
}
