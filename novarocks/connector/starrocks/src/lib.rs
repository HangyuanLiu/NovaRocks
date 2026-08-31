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

//! Provider-private StarRocks external-connector contracts.
//!
//! This crate owns StarRocks metadata resolution and the control/execution
//! binding implementations.  It deliberately does not depend on a NovaRocks
//! host, Compat, or a concrete StarRocks wire client.
//!
//! # The connector has no read capability
//!
//! Every read entry point returns [`STARROCKS_READ_UNSUPPORTED`]. The
//! capability is absent by decision, not broken: when connector reads became a
//! typed contract, StarRocks had no typed read semantics to express, and
//! letting an immature connector shape the generic read model was the worse
//! trade. Restoring a StarRocks read requires its own accepted contract that
//! defines those semantics first, and only then a typed read handle and split.
//!
//! What was removed together with the untyped read stack, so that contract
//! knows what it is restoring instead of rediscovering it:
//!
//! - `rpc::brpc`, `rpc::flight`, `rpc::codec`: the BRPC-chunk and Arrow Flight
//!   batch readers, the remote endpoint and transport ports they were opened
//!   through, and the frozen RPC split codec carrying their remote output
//!   bindings.
//! - The remote scan lifecycle of [`remote_control`]: `prepare_scan`,
//!   `start_scan` and `cleanup_sessions`, plus the read-session lease that
//!   started and finalized one remote scan session. Only the metadata half of
//!   that HTTP API survives here.
//! - `direct::*`: the whole shared-data direct read — tablet planning and
//!   location resolution, the StarOS V1 shard/file-store client, the frozen
//!   direct split codec, and the segment/page/wire storage kernel that decoded
//!   StarRocks rowsets into Arrow.
//! - The provider-private StarOS, tablet-storage and remote-scan protobuf IDL
//!   those readers spoke, and the build script that compiled it.

mod codec;
mod domain;

pub mod control;
pub mod execution;
pub mod remote_control;
pub mod role_binding;

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};

pub use control::{StarRocksControlGeneration, StarRocksMetadataSource};
pub use domain::{
    StarRocksCapabilitySnapshot, StarRocksConnectorConfig, StarRocksLocalBindingRef,
    StarRocksResolvedTable,
};
pub use execution::StarRocksCatalogRuntimeMaterializer;
pub use remote_control::{
    StarRocksHttpRequest, StarRocksHttpTransport, StarRocksRemoteControlClient,
    StarRocksRemoteControlConfig, StarRocksRemoteMetadataSource,
};
pub use role_binding::{StarRocksControlRoleBindingFactory, StarRocksExecutionRoleBindingFactory};

pub const STARROCKS_PROVIDER_ID: &str = "starrocks";
pub const STARROCKS_CONTRACT_VERSION: u16 = 1;

/// The stable refusal returned by every StarRocks read entry point.
///
/// It is one constant so that a caller sees the same answer from scan
/// planning on the frontend and from split preparation on a backend, and so
/// that the absence is greppable from both sides of the process boundary.
pub const STARROCKS_READ_UNSUPPORTED: &str =
    "StarRocks connector reads are not supported: no typed StarRocks read contract exists yet";

/// Builds the refusal every StarRocks read entry point returns before it
/// inspects its request, so no argument can turn it into another error.
pub(crate) fn starrocks_read_unsupported() -> ConnectorError {
    ConnectorError::new(ConnectorErrorKind::Unsupported, STARROCKS_READ_UNSUPPORTED)
}
