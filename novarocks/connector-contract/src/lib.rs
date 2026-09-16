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

//! Neutral, immutable connector vocabulary embedded in physical plans.
//!
//! This crate owns value identity only. Provider services, Arrow runtime
//! values, codecs, and application error policy remain outside this boundary.

mod catalog;
mod codec;
mod identity;
mod mutation;
mod read;
mod write;

pub use catalog::{CATALOG_VERSION_BYTES, CatalogHandle, CatalogVersion};
pub use codec::{
    ConnectorCodecCategory, ConnectorCodecContractError, ConnectorCodecRevision,
    ConnectorEncodedPayload, ConnectorEnvelopeHeader, ConnectorReadRelationPayload,
};
pub use identity::{
    ConnectorIdentityError, ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorProviderId,
};
pub use mutation::{ConnectorRowMutationEffect, ConnectorWriteRouteId};
pub use read::{ConnectorReadBinding, ConnectorReadRelationKind, ConnectorReadWorkSource};
pub use write::{ConnectorWriteFieldToken, MAX_CONNECTOR_WRITE_TARGETS, WriteTargetOrdinal};
