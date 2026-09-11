// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Query application ownership and consumer-facing contracts.
//!
//! Product services depend on this crate. The query application never depends
//! on their implementations; role composition supplies the consumer ports.
// Design: ADR-0144 (docs/adr/ADR-0144-application-domains-follow-stable-ownership-seams.md)

pub mod api;

/// First-wins statement cancellation shared by query application consumers.
pub mod cancellation;

/// Exact client-session identity and protocol-owned termination contracts.
pub mod client_connection;

/// Session registry and cancellation implementation owned by the query application.
pub mod query_control;

/// Query-session errors before protocol-specific encoding.
pub mod session_error;

/// Query-session admission requests independent of a wire protocol.
pub mod session;

/// Session generations, governed statement control, and protocol settlement.
pub mod session_control;

/// SQL source parsing and statement-shape admission owned by the query
/// application before role adapters route a statement to a product consumer.
pub mod sql;

/// Query-scoped acquisition of immutable metadata and optional optimization facts.
pub mod observation;

/// Pure, topology-free preparation and its immutable execution handoff.
pub mod preparation;

// These policies are query-coordination decisions, not worker protocol. The
// hidden visibility supports role-adapter integration while keeping them out
// of the consumer-facing contract.
#[doc(hidden)]
pub mod coordination;

/// High-level lifecycle harness for role-adapter integration tests.
#[cfg(feature = "test-support")]
pub mod test_support;
