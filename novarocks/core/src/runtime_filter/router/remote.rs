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

//! Outbound remote leg of the delivery Router.
//!
//! The delivery Router splits an authorized delivery scope into loopback edges
//! (delivered in-process by [`super::loopback::LoopbackRouter`]) and remote edges.
//! For each remote edge the Service wire-encodes the materialized artifact (or an
//! `Unavailable` sentinel) into an [`EncodedArtifactFrame`] and hands the frame to
//! an `ArtifactRemoteSink`. The sink owns transport: M2C injects a recording fake,
//! while M3 wires the live network sender behind the same seam.
//!
//! The sink never re-authorizes fanout — the [`RuntimeFilterRemoteRoute`] it
//! receives was already vetted by the Router's `route_delivery`, so the sink only
//! transmits to the route's peer participant/endpoint.

use crate::runtime_filter::codec::artifact::EncodedArtifactFrame;
use crate::runtime_filter::port::routing::RuntimeFilterRemoteRoute;

/// Transport seam for the delivery Router's remote leg.
///
/// Implementations receive an already-authorized [`RuntimeFilterRemoteRoute`] and
/// the canonical [`EncodedArtifactFrame`] to transmit to that route's peer. The
/// frame is fully framed and self-describing (it carries the consumer profile
/// digest and the bundle logical version), so the sink is a pure transport.
pub(crate) trait ArtifactRemoteSink: Send + Sync {
    fn deliver_remote(&self, route: &RuntimeFilterRemoteRoute, frame: EncodedArtifactFrame);
}
