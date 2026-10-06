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

//! Native connection admission held outside the HTTP/2 stack.
//!
//! Every Native connection, accepted or dialed, first takes one physical
//! position of its transport class and one handshake position. Both are
//! counts only: the bytes Tokio, Hyper, H2 and Tonic allocate for that
//! connection stay inside those libraries, bounded by their public
//! configuration and verified by the transport measurement gate (spec v6
//! §5.9). Positions are returned at public exit events: the handshake
//! position when bootstrap completes or the connection is dropped, and the
//! physical position when the connection's IO wrapper is dropped.
//!
//! Peer/lane quotas for dialed connections and the incoming key a connection
//! is sealed to are tracked here too, with the same exit points.

use std::fmt;
use std::io;
use std::sync::{Arc, Mutex};

use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::native_channel_identity::InlineNativeChannelIdentity;
use crate::native_connection_key_capacity::{NativeConnectionKeyCapacity, NativeConnectionKeyToken};
use crate::native_incoming_key_capacity::{
    NativeIncomingKey, NativeIncomingKeyCapacity, NativeIncomingKeyToken,
};

/// Independent admission domains; Control never consumes or lends Data positions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportClass {
    /// Incoming FE data and both directions of peer exchange/runtime filters.
    Data,
    /// Incoming FE lifecycle, conservative outgoing reports, and handshakes.
    Control,
}

impl TransportClass {
    const fn index(self) -> usize {
        match self {
            Self::Data => 0,
            Self::Control => 1,
        }
    }
}

/// Position counts derived from the frozen Native result support geometry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdmissionDimensions {
    pub data_positions: usize,
    pub control_positions: usize,
    pub data_handshakes: usize,
    pub control_handshakes: usize,
}

fn invalid() -> io::Error {
    io::ErrorKind::InvalidInput.into()
}

fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b).ok_or_else(invalid)
}

fn mul(a: usize, b: usize) -> io::Result<usize> {
    a.checked_mul(b).ok_or_else(invalid)
}

fn value(value: u64) -> io::Result<usize> {
    usize::try_from(value).map_err(|_| invalid())
}

impl AdmissionDimensions {
    /// Checked arithmetic over the frozen geometry. The counts cover every
    /// legal FE lane and both peer directions with their connecting and
    /// closing headroom, plus the handshake positions of each class.
    pub fn frozen() -> io::Result<Self> {
        let g = NativeResultSupportGeometry::V1;
        let frontends = value(g.transport_authenticated_live_frontends_per_backend)?;
        let connecting = value(g.transport_connecting_positions_per_lane)?;
        let closing = value(g.transport_closing_positions_per_lane)?;
        let fe_live = add(
            add(
                value(g.transport_connections_per_frontend_backend_result)?,
                value(g.transport_connections_per_frontend_backend_observation)?,
            )?,
            value(g.transport_connections_per_frontend_backend_submission)?,
        )?;
        // FE data is incoming only: frontends * (live + 3 lanes * (connecting + closing)).
        let incoming_fe_data = mul(frontends, add(fe_live, mul(3, add(connecting, closing)?)?)?)?;
        let exchange = add(
            value(g.transport_exchange_connections_per_peer)?,
            add(
                value(g.transport_exchange_connecting_positions_per_peer)?,
                value(g.transport_exchange_closing_positions_per_peer)?,
            )?,
        )?;
        let filters = add(
            value(g.transport_runtime_filter_connections_per_peer)?,
            add(
                value(g.transport_runtime_filter_connecting_positions_per_peer)?,
                value(g.transport_runtime_filter_closing_positions_per_peer)?,
            )?,
        )?;
        // Both peer directions conservatively include every live peer.
        let peer_data = mul(
            mul(2, value(g.transport_maximum_live_backends)?)?,
            add(exchange, filters)?,
        )?;
        let data_handshakes = value(g.transport_data_handshake_positions)?;
        let control_handshakes = value(g.transport_control_handshake_positions)?;
        let data_floor = add(add(incoming_fe_data, peer_data)?, data_handshakes)?;
        // Incoming lifecycle lane per frontend, with connecting/closing headroom.
        let control_lane = mul(
            frontends,
            add(
                value(g.transport_connections_per_frontend_backend_lifecycle_control)?,
                add(connecting, closing)?,
            )?,
        )?;
        // Backend announce/report uses the data client runtime in the outgoing
        // FE direction; reserve another control-lane worth there rather than
        // borrowing incoming Control positions.
        let data_positions = add(data_floor, control_lane)?;
        let control_positions = add(mul(2, control_lane)?, control_handshakes)?;
        if data_handshakes == 0 || control_handshakes == 0 {
            return Err(invalid());
        }
        Ok(Self {
            data_positions,
            control_positions,
            data_handshakes,
            control_handshakes,
        })
    }

    const fn positions(&self, class: TransportClass) -> usize {
        match class {
            TransportClass::Data => self.data_positions,
            TransportClass::Control => self.control_positions,
        }
    }

    const fn handshakes(&self, class: TransportClass) -> usize {
        match class {
            TransportClass::Data => self.data_handshakes,
            TransportClass::Control => self.control_handshakes,
        }
    }
}

struct Core {
    dimensions: AdmissionDimensions,
    physical: [Arc<Semaphore>; 2],
    handshake: [Arc<Semaphore>; 2],
    connection_keys: NativeConnectionKeyCapacity,
    incoming_keys: NativeIncomingKeyCapacity,
}

/// Process-scoped Native connection admission. Clones share one set of
/// positions; there is exactly one per role process.
#[derive(Clone)]
pub struct NativeTransportAdmission {
    core: Arc<Core>,
}

impl fmt::Debug for NativeTransportAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeTransportAdmission")
            .field("dimensions", &self.core.dimensions)
            .finish_non_exhaustive()
    }
}

impl NativeTransportAdmission {
    pub fn new() -> io::Result<Self> {
        Self::with_dimensions(AdmissionDimensions::frozen()?)
    }

    pub fn with_dimensions(dimensions: AdmissionDimensions) -> io::Result<Self> {
        let class = |count: usize| Arc::new(Semaphore::new(count));
        Ok(Self {
            core: Arc::new(Core {
                dimensions,
                physical: [
                    class(dimensions.data_positions),
                    class(dimensions.control_positions),
                ],
                handshake: [
                    class(dimensions.data_handshakes),
                    class(dimensions.control_handshakes),
                ],
                connection_keys: NativeConnectionKeyCapacity::new()?,
                incoming_keys: NativeIncomingKeyCapacity::new()?,
            }),
        })
    }

    pub fn dimensions(&self) -> AdmissionDimensions {
        self.core.dimensions
    }

    pub fn positions(&self, class: TransportClass) -> usize {
        self.core.dimensions.positions(class)
    }

    pub fn available_positions(&self, class: TransportClass) -> usize {
        self.core.physical[class.index()].available_permits()
    }

    pub fn handshake_positions(&self, class: TransportClass) -> usize {
        self.core.dimensions.handshakes(class)
    }

    pub fn available_handshakes(&self, class: TransportClass) -> usize {
        self.core.handshake[class.index()].available_permits()
    }

    fn try_positions(
        &self,
        class: TransportClass,
    ) -> io::Result<(OwnedSemaphorePermit, OwnedSemaphorePermit)> {
        let physical = Arc::clone(&self.core.physical[class.index()])
            .try_acquire_owned()
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        let handshake = Arc::clone(&self.core.handshake[class.index()])
            .try_acquire_owned()
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        Ok((physical, handshake))
    }

    /// Admit one accepted socket. Refusal (`WouldBlock`) takes nothing; the
    /// caller closes the socket.
    pub(crate) fn try_accept(&self, class: TransportClass) -> io::Result<AcceptedConnection> {
        let (physical, handshake) = self.try_positions(class)?;
        let seal = Arc::new(IncomingSeal {
            admission: self.clone(),
            state: Mutex::new(SealState::Open),
        });
        Ok(AcceptedConnection {
            connection: NativeConnectionPermit {
                key: None,
                incoming: Some(Arc::clone(&seal)),
                _physical: physical,
            },
            handshake: NativeHandshakePermit::new(handshake),
            binding: NativeIncomingConnectionBinding { seal },
        })
    }

    /// Admit one outgoing connection attempt before any IO. A peer/lane key
    /// claims its connecting position first; refusal rolls everything back.
    pub(crate) fn try_dial(
        &self,
        class: TransportClass,
        key: Option<InlineNativeChannelIdentity>,
    ) -> io::Result<DialAdmission> {
        let key = key
            .map(|key| {
                self.core.connection_keys.claim(key).map(|token| KeyClaim {
                    admission: self.clone(),
                    token,
                })
            })
            .transpose()?;
        let (physical, handshake) = self.try_positions(class)?;
        Ok(DialAdmission {
            connection: NativeConnectionPermit {
                key,
                incoming: None,
                _physical: physical,
            },
            handshake: NativeHandshakePermit::new(handshake),
        })
    }
}

/// Positions of one accepted connection.
pub(crate) struct AcceptedConnection {
    pub(crate) connection: NativeConnectionPermit,
    pub(crate) handshake: NativeHandshakePermit,
    pub(crate) binding: NativeIncomingConnectionBinding,
}

/// Positions of one dial attempt, before its IO is established.
pub(crate) struct DialAdmission {
    connection: NativeConnectionPermit,
    handshake: NativeHandshakePermit,
}

impl DialAdmission {
    /// The attempt produced a live IO: publish its key as Live and return the
    /// handshake position. A full live quota refuses the connection.
    pub(crate) fn established(self) -> io::Result<NativeConnectionPermit> {
        if let Some(key) = &self.connection.key {
            key.admission.core.connection_keys.install(key.token)?;
        }
        self.handshake.release();
        Ok(self.connection)
    }
}

struct KeyClaim {
    admission: NativeTransportAdmission,
    token: NativeConnectionKeyToken,
}

impl Drop for KeyClaim {
    fn drop(&mut self) {
        let keys = &self.admission.core.connection_keys;
        let _ = keys.retire(self.token);
        keys.exit(self.token)
            .expect("exact physical key generation exits once");
    }
}

/// Held by the connection's IO wrapper. Dropping it (after the IO itself) is
/// the connection's public exit: the peer key and the incoming key are
/// cleared first, then the physical position is returned.
pub struct NativeConnectionPermit {
    key: Option<KeyClaim>,
    incoming: Option<Arc<IncomingSeal>>,
    _physical: OwnedSemaphorePermit,
}

impl fmt::Debug for NativeConnectionPermit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeConnectionPermit")
            .field("keyed", &self.key.is_some())
            .field("incoming", &self.incoming.is_some())
            .finish_non_exhaustive()
    }
}

impl Drop for NativeConnectionPermit {
    fn drop(&mut self) {
        if let Some(seal) = self.incoming.take() {
            seal.close();
        }
        drop(self.key.take());
    }
}

/// The handshake position of one connection. Clones share it; the first
/// `release` (bootstrap complete) or the drop of every clone returns it.
#[derive(Clone)]
pub(crate) struct NativeHandshakePermit {
    permit: Arc<Mutex<Option<OwnedSemaphorePermit>>>,
}

impl NativeHandshakePermit {
    fn new(permit: OwnedSemaphorePermit) -> Self {
        Self {
            permit: Arc::new(Mutex::new(Some(permit))),
        }
    }

    pub(crate) fn release(&self) {
        drop(self.permit.lock().expect("handshake permit lock").take());
    }

    pub(crate) fn is_held(&self) -> bool {
        self.permit.lock().expect("handshake permit lock").is_some()
    }
}

enum SealState {
    Open,
    Sealed(NativeIncomingKey, NativeIncomingKeyToken),
    Closed,
}

struct IncomingSeal {
    admission: NativeTransportAdmission,
    state: Mutex<SealState>,
}

impl IncomingSeal {
    fn close(&self) {
        let previous = std::mem::replace(
            &mut *self.state.lock().expect("incoming seal lock"),
            SealState::Closed,
        );
        if let SealState::Sealed(_, token) = previous {
            let keys = &self.admission.core.incoming_keys;
            let _ = keys.retire(token);
            keys.exit(token)
                .expect("exact original incoming key exits once");
        }
    }
}

/// One accepted connection's incoming identity. The first authenticated
/// request seals it to one caller process, endpoint and traffic class; any
/// later request with another key is refused and the connection is closed.
#[derive(Clone)]
pub(crate) struct NativeIncomingConnectionBinding {
    seal: Arc<IncomingSeal>,
}

impl NativeIncomingConnectionBinding {
    pub(crate) fn seal(&self, key: NativeIncomingKey) -> io::Result<()> {
        let mut state = self.seal.state.lock().map_err(|_| io::ErrorKind::InvalidData)?;
        match &*state {
            SealState::Closed => Err(io::ErrorKind::ConnectionAborted.into()),
            SealState::Sealed(existing, _) if *existing == key => Ok(()),
            SealState::Sealed(..) => Err(io::ErrorKind::ConnectionAborted.into()),
            SealState::Open => {
                let token = self.seal.admission.core.incoming_keys.claim(key)?;
                *state = SealState::Sealed(key, token);
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small() -> NativeTransportAdmission {
        NativeTransportAdmission::with_dimensions(AdmissionDimensions {
            data_positions: 2,
            control_positions: 1,
            data_handshakes: 1,
            control_handshakes: 1,
        })
        .unwrap()
    }

    #[test]
    fn frozen_dimensions_are_checked_and_nonzero() {
        let dimensions = AdmissionDimensions::frozen().unwrap();
        assert!(dimensions.data_positions > dimensions.data_handshakes);
        assert!(dimensions.control_positions > dimensions.control_handshakes);
        assert_eq!(dimensions.data_handshakes, 32);
        assert_eq!(dimensions.control_handshakes, 8);
    }

    #[test]
    fn control_and_data_positions_never_borrow_from_each_other() {
        let admission = small();
        let control = admission.try_accept(TransportClass::Control).unwrap();
        assert_eq!(
            admission
                .try_accept(TransportClass::Control)
                .err()
                .map(|error| error.kind()),
            Some(io::ErrorKind::WouldBlock)
        );
        assert_eq!(admission.available_positions(TransportClass::Data), 2);
        let data = admission.try_accept(TransportClass::Data).unwrap();
        assert_eq!(admission.available_positions(TransportClass::Control), 0);
        drop(control);
        drop(data);
        assert_eq!(admission.available_positions(TransportClass::Control), 1);
        assert_eq!(admission.available_positions(TransportClass::Data), 2);
    }

    #[test]
    fn handshake_position_returns_at_bootstrap_while_physical_stays_until_drop() {
        let admission = small();
        let accepted = admission.try_accept(TransportClass::Data).unwrap();
        assert_eq!(admission.available_handshakes(TransportClass::Data), 0);
        assert!(
            admission.try_accept(TransportClass::Data).is_err(),
            "an incomplete bootstrap holds the only data handshake position"
        );
        accepted.handshake.release();
        assert!(!accepted.handshake.is_held());
        assert_eq!(admission.available_handshakes(TransportClass::Data), 1);
        assert_eq!(admission.available_positions(TransportClass::Data), 1);
        drop(accepted.connection);
        assert_eq!(admission.available_positions(TransportClass::Data), 2);
    }

    #[test]
    fn dropping_an_unfinished_bootstrap_returns_its_handshake_position() {
        let admission = small();
        let accepted = admission.try_accept(TransportClass::Data).unwrap();
        let AcceptedConnection {
            connection,
            handshake,
            binding,
        } = accepted;
        drop(handshake);
        drop(binding);
        assert_eq!(admission.available_handshakes(TransportClass::Data), 1);
        drop(connection);
        assert_eq!(admission.available_positions(TransportClass::Data), 2);
    }

    #[test]
    fn refused_handshake_rolls_back_the_physical_position() {
        let admission = small();
        let first = admission.try_accept(TransportClass::Data).unwrap();
        assert!(admission.try_accept(TransportClass::Data).is_err());
        assert_eq!(admission.available_positions(TransportClass::Data), 1);
        drop(first);
    }
}
