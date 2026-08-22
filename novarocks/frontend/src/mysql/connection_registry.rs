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

//! MySQL-owned registry for accepted protocol connections.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tokio::sync::oneshot;

use crate::client_connection::{
    ClientConnectionControlPort, ClientConnectionTerminateOutcome,
    ClientConnectionTerminationReason, ClientConnectionToken,
};

const MAX_CONNECTION_ID: u32 = u32::MAX;
const MAX_GENERATION: u64 = u64::MAX;

/// The protocol owner of accepted client connection identities and signals.
#[derive(Clone)]
pub struct MysqlClientConnectionRegistry {
    state: Arc<Mutex<RegistryState>>,
}

struct RegistryState {
    next_connection_id: u32,
    next_generation: u64,
    max_connection_id: u32,
    max_generation: u64,
    entries: BTreeMap<u32, Entry>,
}

struct Entry {
    token: ClientConnectionToken,
    termination: Option<oneshot::Sender<ClientConnectionTerminationReason>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ConnectionRegistrationError {
    ConnectionIdExhausted,
    GenerationExhausted,
}

/// A registration whose drop removes only the exact generation it created.
pub(crate) struct MysqlClientConnectionRegistration {
    registry: MysqlClientConnectionRegistry,
    token: ClientConnectionToken,
    termination: oneshot::Receiver<ClientConnectionTerminationReason>,
}

impl MysqlClientConnectionRegistry {
    pub fn new() -> Self {
        Self::with_bounds(MAX_CONNECTION_ID, MAX_GENERATION)
    }

    #[cfg(test)]
    fn with_test_bounds(max_connection_id: u32, max_generation: u64) -> Self {
        assert!(
            max_connection_id > 0,
            "test connection ID bound must be non-zero"
        );
        assert!(max_generation > 0, "test generation bound must be non-zero");
        Self::with_bounds(max_connection_id, max_generation)
    }

    fn with_bounds(max_connection_id: u32, max_generation: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(RegistryState {
                next_connection_id: 1,
                next_generation: 1,
                max_connection_id,
                max_generation,
                entries: BTreeMap::new(),
            })),
        }
    }

    pub(crate) fn register(
        &self,
    ) -> Result<MysqlClientConnectionRegistration, ConnectionRegistrationError> {
        let (token, termination) = {
            let mut state = self.lock();
            let connection_id = state.next_available_connection_id()?;
            let generation = state.next_generation()?;
            let token = ClientConnectionToken::new(connection_id, generation)
                .expect("registry allocators never produce zero identity components");
            let (sender, receiver) = oneshot::channel();
            let previous = state.entries.insert(
                connection_id,
                Entry {
                    token,
                    termination: Some(sender),
                },
            );
            debug_assert!(
                previous.is_none(),
                "allocator must skip live connection IDs"
            );
            (token, receiver)
        };
        Ok(MysqlClientConnectionRegistration {
            registry: self.clone(),
            token,
            termination,
        })
    }

    pub(crate) fn terminate_all(&self, reason: ClientConnectionTerminationReason) -> usize {
        let senders = {
            let mut state = self.lock();
            state
                .entries
                .values_mut()
                .filter_map(|entry| entry.termination.take())
                .collect::<Vec<_>>()
        };
        let count = senders.len();
        for sender in senders {
            let _ = sender.send(reason.clone());
        }
        count
    }

    fn unregister(&self, token: ClientConnectionToken) {
        let mut state = self.lock();
        if state
            .entries
            .get(&token.connection_id())
            .is_some_and(|entry| entry.token == token)
        {
            state.entries.remove(&token.connection_id());
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RegistryState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl ClientConnectionControlPort for MysqlClientConnectionRegistry {
    fn terminate(
        &self,
        target: ClientConnectionToken,
        reason: ClientConnectionTerminationReason,
    ) -> ClientConnectionTerminateOutcome {
        let sender = {
            let mut state = self.lock();
            let Some(entry) = state.entries.get_mut(&target.connection_id()) else {
                return ClientConnectionTerminateOutcome::Stale;
            };
            if entry.token != target {
                return ClientConnectionTerminateOutcome::Stale;
            }
            entry.termination.take()
        };
        let Some(sender) = sender else {
            return ClientConnectionTerminateOutcome::AlreadyTerminating;
        };
        let _ = sender.send(reason);
        ClientConnectionTerminateOutcome::Requested
    }
}

impl MysqlClientConnectionRegistration {
    pub(crate) const fn token(&self) -> ClientConnectionToken {
        self.token
    }

    pub(crate) fn termination_receiver(
        &mut self,
    ) -> &mut oneshot::Receiver<ClientConnectionTerminationReason> {
        &mut self.termination
    }
}

impl Drop for MysqlClientConnectionRegistration {
    fn drop(&mut self) {
        self.registry.unregister(self.token);
    }
}

impl RegistryState {
    fn next_available_connection_id(&mut self) -> Result<u32, ConnectionRegistrationError> {
        let first = self.next_connection_id;
        loop {
            let candidate = self.next_connection_id;
            self.next_connection_id = if candidate == self.max_connection_id {
                1
            } else {
                candidate + 1
            };
            if !self.entries.contains_key(&candidate) {
                return Ok(candidate);
            }
            if self.next_connection_id == first {
                return Err(ConnectionRegistrationError::ConnectionIdExhausted);
            }
        }
    }

    fn next_generation(&mut self) -> Result<u64, ConnectionRegistrationError> {
        if self.next_generation == 0 || self.next_generation > self.max_generation {
            return Err(ConnectionRegistrationError::GenerationExhausted);
        }
        let generation = self.next_generation;
        self.next_generation = if generation == self.max_generation {
            0
        } else {
            generation + 1
        };
        Ok(generation)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Barrier;
    use std::thread;

    use super::*;

    fn token(connection_id: u32, generation: u64) -> ClientConnectionToken {
        ClientConnectionToken::new(connection_id, generation).expect("valid test token")
    }

    #[test]
    fn allocator_skips_live_ids_then_reuses_released_id_with_new_generation() {
        let registry = MysqlClientConnectionRegistry::with_test_bounds(2, 8);
        let first = registry.register().expect("register first");
        let second = registry.register().expect("register second");
        assert_eq!(first.token(), token(1, 1));
        assert_eq!(second.token(), token(2, 2));
        assert!(matches!(
            registry.register(),
            Err(ConnectionRegistrationError::ConnectionIdExhausted)
        ));

        drop(first);
        let successor = registry.register().expect("reuse released ID");
        assert_eq!(successor.token(), token(1, 3));
        drop(second);
        drop(successor);
    }

    #[test]
    fn allocator_fails_closed_after_generation_exhaustion() {
        let registry = MysqlClientConnectionRegistry::with_test_bounds(2, 2);
        let first = registry.register().expect("register first");
        let second = registry.register().expect("register second");
        drop(first);
        drop(second);
        assert!(matches!(
            registry.register(),
            Err(ConnectionRegistrationError::GenerationExhausted)
        ));
    }

    #[test]
    fn stale_termination_and_unregister_cannot_target_successor() {
        let registry = MysqlClientConnectionRegistry::with_test_bounds(1, 4);
        let first = registry.register().expect("register first");
        let old = first.token();
        registry.unregister(old);
        let mut successor = registry.register().expect("register successor");
        let current = successor.token();
        assert_ne!(old, current);

        assert_eq!(
            registry.terminate(
                old,
                ClientConnectionTerminationReason::ExplicitKillConnection {
                    requester_connection_id: 9,
                }
            ),
            ClientConnectionTerminateOutcome::Stale
        );
        registry.unregister(old);
        drop(first);
        assert_eq!(
            registry.terminate(
                current,
                ClientConnectionTerminationReason::ExplicitKillConnection {
                    requester_connection_id: 9,
                }
            ),
            ClientConnectionTerminateOutcome::Requested
        );
        assert_eq!(
            successor
                .termination_receiver()
                .try_recv()
                .expect("successor receives its signal"),
            ClientConnectionTerminationReason::ExplicitKillConnection {
                requester_connection_id: 9,
            }
        );
    }

    #[test]
    fn exact_termination_is_first_wins_and_idempotent() {
        let registry = MysqlClientConnectionRegistry::with_test_bounds(1, 4);
        let mut registration = registry.register().expect("register connection");
        let target = registration.token();
        let first = ClientConnectionTerminationReason::ExplicitKillConnection {
            requester_connection_id: 7,
        };
        assert_eq!(
            registry.terminate(target, first.clone()),
            ClientConnectionTerminateOutcome::Requested
        );
        assert_eq!(
            registry.terminate(target, ClientConnectionTerminationReason::ServerShutdown),
            ClientConnectionTerminateOutcome::AlreadyTerminating
        );
        assert_eq!(
            registration
                .termination_receiver()
                .try_recv()
                .expect("first reason is delivered"),
            first
        );
    }

    #[test]
    fn concurrent_terminate_has_exactly_one_requester() {
        let registry = Arc::new(MysqlClientConnectionRegistry::with_test_bounds(1, 8));
        let registration = registry.register().expect("register connection");
        let target = registration.token();
        let barrier = Arc::new(Barrier::new(9));
        let workers = (0..8)
            .map(|requester_connection_id| {
                let registry = Arc::clone(&registry);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    registry.terminate(
                        target,
                        ClientConnectionTerminationReason::ExplicitKillConnection {
                            requester_connection_id,
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let outcomes = workers
            .into_iter()
            .map(|worker| worker.join().expect("worker must not panic"))
            .collect::<Vec<_>>();
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| **outcome == ClientConnectionTerminateOutcome::Requested)
                .count(),
            1
        );
        assert!(outcomes.iter().all(|outcome| {
            matches!(
                outcome,
                ClientConnectionTerminateOutcome::Requested
                    | ClientConnectionTerminateOutcome::AlreadyTerminating
            )
        }));
        drop(registration);
    }

    #[test]
    fn broadcast_latches_every_live_connection_once() {
        let registry = MysqlClientConnectionRegistry::with_test_bounds(3, 8);
        let mut first = registry.register().expect("register first");
        let mut second = registry.register().expect("register second");
        assert_eq!(
            registry.terminate_all(ClientConnectionTerminationReason::ServerShutdown),
            2
        );
        assert_eq!(
            registry.terminate(
                first.token(),
                ClientConnectionTerminationReason::ServerShutdown
            ),
            ClientConnectionTerminateOutcome::AlreadyTerminating
        );
        assert_eq!(
            first
                .termination_receiver()
                .try_recv()
                .expect("first receives shutdown"),
            ClientConnectionTerminationReason::ServerShutdown
        );
        assert_eq!(
            second
                .termination_receiver()
                .try_recv()
                .expect("second receives shutdown"),
            ClientConnectionTerminationReason::ServerShutdown
        );
    }
}
