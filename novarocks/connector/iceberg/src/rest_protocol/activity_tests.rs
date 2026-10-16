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

//! Actual shared HTTP initializer ownership across participant cancellation.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};
use std::task::Poll;
use std::time::Duration;

use crate::activity::CatalogActivity;
use crate::rest_protocol::attempt::{AttemptFailure, SessionGate, ensure_session};
use crate::rest_protocol::auth::{PersistentRestConfig, RestAuthOwner};
use crate::rest_protocol::http::RestHttpClient;
use crate::rest_protocol::test_server::{
    Reply, Script, TestServer, oauth2_identity, wait_until_requests,
};
use novarocks_spi::connector::{
    CatalogActivityPhase as P, CatalogActivityReader, CatalogScopeIdentity, ConnectorInstanceId,
    ConnectorProviderId,
};

#[derive(Clone, Copy, Debug)]
enum Initializer {
    Authorization,
    Session,
}
impl Initializer {
    fn phase(self) -> P {
        match self {
            Self::Authorization => P::AuthorizationInitializer,
            Self::Session => P::SessionInitializer,
        }
    }
    fn route(self) -> (&'static str, &'static str) {
        match self {
            Self::Authorization => ("POST", "/v1/oauth/tokens"),
            Self::Session => ("GET", "/v1/config"),
        }
    }
    fn response(self) -> Reply {
        match self {
            Self::Authorization => Reply::token("shared-token"),
            Self::Session => Reply::config(&[], &[], None),
        }
    }
}

#[derive(Clone, Default)]
struct WireGate(Arc<(Mutex<bool>, Condvar)>);
impl WireGate {
    fn wait(&self) {
        let (lock, changed) = &*self.0;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
    }
    fn release(&self) {
        let (lock, changed) = &*self.0;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
}
struct ReleaseOnDrop(WireGate);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn initialize(
    kind: Initializer,
    owner: Arc<RestAuthOwner>,
    gate: Arc<SessionGate>,
    properties: Arc<HashMap<String, String>>,
) -> Result<(), AttemptFailure> {
    match kind {
        Initializer::Authorization => {
            let token = owner.authorization().await?;
            assert_eq!(token.unwrap(), "Bearer shared-token");
        }
        Initializer::Session => {
            let _binding = ensure_session(&owner, &gate, &properties).await?;
        }
    }
    Ok(())
}

fn assert_phase(root: &CatalogActivity, phase: P, inflight: u64, started: u64, exited: u64) {
    let snapshot = root.snapshot();
    let row = snapshot
        .phases
        .iter()
        .find(|row| row.phase == phase)
        .unwrap();
    assert_eq!(
        (row.inflight, row.started, row.exited),
        (inflight, started, exited)
    );
    assert_eq!(row.elapsed_count, exited);
    // These are initializer-only calls, never publication work.
    for phase in [P::PreDispatch, P::Dispatch, P::Evidence, P::Finalization] {
        let row = snapshot
            .phases
            .iter()
            .find(|row| row.phase == phase)
            .unwrap();
        assert_eq!((row.inflight, row.started, row.exited), (0, 0, 0));
    }
}

async fn shared_initializer_case(kind: Initializer, abandon_survivor: bool) {
    let wire = WireGate::default();
    let responder_gate = wire.clone();
    let (method, path) = kind.route();
    let server = TestServer::start(Script::new().on_fn(method, path, move |_, index| {
        // The route runs on the real server connection thread after the
        // complete request was read/recorded. No simulated transport result.
        assert_eq!(index, 0, "only one HTTP exchange may start");
        responder_gate.wait();
        kind.response()
    }));
    // Declared AFTER server: unwinding releases the wire gate before the
    // server's Drop attempts to join its connection thread.
    let _release_on_failure = ReleaseOnDrop(wire.clone());
    let root = CatalogActivity::new(CatalogScopeIdentity::new(
        ConnectorProviderId::parse("iceberg").unwrap(),
        ConnectorInstanceId::parse("initializer.actual").unwrap(),
        uuid::Uuid::now_v7(),
    ));
    let properties = Arc::new(HashMap::from([("uri".to_owned(), server.url())]));
    let owner = RestAuthOwner::frontend_observed(
        match kind {
            Initializer::Authorization => Some(oauth2_identity("client", "secret")),
            Initializer::Session => None,
        },
        PersistentRestConfig::from_properties(&properties).unwrap(),
        RestHttpClient::new().unwrap(),
        Some(root.generation()),
    )
    .unwrap();
    let session = Arc::new(SessionGate::default());
    let initiator = tokio::spawn(initialize(
        kind,
        owner.clone(),
        session.clone(),
        properties.clone(),
    ));
    wait_until_requests(&server, Duration::from_secs(5), |requests| {
        !requests.is_empty()
    })
    .await;
    assert_eq!(server.count(method, path), 1);
    assert_phase(&root, kind.phase(), 1, 1, 0);

    let mut survivor = Box::pin(initialize(
        kind,
        owner.clone(),
        session.clone(),
        properties.clone(),
    ));
    // This deterministic poll reaches Shared::await and joins the running
    // attempt. The gated server cannot complete it before this observation.
    assert!(matches!(futures::poll!(survivor.as_mut()), Poll::Pending));
    initiator.abort();
    match initiator.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("the gated initiator must be cancelled"),
    }
    assert_phase(&root, kind.phase(), 1, 1, 0);
    assert_eq!(server.count(method, path), 1);
    if abandon_survivor {
        // The last local participant's drop destroys the actual shared future.
        // It does not prove the remote handler stopped: release/join it below.
        drop(survivor);
        assert_phase(&root, kind.phase(), 0, 1, 1);
        assert_eq!(server.count(method, path), 1);
        wire.release();
    } else {
        wire.release();
        tokio::time::timeout(Duration::from_secs(5), survivor)
            .await
            .unwrap()
            .unwrap();
        assert_phase(&root, kind.phase(), 0, 1, 1);
        // Success is cached; neither a second phase nor HTTP exchange starts.
        initialize(kind, owner.clone(), session.clone(), properties.clone())
            .await
            .unwrap();
        assert_phase(&root, kind.phase(), 0, 1, 1);
        assert_eq!(server.count(method, path), 1);
    }
    let other_phase = match kind {
        Initializer::Authorization => P::SessionInitializer,
        Initializer::Session => P::AuthorizationInitializer,
    };
    assert_phase(&root, other_phase, 0, 0, 0);
    drop(owner);
    drop(session);
    drop(properties);
    // This is the explicit remote-thread teardown boundary, after gate release.
    // stop shuts down streams and joins every real connection/acceptor thread.
    server.shut_down();
    assert_phase(&root, kind.phase(), 0, 1, 1);
    assert!(root.snapshot().generations.is_empty());
}

#[tokio::test]
async fn oauth_shared_initializer_survives_one_cancelled_participant() {
    shared_initializer_case(Initializer::Authorization, false).await;
}
#[tokio::test]
async fn oauth_shared_initializer_exits_when_every_participant_is_gone() {
    shared_initializer_case(Initializer::Authorization, true).await;
}
#[tokio::test]
async fn config_shared_initializer_survives_one_cancelled_participant() {
    shared_initializer_case(Initializer::Session, false).await;
}
#[tokio::test]
async fn config_shared_initializer_exits_when_every_participant_is_gone() {
    shared_initializer_case(Initializer::Session, true).await;
}
