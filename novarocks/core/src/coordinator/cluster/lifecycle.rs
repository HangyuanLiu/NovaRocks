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

use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::{BackendRegistry, BeId, HeartbeatOutcome, RegistryEvent};

pub(crate) trait RegistryEventSink: Send + Sync + 'static {
    fn on_event(&self, event: RegistryEvent);
}

pub(crate) fn run_heartbeat_round<F>(registry: &BackendRegistry, send: &F) -> Vec<RegistryEvent>
where
    F: Fn(BeId, SocketAddr) -> HeartbeatOutcome,
{
    let mut events = Vec::new();
    for (be_id, endpoint) in registry.all_for_heartbeat() {
        let outcome = send(be_id, endpoint);
        events.extend(registry.apply_heartbeat_result(be_id, outcome));
    }
    events
}

fn dispatch_event(sink: &dyn RegistryEventSink, event: RegistryEvent) {
    // Sink failures must not terminate the permanent heartbeat manager thread.
    let _ = catch_unwind(AssertUnwindSafe(|| sink.on_event(event)));
}

pub(crate) fn spawn_heartbeat_manager<F>(
    registry: Arc<BackendRegistry>,
    interval: Duration,
    send: F,
    sink: Arc<dyn RegistryEventSink>,
) where
    F: Fn(BeId, SocketAddr) -> HeartbeatOutcome + Send + Sync + 'static,
{
    thread::Builder::new()
        .name("heartbeat-mgr".to_string())
        .spawn(move || {
            loop {
                for event in run_heartbeat_round(&registry, &send) {
                    dispatch_event(sink.as_ref(), event);
                }
                thread::sleep(interval);
            }
        })
        .expect("failed to spawn heartbeat-mgr thread");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::cluster::{BackendState, HeartbeatOutcome, RegistryEvent};
    use std::sync::Mutex;

    fn ep(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[test]
    fn one_round_marks_reachable_live_unreachable_progresses_to_lost() {
        let registry = Arc::new(BackendRegistry::new(1));
        let first = registry.add_backend(ep(9070));
        let second = registry.add_backend(ep(9071));
        let calls = Mutex::new(Vec::new());
        let send = |be_id, endpoint: SocketAddr| -> HeartbeatOutcome {
            calls.lock().unwrap().push((be_id, endpoint));
            if endpoint == ep(9070) {
                HeartbeatOutcome::Ok {
                    start_epoch: 1,
                    version: "v".into(),
                    num_cores: 2,
                    now_ms: 100,
                }
            } else {
                HeartbeatOutcome::Failed {
                    err: "unreachable".into(),
                }
            }
        };

        let events = run_heartbeat_round(&registry, &send);

        assert_eq!(
            *calls.lock().unwrap(),
            vec![(first, ep(9070)), (second, ep(9071))]
        );
        assert_eq!(events, vec![RegistryEvent::BackendLost { be_id: second }]);
        assert_eq!(registry.live_endpoints(), vec![(first, ep(9070))]);
        assert!(
            registry
                .snapshot()
                .iter()
                .any(|entry| entry.be_id == second && entry.state == BackendState::Lost)
        );
    }

    struct PanickingSink;

    impl RegistryEventSink for PanickingSink {
        fn on_event(&self, _event: RegistryEvent) {
            panic!("sink failure");
        }
    }

    #[test]
    fn dispatch_event_isolates_sink_panic() {
        dispatch_event(&PanickingSink, RegistryEvent::BackendLost { be_id: 1 });
    }
}
