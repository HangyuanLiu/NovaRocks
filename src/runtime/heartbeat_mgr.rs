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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::coordinator::cluster::{BackendRegistry, BeId, HeartbeatOutcome, RegistryEvent};
use crate::service::grpc_client::{NovaRocksGrpcRemoteClient, proto};

pub trait RegistryEventSink: Send + Sync + 'static {
    fn on_event(&self, event: RegistryEvent);
}

pub fn run_one_round<F>(registry: &BackendRegistry, send: &F) -> Vec<RegistryEvent>
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

pub fn grpc_heartbeat(be_id: BeId, endpoint: SocketAddr) -> HeartbeatOutcome {
    let start = Instant::now();
    let outcome = match NovaRocksGrpcRemoteClient::connect_blocking(endpoint) {
        Ok(client) => {
            let req = proto::novarocks::HeartbeatRequest {
                assigned_be_id: be_id,
                fe_epoch: 0,
            };
            match client.blocking_heartbeat(req) {
                Ok(resp) if resp.status_code == 0 => {
                    heartbeat_response_to_outcome(resp, current_time_millis())
                }
                Ok(resp) => heartbeat_response_to_outcome(resp, 0),
                Err(err) => HeartbeatOutcome::Failed { err },
            }
        }
        Err(err) => HeartbeatOutcome::Failed { err },
    };
    crate::service::metrics_http::observe_heartbeat_rtt(start.elapsed());
    outcome
}

fn heartbeat_response_to_outcome(
    resp: proto::novarocks::HeartbeatResponse,
    now_ms: i64,
) -> HeartbeatOutcome {
    if resp.status_code != 0 {
        return HeartbeatOutcome::Failed {
            err: format!(
                "heartbeat returned nonzero status_code {}",
                resp.status_code
            ),
        };
    }

    HeartbeatOutcome::Ok {
        start_epoch: resp.start_epoch,
        version: resp.version,
        num_cores: resp.num_cores,
        now_ms,
    }
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().try_into().unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn dispatch_event(sink: &dyn RegistryEventSink, event: RegistryEvent) {
    // Sink failures must not terminate the permanent heartbeat manager thread.
    let _ = catch_unwind(AssertUnwindSafe(|| sink.on_event(event)));
}

pub fn spawn(registry: Arc<BackendRegistry>, interval: Duration, sink: Arc<dyn RegistryEventSink>) {
    thread::Builder::new()
        .name("heartbeat-mgr".to_string())
        .spawn(move || {
            loop {
                for event in run_one_round(&registry, &grpc_heartbeat) {
                    dispatch_event(sink.as_ref(), event);
                }
                thread::sleep(interval);
            }
        })
        .expect("failed to spawn heartbeat-mgr thread");
}

pub struct NoopEventSink;

impl RegistryEventSink for NoopEventSink {
    fn on_event(&self, _event: RegistryEvent) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::cluster::{
        BackendRegistry, BackendState, HeartbeatOutcome, RegistryEvent,
    };
    use crate::service::grpc_client::proto;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    fn ep(p: u16) -> SocketAddr {
        format!("127.0.0.1:{p}").parse().unwrap()
    }

    #[test]
    fn one_round_marks_reachable_live_unreachable_progresses_to_lost() {
        let reg = Arc::new(BackendRegistry::new(1));
        let a = reg.add_backend(ep(9070));
        let b = reg.add_backend(ep(9071));
        let calls = Mutex::new(Vec::new());
        let send = |_be_id, endpoint: SocketAddr| -> HeartbeatOutcome {
            calls.lock().unwrap().push((_be_id, endpoint));
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
        let events = run_one_round(&reg, &send);
        assert_eq!(*calls.lock().unwrap(), vec![(a, ep(9070)), (b, ep(9071))]);
        assert_eq!(events, vec![RegistryEvent::BackendLost { be_id: b }]);
        assert_eq!(reg.live_endpoints(), vec![(a, ep(9070))]);
        let snap = reg.snapshot();
        assert!(
            snap.iter()
                .any(|e| e.be_id == b && e.state == BackendState::Lost)
        );
    }

    #[test]
    fn nonzero_heartbeat_status_maps_to_failed_outcome() {
        let outcome = heartbeat_response_to_outcome(
            proto::novarocks::HeartbeatResponse {
                start_epoch: 1,
                version: "v".into(),
                num_cores: 2,
                status_code: 7,
            },
            100,
        );
        match outcome {
            HeartbeatOutcome::Failed { err } => assert!(err.contains("7")),
            HeartbeatOutcome::Ok { .. } => panic!("nonzero status_code must not mark backend live"),
        }
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
