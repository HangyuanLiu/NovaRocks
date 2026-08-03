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
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::query_execution::backend::{BeId, HeartbeatOutcome};
use crate::service::grpc_client::{NovaRocksGrpcRemoteClient, proto};

pub fn grpc_heartbeat(be_id: BeId, endpoint: SocketAddr) -> HeartbeatOutcome {
    let start = Instant::now();
    let outcome = match NovaRocksGrpcRemoteClient::connect_blocking(endpoint) {
        Ok(client) => {
            let request = proto::novarocks::HeartbeatRequest {
                assigned_be_id: be_id,
                fe_epoch: 0,
            };
            match client.blocking_heartbeat(request) {
                Ok(response) if response.status_code == 0 => {
                    heartbeat_response_to_outcome(response, current_time_millis())
                }
                Ok(response) => heartbeat_response_to_outcome(response, 0),
                Err(err) => HeartbeatOutcome::Failed { err },
            }
        }
        Err(err) => HeartbeatOutcome::Failed { err },
    };
    crate::service::metrics_http::observe_backend_heartbeat_rtt(start.elapsed());
    outcome
}

fn heartbeat_response_to_outcome(
    response: proto::novarocks::HeartbeatResponse,
    now_ms: i64,
) -> HeartbeatOutcome {
    if response.status_code != 0 {
        return HeartbeatOutcome::Failed {
            err: format!(
                "heartbeat returned nonzero status_code {}",
                response.status_code
            ),
        };
    }

    HeartbeatOutcome::Ok {
        start_epoch: response.start_epoch,
        version: response.version,
        num_cores: response.num_cores,
        now_ms,
    }
}

fn current_time_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

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
            HeartbeatOutcome::Failed { err } => assert!(err.contains('7')),
            HeartbeatOutcome::Ok { .. } => {
                panic!("nonzero status_code must not mark backend live")
            }
        }
    }
}
