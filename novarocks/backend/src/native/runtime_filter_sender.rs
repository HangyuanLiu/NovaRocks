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

//! Backend-native gRPC sender for runtime-filter envelopes.
//!
//! The sender owns only bounded unary delivery. Route authority belongs to the
//! Backend participant domain and canonical contribution/artifact semantics
//! remain outside this module.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, mpsc};

use novarocks::novarocks_logging::error;
use novarocks::runtime::global_async_runtime::data_runtime_handle;
use novarocks_protocol::filter::RuntimeFilterEnvelopeResponse;

use crate::native::client::NativeGrpcClient;
use crate::native::runtime_filter_adapter::{
    BackendNativeRouteIdentity, BackendNativeRuntimeFilterEnvelope,
    decode_runtime_filter_envelope_response, encode_runtime_filter_envelope,
};
use crate::runtime_filter::domain::{BackendAcceptStatus, BackendRemoteRoute};

const LIVE_REQUEST_CAPACITY: usize = 1024;
const LIVE_COMPLETION_CAPACITY: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BackendRuntimeFilterUnaryAck {
    identity: BackendNativeRouteIdentity,
    status: BackendAcceptStatus,
}

impl BackendRuntimeFilterUnaryAck {
    pub(crate) const fn new(
        identity: BackendNativeRouteIdentity,
        status: BackendAcceptStatus,
    ) -> Self {
        Self { identity, status }
    }

    pub(crate) const fn identity(&self) -> BackendNativeRouteIdentity {
        self.identity
    }

    pub(crate) const fn status(&self) -> BackendAcceptStatus {
        self.status
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BackendRuntimeFilterUnaryError {
    Transport(String),
    Contract(String),
}

impl BackendRuntimeFilterUnaryError {
    fn transport(error: impl Into<String>) -> Self {
        Self::Transport(error.into())
    }

    fn contract(error: impl Into<String>) -> Self {
        Self::Contract(error.into())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BackendNativeRuntimeFilterTransportEnvelope {
    envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
    deadline: Duration,
}

impl BackendNativeRuntimeFilterTransportEnvelope {
    pub(crate) fn new(
        envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
        deadline: Duration,
    ) -> Result<Self, BackendRuntimeFilterUnaryError> {
        if deadline.is_zero() {
            return Err(BackendRuntimeFilterUnaryError::contract(
                "runtime filter unary deadline must be non-zero",
            ));
        }
        Ok(Self { envelope, deadline })
    }

    pub(crate) fn into_parts(self) -> (Arc<BackendNativeRuntimeFilterEnvelope>, Duration) {
        (self.envelope, self.deadline)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BackendRuntimeFilterSinkSubmitOutcome {
    Submitted,
    QueueFull,
    Shutdown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum BackendRuntimeFilterSinkCompletion {
    Ack(BackendNativeRouteIdentity, BackendAcceptStatus),
    TransportFailure(BackendNativeRouteIdentity, BackendRuntimeFilterUnaryError),
}

/// Backend-native sink contract. It deliberately does not expose the old Core
/// router transport types; the Backend reliable transport integrates through
/// this port once it owns the physical route session.
pub(crate) trait BackendRuntimeFilterEnvelopeSink: Send + Sync {
    fn try_send(
        &self,
        route: BackendRemoteRoute,
        envelope: BackendNativeRuntimeFilterTransportEnvelope,
    ) -> BackendRuntimeFilterSinkSubmitOutcome;

    fn try_recv_completion(&self) -> Option<BackendRuntimeFilterSinkCompletion>;

    fn shutdown(&self);
}

#[async_trait::async_trait]
pub(crate) trait BackendRuntimeFilterEnvelopeUnaryClient: Send + Sync + 'static {
    async fn transmit(
        &self,
        route: BackendRemoteRoute,
        envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
        deadline: Duration,
    ) -> Result<BackendRuntimeFilterUnaryAck, BackendRuntimeFilterUnaryError>;
}

struct LiveRuntimeFilterEnvelopeUnaryClient;

#[async_trait::async_trait]
impl BackendRuntimeFilterEnvelopeUnaryClient for LiveRuntimeFilterEnvelopeUnaryClient {
    async fn transmit(
        &self,
        route: BackendRemoteRoute,
        envelope: Arc<BackendNativeRuntimeFilterEnvelope>,
        deadline: Duration,
    ) -> Result<BackendRuntimeFilterUnaryAck, BackendRuntimeFilterUnaryError> {
        let client = NativeGrpcClient::new_runtime_endpoint(route.endpoint())
            .map_err(BackendRuntimeFilterUnaryError::transport)?;
        let response = client
            .transmit_runtime_filter_envelope_async(
                encode_runtime_filter_envelope(envelope.as_ref()),
                deadline,
            )
            .await
            .map_err(BackendRuntimeFilterUnaryError::transport)?;
        decode_runtime_filter_unary_ack(response)
    }
}

fn decode_runtime_filter_unary_ack(
    response: RuntimeFilterEnvelopeResponse,
) -> Result<BackendRuntimeFilterUnaryAck, BackendRuntimeFilterUnaryError> {
    decode_runtime_filter_envelope_response(response)
        .map(|(identity, status)| BackendRuntimeFilterUnaryAck::new(identity, status))
        .map_err(BackendRuntimeFilterUnaryError::contract)
}

struct SinkRequest {
    route: BackendRemoteRoute,
    envelope: BackendNativeRuntimeFilterTransportEnvelope,
}

pub(crate) struct GrpcRuntimeFilterEnvelopeSink {
    requests: mpsc::Sender<SinkRequest>,
    completions: Mutex<mpsc::Receiver<BackendRuntimeFilterSinkCompletion>>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
}

impl GrpcRuntimeFilterEnvelopeSink {
    pub(crate) fn new() -> Arc<Self> {
        Self::new_with_client_and_capacities(
            Arc::new(LiveRuntimeFilterEnvelopeUnaryClient),
            LIVE_REQUEST_CAPACITY,
            LIVE_COMPLETION_CAPACITY,
        )
    }

    #[cfg(test)]
    fn new_for_test(
        client: Arc<dyn BackendRuntimeFilterEnvelopeUnaryClient>,
        request_capacity: usize,
        completion_capacity: usize,
    ) -> Result<Arc<Self>, String> {
        if request_capacity == 0 || completion_capacity == 0 {
            return Err("runtime filter sink capacities must be nonzero".to_string());
        }
        Ok(Self::new_with_client_and_capacities(
            client,
            request_capacity,
            completion_capacity,
        ))
    }

    fn new_with_client_and_capacities(
        client: Arc<dyn BackendRuntimeFilterEnvelopeUnaryClient>,
        request_capacity: usize,
        completion_capacity: usize,
    ) -> Arc<Self> {
        let (request_tx, request_rx) = mpsc::channel(request_capacity);
        let (completion_tx, completion_rx) = mpsc::channel(completion_capacity);
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_notify = Arc::new(Notify::new());
        let sink = Arc::new(Self {
            requests: request_tx,
            completions: Mutex::new(completion_rx),
            shutdown: Arc::clone(&shutdown),
            shutdown_notify: Arc::clone(&shutdown_notify),
        });
        match data_runtime_handle() {
            Ok(runtime) => {
                runtime.spawn(run_worker(
                    request_rx,
                    completion_tx,
                    client,
                    shutdown,
                    shutdown_notify,
                ));
            }
            Err(runtime_error) => {
                sink.shutdown.store(true, Ordering::Release);
                error!(
                    error = %runtime_error,
                    "runtime filter envelope worker could not start"
                );
            }
        }
        sink
    }
}

impl BackendRuntimeFilterEnvelopeSink for GrpcRuntimeFilterEnvelopeSink {
    fn try_send(
        &self,
        route: BackendRemoteRoute,
        envelope: BackendNativeRuntimeFilterTransportEnvelope,
    ) -> BackendRuntimeFilterSinkSubmitOutcome {
        if self.shutdown.load(Ordering::Acquire) {
            return BackendRuntimeFilterSinkSubmitOutcome::Shutdown;
        }
        match self.requests.try_send(SinkRequest { route, envelope }) {
            Ok(()) => BackendRuntimeFilterSinkSubmitOutcome::Submitted,
            Err(mpsc::error::TrySendError::Full(_)) => {
                BackendRuntimeFilterSinkSubmitOutcome::QueueFull
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                BackendRuntimeFilterSinkSubmitOutcome::Shutdown
            }
        }
    }

    fn try_recv_completion(&self) -> Option<BackendRuntimeFilterSinkCompletion> {
        self.completions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .try_recv()
            .ok()
    }

    fn shutdown(&self) {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            self.shutdown_notify.notify_waiters();
            self.shutdown_notify.notify_one();
        }
    }
}

impl Drop for GrpcRuntimeFilterEnvelopeSink {
    fn drop(&mut self) {
        self.shutdown();
    }
}

async fn run_worker(
    mut requests: mpsc::Receiver<SinkRequest>,
    completions: mpsc::Sender<BackendRuntimeFilterSinkCompletion>,
    client: Arc<dyn BackendRuntimeFilterEnvelopeUnaryClient>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
) {
    loop {
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let request = tokio::select! {
            biased;
            _ = shutdown_notify.notified() => break,
            request = requests.recv() => match request {
                Some(request) => request,
                None => break,
            },
        };
        let (envelope, deadline) = request.envelope.into_parts();
        let requested_identity = *envelope.route_identity();
        let result = tokio::select! {
            biased;
            _ = shutdown_notify.notified() => break,
            result = client.transmit(request.route, envelope, deadline) => result,
        };
        let completion = match result {
            Ok(ack) if ack.identity() == requested_identity => {
                BackendRuntimeFilterSinkCompletion::Ack(ack.identity(), ack.status())
            }
            Ok(ack) => BackendRuntimeFilterSinkCompletion::TransportFailure(
                requested_identity,
                BackendRuntimeFilterUnaryError::contract(format!(
                    "runtime filter ACK identity mismatch: requested={requested_identity:?} acked={:?}",
                    ack.identity(),
                )),
            ),
            Err(error) => {
                BackendRuntimeFilterSinkCompletion::TransportFailure(requested_identity, error)
            }
        };
        tokio::select! {
            biased;
            _ = shutdown_notify.notified() => break,
            sent = completions.send(completion) => {
                if sent.is_err() {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::decode_runtime_filter_unary_ack;
    use crate::runtime_filter::domain::BackendAcceptStatus;
    use novarocks_protocol::filter::{
        RuntimeFilterAcceptStatus, RuntimeFilterContributionRouteIdentity,
        RuntimeFilterEnvelopeResponse, RuntimeFilterRouteIdentity,
        runtime_filter_route_identity::Value,
    };

    fn contribution_route() -> RuntimeFilterRouteIdentity {
        RuntimeFilterRouteIdentity {
            value: Some(Value::Contribution(
                RuntimeFilterContributionRouteIdentity {
                    producer_binding_id: 17,
                    fragment_instance_id: Some(novarocks_protocol::common::UniqueId {
                        hi: 18,
                        lo: 19,
                    }),
                    partition_id: 0,
                    sequence: 0,
                },
            )),
        }
    }

    #[test]
    fn unary_ack_decode_retains_native_route_and_strict_status() {
        let ack = decode_runtime_filter_unary_ack(RuntimeFilterEnvelopeResponse {
            acked_route_identity: Some(contribution_route()),
            accept_status: RuntimeFilterAcceptStatus::Duplicate as i32,
            rejection_reason: String::new(),
        })
        .unwrap();
        assert_eq!(ack.status(), BackendAcceptStatus::Duplicate);
        assert!(ack.identity().as_contribution().is_some());
    }

    #[test]
    fn unary_ack_decode_rejects_success_with_rejection_reason() {
        let error = decode_runtime_filter_unary_ack(RuntimeFilterEnvelopeResponse {
            acked_route_identity: Some(contribution_route()),
            accept_status: RuntimeFilterAcceptStatus::Accepted as i32,
            rejection_reason: "unexpected".to_string(),
        })
        .unwrap_err();
        assert!(matches!(
            error,
            super::BackendRuntimeFilterUnaryError::Contract(_)
        ));
    }
}
