use std::alloc::Layout;
use std::sync::{Arc, Mutex};

use novarocks_execution::exec::pipeline::operator::InputConstructionOwner;
use novarocks_type_contract::ValueTypeStorageLifetime;
use std::time::Duration;

use novarocks_execution::runtime::fragment::io::{
    ExchangeFrame, ExchangeFrameTransmitter, ExchangeFuture, ExchangePermitAcquire,
    ExchangeStreamTarget, ExchangeStreamTransmitter, ExchangeTransmitRejection, FragmentIoError,
    FragmentIoErrorKind, FragmentIoOperation,
};
use novarocks_execution::task_execution::status::CancelReason;
use novarocks_execution_contract::{
    ExchangeCloseDisposition, ExchangeDeliveryRequirement, ExchangeFrameKind,
    ExchangeFrameSequence, ExchangePermitGrant, ExchangePermitRequest, ExchangeStreamClose,
};
use novarocks_proto_codec::{FieldPath, native_rpc::NativeRpcMethod};
use novarocks_proto_models::novarocks as proto;
use tokio_util::sync::CancellationToken;

use crate::native_channel_cache::NativeFlowPosition;
use crate::native_client::NativeExchangePermitReply;
use crate::{BackendDataRuntime, NativeRpcClient};

pub fn grpc_exchange_transmitter(
    runtime: BackendDataRuntime,
    send_timeout: Duration,
    #[cfg(novarocks_dev_controls)] observations: Option<
        Arc<crate::test_observation_controls::NativeObservationControls>,
    >,
) -> Arc<dyn ExchangeFrameTransmitter> {
    Arc::new(GrpcExchangeFrameTransmitter {
        runtime,
        send_timeout,
        #[cfg(novarocks_dev_controls)]
        observations,
    })
}

fn failed(kind: FragmentIoErrorKind, detail: impl Into<String>) -> ExchangeTransmitRejection {
    ExchangeTransmitRejection::Failed(FragmentIoError::new(
        FragmentIoOperation::ExchangeTransmit,
        kind,
        detail,
    ))
}

struct GrpcExchangeFrameTransmitter {
    runtime: BackendDataRuntime,
    send_timeout: Duration,
    #[cfg(novarocks_dev_controls)]
    observations: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
}

impl ExchangeFrameTransmitter for GrpcExchangeFrameTransmitter {
    fn reserve_streams(
        &self,
        targets: &[ExchangeStreamTarget],
        construction: &dyn InputConstructionOwner,
    ) -> Result<Vec<Arc<dyn ExchangeStreamTransmitter>>, ExchangeTransmitRejection> {
        if targets.iter().any(|target| {
            target.sender_count == 0
                || target.stream.source().sender_ordinal() >= target.sender_count
        }) {
            return Err(failed(
                FragmentIoErrorKind::InvalidResponse,
                "exchange stream has an invalid frozen sender ordinal/count",
            ));
        }
        let observe = |layout| {
            construction.request(layout).map_err(|error| {
                ExchangeTransmitRejection::Failed(FragmentIoError::from_domain(
                    FragmentIoOperation::ExchangeTransmit,
                    FragmentIoErrorKind::ResourceExhausted,
                    error,
                ))
            })
        };
        let keys_layout = Layout::array::<(
            crate::native_channel_identity::InlineNativeChannelIdentity,
            novarocks_execution_contract::ExchangeStreamIdentity,
        )>(targets.len())
        .map_err(|error| failed(FragmentIoErrorKind::ResourceExhausted, error.to_string()))?;
        observe(keys_layout)?;
        let mut keys = Vec::new();
        keys.try_reserve_exact(targets.len()).map_err(|_| {
            failed(
                FragmentIoErrorKind::ResourceExhausted,
                "exchange flow reservation allocation failed",
            )
        })?;
        for target in targets {
            if target.sender_count == 0
                || target.stream.source().sender_ordinal() >= target.sender_count
            {
                return Err(failed(
                    FragmentIoErrorKind::InvalidResponse,
                    "exchange stream has an invalid frozen sender ordinal/count",
                ));
            }
            let key = crate::native_channel_identity::InlineNativeChannelIdentity::from_parts(
                Some(target.stream.destination().backend_process_id()),
                target.destination.native_endpoint(),
                NativeRpcMethod::AcquireExchangePermit,
            )
            .map_err(|error| failed(FragmentIoErrorKind::InvalidResponse, error.to_string()))?;
            keys.push((key, target.stream));
        }
        observe(
            Layout::array::<NativeFlowPosition>(targets.len()).map_err(|error| {
                failed(FragmentIoErrorKind::ResourceExhausted, error.to_string())
            })?,
        )?;
        let positions = self
            .runtime
            .channels()
            .reserve_flow_streams(&keys)
            .map_err(|error| failed(FragmentIoErrorKind::ResourceExhausted, error.to_string()))?;
        observe(
            Layout::array::<Arc<dyn ExchangeStreamTransmitter>>(targets.len()).map_err(
                |error| failed(FragmentIoErrorKind::ResourceExhausted, error.to_string()),
            )?,
        )?;
        let mut streams: Vec<Arc<dyn ExchangeStreamTransmitter>> = Vec::new();
        streams.try_reserve_exact(targets.len()).map_err(|_| {
            failed(
                FragmentIoErrorKind::ResourceExhausted,
                "exchange stream capability allocation failed",
            )
        })?;
        for (target, position) in targets.iter().zip(positions) {
            let own = novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
                GrpcExchangeStream,
            >())
            .map_err(|error| {
                failed(
                    FragmentIoErrorKind::ResourceExhausted,
                    format!("exchange capability layout: {error:?}"),
                )
            })?;
            observe(own)?;
            // NativeEndpoint::clone owns a canonical String and, for a DNS
            // reference, a distinct CanonicalDnsName String. Both clones use
            // the exact source string length; they are not a shared host alias.
            let host = Layout::array::<u8>(target.destination.host().len()).map_err(|error| {
                failed(FragmentIoErrorKind::ResourceExhausted, error.to_string())
            })?;
            observe(host)?;
            if target
                .destination
                .host()
                .parse::<std::net::IpAddr>()
                .is_err()
            {
                observe(host)?;
            }
            streams.push(Arc::new(GrpcExchangeStream {
                target: target.clone(),
                runtime: self.runtime.clone(),
                send_timeout: self.send_timeout,
                _position: position,
                _storage: construction.storage_lifetime(),
                state: Mutex::new(StreamState::Idle(ExchangeFrameSequence::FIRST)),
                #[cfg(novarocks_dev_controls)]
                observations: self.observations.clone(),
            }));
        }
        Ok(streams)
    }
}

fn native_endpoint(
    target: &ExchangeStreamTarget,
) -> Result<novarocks_types::NativeEndpoint, ExchangeTransmitRejection> {
    let port = u16::try_from(target.destination.port()).map_err(|error| {
        failed(
            FragmentIoErrorKind::InvalidResponse,
            format!("invalid gRPC exchange destination port: {error}"),
        )
    })?;
    novarocks_types::NativeEndpoint::from_host_port(target.destination.host(), port)
        .map_err(|error| failed(FragmentIoErrorKind::InvalidResponse, error.to_string()))
}

#[derive(Clone, Copy, Debug)]
enum StreamState {
    Idle(ExchangeFrameSequence),
    Acquiring,
    Granted(ExchangePermitGrant),
    Sending,
    Closed,
    Failed,
}

/// A cancelled/unknown call permanently poisons this stream. The attempt owner
/// must stop it; no RPC is replayed after losing its original outcome.
struct ActiveCall<'a> {
    state: &'a Mutex<StreamState>,
    finished: bool,
}
impl ActiveCall<'_> {
    fn finish(&mut self, state: StreamState) {
        *self.state.lock().expect("exchange stream state lock") = state;
        self.finished = true;
    }
}
impl Drop for ActiveCall<'_> {
    fn drop(&mut self) {
        if !self.finished {
            *self.state.lock().expect("exchange stream state lock") = StreamState::Failed;
        }
    }
}

struct GrpcExchangeStream {
    target: ExchangeStreamTarget,
    runtime: BackendDataRuntime,
    send_timeout: Duration,
    _position: NativeFlowPosition,
    _storage: ValueTypeStorageLifetime,
    state: Mutex<StreamState>,
    #[cfg(novarocks_dev_controls)]
    observations: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
}

impl GrpcExchangeStream {
    fn client(&self) -> Result<NativeRpcClient, ExchangeTransmitRejection> {
        Ok(NativeRpcClient::new_backend_endpoint(
            self.runtime.clone(),
            native_endpoint(&self.target)?,
            self.target.stream.destination().backend_process_id(),
        ))
    }
}

impl ExchangeStreamTransmitter for GrpcExchangeStream {
    fn target(&self) -> &ExchangeStreamTarget {
        &self.target
    }

    fn acquire<'a>(
        &'a self,
        request: ExchangePermitRequest,
        stop: CancellationToken,
    ) -> ExchangeFuture<'a, Result<ExchangePermitAcquire, ExchangeTransmitRejection>> {
        Box::pin(async move {
            request
                .validate()
                .map_err(|error| failed(FragmentIoErrorKind::InvalidResponse, error.to_string()))?;
            if request.stream != self.target.stream {
                return Err(failed(
                    FragmentIoErrorKind::InvalidResponse,
                    "permit request differs from its preparation-reserved stream",
                ));
            }
            {
                let mut state = self.state.lock().expect("exchange stream state lock");
                if !matches!(*state, StreamState::Idle(sequence) if sequence == request.sequence) {
                    return Err(failed(
                        FragmentIoErrorKind::InvalidResponse,
                        "exchange stream acquire is duplicate, out of order, or closed",
                    ));
                }
                *state = StreamState::Acquiring;
            }
            let mut activity = ActiveCall {
                state: &self.state,
                finished: false,
            };
            let response = self
                .client()?
                .acquire_exchange_permit(request, self.send_timeout, stop)
                .await
                .map_err(|error| failed(FragmentIoErrorKind::Unavailable, error))?;
            match response {
                NativeExchangePermitReply::Granted(grant) => {
                    activity.finish(StreamState::Granted(grant));
                    Ok(ExchangePermitAcquire::Granted(grant))
                }
                NativeExchangePermitReply::Closed(proof) => {
                    let close = decode_close(&self.target, &proof)?;
                    activity.finish(StreamState::Closed);
                    // The queue owns the final End fact and verifies any strict
                    // close before discarding its still-unencoded pending item.
                    Ok(ExchangePermitAcquire::Closed(close))
                }
            }
        })
    }

    fn transmit<'a>(
        &'a self,
        frame: ExchangeFrame,
        stop: CancellationToken,
    ) -> ExchangeFuture<'a, Result<(), ExchangeTransmitRejection>> {
        Box::pin(async move {
            let mut frame = frame;
            if frame.target != self.target || frame.grant.request.stream != self.target.stream {
                return Err(failed(
                    FragmentIoErrorKind::InvalidResponse,
                    "exchange frame differs from its preparation-reserved stream",
                ));
            }
            let request = frame_request(&mut frame)?;
            {
                let mut state = self.state.lock().expect("exchange stream state lock");
                if !matches!(*state, StreamState::Granted(grant) if grant == frame.grant) {
                    return Err(failed(
                        FragmentIoErrorKind::InvalidResponse,
                        "exchange frame has no exact live grant or is already in flight",
                    ));
                }
                *state = StreamState::Sending;
            }
            let mut activity = ActiveCall {
                state: &self.state,
                finished: false,
            };
            let normal_closed = self
                .client()?
                .exchange_unary(request, self.send_timeout, stop)
                .await
                .map_err(|error| failed(FragmentIoErrorKind::Unavailable, error))?;
            if let Some(proof) = normal_closed {
                validate_normal_close(
                    &frame,
                    &proof,
                    #[cfg(novarocks_dev_controls)]
                    self.observations
                        .as_deref()
                        .map(|controls| {
                            controls.observation_for(
                                "exchange-snapshot",
                                self.target.stream.destination().query_execution_id(),
                            )
                        })
                        .transpose()
                        .map_err(|error| failed(FragmentIoErrorKind::InvalidResponse, error))?
                        .flatten()
                        .as_deref(),
                )?;
            }
            let next =
                if frame.grant.request.kind == ExchangeFrameKind::End {
                    StreamState::Closed
                } else {
                    StreamState::Idle(frame.grant.request.sequence.checked_next().map_err(
                        |error| failed(FragmentIoErrorKind::InvalidResponse, error.to_string()),
                    )?)
                };
            activity.finish(next);
            Ok(())
        })
    }

    fn spawn(&self, future: ExchangeFuture<'static, ()>) {
        self.runtime.handle().spawn(future);
    }
}

fn frame_request(
    frame: &mut ExchangeFrame,
) -> Result<proto::ExchangeRequest, ExchangeTransmitRejection> {
    let grant = frame.grant;
    let stream = grant.request.stream;
    // Validate before moving the original Vec into the RPC, without making a
    // second encoded payload allocation.
    grant
        .request
        .validate()
        .map_err(|error| failed(FragmentIoErrorKind::InvalidResponse, error.to_string()))?;
    if u64::try_from(frame.payload.len())
        .ok()
        .is_none_or(|length| length > grant.request.geometry.wire_bytes())
        || (grant.request.kind == ExchangeFrameKind::End && !frame.payload.is_empty())
    {
        return Err(failed(
            FragmentIoErrorKind::InvalidResponse,
            "exchange payload exceeds its exact grant geometry",
        ));
    }
    let request = proto::ExchangeRequest {
        finst_id_hi: stream.destination_fragment_instance_id().high(),
        finst_id_lo: stream.destination_fragment_instance_id().low(),
        node_id: stream.destination_node().get(),
        source_finst_id_hi: stream.source().fragment_instance_id().high(),
        source_finst_id_lo: stream.source().fragment_instance_id().low(),
        sender_ordinal: stream.source().sender_ordinal(),
        sender_count: frame.target.sender_count,
        sender_id: frame.target.sender_id,
        be_number: frame.target.backend_number,
        eos: grant.request.kind == ExchangeFrameKind::End,
        sequence: grant.request.sequence.get(),
        payload: std::mem::take(&mut frame.payload),
        permit_token: grant.token.to_bytes().to_vec(),
        stream: Some(novarocks_task_codec::exchange_flow_codec::encode_stream(
            stream,
        )),
        frame_kind: match grant.request.kind {
            ExchangeFrameKind::Data => proto::ExchangeFrameKind::Data,
            ExchangeFrameKind::LogicalSummary => proto::ExchangeFrameKind::LogicalSummary,
            ExchangeFrameKind::WriterResults => proto::ExchangeFrameKind::WriterResults,
            ExchangeFrameKind::End => proto::ExchangeFrameKind::End,
        } as i32,
    };
    Ok(request)
}

fn decode_close(
    target: &ExchangeStreamTarget,
    proof: &proto::ExchangeNormalClosed,
) -> Result<ExchangeStreamClose, ExchangeTransmitRejection> {
    novarocks_task_codec::exchange_flow_codec::decode_normal_close(
        proof,
        target.stream,
        target.sender_count,
        FieldPath::root("exchange_response").field("normal_closed"),
    )
    .map_err(|error| failed(FragmentIoErrorKind::InvalidResponse, error.to_string()))
}

fn validate_normal_close(
    frame: &ExchangeFrame,
    proof: &proto::ExchangeNormalClosed,
    #[cfg(novarocks_dev_controls)] observer: Option<
        &dyn novarocks_execution::runtime::exchange::TestEventObserver,
    >,
) -> Result<(), ExchangeTransmitRejection> {
    let close = decode_close(&frame.target, proof)?;
    #[cfg(novarocks_dev_controls)]
    if let Some(observer) = observer {
        observer.observe(&format!(
        "NOVAROCKS_EXCHANGE_SNAPSHOT event=normal_close_validated destination_task={} attempt={} dest_finst={} node_id={} sender_finst={} sender_ordinal={} sender_count={} seq={} eos={}",
        close.stream.destination(), close.stream.destination().query_execution_id().attempt_id().get(), close.stream.destination_fragment_instance_id(), close.stream.destination_node().get(),
        close.stream.source().fragment_instance_id(), close.stream.source().sender_ordinal(), frame.target.sender_count,
        frame.grant.request.sequence.get(), frame.grant.request.kind == ExchangeFrameKind::End,
    ));
    }
    match close.disposition {
        ExchangeCloseDisposition::Consumed { final_sequence }
            if frame.grant.request.kind == ExchangeFrameKind::End
                && final_sequence == frame.grant.request.sequence =>
        {
            Ok(())
        }
        ExchangeCloseDisposition::Withdrawn
            if frame.target.delivery == ExchangeDeliveryRequirement::DemandDriven =>
        {
            Err(ExchangeTransmitRejection::DestinationCanceled(
                CancelReason::UpstreamNoLongerNeeded,
            ))
        }
        ExchangeCloseDisposition::Withdrawn => Err(failed(
            FragmentIoErrorKind::RemoteRejected,
            "MustDrain exchange destination withdrew before exact consumption",
        )),
        ExchangeCloseDisposition::Consumed { .. } => Err(failed(
            FragmentIoErrorKind::InvalidResponse,
            "normal-close Consumed does not match this stream's final End sequence",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_execution_contract::{
        ExchangeFrameGeometry, ExchangePermitToken, ExchangeSource, ExchangeStreamIdentity,
        FragmentNodeId, TaskIdentity,
    };
    use novarocks_types::{
        UniqueId,
        identity::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId},
    };

    struct RejectConstruction;
    impl InputConstructionOwner for RejectConstruction {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            panic!("invalid target must fail before construction")
        }
        fn request(
            &self,
            _: Layout,
        ) -> Result<
            (),
            novarocks_execution::exec::pipeline::operator::input_construction::ConstructionFailure,
        > {
            panic!("invalid target must fail before construction")
        }
        fn observe_opaque_upper_bound(
            &self,
            _: usize,
        ) -> Result<
            (),
            novarocks_execution::exec::pipeline::operator::input_construction::ConstructionFailure,
        > {
            panic!("unexpected construction")
        }
        fn observe(
            &self,
            _: usize,
            _: &[novarocks_execution::exec::chunk::ArrayPathStep],
            _: novarocks_execution::exec::chunk::ArrayNormalizationStep<'_>,
        ) -> Result<
            (),
            novarocks_execution::exec::pipeline::operator::input_construction::ConstructionFailure,
        > {
            panic!("unexpected construction")
        }
    }

    fn task() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }
    fn frame_and_proof() -> (ExchangeFrame, proto::ExchangeNormalClosed) {
        let destination = task();
        let stream = ExchangeStreamIdentity::try_new(
            ExchangeSource::new(task(), UniqueId::new(21, 22), 1),
            destination,
            UniqueId::new(11, 12),
            FragmentNodeId::new(7),
        )
        .unwrap();
        let frame = ExchangeFrame {
            target: ExchangeStreamTarget {
                destination: RuntimeEndpoint::new("127.0.0.1", 9090).unwrap(),
                stream,
                delivery: ExchangeDeliveryRequirement::DemandDriven,
                sender_count: 2,
                sender_id: 1,
                backend_number: 0,
            },
            grant: ExchangePermitGrant {
                request: ExchangePermitRequest {
                    stream,
                    sequence: ExchangeFrameSequence::try_new(5).unwrap(),
                    kind: ExchangeFrameKind::Data,
                    geometry: ExchangeFrameGeometry::try_new(17, 33, 11).unwrap(),
                },
                token: ExchangePermitToken::try_from_bytes([7; 16]).unwrap(),
            },
            payload: vec![1, 2, 3],
        };
        let proof = novarocks_task_codec::exchange_flow_codec::encode_normal_close(
            ExchangeStreamClose {
                stream,
                disposition: ExchangeCloseDisposition::Withdrawn,
            },
            2,
        );
        (frame, proof)
    }
    fn validate(
        frame: &ExchangeFrame,
        proof: &proto::ExchangeNormalClosed,
    ) -> Result<(), ExchangeTransmitRejection> {
        validate_normal_close(
            frame,
            proof,
            #[cfg(novarocks_dev_controls)]
            None,
        )
    }

    #[derive(Default)]
    struct ObservedConstruction(Mutex<Vec<Layout>>);
    impl InputConstructionOwner for ObservedConstruction {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            ValueTypeStorageLifetime::pure()
        }
        fn request(
            &self,
            layout: Layout,
        ) -> Result<(), novarocks_execution::exec::pipeline::operator::ConstructionFailure>
        {
            self.0.lock().unwrap().push(layout);
            Ok(())
        }
        fn observe_opaque_upper_bound(
            &self,
            _: usize,
        ) -> Result<(), novarocks_execution::exec::pipeline::operator::ConstructionFailure>
        {
            panic!("flow capability uses actual layouts")
        }
        fn observe(
            &self,
            _: usize,
            _: &[novarocks_execution::exec::chunk::ArrayPathStep],
            _: novarocks_execution::exec::chunk::ArrayNormalizationStep<'_>,
        ) -> Result<(), novarocks_execution::exec::pipeline::operator::ConstructionFailure>
        {
            panic!("no Arrow construction in flow preparation")
        }
    }

    #[test]
    fn cancelled_acquire_poisoned_capability_cannot_retry_or_dial() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let (mut frame, _) = frame_and_proof();
        frame.target.destination = RuntimeEndpoint::new(
            "127.0.0.1",
            i32::from(listener.local_addr().unwrap().port()),
        )
        .unwrap();
        frame.grant.request.sequence = ExchangeFrameSequence::FIRST;
        let runtime = crate::backend_test_support::test_backend_data_runtime();
        let transmitter = grpc_exchange_transmitter(
            runtime.clone(),
            Duration::from_secs(1),
            #[cfg(novarocks_dev_controls)]
            None,
        );
        let scope = ObservedConstruction::default();
        let streams = transmitter
            .reserve_streams(&[frame.target.clone()], &scope)
            .unwrap();
        let stop = CancellationToken::new();
        stop.cancel();
        let failure = runtime
            .block_on(streams[0].acquire(frame.grant.request, stop))
            .unwrap_err();
        assert_eq!(
            failure.failure().unwrap().kind(),
            FragmentIoErrorKind::Unavailable
        );
        let repeated = runtime
            .block_on(streams[0].acquire(frame.grant.request, CancellationToken::new()))
            .unwrap_err();
        assert_eq!(
            repeated.failure().unwrap().kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        // The shared original slot survives the failed call until actual
        // capability teardown; another caller cannot reset the stream state.
        assert!(
            transmitter
                .reserve_streams(&[frame.target.clone()], &scope)
                .is_err()
        );
        drop(streams);
        assert_eq!(
            transmitter
                .reserve_streams(&[frame.target], &scope)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn capability_construction_observes_dns_reference_and_canonical_backings() {
        let (mut frame, _) = frame_and_proof();
        frame.target.destination = RuntimeEndpoint::new("exchange.example.com", 9090).unwrap();
        let transmitter = grpc_exchange_transmitter(
            crate::backend_test_support::test_backend_data_runtime(),
            Duration::from_secs(1),
            #[cfg(novarocks_dev_controls)]
            None,
        );
        let scope = ObservedConstruction::default();
        let streams = transmitter
            .reserve_streams(&[frame.target.clone()], &scope)
            .unwrap();
        let requests = scope.0.lock().unwrap();
        // Three independent Vec backings, the actual capability Arc and two
        // distinct String clone backings; no endpoint name is borrowed.
        assert_eq!(requests.len(), 6);
        assert_eq!(
            requests[3],
            novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<
                GrpcExchangeStream,
            >())
            .unwrap()
        );
        assert_eq!(requests[4], Layout::array::<u8>(20).unwrap());
        assert_eq!(requests[5], Layout::array::<u8>(20).unwrap());
        assert_eq!(streams[0].target(), &frame.target);
    }

    #[test]
    fn invalid_frozen_sender_count_is_refused_before_tcp_dial() {
        // Exact Task process identity is now required by the target type; a
        // malformed frozen source membership must still fail before dialing.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let (mut frame, _) = frame_and_proof();
        frame.target.destination = RuntimeEndpoint::new(
            "127.0.0.1",
            i32::from(listener.local_addr().unwrap().port()),
        )
        .unwrap();
        frame.target.sender_count = 0;
        let transmitter = grpc_exchange_transmitter(
            crate::backend_test_support::test_backend_data_runtime(),
            Duration::from_secs(1),
            #[cfg(novarocks_dev_controls)]
            None,
        );
        let error = transmitter
            .reserve_streams(&[frame.target], &RejectConstruction)
            .err()
            .expect("invalid frozen target");
        assert_eq!(
            error.failure().unwrap().kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn exact_normal_close_cancels_only_one_demand_driven_destination() {
        let (frame, proof) = frame_and_proof();
        assert_eq!(
            validate(&frame, &proof),
            Err(ExchangeTransmitRejection::DestinationCanceled(
                CancelReason::UpstreamNoLongerNeeded
            ))
        );
    }

    #[test]
    fn stale_process_or_wrong_sender_never_becomes_destination_cancel() {
        let (frame, proof) = frame_and_proof();
        for case in 0..3 {
            let mut invalid = proof.clone();
            match case {
                0 => {
                    invalid.destination_task =
                        Some(novarocks_task_codec::identity::encode_task_identity(task()))
                }
                1 => invalid.source_finst_id_lo += 1,
                _ => invalid.sender_ordinal = 0,
            }
            assert_eq!(
                validate(&frame, &invalid)
                    .unwrap_err()
                    .failure()
                    .unwrap()
                    .kind(),
                FragmentIoErrorKind::InvalidResponse
            );
        }
    }

    #[test]
    fn missing_or_unfrozen_destination_proof_fails_closed() {
        let (frame, mut proof) = frame_and_proof();
        proof.destination_task = None;
        assert_eq!(
            validate(&frame, &proof)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        let (frame, mut proof) = frame_and_proof();
        proof.disposition = None;
        assert_eq!(
            validate(&frame, &proof)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
    }

    #[test]
    fn must_drain_requires_consumed_for_the_exact_final_end() {
        let (mut frame, mut proof) = frame_and_proof();
        frame.target.delivery = ExchangeDeliveryRequirement::MustDrain;
        assert_eq!(
            validate(&frame, &proof)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::RemoteRejected
        );
        proof.disposition = Some(proto::exchange_normal_closed::Disposition::Consumed(
            proto::ExchangeConsumed { final_sequence: 5 },
        ));
        assert_eq!(
            validate(&frame, &proof)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        frame.grant.request.kind = ExchangeFrameKind::End;
        frame.grant.request.geometry = ExchangeFrameGeometry::try_new(0, 0, 0).unwrap();
        frame.payload.clear();
        validate(&frame, &proof).unwrap();
        proof.disposition = Some(proto::exchange_normal_closed::Disposition::Consumed(
            proto::ExchangeConsumed { final_sequence: 4 },
        ));
        assert_eq!(
            validate(&frame, &proof)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
    }

    #[test]
    fn frame_projection_preserves_real_token_stream_kind_and_geometry() {
        let (mut frame, _) = frame_and_proof();
        let request = frame_request(&mut frame).unwrap();
        assert_eq!(request.payload, vec![1, 2, 3]);
        assert_eq!(request.permit_token, vec![7; 16]);
        assert_eq!(request.frame_kind, proto::ExchangeFrameKind::Data as i32);
        assert_eq!(request.sequence, 5);
        assert_eq!(request.sender_count, 2);
        assert_eq!(
            novarocks_task_codec::exchange_flow_codec::decode_stream(
                request.stream.as_ref().unwrap(),
                FieldPath::root("stream")
            )
            .unwrap(),
            frame.target.stream
        );
        let mut over = frame.clone();
        over.payload.resize(18, 0);
        assert_eq!(
            frame_request(&mut over)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
    }
}
