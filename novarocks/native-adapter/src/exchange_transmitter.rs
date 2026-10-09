use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::runtime::fragment::io::{
    ExchangeFrame, ExchangeFrameTransmitter, ExchangeTransmitRejection, FragmentIoError,
    FragmentIoErrorKind, FragmentIoOperation,
};
use novarocks_execution::task_execution::status::CancelReason;
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as proto;
use tokio_util::sync::CancellationToken;

use crate::{BackendDataRuntime, NativeRpcClient};

pub fn grpc_exchange_transmitter(
    runtime: BackendDataRuntime,
    send_timeout: Duration,
) -> Arc<dyn ExchangeFrameTransmitter> {
    Arc::new(GrpcExchangeFrameTransmitter {
        runtime,
        send_timeout,
    })
}

/// Wraps a transport-level cause as the producer's own failure.
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
}

impl ExchangeFrameTransmitter for GrpcExchangeFrameTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), ExchangeTransmitRejection> {
        self.transmit_cancellable(frame, CancellationToken::new())
    }

    fn transmit_cancellable(
        &self,
        mut frame: ExchangeFrame,
        stop: CancellationToken,
    ) -> Result<(), ExchangeTransmitRejection> {
        let port = u16::try_from(frame.destination.port()).map_err(|error| {
            failed(
                FragmentIoErrorKind::InvalidResponse,
                format!("invalid gRPC exchange destination port: {error}"),
            )
        })?;
        let target = frame.destination_task_identity.ok_or_else(|| {
            failed(
                FragmentIoErrorKind::InvalidResponse,
                "exchange destination has no frozen task identity",
            )
        })?;
        let endpoint =
            novarocks_types::NativeEndpoint::from_host_port(frame.destination.host(), port)
                .map_err(|error| failed(FragmentIoErrorKind::InvalidResponse, error.to_string()))?;
        let client = NativeRpcClient::new_backend_endpoint(
            self.runtime.clone(),
            endpoint,
            target.backend_process_id(),
        );
        let normal_closed = client
            .exchange_unary(
                frame.destination_fragment_instance_id,
                frame.destination_node_id,
                frame.sender_fragment_instance_id,
                frame.sender_ordinal,
                frame.sender_count,
                frame.sender_id,
                frame.backend_number,
                frame.eos,
                frame.sequence,
                std::mem::take(&mut frame.payload),
                self.send_timeout,
                stop,
            )
            .map_err(|error| failed(FragmentIoErrorKind::Unavailable, error))?;
        match normal_closed {
            None => Ok(()),
            Some(proof) => validate_normal_close(&frame, &proof),
        }
    }
}

fn validate_normal_close(
    frame: &ExchangeFrame,
    proof: &proto::ExchangeNormalClosed,
) -> Result<(), ExchangeTransmitRejection> {
    let expected_task = frame.destination_task_identity.ok_or_else(|| {
        failed(
            FragmentIoErrorKind::InvalidResponse,
            "exchange destination has no frozen task identity",
        )
    })?;
    let actual_task = proof.destination_task.as_ref().ok_or_else(|| {
        failed(
            FragmentIoErrorKind::InvalidResponse,
            "normal-close response omits destination task",
        )
    })?;
    let actual_task = novarocks_task_codec::identity::decode_task_identity(
        actual_task,
        FieldPath::root("exchange_response")
            .field("normal_closed")
            .field("destination_task"),
    )
    .map_err(|error| failed(FragmentIoErrorKind::InvalidResponse, error.to_string()))?;
    if actual_task != expected_task
        || proof.destination_finst_id_hi != frame.destination_fragment_instance_id.high()
        || proof.destination_finst_id_lo != frame.destination_fragment_instance_id.low()
        || proof.destination_node_id != frame.destination_node_id
        || proof.source_finst_id_hi != frame.sender_fragment_instance_id.high()
        || proof.source_finst_id_lo != frame.sender_fragment_instance_id.low()
        || proof.sender_ordinal != frame.sender_ordinal
        || proof.sender_count != frame.sender_count
    {
        return Err(failed(
            FragmentIoErrorKind::InvalidResponse,
            "normal-close response does not match frozen exchange destination and sender",
        ));
    }
    #[cfg(debug_assertions)]
    if std::env::var("NOVAROCKS_SQL_TEST_EMIT_EXCHANGE_SNAPSHOT_MARKER").is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    }) {
        eprintln!(
            "NOVAROCKS_EXCHANGE_SNAPSHOT event=normal_close_validated destination_task={} attempt={} dest_finst={} node_id={} sender_finst={} sender_ordinal={} sender_count={} seq={} eos={}",
            expected_task,
            expected_task.query_execution_id().attempt_id().get(),
            frame.destination_fragment_instance_id,
            frame.destination_node_id,
            frame.sender_fragment_instance_id,
            frame.sender_ordinal,
            frame.sender_count,
            frame.sequence,
            frame.eos,
        );
    }
    Err(ExchangeTransmitRejection::DestinationCanceled(
        CancelReason::UpstreamNoLongerNeeded,
    ))
}

#[cfg(test)]
mod tests {
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_execution::runtime::fragment::io::{
        ExchangeFrame, ExchangeTransmitRejection, FragmentIoErrorKind,
    };
    use novarocks_execution::task_execution::identity::TaskIdentity;
    use novarocks_execution::task_execution::status::CancelReason;
    use novarocks_proto_models::novarocks as proto;
    use novarocks_types::{
        UniqueId,
        identity::{AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId},
    };

    use super::validate_normal_close;

    fn task() -> TaskIdentity {
        TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).unwrap()).unwrap(),
            StageId::new(1).unwrap(),
            TaskId::new(1).unwrap(),
            BackendProcessId::new_v7(),
        )
    }

    fn frame_and_proof() -> (ExchangeFrame, proto::ExchangeNormalClosed) {
        let task = task();
        let frame = ExchangeFrame {
            destination: RuntimeEndpoint::new("127.0.0.1", 9090).unwrap(),
            destination_fragment_instance_id: UniqueId::new(11, 12),
            destination_task_identity: Some(task),
            sender_fragment_instance_id: UniqueId::new(21, 22),
            sender_ordinal: 1,
            sender_count: 2,
            destination_node_id: 7,
            sender_id: 1,
            backend_number: 0,
            sequence: 5,
            eos: false,
            payload: Vec::new(),
        };
        let proof = proto::ExchangeNormalClosed {
            destination_task: Some(novarocks_task_codec::identity::encode_task_identity(task)),
            destination_finst_id_hi: 11,
            destination_finst_id_lo: 12,
            destination_node_id: 7,
            source_finst_id_hi: 21,
            source_finst_id_lo: 22,
            sender_ordinal: 1,
            sender_count: 2,
        };
        (frame, proof)
    }

    #[test]
    fn missing_frozen_destination_process_is_refused_before_tcp_dial() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let (mut frame, _) = frame_and_proof();
        frame.destination = RuntimeEndpoint::new(
            "127.0.0.1",
            i32::from(listener.local_addr().unwrap().port()),
        )
        .unwrap();
        frame.destination_task_identity = None;
        let transmitter = super::grpc_exchange_transmitter(
            crate::backend_test_support::test_backend_data_runtime(),
            std::time::Duration::from_secs(1),
        );
        let error = transmitter.transmit(frame).unwrap_err();
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
    fn exact_normal_close_cancels_only_one_destination() {
        let (frame, proof) = frame_and_proof();
        assert_eq!(
            validate_normal_close(&frame, &proof),
            Err(ExchangeTransmitRejection::DestinationCanceled(
                CancelReason::UpstreamNoLongerNeeded
            ))
        );
    }

    #[test]
    fn stale_process_or_wrong_sender_never_becomes_destination_cancel() {
        let (frame, mut proof) = frame_and_proof();
        proof.destination_task = Some(novarocks_task_codec::identity::encode_task_identity(task()));
        assert_eq!(
            validate_normal_close(&frame, &proof)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        let (_, mut proof) = frame_and_proof();
        proof.source_finst_id_lo += 1;
        assert_eq!(
            validate_normal_close(&frame, &proof)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        let (_, mut proof) = frame_and_proof();
        proof.sender_ordinal = 0;
        assert_eq!(
            validate_normal_close(&frame, &proof)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
    }

    #[test]
    fn missing_or_unfrozen_destination_proof_fails_closed() {
        let (mut frame, mut proof) = frame_and_proof();
        proof.destination_task = None;
        assert_eq!(
            validate_normal_close(&frame, &proof)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        frame.destination_task_identity = None;
        assert_eq!(
            validate_normal_close(&frame, &proof)
                .unwrap_err()
                .failure()
                .unwrap()
                .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
    }
}
