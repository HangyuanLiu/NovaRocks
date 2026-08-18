use std::sync::Arc;

use novarocks_execution::runtime::fragment::io::{
    ExchangeFrame, ExchangeFrameTransmitter, FragmentIoError, FragmentIoErrorKind,
    FragmentIoOperation,
};

use crate::BackendDataRuntime;
use crate::native::client::NativeGrpcClient;

pub(crate) fn grpc_exchange_transmitter(
    runtime: BackendDataRuntime,
) -> Arc<dyn ExchangeFrameTransmitter> {
    Arc::new(GrpcExchangeFrameTransmitter { runtime })
}

struct GrpcExchangeFrameTransmitter {
    runtime: BackendDataRuntime,
}

impl ExchangeFrameTransmitter for GrpcExchangeFrameTransmitter {
    fn transmit(&self, frame: ExchangeFrame) -> Result<(), FragmentIoError> {
        let port = u16::try_from(frame.destination.port()).map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ExchangeTransmit,
                FragmentIoErrorKind::InvalidResponse,
                format!("invalid gRPC exchange destination port: {error}"),
            )
        })?;
        let client = NativeGrpcClient::new_host_port(
            self.runtime.clone(),
            frame.destination.host().to_string(),
            port,
        )
        .map_err(|error| {
            FragmentIoError::new(
                FragmentIoOperation::ExchangeTransmit,
                FragmentIoErrorKind::Unavailable,
                error,
            )
        })?;
        client
            .exchange_unary(
                frame.destination_fragment_instance_id,
                frame.destination_node_id,
                frame.sender_id,
                frame.backend_number,
                frame.eos,
                frame.sequence,
                frame.payload,
            )
            .map_err(|error| {
                FragmentIoError::new(
                    FragmentIoOperation::ExchangeTransmit,
                    FragmentIoErrorKind::Unavailable,
                    error,
                )
            })
    }
}
