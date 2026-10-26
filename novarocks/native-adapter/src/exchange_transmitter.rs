use std::sync::Arc;
use std::time::Duration;

use novarocks_execution::runtime::fragment::io::{
    ExchangeFrame, ExchangeFrameTransmitter, ExchangeTransmitRejection, FragmentIoError,
    FragmentIoErrorKind, FragmentIoOperation,
};

use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as proto;
use prost::Message;
use tokio_util::sync::CancellationToken;

use crate::{BackendDataRuntime, NativeRpcClient};

pub fn grpc_exchange_transmitter(
    runtime: BackendDataRuntime,
    send_timeout: Duration,
    #[cfg(novarocks_dev_controls)] observations: Option<
        Arc<crate::test_observation_controls::NativeObservationControls>,
    >,
) -> Arc<dyn ExchangeFrameTransmitter> {
    let grants = crate::exchange_grant_subscription::ExchangeGrantClients::new(
        runtime.clone(),
        send_timeout,
    );
    Arc::new(GrpcExchangeFrameTransmitter {
        runtime,
        send_timeout,
        grants,
        #[cfg(novarocks_dev_controls)]
        observations,
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
    #[cfg(novarocks_dev_controls)]
    observations: Option<Arc<crate::test_observation_controls::NativeObservationControls>>,
    runtime: BackendDataRuntime,
    send_timeout: Duration,
    grants: Arc<crate::exchange_grant_subscription::ExchangeGrantClients>,
}

impl ExchangeFrameTransmitter for GrpcExchangeFrameTransmitter {
    fn deliver_unit(
        &self,
        frame: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeUnitFrame,
        stop: CancellationToken,
        activity: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeCallActivity,
        done: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeDeliveryCallback,
    ) {
        use novarocks_execution::runtime::fragment::io::exchange_unit::{
            ExchangeCallOutcome, ExchangeUnitReceipt,
        };
        use novarocks_task_codec::exchange_unit_codec as codec;
        let stream = frame.receiver.stream;
        let sequence = frame.receiver.item.sequence();
        let local = self.runtime.native_trust().local_process_identity();
        if local
            != Some(novarocks_native_trust::NativeProcessIdentity::Backend(
                stream.source().task().backend_process_id(),
            ))
        {
            done(ExchangeCallOutcome::Refused(unit_failure(
                novarocks_execution_contract::failure::FailureClass::Data,
                451,
            )));
            drop(activity);
            return;
        }
        let request = codec::encode_delivery_request(codec::DecodedExchangeUnitFrame {
            stream,
            sender_count: frame.receiver.sender_count,
            sender_id: frame.receiver.sender_id,
            backend_number: frame.receiver.backend_number,
            item: frame.receiver.item,
            payload: frame.receiver.payload,
            next_demand: frame.receiver.next_demand,
        });
        if let Err(fact) = checked_envelope(&request) {
            done(ExchangeCallOutcome::Refused(fact));
            drop(activity);
            return;
        }
        if codec::decode_delivery_request(
            request.clone(),
            FieldPath::root("outgoing_exchange_unit"),
        )
        .is_err()
        {
            done(ExchangeCallOutcome::Refused(unit_failure(
                novarocks_execution_contract::failure::FailureClass::Invariant,
                464,
            )));
            drop(activity);
            return;
        }
        let runtime = self.runtime.clone();
        let timeout = self.send_timeout;
        self.runtime.handle().spawn(async move {
            // This guard follows the real local request future, including its
            // owned DTO/payload aliases. Unknown is only a result notification;
            // the caller cannot release this guard from that callback.
            let _activity = activity;
            let client = NativeRpcClient::new_backend_endpoint(
                runtime,
                frame.destination.native_endpoint().clone(),
                stream.destination().backend_process_id(),
            );
            let outcome = match client
                .deliver_exchange_unit_async(request, timeout, stop)
                .await
            {
                Ok(response) => match codec::decode_delivery_response(
                    &response,
                    stream,
                    sequence,
                    FieldPath::root("exchange_unit_response"),
                ) {
                    Ok(codec::DecodedExchangeDeliveryOutcome::Observed { verdict, facts }) => {
                        ExchangeCallOutcome::Observed(ExchangeUnitReceipt { verdict, facts })
                    }
                    Ok(codec::DecodedExchangeDeliveryOutcome::Refused(fact)) => {
                        ExchangeCallOutcome::Refused(fact)
                    }
                    Err(_) => ExchangeCallOutcome::Unknown(
                        "exchange unit response cannot be fully validated".into(),
                    ),
                },
                Err(crate::native_client::ExchangeRpcFailure::NotProcessed(detail)) => {
                    ExchangeCallOutcome::NotProcessed(detail)
                }
                Err(crate::native_client::ExchangeRpcFailure::Unknown(detail)) => {
                    ExchangeCallOutcome::Unknown(detail)
                }
            };
            done(outcome);
        });
    }

    fn register_unit_demands(
        &self,
        destination: novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        entries: Vec<
            novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeDemandEntry,
        >,
        stop: CancellationToken,
        done: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeDemandCallback,
    ) {
        use novarocks_execution::runtime::fragment::io::exchange_unit::{
            ExchangeDemandCallOutcome, ExchangeDemandOperation,
        };
        use novarocks_task_codec::exchange_unit_codec as codec;
        let streams = entries.iter().map(|entry| entry.stream).collect::<Vec<_>>();
        let Some(first) = streams.first().copied() else {
            done(ExchangeDemandCallOutcome::Refused(unit_failure(
                novarocks_execution_contract::failure::FailureClass::Invariant,
                464,
            )));
            return;
        };
        let local = self.runtime.native_trust().local_process_identity();
        if streams.iter().any(|stream| {
            local
                != Some(novarocks_native_trust::NativeProcessIdentity::Backend(
                    stream.source().task().backend_process_id(),
                ))
        }) {
            done(ExchangeDemandCallOutcome::Refused(unit_failure(
                novarocks_execution_contract::failure::FailureClass::Data,
                451,
            )));
            return;
        }
        let entries = entries
            .into_iter()
            .map(|entry| codec::DecodedExchangeDemandEntry {
                stream: entry.stream,
                operation: match entry.operation {
                    ExchangeDemandOperation::Register(demand) => {
                        codec::ExchangeDemandOperation::Demand(demand)
                    }
                    ExchangeDemandOperation::StopSender => {
                        codec::ExchangeDemandOperation::StopSender
                    }
                },
            })
            .collect::<Vec<_>>();
        let request = match codec::encode_demand_request(
            &entries,
            FieldPath::root("outgoing_exchange_demands"),
        ) {
            Ok(request) => request,
            Err(_) => {
                done(ExchangeDemandCallOutcome::Refused(unit_failure(
                    novarocks_execution_contract::failure::FailureClass::Invariant,
                    464,
                )));
                return;
            }
        };
        if let Err(fact) = checked_envelope(&request) {
            done(ExchangeDemandCallOutcome::Refused(fact));
            return;
        }
        let runtime = self.runtime.clone();
        let timeout = self.send_timeout;
        self.runtime.handle().spawn(async move {
            let client = NativeRpcClient::new_backend_endpoint(
                runtime,
                destination.native_endpoint().clone(),
                first.destination().backend_process_id(),
            );
            let outcome = match client
                .register_exchange_demand_async(request, timeout, stop)
                .await
            {
                Ok(response) => match codec::decode_demand_response(
                    &response,
                    &streams,
                    FieldPath::root("exchange_demand_response"),
                ) {
                    Ok(results) => ExchangeDemandCallOutcome::Observed(
                        results
                            .into_iter()
                            .map(|result| (result.stream, result.outcome))
                            .collect(),
                    ),
                    Err(_) => ExchangeDemandCallOutcome::Unknown(
                        "exchange demand response cannot be fully validated".into(),
                    ),
                },
                Err(crate::native_client::ExchangeRpcFailure::NotProcessed(detail)) => {
                    ExchangeDemandCallOutcome::NotProcessed(detail)
                }
                Err(crate::native_client::ExchangeRpcFailure::Unknown(detail)) => {
                    ExchangeDemandCallOutcome::Unknown(detail)
                }
            };
            done(outcome);
        });
    }

    fn watch_unit_facts(
        &self,
        destination: novarocks_execution::runtime::endpoint::RuntimeEndpoint,
        stream: novarocks_execution_contract::ExchangeStreamIdentity,
        wake: novarocks_execution::runtime::fragment::io::exchange_unit::ExchangeGrantWake,
    ) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
        self.grants.watch(destination, stream, wake)
    }

    fn forget_unit_facts(&self, stream: novarocks_execution_contract::ExchangeStreamIdentity) {
        self.grants.forget(stream);
    }

    fn schedule_unit_retry(
        &self,
        delay: Duration,
        task: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
        self.runtime.handle().spawn(async move {
            tokio::time::sleep(delay).await;
            task();
        });
        Ok(())
    }

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
        let stream = frame.stream.ok_or_else(|| {
            failed(
                FragmentIoErrorKind::InvalidResponse,
                "exchange source has no frozen stream identity",
            )
        })?;
        if stream.destination() != target
            || stream.destination_fragment_instance_id() != frame.destination_fragment_instance_id
            || stream.destination_node().get() != frame.destination_node_id
            || stream.source().fragment_instance_id() != frame.sender_fragment_instance_id
            || stream.source().sender_ordinal() != frame.sender_ordinal
        {
            return Err(failed(
                FragmentIoErrorKind::InvalidResponse,
                "exchange frame does not match its exact stream identity",
            ));
        }
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
                stream,
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
            Some(proof) => validate_normal_close(
                &frame,
                &proof,
                #[cfg(novarocks_dev_controls)]
                self.observations
                    .as_deref()
                    .map(|controls| {
                        controls.observation_for("exchange-snapshot", target.query_execution_id())
                    })
                    .transpose()
                    .map_err(|error| failed(FragmentIoErrorKind::InvalidResponse, error))?
                    .flatten()
                    .as_deref(),
            ),
        }
    }
}

fn unit_failure(
    class: novarocks_execution_contract::failure::FailureClass,
    code: u16,
) -> novarocks_execution_contract::failure::FailureFact {
    crate::exchange_grant_subscription::exchange_failure(class, code)
}

fn checked_envelope(
    value: &impl Message,
) -> Result<(), novarocks_execution_contract::failure::FailureFact> {
    if value.encoded_len() > novarocks_task_codec::operation::NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES
    {
        Err(unit_failure(
            novarocks_execution_contract::failure::FailureClass::Resource,
            463,
        ))
    } else {
        Ok(())
    }
}

fn validate_normal_close(
    frame: &ExchangeFrame,
    proof: &proto::ExchangeNormalClosed,
    #[cfg(novarocks_dev_controls)] observer: Option<
        &dyn novarocks_execution::runtime::exchange::TestEventObserver,
    >,
) -> Result<(), ExchangeTransmitRejection> {
    let expected_task = frame.destination_task_identity.ok_or_else(|| {
        failed(
            FragmentIoErrorKind::InvalidResponse,
            "exchange destination has no frozen task identity",
        )
    })?;
    let stream = frame.stream.ok_or_else(|| {
        failed(
            FragmentIoErrorKind::InvalidResponse,
            "exchange frame omits exact stream identity",
        )
    })?;
    if stream.destination() != expected_task {
        return Err(failed(
            FragmentIoErrorKind::InvalidResponse,
            "exchange destination differs from stream",
        ));
    }
    let close = novarocks_task_codec::exchange_flow_codec::decode_normal_close(
        proof,
        stream,
        frame.sender_count,
        FieldPath::root("exchange_response").field("normal_closed"),
    )
    .map_err(|error| failed(FragmentIoErrorKind::InvalidResponse, error.to_string()))?;
    #[cfg(novarocks_dev_controls)]
    if let Some(observer) = observer {
        observer.observe(&format!(
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
        ));
    }
    Err(ExchangeTransmitRejection::NormallyClosed(close.disposition))
}

#[cfg(test)]
pub(crate) mod tests {
    use novarocks_execution::runtime::endpoint::RuntimeEndpoint;
    use novarocks_execution::runtime::fragment::io::{
        ExchangeFrame, ExchangeTransmitRejection, FragmentIoErrorKind,
    };
    use novarocks_execution::task_execution::identity::TaskIdentity;

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

    pub(crate) fn frame_and_proof() -> (ExchangeFrame, proto::ExchangeNormalClosed) {
        let task = task();
        let source = TaskIdentity::new(
            task.query_execution_id(),
            StageId::new(2).unwrap(),
            TaskId::new(2).unwrap(),
            task.backend_process_id(),
        );
        let stream = novarocks_execution_contract::ExchangeStreamIdentity::try_new(
            novarocks_execution_contract::ExchangeSource::new(source, UniqueId::new(21, 22), 1),
            task,
            UniqueId::new(11, 12),
            novarocks_execution_contract::FragmentNodeId::new(7),
        )
        .unwrap();
        let frame = ExchangeFrame {
            stream: Some(stream),
            delivery: novarocks_execution_contract::ExchangeDeliveryRequirement::DemandDriven,
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
            disposition: Some(
                novarocks_proto_models::novarocks::exchange_normal_closed::Disposition::Withdrawn(
                    novarocks_proto_models::novarocks::ExchangeWithdrawn {},
                ),
            ),
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
            #[cfg(novarocks_dev_controls)]
            None,
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
            validate_normal_close(
                &frame,
                &proof,
                #[cfg(novarocks_dev_controls)]
                None
            ),
            Err(ExchangeTransmitRejection::NormallyClosed(
                novarocks_execution_contract::ExchangeCloseDisposition::Withdrawn
            ))
        );
    }

    #[test]
    fn stale_process_or_wrong_sender_never_becomes_destination_cancel() {
        let (frame, mut proof) = frame_and_proof();
        proof.destination_task = Some(novarocks_task_codec::identity::encode_task_identity(task()));
        assert_eq!(
            validate_normal_close(
                &frame,
                &proof,
                #[cfg(novarocks_dev_controls)]
                None
            )
            .unwrap_err()
            .failure()
            .unwrap()
            .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        let (_, mut proof) = frame_and_proof();
        proof.source_finst_id_lo += 1;
        assert_eq!(
            validate_normal_close(
                &frame,
                &proof,
                #[cfg(novarocks_dev_controls)]
                None
            )
            .unwrap_err()
            .failure()
            .unwrap()
            .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        let (_, mut proof) = frame_and_proof();
        proof.sender_ordinal = 0;
        assert_eq!(
            validate_normal_close(
                &frame,
                &proof,
                #[cfg(novarocks_dev_controls)]
                None
            )
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
            validate_normal_close(
                &frame,
                &proof,
                #[cfg(novarocks_dev_controls)]
                None
            )
            .unwrap_err()
            .failure()
            .unwrap()
            .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        frame.destination_task_identity = None;
        assert_eq!(
            validate_normal_close(
                &frame,
                &proof,
                #[cfg(novarocks_dev_controls)]
                None
            )
            .unwrap_err()
            .failure()
            .unwrap()
            .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
    }
    #[test]
    fn normal_close_requires_disposition_and_preserves_consumed_final_sequence() {
        let (frame, mut proof) = frame_and_proof();
        proof.disposition = None;
        assert_eq!(
            validate_normal_close(
                &frame,
                &proof,
                #[cfg(novarocks_dev_controls)]
                None
            )
            .unwrap_err()
            .failure()
            .unwrap()
            .kind(),
            FragmentIoErrorKind::InvalidResponse
        );
        proof.disposition = Some(proto::exchange_normal_closed::Disposition::Consumed(
            proto::ExchangeConsumed { final_sequence: 5 },
        ));
        assert_eq!(
            validate_normal_close(
                &frame,
                &proof,
                #[cfg(novarocks_dev_controls)]
                None
            ),
            Err(ExchangeTransmitRejection::NormallyClosed(
                novarocks_execution_contract::ExchangeCloseDisposition::Consumed {
                    final_sequence: novarocks_execution_contract::ExchangeFrameSequence::try_new(5)
                        .unwrap()
                }
            ))
        );
        proof.disposition = Some(proto::exchange_normal_closed::Disposition::Consumed(
            proto::ExchangeConsumed { final_sequence: -1 },
        ));
        assert!(
            validate_normal_close(
                &frame,
                &proof,
                #[cfg(novarocks_dev_controls)]
                None
            )
            .unwrap_err()
            .failure()
            .is_some()
        );
    }
}
