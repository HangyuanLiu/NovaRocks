//! Backend RPC data-plane capability.
//!
//! This value owns no listener, query lifecycle admission, backend identity, or
//! report policy. Role-owned gRPC services keep their wire gates and delegate
//! only exchange, lookup, typed-result fetch, and runtime-filter delivery here.

use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_types::UniqueId;

use crate::runtime::result_buffer::{TryFetchTypedResult, wait_fetch_typed_legacy};
use novarocks_execution_contract::task_execution::identity::TaskIdentity;
use novarocks_proto_models as proto;

static FETCH_RESULT_CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug, Default)]
pub struct BackendDataPlane;

impl BackendDataPlane {
    pub fn fetch_result(
        &self,
        request: proto::novarocks::FetchResultRequest,
    ) -> proto::novarocks::FetchResultResponse {
        use proto::novarocks::fetch_result_response::Status as FetchStatus;

        let Some(finst_id) = request.finst_id else {
            return fetch_response(
                FetchStatus::Error,
                "missing finst_id in FetchResultRequest".to_string(),
                0,
                false,
                Vec::new(),
            );
        };
        let finst_id = UniqueId::new(finst_id.hi, finst_id.lo);
        let call_index = FETCH_RESULT_CALLS.fetch_add(1, Ordering::SeqCst) + 1;
        if crate::config::debug_fault_inject_fetch_not_ready_count()
            .is_some_and(|limit| call_index <= limit)
        {
            return fetch_response(FetchStatus::NotReady, String::new(), 0, false, Vec::new());
        }

        match wait_fetch_typed_legacy(finst_id, request.max_wait_ms) {
            TryFetchTypedResult::Ready(result) => {
                emit_typed_fetch_marker(
                    FetchMarkerIdentity::Fragment(finst_id),
                    FetchStatus::Ready,
                    result.packet_seq,
                    result.eos,
                    result.payload.len(),
                );
                fetch_response(
                    FetchStatus::Ready,
                    String::new(),
                    result.packet_seq,
                    result.eos,
                    result.payload,
                )
            }
            TryFetchTypedResult::NotReady => {
                fetch_response(FetchStatus::NotReady, String::new(), 0, false, Vec::new())
            }
            TryFetchTypedResult::EndAcknowledged => {
                fetch_response(FetchStatus::Eof, String::new(), 0, true, Vec::new())
            }
            TryFetchTypedResult::Error(error) => {
                emit_typed_fetch_marker(
                    FetchMarkerIdentity::Fragment(finst_id),
                    FetchStatus::Error,
                    0,
                    false,
                    0,
                );
                fetch_response(FetchStatus::Error, error.message, 0, false, Vec::new())
            }
        }
    }
}

fn fetch_response(
    status: proto::novarocks::fetch_result_response::Status,
    message: String,
    packet_seq: i64,
    eos: bool,
    result_arrow_ipc: impl Into<bytes::Bytes>,
) -> proto::novarocks::FetchResultResponse {
    proto::novarocks::FetchResultResponse {
        status: status as i32,
        message,
        packet_seq,
        eos,
        result_arrow_ipc: result_arrow_ipc.into(),
    }
}

#[derive(Clone, Copy)]
enum FetchMarkerIdentity {
    Fragment(UniqueId),
    Task(TaskIdentity),
}

fn emit_typed_fetch_marker(
    identity: FetchMarkerIdentity,
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
    payload_bytes: usize,
) {
    if crate::config::debug_emit_grpc_fragment_marker()
        && should_emit_typed_fetch_marker(status, packet_seq, eos)
    {
        println!(
            "{}",
            typed_fetch_marker(identity, status, packet_seq, eos, payload_bytes)
        );
    }
}

/// Emits the role-local task-result diagnostic after the result owner has
/// settled the read, while Native Adapter owns the wire response itself.
pub(crate) fn emit_task_fetch_marker(
    identity: TaskIdentity,
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
    payload_bytes: usize,
) {
    emit_typed_fetch_marker(
        FetchMarkerIdentity::Task(identity),
        status,
        packet_seq,
        eos,
        payload_bytes,
    );
}

fn should_emit_typed_fetch_marker(
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
) -> bool {
    use proto::novarocks::fetch_result_response::Status as FetchStatus;

    match status {
        FetchStatus::Ready => packet_seq == 0 || eos,
        FetchStatus::Eof | FetchStatus::Error => true,
        FetchStatus::ResultStatusUnspecified | FetchStatus::NotReady => false,
    }
}

fn typed_fetch_marker(
    identity: FetchMarkerIdentity,
    status: proto::novarocks::fetch_result_response::Status,
    packet_seq: i64,
    eos: bool,
    payload_bytes: usize,
) -> String {
    let identity = match identity {
        FetchMarkerIdentity::Fragment(finst_id) => {
            format!("finst_hi={} finst_lo={}", finst_id.high(), finst_id.low())
        }
        FetchMarkerIdentity::Task(identity) => {
            let execution = identity.query_execution_id();
            format!(
                "query_hi={} query_lo={} attempt={} stage={} task={} backend={}",
                execution.query_id().high(),
                execution.query_id().low(),
                execution.attempt_id().get(),
                identity.stage_id().get(),
                identity.task_id().get(),
                identity.backend_process_id(),
            )
        }
    };
    format!(
        "NOVAROCKS_GRPC_FETCH_TYPED {identity} status={} packet_seq={packet_seq} eos={eos} payload_bytes={payload_bytes}",
        status as i32,
    )
}

#[cfg(test)]
mod tests {
    use super::{FetchMarkerIdentity, proto, should_emit_typed_fetch_marker, typed_fetch_marker};
    use novarocks_execution_contract::task_execution::identity::TaskIdentity;
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId, UniqueId,
    };
    use proto::novarocks::fetch_result_response::Status as FetchStatus;

    #[test]
    fn typed_fetch_marker_identifies_payload_and_eof_without_contents() {
        assert_eq!(
            typed_fetch_marker(
                FetchMarkerIdentity::Fragment(UniqueId::new(7, 9)),
                FetchStatus::Ready,
                3,
                true,
                41,
            ),
            "NOVAROCKS_GRPC_FETCH_TYPED finst_hi=7 finst_lo=9 status=1 packet_seq=3 eos=true payload_bytes=41"
        );
    }

    #[test]
    fn typed_fetch_marker_keeps_the_complete_task_route_identity() {
        let identity = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(7, 9), AttemptId::new(2).expect("attempt id"))
                .expect("execution id"),
            StageId::new(3).expect("stage id"),
            TaskId::new(4).expect("task id"),
            BackendProcessId::new_v7(),
        );
        let marker = typed_fetch_marker(
            FetchMarkerIdentity::Task(identity),
            FetchStatus::Error,
            0,
            false,
            0,
        );
        assert!(marker.contains("query_hi=7 query_lo=9 attempt=2 stage=3 task=4"));
        assert!(marker.contains(&format!("backend={}", identity.backend_process_id())));
        assert!(!marker.contains("unknown"));
    }

    #[test]
    fn typed_fetch_markers_are_limited_to_first_packet_eof_and_failure() {
        assert!(should_emit_typed_fetch_marker(FetchStatus::Ready, 0, false));
        assert!(should_emit_typed_fetch_marker(FetchStatus::Ready, 9, true));
        assert!(should_emit_typed_fetch_marker(FetchStatus::Error, 0, false));
        assert!(!should_emit_typed_fetch_marker(
            FetchStatus::Ready,
            9,
            false
        ));
        assert!(!should_emit_typed_fetch_marker(
            FetchStatus::NotReady,
            0,
            false
        ));
    }
}
