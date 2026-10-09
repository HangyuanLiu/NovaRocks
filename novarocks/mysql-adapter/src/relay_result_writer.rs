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

//! Frame validated Backend text rows without rebuilding their values or rows.

use std::io;
use std::time::Duration;

use novarocks_query_application::api::{
    QueryExecutionError, QueryExecutionErrorKind, ResultDelivery, ResultRowCarrier,
    RootSegmentDelivery, SchemaDelivery,
};
use novarocks_query_application::protocol_delivery::StreamingStatementResult;
use novarocks_query_application::session_control::GovernedStatementVisibilitySealOutcome;
use novarocks_result_contract::{RootOutputKind, ValidatedClientBody};
use opensrv_mysql::{
    CapabilityFlags, ErrorKind, OwnedStreamingMysqlWriter, QueryResultWriter, StatusFlags,
};
use tokio::io::AsyncWrite;
use tokio::time::Instant;

use crate::governed_result_writer::{
    MysqlStatementWriteOutcome, is_terminal_cancellation, wait_terminal_result_failure,
};

pub(crate) const ACTIVE_WRITE_DEADLINE: Duration = Duration::from_secs(30);
pub(crate) const CLOSING_DEADLINE: Duration = Duration::from_secs(5);
// Frozen complete object allowance: metadata, coalescer/index, diagnostic,
// current segment and a compacted tail coexist during the transfer.
pub(crate) const CLOSING_OBJECT_BYTES: u64 = 8 * 1024 * 1024;

enum WriteInterruption {
    Query(QueryExecutionError),
    Io(io::Error),
}

pub(crate) async fn write_relay_result_one<'writer, W: AsyncWrite + Unpin>(
    mut result: StreamingStatementResult,
    schema: SchemaDelivery,
    results: QueryResultWriter<'writer, W>,
    more_results: bool,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")] hook: Option<
        crate::mysql_write_gate::late_binding::MysqlWriteRelayHook,
    >,
    #[cfg(feature = "mem-1-m07-closing-pressure")] pressure_hook: Option<
        crate::closing_pressure_gate::relay::PressureRelayHook,
    >,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    let limits = results.protocol_limits();
    let profile = match schema.row_carrier() {
        ResultRowCarrier::Relayed {
            kind: RootOutputKind::ClientRows,
            client_rows: Some(profile),
        } if !results.is_binary()
            && profile.row_payload_bytes().get() as usize <= limits.row_bytes =>
        {
            profile
        }
        _ => {
            let error = invalid("MySQL text delivery requires a supported ClientRows carrier");
            schema.fail(error.clone());
            let _ = result.fail();
            return Err(io_error(error));
        }
    };
    let mut failure = result.failure_view().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "relayed result has no failure observation",
        )
    })?;
    let capabilities = results.client_capabilities();
    let metadata =
        crate::relay_metadata::preflight_result_schema(schema.schema(), capabilities, limits)
            .and_then(|()| crate::mysql_columns_for_result_fields(schema.schema().fields()))
            .and_then(|columns| {
                crate::relay_metadata::frozen_result_metadata(&columns, capabilities, limits)
            });
    let metadata = match metadata {
        Ok(metadata) => metadata,
        Err(error) => {
            schema.fail(invalid(error.to_string()));
            let _ = result.fail();
            return Err(error);
        }
    };
    let deadline = Instant::now() + ACTIVE_WRITE_DEADLINE;
    let mut lease = tokio::time::timeout_at(deadline, results.into_streaming_result())
        .await
        .map_err(|_| timeout_error())??;
    lease.writer().start_metadata(metadata)?;
    let cancellation = result.cancellation();
    let started = tokio::select! {
        biased;
        error = wait_terminal_result_failure(&mut failure, &cancellation) => Err(WriteInterruption::Query(error)),
        outcome = tokio::time::timeout_at(deadline, lease.writer().finish_metadata()) => outcome.map_err(|_| timeout_error()).and_then(|outcome| outcome).map_err(WriteInterruption::Io),
    };
    if let Err(error) = started {
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        if let Some(hook) = &hook {
            hook.fail_fixture(crate::mysql_write_gate::GateFailure::Transition);
        }
        return match error {
            WriteInterruption::Query(error) => {
                schema.fail(error.clone());
                close_relay(
                    result,
                    lease,
                    error,
                    None,
                    None,
                    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                    hook.as_ref(),
                    #[cfg(feature = "mem-1-m07-closing-pressure")]
                    pressure_hook.as_ref(),
                )
                .await
            }
            WriteInterruption::Io(error) => {
                schema.fail(invalid(error.to_string()));
                drop(lease);
                let _ = result.client_disconnected();
                Err(error)
            }
        };
    }
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    if let Some(hook) = &hook {
        if let Err(error) = hook.begin_rows(lease.receipt()) {
            schema.fail(invalid(error.to_string()));
            drop(lease);
            let _ = result.fail();
            return Err(error);
        }
    }
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    if let Some(pressure) = &pressure_hook {
        if let Err(original) = pressure.begin_rows(lease.receipt()) {
            schema.fail(invalid("pressure original Rows hook refused"));
            drop(lease);
            let _ = result.fail();
            return Err(original);
        }
    }
    schema.complete();
    let mut resident_window = None;
    loop {
        let cancellation = result.cancellation();
        let next = tokio::select! {
            biased;
            error = wait_terminal_result_failure(&mut failure, &cancellation) => Err(error),
            next = result.next_delivery() => next.and_then(|next| next.ok_or_else(|| invalid("relayed stream ended without a success delivery"))),
            _ = tokio::time::sleep_until(deadline) => Err(QueryExecutionError::new(QueryExecutionErrorKind::DeadlineExceeded, "MySQL response write deadline expired")),
        };
        let delivery = match next {
            Ok(delivery) => delivery,
            Err(error) => {
                #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                if let Some(hook) = &hook {
                    hook.fail_fixture(crate::mysql_write_gate::GateFailure::Transition);
                }
                return close_relay(
                    result,
                    lease,
                    error,
                    None,
                    resident_window,
                    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                    hook.as_ref(),
                    #[cfg(feature = "mem-1-m07-closing-pressure")]
                    pressure_hook.as_ref(),
                )
                .await;
            }
        };
        match delivery {
            ResultDelivery::Segment(delivery) => {
                resident_window = delivery.resident_window();
                let body = delivery.client_rows().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "relayed MySQL delivery has no ClientRows spans",
                    )
                })?;
                // The actor validates each complete body before delivery. The
                // writer checks its own cursor as well, across segment receipts.
                let expected_remaining = body.before().remaining() as usize;
                let actual_remaining = lease.receipt().logical_remaining();
                if actual_remaining != expected_remaining
                    || delivery.kind() != RootOutputKind::ClientRows
                    || body.body().len() > profile.segment_bytes()
                {
                    let error =
                        invalid("MySQL framing frontier differs from the validated root body");
                    delivery.fail(error.clone());
                    drop(lease);
                    let _ = result.fail();
                    return Err(io_error(error));
                }
                let cancellation = result.cancellation();
                let written = tokio::select! {
                    biased;
                    error = wait_terminal_result_failure(&mut failure, &cancellation) => Err(WriteInterruption::Query(error)),
                    outcome = tokio::time::timeout_at(deadline, write_body(lease.writer(), &body)) => outcome.map_err(|_| timeout_error()).and_then(|outcome| outcome).map_err(WriteInterruption::Io),
                };
                if let Err(error) = written {
                    return match error {
                        WriteInterruption::Query(error) => {
                            #[cfg(feature = "mem-1-m07-closing-pressure")]
                            if let Some(pressure) = &pressure_hook {
                                // Actual borrowed write_body future has dropped when select! returns.
                                if error.kind() == QueryExecutionErrorKind::Cancelled
                                    && matches!(cancellation.reason(), Some(novarocks_query_application::cancellation::QueryCancellationReason::ExplicitKill { .. })) {
                                    if let Err(original) = pressure.record_cancel(lease.receipt()) {
                                        delivery.fail(error);
                                        drop(lease);
                                        let _ = result.fail();
                                        return Err(original);
                                    }
                                } else {
                                    pressure.fail(crate::closing_pressure_gate::Failure::Transition);
                                }
                            }
                            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                            if let Some(hook) = &hook {
                                // select! has returned: its actual Data write future
                                // has dropped before borrowing this original receipt.
                                if error.kind() == QueryExecutionErrorKind::Cancelled
                                    && matches!(cancellation.reason(), Some(novarocks_query_application::cancellation::QueryCancellationReason::ExplicitKill { .. }))
                                {
                                    if let Err(gate_error) = hook.record_cancel_and_resume(lease.receipt()) {
                                        delivery.fail(error);
                                        drop(lease);
                                        let _ = result.fail();
                                        return Err(gate_error);
                                    }
                                } else {
                                    hook.fail_fixture(crate::mysql_write_gate::GateFailure::Transition);
                                }
                            }
                            close_relay(
                                result,
                                lease,
                                error,
                                Some(delivery),
                                resident_window,
                                #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                                hook.as_ref(),
                                #[cfg(feature = "mem-1-m07-closing-pressure")]
                                pressure_hook.as_ref(),
                            )
                            .await
                        }
                        WriteInterruption::Io(error) => {
                            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                            if let Some(hook) = &hook {
                                hook.fail_fixture(crate::mysql_write_gate::GateFailure::Transition);
                            }
                            delivery.fail(invalid(error.to_string()));
                            drop(lease);
                            let _ = result.client_disconnected();
                            Err(error)
                        }
                    };
                }
                delivery.complete();
            }
            ResultDelivery::End(delivery) => {
                #[cfg(feature = "mem-1-m07-closing-pressure")]
                if let Some(pressure) = &pressure_hook {
                    pressure.fail(crate::closing_pressure_gate::Failure::Transition);
                    let original =
                        invalid("pressure target ended without its original cancelled Rows cut");
                    delivery.fail(original.clone());
                    drop(lease);
                    let _ = result.fail();
                    return Err(io_error(original));
                }
                #[cfg(feature = "mem-1-m07-exact-mysql-write")]
                if let Some(hook) = &hook {
                    // The armed target must observe its exact interrupted Data
                    // cut; ordinary success End is not experiment acceptance.
                    hook.fail_fixture(crate::mysql_write_gate::GateFailure::Transition);
                    let error = invalid("fixture target ended without an exact cancelled Data cut");
                    delivery.fail(error.clone());
                    drop(lease);
                    let _ = result.fail();
                    return Err(io_error(error));
                }
                if lease.receipt().phase != opensrv_mysql::WritePhase::Boundary {
                    let error = invalid("root End arrived inside a MySQL row");
                    delivery.fail(error.clone());
                    drop(lease);
                    let _ = result.fail();
                    return Err(io_error(error));
                }
                if result.seal_success_visibility()
                    != GovernedStatementVisibilitySealOutcome::Sealed
                {
                    let error = invalid("relayed result lost its success visibility generation");
                    delivery.fail(error.clone());
                    drop(lease);
                    let _ = result.fail();
                    return Err(io_error(error));
                }
                let terminal = success_payload(capabilities, more_results);
                match tokio::time::timeout_at(deadline, lease.finish(&terminal)).await {
                    Ok(Ok(writer)) => {
                        delivery.complete();
                        let _ = result.complete();
                        return Ok(MysqlStatementWriteOutcome::Continue(writer));
                    }
                    outcome => {
                        let error = match outcome {
                            Ok(Err(error)) => error,
                            _ => timeout_error(),
                        };
                        delivery.fail(invalid(error.to_string()));
                        let _ = result.client_disconnected();
                        return Err(error);
                    }
                }
            }
        }
    }
}

async fn close_relay<'writer, W: AsyncWrite + Unpin>(
    mut result: StreamingStatementResult,
    mut lease: opensrv_mysql::StreamingResponseLease<'writer, W>,
    error: QueryExecutionError,
    delivery: Option<RootSegmentDelivery>,
    resident_window: Option<novarocks_query_application::api::RootRelayResidentWindow>,
    #[cfg(feature = "mem-1-m07-exact-mysql-write")] hook: Option<
        &crate::mysql_write_gate::late_binding::MysqlWriteRelayHook,
    >,
    #[cfg(feature = "mem-1-m07-closing-pressure")] pressure_hook: Option<
        &crate::closing_pressure_gate::relay::PressureRelayHook,
    >,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    if matches!(result.cancellation().reason(),
        Some(novarocks_query_application::cancellation::QueryCancellationReason::ExplicitKillConnection { .. }
            | novarocks_query_application::cancellation::QueryCancellationReason::ServerShutdown
            | novarocks_query_application::cancellation::QueryCancellationReason::ClientDisconnected)) {
        if let Some(delivery) = delivery { delivery.fail(error.clone()); }
        drop(lease);
        let _ = result.client_disconnected();
        return Err(io_error(error));
    }
    // The accepted cut stops further result work and releases only computation
    // capacity. Pool exhaustion never waits while holding this payload.
    let capacity = match result.try_closing_capacity(
        is_terminal_cancellation(&error) && result.cancellation().is_cancelled(),
    ) {
        Ok(capacity) => capacity,
        Err(admission) => {
            #[cfg(any(
                feature = "mem-1-m07-exact-mysql-write",
                feature = "mem-1-m07-closing-pressure"
            ))]
            let receipt = lease.receipt();
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            let legacy_capacity_refused = matches!(
                admission,
                novarocks_workload_control::WorkError::Capacity(_)
            );
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            let pressure_record = match pressure_hook {
                // The original writer/receipt is still live. Record before its actual destructor.
                Some(pressure) => Some(pressure.record_capacity_refusal(receipt, admission)),
                None => {
                    let _ = admission;
                    None
                }
            };
            if let Some(delivery) = delivery {
                delivery.fail(error.clone());
            }
            drop(lease);
            let _ = result.client_disconnected();
            let original = io_error(error);
            #[cfg(feature = "mem-1-m07-exact-mysql-write")]
            let original = if legacy_capacity_refused {
                match hook {
                    Some(hook) => hook.prescribed_eof(crate::mysql_write_gate::late_binding::PrescribedRelayEofKind::ClosingAdmissionCapacityRefused, receipt, original),
                    None => original,
                }
            } else {
                original
            };
            #[cfg(feature = "mem-1-m07-closing-pressure")]
            let original = match pressure_record {
                Some(Err(hook_source)) => {
                    crate::closing_pressure_gate::relay::preserve_refusal_hook_failure(
                        original,
                        hook_source,
                    )
                }
                _ => original,
            };
            #[cfg(not(any(
                feature = "mem-1-m07-exact-mysql-write",
                feature = "mem-1-m07-closing-pressure"
            )))]
            let _ = admission;
            return Err(original);
        }
    };
    let resident_items = resident_window
        .as_ref()
        .map(|window| window.freeze())
        .unwrap_or([None, None]);
    let mut resident_bodies = resident_items
        .iter()
        .flatten()
        .filter_map(|item| item.client_rows());
    let current_body = resident_bodies
        .next()
        .or_else(|| delivery.as_ref().and_then(RootSegmentDelivery::client_rows));
    let next_body = resident_bodies.next();
    let closing_cursor = lease.receipt();
    let buffered_row_bytes = lease.writer().buffered_row_bytes();
    let resident = resident_tail(
        closing_cursor,
        buffered_row_bytes,
        current_body.as_ref(),
        next_body.as_ref(),
    );
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    if let Some(hook) = hook {
        hook.record_original_freeze(
            crate::mysql_write_gate::original_freeze::OriginalFreezeScalars::capture(
                resident_window.is_some(),
                &resident_items,
                delivery.as_ref(),
                closing_cursor,
                buffered_row_bytes,
                current_body.as_ref(),
                next_body.as_ref(),
                resident.as_deref(),
            ),
        );
    }
    let backing_check = capacity.check_backing_total(CLOSING_OBJECT_BYTES);
    if backing_check.is_err() || resident.is_none() {
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        let prescribed_missing = backing_check.is_ok()
            && resident.is_none()
            && has_missing_resident_tail(
                closing_cursor,
                buffered_row_bytes,
                current_body.as_ref(),
                next_body.as_ref(),
            );
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        let receipt = lease.receipt();
        if let Some(delivery) = delivery {
            delivery.fail(error.clone());
        }
        drop(lease);
        let _ = result.client_disconnected();
        let original = io_error(error);
        #[cfg(feature = "mem-1-m07-exact-mysql-write")]
        let original = if prescribed_missing {
            match hook {
                Some(hook) => hook.prescribed_eof(crate::mysql_write_gate::late_binding::PrescribedRelayEofKind::MissingResidentTail, receipt, original),
                None => original,
            }
        } else {
            original
        };
        return Err(original);
    }
    // The full new position covers old+new coexistence before this compact
    // copy. Only the current row's unsent bytes survive, never following rows.
    let resident = resident.unwrap();
    let size = resident.iter().map(|bytes| bytes.len()).sum();
    let mut bytes = Vec::with_capacity(size);
    for part in resident {
        bytes.extend_from_slice(part);
    }
    let mut tail = Vec::with_capacity(1);
    if !bytes.is_empty() {
        tail.push(opensrv_mysql::ResidentTailPart::new(
            std::sync::Arc::from(bytes),
            0..size,
        )?);
    }
    // This item was prefetched but not delivered: dropping it grants no ACK.
    drop(resident_items);
    let payload = error_payload(&error, lease.writer().protocol_limits().diagnostic_bytes);
    let kind = if is_terminal_cancellation(&error) {
        ErrorKind::ER_QUERY_INTERRUPTED
    } else {
        ErrorKind::ER_UNKNOWN_ERROR
    };
    let writer = match lease.into_closing_lease(tail, kind, &payload[9..]) {
        Ok(writer) => writer,
        Err((lease, rejection)) => {
            if let Some(delivery) = delivery {
                delivery.fail(error);
            }
            drop(lease);
            let _ = result.client_disconnected();
            return Err(rejection);
        }
    };
    drop(payload);
    let mut closing = match result.into_closing_delivery(writer, capacity, CLOSING_OBJECT_BYTES) {
        Ok(closing) => closing,
        Err((result, writer, _capacity)) => {
            if let Some(delivery) = delivery {
                delivery.fail(error);
            }
            drop(writer);
            let _ = result.client_disconnected();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "closing capacity does not cover its protocol owner",
            ));
        }
    };
    if let Some(delivery) = delivery {
        delivery.fail(error);
    }
    // No Native stream, ordinary grant or root payload is held by this wait.
    // Repeated KILL cannot restart the independent absolute closing deadline.
    #[cfg(feature = "mem-1-m07-closing-pressure")]
    let finished = tokio::time::timeout(CLOSING_DEADLINE, async {
        // Original Closing observation and physical Gate hold both run INSIDE this original 5s.
        if let Some(pressure) = pressure_hook {
            return pressure
                .finish_original_closing(closing_cursor, &mut closing)
                .await;
        }
        closing.writer_mut().finish().await
    })
    .await;
    #[cfg(not(feature = "mem-1-m07-closing-pressure"))]
    let finished = tokio::time::timeout(CLOSING_DEADLINE, closing.writer_mut().finish()).await;
    match finished {
        Ok(Ok(writer)) => {
            let _ = closing.settle_after_writer_exit().await;
            writer.no_more_results().await?;
            Ok(MysqlStatementWriteOutcome::Terminated)
        }
        Ok(Err(error)) => {
            drop(closing);
            Err(error)
        }
        Err(_) => {
            drop(closing);
            Err(timeout_error())
        }
    }
}

/// Classify only a validated, contiguous prefix whose current row ends after W2.
/// No second freeze, body traversal, decode, clone, or repaired framing state.
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
fn has_missing_resident_tail(
    cursor: opensrv_mysql::FramingCursor,
    buffered_row_bytes: usize,
    current: Option<&ValidatedClientBody<'_>>,
    next: Option<&ValidatedClientBody<'_>>,
) -> bool {
    if cursor.phase != opensrv_mysql::WritePhase::Row
        || !cursor.row_has_started()
        || !cursor
            .logical_remaining()
            .checked_sub(buffered_row_bytes)
            .is_some_and(|remaining| remaining > 0)
    {
        return false;
    }
    let Some(current) = current else {
        return false;
    };
    if next.is_some_and(|next| next.before() != current.after()) {
        return false;
    }
    let last = next.unwrap_or(current);
    current.before().completed_rows() <= cursor.rows_completed
        && current.after().completed_rows() == cursor.rows_completed
        && last.after().completed_rows() == cursor.rows_completed
        && last.after().remaining() > 0
}

pub(crate) fn resident_tail<'a>(
    cursor: opensrv_mysql::FramingCursor,
    buffered_row_bytes: usize,
    current: Option<&ValidatedClientBody<'a>>,
    next: Option<&ValidatedClientBody<'a>>,
) -> Option<Vec<&'a [u8]>> {
    use opensrv_mysql::WritePhase;
    if matches!(cursor.phase, WritePhase::Boundary | WritePhase::Metadata)
        || (cursor.phase == WritePhase::Row && !cursor.row_has_started())
    {
        return Some(Vec::new());
    }
    if cursor.phase != WritePhase::Row {
        return None;
    }
    let remaining = cursor.logical_remaining().checked_sub(buffered_row_bytes)?;
    if remaining == 0 {
        return Some(Vec::new());
    }
    let current = current?;
    if let Some(next) = &next {
        if next.before() != current.after() {
            return None;
        }
    }
    let mut parts = Vec::with_capacity(2);
    let mut total = 0usize;
    let mut complete = false;
    for body in std::iter::once(current).chain(next) {
        let mut rows = body.before().completed_rows();
        for span in body.payload_spans() {
            if rows == cursor.rows_completed {
                total = total.checked_add(span.bytes.len())?;
                parts.push(span.bytes);
                if span.completes_row {
                    complete = true;
                    break;
                }
            }
            if span.completes_row {
                rows += 1;
            }
        }
        if complete {
            break;
        }
    }
    if !complete || total < remaining {
        return None;
    }
    let mut skip = total - remaining;
    for part in &mut parts {
        let prefix = skip.min(part.len());
        *part = &part[prefix..];
        skip -= prefix;
    }
    Some(parts)
}

pub(crate) async fn write_body<W: AsyncWrite + Unpin>(
    writer: &mut OwnedStreamingMysqlWriter<W>,
    body: &ValidatedClientBody<'_>,
) -> io::Result<()> {
    for span in body.payload_spans() {
        if let Some(total) = span.starts_row {
            if span.completes_row && span.bytes.len() <= writer.buffer_capacity() {
                if !writer.queue_small_row(span.bytes)? {
                    writer.flush_pending().await?;
                    if !writer.queue_small_row(span.bytes)? {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "small row cannot fit its fixed coalescer",
                        ));
                    }
                }
                continue;
            }
            writer.flush_pending().await?;
            writer.start_row(total.get())?;
        }
        writer.write_slice(span.bytes).await?;
    }
    // Do not wait for another batch or a full buffer to expose a short row.
    writer.flush_pending().await?;
    writer.flush_socket().await
}

pub(crate) fn success_payload(capabilities: CapabilityFlags, more: bool) -> Vec<u8> {
    let status = if more {
        StatusFlags::SERVER_MORE_RESULTS_EXISTS.bits()
    } else {
        0
    };
    if capabilities.contains(CapabilityFlags::CLIENT_DEPRECATE_EOF) {
        let mut payload = vec![0xfe, 0, 0];
        if capabilities.contains(CapabilityFlags::CLIENT_PROTOCOL_41) {
            payload.extend_from_slice(&status.to_le_bytes());
            payload.extend_from_slice(&[0, 0]);
        } else if capabilities.contains(CapabilityFlags::CLIENT_TRANSACTIONS) {
            payload.extend_from_slice(&status.to_le_bytes());
        }
        if capabilities.contains(CapabilityFlags::CLIENT_SESSION_TRACK) {
            payload.push(0);
        }
        payload
    } else {
        vec![0xfe, 0, 0, status as u8, (status >> 8) as u8]
    }
}
pub(crate) fn error_payload(error: &QueryExecutionError, limit: usize) -> Vec<u8> {
    let kind = if is_terminal_cancellation(error) {
        ErrorKind::ER_QUERY_INTERRUPTED
    } else {
        ErrorKind::ER_UNKNOWN_ERROR
    };
    let message = error.message();
    let mut payload = Vec::with_capacity(message.len().min(limit) + 9);
    payload.push(0xff);
    payload.extend_from_slice(&(kind as u16).to_le_bytes());
    payload.push(b'#');
    payload.extend_from_slice(kind.sqlstate());
    payload.extend_from_slice(&message.as_bytes()[..message.len().min(limit)]);
    payload
}
pub(crate) fn invalid(message: impl Into<String>) -> QueryExecutionError {
    QueryExecutionError::new(QueryExecutionErrorKind::InvalidRequest, message.into())
}
pub(crate) fn io_error(error: QueryExecutionError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}
pub(crate) fn timeout_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "MySQL response write deadline expired",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_result_contract::{ClientRowProfile, ClientRowStreamCursor};
    use opensrv_mysql::{ProtocolLimits, U24_MAX, WritePhase};
    use tokio::io::AsyncReadExt;

    fn packets(mut bytes: &[u8]) -> Vec<(u8, &[u8])> {
        let mut result = Vec::new();
        while !bytes.is_empty() {
            let size =
                usize::from(bytes[0]) | usize::from(bytes[1]) << 8 | usize::from(bytes[2]) << 16;
            result.push((bytes[3], &bytes[4..4 + size]));
            bytes = &bytes[4 + size..];
        }
        result
    }

    #[tokio::test]
    async fn closing_tail_covers_both_validated_items_and_excludes_following_rows() {
        let profile = ClientRowProfile::try_new(16, 64).unwrap();
        let first = ClientRowStreamCursor::new()
            .validate_body(profile, &[5, 0, 0, 0, b'a', b'b'])
            .unwrap();
        let next = first
            .after()
            .validate_body(profile, &[b'c', b'd', b'e', 1, 0, 0, 0, b'f'])
            .unwrap();
        let mut writer =
            OwnedStreamingMysqlWriter::new(Vec::new(), ProtocolLimits::default(), 1).unwrap();
        writer.start_row(5).unwrap();
        writer.write_slice(b"a").await.unwrap();
        let tail = resident_tail(writer.receipt(), 0, Some(&first), Some(&next)).unwrap();
        assert_eq!(tail.concat(), b"bcde");
        assert!(resident_tail(writer.receipt(), 0, Some(&first), None).is_none());
        // The previous segment has completed its receipt, while the same row
        // remains open and the next segment is already in the actor handoff.
        writer.write_slice(b"b").await.unwrap();
        let tail = resident_tail(writer.receipt(), 0, Some(&next), None).unwrap();
        assert_eq!(tail.concat(), b"cde");
        // A coalescer-owned suffix must not be copied a second time.
        assert_eq!(
            resident_tail(writer.receipt(), 2, Some(&next), None)
                .unwrap()
                .concat(),
            b"e"
        );
    }

    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    #[tokio::test]
    async fn prescribed_missing_tail_rejects_other_none_geometries() {
        let profile = ClientRowProfile::try_new(16, 64).unwrap();
        let first = ClientRowStreamCursor::new()
            .validate_body(profile, &[5, 0, 0, 0, b'a', b'b'])
            .unwrap();
        let complete = first.after().validate_body(profile, b"cde").unwrap();
        let unrelated = ClientRowStreamCursor::new()
            .validate_body(profile, &[5, 0, 0, 0, b'a'])
            .unwrap();
        let mut writer =
            OwnedStreamingMysqlWriter::new(Vec::new(), ProtocolLimits::default(), 1).unwrap();
        let boundary = writer.receipt();
        writer.start_row(5).unwrap();
        writer.write_slice(b"a").await.unwrap();
        let cursor = writer.receipt();
        assert!(resident_tail(cursor, 0, Some(&first), None).is_none());
        assert!(has_missing_resident_tail(cursor, 0, Some(&first), None));
        assert!(!has_missing_resident_tail(boundary, 0, Some(&first), None));
        assert!(!has_missing_resident_tail(cursor, 5, Some(&first), None));
        assert!(!has_missing_resident_tail(cursor, 0, None, None));
        assert!(!has_missing_resident_tail(
            cursor,
            0,
            Some(&first),
            Some(&unrelated)
        ));
        assert!(!has_missing_resident_tail(
            cursor,
            0,
            Some(&first),
            Some(&complete)
        ));
        let mut wrong_rows = cursor;
        wrong_rows.rows_completed = 1;
        assert!(!has_missing_resident_tail(
            wrong_rows,
            0,
            Some(&first),
            None
        ));
        let mut poisoned = cursor;
        poisoned.phase = WritePhase::Poisoned;
        assert!(!has_missing_resident_tail(poisoned, 0, Some(&first), None));
    }

    #[tokio::test]
    async fn validated_spans_cross_segments_without_rebuilding_a_row() {
        let profile = ClientRowProfile::try_new(16, 64).unwrap();
        let first = [4, 0, 0, 0, b'a'];
        let second = [b'b', b'c'];
        let third = [b'd', 1, 0, 0, 0, b'e'];
        let mut cursor = ClientRowStreamCursor::new();
        let mut writer =
            OwnedStreamingMysqlWriter::new(Vec::new(), ProtocolLimits::default(), 255).unwrap();
        for bytes in [&first[..], &second[..], &third[..]] {
            let body = cursor.validate_body(profile, bytes).unwrap();
            write_body(&mut writer, &body).await.unwrap();
            cursor = body.after();
            assert_eq!(writer.buffer_capacity(), 64 * 1024);
        }
        assert_eq!(writer.receipt().phase, WritePhase::Boundary);
        assert_eq!(writer.receipt().rows_completed, 2);
        assert_eq!(
            packets(&writer.into_inner()),
            [(255, &b"abcd"[..]), (0, &b"e"[..])]
        );
    }

    #[tokio::test]
    async fn small_rows_wrap_sequences_and_flush_before_another_body() {
        let mut bytes = Vec::new();
        for _ in 0..300 {
            bytes.extend_from_slice(&[1, 0, 0, 0, b'x']);
        }
        let profile = ClientRowProfile::try_new(4096, 64).unwrap();
        let body = ClientRowStreamCursor::new()
            .validate_body(profile, &bytes)
            .unwrap();
        let mut writer =
            OwnedStreamingMysqlWriter::new(Vec::new(), ProtocolLimits::default(), 1).unwrap();
        write_body(&mut writer, &body).await.unwrap();
        assert_eq!(writer.receipt().rows_completed, 300);
        let wire = writer.into_inner();
        let packets = packets(&wire);
        assert_eq!(packets.len(), 300);
        for (index, (sequence, bytes)) in packets.into_iter().enumerate() {
            assert_eq!(sequence, (index as u8).wrapping_add(1));
            assert_eq!(bytes, b"x");
        }
        let (socket, mut client) = tokio::io::duplex(16);
        let mut writer =
            OwnedStreamingMysqlWriter::new(socket, ProtocolLimits::default(), 1).unwrap();
        let one = ClientRowStreamCursor::new()
            .validate_body(profile, &[1, 0, 0, 0, b'x'])
            .unwrap();
        write_body(&mut writer, &one).await.unwrap();
        let mut wire = [0; 5];
        tokio::time::timeout(Duration::from_millis(100), client.read_exact(&mut wire))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(wire, [1, 0, 0, 1, b'x']);
    }

    #[tokio::test]
    async fn a_u24_multiple_split_across_bodies_has_a_zero_length_terminal_packet() {
        let profile = ClientRowProfile::try_new(1024 * 1024, (U24_MAX + 1) as u32).unwrap();
        let mut writer =
            OwnedStreamingMysqlWriter::new(Vec::new(), ProtocolLimits::default(), 255).unwrap();
        let mut cursor = ClientRowStreamCursor::new();
        let mut remaining = U24_MAX;
        while remaining != 0 {
            let first = cursor.remaining() == 0;
            let count = remaining.min(profile.segment_bytes() - if first { 4 } else { 0 });
            let mut bytes = Vec::with_capacity(count + if first { 4 } else { 0 });
            if first {
                bytes.extend_from_slice(&(U24_MAX as u32).to_le_bytes());
            }
            bytes.resize(count + if first { 4 } else { 0 }, b'x');
            let body = cursor.validate_body(profile, &bytes).unwrap();
            write_body(&mut writer, &body).await.unwrap();
            cursor = body.after();
            remaining -= count;
            assert_eq!(writer.buffer_capacity(), 64 * 1024);
        }
        cursor.validate_end().unwrap();
        assert_eq!(writer.receipt().rows_completed, 1);
        let wire = writer.into_inner();
        let packets = packets(&wire);
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].0, 255);
        assert_eq!(packets[0].1.len(), U24_MAX);
        assert_eq!(packets[1], (0, &[][..]));
    }

    #[test]
    fn success_terminators_preserve_negotiated_flags() {
        assert_eq!(
            success_payload(CapabilityFlags::empty(), false),
            [0xfe, 0, 0, 0, 0]
        );
        assert_eq!(
            success_payload(CapabilityFlags::empty(), true),
            [0xfe, 0, 0, 8, 0]
        );
        let flags = CapabilityFlags::CLIENT_DEPRECATE_EOF | CapabilityFlags::CLIENT_PROTOCOL_41;
        assert_eq!(success_payload(flags, true), [0xfe, 0, 0, 8, 0, 0, 0]);
        assert_eq!(
            success_payload(flags | CapabilityFlags::CLIENT_SESSION_TRACK, false),
            [0xfe, 0, 0, 0, 0, 0, 0, 0]
        );
    }
}
