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

//! Render a closed Local graph through the same finite MySQL framing writer.

use crate::governed_result_writer::{
    MysqlStatementWriteOutcome, cancelled_query_result_delivery as cancelled,
    governed_cancelled_query_result_delivery, is_terminal_cancellation,
};
use crate::relay_result_writer::{
    ACTIVE_WRITE_DEADLINE, CLOSING_DEADLINE, CLOSING_OBJECT_BYTES, error_payload, invalid,
    io_error, resident_tail, success_payload, timeout_error, write_body,
};
use novarocks_query_application::{
    api::{
        LocalColumnKind, LocalRenderCursor, OwnedLocalResult, QueryExecutionError,
        QueryExecutionErrorKind,
    },
    cancellation::QueryCancellationReason,
    protocol_delivery::{GovernedImmediateStatementResult, GovernedProtocolOwner},
    session_control::GovernedStatementVisibilitySealOutcome,
};
use novarocks_result_contract::{ClientRowProfile, ClientRowStreamCursor, RootProfileV1};
use novarocks_result_render::RenderTurnStatus;
use novarocks_workload_control::ResultWindowAlias;
use opensrv_mysql::{
    Column, ColumnFlags, ColumnType, ErrorKind, QueryResultWriter, StreamingResponseLease,
};
use std::{io, sync::Arc};
use tokio::{io::AsyncWrite, time::Instant};

/// Destruction order is the backing/physical-exit proof, including unwind and
/// cancellation of the enclosing async writer. No graph or segment can escape.
struct LocalProtocolBuffers {
    cursor: LocalRenderCursor,
    bytes: Vec<u8>,
    metadata: Option<opensrv_mysql::FrozenMetadata>,
    _window: ResultWindowAlias,
}

pub(crate) async fn write_local_result_one<'writer, W: AsyncWrite + Unpin>(
    result: GovernedImmediateStatementResult,
    results: QueryResultWriter<'writer, W>,
    more_results: bool,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    let (graph, mut protocol) = result.into_parts();
    let limits = results.protocol_limits();
    let capabilities = results.client_capabilities();
    if results.is_binary() || limits.row_bytes < RootProfileV1::ROW_PAYLOAD_BYTES as usize {
        drop(graph);
        return close_initial(
            protocol,
            results,
            invalid("Local delivery requires the supported text-row profile"),
        )
        .await;
    }
    let deadline = Instant::now() + ACTIVE_WRITE_DEADLINE;
    // The complete producer window was admitted before the source callback.
    // Metadata, render scratch and socket work retain that same window.
    let cancellation = protocol.cancellation();
    if let Some(reason) = cancellation.reason() {
        drop(graph);
        return close_initial(protocol, results, cancelled(reason)).await;
    }
    let metadata = local_metadata(&graph, capabilities, limits);
    let metadata = match metadata {
        Ok(metadata) => metadata,
        Err(error) => {
            drop(graph);
            return close_initial(protocol, results, invalid(error.to_string())).await;
        }
    };
    let cursor = graph.into_cursor();
    let mut buffers = LocalProtocolBuffers {
        _window: cursor.retain_physical_guard(),
        cursor,
        bytes: vec![0; RootProfileV1::SEGMENT_BYTES],
        metadata: Some(metadata),
    };
    // From this point all socket/coalescer/metadata work is finite and the
    // actual writer task retains the same producer window throughout.
    let mut lease = match tokio::time::timeout_at(deadline, results.into_streaming_result()).await {
        Ok(Ok(lease)) => lease,
        outcome => {
            drop(buffers);
            let _ = protocol.client_disconnected();
            return Err(match outcome {
                Ok(Err(error)) => error,
                _ => timeout_error(),
            });
        }
    };
    lease
        .writer()
        .start_metadata(buffers.metadata.take().expect("frozen Local metadata"))?;
    let cancellation = protocol.cancellation();
    let started = tokio::select! {
        biased;
        reason = cancellation.cancelled() => Err(LocalWriteInterruption::Query(cancelled(reason))),
        outcome = tokio::time::timeout_at(deadline, lease.writer().finish_metadata()) => finite_write(outcome),
    };
    if let Err(error) = started {
        return interrupt(protocol, lease, buffers, error, None).await;
    }
    let profile = local_profile();
    let mut frontier = ClientRowStreamCursor::new();
    loop {
        if let Some(reason) = protocol.cancellation().reason() {
            return close_local(protocol, lease, buffers, cancelled(reason), None).await;
        }
        if Instant::now() >= deadline {
            return close_local(
                protocol,
                lease,
                buffers,
                QueryExecutionError::new(
                    QueryExecutionErrorKind::DeadlineExceeded,
                    "MySQL response write deadline expired",
                ),
                None,
            )
            .await;
        }
        let turn = match buffers.cursor.step(&mut buffers.bytes) {
            Ok(turn) => turn,
            Err(error) => {
                return close_local(protocol, lease, buffers, invalid(error.to_string()), None)
                    .await;
            }
        };
        // Cancellation observed after a counting/render turn prevents its
        // publication. An unfinished previously written row has no resident
        // tail in this case, so it must disconnect without further encoding.
        if let Some(reason) = protocol.cancellation().reason() {
            return close_local(protocol, lease, buffers, cancelled(reason), None).await;
        }
        if turn.emitted_bytes != 0 {
            let body = match frontier.validate_body(profile, &buffers.bytes[..turn.emitted_bytes]) {
                Ok(body) => body,
                Err(error) => {
                    return close_local(protocol, lease, buffers, invalid(error.to_string()), None)
                        .await;
                }
            };
            if lease.receipt().logical_remaining() != body.before().remaining() as usize {
                drop(lease);
                drop(buffers);
                let _ = protocol.fail();
                return Err(io_error(invalid(
                    "Local framing differs from its validated cursor",
                )));
            }
            let next = body.after();
            let cancellation = protocol.cancellation();
            let written = tokio::select! {
                biased;
                reason = cancellation.cancelled() => Err(LocalWriteInterruption::Query(cancelled(reason))),
                outcome = tokio::time::timeout_at(deadline, write_body(lease.writer(), &body)) => finite_write(outcome),
            };
            if let Err(error) = written {
                return interrupt(
                    protocol,
                    lease,
                    buffers,
                    error,
                    Some((frontier, turn.emitted_bytes)),
                )
                .await;
            }
            frontier = next;
        }
        if turn.status == RenderTurnStatus::InputComplete {
            if let Err(error) = frontier.validate_end() {
                drop(lease);
                drop(buffers);
                let _ = protocol.fail();
                return Err(io_error(invalid(error.to_string())));
            }
            match protocol.seal_success_visibility() {
                GovernedStatementVisibilitySealOutcome::Sealed => (),
                GovernedStatementVisibilitySealOutcome::Cancelled(reason) => {
                    return close_local(
                        protocol,
                        lease,
                        buffers,
                        governed_cancelled_query_result_delivery(reason),
                        None,
                    )
                    .await;
                }
                _ => {
                    return close_local(
                        protocol,
                        lease,
                        buffers,
                        invalid("Local result lost its success visibility generation"),
                        None,
                    )
                    .await;
                }
            }
            let terminal = success_payload(capabilities, more_results);
            let written = tokio::time::timeout_at(deadline, lease.finish(&terminal)).await;
            drop(terminal);
            // The segment, encoder/schema and legacy protection precede the
            // original statement grant even on a failed terminal write.
            drop(buffers);
            return match written {
                Ok(Ok(writer)) => {
                    let _ = protocol.complete();
                    Ok(MysqlStatementWriteOutcome::Continue(writer))
                }
                outcome => {
                    let _ = protocol.client_disconnected();
                    Err(match outcome {
                        Ok(Err(error)) => error,
                        _ => timeout_error(),
                    })
                }
            };
        }
        // Empty batches, length counting and every emitted turn yield before
        // continuing. No unbounded scan or row build runs in one actor poll.
        tokio::task::yield_now().await;
    }
}

fn local_profile() -> ClientRowProfile {
    ClientRowProfile::try_new(
        RootProfileV1::SEGMENT_BYTES,
        RootProfileV1::ROW_PAYLOAD_BYTES,
    )
    .expect("frozen Local row profile")
}
fn local_metadata(
    graph: &OwnedLocalResult,
    capabilities: opensrv_mysql::CapabilityFlags,
    limits: opensrv_mysql::ProtocolLimits,
) -> io::Result<opensrv_mysql::FrozenMetadata> {
    // Borrowed preflight precedes every adapter String/Column copy.
    crate::relay_metadata::preflight_column_names(
        graph.columns().map(|column| column.name),
        capabilities,
        limits,
    )?;
    let columns = graph
        .columns()
        .map(|column| Column {
            table: String::new(),
            column: column.name.to_owned(),
            coltype: match column.kind {
                LocalColumnKind::Utf8 => ColumnType::MYSQL_TYPE_VAR_STRING,
                LocalColumnKind::Boolean => ColumnType::MYSQL_TYPE_TINY,
                LocalColumnKind::Int32 => ColumnType::MYSQL_TYPE_LONG,
                LocalColumnKind::Int64 => ColumnType::MYSQL_TYPE_LONGLONG,
            },
            colflags: if column.nullable {
                ColumnFlags::empty()
            } else {
                ColumnFlags::NOT_NULL_FLAG
            },
        })
        .collect::<Vec<_>>();
    crate::relay_metadata::frozen_result_metadata(&columns, capabilities, limits)
}
enum LocalWriteInterruption {
    Query(QueryExecutionError),
    Io(io::Error),
}
fn finite_write(
    outcome: Result<io::Result<()>, tokio::time::error::Elapsed>,
) -> Result<(), LocalWriteInterruption> {
    outcome
        .map_err(|_| timeout_error())
        .and_then(|outcome| outcome)
        .map_err(LocalWriteInterruption::Io)
}
async fn interrupt<'writer, W: AsyncWrite + Unpin>(
    mut protocol: GovernedProtocolOwner,
    lease: StreamingResponseLease<'writer, W>,
    buffers: LocalProtocolBuffers,
    error: LocalWriteInterruption,
    current: Option<(ClientRowStreamCursor, usize)>,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    match error {
        LocalWriteInterruption::Query(error) => {
            close_local(protocol, lease, buffers, error, current).await
        }
        LocalWriteInterruption::Io(error) => {
            drop(lease);
            drop(buffers);
            let _ = protocol.client_disconnected();
            Err(error)
        }
    }
}
async fn close_initial<'writer, W: AsyncWrite + Unpin>(
    mut protocol: GovernedProtocolOwner,
    results: QueryResultWriter<'writer, W>,
    error: QueryExecutionError,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    if matches!(
        protocol.cancellation().reason(),
        Some(
            QueryCancellationReason::ExplicitKillConnection { .. }
                | QueryCancellationReason::ServerShutdown
                | QueryCancellationReason::ClientDisconnected
        )
    ) {
        let _ = protocol.client_disconnected();
        return Err(io_error(error));
    }
    let capacity = match protocol.try_closing_capacity(
        is_terminal_cancellation(&error) && protocol.cancellation().is_cancelled(),
    ) {
        Ok(capacity) if capacity.check_backing_total(CLOSING_OBJECT_BYTES).is_ok() => capacity,
        _ => {
            let _ = protocol.client_disconnected();
            return Err(io_error(error));
        }
    };
    let deadline = Instant::now() + CLOSING_DEADLINE;
    // A preceding negotiated OK may still need flushing. Move that exact
    // writer/future into Closing before polling it, so a slow preceding
    // terminator cannot retain this statement's ordinary Local position.
    let writer = InitialLocalClosingWriter {
        writer: Some(results),
        error,
    };
    let mut closing = match protocol.into_closing_delivery(writer, capacity, CLOSING_OBJECT_BYTES) {
        Ok(closing) => closing,
        Err((mut protocol, writer, _capacity)) => {
            drop(writer);
            let _ = protocol.client_disconnected();
            return Err(io_error(invalid(
                "initial Local closing lost its exact owner",
            )));
        }
    };
    match tokio::time::timeout_at(deadline, closing.writer_mut().finish()).await {
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
struct InitialLocalClosingWriter<'writer, W: AsyncWrite + Unpin> {
    writer: Option<QueryResultWriter<'writer, W>>,
    error: QueryExecutionError,
}
impl<'writer, W: AsyncWrite + Unpin> InitialLocalClosingWriter<'writer, W> {
    async fn finish(&mut self) -> io::Result<QueryResultWriter<'writer, W>> {
        let mut lease = self
            .writer
            .take()
            .expect("initial closing owns its writer")
            .into_streaming_result()
            .await?;
        let kind = if is_terminal_cancellation(&self.error) {
            ErrorKind::ER_QUERY_INTERRUPTED
        } else {
            ErrorKind::ER_UNKNOWN_ERROR
        };
        let payload = error_payload(
            &self.error,
            lease.writer().protocol_limits().diagnostic_bytes,
        );
        let mut writer = match lease.into_closing_lease(Vec::new(), kind, &payload[9..]) {
            Ok(writer) => writer,
            Err((lease, rejection)) => {
                drop(lease);
                return Err(rejection);
            }
        };
        drop(payload);
        writer.finish().await
    }
}

async fn close_local<'writer, W: AsyncWrite + Unpin>(
    mut protocol: GovernedProtocolOwner,
    mut lease: StreamingResponseLease<'writer, W>,
    mut buffers: LocalProtocolBuffers,
    error: QueryExecutionError,
    current: Option<(ClientRowStreamCursor, usize)>,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    buffers.cursor.cancel();
    if matches!(
        protocol.cancellation().reason(),
        Some(
            QueryCancellationReason::ExplicitKillConnection { .. }
                | QueryCancellationReason::ServerShutdown
                | QueryCancellationReason::ClientDisconnected
        )
    ) {
        drop(lease);
        drop(buffers);
        let _ = protocol.client_disconnected();
        return Err(io_error(error));
    }
    let capacity = match protocol.try_closing_capacity(
        is_terminal_cancellation(&error) && protocol.cancellation().is_cancelled(),
    ) {
        Ok(capacity) if capacity.check_backing_total(CLOSING_OBJECT_BYTES).is_ok() => capacity,
        _ => {
            drop(lease);
            drop(buffers);
            let _ = protocol.client_disconnected();
            return Err(io_error(error));
        }
    };
    let body = current.and_then(|(frontier, length)| {
        frontier
            .validate_body(local_profile(), &buffers.bytes[..length])
            .ok()
    });
    let resident = resident_tail(
        lease.receipt(),
        lease.writer().buffered_row_bytes(),
        body.as_ref(),
        None,
    );
    let Some(resident) = resident else {
        drop(lease);
        drop(buffers);
        let _ = protocol.client_disconnected();
        return Err(io_error(error));
    };
    // Only the current row survives. New independent capacity covers the
    // compact copy and the still-live ordinary objects during this transfer.
    let size = resident.iter().map(|part| part.len()).sum();
    let mut bytes = Vec::with_capacity(size);
    for part in resident {
        bytes.extend_from_slice(part);
    }
    let mut tail = Vec::with_capacity(1);
    if !bytes.is_empty() {
        tail.push(opensrv_mysql::ResidentTailPart::new(
            Arc::from(bytes),
            0..size,
        )?);
    }
    let payload = error_payload(&error, lease.writer().protocol_limits().diagnostic_bytes);
    let kind = if is_terminal_cancellation(&error) {
        ErrorKind::ER_QUERY_INTERRUPTED
    } else {
        ErrorKind::ER_UNKNOWN_ERROR
    };
    let writer = match lease.into_closing_lease(tail, kind, &payload[9..]) {
        Ok(writer) => writer,
        Err((lease, error)) => {
            drop(payload);
            drop(lease);
            drop(buffers);
            let _ = protocol.client_disconnected();
            return Err(error);
        }
    };
    drop(payload);
    drop(error);
    // There can be no render/fetch in the closing waiter. Drop source/schema,
    // segment and ordinary alias before its first poll. The independently
    // admitted Closing window owns every remaining writer-tail object.
    drop(buffers);
    finish_closing(
        protocol,
        writer,
        capacity,
        Instant::now() + CLOSING_DEADLINE,
    )
    .await
}
async fn finish_closing<'writer, W: AsyncWrite + Unpin>(
    protocol: GovernedProtocolOwner,
    writer: opensrv_mysql::ClosingResponseLease<'writer, W>,
    capacity: novarocks_workload_control::ResultWindowGrant,
    deadline: Instant,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    let mut closing = match protocol.into_closing_delivery(writer, capacity, CLOSING_OBJECT_BYTES) {
        Ok(closing) => closing,
        Err((mut protocol, writer, _capacity)) => {
            drop(writer);
            let _ = protocol.client_disconnected();
            return Err(io_error(invalid(
                "Local closing handoff lost its exact owner",
            )));
        }
    };
    match tokio::time::timeout_at(deadline, closing.writer_mut().finish()).await {
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

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::{
        api::build_string_query_result,
        client_connection::ClientConnectionToken,
        query_control::QueryApplicationControl,
        session_control::{QueryControlService, QuerySessionLease, SessionIdentity},
    };
    use novarocks_workload_control::{
        ResultCapacityConfig, ResultWindowClass, WorkClass, WorkloadConfig, WorkloadControl,
    };
    use opensrv_mysql::{
        AsyncMysqlIntermediary, AsyncMysqlShim, CapabilityFlags, ParamParser, StatementMetaWriter,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    struct Fixture {
        host: WorkloadControl,
        capacity: novarocks_workload_control::ResultCapacityHandle,
        control: QueryControlService,
        session: QuerySessionLease,
    }
    impl Fixture {
        fn new() -> Self {
            let host = WorkloadControl::try_new_counted(WorkloadConfig::default())
                .unwrap()
                .owner;
            let capacity = host
                .configure_result_capacity(ResultCapacityConfig::V1)
                .unwrap();
            host.mark_ready().unwrap();
            let control = QueryControlService::new(Arc::new(QueryApplicationControl::default()));
            let session = control
                .register_session(SessionIdentity::new(
                    ClientConnectionToken::new(901, 1).unwrap(),
                    "root",
                ))
                .unwrap();
            Self {
                host,
                capacity,
                control,
                session,
            }
        }
        fn protocol(&self) -> GovernedProtocolOwner {
            let statement = self
                .control
                .begin_governed_statement_with_result(
                    self.session.token(),
                    &self.host.root_admission(),
                    WorkClass::Management,
                    None,
                    None,
                    None,
                    ResultWindowClass::Local,
                )
                .unwrap();
            GovernedProtocolOwner::new(statement)
        }
        fn result(&self, value: String) -> GovernedImmediateStatementResult {
            let statement = self
                .control
                .begin_governed_statement_with_result(
                    self.session.token(),
                    &self.host.root_admission(),
                    WorkClass::Management,
                    None,
                    None,
                    None,
                    ResultWindowClass::Local,
                )
                .unwrap();
            // The source is called only after its complete producer admission.
            let graph = build_string_query_result("value", vec![value]).unwrap();
            GovernedImmediateStatementResult::try_new(graph, statement)
                .unwrap_or_else(|(error, _)| panic!("{error}"))
        }
    }
    struct Shim {
        fixture: Fixture,
        gate: Arc<WriteGate>,
    }
    struct WriteGate {
        remaining: std::sync::atomic::AtomicUsize,
        pause_flush: std::sync::atomic::AtomicBool,
        waiter: std::sync::Mutex<Option<std::task::Waker>>,
    }
    impl WriteGate {
        fn new() -> Self {
            Self {
                remaining: std::sync::atomic::AtomicUsize::new(usize::MAX),
                pause_flush: std::sync::atomic::AtomicBool::new(false),
                waiter: std::sync::Mutex::new(None),
            }
        }
        fn release(&self) {
            self.pause_flush
                .store(false, std::sync::atomic::Ordering::SeqCst);
            self.remaining
                .store(usize::MAX, std::sync::atomic::Ordering::SeqCst);
            if let Some(waker) = self.waiter.lock().unwrap().take() {
                waker.wake();
            }
        }
    }
    struct GatedWrite<W> {
        inner: W,
        gate: Arc<WriteGate>,
    }
    impl<W: AsyncWrite + Unpin> AsyncWrite for GatedWrite<W> {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<io::Result<usize>> {
            let remaining = self
                .gate
                .remaining
                .load(std::sync::atomic::Ordering::SeqCst);
            if remaining == 0 {
                *self.gate.waiter.lock().unwrap() = Some(cx.waker().clone());
                return std::task::Poll::Pending;
            }
            let count = bytes.len().min(remaining);
            let result = std::pin::Pin::new(&mut self.inner).poll_write(cx, &bytes[..count]);
            if let std::task::Poll::Ready(Ok(written)) = &result {
                if remaining != usize::MAX {
                    self.gate
                        .remaining
                        .fetch_sub(*written, std::sync::atomic::Ordering::SeqCst);
                }
            }
            result
        }
        fn poll_flush(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            if self
                .gate
                .pause_flush
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                *self.gate.waiter.lock().unwrap() = Some(cx.waker().clone());
                return std::task::Poll::Pending;
            }
            std::pin::Pin::new(&mut self.inner).poll_flush(cx)
        }
        fn poll_shutdown(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<io::Result<()>> {
            std::pin::Pin::new(&mut self.inner).poll_shutdown(cx)
        }
    }
    #[async_trait::async_trait]
    impl<W: AsyncWrite + Unpin + Send> AsyncMysqlShim<W> for Shim {
        type Error = io::Error;
        async fn on_prepare<'a>(
            &'a mut self,
            _: &'a str,
            _: StatementMetaWriter<'a, W>,
        ) -> io::Result<()> {
            Err(io::Error::other("test does not prepare"))
        }
        async fn on_execute<'a>(
            &'a mut self,
            _: u32,
            _: ParamParser<'a>,
            _: QueryResultWriter<'a, W>,
        ) -> io::Result<()> {
            Err(io::Error::other(
                "test does not execute prepared statements",
            ))
        }
        async fn on_close<'a>(&'a mut self, _: u32)
        where
            W: 'async_trait,
        {
        }
        async fn on_init<'a>(
            &'a mut self,
            schema: &'a str,
            writer: opensrv_mysql::InitWriter<'a, W>,
        ) -> io::Result<()> {
            if schema == "slow-init" {
                self.gate
                    .remaining
                    .store(2, std::sync::atomic::Ordering::SeqCst);
            }
            if schema == "missing" {
                crate::terminal::write_governed_init_error(
                    novarocks_query_application::session_error::QueryServiceError::new(
                        novarocks_query_application::session_error::QueryServiceErrorKind::BadDatabase,
                        "unknown database"), self.fixture.protocol(), writer).await
            } else {
                crate::terminal::write_governed_init_ok(self.fixture.protocol(), writer).await
            }
        }
        async fn on_query<'a>(
            &'a mut self,
            query: &'a str,
            results: QueryResultWriter<'a, W>,
        ) -> io::Result<()> {
            if matches!(
                query,
                "terminal"
                    | "terminal-more"
                    | "slow-terminal"
                    | "flush-terminal"
                    | "cancel-terminal"
            ) {
                if query == "slow-terminal" {
                    self.gate
                        .remaining
                        .store(2, std::sync::atomic::Ordering::SeqCst);
                }
                if query == "flush-terminal" {
                    self.gate
                        .pause_flush
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
                let protocol = self.fixture.protocol();
                if query == "cancel-terminal" {
                    self.fixture.control.cancel_session_statement(
                        self.fixture.session.token(),
                        QueryCancellationReason::ExplicitKill {
                            requester_connection_id: 901,
                        },
                    );
                }
                let more = query == "terminal-more";
                return match crate::terminal::write_governed_terminal_ok_with_more(
                    protocol, results, more,
                )
                .await?
                {
                    MysqlStatementWriteOutcome::Continue(writer) if more => {
                        match write_local_result_one(
                            self.fixture.result("next".into()),
                            writer,
                            false,
                        )
                        .await?
                        {
                            MysqlStatementWriteOutcome::Continue(writer) => {
                                writer.no_more_results().await
                            }
                            MysqlStatementWriteOutcome::Terminated => Ok(()),
                        }
                    }
                    MysqlStatementWriteOutcome::Continue(writer) => writer.no_more_results().await,
                    MysqlStatementWriteOutcome::Terminated => Ok(()),
                };
            }
            if matches!(query, "typed-error" | "slow-typed") {
                if query == "slow-typed" {
                    self.gate
                        .remaining
                        .store(2, std::sync::atomic::Ordering::SeqCst);
                }
                let error = novarocks_query_application::session_error::QueryServiceError::new(
                    novarocks_query_application::session_error::QueryServiceErrorKind::BadDatabase,
                    "unknown database",
                );
                return crate::terminal::write_governed_terminal_error(
                    error,
                    self.fixture.protocol(),
                    results,
                )
                .await;
            }
            let value = if matches!(query, "wide" | "partialwide") {
                "x".repeat(128 * 1024)
            } else {
                query.to_owned()
            };
            // For this one-column fixture the count/definition/EOF occupy
            // 5 + 31 + 9 bytes. Stop after two bytes of the row packet header.
            if matches!(query, "partial" | "partialwide") {
                self.gate
                    .remaining
                    .store(47, std::sync::atomic::Ordering::SeqCst);
            }
            let more = query == "multi";
            let results = if query == "cancel-after-ok" {
                self.gate
                    .remaining
                    .store(2, std::sync::atomic::Ordering::SeqCst);
                results
                    .complete_one(opensrv_mysql::OkResponse::default())
                    .await?
            } else {
                results
            };
            let result = self.fixture.result(value);
            if matches!(query, "cancel" | "cancel-after-ok") {
                self.fixture.control.cancel_session_statement(
                    self.fixture.session.token(),
                    QueryCancellationReason::ExplicitKill {
                        requester_connection_id: 901,
                    },
                );
            }
            match write_local_result_one(result, results, more).await? {
                MysqlStatementWriteOutcome::Continue(writer) => {
                    if more {
                        match write_local_result_one(
                            self.fixture.result("next".into()),
                            writer,
                            false,
                        )
                        .await?
                        {
                            MysqlStatementWriteOutcome::Continue(writer) => {
                                writer.no_more_results().await
                            }
                            MysqlStatementWriteOutcome::Terminated => Ok(()),
                        }
                    } else {
                        writer.no_more_results().await
                    }
                }
                MysqlStatementWriteOutcome::Terminated => Ok(()),
            }
        }
    }
    async fn read_packet(client: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut header = [0; 4];
        client.read_exact(&mut header).await.unwrap();
        let len =
            usize::from(header[0]) | usize::from(header[1]) << 8 | usize::from(header[2]) << 16;
        let mut bytes = vec![0; len];
        client.read_exact(&mut bytes).await.unwrap();
        (header[3], bytes)
    }
    async fn packet(client: &mut TcpStream, sequence: u8, bytes: &[u8]) {
        let size = bytes.len() as u32;
        let mut header = size.to_le_bytes();
        header[3] = sequence;
        client.write_all(&header).await.unwrap();
        client.write_all(bytes).await.unwrap();
    }
    async fn socket() -> (TcpStream, tokio::task::JoinHandle<io::Result<()>>) {
        socket_with(Fixture::new(), Arc::new(WriteGate::new())).await
    }
    async fn socket_with(
        fixture: Fixture,
        gate: Arc<WriteGate>,
    ) -> (TcpStream, tokio::task::JoinHandle<io::Result<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (read, write) = stream.into_split();
            AsyncMysqlIntermediary::run_with_options(
                Shim {
                    fixture,
                    gate: Arc::clone(&gate),
                },
                read,
                GatedWrite { inner: write, gate },
                &crate::MYSQL_INTERMEDIARY_OPTIONS,
            )
            .await
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        let (sequence, greeting) = read_packet(&mut client).await;
        assert_eq!(sequence, 0);
        assert_eq!(greeting[0], 10);
        let capabilities = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_PLUGIN_AUTH;
        let mut response = Vec::new();
        response.extend_from_slice(&capabilities.bits().to_le_bytes());
        response.extend_from_slice(&(64_u32 * 1024 * 1024).to_le_bytes());
        response.push(33);
        response.extend_from_slice(&[0; 23]);
        response.extend_from_slice(b"root\0");
        response.push(0);
        response.extend_from_slice(b"mysql_native_password\0");
        packet(&mut client, 1, &response).await;
        assert_eq!(read_packet(&mut client).await.1[0], 0);
        (client, server)
    }
    async fn read_result(client: &mut TcpStream, first_sequence: u8) -> (Vec<u8>, Vec<u8>, u8) {
        let count = read_packet(client).await;
        assert_eq!(count, (first_sequence, vec![1]));
        let definition = read_packet(client).await;
        assert_eq!(definition.0, first_sequence.wrapping_add(1));
        let metadata_end = read_packet(client).await;
        assert_eq!(metadata_end.1, [0xfe, 0, 0, 0, 0]);
        let row = read_packet(client).await;
        let end = read_packet(client).await;
        assert_eq!(end.0, first_sequence.wrapping_add(4));
        assert_eq!(end.1[0], 0xfe);
        (row.1, end.1, end.0.wrapping_add(1))
    }
    #[tokio::test]
    async fn local_socket_renders_short_and_cross_turn_rows_and_reenters() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            for (query, expected) in [
                ("hello", b"\x05hello".to_vec()),
                (
                    "wide",
                    [vec![0xfd, 0, 0, 2], vec![b'x'; 128 * 1024]].concat(),
                ),
                ("again", b"\x05again".to_vec()),
            ] {
                packet(&mut client, 0, &[&[3], query.as_bytes()].concat()).await;
                let (row, end, _) = read_result(&mut client, 1).await;
                assert_eq!(row, expected);
                assert_eq!(end, [0xfe, 0, 0, 0, 0]);
            }
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn local_socket_more_results_flag_matches_following_result() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            packet(&mut client, 0, b"\x03multi").await;
            let (row, end, next_sequence) = read_result(&mut client, 1).await;
            assert_eq!(row, b"\x05multi");
            assert_eq!(end, [0xfe, 0, 0, 8, 0]);
            let (row, end, _) = read_result(&mut client, next_sequence).await;
            assert_eq!(row, b"\x04next");
            assert_eq!(end, [0xfe, 0, 0, 0, 0]);
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn initial_local_cancel_writes_error_and_reuses_the_connection() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            packet(&mut client, 0, b"\x03cancel").await;
            let (sequence, error) = read_packet(&mut client).await;
            assert_eq!(sequence, 1);
            assert_eq!(&error[..3], &[0xff, 0x25, 0x05]); // ER_QUERY_INTERRUPTED
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn resident_local_partial_header_moves_only_writer_tail_to_closing() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let control = fixture.control.clone();
            let session = fixture.session.token();
            let gate = Arc::new(WriteGate::new());
            let (mut client, server) = socket_with(fixture, Arc::clone(&gate)).await;
            packet(&mut client, 0, b"\x03partial").await;
            for _ in 0..3 {
                read_packet(&mut client).await;
            }
            let mut header = [0; 4];
            client.read_exact(&mut header[..2]).await.unwrap();
            control.cancel_session_statement(
                session,
                QueryCancellationReason::ExplicitKill {
                    requester_connection_id: 901,
                },
            );
            while capacity.snapshot().held_positions != [0, 0, 0, 1] {
                tokio::task::yield_now().await;
            }
            assert!(
                control.begin_statement(session).is_err(),
                "closing retains the statement generation"
            );
            gate.release();
            client.read_exact(&mut header[2..]).await.unwrap();
            assert_eq!(header, [8, 0, 0, 4]);
            let mut row = [0; 8];
            client.read_exact(&mut row).await.unwrap();
            assert_eq!(&row, b"\x07partial");
            let (sequence, error) = read_packet(&mut client).await;
            assert_eq!(sequence, 5);
            assert_eq!(&error[..3], &[0xff, 0x25, 0x05]);
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn incomplete_local_resident_row_disconnects_without_rendering_the_tail() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let control = fixture.control.clone();
            let session = fixture.session.token();
            let (mut client, server) = socket_with(fixture, Arc::new(WriteGate::new())).await;
            packet(&mut client, 0, b"\x03partialwide").await;
            for _ in 0..3 {
                read_packet(&mut client).await;
            }
            let mut header = [0; 2];
            client.read_exact(&mut header).await.unwrap();
            control.cancel_session_statement(
                session,
                QueryCancellationReason::ExplicitKill {
                    requester_connection_id: 901,
                },
            );
            let mut suffix = Vec::new();
            client.read_to_end(&mut suffix).await.unwrap();
            assert!(
                suffix.is_empty(),
                "missing tail cannot be rendered after the cut"
            );
            assert!(server.await.unwrap().is_err());
            assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn initial_cancel_transfers_before_a_pending_previous_ok_can_block() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let gate = Arc::new(WriteGate::new());
            let (mut client, server) = socket_with(fixture, Arc::clone(&gate)).await;
            packet(&mut client, 0, b"\x03cancel-after-ok").await;
            let mut header = [0; 4];
            client.read_exact(&mut header[..2]).await.unwrap();
            assert_eq!(
                capacity.snapshot().held_positions,
                [0, 0, 0, 1],
                "previous pending OK is already exclusively in Closing"
            );
            gate.release();
            client.read_exact(&mut header[2..]).await.unwrap();
            let len =
                usize::from(header[0]) | usize::from(header[1]) << 8 | usize::from(header[2]) << 16;
            let mut ok = vec![0; len];
            client.read_exact(&mut ok).await.unwrap();
            assert_eq!(header[3], 1);
            assert_eq!(ok[0], 0);
            assert_eq!(ok[3], 8);
            let (sequence, error) = read_packet(&mut client).await;
            assert_eq!(sequence, 2);
            assert_eq!(&error[..3], &[0xff, 0x25, 0x05]);
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn finite_terminal_ok_has_explicit_more_results_and_no_deferred_owner() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            packet(&mut client, 0, b"\x03terminal-more").await;
            assert_eq!(
                read_packet(&mut client).await,
                (1, vec![0, 0, 0, 8, 0, 0, 0])
            );
            assert_eq!(read_result(&mut client, 2).await.0, b"\x04next");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn finite_terminal_and_init_packets_preserve_types_and_next_command() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            for query in [b"\x03terminal".as_slice(), b"\x02db"] {
                packet(&mut client, 0, query).await;
                assert_eq!(
                    read_packet(&mut client).await,
                    (1, vec![0, 0, 0, 0, 0, 0, 0])
                );
            }
            for query in [b"\x03typed-error".as_slice(), b"\x02missing"] {
                packet(&mut client, 0, query).await;
                let (sequence, error) = read_packet(&mut client).await;
                assert_eq!(sequence, 1);
                assert_eq!(&error[..3], &[0xff, 0x19, 0x04]); // ER_BAD_DB_ERROR
                assert_eq!(&error[4..9], b"42000");
                assert_eq!(&error[9..], b"unknown database");
            }
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn slow_terminal_success_retains_ordinary_window_and_generation_until_write_exit() {
        for command in [b"\x03slow-terminal".as_slice(), b"\x02slow-init"] {
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                let fixture = Fixture::new();
                let capacity = fixture.capacity.clone();
                let control = fixture.control.clone();
                let session = fixture.session.token();
                let gate = Arc::new(WriteGate::new());
                let (mut client, server) = socket_with(fixture, Arc::clone(&gate)).await;
                packet(&mut client, 0, command).await;
                let mut header = [0; 4];
                client.read_exact(&mut header[..2]).await.unwrap();
                assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
                assert!(control.begin_statement(session).is_err());
                gate.release();
                client.read_exact(&mut header[2..]).await.unwrap();
                assert_eq!(header, [7, 0, 0, 1]);
                let mut ok = [0; 7];
                client.read_exact(&mut ok).await.unwrap();
                assert_eq!(ok, [0; 7]);
                packet(&mut client, 0, b"\x03again").await;
                assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
                packet(&mut client, 0, &[1]).await;
                server.await.unwrap().unwrap();
            })
            .await
            .unwrap();
        }
    }
    #[tokio::test]
    async fn slow_typed_error_retires_local_window_but_retains_closing_generation() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let control = fixture.control.clone();
            let session = fixture.session.token();
            let gate = Arc::new(WriteGate::new());
            let (mut client, server) = socket_with(fixture, Arc::clone(&gate)).await;
            packet(&mut client, 0, b"\x03slow-typed").await;
            let mut header = [0; 4];
            client.read_exact(&mut header[..2]).await.unwrap();
            assert_eq!(capacity.snapshot().held_positions, [0, 0, 0, 1]);
            assert!(control.begin_statement(session).is_err());
            gate.release();
            client.read_exact(&mut header[2..]).await.unwrap();
            let len = usize::from(header[0]);
            let mut error = vec![0; len];
            client.read_exact(&mut error).await.unwrap();
            assert_eq!(&error[..3], &[0xff, 0x19, 0x04]);
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn terminal_flush_holds_ordinary_generation_after_the_complete_packet() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let fixture = Fixture::new();
            let capacity = fixture.capacity.clone();
            let control = fixture.control.clone();
            let session = fixture.session.token();
            let gate = Arc::new(WriteGate::new());
            let (mut client, server) = socket_with(fixture, Arc::clone(&gate)).await;
            packet(&mut client, 0, b"\x03flush-terminal").await;
            assert_eq!(read_packet(&mut client).await, (1, vec![0; 7]));
            assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
            assert!(
                control.begin_statement(session).is_err(),
                "packet completion alone is not flush exit"
            );
            gate.release();
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn cancelled_terminal_uses_closing_and_keeps_the_connection() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut client, server) = socket().await;
            packet(&mut client, 0, b"\x03cancel-terminal").await;
            let (sequence, error) = read_packet(&mut client).await;
            assert_eq!(sequence, 1);
            assert_eq!(&error[..3], &[0xff, 0x25, 0x05]);
            packet(&mut client, 0, b"\x03again").await;
            assert_eq!(read_result(&mut client, 1).await.0, b"\x05again");
            packet(&mut client, 0, &[1]).await;
            server.await.unwrap().unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn local_buffers_keep_original_window_after_statement_owner_exits() {
        let fixture = Fixture::new();
        let capacity = fixture.capacity.clone();
        let (graph, mut protocol) = fixture.result("hello".into()).into_parts();
        let cursor = graph.into_cursor();
        let buffers = LocalProtocolBuffers {
            _window: cursor.retain_physical_guard(),
            cursor,
            bytes: vec![0; RootProfileV1::SEGMENT_BYTES],
            metadata: None,
        };
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        assert_eq!(
            protocol.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        protocol.complete();
        // Logical completion cannot free the actual cursor/segment owner.
        assert_eq!(capacity.snapshot().held_positions, [0, 1, 0, 0]);
        drop(buffers);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}
