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

//! Terminal MySQL response encoding for governed Query Application statements.

use std::io;

use crate::relay_result_writer::{
    ACTIVE_WRITE_DEADLINE, CLOSING_DEADLINE, CLOSING_OBJECT_BYTES, timeout_error,
};
use novarocks_query_application::cancellation::QueryCancellationReason;
use novarocks_query_application::protocol_delivery::GovernedProtocolOwner;
use novarocks_query_application::session_control::GovernedStatementVisibilitySealOutcome;
use novarocks_query_application::session_error::QueryServiceError;
use opensrv_mysql::{
    CapabilityFlags, ErrorKind, InitWriter, OkResponse, QueryResultWriter, StatusFlags,
};
use tokio::io::AsyncWrite;
use tokio::time::Instant;

use crate::{error_kind_for_domain_code, error_kind_for_query_service_error};

/// Maps a typed Query Application error to its MySQL wire kind.
pub fn mysql_error_kind(error: &QueryServiceError) -> ErrorKind {
    error
        .user_error()
        .and_then(|user_error| error_kind_for_domain_code(user_error.code().as_str()))
        .unwrap_or_else(|| error_kind_for_query_service_error(error.kind()))
}

/// Writes an ungoverned terminal OK response.
pub async fn write_terminal_ok<W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    write_terminal_ok_one(results)
        .await?
        .no_more_results()
        .await
}

/// Writes one terminal OK and returns the writer for a negotiated following
/// statement result.
pub async fn write_terminal_ok_one<'writer, W: AsyncWrite + Unpin>(
    results: QueryResultWriter<'writer, W>,
) -> io::Result<QueryResultWriter<'writer, W>> {
    results.complete_one(OkResponse::default()).await
}

/// Writes the final OK response and settles its governed statement owner.
pub async fn write_governed_terminal_ok<W: AsyncWrite + Unpin>(
    protocol: GovernedProtocolOwner,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    match write_governed_terminal_ok_one(protocol, results).await? {
        crate::governed_result_writer::MysqlStatementWriteOutcome::Continue(results) => {
            results.no_more_results().await
        }
        crate::governed_result_writer::MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

pub async fn write_governed_terminal_ok_one<'writer, W: AsyncWrite + Unpin>(
    protocol: GovernedProtocolOwner,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<crate::MysqlStatementWriteOutcome<'writer, W>> {
    write_governed_terminal_ok_with_more(protocol, results, false).await
}

pub(crate) async fn write_governed_terminal_ok_with_more<'writer, W: AsyncWrite + Unpin>(
    protocol: GovernedProtocolOwner,
    results: QueryResultWriter<'writer, W>,
    more_results: bool,
) -> io::Result<crate::MysqlStatementWriteOutcome<'writer, W>> {
    write_success(protocol, TerminalWriter::Query(Some(results)), more_results).await
}

/// COM_INIT_DB transfers into the same exact writer/flush lifetime.
pub async fn write_governed_init_ok<W: AsyncWrite + Unpin>(
    protocol: GovernedProtocolOwner,
    writer: InitWriter<'_, W>,
) -> io::Result<()> {
    finish_outcome(write_success(protocol, TerminalWriter::Init(Some(writer)), false).await?).await
}
pub async fn write_governed_init_error<W: AsyncWrite + Unpin>(
    error: QueryServiceError,
    protocol: GovernedProtocolOwner,
    writer: InitWriter<'_, W>,
) -> io::Result<()> {
    finish_outcome(write_typed_error(error, protocol, TerminalWriter::Init(Some(writer))).await?)
        .await
}
pub async fn write_governed_terminal_error<W: AsyncWrite + Unpin>(
    error: QueryServiceError,
    protocol: GovernedProtocolOwner,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    finish_outcome(write_typed_error(error, protocol, TerminalWriter::Query(Some(results))).await?)
        .await
}
async fn finish_outcome<W: AsyncWrite + Unpin>(
    outcome: crate::MysqlStatementWriteOutcome<'_, W>,
) -> io::Result<()> {
    match outcome {
        crate::MysqlStatementWriteOutcome::Continue(writer) => writer.no_more_results().await,
        crate::MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

/// Move-only writer/finalize capability, without a row producer or raw IO.
enum TerminalWriter<'writer, W: AsyncWrite + Unpin> {
    Query(Option<QueryResultWriter<'writer, W>>),
    Init(Option<InitWriter<'writer, W>>),
}
impl<'writer, W: AsyncWrite + Unpin> TerminalWriter<'writer, W> {
    fn capabilities(&self) -> CapabilityFlags {
        match self {
            Self::Query(writer) => writer
                .as_ref()
                .expect("terminal query writer")
                .client_capabilities(),
            Self::Init(writer) => writer
                .as_ref()
                .expect("terminal init writer")
                .client_capabilities(),
        }
    }
    fn diagnostic_limit(&self) -> usize {
        match self {
            Self::Query(writer) => {
                writer
                    .as_ref()
                    .expect("terminal query writer")
                    .protocol_limits()
                    .diagnostic_bytes
            }
            Self::Init(writer) => {
                writer
                    .as_ref()
                    .expect("terminal init writer")
                    .protocol_limits()
                    .diagnostic_bytes
            }
        }
    }
    async fn transfer(&mut self) -> io::Result<opensrv_mysql::StreamingResponseLease<'writer, W>> {
        match self {
            Self::Query(writer) => {
                writer
                    .take()
                    .expect("terminal query writer")
                    .into_streaming_result()
                    .await
            }
            Self::Init(writer) => writer
                .take()
                .expect("terminal init writer")
                .into_streaming(),
        }
    }
}
async fn write_success<'writer, W: AsyncWrite + Unpin>(
    mut protocol: GovernedProtocolOwner,
    mut writer: TerminalWriter<'writer, W>,
    more_results: bool,
) -> io::Result<crate::MysqlStatementWriteOutcome<'writer, W>> {
    match protocol.seal_success_visibility() {
        GovernedStatementVisibilitySealOutcome::Sealed => (),
        GovernedStatementVisibilitySealOutcome::Cancelled(_) => {
            return write_closing(
                protocol,
                writer,
                ErrorKind::ER_QUERY_INTERRUPTED,
                b"query cancelled before terminal OK",
            )
            .await;
        }
        _ => {
            return write_closing(
                protocol,
                writer,
                ErrorKind::ER_UNKNOWN_ERROR,
                b"governed statement became stale before terminal OK",
            )
            .await;
        }
    }
    let capabilities = writer.capabilities();
    let deadline = Instant::now() + ACTIVE_WRITE_DEADLINE;
    let lease = match tokio::time::timeout_at(deadline, writer.transfer()).await {
        Ok(Ok(lease)) => lease,
        outcome => {
            drop(writer);
            let _ = protocol.client_disconnected();
            return Err(match outcome {
                Ok(Err(error)) => error,
                _ => timeout_error(),
            });
        }
    };
    // The explicit negotiated flag replaces deferred complete_one. Every
    // successful owner remains alive through its own packet and socket flush.
    let payload = ok_payload(capabilities, more_results);
    let outcome = tokio::time::timeout_at(deadline, lease.finish(&payload)).await;
    drop(payload);
    drop(writer);
    match outcome {
        Ok(Ok(writer)) => {
            let _ = protocol.complete();
            Ok(crate::MysqlStatementWriteOutcome::Continue(writer))
        }
        outcome => {
            let _ = protocol.client_disconnected();
            Err(match outcome {
                Ok(Err(error)) => error,
                _ => timeout_error(),
            })
        }
    }
}
fn ok_payload(capabilities: CapabilityFlags, more_results: bool) -> Vec<u8> {
    let status = if more_results {
        StatusFlags::SERVER_MORE_RESULTS_EXISTS.bits()
    } else {
        0
    };
    let mut payload = vec![0, 0, 0]; // OK, affected rows, last insert id
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
}
async fn write_typed_error<'writer, W: AsyncWrite + Unpin>(
    error: QueryServiceError,
    protocol: GovernedProtocolOwner,
    writer: TerminalWriter<'writer, W>,
) -> io::Result<crate::MysqlStatementWriteOutcome<'writer, W>> {
    let (kind, message) = if cancellation_overrides_typed_error(protocol.cancellation().reason()) {
        (
            ErrorKind::ER_QUERY_INTERRUPTED,
            b"query cancelled".as_slice(),
        )
    } else {
        (mysql_error_kind(&error), error.message().as_bytes())
    };
    let prepared = prepare_closing(protocol, writer, kind, message);
    // The bounded copy now has complete capacity, while this original service
    // diagnostic still has its ordinary owner. Retire it before transferring
    // that owner or entering a potentially slow closing future.
    drop(error);
    match prepared {
        Ok(prepared) => finish_prepared_closing(prepared).await,
        Err(refusal) => Err(refusal.disconnect()),
    }
}
struct TerminalClosingWriter<'writer, W: AsyncWrite + Unpin> {
    writer: TerminalWriter<'writer, W>,
    kind: ErrorKind,
    message: Vec<u8>,
}
impl<'writer, W: AsyncWrite + Unpin> TerminalClosingWriter<'writer, W> {
    async fn finish(&mut self) -> io::Result<QueryResultWriter<'writer, W>> {
        let lease = self.writer.transfer().await?;
        let mut writer = match lease.into_closing_lease(Vec::new(), self.kind, &self.message) {
            Ok(writer) => writer,
            Err((lease, error)) => {
                drop(lease);
                return Err(error);
            }
        };
        writer.finish().await
    }
}
struct PreparedTerminalClosing<'writer, W: AsyncWrite + Unpin> {
    writer: TerminalClosingWriter<'writer, W>,
    protocol: GovernedProtocolOwner,
    capacity: novarocks_workload_control::ResultWindowGrant,
}
struct TerminalClosingRefusal<'writer, W: AsyncWrite + Unpin> {
    writer: TerminalWriter<'writer, W>,
    protocol: GovernedProtocolOwner,
    error: io::Error,
}
impl<W: AsyncWrite + Unpin> TerminalClosingRefusal<'_, W> {
    fn disconnect(self) -> io::Error {
        let Self {
            writer,
            mut protocol,
            error,
        } = self;
        drop(writer);
        let _ = protocol.client_disconnected();
        error
    }
}
async fn write_closing<'writer, W: AsyncWrite + Unpin>(
    protocol: GovernedProtocolOwner,
    writer: TerminalWriter<'writer, W>,
    kind: ErrorKind,
    message: &[u8],
) -> io::Result<crate::MysqlStatementWriteOutcome<'writer, W>> {
    match prepare_closing(protocol, writer, kind, message) {
        Ok(prepared) => finish_prepared_closing(prepared).await,
        Err(refusal) => Err(refusal.disconnect()),
    }
}
#[allow(clippy::result_large_err)]
fn prepare_closing<'writer, W: AsyncWrite + Unpin>(
    mut protocol: GovernedProtocolOwner,
    writer: TerminalWriter<'writer, W>,
    kind: ErrorKind,
    message: &[u8],
) -> Result<PreparedTerminalClosing<'writer, W>, TerminalClosingRefusal<'writer, W>> {
    if matches!(
        protocol.cancellation().reason(),
        Some(
            QueryCancellationReason::ExplicitKillConnection { .. }
                | QueryCancellationReason::ServerShutdown
                | QueryCancellationReason::ClientDisconnected
        )
    ) {
        return Err(TerminalClosingRefusal {
            writer,
            protocol,
            error: io::Error::new(io::ErrorKind::Interrupted, "terminal connection was closed"),
        });
    }
    let capacity = match protocol.try_closing_capacity(protocol.cancellation().is_cancelled()) {
        Ok(capacity) if capacity.check_backing_total(CLOSING_OBJECT_BYTES).is_ok() => capacity,
        _ => {
            return Err(TerminalClosingRefusal {
                writer,
                protocol,
                error: io::Error::other("terminal closing capacity is unavailable"),
            });
        }
    };
    // Preflight before copy; the original service diagnostic stays borrowed
    // under its ordinary owner while the complete closing position is taken.
    let limit = writer.diagnostic_limit();
    let writer = TerminalClosingWriter {
        writer,
        kind,
        message: message[..message.len().min(limit)].to_vec(),
    };
    Ok(PreparedTerminalClosing {
        writer,
        protocol,
        capacity,
    })
}
async fn finish_prepared_closing<'writer, W: AsyncWrite + Unpin>(
    prepared: PreparedTerminalClosing<'writer, W>,
) -> io::Result<crate::MysqlStatementWriteOutcome<'writer, W>> {
    let PreparedTerminalClosing {
        writer,
        protocol,
        capacity,
    } = prepared;
    let mut closing = match protocol.into_closing_delivery(writer, capacity, CLOSING_OBJECT_BYTES) {
        Ok(closing) => closing,
        Err((mut protocol, writer, _capacity)) => {
            drop(writer);
            let _ = protocol.client_disconnected();
            return Err(io::Error::other("terminal closing lost its exact owner"));
        }
    };
    let deadline = Instant::now() + CLOSING_DEADLINE;
    match tokio::time::timeout_at(deadline, closing.writer_mut().finish()).await {
        Ok(Ok(writer)) => {
            let _ = closing.settle_after_writer_exit().await;
            writer.no_more_results().await?;
            Ok(crate::MysqlStatementWriteOutcome::Terminated)
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

fn cancellation_overrides_typed_error(reason: Option<QueryCancellationReason>) -> bool {
    !matches!(
        reason,
        Some(QueryCancellationReason::DeadlineExceeded { .. })
    ) && reason.is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_user_error::{
        ErrorCodeDescriptor, ErrorCodeId, ErrorCodeStatus, ErrorPhase, RetryClass, UserError,
    };

    #[test]
    fn typed_user_error_overrides_the_legacy_session_error_kind() {
        let user_error = UserError::from_descriptor(
            ErrorCodeDescriptor {
                code: ErrorCodeId::new("sql.analyze.unknown_table"),
                phase: ErrorPhase::Analyze,
                status: ErrorCodeStatus::Active,
            },
            "unknown table",
            None,
            RetryClass::Never,
        );

        assert_eq!(
            mysql_error_kind(&QueryServiceError::from_user_error(user_error)),
            ErrorKind::ER_NO_SUCH_TABLE
        );
    }

    #[test]
    fn deadline_cancellation_keeps_the_typed_timeout_error() {
        assert!(!cancellation_overrides_typed_error(Some(
            QueryCancellationReason::DeadlineExceeded { timeout_ms: 1_000 },
        )));
        assert!(cancellation_overrides_typed_error(Some(
            QueryCancellationReason::ClientDisconnected,
        )));
    }
}
