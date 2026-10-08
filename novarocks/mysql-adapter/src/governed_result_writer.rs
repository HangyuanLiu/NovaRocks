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

//! Governed Query Application result delivery over the MySQL wire.

use std::io;

use novarocks_query_application::api::{
    QueryExecutionError, QueryExecutionErrorKind, QueryResult, ResultFailureView,
};
use novarocks_query_application::cancellation::{QueryCancellationReason, QueryCancellationView};
use novarocks_query_application::protocol_delivery::{
    GovernedImmediateStatementResult, StreamingStatementResult,
};
use opensrv_mysql::QueryResultWriter;
use tokio::io::AsyncWrite;

pub enum MysqlStatementWriteOutcome<'writer, W: AsyncWrite + Unpin> {
    Continue(QueryResultWriter<'writer, W>),
    Terminated,
}

/// Delivers an already-materialized immediate Query Application result.
///
/// This result has no governed delivery owner, but the MySQL adapter still
/// owns its schema, rows, and terminal wire transitions.
pub async fn write_query_result<W: AsyncWrite + Unpin>(
    result: QueryResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    write_query_result_one(result, results)
        .await?
        .no_more_results()
        .await
}

/// Writes one materialized result and returns the writer for the next
/// negotiated result on this exact connection.
pub async fn write_query_result_one<'writer, W: AsyncWrite + Unpin>(
    result: QueryResult,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<QueryResultWriter<'writer, W>> {
    let batches = result.batches.iter().collect::<Vec<_>>();
    crate::write_record_batches_one(&result.columns, &batches, results).await
}

pub async fn write_governed_query_result<W: AsyncWrite + Unpin>(
    result: GovernedImmediateStatementResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    match write_governed_query_result_one(result, results).await? {
        MysqlStatementWriteOutcome::Continue(results) => results.no_more_results().await,
        MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

pub async fn write_governed_query_result_one<'writer, W: AsyncWrite + Unpin>(
    result: GovernedImmediateStatementResult,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    crate::local_result_writer::write_local_result_one(result, results, false).await
}

/// Writes one Query Application result without detaching its delivery and
/// resource owners from the protocol operation which consumes them.
pub async fn write_streaming_query_result<W: AsyncWrite + Unpin>(
    result: StreamingStatementResult,
    results: QueryResultWriter<'_, W>,
) -> io::Result<()> {
    match write_streaming_query_result_one(result, results).await? {
        MysqlStatementWriteOutcome::Continue(results) => results.no_more_results().await,
        MysqlStatementWriteOutcome::Terminated => Ok(()),
    }
}

pub async fn write_streaming_query_result_one<'writer, W: AsyncWrite + Unpin>(
    result: StreamingStatementResult,
    results: QueryResultWriter<'writer, W>,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    write_streaming_query_result_with_more(result, results, false).await
}

pub(crate) async fn write_streaming_query_result_with_more<'writer, W: AsyncWrite + Unpin>(
    mut result: StreamingStatementResult,
    results: QueryResultWriter<'writer, W>,
    more_results: bool,
) -> io::Result<MysqlStatementWriteOutcome<'writer, W>> {
    let schema_delivery = match result.begin_schema() {
        Some(delivery) => delivery,
        None => {
            let _ = result.fail();
            return Err(invalid_data_error(
                "streaming query result has no schema delivery".to_string(),
            ));
        }
    };
    crate::relay_result_writer::write_relay_result_one(
        result,
        schema_delivery,
        results,
        more_results,
    )
    .await
}

pub(crate) fn is_terminal_cancellation(error: &QueryExecutionError) -> bool {
    matches!(
        error.kind(),
        QueryExecutionErrorKind::Cancelled | QueryExecutionErrorKind::DeadlineExceeded
    )
}

pub(crate) async fn wait_terminal_result_failure(
    failure: &mut ResultFailureView,
    cancellation: &QueryCancellationView,
) -> QueryExecutionError {
    tokio::select! {
        biased;
        error = failure.wait() => normalize_terminal_cancellation(error, cancellation.reason()),
        reason = cancellation.cancelled() => cancelled_query_result_delivery(reason),
    }
}

fn normalize_terminal_cancellation(
    error: QueryExecutionError,
    reason: Option<QueryCancellationReason>,
) -> QueryExecutionError {
    match error.kind() {
        QueryExecutionErrorKind::DeadlineExceeded => {
            QueryExecutionError::new(QueryExecutionErrorKind::DeadlineExceeded, "query timed out")
        }
        QueryExecutionErrorKind::Cancelled => match reason {
            Some(reason) => cancelled_query_result_delivery(reason),
            None => error,
        },
        _ => error,
    }
}

pub(crate) fn cancelled_query_result_delivery(
    reason: QueryCancellationReason,
) -> QueryExecutionError {
    let message = match reason {
        QueryCancellationReason::DeadlineExceeded { timeout_ms } => {
            format!("query timed out after {timeout_ms} ms")
        }
        QueryCancellationReason::ExecutionCancellationRequested => {
            "Query execution cancellation was requested".to_string()
        }
        QueryCancellationReason::ExecutionOwnerDropped => {
            "Query execution protocol owner was dropped".to_string()
        }
        QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms } => format!(
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded after {timeout_ms} ms"
        ),
        QueryCancellationReason::ExplicitKill { .. } => {
            "Query execution was interrupted".to_string()
        }
        QueryCancellationReason::ExplicitKillConnection { .. } => {
            "Query execution was interrupted because the connection was killed".to_string()
        }
        QueryCancellationReason::ClientDisconnected => {
            "Query execution was interrupted because the client disconnected".to_string()
        }
        QueryCancellationReason::ServerShutdown => {
            "Query execution was interrupted because the server is shutting down".to_string()
        }
    };
    QueryExecutionError::new(QueryExecutionErrorKind::Cancelled, message)
}

pub(crate) fn governed_cancelled_query_result_delivery(
    reason: novarocks_workload_control::CancellationReason,
) -> QueryExecutionError {
    let message = match reason {
        novarocks_workload_control::CancellationReason::FrontendDrainDeadlineExceeded => {
            "FRONTEND_DRAIN_DEADLINE_EXCEEDED: frontend drain deadline exceeded".to_string()
        }
        reason => format!("MySQL result delivery cancelled: {reason:?}"),
    };
    QueryExecutionError::new(QueryExecutionErrorKind::Cancelled, message)
}

fn invalid_data_error(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod streaming_result_tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use novarocks_query_application::api::{
        QueryExecutionError, QueryExecutionErrorKind, ResultDelivery, ResultField,
    };
    use novarocks_query_application::test_support::{
        ResultStreamTestProducer, TestResultDeliveryDisposition,
    };
    use novarocks_types::{AttemptId, QueryExecutionId, QueryId};
    use novarocks_workload_control::{
        CancellationReason, ResourceConfig, WorkClass, WorkRequest, WorkloadConfig, WorkloadControl,
    };

    use super::*;
    use novarocks_query_application::client_connection::ClientConnectionToken;
    use novarocks_query_application::protocol_delivery::StreamingStatementResult;
    use novarocks_query_application::query_control::QueryApplicationControl;
    use novarocks_query_application::session_control::{
        QueryCancelOutcome, QueryControlPort, QueryControlService, QuerySessionLease,
        SessionIdentity,
    };

    struct Fixture {
        producer: ResultStreamTestProducer,
        result: Option<StreamingStatementResult>,
        capacity: novarocks_workload_control::ResultCapacityHandle,
        _governance: WorkloadControl,
        control: QueryControlService,
        session: QuerySessionLease,
        schema_receipt: novarocks_query_application::test_support::TestResultDeliveryReceipt,
    }

    fn fixture(fields: Vec<ResultField>) -> Fixture {
        fixture_with_timeout(fields, None)
    }

    fn fixture_with_timeout(fields: Vec<ResultField>, timeout_ms: Option<u64>) -> Fixture {
        let execution_id = QueryExecutionId::new(
            QueryId::new(71, 1),
            AttemptId::new(1).expect("test attempt"),
        )
        .expect("test execution");
        let (producer, execution, resources, schema_receipt) = ResultStreamTestProducer::open(
            execution_id,
            fields,
            1,
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .expect("open result stream");

        let port: Arc<dyn QueryControlPort> = Arc::new(QueryApplicationControl::default());
        let control = QueryControlService::new(port);
        let session = control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(91, 1).expect("connection token"),
                "root",
            ))
            .expect("register session");
        let governance = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .expect("test statement workload");
        governance.mark_ready().expect("statement workload ready");
        let mut statement = control
            .begin_governed_query_statement(
                session.token(),
                &governance.root_admission(),
                None,
                timeout_ms,
                None,
            )
            .expect("begin governed statement");
        statement
            .take_execution_owner()
            .expect("test transfers execution owner")
            .complete();
        let result =
            StreamingStatementResult::try_from_execution(execution, resources.clone(), statement)
                .expect("bind streaming statement");
        Fixture {
            result: Some(result),
            capacity: producer.result_capacity(),
            producer,
            _governance: governance,
            control,
            session,
            schema_receipt,
        }
    }

    fn string_fields() -> Vec<ResultField> {
        vec![ResultField::new("value", DataType::Utf8, false, None)]
    }

    fn text_body(value: &str) -> Vec<u8> {
        assert!(value.len() < 251);
        let mut body = Vec::new();
        body.extend_from_slice(&((value.len() + 1) as u32).to_le_bytes());
        body.push(value.len() as u8);
        body.extend_from_slice(value.as_bytes());
        body
    }

    #[test]
    fn deadline_delivery_cancellation_keeps_the_statement_timeout_message() {
        let error = cancelled_query_result_delivery(QueryCancellationReason::DeadlineExceeded {
            timeout_ms: 1_000,
        });
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        assert_eq!(error.to_string(), "query timed out after 1000 ms");
    }

    #[test]
    fn actor_deadline_terminal_is_settled_as_a_mysql_timeout() {
        let error = normalize_terminal_cancellation(
            QueryExecutionError::new(
                QueryExecutionErrorKind::DeadlineExceeded,
                "logical execution deadline expired before success EOF",
            ),
            None,
        );

        assert!(is_terminal_cancellation(&error));
        assert_eq!(error.kind(), QueryExecutionErrorKind::DeadlineExceeded);
        assert_eq!(error.to_string(), "query timed out");
    }

    #[test]
    fn explicit_kill_delivery_cancellation_keeps_the_statement_message() {
        let error = cancelled_query_result_delivery(QueryCancellationReason::ExplicitKill {
            requester_connection_id: 7,
        });
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        assert_eq!(error.to_string(), "Query execution was interrupted");
    }

    async fn observe_ready_actor_failure(
        error: QueryExecutionError,
        reason: Option<QueryCancellationReason>,
    ) -> QueryExecutionError {
        let timeout_ms = match reason.as_ref() {
            Some(QueryCancellationReason::DeadlineExceeded { timeout_ms })
            | Some(QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms }) => {
                Some(*timeout_ms)
            }
            _ => None,
        };
        let mut fixture = fixture_with_timeout(string_fields(), timeout_ms);
        let cancellation = fixture.result.as_ref().unwrap().cancellation();
        let mut failure = fixture.result.as_ref().unwrap().failure_view().unwrap();
        if let Some(reason) = reason {
            assert!(matches!(
                fixture
                    .control
                    .cancel_session_statement(fixture.session.token(), reason),
                QueryCancelOutcome::Requested
            ));
        }
        // Both observations are ready before the production selector is polled.
        fixture.producer.fail(error);
        let observed = wait_terminal_result_failure(&mut failure, &cancellation).await;
        if is_terminal_cancellation(&observed) {
            fixture.result.take().unwrap().settle_cancellation();
        } else {
            fixture.result.take().unwrap().fail();
        }
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::NoActiveStatement
        ));
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        observed
    }

    #[tokio::test]
    async fn ready_actor_cancellation_and_explicit_kill_use_the_statement_message() {
        let error = observe_ready_actor_failure(
            QueryExecutionError::new(
                QueryExecutionErrorKind::Cancelled,
                "logical execution was cancelled before success EOF",
            ),
            Some(QueryCancellationReason::ExplicitKill {
                requester_connection_id: 7,
            }),
        )
        .await;
        assert_eq!(error.kind(), QueryExecutionErrorKind::Cancelled);
        assert_eq!(error.to_string(), "Query execution was interrupted");
    }

    #[tokio::test]
    async fn ready_actor_cancellation_uses_each_exact_statement_reason() {
        for reason in [
            QueryCancellationReason::ExecutionCancellationRequested,
            QueryCancellationReason::ExecutionOwnerDropped,
            QueryCancellationReason::ExplicitKillConnection {
                requester_connection_id: 7,
            },
            QueryCancellationReason::ClientDisconnected,
            QueryCancellationReason::DeadlineExceeded { timeout_ms: 1_000 },
            QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 2_000 },
            QueryCancellationReason::ServerShutdown,
        ] {
            let expected = cancelled_query_result_delivery(reason.clone());
            let error = observe_ready_actor_failure(
                QueryExecutionError::new(QueryExecutionErrorKind::Cancelled, "actor cancellation"),
                Some(reason),
            )
            .await;
            assert_eq!(error, expected);
        }
    }

    #[tokio::test]
    async fn actor_cancellation_without_a_statement_reason_is_not_invented() {
        let expected =
            QueryExecutionError::new(QueryExecutionErrorKind::Cancelled, "actor cancellation");
        assert_eq!(
            observe_ready_actor_failure(expected.clone(), None).await,
            expected
        );
    }

    #[tokio::test]
    async fn ready_actor_failures_are_not_retagged_by_a_statement_kill() {
        for kind in [
            QueryExecutionErrorKind::Failed,
            QueryExecutionErrorKind::InvalidRequest,
        ] {
            let expected = QueryExecutionError::new(kind, "original actor failure");
            let error = observe_ready_actor_failure(
                expected.clone(),
                Some(QueryCancellationReason::ExplicitKill {
                    requester_connection_id: 7,
                }),
            )
            .await;
            assert_eq!(error, expected);
        }
    }

    #[tokio::test]
    async fn ready_actor_deadline_keeps_its_typed_origin_despite_a_statement_kill() {
        let error = observe_ready_actor_failure(
            QueryExecutionError::new(
                QueryExecutionErrorKind::DeadlineExceeded,
                "logical execution deadline expired before success EOF",
            ),
            Some(QueryCancellationReason::ExplicitKill {
                requester_connection_id: 7,
            }),
        )
        .await;
        assert_eq!(error.kind(), QueryExecutionErrorKind::DeadlineExceeded);
        assert_eq!(error.to_string(), "query timed out");
    }

    #[tokio::test]
    async fn schema_batch_and_eof_complete_only_in_protocol_order() {
        let mut fixture = fixture(string_fields());
        let result = fixture.result.as_mut().unwrap();
        result.begin_schema().expect("schema").complete();
        assert_eq!(
            fixture.schema_receipt.wait().await,
            TestResultDeliveryDisposition::Completed
        );

        let batch_receipt = fixture
            .producer
            .enqueue_client_body(0, text_body("abc"), 1)
            .await
            .unwrap();
        let ResultDelivery::Segment(delivery) = result.next_delivery().await.unwrap().unwrap()
        else {
            panic!("expected batch")
        };
        assert_eq!(fixture.capacity.snapshot().held_positions, [1, 0, 0, 0]);
        delivery.complete();
        assert_eq!(
            batch_receipt.wait().await,
            TestResultDeliveryDisposition::Completed
        );
        assert_eq!(fixture.capacity.snapshot().held_positions, [1, 0, 0, 0]);

        let end_receipt = fixture.producer.enqueue_end(1).await;
        let ResultDelivery::End(end) = result.next_delivery().await.unwrap().unwrap() else {
            panic!("expected EOF")
        };
        end.complete();
        assert_eq!(
            end_receipt.wait().await,
            TestResultDeliveryDisposition::Completed
        );
        fixture.result.take().unwrap().complete();
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::NoActiveStatement
        ));
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn slow_protocol_owner_retains_original_window_until_actual_exit() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let receipt = fixture
            .producer
            .enqueue_client_body(0, text_body("slow"), 1)
            .await
            .unwrap();
        let ResultDelivery::Segment(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected batch")
        };
        tokio::task::yield_now().await;
        assert_eq!(fixture.capacity.snapshot().held_positions, [1, 0, 0, 0]);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), receipt.wait())
                .await
                .is_err()
        );
        delivery.complete();
        assert_eq!(fixture.capacity.snapshot().held_positions, [1, 0, 0, 0]);
        drop(fixture.result.take());
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn partial_protocol_failure_fails_receipt_under_original_window() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let receipt = fixture
            .producer
            .enqueue_client_body(0, text_body("partial"), 1)
            .await
            .unwrap();
        let ResultDelivery::Segment(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected batch")
        };
        delivery.fail(QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "client disconnected after a partial row write",
        ));
        assert!(matches!(
            receipt.wait().await,
            TestResultDeliveryDisposition::Failed(_)
        ));
        assert_eq!(fixture.capacity.snapshot().held_positions, [1, 0, 0, 0]);
        fixture.result.take().unwrap().fail();
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn malformed_client_body_is_refused_before_protocol_delivery() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let error = fixture
            .producer
            .enqueue_client_body(0, vec![2, 0, 0, 0, b'a'], 1)
            .await;
        assert!(error.is_err(), "truncated row must fail before enqueue");
        fixture.result.take().unwrap().fail();
        assert_eq!(
            fixture.producer.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn native_failure_view_interrupts_an_in_progress_protocol_owner() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        let receipt = fixture
            .producer
            .enqueue_client_body(0, text_body("blocked client"), 1)
            .await
            .unwrap();
        let ResultDelivery::Segment(delivery) = fixture
            .result
            .as_mut()
            .unwrap()
            .next_delivery()
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected batch")
        };
        let mut failure = fixture.result.as_ref().unwrap().failure_view().unwrap();
        let expected = QueryExecutionError::new(
            QueryExecutionErrorKind::Failed,
            "native attempt failed during client backpressure",
        );
        fixture.producer.fail(expected.clone());
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_millis(50), failure.wait())
                .await
                .expect("failure observation must not wait for another batch"),
            expected
        );
        delivery.fail(expected);
        fixture.result.take().unwrap().fail();
        assert!(matches!(
            receipt.wait().await,
            TestResultDeliveryDisposition::Failed(_)
        ));
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn owner_drop_cancels_execution_and_drops_undelivered_schema() {
        let mut fixture = fixture(string_fields());
        drop(fixture.result.take());
        assert_eq!(
            fixture.schema_receipt.wait().await,
            TestResultDeliveryDisposition::Dropped
        );
        assert_eq!(
            fixture.producer.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::NoActiveStatement
        ));
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[tokio::test]
    async fn query_control_cancellation_remains_live_until_stream_owner_finishes() {
        let mut fixture = fixture(string_fields());
        fixture
            .result
            .as_mut()
            .unwrap()
            .begin_schema()
            .unwrap()
            .complete();
        let _ = fixture.schema_receipt.wait().await;
        assert!(matches!(
            fixture.control.cancel_session_statement(
                fixture.session.token(),
                QueryCancellationReason::ClientDisconnected,
            ),
            QueryCancelOutcome::Requested
        ));
        assert_eq!(
            fixture.result.as_ref().unwrap().cancellation().reason(),
            Some(QueryCancellationReason::ClientDisconnected)
        );
        fixture.result.take().unwrap().fail();
        assert_eq!(
            fixture.producer.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
        let capacity = fixture.capacity.clone();
        fixture.producer.finish();
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}
