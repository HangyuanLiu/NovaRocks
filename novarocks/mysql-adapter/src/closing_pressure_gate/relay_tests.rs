// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! No-service components. One real V1 governed Closing grant is not Closing64 Native.
use super::*;
use novarocks_query_application::{
    cancellation::QueryCancellationReason,
    protocol_delivery::GovernedProtocolOwner,
    query_control::QueryApplicationControl,
    session_control::{QueryControlService, QuerySessionLease, SessionIdentity},
};
use novarocks_workload_control::{
    ResultCapacityConfig, ResultCapacityHandle, ResultWindowClass, WorkloadConfig, WorkloadControl,
};
use opensrv_mysql::{
    ClosingMysqlWriter, ErrorKind, OwnedStreamingMysqlWriter, ProtocolLimits, ResidentTailPart,
};
use std::{future::Future, sync::atomic::AtomicUsize, time::Duration};
use tokio::io::{AsyncWriteExt, Sink};

type Original = InitiallyRawPressureWriter<Sink>;
type Framer = OwnedStreamingMysqlWriter<Original>;
struct Fixture {
    _host: WorkloadControl,
    capacity: ResultCapacityHandle,
    control: QueryControlService,
    _session: QuerySessionLease,
    protocol: Option<GovernedProtocolOwner>,
    owner: Arc<PressureOwner>,
    controller: PressureController,
    hook: PressureRelayHook,
}
impl Fixture {
    async fn new() -> Self {
        let host = WorkloadControl::try_new_counted(WorkloadConfig::default())
            .unwrap()
            .owner;
        let capacity = host
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        host.mark_ready().unwrap();
        let control = QueryControlService::new(Arc::new(QueryApplicationControl::default()));
        let connection = ClientConnectionToken::new(100, 59).unwrap();
        let session = control
            .register_session(SessionIdentity::new(connection, "root"))
            .unwrap();
        let statement = control
            .begin_queued_governed_query_statement_with_result(
                session.token(),
                &host.root_admission(),
                None,
                None,
                None,
                ResultWindowClass::Client,
            )
            .await
            .unwrap();
        let protocol = GovernedProtocolOwner::new(statement);
        let actual_statement = protocol.statement_token().unwrap();
        let fe = FrontendProcessId::new_v7();
        let (owner, mut controller) =
            PressureOwner::new(fe, Instant::now() + Duration::from_secs(20)).unwrap();
        let owner = Arc::new(owner);
        // Unused entries exercise finite geometry only: they are NOT sixty-four real statements.
        let targets = std::array::from_fn(|i| ArmInput {
            handshake_connection_id: 100 + i as u32,
            original_sql_sha256: ORIGINAL_SQL_SHA256,
        });
        controller.arm_targets(fe, targets).unwrap();
        let hook = owner
            .bind_relay(connection, actual_statement, ORIGINAL_SQL_SHA256)
            .unwrap()
            .unwrap();
        Self {
            _host: host,
            capacity,
            control,
            _session: session,
            protocol: Some(protocol),
            owner,
            controller,
            hook,
        }
    }
    fn framer(&self) -> Framer {
        OwnedStreamingMysqlWriter::new(
            InitiallyRawPressureWriter::new(
                tokio::io::sink(),
                ClientConnectionToken::new(100, 59).unwrap(),
                Arc::clone(&self.owner),
            ),
            ProtocolLimits::default(),
            4,
        )
        .unwrap()
    }
    async fn real_rows_cut(&self, writer: &mut Framer) -> io::Result<FramingCursor> {
        writer.flush_socket().await?; // Actual original carrier attaches; no fabricated attached flag.
        self.hook.begin_rows(writer.receipt())?;
        writer.start_row(1_048_580)?;
        {
            let original = async {
                writer.write_slice(&[0xfd, 0, 0, 16]).await?;
                let chunk = [b'x'; 4096];
                for _ in 0..256 {
                    writer.write_slice(&chunk).await?;
                }
                writer.flush_pending().await
            };
            tokio::pin!(original);
            tokio::select! {
                result=&mut original=> {result?;return Err(error(Failure::Transition));},
                result=self.hook.scope.wait_rows_blocked()=>{result?;},
            }
        } // Actual borrowed original Data write dropped before reading its original cursor.
        let actual = writer.receipt();
        self.control.cancel_session_statement(
            self.hook.statement.session(),
            QueryCancellationReason::ExplicitKill {
                requester_connection_id: 100,
            },
        );
        if !self
            .protocol
            .as_ref()
            .unwrap()
            .cancellation()
            .is_cancelled()
        {
            return Err(error(Failure::Transition));
        }
        self.hook.record_cancel(actual)?;
        Ok(actual)
    }
    fn closing(&mut self, writer: Framer) -> ClosingDelivery<ComponentClosing<Original>> {
        let receipt = writer.receipt();
        let missing = receipt
            .logical_remaining()
            .checked_sub(writer.buffered_row_bytes())
            .unwrap();
        assert!(missing <= 9, "original x frozen remaining wire geometry");
        let mut tail = Vec::with_capacity(1);
        if missing != 0 {
            tail.push(ResidentTailPart::new(Arc::from(vec![b'x'; missing]), 0..missing).unwrap());
        }
        let writer = writer
            .into_closing(tail, ErrorKind::ER_QUERY_INTERRUPTED, b"interrupted")
            .unwrap_or_else(|(_, source)| {
                panic!(
                    "component original Closing writer refused: kind={:?}",
                    source.kind()
                )
            });
        let mut protocol = self.protocol.take().unwrap();
        let actual_capacity = protocol.try_closing_capacity(true).unwrap();
        protocol
            .into_closing_delivery(
                ComponentClosing {
                    writer: Some(writer),
                },
                actual_capacity,
                crate::relay_result_writer::CLOSING_OBJECT_BYTES,
            )
            .unwrap_or_else(|(_, _, _)| {
                panic!("component original governed Closing handoff refused")
            })
    }
}
/// NO-service adapter for public consuming ClosingMysqlWriter::finish. The production helper
/// uses the original ClosingResponseLease directly; this component does not instantiate that lease.
struct ComponentClosing<W> {
    writer: Option<ClosingMysqlWriter<W>>,
}
impl<W: AsyncWrite + Unpin> ComponentClosing<W> {
    async fn finish(&mut self) -> io::Result<W> {
        self.writer
            .take()
            .ok_or_else(|| error(Failure::Transition))?
            .finish()
            .await
    }
}
async fn first_poll<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
    poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
}

#[tokio::test]
async fn same_original_carrier_holds_real_v1_closing_then_release_and_settlement() {
    let mut fixture = Fixture::new().await;
    let mut writer = fixture.framer();
    let actual = fixture.real_rows_cut(&mut writer).await.unwrap();
    let mut closing = fixture.closing(writer);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    let original_restored = {
        let work = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
            fixture.hook.scope.observe_installed_closing(
                fixture.hook.statement,
                actual,
                &closing,
            )?;
            let original = closing.writer_mut().finish();
            tokio::pin!(original);
            paired_finish(&fixture.hook.scope, original).await
        });
        tokio::pin!(work);
        assert!(first_poll(work.as_mut()).await.is_pending());
        let facts = fixture.controller.snapshot(0).unwrap();
        assert!(
            facts.real_closing_observed
                && facts.paired_closing_polls > 0
                && facts.closing_write_blocked
        );
        assert!(!facts.writer_destructor_returned);
        fixture.controller.release(0).unwrap();
        work.await.unwrap().unwrap()
    };
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    let _ = closing.settle_after_writer_exit().await;
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
    assert!(
        !fixture
            .hook
            .scope
            .snapshot()
            .unwrap()
            .writer_destructor_returned
    );
    drop(original_restored);
    assert!(
        fixture
            .hook
            .scope
            .snapshot()
            .unwrap()
            .writer_destructor_returned
    );
    assert!(fixture.hook.scope.snapshot().unwrap().failure.is_none());
}

#[tokio::test]
async fn original_five_second_timeout_runs_while_gate_held_no_pre_timeout_wait() {
    let mut fixture = Fixture::new().await;
    let mut writer = fixture.framer();
    let actual = fixture.real_rows_cut(&mut writer).await.unwrap();
    let mut closing = fixture.closing(writer);
    let outcome = {
        let work = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
            fixture.hook.scope.observe_installed_closing(
                fixture.hook.statement,
                actual,
                &closing,
            )?;
            let original = closing.writer_mut().finish();
            tokio::pin!(original);
            paired_finish(&fixture.hook.scope, original).await
        });
        work.await
    }; // Original timeout dropped its actual borrowed finish, never the outer owner.
    assert!(outcome.is_err());
    let facts = fixture.hook.scope.snapshot().unwrap();
    assert!(
        facts.real_closing_observed
            && facts.paired_closing_polls > 0
            && facts.writer_destructor_returned
    );
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
}

#[tokio::test]
async fn dropping_borrowed_finish_drops_writer_but_original_protocol_grant_remains_outer() {
    let mut fixture = Fixture::new().await;
    let mut writer = fixture.framer();
    let actual = fixture.real_rows_cut(&mut writer).await.unwrap();
    let mut closing = fixture.closing(writer);
    {
        let work = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
            fixture.hook.scope.observe_installed_closing(
                fixture.hook.statement,
                actual,
                &closing,
            )?;
            let original = closing.writer_mut().finish();
            tokio::pin!(original);
            paired_finish(&fixture.hook.scope, original).await
        });
        tokio::pin!(work);
        assert!(first_poll(work.as_mut()).await.is_pending());
    } // Whole original borrowed future/storage dropped here, not merely its Pin reference.
    assert!(
        !fixture
            .hook
            .scope
            .core
            .state(0)
            .unwrap()
            .closing_poll_active
    );
    assert!(
        fixture
            .hook
            .scope
            .snapshot()
            .unwrap()
            .writer_destructor_returned
    );
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
}

#[tokio::test]
async fn wrong_actual_closing_writer_cannot_claim_pair_from_hook_identity_alone() {
    let mut fixture = Fixture::new().await;
    let mut original = fixture.framer();
    let actual = fixture.real_rows_cut(&mut original).await.unwrap();
    // A valid real V1 governed Closing object, deliberately containing a different ungated W.
    let unrelated =
        OwnedStreamingMysqlWriter::new(tokio::io::sink(), ProtocolLimits::default(), 4).unwrap();
    let unrelated = unrelated
        .into_closing(Vec::new(), ErrorKind::ER_QUERY_INTERRUPTED, b"interrupted")
        .unwrap_or_else(|(_, _)| panic!("valid unrelated component Closing"));
    let mut protocol = fixture.protocol.take().unwrap();
    let capacity = protocol.try_closing_capacity(true).unwrap();
    let mut closing = protocol
        .into_closing_delivery(
            ComponentClosing {
                writer: Some(unrelated),
            },
            capacity,
            crate::relay_result_writer::CLOSING_OBJECT_BYTES,
        )
        .unwrap_or_else(|(_, _, _)| panic!("actual Closing handoff"));
    let refused = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
        fixture
            .hook
            .scope
            .observe_installed_closing(fixture.hook.statement, actual, &closing)?;
        let wrong = closing.writer_mut().finish();
        tokio::pin!(wrong);
        paired_finish(&fixture.hook.scope, wrong).await
    })
    .await
    .unwrap();
    assert!(refused.is_err());
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().paired_closing_polls,
        0
    );
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().failure,
        Some(Failure::Identity)
    );
    drop(original);
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
}

#[tokio::test]
async fn unrelated_async_pending_is_not_a_same_writer_witness() {
    let mut fixture = Fixture::new().await;
    let mut original = fixture.framer();
    let actual = fixture.real_rows_cut(&mut original).await.unwrap();
    let mut closing = fixture.closing(original);
    let refusal = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
        fixture
            .hook
            .scope
            .observe_installed_closing(fixture.hook.statement, actual, &closing)?;
        let wrong = std::future::pending::<io::Result<()>>();
        tokio::pin!(wrong);
        paired_finish(&fixture.hook.scope, wrong).await
    })
    .await
    .unwrap();
    assert!(refusal.is_err());
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().paired_closing_polls,
        0
    );
    assert!(
        !fixture
            .hook
            .scope
            .core
            .state(0)
            .unwrap()
            .closing_poll_active
    );
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
}

#[tokio::test]
async fn original_async_panic_preserves_payload_and_clears_only_poll_guard_not_outer_grant() {
    let mut fixture = Fixture::new().await;
    let mut original = fixture.framer();
    let actual = fixture.real_rows_cut(&mut original).await.unwrap();
    let mut closing = fixture.closing(original);
    let identity = Arc::new(());
    let payload = {
        let original_identity = Arc::clone(&identity);
        let work = tokio::time::timeout(crate::relay_result_writer::CLOSING_DEADLINE, async {
            fixture.hook.scope.observe_installed_closing(
                fixture.hook.statement,
                actual,
                &closing,
            )?;
            let panic_future = async move {
                std::panic::panic_any(Canary(original_identity));
                #[allow(unreachable_code)]
                Ok::<(), io::Error>(())
            };
            tokio::pin!(panic_future);
            paired_finish(&fixture.hook.scope, panic_future).await
        });
        tokio::pin!(work);
        std::future::poll_fn(|cx| {
            Poll::Ready(std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || work.as_mut().poll(cx),
            )))
        })
        .await
        .unwrap_err()
    }; // The original panic is still owned, and the poisoned async future is never repolled.
    assert!(Arc::ptr_eq(
        &payload.downcast_ref::<Canary>().unwrap().0,
        &identity
    ));
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().failure,
        Some(Failure::Panic)
    );
    assert!(fixture.controller.release(0).is_err());
    let _ = fixture.owner.core.fail(Failure::Deadline);
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().failure,
        Some(Failure::Panic)
    );
    assert!(
        !fixture
            .hook
            .scope
            .core
            .state(0)
            .unwrap()
            .closing_poll_active
    );
    assert_eq!(fixture.capacity.snapshot().held_positions, [0, 0, 0, 1]);
    assert!(
        !fixture
            .hook
            .scope
            .snapshot()
            .unwrap()
            .writer_destructor_returned
    );
    drop(closing);
    assert_eq!(fixture.capacity.snapshot().held_positions, [0; 4]);
    assert!(
        fixture
            .hook
            .scope
            .snapshot()
            .unwrap()
            .writer_destructor_returned
    );
    fixture.controller.stop();
    assert!(fixture.controller.inspect_after_original_join().is_err());
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().failure,
        Some(Failure::Panic)
    );
}

struct ShutdownCount(Arc<AtomicUsize>);
impl AsyncWrite for ShutdownCount {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(bytes.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.fetch_add(1, Ordering::AcqRel);
        Poll::Ready(Ok(()))
    }
}
#[tokio::test]
async fn mismatched_connection_fails_attachment_but_actual_shutdown_still_passes() {
    let fixture = Fixture::new().await;
    let count = Arc::new(AtomicUsize::new(0));
    let mut wrong = InitiallyRawPressureWriter::new(
        ShutdownCount(Arc::clone(&count)),
        ClientConnectionToken::new(100, 60).unwrap(),
        Arc::clone(&fixture.owner),
    );
    assert!(wrong.flush().await.is_err());
    assert!(!fixture.hook.scope.snapshot().unwrap().writer_attached);
    wrong.shutdown().await.unwrap();
    assert_eq!(count.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture.hook.scope.snapshot().unwrap().failure,
        Some(Failure::Identity)
    );
}

#[derive(Debug)]
struct Canary(Arc<()>);
impl std::fmt::Display for Canary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("secret source canary")
    }
}
impl std::error::Error for Canary {}
#[test]
fn refusal_primary_and_secondary_original_sources_survive_finite_presentation() {
    let a = Arc::new(());
    let b = Arc::new(());
    let combined = preserve_refusal_hook_failure(
        io::Error::new(io::ErrorKind::BrokenPipe, Canary(Arc::clone(&a))),
        io::Error::new(io::ErrorKind::InvalidData, Canary(Arc::clone(&b))),
    );
    let owned = combined
        .get_ref()
        .unwrap()
        .downcast_ref::<RefusalHookFailure>()
        .unwrap();
    assert!(Arc::ptr_eq(
        &owned
            .original
            .get_ref()
            .unwrap()
            .downcast_ref::<Canary>()
            .unwrap()
            .0,
        &a
    ));
    assert!(Arc::ptr_eq(
        &owned
            .hook
            .get_ref()
            .unwrap()
            .downcast_ref::<Canary>()
            .unwrap()
            .0,
        &b
    ));
    assert!(!format!("{combined:?} {combined}").contains("secret source canary"));
}
