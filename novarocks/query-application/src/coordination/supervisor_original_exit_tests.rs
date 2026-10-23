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

// Included in the original supervisor tests; no synthetic completion packet.

struct OriginalOpenPanic {
    dropped: Arc<AtomicU64>,
}
impl Drop for OriginalOpenPanic {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct OriginalOpenPanicPort {
    dropped: Arc<AtomicU64>,
}
impl LogicalExecutionNativePort for OriginalOpenPanicPort {
    fn open(&self, _request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
        let dropped = Arc::clone(&self.dropped);
        Box::pin(async move { std::panic::panic_any(OriginalOpenPanic { dropped }) })
    }
}
struct OriginalExitPanicWake;
impl std::task::Wake for OriginalExitPanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(OriginalExitWakePanic);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(OriginalExitWakePanic);
    }
}
struct OriginalExitWakePanic;

#[tokio::test]
async fn observed_unissued_start_keeps_ready_exit_and_original_footer_separate() {
    let (control, root) = governance().await;
    let mut control = {
        let retirement = root.owner.retirement_observer().unwrap();
        let (mut bank, shutdown, mut registry, client) =
            unpolled_test_driver(&control, Arc::new(PanickingOpenNativePort));
        let mut input = match client
            .prepare_observed_start(completion_request(UnreachablePreparationPort), None)
        {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(root.owner);
        let mut ready = None;
        let mut exit = None;
        let tail = client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap();
        assert!(tail.result().is_ok());
        assert!(input.is_none() && owner.is_none() && ready.is_some() && exit.is_some());
        tail.finish().unwrap();
        let mut observer = exit.take().unwrap();
        let mut wait = Box::pin(std::future::poll_fn(|cx| observer.poll_exit(cx)));
        assert!(futures::poll!(&mut wait).is_pending());
        drop(wait);
        shutdown.send_replace(true);
        bank.stop_ingress();
        let completion = bank.progress_turn(NonZeroUsize::new(7).unwrap()).unwrap();
        observe_logical_completion(&registry.handle(), &mut bank, completion, &mut None);
        let error = ready
            .take()
            .unwrap()
            .await
            .err()
            .expect("unissued shutdown rejects start");
        assert_eq!(error.kind(), QueryExecutionErrorKind::Rejected);
        let receipt = std::future::poll_fn(|cx| observer.poll_exit(cx))
            .await
            .unwrap();
        assert_eq!(
            receipt.local_disposition(),
            crate::api::LocalExecutionDisposition::ActualSourcesExited
        );
        assert_eq!(
            receipt.remote_disposition(),
            crate::api::RemoteExecutionDisposition::RemoteNotReported
        );
        assert_eq!(
            receipt.view().actor_phase(),
            crate::api::LogicalActorExitPhase::Unreserved
        );
        root.business.release();
        drop(bank);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        acknowledge_all_control(&control);
        assert!(
            !retirement.is_reclaimed().unwrap(),
            "receipt still owns the exact original footer"
        );
        drop(receipt);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !retirement.is_reclaimed().unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn observed_real_open_panic_survives_cancelled_wait_until_original_health_disposes() {
    let (control, root) = governance().await;
    let mut control = {
        let dropped = Arc::new(AtomicU64::new(0));
        let (mut bank, _shutdown, mut registry, client) = unpolled_test_driver(
            &control,
            Arc::new(OriginalOpenPanicPort {
                dropped: Arc::clone(&dropped),
            }),
        );
        let mut input = match client
            .prepare_observed_start(completion_request(UnreachablePreparationPort), None)
        {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(root.owner);
        let mut ready = None;
        let mut exit = None;
        client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap()
            .finish()
            .unwrap();
        let mut observer = exit.take().unwrap();
        let mut wait = Box::pin(std::future::poll_fn(|cx| observer.poll_exit(cx)));
        assert!(futures::poll!(&mut wait).is_pending());
        drop(wait);
        let completion = bank.progress_turn(NonZeroUsize::new(7).unwrap()).unwrap();
        observe_logical_completion(&registry.handle(), &mut bank, completion, &mut None);
        assert!(ready.take().unwrap().await.is_err());
        {
            let view = observer.view().unwrap();
            assert_eq!(
                view.actor_phase(),
                crate::api::LogicalActorExitPhase::Unreserved
            );
            assert!(view.native_action_started());
            match view.role_result().unwrap() {
                PollCompletion::Panicked(packet) => {
                    assert!(packet.value().primary().is::<OriginalOpenPanic>())
                }
                _ => panic!("actual Native open panic was lost"),
            }
        }
        assert!(
            futures::poll!(std::future::poll_fn(|cx| observer.poll_exit(cx))).is_pending(),
            "Native session return is not fabricated from no actor"
        );
        drop(observer);
        root.business.release();
        drop(bank);
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            0,
            "original health still retains original payload"
        );
        registry.abandon_for_process_exit();
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "explicit original health disposal destroys the actual payload"
        );
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[tokio::test]
async fn rejected_late_health_completion_and_callback_panic_return_the_same_bank_on_original_join()
{
    let control = governance_empty();
    let mut control = {
        let first = control.begin_root().await;
        let second = control.begin_root().await;
        let dropped = Arc::new(AtomicU64::new(0));
        let (mut bank, shutdown, mut registry, client) = unpolled_test_driver(
            &control,
            Arc::new(OriginalOpenPanicPort {
                dropped: Arc::clone(&dropped),
            }),
        );
        let first_ready = client.start(completion_request(UnreachablePreparationPort), first.owner);
        let completion = bank.progress_turn(NonZeroUsize::new(7).unwrap()).unwrap();
        observe_logical_completion(&registry.handle(), &mut bank, completion, &mut None);
        assert!(first_ready.await.is_err());
        let mut input = match client
            .prepare_observed_start(completion_request(UnreachablePreparationPort), None)
        {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(second.owner);
        let mut ready = None;
        let mut exit = None;
        client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap()
            .finish()
            .unwrap();
        let mut observer = exit.take().unwrap();
        let wake = std::task::Waker::from(Arc::new(OriginalExitPanicWake));
        let mut cx = std::task::Context::from_waker(&wake);
        assert!(observer.poll_exit(&mut cx).is_pending());
        let root_retirement = OriginalRootPanic::empty();
        let mut original_join = Handle::current().spawn(run_supervisor_owned(
            registry.handle(),
            bank,
            NonZeroUsize::new(7).unwrap(),
            shutdown.subscribe(),
            root_retirement.clone(),
        ));
        let outcome = tokio::time::timeout(Duration::from_secs(2), &mut original_join)
            .await
            .expect("the same root actually returns its failure custody")
            .unwrap();
        let original = match outcome {
            OriginalRootOutcome::Panicked(original) => original,
            OriginalRootOutcome::Completed(_) => panic!("actual registered callback must panic"),
        };
        drop(root_retirement);
        assert!(
            original
                .custody()
                .primary
                .as_ref()
                .unwrap()
                .is::<OriginalExitWakePanic>()
        );
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            0,
            "both actual role causes are still retained"
        );
        assert!(ready.take().unwrap().await.is_err());
        assert!(
            observer
                .poll_exit(&mut std::task::Context::from_waker(
                    futures::task::noop_waker_ref()
                ))
                .is_pending()
        );
        assert!(
            observer.view().unwrap().role_result().is_none(),
            "rejected whole packet remains in the original Position"
        );
        drop(observer);
        first.business.release();
        second.business.release();
        registry
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        registry.abandon_for_process_exit();
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "first original health explicitly disposed only its own source"
        );
        // Irrevocable process-exit disposition of the SAME actual RootFailure.
        original.dispose_for_process_exit();
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            2,
            "returned bank still held the rejected whole source"
        );
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

struct OriginalReplyPanicOnce(std::sync::atomic::AtomicBool);
impl std::task::Wake for OriginalReplyPanicOnce {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self.0.swap(false, Ordering::SeqCst) {
            std::panic::panic_any(OriginalExitWakePanic);
        }
    }
}

#[tokio::test]
async fn original_root_joins_installed_actor_after_actual_reply_wake_panics_and_keeps_serving() {
    let (control, first) = governance().await;
    let mut control = {
        let frontend = FrontendProcessId::new_v7();
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(BindingNativePort),
            QueryProcessNamespace::new(0x7d),
            frontend,
            supervisor_config(),
        );
        let mut input = match client.prepare_observed_start(
            completion_request(PanickingAttemptPreparationPort {
                backend: BackendProcessId::new_v7(),
                phase: AttemptPanicPhase::Activate,
                convergence_calls: Arc::new(AtomicU64::new(0)),
            }),
            None,
        ) {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(first.owner);
        let mut ready = None;
        let mut exit = None;
        let tail = client
            .start_observed_in(&mut input, &mut owner, &mut ready, &mut exit)
            .unwrap();
        let wake = std::task::Waker::from(Arc::new(OriginalReplyPanicOnce(
            std::sync::atomic::AtomicBool::new(true),
        )));
        let mut cx = std::task::Context::from_waker(&wake);
        assert!(ready.as_mut().unwrap().as_mut().poll(&mut cx).is_pending());
        tail.finish().unwrap();
        let mut observer = exit.take().unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while observer.view().unwrap().actor_phase()
                != crate::api::LogicalActorExitPhase::Joined
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the same original root continues exact actor close/join without process shutdown");
        assert!(ready.take().unwrap().await.is_err());
        {
            let view = observer.view().unwrap();
            assert_eq!(
                view.pending_native_returns(),
                0,
                "unused original reservation returned exactly once"
            );
            assert!(matches!(view.actor_result(), Some(Ok(()))));
            match view.role_result().unwrap() {
                PollCompletion::Panicked(packet) => {
                    assert!(packet.value().primary().is::<OriginalExitWakePanic>())
                }
                _ => panic!("actual reply wake panic was not retained"),
            }
        }
        assert!(futures::poll!(std::future::poll_fn(|cx| observer.poll_exit(cx))).is_pending());
        let second = control.begin_root().await;
        let second_handle = tokio::time::timeout(
            Duration::from_secs(2),
            client.start(
                completion_request(SuccessfulCompletionPreparationPort {
                    backend: BackendProcessId::new_v7(),
                    expected_frontend: frontend,
                }),
                second.owner,
            ),
        )
        .await
        .expect("healthy peer remains served by the same root")
        .unwrap();
        drop(second_handle);
        first.business.release();
        second.business.release();
        let shutdown = supervisor
            .shutdown_until(Instant::now() + Duration::from_secs(2))
            .await;
        let actual_remaining = (
            supervisor.registry.handle().statement_native_count(),
            supervisor.registry.handle().root_ack_tail_count(),
            supervisor.registry.handle().original_exit_pending(),
            supervisor.join.as_ref().map(tokio::task::JoinHandle::is_finished),
            supervisor.original_root_panic().is_some(),
        );
        // Dispose through the real process owner before asserting the health
        // classification. A failed assertion must not strand a live root.
        supervisor.abandon_for_process_exit();
        drop(observer);
        assert!(
            matches!(
                shutdown,
                Err(LogicalExecutionSupervisorShutdownError::Registry(
                    LogicalExecutionRuntimeRegistryError::OriginalExitFailureUnresolved
                ))
            ),
            "actual shutdown must retain the original exit failure: {shutdown:?}; remaining native/ack/pending/join/root-failure: {actual_remaining:?}"
        );
        control
    };
    acknowledge_all_control(&control);
    control.finish().await;
}

#[derive(Debug)]
struct OriginalAbortPendingOpen {
    entered: Arc<tokio::sync::Notify>,
}
impl LogicalExecutionNativePort for OriginalAbortPendingOpen {
    fn open(&self, request: LogicalNativeOpenRequest) -> LogicalNativeOpenFuture {
        let entered = Arc::clone(&self.entered);
        Box::pin(async move {
            entered.notify_one();
            std::future::pending::<()>().await;
            request.bind().map_err(Into::into)
        })
    }
}

#[tokio::test(flavor = "current_thread")]
async fn actual_root_abort_closes_ingress_before_first_poll_and_after_real_open_pending() {
    for after_open in [false, true] {
        let control = governance_empty();
        let first = control.begin_root().await;
        let second = control.begin_root().await;
        let first_retirement = first.owner.retirement_observer().unwrap();
        let dropped = Arc::new(AtomicU64::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let (mut supervisor, client) = test_supervisor(
            &control,
            Handle::current(),
            Arc::new(OriginalAbortPendingOpen {
                entered: Arc::clone(&entered),
            }),
            QueryProcessNamespace::new(0x8e),
            FrontendProcessId::new_v7(),
            supervisor_config(),
        );
        // No await between spawning this original root and aborting it in the
        // first branch: current_thread proves its body has never been polled.
        let first_ready = client.start(drop_tracked_request(&dropped), first.owner);
        if after_open {
            tokio::time::timeout(Duration::from_secs(2), entered.notified())
                .await
                .expect("actual original open was entered");
        }
        supervisor.join.as_ref().unwrap().abort();
        let joined =
            tokio::time::timeout(Duration::from_secs(2), supervisor.join.as_mut().unwrap())
                .await
                .expect("same original root JoinHandle must actually exit");
        let raw = match joined {
            Err(raw) => raw,
            Ok(_) => panic!("the actual root was aborted before normal completion"),
        };
        let was_cancelled = raw.is_cancelled();
        let outcome = supervisor.receive_original_root_join_failure(raw);
        supervisor.run_outcome = Some(outcome);
        supervisor.join.take();
        let shutdown_still_open = !*supervisor.shutdown.borrow();

        let mut input = match client
            .prepare_observed_start(completion_request(UnreachablePreparationPort), None)
        {
            Ok(input) => Some(input),
            Err(original) => std::panic::panic_any(original),
        };
        let mut owner = Some(second.owner);
        let mut ready = None;
        let mut exit = None;
        let attempted = client.start_observed_in(&mut input, &mut owner, &mut ready, &mut exit);
        let refused = match attempted {
            Ok(attempt) => attempt.finish().is_err(),
            Err(_) => true,
        };
        let sources_preserved =
            input.is_some() && owner.is_some() && ready.is_none() && exit.is_none();
        let original_input_retained = dropped.load(Ordering::SeqCst) == 0;
        let not_reclaimed = !first_retirement.is_reclaimed().unwrap();
        let cancelled_bank_retained = supervisor
            .original_root_panic()
            .is_some_and(|view| view.was_cancelled() && view.primary().is_none());
        let exact_join_retained = supervisor
            .original_root_join_failure()
            .is_some_and(tokio::task::JoinError::is_cancelled);
        let same_raw_join = Arc::ptr_eq(
            supervisor.original_root_failure.as_ref().unwrap(),
            supervisor
                .original_root_panic
                .as_ref()
                .unwrap()
                .shared
                .join_failure
                .get()
                .unwrap(),
        );

        // These are abnormal-root facts, not normal shutdown success. Dispose
        // through the original process-exit author before asserting them.
        drop(input.take());
        owner
            .take()
            .unwrap()
            .complete_after_terminal_cancel_settled();
        first.business.release();
        second.business.release();
        drop(first_ready);
        supervisor.abandon_for_process_exit();
        drop(client);
        let after_disposition = dropped.load(Ordering::SeqCst);
        let mut control = control;
        acknowledge_all_control(&control);
        // Explicit process-exit disposal destroys the unpolled original owner
        // as Orphaned; it does not author normal completion or WC Drained.
        control.finish_for_process_exit().await;
        assert!(was_cancelled && exact_join_retained && same_raw_join);
        assert!(
            shutdown_still_open,
            "the same live Supervisor did not initiate shutdown"
        );
        assert!(
            refused && sources_preserved,
            "a retained but service-less original ingress must refuse without consuming sources"
        );
        assert!(original_input_retained && not_reclaimed && cancelled_bank_retained);
        assert_eq!(
            after_disposition, 1,
            "only explicit original process disposition destroyed the accepted source"
        );
    }
}
