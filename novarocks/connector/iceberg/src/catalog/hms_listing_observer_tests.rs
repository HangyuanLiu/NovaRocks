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

// Feature-only components. No stock service, Native or RPC-exit claims.
use super::*;
use crate::catalog::hms_listing_observer::{ExitSelection, HmsListingOperation, sdk_call};
use novarocks_spi::connector::ConnectorStopOwner;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

fn context(stop: &ConnectorStopOwner, deadline: Instant) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(deadline, stop.view(), 1024, 4096).unwrap()
}

struct ActualFuture {
    positions: Arc<Semaphore>,
    drops: Arc<AtomicUsize>,
    ready: bool,
}
impl Future for ActualFuture {
    type Output = Result<(), ConnectorError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if self.ready {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
impl Drop for ActualFuture {
    fn drop(&mut self) {
        assert!(
            self.positions.available_permits() < LISTING_CONCURRENCY,
            "the actual future destructor must still hold its own position"
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
fn actual(gate: &ListingAdmission, drops: &Arc<AtomicUsize>, ready: bool) -> ActualFuture {
    ActualFuture {
        positions: gate.positions.clone(),
        drops: drops.clone(),
        ready,
    }
}
fn settled(record: crate::catalog::hms_listing_observer::InvocationRecord) {
    assert!(record.started < record.acquired);
    assert!(record.acquired < record.sdk_created);
    assert!(record.sdk_created < record.sdk_first_poll);
    assert!(record.sdk_first_poll < record.sdk_dropped);
    assert!(record.sdk_dropped < record.wrapper_dropped);
    assert!(record.wrapper_dropped < record.permit_returned);
    assert!(record.permit_returned < record.settled);
}

#[tokio::test]
async fn actual_ready_future_drops_before_wrapper_and_permit_return() {
    let gate = ListingAdmission::default();
    let stop = ConnectorStopOwner::new();
    let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
    let drops = Arc::new(AtomicUsize::new(0));
    gate.run_hms(&ctx, HmsListingOperation::Tables, Some([7; 32]), |invoke| {
        let gate = &gate;
        let drops = &drops;
        async move { sdk_call(Some(&invoke), actual(gate, drops, true)).await }
    })
    .await
    .unwrap();
    let snapshot = gate.hms_snapshot().unwrap();
    assert_eq!(snapshot.used, 1);
    assert_eq!(snapshot.available_positions_sample, Some(8));
    assert_eq!(snapshot.sdk_objects_live, 0);
    assert_eq!(snapshot.admitted_wrappers_live, 0);
    assert_eq!(snapshot.invocations_in_flight, 0);
    let record = snapshot.records[0].unwrap();
    assert_eq!(record.target_sha256, Some([7; 32]));
    assert_eq!(record.original_deadline, ctx.deadline());
    assert_eq!(record.selection, ExitSelection::ReadyOk);
    assert!(record.sdk_ready > record.sdk_first_poll && record.sdk_ready < record.sdk_dropped);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    settled(record);
}

#[tokio::test]
async fn eight_real_objects_ninth_waiting_and_same_domain_reuse_without_tasks() {
    let gate = ListingAdmission::default();
    let stop = ConnectorStopOwner::new();
    let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut calls = Vec::new();
    for _ in 0..9 {
        let call = gate.run_hms(&ctx, HmsListingOperation::Tables, Some([3; 32]), |invoke| {
            let gate = &gate;
            let drops = &drops;
            async move { sdk_call(Some(&invoke), actual(gate, drops, false)).await }
        });
        calls.push(Box::pin(call));
    }
    for call in &mut calls {
        assert!(futures::poll!(call.as_mut()).is_pending());
    }
    let before = gate.hms_snapshot().unwrap();
    assert_eq!(before.used, 9);
    assert_eq!(before.sdk_objects_live, 8);
    assert_eq!(before.peak_sdk_objects_live, 8);
    assert_eq!(before.peak_admitted_wrappers_live, 8);
    assert_eq!(before.available_positions_sample, Some(0));
    assert_eq!(before.records[8].unwrap().acquired, 0);
    assert_eq!(before.records[8].unwrap().sdk_created, 0);
    assert!(
        gate.reset_hms_observation_idle(before.domain, before.phase, before.sequence)
            .is_err()
    );
    stop.request_stop();
    for call in &mut calls {
        assert_eq!(
            call.await.as_ref().unwrap_err().kind(),
            ConnectorErrorKind::Cancelled
        );
    }
    drop(calls);
    let after = gate.hms_snapshot().unwrap();
    assert_eq!(after.domain, before.domain);
    assert_eq!(after.available_positions_sample, Some(8));
    assert_eq!(after.invocations_in_flight, 0);
    assert_eq!(after.sdk_objects_live, 0);
    for record in after.records[..8].iter().flatten() {
        settled(*record);
    }
    let waiting = after.records[8].unwrap();
    assert_eq!(waiting.selection, ExitSelection::StopWaiting);
    assert_eq!(waiting.sdk_created, 0);
    assert_eq!(waiting.sdk_dropped, 0);
    assert_eq!(waiting.permit_returned, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 8);
    // The original snapshot keeps its own active history after live state exits.
    assert_eq!(before.records.len(), 1024);
    assert_eq!(before.records[0].unwrap().sdk_dropped, 0);
    assert_ne!(after.records[0].unwrap().sdk_dropped, 0);
    assert!(
        gate.reset_hms_observation_idle(uuid::Uuid::nil(), after.phase, after.sequence)
            .is_err()
    );
    gate.reset_hms_observation_idle(after.domain, after.phase, after.sequence)
        .unwrap();
    let healthy = ConnectorStopOwner::new();
    let ctx = context(&healthy, ctx.deadline());
    gate.run_hms(&ctx, HmsListingOperation::Tables, None, |invoke| {
        let gate = &gate;
        let drops = &drops;
        async move { sdk_call(Some(&invoke), actual(gate, drops, true)).await }
    })
    .await
    .unwrap();
    let reused = gate.hms_snapshot().unwrap();
    assert_eq!(reused.domain, after.domain);
    assert!(reused.phase > after.phase);
    assert!(reused.records[0].unwrap().ordinal > waiting.ordinal);
    assert_eq!(reused.records.len(), 1024);
    assert_eq!(after.used, 9);
    assert_eq!(after.records[8].unwrap().ordinal, waiting.ordinal);
    settled(reused.records[0].unwrap());
}

#[tokio::test]
async fn original_deadline_drops_pending_sdk_object_before_return() {
    let gate = ListingAdmission::default();
    let stop = ConnectorStopOwner::new();
    let ctx = context(&stop, Instant::now() + Duration::from_millis(100));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut call = Box::pin(
        gate.run_hms(&ctx, HmsListingOperation::Namespaces, None, |invoke| {
            let gate = &gate;
            let drops = &drops;
            async move { sdk_call(Some(&invoke), actual(gate, drops, false)).await }
        }),
    );
    assert!(futures::poll!(call.as_mut()).is_pending());
    let active = gate.hms_snapshot().unwrap();
    let active_record = active.records[0].unwrap();
    assert_ne!(active_record.acquired, 0);
    assert_ne!(active_record.sdk_created, 0);
    assert_ne!(active_record.sdk_first_poll, 0);
    assert_eq!(active_record.sdk_dropped, 0);
    let result = call.as_mut().await;
    drop(call);
    assert_eq!(
        result.unwrap_err().kind(),
        ConnectorErrorKind::DeadlineExceeded
    );
    let snapshot = gate.hms_snapshot().unwrap();
    let record = snapshot.records[0].unwrap();
    assert_eq!(record.selection, ExitSelection::DeadlineAdmitted);
    assert_eq!(record.original_deadline, ctx.deadline());
    assert_eq!(active_record.original_deadline, record.original_deadline);
    assert!(record.deadline_at_selection);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    settled(record);
}

#[tokio::test]
async fn actual_sdk_poll_panic_preserves_payload_returns_permit_and_invalidates_observation() {
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;

    struct PanicFuture {
        _actual: ActualFuture,
        payload: Arc<u8>,
    }
    impl Future for PanicFuture {
        type Output = Result<(), ConnectorError>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            std::panic::panic_any(self.payload.clone());
        }
    }

    let gate = ListingAdmission::default();
    let stop = ConnectorStopOwner::new();
    let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
    let drops = Arc::new(AtomicUsize::new(0));
    let payload = Arc::new(17_u8);
    let panic = AssertUnwindSafe(
        gate.run_hms(&ctx, HmsListingOperation::Tables, None, |invoke| {
            let gate = &gate;
            let drops = &drops;
            let payload = &payload;
            async move {
                sdk_call(
                    Some(&invoke),
                    PanicFuture {
                        _actual: actual(gate, drops, false),
                        payload: payload.clone(),
                    },
                )
                .await
            }
        }),
    )
    .catch_unwind()
    .await
    .unwrap_err();
    let original = panic.downcast::<Arc<u8>>().unwrap();
    assert!(Arc::ptr_eq(&payload, &original));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(gate.positions.available_permits(), 8);
    assert!(gate.hms_snapshot().is_err());
    // Observation failure does not replace a later original business outcome.
    gate.run_hms(&ctx, HmsListingOperation::Tables, None, |invoke| {
        let gate = &gate;
        let drops = &drops;
        async move { sdk_call(Some(&invoke), actual(gate, drops, true)).await }
    })
    .await
    .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(gate.hms_snapshot().is_err());
}

#[tokio::test]
async fn both_ready_waiting_stop_keeps_original_biased_selection_and_never_calls_sdk() {
    let gate = ListingAdmission::default();
    let occupied = gate.positions.clone().acquire_many_owned(8).await.unwrap();
    let stop = ConnectorStopOwner::new();
    let ctx = context(&stop, Instant::now() + Duration::from_millis(20));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut call =
        Box::pin(
            gate.run_hms(&ctx, HmsListingOperation::Views, Some([9; 32]), |invoke| {
                let gate = &gate;
                let drops = &drops;
                async move { sdk_call(Some(&invoke), actual(gate, drops, false)).await }
            }),
        );
    assert!(futures::poll!(call.as_mut()).is_pending());
    tokio::time::sleep(Duration::from_millis(25)).await;
    stop.request_stop();
    assert_eq!(
        call.as_mut().await.unwrap_err().kind(),
        ConnectorErrorKind::Cancelled
    );
    drop(call);
    let record = gate.hms_snapshot().unwrap().records[0].unwrap();
    assert_eq!(record.selection, ExitSelection::StopWaiting);
    assert!(record.stop_at_selection && record.deadline_at_selection);
    assert_eq!(record.acquired, 0);
    assert_eq!(record.sdk_created, 0);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(occupied);
}

#[tokio::test]
async fn dropping_the_outer_future_still_drops_the_actual_sdk_before_permit() {
    let gate = ListingAdmission::default();
    let stop = ConnectorStopOwner::new();
    let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
    let drops = Arc::new(AtomicUsize::new(0));
    let mut call = Box::pin(
        gate.run_hms(&ctx, HmsListingOperation::Tables, None, |invoke| {
            let gate = &gate;
            let drops = &drops;
            async move { sdk_call(Some(&invoke), actual(gate, drops, false)).await }
        }),
    );
    assert!(futures::poll!(call.as_mut()).is_pending());
    drop(call);
    let snapshot = gate.hms_snapshot().unwrap();
    let record = snapshot.records[0].unwrap();
    assert_eq!(record.selection, ExitSelection::OwnerDropped);
    assert_eq!(snapshot.available_positions_sample, Some(8));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    settled(record);
}

#[tokio::test]
async fn ready_error_and_unsupported_are_original_failures_not_synthetic_success() {
    let gate = ListingAdmission::default();
    let stop = ConnectorStopOwner::new();
    let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
    let result: Result<(), ConnectorError> = gate
        .run_hms(
            &ctx,
            HmsListingOperation::Views,
            None,
            |invoke| async move {
                sdk_call(Some(&invoke), async {
                    Err(ConnectorError::new(
                        ConnectorErrorKind::Unsupported,
                        "component source canary",
                    ))
                })
                .await
            },
        )
        .await;
    let error = result.unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Unsupported);
    assert!(error.to_string().contains("component source canary"));
    let record = gate.hms_snapshot().unwrap().records[0].unwrap();
    assert_eq!(record.selection, ExitSelection::ReadyErr);
    settled(record);
}

#[tokio::test]
async fn journal_full_cannot_overwrite_history_or_be_reset_into_valid_evidence() {
    let gate = ListingAdmission::default();
    let stop = ConnectorStopOwner::new();
    let ctx = context(&stop, Instant::now() + Duration::from_secs(60));
    for _ in 0..crate::catalog::hms_listing_observer::JOURNAL_CAPACITY {
        gate.run_hms(
            &ctx,
            HmsListingOperation::Tables,
            None,
            |invoke| async move { sdk_call(Some(&invoke), async { Ok(()) }).await },
        )
        .await
        .unwrap();
    }
    let full = gate.hms_snapshot().unwrap();
    assert_eq!(full.used, 1024);
    gate.run_hms(
        &ctx,
        HmsListingOperation::Tables,
        None,
        |invoke| async move { sdk_call(Some(&invoke), async { Ok(()) }).await },
    )
    .await
    .unwrap();
    assert!(gate.hms_snapshot().is_err());
    assert!(
        gate.reset_hms_observation_idle(full.domain, full.phase, full.sequence)
            .is_err()
    );
    assert_eq!(gate.positions.available_permits(), 8);
}

#[tokio::test]
async fn ordinary_run_cannot_be_mislabelled_as_an_hms_sdk_invocation() {
    let gate = ListingAdmission::default();
    let stop = ConnectorStopOwner::new();
    let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
    gate.run(&ctx, async { Ok(()) }).await.unwrap();
    let snapshot = gate.hms_snapshot().unwrap();
    assert_eq!(snapshot.used, 0);
    assert_eq!(snapshot.sdk_objects_live, 0);
    assert_eq!(snapshot.available_positions_sample, Some(8));
}

#[tokio::test]
async fn rejection_before_sdk_construction_cannot_claim_sdk_drop() {
    let gate = ListingAdmission::default();
    let stop = ConnectorStopOwner::new();
    let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
    let result: Result<(), ConnectorError> = gate
        .run_hms(&ctx, HmsListingOperation::Tables, None, |_invoke| async {
            Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "component pre-SDK refusal",
            ))
        })
        .await;
    assert_eq!(
        result.unwrap_err().kind(),
        ConnectorErrorKind::InvalidRequest
    );
    let snapshot = gate.hms_snapshot().unwrap();
    let record = snapshot.records[0].unwrap();
    assert_ne!(record.acquired, 0);
    assert_eq!(record.sdk_created, 0);
    assert_eq!(record.sdk_first_poll, 0);
    assert_eq!(record.sdk_dropped, 0);
    assert!(record.wrapper_dropped < record.permit_returned);
    assert_eq!(snapshot.available_positions_sample, Some(8));
}
