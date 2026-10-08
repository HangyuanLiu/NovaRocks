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

//! Catalog-generation admission for trusted external SDK listings.

use std::future::Future;
use std::sync::Arc;

use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorOperationControl, ConnectorRequestContext,
};
use tokio::sync::Semaphore;

pub(crate) const LISTING_CONCURRENCY: usize = 8;

#[derive(Debug)]
pub(crate) struct ListingAdmission {
    positions: Arc<Semaphore>,
}

impl Default for ListingAdmission {
    fn default() -> Self {
        Self {
            positions: Arc::new(Semaphore::new(LISTING_CONCURRENCY)),
        }
    }
}

impl ListingAdmission {
    #[cfg(test)]
    pub(crate) fn available_positions(&self) -> usize {
        self.positions.available_permits()
    }

    pub(crate) async fn run<T>(
        &self,
        context: &ConnectorRequestContext,
        call: impl Future<Output = Result<T, ConnectorError>>,
    ) -> Result<T, ConnectorError> {
        context.check_active()?;
        let deadline = tokio::time::Instant::from_std(context.deadline());
        let permit = tokio::select! {
            biased;
            _ = context.stop().stopped() => return Err(cancelled()),
            _ = tokio::time::sleep_until(deadline) => return Err(expired()),
            permit = self.positions.clone().acquire_owned() => permit.map_err(|_| ConnectorError::new(ConnectorErrorKind::Internal, "catalog listing admission was closed"))?,
        };
        let result = {
            // This scope destroys the SDK future before its position can return.
            // A timeout or stop is not itself evidence that the future exited.
            tokio::pin!(call);
            tokio::select! {
                biased;
                _ = context.stop().stopped() => Err(cancelled()),
                _ = tokio::time::sleep_until(deadline) => Err(expired()),
                result = &mut call => result,
            }
        };
        drop(permit);
        result
    }
}

fn cancelled() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::Cancelled,
        "catalog listing was cancelled",
    )
}
fn expired() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::DeadlineExceeded,
        "catalog listing absolute deadline elapsed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorStopOwner;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    fn context(stop: &ConnectorStopOwner, deadline: Instant) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(deadline, stop.view(), 1024, 4096).unwrap()
    }

    struct PendingCall {
        gate: Arc<Semaphore>,
        dropped: Arc<AtomicBool>,
    }
    impl Future for PendingCall {
        type Output = Result<(), ConnectorError>;
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::task::Poll::Pending
        }
    }
    impl Drop for PendingCall {
        fn drop(&mut self) {
            assert_eq!(
                self.gate.available_permits(),
                0,
                "SDK future must exit before its admission position returns"
            );
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn deadline_drops_sdk_before_returning_its_position() {
        let gate = ListingAdmission {
            positions: Arc::new(Semaphore::new(1)),
        };
        let stop = ConnectorStopOwner::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let call = PendingCall {
            gate: gate.positions.clone(),
            dropped: dropped.clone(),
        };
        let error = gate
            .run(
                &context(&stop, Instant::now() + Duration::from_millis(20)),
                call,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(gate.positions.available_permits(), 1);
    }

    #[tokio::test]
    async fn stop_drops_sdk_before_returning_its_position() {
        let gate = ListingAdmission {
            positions: Arc::new(Semaphore::new(1)),
        };
        let stop = ConnectorStopOwner::new();
        let ctx = context(&stop, Instant::now() + Duration::from_secs(5));
        let dropped = Arc::new(AtomicBool::new(false));
        let call = PendingCall {
            gate: gate.positions.clone(),
            dropped: dropped.clone(),
        };
        let outcome = gate.run(&ctx, call);
        let cancel = async {
            tokio::task::yield_now().await;
            stop.request_stop();
        };
        let (error, _) = tokio::join!(outcome, cancel);
        assert_eq!(error.unwrap_err().kind(), ConnectorErrorKind::Cancelled);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(gate.positions.available_permits(), 1);
    }

    #[tokio::test]
    async fn full_admission_never_polls_another_sdk_call() {
        let gate = ListingAdmission::default();
        let occupied = gate
            .positions
            .clone()
            .acquire_many_owned(LISTING_CONCURRENCY as u32)
            .await
            .unwrap();
        let stop = ConnectorStopOwner::new();
        let polled = Arc::new(AtomicBool::new(false));
        let marker = polled.clone();
        let call = async move {
            marker.store(true, Ordering::SeqCst);
            Ok(())
        };
        let error = gate
            .run(
                &context(&stop, Instant::now() + Duration::from_millis(20)),
                call,
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::DeadlineExceeded);
        assert!(!polled.load(Ordering::SeqCst));
        drop(occupied);
        gate.run(
            &context(&stop, Instant::now() + Duration::from_secs(5)),
            async { Ok(()) },
        )
        .await
        .unwrap();
        assert_eq!(gate.positions.available_permits(), LISTING_CONCURRENCY);
    }
}
