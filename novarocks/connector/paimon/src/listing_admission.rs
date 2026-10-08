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

use crate::resources::PaimonRequestControl;
use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Per catalog generation; clones share admission across request-local catalogs.
#[derive(Clone, Debug)]
pub(crate) struct ListingAdmission(Arc<Semaphore>);

pub(crate) const LISTING_CONCURRENCY: usize = 8;

impl Default for ListingAdmission {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(LISTING_CONCURRENCY)))
    }
}

impl ListingAdmission {
    pub(crate) async fn acquire(
        &self,
        control: &PaimonRequestControl,
    ) -> Result<OwnedSemaphorePermit, ConnectorError> {
        control
            .until(Arc::clone(&self.0).acquire_owned())
            .await?
            .map_err(|_| {
                ConnectorError::new(
                    ConnectorErrorKind::Internal,
                    "Paimon listing admission closed",
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::ConnectorStopOwner;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    struct PendingSdk(Arc<AtomicBool>);
    impl std::future::Future for PendingSdk {
        type Output = ();
        fn poll(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            std::task::Poll::Pending
        }
    }
    impl Drop for PendingSdk {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn deadline_drops_sdk_before_admission_is_returned() {
        let admission = ListingAdmission::default();
        let stop = Arc::new(ConnectorStopOwner::new());
        let control =
            PaimonRequestControl::new(stop.view(), Instant::now() + Duration::from_millis(20));
        let dropped = Arc::new(AtomicBool::new(false));
        let permit = admission.acquire(&control).await.unwrap();
        assert_eq!(
            control
                .until(PendingSdk(dropped.clone()))
                .await
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::DeadlineExceeded
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(admission.0.available_permits(), LISTING_CONCURRENCY - 1);
        drop(permit);
        assert_eq!(admission.0.available_permits(), LISTING_CONCURRENCY);
    }

    #[tokio::test]
    async fn stop_drops_sdk_and_restores_shared_admission() {
        let admission = ListingAdmission::default();
        let stop = Arc::new(ConnectorStopOwner::new());
        let control =
            PaimonRequestControl::new(stop.view(), Instant::now() + Duration::from_secs(10));
        let dropped = Arc::new(AtomicBool::new(false));
        let permit = admission.acquire(&control).await.unwrap();
        let wait = control.until(PendingSdk(dropped.clone()));
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        stop.request_stop();
        assert_eq!(
            wait.await.unwrap_err().kind(),
            ConnectorErrorKind::Cancelled
        );
        assert!(dropped.load(Ordering::SeqCst));
        drop(permit);
        assert_eq!(admission.0.available_permits(), LISTING_CONCURRENCY);
    }

    #[tokio::test]
    async fn shared_admission_caps_concurrent_calls_and_cancelled_waiter_does_not_borrow() {
        let admission = ListingAdmission::default();
        let stop = Arc::new(ConnectorStopOwner::new());
        let control =
            PaimonRequestControl::new(stop.view(), Instant::now() + Duration::from_secs(10));
        let mut permits = Vec::new();
        for _ in 0..LISTING_CONCURRENCY {
            permits.push(admission.acquire(&control).await.unwrap());
        }
        let other_stop = Arc::new(ConnectorStopOwner::new());
        let other =
            PaimonRequestControl::new(other_stop.view(), Instant::now() + Duration::from_secs(10));
        let clone = admission.clone();
        let waiter = clone.acquire(&other);
        tokio::pin!(waiter);
        assert!(futures::poll!(&mut waiter).is_pending());
        other_stop.request_stop();
        assert_eq!(
            waiter.await.unwrap_err().kind(),
            ConnectorErrorKind::Cancelled
        );
        assert_eq!(admission.0.available_permits(), 0);
        permits.pop();
        let returned = admission.acquire(&control).await.unwrap();
        assert_eq!(admission.0.available_permits(), 0);
        drop(returned);
        drop(permits);
        assert_eq!(admission.0.available_permits(), LISTING_CONCURRENCY);
    }
}
