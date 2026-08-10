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

//! StateStore change-hint controller for local catalog runtime projections.
//!
//! Change pages are intentionally only wakeups. Every relevant hint and every
//! retention gap triggers a complete authoritative attachment reread.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use novarocks_spi::state_store::{ChangeCursor, ChangePollRequest, StateStore, StoreIdentity};
use tokio::task::JoinHandle;

use crate::catalog_application::FrontendCatalogApplicationPort;
use crate::catalog_attachment::attachment_prefix;

#[derive(Clone, Debug)]
pub struct CatalogProjectionConfig {
    pub page_size: usize,
    pub poll_interval: Duration,
    pub freshness_budget: Duration,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub worker_count: usize,
    pub shutdown_deadline: Duration,
}

impl Default for CatalogProjectionConfig {
    fn default() -> Self {
        Self {
            page_size: 256,
            poll_interval: Duration::from_millis(250),
            freshness_budget: Duration::from_secs(30),
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_secs(5),
            worker_count: 8,
            shutdown_deadline: Duration::from_secs(5),
        }
    }
}

pub struct FrontendCatalogController {
    store: Arc<dyn StateStore>,
    projection: Arc<FrontendCatalogApplicationPort>,
    config: CatalogProjectionConfig,
    stopping: AtomicBool,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl FrontendCatalogController {
    pub fn new(
        store: Arc<dyn StateStore>,
        projection: Arc<FrontendCatalogApplicationPort>,
        config: CatalogProjectionConfig,
    ) -> Result<Arc<Self>, String> {
        if config.page_size == 0 || config.page_size > store.limits().max_page_size {
            return Err("catalog controller page size is outside StateStore limits".to_string());
        }
        if config.poll_interval.is_zero()
            || config.freshness_budget.is_zero()
            || config.retry_initial.is_zero()
            || config.retry_max < config.retry_initial
            || config.worker_count == 0
            || config.shutdown_deadline.is_zero()
        {
            return Err(
                "catalog controller config contains an invalid zero or retry bound".to_string(),
            );
        }
        Ok(Arc::new(Self {
            store,
            projection,
            config,
            stopping: AtomicBool::new(false),
            worker: Mutex::new(None),
        }))
    }

    /// Captures a polling HWM before the first authoritative attachment scan.
    pub async fn bootstrap(&self) -> Result<ChangeCursor, String> {
        let identity = self
            .store
            .identity()
            .await
            .map_err(|error| error.to_string())?;
        let page = self
            .store
            .poll_changes(&ChangePollRequest {
                after: None,
                page_size: self.config.page_size,
            })
            .await
            .map_err(|error| error.to_string())?;
        self.projection
            .reconcile_with_page_size(self.config.page_size)
            .await
            .map_err(|error| error.to_string())?;
        page.next_cursor
            .decode(identity.store_id)
            .map_err(|error| error.to_string())?;
        Ok(page.next_cursor)
    }

    pub fn start(self: &Arc<Self>) -> Result<(), String> {
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| "catalog controller worker lock is poisoned".to_string())?;
        if worker.is_some() {
            return Err("catalog controller is already running".to_string());
        }
        self.stopping.store(false, Ordering::Release);
        let controller = Arc::clone(self);
        *worker = Some(tokio::spawn(async move {
            controller.run().await;
        }));
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), String> {
        self.stopping.store(true, Ordering::Release);
        let handle = self
            .worker
            .lock()
            .map_err(|_| "catalog controller worker lock is poisoned".to_string())?
            .take();
        if let Some(mut handle) = handle {
            if tokio::time::timeout(self.config.shutdown_deadline, &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
        self.projection.unpublish_all();
        Ok(())
    }

    async fn run(&self) {
        let mut identity = None;
        let mut cursor = None;
        let mut last_fresh = Instant::now();
        let mut retry = self.config.retry_initial;
        let mut force_resync = true;

        while !self.stopping.load(Ordering::Acquire) {
            let outcome = self
                .poll_once(&mut identity, &mut cursor, &mut force_resync)
                .await;
            match outcome {
                Ok(()) => {
                    last_fresh = Instant::now();
                    retry = self.config.retry_initial;
                    tokio::time::sleep(self.config.poll_interval).await;
                }
                Err(error) => {
                    tracing::warn!(%error, "catalog attachment projection poll failed");
                    if last_fresh.elapsed() >= self.config.freshness_budget {
                        self.projection.unpublish_all();
                        force_resync = true;
                    }
                    tokio::time::sleep(retry).await;
                    retry = retry.saturating_mul(2).min(self.config.retry_max);
                }
            }
        }
    }

    async fn poll_once(
        &self,
        known_identity: &mut Option<StoreIdentity>,
        cursor: &mut Option<ChangeCursor>,
        force_resync: &mut bool,
    ) -> Result<(), String> {
        let identity = self
            .store
            .identity()
            .await
            .map_err(|error| error.to_string())?;
        if known_identity.as_ref() != Some(&identity) {
            *known_identity = Some(identity.clone());
            *cursor = None;
            *force_resync = true;
        }
        let page = self
            .store
            .poll_changes(&ChangePollRequest {
                after: cursor.clone(),
                page_size: self.config.page_size,
            })
            .await
            .map_err(|error| error.to_string())?;
        page.next_cursor
            .decode(identity.store_id)
            .map_err(|error| error.to_string())?;
        *cursor = Some(page.next_cursor);

        let prefix = attachment_prefix()?;
        let relevant = page
            .hints
            .iter()
            .any(|hint| hint.key.as_bytes().starts_with(prefix.as_bytes()));
        if *force_resync || page.resync_required || relevant {
            self.projection
                .reconcile_with_page_size(self.config.page_size)
                .await
                .map_err(|error| error.to_string())?;
            *force_resync = false;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_cp2_operational_contract() {
        let config = CatalogProjectionConfig::default();
        assert_eq!(config.page_size, 256);
        assert_eq!(config.poll_interval, Duration::from_millis(250));
        assert_eq!(config.freshness_budget, Duration::from_secs(30));
        assert_eq!(config.retry_initial, Duration::from_millis(100));
        assert_eq!(config.retry_max, Duration::from_secs(5));
        assert_eq!(config.worker_count, 8);
        assert_eq!(config.shutdown_deadline, Duration::from_secs(5));
    }
}
