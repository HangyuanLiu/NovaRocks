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

//! Backend role-local async runtime dependencies.
//!
//! The server composition root creates this adapter from the role data-plane
//! runtime and passes clones through every native outbound client.  It keeps
//! the runtime handle and channel cache instance-owned; no Core or process
//! global runtime participates in Backend transport work.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct BackendDataRuntime {
    handle: Handle,
    channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl BackendDataRuntime {
    pub fn new(handle: Handle) -> Self {
        Self {
            handle,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future + Send,
        F::Output: Send,
    {
        if Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.handle.block_on(future))
        } else {
            self.handle.block_on(future)
        }
    }

    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }

    pub(crate) fn channels(&self) -> &Arc<Mutex<HashMap<String, Channel>>> {
        &self.channels
    }
}

#[cfg(test)]
pub(crate) fn test_backend_data_runtime() -> BackendDataRuntime {
    static TEST_RUNTIME: std::sync::LazyLock<(tokio::runtime::Runtime, BackendDataRuntime)> =
        std::sync::LazyLock::new(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(1)
                .build()
                .expect("build Backend test data runtime");
            let adapter = BackendDataRuntime::new(runtime.handle().clone());
            (runtime, adapter)
        });
    TEST_RUNTIME.1.clone()
}

#[cfg(test)]
mod tests {
    use super::BackendDataRuntime;

    #[test]
    fn cloned_adapter_shares_one_role_cache_and_distinct_adapters_do_not() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .expect("build adapter test runtime");
        let first = BackendDataRuntime::new(runtime.handle().clone());
        let clone = first.clone();
        let second = BackendDataRuntime::new(runtime.handle().clone());

        assert!(std::sync::Arc::ptr_eq(first.channels(), clone.channels()));
        assert!(!std::sync::Arc::ptr_eq(first.channels(), second.channels()));
    }

    #[test]
    fn block_on_preserves_external_and_active_runtime_paths() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(1)
            .build()
            .expect("build adapter test runtime");
        let adapter = BackendDataRuntime::new(runtime.handle().clone());

        assert_eq!(adapter.block_on(async { 7_u8 }), 7);
        let active_adapter = adapter.clone();
        runtime.block_on(async move {
            assert_eq!(active_adapter.block_on(async { 11_u8 }), 11);
        });
    }
}
