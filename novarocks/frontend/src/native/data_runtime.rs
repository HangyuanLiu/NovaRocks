//! FE-owned runtime access for synchronous native transport ports.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use tokio::runtime::Handle;
use tonic::transport::Channel;

/// The Frontend role's explicitly composed Tokio runtime capability.
///
/// Native transport ports are synchronous Core-facing traits.  Their RPC work
/// runs on this role-owned handle, and they retain the historical two-path
/// `block_on` behavior when called both inside and outside a Tokio context.
#[derive(Clone)]
pub(crate) struct FrontendDataRuntime {
    handle: Handle,
    channels: Arc<Mutex<HashMap<String, Channel>>>,
}

impl FrontendDataRuntime {
    pub(crate) fn new(handle: Handle) -> Self {
        Self {
            handle,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn block_on<F>(&self, future: F) -> Result<F::Output, String>
    where
        F: Future,
    {
        if Handle::try_current().is_ok() {
            Ok(tokio::task::block_in_place(|| self.handle.block_on(future)))
        } else {
            Ok(self.handle.block_on(future))
        }
    }

    pub(crate) fn spawn<F>(&self, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.handle.spawn(future)
    }

    pub(crate) fn cached_channel(&self, key: &str) -> Option<Channel> {
        self.channels
            .lock()
            .expect("frontend native channel cache lock")
            .get(key)
            .cloned()
    }

    pub(crate) fn cache_channel(&self, key: String, channel: Channel) {
        self.channels
            .lock()
            .expect("frontend native channel cache lock")
            .insert(key, channel);
    }
}

#[cfg(test)]
mod tests {
    use super::FrontendDataRuntime;

    #[test]
    fn block_on_runs_outside_a_tokio_context() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let data_runtime = FrontendDataRuntime::new(runtime.handle().clone());
        assert_eq!(data_runtime.block_on(async { 7_u8 }).expect("block_on"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn block_on_runs_inside_a_tokio_context() {
        let data_runtime = FrontendDataRuntime::new(tokio::runtime::Handle::current());
        assert_eq!(
            data_runtime.block_on(async { 11_u8 }).expect("block_on"),
            11
        );
    }

    #[test]
    fn channel_cache_is_scoped_to_one_role_runtime_generation() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let first = FrontendDataRuntime::new(runtime.handle().clone());
        let channel = runtime.block_on(async {
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy()
        });
        first.cache_channel("be-1".to_string(), channel);
        assert!(first.cached_channel("be-1").is_some());

        let next_generation = FrontendDataRuntime::new(runtime.handle().clone());
        assert!(next_generation.cached_channel("be-1").is_none());
    }
}
