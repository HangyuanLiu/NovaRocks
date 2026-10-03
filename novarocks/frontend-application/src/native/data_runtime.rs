//! FE-owned runtime access for synchronous native transport ports.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use novarocks_native_trust::NativeTrust;
use novarocks_proto_codec::native_rpc::FrontendNativeLane;
use novarocks_task_codec::TransportBudget;
use novarocks_types::{BackendProcessId, NativeEndpoint};
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::transport::Channel;

use super::transport_supervisor::NativeTransportSupervisor;
use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;
use novarocks_native_adapter::FrontendNativeTransport;

/// Process-wide root-result I/O concurrency. Long polls are parked async, but
/// their channels and response buffers still consume finite process capacity.
const MAX_CONCURRENT_RESULT_FETCHES: usize = 16;

/// Exact peer generation and physical lane. Methods in one manifest lane
/// share its channel; other lanes and replacement processes cannot alias it.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct NativeChannelKey {
    pub(super) endpoint: NativeEndpoint,
    pub(super) peer: BackendProcessId,
    pub(super) lane: FrontendNativeLane,
}

/// A cached connection and its identity travel together. Channel clones do
/// not identify the cache entry that supplied an older request.
#[derive(Clone)]
pub(super) struct CachedNativeChannel {
    pub(super) channel: Channel,
    generation: Arc<DialGeneration>,
}

struct DialGeneration {
    retired: AtomicBool,
}

enum NativeChannelCacheRow {
    Dialing(Arc<DialGeneration>),
    Ready(CachedNativeChannel),
}

/// One cache generation elected before any connector IO. Cancellation removes
/// only this generation; replacement invalidation makes late publication fail.
pub(super) struct NativeDialReservation {
    runtime: FrontendDataRuntime,
    key: NativeChannelKey,
    generation: Arc<DialGeneration>,
    published: bool,
    leader: bool,
}

impl NativeDialReservation {
    pub(super) fn is_retired(&self) -> bool {
        self.generation.retired.load(Ordering::Acquire)
    }
    pub(super) fn is_leader(&self) -> bool {
        self.leader
    }

    pub(super) fn ready_channel(&self) -> Result<Option<CachedNativeChannel>, String> {
        let channels = self
            .runtime
            .channels
            .lock()
            .expect("frontend native channel cache lock");
        if self.is_retired() {
            return Err("Native channel generation retired before ready".to_owned());
        }
        match channels.get(&self.key) {
            Some(NativeChannelCacheRow::Ready(channel))
                if Arc::ptr_eq(&channel.generation, &self.generation) =>
            {
                Ok(Some(channel.clone()))
            }
            Some(NativeChannelCacheRow::Dialing(current))
                if Arc::ptr_eq(current, &self.generation) =>
            {
                Ok(None)
            }
            _ => Err("Native channel dial generation is no longer current".to_owned()),
        }
    }

    pub(super) fn publish(mut self, channel: Channel) -> Result<CachedNativeChannel, String> {
        let mut channels = self
            .runtime
            .channels
            .lock()
            .expect("frontend native channel cache lock");
        let exact = matches!(channels.get(&self.key), Some(NativeChannelCacheRow::Dialing(current)) if Arc::ptr_eq(current, &self.generation));
        if !exact || !self.leader || self.is_retired() {
            drop(channels);
            // Actual IO/worker teardown occurs outside the cache lock.
            drop(channel);
            return Err("Native channel generation retired before publication".to_owned());
        }
        let acquired = CachedNativeChannel {
            channel,
            generation: Arc::clone(&self.generation),
        };
        channels.insert(
            self.key.clone(),
            NativeChannelCacheRow::Ready(acquired.clone()),
        );
        self.published = true;
        Ok(acquired)
    }
}

impl Drop for NativeDialReservation {
    fn drop(&mut self) {
        if self.leader && !self.published {
            let mut channels = self
                .runtime
                .channels
                .lock()
                .expect("frontend native channel cache lock");
            if matches!(channels.get(&self.key), Some(NativeChannelCacheRow::Dialing(current)) if Arc::ptr_eq(current, &self.generation))
            {
                // Every follower retains this same scope. It must not silently
                // start a new generation after its elected leader exits.
                self.generation.retired.store(true, Ordering::Release);
                channels.remove(&self.key);
            }
        }
    }
}

/// The Frontend role's explicitly composed Tokio runtime capability.
///
/// Native transport ports are synchronous Core-facing traits.  Their RPC work
/// runs on this role-owned handle, and they retain the historical two-path
/// `block_on` behavior when called both inside and outside a Tokio context.
#[derive(Clone)]
pub(crate) struct FrontendDataRuntime {
    handle: Handle,
    native_trust: Arc<NativeTrust>,
    native_transport: FrontendNativeTransport,
    channels: Arc<Mutex<HashMap<NativeChannelKey, NativeChannelCacheRow>>>,
    dial_gates: Arc<[tokio::sync::Mutex<()>; 4]>,
    task_transport_supervisor: NativeTransportSupervisor,
    result_fetch_permits: Arc<Semaphore>,
    connector_blocking_io: ConnectorBlockingIoSupervisor,
}

impl FrontendDataRuntime {
    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }

    pub(crate) fn new_with_native_trust(
        handle: Handle,
        native_trust: Arc<NativeTrust>,
        native_transport: FrontendNativeTransport,
        task_transport_budget: TransportBudget,
    ) -> Result<Self, String> {
        let connector_blocking_io = ConnectorBlockingIoSupervisor::new(handle.clone());
        Ok(Self {
            handle,
            native_trust,
            native_transport,
            channels: Arc::new(Mutex::new(HashMap::new())),
            dial_gates: Arc::new(std::array::from_fn(|_| tokio::sync::Mutex::new(()))),
            task_transport_supervisor: NativeTransportSupervisor::from_transport(
                task_transport_budget,
            )?,
            result_fetch_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_RESULT_FETCHES)),
            connector_blocking_io,
        })
    }

    #[cfg(test)]
    pub(crate) fn new(handle: Handle) -> Self {
        use novarocks_native_trust::{
            DeploymentId, NativeCallerSubject, NativeTransportMode, ValidatedSharedSecret,
        };
        use novarocks_secret::SecretValue;

        let trust = NativeTrust::new(
            DeploymentId::parse("frontend-test").expect("fixed test deployment"),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .expect("fixed test shared secret"),
            NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("fixed test caller"),
            NativeTransportMode::Disabled,
        );
        Self::new_with_native_trust(
            handle,
            Arc::new(trust),
            FrontendNativeTransport::plaintext(),
            TransportBudget::DEFAULT,
        )
        .expect("the default task transport budget is valid")
    }

    pub(crate) fn native_trust(&self) -> &Arc<NativeTrust> {
        &self.native_trust
    }

    pub(crate) fn native_transport(&self) -> &FrontendNativeTransport {
        &self.native_transport
    }

    pub(crate) fn task_transport_supervisor(&self) -> &NativeTransportSupervisor {
        &self.task_transport_supervisor
    }

    pub(crate) async fn acquire_result_fetch(&self) -> Result<OwnedSemaphorePermit, String> {
        Arc::clone(&self.result_fetch_permits)
            .acquire_owned()
            .await
            .map_err(|_| "frontend result-fetch supervisor is closed".to_owned())
    }

    pub(crate) fn connector_blocking_io(&self) -> &ConnectorBlockingIoSupervisor {
        &self.connector_blocking_io
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

    pub(super) fn cached_channel(&self, key: &NativeChannelKey) -> Option<CachedNativeChannel> {
        self.channels
            .lock()
            .expect("frontend native channel cache lock")
            .get(key)
            .and_then(|row| match row {
                NativeChannelCacheRow::Ready(channel) => Some(channel.clone()),
                NativeChannelCacheRow::Dialing(_) => None,
            })
    }

    pub(super) fn begin_dial(
        &self,
        key: NativeChannelKey,
    ) -> Result<NativeDialReservation, String> {
        let mut channels = self
            .channels
            .lock()
            .expect("frontend native channel cache lock");
        let (generation, leader) = match channels.get(&key) {
            Some(NativeChannelCacheRow::Dialing(generation)) => (Arc::clone(generation), false),
            Some(NativeChannelCacheRow::Ready(channel)) => (Arc::clone(&channel.generation), false),
            None => {
                let generation = Arc::new(DialGeneration {
                    retired: AtomicBool::new(false),
                });
                channels.insert(
                    key.clone(),
                    NativeChannelCacheRow::Dialing(Arc::clone(&generation)),
                );
                (generation, true)
            }
        };
        Ok(NativeDialReservation {
            runtime: self.clone(),
            key,
            generation,
            published: false,
            leader,
        })
    }

    #[cfg(test)]
    fn cache_channel(&self, key: NativeChannelKey, channel: Channel) -> CachedNativeChannel {
        self.begin_dial(key).unwrap().publish(channel).unwrap()
    }

    /// An old RPC failure cannot evict a replacement connection that another
    /// attempt installed while the old RPC was still in flight.
    pub(super) fn invalidate_channel_if_current(
        &self,
        key: &NativeChannelKey,
        acquired: &CachedNativeChannel,
    ) -> bool {
        let mut channels = self
            .channels
            .lock()
            .expect("frontend native channel cache lock");
        if channels
            .get(key)
            .is_some_and(|current| matches!(current, NativeChannelCacheRow::Ready(current) if Arc::ptr_eq(&current.generation, &acquired.generation)))
        {
            acquired.generation.retired.store(true, Ordering::Release);
            channels.remove(key);
            true
        } else {
            false
        }
    }

    #[cfg(test)]
    pub(crate) fn invalidate_channel(&self, endpoint: &NativeEndpoint) {
        self.channels
            .lock()
            .expect("frontend native channel cache lock")
            .retain(|key, _| &key.endpoint != endpoint);
    }

    pub(crate) fn invalidate_peer(&self, peer: BackendProcessId) {
        let mut channels = self
            .channels
            .lock()
            .expect("frontend native channel cache lock");
        channels.retain(|key, row| {
            if key.peer != peer {
                return true;
            }
            let generation = match row {
                NativeChannelCacheRow::Dialing(generation) => generation,
                NativeChannelCacheRow::Ready(channel) => &channel.generation,
            };
            generation.retired.store(true, Ordering::Release);
            false
        });
    }

    pub(super) async fn dial_lane(
        &self,
        lane: FrontendNativeLane,
    ) -> tokio::sync::MutexGuard<'_, ()> {
        let index = match lane {
            FrontendNativeLane::ResultData => 0,
            FrontendNativeLane::Submission => 1,
            FrontendNativeLane::Observation => 2,
            FrontendNativeLane::LifecycleControl => 3,
        };
        self.dial_gates[index].lock().await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use novarocks_native_trust::{
        DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
    };
    use novarocks_secret::SecretValue;
    use novarocks_task_codec::TransportBudget;
    use novarocks_types::NativeEndpoint;

    use super::{FrontendDataRuntime, NativeChannelKey};
    use novarocks_native_adapter::FrontendNativeTransport;

    fn data_runtime(handle: tokio::runtime::Handle) -> FrontendDataRuntime {
        let trust = NativeTrust::new(
            DeploymentId::parse("frontend-test").expect("deployment"),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .expect("secret"),
            NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("subject"),
            NativeTransportMode::Disabled,
        );
        FrontendDataRuntime::new_with_native_trust(
            handle,
            Arc::new(trust),
            FrontendNativeTransport::plaintext(),
            TransportBudget::DEFAULT,
        )
        .expect("the default task transport budget is valid")
    }

    #[test]
    fn block_on_runs_outside_a_tokio_context() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let data_runtime = data_runtime(runtime.handle().clone());
        assert_eq!(data_runtime.block_on(async { 7_u8 }).expect("block_on"), 7);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn block_on_runs_inside_a_tokio_context() {
        let data_runtime = data_runtime(tokio::runtime::Handle::current());
        assert_eq!(
            data_runtime.block_on(async { 11_u8 }).expect("block_on"),
            11
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cloned_role_runtime_shares_one_result_fetch_limit() {
        let runtime = data_runtime(tokio::runtime::Handle::current());
        let mut permits = Vec::new();
        for _ in 0..super::MAX_CONCURRENT_RESULT_FETCHES {
            permits.push(runtime.acquire_result_fetch().await.expect("fetch permit"));
        }

        let waiting_runtime = runtime.clone();
        let mut waiting = Box::pin(waiting_runtime.acquire_result_fetch());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut waiting)
                .await
                .is_err(),
            "a clone must not create a separate result-fetch pool"
        );
        permits.pop();
        let _permit = tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("released process capacity wakes the waiter")
            .expect("fetch permit after release");
    }

    #[test]
    fn channel_cache_is_scoped_to_one_role_runtime_generation() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let first = data_runtime(runtime.handle().clone());
        let (channel, _updates) =
            runtime.block_on(async { tonic::transport::Channel::balance_channel::<String>(1) });
        let endpoint = NativeEndpoint::from_host_port("be.example", 19040).expect("endpoint");
        let key = NativeChannelKey {
            endpoint: endpoint.clone(),
            peer: novarocks_types::BackendProcessId::new_v7(),
            lane: novarocks_proto_codec::native_rpc::FrontendNativeLane::Submission,
        };
        first.cache_channel(key.clone(), channel);
        assert!(first.cached_channel(&key).is_some());
        first.invalidate_channel(&endpoint);
        assert!(first.cached_channel(&key).is_none());

        let next_generation = data_runtime(runtime.handle().clone());
        assert!(next_generation.cached_channel(&key).is_none());
    }

    #[test]
    fn stale_failure_cannot_evict_reconnected_channel() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("build runtime");
        let data_runtime = data_runtime(runtime.handle().clone());
        let endpoint = NativeEndpoint::from_host_port("be.example", 19040).expect("endpoint");
        let (first_channel, _first_updates) =
            runtime.block_on(async { tonic::transport::Channel::balance_channel::<String>(1) });
        let key = NativeChannelKey {
            endpoint: endpoint.clone(),
            peer: novarocks_types::BackendProcessId::new_v7(),
            lane: novarocks_proto_codec::native_rpc::FrontendNativeLane::Submission,
        };
        let old = data_runtime.cache_channel(key.clone(), first_channel);
        assert!(data_runtime.invalidate_channel_if_current(&key, &old));

        let (second_channel, _second_updates) =
            runtime.block_on(async { tonic::transport::Channel::balance_channel::<String>(1) });
        let replacement = data_runtime.cache_channel(key.clone(), second_channel);
        assert!(!data_runtime.invalidate_channel_if_current(&key, &old));
        assert!(data_runtime.cached_channel(&key).is_some());
        assert!(data_runtime.invalidate_channel_if_current(&key, &replacement));
        assert!(data_runtime.cached_channel(&key).is_none());
    }
    #[tokio::test]
    async fn independent_lane_dials_and_cancelled_reservations_do_not_hold_other_lanes() {
        use novarocks_proto_codec::native_rpc::FrontendNativeLane::{
            LifecycleControl, Observation,
        };
        let runtime = data_runtime(tokio::runtime::Handle::current());
        let observation = runtime.dial_lane(Observation).await;
        let control = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            runtime.dial_lane(LifecycleControl),
        )
        .await
        .unwrap();
        let mut pending = Box::pin(runtime.dial_lane(Observation));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                pending.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        drop(pending);
        drop(observation);
        drop(control);
        drop(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                runtime.dial_lane(Observation),
            )
            .await
            .unwrap(),
        );
    }

    #[tokio::test]
    async fn retired_inflight_dial_cannot_publish_or_delete_its_replacement_generation() {
        let runtime = data_runtime(tokio::runtime::Handle::current());
        let key = NativeChannelKey {
            endpoint: NativeEndpoint::from_host_port("be.example", 19040).unwrap(),
            peer: novarocks_types::BackendProcessId::new_v7(),
            lane: novarocks_proto_codec::native_rpc::FrontendNativeLane::Submission,
        };
        let old = runtime.begin_dial(key.clone()).unwrap();
        runtime.invalidate_peer(key.peer);
        let replacement = runtime.begin_dial(key.clone()).unwrap();
        let (late_channel, _late_updates) = tonic::transport::Channel::balance_channel::<String>(1);
        assert!(old.publish(late_channel).is_err());
        let (current_channel, _current_updates) =
            tonic::transport::Channel::balance_channel::<String>(1);
        let current = replacement.publish(current_channel).unwrap();
        assert!(Arc::ptr_eq(
            &runtime.cached_channel(&key).unwrap().generation,
            &current.generation
        ));
        let mut other = key.clone();
        other.peer = novarocks_types::BackendProcessId::new_v7();
        let cancelled = runtime.begin_dial(other.clone()).unwrap();
        assert!(!runtime.begin_dial(other.clone()).unwrap().is_leader());
        drop(cancelled);
        assert!(runtime.begin_dial(other).is_ok());
        assert!(runtime.cached_channel(&key).is_some());
    }
}
