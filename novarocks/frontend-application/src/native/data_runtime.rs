//! FE-owned runtime access for synchronous native transport ports.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use novarocks_execution_contract::native_result_support::NativeResultSupportGeometry;
use novarocks_native_adapter::native_lane::{NativeLaneChannel, frontend_lane_connections};
use novarocks_native_adapter::native_transport_admission::NativeTransportAdmission;
use novarocks_native_trust::NativeTrust;
use novarocks_proto_codec::native_rpc::FrontendNativeLane;
use novarocks_task_codec::TransportBudget;
use novarocks_types::{BackendProcessId, NativeEndpoint};
use tokio::runtime::Handle;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::transport_supervisor::NativeTransportSupervisor;
use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;
use novarocks_native_adapter::FrontendNativeTransport;

/// Process-wide root-result I/O concurrency. Long polls are parked async, but
/// their channels and response buffers still consume finite process capacity.
const MAX_CONCURRENT_RESULT_FETCHES: usize = 16;

/// Exact peer generation and physical lane. Methods in one manifest lane
/// share its connections; other lanes and replacement processes cannot alias
/// them.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct NativeChannelKey {
    pub(super) endpoint: NativeEndpoint,
    pub(super) peer: BackendProcessId,
    pub(super) lane: FrontendNativeLane,
}

/// One of a lane's connections: the geometry fixes how many connections a
/// Frontend keeps per Backend process, endpoint and lane, and calls rotate
/// over them.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct NativeChannelSlot {
    pub(super) key: NativeChannelKey,
    pub(super) index: usize,
}

/// A cached connection and its identity travel together. Channel clones do
/// not identify the cache entry that supplied an older request.
#[derive(Clone)]
pub(super) struct CachedNativeChannel {
    pub(super) channel: NativeLaneChannel,
    pub(super) slot: NativeChannelSlot,
    generation: Arc<DialGeneration>,
}

struct DialGeneration {
    retired: AtomicBool,
}

enum NativeChannelCacheRow {
    Dialing(Arc<DialGeneration>),
    Ready(Box<CachedNativeChannel>),
}

impl NativeChannelCacheRow {
    fn generation(&self) -> &Arc<DialGeneration> {
        match self {
            Self::Dialing(generation) => generation,
            Self::Ready(channel) => &channel.generation,
        }
    }
}

/// The connections of one Backend process, endpoint and lane.
struct LanePool {
    next: usize,
    rows: Vec<Option<NativeChannelCacheRow>>,
}

/// Bounded cache: at most `pool_limit()` lane pools, each with exactly the
/// geometry's connection count.
#[derive(Default)]
struct ChannelPools {
    pools: HashMap<NativeChannelKey, LanePool>,
}

/// Every live Backend's four lanes, once for a process and once for its
/// replacement while the old one is still being invalidated.
fn pool_limit() -> usize {
    let backends = usize::try_from(NativeResultSupportGeometry::V1.transport_maximum_live_backends)
        .expect("validated Native geometry fits the target");
    backends
        .checked_mul(4 * 2)
        .expect("validated Native geometry fits the target")
}

impl ChannelPools {
    fn row(&self, slot: &NativeChannelSlot) -> Option<&NativeChannelCacheRow> {
        self.pools
            .get(&slot.key)
            .and_then(|pool| pool.rows.get(slot.index))
            .and_then(Option::as_ref)
    }

    fn row_mut(&mut self, slot: &NativeChannelSlot) -> Option<&mut Option<NativeChannelCacheRow>> {
        self.pools
            .get_mut(&slot.key)
            .and_then(|pool| pool.rows.get_mut(slot.index))
    }

    /// Admit a pool for `key`. When full, only a pool with no dial in flight
    /// may be evicted; eviction drops cache aliases only, while escaped
    /// channels and their connections keep their own owners.
    fn pool(&mut self, key: &NativeChannelKey) -> Result<&mut LanePool, String> {
        if !self.pools.contains_key(key) {
            if self.pools.len() >= pool_limit() {
                let cold = self
                    .pools
                    .iter()
                    .find(|(_, pool)| {
                        pool.rows
                            .iter()
                            .all(|row| !matches!(row, Some(NativeChannelCacheRow::Dialing(_))))
                    })
                    .map(|(cold, _)| cold.clone())
                    .ok_or_else(|| "Frontend Native channel cache is full of dials".to_owned())?;
                self.pools.remove(&cold);
            }
            let mut rows = Vec::new();
            rows.resize_with(frontend_lane_connections(key.lane), || None);
            self.pools.insert(key.clone(), LanePool { next: 0, rows });
        }
        Ok(self.pools.get_mut(key).expect("pool admitted above"))
    }
}

/// One cache generation elected before any connector IO. Cancellation removes
/// only this generation; replacement invalidation makes late publication fail.
pub(super) struct NativeDialReservation {
    runtime: FrontendDataRuntime,
    slot: NativeChannelSlot,
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
        match channels.row(&self.slot) {
            Some(NativeChannelCacheRow::Ready(channel))
                if Arc::ptr_eq(&channel.generation, &self.generation) =>
            {
                Ok(Some(CachedNativeChannel::clone(channel)))
            }
            Some(NativeChannelCacheRow::Dialing(current))
                if Arc::ptr_eq(current, &self.generation) =>
            {
                Ok(None)
            }
            _ => Err("Native channel dial generation is no longer current".to_owned()),
        }
    }

    pub(super) fn publish(
        mut self,
        channel: NativeLaneChannel,
    ) -> Result<CachedNativeChannel, String> {
        let mut channels = self
            .runtime
            .channels
            .lock()
            .expect("frontend native channel cache lock");
        let exact = matches!(channels.row(&self.slot), Some(NativeChannelCacheRow::Dialing(current)) if Arc::ptr_eq(current, &self.generation));
        if !exact || !self.leader || self.is_retired() {
            drop(channels);
            // Actual IO/worker teardown occurs outside the cache lock.
            drop(channel);
            return Err("Native channel generation retired before publication".to_owned());
        }
        let acquired = CachedNativeChannel {
            channel,
            slot: self.slot.clone(),
            generation: Arc::clone(&self.generation),
        };
        *channels
            .row_mut(&self.slot)
            .expect("exact dialing row exists") =
            Some(NativeChannelCacheRow::Ready(Box::new(acquired.clone())));
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
            if let Some(row) = channels.row_mut(&self.slot)
                && matches!(row, Some(NativeChannelCacheRow::Dialing(current)) if Arc::ptr_eq(current, &self.generation))
            {
                // Every follower retains this same scope. It must not silently
                // start a new generation after its elected leader exits.
                self.generation.retired.store(true, Ordering::Release);
                *row = None;
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
    transport_admission: NativeTransportAdmission,
    channels: Arc<Mutex<ChannelPools>>,
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
        // An inconsistent frozen geometry is refused before any channel. The
        // envelope's count part is logged; P00b freezes the coefficients.
        let geometry =
            novarocks_native_adapter::native_transport_geometry::validate_native_transport_geometry(
                &NativeResultSupportGeometry::V1,
            )
            .map_err(|error| format!("validate Native transport geometry: {error}"))?;
        let transport_admission = NativeTransportAdmission::frontend(Some(
            crate::metrics::native_transport::frontend_native_transport_observer(),
        ))
        .map_err(|error| format!("compose Frontend Native transport admission: {error}"))?;
        tracing::debug!(
            connections = geometry.frontend.connections,
            streams = geometry.frontend.streams,
            structural_bytes = geometry.frontend.structural_bytes,
            native_sockets = geometry.frontend_socket_positions,
            coefficients_frozen = geometry.frontend.coefficients.is_some(),
            "Frontend Native transport admission composed"
        );
        let connector_blocking_io = ConnectorBlockingIoSupervisor::new(handle.clone());
        Ok(Self {
            handle,
            native_trust,
            native_transport,
            transport_admission,
            channels: Arc::new(Mutex::new(ChannelPools::default())),
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

    /// The Frontend's outgoing connection admission: every dial, including
    /// Tonic's reconnect, takes a connection and a handshake position.
    pub(crate) fn transport_admission(&self) -> &NativeTransportAdmission {
        &self.transport_admission
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

    /// The next of `key`'s connections, in deterministic rotation.
    pub(super) fn select_slot(&self, key: &NativeChannelKey) -> Result<NativeChannelSlot, String> {
        let mut channels = self
            .channels
            .lock()
            .expect("frontend native channel cache lock");
        let pool = channels.pool(key)?;
        let index = pool.next;
        pool.next = (index + 1) % pool.rows.len();
        Ok(NativeChannelSlot {
            key: key.clone(),
            index,
        })
    }

    pub(super) fn cached_channel(&self, slot: &NativeChannelSlot) -> Option<CachedNativeChannel> {
        self.channels
            .lock()
            .expect("frontend native channel cache lock")
            .row(slot)
            .and_then(|row| match row {
                NativeChannelCacheRow::Ready(channel) => Some(CachedNativeChannel::clone(channel)),
                NativeChannelCacheRow::Dialing(_) => None,
            })
    }

    pub(super) fn begin_dial(
        &self,
        slot: NativeChannelSlot,
    ) -> Result<NativeDialReservation, String> {
        let mut channels = self
            .channels
            .lock()
            .expect("frontend native channel cache lock");
        let pool = channels.pool(&slot.key)?;
        let row = pool
            .rows
            .get_mut(slot.index)
            .ok_or_else(|| "Native channel slot is outside its lane".to_owned())?;
        let (generation, leader) = match row {
            Some(row) => (Arc::clone(row.generation()), false),
            None => {
                let generation = Arc::new(DialGeneration {
                    retired: AtomicBool::new(false),
                });
                *row = Some(NativeChannelCacheRow::Dialing(Arc::clone(&generation)));
                (generation, true)
            }
        };
        Ok(NativeDialReservation {
            runtime: self.clone(),
            slot,
            generation,
            published: false,
            leader,
        })
    }

    #[cfg(test)]
    fn cache_channel(
        &self,
        slot: NativeChannelSlot,
        channel: tonic::transport::Channel,
    ) -> CachedNativeChannel {
        let lane = novarocks_native_adapter::native_lane::NativeLane::frontend(slot.key.lane);
        self.begin_dial(slot)
            .unwrap()
            .publish(NativeLaneChannel::new(channel, lane, None))
            .unwrap()
    }

    /// The live connections currently cached for `key`.
    #[cfg(test)]
    pub(super) fn cached_connections(&self, key: &NativeChannelKey) -> usize {
        self.channels
            .lock()
            .expect("frontend native channel cache lock")
            .pools
            .get(key)
            .map_or(0, |pool| {
                pool.rows
                    .iter()
                    .filter(|row| matches!(row, Some(NativeChannelCacheRow::Ready(_))))
                    .count()
            })
    }

    /// An old RPC failure cannot evict a replacement connection that another
    /// attempt installed while the old RPC was still in flight.
    pub(super) fn invalidate_channel_if_current(&self, acquired: &CachedNativeChannel) -> bool {
        let mut channels = self
            .channels
            .lock()
            .expect("frontend native channel cache lock");
        let Some(row) = channels.row_mut(&acquired.slot) else {
            return false;
        };
        if !matches!(row, Some(NativeChannelCacheRow::Ready(current)) if Arc::ptr_eq(&current.generation, &acquired.generation))
        {
            return false;
        }
        acquired.generation.retired.store(true, Ordering::Release);
        *row = None;
        true
    }

    #[cfg(test)]
    pub(crate) fn invalidate_channel(&self, endpoint: &NativeEndpoint) {
        self.channels
            .lock()
            .expect("frontend native channel cache lock")
            .pools
            .retain(|key, _| &key.endpoint != endpoint);
    }

    pub(crate) fn invalidate_peer(&self, peer: BackendProcessId) {
        let mut channels = self
            .channels
            .lock()
            .expect("frontend native channel cache lock");
        channels.pools.retain(|key, pool| {
            if key.peer != peer {
                return true;
            }
            for row in pool.rows.iter().flatten() {
                row.generation().retired.store(true, Ordering::Release);
            }
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

    use super::{FrontendDataRuntime, NativeChannelKey, NativeChannelSlot};
    use novarocks_native_adapter::FrontendNativeTransport;

    fn slot(key: NativeChannelKey) -> NativeChannelSlot {
        NativeChannelSlot { key, index: 0 }
    }

    fn lane_channel(
        channel: tonic::transport::Channel,
    ) -> novarocks_native_adapter::native_lane::NativeLaneChannel {
        novarocks_native_adapter::native_lane::NativeLaneChannel::new(
            channel,
            novarocks_native_adapter::native_lane::NativeLane::Submission,
            None,
        )
    }

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
        let key = slot(NativeChannelKey {
            endpoint: endpoint.clone(),
            peer: novarocks_types::BackendProcessId::new_v7(),
            lane: novarocks_proto_codec::native_rpc::FrontendNativeLane::Submission,
        });
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
        let key = slot(NativeChannelKey {
            endpoint: endpoint.clone(),
            peer: novarocks_types::BackendProcessId::new_v7(),
            lane: novarocks_proto_codec::native_rpc::FrontendNativeLane::Submission,
        });
        let old =
            runtime.block_on(async { data_runtime.cache_channel(key.clone(), first_channel) });
        assert!(data_runtime.invalidate_channel_if_current(&old));

        let (second_channel, _second_updates) =
            runtime.block_on(async { tonic::transport::Channel::balance_channel::<String>(1) });
        let replacement =
            runtime.block_on(async { data_runtime.cache_channel(key.clone(), second_channel) });
        assert!(!data_runtime.invalidate_channel_if_current(&old));
        assert!(data_runtime.cached_channel(&key).is_some());
        assert!(data_runtime.invalidate_channel_if_current(&replacement));
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
        let key = slot(NativeChannelKey {
            endpoint: NativeEndpoint::from_host_port("be.example", 19040).unwrap(),
            peer: novarocks_types::BackendProcessId::new_v7(),
            lane: novarocks_proto_codec::native_rpc::FrontendNativeLane::Submission,
        });
        let old = runtime.begin_dial(key.clone()).unwrap();
        runtime.invalidate_peer(key.key.peer);
        let replacement = runtime.begin_dial(key.clone()).unwrap();
        let (late_channel, _late_updates) = tonic::transport::Channel::balance_channel::<String>(1);
        assert!(old.publish(lane_channel(late_channel)).is_err());
        let (current_channel, _current_updates) =
            tonic::transport::Channel::balance_channel::<String>(1);
        let current = replacement.publish(lane_channel(current_channel)).unwrap();
        assert!(Arc::ptr_eq(
            &runtime.cached_channel(&key).unwrap().generation,
            &current.generation
        ));
        let mut other = key.clone();
        other.key.peer = novarocks_types::BackendProcessId::new_v7();
        let cancelled = runtime.begin_dial(other.clone()).unwrap();
        assert!(!runtime.begin_dial(other.clone()).unwrap().is_leader());
        drop(cancelled);
        assert!(runtime.begin_dial(other).is_ok());
        assert!(runtime.cached_channel(&key).is_some());
    }
}
