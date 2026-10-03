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

//! Actual logical Channel Workers issued by the bounded cache's Leader.
//! Physical TCP/H2 fixtures use ordinary executors and no connection position.
//! The original StockCore receipt funds cache/Worker backings; this target does
//! not attest peer/socket/queue/body/runtime backing or Native authentication.

use super::{Election, Leader, NativeChannelCache, NativeChannelKey};
use crate::native_transport_capacity::NativeTransportCapacityFactory;
use hyper::http::{Request, Response, Uri};
use hyper::rt::Executor;
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
use novarocks_types::NativeEndpoint;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::{Endpoint, Http2ConnectionConfig};
use tower::{ServiceExt, service_fn};

const WATCHDOG: Duration = Duration::from_secs(5);
const PHASE: Duration = Duration::from_secs(2);

fn fixture() -> (
    NativeChannelCache,
    NativeTransportCapacityFactory,
    Arc<ResultRetainedBudget>,
    usize,
) {
    let bytes = NativeTransportCapacityFactory::allocation_capacity_bound().unwrap();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
    let factory = NativeTransportCapacityFactory::try_new(budget.clone()).unwrap();
    let cache = NativeChannelCache::bounded(factory.clone()).unwrap();
    (cache, factory, budget, bytes)
}

async fn elect(cache: &NativeChannelCache, key: &NativeChannelKey) -> Leader {
    match cache.acquire(key.inline_identity().unwrap()).await.unwrap() {
        Election::Leader(leader) => leader,
        Election::Ready(_) => panic!("removed exact row must elect a fresh generation"),
    }
}

fn blocked_cache(factory: &NativeTransportCapacityFactory) {
    match NativeChannelCache::bounded(factory.clone()) {
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
        Ok(_) => panic!("detached actual Worker still owns the singleton cache family"),
    }
}

fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn returned(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("actual final Worker/StockCore exit must return the whole original grant");
    };
    drop(credit);
}

#[derive(Clone, Default)]
struct JoinedExecutor(Arc<Mutex<Vec<JoinHandle<()>>>>);
impl<F> Executor<F> for JoinedExecutor
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        self.0.lock().unwrap().push(tokio::spawn(async move {
            let _ = future.await;
        }));
    }
}
impl JoinedExecutor {
    async fn join(&self) {
        loop {
            let tasks = std::mem::take(&mut *self.0.lock().unwrap());
            if tasks.is_empty() {
                break;
            }
            for task in tasks {
                tokio::time::timeout(WATCHDOG, task)
                    .await
                    .expect("actual physical executor task failed to exit")
                    .unwrap();
            }
        }
    }
}

async fn joined(handle: JoinHandle<()>) {
    tokio::time::timeout(WATCHDOG, handle)
        .await
        .expect("actual task failed to exit")
        .unwrap();
}
async fn completed(handle: &JoinHandle<()>) {
    tokio::time::timeout(WATCHDOG, async {
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actual logical Tower Worker failed to complete");
}

async fn roundtrip(channel: tonic::transport::Channel, generation: usize) {
    let response = tokio::time::timeout(
        WATCHDOG,
        channel.oneshot(Request::new(tonic::body::empty_body())),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        response.headers()["x-actual-worker"],
        generation.to_string()
    );
    drop(response);
}

fn serve_one(
    listener: Arc<TcpListener>,
    generation: usize,
) -> (oneshot::Sender<()>, JoinHandle<()>) {
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (io, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(io).await.unwrap();
        loop {
            tokio::select! {
                _ = &mut stopped => break,
                request = connection.accept() => match request {
                    Some(Ok((_request, mut respond))) => {
                        respond.send_response(Response::builder()
                            .header("x-actual-worker", generation.to_string())
                            .body(()).unwrap(), true).unwrap();
                    }
                    Some(Err(error)) => panic!("actual peer H2 error: {error:?}"),
                    None => break,
                }
            }
        }
        // Explicit fixture close destroys the real peer H2 connection and IO.
        drop(connection);
    });
    (stop, task)
}

struct Detached {
    channel: tonic::transport::Channel,
    handle: JoinHandle<()>,
}

async fn open_and_detach(
    cache: &NativeChannelCache,
    key: &NativeChannelKey,
    listener: &Arc<TcpListener>,
    generation: usize,
) -> Detached {
    open_with_cache_mode(cache, key, listener, generation, true).await
}

async fn open_with_cache_mode(
    cache: &NativeChannelCache,
    key: &NativeChannelKey,
    listener: &Arc<TcpListener>,
    generation: usize,
    detach: bool,
) -> Detached {
    let mut leader = elect(cache, key).await;
    let original = leader.original_channel_worker().unwrap();
    let (stop, peer) = serve_one(listener.clone(), generation);
    let address = listener.local_addr().unwrap();
    let executor = JoinedExecutor::default();
    let endpoint = Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .executor(executor.clone())
        .http2_connection_factory(|| {
            Ok::<_, io::Error>(Http2ConnectionConfig {
                initial_settings_timeout: Some(PHASE),
                ..Default::default()
            })
        });
    let connector =
        service_fn(
            move |_: Uri| async move { TcpStream::connect(address).await.map(TokioIo::new) },
        );
    let channel = endpoint
        .connect_with_connector_and_original_worker(connector, original.clone())
        .await
        .unwrap();
    assert_eq!(
        executor.0.lock().unwrap().len(),
        2,
        "only logical Worker bypasses executor; physical driver and H2 task stay ordinary"
    );
    roundtrip(channel.clone(), generation).await;
    let handle = original
        .take_task_handle()
        .expect("actual spawned Tower Worker");
    drop(original);
    leader.publish(channel.clone()).unwrap();
    let escaped = if detach {
        cache
            .remove(key)
            .expect("same actual Ready row must detach")
    } else {
        channel.clone()
    };
    drop(channel);
    drop(endpoint);
    stop.send(()).unwrap();
    joined(peer).await;
    executor.join().await;
    assert!(
        !handle.is_finished(),
        "escaped real Channel must keep Worker alive after cache removal and physical IO exit"
    );
    Detached {
        channel: escaped,
        handle,
    }
}

fn endpoint_key(listener: &Arc<TcpListener>) -> NativeChannelKey {
    let address = listener.local_addr().unwrap();
    NativeChannelKey::membership(
        NativeEndpoint::from_host_port("127.0.0.1", address.port()).unwrap(),
    )
}

#[tokio::test]
async fn actual_230_detached_old_channel_workers_exhaust_original_stock_until_last_cell_exit() {
    let (cache, factory, budget, bytes) = fixture();
    assert_eq!(super::entry_positions().unwrap(), 230);
    let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
    let key = endpoint_key(&listener);
    let mut old = Vec::with_capacity(230);
    // Same exact endpoint/peer/lane, 230 cache generations. No 230-peer model or
    // physical connection position borrowing: every generation has a real Worker.
    for generation in 1..=230 {
        old.push(open_and_detach(&cache, &key, &listener, generation).await);
    }
    let mut excess = elect(&cache, &key).await;
    match excess.original_channel_worker() {
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
        Ok(_) => panic!("231st logical Worker cannot reuse detached owners"),
    }
    drop(excess);
    match cache.transient_original_channel_worker() {
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
        Ok(_) => panic!("transient readiness cannot borrow a detached logical Worker position"),
    }
    let Detached { channel, handle } = old.pop().unwrap();
    drop(channel);
    joined(handle).await;
    let transient = cache.transient_original_channel_worker().unwrap().unwrap();
    drop(transient);
    let mut replacement = elect(&cache, &key).await;
    // This is a metadata claim/reuse oracle after the actual old TaskCell exits.
    // The 230 original claims above all spawned actual Tower Workers.
    let unused_replacement = replacement.original_channel_worker().unwrap();
    drop(replacement);
    drop(unused_replacement);
    for Detached { channel, handle } in old {
        drop(channel);
        joined(handle).await;
    }
    drop(listener);
    drop(cache);
    let next_cache = NativeChannelCache::bounded(factory.clone()).unwrap();
    drop(next_cache);
    held(&budget);
    drop(factory);
    returned(&budget, bytes);
}

#[tokio::test]
async fn actual_completed_worker_join_abort_block_shell_replacement_after_cache_final_drop() {
    let (cache, factory, budget, bytes) = fixture();
    let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
    let key = endpoint_key(&listener);
    let Detached { channel, handle } = open_and_detach(&cache, &key, &listener, 1).await;
    drop(cache);
    blocked_cache(&factory);
    let abort = handle.abort_handle();
    drop(channel);
    completed(&handle).await;
    blocked_cache(&factory);
    joined(handle).await;
    blocked_cache(&factory);
    held(&budget);
    // Final actual AbortHandle is a strong TaskCell alias, not a phase flag.
    drop(abort);
    let replacement = NativeChannelCache::bounded(factory.clone()).unwrap();
    drop(replacement);
    drop(listener);
    drop(factory);
    returned(&budget, bytes);
}

#[tokio::test]
async fn actual_worker_last_abort_alone_holds_stock_after_factory_and_cache_public_exit() {
    let (cache, factory, budget, bytes) = fixture();
    let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
    let key = endpoint_key(&listener);
    let Detached { channel, handle } = open_and_detach(&cache, &key, &listener, 1).await;
    let abort = handle.abort_handle();
    drop(channel);
    completed(&handle).await;
    joined(handle).await;
    drop(listener);
    drop(cache);
    drop(factory);
    held(&budget);
    drop(abort);
    returned(&budget, bytes);
}

#[tokio::test]
async fn actual_cold_ready_eviction_detaches_worker_without_retiring_escaped_channel() {
    let (cache, factory, budget, bytes) = fixture();
    let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
    let key = endpoint_key(&listener);
    let Detached { channel, handle } =
        open_with_cache_mode(&cache, &key, &listener, 1, false).await;
    // Fill the other metadata rows with aliases of the same actual Channel.
    // These are cache eviction fixtures, not 229 additional funded Workers.
    let mut filled = 1;
    for port in 1..=231 {
        let other = NativeChannelKey::membership(
            NativeEndpoint::from_host_port("127.0.0.1", port).unwrap(),
        );
        if other.inline_identity().unwrap() == key.inline_identity().unwrap() {
            continue;
        }
        if filled == 230 {
            break;
        }
        elect(&cache, &other)
            .await
            .publish(channel.clone())
            .unwrap();
        filled += 1;
    }
    assert_eq!(filled, 230);
    let cold = [65000, 65001]
        .into_iter()
        .map(|port| {
            NativeChannelKey::membership(NativeEndpoint::from_host_port("127.0.0.1", port).unwrap())
        })
        .find(|candidate| candidate.inline_identity().unwrap() != key.inline_identity().unwrap())
        .unwrap();
    let mut replacement = elect(&cache, &cold).await;
    // The first cold eviction takes the old row's attachment before reuse.
    let worker = replacement.original_channel_worker().unwrap();
    drop(worker);
    drop(replacement);
    assert!(!handle.is_finished());
    drop(cache);
    blocked_cache(&factory);
    drop(channel);
    joined(handle).await;
    let next = NativeChannelCache::bounded(factory.clone()).unwrap();
    drop(next);
    drop(listener);
    held(&budget);
    drop(factory);
    returned(&budget, bytes);
}
