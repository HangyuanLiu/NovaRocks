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

//! Production readiness refuses before dialing when its original Worker stock
//! is occupied. Unsubmitted holders below reserve real metadata/positions, not
//! alleged TCP connections. A separate actual eager Worker tests Cell aliases.
//! Queue, socket, runtime and the whole Native allocation graph remain separate.

use super::wait_for_backend_native_endpoint_ready;
use crate::BackendDataRuntime;
use crate::backend_test_support::test_backend_data_runtime;
use crate::native_channel_cache::NativeChannelCache;
use crate::native_transport_capacity::{NativeTransportCapacityFactory, TransportClass};
use hyper_util::rt::TokioIo;
use novarocks_execution::runtime::fragment::io::ResultWriteAdmission;
use novarocks_types::NativeEndpoint;
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tower::service_fn;

const WATCHDOG: Duration = Duration::from_secs(5);

fn fixture() -> (
    BackendDataRuntime,
    NativeTransportCapacityFactory,
    Arc<ResultRetainedBudget>,
    usize,
) {
    let bytes = NativeTransportCapacityFactory::allocation_capacity_bound().unwrap();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
    let factory = NativeTransportCapacityFactory::try_new(budget.clone()).unwrap();
    let runtime = test_backend_data_runtime()
        .with_transport_capacity(factory.clone())
        .unwrap();
    (runtime, factory, budget, bytes)
}

#[test]
fn full_original_worker_stock_refuses_actual_readiness_before_tcp_connector() {
    let (runtime, factory, budget, bytes) = fixture();
    // A discovered live TCP listener deliberately serves no HTTP/2. An ordinary
    // fallback would dial it and time out, giving a different error and a real
    // accepted socket; this test does not infer non-dialing from elapsed time.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let endpoint = NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let count = crate::native_channel_cache::entry_positions().unwrap();
    assert_eq!(count, 230, "the existing logical Channel geometry changed");
    let mut workers = Vec::with_capacity(count);
    for _ in 0..count {
        workers.push(
            runtime
                .channels()
                .transient_original_channel_worker()
                .unwrap()
                .expect("funded runtime must never return an ordinary Worker"),
        );
    }
    let full = runtime
        .channels()
        .transient_original_channel_worker()
        .unwrap_err();
    assert_eq!(full.kind(), io::ErrorKind::WouldBlock);
    let expected = format!(
        "Native readiness Worker election refused: {}",
        io::Error::from(io::ErrorKind::WouldBlock)
    );
    let error = wait_for_backend_native_endpoint_ready(
        &runtime,
        endpoint,
        TransportClass::Data,
        Duration::from_millis(100),
    )
    .expect_err("full original Worker stock must refuse the product entry");
    assert_eq!(error, expected);
    match listener.accept() {
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::WouldBlock),
        Ok(_) => panic!("Worker refusal must precede the actual TCP connector"),
    }
    assert_eq!(
        factory.available_positions(TransportClass::Data),
        factory.positions(TransportClass::Data)
    );
    assert_eq!(
        factory.available_acquisitions(TransportClass::Data),
        factory.acquisition_positions(TransportClass::Data)
    );
    drop(workers);
    // An actually exited unsubmitted holder makes exactly the same stock usable.
    let replacement = runtime
        .channels()
        .transient_original_channel_worker()
        .unwrap()
        .unwrap();
    drop(replacement);
    drop(runtime);
    drop(factory);
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("unsubmitted Worker holders retained the original stock")
    };
    drop(credit);
}

#[test]
fn actual_transient_worker_cell_alias_delays_cache_singleton_after_runtime_drop() {
    let (runtime, factory, budget, bytes) = fixture();
    let reactor = runtime.handle().clone();
    let worker = runtime
        .channels()
        .transient_original_channel_worker()
        .unwrap()
        .unwrap();
    let (channel, peer) = runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (io, _) = listener.accept().await.unwrap();
            let mut connection = h2::server::handshake(io).await.unwrap();
            assert!(
                connection.accept().await.is_none(),
                "readiness bootstrap must not submit an application RPC"
            );
        });
        let endpoint = NativeEndpoint::from_socket_addr(address);
        let endpoint =
            crate::native_client::capacity_endpoint(&runtime, &endpoint, TransportClass::Data)
                .unwrap();
        let connector =
            service_fn(move |_| async move { TcpStream::connect(address).await.map(TokioIo::new) });
        let channel = tokio::time::timeout(
            WATCHDOG,
            endpoint.connect_with_connector_and_original_worker(connector, worker.clone()),
        )
        .await
        .expect("actual eager Channel bootstrap did not finish")
        .unwrap();
        (channel, peer)
    });
    let task = worker
        .take_task_handle()
        .expect("the original eager constructor must publish its actual Worker handle");
    let abort_alias = task.abort_handle();
    drop(worker);
    drop(channel);
    runtime.block_on(async {
        tokio::time::timeout(WATCHDOG, task)
            .await
            .expect("actual Worker future did not exit")
            .unwrap();
        tokio::time::timeout(WATCHDOG, peer)
            .await
            .expect("actual protocol/socket did not exit")
            .unwrap();
    });
    drop(runtime);
    assert!(matches!(
        NativeChannelCache::bounded(factory.clone()),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock
    ));
    // The real Worker future and actual IO have exited. This last AbortHandle
    // retains its completed TaskCell and must still protect the cache singleton.
    drop(abort_alias);
    let replacement = reactor.block_on(async {
        tokio::time::timeout(WATCHDOG, async {
            loop {
                match NativeChannelCache::bounded(factory.clone()) {
                    Ok(cache) => break cache,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => panic!("unexpected original cache refusal: {error}"),
                }
            }
        })
        .await
        .expect("last physical Worker alias did not release the singleton")
    });
    drop(replacement);
    drop(factory);
    reactor.block_on(async {
        tokio::time::timeout(WATCHDOG, async {
            loop {
                match budget.try_reserve_process(bytes).unwrap() {
                    ResultWriteAdmission::Granted(credit) => {
                        drop(credit);
                        break;
                    }
                    ResultWriteAdmission::Blocked => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("all actual original stock owners must exit after reactor progress");
    });
}
