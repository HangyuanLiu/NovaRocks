// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! The actual post-connect publication owns IO through refusal and unwind.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::task::{Context, Poll};
use std::time::Duration;

use novarocks_types::{BackendProcessId, NativeEndpoint};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};

use super::*;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("dial exit fixture must finish")
}

async fn sockets(listener: &TcpListener) -> (TcpStream, TcpStream) {
    let (client, (peer, _)) = bounded(async {
        tokio::try_join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        )
    })
    .await
    .unwrap();
    (client, peer)
}

#[derive(Default)]
struct ExitFacts {
    used_positions: AtomicUsize,
    connecting_still_held: AtomicBool,
    socket_destroyed: AtomicBool,
}

struct ObservedSocket {
    socket: Option<TcpStream>,
    admission: NativeTransportAdmission,
    key: Option<InlineNativeChannelIdentity>,
    facts: Arc<ExitFacts>,
}

impl Drop for ObservedSocket {
    fn drop(&mut self) {
        // Record rather than assert here: an observer may already be unwinding.
        self.facts.used_positions.store(
            self.admission.positions(TransportClass::Data)
                - self.admission.available_positions(TransportClass::Data),
            Ordering::SeqCst,
        );
        if let Some(key) = self.key {
            match self.admission.core.connection_keys.claim(key) {
                Err(error) => self
                    .facts
                    .connecting_still_held
                    .store(error.kind() == io::ErrorKind::WouldBlock, Ordering::SeqCst),
                Ok(token) => {
                    // Keep the failing counterexample itself free of leaked rows.
                    let _ = self.admission.core.connection_keys.retire(token);
                    let _ = self.admission.core.connection_keys.exit(token);
                }
            }
        }
        drop(self.socket.take());
        self.facts.socket_destroyed.store(true, Ordering::SeqCst);
    }
}

impl AsyncRead for ObservedSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().socket.as_mut().unwrap()).poll_read(cx, buf)
    }
}
impl AsyncWrite for ObservedSocket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(self.get_mut().socket.as_mut().unwrap()).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().socket.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(self.get_mut().socket.as_mut().unwrap()).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn full_live_lane_refusal_destroys_actual_io_before_returning_original_positions() {
    let admission = NativeTransportAdmission::new().unwrap();
    let listener = bounded(TcpListener::bind("127.0.0.1:0")).await.unwrap();
    let endpoint = NativeEndpoint::from_socket_addr(listener.local_addr().unwrap());
    let key = InlineNativeChannelIdentity::from_parts(
        Some(BackendProcessId::new_v7()),
        &endpoint,
        NativeRpcMethod::TransmitRuntimeFilterEnvelope,
    )
    .unwrap();
    let first = admission.try_dial(TransportClass::Data, Some(key)).unwrap();
    let (socket, mut first_peer) = sockets(&listener).await;
    let first = first.established_io(Box::new(socket)).unwrap();
    assert_eq!(
        admission.available_handshakes(TransportClass::Data),
        admission.handshake_positions(TransportClass::Data)
    );
    let second = admission.try_dial(TransportClass::Data, Some(key)).unwrap();
    let (socket, mut second_peer) = sockets(&listener).await;
    let facts = Arc::new(ExitFacts::default());
    let error = second
        .established_io(Box::new(ObservedSocket {
            socket: Some(socket),
            admission: admission.clone(),
            key: Some(key),
            facts: Arc::clone(&facts),
        }))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(facts.used_positions.load(Ordering::SeqCst), 2);
    assert!(facts.connecting_still_held.load(Ordering::SeqCst));
    assert!(facts.socket_destroyed.load(Ordering::SeqCst));
    assert_eq!(bounded(second_peer.read(&mut [0])).await.unwrap(), 0);
    assert_eq!(
        admission.available_positions(TransportClass::Data),
        admission.positions(TransportClass::Data) - 1
    );
    drop(first);
    assert_eq!(bounded(first_peer.read(&mut [0])).await.unwrap(), 0);
    assert_eq!(
        admission.available_positions(TransportClass::Data),
        admission.positions(TransportClass::Data)
    );
}

struct PanicObserver {
    handshake: bool,
    armed: AtomicBool,
}
impl NativeTransportObserver for PanicObserver {
    fn positions(&self, _: TransportClass, kind: PositionKind, used: usize, _: usize) {
        if self.handshake
            && kind == PositionKind::Handshake
            && used == 0
            && self.armed.swap(false, Ordering::SeqCst)
        {
            panic_any(93_u8);
        }
    }
    fn lane_connections(&self, _: NativeLane, delta: i64) {
        if !self.handshake && delta == 1 && self.armed.swap(false, Ordering::SeqCst) {
            panic_any(93_u8);
        }
    }
}

#[tokio::test]
async fn observer_unwind_destroys_actual_io_before_returning_original_positions() {
    for handshake in [true, false] {
        let observer = Arc::new(PanicObserver {
            handshake,
            armed: AtomicBool::new(false),
        });
        let admission = NativeTransportAdmission::frontend(Some(observer.clone())).unwrap();
        let listener = bounded(TcpListener::bind("127.0.0.1:0")).await.unwrap();
        let dial = admission
            .try_dial_lane(FrontendNativeLane::ResultData)
            .unwrap();
        let (socket, mut peer) = sockets(&listener).await;
        let facts = Arc::new(ExitFacts::default());
        observer.armed.store(true, Ordering::SeqCst);
        let result = catch_unwind(AssertUnwindSafe(|| {
            dial.established_io(Box::new(ObservedSocket {
                socket: Some(socket),
                admission: admission.clone(),
                key: None,
                facts: Arc::clone(&facts),
            }))
        }));
        let cause = result.expect_err("original observer must unwind");
        assert_eq!(cause.downcast_ref::<u8>(), Some(&93));
        assert_eq!(facts.used_positions.load(Ordering::SeqCst), 1);
        assert!(facts.socket_destroyed.load(Ordering::SeqCst));
        assert_eq!(bounded(peer.read(&mut [0])).await.unwrap(), 0);
        assert_eq!(
            admission.available_positions(TransportClass::Data),
            admission.positions(TransportClass::Data)
        );
        assert_eq!(
            admission.available_handshakes(TransportClass::Data),
            admission.handshake_positions(TransportClass::Data)
        );
    }
}
