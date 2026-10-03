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

//! An unsupported admission provider must fail before protocol/IO dispatch.
use bytes::Bytes;
use hyper::rt::{
    ClientRequestAdmission, ClientRequestAdmissionProvider, ClientRequestTaskLease, Executor,
};
use hyper_util::rt::TokioIo;
use std::{
    future::Future,
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

struct Provider(Arc<AtomicUsize>);
impl ClientRequestAdmissionProvider for Provider {
    fn try_acquire(&self, _: &hyper::http::Method) -> io::Result<ClientRequestTaskLease> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Err(io::ErrorKind::WouldBlock.into())
    }
}
#[derive(Clone)]
struct UnsupportedExecutor {
    admission: ClientRequestAdmission,
    dispatched: Arc<AtomicUsize>,
}
impl<F: Future<Output = ()> + 'static> Executor<F> for UnsupportedExecutor {
    fn execute(&self, _: F) {
        self.dispatched.fetch_add(1, Ordering::SeqCst);
    }
    fn client_request_admission(&self) -> io::Result<Option<ClientRequestAdmission>> {
        Ok(Some(self.admission.clone()))
    }
    // Intentionally retain the default false lease support.
}
struct PendingIo(Arc<AtomicUsize>);
impl AsyncRead for PendingIo {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        _: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}
impl AsyncWrite for PendingIo {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}
#[test]
fn actual_unsupported_admission_refuses_before_io_and_executor_dispatch() {
    let acquired = Arc::new(AtomicUsize::new(0));
    let dispatched = Arc::new(AtomicUsize::new(0));
    let io_polls = Arc::new(AtomicUsize::new(0));
    let executor = UnsupportedExecutor {
        admission: ClientRequestAdmission::new(Arc::new(Provider(acquired.clone())), Bytes::new()),
        dispatched: dispatched.clone(),
    };
    let builder = hyper::client::conn::http2::Builder::new(executor);
    let future =
        builder.handshake::<_, tonic::body::BoxBody>(TokioIo::new(PendingIo(io_polls.clone())));
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    let Poll::Ready(Err(error)) = future.as_mut().poll(&mut cx) else {
        panic!("unsupported lease admission must refuse on its first poll");
    };
    let mut source: &(dyn std::error::Error + 'static) = &error;
    let kind = loop {
        if let Some(error) = source.downcast_ref::<io::Error>() {
            break error.kind();
        }
        source = source.source().expect("preserved IO cause");
    };
    assert_eq!(kind, io::ErrorKind::Unsupported);
    assert_eq!(acquired.load(Ordering::SeqCst), 0);
    assert_eq!(dispatched.load(Ordering::SeqCst), 0);
    assert_eq!(io_polls.load(Ordering::SeqCst), 0);
}
