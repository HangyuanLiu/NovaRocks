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

//! Cloned Hyper configurations must preserve the local outbound frame cap and
//! original fixed writer owner. This is not a complete connection budget.

use bytes::Bytes;
use h2::SendFrameBuffer;
use hyper::body::{Body, Frame};
use hyper::http::{Request, Response};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use novarocks_execution::runtime::fragment::io::{ResultWriteAdmission, ResultWriteCredit};
use novarocks_worker::result_buffer::ResultRetainedBudget;
use std::convert::Infallible;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

struct OneBody(Option<Bytes>);
impl Body for OneBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(self.0.take().map(|bytes| Ok(Frame::data(bytes))))
    }
    fn is_end_stream(&self) -> bool {
        self.0.is_none()
    }
}

struct GatedBody {
    body: Option<Bytes>,
    gate: Option<tokio::sync::oneshot::Receiver<()>>,
}
impl Body for GatedBody {
    type Data = Bytes;
    type Error = Infallible;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if let Some(gate) = &mut self.gate {
            match Pin::new(gate).poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => result.expect("peer must acknowledge SETTINGS before DATA"),
            }
            self.gate = None;
        }
        Poll::Ready(self.body.take().map(|bytes| Ok(Frame::data(bytes))))
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_none()
    }
}

fn funded() -> (SendFrameBuffer, Arc<ResultRetainedBudget>, usize) {
    let bytes = SendFrameBuffer::allocation_capacity_bound(65536, 16384).unwrap()
        + Bytes::owner_with_exit_guard_metadata_size::<Bytes, ResultWriteCredit>();
    let budget = ResultRetainedBudget::new(NonZeroUsize::new(bytes).unwrap());
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("original fixed Hyper writer grant");
    };
    let buffer = SendFrameBuffer::new(
        65536,
        16384,
        Bytes::from_owner_with_exit_guard(Bytes::new(), credit),
    )
    .unwrap();
    (buffer, budget, bytes)
}
fn held(budget: &Arc<ResultRetainedBudget>) {
    assert!(matches!(
        budget.try_reserve_process(1).unwrap(),
        ResultWriteAdmission::Blocked
    ));
}
fn released(budget: &Arc<ResultRetainedBudget>, bytes: usize) {
    let ResultWriteAdmission::Granted(credit) = budget.try_reserve_process(bytes).unwrap() else {
        panic!("actual Hyper connection future must release the fixed writer");
    };
    drop(credit);
}
async fn sizes(mut stream: h2::RecvStream) -> Vec<usize> {
    let mut sizes = Vec::new();
    while let Some(bytes) = stream.data().await {
        let bytes = bytes.unwrap();
        sizes.push(bytes.len());
        stream.flow_control().release_capacity(bytes.len()).unwrap();
    }
    sizes
}

#[tokio::test]
async fn cloned_hyper_server_caps_peer_settings_and_keeps_original_writer_until_exit() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (buffer, budget, bytes) = funded();
        let (server_io, client_io) = tokio::io::duplex(65536);
        let mut builder = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        builder
            .send_frame_buffer(buffer.clone())
            .retain_data_payloads(true);
        let service = service_fn(|_: Request<hyper::body::Incoming>| async {
            Ok::<_, Infallible>(Response::new(OneBody(Some(Bytes::from(vec![0xab; 32769])))))
        });
        let connection = builder
            .clone()
            .serve_connection(TokioIo::new(server_io), service);
        drop(builder);
        drop(buffer);
        let server = tokio::spawn(connection);
        let mut peer = h2::client::Builder::new();
        peer.max_frame_size(16777215);
        let (mut sender, mut connection) = peer.handshake::<_, Bytes>(client_io).await.unwrap();
        let mut ping = connection.ping_pong().unwrap();
        let client = tokio::spawn(connection);
        // The matching PONG is after the peer received its preceding SETTINGS.
        ping.ping(h2::Ping::opaque()).await.unwrap();
        let (response, stream) = sender
            .send_request(
                Request::builder()
                    .uri("http://localhost/test")
                    .body(())
                    .unwrap(),
                true,
            )
            .unwrap();
        drop(stream);
        let response = response.await.unwrap();
        assert_eq!(sizes(response.into_body()).await, [16384, 16384, 1]);
        held(&budget);
        // Abort requests cancellation; awaiting the exact task observes actual
        // future Drop. Neither the abort call nor a logical EOF is the oracle.
        server.abort();
        assert!(server.await.unwrap_err().is_cancelled());
        released(&budget, bytes);
        drop(sender);
        client.await.unwrap().unwrap();
    })
    .await
    .expect("actual Hyper server fixture failed to progress");
}

#[tokio::test]
async fn cloned_hyper_client_caps_peer_settings_and_keeps_original_writer_until_exit() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (buffer, budget, bytes) = funded();
        let (server_io, client_io) = tokio::io::duplex(65536);
        let (release_data, data_ready) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut builder = h2::server::Builder::new();
            builder.max_frame_size(16777215);
            let mut connection = builder.handshake::<_, Bytes>(server_io).await.unwrap();
            let (request, mut response) = connection.accept().await.unwrap().unwrap();
            let mut ping = connection.ping_pong().unwrap();
            let ack = ping.ping(h2::Ping::opaque());
            tokio::pin!(ack);
            tokio::select! {
                result = &mut ack => { result.unwrap(); },
                other = connection.accept() => {
                    assert!(other.is_none(), "fixture owns one request");
                    panic!("peer exited before SETTINGS progress proof");
                }
            }
            release_data.send(()).unwrap();
            let read = sizes(request.into_body());
            tokio::pin!(read);
            let seen = tokio::select! {
                sizes = &mut read => sizes,
                other = connection.accept() => {
                    assert!(other.is_none(), "fixture owns one request");
                    panic!("peer exited before actual request DATA");
                }
            };
            assert_eq!(seen, [16384, 16384, 1]);
            response.send_response(Response::new(()), true).unwrap();
            while connection.accept().await.is_some() {}
        });
        let mut builder = hyper::client::conn::http2::Builder::new(TokioExecutor::new());
        builder
            .send_frame_buffer(buffer.clone())
            .retain_data_payloads(true);
        let (mut sender, connection) = builder
            .clone()
            .handshake(TokioIo::new(client_io))
            .await
            .unwrap();
        drop(builder);
        drop(buffer);
        let client = tokio::spawn(connection);
        let response = sender
            .send_request(
                Request::builder()
                    .uri("http://localhost/test")
                    .body(GatedBody {
                        body: Some(Bytes::from(vec![0xcd; 32769])),
                        gate: Some(data_ready),
                    })
                    .unwrap(),
            )
            .await
            .unwrap();
        drop(response);
        held(&budget);
        client.abort();
        assert!(client.await.unwrap_err().is_cancelled());
        released(&budget, bytes);
        drop(sender);
        peer.await.unwrap();
    })
    .await
    .expect("actual Hyper client fixture failed to progress");
}
