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
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use tonic::Request;
use tonic::transport::Channel;

use crate::common::network::format_host_for_url;
use crate::common::types::UniqueId;
use crate::novarocks_logging::error;
use crate::runtime::global_async_runtime::{data_block_on, data_runtime_handle};

pub use crate::proto;

/// gRPC client for NovaRocks BE-to-BE and coordinator RPCs.
///
/// Wraps the tonic async client with blocking wrappers so that PR-4's
/// `RemoteDispatcher` can drive it from a non-async context.  One
/// `NovaRocksGrpcRemoteClient` per remote BE address; callers are
/// responsible for caching instances.
pub struct NovaRocksGrpcRemoteClient {
    host: String,
    port: u16,
}

impl NovaRocksGrpcRemoteClient {
    /// Create a client for `addr`.
    ///
    /// The underlying HTTP/2 channel is established lazily via the shared
    /// channel cache, so construction itself is cheap.
    pub fn new(addr: SocketAddr) -> Result<Self, String> {
        let host = addr.ip().to_string();
        let port = addr.port();
        // Eagerly verify the endpoint can be parsed; actual TCP setup is lazy.
        channel_endpoint(&host, port)
            .map_err(|e| format!("invalid BE endpoint {host}:{port}: {e}"))?;
        Ok(Self { host, port })
    }

    /// Connect to `addr` and return a ready client.
    ///
    /// The underlying HTTP/2 channel is established lazily via the shared
    /// channel cache, so the connect itself is cheap.
    pub fn connect_blocking(addr: SocketAddr) -> Result<Self, String> {
        Self::new(addr)
    }

    fn make_client(
        &self,
    ) -> Result<proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient<Channel>, String>
    {
        let host = self.host.clone();
        let port = self.port;
        let ch = data_block_on(async move { get_or_create_channel(&host, port).await })??;
        Ok(Self::client_from_channel(ch))
    }

    async fn make_async_client(
        &self,
    ) -> Result<proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient<Channel>, String>
    {
        let ch = get_or_create_channel(&self.host, self.port).await?;
        Ok(Self::client_from_channel(ch))
    }

    fn client_from_channel(
        ch: Channel,
    ) -> proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient<Channel> {
        proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::new(ch)
            .max_encoding_message_size(GRPC_MAX_ENCODING_BYTES)
            .max_decoding_message_size(GRPC_MAX_DECODING_BYTES)
    }

    pub fn blocking_submit_fragment(
        &self,
        req: proto::novarocks::SubmitFragmentRequest,
    ) -> Result<proto::novarocks::SubmitFragmentResponse, String> {
        let mut cli = self.make_client()?;
        data_block_on(async move {
            cli.submit_fragment(req)
                .await
                .map(|r| r.into_inner())
                .map_err(|e| format!("submit_fragment rpc failed: {e}"))
        })?
    }

    pub fn blocking_fetch_result(
        &self,
        req: proto::novarocks::FetchResultRequest,
    ) -> Result<proto::novarocks::FetchResultResponse, String> {
        let mut cli = self.make_client()?;
        data_block_on(async move {
            cli.fetch_result(req)
                .await
                .map(|r| r.into_inner())
                .map_err(|e| format!("fetch_result rpc failed: {e}"))
        })?
    }

    pub fn blocking_cancel_fragment(
        &self,
        req: proto::novarocks::CancelFragmentRequest,
    ) -> Result<proto::novarocks::CancelFragmentResponse, String> {
        let mut cli = self.make_client()?;
        data_block_on(async move {
            cli.cancel_fragment(req)
                .await
                .map(|r| r.into_inner())
                .map_err(|e| format!("cancel_fragment rpc failed: {e}"))
        })?
    }

    pub fn blocking_report_exec_status(
        &self,
        req: proto::novarocks::ReportExecStatusRequest,
    ) -> Result<proto::novarocks::ReportExecStatusResponse, String> {
        let mut cli = self.make_client()?;
        data_block_on(async move {
            cli.report_exec_status(req)
                .await
                .map(|r| r.into_inner())
                .map_err(|e| format!("report_exec_status rpc failed: {e}"))
        })?
    }

    pub fn blocking_batch_report_exec_status(
        &self,
        req: proto::novarocks::BatchReportExecStatusRequest,
    ) -> Result<proto::novarocks::BatchReportExecStatusResponse, String> {
        let mut cli = self.make_client()?;
        data_block_on(async move {
            cli.batch_report_exec_status(req)
                .await
                .map(|r| r.into_inner())
                .map_err(|e| format!("batch_report_exec_status rpc failed: {e}"))
        })?
    }

    pub async fn cancel_fragment_async(
        &self,
        req: proto::novarocks::CancelFragmentRequest,
    ) -> Result<proto::novarocks::CancelFragmentResponse, String> {
        let mut cli = self.make_async_client().await?;
        let mut req = Request::new(req);
        req.set_timeout(Duration::from_secs(3));
        cli.cancel_fragment(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| format!("cancel_fragment rpc failed: {e}"))
    }

    pub async fn heartbeat_async(
        &self,
        req: proto::novarocks::HeartbeatRequest,
    ) -> Result<proto::novarocks::HeartbeatResponse, String> {
        let mut cli = self.make_async_client().await?;
        let mut req = Request::new(req);
        req.set_timeout(Duration::from_secs(3));
        cli.heartbeat(req)
            .await
            .map(|r| r.into_inner())
            .map_err(|e| format!("heartbeat rpc failed: {e}"))
    }

    pub fn blocking_heartbeat(
        &self,
        req: proto::novarocks::HeartbeatRequest,
    ) -> Result<proto::novarocks::HeartbeatResponse, String> {
        let mut cli = self.make_client()?;
        data_block_on(async move {
            let mut req = Request::new(req);
            req.set_timeout(Duration::from_secs(3));
            cli.heartbeat(req)
                .await
                .map(|r| r.into_inner())
                .map_err(|e| format!("heartbeat rpc failed: {e}"))
        })?
    }
}

const GRPC_MAX_ENCODING_BYTES: usize = 64 * 1024 * 1024;
const GRPC_MAX_DECODING_BYTES: usize = 64 * 1024 * 1024;

#[derive(Default)]
struct ChannelCache {
    mu: Mutex<HashMap<String, Channel>>,
}

static CHANNELS: OnceLock<ChannelCache> = OnceLock::new();

fn channels() -> &'static ChannelCache {
    CHANNELS.get_or_init(|| ChannelCache {
        mu: Mutex::new(HashMap::new()),
    })
}

fn channel_endpoint_uri(host: &str, port: u16) -> String {
    format!("http://{}:{port}", format_host_for_url(host))
}

fn channel_endpoint(
    host: &str,
    port: u16,
) -> Result<tonic::transport::Endpoint, tonic::transport::Error> {
    channel_endpoint_uri(host, port).parse::<tonic::transport::Endpoint>()
}

/// Return a cached channel for the given endpoint, creating one if needed.
///
/// Must be called from within an async Tokio context (inside data_block_on or
/// a spawned task), because `connect()` drives TCP+HTTP2 setup via the reactor.
/// One channel per (host, port) is sufficient — HTTP/2 multiplexes all
/// concurrent RPCs over the single connection.
async fn get_or_create_channel(host: &str, port: u16) -> Result<Channel, String> {
    let key = format!("{}:{port}", format_host_for_url(host));
    {
        let guard = channels().mu.lock().expect("channel cache lock");
        if let Some(ch) = guard.get(&key).cloned() {
            return Ok(ch);
        }
    }
    let ch = channel_endpoint(host, port)
        .map_err(|e| format!("invalid endpoint: {e}"))?
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .timeout(Duration::from_secs(600))
        .connect_timeout(Duration::from_secs(10))
        .http2_adaptive_window(true)
        .initial_stream_window_size(Some(32 * 1024 * 1024))
        .initial_connection_window_size(Some(128 * 1024 * 1024))
        .connect()
        .await
        .map_err(|e| format!("connect exchange endpoint failed: {e}"))?;
    channels()
        .mu
        .lock()
        .expect("channel cache lock")
        .insert(key, ch.clone());
    Ok(ch)
}

#[cfg(test)]
mod pr3_tests {
    use super::*;

    #[test]
    fn remote_client_connect_accepts_socket_addr() {
        let addr: SocketAddr = "127.0.0.1:19030".parse().expect("valid addr");
        let client = NovaRocksGrpcRemoteClient::connect_blocking(addr)
            .expect("connect wrapper should accept SocketAddr");
        assert_eq!(client.host, "127.0.0.1");
        assert_eq!(client.port, 19030);
    }

    #[test]
    fn channel_endpoint_uri_formats_ipv4_and_ipv6_hosts() {
        assert_eq!(
            channel_endpoint_uri("127.0.0.1", 9070),
            "http://127.0.0.1:9070"
        );
        assert_eq!(channel_endpoint_uri("::1", 9070), "http://[::1]:9070");
    }

    #[test]
    fn remote_client_connect_accepts_ipv6_socket_addr() {
        let addr: SocketAddr = "[::1]:19030".parse().expect("valid ipv6 addr");
        let client = NovaRocksGrpcRemoteClient::connect_blocking(addr)
            .expect("connect wrapper should accept IPv6 SocketAddr");
        assert_eq!(client.host, "::1");
        assert_eq!(client.port, 19030);
    }
}

/// Synchronous exchange send — blocks until the server acknowledges receipt.
///
/// Each call opens a single-message gRPC stream, sends the request, and waits
/// for the server ack before returning.  This matches the delivery guarantee of
/// the brpc path and ensures `ExchangeSendTracker::on_complete` fires only
/// after the data has actually been received by the exchange registry.
pub fn send_chunks(
    dest_host: &str,
    dest_port: u16,
    finst_id: UniqueId,
    node_id: i32,
    sender_id: i32,
    be_number: i32,
    eos: bool,
    sequence: i64,
    payload: Vec<u8>,
) -> Result<(), String> {
    let host = dest_host.to_string();
    let port = dest_port;
    let req = proto::novarocks::ExchangeRequest {
        finst_id_hi: finst_id.hi,
        finst_id_lo: finst_id.lo,
        node_id,
        sender_id,
        be_number,
        eos,
        sequence,
        payload,
    };

    data_block_on(async move {
        let ch = get_or_create_channel(&host, port).await?;
        let mut cli = proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::new(ch)
            .max_encoding_message_size(64 * 1024 * 1024)
            .max_decoding_message_size(64 * 1024 * 1024);

        let response = cli
            .exchange_unary(req)
            .await
            .map_err(|e| format!("exchange rpc failed: {e}"))?
            .into_inner();
        if let Some(status) = response.status.as_ref()
            && status.code != 0
        {
            return Err(if status.message.is_empty() {
                format!("exchange rpc returned status_code={}", status.code)
            } else {
                format!("exchange rpc failed: {}", status.message)
            });
        }
        Ok(())
    })?
}

pub fn transmit_runtime_filter(
    dest_host: &str,
    dest_port: u16,
    params: proto::filter::TransmitRuntimeFilterRequest,
) -> Result<(), String> {
    let dest_host = dest_host.to_string();
    let port = dest_port;
    let runtime_handle = data_runtime_handle()?;
    runtime_handle.spawn(async move {
        let ch = match get_or_create_channel(&dest_host, port).await {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "runtime filter connect failed: dest={}:{} error={}",
                    dest_host, port, e
                );
                return;
            }
        };
        let mut cli = proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::new(ch)
            .max_encoding_message_size(64 * 1024 * 1024)
            .max_decoding_message_size(64 * 1024 * 1024);
        match cli.transmit_runtime_filter(params).await {
            Ok(resp) => {
                if let Some(status) = resp.get_ref().status.as_ref()
                    && status.code != 0
                {
                    error!(
                        "runtime filter send failed: dest={}:{} code={} message={}",
                        dest_host, port, status.code, status.message
                    );
                }
            }
            Err(e) => {
                error!(
                    "runtime filter send failed: dest={}:{} error={}",
                    dest_host, port, e
                );
            }
        }
    });
    Ok(())
}

pub fn lookup(
    dest_host: &str,
    dest_port: u16,
    params: proto::filter::LookupRequest,
) -> Result<proto::filter::LookupResponse, String> {
    #[cfg(test)]
    if let Some(result) = maybe_lookup_hook(dest_host, dest_port, params.clone()) {
        return result;
    }

    let dest_host = dest_host.to_string();
    let port = dest_port;
    data_block_on(async move {
        let ch = get_or_create_channel(&dest_host, port)
            .await
            .map_err(|e| format!("lookup connect failed: dest={dest_host}:{port} error={e}"))?;
        let mut cli = proto::novarocks::nova_rocks_grpc_client::NovaRocksGrpcClient::new(ch)
            .max_encoding_message_size(64 * 1024 * 1024)
            .max_decoding_message_size(64 * 1024 * 1024);
        let resp = cli
            .lookup(params)
            .await
            .map_err(|e| format!("lookup request failed: dest={dest_host}:{port} error={e}"))?;
        Ok(resp.into_inner())
    })
    .map_err(|e| format!("lookup runtime execution failed: {e}"))?
}

#[cfg(test)]
type LookupHook = std::sync::Arc<
    dyn Fn(&str, u16, proto::filter::LookupRequest) -> Result<proto::filter::LookupResponse, String>
        + Send
        + Sync,
>;

#[cfg(test)]
fn lookup_hook() -> &'static Mutex<Option<LookupHook>> {
    static HOOK: OnceLock<Mutex<Option<LookupHook>>> = OnceLock::new();
    HOOK.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
fn test_hook_mutex() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[cfg(test)]
fn maybe_lookup_hook(
    host: &str,
    port: u16,
    params: proto::filter::LookupRequest,
) -> Option<Result<proto::filter::LookupResponse, String>> {
    let hook = lookup_hook().lock().expect("lookup hook lock").clone();
    hook.map(|hook| hook(host, port, params))
}

#[cfg(test)]
pub(crate) fn test_hook_lock() -> std::sync::MutexGuard<'static, ()> {
    test_hook_mutex().lock().expect("test hook global lock")
}

#[cfg(test)]
pub(crate) fn clear_test_hooks() {
    *lookup_hook().lock().expect("lookup hook lock") = None;
}

#[cfg(test)]
pub(crate) fn set_lookup_hook<F>(hook: F)
where
    F: Fn(&str, u16, proto::filter::LookupRequest) -> Result<proto::filter::LookupResponse, String>
        + Send
        + Sync
        + 'static,
{
    *lookup_hook().lock().expect("lookup hook lock") = Some(std::sync::Arc::new(hook));
}

#[cfg(test)]
mod lookup_tests {
    use super::*;
    use crate::runtime::global_async_runtime::data_block_on;
    use crate::service::grpc_server::GrpcService;

    fn spawn_lookup_server() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind lookup server");
        let addr = listener.local_addr().expect("lookup server local addr");
        data_block_on(async move {
            listener
                .set_nonblocking(true)
                .expect("set lookup server nonblocking");
            let listener = tokio::net::TcpListener::from_std(listener).expect("tokio listener");
            let incoming = futures::stream::unfold(listener, |listener| async {
                let item = listener.accept().await.map(|(stream, _)| stream);
                Some((item, listener))
            });
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(
                        proto::novarocks::nova_rocks_grpc_server::NovaRocksGrpcServer::new(
                            GrpcService::full_execution(),
                        ),
                    )
                    .serve_with_incoming(incoming),
            );
        })
        .expect("spawn lookup server");

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(50))
                .is_ok()
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "lookup grpc server did not become ready at {addr}"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        addr
    }

    #[test]
    fn test_lookup_uses_native_tonic_server_without_hook() {
        let _hook_guard = test_hook_lock();
        clear_test_hooks();
        let addr = spawn_lookup_server();

        let response = lookup(
            "127.0.0.1",
            addr.port(),
            proto::filter::LookupRequest {
                query_id: None,
                lookup_node_id: 77,
                request_tuple_id: 1,
                request_columns: Vec::new(),
            },
        )
        .expect("lookup rpc should return native response");

        let status = response.status.expect("lookup response status");
        assert_ne!(status.code, 0);
        assert!(status.message.contains("missing query_id for lookup"));
        clear_test_hooks();
    }
}
