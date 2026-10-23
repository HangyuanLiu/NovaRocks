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

use super::super::route::HttpRouteConfig;
use super::*;
use hyper_util::client::proxy::matcher::Matcher;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Duration, timeout};

struct NoDns;
impl HttpDnsPort for NoDns {
    type Addresses = [SocketAddr; 0];
    async fn resolve(&self, _: &HttpOrigin) -> FileResult<Self::Addresses> {
        panic!("literal route must not resolve DNS")
    }
}
struct Guard(Arc<AtomicBool>);
impl Drop for Guard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
fn has_proxy_authorization(header: &str) -> bool {
    header.lines().any(|line| matches!(line.split_once(':'), Some((name, value))
        if name.eq_ignore_ascii_case("proxy-authorization") && value.trim() == "Basic dXNlcjpwYXNzd29yZA=="))
}
fn config() -> Arc<ClientConfig> {
    Arc::new(
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth(),
    )
}

#[tokio::test]
async fn actual_plain_connection_keeps_guard_until_returned_owned_io_drops() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let uri = format!(
            "http://{}/actual/path?secret=query",
            listener.local_addr().unwrap()
        )
        .parse()
        .unwrap();
        let route = HttpRouteConfig::local().prepare(&uri).unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let connector = HttpConnector::new(NoDns, HttpTlsBackend::rustls(config()));
        let mut connection = connector
            .connect(&route, Guard(Arc::clone(&dropped)))
            .await
            .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        assert_eq!(connection.negotiated_protocol, NegotiatedHttpProtocol::H1);
        assert_eq!(connection.request_target_form, RequestTargetForm::Origin);
        connection.io.write_all(b"real-io").await.unwrap();
        let mut bytes = [0; 7];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"real-io");
        assert!(!dropped.load(Ordering::SeqCst));
        drop(connection);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(peer.read(&mut bytes).await.unwrap(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn connect_auth_stays_at_proxy_and_origin_tls_failure_releases_socket_guard() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let matcher = Matcher::builder()
            .all(format!(
                "http://user:password@{}",
                listener.local_addr().unwrap()
            ))
            .build();
        let route = HttpRouteConfig::with_matcher(matcher)
            .prepare(
                &"https://127.0.0.1:443/object?signature=secret"
                    .parse()
                    .unwrap(),
            )
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let server_dropped = Arc::clone(&dropped);
        let proxy = async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                assert!(header.len() < 8192);
                let mut byte = [0];
                stream.read_exact(&mut byte).await.unwrap();
                header.push(byte[0]);
            }
            let text = String::from_utf8(header).unwrap();
            assert!(text.starts_with("CONNECT 127.0.0.1:443 HTTP/1.1\r\n"));
            assert!(has_proxy_authorization(&text));
            assert!(!text.contains("signature") && !text.contains("/object"));
            assert!(!server_dropped.load(Ordering::SeqCst));
            stream
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();
            let mut client_hello = [0; 5];
            stream.read_exact(&mut client_hello).await.unwrap();
            assert_eq!(client_hello[0], 22);
            assert!(!server_dropped.load(Ordering::SeqCst));
            // Closing now makes the real origin TLS handshake fail; no fake TLS verifier.
        };
        let connector = HttpConnector::new(NoDns, HttpTlsBackend::rustls(config()));
        let (connected, ()) = tokio::join!(
            connector.connect(&route, Guard(Arc::clone(&dropped))),
            proxy
        );
        assert!(connected.is_err());
        assert!(dropped.load(Ordering::SeqCst));
        assert!(route.forward_proxy_authorization().is_none());
    })
    .await
    .unwrap();
}

#[test]
fn abandoned_connection_future_drops_before_its_guard_even_during_unwind() {
    use std::sync::Mutex;
    struct PendingSocket {
        order: Arc<Mutex<Vec<&'static str>>>,
        panics: bool,
    }
    impl Future for PendingSocket {
        type Output = io::Result<TcpStream>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }
    impl Drop for PendingSocket {
        fn drop(&mut self) {
            self.order.lock().unwrap().push("socket-future");
            if self.panics {
                panic!("socket future drop");
            }
        }
    }
    struct OrderedGuard(Arc<Mutex<Vec<&'static str>>>);
    impl Drop for OrderedGuard {
        fn drop(&mut self) {
            self.0.lock().unwrap().push("guard");
        }
    }
    for panics in [false, true] {
        let order = Arc::new(Mutex::new(Vec::new()));
        let retained = RetainedFuture::new(
            PendingSocket {
                order: Arc::clone(&order),
                panics,
            },
            Arc::new(OrderedGuard(Arc::clone(&order))),
        );
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(retained)));
        assert_eq!(result.is_err(), panics);
        assert_eq!(*order.lock().unwrap(), ["socket-future", "guard"]);
    }
}

#[tokio::test]
async fn cancelling_real_connect_handshake_releases_only_after_future_drop() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let matcher = Matcher::builder()
            .all(format!("http://{}", listener.local_addr().unwrap()))
            .build();
        let route = HttpRouteConfig::with_matcher(matcher)
            .prepare(&"https://127.0.0.1:443/object".parse().unwrap())
            .unwrap();
        let dropped = Arc::new(AtomicBool::new(false));
        let connector = HttpConnector::new(NoDns, HttpTlsBackend::rustls(config()));
        let mut connecting = Box::pin(connector.connect(&route, Guard(Arc::clone(&dropped))));
        let proxy = async {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                assert!(header.len() < 8192);
                let mut byte = [0];
                stream.read_exact(&mut byte).await.unwrap();
                header.push(byte[0]);
            }
            stream
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();
            let mut hello = [0; 5];
            stream.read_exact(&mut hello).await.unwrap();
            assert_eq!(hello[0], 22);
            stream
        };
        let mut peer = tokio::select! {
            _ = &mut connecting => panic!("TLS must remain pending before server handshake"),
            peer = proxy => peer,
        };
        assert!(!dropped.load(Ordering::SeqCst));
        drop(connecting);
        assert!(dropped.load(Ordering::SeqCst));
        let mut rest = Vec::new();
        peer.read_to_end(&mut rest).await.unwrap();
    })
    .await
    .unwrap();
}

struct FixedDns(SocketAddr);
impl HttpDnsPort for FixedDns {
    type Addresses = [SocketAddr; 1];
    async fn resolve(&self, origin: &HttpOrigin) -> FileResult<Self::Addresses> {
        assert!(matches!(origin.host(), ReferenceHost::Dns(_)));
        assert_eq!(origin.port(), self.0.port());
        Ok([self.0])
    }
}
fn tls_fixture(
    names: Vec<String>,
    alpn: &[&[u8]],
) -> (
    Arc<ClientConfig>,
    Arc<rustls::ServerConfig>,
    native_tls::TlsConnector,
) {
    let rcgen::CertifiedKey { cert, key_pair } = rcgen::generate_simple_self_signed(names).unwrap();
    let mut native = native_tls::TlsConnector::builder();
    native
        .disable_built_in_roots(true)
        .add_root_certificate(native_tls::Certificate::from_der(cert.der()).unwrap());
    let native = native.build().unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.der().clone()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let client = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let mut server = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.der().clone()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(key_pair.serialize_der()).into(),
        )
        .unwrap();
    server.alpn_protocols = alpn.iter().map(|value| value.to_vec()).collect();
    (Arc::new(client), Arc::new(server), native)
}

#[tokio::test]
async fn real_tls_verifies_reference_host_and_selects_h2() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client_tls, server_tls, _) =
            tls_fixture(vec!["origin.example".into()], &[b"h2", b"http/1.1"]);
        let uri = format!(
            "https://origin.example:{}/object?signature=secret",
            address.port()
        )
        .parse()
        .unwrap();
        let route = HttpRouteConfig::local().prepare(&uri).unwrap();
        let connector = HttpConnector::new(FixedDns(address), HttpTlsBackend::rustls(client_tls));
        let dropped = Arc::new(AtomicBool::new(false));
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = tokio_rustls::TlsAcceptor::from(server_tls)
                .accept(socket)
                .await
                .unwrap();
            assert_eq!(stream.get_ref().1.server_name(), Some("origin.example"));
            assert_eq!(stream.get_ref().1.alpn_protocol(), Some(b"h2".as_slice()));
            let mut byte = [0];
            stream.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte, [42]);
        };
        let client = async {
            let mut connected = connector
                .connect(&route, Guard(Arc::clone(&dropped)))
                .await
                .unwrap();
            assert_eq!(connected.negotiated_protocol, NegotiatedHttpProtocol::H2);
            assert_eq!(connected.request_target_form, RequestTargetForm::Origin);
            assert!(!dropped.load(Ordering::SeqCst));
            connected.io.write_all(&[42]).await.unwrap();
            drop(connected);
            assert!(dropped.load(Ordering::SeqCst));
        };
        tokio::join!(client, server);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn real_tls_rejects_wrong_reference_host_without_disabling_certificate_checks() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client_tls, server_tls, _) = tls_fixture(vec!["origin.example".into()], &[b"h2"]);
        let route = HttpRouteConfig::local()
            .prepare(
                &format!("https://wrong.example:{}/object", address.port())
                    .parse()
                    .unwrap(),
            )
            .unwrap();
        let connector = HttpConnector::new(FixedDns(address), HttpTlsBackend::rustls(client_tls));
        let dropped = Arc::new(AtomicBool::new(false));
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            assert!(
                tokio_rustls::TlsAcceptor::from(server_tls)
                    .accept(socket)
                    .await
                    .is_err()
            );
        };
        let (result, ()) = tokio::join!(
            connector.connect(&route, Guard(Arc::clone(&dropped))),
            server
        );
        assert!(result.is_err());
        assert!(dropped.load(Ordering::SeqCst));
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn https_proxy_keeps_proxy_h1_and_origin_h2_sni_separate() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client_tls, server_tls, _) = tls_fixture(
            vec!["proxy.example".into(), "origin.example".into()],
            &[b"h2", b"http/1.1"],
        );
        let matcher = Matcher::builder()
            .all(format!(
                "https://user:password@proxy.example:{}",
                address.port()
            ))
            .build();
        let route = HttpRouteConfig::with_matcher(matcher)
            .prepare(
                &"https://origin.example/object?signature=secret"
                    .parse()
                    .unwrap(),
            )
            .unwrap();
        let connector = HttpConnector::new(FixedDns(address), HttpTlsBackend::rustls(client_tls));
        let dropped = Arc::new(AtomicBool::new(false));
        let server = async {
            let (socket, _) = listener.accept().await.unwrap();
            let acceptor = tokio_rustls::TlsAcceptor::from(server_tls);
            let mut proxy_tls = acceptor.accept(socket).await.unwrap();
            assert_eq!(proxy_tls.get_ref().1.server_name(), Some("proxy.example"));
            assert_eq!(
                proxy_tls.get_ref().1.alpn_protocol(),
                Some(b"http/1.1".as_slice())
            );
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                assert!(header.len() < 8192);
                let mut byte = [0];
                proxy_tls.read_exact(&mut byte).await.unwrap();
                header.push(byte[0]);
            }
            let header = String::from_utf8(header).unwrap();
            assert!(header.starts_with("CONNECT origin.example:443 HTTP/1.1\r\n"));
            assert!(has_proxy_authorization(&header));
            assert!(!header.contains("signature") && !header.contains("/object"));
            proxy_tls
                .write_all(b"HTTP/1.1 200 Connection established\r\n\r\n")
                .await
                .unwrap();
            let mut origin_tls = acceptor.accept(proxy_tls).await.unwrap();
            assert_eq!(origin_tls.get_ref().1.server_name(), Some("origin.example"));
            assert_eq!(
                origin_tls.get_ref().1.alpn_protocol(),
                Some(b"h2".as_slice())
            );
            let mut byte = [0];
            origin_tls.read_exact(&mut byte).await.unwrap();
            assert_eq!(byte, [42]);
        };
        let client = async {
            let mut connected = connector
                .connect(&route, Guard(Arc::clone(&dropped)))
                .await
                .unwrap();
            assert_eq!(connected.negotiated_protocol, NegotiatedHttpProtocol::H2);
            assert_eq!(connected.request_target_form, RequestTargetForm::Origin);
            assert!(route.forward_proxy_authorization().is_none());
            assert!(!dropped.load(Ordering::SeqCst));
            connected.io.write_all(&[42]).await.unwrap();
            drop(connected);
            assert!(dropped.load(Ordering::SeqCst));
        };
        tokio::join!(client, server);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_process_backend_keeps_certificate_and_hostname_checks_without_claiming_h2() {
    timeout(Duration::from_secs(10), async {
        for good_name in [true, false] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (_, server_tls, native) =
                tls_fixture(vec!["origin.example".into()], &[b"h2", b"http/1.1"]);
            let hostname = if good_name {
                "origin.example"
            } else {
                "wrong.example"
            };
            let route = HttpRouteConfig::local()
                .prepare(
                    &format!("https://{hostname}:{}/object", address.port())
                        .parse()
                        .unwrap(),
                )
                .unwrap();
            let connector =
                HttpConnector::new(FixedDns(address), HttpTlsBackend::native_h1(native));
            let dropped = Arc::new(AtomicBool::new(false));
            let server = async {
                let (socket, _) = listener.accept().await.unwrap();
                let accepted = tokio_rustls::TlsAcceptor::from(server_tls)
                    .accept(socket)
                    .await;
                if good_name {
                    let mut stream = accepted.unwrap();
                    assert_eq!(stream.get_ref().1.server_name(), Some("origin.example"));
                    assert_eq!(stream.get_ref().1.alpn_protocol(), None);
                    let mut byte = [0];
                    stream.read_exact(&mut byte).await.unwrap();
                    assert_eq!(byte, [42]);
                } else {
                    assert!(accepted.is_err());
                }
            };
            let client = async {
                let result = connector.connect(&route, Guard(Arc::clone(&dropped))).await;
                if good_name {
                    let mut connected = result.unwrap();
                    assert_eq!(connected.negotiated_protocol, NegotiatedHttpProtocol::H1);
                    assert!(!dropped.load(Ordering::SeqCst));
                    connected.io.write_all(&[42]).await.unwrap();
                    drop(connected);
                } else {
                    assert!(result.is_err());
                }
                assert!(dropped.load(Ordering::SeqCst));
            };
            tokio::join!(client, server);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn resolver_cannot_redirect_the_reference_port() {
    struct WrongPort;
    impl HttpDnsPort for WrongPort {
        type Addresses = [SocketAddr; 1];
        async fn resolve(&self, _: &HttpOrigin) -> FileResult<Self::Addresses> {
            Ok(["127.0.0.1:1".parse().unwrap()])
        }
    }
    let route = HttpRouteConfig::local()
        .prepare(&"http://origin.example:2/object".parse().unwrap())
        .unwrap();
    let connector = HttpConnector::new(WrongPort, HttpTlsBackend::rustls(config()));
    let dropped = Arc::new(AtomicBool::new(false));
    let result = connector.connect(&route, Guard(Arc::clone(&dropped))).await;
    assert!(matches!(result, Err(error) if error.kind() == FileErrorKind::Internal));
    assert!(dropped.load(Ordering::SeqCst));
}

async fn tunnel_for_test(
    io: novarocks_transport_io::BoxedIo,
    route: &PreparedHttpRoute,
    dropped: Arc<AtomicBool>,
) -> FileResult<OwnedIo> {
    let guard = Arc::new(Guard(dropped));
    let io = OwnedIo::with_guard(io, Arc::clone(&guard));
    let (result, guard) = RetainedFuture::new(connect_tunnel(io, route, Arc::clone(&guard)), guard)
        .finish()
        .await;
    drop(guard);
    result
}
async fn connect_header<R: AsyncRead + Unpin>(io: &mut R) -> Vec<u8> {
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        assert!(header.len() < 8192);
        let mut byte = [0];
        io.read_exact(&mut byte).await.unwrap();
        header.push(byte[0]);
    }
    header
}
struct FirstByteWitness {
    io: TcpStream,
    observed: Arc<tokio::sync::Notify>,
    seen: bool,
}
impl AsyncRead for FirstByteWitness {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut this.io).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() != before && !this.seen {
            assert_eq!(buf.filled().len() - before, 1);
            this.seen = true;
            this.observed.notify_one();
        }
        result
    }
}
impl AsyncWrite for FirstByteWitness {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, bytes)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

#[tokio::test]
async fn public_parser_accepts_a_real_one_byte_first_tcp_fragment() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let matcher = Matcher::builder()
            .all(format!("http://user:password@{address}"))
            .build();
        let route = HttpRouteConfig::with_matcher(matcher)
            .prepare(
                &"https://origin.example/object?signature=secret"
                    .parse()
                    .unwrap(),
            )
            .unwrap();
        let observed = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let socket = TcpStream::connect(address).await.unwrap();
        let client = async {
            let witnessed = FirstByteWitness {
                io: socket,
                observed: Arc::clone(&observed),
                seen: false,
            };
            let mut tunnel = tunnel_for_test(Box::new(witnessed), &route, Arc::clone(&dropped))
                .await
                .unwrap();
            let mut banner = [0; 10];
            tunnel.read_exact(&mut banner).await.unwrap();
            assert_eq!(&banner, b"prefetched");
            assert!(!dropped.load(Ordering::SeqCst));
            tunnel.write_all(b"x").await.unwrap();
            drop(tunnel);
            assert!(dropped.load(Ordering::SeqCst));
        };
        let server = async {
            let (mut peer, _) = listener.accept().await.unwrap();
            let request = String::from_utf8(connect_header(&mut peer).await).unwrap();
            assert!(request.starts_with("CONNECT origin.example:443 HTTP/1.1\r\n"));
            assert!(has_proxy_authorization(&request));
            assert!(!request.contains("signature") && !request.contains("/object"));
            peer.write_all(b"H").await.unwrap();
            // This acknowledgment is issued only after the parser's actual TCP read
            // returned that single byte; the rest cannot be coalesced with that read.
            observed.notified().await;
            peer.write_all(b"TTP/1.1 204 Connection established\r\n\r\nprefetched")
                .await
                .unwrap();
            let mut byte = [0];
            peer.read_exact(&mut byte).await.unwrap();
            assert_eq!(&byte, b"x");
        };
        tokio::join!(client, server);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn successful_connect_replays_read_ahead_bytes_for_200_201_and_204() {
    timeout(Duration::from_secs(5), async {
        for status in [200, 201, 204] {
            let (client, mut peer) = tokio::io::duplex(4096);
            let route = HttpRouteConfig::local()
                .prepare(&"https://origin.example/object".parse().unwrap())
                .unwrap();
            let dropped = Arc::new(AtomicBool::new(false));
            let consumed = Arc::new(tokio::sync::Notify::new());
            let client = async {
                let mut tunnel = tunnel_for_test(Box::new(client), &route, Arc::clone(&dropped))
                    .await
                    .unwrap();
                assert_eq!(tunnel.read_u8().await.unwrap(), b'p');
                let mut rest = [0; 5];
                tunnel.read_exact(&mut rest).await.unwrap();
                assert_eq!(&rest, b"refix");
                consumed.notify_one();
                let mut tail = [0; 4];
                tunnel.read_exact(&mut tail).await.unwrap();
                assert_eq!(&tail, b"tail");
                assert!(!dropped.load(Ordering::SeqCst));
                drop(tunnel);
                assert!(dropped.load(Ordering::SeqCst));
            };
            let server = async {
                let request = String::from_utf8(connect_header(&mut peer).await).unwrap();
                assert!(request.starts_with("CONNECT origin.example:443 HTTP/1.1\r\n"));
                // One duplex write fits entirely, putting header+prefix in the next
                // actual read buffer after the server really received CONNECT.
                peer.write_all(format!("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 {status} Tunnel ready\r\n\r\nprefix").as_bytes())
                    .await
                    .unwrap();
                consumed.notified().await;
                // These bytes cannot have been prefetched by the HTTP parser.
                peer.write_all(b"tail").await.unwrap();
            };
            tokio::join!(client, server);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn refused_connect_does_not_wait_for_or_drain_an_unbounded_body() {
    timeout(Duration::from_secs(5), async {
        for status in [407, 101] {
            let (client, mut peer) = tokio::io::duplex(4096);
            let route = HttpRouteConfig::local()
                .prepare(&"https://origin.example/object".parse().unwrap())
                .unwrap();
            let dropped = Arc::new(AtomicBool::new(false));
            let rejected = Arc::new(tokio::sync::Notify::new());
            let client = async {
                let result = tunnel_for_test(Box::new(client), &route, Arc::clone(&dropped)).await;
                assert!(
                    matches!(result, Err(ref error) if error.kind() == FileErrorKind::Transient)
                );
                assert!(dropped.load(Ordering::SeqCst));
                rejected.notify_one();
            };
            let server = async {
                connect_header(&mut peer).await;
                peer.write_all(
                    format!("HTTP/1.1 {status} Refused\r\nContent-Length: 999999999\r\n\r\nx")
                        .as_bytes(),
                )
                .await
                .unwrap();
                // Keep the peer open without sending the remaining body: a client drain
                // or a wait for body completion would fail this real liveness regression.
                rejected.notified().await;
            };
            tokio::join!(client, server);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn malformed_and_oversized_connect_heads_reject_without_waiting_for_peer_close() {
    timeout(Duration::from_secs(5), async {
        let malformed =
            b"HTTP/1.1 204 Tunnel ready\r\nBad\0Name: value\r\n\r\nunread body".to_vec();
        // Independent wire fixture: one legal interim head and an unfinished
        // final header exhaust the 8192-byte total head contract, not EOF.
        let mut oversized =
            b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 204 Tunnel ready\r\nX-Unfinished: ".to_vec();
        oversized.resize(8192, b'a');
        for (wire, expected) in [
            (malformed, FileErrorKind::Corrupt),
            (oversized, FileErrorKind::ResourceExhausted),
        ] {
            let (client, mut peer) = tokio::io::duplex(8192);
            let route = HttpRouteConfig::local()
                .prepare(&"https://origin.example/object".parse().unwrap())
                .unwrap();
            let dropped = Arc::new(AtomicBool::new(false));
            let rejected = Arc::new(tokio::sync::Notify::new());
            let client = async {
                let result = tunnel_for_test(Box::new(client), &route, Arc::clone(&dropped)).await;
                let error = match result {
                    Err(error) => error,
                    Ok(_) => panic!("invalid CONNECT head established a tunnel"),
                };
                assert_eq!(error.kind(), expected);
                assert!(dropped.load(Ordering::SeqCst));
                rejected.notify_one();
            };
            let server = async {
                let request = String::from_utf8(connect_header(&mut peer).await).unwrap();
                assert!(request.starts_with("CONNECT origin.example:443 HTTP/1.1\r\n"));
                peer.write_all(&wire).await.unwrap();
                // Keep the original peer open and send no more bytes. Waiting
                // for EOF, completing the head, or draining would time out.
                rejected.notified().await;
                assert!(dropped.load(Ordering::SeqCst));
                // The original client IO, not only its guard, actually exited.
                assert_eq!(
                    peer.read_u8().await.unwrap_err().kind(),
                    io::ErrorKind::UnexpectedEof
                );
            };
            tokio::join!(client, server);
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn original_happy_eyeballs_author_falls_back_between_address_families() {
    timeout(Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        struct DualDns(SocketAddr);
        impl HttpDnsPort for DualDns {
            type Addresses = [SocketAddr; 2];
            async fn resolve(&self, origin: &HttpOrigin) -> FileResult<Self::Addresses> {
                assert_eq!(origin.port(), self.0.port());
                Ok([
                    SocketAddr::new(std::net::Ipv6Addr::LOCALHOST.into(), origin.port()),
                    self.0,
                ])
            }
        }
        let route = HttpRouteConfig::local()
            .prepare(
                &format!("http://origin.example:{}/object", address.port())
                    .parse()
                    .unwrap(),
            )
            .unwrap();
        let connector = HttpConnector::new(DualDns(address), HttpTlsBackend::rustls(config()));
        let dropped = Arc::new(AtomicBool::new(false));
        let mut connected = connector
            .connect(&route, Guard(Arc::clone(&dropped)))
            .await
            .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        assert!(peer.local_addr().unwrap().is_ipv4());
        connected.io.write_all(b"family-fallback").await.unwrap();
        let mut bytes = [0; 15];
        peer.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"family-fallback");
        assert!(!dropped.load(Ordering::SeqCst));
        drop(connected);
        assert!(dropped.load(Ordering::SeqCst));
    })
    .await
    .unwrap();
}
