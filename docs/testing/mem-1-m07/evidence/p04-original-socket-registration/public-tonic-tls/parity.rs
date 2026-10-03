//! Public Endpoint parity through the actual vendored Tonic Connector.
//! This is not a TLS handshake, original-owner allocator or Native acceptance proof.
use bytes::Bytes;
use hyper_util::rt::TokioIo;
use std::{
    io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tonic::transport::{Endpoint, Http2ConnectionAttempt, Http2ConnectionConfig};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn has_https_without_tls(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut next = Some(error);
    while let Some(error) = next {
        if error.to_string() == "Connecting to HTTPS without TLS enabled" {
            return true;
        }
        next = error.source();
    }
    false
}
#[test]
fn typed_https_without_config_matches_uri_connector_tls_refusal() {
    runtime().block_on(async {
        let legacy_called = Arc::new(AtomicBool::new(false));
        let typed_called = Arc::new(AtomicBool::new(false));
        let called = legacy_called.clone();
        let legacy = tower::service_fn(move |uri: http::Uri| {
            assert_eq!(uri.scheme_str(), Some("https"));
            called.store(true, Ordering::SeqCst);
            async {
                let (io, _) = tokio::io::duplex(64);
                Ok::<_, io::Error>(TokioIo::new(io))
            }
        });
        let called = typed_called.clone();
        let typed = tower::service_fn(move |attempt: Http2ConnectionAttempt| {
            assert_eq!(attempt.uri().scheme_str(), Some("https"));
            assert!(attempt.into_parts().1.is_none());
            called.store(true, Ordering::SeqCst);
            async {
                let (io, _) = tokio::io::duplex(64);
                Ok::<_, io::Error>(TokioIo::new(io))
            }
        });
        let endpoint = Endpoint::from_static("https://example.test");
        let legacy = endpoint.connect_with_connector(legacy).await.unwrap_err();
        let typed = endpoint
            .connect_with_attempt_connector(typed)
            .await
            .unwrap_err();
        assert!(legacy_called.load(Ordering::SeqCst));
        assert!(typed_called.load(Ordering::SeqCst));
        assert!(has_https_without_tls(&legacy));
        assert!(has_https_without_tls(&typed));
    });
}
#[test]
fn typed_http_preserves_original_owner_without_tonic_tls() {
    runtime().block_on(async {
        let called = Arc::new(AtomicBool::new(false));
        let observed = called.clone();
        let (io, peer) = tokio::io::duplex(65536);
        let io = Arc::new(std::sync::Mutex::new(Some(io)));
        let (ready, completed) = tokio::sync::oneshot::channel();
        let (close, closed) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let connection = h2::server::handshake(peer).await.unwrap();
            ready.send(()).unwrap();
            closed.await.unwrap();
            drop(connection);
        });
        let typed = tower::service_fn(move |attempt: Http2ConnectionAttempt| {
            let (uri, owner) = attempt.into_parts();
            assert_eq!(uri.scheme_str(), Some("http"));
            assert_eq!(owner.unwrap().as_ref(), b"original owner");
            observed.store(true, Ordering::SeqCst);
            let io = io.lock().unwrap().take().unwrap();
            async move { Ok::<_, io::Error>(TokioIo::new(io)) }
        });
        let endpoint =
            Endpoint::from_static("http://example.test").http2_connection_factory(|| {
                Ok::<_, io::Error>(Http2ConnectionConfig {
                    io_owner: Some(Bytes::from_static(b"original owner")),
                    ..Default::default()
                })
            });
        let channel = endpoint
            .connect_with_attempt_connector(typed)
            .await
            .unwrap();
        assert!(called.load(Ordering::SeqCst));
        tokio::time::timeout(std::time::Duration::from_secs(5), completed)
            .await
            .unwrap()
            .unwrap();
        drop(channel);
        close.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), peer)
            .await
            .unwrap()
            .unwrap();
    });
}
