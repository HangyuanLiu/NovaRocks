#[cfg(feature = "tls")]
use super::TlsConnector;
use super::{attempt_connector::RequestUri, BoxedIo};
use crate::transport::channel::BoxFuture;
use crate::ConnectError;
#[cfg(feature = "tls")]
use std::fmt;
use std::task::{Context, Poll};

use hyper::rt;

#[cfg(feature = "tls")]
use hyper_util::rt::TokioIo;
use tower_service::Service;

pub(crate) struct Connector<C> {
    inner: C,
    #[cfg(feature = "tls")]
    tls: Option<TlsConnector>,
}

impl<C> Connector<C> {
    pub(crate) fn new(inner: C, #[cfg(feature = "tls")] tls: Option<TlsConnector>) -> Self {
        Self {
            inner,
            #[cfg(feature = "tls")]
            tls,
        }
    }
}

impl<C, R> Service<R> for Connector<C>
where
    C: Service<R>,
    R: RequestUri,
    C::Response: rt::Read + rt::Write + Unpin + Send + 'static,
    C::Future: Send + 'static,
    crate::Error: From<C::Error> + Send + 'static,
{
    type Response = BoxedIo;
    type Error = ConnectError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner
            .poll_ready(cx)
            .map_err(|err| ConnectError(From::from(err)))
    }

    fn call(&mut self, request: R) -> Self::Future {
        #[cfg(feature = "tls")]
        let tls = self.tls.clone();

        let _scheme = request.uri().scheme_str();
        #[cfg(feature = "tls")]
        let is_https = _scheme == Some("https");
        let connect = self.inner.call(request);

        Box::pin(async move {
            async {
                let io = connect.await?;

                #[cfg(feature = "tls")]
                if is_https {
                    return if let Some(tls) = tls {
                        let io = tls.connect(TokioIo::new(io)).await?;
                        Ok(io)
                    } else {
                        Err(HttpsUriWithoutTlsSupport(()).into())
                    };
                }

                Ok::<_, crate::Error>(BoxedIo::new(io))
            }
            .await
            .map_err(ConnectError)
        })
    }
}

/// Error returned when trying to connect to an HTTPS endpoint without TLS enabled.
#[cfg(feature = "tls")]
#[derive(Debug)]
pub(crate) struct HttpsUriWithoutTlsSupport(());

#[cfg(feature = "tls")]
impl fmt::Display for HttpsUriWithoutTlsSupport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connecting to HTTPS without TLS enabled")
    }
}

// std::error::Error only requires a type to impl Debug and Display
#[cfg(feature = "tls")]
impl std::error::Error for HttpsUriWithoutTlsSupport {}

#[cfg(all(test, feature = "tls"))]
mod attempt_tls_tests {
    use super::*;
    use crate::transport::channel::http2_connection::Http2ConnectionAttempt;
    use http::Uri;

    #[tokio::test]
    async fn typed_https_without_config_matches_uri_connector_tls_refusal() {
        let legacy = tower::service_fn(|_: Uri| async {
            let (io, _) = tokio::io::duplex(64);
            Ok::<_, std::io::Error>(TokioIo::new(io))
        });
        let typed = tower::service_fn(|attempt: Http2ConnectionAttempt| async {
            assert_eq!(attempt.uri().scheme_str(), Some("https"));
            let (io, _) = tokio::io::duplex(64);
            Ok::<_, std::io::Error>(TokioIo::new(io))
        });
        let uri = Uri::from_static("https://example.test");
        let legacy = Connector::new(legacy, None).call(uri.clone()).await;
        let typed = Connector::new(typed, None)
            .call(Http2ConnectionAttempt::new(uri, None))
            .await;
        assert!(
            matches!(legacy, Err(ConnectError(error)) if error.is::<HttpsUriWithoutTlsSupport>())
        );
        assert!(
            matches!(typed, Err(ConnectError(error)) if error.is::<HttpsUriWithoutTlsSupport>())
        );
    }

    #[tokio::test]
    async fn typed_http_preserves_original_owner_without_tonic_tls() {
        let typed = tower::service_fn(|attempt: Http2ConnectionAttempt| async {
            let (uri, owner) = attempt.into_parts();
            assert_eq!(uri.scheme_str(), Some("http"));
            assert_eq!(owner.unwrap().as_ref(), b"original owner");
            let (io, _) = tokio::io::duplex(64);
            Ok::<_, std::io::Error>(TokioIo::new(io))
        });
        let result = Connector::new(typed, None)
            .call(Http2ConnectionAttempt::new(
                Uri::from_static("http://example.test"),
                Some(bytes::Bytes::from_static(b"original owner")),
            ))
            .await;
        assert!(result.is_ok());
    }
}
