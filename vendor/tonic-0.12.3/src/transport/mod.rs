//! Batteries included server and client.
//!
//! This module provides a set of batteries included, fully featured and
//! fast set of HTTP/2 server and client's. These components each provide a
//! `rustls` tls backend when the respective feature flag is enabled, and
//! provides builders to configure transport behavior.
//!
//! # Features
//!
//! - TLS support via [rustls].
//! - Load balancing
//! - Timeouts
//! - Concurrency Limits
//! - Rate limiting
//!
//! # Examples
//!
//! ## Client
//!
//! ```no_run
//! # #[cfg(feature = "rustls")]
//! # use tonic::transport::{Channel, Certificate, ClientTlsConfig};
//! # use std::time::Duration;
//! # use tonic::body::BoxBody;
//! # use tonic::client::GrpcService;;
//! # use http::Request;
//! # #[cfg(feature = "rustls")]
//! # async fn do_thing() -> Result<(), Box<dyn std::error::Error>> {
//! let cert = std::fs::read_to_string("ca.pem")?;
//!
//! let mut channel = Channel::from_static("https://example.com")
//!     .tls_config(ClientTlsConfig::new()
//!         .ca_certificate(Certificate::from_pem(&cert))
//!         .domain_name("example.com".to_string()))?
//!     .timeout(Duration::from_secs(5))
//!     .rate_limit(5, Duration::from_secs(1))
//!     .concurrency_limit(256)
//!     .connect()
//!     .await?;
//!
//! channel.call(Request::new(tonic::body::empty_body())).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Server
//!
//! ```no_run
//! # use std::convert::Infallible;
//! # #[cfg(feature = "rustls")]
//! # use tonic::transport::{Server, Identity, ServerTlsConfig};
//! # use tonic::body::BoxBody;
//! # use tower::Service;
//! # #[cfg(feature = "rustls")]
//! # async fn do_thing() -> Result<(), Box<dyn std::error::Error>> {
//! # #[derive(Clone)]
//! # pub struct Svc;
//! # impl Service<hyper::Request<BoxBody>> for Svc {
//! #   type Response = hyper::Response<BoxBody>;
//! #   type Error = Infallible;
//! #   type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
//! #   fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
//! #       Ok(()).into()
//! #  }
//! #   fn call(&mut self, _req: hyper::Request<BoxBody>) -> Self::Future {
//! #       unimplemented!()
//! #   }
//! # }
//! # impl tonic::server::NamedService for Svc {
//! # const NAME: &'static str = "some_svc";
//! # }
//! # let my_svc = Svc;
//! let cert = std::fs::read_to_string("server.pem")?;
//! let key = std::fs::read_to_string("server.key")?;
//!
//! let addr = "[::1]:50051".parse()?;
//!
//! Server::builder()
//!     .tls_config(ServerTlsConfig::new()
//!         .identity(Identity::from_pem(&cert, &key)))?
//!     .concurrency_limit_per_connection(256)
//!     .add_service(my_svc)
//!     .serve(addr)
//!     .await?;
//!
//! # Ok(())
//! # }
//! ```
//!
//! [rustls]: https://docs.rs/rustls/0.16.0/rustls/

#[cfg(feature = "channel")]
pub mod channel;
#[cfg(feature = "server")]
pub mod server;

mod error;
mod service;
#[cfg(feature = "tls")]
mod tls;

#[doc(inline)]
#[cfg(feature = "channel")]
pub use self::channel::{Channel, Endpoint, Http2ConnectionAttempt, Http2ConnectionConfig};
pub use self::error::Error;
#[doc(inline)]
#[cfg(feature = "server")]
pub use self::server::Server;
/// Deprecated. Please use [`crate::status::TimeoutExpired`] instead.
pub use crate::status::TimeoutExpired;

#[cfg(feature = "tls")]
pub use self::tls::Certificate;
pub use hyper::{body::Body, Uri};
#[cfg(feature = "tls")]
pub use tokio_rustls::rustls::pki_types::CertificateDer;

#[cfg(all(feature = "channel", feature = "tls"))]
pub use self::channel::ClientTlsConfig;
#[cfg(all(feature = "server", feature = "tls"))]
pub use self::server::ServerTlsConfig;
#[cfg(feature = "tls")]
pub use self::tls::Identity;

/// Inline owner for one connection acquisition future.
///
/// The future is destroyed before the original owner on cancellation, error or
/// completion. No extra backing is allocated by this wrapper. The enclosing
/// future, IO/TLS, task, stack and output-owned aliases remain caller scopes.
/// Put every success validation, including an absolute deadline's late-Ready
/// check, inside the future before returning its live connection output.
#[pin_project::pin_project(PinnedDrop)]
pub struct ConnectionAcquisition<F> {
    #[pin]
    future: Option<F>,
    owner: Option<bytes::Bytes>,
}
impl<F> std::fmt::Debug for ConnectionAcquisition<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionAcquisition")
            .field("active", &self.future.is_some())
            .field("has_owner", &self.owner.is_some())
            .finish_non_exhaustive()
    }
}
impl<F> ConnectionAcquisition<F> {
    /// Hold an original acquisition capability until the actual future exits.
    /// A successful output can retain independently funded connection owners.
    pub fn new(future: F, owner: Option<bytes::Bytes>) -> Self {
        Self {
            future: Some(future),
            owner,
        }
    }
}
impl<F: std::future::Future> std::future::Future for ConnectionAcquisition<F> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        let result = this
            .future
            .as_mut()
            .as_pin_mut()
            .expect("live acquisition future")
            .poll(cx);
        if result.is_ready() {
            this.future.set(None);
            drop(this.owner.take());
        }
        result
    }
}
#[pin_project::pinned_drop]
impl<F> PinnedDrop for ConnectionAcquisition<F> {
    fn drop(self: std::pin::Pin<&mut Self>) {
        let mut this = self.project();
        this.future.set(None);
        drop(this.owner.take());
    }
}
