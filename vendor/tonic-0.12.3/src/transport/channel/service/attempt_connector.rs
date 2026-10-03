//! Request-aware connection timeout for opt-in original-capability connectors.

use crate::transport::channel::{http2_connection::Http2ConnectionAttempt, BoxFuture};
use http::Uri;
use std::{
    io,
    task::{Context, Poll},
    time::Duration,
};
use tower_service::Service;

pub(crate) trait RequestUri {
    fn uri(&self) -> &Uri;
}
impl RequestUri for Uri {
    fn uri(&self) -> &Uri {
        self
    }
}
impl RequestUri for Http2ConnectionAttempt {
    fn uri(&self) -> &Uri {
        self.uri()
    }
}

/// Unlike the legacy URI-only TimeoutConnector, this forwards the complete
/// original attempt. It applies only connect_timeout, not IO read/write timers.
pub(crate) struct AttemptTimeoutConnector<C> {
    inner: C,
    timeout: Duration,
}
impl<C> AttemptTimeoutConnector<C> {
    pub(crate) fn new(inner: C, timeout: Duration) -> Self {
        Self { inner, timeout }
    }
}
impl<C> Service<Http2ConnectionAttempt> for AttemptTimeoutConnector<C>
where
    C: Service<Http2ConnectionAttempt>,
    C::Future: Send + 'static,
    C::Response: Send + 'static,
    C::Error: Into<crate::Error>,
{
    type Response = C::Response;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }
    fn call(&mut self, request: Http2ConnectionAttempt) -> Self::Future {
        let future = self.inner.call(request);
        let timeout = self.timeout;
        Box::pin(async move {
            tokio::time::timeout(timeout, future)
                .await
                .map_err(|elapsed| {
                    crate::Error::from(io::Error::new(io::ErrorKind::TimedOut, elapsed))
                })?
                .map_err(Into::into)
        })
    }
}
