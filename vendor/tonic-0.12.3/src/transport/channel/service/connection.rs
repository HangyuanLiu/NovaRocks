use super::{AddOrigin, Reconnect, SharedExec, UserAgent};
use crate::transport::channel::http2_connection::Http2ConnectionFactory;
use crate::{
    body::{boxed, BoxBody},
    transport::{channel::BoxFuture, service::GrpcTimeout, Endpoint},
};
use http::{Request, Response, Uri};
use hyper::rt;
use hyper::{client::conn::http2::Builder, rt::Executor};
use hyper_util::rt::TokioTimer;
use std::{
    fmt,
    task::{Context, Poll},
};
use tower::load::Load;
use tower::{
    layer::Layer,
    limit::{concurrency::ConcurrencyLimitLayer, rate::RateLimitLayer},
    util::BoxService,
    ServiceBuilder, ServiceExt,
};
use tower_service::Service;

pub(crate) struct Connection {
    inner: BoxService<Request<BoxBody>, Response<BoxBody>, crate::Error>,
}

impl Connection {
    fn new<C>(connector: C, endpoint: Endpoint, is_lazy: bool) -> Self
    where
        C: Service<Uri> + Send + 'static,
        C::Error: Into<crate::Error> + Send,
        C::Future: Send,
        C::Response: rt::Read + rt::Write + Unpin + Send + 'static,
    {
        let mut settings: Builder<SharedExec> = Builder::new(endpoint.executor.clone())
            .initial_stream_window_size(endpoint.init_stream_window_size)
            .initial_connection_window_size(endpoint.init_connection_window_size)
            .keep_alive_interval(endpoint.http2_keep_alive_interval)
            .timer(TokioTimer::new())
            .clone();

        if let Some(val) = endpoint.http2_keep_alive_timeout {
            settings.keep_alive_timeout(val);
        }

        if let Some(val) = endpoint.http2_keep_alive_while_idle {
            settings.keep_alive_while_idle(val);
        }

        if let Some(val) = endpoint.http2_adaptive_window {
            settings.adaptive_window(val);
        }

        if let Some(val) = endpoint.http2_max_header_list_size {
            settings.max_header_list_size(val);
        }

        let stack = ServiceBuilder::new()
            .layer_fn(|s| {
                let origin = endpoint.origin.as_ref().unwrap_or(&endpoint.uri).clone();

                AddOrigin::new(s, origin)
            })
            .layer_fn(|s| UserAgent::new(s, endpoint.user_agent.clone()))
            .layer_fn(|s| GrpcTimeout::new(s, endpoint.timeout))
            .option_layer(endpoint.concurrency_limit.map(ConcurrencyLimitLayer::new))
            .option_layer(endpoint.rate_limit.map(|(l, d)| RateLimitLayer::new(l, d)))
            .into_inner();

        let make_service = MakeSendRequestService::new(
            connector,
            endpoint.executor.clone(),
            settings,
            endpoint.http2_connection_factory.clone(),
            endpoint.http2_max_header_list_size,
        );

        let conn = Reconnect::new(make_service, endpoint.uri.clone(), is_lazy);

        Self {
            inner: BoxService::new(stack.layer(conn)),
        }
    }

    pub(crate) async fn connect<C>(connector: C, endpoint: Endpoint) -> Result<Self, crate::Error>
    where
        C: Service<Uri> + Send + 'static,
        C::Error: Into<crate::Error> + Send,
        C::Future: Unpin + Send,
        C::Response: rt::Read + rt::Write + Unpin + Send + 'static,
    {
        Self::new(connector, endpoint, false).ready_oneshot().await
    }

    pub(crate) fn lazy<C>(connector: C, endpoint: Endpoint) -> Self
    where
        C: Service<Uri> + Send + 'static,
        C::Error: Into<crate::Error> + Send,
        C::Future: Send,
        C::Response: rt::Read + rt::Write + Unpin + Send + 'static,
    {
        Self::new(connector, endpoint, true)
    }
}

impl Service<Request<BoxBody>> for Connection {
    type Response = Response<BoxBody>;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.inner, cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        self.inner.call(req)
    }
}

impl Load for Connection {
    type Metric = usize;

    fn load(&self) -> Self::Metric {
        0
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connection").finish()
    }
}

struct SendRequest {
    inner: hyper::client::conn::http2::SendRequest<BoxBody>,
}

impl From<hyper::client::conn::http2::SendRequest<BoxBody>> for SendRequest {
    fn from(inner: hyper::client::conn::http2::SendRequest<BoxBody>) -> Self {
        Self { inner }
    }
}

impl tower::Service<Request<BoxBody>> for SendRequest {
    type Response = Response<BoxBody>;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<BoxBody>) -> Self::Future {
        let fut = self.inner.send_request(req);

        Box::pin(async move { fut.await.map_err(Into::into).map(|res| res.map(boxed)) })
    }
}

struct MakeSendRequestService<C> {
    connector: C,
    executor: SharedExec,
    settings: Builder<SharedExec>,
    factory: Option<Http2ConnectionFactory>,
    inherited_max_header_list_size: Option<u32>,
}

impl<C> MakeSendRequestService<C> {
    fn new(
        connector: C,
        executor: SharedExec,
        settings: Builder<SharedExec>,
        factory: Option<Http2ConnectionFactory>,
        inherited_max_header_list_size: Option<u32>,
    ) -> Self {
        Self {
            connector,
            executor,
            settings,
            factory,
            inherited_max_header_list_size,
        }
    }
}

impl<C> tower::Service<Uri> for MakeSendRequestService<C>
where
    C: Service<Uri> + Send + 'static,
    C::Error: Into<crate::Error> + Send,
    C::Future: Send,
    C::Response: rt::Read + rt::Write + Unpin + Send,
{
    type Response = SendRequest;
    type Error = crate::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.connector.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Uri) -> Self::Future {
        let started = std::time::Instant::now();
        let mut initial_settings_deadline = None;
        let mut builder = self.settings.clone();
        if let Some(factory) = &self.factory {
            let configured = factory().and_then(|config| {
                if let Some(timeout) = config.initial_settings_timeout {
                    if timeout.is_zero() {
                        return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput).into());
                    }
                    let deadline = started.checked_add(timeout).ok_or_else(|| {
                        crate::Error::from(std::io::Error::from(std::io::ErrorKind::InvalidInput))
                    })?;
                    if std::time::Instant::now() >= deadline {
                        return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
                    }
                    initial_settings_deadline = Some(deadline);
                    builder.initial_settings_deadline(deadline);
                }
                config
                    .apply(&mut builder, self.inherited_max_header_list_size)
                    .map_err(Into::into)
            });
            if let Err(error) = configured {
                // No dial future or handshake is created on factory refusal.
                return Box::pin(async move { Err(error) });
            }
        }
        if initial_settings_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return Box::pin(async {
                Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into())
            });
        }
        let fut = self.connector.call(req);
        let executor = self.executor.clone();

        Box::pin(async move {
            let acquisition = async move {
                // A queued first poll cannot perform connector I/O after D.
                if initial_settings_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                    return Err(crate::Error::from(std::io::Error::from(
                        std::io::ErrorKind::TimedOut,
                    )));
                }
                let io = fut
                    .await
                    .map_err(|error| -> crate::Error { error.into() })?;
                builder
                    .handshake(io)
                    .await
                    .map_err(|error| -> crate::Error { error.into() })
            };
            let (send_request, conn) = match initial_settings_deadline {
                Some(deadline) => {
                    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), acquisition)
                        .await
                        .map_err(|_| {
                            crate::Error::from(std::io::Error::from(std::io::ErrorKind::TimedOut))
                        })??
                }
                None => acquisition.await?,
            };
            if initial_settings_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                return Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into());
            }

            Executor::<BoxFuture<'static, ()>>::execute(
                &executor,
                Box::pin(async move {
                    if let Err(e) = conn.await {
                        tracing::debug!("connection task error: {:?}", e);
                    }
                }) as _,
            );

            Ok(SendRequest::from(send_request))
        })
    }
}
