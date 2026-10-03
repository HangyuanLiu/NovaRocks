use super::io::OwnedConnectionIo;
use super::{AddOrigin, Reconnect, SharedExec, UserAgent};
use crate::transport::channel::http2_connection::{Http2ConnectionAttempt, Http2ConnectionFactory};
use crate::{
    body::{boxed, BoxBody},
    transport::{channel::BoxFuture, service::GrpcTimeout, ConnectionAcquisition, Endpoint},
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

// An attempt may be canceled before binding IO while factory aliases remain.
// Retirement is a logical verdict; its original owner remains with every alias.
struct AcquisitionLifecycle {
    lifecycle: Option<h2::ConnectionLifecycle>,
    completed: bool,
}

impl AcquisitionLifecycle {
    fn complete(mut self) {
        self.completed = true;
        drop(self);
    }
}

impl Drop for AcquisitionLifecycle {
    fn drop(&mut self) {
        if !self.completed {
            if let Some(lifecycle) = &self.lifecycle {
                let _ = lifecycle.retire();
            }
        }
    }
}

impl Connection {
    fn new<C, R>(
        connector: C,
        endpoint: Endpoint,
        is_lazy: bool,
        request: fn(Uri, Option<bytes::Bytes>) -> R,
    ) -> Self
    where
        R: Send + 'static,
        C: Service<R> + Send + 'static,
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
            request,
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
        Self::new(connector, endpoint, false, |uri, _| uri)
            .ready_oneshot()
            .await
    }

    pub(crate) fn lazy<C>(connector: C, endpoint: Endpoint) -> Self
    where
        C: Service<Uri> + Send + 'static,
        C::Error: Into<crate::Error> + Send,
        C::Future: Send,
        C::Response: rt::Read + rt::Write + Unpin + Send + 'static,
    {
        Self::new(connector, endpoint, true, |uri, _| uri)
    }

    pub(crate) async fn connect_attempt<C>(
        connector: C,
        endpoint: Endpoint,
    ) -> Result<Self, crate::Error>
    where
        C: Service<Http2ConnectionAttempt> + Send + 'static,
        C::Error: Into<crate::Error> + Send,
        C::Future: Unpin + Send,
        C::Response: rt::Read + rt::Write + Unpin + Send + 'static,
    {
        Self::new(connector, endpoint, false, Http2ConnectionAttempt::new)
            .ready_oneshot()
            .await
    }

    pub(crate) fn lazy_attempt<C>(connector: C, endpoint: Endpoint) -> Self
    where
        C: Service<Http2ConnectionAttempt> + Send + 'static,
        C::Error: Into<crate::Error> + Send,
        C::Future: Send,
        C::Response: rt::Read + rt::Write + Unpin + Send + 'static,
    {
        Self::new(connector, endpoint, true, Http2ConnectionAttempt::new)
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

struct MakeSendRequestService<C, R> {
    connector: C,
    executor: SharedExec,
    settings: Builder<SharedExec>,
    factory: Option<Http2ConnectionFactory>,
    inherited_max_header_list_size: Option<u32>,
    request: fn(Uri, Option<bytes::Bytes>) -> R,
}

impl<C, R> MakeSendRequestService<C, R> {
    fn new(
        connector: C,
        executor: SharedExec,
        settings: Builder<SharedExec>,
        factory: Option<Http2ConnectionFactory>,
        inherited_max_header_list_size: Option<u32>,
        request: fn(Uri, Option<bytes::Bytes>) -> R,
    ) -> Self {
        Self {
            connector,
            executor,
            settings,
            factory,
            inherited_max_header_list_size,
            request,
        }
    }
}

impl<C, R> tower::Service<Uri> for MakeSendRequestService<C, R>
where
    R: Send + 'static,
    C: Service<R> + Send + 'static,
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
        let mut acquisition_owner = None;
        let mut io_owner = None;
        let mut connection_driver = None;
        let mut driver_lifecycle = AcquisitionLifecycle {
            lifecycle: None,
            completed: false,
        };
        let mut connection_lifecycle = AcquisitionLifecycle {
            lifecycle: None,
            completed: false,
        };
        let mut builder = self.settings.clone();
        if let Some(factory) = &self.factory {
            let configured = factory().and_then(|mut config| {
                // Extract the original position before validation or moving
                // any builder capabilities. Even rejected attempts retain it
                // in the returned future until that future actually exits.
                acquisition_owner = config.acquisition_owner.take();
                io_owner = config.io_owner.take();
                connection_lifecycle.lifecycle = config.connection_lifecycle.clone();
                if let Some(driver) = config.connection_driver.take() {
                    // Claim exactly once before creating the connector future.
                    // The actual response type is checked, even for custom IO.
                    connection_driver = Some(driver.reserve::<C::Response>()?);
                    driver_lifecycle.lifecycle = config.connection_lifecycle.clone();
                }
                if (acquisition_owner.is_some() || connection_lifecycle.lifecycle.is_some())
                    && config.initial_settings_timeout.is_none_or(|d| d.is_zero())
                {
                    return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput).into());
                }
                if let (Some(lifecycle), Some(owner)) =
                    (&connection_lifecycle.lifecycle, &acquisition_owner)
                {
                    lifecycle.retain_acquisition_owner(owner.clone())?;
                }
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
                return Box::pin(ConnectionAcquisition::new(
                    ConnectionAcquisition::new(async move { Err(error) }, io_owner),
                    acquisition_owner,
                ));
            }
        }
        if initial_settings_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return Box::pin(ConnectionAcquisition::new(
                ConnectionAcquisition::new(
                    async { Err(std::io::Error::from(std::io::ErrorKind::TimedOut).into()) },
                    io_owner,
                ),
                acquisition_owner,
            ));
        }
        let request = (self.request)(req, io_owner.clone());
        let fut = self.connector.call(request);
        let executor = self.executor.clone();

        // Construct the ordered owner wrapper before returning the queued
        // future. Cancellation before its first poll must also retire the
        // captured connector future before returning the acquisition position.
        let output_io_owner = io_owner.clone();
        let io_scope = ConnectionAcquisition::new(
            async move {
                let connecting = async move {
                    if initial_settings_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                        return Err(crate::Error::from(std::io::Error::from(
                            std::io::ErrorKind::TimedOut,
                        )));
                    }
                    let io = fut
                        .await
                        .map_err(|error| -> crate::Error { error.into() })?;
                    // The final output owns Native and Tonic IO boxes. Keep
                    // their original backing capability outside both layers.
                    let io = OwnedConnectionIo::new(io, output_io_owner);
                    builder
                        .handshake(io)
                        .await
                        .map_err(|error| -> crate::Error { error.into() })
                };
                let result = match initial_settings_deadline {
                    Some(deadline) => tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        connecting,
                    )
                    .await
                    .map_err(|_| {
                        crate::Error::from(std::io::Error::from(std::io::ErrorKind::TimedOut))
                    })?,
                    None => connecting.await,
                }?;
                if initial_settings_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                    // Drop the late sender/output here. Hyper's independent
                    // protocol task retains the original acquisition alias
                    // through its actual IO exit on this failed verdict.
                    drop(result);
                    return Err(crate::Error::from(std::io::Error::from(
                        std::io::ErrorKind::TimedOut,
                    )));
                }
                if let Some(lifecycle) = &connection_lifecycle.lifecycle {
                    if let Err(error) = lifecycle.on_acquisition_complete() {
                        drop(result);
                        return Err(crate::Error::from(error));
                    }
                    // A callback is finite work but may cross the same absolute
                    // D. No late-success output can escape final validation.
                    if initial_settings_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
                        let _ = lifecycle.retire();
                        drop(result);
                        return Err(crate::Error::from(std::io::Error::from(
                            std::io::ErrorKind::TimedOut,
                        )));
                    }
                    lifecycle.release_acquisition_owner()?;
                }
                connection_lifecycle.complete();
                Ok(result)
            },
            io_owner,
        );
        let acquisition = ConnectionAcquisition::new(io_scope, acquisition_owner);

        Box::pin(async move {
            let (send_request, conn) = acquisition.await?;
            // The ordered acquisition future has now exited and released its
            // position. The live connection retains its independent pools.
            if let Some(driver) = connection_driver {
                // Same concrete constructor as the static layout query; bypass
                // both legacy type-erasure Boxes with the original task owner.
                driver.spawn(conn)?;
                driver_lifecycle.complete();
            } else {
                Executor::<BoxFuture<'static, ()>>::execute(
                    &executor,
                    Box::pin(super::connection_driver::run_driver(conn)) as _,
                );
            }
            Ok(SendRequest::from(send_request))
        })
    }
}
