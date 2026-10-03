//! Trait aliases
//!
//! Traits in this module ease setting bounds and usually automatically
//! implemented by implementing another trait.

#[cfg(all(feature = "client", feature = "http2"))]
pub use self::h2_client::Http2ClientConnExec;
#[cfg(all(feature = "server", feature = "http2"))]
pub use self::h2_server::Http2ServerConnExec;

#[cfg(all(any(feature = "client", feature = "server"), feature = "http2"))]
pub(crate) use self::h2_common::Http2UpgradedExec;

#[cfg(all(any(feature = "client", feature = "server"), feature = "http2"))]
mod h2_common {
    use crate::proto::h2::upgrade::UpgradedSendStreamTask;
    use crate::rt::Executor;

    pub trait Http2UpgradedExec<B> {
        #[doc(hidden)]
        fn execute_upgrade(&self, fut: UpgradedSendStreamTask<B>);
    }

    #[doc(hidden)]
    impl<E, B> Http2UpgradedExec<B> for E
    where
        E: Executor<UpgradedSendStreamTask<B>>,
    {
        fn execute_upgrade(&self, fut: UpgradedSendStreamTask<B>) {
            self.execute(fut)
        }
    }
}

#[cfg(all(feature = "client", feature = "http2"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "client", feature = "http2"))))]
mod h2_client {
    use std::{error::Error, future::Future};

    use crate::rt::{Read, Write};
    use crate::{proto::h2::client::H2ClientFuture, rt::Executor};

    /// An executor to spawn http2 futures for the client.
    ///
    /// This trait is implemented for any type that implements [`Executor`]
    /// trait for any future.
    ///
    /// This trait is sealed and cannot be implemented for types outside this crate.
    ///
    /// [`Executor`]: crate::rt::Executor
    pub trait Http2ClientConnExec<B, T>:
        super::Http2UpgradedExec<B::Data> + sealed_client::Sealed<(B, T)> + Clone
    where
        B: http_body::Body,
        B::Error: Into<Box<dyn Error + Send + Sync>>,
        T: Read + Write + Unpin,
    {
        #[doc(hidden)]
        fn client_task_allocation_capacity_bound() -> std::io::Result<usize>;

        #[doc(hidden)]
        fn try_take_prepared_client_task(&mut self) -> std::io::Result<Option<Self>>;

        #[doc(hidden)]
        fn try_execute_h2_future(
            &mut self,
            future: H2ClientFuture<B, T, Self>,
        ) -> std::io::Result<()>;

        #[doc(hidden)]
        fn client_request_admission(
            &self,
        ) -> std::io::Result<Option<crate::rt::ClientRequestAdmission>>;

        #[doc(hidden)]
        fn execute_h2_future(&mut self, future: H2ClientFuture<B, T, Self>);
    }

    impl<E, B, T> Http2ClientConnExec<B, T> for E
    where
        E: Clone,
        E: Executor<H2ClientFuture<B, T, E>>,
        E: super::Http2UpgradedExec<B::Data>,
        B: http_body::Body + 'static,
        B::Error: Into<Box<dyn Error + Send + Sync>>,
        H2ClientFuture<B, T, E>: Future<Output = ()>,
        T: Read + Write + Unpin,
    {
        fn client_task_allocation_capacity_bound() -> std::io::Result<usize> {
            <E as Executor<H2ClientFuture<B, T, E>>>::task_allocation_capacity_bound()
        }

        fn try_take_prepared_client_task(&mut self) -> std::io::Result<Option<Self>> {
            <E as Executor<H2ClientFuture<B, T, E>>>::try_take_prepared_task(self)
        }

        fn try_execute_h2_future(
            &mut self,
            future: H2ClientFuture<B, T, E>,
        ) -> std::io::Result<()> {
            <E as Executor<H2ClientFuture<B, T, E>>>::try_execute(self, future)
        }

        fn client_request_admission(
            &self,
        ) -> std::io::Result<Option<crate::rt::ClientRequestAdmission>> {
            let admission =
                <E as Executor<H2ClientFuture<B, T, E>>>::client_request_admission(self)?;
            if admission.is_some()
                && !<E as Executor<H2ClientFuture<B, T, E>>>::supports_client_request_task_lease()
            {
                return Err(std::io::ErrorKind::Unsupported.into());
            }
            Ok(admission)
        }

        fn execute_h2_future(&mut self, future: H2ClientFuture<B, T, E>) {
            self.execute(future)
        }
    }

    impl<E, B, T> sealed_client::Sealed<(B, T)> for E
    where
        E: Clone,
        E: Executor<H2ClientFuture<B, T, E>>,
        E: super::Http2UpgradedExec<B::Data>,
        B: http_body::Body + 'static,
        B::Error: Into<Box<dyn Error + Send + Sync>>,
        H2ClientFuture<B, T, E>: Future<Output = ()>,
        T: Read + Write + Unpin,
    {
    }

    mod sealed_client {
        pub trait Sealed<X> {}
    }
}

#[cfg(all(feature = "server", feature = "http2"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "server", feature = "http2"))))]
mod h2_server {
    use crate::{proto::h2::server::H2Stream, rt::Executor};
    use http_body::Body;
    use std::future::Future;

    /// An executor to spawn http2 connections.
    ///
    /// This trait is implemented for any type that implements [`Executor`]
    /// trait for any future.
    ///
    /// This trait is sealed and cannot be implemented for types outside this crate.
    ///
    /// [`Executor`]: crate::rt::Executor
    pub trait Http2ServerConnExec<F, B: Body>:
        super::Http2UpgradedExec<B::Data> + sealed::Sealed<(F, B)> + Clone
    {
        #[doc(hidden)]
        fn stream_task_allocation_capacity_bound() -> std::io::Result<usize>
        where
            Self: Sized;

        #[doc(hidden)]
        fn try_prepare_h2stream(&self) -> std::io::Result<Option<Self>>
        where
            Self: Sized;

        #[doc(hidden)]
        fn admit_h2_request_head(
            &self,
            uri: &http::Uri,
            headers: &http::HeaderMap,
        ) -> std::io::Result<()>;

        #[doc(hidden)]
        fn execute_h2stream(&mut self, fut: H2Stream<F, B, Self>);
    }

    #[doc(hidden)]
    impl<E, F, B> Http2ServerConnExec<F, B> for E
    where
        E: Clone,
        E: Executor<H2Stream<F, B, E>>,
        E: super::Http2UpgradedExec<B::Data>,
        H2Stream<F, B, E>: Future<Output = ()>,
        B: Body,
    {
        fn stream_task_allocation_capacity_bound() -> std::io::Result<usize> {
            <E as Executor<H2Stream<F, B, E>>>::task_allocation_capacity_bound()
        }

        fn try_prepare_h2stream(&self) -> std::io::Result<Option<Self>> {
            <E as Executor<H2Stream<F, B, E>>>::try_prepare_task(self)
        }

        fn admit_h2_request_head(
            &self,
            uri: &http::Uri,
            headers: &http::HeaderMap,
        ) -> std::io::Result<()> {
            <E as Executor<H2Stream<F, B, E>>>::admit_request_head(self, uri, headers)
        }

        fn execute_h2stream(&mut self, fut: H2Stream<F, B, E>) {
            self.execute(fut)
        }
    }

    impl<E, F, B> sealed::Sealed<(F, B)> for E
    where
        E: Clone,
        E: Executor<H2Stream<F, B, E>>,
        E: super::Http2UpgradedExec<B::Data>,
        H2Stream<F, B, E>: Future<Output = ()>,
        B: Body,
    {
    }

    mod sealed {
        pub trait Sealed<T> {}
    }
}
