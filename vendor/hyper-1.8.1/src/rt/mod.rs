//! Runtime components
//!
//! This module provides traits and types that allow hyper to be runtime-agnostic.
//! By abstracting over async runtimes, hyper can work with different executors, timers, and IO transports.
//!
//! The main components in this module are:
//!
//! - **Executors**: Traits for spawning and running futures, enabling integration with any async runtime.
//! - **Timers**: Abstractions for sleeping and scheduling tasks, allowing time-based operations to be runtime-independent.
//! - **IO Transports**: Traits for asynchronous reading and writing, so hyper can work with various IO backends.
//!
//! By implementing these traits, you can customize how hyper interacts with your chosen runtime environment.
//!
//! To learn more, [check out the runtime guide](https://hyper.rs/guides/1/init/runtime/).

pub mod bounds;
mod io;
mod timer;

pub use self::io::{Read, ReadBuf, ReadBufCursor, Write};
pub use self::timer::{Sleep, Timer};

/// An executor of futures.
///
/// This trait allows Hyper to abstract over async runtimes. Implement this trait for your own type.
///
/// # Example
///
/// ```
/// # use hyper::rt::Executor;
/// # use std::future::Future;
/// #[derive(Clone)]
/// struct TokioExecutor;
///
/// impl<F> Executor<F> for TokioExecutor
/// where
///     F: Future + Send + 'static,
///     F::Output: Send + 'static,
/// {
///     fn execute(&self, future: F) {
///         tokio::spawn(future);
///     }
/// }
/// ```
pub trait Executor<Fut> {
    /// Return the caller-funded allocation bound for this concrete future type.
    ///
    /// This optional hook constructs neither a future nor an executor. The
    /// default rejects the query; ordinary execution does not call it. A bound
    /// describes only the allocations covered by the executor's receipt, not
    /// the future's external data or the complete connection.
    fn task_allocation_capacity_bound() -> std::io::Result<usize>
    where
        Self: Sized,
    {
        Err(std::io::ErrorKind::Unsupported.into())
    }

    /// Prepare an executor under an original task allocation capability.
    ///
    /// Hyper calls this before creating an incoming HTTP/2 service future.
    /// `Some` must retain the previously funded capability for the concrete
    /// `Fut` through its actual task allocation exit. `Err` refuses dispatch
    /// before the service is called. `None`, the default, preserves ordinary
    /// execution and executor cloning without claiming any task ownership.
    ///
    /// This hook must not acquire an independent allocation budget. Preparing
    /// an executor does not cover CONNECT's separately spawned upgrade task;
    /// callers installing this hook should also configure the server's
    /// preallocated-task CONNECT rejection policy.
    fn try_prepare_task(&self) -> std::io::Result<Option<Self>>
    where
        Self: Sized,
    {
        Ok(None)
    }

    /// Place the future into the executor to be run.
    fn execute(&self, fut: Fut);
}
