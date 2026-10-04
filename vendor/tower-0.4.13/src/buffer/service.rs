use super::{
    future::ResponseFuture,
    message::Message,
    queue,
    worker::{Handle, Worker},
};

use futures_core::ready;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::PollSemaphore;
use tower_service::Service;

/// Adds a request buffer in front of an inner service.
///
/// See the module documentation for more details.
#[derive(Debug)]
pub struct Buffer<T, Request>
where
    T: Service<Request>,
{
    // Note: this actually _is_ bounded, but rather than using Tokio's bounded
    // channel, we use Tokio's semaphore separately to implement the bound.
    tx: queue::Sender<Message<Request, T::Future>>,
    // When the buffer's channel is full, we want to exert backpressure in
    // `poll_ready`, so that callers such as load balancers could choose to call
    // another service rather than waiting for buffer capacity.
    //
    // Unfortunately, this can't be done easily using Tokio's bounded MPSC
    // channel, because it doesn't expose a polling-based interface, only an
    // `async fn ready`, which borrows the sender. Therefore, we implement our
    // own bounded MPSC on top of the unbounded channel, using a semaphore to
    // limit how many items are in the channel.
    semaphore: PollSemaphore,
    // The current semaphore permit, if one has been acquired.
    //
    // This is acquired in `poll_ready` and taken in `call`.
    permit: Option<OwnedSemaphorePermit>,
    handle: Handle,
    // This capability outlives every preceding Buffer-owned backing.
    #[cfg(feature = "original-response-cells")]
    original_response_cells: Option<OriginalResponseCells>,
}

#[cfg(feature = "original-response-cells")]
#[derive(Clone, Debug)]
struct OriginalResponseCells {
    cell_allocation_bound: usize,
    original: bytes::Bytes,
}

// The Bytes owner wrapper physically exits before either of these fields.
// A permit wake may reenter a caller, so retain the original through that wake.
#[cfg(feature = "original-response-cells")]
struct ResponseCellExit {
    _permit: OwnedSemaphorePermit,
    _original: bytes::Bytes,
}

impl<T, Request> Buffer<T, Request>
where
    T: Service<Request>,
    T::Error: Into<crate::BoxError>,
{
    /// Creates a new [`Buffer`] wrapping `service`.
    ///
    /// `bound` gives the maximal number of requests that can be queued for the service before
    /// backpressure is applied to callers.
    ///
    /// The default Tokio executor is used to run the given service, which means that this method
    /// must be called while on the Tokio runtime.
    ///
    /// # A note on choosing a `bound`
    ///
    /// When [`Buffer`]'s implementation of [`poll_ready`] returns [`Poll::Ready`], it reserves a
    /// slot in the channel for the forthcoming [`call`]. However, if this call doesn't arrive,
    /// this reserved slot may be held up for a long time. As a result, it's advisable to set
    /// `bound` to be at least the maximum number of concurrent requests the [`Buffer`] will see.
    /// If you do not, all the slots in the buffer may be held up by futures that have just called
    /// [`poll_ready`] but will not issue a [`call`], which prevents other senders from issuing new
    /// requests.
    ///
    /// [`Poll::Ready`]: std::task::Poll::Ready
    /// [`call`]: crate::Service::call
    /// [`poll_ready`]: crate::Service::poll_ready
    pub fn new(service: T, bound: usize) -> Self
    where
        T: Send + 'static,
        T::Future: Send,
        T::Error: Send + Sync,
        Request: Send + 'static,
    {
        let (service, worker) = Self::pair(service, bound);
        tokio::spawn(worker);
        service
    }

    /// Creates a new [`Buffer`] wrapping `service`, but returns the background worker.
    ///
    /// This is useful if you do not want to spawn directly onto the tokio runtime
    /// but instead want to use your own executor. This will return the [`Buffer`] and
    /// the background `Worker` that you can then spawn.
    pub fn pair(service: T, bound: usize) -> (Buffer<T, Request>, Worker<T, Request>)
    where
        T: Send + 'static,
        T::Error: Send + Sync,
        Request: Send + 'static,
    {
        let (tx, rx) = queue::pair();
        Self::pair_with_queue(service, bound, tx, rx)
    }

    fn pair_with_queue(
        service: T,
        bound: usize,
        tx: queue::Sender<Message<Request, T::Future>>,
        rx: queue::Receiver<Message<Request, T::Future>>,
    ) -> (Buffer<T, Request>, Worker<T, Request>)
    where
        T: Send + 'static,
        T::Error: Send + Sync,
        Request: Send + 'static,
    {
        let semaphore = Arc::new(Semaphore::new(bound));
        let (handle, worker) = Worker::new(service, rx, &semaphore);
        let buffer = Buffer {
            tx,
            handle,
            semaphore: PollSemaphore::new(semaphore),
            permit: None,
            #[cfg(feature = "original-response-cells")]
            original_response_cells: None,
        };
        (buffer, worker)
    }

    /// Query the actual shared Semaphore and error Handle metadata backing.
    ///
    /// This includes their standard Arc allocations and private mutex backing.
    /// Queues, waiters, external Wakers, service errors and worker tasks are
    /// separate. The caller obtains this amount before original pair growth.
    #[cfg(feature = "original-response-cells")]
    pub fn common_metadata_capacity_bound() -> std::io::Result<usize> {
        Semaphore::allocation_capacity_bound()?
            .checked_add(Handle::allocation_capacity_bound()?)
            .ok_or_else(|| std::io::ErrorKind::InvalidInput.into())
    }

    /// Query the actual fixed FIFO Core, typed slots and std mutex PAL backing.
    ///
    /// This includes every preallocated Option<Message> slot. It excludes the
    /// external request/future/Span/Waker backing and creates no authority.
    #[cfg(feature = "original-response-cells")]
    pub fn queue_metadata_capacity_bound(bound: usize) -> std::io::Result<usize> {
        queue::allocation_capacity_bound::<Message<Request, T::Future>>(bound)
    }

    /// Query the actual typed response-cell Arc allocation without constructing it.
    ///
    /// This excludes the independent owner wrapper, queue, semaphore, service
    /// future backing, and worker task. No capacity authority is created here.
    #[cfg(feature = "original-response-cells")]
    pub fn response_cell_allocation_capacity_bound() -> std::io::Result<usize> {
        oneshot::allocation_capacity_bound::<Result<T::Future, super::error::ServiceError>>()
    }

    /// Query one response cell plus its physical-exit owner wrapper.
    ///
    /// A caller prepays this amount for each pending position before pair
    /// construction. It must account separately for every omitted backing.
    #[cfg(feature = "original-response-cells")]
    pub fn response_cell_total_capacity_bound() -> std::io::Result<usize> {
        Self::response_cell_allocation_capacity_bound()?
            .checked_add(bytes::Bytes::owner_with_exit_guard_metadata_size::<
                bytes::Bytes,
                ResponseCellExit,
            >())
            .ok_or_else(|| std::io::ErrorKind::InvalidInput.into())
    }

    /// Construct an opt-in buffer whose pending permits follow physical cells.
    ///
    /// `bound` is the existing semaphore's pending-position count. The caller
    /// prepays `bound` complete response cells and supplies the same original
    /// generation owner. The typed cell bound and wrapper geometry are checked
    /// before any queue, semaphore, or worker handle construction. The original
    /// does not point back to a Buffer, Worker, Channel, or task JoinHandle.
    /// The caller also prepays queue_metadata_capacity_bound(bound); this path
    /// constructs a fixed FIFO directly rather than first creating an MPSC.
    ///
    /// A handed-off service future does not free its pending position until the
    /// response cell's final endpoint, retained value and Wakers actually exit.
    /// This API does not fund the worker task or external request/error backing.
    #[cfg(feature = "original-response-cells")]
    pub fn pair_with_original_response_cells(
        service: T,
        bound: usize,
        cell_allocation_bound: usize,
        original: bytes::Bytes,
    ) -> std::io::Result<(Buffer<T, Request>, Worker<T, Request>)>
    where
        T: Send + 'static,
        T::Error: Send + Sync,
        Request: Send + 'static,
    {
        let common = Self::common_metadata_capacity_bound()?;
        let queue = Self::queue_metadata_capacity_bound(bound)?;
        let actual = Self::response_cell_allocation_capacity_bound()?;
        let total = Self::response_cell_total_capacity_bound()?;
        if bound == 0
            || bound > Semaphore::MAX_PERMITS
            || cell_allocation_bound < actual
            || total
                .checked_mul(bound)
                .and_then(|value| value.checked_add(common))
                .and_then(|value| value.checked_add(queue))
                .is_none()
        {
            return Err(std::io::ErrorKind::InvalidInput.into());
        }
        let (tx, rx) = queue::pair_original(bound, original.clone())?;
        let (mut buffer, mut worker) = Self::pair_with_queue(service, bound, tx, rx);
        worker.retain_common_metadata_owner(original.clone());
        // Prewarm each final, unpublished mutex once while both original
        // holders retain the same capability. No permits or errors change.
        buffer.semaphore.as_ref().prewarm_allocation_metadata()?;
        buffer.handle.prewarm_allocation_metadata()?;
        buffer.original_response_cells = Some(OriginalResponseCells {
            cell_allocation_bound,
            original,
        });
        Ok((buffer, worker))
    }

    fn get_worker_error(&self) -> crate::BoxError {
        self.handle.get_error_on_closed()
    }
}

impl<T, Request> Service<Request> for Buffer<T, Request>
where
    T: Service<Request>,
    T::Error: Into<crate::BoxError>,
{
    type Response = T::Response;
    type Error = crate::BoxError;
    type Future = ResponseFuture<T::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // First, check if the worker is still alive.
        if self.tx.is_closed() {
            // If the inner service has errored, then we error here.
            return Poll::Ready(Err(self.get_worker_error()));
        }

        // Then, check if we've already acquired a permit.
        if self.permit.is_some() {
            // We've already reserved capacity to send a request. We're ready!
            return Poll::Ready(Ok(()));
        }

        // Finally, if we haven't already acquired a permit, poll the semaphore
        // to acquire one. If we acquire a permit, then there's enough buffer
        // capacity to send a new request. Otherwise, we need to wait for
        // capacity.
        let permit =
            ready!(self.semaphore.poll_acquire(cx)).ok_or_else(|| self.get_worker_error())?;
        self.permit = Some(permit);

        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        tracing::trace!("sending request to buffer worker");
        let _permit = self
            .permit
            .take()
            .expect("buffer full; poll_ready must be called first");

        // get the current Span so that we can explicitly propagate it to the worker
        // if we didn't do this, events on the worker related to this span wouldn't be counted
        // towards that span since the worker would have no way of entering it.
        let span = tracing::Span::current();

        // Original mode moves the already acquired permit into the response
        // cell's physical-exit guard before constructing its actual allocation.
        // Ordinary mode retains the pre-existing message-scoped permit lifetime.
        #[cfg(feature = "original-response-cells")]
        let (tx, rx, _permit) = match &self.original_response_cells {
            Some(original) => {
                let owner = bytes::Bytes::from_owner_with_exit_guard(
                    bytes::Bytes::new(),
                    ResponseCellExit {
                        _permit,
                        _original: original.original.clone(),
                    },
                );
                match oneshot::channel_with_original_owner(original.cell_allocation_bound, owner) {
                    Ok((tx, rx)) => (tx, rx, None),
                    // The constructor was checked before pair growth. Preserve
                    // any actual refusal rather than using an ordinary channel.
                    Err(error) => return ResponseFuture::failed(Box::new(error)),
                }
            }
            None => {
                let (tx, rx) = oneshot::channel();
                (tx, rx, Some(_permit))
            }
        };
        #[cfg(not(feature = "original-response-cells"))]
        let (tx, rx, _permit) = {
            let (tx, rx) = oneshot::channel();
            (tx, rx, Some(_permit))
        };

        let result = self.tx.send(Message {
            request,
            span,
            tx,
            _permit,
        });
        #[cfg(feature = "original-response-cells")]
        match result {
            Err(queue::SendError::Closed(_)) => ResponseFuture::failed(self.get_worker_error()),
            // All actual messages hold one of the same pending permits. This
            // branch refuses an internal invariant violation without growth.
            Err(queue::SendError::Capacity(_)) => ResponseFuture::failed(Box::new(
                std::io::Error::from(std::io::ErrorKind::InvalidData),
            )),
            Ok(()) => ResponseFuture::new(rx),
        }
        #[cfg(not(feature = "original-response-cells"))]
        match result {
            Err(_) => ResponseFuture::failed(self.get_worker_error()),
            Ok(()) => ResponseFuture::new(rx),
        }
    }
}

impl<T, Request> Clone for Buffer<T, Request>
where
    T: Service<Request>,
{
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            handle: self.handle.clone(),
            semaphore: self.semaphore.clone(),
            // The new clone hasn't acquired a permit yet. It will when it's
            // next polled ready.
            permit: None,
            #[cfg(feature = "original-response-cells")]
            original_response_cells: self.original_response_cells.clone(),
        }
    }
}
