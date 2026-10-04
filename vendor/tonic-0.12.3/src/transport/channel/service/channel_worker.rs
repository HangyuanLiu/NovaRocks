//! Separate original position for an eager Channel's actual Tower Worker.
use super::super::Svc;
use super::connection_driver::{OriginalConnectionDriver, PreparedConnectionDriver};
use crate::body::BoxBody;
use bytes::Bytes;
use http::Request;
use tower::buffer::Buffer;
type ChannelBuffer = Buffer<Svc, Request<BoxBody>>;
use std::{future::Future, io};

/// One caller-prepaid logical Channel Worker, independent of physical reconnects.
/// No worker capability is installed in Endpoint or its connection executor.
/// Clones share a once-only election and retain the same original capability.
/// Queue/Semaphore/Handle backing and finite logical admission remain separate.
#[derive(Clone, Debug)]
pub struct OriginalChannelWorker {
    position: OriginalConnectionDriver,
    #[cfg(feature = "original-response-cells")]
    response_positions: Option<usize>,
}
impl OriginalChannelWorker {
    /// Query the real private Worker return type of the actual Buffer::pair
    /// constructor without executing it or allocating a service/queue/model.
    pub fn task_allocation_capacity_bound() -> io::Result<usize> {
        use super::super::Svc;
        use crate::body::BoxBody;
        use http::Request;
        use tower::buffer::Buffer;
        fn query<W>(_: fn(Svc, usize) -> (Buffer<Svc, Request<BoxBody>>, W)) -> io::Result<usize>
        where
            W: Future<Output = ()> + Send + 'static,
        {
            tokio::runtime::Handle::task_allocation_capacity_bound::<W>()
        }
        query(Buffer::<Svc, Request<BoxBody>>::pair)
    }
    /// Strong-only Core and prewarmed metadata reused by the physical driver.
    /// This is metadata geometry; it does not borrow a driver's position.
    pub fn metadata_allocation_capacity_bound() -> io::Result<usize> {
        OriginalConnectionDriver::metadata_allocation_capacity_bound()
    }
    /// Construct from an already prepaid logical Channel task and metadata grant.
    /// This creates no budget and attests no omitted external allocation graph.
    pub fn with_original(task_bound: usize, original: Bytes) -> io::Result<Self> {
        Ok(Self {
            position: OriginalConnectionDriver::with_original(task_bound, original)?,
            #[cfg(feature = "original-response-cells")]
            response_positions: None,
        })
    }
    /// Exact cell and owner-wrapper backing per pending response position.
    /// External service future, queue, semaphore and shared runtime are separate.
    #[cfg(feature = "original-response-cells")]
    pub fn response_cell_total_capacity_bound() -> io::Result<usize> {
        ChannelBuffer::response_cell_total_capacity_bound()
    }
    /// Opt into physical response-cell admission on the same original stock.
    /// Obtain task/metadata and positions * response_cell_total_capacity_bound
    /// before construction. This creates no funding authority or second wallet.
    #[cfg(feature = "original-response-cells")]
    pub fn with_original_response_cells(
        task_bound: usize,
        positions: usize,
        original: Bytes,
    ) -> io::Result<Self> {
        if positions == 0 || positions > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Self::response_cell_total_capacity_bound()?
            .checked_mul(positions)
            .ok_or(io::ErrorKind::InvalidInput)?;
        Ok(Self {
            position: OriginalConnectionDriver::with_original(task_bound, original)?,
            response_positions: Some(positions),
        })
    }
    /// Taking the actual JoinHandle does not retire the original TaskCell.
    pub fn take_task_handle(&self) -> Option<tokio::task::JoinHandle<()>> {
        self.position.take_task_handle()
    }
    /// Completion alone proves no physical exit or original grant return.
    pub fn task_finished(&self) -> bool {
        self.position.task_finished()
    }
    pub(crate) fn prepare(&self, _buffer_size: usize) -> io::Result<PreparedChannelWorker> {
        #[cfg(feature = "original-response-cells")]
        if self
            .response_positions
            .is_some_and(|positions| positions != _buffer_size)
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        #[cfg(feature = "original-response-cells")]
        let response_cells = self
            .response_positions
            .map(|_| {
                Ok::<_, io::Error>((
                    ChannelBuffer::response_cell_allocation_capacity_bound()?,
                    self.position.original_owner(),
                ))
            })
            .transpose()?;
        Ok(PreparedChannelWorker {
            prepared: self
                .position
                .reserve_task_bound(Self::task_allocation_capacity_bound()?)?,
            #[cfg(feature = "original-response-cells")]
            response_cells,
        })
    }
}
pub(crate) struct PreparedChannelWorker {
    prepared: PreparedConnectionDriver,
    #[cfg(feature = "original-response-cells")]
    response_cells: Option<(usize, Bytes)>,
}
impl PreparedChannelWorker {
    pub(crate) fn pair(
        _original: Option<&Self>,
        service: Svc,
        bound: usize,
    ) -> io::Result<(ChannelBuffer, impl Future<Output = ()> + Send)> {
        #[cfg(feature = "original-response-cells")]
        {
            match _original.and_then(|original| original.response_cells.as_ref()) {
                Some((cell_bound, owner)) => ChannelBuffer::pair_with_original_response_cells(
                    service,
                    bound,
                    *cell_bound,
                    owner.clone(),
                ),
                None => Ok(ChannelBuffer::pair(service, bound)),
            }
        }
        #[cfg(not(feature = "original-response-cells"))]
        {
            Ok(ChannelBuffer::pair(service, bound))
        }
    }
    pub(crate) fn spawn<F>(self, future: F) -> io::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.prepared.spawn_future(future)
    }
}
