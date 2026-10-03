//! Separate original position for an eager Channel's actual Tower Worker.
use super::connection_driver::{OriginalConnectionDriver, PreparedConnectionDriver};
use bytes::Bytes;
use std::{future::Future, io};

/// One caller-prepaid logical Channel Worker, independent of physical reconnects.
/// No worker capability is installed in Endpoint or its connection executor.
/// Clones share a once-only election and retain the same original capability.
/// Queue/Semaphore/Handle backing and finite logical admission remain separate.
#[derive(Clone, Debug)]
pub struct OriginalChannelWorker {
    position: OriginalConnectionDriver,
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
    pub(crate) fn prepare(&self) -> io::Result<PreparedChannelWorker> {
        Ok(PreparedChannelWorker {
            prepared: self
                .position
                .reserve_task_bound(Self::task_allocation_capacity_bound()?)?,
        })
    }
}
pub(crate) struct PreparedChannelWorker {
    prepared: PreparedConnectionDriver,
}
impl PreparedChannelWorker {
    pub(crate) fn spawn<F>(self, future: F) -> io::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.prepared.spawn_future(future)
    }
}
