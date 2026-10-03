//! Original ownership for the real Tonic live connection driver only.
use super::{
    io::{BoxedIo, OwnedConnectionIo},
    SharedExec,
};
use crate::body::BoxBody;
use bytes::Bytes;
use hyper::rt;
use std::{
    alloc::Layout,
    fmt,
    future::Future,
    io,
    sync::{
        atomic::{AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::task::JoinHandle;

type DriverConnection<I> =
    hyper::client::conn::http2::Connection<OwnedConnectionIo<I>, BoxBody, SharedExec>;
const UNBOUND: u8 = 0;
const RESERVED: u8 = 1;
const SPAWNED: u8 = 2;
const ABANDONED: u8 = 3;
struct Core {
    phase: AtomicU8,
    task_bound: usize,
    task: Mutex<Option<JoinHandle<()>>>,
    original: Bytes,
}
/// One caller-prepaid driver position for one physical attempt. Clones share
/// its one-shot election; no Weak or IO is stored in this holder.
pub struct OriginalConnectionDriver {
    core: Option<Arc<Core>>,
}
impl Clone for OriginalConnectionDriver {
    fn clone(&self) -> Self {
        Self {
            core: Some(Arc::clone(self.core())),
        }
    }
}
impl Drop for OriginalConnectionDriver {
    fn drop(&mut self) {
        if let Some(core) = self.core.take() {
            // Free the actual Arc allocation before its mutex/handle and owner.
            drop(Arc::into_inner(core));
        }
    }
}
impl fmt::Debug for OriginalConnectionDriver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OriginalConnectionDriver")
            .field("task_bound", &self.core().task_bound)
            .finish_non_exhaustive()
    }
}
impl OriginalConnectionDriver {
    fn core(&self) -> &Arc<Core> {
        self.core.as_ref().expect("live original driver holder")
    }
    /// Query the actual live driver constructor for Endpoint's closed connector
    /// output types without constructing a future, IO, or task allocation.
    pub fn task_allocation_capacity_bound() -> io::Result<usize> {
        endpoint_driver_task_bound()
    }
    /// Exact requested Rust metadata in the pinned toolchain plus prewarmed PAL.
    /// Obtain this and the queried actual driver task bound before construction.
    pub fn metadata_allocation_capacity_bound() -> io::Result<usize> {
        let bytes = Layout::new::<[AtomicUsize; 2]>()
            .extend(Layout::new::<Core>())
            .map_err(|_| io::ErrorKind::InvalidInput)?
            .0
            .pad_to_align()
            .size();
        #[cfg(target_vendor = "apple")]
        let bytes = bytes
            .checked_add(Layout::new::<(isize, [u8; 56])>().size())
            .ok_or(io::ErrorKind::InvalidInput)?;
        #[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
        return Err(io::ErrorKind::Unsupported.into());
        #[cfg(any(target_vendor = "apple", target_os = "linux"))]
        Ok(bytes)
    }
    /// Construct only after pregranting the queried task and metadata backing.
    /// The supplied Bytes must retain that original physical capability; this
    /// holder creates no budget authority and does not attest external heaps.
    pub fn with_original(task_bound: usize, owner: Bytes) -> io::Result<Self> {
        Self::metadata_allocation_capacity_bound()?;
        if task_bound == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let original = owner;
        let task = Mutex::new(None);
        drop(task.lock().map_err(|_| io::ErrorKind::InvalidData)?);
        let core = Arc::new(Core {
            phase: AtomicU8::new(UNBOUND),
            task_bound,
            task,
            original: original.clone(),
        });
        drop(original);
        Ok(Self { core: Some(core) })
    }
    /// A finite observation/control handle for transport lifecycle owners.
    /// An unpolled completed handle still retains the real TaskCell allocation.
    pub fn take_task_handle(&self) -> Option<JoinHandle<()>> {
        self.core()
            .task
            .lock()
            .expect("original driver handle lock")
            .take()
    }
    /// Observe the installed handle without polling or consuming it. Completion
    /// does not prove physical TaskCell deallocation or return original credit.
    pub fn task_finished(&self) -> bool {
        self.core()
            .task
            .lock()
            .expect("original driver handle lock")
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
    }
    pub(super) fn reserve<I>(&self) -> io::Result<PreparedConnectionDriver>
    where
        I: rt::Read + rt::Write + Unpin + Send + 'static,
    {
        if driver_task_bound::<I>()? > self.core().task_bound {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.core()
            .phase
            .compare_exchange(UNBOUND, RESERVED, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::ErrorKind::WouldBlock)?;
        Ok(PreparedConnectionDriver {
            grant: self.clone(),
            spawned: false,
        })
    }
}
pub(super) struct PreparedConnectionDriver {
    grant: OriginalConnectionDriver,
    spawned: bool,
}
impl Drop for PreparedConnectionDriver {
    fn drop(&mut self) {
        if !self.spawned {
            let _ = self.grant.core().phase.compare_exchange(
                RESERVED,
                ABANDONED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
}
impl PreparedConnectionDriver {
    pub(super) fn spawn<I>(mut self, conn: DriverConnection<I>) -> io::Result<()>
    where
        I: rt::Read + rt::Write + Unpin + Send + 'static,
    {
        let future = run_driver(conn);
        // This is the constructor used by the static query, not an erased model.
        fn actual<F: Future<Output = ()> + Send + 'static>(_: &F) -> io::Result<usize> {
            tokio::runtime::Handle::task_allocation_capacity_bound::<F>()
        }
        if actual(&future)? > self.grant.core().task_bound {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| io::ErrorKind::NotConnected)?;
        let core = self.grant.core();
        // Runtime spawn hooks may synchronously reenter the public observer.
        // Keep RESERVED through spawn; Prepared Drop retires errors and unwind.
        let task = runtime.spawn_with_task_owner(future, core.original.clone())?;
        let mut slot = core.task.lock().map_err(|_| io::ErrorKind::InvalidData)?;
        *slot = Some(task);
        core.phase.store(SPAWNED, Ordering::Release);
        drop(slot);
        self.spawned = true;
        Ok(())
    }
}
pub(super) async fn run_driver<I>(conn: DriverConnection<I>)
where
    I: rt::Read + rt::Write + Unpin + Send + 'static,
{
    if let Err(error) = conn.await {
        tracing::debug!("connection task error: {:?}", error);
    }
}
fn driver_task_bound<I>() -> io::Result<usize>
where
    I: rt::Read + rt::Write + Unpin + Send + 'static,
{
    fn query<I, F>(_: fn(DriverConnection<I>) -> F) -> io::Result<usize>
    where
        I: rt::Read + rt::Write + Unpin + Send + 'static,
        F: Future<Output = ()> + Send + 'static,
    {
        tokio::runtime::Handle::task_allocation_capacity_bound::<F>()
    }
    query(run_driver::<I>)
}
/// The closed connector outputs installed by Endpoint: direct/typed BoxedIo
/// and the legacy timeout wrapper. This creates no future, IO or allocation.
pub(super) fn endpoint_driver_task_bound() -> io::Result<usize> {
    let direct = driver_task_bound::<BoxedIo>()?;
    type LegacyOutput = <hyper_timeout::TimeoutConnector<
        super::Connector<hyper_util::client::legacy::connect::HttpConnector>,
    > as tower_service::Service<http::Uri>>::Response;
    let timeout = driver_task_bound::<LegacyOutput>()?;
    Ok(direct.max(timeout))
}
