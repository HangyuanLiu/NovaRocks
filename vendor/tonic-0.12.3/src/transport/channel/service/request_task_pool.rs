//! Finite request pairs backed by one caller-prepaid physical IO capability.
use bytes::Bytes;
use hyper::rt::{
    ClientRequestAdmission, ClientRequestAdmissionProvider, ClientRequestTaskGrant,
    ClientRequestTaskKind, ClientRequestTaskLease,
};
use std::{
    alloc::Layout,
    io,
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc,
    },
};

fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| io::ErrorKind::InvalidInput.into())
}
fn multiply(a: usize, b: usize) -> io::Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| io::ErrorKind::InvalidInput.into())
}
fn arc_metadata<T>() -> io::Result<usize> {
    Ok(Layout::new::<[AtomicUsize; 2]>()
        .extend(Layout::new::<T>())
        .map_err(|_| io::ErrorKind::InvalidInput)?
        .0
        .pad_to_align()
        .size())
}
struct PoolCore {
    positions: Vec<AtomicU8>,
    bounds: [usize; 2],
    admission_claimed: AtomicBool,
    original: Bytes,
}
struct Pool {
    core: Option<Arc<PoolCore>>,
}
impl Clone for Pool {
    fn clone(&self) -> Self {
        Self {
            core: self.core.clone(),
        }
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        // No Weak exists. Free the real Arc before its Vec and original owner.
        if let Some(core) = self.core.take() {
            drop(Arc::into_inner(core));
        }
    }
}
impl Pool {
    fn core(&self) -> &Arc<PoolCore> {
        self.core.as_ref().expect("live original request pool")
    }
    fn acquire(&self) -> io::Result<ClientRequestTaskLease> {
        for (index, position) in self.core().positions.iter().enumerate() {
            if position
                .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                // This wrapper's guard exits after its allocation is freed. It
                // stays outside GrantCore, so GrantCore Arc free precedes reuse.
                let original = Bytes::from_owner_with_exit_guard(
                    Bytes::new(),
                    PairExit {
                        pool: self.clone(),
                        index,
                    },
                );
                let grant = Arc::new(GrantCore {
                    bounds: self.core().bounds,
                    phases: [AtomicU8::new(0), AtomicU8::new(0)],
                });
                return Ok(ClientRequestTaskLease::new(grant, original));
            }
        }
        Err(io::ErrorKind::WouldBlock.into())
    }
}
struct PairExit {
    pool: Pool,
    index: usize,
}
impl Drop for PairExit {
    fn drop(&mut self) {
        let old = self.pool.core().positions[self.index].swap(0, Ordering::Release);
        assert_eq!(old, 1, "original request position exits exactly once");
    }
}
struct GrantCore {
    bounds: [usize; 2],
    phases: [AtomicU8; 2],
}
fn role(kind: ClientRequestTaskKind) -> usize {
    match kind {
        ClientRequestTaskKind::Pipe => 0,
        ClientRequestTaskKind::Send => 1,
    }
}
impl ClientRequestTaskGrant for GrantCore {
    fn task_allocation_capacity_bound(&self, kind: ClientRequestTaskKind) -> io::Result<usize> {
        Ok(self.bounds[role(kind)])
    }
    fn elect_dispatch(&self, kind: ClientRequestTaskKind, actual_bound: usize) -> io::Result<()> {
        let index = role(kind);
        if actual_bound > self.bounds[index] {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.phases[index]
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::ErrorKind::WouldBlock)?;
        Ok(())
    }
}
struct Provider {
    pool: Pool,
}
impl ClientRequestAdmissionProvider for Provider {
    fn try_acquire(&self, method: &http::Method) -> io::Result<ClientRequestTaskLease> {
        if method == http::Method::CONNECT {
            return Err(io::ErrorKind::Unsupported.into());
        }
        self.pool.acquire()
    }
}

/// Original finite pair storage for one physical connection. This creates no
/// budget authority; construction requires the queried complete original grant.
/// It contains no task handles, IO, Channel, Weak, or provider backlink.
pub struct OriginalHttp2RequestTaskPool {
    pool: Pool,
}
impl std::fmt::Debug for OriginalHttp2RequestTaskPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OriginalHttp2RequestTaskPool")
            .finish_non_exhaustive()
    }
}
impl Clone for OriginalHttp2RequestTaskPool {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}
impl OriginalHttp2RequestTaskPool {
    /// Complete pool/provider metadata and all pair Cells/carriers. Each physical
    /// connection creates one admission provider; repeated provider construction
    /// is not included. Callback/queue/body/outer Future backing remains separate.
    pub fn allocation_capacity_bound(
        positions: usize,
        pipe_bound: usize,
        send_bound: usize,
    ) -> io::Result<usize> {
        if positions == 0 || pipe_bound == 0 || send_bound == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let per = Self::pair_allocation_capacity_bound(pipe_bound, send_bound)?;
        add(
            add(arc_metadata::<PoolCore>()?, positions)?,
            add(arc_metadata::<Provider>()?, multiply(positions, per)?)?,
        )
    }
    /// Queried derived task pair plus its actual GrantArc, final-exit wrapper and
    /// two independent TaskCell owner wrappers. No callback/queue claim is made.
    pub fn pair_allocation_capacity_bound(
        pipe_bound: usize,
        send_bound: usize,
    ) -> io::Result<usize> {
        if pipe_bound == 0 || send_bound == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        add(
            add(pipe_bound, send_bound)?,
            add(
                arc_metadata::<GrantCore>()?,
                add(
                    Bytes::owner_with_exit_guard_metadata_size::<Bytes, PairExit>(),
                    multiply(
                        2,
                        ClientRequestTaskLease::task_owner_metadata_allocation_capacity_bound(),
                    )?,
                )?,
            )?,
        )
    }
    /// Construct only after reserving the complete queried original backing.
    /// The bounds must come from the final actual executor/future constructors.
    pub fn with_original(
        positions: usize,
        pipe_bound: usize,
        send_bound: usize,
        original: Bytes,
    ) -> io::Result<Self> {
        Self::allocation_capacity_bound(positions, pipe_bound, send_bound)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(positions)
            .map_err(|_| io::ErrorKind::OutOfMemory)?;
        if entries.capacity() != positions {
            return Err(io::ErrorKind::InvalidData.into());
        }
        entries.resize_with(positions, || AtomicU8::new(0));
        Ok(Self {
            pool: Pool {
                core: Some(Arc::new(PoolCore {
                    positions: entries,
                    bounds: [pipe_bound, send_bound],
                    admission_claimed: AtomicBool::new(false),
                    original,
                })),
            },
        })
    }
    pub(crate) fn admission(
        &self,
        pipe_bound: usize,
        send_bound: usize,
    ) -> io::Result<ClientRequestAdmission> {
        // Validate the actual final constructor before provider/connector growth.
        if pipe_bound > self.pool.core().bounds[0] || send_bound > self.pool.core().bounds[1] {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        // Each physical connection prepays exactly one provider. A pool clone
        // cannot reset this election or allocate another provider on reconnect.
        self.pool
            .core()
            .admission_claimed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::ErrorKind::WouldBlock)?;
        Ok(ClientRequestAdmission::new(
            Arc::new(Provider {
                pool: self.pool.clone(),
            }),
            self.pool.core().original.clone(),
        ))
    }
}
