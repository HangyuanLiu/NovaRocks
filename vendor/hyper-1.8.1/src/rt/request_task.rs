//! Caller-owned finite admission for an HTTP/2 request's two derived tasks.
use bytes::Bytes;
use std::{fmt, io, sync::Arc};

/// The two independent actual task positions of one request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientRequestTaskKind {
    /// Pending request body pipe.
    Pipe,
    /// Response/callback task.
    Send,
}

/// A caller's already-prepaid pair. This interface creates no budget authority.
/// Each role must be elected at most once; errors must not reset its election.
/// The enclosing original owner must cover all grant metadata aliases.
pub trait ClientRequestTaskGrant: Send + Sync {
    /// Queried actual constructor bound prepaid before this grant was allocated.
    fn task_allocation_capacity_bound(&self, kind: ClientRequestTaskKind) -> io::Result<usize>;
    /// Refuse an actual future exceeding its prepaid bound or a repeated role.
    fn elect_dispatch(&self, kind: ClientRequestTaskKind, actual_bound: usize) -> io::Result<()>;
}

/// A pair's metadata and original owner, retained together through every alias.
/// Clones share one election. No Weak or independently extractable grant exists.
/// The caller must put the position's final exit guard in `original`, so it runs
/// only after this handle's last grant Arc has actually been deallocated.
pub struct ClientRequestTaskLease {
    grant: Option<Arc<dyn ClientRequestTaskGrant>>,
    original: Bytes,
}
impl ClientRequestTaskLease {
    /// Move in caller-preallocated metadata and its prepaid original owner.
    /// This constructor allocates nothing and validates no external backing.
    pub fn new(grant: Arc<dyn ClientRequestTaskGrant>, original: Bytes) -> Self {
        Self {
            grant: Some(grant),
            original,
        }
    }
    /// Query the role's original prepaid bound.
    pub fn task_allocation_capacity_bound(&self, kind: ClientRequestTaskKind) -> io::Result<usize> {
        self.grant
            .as_ref()
            .expect("live request task lease")
            .task_allocation_capacity_bound(kind)
    }
    /// Elect one actual dispatch. A refusal creates no task and never falls back.
    pub fn elect_dispatch(
        &self,
        kind: ClientRequestTaskKind,
        actual_bound: usize,
    ) -> io::Result<()> {
        self.grant
            .as_ref()
            .expect("live request task lease")
            .elect_dispatch(kind, actual_bound)
    }
    /// Exact metadata of the separate Bytes task-owner wrapper below. Prepay it
    /// before claiming a pair; it is not covered by the actual future's Cell.
    pub fn task_owner_metadata_allocation_capacity_bound() -> usize {
        Bytes::owner_with_exit_guard_metadata_size::<Bytes, Self>()
    }
    /// Retain this same pair through actual TaskCell deallocation, beyond the
    /// future's own exit. The original pregrant must cover the queried wrapper.
    pub fn into_task_owner(self) -> Bytes {
        Bytes::from_owner_with_exit_guard(Bytes::new(), self)
    }
}
impl Clone for ClientRequestTaskLease {
    fn clone(&self) -> Self {
        Self {
            grant: self.grant.clone(),
            original: self.original.clone(),
        }
    }
}
impl Drop for ClientRequestTaskLease {
    fn drop(&mut self) {
        // The original/position exit guard remains live during actual Arc free.
        drop(self.grant.take());
    }
}

/// A finite caller-owned pool. Refusal must precede callback/channel allocation,
/// request body inspection and HEADERS. An original mode with no upgrade grant
/// must reject CONNECT here before returning any request task pair.
pub trait ClientRequestAdmissionProvider: Send + Sync {
    /// Obtain the complete pair atomically; never wait for its second position.
    fn try_acquire(&self, method: &http::Method) -> io::Result<ClientRequestTaskLease>;
}

/// The connection's admission metadata and original owner, inseparable by clone.
/// Every externally retained provider Arc must also retain its prepaid owner.
/// Native composition uses a closed provider; this interface attests no caller
/// allocations or ownership omitted from that provider's original pregrant.
pub struct ClientRequestAdmission {
    provider: Option<Arc<dyn ClientRequestAdmissionProvider>>,
    original: Bytes,
}
impl ClientRequestAdmission {
    /// Move in caller-preallocated provider metadata and its original owner.
    pub fn new(provider: Arc<dyn ClientRequestAdmissionProvider>, original: Bytes) -> Self {
        Self {
            provider: Some(provider),
            original,
        }
    }
    /// Perform finite admission before constructing the request's dispatch state.
    pub fn try_acquire(&self, method: &http::Method) -> io::Result<ClientRequestTaskLease> {
        self.provider
            .as_ref()
            .expect("live request admission")
            .try_acquire(method)
    }
}
impl Clone for ClientRequestAdmission {
    fn clone(&self) -> Self {
        Self {
            provider: self.provider.clone(),
            original: self.original.clone(),
        }
    }
}
impl Drop for ClientRequestAdmission {
    fn drop(&mut self) {
        drop(self.provider.take());
    }
}

impl fmt::Debug for ClientRequestTaskLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientRequestTaskLease")
            .finish_non_exhaustive()
    }
}
impl fmt::Debug for ClientRequestAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientRequestAdmission")
            .finish_non_exhaustive()
    }
}

pin_project_lite::pin_project! {
    /// Keep callback receiver destruction before returning the request position.
    pub(crate) struct OriginalClientResponseFuture<F> {
        #[pin]
        future: Option<F>,
        lease: Option<ClientRequestTaskLease>,
    }
    impl<F> PinnedDrop for OriginalClientResponseFuture<F> {
        fn drop(this: Pin<&mut Self>) {
            let mut this = this.project();
            this.future.set(None);
            drop(this.lease.take());
        }
    }
}
impl<F> OriginalClientResponseFuture<F> {
    pub(crate) fn new(future: F, lease: Option<ClientRequestTaskLease>) -> Self {
        Self {
            future: Some(future),
            lease,
        }
    }
}
impl<F: std::future::Future> std::future::Future for OriginalClientResponseFuture<F> {
    type Output = F::Output;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.project()
            .future
            .as_pin_mut()
            .expect("live request response future")
            .poll(cx)
    }
}
