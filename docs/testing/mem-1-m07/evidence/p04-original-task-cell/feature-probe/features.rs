//! Actual Tokio feature/API behavior, with marker facts rather than funding.
//! The source is the real vendored runtime. These probes do not measure task
//! allocator capacity, external Future heap, scheduler/task graph or Native 2MiB.

use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use tokio::runtime::{Builder, Handle, Runtime};

#[derive(Default)]
struct Facts {
    polls: AtomicUsize,
    future_exited: AtomicBool,
    owner_exited: AtomicBool,
    owner_before_future: AtomicBool,
}
struct TinyFuture(Arc<Facts>);
impl Future for TinyFuture {
    type Output = usize;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<usize> {
        self.0.polls.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(17)
    }
}
impl Drop for TinyFuture {
    fn drop(&mut self) {
        self.0.future_exited.store(true, Ordering::SeqCst);
    }
}
struct OwnerExit(Arc<Facts>);
impl Drop for OwnerExit {
    fn drop(&mut self) {
        self.0.owner_before_future.store(
            !self.0.future_exited.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.0.owner_exited.store(true, Ordering::SeqCst);
    }
}
fn marker(facts: &Arc<Facts>) -> bytes::Bytes {
    // This marker has no allocation authority. It only observes physical final
    // Bytes-wrapper exit; this target makes no original-credit capacity claim.
    bytes::Bytes::from_owner_with_exit_guard(bytes::Bytes::new(), OwnerExit(facts.clone()))
}
fn runtime() -> Runtime {
    #[cfg(feature = "multi")]
    let mut builder = {
        let mut builder = Builder::new_multi_thread();
        builder.worker_threads(1);
        builder
    };
    #[cfg(not(feature = "multi"))]
    let mut builder = Builder::new_current_thread();
    builder.build().unwrap()
}

#[test]
fn ordinary_handle_spawn_still_runs_actual_task() {
    let runtime = runtime();
    let facts = Arc::new(Facts::default());
    let task = runtime.handle().spawn(TinyFuture(facts.clone()));
    assert_eq!(runtime.block_on(task).unwrap(), 17);
    drop(runtime);
    assert_eq!(facts.polls.load(Ordering::SeqCst), 1);
    assert!(facts.future_exited.load(Ordering::SeqCst));
}

#[cfg(all(feature = "owner", not(all(tokio_unstable, feature = "trace"))))]
#[test]
fn supported_original_task_owner_api_runs_tiny_future() {
    let runtime = runtime();
    let facts = Arc::new(Facts::default());
    let requested = Handle::task_allocation_capacity_bound::<TinyFuture>().unwrap();
    assert!(
        requested > 0,
        "actual task Cell has a nonzero requested Layout"
    );
    let task = runtime
        .handle()
        .spawn_with_task_owner(TinyFuture(facts.clone()), marker(&facts))
        .unwrap();
    assert_eq!(runtime.block_on(task).unwrap(), 17);
    drop(runtime);
    assert_eq!(facts.polls.load(Ordering::SeqCst), 1);
    assert!(facts.future_exited.load(Ordering::SeqCst));
    assert!(facts.owner_exited.load(Ordering::SeqCst));
    assert!(!facts.owner_before_future.load(Ordering::SeqCst));
}

#[cfg(all(feature = "owner", tokio_unstable, feature = "trace"))]
#[test]
fn unstable_tracing_refuses_owner_api_before_poll_and_drops_future_first() {
    let bound = Handle::task_allocation_capacity_bound::<TinyFuture>().unwrap_err();
    assert_eq!(bound.kind(), std::io::ErrorKind::Unsupported);
    let runtime = runtime();
    let facts = Arc::new(Facts::default());
    let refusal = runtime
        .handle()
        .spawn_with_task_owner(TinyFuture(facts.clone()), marker(&facts))
        .unwrap_err();
    assert_eq!(refusal.kind(), std::io::ErrorKind::Unsupported);
    assert_eq!(
        facts.polls.load(Ordering::SeqCst),
        0,
        "refused Future must never be scheduled"
    );
    assert!(facts.future_exited.load(Ordering::SeqCst));
    assert!(facts.owner_exited.load(Ordering::SeqCst));
    assert!(!facts.owner_before_future.load(Ordering::SeqCst));
    drop(runtime);
}
