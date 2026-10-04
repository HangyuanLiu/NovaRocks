//! Private ordinary or caller-prepaid fixed Buffer queue.

#[cfg(not(feature = "original-response-cells"))]
pub(super) use tokio::sync::mpsc::{
    unbounded_channel as pair, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};

#[cfg(feature = "original-response-cells")]
pub(super) use original::{
    allocation_capacity_bound, pair, pair_original, Receiver, SendError, Sender,
};

#[cfg(feature = "original-response-cells")]
mod original {
    use bytes::Bytes;
    use std::{
        alloc::Layout,
        fmt, io,
        sync::{atomic::AtomicUsize, Arc, Mutex, MutexGuard},
        task::{Context, Poll, Waker},
    };
    use tokio::sync::mpsc;

    pub(crate) enum Sender<T> {
        Ordinary(mpsc::UnboundedSender<T>),
        Original(FixedSender<T>),
    }

    pub(crate) enum Receiver<T> {
        Ordinary(mpsc::UnboundedReceiver<T>),
        Original(FixedReceiver<T>),
    }

    pub(crate) enum SendError<T> {
        Closed(T),
        Capacity(T),
    }

    pub(crate) struct FixedSender<T> {
        handle: QueueHandle<T>,
    }

    pub(crate) struct FixedReceiver<T> {
        handle: QueueHandle<T>,
    }

    // No Weak, raw Arc or reference to this Core escapes these private handles.
    struct QueueHandle<T> {
        core: Option<Arc<Core<T>>>,
    }

    struct Core<T> {
        state: Option<Mutex<State<T>>>,
        original: Option<Bytes>,
    }

    struct State<T> {
        slots: Vec<Option<T>>,
        head: usize,
        tail: usize,
        len: usize,
        senders: usize,
        receiver_closed: bool,
        waker: Option<Waker>,
    }

    impl<T> Drop for Core<T> {
        fn drop(&mut self) {
            // Keep the original on the stack through every State destructor,
            // the Vec's unwind cleanup, and the actual mutex PAL destruction.
            let original = self.original.take();
            let state = self.state.take();
            drop(state);
            drop(original);
        }
    }

    impl<T> QueueHandle<T> {
        fn state(&self) -> MutexGuard<'_, State<T>> {
            self.core
                .as_ref()
                .expect("fixed queue handle missing core")
                .state
                .as_ref()
                .expect("fixed queue core missing state")
                .lock()
                // The short critical sections only move values and integers;
                // cleanup still owns the fixed state after an internal panic.
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        }
    }

    impl<T> Clone for QueueHandle<T> {
        fn clone(&self) -> Self {
            Self {
                core: Some(
                    self.core
                        .as_ref()
                        .expect("fixed queue handle missing core")
                        .clone(),
                ),
            }
        }
    }

    impl<T> Drop for QueueHandle<T> {
        fn drop(&mut self) {
            if let Some(core) =
                Arc::into_inner(self.core.take().expect("fixed queue handle missing core"))
            {
                // With no exported Weak, Arc's allocation is already gone.
                drop(core);
            }
        }
    }

    /// Exact requested Core Arc, fixed typed slots, and supported std Mutex PAL.
    /// External values and Waker targets remain independently owned.
    pub(crate) fn allocation_capacity_bound<T>(capacity: usize) -> io::Result<usize> {
        if capacity == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let slots =
            Layout::array::<Option<T>>(capacity).map_err(|_| io::ErrorKind::InvalidInput)?;
        let core = Layout::new::<[AtomicUsize; 2]>()
            .extend(Layout::new::<Core<T>>())
            .map_err(|_| io::ErrorKind::InvalidInput)?
            .0
            .pad_to_align()
            .size();
        #[cfg(all(target_os = "macos", target_pointer_width = "64"))]
        let pal = Layout::new::<(isize, [u8; 56])>().size();
        #[cfg(all(target_os = "linux", target_has_atomic = "32"))]
        let pal = 0;
        #[cfg(not(any(
            all(target_os = "macos", target_pointer_width = "64"),
            all(target_os = "linux", target_has_atomic = "32")
        )))]
        return Err(io::ErrorKind::Unsupported.into());
        #[cfg(any(
            all(target_os = "macos", target_pointer_width = "64"),
            all(target_os = "linux", target_has_atomic = "32")
        ))]
        core.checked_add(slots.size())
            .and_then(|value| value.checked_add(pal))
            .ok_or_else(|| io::ErrorKind::InvalidInput.into())
    }

    pub(crate) fn pair<T>() -> (Sender<T>, Receiver<T>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (Sender::Ordinary(tx), Receiver::Ordinary(rx))
    }

    pub(crate) fn pair_original<T>(
        capacity: usize,
        original: Bytes,
    ) -> io::Result<(Sender<T>, Receiver<T>)> {
        let _bound = allocation_capacity_bound::<T>(capacity)?;
        let mut slots = Vec::new();
        slots
            .try_reserve_exact(capacity)
            .map_err(|_| io::ErrorKind::OutOfMemory)?;
        if slots.capacity() != capacity {
            return Err(io::ErrorKind::InvalidData.into());
        }
        slots.resize_with(capacity, || None);
        let handle = QueueHandle {
            core: Some(Arc::new(Core {
                state: Some(Mutex::new(State {
                    slots,
                    head: 0,
                    tail: 0,
                    len: 0,
                    senders: 1,
                    receiver_closed: false,
                    waker: None,
                })),
                original: Some(original),
            })),
        };
        // Prewarm the actual final Arc-resident mutex before either endpoint
        // is published. This does not alter queue state or invoke callbacks.
        drop(handle.state());
        let rx = FixedReceiver {
            handle: handle.clone(),
        };
        let tx = FixedSender { handle };
        Ok((Sender::Original(tx), Receiver::Original(rx)))
    }

    impl<T> Sender<T> {
        pub(crate) fn is_closed(&self) -> bool {
            match self {
                Self::Ordinary(tx) => tx.is_closed(),
                Self::Original(tx) => tx.handle.state().receiver_closed,
            }
        }

        pub(crate) fn send(&self, value: T) -> Result<(), SendError<T>> {
            match self {
                Self::Ordinary(tx) => tx.send(value).map_err(|error| SendError::Closed(error.0)),
                Self::Original(tx) => {
                    let wake = {
                        let mut state = tx.handle.state();
                        if state.receiver_closed {
                            return Err(SendError::Closed(value));
                        }
                        if state.len == state.slots.len() {
                            return Err(SendError::Capacity(value));
                        }
                        let tail = state.tail;
                        // The occupied prefix cannot contain this free tail.
                        if state.slots[tail].is_some() {
                            return Err(SendError::Capacity(value));
                        }
                        state.slots[tail] = Some(value);
                        state.tail = next(tail, state.slots.len());
                        state.len += 1;
                        state.waker.take()
                    };
                    if let Some(wake) = wake {
                        wake.wake();
                    }
                    Ok(())
                }
            }
        }
    }

    fn next(index: usize, capacity: usize) -> usize {
        if index == capacity - 1 {
            0
        } else {
            index + 1
        }
    }

    impl<T> Clone for Sender<T> {
        fn clone(&self) -> Self {
            match self {
                Self::Ordinary(tx) => Self::Ordinary(tx.clone()),
                Self::Original(tx) => {
                    let handle = tx.handle.clone();
                    {
                        let mut state = handle.state();
                        // Arc's strong-count overflow limit is reached before
                        // a sender count represented by usize can overflow.
                        state.senders = state
                            .senders
                            .checked_add(1)
                            .expect("fixed queue sender count overflow");
                    }
                    Self::Original(FixedSender { handle })
                }
            }
        }
    }

    impl<T> Drop for FixedSender<T> {
        fn drop(&mut self) {
            let wake = {
                let mut state = self.handle.state();
                state.senders = state
                    .senders
                    .checked_sub(1)
                    .expect("fixed queue sender count underflow");
                if state.senders == 0 {
                    state.waker.take()
                } else {
                    None
                }
            };
            // Keep QueueHandle as a field through any user wake panic. Its
            // unwind Drop still uses the last-strong physical Arc exit path.
            if let Some(wake) = wake {
                wake.wake();
            }
        }
    }

    impl<T> Receiver<T> {
        pub(crate) fn close(&mut self) {
            match self {
                Self::Ordinary(rx) => rx.close(),
                Self::Original(rx) => {
                    let wake = {
                        let mut state = rx.handle.state();
                        state.receiver_closed = true;
                        state.waker.take()
                    };
                    if let Some(wake) = wake {
                        wake.wake();
                    }
                }
            }
        }

        pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<T>> {
            match self {
                Self::Ordinary(rx) => rx.poll_recv(cx),
                Self::Original(rx) => {
                    // The ready path neither clones nor invokes the caller's
                    // Waker. Only a possible Pending path needs registration.
                    let ready = {
                        let mut state = rx.handle.state();
                        if state.len != 0 {
                            let head = state.head;
                            let value = state.slots[head].take();
                            debug_assert!(value.is_some());
                            state.head = next(head, state.slots.len());
                            state.len -= 1;
                            Some((Poll::Ready(value), state.waker.take()))
                        } else if state.receiver_closed || state.senders == 0 {
                            Some((Poll::Ready(None), state.waker.take()))
                        } else {
                            None
                        }
                    };
                    if let Some((result, old)) = ready {
                        drop(old);
                        return result;
                    }
                    // User Waker clone runs outside the state lock. The second
                    // state check linearizes registration vs concurrent send.
                    let candidate = cx.waker().clone();
                    let (result, old, unused) = {
                        let mut state = rx.handle.state();
                        if state.len != 0 {
                            let head = state.head;
                            let value = state.slots[head].take();
                            debug_assert!(value.is_some());
                            state.head = next(head, state.slots.len());
                            state.len -= 1;
                            (Poll::Ready(value), state.waker.take(), Some(candidate))
                        } else if state.receiver_closed || state.senders == 0 {
                            (Poll::Ready(None), state.waker.take(), Some(candidate))
                        } else {
                            (Poll::Pending, state.waker.replace(candidate), None)
                        }
                    };
                    drop(old);
                    drop(unused);
                    result
                }
            }
        }
    }

    impl<T> Drop for FixedReceiver<T> {
        fn drop(&mut self) {
            let (slots, wake) = {
                let mut state = self.handle.state();
                state.receiver_closed = true;
                state.len = 0;
                (std::mem::take(&mut state.slots), state.waker.take())
            };
            // A single callback panic cannot skip Vec's remaining-slot/heap
            // unwind cleanup. QueueHandle remains an original-funded field.
            drop(wake);
            drop(slots);
        }
    }

    impl<T> fmt::Debug for Sender<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("BufferQueueSender")
        }
    }
    impl<T> fmt::Debug for Receiver<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("BufferQueueReceiver")
        }
    }
}
