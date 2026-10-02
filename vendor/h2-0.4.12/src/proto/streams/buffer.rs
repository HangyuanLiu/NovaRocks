use slab::Slab;
use std::task::{Context, Poll, Waker};

/// Buffers frames for multiple streams.
#[derive(Debug)]
pub struct Buffer<T> {
    slab: Slab<Slot<T>>,
    receive_limit: Option<usize>,
    receive_task: Option<Waker>,
}

/// A sequence of frames in a `Buffer`
#[derive(Debug)]
pub struct Deque {
    indices: Option<Indices>,
}

/// Tracks the head & tail for a sequence of frames in a `Buffer`.
#[derive(Debug, Default, Copy, Clone)]
struct Indices {
    head: usize,
    tail: usize,
}

#[derive(Debug)]
struct Slot<T> {
    value: T,
    next: Option<usize>,
}

impl<T> Buffer<T> {
    pub fn new() -> Self {
        Buffer {
            slab: Slab::new(),
            receive_limit: None,
            receive_task: None,
        }
    }

    /// Preallocate the complete receive-node slab before any frame can enter.
    pub fn with_receive_limit(limit: Option<usize>) -> Self {
        match limit {
            None => Self::new(),
            Some(limit) => {
                assert!(limit > 0, "receive event capacity must be positive");
                Self {
                    slab: Slab::with_capacity(limit),
                    receive_limit: Some(limit),
                    receive_task: None,
                }
            }
        }
    }

    pub fn poll_receive_ready(&mut self, cx: &Context<'_>) -> Poll<()> {
        if self
            .receive_limit
            .is_some_and(|limit| self.slab.len() >= limit)
        {
            if self
                .receive_task
                .as_ref()
                .map_or(true, |w| !w.will_wake(cx.waker()))
            {
                self.receive_task = Some(cx.waker().clone());
            }
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }

    pub fn is_receive_bounded(&self) -> bool {
        self.receive_limit.is_some()
    }

    pub fn is_empty(&self) -> bool {
        self.slab.is_empty()
    }
}

impl Deque {
    pub fn new() -> Self {
        Deque { indices: None }
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_none()
    }

    pub fn push_back<T>(&mut self, buf: &mut Buffer<T>, value: T) {
        assert!(
            buf.receive_limit
                .map_or(true, |limit| buf.slab.len() < limit),
            "receive event entered without a free node"
        );
        let key = buf.slab.insert(Slot { value, next: None });

        match self.indices {
            Some(ref mut idxs) => {
                buf.slab[idxs.tail].next = Some(key);
                idxs.tail = key;
            }
            None => {
                self.indices = Some(Indices {
                    head: key,
                    tail: key,
                });
            }
        }
    }

    pub fn push_front<T>(&mut self, buf: &mut Buffer<T>, value: T) {
        assert!(
            buf.receive_limit
                .map_or(true, |limit| buf.slab.len() < limit),
            "receive event entered without a free node"
        );
        let key = buf.slab.insert(Slot { value, next: None });

        match self.indices {
            Some(ref mut idxs) => {
                buf.slab[key].next = Some(idxs.head);
                idxs.head = key;
            }
            None => {
                self.indices = Some(Indices {
                    head: key,
                    tail: key,
                });
            }
        }
    }

    pub fn pop_front<T>(&mut self, buf: &mut Buffer<T>) -> Option<T> {
        match self.indices {
            Some(mut idxs) => {
                let mut slot = buf.slab.remove(idxs.head);

                if idxs.head == idxs.tail {
                    assert!(slot.next.is_none());
                    self.indices = None;
                } else {
                    idxs.head = slot.next.take().unwrap();
                    self.indices = Some(idxs);
                }

                // Both DATA (including empty payload) and header events use
                // real nodes. Wake only after the old node leaves the slab.
                if let Some(task) = buf.receive_task.take() {
                    task.wake();
                }
                Some(slot.value)
            }
            None => None,
        }
    }
}
