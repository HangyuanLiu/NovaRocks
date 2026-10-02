use super::*;

use super::stream_store::FixedStreamStore;
use crate::codec::UserError;
use indexmap::IndexMap;
use std::task::Poll;

use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::ops;

/// Storage for streams
#[derive(Debug)]
pub(super) struct Store {
    storage: Storage,
    resident_notifications_pending: bool,
}

#[derive(Debug)]
enum Storage {
    Default {
        slab: slab::Slab<Stream>,
        ids: IndexMap<StreamId, SlabIndex>,
    },
    Fixed(FixedStreamStore),
}

/// "Pointer" to an entry in the store
pub(super) struct Ptr<'a> {
    key: Key,
    store: &'a mut Store,
}

/// References an entry in the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Key {
    index: SlabIndex,
    /// Keep the stream ID in the key as an ABA guard, since slab indices
    /// could be re-used with a new stream.
    stream_id: StreamId,
}

// We can never have more than `StreamId::MAX` streams in the store,
// so we can save a smaller index (u32 vs usize).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SlabIndex(u32);

pub(super) struct Queue<N> {
    indices: Option<store::Indices>,
    _p: PhantomData<N>,
}

pub(super) trait Next {
    fn next(stream: &Stream) -> Option<Key>;

    fn set_next(stream: &mut Stream, key: Option<Key>);

    fn take_next(stream: &mut Stream) -> Option<Key>;

    fn is_queued(stream: &Stream) -> bool;

    fn set_queued(stream: &mut Stream, val: bool);
}

/// A linked list
#[derive(Debug, Clone, Copy)]
struct Indices {
    pub head: Key,
    pub tail: Key,
}

pub(super) enum Entry<'a> {
    Occupied(OccupiedEntry<'a>),
    Vacant(VacantEntry<'a>),
}

pub(super) struct OccupiedEntry<'a> {
    key: Key,
    _store: PhantomData<&'a mut Store>,
}

pub(super) struct VacantEntry<'a> {
    store: &'a mut Store,
    id: StreamId,
}

pub(super) trait Resolve {
    fn resolve(&mut self, key: Key) -> Ptr<'_>;
}

// ===== impl Store =====

impl Store {
    pub fn new(fixed: Option<FixedStreamStore>) -> Self {
        Self {
            resident_notifications_pending: false,
            storage: match fixed {
                Some(storage) => Storage::Fixed(storage),
                None => Storage::Default {
                    slab: slab::Slab::new(),
                    ids: IndexMap::new(),
                },
            },
        }
    }

    fn linked_index(&self, id: StreamId) -> Option<SlabIndex> {
        match &self.storage {
            Storage::Default { ids, .. } => ids.get(&id).copied(),
            Storage::Fixed(storage) => storage
                .slots
                .iter()
                .position(|slot| {
                    slot.linked && slot.value.as_ref().is_some_and(|stream| stream.id == id)
                })
                .map(|index| SlabIndex(index as u32)),
        }
    }

    fn linked_len(&self) -> usize {
        match &self.storage {
            Storage::Default { ids, .. } => ids.len(),
            Storage::Fixed(storage) => storage.slots.iter().filter(|slot| slot.linked).count(),
        }
    }

    #[cfg(feature = "unstable")]
    fn resident_len(&self) -> usize {
        match &self.storage {
            Storage::Default { slab, .. } => slab.len(),
            Storage::Fixed(storage) => storage
                .slots
                .iter()
                .filter(|slot| slot.value.is_some())
                .count(),
        }
    }

    fn linked_at(&self, ordinal: usize) -> Option<Key> {
        match &self.storage {
            Storage::Default { ids, .. } => ids
                .get_index(ordinal)
                .map(|(&stream_id, &index)| Key { index, stream_id }),
            Storage::Fixed(storage) => storage
                .slots
                .iter()
                .enumerate()
                .filter(|(_, slot)| slot.linked)
                .nth(ordinal)
                .map(|(index, slot)| Key {
                    index: SlabIndex(index as u32),
                    stream_id: slot.value.as_ref().expect("linked slot owns a stream").id,
                }),
        }
    }

    /// A closed wire stream retains its position until its actual state exits.
    pub fn has_capacity(&self) -> bool {
        match &self.storage {
            Storage::Default { .. } => true,
            Storage::Fixed(storage) => storage.slots.iter().any(|slot| slot.value.is_none()),
        }
    }

    /// Registration belongs to one SendRequest handle, so repolls and canceled
    /// handles cannot accumulate stale positions. Waker callbacks run outside
    /// the connection lock; return the previous waker to the caller for drop.
    pub fn poll_resident_ready(
        &mut self,
        registration: &mut Option<usize>,
        new_waker: &mut Option<std::task::Waker>,
        retired: &mut Option<std::task::Waker>,
    ) -> Poll<Result<(), UserError>> {
        if self.has_capacity() {
            *retired = self.unregister_resident_waiter(registration);
            return Poll::Ready(Ok(()));
        }
        let Storage::Fixed(storage) = &mut self.storage else {
            unreachable!()
        };
        let index = match *registration {
            Some(index) => index,
            None => {
                let Some(index) = storage.waiters.iter().position(|waiter| !waiter.leased) else {
                    return Poll::Ready(Err(UserError::Rejected));
                };
                storage.waiters[index].leased = true;
                *registration = Some(index);
                index
            }
        };
        let waiter = &mut storage.waiters[index];
        *retired = std::mem::replace(&mut waiter.waker, new_waker.take());
        Poll::Pending
    }

    pub fn unregister_resident_waiter(
        &mut self,
        registration: &mut Option<usize>,
    ) -> Option<std::task::Waker> {
        let index = registration.take()?;
        let Storage::Fixed(storage) = &mut self.storage else {
            unreachable!()
        };
        let waiter = &mut storage.waiters[index];
        assert!(waiter.leased);
        waiter.leased = false;
        waiter.notify = false;
        waiter.waker.take()
    }

    pub fn wake_resident_waiters(&mut self) {
        if let Storage::Fixed(storage) = &mut self.storage {
            for waiter in &mut storage.waiters {
                if waiter.waker.is_some() {
                    waiter.notify = true;
                    self.resident_notifications_pending = true;
                }
            }
        }
    }

    pub fn resident_notification_count(&mut self) -> usize {
        if !std::mem::take(&mut self.resident_notifications_pending) {
            return 0;
        }
        match &self.storage {
            Storage::Fixed(storage) => storage.waiters.len(),
            Storage::Default { .. } => 0,
        }
    }

    pub fn take_resident_notification(&mut self, index: usize) -> Option<std::task::Waker> {
        let Storage::Fixed(storage) = &mut self.storage else {
            return None;
        };
        let waiter = &mut storage.waiters[index];
        if !waiter.notify {
            return None;
        }
        waiter.notify = false;
        waiter.waker.take()
    }

    pub fn find_mut(&mut self, id: &StreamId) -> Option<Ptr<'_>> {
        let index = self.linked_index(*id)?;
        Some(Ptr {
            key: Key {
                index,
                stream_id: *id,
            },
            store: self,
        })
    }

    pub fn insert(&mut self, id: StreamId, value: Stream) -> Result<Ptr<'_>, UserError> {
        let key = self.insert_key(id, value)?;
        Ok(Ptr { key, store: self })
    }

    fn insert_key(&mut self, id: StreamId, value: Stream) -> Result<Key, UserError> {
        if !self.has_capacity() {
            return Err(UserError::Rejected);
        }
        assert_eq!(value.id, id);
        assert!(self.linked_index(id).is_none());
        let index = match &mut self.storage {
            Storage::Default { slab, ids } => {
                let index = SlabIndex(slab.insert(value) as u32);
                assert!(ids.insert(id, index).is_none());
                index
            }
            Storage::Fixed(storage) => {
                let index = storage
                    .slots
                    .iter()
                    .position(|slot| slot.value.is_none())
                    .expect("capacity checked under the connection lock");
                storage.slots[index].value = Some(value);
                storage.slots[index].linked = true;
                SlabIndex(index as u32)
            }
        };
        Ok(Key {
            index,
            stream_id: id,
        })
    }

    pub fn find_entry(&mut self, id: StreamId) -> Entry<'_> {
        match self.linked_index(id) {
            Some(index) => Entry::Occupied(OccupiedEntry {
                key: Key {
                    index,
                    stream_id: id,
                },
                _store: PhantomData,
            }),
            None => Entry::Vacant(VacantEntry { store: self, id }),
        }
    }

    fn unlink(&mut self, key: Key) {
        match &mut self.storage {
            Storage::Default { ids, .. } => {
                ids.swap_remove(&key.stream_id);
            }
            Storage::Fixed(storage) => {
                let slot = &mut storage.slots[key.index.0 as usize];
                assert_eq!(
                    slot.value.as_ref().expect("live store key").id,
                    key.stream_id
                );
                slot.linked = false;
            }
        }
    }

    fn remove(&mut self, key: Key) -> StreamId {
        debug_assert!(self.linked_index(key.stream_id).is_none());
        let stream = match &mut self.storage {
            Storage::Default { slab, .. } => slab.remove(key.index.0 as usize),
            Storage::Fixed(storage) => storage.slots[key.index.0 as usize]
                .value
                .take()
                .expect("live store key"),
        };
        assert_eq!(stream.id, key.stream_id);
        let id = stream.id;
        // Destruct the state and its remaining fields before notifying anyone
        // that a new resident can occupy the original fixed position.
        drop(stream);
        self.wake_resident_waiters();
        id
    }

    #[allow(clippy::blocks_in_conditions)]
    pub(crate) fn for_each<F>(&mut self, mut f: F)
    where
        F: FnMut(Ptr),
    {
        match self.try_for_each(|ptr| {
            f(ptr);
            Ok::<_, Infallible>(())
        }) {
            Ok(()) => (),
            #[allow(unused)]
            Err(infallible) => match infallible {},
        }
    }

    pub fn try_for_each<F, E>(&mut self, mut f: F) -> Result<(), E>
    where
        F: FnMut(Ptr) -> Result<(), E>,
    {
        let mut len = self.linked_len();
        let mut i = 0;

        while i < len {
            // Get the key by index, this makes the borrow checker happy
            let key = self.linked_at(i).expect("linked traversal index");
            f(Ptr { key, store: self })?;

            // TODO: This logic probably could be better...
            let new_len = self.linked_len();

            if new_len < len {
                debug_assert!(new_len == len - 1);
                len -= 1;
            } else {
                i += 1;
            }
        }

        Ok(())
    }
}

impl Resolve for Store {
    fn resolve(&mut self, key: Key) -> Ptr<'_> {
        Ptr { key, store: self }
    }
}

impl ops::Index<Key> for Store {
    type Output = Stream;

    fn index(&self, key: Key) -> &Self::Output {
        let stream = match &self.storage {
            Storage::Default { slab, .. } => slab.get(key.index.0 as usize),
            Storage::Fixed(storage) => storage
                .slots
                .get(key.index.0 as usize)
                .and_then(|slot| slot.value.as_ref()),
        };
        stream
            .filter(|stream| stream.id == key.stream_id)
            .unwrap_or_else(|| panic!("dangling store key for stream_id={:?}", key.stream_id))
    }
}

impl ops::IndexMut<Key> for Store {
    fn index_mut(&mut self, key: Key) -> &mut Stream {
        let stream = match &mut self.storage {
            Storage::Default { slab, .. } => slab.get_mut(key.index.0 as usize),
            Storage::Fixed(storage) => storage
                .slots
                .get_mut(key.index.0 as usize)
                .and_then(|slot| slot.value.as_mut()),
        };
        stream
            .filter(|stream| stream.id == key.stream_id)
            .unwrap_or_else(|| panic!("dangling store key for stream_id={:?}", key.stream_id))
    }
}

impl Store {
    #[cfg(feature = "unstable")]
    pub fn num_active_streams(&self) -> usize {
        self.linked_len()
    }

    #[cfg(feature = "unstable")]
    pub fn num_wired_streams(&self) -> usize {
        self.resident_len()
    }
}

// While running h2 unit/integration tests, enable this debug assertion.
//
// In practice, we don't need to ensure this. But the integration tests
// help to make sure we've cleaned up in cases where we could (like, the
// runtime isn't suddenly dropping the task for unknown reasons).
#[cfg(feature = "unstable")]
impl Drop for Store {
    fn drop(&mut self) {
        use std::thread;

        if !thread::panicking() {
            debug_assert_eq!(self.resident_len(), 0);
        }
    }
}

// ===== impl Queue =====

impl<N> Queue<N>
where
    N: Next,
{
    pub fn new() -> Self {
        Queue {
            indices: None,
            _p: PhantomData,
        }
    }

    pub fn take(&mut self) -> Self {
        Queue {
            indices: self.indices.take(),
            _p: PhantomData,
        }
    }

    /// Queue the stream.
    ///
    /// If the stream is already contained by the list, return `false`.
    pub fn push(&mut self, stream: &mut store::Ptr) -> bool {
        tracing::trace!("Queue::push_back");

        if N::is_queued(stream) {
            tracing::trace!(" -> already queued");
            return false;
        }

        N::set_queued(stream, true);

        // The next pointer shouldn't be set
        debug_assert!(N::next(stream).is_none());

        // Queue the stream
        match self.indices {
            Some(ref mut idxs) => {
                tracing::trace!(" -> existing entries");

                // Update the current tail node to point to `stream`
                let key = stream.key();
                N::set_next(&mut stream.resolve(idxs.tail), Some(key));

                // Update the tail pointer
                idxs.tail = stream.key();
            }
            None => {
                tracing::trace!(" -> first entry");
                self.indices = Some(store::Indices {
                    head: stream.key(),
                    tail: stream.key(),
                });
            }
        }

        true
    }

    /// Queue the stream
    ///
    /// If the stream is already contained by the list, return `false`.
    pub fn push_front(&mut self, stream: &mut store::Ptr) -> bool {
        tracing::trace!("Queue::push_front");

        if N::is_queued(stream) {
            tracing::trace!(" -> already queued");
            return false;
        }

        N::set_queued(stream, true);

        // The next pointer shouldn't be set
        debug_assert!(N::next(stream).is_none());

        // Queue the stream
        match self.indices {
            Some(ref mut idxs) => {
                tracing::trace!(" -> existing entries");

                // Update the provided stream to point to the head node
                let head_key = stream.resolve(idxs.head).key();
                N::set_next(stream, Some(head_key));

                // Update the head pointer
                idxs.head = stream.key();
            }
            None => {
                tracing::trace!(" -> first entry");
                self.indices = Some(store::Indices {
                    head: stream.key(),
                    tail: stream.key(),
                });
            }
        }

        true
    }

    pub fn pop<'a, R>(&mut self, store: &'a mut R) -> Option<store::Ptr<'a>>
    where
        R: Resolve,
    {
        if let Some(mut idxs) = self.indices {
            let mut stream = store.resolve(idxs.head);

            if idxs.head == idxs.tail {
                assert!(N::next(&stream).is_none());
                self.indices = None;
            } else {
                idxs.head = N::take_next(&mut stream).unwrap();
                self.indices = Some(idxs);
            }

            debug_assert!(N::is_queued(&stream));
            N::set_queued(&mut stream, false);

            return Some(stream);
        }

        None
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_none()
    }

    pub fn pop_if<'a, R, F>(&mut self, store: &'a mut R, f: F) -> Option<store::Ptr<'a>>
    where
        R: Resolve,
        F: Fn(&Stream) -> bool,
    {
        if let Some(idxs) = self.indices {
            let should_pop = f(&store.resolve(idxs.head));
            if should_pop {
                return self.pop(store);
            }
        }

        None
    }
}

impl<N> fmt::Debug for Queue<N> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Queue")
            .field("indices", &self.indices)
            // skip phantom data
            .finish()
    }
}

// ===== impl Ptr =====

impl<'a> Ptr<'a> {
    /// Returns the Key associated with the stream
    pub fn key(&self) -> Key {
        self.key
    }

    pub fn store_mut(&mut self) -> &mut Store {
        self.store
    }

    /// Remove the stream from the store
    pub fn remove(self) -> StreamId {
        self.store.remove(self.key)
    }

    /// Remove only the wire association; the original resident remains.
    pub fn unlink(&mut self) {
        self.store.unlink(self.key);
    }
}

impl<'a> Resolve for Ptr<'a> {
    fn resolve(&mut self, key: Key) -> Ptr<'_> {
        Ptr {
            key,
            store: &mut *self.store,
        }
    }
}

impl<'a> ops::Deref for Ptr<'a> {
    type Target = Stream;

    fn deref(&self) -> &Stream {
        &self.store[self.key]
    }
}

impl<'a> ops::DerefMut for Ptr<'a> {
    fn deref_mut(&mut self) -> &mut Stream {
        &mut self.store[self.key]
    }
}

impl<'a> fmt::Debug for Ptr<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        (**self).fmt(fmt)
    }
}

// ===== impl OccupiedEntry =====

impl<'a> OccupiedEntry<'a> {
    pub fn key(&self) -> Key {
        self.key
    }
}

// ===== impl VacantEntry =====

impl<'a> VacantEntry<'a> {
    pub fn has_capacity(&self) -> bool {
        self.store.has_capacity()
    }

    pub fn insert(self, value: Stream) -> Result<Key, UserError> {
        self.store.insert_key(self.id, value)
    }
}
