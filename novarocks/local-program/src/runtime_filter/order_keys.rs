// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Immutable static order keys retain their actual construction lifetime.

use super::FilterOrderKey;
use novarocks_type_contract::{
    ValueTypeStorageLifetime,
    owned_resources::layout::{LayoutResourceError, arc_layout},
};
use std::{alloc::Layout, fmt, ops::Deref, sync::Arc};

struct CreatedOrderKeys {
    keys: Box<[FilterOrderKey]>,
}

#[derive(Clone)]
enum KeyStorage {
    Legacy(Arc<[FilterOrderKey]>),
    Created(Arc<CreatedOrderKeys>),
}

/// Whole immutable key inventory. Sharing does not copy member declarations or
/// detach the creator of a newly constructed collection.
#[derive(Clone)]
pub struct FilterOrderKeys {
    keys: KeyStorage,
    // Drop the complete Arc, including its allocation, before creator release.
    _storage: ValueTypeStorageLifetime,
}

impl FilterOrderKeys {
    /// Preserve the original ordinary-reference adapter and its exact sharing.
    pub fn from_legacy(keys: Arc<[FilterOrderKey]>) -> Self {
        Self {
            keys: KeyStorage::Legacy(keys),
            _storage: ValueTypeStorageLifetime::pure(),
        }
    }

    /// Admit the actual one-key Box and private Arc holder before each request.
    /// The supplied lifetime comes from the caller's original creation scope;
    /// this constructor creates no capacity or source authority.
    pub fn try_single_observed<E: From<LayoutResourceError>>(
        declared_key: FilterOrderKey,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(Layout) -> Result<(), E>,
    ) -> Result<Self, E> {
        // Locals also keep a rejected prefix ahead of its creator's release.
        let lifetime = storage;
        let key = declared_key;
        let holder = arc_layout(Layout::new::<CreatedOrderKeys>())?;
        request(Layout::new::<[FilterOrderKey; 1]>())?;
        let keys: Box<[FilterOrderKey]> = Box::new([key]);
        request(holder)?;
        Ok(Self {
            keys: KeyStorage::Created(Arc::new(CreatedOrderKeys { keys })),
            _storage: lifetime,
        })
    }

    pub fn as_slice(&self) -> &[FilterOrderKey] {
        match &self.keys {
            KeyStorage::Legacy(keys) => keys,
            KeyStorage::Created(owner) => &owner.keys,
        }
    }
}

impl Deref for FilterOrderKeys {
    type Target = [FilterOrderKey];
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}
impl AsRef<[FilterOrderKey]> for FilterOrderKeys {
    fn as_ref(&self) -> &[FilterOrderKey] {
        self.as_slice()
    }
}
impl fmt::Debug for FilterOrderKeys {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}
impl PartialEq for FilterOrderKeys {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl Eq for FilterOrderKeys {}

impl From<Arc<[FilterOrderKey]>> for FilterOrderKeys {
    fn from(keys: Arc<[FilterOrderKey]>) -> Self {
        Self::from_legacy(keys)
    }
}
impl From<Vec<FilterOrderKey>> for FilterOrderKeys {
    fn from(keys: Vec<FilterOrderKey>) -> Self {
        Self::from_legacy(keys.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FilterNullOrder, FilterSortDirection};
    use novarocks_type_contract::{
        CompileControlError, FunctionValueType, ValueLogicalType, ValueSchemaCreationScope,
        ValueTypeStorageHold,
    };
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct ElementLifetime(Arc<Mutex<Vec<&'static str>>>);
    impl ValueTypeStorageHold for ElementLifetime {}
    impl Drop for ElementLifetime {
        fn drop(&mut self) {
            self.0.lock().unwrap().push("element");
        }
    }
    struct CreationLifetime {
        held: Arc<AtomicUsize>,
        bytes: AtomicUsize,
        exits: Arc<Mutex<Vec<&'static str>>>,
    }
    impl ValueTypeStorageHold for CreationLifetime {}
    impl Drop for CreationLifetime {
        fn drop(&mut self) {
            self.held
                .fetch_sub(self.bytes.load(Ordering::SeqCst), Ordering::SeqCst);
            self.exits.lock().unwrap().push("creator");
        }
    }
    #[derive(Debug, Eq, PartialEq)]
    enum Refusal {
        Layout(LayoutResourceError),
        Host,
        Control(CompileControlError),
    }
    impl From<LayoutResourceError> for Refusal {
        fn from(error: LayoutResourceError) -> Self {
            Self::Layout(error)
        }
    }
    struct Scope {
        hold: Arc<CreationLifetime>,
        remaining: usize,
        calls: usize,
        stop: Option<(usize, CompileControlError)>,
    }
    impl Scope {
        fn new(
            remaining: usize,
            exits: Arc<Mutex<Vec<&'static str>>>,
            held: Arc<AtomicUsize>,
        ) -> Self {
            Self {
                hold: Arc::new(CreationLifetime {
                    held,
                    bytes: AtomicUsize::new(0),
                    exits,
                }),
                remaining,
                calls: 0,
                stop: None,
            }
        }
    }
    impl ValueSchemaCreationScope<Refusal> for Scope {
        fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
            ValueTypeStorageLifetime::held(self.hold.clone())
        }
        fn request_layout(&mut self, layout: Layout) -> Result<(), Refusal> {
            let ordinal = self.calls;
            self.calls += 1;
            if let Some((stop, cause)) = self.stop.clone()
                && stop == ordinal
            {
                return Err(Refusal::Control(cause));
            }
            if layout.size() > self.remaining {
                return Err(Refusal::Host);
            }
            self.remaining -= layout.size();
            self.hold.bytes.fetch_add(layout.size(), Ordering::SeqCst);
            self.hold.held.fetch_add(layout.size(), Ordering::SeqCst);
            Ok(())
        }
        fn observe_opaque_upper_bound(&mut self, _: usize) -> Result<(), Refusal> {
            panic!("static key construction has exact Layout requests");
        }
    }
    fn key(exits: Arc<Mutex<Vec<&'static str>>>) -> FilterOrderKey {
        let storage = ValueTypeStorageLifetime::held(Arc::new(ElementLifetime(exits)));
        FilterOrderKey {
            value_type: FunctionValueType::try_with_logical_type_with_construction::<
                std::convert::Infallible,
            >(
                arrow_schema::DataType::Utf8,
                true,
                ValueLogicalType::Physical,
                storage,
                None,
            )
            .unwrap(),
            direction: FilterSortDirection::Descending,
            null_order: FilterNullOrder::First,
        }
    }

    #[test]
    fn ordered_static_keys_keep_creator_until_last_alias_and_preserve_legacy_content() {
        let exits = Arc::new(Mutex::new(Vec::new()));
        let held = Arc::new(AtomicUsize::new(0));
        let mut scope = Scope::new(usize::MAX, exits.clone(), held.clone());
        let keys = FilterOrderKeys::try_single_observed(
            key(exits.clone()),
            scope.storage_lifetime(),
            |layout| scope.request_layout(layout),
        )
        .unwrap();
        assert_eq!(scope.calls, 2);
        let paid = held.load(Ordering::SeqCst);
        assert!(paid > std::mem::size_of::<FilterOrderKey>());
        let legacy: FilterOrderKeys = vec![key(Arc::new(Mutex::new(Vec::new())))].into();
        assert_eq!(keys, legacy);
        assert_eq!(format!("{keys:?}"), format!("{legacy:?}"));
        let legacy_copy = legacy.clone();
        assert_eq!(legacy.as_ptr(), legacy_copy.as_ptr());
        let last = keys.clone();
        assert_eq!(keys.as_ptr(), last.as_ptr());
        drop(scope);
        drop(keys);
        assert_eq!(held.load(Ordering::SeqCst), paid);
        assert!(exits.lock().unwrap().is_empty());
        std::thread::spawn(move || {
            assert_eq!(last, legacy);
            drop(last);
        })
        .join()
        .unwrap();
        assert_eq!(held.load(Ordering::SeqCst), 0);
        assert_eq!(*exits.lock().unwrap(), ["element", "creator"]);
    }

    #[test]
    fn ordered_static_keys_preserve_each_request_refusal_and_release_the_prefix() {
        for stop in 0..2 {
            for cause in [
                None,
                Some(CompileControlError::Cancelled),
                Some(CompileControlError::DeadlineExceeded),
                Some(CompileControlError::ResourceExhausted),
            ] {
                let exits = Arc::new(Mutex::new(Vec::new()));
                let held = Arc::new(AtomicUsize::new(0));
                let limit = if stop == 0 {
                    0
                } else {
                    std::mem::size_of::<FilterOrderKey>()
                };
                let mut scope = Scope::new(
                    if cause.is_some() { usize::MAX } else { limit },
                    exits.clone(),
                    held.clone(),
                );
                scope.stop = cause.clone().map(|reason| (stop, reason));
                let result = FilterOrderKeys::try_single_observed(
                    key(exits.clone()),
                    scope.storage_lifetime(),
                    |layout| scope.request_layout(layout),
                );
                assert_eq!(
                    result.unwrap_err(),
                    cause.map(Refusal::Control).unwrap_or(Refusal::Host)
                );
                assert_eq!(scope.calls, stop + 1);
                assert_eq!(*exits.lock().unwrap(), ["element"]);
                drop(scope);
                assert_eq!(held.load(Ordering::SeqCst), 0);
                assert_eq!(*exits.lock().unwrap(), ["element", "creator"]);
            }
        }
    }
}
