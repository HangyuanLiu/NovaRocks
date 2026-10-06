// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The synchronization vocabulary shared by production and model checking.
#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicI64, AtomicPtr, AtomicU32, AtomicU64, Ordering};
#[cfg(loom)]
pub(crate) use loom::sync::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicI64, AtomicPtr, AtomicU32, AtomicU64, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::{
    Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
};

// Loom does not expose Weak. Model the same strong/weak ownership protocol
// with tracked strong admission over stable std backing. The backing supplies
// actual address validity; its lifetime is checked separately with Miri. Every
// participating clone, final release, weak upgrade and strong-count query uses
// the tracked counter, so registry/last-reference interleavings are explored.
#[cfg(loom)]
pub(crate) use modeled::{Arc, Weak};
#[cfg(loom)]
mod modeled {
    use loom::sync::atomic::{AtomicUsize, Ordering};
    use std::{fmt, ops::Deref};
    struct Inner<T> {
        value: T,
        strong: AtomicUsize,
    }
    pub struct Arc<T>(std::sync::Arc<Inner<T>>);
    pub struct Weak<T>(std::sync::Weak<Inner<T>>);
    impl<T> Arc<T> {
        pub fn new(value: T) -> Self {
            Self(std::sync::Arc::new(Inner {
                value,
                strong: AtomicUsize::new(1),
            }))
        }
        pub fn downgrade(value: &Self) -> Weak<T> {
            Weak(std::sync::Arc::downgrade(&value.0))
        }
        pub fn ptr_eq(a: &Self, b: &Self) -> bool {
            std::sync::Arc::ptr_eq(&a.0, &b.0)
        }
        pub fn strong_count(value: &Self) -> usize {
            value.0.strong.load(Ordering::Acquire)
        }
    }
    impl<T> Clone for Arc<T> {
        fn clone(&self) -> Self {
            let old = self.0.strong.fetch_add(1, Ordering::Relaxed);
            assert!(old != 0);
            Self(self.0.clone())
        }
    }
    impl<T> Drop for Arc<T> {
        fn drop(&mut self) {
            assert!(self.0.strong.fetch_sub(1, Ordering::AcqRel) != 0);
        }
    }
    impl<T> Deref for Arc<T> {
        type Target = T;
        fn deref(&self) -> &T {
            &self.0.value
        }
    }
    impl<T: fmt::Debug> fmt::Debug for Arc<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.value.fmt(f)
        }
    }
    impl<T> Weak<T> {
        pub fn upgrade(&self) -> Option<Arc<T>> {
            let pinned = self.0.upgrade()?;
            let mut strong = pinned.strong.load(Ordering::Acquire);
            loop {
                if strong == 0 {
                    return None;
                }
                match pinned.strong.compare_exchange_weak(
                    strong,
                    strong + 1,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Some(Arc(pinned)),
                    Err(actual) => strong = actual,
                }
            }
        }
    }
    impl<T> fmt::Debug for Weak<T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("WeakAccount")
        }
    }
}
