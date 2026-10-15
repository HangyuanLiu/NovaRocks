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
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Immutable type-fact collections with their own finite creation lifetime.

use crate::owned_resources::layout::{LayoutResourceError, arc_layout};
use crate::{FunctionArgumentType, FunctionValueType, ValueTypeStorageLifetime};
use std::{
    alloc::Layout,
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
};

mod sealed {
    pub trait Sealed {}
}
/// Closed immutable type vocabulary; runtime services cannot inhabit this owner.
pub trait TypedFact: sealed::Sealed + Clone + Eq + Hash + fmt::Debug {}
impl<T: sealed::Sealed + Clone + Eq + Hash + fmt::Debug> TypedFact for T {}
impl sealed::Sealed for FunctionValueType {}
impl sealed::Sealed for FunctionArgumentType {}
impl sealed::Sealed for crate::AggregateStateOrderKey {}

struct FactSliceBacking<T> {
    items: Box<[T]>,
    // Drop the actual collection before releasing its creator responsibility.
    storage: ValueTypeStorageLifetime,
}

pub struct TypedFactSlice<T: TypedFact>(Option<Arc<FactSliceBacking<T>>>);
pub type FunctionArgumentTypes = TypedFactSlice<FunctionArgumentType>;
pub type FunctionParameterTypes = TypedFactSlice<FunctionValueType>;
pub type AggregateStateOrderKeys = TypedFactSlice<crate::AggregateStateOrderKey>;

impl<T: TypedFact> TypedFactSlice<T> {
    /// Actual private holder request, separate from the creator's collection
    /// payload. An empty collection constructs neither payload nor holder.
    pub fn storage_allocation_layout() -> Result<Layout, LayoutResourceError> {
        arc_layout(Layout::new::<FactSliceBacking<T>>())
    }
    pub fn from_pure_boxed(items: Box<[T]>) -> Self {
        Self::from_boxed_in(items, ValueTypeStorageLifetime::pure())
    }
    /// The actual author has already admitted its Box and private Arc requests.
    /// This consumes the source collection without copying or escaping it.
    pub fn from_boxed_in(items: Box<[T]>, storage: ValueTypeStorageLifetime) -> Self {
        if items.is_empty() {
            return Self(None);
        }
        Self(Some(Arc::new(FactSliceBacking { items, storage })))
    }
    /// Consume a collection already made by this creator, observing the sole
    /// actual holder allocation. A host refusal returns without later work.
    pub fn try_from_boxed_observed<E: From<LayoutResourceError>>(
        items: Box<[T]>,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(Layout) -> Result<(), E>,
    ) -> Result<Self, E> {
        if items.is_empty() {
            return Ok(Self(None));
        }
        request(Self::storage_allocation_layout()?)?;
        Ok(Self::from_boxed_in(items, storage))
    }
    pub fn as_slice(&self) -> &[T] {
        self.0.as_ref().map_or(&[], |backing| &backing.items)
    }
    pub fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.0
            .as_ref()
            .map_or_else(ValueTypeStorageLifetime::pure, |backing| {
                backing.storage.clone()
            })
    }
    /// Observe one actual shared-holder clone; no collection or member copy.
    pub fn clone_observed<E>(&self, mut work: impl FnMut() -> Result<(), E>) -> Result<Self, E> {
        work()?;
        Ok(self.clone())
    }
}
impl<T: TypedFact> Default for TypedFactSlice<T> {
    fn default() -> Self {
        Self(None)
    }
}
impl<T: TypedFact> Clone for TypedFactSlice<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<T: TypedFact> Deref for TypedFactSlice<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        self.as_slice()
    }
}
impl<T: TypedFact> AsRef<[T]> for TypedFactSlice<T> {
    fn as_ref(&self) -> &[T] {
        self.as_slice()
    }
}
impl<T: TypedFact> fmt::Debug for TypedFactSlice<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}
impl<T: TypedFact> PartialEq for TypedFactSlice<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl<T: TypedFact> Eq for TypedFactSlice<T> {}
impl<T: TypedFact> Hash for TypedFactSlice<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state)
    }
}
/// Explicit ordinary-reference adapter for pure authors. Runtime authors use
/// try_from_boxed_observed with their original creator instead.
impl<T: TypedFact> From<Box<[T]>> for TypedFactSlice<T> {
    fn from(items: Box<[T]>) -> Self {
        Self::from_pure_boxed(items)
    }
}
impl<T: TypedFact> From<Vec<T>> for TypedFactSlice<T> {
    fn from(items: Vec<T>) -> Self {
        Self::from_pure_boxed(items.into_boxed_slice())
    }
}
impl<T: TypedFact, const N: usize> From<[T; N]> for TypedFactSlice<T> {
    fn from(items: [T; N]) -> Self {
        Self::from_pure_boxed(Box::new(items))
    }
}
impl<'a, T: TypedFact> IntoIterator for &'a TypedFactSlice<T> {
    type Item = &'a T;
    type IntoIter = std::slice::Iter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.as_slice().iter()
    }
}
/// Owning traversal keeps the guarded backing; members are independently
/// cloned immutable facts, never a bare Box/Vec taken from this owner.
pub struct TypedFactIntoIter<T: TypedFact> {
    owner: TypedFactSlice<T>,
    index: usize,
}
impl<T: TypedFact> Iterator for TypedFactIntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        let value = self.owner.get(self.index)?.clone();
        self.index += 1;
        Some(value)
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.owner.len() - self.index;
        (left, Some(left))
    }
}
impl<T: TypedFact> ExactSizeIterator for TypedFactIntoIter<T> {}
impl<T: TypedFact> IntoIterator for TypedFactSlice<T> {
    type Item = T;
    type IntoIter = TypedFactIntoIter<T>;
    fn into_iter(self) -> Self::IntoIter {
        TypedFactIntoIter {
            owner: self,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Lifetime;
    impl crate::ValueTypeStorageHold for Lifetime {}
    #[test]
    fn independent_collection_and_owning_iterator_keep_only_the_actual_backing_creator() {
        let owner = Arc::new(Lifetime);
        let weak = Arc::downgrade(&owner);
        let items: Box<[FunctionValueType]> =
            vec![FunctionValueType::new(arrow_schema::DataType::Int64, false)].into_boxed_slice();
        let ptr = items.as_ptr();
        let mut requests = 0;
        let values = FunctionParameterTypes::try_from_boxed_observed::<LayoutResourceError>(
            items,
            ValueTypeStorageLifetime::held(owner),
            |layout| {
                requests += 1;
                assert_eq!(
                    layout,
                    FunctionParameterTypes::storage_allocation_layout().unwrap()
                );
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(requests, 1);
        assert_eq!(values.as_slice().as_ptr(), ptr);
        let copy = values.clone();
        assert_eq!(copy.as_slice().as_ptr(), ptr);
        drop(values);
        assert!(weak.upgrade().is_some());
        let mut iterator = copy.into_iter();
        assert!(weak.upgrade().is_some());
        let member = iterator.next().unwrap();
        assert_eq!(member.data_type(), &arrow_schema::DataType::Int64);
        assert_eq!(iterator.len(), 0);
        assert!(weak.upgrade().is_some());
        drop(iterator);
        assert!(weak.upgrade().is_none());
        assert_eq!(member.data_type(), &arrow_schema::DataType::Int64);
    }
    #[test]
    fn empty_collection_has_no_fabricated_holder_request_or_lifetime() {
        let owner = Arc::new(Lifetime);
        let weak = Arc::downgrade(&owner);
        let empty = FunctionParameterTypes::try_from_boxed_observed::<LayoutResourceError>(
            Box::default(),
            ValueTypeStorageLifetime::held(owner),
            |_| panic!("no allocation is constructed"),
        )
        .unwrap();
        assert!(empty.is_empty());
        assert!(weak.upgrade().is_none());
    }
}
