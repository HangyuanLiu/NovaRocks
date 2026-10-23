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

//! Borrowed logical row domains. Visits preserve occurrences; sets do not.
use crate::{
    ValueError, ValueFailure, ValueViolation,
    control::{Growth, Work},
};
use arrow_array::{Array, Int32Array};
use arrow_buffer::NullBuffer;
use std::{alloc::Layout, ops::Range};

fn bounds() -> ValueError {
    ValueError::Invariant(ValueViolation::Bounds)
}

/// A lazy composition of parent validity and child coordinates. Building or
/// visiting this domain allocates neither a row object nor a tree bitmap.
#[derive(Clone, Copy)]
pub enum ReachableRows<'a> {
    All {
        len: usize,
    },
    Range {
        len: usize,
        start: usize,
        end: usize,
    },
    Indices {
        len: usize,
        rows: &'a [usize],
    },
    Mask {
        parent: &'a ReachableRows<'a>,
        validity: &'a NullBuffer,
    },
    List32 {
        parent: &'a ReachableRows<'a>,
        offsets: &'a [i32],
        validity: Option<&'a NullBuffer>,
        child_len: usize,
    },
    List64 {
        parent: &'a ReachableRows<'a>,
        offsets: &'a [i64],
        validity: Option<&'a NullBuffer>,
        child_len: usize,
    },
    FixedSizeList {
        parent: &'a ReachableRows<'a>,
        width: usize,
        child_start: usize,
        validity: Option<&'a NullBuffer>,
        child_len: usize,
    },
    Dictionary {
        parent: &'a ReachableRows<'a>,
        keys: &'a Int32Array,
        values_len: usize,
    },
}
impl<'a> ReachableRows<'a> {
    pub const fn all(len: usize) -> Self {
        Self::All { len }
    }
    pub fn range(len: usize, rows: Range<usize>) -> Result<Self, ValueError> {
        if rows.start > rows.end || rows.end > len {
            return Err(bounds());
        }
        Ok(Self::Range {
            len,
            start: rows.start,
            end: rows.end,
        })
    }
    pub const fn indices(len: usize, rows: &'a [usize]) -> Self {
        Self::Indices { len, rows }
    }
    pub const fn len(&self) -> usize {
        match self {
            Self::All { len } | Self::Range { len, .. } | Self::Indices { len, .. } => *len,
            Self::Mask { parent, .. } => parent.len(),
            Self::List32 { child_len, .. }
            | Self::List64 { child_len, .. }
            | Self::FixedSizeList { child_len, .. } => *child_len,
            Self::Dictionary { values_len, .. } => *values_len,
        }
    }
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn masked(&'a self, validity: &'a NullBuffer) -> Self {
        Self::Mask {
            parent: self,
            validity,
        }
    }

    /// Logical row occurrences, in parent order, including repeated references.
    /// A NULL dictionary key is skipped before its physical integer is read.
    pub fn for_each<E>(
        &self,
        visit: &mut impl FnMut(usize) -> Result<(), E>,
    ) -> Result<(), ValueFailure<E>> {
        self.visit(0, &mut |row| visit(row).map_err(ValueFailure::Caller))
    }
    fn visit<E>(
        &self,
        depth: usize,
        visit: &mut dyn FnMut(usize) -> Result<(), ValueFailure<E>>,
    ) -> Result<(), ValueFailure<E>> {
        if depth > novarocks_type_contract::MAX_VALUE_TYPE_DEPTH {
            return Err(ValueError::Invariant(ValueViolation::Depth).into());
        }
        match self {
            Self::All { len } => {
                for row in 0..*len {
                    visit(row)?;
                }
            }
            Self::Range { len, start, end } => {
                if start > end || end > len {
                    return Err(bounds().into());
                }
                for row in *start..*end {
                    visit(row)?;
                }
            }
            Self::Indices { len, rows } => {
                for &row in *rows {
                    if row >= *len {
                        return Err(bounds().into());
                    }
                    visit(row)?;
                }
            }
            Self::Mask { parent, validity } => {
                if validity.len() != parent.len() {
                    return Err(bounds().into());
                }
                parent.visit(depth + 1, &mut |row| {
                    if validity.is_valid(row) {
                        visit(row)
                    } else {
                        Ok(())
                    }
                })?;
            }
            Self::List32 {
                parent,
                offsets,
                validity,
                child_len,
            } => {
                self.list(
                    parent,
                    offsets.len(),
                    *validity,
                    *child_len,
                    depth,
                    |i| usize::try_from(offsets[i]).map_err(|_| bounds()),
                    visit,
                )?;
            }
            Self::List64 {
                parent,
                offsets,
                validity,
                child_len,
            } => {
                self.list(
                    parent,
                    offsets.len(),
                    *validity,
                    *child_len,
                    depth,
                    |i| usize::try_from(offsets[i]).map_err(|_| bounds()),
                    visit,
                )?;
            }
            Self::FixedSizeList {
                parent,
                width,
                child_start,
                validity,
                child_len,
            } => {
                if validity.is_some_and(|v| v.len() != parent.len()) {
                    return Err(bounds().into());
                }
                parent.visit(depth + 1, &mut |row| {
                    if validity.is_some_and(|v| v.is_null(row)) {
                        return Ok(());
                    }
                    let start = row
                        .checked_mul(*width)
                        .and_then(|v| v.checked_add(*child_start))
                        .ok_or_else(|| ValueFailure::Value(bounds()))?;
                    let end = start
                        .checked_add(*width)
                        .ok_or_else(|| ValueFailure::Value(bounds()))?;
                    if end > *child_len {
                        return Err(ValueFailure::Value(bounds()));
                    }
                    for child in start..end {
                        visit(child)?;
                    }
                    Ok(())
                })?;
            }
            Self::Dictionary {
                parent,
                keys,
                values_len,
            } => {
                if parent.len() != keys.len() {
                    return Err(bounds().into());
                }
                parent.visit(depth + 1, &mut |row| {
                    if keys.is_null(row) {
                        return Ok(());
                    }
                    let key = usize::try_from(keys.value(row))
                        .map_err(|_| ValueFailure::Value(bounds()))?;
                    if key >= *values_len {
                        return Err(ValueFailure::Value(bounds()));
                    }
                    visit(key)
                })?;
            }
        }
        Ok(())
    }
    fn list<E>(
        &self,
        parent: &ReachableRows<'_>,
        offsets_len: usize,
        validity: Option<&NullBuffer>,
        child_len: usize,
        depth: usize,
        offset: impl Fn(usize) -> Result<usize, ValueError>,
        visit: &mut dyn FnMut(usize) -> Result<(), ValueFailure<E>>,
    ) -> Result<(), ValueFailure<E>> {
        if parent.len().checked_add(1) != Some(offsets_len)
            || validity.is_some_and(|v| v.len() != parent.len())
        {
            return Err(bounds().into());
        }
        parent.visit(depth + 1, &mut |row| {
            if validity.is_some_and(|v| v.is_null(row)) {
                return Ok(());
            }
            let start = offset(row).map_err(ValueFailure::Value)?;
            let end = offset(row + 1).map_err(ValueFailure::Value)?;
            if start > end || end > child_len {
                return Err(ValueFailure::Value(bounds()));
            }
            for child in start..end {
                visit(child)?;
            }
            Ok(())
        })
    }
    pub fn materialize<E>(
        &self,
        growth: &mut Growth<'_, E>,
    ) -> Result<SelectionSet, ValueFailure<E>> {
        self.materialize_observed(growth, &mut |_| Ok(()))
    }
    pub fn materialize_observed<E>(
        &self,
        growth: &mut Growth<'_, E>,
        work: &mut Work<'_, E>,
    ) -> Result<SelectionSet, ValueFailure<E>> {
        let mut set = SelectionSet::try_new_observed(self.len(), growth)?;
        self.visit(0, &mut |row| {
            work(1).map_err(ValueFailure::Caller)?;
            set.insert(row).map_err(ValueFailure::Value)
        })?;
        Ok(set)
    }
}
/// A membership set in an explicitly sized coordinate domain. It does not
/// encode order or repeated occurrences, and NULL is independent of membership.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectionSet {
    len: usize,
    words: Vec<u64>,
}
impl SelectionSet {
    pub fn try_new(len: usize) -> Result<Self, ValueError> {
        Self::try_new_observed(len, &mut |_| Ok::<_, std::convert::Infallible>(()))
            .map_err(ValueFailure::into_error)
    }
    pub fn try_new_observed<E>(
        len: usize,
        growth: &mut Growth<'_, E>,
    ) -> Result<Self, ValueFailure<E>> {
        let count = len / 64 + usize::from(len % 64 != 0);
        let layout = Layout::array::<u64>(count)
            .map_err(|_| ValueError::Invariant(ValueViolation::Bounds))?;
        if count != 0 {
            growth(layout).map_err(ValueFailure::Caller)?;
        }
        let mut words = Vec::new();
        words
            .try_reserve_exact(count)
            .map_err(|source| ValueError::Allocation {
                requested: layout.size(),
                source,
            })?;
        words.resize(count, 0);
        Ok(Self { len, words })
    }
    pub const fn len(&self) -> usize {
        self.len
    }
    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|v| *v == 0)
    }
    pub fn contains(&self, row: usize) -> bool {
        row < self.len && self.words[row / 64] & (1 << (row % 64)) != 0
    }
    pub fn insert(&mut self, row: usize) -> Result<(), ValueError> {
        if row >= self.len {
            return Err(bounds());
        }
        self.words[row / 64] |= 1 << (row % 64);
        Ok(())
    }
    pub fn count(&self) -> usize {
        self.words.iter().map(|v| v.count_ones() as usize).sum()
    }
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.words
            .iter()
            .enumerate()
            .flat_map(|(word_index, &word)| {
                let mut pending = word;
                std::iter::from_fn(move || {
                    if pending == 0 {
                        return None;
                    }
                    let bit = pending.trailing_zeros() as usize;
                    pending &= pending - 1;
                    Some(word_index * 64 + bit)
                })
            })
    }
    pub fn descriptor_bytes(&self) -> usize {
        self.words.capacity() * std::mem::size_of::<u64>()
    }
    pub fn union_with(&mut self, other: &Self) -> Result<(), ValueError> {
        if self.len != other.len {
            return Err(bounds());
        }
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a |= *b;
        }
        Ok(())
    }
    pub fn try_clone_observed<E>(
        &self,
        growth: &mut Growth<'_, E>,
    ) -> Result<Self, ValueFailure<E>> {
        let mut out = Self::try_new_observed(self.len, growth)?;
        out.words.copy_from_slice(&self.words);
        Ok(out)
    }
    pub fn is_subset(&self, other: &Self) -> bool {
        self.len == other.len
            && self
                .words
                .iter()
                .zip(&other.words)
                .all(|(a, b)| a & !b == 0)
    }
    pub fn difference_observed<E>(
        &self,
        other: &Self,
        growth: &mut Growth<'_, E>,
    ) -> Result<Self, ValueFailure<E>> {
        if self.len != other.len {
            return Err(bounds().into());
        }
        let mut out = Self::try_new_observed(self.len, growth)?;
        for ((o, a), b) in out.words.iter_mut().zip(&self.words).zip(&other.words) {
            *o = a & !b;
        }
        Ok(out)
    }
    pub fn difference(&self, other: &Self) -> Result<Self, ValueError> {
        self.binary(other, |a, b| a & !b)
    }
    pub fn intersection(&self, other: &Self) -> Result<Self, ValueError> {
        self.binary(other, |a, b| a & b)
    }
    fn binary(&self, other: &Self, op: impl Fn(u64, u64) -> u64) -> Result<Self, ValueError> {
        if self.len != other.len {
            return Err(bounds());
        }
        let mut out = Self::try_new(self.len)?;
        for ((out, a), b) in out.words.iter_mut().zip(&self.words).zip(&other.words) {
            *out = op(*a, *b);
        }
        Ok(out)
    }
    pub fn intervals(&self) -> Vec<Range<usize>> {
        let mut out: Vec<Range<usize>> = Vec::new();
        for row in self.iter() {
            if let Some(last) = out.last_mut() {
                if last.end == row {
                    last.end = row + 1;
                    continue;
                }
            }
            out.push(row..row + 1);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn borrowed_nested_rows_preserve_occurrences_and_parent_nulls() {
        let parent = ReachableRows::all(3);
        let validity = NullBuffer::from(vec![true, false, true]);
        let list = ReachableRows::List32 {
            parent: &parent,
            offsets: &[1, 3, 5, 7],
            validity: Some(&validity),
            child_len: 8,
        };
        let mut occurrences = vec![];
        list.for_each(&mut |r| {
            occurrences.push(r);
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
        assert_eq!(occurrences, vec![1, 2, 5, 6]);
        let keys = Int32Array::from(vec![
            Some(1),
            Some(0),
            None,
            Some(1),
            Some(0),
            Some(2),
            Some(2),
            Some(0),
        ]);
        let dict = ReachableRows::Dictionary {
            parent: &list,
            keys: &keys,
            values_len: 3,
        };
        occurrences.clear();
        dict.for_each(&mut |r| {
            occurrences.push(r);
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
        assert_eq!(occurrences, vec![0, 2, 2]);
        let set = dict
            .materialize(&mut |_| Ok::<_, std::convert::Infallible>(()))
            .unwrap();
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![0, 2]);
        assert_eq!(set.intervals(), vec![0..1, 2..3]);
    }
    #[test]
    fn fixed_size_and_validity_slice_use_their_actual_coordinates() {
        let parent = ReachableRows::all(2);
        let validity = NullBuffer::from(vec![false, true, false, true]).slice(1, 2);
        let fixed = ReachableRows::FixedSizeList {
            parent: &parent,
            width: 2,
            child_start: 3,
            validity: Some(&validity),
            child_len: 7,
        };
        let set = fixed
            .materialize(&mut |_| Ok::<_, std::convert::Infallible>(()))
            .unwrap();
        assert_eq!(set.iter().collect::<Vec<_>>(), vec![3, 4]);
        let all = ReachableRows::indices(7, &[4, 3, 4]);
        let mut rows = vec![];
        all.for_each(&mut |r| {
            rows.push(r);
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
        assert_eq!(rows, vec![4, 3, 4]);
    }
    #[test]
    fn coverage_intersection_and_difference_keep_the_original_domain() {
        let mut a = SelectionSet::try_new(3).unwrap();
        a.insert(0).unwrap();
        let mut b = SelectionSet::try_new(3).unwrap();
        b.insert(1).unwrap();
        assert!(a.intersection(&b).unwrap().is_empty());
        assert_eq!(b.difference(&a).unwrap(), b);
        a.union_with(&b).unwrap();
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![0, 1]);
    }
}
