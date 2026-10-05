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

//! One sparse receiving index for existing binding namespaces. The caller
//! admits the actual usize Layout, source/coexistence and O(N log N) work
//! before entering this allocation owner. IDs never size an allocation.

use crate::{allocation_exit_v2::reserve_exit, physical_binding_v2::BindingCodecError};
use novarocks_type_contract::{CompileCheckpoints, CompileControlError};
use std::alloc::Layout;

/// The actual count-sized heap-sort owner below admits its construction once.
/// Each sift level performs bounded identity reads, comparisons and movement;
/// the conservative multiplier includes fill, both heap passes and duplicates.
pub(crate) fn prepare_work_upper_bound(count: usize) -> Result<usize, BindingCodecError> {
    let height = (usize::BITS - count.leading_zeros()) as usize + 1;
    count
        .checked_mul(height)
        .and_then(|n| n.checked_mul(32))
        .and_then(|n| n.checked_add(64))
        .ok_or_else(|| CompileControlError::ResourceExhausted.into())
}

pub(crate) fn lookup_work_upper_bound(count: usize) -> usize {
    (usize::BITS - count.leading_zeros()) as usize + 1
}

pub(crate) struct BindingIndex {
    indices: Vec<usize>,
}
impl BindingIndex {
    pub(crate) fn prepare(
        count: usize,
        id: impl Fn(usize) -> u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, BindingCodecError> {
        Layout::array::<usize>(count)
            .map_err(|_| invalid("binding index layout is unrepresentable"))?;
        work.flush()?;
        let mut indices = Vec::new();
        let reserved = indices.try_reserve_exact(count);
        reserve_exit::<BindingCodecError>(reserved, work)?;
        for index in 0..count {
            indices.push(index);
            work.step()?;
        }
        for root in (0..indices.len() / 2).rev() {
            sift(&mut indices, root, &id, work)?;
        }
        for end in (1..indices.len()).rev() {
            indices.swap(0, end);
            work.step()?;
            sift(&mut indices[..end], 0, &id, work)?;
        }
        for pair in indices.windows(2) {
            let duplicate = id(pair[0]) == id(pair[1]);
            work.step()?;
            if duplicate {
                return Err(invalid("binding definition ID is duplicated"));
            }
        }
        Ok(Self { indices })
    }
    pub(crate) fn find(
        &self,
        sought: u32,
        id: impl Fn(usize) -> u32,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<usize>, BindingCodecError> {
        let mut lower = 0;
        let mut upper = self.indices.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let index = self.indices[middle];
            let order = id(index).cmp(&sought);
            work.step()?;
            match order {
                std::cmp::Ordering::Less => lower = middle + 1,
                std::cmp::Ordering::Greater => upper = middle,
                std::cmp::Ordering::Equal => return Ok(Some(index)),
            }
        }
        Ok(None)
    }
    pub(crate) fn backing_bytes(&self) -> Result<usize, BindingCodecError> {
        Layout::array::<usize>(self.indices.capacity())
            .map(|layout| layout.size())
            .map_err(|_| invalid("binding index backing layout is unrepresentable"))
    }
}
fn invalid(message: &'static str) -> BindingCodecError {
    BindingCodecError::InvalidShape(message)
}
fn sift(
    indices: &mut [usize],
    mut root: usize,
    id: &impl Fn(usize) -> u32,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), BindingCodecError> {
    loop {
        let left = root
            .checked_mul(2)
            .and_then(|v| v.checked_add(1))
            .ok_or_else(|| invalid("binding index arithmetic overflow"))?;
        work.step()?;
        if left >= indices.len() {
            return Ok(());
        }
        let right = left
            .checked_add(1)
            .ok_or_else(|| invalid("binding index arithmetic overflow"))?;
        let mut child = left;
        if right < indices.len() {
            let greater = id(indices[right]) > id(indices[left]);
            work.step()?;
            if greater {
                child = right;
            }
        }
        let greater = id(indices[child]) > id(indices[root]);
        work.step()?;
        if !greater {
            return Ok(());
        }
        indices.swap(root, child);
        root = child;
        work.step()?;
    }
}

#[cfg(test)]
mod work_tests {
    use super::*;
    use novarocks_type_contract::{CompilePhase, PureCompileControl};
    use std::sync::Mutex;
    #[derive(Default)]
    struct Control(Mutex<Vec<u32>>);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            self.0.lock().unwrap().push(units);
            Ok(())
        }
    }
    #[test]
    fn numerical_bounds_cover_actual_sparse_heap_sort_and_binary_lookup() {
        for count in [0, 1, 2, 3, 15, 64, 320] {
            let ids: Vec<u32> = (0..count)
                .map(|at| {
                    if at + 1 == count {
                        0
                    } else {
                        u32::MAX - at as u32
                    }
                })
                .collect();
            let control = Control::default();
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            let index = BindingIndex::prepare(count, |at| ids[at], &mut work).unwrap();
            work.finish().unwrap();
            let units = control
                .0
                .lock()
                .unwrap()
                .iter()
                .map(|x| *x as usize)
                .sum::<usize>();
            assert!(units <= prepare_work_upper_bound(count).unwrap());
            for sought in [0, u32::MAX, 1] {
                let lookup = Control::default();
                let mut work = CompileCheckpoints::try_new(&lookup, CompilePhase::Decode).unwrap();
                let actual = index.find(sought, |at| ids[at], &mut work).unwrap();
                work.finish().unwrap();
                assert_eq!(actual, ids.iter().position(|id| *id == sought));
                let units = lookup
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|x| *x as usize)
                    .sum::<usize>();
                assert!(units <= lookup_work_upper_bound(count));
            }
        }
    }
    #[test]
    fn unrepresentable_index_work_is_typed_resource_without_observer_calls() {
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        work.step().unwrap();
        assert!(matches!(
            prepare_work_upper_bound(usize::MAX),
            Err(BindingCodecError::Control(
                CompileControlError::ResourceExhausted
            ))
        ));
        assert_eq!(*control.0.lock().unwrap(), [0]);
    }
}
