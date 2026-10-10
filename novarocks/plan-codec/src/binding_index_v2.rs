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
        self.find_captured(sought, id, &mut |_, _| Ok(()), work)
    }
    /// Borrow the actual matched position before observing its completed
    /// comparison. The consuming namespace admits requests made possible by
    /// this captured source; this index neither clones nor bills that source.
    pub(crate) fn find_captured<E>(
        &self,
        sought: u32,
        id: impl Fn(usize) -> u32,
        capture: &mut impl FnMut(usize, &mut CompileCheckpoints<'_>) -> Result<(), E>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Option<usize>, E>
    where
        E: From<BindingCodecError> + From<CompileControlError>,
    {
        let mut lower = 0;
        let mut upper = self.indices.len();
        while lower < upper {
            let middle = lower + (upper - lower) / 2;
            let index = self.indices[middle];
            let order = id(index).cmp(&sought);
            if order == std::cmp::Ordering::Equal {
                capture(index, work)?;
            }
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

    #[test]
    fn captured_search_preserves_original_sparse_positions_and_control_trace() {
        let ids = [u32::MAX, 0, 7, 42];
        let preparation = Control::default();
        let mut work = CompileCheckpoints::try_new(&preparation, CompilePhase::Decode).unwrap();
        let index = BindingIndex::prepare(ids.len(), |at| ids[at], &mut work).unwrap();
        work.finish().unwrap();
        for sought in [0, 7, 42, u32::MAX, 1, u32::MAX - 1] {
            let plain = Control::default();
            let mut work = CompileCheckpoints::try_new(&plain, CompilePhase::Decode).unwrap();
            let expected = index.find(sought, |at| ids[at], &mut work).unwrap();
            work.finish().unwrap();
            let captured = Control::default();
            let mut work = CompileCheckpoints::try_new(&captured, CompilePhase::Decode).unwrap();
            let mut positions = Vec::new();
            let actual = index
                .find_captured::<BindingCodecError>(
                    sought,
                    |at| ids[at],
                    &mut |at, _| {
                        positions.push(at);
                        Ok(())
                    },
                    &mut work,
                )
                .unwrap();
            work.finish().unwrap();
            assert_eq!(actual, ids.iter().position(|id| *id == sought));
            assert_eq!(actual, expected);
            assert_eq!(positions, actual.into_iter().collect::<Vec<_>>());
            assert_eq!(*captured.0.lock().unwrap(), *plain.0.lock().unwrap());
        }
    }

    #[test]
    fn matched_source_admission_precedes_its_pending_completed_comparison() {
        struct LateControl {
            trace: Mutex<Vec<u32>>,
            cause: CompileControlError,
        }
        impl PureCompileControl for LateControl {
            fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
                self.trace.lock().unwrap().push(units);
                if units == 256 {
                    Err(self.cause)
                } else {
                    Ok(())
                }
            }
        }
        let preparation = Control::default();
        let mut work = CompileCheckpoints::try_new(&preparation, CompilePhase::Decode).unwrap();
        let index = BindingIndex::prepare(1, |_| u32::MAX, &mut work).unwrap();
        work.finish().unwrap();
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = LateControl {
                trace: Mutex::new(Vec::new()),
                cause,
            };
            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            // Load the caller's pending meter seam; this is not a claim about
            // cooperative work inside a library lookup or allocation.
            for _ in 0..255 {
                work.step().unwrap();
            }
            let mut captured = 0;
            let outcome = index.find_captured::<BindingCodecError>(
                u32::MAX,
                |_| u32::MAX,
                &mut |at, _| {
                    assert_eq!(at, 0);
                    captured += 1;
                    Err(CompileControlError::ResourceExhausted.into())
                },
                &mut work,
            );
            assert!(matches!(
                outcome,
                Err(BindingCodecError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(captured, 1);
            assert_eq!(*control.trace.lock().unwrap(), [0]);

            let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
            for _ in 0..255 {
                work.step().unwrap();
            }
            let missing = index.find_captured::<BindingCodecError>(
                0,
                |_| u32::MAX,
                &mut |_, _| panic!("missing source must not be captured"),
                &mut work,
            );
            assert!(matches!(missing, Err(BindingCodecError::Control(actual)) if actual == cause));
            assert_eq!(*control.trace.lock().unwrap(), [0, 0, 256]);
        }
    }
}
