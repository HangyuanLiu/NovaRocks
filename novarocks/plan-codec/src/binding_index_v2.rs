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
use novarocks_type_contract::CompileCheckpoints;
use std::alloc::Layout;

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
