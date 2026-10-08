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

//! First-non-NULL state and its original recursive scalar storage.
use crate::aggregate_scalar::{
    self as scalar, AggScalarValue, ScalarStateAllocator, ScalarStateError, ScalarWork,
    TrackedAggScalarValue,
};
use arrow_array::ArrayRef;

#[derive(Debug)]
pub struct AnyValueState<A: ScalarStateAllocator> {
    pub allocator: A,
    pub has_value: bool,
    pub value: Option<TrackedAggScalarValue<A>>,
    retained: usize,
    pub failed: bool,
}
impl<A: ScalarStateAllocator> AnyValueState<A> {
    pub fn new(allocator: A) -> Self {
        Self {
            allocator,
            has_value: false,
            value: None,
            retained: 0,
            failed: false,
        }
    }
    /// A selected lifecycle refusal invalidates this instance and releases
    /// values published during the refused operation. The legacy shell does
    /// not call this hook and preserves its original retry semantics.
    pub fn latch_failure(&mut self) {
        self.failed = true;
        self.value = None;
        self.has_value = false;
        self.retained = 0;
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained
    }
    pub fn update_from_array(
        &mut self,
        array: &ArrayRef,
        row: usize,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<(), ScalarStateError> {
        if self.has_value {
            return Ok(());
        }
        let value = scalar::tracked_scalar_from_array(array, row, &self.allocator, work)?;
        if let Some(value) = value {
            let retained = scalar_heap_capacity(&value, work)?;
            self.has_value = true;
            self.value = Some(value);
            self.retained = retained;
        }
        Ok(())
    }
    pub fn output(
        &self,
        work: &mut ScalarWork<'_, '_>,
    ) -> Result<Option<AggScalarValue>, ScalarStateError> {
        if self.has_value {
            self.value
                .as_ref()
                .map(|value| scalar::tracked_scalar_to_output(value, work))
                .transpose()
        } else {
            Ok(None)
        }
    }
}
fn scalar_heap_capacity<A: ScalarStateAllocator>(
    value: &TrackedAggScalarValue<A>,
    work: &mut ScalarWork<'_, '_>,
) -> Result<usize, ScalarStateError> {
    work.step()?;
    let mut bytes = match value {
        TrackedAggScalarValue::Utf8(values) | TrackedAggScalarValue::Binary(values) => {
            values.capacity()
        }
        TrackedAggScalarValue::Struct(values) | TrackedAggScalarValue::List(values) => values
            .capacity()
            .checked_mul(std::mem::size_of::<Option<TrackedAggScalarValue<A>>>())
            .ok_or(crate::KernelFailure::ResourceExhausted)?,
        TrackedAggScalarValue::Map(values) => values
            .capacity()
            .checked_mul(std::mem::size_of::<(
                Option<TrackedAggScalarValue<A>>,
                Option<TrackedAggScalarValue<A>>,
            )>())
            .ok_or(crate::KernelFailure::ResourceExhausted)?,
        _ => 0,
    };
    match value {
        TrackedAggScalarValue::Struct(values) | TrackedAggScalarValue::List(values) => {
            for value in values {
                work.step()?;
                if let Some(value) = value {
                    bytes = bytes
                        .checked_add(scalar_heap_capacity(value, work)?)
                        .ok_or(crate::KernelFailure::ResourceExhausted)?;
                }
            }
        }
        TrackedAggScalarValue::Map(values) => {
            for (key, value) in values {
                work.step()?;
                for value in [key, value].into_iter().flatten() {
                    bytes = bytes
                        .checked_add(scalar_heap_capacity(value, work)?)
                        .ok_or(crate::KernelFailure::ResourceExhausted)?;
                }
            }
        }
        _ => {}
    }
    Ok(bytes)
}
