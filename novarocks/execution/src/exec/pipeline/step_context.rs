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

//! Short-lived driver capabilities. Operator-private construction, profiles and
//! containers are installed on the operator, never indexed through this view.

use crate::runtime::{fragment::ExecutionResult, runtime_state::RuntimeErrorState};
use novarocks_functions::KernelFailure;
use std::num::NonZeroU64;

/// One driver turn's shared work allowance. Nested calls cannot refill it.
pub struct TurnWork {
    generation: NonZeroU64,
    remaining: u32,
}
impl TurnWork {
    pub(crate) fn new(generation: NonZeroU64, units: u32) -> Self {
        Self {
            generation,
            remaining: units,
        }
    }
    pub fn generation(&self) -> NonZeroU64 {
        self.generation
    }
    pub fn remaining(&self) -> u32 {
        self.remaining
    }
    /// A refusal consumes no work. The caller yields, retaining its cursor.
    pub fn try_consume(&mut self, units: u32) -> bool {
        match self.remaining.checked_sub(units) {
            Some(left) => {
                self.remaining = left;
                true
            }
            None => false,
        }
    }
}

pub struct StepContext<'a> {
    control: &'a RuntimeErrorState,
    work: &'a mut TurnWork,
}
impl<'a> StepContext<'a> {
    pub(crate) fn new(control: &'a RuntimeErrorState, work: &'a mut TurnWork) -> Self {
        Self { control, work }
    }
    pub fn work(&mut self) -> &mut TurnWork {
        self.work
    }
    /// Observing another owner's stop does not invent a user cancellation.
    pub fn checkpoint(&self) -> ExecutionResult<()> {
        if self.control.is_stopped() {
            Err(KernelFailure::Cancelled.into())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn repeated_and_nested_calls_share_one_turn_allowance() {
        let mut work = TurnWork::new(NonZeroU64::new(9).unwrap(), 3);
        assert!(work.try_consume(2));
        assert!(!work.try_consume(2));
        assert_eq!(work.remaining(), 1);
        assert!(work.try_consume(1));
        assert!(!work.try_consume(1));
        assert_eq!(work.generation().get(), 9);
    }
}
