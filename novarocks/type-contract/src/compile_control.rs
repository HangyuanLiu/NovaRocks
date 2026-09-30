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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arrow_schema::{DataType, Field};
use std::fmt;

/// Cooperative control of pure preparation. A caller owns clocks, cancellation
/// and work budgets; the compiler has no scheduler, I/O or runtime capability.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CompilePhase {
    CarrierPreflight,
    Decode,
    Validate,
    ProviderValidation,
    FunctionSpecialization,
    LowerProgram,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompileControlError {
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
}
impl fmt::Display for CompileControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Cancelled => "pure compilation was cancelled",
            Self::DeadlineExceeded => "pure compilation deadline was exceeded",
            Self::ResourceExhausted => "pure compilation work budget was exhausted",
        })
    }
}
impl std::error::Error for CompileControlError {}

pub trait PureCompileControl: Send + Sync {
    /// `work_units` accounts completed bounded operations since the previous
    /// checkpoint. Zero checks control before any work begins. Resource/control
    /// failures abort compilation and never enter the selected-row error carrier.
    fn checkpoint(&self, phase: CompilePhase, work_units: u32) -> Result<(), CompileControlError>;
}

pub const MAX_UNOBSERVED_COMPILE_WORK: u32 = 256;

pub struct CompileCheckpoints<'a> {
    owner: &'a dyn PureCompileControl,
    phase: CompilePhase,
    pending: u32,
    failed: Option<CompileControlError>,
}
impl<'a> CompileCheckpoints<'a> {
    pub fn try_new(
        owner: &'a dyn PureCompileControl,
        phase: CompilePhase,
    ) -> Result<Self, CompileControlError> {
        owner.checkpoint(phase, 0)?;
        Ok(Self {
            owner,
            phase,
            pending: 0,
            failed: None,
        })
    }
    /// Call after one bounded operation; expensive library operations require
    /// their own preflight and checkpoints within their expansion loop.
    pub fn step(&mut self) -> Result<(), CompileControlError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        self.pending += 1;
        if self.pending == MAX_UNOBSERVED_COMPILE_WORK {
            match self.owner.checkpoint(self.phase, self.pending) {
                Ok(()) => self.pending = 0,
                Err(error) => {
                    self.failed = Some(error);
                    return Err(error);
                }
            }
        }
        Ok(())
    }
    pub fn finish(self) -> Result<(), CompileControlError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        self.owner.checkpoint(self.phase, self.pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    struct Owner {
        units: Mutex<Vec<u32>>,
        fail: Option<CompileControlError>,
    }
    impl PureCompileControl for Owner {
        fn checkpoint(&self, _: CompilePhase, work_units: u32) -> Result<(), CompileControlError> {
            self.units.lock().unwrap().push(work_units);
            if work_units > 0
                && let Some(error) = self.fail
            {
                return Err(error);
            }
            Ok(())
        }
    }
    #[test]
    fn bounded_observation_accounts_all_work() {
        let owner = Owner {
            units: Mutex::default(),
            fail: None,
        };
        let mut scope = CompileCheckpoints::try_new(&owner, CompilePhase::Validate).unwrap();
        for _ in 0..2000 {
            scope.step().unwrap();
        }
        scope.finish().unwrap();
        let units = owner.units.lock().unwrap();
        assert_eq!(units.iter().sum::<u32>(), 2000);
        assert!(
            units
                .iter()
                .all(|units| *units <= MAX_UNOBSERVED_COMPILE_WORK)
        );
    }
    #[test]
    fn control_failures_latch_without_reaccounting_work() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let owner = Owner {
                units: Mutex::default(),
                fail: Some(error),
            };
            let mut scope =
                CompileCheckpoints::try_new(&owner, CompilePhase::LowerProgram).unwrap();
            for _ in 1..MAX_UNOBSERVED_COMPILE_WORK {
                scope.step().unwrap();
            }
            assert_eq!(scope.step(), Err(error));
            assert_eq!(scope.step(), Err(error));
            assert_eq!(scope.finish(), Err(error));
            assert_eq!(
                *owner.units.lock().unwrap(),
                vec![0, MAX_UNOBSERVED_COMPILE_WORK]
            );
        }
    }
}
