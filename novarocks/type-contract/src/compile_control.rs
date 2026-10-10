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

use std::fmt;

/// Cooperative control of pure preparation. A caller owns clocks, cancellation
/// and work budgets; the compiler has no scheduler, I/O or runtime capability.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CompilePhase {
    CarrierPreflight,
    Encode,
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
    /// Observe the completed tail before handing work to an exact owner. The
    /// next operation keeps this same control and budget; flushing is not a
    /// new admission or a resource reset.
    pub fn flush(&mut self) -> Result<(), CompileControlError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        match self.owner.checkpoint(self.phase, self.pending) {
            Ok(()) => {
                self.pending = 0;
                Ok(())
            }
            Err(error) => {
                self.failed = Some(error);
                Err(error)
            }
        }
    }
    /// Borrow the original caller control for work delegated to another owner.
    /// The caller flushes its pending work before entering that owner.
    pub fn control(&self) -> &'a dyn PureCompileControl {
        self.owner
    }
    pub fn finish(mut self) -> Result<(), CompileControlError> {
        self.flush()
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

    struct PhaseOwner {
        calls: Mutex<Vec<(CompilePhase, u32)>>,
        fail_at: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for PhaseOwner {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            let mut calls = self.calls.lock().unwrap();
            calls.push((phase, units));
            if let Some((index, error)) = self.fail_at
                && calls.len() == index
            {
                return Err(error);
            }
            Ok(())
        }
    }

    #[test]
    fn flush_observes_and_resets_tails_without_replacing_the_borrowed_control() {
        let owner = PhaseOwner {
            calls: Mutex::default(),
            fail_at: None,
        };
        let phase = CompilePhase::FunctionSpecialization;
        let mut checksum = 0u64;
        let original_control = {
            let mut scope = CompileCheckpoints::try_new(&owner, phase).unwrap();
            for value in 0u64..7 {
                checksum += value;
                scope.step().unwrap();
            }
            scope.flush().unwrap();
            // A flush with no intervening work still observes control.
            scope.flush().unwrap();
            for value in 7u64..266 {
                checksum += value;
                scope.step().unwrap();
            }
            let original = scope.control();
            scope.finish().unwrap();
            original
        };
        assert_eq!(checksum, (0u64..266).sum::<u64>());
        assert_eq!(
            *owner.calls.lock().unwrap(),
            vec![(phase, 0), (phase, 7), (phase, 0), (phase, 256), (phase, 3)]
        );
        // The getter borrows the original owner's lifetime, rather than the
        // completed checkpoint scope. This call still reaches that owner.
        original_control
            .checkpoint(CompilePhase::Encode, 0)
            .unwrap();
        assert_eq!(
            owner.calls.lock().unwrap().last(),
            Some(&(CompilePhase::Encode, 0))
        );
    }

    #[test]
    fn flush_failures_latch_short_and_zero_tails_without_repeated_charges() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for tail in [0u32, 7] {
                let owner = PhaseOwner {
                    calls: Mutex::default(),
                    // Only the first flush fails. Re-entering the owner would
                    // return success, exposing a lost latch or double charge.
                    fail_at: Some((2, error)),
                };
                let phase = CompilePhase::LowerProgram;
                let mut scope = CompileCheckpoints::try_new(&owner, phase).unwrap();
                let mut checksum = 0u64;
                for value in 0..tail {
                    checksum += u64::from(value);
                    scope.step().unwrap();
                }
                assert_eq!(checksum, (0..tail).map(u64::from).sum::<u64>());
                assert_eq!(scope.flush(), Err(error));
                assert_eq!(scope.step(), Err(error));
                assert_eq!(scope.flush(), Err(error));
                assert_eq!(scope.finish(), Err(error));
                assert_eq!(
                    *owner.calls.lock().unwrap(),
                    vec![(phase, 0), (phase, tail)]
                );
            }
        }
    }
}
