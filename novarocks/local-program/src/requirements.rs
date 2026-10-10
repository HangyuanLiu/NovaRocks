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

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;

use novarocks_connector_contract::ConnectorEnvelopeHeader;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};

use crate::StaticLayout;

/// Dense index in one local program. It is distinct from a native plan node
/// ID, which need not be dense and is checked separately during lowering.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProgramNodeId(usize);

impl ProgramNodeId {
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug)]
pub enum ScanSourceKind {
    File,
    BrokerFile,
    SchemaSelection,
    TypedConnector { relation: ConnectorEnvelopeHeader },
}

/// A pure program declares every task-owned capability it will consume.
/// Instantiation must match the full set and each expected kind and layout.
#[derive(Clone, Debug)]
pub enum BindingRequirement {
    Scan {
        node: ProgramNodeId,
        kind: ScanSourceKind,
        layout: StaticLayout,
    },
    ExchangeInput {
        node: ProgramNodeId,
        layout: StaticLayout,
    },
    ExchangeOutput {
        branch: usize,
        layout: StaticLayout,
    },
    RuntimeFilter {
        binding_id: i32,
    },
    TableWriter {
        node: ProgramNodeId,
        layout: StaticLayout,
    },
    TableFinish {
        node: ProgramNodeId,
        layout: StaticLayout,
    },
    ResultSink {
        layout: StaticLayout,
    },
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum BindingKey {
    Scan(ProgramNodeId),
    ExchangeInput(ProgramNodeId),
    ExchangeOutput(usize),
    RuntimeFilter(i32),
    TableWriter(ProgramNodeId),
    TableFinish(ProgramNodeId),
    ResultSink,
}

impl BindingRequirement {
    fn key(&self) -> BindingKey {
        match self {
            Self::Scan { node, .. } => BindingKey::Scan(*node),
            Self::ExchangeInput { node, .. } => BindingKey::ExchangeInput(*node),
            Self::ExchangeOutput { branch, .. } => BindingKey::ExchangeOutput(*branch),
            Self::RuntimeFilter { binding_id } => BindingKey::RuntimeFilter(*binding_id),
            Self::TableWriter { node, .. } => BindingKey::TableWriter(*node),
            Self::TableFinish { node, .. } => BindingKey::TableFinish(*node),
            Self::ResultSink { .. } => BindingKey::ResultSink,
        }
    }
}

#[derive(Clone, Debug)]
pub struct BindingRequirements {
    entries: Arc<[BindingRequirement]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindingRequirementsError {
    Duplicate,
}

impl fmt::Display for BindingRequirementsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("local program contains duplicate binding requirements")
    }
}

impl std::error::Error for BindingRequirementsError {}

/// Compilation preserves the caller's control cause separately from structural
/// requirement errors. It does not authorize task capabilities or memory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindingRequirementsCompileError {
    Requirement(BindingRequirementsError),
    Control(CompileControlError),
}

impl From<CompileControlError> for BindingRequirementsCompileError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for BindingRequirementsCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Requirement(error) => error.fmt(formatter),
            Self::Control(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for BindingRequirementsCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Requirement(error) => Some(error),
            Self::Control(error) => Some(error),
        }
    }
}

impl BindingRequirements {
    pub fn try_new(entries: Vec<BindingRequirement>) -> Result<Self, BindingRequirementsError> {
        match Self::try_new_core(entries, None) {
            Ok(value) => Ok(value),
            Err(BindingRequirementsCompileError::Requirement(error)) => Err(error),
            Err(BindingRequirementsCompileError::Control(_)) => {
                unreachable!("legacy construction has no compile control")
            }
        }
    }

    pub fn try_new_for_compile(
        entries: Vec<BindingRequirement>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, BindingRequirementsCompileError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = Self::try_new_core(entries, Some(&mut work));
        if matches!(&result, Err(BindingRequirementsCompileError::Control(_))) {
            return result;
        }
        // An ordinary structural error still observes completed work. A
        // primary control failure above never calls the owner a second time.
        work.finish()?;
        result
    }

    fn try_new_core(
        entries: Vec<BindingRequirement>,
        mut work: Option<&mut CompileCheckpoints<'_>>,
    ) -> Result<Self, BindingRequirementsCompileError> {
        let mut keys = BTreeSet::new();
        for entry in &entries {
            let inserted = keys.insert(entry.key());
            if let Some(work) = &mut work {
                work.step()?;
            }
            if !inserted {
                return Err(BindingRequirementsCompileError::Requirement(
                    BindingRequirementsError::Duplicate,
                ));
            }
        }
        // Arc's allocation/move is opaque. Observe both boundaries; this is
        // not a claim about its first allocation or internal cooperation.
        if let Some(work) = &mut work {
            work.flush()?;
        }
        let entries = Arc::from(entries);
        if let Some(work) = &mut work {
            work.flush()?;
        }
        Ok(Self { entries })
    }

    pub fn entries(&self) -> &[BindingRequirement] {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct Control {
        calls: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::LowerProgram);
            let mut calls = self.calls.lock().unwrap();
            calls.push(units);
            if let Some((call, error)) = self.refusal
                && calls.len() == call
            {
                return Err(error);
            }
            Ok(())
        }
    }
    fn filters(count: i32) -> Vec<BindingRequirement> {
        (0..count)
            .map(|binding_id| BindingRequirement::RuntimeFilter { binding_id })
            .collect()
    }

    #[test]
    fn compile_requirements_preserve_legacy_order_and_observe_real_work() {
        let control = Control::default();
        let actual = BindingRequirements::try_new_for_compile(filters(513), &control).unwrap();
        let legacy = BindingRequirements::try_new(filters(513)).unwrap();
        assert_eq!(
            actual
                .entries()
                .iter()
                .map(BindingRequirement::key)
                .collect::<Vec<_>>(),
            legacy
                .entries()
                .iter()
                .map(BindingRequirement::key)
                .collect::<Vec<_>>()
        );
        assert_eq!(*control.calls.lock().unwrap(), vec![0, 256, 256, 1, 0, 0]);
    }

    #[test]
    fn compile_requirements_observe_duplicate_error_tail() {
        let control = Control::default();
        let result = BindingRequirements::try_new_for_compile(
            vec![
                BindingRequirement::RuntimeFilter { binding_id: 7 },
                BindingRequirement::RuntimeFilter { binding_id: 7 },
            ],
            &control,
        );
        assert!(matches!(
            result,
            Err(BindingRequirementsCompileError::Requirement(
                BindingRequirementsError::Duplicate
            ))
        ));
        assert_eq!(*control.calls.lock().unwrap(), vec![0, 2]);
    }

    #[test]
    fn compile_requirements_preserve_each_primary_control_cause_without_rechecking() {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for call in 1..=6 {
                let control = Control {
                    calls: Mutex::default(),
                    refusal: Some((call, error)),
                };
                let result = BindingRequirements::try_new_for_compile(filters(513), &control);
                assert!(
                    matches!(result, Err(BindingRequirementsCompileError::Control(actual)) if actual == error)
                );
                assert_eq!(control.calls.lock().unwrap().len(), call);
            }
        }
    }

    #[test]
    fn compile_requirements_control_tail_overrides_duplicate_with_typed_source() {
        use std::error::Error;
        let control = Control {
            calls: Mutex::default(),
            refusal: Some((2, CompileControlError::DeadlineExceeded)),
        };
        let mut entries = filters(1);
        entries.extend(filters(1));
        let error = BindingRequirements::try_new_for_compile(entries, &control).unwrap_err();
        assert_eq!(
            error,
            BindingRequirementsCompileError::Control(CompileControlError::DeadlineExceeded)
        );
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<CompileControlError>(),
            Some(&CompileControlError::DeadlineExceeded)
        );
        assert_eq!(*control.calls.lock().unwrap(), vec![0, 2]);
    }

    #[test]
    fn empty_compile_requirements_still_observe_publication() {
        let baseline = Control::default();
        assert!(
            BindingRequirements::try_new_for_compile(vec![], &baseline)
                .unwrap()
                .entries()
                .is_empty()
        );
        assert_eq!(*baseline.calls.lock().unwrap(), vec![0, 0, 0, 0]);
        let control = Control {
            calls: Mutex::default(),
            refusal: Some((4, CompileControlError::Cancelled)),
        };
        assert!(matches!(
            BindingRequirements::try_new_for_compile(vec![], &control),
            Err(BindingRequirementsCompileError::Control(
                CompileControlError::Cancelled
            ))
        ));
        assert_eq!(*control.calls.lock().unwrap(), vec![0, 0, 0, 0]);
    }

    #[test]
    fn rejects_ambiguous_binding_slot() {
        let requirements = BindingRequirements::try_new(vec![
            BindingRequirement::RuntimeFilter { binding_id: 7 },
            BindingRequirement::RuntimeFilter { binding_id: 7 },
        ]);
        assert!(matches!(
            requirements,
            Err(BindingRequirementsError::Duplicate)
        ));
    }
}
