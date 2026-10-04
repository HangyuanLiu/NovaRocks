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

//! Carrier-neutral typed failures and observed kernel evaluation control.

use novarocks_type_contract::CompileControlError;
use std::{fmt, sync::Mutex, time::Duration};

/// Bounded diagnostics on outer failures. Only RowDataError enters the
/// maskable row channel; this carrier cannot be converted to it implicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KernelDiagnostic(Box<str>);
impl KernelDiagnostic {
    pub fn new(message: &str) -> Self {
        let mut end = message.len().min(crate::MAX_ROW_ERROR_MESSAGE_BYTES);
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        Self(message[..end].into())
    }
    pub fn message(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KernelFailure {
    Cancelled,
    DeadlineExceeded,
    ResourceExhausted,
    InvalidProgram(KernelDiagnostic),
    Internal(KernelDiagnostic),
    Operational(KernelDiagnostic),
    InstanceFailed,
}
impl fmt::Display for KernelFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InstanceFailed => f.write_str("kernel instance has already failed"),
            Self::Cancelled => f.write_str("kernel evaluation was cancelled"),
            Self::DeadlineExceeded => f.write_str("kernel evaluation deadline was exceeded"),
            Self::ResourceExhausted => f.write_str("kernel evaluation resources were exhausted"),
            Self::InvalidProgram(message) => {
                write!(f, "invalid kernel program: {}", message.message())
            }
            Self::Internal(message) => write!(f, "kernel internal failure: {}", message.message()),
            Self::Operational(message) => {
                write!(f, "kernel operational failure: {}", message.message())
            }
        }
    }
}
impl std::error::Error for KernelFailure {}

pub const MAX_UNOBSERVED_KERNEL_WORK: u32 = 256;

/// Runtime-owned interruption/work control, independent of statement semantic
/// time. Waiting is the exact sleep implementation's observable operation.
/// The host installs its formal memory scopes and authorizes known allocation
/// steps before invocation; this interface does not mint a second budget.
pub trait KernelEvaluationControl: Send + Sync {
    fn checkpoint(&self, work_units: u32) -> Result<(), KernelFailure>;
    fn wait(&self, duration: Duration) -> Result<(), KernelFailure>;
}

/// Borrow the original host control across a nested lifecycle call. This
/// observer forwards the original units and waits, without creating a meter,
/// budget or cancellation authority. A refusal remains primary across nested
/// post-call checks, including diagnostic failures returned by that control.
pub(crate) struct KernelControlObservation<'a> {
    original: &'a dyn KernelEvaluationControl,
    refusal: Mutex<Option<KernelFailure>>,
}
impl<'a> KernelControlObservation<'a> {
    pub(crate) fn new(original: &'a dyn KernelEvaluationControl) -> Self {
        Self {
            original,
            refusal: Mutex::new(None),
        }
    }
    fn observe(
        &self,
        operation: impl FnOnce() -> Result<(), KernelFailure>,
    ) -> Result<(), KernelFailure> {
        let mut refusal = self.refusal.lock().unwrap();
        if let Some(error) = refusal.as_ref() {
            return Err(error.clone());
        }
        let result = operation();
        if let Err(error) = &result {
            *refusal = Some(error.clone());
        }
        result
    }
    pub(crate) fn finish<T>(&self, result: Result<T, KernelFailure>) -> Result<T, KernelFailure> {
        match self.refusal.lock().unwrap().as_ref() {
            Some(error) => Err(error.clone()),
            None => result,
        }
    }
}
impl KernelEvaluationControl for KernelControlObservation<'_> {
    fn checkpoint(&self, work_units: u32) -> Result<(), KernelFailure> {
        self.observe(|| self.original.checkpoint(work_units))
    }
    fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
        self.observe(|| self.original.wait(duration))
    }
}

pub(crate) fn invalid(message: &str) -> KernelFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message))
}
pub(crate) fn internal(message: &str) -> KernelFailure {
    KernelFailure::Internal(KernelDiagnostic::new(message))
}

pub(crate) fn compile_failure(error: CompileControlError) -> KernelFailure {
    match error {
        CompileControlError::Cancelled => KernelFailure::Cancelled,
        CompileControlError::DeadlineExceeded => KernelFailure::DeadlineExceeded,
        CompileControlError::ResourceExhausted => KernelFailure::ResourceExhausted,
    }
}
pub(crate) fn type_failure(error: novarocks_type_contract::ValueTypeError) -> KernelFailure {
    match error {
        novarocks_type_contract::ValueTypeError::TooDeep
        | novarocks_type_contract::ValueTypeError::TooManyNodes => KernelFailure::ResourceExhausted,
        _ => invalid("binding has invalid exact logical type"),
    }
}
impl From<novarocks_type_contract::ValueTypeError> for KernelFailure {
    fn from(error: novarocks_type_contract::ValueTypeError) -> Self {
        type_failure(error)
    }
}
impl From<novarocks_type_contract::CarrierParameterError> for KernelFailure {
    fn from(error: novarocks_type_contract::CarrierParameterError) -> Self {
        invalid(&error.to_string())
    }
}

impl From<crate::EvaluationContractError> for KernelFailure {
    fn from(_: crate::EvaluationContractError) -> Self {
        invalid("evaluated argument violates its exact selected carrier")
    }
}
