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

//! Runtime-owned loan at the original ScalarV1 invocation site. Pure source
//! facts never carry an account, controller or allocation permission.

use novarocks_functions::{
    EvaluatedArgument, KernelEvaluationControl, KernelFailure, ScalarEvaluationInstance,
    ScalarInvocationData, ScalarInvocationFailure, ScalarResourceError, SelectedValues, Selection,
};
use novarocks_memory::{CapacityError, ShortageReceipt};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeScalarMemoryRefusal {
    MissingQueryMemory,
    SharedShortage(ShortageReceipt),
    Capacity(CapacityError),
}
impl std::fmt::Display for RuntimeScalarMemoryRefusal {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingQueryMemory => out.write_str("scalar runtime requires query memory"),
            Self::SharedShortage(receipt) => {
                write!(out, "scalar runtime shared shortage: {receipt:?}")
            }
            Self::Capacity(cause) => cause.fmt(out),
        }
    }
}
impl std::error::Error for RuntimeScalarMemoryRefusal {}

/// The original Frame transports each first cause nominally. Source/host
/// refusal must bypass the old kernel footer and fail the original root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeScalarEvaluationFailure {
    Kernel(KernelFailure),
    Data(ScalarInvocationData),
    Source(ScalarResourceError),
    Host(RuntimeScalarMemoryRefusal),
}
impl From<KernelFailure> for RuntimeScalarEvaluationFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}
impl From<ScalarInvocationFailure> for RuntimeScalarEvaluationFailure {
    fn from(cause: ScalarInvocationFailure) -> Self {
        match cause {
            ScalarInvocationFailure::Kernel(cause) => Self::Kernel(cause),
            ScalarInvocationFailure::Data(cause) => Self::Data(cause),
        }
    }
}
impl std::fmt::Display for RuntimeScalarEvaluationFailure {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kernel(cause) => cause.fmt(out),
            Self::Data(cause) => cause.fmt(out),
            Self::Source(cause) => write!(out, "scalar runtime request: {cause}"),
            Self::Host(cause) => cause.fmt(out),
        }
    }
}
impl std::error::Error for RuntimeScalarEvaluationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Kernel(cause) => Some(cause),
            Self::Data(cause) => Some(cause),
            Self::Source(cause) => Some(cause),
            Self::Host(cause) => Some(cause),
        }
    }
}

/// Explicit synchronous loan from the real runtime caller to its one Frame.
/// Lifetime-only generics keep this trusted Execution port dyn-compatible.
/// The concrete host owns the static FnOnce and any account qualification.
/// Its original observed controller must come from that same runtime caller;
/// this interface does not prove arbitrary third-party trait implementations.
pub trait RuntimeScalarOperationScope {
    fn evaluate<'a>(
        &mut self,
        instance: &mut ScalarEvaluationInstance,
        selection: Selection<'a>,
        arguments: &'a [EvaluatedArgument<'a>],
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, RuntimeScalarEvaluationFailure>;
}

#[cfg(test)]
mod tests {
    use super::*;

    // Compile the actual dyn/lifetime contract before any production grant.
    fn borrow_scope<'loan>(
        scope: &'loan mut impl RuntimeScalarOperationScope,
    ) -> &'loan mut dyn RuntimeScalarOperationScope {
        scope
    }
    struct Refuse;
    impl RuntimeScalarOperationScope for Refuse {
        fn evaluate<'a>(
            &mut self,
            _: &mut ScalarEvaluationInstance,
            _: Selection<'a>,
            _: &'a [EvaluatedArgument<'a>],
            _: &dyn KernelEvaluationControl,
        ) -> Result<SelectedValues<'a>, RuntimeScalarEvaluationFailure> {
            Err(RuntimeScalarEvaluationFailure::Host(
                RuntimeScalarMemoryRefusal::MissingQueryMemory,
            ))
        }
    }
    #[test]
    fn runtime_scalar_memory_scope_is_a_borrowed_dyn_compatible_port() {
        let mut scope = Refuse;
        let _: &mut dyn RuntimeScalarOperationScope = borrow_scope(&mut scope);
    }
}
