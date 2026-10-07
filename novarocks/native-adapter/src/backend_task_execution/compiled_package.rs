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

//! The compiled-package static plan interpreter.
//!
//! A host composes exactly one static plan interpreter. When it composes this
//! one, a task's static carrier is a physical package that is received,
//! provider-validated and compiled into a LocalProgram during preparation;
//! the plan-tree decoder is never consulted. The host-owned decode model,
//! limits and catalogs are built once at composition, never per task.

use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use novarocks_connector_contract::PureProviderProgramCatalog;
use novarocks_functions::{ConstantPolicy, PureEngineFunctionCatalog};
use novarocks_local_compiler::{
    FragmentCompileError, LocalCompileOptions, ProviderPreparationError, compile_fragment,
    validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram};
use novarocks_plan_codec::physical_package_v2::{
    PackageDecodeError, PackageDecodeLimits, decode_fragment_package,
};
use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
use novarocks_spi::connector::ConnectorStopView;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};

/// The task facts a compiled program is specialized for.
#[derive(Clone, Copy, Debug)]
pub struct CompiledTaskOptions {
    pub pipeline_dop: NonZeroUsize,
    /// Host-admitted receive wait for every compiled exchange source.
    pub exchange_wait: Duration,
}

/// Why a task's package did not become a LocalProgram.
#[derive(Debug)]
pub enum CompiledPackageError {
    /// The task's own control stopped the work; the cause stays primary.
    Control(CompileControlError),
    /// The package is not receivable, its providers refuse it, or the
    /// compiler refuses one of its shapes.
    Refused(String),
}

impl fmt::Display for CompiledPackageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Refused(detail) => f.write_str(detail),
        }
    }
}

impl Error for CompiledPackageError {}

/// Turns one task's package bytes into its LocalProgram.
pub trait CompiledPackageCompiler: Send + Sync + 'static {
    fn compile(
        &self,
        package: &[u8],
        options: CompiledTaskOptions,
        control: &dyn PureCompileControl,
    ) -> Result<LocalProgram, CompiledPackageError>;
}

/// Receiver, provider validation and local compiler over host-owned inputs.
pub struct CompiledPackageInterpreter<E: Error + Send + Sync + 'static> {
    model: FragmentDecodeResourceModel,
    decode_limits: PackageDecodeLimits,
    functions: Arc<PureEngineFunctionCatalog>,
    providers: Arc<PureProviderProgramCatalog<E>>,
    constants: ConstantPolicy,
}

impl<E: Error + Send + Sync + 'static> CompiledPackageInterpreter<E> {
    pub fn new(
        model: FragmentDecodeResourceModel,
        decode_limits: PackageDecodeLimits,
        functions: Arc<PureEngineFunctionCatalog>,
        providers: Arc<PureProviderProgramCatalog<E>>,
        constants: ConstantPolicy,
    ) -> Self {
        Self {
            model,
            decode_limits,
            functions,
            providers,
            constants,
        }
    }
}

impl<E: Error + Send + Sync + 'static> CompiledPackageCompiler for CompiledPackageInterpreter<E>
where
    PureProviderProgramCatalog<E>: Send + Sync,
{
    fn compile(
        &self,
        package: &[u8],
        options: CompiledTaskOptions,
        control: &dyn PureCompileControl,
    ) -> Result<LocalProgram, CompiledPackageError> {
        let package = decode_fragment_package(package, &self.model, &self.decode_limits, control)
            .map_err(|error| match error {
            PackageDecodeError::Control(cause) => CompiledPackageError::Control(cause),
            other => CompiledPackageError::Refused(format!("package is not receivable: {other}")),
        })?;
        let validated = validate_fragment_providers(Arc::new(package), &self.providers, control)
            .map_err(|error| match error {
                ProviderPreparationError::Control(cause) => CompiledPackageError::Control(cause),
                other => {
                    CompiledPackageError::Refused(format!("package providers refuse it: {other}"))
                }
            })?;
        compile_fragment(
            validated,
            &self.functions,
            LocalCompileOptions {
                pipeline_dop: options.pipeline_dop,
                // The plan-tree path places a root sink by the task's DOP,
                // so the compiled profile freezes no separate width.
                root_sink_dop: None,
                kernel_abi: KernelAbiVersion::CURRENT,
                constants: self.constants,
                exchange_wait: options.exchange_wait,
            },
            control,
        )
        .map_err(|error| match error {
            FragmentCompileError::Control(cause) => CompiledPackageError::Control(cause),
            other => CompiledPackageError::Refused(format!("package does not compile: {other}")),
        })
    }
}

/// Compile control for one task's preparation: a stop of the task refuses the
/// next checkpoint, so decode, provider validation and compile all end with
/// the task's own cancellation as their primary cause.
pub(crate) struct TaskPreparationControl {
    stop: ConnectorStopView,
}

impl TaskPreparationControl {
    pub(crate) fn new(stop: ConnectorStopView) -> Self {
        Self { stop }
    }
}

impl PureCompileControl for TaskPreparationControl {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        if self.stop.is_stopped() {
            return Err(CompileControlError::Cancelled);
        }
        Ok(())
    }
}
