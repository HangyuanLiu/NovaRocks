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
    FragmentCreationError, LocalCompileOptions, ProviderPreparationError,
    compile_fragment_with_creation, validate_fragment_providers,
};
use novarocks_local_program::{KernelAbiVersion, LocalProgram};
use novarocks_plan_codec::host_projection_v2::ProjectionFailure;
use novarocks_plan_codec::physical_package_v2::{
    PackageDecodeError, PackageDecodeLimits, decode_fragment_package_with_factory,
};
use novarocks_plan_codec::resource_preflight_v2::FragmentDecodeResourceModel;
use novarocks_spi::connector::ConnectorStopView;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueSchemaCreation,
};

pub use crate::native_type_construction::NativeTypeConstructionError;

use crate::compiled_runtime_filter::CompiledRuntimeFilterEndpoints;

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
    /// The original BE construction owner refused an actual request.
    Construction(NativeTypeConstructionError),
    /// The original receiver cause, including its source/resource model.
    Decode(PackageDecodeError),
    /// The package is not receivable, its providers refuse it, or the
    /// compiler refuses one of its shapes.
    Refused(String),
}

impl fmt::Display for CompiledPackageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(cause) => cause.fmt(f),
            Self::Construction(cause) => cause.fmt(f),
            Self::Decode(cause) => cause.fmt(f),
            Self::Refused(detail) => f.write_str(detail),
        }
    }
}

impl Error for CompiledPackageError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(cause) => Some(cause),
            Self::Construction(cause) => Some(cause),
            Self::Decode(cause) => Some(cause),
            Self::Refused(_) => None,
        }
    }
}

/// One task's compiled program and the runtime-filter bindings its package
/// numbers.
///
/// The program names each runtime-filter site by its plan-global binding
/// identity only; which filter and endpoint a binding is stays in the
/// package's cuts, so it is projected here, before the package is consumed.
#[derive(Debug)]
pub struct CompiledTaskProgram {
    pub program: LocalProgram,
    pub runtime_filters: CompiledRuntimeFilterEndpoints,
}

/// A sealed, borrowed factory supplied by the actual admitted BE preparation.
/// This public port preserves the compiler's object safety; constructing it is
/// restricted to the Native owner that already has the tracker and controls.
pub struct CompiledTypeScopes<'borrow, 'control> {
    inner: crate::native_type_construction::NativeTypeFactory<'borrow, 'control>,
}
impl<'borrow, 'control> CompiledTypeScopes<'borrow, 'control> {
    pub(crate) fn new(
        creation: &'borrow crate::native_type_construction::NativeTypeConstruction<'control>,
    ) -> Self {
        Self {
            inner: crate::native_type_construction::NativeTypeFactory::new(creation),
        }
    }
}
pub struct CompiledTypeScope<'borrow> {
    inner: crate::native_type_construction::NativeFactoryScope<'borrow>,
}
impl novarocks_type_contract::ValueSchemaCreationScope<NativeTypeConstructionError>
    for CompiledTypeScope<'_>
{
    fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        novarocks_type_contract::ValueSchemaCreationScope::storage_lifetime(&self.inner)
    }
    fn request_layout(
        &mut self,
        layout: std::alloc::Layout,
    ) -> Result<(), NativeTypeConstructionError> {
        novarocks_type_contract::ValueSchemaCreationScope::request_layout(&mut self.inner, layout)
    }
    fn observe_opaque_upper_bound(
        &mut self,
        bytes: usize,
    ) -> Result<(), NativeTypeConstructionError> {
        novarocks_type_contract::ValueSchemaCreationScope::observe_opaque_upper_bound(
            &mut self.inner,
            bytes,
        )
    }
}
impl novarocks_type_contract::ValueSchemaCreationFactory<NativeTypeConstructionError>
    for CompiledTypeScopes<'_, '_>
{
    type Scope<'a>
        = CompiledTypeScope<'a>
    where
        Self: 'a;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, NativeTypeConstructionError> {
        Ok(CompiledTypeScope {
            inner: self.inner.begin_scope()?,
        })
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(std::alloc::Layout) -> Result<(), O>,
    ) -> Result<
        Self::Scope<'_>,
        novarocks_type_contract::ValueSchemaScopeBeginFailure<O, NativeTypeConstructionError>,
    > {
        Ok(CompiledTypeScope {
            inner: self.inner.begin_scope_observed(observe)?,
        })
    }
}

/// Turns one task's package bytes into its LocalProgram.
pub trait CompiledPackageCompiler: Send + Sync + 'static {
    fn compile(
        &self,
        package: &[u8],
        options: CompiledTaskOptions,
        creation: &mut ValueSchemaCreation<'_, NativeTypeConstructionError>,
        type_scopes: &CompiledTypeScopes<'_, '_>,
        control: &dyn PureCompileControl,
    ) -> Result<CompiledTaskProgram, CompiledPackageError>;
}

/// Receiver, provider validation and local compiler over host-owned inputs.
pub struct CompiledPackageInterpreter<E: Error + Send + Sync + 'static> {
    /// Shared with the ingress package gate, so one process builds one model.
    model: Arc<FragmentDecodeResourceModel>,
    decode_limits: PackageDecodeLimits,
    functions: Arc<PureEngineFunctionCatalog>,
    providers: Arc<PureProviderProgramCatalog<E>>,
    constants: ConstantPolicy,
}

impl<E: Error + Send + Sync + 'static> CompiledPackageInterpreter<E> {
    pub fn new(
        model: impl Into<Arc<FragmentDecodeResourceModel>>,
        decode_limits: PackageDecodeLimits,
        functions: Arc<PureEngineFunctionCatalog>,
        providers: Arc<PureProviderProgramCatalog<E>>,
        constants: ConstantPolicy,
    ) -> Self {
        Self {
            model: model.into(),
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
        creation: &mut ValueSchemaCreation<'_, NativeTypeConstructionError>,
        type_scopes: &CompiledTypeScopes<'_, '_>,
        control: &dyn PureCompileControl,
    ) -> Result<CompiledTaskProgram, CompiledPackageError> {
        let package = decode_fragment_package_with_factory(
            package,
            &self.model,
            &self.decode_limits,
            creation,
            type_scopes,
            control,
        )
        .map_err(package_decode_failure)?;
        let runtime_filters =
            CompiledRuntimeFilterEndpoints::from_package(&package).map_err(|error| {
                CompiledPackageError::Refused(format!(
                    "package runtime-filter bindings are not resolvable: {error}"
                ))
            })?;
        // The compiled RootResult algorithm gathers its terminal pipeline to
        // one driver; this width is independent of the task pipeline DOP.
        let root_sink_dop = matches!(
            package.fragment().sink(),
            novarocks_physical_plan::FragmentSink::RootResult(_)
        )
        .then_some(NonZeroUsize::MIN);
        let validated = validate_fragment_providers(Arc::new(package), &self.providers, control)
            .map_err(|error| match error {
                ProviderPreparationError::Control(cause) => CompiledPackageError::Control(cause),
                other => {
                    CompiledPackageError::Refused(format!("package providers refuse it: {other}"))
                }
            })?;
        use novarocks_type_contract::ValueSchemaCreationFactory;
        let mut temporary = type_scopes
            .begin_scope()
            .map_err(CompiledPackageError::Construction)?;
        let program = compile_fragment_with_creation(
            validated,
            &self.functions,
            LocalCompileOptions {
                pipeline_dop: options.pipeline_dop,
                root_sink_dop,
                kernel_abi: KernelAbiVersion::CURRENT,
                constants: self.constants,
                exchange_wait: options.exchange_wait,
            },
            creation,
            &mut temporary,
            type_scopes,
            control,
        )
        .map_err(|error| match error {
            FragmentCreationError::Control(cause) => CompiledPackageError::Control(cause),
            FragmentCreationError::Creator(cause) => CompiledPackageError::Construction(cause),
            FragmentCreationError::Resource(cause) => {
                CompiledPackageError::Construction(NativeTypeConstructionError::Resource(cause))
            }
            FragmentCreationError::Compile(other) => {
                CompiledPackageError::Refused(format!("package does not compile: {other}"))
            }
        })?;
        Ok(CompiledTaskProgram {
            program,
            runtime_filters,
        })
    }
}

fn package_decode_failure(
    error: ProjectionFailure<PackageDecodeError, NativeTypeConstructionError>,
) -> CompiledPackageError {
    match error {
        ProjectionFailure::Host(cause) => CompiledPackageError::Construction(cause),
        ProjectionFailure::Codec(PackageDecodeError::Control(cause)) => {
            CompiledPackageError::Control(cause)
        }
        ProjectionFailure::Codec(other) => CompiledPackageError::Decode(other),
    }
}

/// Compile control for one task's preparation: a stop of the task refuses the
/// next checkpoint, so decode, provider validation and compile all end with
/// the task's own cancellation as their primary cause.
pub(crate) struct TaskPreparationControl {
    stop: ConnectorStopView,
    original: novarocks_worker::TaskPreparationStopView,
}

impl TaskPreparationControl {
    pub(crate) fn new(
        stop: ConnectorStopView,
        original: novarocks_worker::TaskPreparationStopView,
    ) -> Self {
        Self { stop, original }
    }
}

impl PureCompileControl for TaskPreparationControl {
    fn checkpoint(&self, _phase: CompilePhase, _units: u32) -> Result<(), CompileControlError> {
        match self.original.stop_cause() {
            Some(novarocks_worker::TaskPreparationStopCause::LeaseExpired) => {
                return Err(CompileControlError::DeadlineExceeded);
            }
            Some(novarocks_worker::TaskPreparationStopCause::Cancelled) => {
                return Err(CompileControlError::Cancelled);
            }
            None => {}
        }
        if self.stop.is_stopped() {
            return Err(CompileControlError::Cancelled);
        }
        Ok(())
    }
}
