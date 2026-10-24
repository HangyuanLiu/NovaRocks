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

//! Closed Rewrite recipe/freeze inputs borrowed by the existing CPU factory.
use super::*;
use crate::query_execution::{
    completion_facts::{CompletionFactOwners, FrontendCompletionCustody, StatementFactScope},
    provider_read_facts::FrozenProviderRead,
};
use novarocks_query_application::{
    admitted_query_context::{QueryExecutionContext, QueryResultCapacityBinding},
    preparation::{FinalPlanRuntimeAccess, ReadAccessSink},
};
use novarocks_spi::connector::{
    ConnectorDistributedRewriteLease, ConnectorInstanceDescriptor, ConnectorRequestContext,
    ProviderBindingEpoch, RewriteCpuInput, RewriteCpuPort, RewritePlanCpuSource,
};
use std::sync::Mutex;

type Access = FinalPlanRuntimeAccess<FrozenProviderRead>;
type CompileResult<T> = Result<T, novarocks_sql::compiler::SqlCompileError>;
enum OriginalInput {
    Recipe {
        session: crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession,
        cohort_id: ConnectorWriteCohortId,
        execution: QueryExecutionContext,
        context: ConnectorRequestContext,
    },
    Freeze {
        recipe: Arc<RewriteCohortRecipe>,
        fact: novarocks_sql::compiler::ProviderReadFact,
        access: Option<Access>,
        functions: Arc<novarocks_functions::EngineFunctionCatalog>,
        carrier: crate::query_execution::package_freeze::CompiledPackageCarrier,
        constant_policy: novarocks_functions::ConstantPolicy,
    },
}
enum OriginalOutput {
    Recipe(RewriteCohortRecipe),
    Frozen(RewriteCohortPlanParts),
}
struct CpuBody {
    input: OriginalInput,
    started: bool,
    result: Option<CompileResult<OriginalOutput>>,
}
/// Exact FE first-party input type admitted by its closed cohort factory.
/// It cannot carry arbitrary closures or Provider-selected operations.
pub(crate) struct RewriteCohortCpuOriginal {
    descriptor: ConnectorInstanceDescriptor,
    incarnation: ProviderBindingEpoch,
    body: Mutex<Option<CpuBody>>,
}
struct CpuLoan<'a> {
    source: &'a RewriteCohortCpuOriginal,
    body: Option<CpuBody>,
}
impl<'a> CpuLoan<'a> {
    fn take(source: &'a RewriteCohortCpuOriginal) -> Self {
        let body = source.body.lock().unwrap_or_else(|p| p.into_inner()).take();
        assert!(body.is_some(), "one original Rewrite CPU loan");
        Self { source, body }
    }
}
impl Drop for CpuLoan<'_> {
    fn drop(&mut self) {
        let mut slot = self.source.body.lock().unwrap_or_else(|p| p.into_inner());
        assert!(slot.is_none(), "same original Rewrite CPU slot");
        *slot = self.body.take();
    }
}
impl RewritePlanCpuSource for RewriteCohortCpuOriginal {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn incarnation(&self) -> ProviderBindingEpoch {
        self.incarnation
    }
    fn run_plan(&self) {
        let mut loan = CpuLoan::take(self);
        let body = loan.body.as_mut().unwrap();
        assert!(!body.started, "one original Rewrite CPU invocation");
        body.started = true;
        body.result = Some(match &body.input {
            OriginalInput::Recipe {
                session,
                cohort_id,
                execution,
                context,
            } => prepare_rewrite_cohort_recipe(session, *cohort_id, execution, context)
                .map(OriginalOutput::Recipe),
            OriginalInput::Freeze {
                recipe,
                fact,
                access,
                functions,
                carrier,
                constant_policy,
            } => {
                let control = crate::query_execution::planning::sql_compile_control_from_execution(
                    &recipe.execution,
                );
                freeze_rewrite_cohort_recipe(
                    recipe,
                    fact.clone(),
                    access.as_ref().expect("original Rewrite access"),
                    functions.clone(),
                    carrier,
                    *constant_policy,
                    &control,
                )
                .map(OriginalOutput::Frozen)
            }
        });
    }
}

/// The Action owns this whole stable custody before polling its borrowed F.
/// Its original access/sink/source survive observer cancellation and CPU panic.
#[derive(Default)]
pub(crate) struct RewritePreparationCustody {
    original: Option<Arc<RewriteCohortCpuOriginal>>,
    recipe: Option<Arc<RewriteCohortRecipe>>,
    sink: Option<ReadAccessSink<FrozenProviderRead>>,
    returned:
        Option<Vec<novarocks_query_application::preparation::FrozenReadAccess<FrozenProviderRead>>>,
    output: Option<CompileResult<PreparedDistributedRewriteCohort>>,
    started: bool,
    closed: bool,
}
impl RewritePreparationCustody {
    pub(crate) fn layout() -> std::alloc::Layout {
        std::alloc::Layout::new::<Self>()
    }
    /// Fixed heap metadata only; the inline custody is already included in
    /// its actual Action layout. This quote does not grant running CPU, graph
    /// formation bytes, read access, or capacity for a result/tail vector.
    pub(crate) fn fixed_heap_bytes()
    -> Result<std::num::NonZeroU64, novarocks_workload_control::WorkError> {
        use novarocks_type_contract::owned_resources::layout::arc_layout;
        use novarocks_workload_control::WorkError;
        let sink = ReadAccessSink::<FrozenProviderRead>::known_source_layouts()
            .map_err(|_| WorkError::ArithmeticOverflow)?;
        let source = arc_layout(std::alloc::Layout::new::<RewriteCohortCpuOriginal>())
            .map_err(|_| WorkError::ArithmeticOverflow)?
            .size();
        let recipe = arc_layout(std::alloc::Layout::new::<RewriteCohortRecipe>())
            .map_err(|_| WorkError::ArithmeticOverflow)?
            .size();
        let mutex = if cfg!(all(target_os = "macos", target_pointer_width = "64")) {
            64
        } else {
            0
        };
        let port = usize::try_from(crate::query_execution::internal_result_cpu::InternalResultCpu::rewrite_port_metadata_bytes_for_composition()?.get())
            .map_err(|_| WorkError::ArithmeticOverflow)?;
        let bytes = source
            .checked_add(recipe)
            .and_then(|n| n.checked_add(mutex))
            .and_then(|n| n.checked_add(sink[0].size()))
            .and_then(|n| n.checked_add(sink[1].size()))
            .and_then(|n| n.checked_add(port))
            .ok_or(WorkError::ArithmeticOverflow)?;
        std::num::NonZeroU64::new(u64::try_from(bytes).map_err(|_| WorkError::ArithmeticOverflow)?)
            .ok_or(WorkError::ArithmeticOverflow)
    }

    // Called only after the same worker/forwarder and Provider have joined.
    // Every capability returns to the original owner before source retirement.
    fn receive_unpaired_after_exit(&mut self) {
        if let Some(sink) = self.sink.take() {
            assert!(self.returned.is_none());
            self.returned = Some(sink.into_taken());
        }
        if let Some(original) = self.original.as_ref() {
            let mut loan = CpuLoan::take(original);
            if let OriginalInput::Freeze { access, .. } = &mut loan.body.as_mut().unwrap().input {
                if let Some(access) = access.take() {
                    assert!(self.returned.is_none());
                    self.returned = Some(access.into_taken());
                }
            }
        }
    }
    pub(crate) fn take_prepared_in(
        &mut self,
        target: &mut Option<CompileResult<PreparedDistributedRewriteCohort>>,
    ) -> Result<(), &'static str> {
        if target.is_some() || !self.closed || self.output.is_none() {
            return Err("Rewrite preparation has no closed transferable result");
        }
        *target = self.output.take();
        Ok(())
    }
    /// This close is driven by the same original Source even after the Product
    /// waiter exits. The provider observer and CPU original remain affirmative.
    pub(crate) async fn close(
        &mut self,
        cpu: &dyn RewriteCpuPort,
        provider: &FrontendCompletionCustody,
    ) -> Result<(), novarocks_query_application::api::StatementNativeHealthFailure> {
        std::future::poll_fn(|cx| cpu.poll_closed(cx))
            .await
            .map_err(|e| novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven("original Rewrite CPU close is unproven", Arc::new(e)))?;
        provider.close().await?;
        self.receive_unpaired_after_exit();
        self.closed = true;
        Ok(())
    }
}

pub(crate) async fn prepare_original_cohort_in(
    custody: &mut RewritePreparationCustody,
    engine: &RequestScopedMaintenanceEngine,
    session: &crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession,
    cohort_id: ConnectorWriteCohortId,
    lease: &ConnectorDistributedRewriteLease,
    cpu: &dyn RewriteCpuPort,
    provider: &FrontendCompletionCustody,
    completion: CompletionFactOwners,
    capacity: QueryResultCapacityBinding,
) -> Result<(), String> {
    if custody.started || custody.original.is_some() || custody.output.is_some() || custody.closed {
        return Err("Rewrite preparation already owns an original invocation".into());
    }
    custody.started = true;
    let original = Arc::new(RewriteCohortCpuOriginal {
        descriptor: lease.descriptor().clone(),
        incarnation: lease.incarnation(),
        body: Mutex::new(Some(CpuBody {
            input: OriginalInput::Recipe {
                session: session.clone(),
                cohort_id,
                execution: engine.execution.clone(),
                context: engine.connector_context.clone(),
            },
            started: false,
            result: None,
        })),
    });
    custody.original = Some(original.clone());
    let mut input = Some(RewriteCpuInput(original.clone()));
    cpu.submit_in(&mut input).map_err(|e| e.to_string())?;
    std::future::poll_fn(|cx| cpu.poll_finished(cx))
        .await
        .map_err(|e| e.to_string())?;
    let recipe = {
        let mut loan = CpuLoan::take(&original);
        match loan
            .body
            .as_mut()
            .unwrap()
            .result
            .take()
            .ok_or("Rewrite recipe has no original CPU result")?
        {
            Ok(OriginalOutput::Recipe(recipe)) => Arc::new(recipe),
            Err(error) => {
                custody.output = Some(Err(error));
                custody.closed = true;
                return Ok(());
            }
            _ => return Err("Rewrite recipe CPU returned a foreign result".into()),
        }
    };
    custody.recipe = Some(recipe.clone());
    // The first physical worker and forwarder have exited. Retire this empty
    // original before allocating its successor; only one CPU source Arc is
    // live at a time and the fixed heap quote states that actual geometry.
    drop(custody.original.take());
    drop(original);
    // A real governed source executes the actual read factory, not BindingOnly.
    let scope = StatementFactScope::governed_read_io(
        recipe.bindings.clone(),
        engine.connector_context.clone(),
        None,
        capacity.scope(),
        capacity.clone(),
    )
    .map_err(|e| e.to_string())?;
    crate::query_execution::completion_facts::install_frontend_fact_source(
        provider,
        completion,
        scope,
        crate::query_execution::compiler::typed_connector_session()?,
    )?;
    custody.sink = Some(ReadAccessSink::new());
    let mut facts = provider
        .resolve_provider_reads(&[recipe.need.clone()], custody.sink.as_ref().unwrap())
        .await?;
    if facts.len() != 1 {
        return Err("Rewrite preparation requires its exact single read fact".into());
    }
    let access = match custody.sink.take().unwrap().try_into_access() {
        Ok(access) => access,
        Err((error, returned)) => {
            custody.returned = Some(returned);
            return Err(error.to_string());
        }
    };
    // The unique access moves into another ORIGINAL stable Source, which the
    // Action retains outside the CPU input alias and every borrowed future.
    let original = Arc::new(RewriteCohortCpuOriginal {
        descriptor: lease.descriptor().clone(),
        incarnation: lease.incarnation(),
        body: Mutex::new(Some(CpuBody {
            input: OriginalInput::Freeze {
                recipe: recipe.clone(),
                fact: facts.pop().unwrap(),
                access: Some(access),
                functions: engine.kernel.function_catalog().clone(),
                carrier: *engine.kernel.static_plan_carrier(),
                constant_policy: engine.kernel.constant_policy(),
            },
            started: false,
            result: None,
        })),
    });
    custody.original = Some(original.clone());
    let mut input = Some(RewriteCpuInput(original.clone()));
    cpu.submit_in(&mut input).map_err(|e| e.to_string())?;
    std::future::poll_fn(|cx| cpu.poll_finished(cx))
        .await
        .map_err(|e| e.to_string())?;
    // Exact normal F/worker/forwarder exit precedes any capability transfer.
    provider.close().await.map_err(|e| e.detail().to_owned())?;
    let mut loan = CpuLoan::take(&original);
    let body = loan.body.as_mut().unwrap();
    let result = body
        .result
        .take()
        .ok_or("Rewrite freeze has no CPU result")?;
    custody.output = Some(match result {
        Ok(OriginalOutput::Frozen(parts)) => {
            let OriginalInput::Freeze { access, .. } = &mut body.input else {
                unreachable!()
            };
            finish_rewrite_cohort_recipe(
                &recipe,
                parts,
                access.take().expect("same original access"),
                engine.kernel.query_execution(),
            )
        }
        Err(error) => Err(error),
        _ => return Err("Rewrite freeze CPU returned a foreign result".into()),
    });
    drop(loan);
    custody.receive_unpaired_after_exit();
    custody.closed = true;
    Ok(())
}
