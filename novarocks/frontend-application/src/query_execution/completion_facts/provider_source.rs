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

//! The original statement retains its Provider outside every borrowed compiler
//! and close future. This is one fixed source slot, not an execution authority.

use super::*;
use novarocks_query_application::{
    api::StatementNativeHealthFailure,
    preparation::{CompletedPlanWithAccess, FinalPlanCompletionDriver, FinalPlanCompletionFailure},
};
use std::sync::Mutex;

type CompletionResult = Result<
    CompletedPlanWithAccess<FrozenProviderRead>,
    FinalPlanCompletionFailure<FrozenProviderRead>,
>;
type Facts = QueryCompletionFactSource<FrozenProviderRead>;

#[derive(Default)]
struct ProviderCompletionState {
    facts: Option<Arc<Facts>>,
    provider: Option<Arc<FrontendProviderReadFacts>>,
    outcome: Option<CompletionResult>,
    health: Option<StatementNativeHealthFailure>,
    joined: bool,
}

pub(crate) struct FrontendCompletionCustody {
    state: Mutex<ProviderCompletionState>,
}
impl FrontendCompletionCustody {
    pub(crate) fn new() -> Arc<Self> {
        let owner = Arc::new(Self {
            state: Mutex::new(ProviderCompletionState::default()),
        });
        // Materialize the same platform mutex at the source constructor.
        drop(owner.state.lock().unwrap_or_else(|p| p.into_inner()));
        owner
    }
    pub(super) fn install(
        &self,
        facts: Arc<Facts>,
        provider: Arc<FrontendProviderReadFacts>,
    ) -> Result<(), (Arc<Facts>, Arc<FrontendProviderReadFacts>)> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.facts.is_some() || state.provider.is_some() || state.joined {
            return Err((facts, provider));
        }
        state.facts = Some(facts);
        state.provider = Some(provider);
        Ok(())
    }
    /// Borrow the already installed catalog port; no second fact source is created.
    pub(crate) async fn resolve_catalog_relations(
        &self,
        needs: &[novarocks_sql::compiler::CatalogRelationNeed],
    ) -> Result<Vec<novarocks_sql::compiler::CatalogRelationFact>, String> {
        use novarocks_query_application::preparation::SqlCompletionFactSource;
        let facts = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.joined || state.health.is_some() {
                return Err("completion fact source is closed or unhealthy".into());
            }
            state
                .facts
                .clone()
                .ok_or_else(|| "completion fact source is not installed".to_owned())?
        };
        let batch = novarocks_sql::compiler::SqlNeedBatch::CatalogRelations(
            needs.to_vec().into_boxed_slice(),
        );
        let taken = novarocks_query_application::preparation::ReadAccessSink::new();
        match facts.resolve(&batch, &taken).await? {
            novarocks_sql::compiler::SqlFactBatch::CatalogRelations(answer) => {
                Ok(answer.into_vec())
            }
            _ => Err("completion catalog port returned another fact family".into()),
        }
    }

    /// The original statement retains this Provider across the borrowed wait.
    /// A failed or cancelled compiler borrow must still be closed by that owner.
    pub(crate) async fn resolve_provider_reads(
        &self,
        needs: &[novarocks_sql::compiler::ProviderReadNeed],
        taken: &novarocks_query_application::preparation::ReadAccessSink<FrozenProviderRead>,
    ) -> Result<Vec<novarocks_sql::compiler::ProviderReadFact>, String> {
        use novarocks_query_application::preparation::ProviderReadFactPort;
        let provider = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.joined || state.health.is_some() {
                return Err("completion Provider source is closed or unhealthy".into());
            }
            state
                .provider
                .clone()
                .ok_or_else(|| "completion Provider source is not installed".to_owned())?
        };
        provider.resolve_provider_reads(needs, taken).await
    }

    pub(crate) fn local_health_failure(&self) -> Option<StatementNativeHealthFailure> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .health
            .clone()
    }
    /// Complete the same original driver, then join the same Provider source.
    /// No caller cancellation can detach the borrowed close from its holder.
    pub(crate) async fn complete(
        &self,
        request: novarocks_sql::compiler::SqlFinalPlanCompileRequest,
        scope: &novarocks_workload_control::WorkScope,
    ) -> Result<CompletionResult, StatementNativeHealthFailure> {
        let facts = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            Arc::clone(state.facts.as_ref().expect("installed original facts"))
        };
        let driver = FinalPlanCompletionDriver::new(facts);
        let outcome = driver.complete(request, scope).await;
        {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            assert!(state.outcome.is_none() && !state.joined);
            state.outcome = Some(outcome);
        }
        // The canonical facts/Provider remain in State, so this cannot be a
        // last alias of the source. The result is stable before any cleanup.
        drop(driver);
        self.close().await?;
        let outcome = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .outcome
            .take();
        Ok(outcome.expect("same completed original result"))
    }
    pub(crate) async fn close(&self) -> Result<(), StatementNativeHealthFailure> {
        let provider = {
            let state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(failure) = &state.health {
                return Err(failure.clone());
            }
            if state.joined {
                return Ok(());
            }
            state.provider.clone()
        };
        // No Provider can issue a job before install. Logical-only EXPLAIN
        // does not construct this family; this is its exact unused branch.
        let Some(provider) = provider else {
            return Ok(());
        };
        if let Err(cause) = provider.join_local_closed().await {
            let failure = StatementNativeHealthFailure::actual_exit_unproven(
                "completion Provider creator actual exit is unproven",
                cause,
            );
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            assert!(state.health.is_none());
            state.health = Some(failure.clone());
            return Err(failure);
        }
        self.state.lock().unwrap_or_else(|p| p.into_inner()).joined = true;
        Ok(())
    }
    pub(crate) fn known_layouts() -> Result<
        [std::alloc::Layout; 10],
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    > {
        use novarocks_type_contract::owned_resources::layout::arc_layout;
        let provider = FrontendProviderReadFacts::known_source_layouts()?;
        // Same pinned target model as the original QA Source author. The
        // mutex pointee is additional to the inline Mutex stored in Self.
        let mutex_backing = if cfg!(all(target_os = "macos", target_pointer_width = "64")) {
            std::alloc::Layout::new::<(isize, [u8; 56])>()
        } else if cfg!(target_os = "linux") {
            std::alloc::Layout::new::<()>()
        } else {
            return Err(
                novarocks_type_contract::owned_resources::layout::LayoutResourceError::SourceModel,
            );
        };
        Ok([
            arc_layout(std::alloc::Layout::new::<Self>())?,
            arc_layout(std::alloc::Layout::new::<Facts>())?,
            provider[0],
            provider[1],
            provider[2],
            provider[3],
            arc_layout(std::alloc::Layout::new::<FrontendCatalogFacts>())?,
            arc_layout(std::alloc::Layout::new::<FrontendStatisticsFacts>())?,
            arc_layout(std::alloc::Layout::new::<FrontendMaterializedViewFacts>())?,
            mutex_backing,
        ])
    }
    #[cfg(test)]
    pub(crate) fn install_provider_for_test(&self, provider: Arc<FrontendProviderReadFacts>) {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        assert!(state.provider.is_none() && !state.joined);
        state.provider = Some(provider);
    }
}
