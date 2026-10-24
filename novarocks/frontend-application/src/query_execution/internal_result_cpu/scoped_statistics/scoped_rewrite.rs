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

//! Rewrite's typed facade over the SAME original serial creator slot.
use super::*;
use novarocks_spi::connector::{ConnectorDistributedRewriteLease, RewriteCpuInput, RewriteCpuPort};

impl InternalResultCpu {
    pub(crate) fn rewrite_cohort_cpu_port(
        &self,
        scope: &WorkScope,
        window: ResultWindowAlias,
        lease: &ConnectorDistributedRewriteLease,
        cancellation: QueryCancellationView,
        runtime: tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> Result<Arc<dyn RewriteCpuPort>, WorkError> {
        if !window.is_for_scope(scope) {
            return Err(WorkError::Conflict);
        }
        scope.check()?;
        self.provider_creator_source(scope)?;
        let source_type = std::any::TypeId::of::<
            crate::query_execution::maintenance::rewrite_preparation::RewriteCohortCpuOriginal,
        >();
        Ok(Arc::new(ScopedProviderCreatorCpu {
            cpu: self.clone(),
            scope: scope.clone(),
            window,
            descriptor: lease.descriptor().clone(),
            incarnation: lease.incarnation(),
            source_type,
            mode: CreatorCpuMode::RewriteProvider,
            cancellation,
            runtime,
            wake,
            state: std::sync::Mutex::new(CreatorPortState {
                job: None,
                busy: false,
            }),
        }))
    }
    pub(crate) fn rewrite_port_metadata_bytes_for_composition() -> Result<NonZeroU64, WorkError> {
        // Same concrete header and original serial slot as Statistics. No
        // second job, forwarder, physical factory, or alias authority exists.
        Self::statistics_port_metadata_bytes_for_composition()
    }
}
impl RewriteCpuPort for ScopedProviderCreatorCpu {
    fn submit_in(&self, input: &mut Option<RewriteCpuInput>) -> Result<(), ConnectorError> {
        let original = input.as_ref().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Rewrite CPU input is empty",
            )
        })?;
        if !matches!(self.mode, CreatorCpuMode::RewriteProvider)
            || original.original_source_type_id() != self.source_type
            || original.descriptor() != &self.descriptor
            || original.incarnation() != self.incarnation
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Rewrite CPU input belongs to a foreign original Provider generation",
            ));
        }
        let mut loan = CreatorPortLoan::take(self)?;
        if loan.job.is_some() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "Rewrite CPU source retains its previous original job",
            ));
        }
        let source = self.cpu.provider_creator_source(&self.scope).map_err(|e| {
            ConnectorError::from_domain(statistics_failure_kind(e.failure_fact()), e)
        })?;
        let window = self.window.try_clone().map_err(|e| {
            ConnectorError::from_domain(statistics_failure_kind(e.failure_fact()), e)
        })?;
        // Every refusal above precedes the move from the actual input slot.
        loan.job = Some(self.cpu.submit_creator_input(
            ClosedCreatorInput::RewriteProvider(input.take().expect("checked Rewrite input")),
            window,
            None,
            Some(source),
            self.cancellation.clone(),
            &self.runtime,
            Arc::clone(&self.wake),
        ));
        Ok(())
    }
    fn poll_finished(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.poll_original(cx, false)
    }
    fn poll_closed(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        self.poll_original(cx, true)
    }
    fn local_failure(&self) -> Option<Arc<dyn std::error::Error + Send + Sync>> {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .job
            .as_ref()
            .and_then(ProviderReadCpuJob::health_failure)
    }
    fn metadata_bytes(&self) -> Result<NonZeroU64, ConnectorError> {
        InternalResultCpu::rewrite_port_metadata_bytes_for_composition()
            .map_err(|e| ConnectorError::from_domain(ConnectorErrorKind::ResourceExhausted, e))
    }
}
