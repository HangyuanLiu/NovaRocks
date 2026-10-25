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

//! Closed MV turns on the existing creator queue and actual-exit packet.
use super::*;

pub(crate) enum MvRefreshCpuTurn {
    Preparation(Arc<crate::query_execution::mv_assembly::refresh_preparation::async_original::MvRefreshPreparationOriginal>),
    Activation(Arc<crate::query_execution::mv_assembly::activation_cpu::MvWriteActivationOriginal>),
}
impl MvRefreshCpuTurn {
    pub(super) fn run_once(&self) {
        match self {
            Self::Preparation(original) => original.run_once(),
            Self::Activation(original) => original.run_once(),
        }
    }
}

impl InternalResultCpu {
    pub(crate) fn submit_mv_refresh_turn(
        &self,
        turn: MvRefreshCpuTurn,
        window: ResultWindowAlias,
        source: InternalCpuActionSource,
        cancellation: QueryCancellationView,
        runtime: &tokio::runtime::Handle,
        wake: Arc<dyn StatusIntakeWake>,
    ) -> MvRefreshCpuJob {
        MvRefreshCpuJob {
            original: self.submit_creator_input(
                ClosedCreatorInput::MvRefresh(turn),
                window,
                None,
                Some(source),
                cancellation,
                runtime,
                wake,
            ),
        }
    }
}

/// The result graph remains in the same original Source. This facade receives
/// only the existing CPU marker and joins its physical worker and forwarder.
pub(crate) struct MvRefreshCpuJob {
    original: ProviderReadCpuJob,
}
impl MvRefreshCpuJob {
    pub(crate) fn poll_finish_in(
        &mut self,
        cx: &mut Context<'_>,
        destination: &mut Option<Result<(), ExecutionFailureInput>>,
    ) -> Poll<Result<(), Arc<dyn std::error::Error + Send + Sync>>> {
        let original = &mut self.original;
        let job = original.job.as_mut().expect("same original MV CPU job");
        if original.marker.is_none() {
            original.marker = Some(std::task::ready!(job.poll_take(cx)));
        }
        std::task::ready!(job.poll_forwarder_closed(cx))?;
        if destination.is_none() {
            match original.marker.as_ref() {
                Some(Ok(value))
                    if matches!(
                        value.value(),
                        Ok(InternalDomainSlot::ProviderCreatorFinished)
                    ) =>
                {
                    *destination = Some(Ok(()));
                }
                Some(Err(_)) => {
                    let Some(Err(error)) = original.marker.take() else {
                        unreachable!()
                    };
                    *destination = Some(Err(error));
                }
                _ => panic!("closed MV creator returned a foreign marker"),
            }
        }
        // The Source received its phase result before this alias can retire.
        original.retire_sources()?;
        Poll::Ready(Ok(()))
    }

    pub(crate) fn poll_closed(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), Arc<dyn std::error::Error + Send + Sync>>> {
        let original = &mut self.original;
        let job = original.job.as_mut().expect("same original MV CPU job");
        if !job.closing {
            job.closing = true;
            if let Err(cause) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.completion.close()))
            {
                job.retirement
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .provider_close_failure = Some(QueryBlockingFailure::panicked(cause));
            }
        }
        std::task::ready!(job.poll_forwarder_closed(cx))?;
        Poll::Ready(original.retire_sources())
    }

    pub(crate) async fn close(&mut self) -> Result<(), Arc<dyn std::error::Error + Send + Sync>> {
        std::future::poll_fn(|cx| self.poll_closed(cx)).await
    }

    pub(crate) fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.original.health_failure().map(|cause| {
            novarocks_query_application::api::StatementNativeHealthFailure::actual_exit_unproven(
                "original MV CPU exit is unproven",
                cause,
            )
        })
    }
}
