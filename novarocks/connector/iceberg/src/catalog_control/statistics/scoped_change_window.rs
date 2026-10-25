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

//! One exact change-window source. CPU handles only the closed pure phases;
//! the borrowed async driver performs the original catalog and raw FileIO reads.
use super::*;
use crate::change_planning::ChangeWindowPlan;
use novarocks_spi::connector::{
    ConnectorBeginScanRequest, ConnectorOriginalResultScope, ConnectorRequestContext,
    ConnectorScan, ConnectorTableHandle, ProviderMetadataCpuPort,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

enum Phase {
    Prepare,
    Inspect,
    Finish,
}
type TableLoad = Pin<
    Box<
        dyn Future<Output = Result<crate::loaded_table::IcebergPhysicalTable, ConnectorError>>
            + Send,
    >,
>;
pub(super) struct ChangeWindowBody {
    provider: IcebergMetadata,
    table: ConnectorTableHandle,
    request: ConnectorBeginScanRequest,
    phase: Phase,
    prepared: Option<crate::metadata::PreparedChangeWindowScan>,
    loader: Option<TableLoad>,
    loaded: Option<Result<crate::loaded_table::IcebergPhysicalTable, ConnectorError>>,
    load_unproven: bool,
    load_exited: bool,
    metadata_uuid: Option<String>,
    plan: Option<ChangeWindowPlan>,
    snapshots: [Option<Arc<IcebergMetadataCpuOriginal>>; 2],
    endpoints: [Option<super::scoped_snapshot::ScopedReadSnapshot>; 2],
    result: Option<Result<ConnectorScan, ConnectorError>>,
    // The original scope outlives every input, SDK handle and returned endpoint.
    original: ConnectorOriginalResultScope,
}
impl ChangeWindowBody {
    pub(super) fn run(&mut self) {
        let outcome = self.run_phase();
        if let Err(error) = outcome {
            self.result = Some(Err(error));
        }
    }
    fn run_phase(&mut self) -> Result<(), ConnectorError> {
        crate::read_snapshot::cow_capture::CaptureScope::new(&self.original)
            .map_err(super::scoped_read::checked_read)?;
        match self.phase {
            Phase::Prepare => {
                self.prepared = Some(
                    self.provider
                        .prepare_change_window_scan(&self.table, &self.request)?,
                );
            }
            Phase::Inspect => {
                let physical = self
                    .loaded
                    .as_ref()
                    .expect("same loaded table result")
                    .as_ref()
                    .map_err(Clone::clone)?;
                let prepared = self.prepared.as_ref().expect("same prepared scan");
                let metadata_uuid = self
                    .provider
                    .validate_change_window_physical(prepared, &physical.table)?;
                let plan = crate::change_planning::prepare_change_window(
                    physical.table.metadata(),
                    prepared.window.from_exclusive(),
                    prepared.window.to_inclusive(),
                    &self.request.context,
                )?;
                if let ChangeWindowPlan::Resolved(admission) = plan {
                    self.result = Some(self.provider.finish_change_window_scan(
                        prepared,
                        &self.request,
                        admission,
                        metadata_uuid,
                    ));
                } else {
                    self.metadata_uuid = Some(metadata_uuid);
                    self.plan = Some(plan);
                }
            }
            Phase::Finish => {
                let physical = self
                    .loaded
                    .as_ref()
                    .unwrap()
                    .as_ref()
                    .map_err(Clone::clone)?;
                let admission = crate::change_planning::finish_change_window(
                    physical.table.metadata(),
                    &self.endpoints[0]
                        .as_ref()
                        .expect("original From endpoint")
                        .files,
                    &self.endpoints[1]
                        .as_ref()
                        .expect("original To endpoint")
                        .files,
                    &self.request.context,
                )?;
                self.result = Some(self.provider.finish_change_window_scan(
                    self.prepared.as_ref().unwrap(),
                    &self.request,
                    admission,
                    self.metadata_uuid.as_ref().unwrap().clone(),
                ));
            }
        }
        Ok(())
    }
}
async fn load_original_table(
    runtime: Arc<crate::metadata_context::IcebergMetadataContext>,
    namespace: String,
    table: String,
    context: ConnectorRequestContext,
) -> Result<crate::loaded_table::IcebergPhysicalTable, ConnectorError> {
    runtime
        .load_table_original_for_request_async(&namespace, &table, &context)
        .await
}
impl IcebergMetadataCpuOriginal {
    pub(crate) fn change_window_source(
        provider: IcebergMetadata,
        table: ConnectorTableHandle,
        request: ConnectorBeginScanRequest,
        original: ConnectorOriginalResultScope,
    ) -> Arc<Self> {
        Self::new(
            provider.descriptor().clone(),
            provider.incarnation(),
            CpuBody::ChangeWindow(ChangeWindowBody {
                provider,
                table,
                request,
                phase: Phase::Prepare,
                prepared: None,
                loader: None,
                loaded: None,
                load_unproven: false,
                load_exited: false,
                metadata_uuid: None,
                plan: None,
                snapshots: [None, None],
                endpoints: [None, None],
                result: None,
                original,
            }),
        )
    }
    fn poll_change_window_table(&self, cx: &mut Context<'_>) -> Poll<Result<(), ConnectorError>> {
        let mut loan = BodyLoan::take(self);
        let CpuBody::ChangeWindow(body) = &mut loan.body.as_mut().unwrap().body else {
            return Poll::Ready(Err(invalid("foreign change-window table source")));
        };
        if body.load_unproven {
            return Poll::Ready(Err(internal(
                "original change-window table poll or Drop is unproven",
            )));
        }
        if body.load_exited {
            return Poll::Ready(Ok(()));
        }
        body.load_unproven = true;
        let returned = match body
            .loader
            .as_mut()
            .expect("same original table F")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => {
                body.load_unproven = false;
                return Poll::Pending;
            }
            Poll::Ready(returned) => returned,
        };
        // Receive first, foreign destructor second. Panic leaves the raw result
        // and poison in this same body; neither F nor its inputs are repolled.
        body.loaded = Some(returned);
        drop(body.loader.take());
        body.load_exited = true;
        body.load_unproven = false;
        Poll::Ready(Ok(()))
    }
    pub(crate) async fn drive_change_window(
        self: &Arc<Self>,
        cpu: &Arc<dyn ProviderMetadataCpuPort>,
    ) -> Result<(), ConnectorError> {
        super::run_provider_metadata(cpu, self).await?;
        {
            let mut loan = BodyLoan::take(self);
            let CpuBody::ChangeWindow(body) = &mut loan.body.as_mut().unwrap().body else {
                return Err(invalid("foreign change-window source"));
            };
            if body.result.is_some() {
                return Ok(());
            }
            let prepared = body.prepared.as_ref().expect("original scan preparation");
            body.loader = Some(Box::pin(load_original_table(
                body.provider.runtime().clone(),
                prepared.table.namespace.clone(),
                prepared.table.table.clone(),
                body.request.context.clone(),
            )));
        }
        std::future::poll_fn(|cx| self.poll_change_window_table(cx)).await?;
        {
            let mut loan = BodyLoan::take(self);
            let original = loan.body.as_mut().unwrap();
            let CpuBody::ChangeWindow(body) = &mut original.body else {
                return Err(invalid("foreign change-window source"));
            };
            body.phase = Phase::Inspect;
            original.started = false;
        }
        super::run_provider_metadata(cpu, self).await?;
        {
            let mut loan = BodyLoan::take(self);
            let CpuBody::ChangeWindow(body) = &mut loan.body.as_mut().unwrap().body else {
                return Err(invalid("foreign change-window source"));
            };
            if body.result.is_some() {
                return Ok(());
            }
            let ChangeWindowPlan::Endpoints { schema } =
                body.plan.as_ref().expect("same endpoint plan")
            else {
                return Err(internal("change-window source has no endpoint plan"));
            };
            let prepared = body.prepared.as_ref().unwrap();
            let physical = body
                .loaded
                .as_ref()
                .unwrap()
                .as_ref()
                .map_err(Clone::clone)?;
            for (index, snapshot) in [
                prepared.window.from_exclusive(),
                prepared.window.to_inclusive(),
            ]
            .into_iter()
            .enumerate()
            {
                body.snapshots[index] = Some(Self::read_snapshot_source(
                    body.provider.clone(),
                    physical.table.clone(),
                    snapshot,
                    body.request.context.clone(),
                    body.original.clone(),
                    super::scoped_snapshot::SnapshotReadKind::ChangeWindow {
                        schema: schema.clone(),
                    },
                ));
            }
        }
        for index in 0..2 {
            let snapshot = {
                let mut loan = BodyLoan::take(self);
                let CpuBody::ChangeWindow(body) = &mut loan.body.as_mut().unwrap().body else {
                    return Err(invalid("foreign change-window source"));
                };
                body.snapshots[index].as_ref().unwrap().clone()
            };
            super::scoped_snapshot::drive_read_snapshot(&snapshot, cpu).await?;
            let mut loan = BodyLoan::take(self);
            let CpuBody::ChangeWindow(body) = &mut loan.body.as_mut().unwrap().body else {
                return Err(invalid("foreign change-window source"));
            };
            let mut returned = None;
            snapshot.take_read_snapshot_in(&mut returned)?;
            body.endpoints[index] = Some(returned.expect("original endpoint receiver")?);
        }
        {
            let mut loan = BodyLoan::take(self);
            let original = loan.body.as_mut().unwrap();
            let CpuBody::ChangeWindow(body) = &mut original.body else {
                return Err(invalid("foreign change-window source"));
            };
            body.phase = Phase::Finish;
            original.started = false;
        }
        super::run_provider_metadata(cpu, self).await
    }
    #[cfg(test)]
    pub(crate) fn change_window_endpoint_facts_for_test(
        &self,
    ) -> [Vec<crate::read_model::IcebergReadFile>; 2] {
        let mut loan = BodyLoan::take(self);
        let CpuBody::ChangeWindow(body) = &mut loan.body.as_mut().unwrap().body else {
            panic!("same original change-window source");
        };
        std::array::from_fn(|index| {
            body.endpoints[index]
                .as_ref()
                .expect("actual endpoint returned")
                .files
                .clone()
        })
    }
    pub(crate) fn take_change_window_result(&self) -> Result<ConnectorScan, ConnectorError> {
        let mut loan = BodyLoan::take(self);
        let CpuBody::ChangeWindow(body) = &mut loan.body.as_mut().unwrap().body else {
            return Err(invalid("foreign change-window result source"));
        };
        body.result
            .take()
            .ok_or_else(|| invalid("change-window original result is not ready"))?
    }
}
