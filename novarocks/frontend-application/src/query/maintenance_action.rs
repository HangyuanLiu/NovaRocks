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

//! Metadata maintenance progresses in its original registered statement.

use super::*;
use crate::query_execution::maintenance::{
    MaintenanceStatementResult, command::MaintenanceCommandCall,
};
use novarocks_query_application::{
    api::LocalResultProducer,
    session_control::{GovernedQueryStatementOwner, GovernedStatementAction, StatementActionValue},
};
use std::task::{Context, Poll};

pub(super) fn prepare(
    executor: &MaintenanceCommandExecutor,
    command: &novarocks_sql::semantic::MaintenanceSqlCommand,
    context: &RequestContext,
    command_context: &CommandContext,
) -> Result<Box<dyn GovernedStatementAction>, CommandError> {
    let capacity = context.execution().result_capacity().ok_or_else(|| {
        CommandError::new(
            CommandErrorKind::Rejected,
            "maintenance has no admitted result capacity",
        )
    })?;
    let origin = capacity.try_window_alias().map_err(|error| {
        CommandError::new(
            CommandErrorKind::Rejected,
            format!("maintenance result custody refused: {error:?}"),
        )
    })?;
    if !origin.is_for_scope(command_context.scope()) {
        return Err(CommandError::new(
            CommandErrorKind::Conflict,
            "maintenance contexts belong to different original scopes",
        ));
    }
    let call = executor
        .prepare_command(
            command,
            context.session().current_catalog(),
            context.session().current_database(),
            context.execution(),
            command_context.connector_context(),
        )
        .map_err(maintenance_command_error)?;
    Ok(Box::new(MetadataMaintenanceAction {
        call,
        origin,
        started: false,
        result: None,
    }))
}

struct MetadataMaintenanceAction {
    call: MaintenanceCommandCall,
    origin: novarocks_workload_control::ResultWindowAlias,
    started: bool,
    result: Option<
        Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>,
    >,
}
impl MetadataMaintenanceAction {
    fn poll_result(
        &mut self,
        owner: &mut GovernedQueryStatementOwner,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        if !self.origin.is_for_scope(owner.scope()) {
            return Poll::Ready(Err(internal_error(
                "maintenance action has a foreign original owner",
            )));
        }
        let cancellation =
            QueryCancellationView::governed(owner.cancellation().clone(), owner.timeout_ms());
        if !self.started {
            if let Some(reason) = cancellation.reason() {
                return Poll::Ready(Err(cancellation_error(reason)));
            }
            self.started = true;
        }
        if self.result.is_none() {
            let execution = match owner.execution_owner_slot().as_ref() {
                Some(execution) => execution,
                None => {
                    return Poll::Ready(Err(internal_error(
                        "metadata source lost its original execution owner",
                    )));
                }
            };
            match self.call.poll_result(execution, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => self.result = Some(result),
            }
        }
        // The source owns the Call and its publication facts independently of
        // its completed Product F and of the UI observer's continued presence.
        let disposition = self.call.publication_disposition();
        let result = self
            .result
            .take()
            .expect("stable original maintenance result");
        let result = match result {
            Err(error) => {
                let routed = product_command_error(maintenance_command_error(error));
                let service = match cancellation.reason() {
                    Some(reason) => selected_execution_or_worker_fallback(
                        Err(routed),
                        cancellation_error(reason),
                    ),
                    None => routed_execution_service_error(routed),
                };
                return Poll::Ready(Err(service));
            }
            Ok(result) => result,
        };
        if let Some(reason) = cancellation.reason() {
            let mut error = cancellation_error(reason);
            if let Some(disposition) = disposition {
                error = error.attach_maintenance_publication_disposition(disposition);
            }
            // Closed maintenance presentation data is destroyed while the
            // same source still holds its original owner and result window.
            drop(result);
            return Poll::Ready(Err(error));
        }
        Poll::Ready(match result {
            MaintenanceStatementResult::Ok => Ok(StatementActionValue::Ok),
            MaintenanceStatementResult::Query(result) => {
                let window = match owner.result_window_alias() {
                    Some(window) => window,
                    None => {
                        return Poll::Ready(Err(internal_error(
                            "maintenance has no original result window",
                        )));
                    }
                };
                let producer = match LocalResultProducer::try_new(owner.scope(), window) {
                    Ok(producer) => producer,
                    Err(error) => return Poll::Ready(Err(internal_error(error))),
                };
                producer
                    .produce(|| Ok(result))
                    .map(StatementActionValue::Local)
                    .map_err(internal_error)
            }
        })
    }
}

impl GovernedStatementAction for MetadataMaintenanceAction {
    fn local_health_failure(
        &self,
    ) -> Option<novarocks_query_application::api::StatementNativeHealthFailure> {
        self.call.local_health_failure()
    }

    fn publication_disposition(
        &self,
    ) -> Option<novarocks_spi::connector::LakePublicationDisposition> {
        self.call.publication_disposition()
    }

    fn poll_action(
        &mut self,
        owner: &mut GovernedQueryStatementOwner,
        cx: &mut Context<'_>,
    ) -> Poll<Result<StatementActionValue, QueryServiceError>> {
        match self.poll_result(owner, cx) {
            Poll::Ready(Err(error)) => {
                Poll::Ready(Err(match self.call.publication_disposition() {
                    Some(disposition) => {
                        error.attach_maintenance_publication_disposition(disposition)
                    }
                    None => error,
                }))
            }
            result => result,
        }
    }
}
