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

//! Closed table-maintenance command capabilities.

use std::sync::Arc;

use super::{
    MaintenanceActionState, MaintenanceRequestContext, MaintenanceStatementResult,
    RequestScopedMaintenanceEngine, TableMaintenanceService,
};
use novarocks_query_application::protocol_delivery::QuerySessionOutput as StatementResult;

/// Foreground maintenance command capability.  Each invocation creates a
/// short-lived engine carrying only the Frontend-composed maintenance ports
/// and the already-admitted request execution.  It cannot use the legacy
/// Core application façade or manufacture a topology/cancellation fallback.
#[derive(Clone)]
pub struct MaintenanceCommandExecutor {
    kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
    runtime: tokio::runtime::Handle,
}

impl MaintenanceCommandExecutor {
    pub fn new(
        kernel: crate::query_execution::kernels::MaintenanceExecutionKernel,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        Self { kernel, runtime }
    }

    /// Construct the actual original producer, without polling provider I/O.
    /// The caller installs this WHOLE value in its registered Statement Source.
    pub fn prepare_command(
        &self,
        command: &novarocks_sql::semantic::MaintenanceSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<MaintenanceCommandCall, novarocks_table_maintenance::runtime::TerminalError> {
        let lowered = crate::table_maintenance::lower_semantic_maintenance_statement(
            command,
            MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
        )?;
        if !matches!(
            &lowered,
            crate::table_maintenance::ParsedMaintenanceStatement::Execute {
                action: crate::table_maintenance::ParsedMaintenanceAction::RewriteManifests { .. }
                    | crate::table_maintenance::ParsedMaintenanceAction::ExpireSnapshots { .. },
                ..
            }
        ) {
            return Err("command is not owned by the async metadata producer"
                .to_string()
                .into());
        }
        let engine = RequestScopedMaintenanceEngine::new(
            self.kernel.clone(),
            execution.clone(),
            connector_context.clone(),
        );
        let original = MaintenanceActionState::default();
        original.install_engine(engine.clone())?;
        let retained = original.clone();
        let service = Arc::clone(self.kernel.service());
        let catalog = current_catalog.map(str::to_owned);
        let database = current_database.to_owned();
        let spark_call = matches!(
            command,
            novarocks_sql::semantic::MaintenanceSqlCommand::Call { .. }
        );
        let future = Box::pin(async move {
            service
                .handle_typed_statement(
                    &engine,
                    &retained,
                    lowered,
                    spark_call,
                    MaintenanceRequestContext {
                        current_catalog: catalog.as_deref(),
                        current_database: &database,
                    },
                )
                .await
        });
        Ok(MaintenanceCommandCall {
            future: Some(future),
            result: None,
            poll_unproven: false,
            original,
        })
    }

    /// Executes one Query-Application semantic maintenance command.
    ///
    /// The command has already been parser-admitted and lowered, so this
    /// adapter never receives source syntax or performs an AST round trip.
    pub fn execute_non_metadata_command(
        &self,
        command: &novarocks_sql::semantic::MaintenanceSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
        execution: &novarocks_query_application::admitted_query_context::QueryExecutionContext,
        connector_context: &novarocks_spi::connector::ConnectorRequestContext,
    ) -> Result<StatementResult, novarocks_table_maintenance::runtime::TerminalError> {
        let lowered = crate::table_maintenance::lower_semantic_maintenance_statement(
            command,
            MaintenanceRequestContext {
                current_catalog,
                current_database,
            },
        )?;
        if matches!(
            &lowered,
            crate::table_maintenance::ParsedMaintenanceStatement::Execute {
                action: crate::table_maintenance::ParsedMaintenanceAction::RewriteManifests { .. }
                    | crate::table_maintenance::ParsedMaintenanceAction::ExpireSnapshots { .. },
                ..
            }
        ) {
            return Err(
                "metadata command requires its original async statement source"
                    .to_string()
                    .into(),
            );
        }
        let engine = RequestScopedMaintenanceEngine::new(
            self.kernel.clone(),
            execution.clone(),
            connector_context.clone(),
        );
        let spark_call = matches!(
            command,
            novarocks_sql::semantic::MaintenanceSqlCommand::Call { .. }
        );
        let service = self.kernel.service();
        let context = MaintenanceRequestContext {
            current_catalog,
            current_database,
        };
        let state = MaintenanceActionState::default();
        self.runtime
            .block_on(service.handle_typed_statement(&engine, &state, lowered, spark_call, context))
            .map(statement_result)
    }
}

pub(crate) fn is_async_metadata_command(
    command: &novarocks_sql::semantic::MaintenanceSqlCommand,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<bool, novarocks_table_maintenance::runtime::TerminalError> {
    let lowered = crate::table_maintenance::lower_semantic_maintenance_statement(
        command,
        MaintenanceRequestContext {
            current_catalog,
            current_database,
        },
    )?;
    Ok(matches!(
        lowered,
        crate::table_maintenance::ParsedMaintenanceStatement::Execute {
            action: crate::table_maintenance::ParsedMaintenanceAction::RewriteManifests { .. }
                | crate::table_maintenance::ParsedMaintenanceAction::ExpireSnapshots { .. },
            ..
        }
    ))
}

/// Source-owned product future and its independent fixed row. Field order
/// leaves the row alive if completed Product F Drop unwinds. It is not a grant.
pub struct MaintenanceCommandCall {
    future: Option<
        std::pin::Pin<
            Box<
                dyn std::future::Future<
                        Output = Result<
                            MaintenanceStatementResult,
                            novarocks_table_maintenance::runtime::TerminalError,
                        >,
                    > + Send,
            >,
        >,
    >,
    result: Option<
        Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>,
    >,
    poll_unproven: bool,
    original: MaintenanceActionState,
}
impl MaintenanceCommandCall {
    pub fn publication_disposition(
        &self,
    ) -> Option<novarocks_spi::connector::LakePublicationDisposition> {
        self.original.publication_disposition()
    }

    pub fn poll_result(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>,
    > {
        use std::task::Poll;
        assert!(
            !self.poll_unproven,
            "unproven maintenance producer cannot be repolled"
        );
        self.poll_unproven = true;
        match self
            .future
            .as_mut()
            .expect("producer consumed once")
            .as_mut()
            .poll(cx)
        {
            Poll::Pending => {
                self.poll_unproven = false;
                Poll::Pending
            }
            Poll::Ready(result) => {
                self.result = Some(result);
                drop(self.future.take());
                self.poll_unproven = false;
                Poll::Ready(self.result.take().expect("saved before F Drop"))
            }
        }
    }
}

#[derive(Clone)]
pub struct MaintenanceReadCommandExecutor {
    service: Arc<dyn TableMaintenanceService>,
}

impl MaintenanceReadCommandExecutor {
    pub fn new(service: Arc<dyn TableMaintenanceService>) -> Self {
        Self { service }
    }

    /// Executes a semantic `SHOW ALTER TABLE OPTIMIZE` presentation command.
    pub fn execute_command(
        &self,
        statement: &novarocks_sql::semantic::command::ShowOptimizeSqlCommand,
        current_catalog: Option<&str>,
        current_database: &str,
    ) -> Result<StatementResult, String> {
        self.service
            .handle_typed_show_optimize(
                crate::table_maintenance::lower_semantic_show_optimize(statement)?,
                MaintenanceRequestContext {
                    current_catalog,
                    current_database,
                },
            )
            .map(statement_result)
    }
}

fn statement_result(result: MaintenanceStatementResult) -> StatementResult {
    match result {
        MaintenanceStatementResult::Ok => StatementResult::Ok,
        MaintenanceStatementResult::Query(result) => StatementResult::Query(result),
    }
}

#[cfg(test)]
mod tests {
    use super::super::TableMaintenanceEngine;
    use super::*;
    use std::sync::Arc;
    use std::thread;

    struct ReadOnlyService {
        called: std::sync::atomic::AtomicBool,
    }

    #[async_trait::async_trait]
    impl TableMaintenanceService for ReadOnlyService {
        fn start(&self, _engine: Arc<dyn TableMaintenanceEngine>) -> Result<(), String> {
            Ok(())
        }

        fn handle_typed_show_optimize(
            &self,
            _statement: crate::table_maintenance::ParsedShowOptimize,
            _context: MaintenanceRequestContext<'_>,
        ) -> Result<MaintenanceStatementResult, String> {
            self.called.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(MaintenanceStatementResult::Ok)
        }

        async fn shutdown_until(&self, _deadline: std::time::Instant) -> Result<(), String> {
            Ok(())
        }

        fn request_shutdown_for_process_exit(&self) {}
    }

    #[test]
    fn show_optimize_uses_read_only_service_without_engine() {
        let service = Arc::new(ReadOnlyService {
            called: std::sync::atomic::AtomicBool::new(false),
        });
        let executor = MaintenanceReadCommandExecutor::new(Arc::clone(&service) as Arc<_>);
        let statement =
            novarocks_query_application::sql::parse_single_statement("SHOW ALTER TABLE OPTIMIZE")
                .expect("parser statement");
        let Some(novarocks_query_application::sql::ProductSqlCommand::Maintenance(
            novarocks_sql::semantic::MaintenanceSqlCommand::ShowOptimize(command),
        )) = novarocks_query_application::sql::lower_product_sql_command(&statement)
            .expect("lower semantic command")
        else {
            panic!("expected semantic SHOW ALTER TABLE OPTIMIZE");
        };
        let result = executor
            .execute_command(&command, Some("ice"), "db")
            .expect("execute");
        assert!(matches!(result, StatementResult::Ok));
        assert!(service.called.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn injected_process_runtime_drives_work_from_a_plain_worker_thread() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let handle = runtime.handle().clone();
        let result = thread::spawn(move || handle.block_on(async { 17_u8 }))
            .join()
            .expect("plain worker must not require a current Tokio reactor");
        assert_eq!(result, 17);
    }
}

#[cfg(test)]
mod original_producer_tests {
    use super::*;
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicUsize, Ordering},
        task::{Context, Poll},
    };

    struct OriginalGate {
        release: futures::channel::oneshot::Receiver<()>,
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl Future for OriginalGate {
        type Output =
            Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>;
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Pin::new(&mut self.release).poll(cx).map(|result| {
                result.expect("original gate sender");
                Ok(MaintenanceStatementResult::Ok)
            })
        }
    }
    impl Drop for OriginalGate {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[tokio::test]
    async fn cancelled_borrow_keeps_the_same_product_future_and_original_row() {
        let (release, receiver) = futures::channel::oneshot::channel();
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let state = MaintenanceActionState::default();
        let mut original = MaintenanceCommandCall {
            future: Some(Box::pin(OriginalGate {
                release: receiver,
                polls: Arc::clone(&polls),
                drops: Arc::clone(&drops),
            })),
            result: None,
            poll_unproven: false,
            original: state,
        };
        use futures::FutureExt;
        let mut wait = Box::pin(std::future::poll_fn(|cx| original.poll_result(cx)));
        assert!(wait.as_mut().now_or_never().is_none());
        drop(wait);
        assert!(original.future.is_some());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        release.send(()).unwrap();
        assert!(matches!(
            std::future::poll_fn(|cx| original.poll_result(cx)).await,
            Ok(MaintenanceStatementResult::Ok)
        ));
        assert!(original.future.is_none());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(polls.load(Ordering::SeqCst) >= 2);
    }
    struct ReadyThenPanic;
    impl Future for ReadyThenPanic {
        type Output =
            Result<MaintenanceStatementResult, novarocks_table_maintenance::runtime::TerminalError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(Ok(MaintenanceStatementResult::Ok))
        }
    }
    impl Drop for ReadyThenPanic {
        fn drop(&mut self) {
            std::panic::panic_any(73_u32)
        }
    }
    #[test]
    fn completed_product_drop_panic_keeps_ready_and_original_row_outside_future() {
        let mut original = MaintenanceCommandCall {
            future: Some(Box::pin(ReadyThenPanic)),
            result: None,
            poll_unproven: false,
            original: MaintenanceActionState::default(),
        };
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let Err(cause) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original.poll_result(&mut cx)
        })) else {
            panic!("actual completed F Drop must panic")
        };
        assert_eq!(cause.downcast_ref::<u32>(), Some(&73));
        assert!(matches!(
            original.result,
            Some(Ok(MaintenanceStatementResult::Ok))
        ));
        assert!(original.future.is_none() && original.poll_unproven);
        // Same row remains independently reachable after the actual F Drop.
        assert!(original.original.has_engine().is_ok());
    }
}
