//! Sealed distributed-write dispatch for DML reverse ports.

use crate::common::admitted_query_context::QueryExecutionContext;
use crate::query_execution::contract::{DistributedQueryOutcome, DistributedQueryRequest};
use crate::query_execution::outcome::{QueryExecutionResult, WriteExecutionOutcome};
use crate::query_execution::service::QueryExecutionService;

pub(crate) fn execute_bound_distributed_write_request(
    query_execution: &QueryExecutionService,
    request: DistributedQueryRequest,
) -> Result<QueryExecutionResult, String> {
    query_execution
        .execute(request)
        .and_then(DistributedQueryOutcome::into_write)
        .map(WriteExecutionOutcome::into_execution_result)
        .map_err(|error| error.to_string())
}

pub(crate) fn scan_preparation_options(
    typed_connector_control: &std::sync::Arc<crate::connector::ConnectorControlHost>,
    settings: &novarocks_sql::compiler::SessionOptimizerSettings,
    execution: &QueryExecutionContext,
) -> Result<crate::query_execution::preparation::ScanPreparationOptions, String> {
    let target_parallelism = std::num::NonZeroUsize::new(execution.topology().targets().len())
        .or({
            #[cfg(test)]
            {
                Some(std::num::NonZeroUsize::new(1).expect("one is non-zero"))
            }
            #[cfg(not(test))]
            {
                None
            }
        })
        .ok_or_else(|| {
            "connector split preparation requires a non-empty admitted backend topology".to_string()
        })?;
    Ok(
        crate::query_execution::preparation::ScanPreparationOptions::new(
            settings.connector_static_predicate_pushdown_enabled(),
            target_parallelism,
            None,
        )
        .with_typed_connector_control(
            std::sync::Arc::clone(typed_connector_control),
            crate::query_execution::compiler::typed_connector_session()?,
        ),
    )
}
