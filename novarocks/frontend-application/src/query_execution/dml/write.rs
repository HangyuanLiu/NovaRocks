//! Sealed distributed-write dispatch for DML reverse ports.

use crate::query_execution::contract::DistributedQueryRequest;
use crate::query_execution::outcome::{
    DistributedQueryOutcome, QueryExecutionResult, WriteExecutionOutcome,
};
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
