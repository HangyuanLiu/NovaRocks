//! Runtime-filter table acceptance for completed Native fragments.

use crate::query_execution::artifact::{
    PreparedDistributedQuery, RuntimeFilterBoundPreparedDistributedQuery,
};
use crate::query_execution::contract::{DistributedQueryError, DistributedQueryErrorKind};

/// Final-plan encoding writes the binding table into every fragment. A missing
/// table means the payload did not come from the completed-plan encoder.
pub fn bind_runtime_filters(
    artifacts: PreparedDistributedQuery,
) -> Result<RuntimeFilterBoundPreparedDistributedQuery, DistributedQueryError> {
    if artifacts.needs_runtime_filter_bindings() {
        return Err(DistributedQueryError::new(
            DistributedQueryErrorKind::ContractViolation,
            "completed Native fragment is missing its runtime-filter binding table",
        ));
    }
    Ok(artifacts.retain_encoded_runtime_filter_bindings())
}
