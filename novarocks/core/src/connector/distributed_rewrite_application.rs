// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.  The ASF
// licenses this file to you under the Apache License, Version 2.0.

//! Provider-neutral application session for a frozen distributed rewrite.
//!
//! This layer owns exact-generation acquisition, strict table loading, and
//! C1 operation sealing.  Provider-specific source execution is deliberately
//! injected by the concrete engine implementation after this session exists.

use novarocks_spi::connector::{
    ConnectorDistributedRewriteOperation, ConnectorDistributedRewritePlanningRequest,
    ConnectorDistributedRewriteResolver, ConnectorInstanceId, ConnectorRequestContext,
    ConnectorTableIdentity, ConnectorTableRequest, ConnectorTableResolution,
    ConnectorWriteOperationId,
};
use sha2::{Digest, Sha256};

use crate::query_execution::distributed_rewrite::ConnectorDistributedRewriteSession;
use crate::query_execution::request_context::QueryExecutionContext;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DistributedRewriteIntent {
    DataFiles {
        rewrite_all: bool,
    },
    PositionDeletes {
        rewrite_all: bool,
        min_input_files: Option<u32>,
    },
}

pub struct DistributedRewriteMaintenanceSession {
    session: ConnectorDistributedRewriteSession,
    context: ConnectorRequestContext,
    execution: QueryExecutionContext,
}

impl DistributedRewriteMaintenanceSession {
    pub fn plan(&self) -> &novarocks_spi::connector::ConnectorDistributedRewritePlan {
        self.session.plan()
    }

    pub fn is_noop(&self) -> bool {
        self.session.is_noop()
    }

    /// Stable durable digest for the sealed cohort membership.  It contains
    /// no provider payload or source path; those remain provider-private in
    /// the plan artifact.
    pub fn cohort_set_digest(&self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"novarocks.distributed-rewrite.cohort-set.v1\0");
        hash.update((self.session.plan().cohorts().len() as u64).to_be_bytes());
        for cohort in self.session.plan().cohorts() {
            hash.update(cohort.cohort_id().to_bytes());
            hash.update(cohort.group_digest());
        }
        hash.finalize().into()
    }

    pub(crate) fn session(&self) -> &ConnectorDistributedRewriteSession {
        &self.session
    }

    pub(crate) fn context(&self) -> &ConnectorRequestContext {
        &self.context
    }

    pub(crate) fn execution(&self) -> &QueryExecutionContext {
        &self.execution
    }
}

/// Plan exactly once.  The caller captures topology before this function and
/// retains the returned session through every staged cohort and terminal C1
/// commit.  No current-generation lookup is available after this point.
pub fn plan_distributed_rewrite_session(
    query_execution: &crate::query_execution::service::QueryExecutionService,
    resolver: &dyn ConnectorDistributedRewriteResolver,
    instance_id: &ConnectorInstanceId,
    table: ConnectorTableIdentity,
    operation_id: ConnectorWriteOperationId,
    intent: DistributedRewriteIntent,
    execution: QueryExecutionContext,
    context: ConnectorRequestContext,
) -> Result<DistributedRewriteMaintenanceSession, String> {
    if table.instance_id != *instance_id {
        return Err(
            "distributed rewrite table does not belong to requested connector instance".to_string(),
        );
    }
    let lease = resolver
        .acquire_current_distributed_rewrite(instance_id)
        .map_err(|error| format!("acquire distributed rewrite exact lease: {error}"))?;
    let metadata = lease
        .metadata()
        .load_table(ConnectorTableRequest {
            table: table.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context.clone(),
        })
        .map_err(|error| format!("load distributed rewrite target metadata: {error}"))?;
    if metadata.identity != table || metadata.table.owner() != &lease.binding_key().instance_id {
        return Err("distributed rewrite metadata returned a foreign table handle".to_string());
    }
    let operation = match intent {
        DistributedRewriteIntent::DataFiles { rewrite_all } => {
            ConnectorDistributedRewriteOperation::RewriteDataFiles {
                table: metadata.table,
                rewrite_all,
            }
        }
        DistributedRewriteIntent::PositionDeletes {
            rewrite_all,
            min_input_files,
        } => ConnectorDistributedRewriteOperation::RewritePositionDeletes {
            table: metadata.table,
            rewrite_all,
            min_input_files,
        },
    };
    let request = ConnectorDistributedRewritePlanningRequest::try_new(
        operation_id,
        lease.binding_key().clone(),
        operation,
        context.clone(),
    )
    .map_err(|error| format!("build distributed rewrite request: {error}"))?;
    let plan = lease
        .plan_rewrite(request)
        .map_err(|error| format!("plan distributed rewrite: {error}"))?;
    let session = query_execution
        .begin_distributed_rewrite_operation_with_lease(plan, lease, context.clone())
        .map_err(|error| error.to_string())?;
    Ok(DistributedRewriteMaintenanceSession {
        session,
        context,
        execution,
    })
}
