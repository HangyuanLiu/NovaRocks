use crate::connector::ConnectorRegistry;
use crate::engine::mv::refresh_context::IcebergMvRefreshContext;
use crate::sql::catalog::CatalogProvider;
use crate::sql::codegen::iceberg_change_stream_write::IcebergChangeStreamWriteDagSpec;
use crate::sql::codegen::iceberg_write_sink::IcebergWriteSinkSpec;
use crate::sql::planner::DistributedPlan;

pub(crate) struct FragmentBuildRequest<'a> {
    pub distributed_plan: &'a DistributedPlan,
    pub catalog: &'a dyn CatalogProvider,
    pub connectors: &'a ConnectorRegistry,
    pub mv_refresh_ctx: Option<&'a IcebergMvRefreshContext>,
    pub output: FragmentBuildOutput<'a>,
}

pub(crate) enum FragmentBuildOutput<'a> {
    Result,
    IcebergWrite {
        current_database: &'a str,
        sink_spec: &'a IcebergWriteSinkSpec,
    },
    ChangeStreamWrite {
        current_database: &'a str,
        dag: &'a mut IcebergChangeStreamWriteDagSpec,
    },
}

impl<'a> FragmentBuildRequest<'a> {
    pub(crate) fn result(
        distributed_plan: &'a DistributedPlan,
        catalog: &'a dyn CatalogProvider,
        connectors: &'a ConnectorRegistry,
        mv_refresh_ctx: Option<&'a IcebergMvRefreshContext>,
    ) -> Self {
        Self {
            distributed_plan,
            catalog,
            connectors,
            mv_refresh_ctx,
            output: FragmentBuildOutput::Result,
        }
    }
}
