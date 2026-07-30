use std::sync::Arc;

use novarocks::connector::schema::{SchemaLoadProvider, SchemaRow, SchemaScanContext, SchemaTable};
use novarocks::runtime::endpoint::RuntimeEndpoint;

pub(crate) fn schema_load_provider() -> Arc<dyn SchemaLoadProvider> {
    Arc::new(CompatSchemaLoadProvider)
}

struct CompatSchemaLoadProvider;

impl SchemaLoadProvider for CompatSchemaLoadProvider {
    fn fetch_load_rows(
        &self,
        context: &SchemaScanContext,
        endpoint: Option<&RuntimeEndpoint>,
    ) -> Result<Vec<SchemaRow>, String> {
        crate::schema_loads::fetch_rows(
            context,
            crate::schema_frontend::transport_address(endpoint).as_ref(),
        )
    }

    fn fetch_tracking_load_log_rows(
        &self,
        context: &SchemaScanContext,
        endpoint: Option<&RuntimeEndpoint>,
    ) -> Result<Vec<SchemaRow>, String> {
        crate::schema_tracking_logs::fetch_rows(
            context,
            crate::schema_frontend::transport_address(endpoint).as_ref(),
        )
    }

    fn fetch_fe_table_rows(
        &self,
        table: &SchemaTable,
        context: &SchemaScanContext,
        endpoint: Option<&RuntimeEndpoint>,
    ) -> Result<Vec<SchemaRow>, String> {
        crate::schema_fe_tables::fetch_rows(
            table,
            context,
            crate::schema_frontend::transport_address(endpoint).as_ref(),
        )
    }
}
