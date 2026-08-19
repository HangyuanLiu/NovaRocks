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

use std::collections::HashMap;

use novarocks_catalog::identifier::normalize_identifier;
use novarocks_spi::connector::{
    ConnectorColumnDefinition, ConnectorViewDefinition, ConnectorViewSourceFormat,
};

use crate::catalog_config::IcebergCatalogKind;
use crate::control_runtime::IcebergControlRuntime;
use crate::iceberg::spec::{
    Schema, SqlViewRepresentation, ViewMetadata, ViewRepresentation, ViewRepresentations,
    ViewVersion,
};
use crate::iceberg::{
    Catalog, NamespaceIdent, TableIdent, ViewCommit, ViewCreation, ViewRequirement,
};

pub(crate) const VIEW_DIALECT_STARROCKS: &str = "starrocks";
const NOVAROCKS_ENGINE_NAME: &str = "novarocks";
const NOVAROCKS_SOURCE_FORMAT_KEY: &str = "novarocks.source-format";
const EFFECTIVE_USER_SOURCE_V1: &str = "effective-user-source-v1";

#[derive(Clone, Debug)]
pub(crate) struct LoadedIcebergView {
    pub raw_sql: String,
    pub dialect: String,
    pub default_catalog: Option<String>,
    pub default_namespace: String,
    pub source_format: Option<ConnectorViewSourceFormat>,
    pub column_names: Vec<String>,
    pub comment: Option<String>,
    pub properties: HashMap<String, String>,
}

fn ensure_rest(runtime: &IcebergControlRuntime) -> Result<(), String> {
    let kind = runtime.control_state().configuration().kind;
    if kind != IcebergCatalogKind::Rest {
        return Err(format!(
            "view operations require a REST iceberg catalog; this catalog is {kind:?}"
        ));
    }
    Ok(())
}

fn view_ident(namespace: &str, view: &str) -> Result<(NamespaceIdent, TableIdent), String> {
    let namespace = normalize_identifier(namespace)?;
    let view = normalize_identifier(view)?;
    let ident = TableIdent::from_strs([namespace.as_str(), view.as_str()])
        .map_err(|error| format!("build view identity: {error}"))?;
    Ok((NamespaceIdent::new(namespace), ident))
}

fn build_view_schema(columns: &[ConnectorColumnDefinition]) -> Result<Schema, String> {
    Schema::builder()
        .with_fields(super::type_mapping::schema_fields(columns)?)
        .build()
        .map_err(|error| format!("build Iceberg view schema: {error}"))
}

fn novarocks_source_context(
    definition: &ConnectorViewDefinition,
) -> Result<(String, NamespaceIdent, &'static str), String> {
    if definition.dialect != ConnectorViewDialect::StarRocks {
        return Err("unsupported SQL dialect for NovaRocks Iceberg view creation".to_string());
    }
    if definition.raw_sql.is_empty() {
        return Err("NovaRocks Iceberg view source SQL is empty".to_string());
    }
    let source_catalog = definition
        .default_catalog
        .as_ref()
        .ok_or_else(|| "NovaRocks Iceberg view source is missing default catalog".to_string())?;
    let source_catalog = normalize_identifier(source_catalog)
        .map_err(|error| format!("normalize NovaRocks view source catalog: {error}"))?;
    let source_namespace = normalize_identifier(&definition.default_namespace)
        .map_err(|error| format!("normalize NovaRocks view source namespace: {error}"))?;
    let source_format = match definition.source_format {
        Some(ConnectorViewSourceFormat::EffectiveUserSourceV1) => EFFECTIVE_USER_SOURCE_V1,
        None => {
            return Err(
                "NovaRocks Iceberg view creation requires effective-user-source-v1 provenance"
                    .to_string(),
            );
        }
    };
    Ok((
        source_catalog,
        NamespaceIdent::new(source_namespace),
        source_format,
    ))
}

fn source_format_from_summary(
    ident: &TableIdent,
    summary: &HashMap<String, String>,
) -> Result<Option<ConnectorViewSourceFormat>, String> {
    if summary.get("engine-name").map(String::as_str) != Some(NOVAROCKS_ENGINE_NAME) {
        return Ok(None);
    }
    match summary.get(NOVAROCKS_SOURCE_FORMAT_KEY).map(String::as_str) {
        Some(EFFECTIVE_USER_SOURCE_V1) => {
            Ok(Some(ConnectorViewSourceFormat::EffectiveUserSourceV1))
        }
        None => Err(format!(
            "corrupt NovaRocks Iceberg view {ident}: missing {NOVAROCKS_SOURCE_FORMAT_KEY}"
        )),
        Some(value) => Err(format!(
            "unsupported NovaRocks Iceberg view {ident}: unknown {NOVAROCKS_SOURCE_FORMAT_KEY} `{value}`"
        )),
    }
}

pub(crate) fn create_view(
    runtime: &IcebergControlRuntime,
    namespace: &str,
    view: &str,
    columns: &[ConnectorColumnDefinition],
    definition: &ConnectorViewDefinition,
    comment: Option<&str>,
    replace: bool,
    extra_properties: &[(String, String)],
) -> Result<(), String> {
    ensure_rest(runtime)?;
    let (source_catalog, source_namespace, source_format) = novarocks_source_context(definition)?;
    let (namespace_ident, ident) = view_ident(namespace, view)?;
    let schema = build_view_schema(columns)?;
    let representations =
        ViewRepresentations::new(vec![ViewRepresentation::Sql(SqlViewRepresentation {
            sql: definition.raw_sql.to_string(),
            dialect: VIEW_DIALECT_STARROCKS.to_string(),
        })]);
    let mut properties = extra_properties.iter().cloned().collect::<HashMap<_, _>>();
    if let Some(comment) = comment {
        if properties
            .insert("comment".to_string(), comment.to_string())
            .is_some()
        {
            return Err("duplicate Iceberg view comment property".to_string());
        }
    }
    let summary = HashMap::from([
        ("engine-name".to_string(), NOVAROCKS_ENGINE_NAME.to_string()),
        (
            NOVAROCKS_SOURCE_FORMAT_KEY.to_string(),
            source_format.to_string(),
        ),
    ]);
    let catalog = runtime.catalog().clone();

    if replace {
        let current = runtime
            .resources()
            .catalog_runtime()
            .block_on({
                let catalog = catalog.clone();
                let ident = ident.clone();
                async move { catalog.load_view(&ident).await }
            })?
            .map_err(|error| format!("load Iceberg view {ident}: {error}"))?;
        return replace_view(
            runtime,
            catalog,
            &ident,
            current,
            schema,
            representations,
            properties,
            summary,
            source_catalog,
            source_namespace,
        );
    }

    let creation = ViewCreation::builder()
        .name(ident.name.clone())
        .location(None)
        .representations(representations)
        .schema(schema)
        .properties(properties)
        .default_namespace(source_namespace)
        .default_catalog(Some(source_catalog))
        .summary(summary)
        .build();
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.create_view(&namespace_ident, creation).await })?
        .map_err(|error| format!("create Iceberg view {ident}: {error}"))?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn replace_view(
    runtime: &IcebergControlRuntime,
    catalog: std::sync::Arc<dyn Catalog>,
    ident: &TableIdent,
    current: ViewMetadata,
    schema: Schema,
    representations: ViewRepresentations,
    properties: HashMap<String, String>,
    summary: HashMap<String, String>,
    source_catalog: String,
    source_namespace: NamespaceIdent,
) -> Result<(), String> {
    let uuid = current.uuid();
    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("system clock before epoch: {error}"))?
        .as_millis() as i64;
    let new_version = ViewVersion::builder()
        .with_version_id(1)
        .with_schema_id(schema.schema_id())
        .with_timestamp_ms(timestamp_ms)
        .with_summary(summary)
        .with_representations(representations)
        .with_default_catalog(Some(source_catalog))
        .with_default_namespace(source_namespace)
        .build();
    let mut builder = current.into_builder();
    if !properties.is_empty() {
        builder = builder
            .set_properties(properties)
            .map_err(|error| format!("set replaced view properties: {error}"))?;
    }
    let result = builder
        .set_current_version(new_version, schema)
        .map_err(|error| format!("stage replaced view version: {error}"))?
        .build()
        .map_err(|error| format!("build replaced view metadata: {error}"))?;
    let commit = ViewCommit::builder()
        .ident(ident.clone())
        .requirements(vec![ViewRequirement::UuidMatch { uuid }])
        .updates(result.changes)
        .build();
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.update_view(commit).await })?
        .map_err(|error| format!("replace Iceberg view {ident}: {error}"))?;
    Ok(())
}

pub(crate) fn load_view(
    runtime: &IcebergControlRuntime,
    namespace: &str,
    view: &str,
) -> Result<LoadedIcebergView, String> {
    ensure_rest(runtime)?;
    let (_, ident) = view_ident(namespace, view)?;
    let catalog = runtime.catalog().clone();
    let metadata = runtime
        .resources()
        .catalog_runtime()
        .block_on({
            let ident = ident.clone();
            async move { catalog.load_view(&ident).await }
        })?
        .map_err(|error| map_view_error(&ident, "load", error))?;
    loaded_view_from_metadata(&ident, &metadata)
}

pub(crate) fn drop_view(
    runtime: &IcebergControlRuntime,
    namespace: &str,
    view: &str,
) -> Result<(), String> {
    ensure_rest(runtime)?;
    let (_, ident) = view_ident(namespace, view)?;
    let catalog = runtime.catalog().clone();
    runtime
        .resources()
        .catalog_runtime()
        .block_on({
            let ident = ident.clone();
            async move { catalog.drop_view(&ident).await }
        })?
        .map_err(|error| map_view_error(&ident, "drop", error))
}

pub(crate) fn view_exists(
    runtime: &IcebergControlRuntime,
    namespace: &str,
    view: &str,
) -> Result<bool, String> {
    // Same reasoning as `list_views`: a catalog that cannot create views holds
    // none, so existence is answerable without a view catalog.
    if runtime.rest_catalog().is_none() {
        return Ok(false);
    }
    ensure_rest(runtime)?;
    let (_, ident) = view_ident(namespace, view)?;
    let catalog = runtime.catalog().clone();
    runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.view_exists(&ident).await })?
        .map_err(|error| format!("check Iceberg view: {error}"))
}

pub(crate) fn list_views(
    runtime: &IcebergControlRuntime,
    namespace: &str,
) -> Result<Vec<String>, String> {
    // A catalog that cannot create views cannot contain any, so enumerating
    // them answers "none" rather than failing. Callers that merely need to know
    // whether a namespace holds views -- DROP DATABASE ... FORCE is the one
    // that matters -- must not be turned into a hard error by a catalog kind
    // they never asked about. Mutating view operations still fail loudly.
    if runtime.rest_catalog().is_none() {
        return Ok(Vec::new());
    }
    ensure_rest(runtime)?;
    let namespace = NamespaceIdent::new(normalize_identifier(namespace)?);
    let catalog = runtime.catalog().clone();
    let mut views = runtime
        .resources()
        .catalog_runtime()
        .block_on(async move { catalog.list_views(&namespace).await })?
        .map_err(|error| format!("list Iceberg views: {error}"))?
        .into_iter()
        .map(|ident| ident.name)
        .collect::<Vec<_>>();
    views.sort();
    Ok(views)
}

fn map_view_error(ident: &TableIdent, action: &str, error: impl std::fmt::Display) -> String {
    let message = error.to_string();
    if message.contains("view that does not exist") {
        format!("unknown view: {ident}")
    } else {
        format!("{action} REST Iceberg view {ident}: {message}")
    }
}

fn loaded_view_from_metadata(
    ident: &TableIdent,
    metadata: &ViewMetadata,
) -> Result<LoadedIcebergView, String> {
    let version = metadata.current_version();
    let mut selected = None;
    for representation in version.representations().iter() {
        let ViewRepresentation::Sql(sql) = representation;
        if sql.dialect.eq_ignore_ascii_case(VIEW_DIALECT_STARROCKS) {
            selected = Some(sql);
            break;
        }
        selected.get_or_insert(sql);
    }
    let selected =
        selected.ok_or_else(|| format!("Iceberg view {ident} has no SQL representation"))?;
    let default_namespace = version
        .default_namespace()
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(".");
    let properties = metadata.properties().clone();
    let source_format = source_format_from_summary(ident, version.summary())?;
    Ok(LoadedIcebergView {
        raw_sql: selected.sql.clone(),
        dialect: selected.dialect.clone(),
        default_catalog: version.default_catalog().cloned(),
        default_namespace,
        source_format,
        column_names: metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect(),
        comment: properties.get("comment").cloned(),
        properties,
    })
}

use crate::control_provider::IcebergControlProvider;
use novarocks_spi::connector::{
    ConnectorError, ConnectorErrorKind, ConnectorInstanceDescriptor, ConnectorInstanceIncarnation,
    ConnectorListViewsRequest, ConnectorViewDialect, ConnectorViewIdentity, ConnectorViewMetadata,
    ConnectorViewMetadataValue, ConnectorViewRequest,
};

impl ConnectorViewMetadata for IcebergControlProvider {
    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        self.descriptor()
    }

    fn incarnation(&self) -> ConnectorInstanceIncarnation {
        self.incarnation()
    }

    fn view_exists(&self, request: ConnectorViewRequest) -> Result<bool, ConnectorError> {
        ensure_request(self, &request.view.instance_id, &request.context)?;
        view_exists(self.runtime(), &request.view.namespace, &request.view.view)
            .map_err(unavailable)
    }

    fn load_view(
        &self,
        request: ConnectorViewRequest,
    ) -> Result<ConnectorViewMetadataValue, ConnectorError> {
        ensure_request(self, &request.view.instance_id, &request.context)?;
        let loaded = load_view(self.runtime(), &request.view.namespace, &request.view.view)
            .map_err(unavailable)?;
        if !loaded.dialect.eq_ignore_ascii_case(VIEW_DIALECT_STARROCKS) {
            return Err(ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                format!(
                    "Iceberg view uses unsupported SQL dialect {}",
                    loaded.dialect
                ),
            ));
        }
        ConnectorViewMetadataValue::try_new(
            request.view,
            ConnectorViewDefinition {
                dialect: ConnectorViewDialect::StarRocks,
                raw_sql: loaded.raw_sql.into(),
                default_catalog: loaded.default_catalog.map(Into::into),
                default_namespace: loaded.default_namespace.into(),
                source_format: loaded.source_format,
            },
            loaded.column_names.into_iter().map(Into::into).collect(),
            loaded.comment.map(Into::into),
            loaded
                .properties
                .into_iter()
                .map(|(key, value)| (key.into(), value.into()))
                .collect(),
            &request.context,
        )
    }

    fn list_views(
        &self,
        request: ConnectorListViewsRequest,
    ) -> Result<Vec<ConnectorViewIdentity>, ConnectorError> {
        ensure_request(self, &request.namespace.instance_id, &request.context)?;
        list_views(self.runtime(), &request.namespace.namespace)
            .map_err(unavailable)?
            .into_iter()
            .map(|view| {
                Ok(ConnectorViewIdentity {
                    instance_id: self.descriptor().instance_id.clone(),
                    namespace: request.namespace.namespace.clone(),
                    view: view.into(),
                })
            })
            .collect()
    }
}

fn ensure_request(
    provider: &IcebergControlProvider,
    owner: &novarocks_spi::connector::ConnectorInstanceId,
    context: &novarocks_spi::connector::ConnectorRequestContext,
) -> Result<(), ConnectorError> {
    if owner != &provider.descriptor().instance_id {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "Iceberg view request belongs to another connector instance",
        ));
    }
    if context.cancellation().is_cancelled() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Cancelled,
            "connector request was cancelled",
        ));
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ConnectorError::new(
            ConnectorErrorKind::DeadlineExceeded,
            "connector request deadline elapsed",
        ));
    }
    Ok(())
}

fn unavailable(error: String) -> ConnectorError {
    let kind = if error.starts_with("unknown view:") {
        ConnectorErrorKind::NotFound
    } else if error.starts_with("corrupt NovaRocks Iceberg view") {
        ConnectorErrorKind::CorruptData
    } else if error.contains("require a REST")
        || error.contains("unsupported SQL dialect")
        || error.starts_with("unsupported NovaRocks Iceberg view")
    {
        ConnectorErrorKind::Unsupported
    } else {
        ConnectorErrorKind::Unavailable
    };
    ConnectorError::new(kind, error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::access_binding::IcebergReadBinding;
    use crate::catalog_control::IcebergCatalogControlState;
    use crate::resources::IcebergControlResources;

    fn novarocks_definition(raw_sql: &str) -> ConnectorViewDefinition {
        ConnectorViewDefinition {
            dialect: ConnectorViewDialect::StarRocks,
            raw_sql: raw_sql.into(),
            default_catalog: Some("source_catalog".into()),
            default_namespace: "source_namespace".into(),
            source_format: Some(ConnectorViewSourceFormat::EffectiveUserSourceV1),
        }
    }

    fn hadoop_runtime() -> (
        tokio::runtime::Runtime,
        tempfile::TempDir,
        IcebergControlRuntime,
    ) {
        let executor = tokio::runtime::Runtime::new().expect("runtime");
        let warehouse = tempfile::tempdir().expect("warehouse");
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "ice",
            &[(
                "iceberg.catalog.warehouse".to_string(),
                warehouse.path().display().to_string(),
            )],
        )
        .expect("configuration");
        let binding = IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(
                executor.handle().clone(),
            )),
            Arc::new(novarocks_fs::TokioFileTaskSpawner::new(
                executor.handle().clone(),
            )),
        );
        let runtime = IcebergControlRuntime::try_new(
            IcebergCatalogControlState::new(configuration),
            IcebergControlResources::new(binding, executor.handle().clone()),
        )
        .expect("control runtime");
        (executor, warehouse, runtime)
    }

    #[test]
    fn hadoop_rejects_view_mutations_and_loads_but_answers_empty_probes() {
        let (_executor, _warehouse, runtime) = hadoop_runtime();
        let assert_gate = |result: Result<(), String>| {
            let error = result.expect_err("Hadoop must not expose a view operation");
            assert!(error.contains("require a REST"), "{error}");
        };

        assert_gate(create_view(
            &runtime,
            "db",
            "v",
            &[],
            &novarocks_definition("select 1"),
            None,
            false,
            &[],
        ));
        assert_gate(drop_view(&runtime, "db", "v"));
        assert_gate(load_view(&runtime, "db", "v").map(|_| ()));
        assert!(!view_exists(&runtime, "db", "v").expect("Hadoop view probe"));
        assert!(
            list_views(&runtime, "db")
                .expect("Hadoop view listing probe")
                .is_empty()
        );
    }

    #[test]
    fn view_error_mapping_keeps_capability_and_lookup_failures_typed() {
        assert_eq!(
            unavailable("view operations require a REST iceberg catalog".to_string()).kind(),
            ConnectorErrorKind::Unsupported
        );
        assert_eq!(
            unavailable("unsupported SQL dialect: spark".to_string()).kind(),
            ConnectorErrorKind::Unsupported
        );
        assert_eq!(
            unavailable("unknown view: db.v".to_string()).kind(),
            ConnectorErrorKind::NotFound
        );
        assert_eq!(
            unavailable("corrupt NovaRocks Iceberg view db.v: missing format".to_string()).kind(),
            ConnectorErrorKind::CorruptData
        );
        assert_eq!(
            unavailable("unsupported NovaRocks Iceberg view db.v: unknown format".to_string())
                .kind(),
            ConnectorErrorKind::Unsupported
        );
        assert_eq!(
            unavailable("REST response lost".to_string()).kind(),
            ConnectorErrorKind::Unavailable
        );
    }

    #[test]
    fn novarocks_view_source_context_is_frozen_independently_of_target() {
        let definition = novarocks_definition("SELECT  /* preserve */ 1");
        let (catalog, namespace, format) =
            novarocks_source_context(&definition).expect("valid NovaRocks source context");

        assert_eq!(catalog, "source_catalog");
        assert_eq!(namespace.to_string(), "source_namespace");
        assert_eq!(format, EFFECTIVE_USER_SOURCE_V1);
        assert_eq!(definition.raw_sql.as_ref(), "SELECT  /* preserve */ 1");
    }

    #[test]
    fn provenance_validation_rejects_old_novarocks_and_accepts_third_party_views() {
        let ident = TableIdent::from_strs(["db", "v"]).expect("view ident");
        let third_party = HashMap::new();
        assert_eq!(
            source_format_from_summary(&ident, &third_party).expect("third-party view"),
            None
        );

        let old_novarocks =
            HashMap::from([("engine-name".to_string(), NOVAROCKS_ENGINE_NAME.to_string())]);
        let error = source_format_from_summary(&ident, &old_novarocks)
            .expect_err("NovaRocks provenance without format is corrupt");
        assert!(
            error.starts_with("corrupt NovaRocks Iceberg view"),
            "{error}"
        );

        let unknown_novarocks = HashMap::from([
            ("engine-name".to_string(), NOVAROCKS_ENGINE_NAME.to_string()),
            (
                NOVAROCKS_SOURCE_FORMAT_KEY.to_string(),
                "future-v2".to_string(),
            ),
        ]);
        let error = source_format_from_summary(&ident, &unknown_novarocks)
            .expect_err("unknown NovaRocks format is unsupported");
        assert!(
            error.starts_with("unsupported NovaRocks Iceberg view"),
            "{error}"
        );
    }
}
