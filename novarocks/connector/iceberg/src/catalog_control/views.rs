// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with this
// work for additional information regarding copyright ownership.

use std::collections::HashMap;

use novarocks_catalog::identifier::normalize_identifier;
use novarocks_spi::connector::ConnectorColumnDefinition;

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

#[derive(Clone, Debug)]
pub(crate) struct LoadedIcebergView {
    pub sql: String,
    pub dialect: String,
    pub default_namespace: String,
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

pub(crate) fn create_view(
    runtime: &IcebergControlRuntime,
    namespace: &str,
    view: &str,
    columns: &[ConnectorColumnDefinition],
    sql: &str,
    comment: Option<&str>,
    replace: bool,
    extra_properties: &[(String, String)],
) -> Result<(), String> {
    ensure_rest(runtime)?;
    let (namespace_ident, ident) = view_ident(namespace, view)?;
    let schema = build_view_schema(columns)?;
    let representations =
        ViewRepresentations::new(vec![ViewRepresentation::Sql(SqlViewRepresentation {
            sql: sql.to_string(),
            dialect: VIEW_DIALECT_STARROCKS.to_string(),
        })]);
    let mut properties = extra_properties.iter().cloned().collect::<HashMap<_, _>>();
    if let Some(comment) = comment {
        properties.insert("comment".to_string(), comment.to_string());
    }
    let summary = HashMap::from([("engine-name".to_string(), "novarocks".to_string())]);
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
        );
    }

    let creation = ViewCreation::builder()
        .name(ident.name.clone())
        .location(None)
        .representations(representations)
        .schema(schema)
        .properties(properties)
        .default_namespace(namespace_ident.clone())
        .default_catalog(None)
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
        .with_default_catalog(None)
        .with_default_namespace(ident.namespace.clone())
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
    Ok(LoadedIcebergView {
        sql: selected.sql.clone(),
        dialect: selected.dialect.clone(),
        default_namespace,
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
    ConnectorListViewsRequest, ConnectorViewDefinition, ConnectorViewDialect,
    ConnectorViewIdentity, ConnectorViewMetadata, ConnectorViewMetadataValue, ConnectorViewRequest,
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
                sql: loaded.sql.into(),
            },
            loaded.default_namespace.into(),
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
    } else if error.contains("require a REST") || error.contains("unsupported SQL dialect") {
        ConnectorErrorKind::Unsupported
    } else {
        ConnectorErrorKind::Unavailable
    };
    ConnectorError::new(kind, error)
}
