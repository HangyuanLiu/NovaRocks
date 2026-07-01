//! Lake-native Iceberg MV package discovery.
//!
//! W1 keeps SQLite as the runtime authority, but the lake already carries
//! enough structure to enumerate MV packages: projection views point at storage
//! tables, and storage tables carry the inline descriptor.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::connector::iceberg::catalog::IcebergLoadedTable;
use crate::connector::iceberg::catalog::registry::{
    IcebergCatalogEntry, IcebergCatalogKind, list_tables, load_table,
};
use crate::engine::StandaloneState;
use crate::engine::mv::iceberg_refresh::{
    NR_MV_STORAGE_PREFIX, NR_MV_VIEW_MARKER_PROP, NR_MV_VIEW_STORAGE_TABLE_PROP, nr_mv_public_name,
};
use crate::meta::repository::mv_descriptor::{MV_DESCRIPTOR_INLINE_PROP, MvDescriptorV1};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergMvDiscoverySource {
    ProjectionView,
    StorageTable,
}

#[derive(Clone, Debug)]
pub(crate) struct DiscoveredIcebergMv {
    pub(crate) catalog: String,
    pub(crate) namespace: String,
    pub(crate) public_name: String,
    pub(crate) storage_table: String,
    pub(crate) descriptor: MvDescriptorV1,
    pub(crate) source: IcebergMvDiscoverySource,
}

pub(crate) fn discover_iceberg_mvs(
    state: &Arc<StandaloneState>,
    catalog: &str,
    namespace: &str,
) -> Result<Vec<DiscoveredIcebergMv>, String> {
    let entry = {
        let catalogs = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        catalogs.get(catalog)?
    };
    discover_iceberg_mvs_from_entry(&entry, catalog, namespace)
}

pub(crate) fn discover_iceberg_mvs_from_entry(
    entry: &IcebergCatalogEntry,
    catalog: &str,
    namespace: &str,
) -> Result<Vec<DiscoveredIcebergMv>, String> {
    let mut discovered = Vec::new();
    let mut seen_storage = BTreeSet::new();
    discover_from_projection_views(
        entry,
        catalog,
        namespace,
        &mut seen_storage,
        &mut discovered,
    )?;
    discover_from_storage_tables(
        entry,
        catalog,
        namespace,
        &mut seen_storage,
        &mut discovered,
    )?;
    discovered.sort_by(|left, right| {
        left.namespace
            .cmp(&right.namespace)
            .then(left.public_name.cmp(&right.public_name))
            .then(left.storage_table.cmp(&right.storage_table))
    });
    Ok(discovered)
}

fn discover_from_projection_views(
    entry: &IcebergCatalogEntry,
    catalog: &str,
    namespace: &str,
    seen_storage: &mut BTreeSet<(String, String)>,
    discovered: &mut Vec<DiscoveredIcebergMv>,
) -> Result<(), String> {
    if !matches!(entry.kind, IcebergCatalogKind::Rest) {
        return Ok(());
    }
    for view_name in crate::connector::iceberg::catalog::views::list_views(entry, namespace)? {
        let view =
            crate::connector::iceberg::catalog::views::load_view(entry, namespace, &view_name)?;
        if view
            .properties
            .get(NR_MV_VIEW_MARKER_PROP)
            .map(String::as_str)
            != Some("true")
        {
            continue;
        }
        let storage_pointer = view
            .properties
            .get(NR_MV_VIEW_STORAGE_TABLE_PROP)
            .ok_or_else(|| {
                format!(
                    "Iceberg MV projection view {catalog}.{namespace}.{view_name} is missing `{NR_MV_VIEW_STORAGE_TABLE_PROP}`"
                )
            })?;
        let (storage_namespace, storage_table) = parse_storage_table_pointer(storage_pointer)?;
        let descriptor = load_required_mv_descriptor(entry, &storage_namespace, &storage_table)?;
        let key = (storage_namespace.clone(), storage_table.clone());
        if seen_storage.insert(key) {
            discovered.push(DiscoveredIcebergMv {
                catalog: catalog.to_string(),
                namespace: storage_namespace,
                public_name: view_name,
                storage_table,
                descriptor,
                source: IcebergMvDiscoverySource::ProjectionView,
            });
        }
    }
    Ok(())
}

fn discover_from_storage_tables(
    entry: &IcebergCatalogEntry,
    catalog: &str,
    namespace: &str,
    seen_storage: &mut BTreeSet<(String, String)>,
    discovered: &mut Vec<DiscoveredIcebergMv>,
) -> Result<(), String> {
    let tables = list_tables(entry, namespace)?;
    for storage_table in tables {
        if !storage_table.starts_with(NR_MV_STORAGE_PREFIX) {
            continue;
        }
        let key = (namespace.to_string(), storage_table.clone());
        if seen_storage.contains(&key) {
            continue;
        }
        let loaded = load_table(entry, namespace, &storage_table)?;
        let Some(descriptor) = descriptor_from_loaded_table(&loaded)? else {
            continue;
        };
        seen_storage.insert(key);
        let public_name = public_name_from_descriptor(&descriptor)
            .or_else(|| nr_mv_public_name(&storage_table))
            .unwrap_or_else(|| storage_table.clone());
        discovered.push(DiscoveredIcebergMv {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            public_name,
            storage_table,
            descriptor,
            source: IcebergMvDiscoverySource::StorageTable,
        });
    }
    Ok(())
}

fn descriptor_from_loaded_table(
    loaded: &IcebergLoadedTable,
) -> Result<Option<MvDescriptorV1>, String> {
    let props = loaded.table.metadata().properties();
    if !props.contains_key(MV_DESCRIPTOR_INLINE_PROP) {
        return Ok(None);
    }
    MvDescriptorV1::from_storage_properties(props).map(Some)
}

fn load_required_mv_descriptor(
    entry: &IcebergCatalogEntry,
    namespace: &str,
    storage_table: &str,
) -> Result<MvDescriptorV1, String> {
    let loaded = load_table(entry, namespace, storage_table)?;
    descriptor_from_loaded_table(&loaded)?.ok_or_else(|| {
        format!(
            "Iceberg MV storage table {namespace}.{storage_table} is missing descriptor properties"
        )
    })
}

fn parse_storage_table_pointer(pointer: &str) -> Result<(String, String), String> {
    let mut parts = pointer.split('.');
    let namespace = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| format!("invalid MV storage table pointer `{pointer}`"))?;
    let table = parts
        .next()
        .filter(|part| !part.is_empty())
        .ok_or_else(|| format!("invalid MV storage table pointer `{pointer}`"))?;
    if parts.next().is_some() {
        return Err(format!(
            "invalid MV storage table pointer `{pointer}`; W1 supports single-level namespaces"
        ));
    }
    Ok((namespace.to_string(), table.to_string()))
}

fn public_name_from_descriptor(descriptor: &MvDescriptorV1) -> Option<String> {
    descriptor
        .public_view
        .rsplit('.')
        .next()
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}
