//! Lake-native Iceberg MV package discovery.
//!
//! The lake carries enough structure to enumerate MV packages: MV target tables
//! carry the inline descriptor.

use std::collections::BTreeSet;
use std::sync::Arc;

use crate::connector::iceberg::catalog::IcebergLoadedTable;
use crate::connector::iceberg::catalog::registry::{IcebergCatalogEntry, list_tables, load_table};
use crate::engine::StandaloneState;
use crate::meta::repository::mv_descriptor::{MV_DESCRIPTOR_INLINE_PROP, MvDescriptorV1};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergMvDiscoverySource {
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

fn discover_from_storage_tables(
    entry: &IcebergCatalogEntry,
    catalog: &str,
    namespace: &str,
    seen_storage: &mut BTreeSet<(String, String)>,
    discovered: &mut Vec<DiscoveredIcebergMv>,
) -> Result<(), String> {
    let tables = list_tables(entry, namespace)?;
    for storage_table in tables {
        let key = (namespace.to_string(), storage_table.clone());
        if seen_storage.contains(&key) {
            continue;
        }
        let loaded = load_table(entry, namespace, &storage_table)?;
        let Some(descriptor) = descriptor_from_loaded_table(&loaded)? else {
            continue;
        };
        let expected_package_id = format!("{namespace}.{storage_table}");
        if descriptor.package_id != expected_package_id {
            return Err(format!(
                "Iceberg MV descriptor package id mismatch for discovered table {catalog}.{namespace}.{storage_table}: expected {expected_package_id}, got {}",
                descriptor.package_id
            ));
        }
        seen_storage.insert(key);
        discovered.push(DiscoveredIcebergMv {
            catalog: catalog.to_string(),
            namespace: namespace.to_string(),
            public_name: storage_table.clone(),
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
