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

use std::sync::{Arc, RwLock};

use crate::catalog::memory::{MemoryCatalog, MemoryCatalogEntry};
use crate::catalog::registry::CatalogRegistry;

pub(crate) struct CatalogService<L, M>
where
    L: MemoryCatalogEntry,
{
    local: Arc<RwLock<MemoryCatalog<L>>>,
    registry: RwLock<CatalogRegistry<M>>,
}

impl<L, M> CatalogService<L, M>
where
    L: MemoryCatalogEntry,
{
    pub(crate) fn new(local: Arc<RwLock<MemoryCatalog<L>>>, registry: CatalogRegistry<M>) -> Self {
        Self {
            local,
            registry: RwLock::new(registry),
        }
    }

    pub(crate) fn local(&self) -> &Arc<RwLock<MemoryCatalog<L>>> {
        &self.local
    }

    pub(crate) fn registry(&self) -> &RwLock<CatalogRegistry<M>> {
        &self.registry
    }

    pub(crate) fn local_snapshot(&self) -> MemoryCatalog<L> {
        self.local
            .read()
            .expect("catalog service local read lock")
            .clone()
    }

    pub(crate) fn registry_snapshot(&self) -> CatalogRegistry<M> {
        self.registry
            .read()
            .expect("catalog service registry read lock")
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};

    use super::CatalogService;
    use crate::catalog::identifier::TableIdentity;
    use crate::catalog::memory::{DEFAULT_DATABASE, MemoryCatalog, MemoryCatalogEntry};
    use crate::catalog::registry::{Catalog, CatalogRegistry};
    use crate::catalog::table::CatalogTable;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TestEntry {
        name: String,
        revision: u64,
    }

    impl TestEntry {
        fn new(name: &str, revision: u64) -> Self {
            Self {
                name: name.to_string(),
                revision,
            }
        }
    }

    impl MemoryCatalogEntry for TestEntry {
        fn table_name(&self) -> &str {
            &self.name
        }

        fn to_catalog_table(&self, catalog: &str, database: &str) -> CatalogTable {
            CatalogTable {
                identity: TableIdentity::new(catalog, database, &self.name),
                columns: vec![],
                hidden_columns: vec![],
            }
        }
    }

    struct TestCatalog;

    impl Catalog<u64> for TestCatalog {
        fn name(&self) -> &str {
            "named"
        }

        fn get_table_metadata(&self, _namespace: &str, _table: &str) -> Result<u64, String> {
            Ok(7)
        }
    }

    fn service() -> CatalogService<TestEntry, u64> {
        let local = Arc::new(RwLock::new(MemoryCatalog::default()));
        let mut registry = CatalogRegistry::new();
        registry.register(Arc::new(TestCatalog));
        CatalogService::new(local, registry)
    }

    #[test]
    fn exposes_the_shared_local_catalog_and_named_registry() {
        let service = service();
        let local = Arc::clone(service.local());

        local
            .write()
            .expect("local write lock")
            .register(DEFAULT_DATABASE, TestEntry::new("orders", 1))
            .expect("register local table");
        assert_eq!(
            service
                .local()
                .read()
                .expect("local read lock")
                .get(DEFAULT_DATABASE, "orders")
                .expect("local table"),
            TestEntry::new("orders", 1)
        );
        assert_eq!(
            service
                .registry()
                .read()
                .expect("registry read lock")
                .resolve("NAMED", "ns", "t"),
            Ok(7)
        );
    }

    #[test]
    fn local_snapshot_is_an_independent_point_in_time_clone() {
        let service = service();
        service
            .local()
            .write()
            .expect("local write lock")
            .register(DEFAULT_DATABASE, TestEntry::new("orders", 1))
            .expect("register first revision");

        let snapshot = service.local_snapshot();
        service
            .local()
            .write()
            .expect("local write lock")
            .register(DEFAULT_DATABASE, TestEntry::new("orders", 2))
            .expect("register second revision");

        assert_eq!(
            snapshot
                .get(DEFAULT_DATABASE, "orders")
                .expect("snapshot table"),
            TestEntry::new("orders", 1)
        );
        assert_eq!(
            service
                .local()
                .read()
                .expect("local read lock")
                .get(DEFAULT_DATABASE, "orders")
                .expect("live table"),
            TestEntry::new("orders", 2)
        );
    }

    #[test]
    fn registry_snapshot_clones_registry_membership() {
        let service = service();
        let mut snapshot = service.registry_snapshot();
        snapshot.unregister("named");

        assert_eq!(
            snapshot.resolve("named", "ns", "t"),
            Err("unknown catalog: named".to_string())
        );
        assert_eq!(
            service
                .registry()
                .read()
                .expect("registry read lock")
                .resolve("named", "ns", "t"),
            Ok(7)
        );
    }
}
