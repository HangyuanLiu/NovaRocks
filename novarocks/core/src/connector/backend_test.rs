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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::connector::ConnectorRegistry;
    use crate::connector::backend::{CatalogBackend, CreateTableRequest, ResolvedTable};

    struct DummyCatalog;

    impl CatalogBackend for DummyCatalog {
        fn name(&self) -> &'static str {
            "dummy"
        }

        fn namespace_exists(&self, _catalog: &str, _namespace: &str) -> Result<bool, String> {
            Ok(false)
        }

        fn create_namespace(&self, _catalog: &str, _namespace: &str) -> Result<(), String> {
            Ok(())
        }

        fn drop_namespace(
            &self,
            _catalog: &str,
            _namespace: &str,
            _force: bool,
        ) -> Result<(), String> {
            Ok(())
        }

        fn create_table(&self, _req: CreateTableRequest) -> Result<(), String> {
            Ok(())
        }

        fn table_exists(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
        ) -> Result<bool, String> {
            Ok(false)
        }

        fn drop_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
            _if_exists: bool,
        ) -> Result<(), String> {
            Ok(())
        }

        fn load_table(
            &self,
            _catalog: &str,
            _namespace: &str,
            _table: &str,
        ) -> Result<ResolvedTable, String> {
            Err("dummy".to_string())
        }
    }

    #[test]
    fn registry_registers_and_resolves_catalog_backend() {
        let mut registry = ConnectorRegistry::default();
        registry.register_catalog_backend(Arc::new(DummyCatalog));

        let backend = registry.catalog_backend("dummy").expect("resolve backend");
        assert_eq!(backend.name(), "dummy");
        assert!(registry.catalog_backend("missing").is_err());
    }
}
