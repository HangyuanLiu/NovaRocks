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

use crate::catalog_application::query_catalog::QueryCatalogService;
use crate::dml::error::DmlError;
use crate::statistics::{FrontendStatisticsService, StatisticsColumn};

/// Stateless Frontend facade for DML statement families.
///
/// The service retains only local statistics observation. Publication attempts,
/// provider sessions, and terminal evidence belong to the current request.
pub struct DmlService {
    statistics: Arc<FrontendStatisticsService>,
    local_catalog: RwLock<Option<Arc<QueryCatalogService>>>,
}

impl DmlService {
    pub fn new(statistics: Arc<FrontendStatisticsService>) -> Self {
        Self {
            statistics,
            local_catalog: RwLock::new(None),
        }
    }

    pub(crate) fn statistics(&self) -> &FrontendStatisticsService {
        self.statistics.as_ref()
    }

    /// Install the Frontend-local catalog used only for local statistics after
    /// a successful statement. This cannot resolve external metadata.
    pub(crate) fn install_local_catalog(&self, catalog: Arc<QueryCatalogService>) {
        *self
            .local_catalog
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(catalog);
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn local_statistics_columns(
        &self,
        database: &str,
        table: &str,
    ) -> Result<Option<Vec<StatisticsColumn>>, DmlError> {
        let catalog = self
            .local_catalog
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(catalog) = catalog else {
            return Ok(None);
        };
        match local_table_columns(catalog.as_ref(), database, table) {
            Ok(columns) => Ok(Some(columns)),
            Err(error)
                if error.starts_with("unknown database:")
                    || error.starts_with("unknown table:") =>
            {
                Ok(None)
            }
            Err(error) => Err(DmlError::executor(error)),
        }
    }
}

fn local_table_columns(
    catalog_service: &QueryCatalogService,
    database: &str,
    table: &str,
) -> Result<Vec<StatisticsColumn>, String> {
    let catalog = catalog_service
        .local()
        .read()
        .expect("frontend local catalog read lock");
    let table = novarocks_sql::planning::catalog::local_catalog_table(&catalog, database, table)?;
    Ok(table
        .columns
        .iter()
        .map(|column| StatisticsColumn {
            name: column.name.clone(),
            data_type: column.data_type.clone(),
        })
        .collect())
}
