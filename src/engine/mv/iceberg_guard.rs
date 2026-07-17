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
use std::sync::Arc;

use crate::engine::StandaloneState;
use crate::engine::backend_resolver::TargetBackend;
use crate::mv::persistence::descriptor::MV_DESCRIPTOR_PACKAGE_ID_PROP;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IcebergMvUserMutation {
    Insert,
    Update,
    Delete,
    Merge,
    Truncate,
    DropTable,
    AlterTable,
}

impl IcebergMvUserMutation {
    fn guidance(self) -> &'static str {
        match self {
            IcebergMvUserMutation::Insert
            | IcebergMvUserMutation::Update
            | IcebergMvUserMutation::Delete
            | IcebergMvUserMutation::Merge
            | IcebergMvUserMutation::Truncate => "use REFRESH MATERIALIZED VIEW to update it",
            IcebergMvUserMutation::DropTable => "use DROP MATERIALIZED VIEW",
            IcebergMvUserMutation::AlterTable => {
                "use ALTER MATERIALIZED VIEW for MV metadata changes"
            }
        }
    }
}

pub(crate) fn is_iceberg_mv_table_properties(props: &HashMap<String, String>) -> bool {
    props.contains_key(MV_DESCRIPTOR_PACKAGE_ID_PROP)
}

pub(crate) fn reject_if_iceberg_mv_properties(
    target: &TargetBackend,
    props: &HashMap<String, String>,
    mutation: IcebergMvUserMutation,
) -> Result<(), String> {
    if target.backend_name == "iceberg" && is_iceberg_mv_table_properties(props) {
        return Err(format!(
            "table {}.{}.{} is a materialized view; {}",
            target.catalog,
            target.namespace,
            target.table,
            mutation.guidance()
        ));
    }
    Ok(())
}

pub(crate) fn reject_if_iceberg_mv_table(
    state: &Arc<StandaloneState>,
    target: &TargetBackend,
    mutation: IcebergMvUserMutation,
) -> Result<(), String> {
    if target.backend_name != "iceberg" {
        return Ok(());
    }

    let entry = {
        let catalogs = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        catalogs.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let loaded = crate::connector::iceberg::catalog::registry::load_table(
        &entry,
        &target.namespace,
        &target.table,
    )?;
    reject_if_iceberg_mv_properties(target, loaded.table.metadata().properties(), mutation)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn iceberg_target() -> TargetBackend {
        TargetBackend {
            backend_name: "iceberg",
            catalog: "ice".to_string(),
            namespace: "analytics".to_string(),
            table: "mv_orders".to_string(),
        }
    }

    #[test]
    fn property_guard_allows_plain_iceberg_tables() {
        let props = HashMap::new();

        reject_if_iceberg_mv_properties(&iceberg_target(), &props, IcebergMvUserMutation::Insert)
            .expect("plain iceberg tables should pass");
    }

    #[test]
    fn property_guard_rejects_mv_tables_with_operation_guidance() {
        let props = HashMap::from([(
            MV_DESCRIPTOR_PACKAGE_ID_PROP.to_string(),
            "analytics.mv_orders".to_string(),
        )]);

        let err = reject_if_iceberg_mv_properties(
            &iceberg_target(),
            &props,
            IcebergMvUserMutation::DropTable,
        )
        .expect_err("iceberg MV tables should reject direct user mutations");

        assert!(err.contains("ice.analytics.mv_orders"), "{err}");
        assert!(err.contains("materialized view"), "{err}");
        assert!(err.contains("DROP MATERIALIZED VIEW"), "{err}");
    }
}
