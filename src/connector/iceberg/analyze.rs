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

//! NovaRocks-side `compute_table_stats` equivalent: scan an iceberg table's
//! current snapshot, build per-column Theta sketches, write a Puffin
//! `StatisticsFile`, and register it via a metadata-only commit. Returns the
//! per-column NDV estimates (lowercased column name -> ndv).
//!
//! This mirrors Spark's `compute_table_stats` procedure. It is intended to back
//! `ANALYZE TABLE` for iceberg tables (wired in a later task). Non-iceberg
//! targets return an empty map so the caller keeps its in-memory-only stats
//! path.

use std::collections::HashMap;
use std::sync::Arc;

use crate::connector::iceberg::catalog::registry::{block_on_iceberg, build_iceberg_catalog};
use crate::connector::iceberg::commit::statistics::commit_statistics_file;
use crate::connector::iceberg::sink::collect_theta_sketches_by_name;
use crate::connector::iceberg::stats_assembler::{puffin_path_for_snapshot, write_puffin};
use crate::connector::iceberg::theta_sketch::ThetaSketchHandle;
use crate::engine::StandaloneState;
use crate::sql::parser::ast::ObjectName;

/// Scan `name`'s current snapshot, compute per-column Theta sketches over
/// `columns`, write a Puffin `StatisticsFile`, register it via a metadata-only
/// `update_statistics` commit, and return the per-column NDV estimates keyed by
/// lowercased column name.
///
/// Returns an empty map (no error) when the target is not an iceberg table,
/// when the table has no current snapshot, or when no sketchable column data
/// was produced — these are all "nothing to register" outcomes rather than
/// failures. Resolution, scan, write, and commit errors propagate.
pub(crate) fn analyze_iceberg_puffin_stats(
    state: &Arc<StandaloneState>,
    current_catalog: Option<&str>,
    current_database: &str,
    name: &ObjectName,
    columns: &[String],
) -> Result<HashMap<String, f64>, String> {
    // 1. Resolve the backend target. Non-iceberg backends keep the caller's
    //    in-memory-only statistics path; nothing to register here.
    let target = crate::engine::backend_resolver::resolve_table_target(
        state,
        name,
        current_catalog,
        current_database,
    )?;
    if target.backend_name != "iceberg" {
        return Ok(HashMap::new());
    }

    // 2. Load the iceberg table (for metadata + FileIO) and build a catalog
    //    handle for the eventual metadata-only commit. Both come from the same
    //    catalog entry, matching the INSERT path in `iceberg_writer.rs`.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    let catalog: Arc<dyn iceberg::Catalog> = build_iceberg_catalog(&entry)?;
    let loaded =
        crate::connector::iceberg::catalog::load_table(&entry, &target.namespace, &target.table)?;
    let metadata = loaded.table.metadata();

    let Some(snapshot) = metadata.current_snapshot() else {
        // Empty table / no snapshot: nothing to summarize.
        return Ok(HashMap::new());
    };
    let snapshot_id = snapshot.snapshot_id();
    let sequence_number = snapshot.sequence_number();

    // 3. Build the lowercased column-name -> field_id map from the current
    //    schema. Lowercased keys match `collect_theta_sketches_by_name`.
    let name_to_field_id = name_to_field_id_from_metadata(metadata);
    if name_to_field_id.is_empty() {
        return Ok(HashMap::new());
    }

    // 4. Full-table scan of the requested columns. Route through the
    //    catalog-service provider so the Iceberg table resolves even though it
    //    is not registered in the local planner catalog (the ANALYZE handler does not
    //    go through the SELECT query-prep flow).
    let sql = build_scan_sql(&target.catalog, &target.namespace, &target.table, columns);
    let query = crate::sql::parser::parse_normalized_sql_raw(&sql)
        .map_err(|e| format!("analyze scan parse failed: {e}"))?;
    let sqlparser::ast::Statement::Query(query) = query else {
        return Err("analyze scan did not parse as a query".to_string());
    };
    let result = crate::engine::execute_query_with_catalog_service(
        state,
        Some(target.catalog.as_str()),
        &target.namespace,
        &query,
        None,
    )?;

    // 5. Accumulate one sketch per field id across all result chunks, then
    //    union per field into a single sketch.
    let mut per_field: HashMap<i32, Vec<ThetaSketchHandle>> = HashMap::new();
    for chunk in &result.chunks {
        for (field_id, sketch) in collect_theta_sketches_by_name(&chunk.batch, &name_to_field_id) {
            per_field.entry(field_id).or_default().push(sketch);
        }
    }
    let sketches = union_per_field(per_field);
    if sketches.is_empty() {
        return Ok(HashMap::new());
    }

    // 6. Write the Puffin file and register it via a metadata-only commit.
    //    Both are async; drive them from this synchronous flow with the same
    //    `block_on_iceberg` helper the INSERT/commit path uses.
    let file_io = loaded.table.file_io().clone();
    let puffin_path = puffin_path_for_snapshot(metadata, snapshot_id);
    let stats_file = block_on_iceberg(write_puffin(
        &file_io,
        &puffin_path,
        snapshot_id,
        sequence_number,
        &sketches,
    ))??;
    let Some(stats_file) = stats_file else {
        // No primitive-column blobs were emitted; nothing to register.
        return Ok(HashMap::new());
    };
    block_on_iceberg(commit_statistics_file(
        &loaded.table,
        catalog.as_ref(),
        stats_file,
    ))??;

    // Invalidate the cached table metadata (registry table_cache + catalog-mgr)
    // so a query in the SAME session reloads the snapshot WITH the
    // freshly-committed StatisticsFile. Mirrors the INSERT/CTAS/TRUNCATE commit
    // paths; without it, ANALYZE-then-query in one session keeps the stale
    // (no-stats) metadata and the optimizer falls back to the many-to-many NDV
    // estimate.
    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, &target)?;

    // 7. Translate field_id -> ndv back to lowercased column names.
    Ok(ndv_by_name(&sketches, &name_to_field_id))
}

/// Build the lowercased column-name -> field_id map from the table's current
/// schema. Mirrors `connector::iceberg::stats` field-id mapping so the
/// keys line up with `collect_theta_sketches_by_name`.
fn name_to_field_id_from_metadata(metadata: &iceberg::spec::TableMetadata) -> HashMap<String, i32> {
    let mut map = HashMap::new();
    for field in metadata.current_schema().as_struct().fields() {
        map.insert(field.name.to_lowercase(), field.id);
    }
    map
}

/// Build the `select <cols> from <catalog>.<namespace>.<table>` scan SQL,
/// backtick-quoting every identifier (doubling embedded backticks).
fn build_scan_sql(catalog: &str, namespace: &str, table: &str, columns: &[String]) -> String {
    let col_list = columns
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "select {col_list} from {}.{}.{}",
        quote_ident(catalog),
        quote_ident(namespace),
        quote_ident(table),
    )
}

fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

/// Union the accumulated per-field sketches into a single sketch per field id.
fn union_per_field(
    per_field: HashMap<i32, Vec<ThetaSketchHandle>>,
) -> HashMap<i32, ThetaSketchHandle> {
    per_field
        .into_iter()
        .map(|(field_id, list)| {
            let refs: Vec<&ThetaSketchHandle> = list.iter().collect();
            (field_id, ThetaSketchHandle::union(&refs))
        })
        .collect()
}

/// Translate per-field-id sketch estimates back to lowercased column names.
/// Field ids without a name in the map are dropped.
fn ndv_by_name(
    sketches: &HashMap<i32, ThetaSketchHandle>,
    name_to_field_id: &HashMap<String, i32>,
) -> HashMap<String, f64> {
    let field_id_to_name: HashMap<i32, &String> =
        name_to_field_id.iter().map(|(n, id)| (*id, n)).collect();
    sketches
        .iter()
        .filter_map(|(field_id, sketch)| {
            field_id_to_name
                .get(field_id)
                .map(|name| ((*name).clone(), sketch.estimate()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_scan_sql_quotes_and_doubles_backticks() {
        let sql = build_scan_sql(
            "cat",
            "ns",
            "tbl",
            &["a".to_string(), "weird`col".to_string()],
        );
        assert_eq!(sql, "select `a`, `weird``col` from `cat`.`ns`.`tbl`");
    }

    #[test]
    fn build_scan_sql_handles_single_column() {
        let sql = build_scan_sql("c", "d", "t", &["only".to_string()]);
        assert_eq!(sql, "select `only` from `c`.`d`.`t`");
    }

    #[test]
    fn union_per_field_unions_all_sketches_for_a_field() {
        // Two batches of the same field, disjoint value ranges -> union NDV
        // should reflect the combined distinct count, larger than either part.
        let mut a = ThetaSketchHandle::new(12);
        for v in 0i64..500 {
            a.update(v);
        }
        let mut b = ThetaSketchHandle::new(12);
        for v in 500i64..1000 {
            b.update(v);
        }
        let a_est = a.estimate();
        let mut per_field: HashMap<i32, Vec<ThetaSketchHandle>> = HashMap::new();
        per_field.insert(7, vec![a, b]);

        let unioned = union_per_field(per_field);
        assert_eq!(unioned.len(), 1);
        let combined = unioned.get(&7).expect("field 7 present").estimate();
        // The union of two disjoint 500-value sketches must exceed either part
        // and approach 1000.
        assert!(
            combined > a_est,
            "union estimate {combined} should exceed single-part estimate {a_est}"
        );
        assert!(
            combined > 800.0,
            "union of two disjoint 500-value sketches should approach 1000, got {combined}"
        );
    }

    #[test]
    fn ndv_by_name_maps_field_ids_back_to_names_and_drops_unknown() {
        let mut s = ThetaSketchHandle::new(12);
        for v in 0i64..10 {
            s.update(v);
        }
        let est = s.estimate();
        let mut sketches: HashMap<i32, ThetaSketchHandle> = HashMap::new();
        sketches.insert(1, s);
        // Field id 99 has a sketch but no name mapping -> dropped.
        let mut orphan = ThetaSketchHandle::new(12);
        orphan.update(0i64);
        sketches.insert(99, orphan);

        let mut name_to_field_id = HashMap::new();
        name_to_field_id.insert("col_a".to_string(), 1);
        name_to_field_id.insert("col_b".to_string(), 2);

        let by_name = ndv_by_name(&sketches, &name_to_field_id);
        assert_eq!(by_name.len(), 1, "only mapped field ids survive");
        let got = by_name.get("col_a").copied().expect("col_a present");
        assert!((got - est).abs() < f64::EPSILON);
        assert!(!by_name.contains_key("col_b"), "no sketch for col_b");
    }
}
